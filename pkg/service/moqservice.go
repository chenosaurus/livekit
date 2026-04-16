package service

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils/guid"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

const (
	moqALPN              = "livekit-moq/0"
	moqFrameConnect byte = 0x01
	moqFrameReady   byte = 0x02
	moqFrameEvent   byte = 0x03
	moqFrameVideo   byte = 0x10
	moqFrameClose   byte = 0x11
	moqFrameError   byte = 0x7f
)

type MOQService struct {
	conf        *config.Config
	roomManager *RoomManager
	provider    auth.KeyProvider

	sessionMu sync.Mutex
	sessions  map[string]*moqSession

	listeners []*quic.Listener
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

type moqConnectRequest struct {
	Token     string `json:"token"`
	SessionID string `json:"session_id,omitempty"`
	Room      string `json:"room,omitempty"`
	Identity  string `json:"identity,omitempty"`
	Name      string `json:"name,omitempty"`
	TrackName string `json:"track_name,omitempty"`
	MimeType  string `json:"mime_type"`
	Width     uint32 `json:"width,omitempty"`
	Height    uint32 `json:"height,omitempty"`
	FrameRate uint32 `json:"framerate,omitempty"`
	Source    string `json:"source,omitempty"`
}

type moqReadyResponse struct {
	ParticipantID string `json:"participant_id"`
	SessionID     string `json:"session_id"`
	TrackID       string `json:"track_id"`
	MimeType      string `json:"mime_type"`
	Resumed       bool   `json:"resumed"`
}

type moqEvent struct {
	Type string `json:"type"`
}

func NewMOQService(conf *config.Config, roomManager *RoomManager, provider auth.KeyProvider) (*MOQService, error) {
	return &MOQService{
		conf:        conf,
		roomManager: roomManager,
		provider:    provider,
		sessions:    make(map[string]*moqSession),
	}, nil
}

func (s *MOQService) Start() error {
	if s == nil || !s.conf.MOQ.Enabled {
		return nil
	}

	tlsConfig, err := s.loadTLSConfig()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	binds := s.conf.MOQ.BindAddresses
	if len(binds) == 0 {
		binds = s.conf.BindAddresses
	}
	if len(binds) == 0 {
		binds = []string{""}
	}

	for _, bind := range binds {
		addr := net.JoinHostPort(bind, fmt.Sprintf("%d", s.conf.MOQ.Port))
		listener, err := quic.ListenAddr(addr, tlsConfig, &quic.Config{
			EnableDatagrams: false,
			KeepAlivePeriod: 15 * time.Second,
		})
		if err != nil {
			cancel()
			return err
		}
		s.listeners = append(s.listeners, listener)
		s.wg.Add(1)
		go s.acceptLoop(ctx, listener)
	}

	logger.Infow("MoQ service started", "port", s.conf.MOQ.Port, "bindAddresses", binds)
	return nil
}

func (s *MOQService) Stop() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	for _, listener := range s.listeners {
		_ = listener.Close()
	}
	s.wg.Wait()
}

func (s *MOQService) acceptLoop(ctx context.Context, listener *quic.Listener) {
	defer s.wg.Done()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			logger.Errorw("moq: failed to accept connection", err)
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(ctx, conn)
		}()
	}
}

func (s *MOQService) handleConnection(ctx context.Context, conn quic.Connection) {
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		logger.Warnw("moq: failed to accept control stream", err)
		return
	}
	defer stream.Close()

	kind, payload, err := readMOQFrame(stream)
	if err != nil {
		logger.Warnw("moq: failed to read connect frame", err)
		return
	}
	if kind != moqFrameConnect {
		_ = writeMOQJSONFrame(stream, moqFrameError, map[string]string{"error": "expected connect frame"})
		return
	}

	var req moqConnectRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = writeMOQJSONFrame(stream, moqFrameError, map[string]string{"error": "invalid connect payload"})
		return
	}

	session, attachmentID, ready, err := s.startSession(req, stream)
	if err != nil {
		_ = writeMOQJSONFrame(stream, moqFrameError, map[string]string{"error": err.Error()})
		return
	}
	defer session.detach(attachmentID)

	if err := writeMOQJSONFrame(stream, moqFrameReady, ready); err != nil {
		return
	}

	for {
		kind, payload, err = readMOQFrame(stream)
		if err != nil {
			if !errors.Is(err, io.EOF) && !strings.Contains(err.Error(), "closed") {
				session.participant.GetLogger().Warnw("moq: session read failed", err)
			}
			return
		}

		switch kind {
		case moqFrameVideo:
			if err := session.handleVideoFrame(payload); err != nil {
				session.participant.GetLogger().Warnw("moq: failed to handle frame", err)
				return
			}
		case moqFrameClose:
			session.close()
			return
		default:
			session.participant.GetLogger().Debugw("moq: ignoring unknown frame", "kind", kind)
		}
	}
}

type moqSession struct {
	id          string
	service     *MOQService
	room        *rtc.Room
	participant *rtc.ParticipantImpl
	publisher   *rtc.ExternalTrackPublisher

	identity livekit.ParticipantIdentity
	roomName livekit.RoomName
	mimeType string
	trackID  string

	lock         sync.Mutex
	stream       quic.Stream
	attachmentID uint64
	closed       bool
	expireTimer  *time.Timer
}

func (s *MOQService) startSession(req moqConnectRequest, stream quic.Stream) (*moqSession, uint64, *moqReadyResponse, error) {
	ctx, claims, _, err := s.verifyToken(req.Token)
	if err != nil {
		return nil, 0, nil, err
	}
	roomName, err := EnsureJoinPermission(ctx)
	if err != nil {
		return nil, 0, nil, err
	}
	if req.Room != "" && req.Room != string(roomName) {
		return nil, 0, nil, errors.New("connect room does not match token")
	}
	if claims.Identity == "" {
		return nil, 0, nil, errors.New("token identity is required")
	}
	if claims.Video == nil || !claims.Video.GetCanPublish() {
		return nil, 0, nil, ErrPermissionDenied
	}

	if req.SessionID != "" {
		session, attachmentID, err := s.resumeSession(req, roomName, claims, stream)
		if err != nil {
			return nil, 0, nil, err
		}
		return session, attachmentID, &moqReadyResponse{
			ParticipantID: string(session.participant.ID()),
			SessionID:     session.id,
			TrackID:       session.trackID,
			MimeType:      session.mimeType,
			Resumed:       true,
		}, nil
	}

	pi := routing.ParticipantInit{
		Identity:      livekit.ParticipantIdentity(claims.Identity),
		Name:          livekit.ParticipantName(claims.Name),
		AutoSubscribe: false,
		Client: &livekit.ClientInfo{
			Protocol: int32(types.CurrentProtocol),
			Sdk:      livekit.ClientInfo_RUST,
			Version:  "moq/0",
		},
		Grants: claims,
		CreateRoom: &livekit.CreateRoomRequest{
			Name:       string(roomName),
			RoomPreset: claims.RoomPreset,
		},
		AdaptiveStream: false,
		DisableICELite: true,
	}
	if req.Name != "" {
		pi.Name = livekit.ParticipantName(req.Name)
	}

	connID := livekit.ConnectionID(guid.New("MQ_"))
	if err := s.roomManager.StartSession(
		ctx,
		pi,
		routing.NewNullMessageSource(connID),
		routing.NewNullMessageSink(connID),
		true,
	); err != nil {
		return nil, 0, nil, err
	}

	room := s.roomManager.GetRoom(ctx, roomName)
	if room == nil {
		return nil, 0, nil, ErrRoomNotFound
	}
	lp := room.GetParticipant(pi.Identity)
	participant, ok := lp.(*rtc.ParticipantImpl)
	if !ok || participant == nil {
		return nil, 0, nil, ErrParticipantNotFound
	}

	addTrack := &livekit.AddTrackRequest{
		Cid:    guid.New("TR_"),
		Name:   req.TrackName,
		Type:   livekit.TrackType_VIDEO,
		Width:  req.Width,
		Height: req.Height,
		Source: parseMOQSource(req.Source),
	}
	if addTrack.Name == "" {
		addTrack.Name = "moq-video"
	}

	session := &moqSession{
		id:          guid.New("MS_"),
		service:     s,
		room:        room,
		participant: participant,
		identity:    participant.Identity(),
		roomName:    roomName,
		mimeType:    req.MimeType,
	}
	publisher, ti, err := participant.PublishExternalTrack(addTrack, rtc.ExternalTrackParams{
		MimeType: req.MimeType,
		OnPLI: func() {
			_ = session.writeEvent(moqEvent{Type: "request_keyframe"})
		},
	})
	if err != nil {
		return nil, 0, nil, err
	}
	session.publisher = publisher
	session.trackID = ti.Sid
	attachmentID := session.attach(stream)
	s.storeSession(session)

	return session, attachmentID, &moqReadyResponse{
		ParticipantID: string(participant.ID()),
		SessionID:     session.id,
		TrackID:       ti.Sid,
		MimeType:      req.MimeType,
		Resumed:       false,
	}, nil
}

func (s *moqSession) handleVideoFrame(payload []byte) error {
	if len(payload) < 8 {
		return errors.New("video payload too short")
	}
	ptsMicros := binary.BigEndian.Uint64(payload[:8])
	return s.publisher.WriteAnnexB(time.Duration(ptsMicros)*time.Microsecond, payload[8:])
}

func (s *moqSession) close() {
	s.lock.Lock()
	if s.closed {
		s.lock.Unlock()
		return
	}
	s.closed = true
	if s.expireTimer != nil {
		s.expireTimer.Stop()
		s.expireTimer = nil
	}
	s.stream = nil
	s.lock.Unlock()

	s.service.deleteSession(s.id)
	if s.publisher != nil {
		s.publisher.Close()
	}
	if s.room != nil && s.participant != nil {
		s.room.RemoveParticipant(s.participant.Identity(), "", types.ParticipantCloseReasonPeerConnectionDisconnected)
	}
}

func (s *moqSession) detach(attachmentID uint64) {
	s.lock.Lock()
	if s.closed || s.attachmentID != attachmentID {
		s.lock.Unlock()
		return
	}
	s.stream = nil
	if s.expireTimer != nil {
		s.expireTimer.Stop()
	}
	s.expireTimer = time.AfterFunc(15*time.Second, s.close)
	s.lock.Unlock()
}

func (s *moqSession) attach(stream quic.Stream) uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.expireTimer != nil {
		s.expireTimer.Stop()
		s.expireTimer = nil
	}
	s.stream = stream
	s.attachmentID++
	return s.attachmentID
}

func (s *moqSession) writeEvent(event moqEvent) error {
	s.lock.Lock()
	if s.closed || s.stream == nil {
		s.lock.Unlock()
		return nil
	}
	stream := s.stream
	s.lock.Unlock()
	return writeMOQJSONFrame(stream, moqFrameEvent, event)
}

func (s *MOQService) storeSession(session *moqSession) {
	s.sessionMu.Lock()
	s.sessions[session.id] = session
	s.sessionMu.Unlock()
}

func (s *MOQService) deleteSession(sessionID string) {
	s.sessionMu.Lock()
	delete(s.sessions, sessionID)
	s.sessionMu.Unlock()
}

func (s *MOQService) resumeSession(
	req moqConnectRequest,
	roomName livekit.RoomName,
	claims *auth.ClaimGrants,
	stream quic.Stream,
) (*moqSession, uint64, error) {
	s.sessionMu.Lock()
	session := s.sessions[req.SessionID]
	s.sessionMu.Unlock()
	if session == nil {
		return nil, 0, errors.New("unknown session_id")
	}

	session.lock.Lock()
	defer session.lock.Unlock()

	if session.closed {
		return nil, 0, errors.New("session already closed")
	}
	if session.identity != livekit.ParticipantIdentity(claims.Identity) {
		return nil, 0, errors.New("session identity mismatch")
	}
	if session.roomName != roomName {
		return nil, 0, errors.New("session room mismatch")
	}
	if session.mimeType != req.MimeType {
		return nil, 0, errors.New("session mime type mismatch")
	}
	if session.expireTimer != nil {
		session.expireTimer.Stop()
		session.expireTimer = nil
	}
	session.stream = stream
	session.attachmentID++
	return session, session.attachmentID, nil
}

func (s *MOQService) verifyToken(token string) (context.Context, *auth.ClaimGrants, string, error) {
	if token == "" {
		return nil, nil, "", ErrMissingAuthorization
	}
	parsed, err := auth.ParseAPIToken(token)
	if err != nil {
		return nil, nil, "", ErrInvalidAuthorizationToken
	}
	secret := s.provider.GetSecret(parsed.APIKey())
	if secret == "" {
		return nil, nil, "", ErrInvalidAPIKey
	}
	_, claims, err := parsed.Verify(secret)
	if err != nil {
		return nil, nil, "", err
	}
	return WithGrants(context.Background(), claims, parsed.APIKey()), claims, parsed.APIKey(), nil
}

func (s *MOQService) loadTLSConfig() (*tls.Config, error) {
	if s.conf.MOQ.CertFile != "" && s.conf.MOQ.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(s.conf.MOQ.CertFile, s.conf.MOQ.KeyFile)
		if err != nil {
			return nil, err
		}
		return &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{moqALPN},
			MinVersion:   tls.VersionTLS13,
		}, nil
	}

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{certDER},
			PrivateKey:  privateKey,
		}},
		NextProtos: []string{moqALPN},
		MinVersion: tls.VersionTLS13,
	}, nil
}

func parseMOQSource(source string) livekit.TrackSource {
	switch strings.ToLower(source) {
	case "screen_share", "screenshare", "screen":
		return livekit.TrackSource_SCREEN_SHARE
	case "camera", "":
		return livekit.TrackSource_CAMERA
	default:
		return livekit.TrackSource_CAMERA
	}
}

func readMOQFrame(r io.Reader) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	size := binary.BigEndian.Uint32(header[1:])
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return header[0], payload, nil
}

func writeMOQJSONFrame(w io.Writer, kind byte, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeMOQFrame(w, kind, payload)
}

func writeMOQFrame(w io.Writer, kind byte, payload []byte) error {
	header := make([]byte, 5)
	header[0] = kind
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}
