package rtc

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/transport/v4/packetio"
	"github.com/pion/webrtc/v4"

	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils"
	protocolcodecs "github.com/livekit/protocol/codecs"

	lkbuffer "github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu"
)

const (
	externalVideoClockRate = 90000
	externalVideoMTU       = 1200
)

var (
	ErrUnsupportedExternalCodec = errors.New("unsupported external track codec")
	ErrUnsupportedExternalTrack = errors.New("unsupported external track")
)

type ExternalTrackParams struct {
	MimeType       string
	FmtpLine       string
	OnPLI          func()
	PLIThrottle    sfu.PLIThrottleConfig
	ClockRate      uint32
	EnableRestart  bool
	TrackName      string
}

type ExternalTrackPublisher struct {
	track      *MediaTrack
	receiver   *ExternalReceiver
	packetizer externalPacketizer
	ssrc       uint32
	payload    uint8
	clockRate  uint32

	lock          sync.Mutex
	basePTS       time.Duration
	baseTimestamp uint32
	seqNo         uint16
	closed        bool
}

type externalPacketizer interface {
	Payload(mtu uint16, payload []byte) [][]byte
}

type ExternalReceiver struct {
	*sfu.ReceiverBase

	bufferFactory   *lkbuffer.Factory
	ssrc            uint32
	onCloseHandler  func()
	startForwarder  sync.Once
}

func NewExternalReceiver(
	trackID livekit.TrackID,
	streamID string,
	codec webrtc.RTPCodecParameters,
	trackInfo *livekit.TrackInfo,
	bufferFactory *lkbuffer.Factory,
	logger logger.Logger,
	onPLI func(),
	streamTrackerConfig sfu.StreamTrackerManagerConfig,
	enableRestart bool,
) *ExternalReceiver {
	r := &ExternalReceiver{
		bufferFactory: bufferFactory,
		ssrc:          uint32(rand.Uint32()),
	}

	r.ReceiverBase = sfu.NewReceiverBase(
		sfu.ReceiverBaseParams{
			TrackID:                    trackID,
			StreamID:                   streamID,
			Kind:                       webrtc.RTPCodecTypeVideo,
			Codec:                      codec,
			Logger:                     logger,
			StreamTrackerManagerConfig: streamTrackerConfig,
			IsSelfClosing:              true,
			OnNewBufferNeeded: func(layer int32, _ *livekit.TrackInfo) (lkbuffer.BufferProvider, error) {
				buffRW := bufferFactory.GetOrNew(packetio.RTPBufferPacket, r.ssrc)
				buff, ok := buffRW.(*lkbuffer.Buffer)
				if !ok {
					return nil, errors.New("could not create RTP buffer")
				}
				if err := buff.Bind(
					webrtc.RTPParameters{
						HeaderExtensions: nil,
						Codecs:           []webrtc.RTPCodecParameters{codec},
					},
					codec.RTPCodecCapability,
					0,
				); err != nil {
					return nil, err
				}
				if onPLI != nil {
					buff.OnRtcpFeedback(func(pkts []rtcp.Packet) {
						onPLI()
					})
				}
				return buff, nil
			},
			OnClosed: r.onClosed,
		},
		trackInfo,
		sfu.ReceiverCodecStateNormal,
	)
	r.ReceiverBase.SetEnableRTPStreamRestartDetection(enableRestart)
	return r
}

func (r *ExternalReceiver) OnCloseHandler(fn func()) {
	r.onCloseHandler = fn
}

func (r *ExternalReceiver) onClosed() {
	if r.onCloseHandler != nil {
		r.onCloseHandler()
	}
}

func (r *ExternalReceiver) PushPacket(pkt *rtp.Packet, arrivalTime int64) error {
	buff := r.GetOrCreateBuffer(0)
	if buff == nil {
		return errors.New("could not create buffer")
	}
	r.startForwarder.Do(func() {
		r.StartBuffer(buff, 0)
	})

	raw, err := pkt.Marshal()
	if err != nil {
		return err
	}

	_, err = buff.HandleIncomingPacket(raw, pkt, arrivalTime, false, false, nil, 0)
	return err
}

func (p *ParticipantImpl) PublishExternalTrack(req *livekit.AddTrackRequest, params ExternalTrackParams) (*ExternalTrackPublisher, *livekit.TrackInfo, error) {
	if req == nil {
		return nil, nil, errors.New("add track request required")
	}
	if req.Type != livekit.TrackType_VIDEO {
		return nil, nil, ErrUnsupportedExternalTrack
	}
	if !p.CanPublishSource(req.Source) {
		return nil, nil, ErrPermissionDenied
	}

	codec, payloader, err := p.getExternalVideoCodec(params)
	if err != nil {
		return nil, nil, err
	}

	p.pendingTracksLock.Lock()
	ti := p.addPendingTrackLocked(req)
	if ti == nil {
		p.pendingTracksLock.Unlock()
		return nil, nil, errors.New("could not create pending track")
	}
	ti = utils.CloneProto(ti)
	ti.MimeType = codec.MimeType
	if len(ti.Codecs) == 0 {
		ti.Codecs = []*livekit.SimulcastCodecInfo{{MimeType: codec.MimeType, Cid: req.Cid, VideoLayerMode: livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM}}
	} else {
		ti.Codecs[0].MimeType = codec.MimeType
	}
	p.pendingTracks[req.Cid].trackInfos[0] = ti
	mt := p.addMediaTrack(req.Cid, ti)
	p.pendingTracksLock.Unlock()

	receiver := NewExternalReceiver(
		livekit.TrackID(ti.Sid),
		string(p.ID()),
		codec,
		ti,
		p.params.Config.BufferFactory,
		LoggerWithCodecMime(p.pubLogger, mime.NormalizeMimeType(codec.MimeType)),
		params.OnPLI,
		p.params.VideoConfig.StreamTrackerManager,
		p.params.EnableRTPStreamRestartDetection,
	)
	receiver.OnCloseHandler(func() {
		mt.MediaTrackReceiver.SetClosing(false)
		mt.MediaTrackReceiver.ClearReceiver(mime.NormalizeMimeType(codec.MimeType), false)
		if mt.MediaTrackReceiver.TryClose() && mt.dynacastManager != nil {
			mt.dynacastManager.Close()
		}
	})
	mt.MediaTrackReceiver.SetupReceiver(receiver, 0, "")

	p.setIsPublisher(true)
	p.updateState(livekit.ParticipantInfo_ACTIVE)
	p.handleTrackPublished(mt, false)

	return &ExternalTrackPublisher{
		track:         mt,
		receiver:      receiver,
		packetizer:    payloader,
		ssrc:          receiver.ssrc,
		payload:       uint8(codec.PayloadType),
		clockRate:     codec.ClockRate,
		baseTimestamp: rand.Uint32(),
		seqNo:         uint16(rand.Uint32()),
	}, ti, nil
}

func (p *ParticipantImpl) getExternalVideoCodec(params ExternalTrackParams) (webrtc.RTPCodecParameters, externalPacketizer, error) {
	mimeType := mime.NormalizeMimeType(params.MimeType)
	if mimeType != mime.MimeTypeH264 && mimeType != mime.MimeTypeH265 {
		return webrtc.RTPCodecParameters{}, nil, ErrUnsupportedExternalCodec
	}

	fmtpLine := params.FmtpLine
	if mimeType == mime.MimeTypeH264 && fmtpLine == "" {
		fmtpLine = protocolcodecs.H264ProfileLevelId42e01fPacketizationMode1CodecParameters.SDPFmtpLine
	}

	codec := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  mimeType.String(),
			ClockRate: externalVideoClockRate,
			Channels:  0,
			SDPFmtpLine: fmtpLine,
		},
		PayloadType: 102,
	}
	if mimeType == mime.MimeTypeH265 {
		codec.PayloadType = 104
		return codec, &codecs.H265Payloader{}, nil
	}
	return codec, &codecs.H264Payloader{}, nil
}

func (p *ExternalTrackPublisher) WriteAnnexB(pts time.Duration, payload []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return errors.New("publisher closed")
	}

	if len(payload) == 0 {
		return nil
	}

	if p.basePTS == 0 {
		p.basePTS = pts
	}
	rtpTimestamp := p.baseTimestamp + uint32((pts-p.basePTS).Seconds()*float64(p.clockRate))
	packets := p.packetizer.Payload(externalVideoMTU, payload)
	now := time.Now().UnixNano()
	for i, pay := range packets {
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    p.payload,
				SequenceNumber: p.seqNo,
				Timestamp:      rtpTimestamp,
				SSRC:           p.ssrc,
				Marker:         i == len(packets)-1,
			},
			Payload: pay,
		}
		p.seqNo++
		if err := p.receiver.PushPacket(pkt, now); err != nil {
			return err
		}
	}
	return nil
}

func (p *ExternalTrackPublisher) Close() {
	p.lock.Lock()
	if p.closed {
		p.lock.Unlock()
		return
	}
	p.closed = true
	p.lock.Unlock()

	p.receiver.Close("external-track-closed", true)
}

func (p *ExternalTrackPublisher) Track() *MediaTrack {
	return p.track
}
