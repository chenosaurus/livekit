package rtc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
)

func TestPublishExternalTrack(t *testing.T) {
	p := newParticipantForTest("moq-publisher")

	pub, ti, err := p.PublishExternalTrack(&livekit.AddTrackRequest{
		Cid:    "moq-video",
		Name:   "camera",
		Type:   livekit.TrackType_VIDEO,
		Source: livekit.TrackSource_CAMERA,
		Width:  1280,
		Height: 720,
	}, ExternalTrackParams{
		MimeType: "video/h264",
	})
	require.NoError(t, err)
	require.NotNil(t, pub)
	require.Equal(t, "video/H264", ti.MimeType)
	require.NotNil(t, p.GetPublishedTrack(livekit.TrackID(ti.Sid)))

	err = pub.WriteAnnexB(33*time.Millisecond, []byte{
		0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21,
	})
	require.NoError(t, err)

	pub.Close()

	require.Eventually(t, func() bool {
		return p.GetPublishedTrack(livekit.TrackID(ti.Sid)) == nil
	}, time.Second, 10*time.Millisecond)
}
