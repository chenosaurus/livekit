# MoQ Camera Publisher

This is a minimal Rust publisher that:

- captures frames from a local camera with `nokhwa`
- encodes them with `openh264`
- publishes Annex-B H.264 access units to the LiveKit MoQ QUIC endpoint added in this repo

## Usage

```bash
cargo run -- \
  --server 127.0.0.1:7882 \
  --server-name localhost \
  --token "$LIVEKIT_TOKEN" \
  --room my-room \
  --identity camera-bot \
  --camera 0 \
  --width 1280 \
  --height 720 \
  --framerate 30 \
  --insecure
```

`--insecure` is convenient when the server is using the default ephemeral self-signed certificate.

## Notes

- The example is intentionally basic: one H.264 video track, no audio, no simulcast.
- The token must allow room join and video publish.
- The current server-side transport also accepts `video/h265`, but this example only publishes H.264.
