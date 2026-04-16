use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Parser;
use nokhwa::{
    Camera,
    pixel_format::RgbFormat,
    utils::{
        CameraFormat, CameraIndex, FrameFormat, RequestedFormat, RequestedFormatType, Resolution,
    },
};
use openh264::{
    OpenH264API,
    encoder::{BitRate, Encoder, EncoderConfig, FrameRate, Level, Profile},
    formats::{RgbSliceU8, YUVBuffer},
};
use quinn::{ClientConfig, Endpoint};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use serde::{Deserialize, Serialize};
const FRAME_CONNECT: u8 = 0x01;
const FRAME_READY: u8 = 0x02;
const FRAME_EVENT: u8 = 0x03;
const FRAME_VIDEO: u8 = 0x10;
const FRAME_ERROR: u8 = 0x7f;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    server: SocketAddr,
    #[arg(long, default_value = "localhost")]
    server_name: String,
    #[arg(long)]
    token: String,
    #[arg(long)]
    room: String,
    #[arg(long)]
    identity: String,
    #[arg(long, default_value_t = 0)]
    camera: u32,
    #[arg(long, default_value_t = 1280)]
    width: u32,
    #[arg(long, default_value_t = 720)]
    height: u32,
    #[arg(long, default_value_t = 30)]
    framerate: u32,
    #[arg(long, default_value = "camera")]
    track_name: String,
    #[arg(long, default_value_t = false)]
    insecure: bool,
    #[arg(long, default_value_t = false)]
    test_pattern: bool,
}

#[derive(Debug, Serialize)]
struct ConnectRequest<'a> {
    token: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<&'a str>,
    room: &'a str,
    identity: &'a str,
    track_name: &'a str,
    mime_type: &'a str,
    width: u32,
    height: u32,
    framerate: u32,
    source: &'a str,
}

#[derive(Debug, Deserialize)]
struct ReadyResponse {
    participant_id: String,
    session_id: String,
    track_id: String,
    mime_type: String,
    resumed: bool,
}

#[derive(Debug, Deserialize)]
struct EventMessage {
    #[serde(rename = "type")]
    kind: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls crypto provider"))?;

    let args = Args::parse();

    #[cfg(target_os = "macos")]
    nokhwa::nokhwa_initialize(|_| {});

    let mut endpoint = Endpoint::client("[::]:0".parse().unwrap())?;
    endpoint.set_default_client_config(make_client_config(args.insecure)?);

    let keyframe_requested = Arc::new(AtomicBool::new(false));
    let disconnected = Arc::new(AtomicBool::new(false));

    let mut camera = if args.test_pattern {
        None
    } else {
        let requested =
            RequestedFormat::new::<RgbFormat>(RequestedFormatType::Exact(CameraFormat::new(
                Resolution::new(args.width, args.height),
                FrameFormat::MJPEG,
                args.framerate,
            )));
        let mut camera = Camera::new(CameraIndex::Index(args.camera), requested)?;
        camera.open_stream()?;
        let actual_format = camera.camera_format();
        eprintln!(
            "camera format: {}x{} @ {}fps {:?}",
            actual_format.width(),
            actual_format.height(),
            actual_format.frame_rate(),
            actual_format.format()
        );
        Some(camera)
    };

    let encoder_config = EncoderConfig::new()
        .profile(Profile::Baseline)
        .level(Level::Level_3_0)
        .bitrate(BitRate::from_bps(2_000_000))
        .max_frame_rate(FrameRate::from_hz(args.framerate as f32));
    let mut encoder = Encoder::with_api_config(OpenH264API::from_source(), encoder_config)
        .context("failed to create H264 encoder")?;
    let start = Instant::now();
    let frame_interval = Duration::from_secs_f64(1.0 / args.framerate.max(1) as f64);
    let mut logged_frame_stats = false;
    let mut session_id: Option<String> = None;

    loop {
        disconnected.store(false, Ordering::SeqCst);

        let (_connection, mut send, mut recv, ready) = loop {
            match connect_session(&endpoint, &args, session_id.as_deref()).await {
                Ok(connected) => break connected,
                Err(err) => {
                    eprintln!("connect failed: {err:#}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };

        session_id = Some(ready.session_id.clone());
        eprintln!(
            "connected: participant={}, session={}, track={}, mime={}, resumed={}",
            ready.participant_id, ready.session_id, ready.track_id, ready.mime_type, ready.resumed
        );

        let keyframe_reader = Arc::clone(&keyframe_requested);
        let disconnected_reader = Arc::clone(&disconnected);
        tokio::spawn(async move {
            loop {
                match read_frame(&mut recv).await {
                    Ok((FRAME_EVENT, payload)) => {
                        if let Ok(event) = serde_json::from_slice::<EventMessage>(&payload) {
                            if event.kind == "request_keyframe" {
                                keyframe_reader.store(true, Ordering::SeqCst);
                            }
                        }
                    }
                    Ok((FRAME_ERROR, payload)) => {
                        eprintln!("server error: {}", String::from_utf8_lossy(&payload));
                        disconnected_reader.store(true, Ordering::SeqCst);
                        break;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("control stream interrupted: {err:#}");
                        disconnected_reader.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            }
        });

        loop {
            let rgb_bytes = if let Some(camera) = camera.as_mut() {
                let frame = camera.frame().context("failed to capture frame")?;
                let rgb_image = frame
                    .decode_image::<RgbFormat>()
                    .context("failed to decode frame into RGB")?;
                rgb_image.into_raw()
            } else {
                make_test_pattern(args.width, args.height, start.elapsed())
            };

            if !logged_frame_stats {
                logged_frame_stats = true;
                let (avg_r, avg_g, avg_b) = average_rgb(&rgb_bytes);
                eprintln!(
                    "first frame avg rgb: r={avg_r:.1} g={avg_g:.1} b={avg_b:.1} source={}",
                    if args.test_pattern {
                        "test-pattern"
                    } else {
                        "camera"
                    }
                );
            }

            let rgb = RgbSliceU8::new(&rgb_bytes, (args.width as usize, args.height as usize));
            let yuv = YUVBuffer::from_rgb_source(rgb);

            if keyframe_requested.swap(false, Ordering::SeqCst) {
                encoder.force_intra_frame();
            }

            let bitstream = encoder.encode(&yuv).context("failed to encode frame")?;
            let annexb = bitstream.to_vec();
            if !annexb.is_empty() {
                let pts_micros = start.elapsed().as_micros() as u64;
                let mut payload = Vec::with_capacity(8 + annexb.len());
                payload.extend_from_slice(&pts_micros.to_be_bytes());
                payload.extend_from_slice(&annexb);
                if let Err(err) = write_frame(&mut send, FRAME_VIDEO, &payload).await {
                    eprintln!("publish stream interrupted: {err:#}");
                    disconnected.store(true, Ordering::SeqCst);
                }
            }

            if disconnected.load(Ordering::SeqCst) {
                if let Some(session_id) = session_id.as_deref() {
                    eprintln!("reconnecting resumable session {session_id}...");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                break;
            }

            tokio::time::sleep(frame_interval).await;
        }
    }
}

async fn connect_session(
    endpoint: &Endpoint,
    args: &Args,
    session_id: Option<&str>,
) -> Result<(
    quinn::Connection,
    quinn::SendStream,
    quinn::RecvStream,
    ReadyResponse,
)> {
    let server_name = ServerName::try_from(args.server_name.clone())
        .context("invalid server name")?
        .to_owned();
    let connection = endpoint
        .connect(args.server, server_name.to_str().as_ref())?
        .await
        .context("failed to connect to server")?;

    let (mut send, mut recv) = connection.open_bi().await?;

    let connect = ConnectRequest {
        token: &args.token,
        session_id,
        room: &args.room,
        identity: &args.identity,
        track_name: &args.track_name,
        mime_type: "video/h264",
        width: args.width,
        height: args.height,
        framerate: args.framerate,
        source: "camera",
    };
    write_json_frame(&mut send, FRAME_CONNECT, &connect).await?;

    let (kind, payload) = read_frame(&mut recv).await?;
    if kind == FRAME_ERROR {
        bail!("server error: {}", String::from_utf8_lossy(&payload));
    }
    if kind != FRAME_READY {
        bail!("expected ready frame, got {kind}");
    }

    let ready: ReadyResponse = serde_json::from_slice(&payload)?;
    Ok((connection, send, recv, ready))
}

fn average_rgb(rgb: &[u8]) -> (f64, f64, f64) {
    if rgb.len() < 3 {
        return (0.0, 0.0, 0.0);
    }

    let mut r = 0u64;
    let mut g = 0u64;
    let mut b = 0u64;
    let mut count = 0u64;
    for chunk in rgb.chunks_exact(3) {
        r += u64::from(chunk[0]);
        g += u64::from(chunk[1]);
        b += u64::from(chunk[2]);
        count += 1;
    }

    if count == 0 {
        return (0.0, 0.0, 0.0);
    }

    (
        r as f64 / count as f64,
        g as f64 / count as f64,
        b as f64 / count as f64,
    )
}

fn make_test_pattern(width: u32, height: u32, elapsed: Duration) -> Vec<u8> {
    let mut rgb = vec![0u8; (width as usize) * (height as usize) * 3];
    let phase = ((elapsed.as_millis() / 16) % 256) as u8;
    for y in 0..height as usize {
        for x in 0..width as usize {
            let offset = (y * width as usize + x) * 3;
            rgb[offset] = ((x * 255) / width as usize) as u8 ^ phase;
            rgb[offset + 1] = ((y * 255) / height as usize) as u8;
            rgb[offset + 2] = 255u8.saturating_sub(phase);
        }
    }
    rgb
}

fn make_client_config(insecure: bool) -> Result<ClientConfig> {
    let crypto = if insecure {
        let verifier = Arc::new(SkipServerVerification);
        let mut config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();
        config.alpn_protocols = vec![b"livekit-moq/0".to_vec()];
        config
    } else {
        let mut config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        config.alpn_protocols = vec![b"livekit-moq/0".to_vec()];
        config
    };

    Ok(ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
    )))
}

async fn write_json_frame<T: Serialize>(
    send: &mut quinn::SendStream,
    kind: u8,
    msg: &T,
) -> Result<()> {
    let payload = serde_json::to_vec(msg)?;
    write_frame(send, kind, &payload).await
}

async fn write_frame(send: &mut quinn::SendStream, kind: u8, payload: &[u8]) -> Result<()> {
    let mut header = [0u8; 5];
    header[0] = kind;
    header[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    send.write_all(&header).await?;
    send.write_all(payload).await?;
    Ok(())
}

async fn read_frame(recv: &mut quinn::RecvStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 5];
    recv.read_exact(&mut header).await?;
    let size = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; size];
    recv.read_exact(&mut payload).await?;
    Ok((header[0], payload))
}

#[derive(Debug)]
struct SkipServerVerification;

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}
