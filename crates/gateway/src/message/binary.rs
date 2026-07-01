use super::*;
use crate::message::artifacts::ARTIFACT_UPLOAD_CHUNK_FRAME_MAGIC;
use crate::message::skills::SKILL_UPLOAD_CHUNK_FRAME_MAGIC;
use pioneer_protocol::{
    VOICE_AUDIO_FORMAT_CONTRACT, VOICE_AUDIO_MAX_CHUNK_BYTES, VOICE_AUDIO_MAX_CHUNK_DURATION_MS,
    VOICE_AUDIO_TARGET_BYTES_PER_SAMPLE, VOICE_CHUNK_FRAME_MAGIC, VoiceAudioFormat,
    VoiceChunkAckNotification, VoiceChunkFrameHeader, VoiceError, VoiceErrorKind,
    VoiceFrameDecodeError, decode_voice_chunk_frame, validate_voice_streaming_audio_format,
};

pub(in crate::message) const MAX_GATEWAY_VOICE_CHUNK_BYTES: usize = VOICE_AUDIO_MAX_CHUNK_BYTES;
pub(in crate::message) const MAX_GATEWAY_VOICE_CHUNK_SEQUENCE: u64 = 1_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::message) struct GatewayVoiceChunkFrame {
    pub header: VoiceChunkFrameHeader,
    pub audio_payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewayBinaryFrameKind {
    ArtifactUploadChunk,
    SkillUploadChunk,
    VoiceChunk,
}

impl GatewayBinaryFrameKind {
    fn from_payload(payload: &[u8]) -> Option<Self> {
        if payload.starts_with(VOICE_CHUNK_FRAME_MAGIC) {
            return Some(Self::VoiceChunk);
        }
        if payload.starts_with(SKILL_UPLOAD_CHUNK_FRAME_MAGIC) {
            return Some(Self::SkillUploadChunk);
        }
        if payload.starts_with(ARTIFACT_UPLOAD_CHUNK_FRAME_MAGIC) {
            return Some(Self::ArtifactUploadChunk);
        }

        None
    }

    fn name(self) -> &'static str {
        match self {
            Self::ArtifactUploadChunk => "artifact/upload/chunk",
            Self::SkillUploadChunk => "skills/upload/chunk",
            Self::VoiceChunk => "voice/chunk",
        }
    }
}

impl MessageProcessor {
    pub(crate) async fn process_binary_frame(&self, connection_id: ConnectionId, payload: &[u8]) {
        let Some(kind) = GatewayBinaryFrameKind::from_payload(payload) else {
            warn!(
                connection_id,
                frame_len = payload.len(),
                "unknown gateway binary frame ignored"
            );
            return;
        };

        debug!(
            connection_id,
            frame_kind = kind.name(),
            frame_len = payload.len(),
            "processing gateway binary frame"
        );

        match kind {
            GatewayBinaryFrameKind::ArtifactUploadChunk => {
                self.process_artifact_upload_chunk_frame(connection_id, payload)
                    .await;
            }
            GatewayBinaryFrameKind::SkillUploadChunk => {
                self.process_skill_upload_chunk_frame(connection_id, payload)
                    .await;
            }
            GatewayBinaryFrameKind::VoiceChunk => {
                self.process_voice_chunk_frame(connection_id, payload).await;
            }
        }
    }

    pub(crate) async fn process_voice_chunk_frame(
        &self,
        connection_id: ConnectionId,
        payload: &[u8],
    ) {
        match decode_gateway_voice_chunk_frame(payload) {
            Ok(chunk) => {
                self.forward_decoded_voice_chunk(connection_id, chunk).await;
            }
            Err(error) => {
                warn!(
                    connection_id,
                    error_kind = ?error.kind,
                    error = %error.message,
                    "rejected malformed voice binary frame"
                );
            }
        }
    }

    async fn forward_decoded_voice_chunk(
        &self,
        connection_id: ConnectionId,
        chunk: GatewayVoiceChunkFrame,
    ) {
        let session = match self
            .voice_sessions
            .lookup_session(chunk.header.session_id.as_str(), connection_id)
        {
            Ok(session) => session,
            Err(error) => {
                let voice_error = error.into_voice_error();
                warn!(
                    connection_id,
                    session_id = %chunk.header.session_id,
                    error_kind = ?voice_error.kind,
                    error = %voice_error.message,
                    "rejected voice chunk for inactive or foreign session"
                );
                return;
            }
        };
        if session.state.is_terminal_for_chunk_ingest() {
            warn!(
                connection_id,
                session_id = %session.session_id,
                state = ?session.state,
                sequence = chunk.header.sequence,
                "rejected late voice chunk after session finalization started"
            );
            return;
        }

        match self
            .voice_session_buffers
            .append_chunk(chunk.header, chunk.audio_payload)
        {
            Ok(report) => {
                debug!(
                    connection_id,
                    session_id = %report.session_id,
                    sequence = report.sequence,
                    buffered_chunks = report.buffered_chunks,
                    buffered_bytes = report.buffered_bytes,
                    "decoded voice chunk appended to session buffer"
                );
                self.send_notification_to_connections(
                    events::VOICE_CHUNK_ACK,
                    &VoiceChunkAckNotification {
                        session_id: report.session_id,
                        sequence: report.sequence,
                    },
                    vec![connection_id],
                )
                .await;
            }
            Err(error) => {
                let voice_error = error.into_voice_error();
                warn!(
                    connection_id,
                    error_kind = ?voice_error.kind,
                    error = %voice_error.message,
                    "rejected voice chunk during session ingest"
                );
            }
        }
    }
}

pub(in crate::message) fn decode_gateway_voice_chunk_frame(
    payload: &[u8],
) -> Result<GatewayVoiceChunkFrame, VoiceError> {
    if payload.len() > MAX_GATEWAY_VOICE_CHUNK_BYTES + 512 {
        return Err(voice_frame_error(
            VoiceErrorKind::InvalidSession,
            format!(
                "voice frame exceeds maximum size: {} > {}",
                payload.len(),
                MAX_GATEWAY_VOICE_CHUNK_BYTES
            ),
        ));
    }

    let decoded = decode_voice_chunk_frame(payload).map_err(map_voice_frame_decode_error)?;
    validate_gateway_voice_chunk_header(&decoded.header)?;

    if decoded.audio_payload.len() > MAX_GATEWAY_VOICE_CHUNK_BYTES {
        return Err(voice_frame_error(
            VoiceErrorKind::InvalidSession,
            format!(
                "voice audio payload exceeds maximum size: {} > {}",
                decoded.audio_payload.len(),
                MAX_GATEWAY_VOICE_CHUNK_BYTES
            ),
        ));
    }
    let bytes_per_frame =
        VOICE_AUDIO_TARGET_BYTES_PER_SAMPLE * usize::from(decoded.header.channels);
    if decoded.audio_payload.len() % bytes_per_frame != 0 {
        return Err(voice_frame_error(
            VoiceErrorKind::StaleChunk,
            "voice pcm_s16le payload must contain complete samples",
        ));
    }

    Ok(GatewayVoiceChunkFrame {
        header: decoded.header,
        audio_payload: decoded.audio_payload.to_vec(),
    })
}

fn validate_gateway_voice_chunk_header(header: &VoiceChunkFrameHeader) -> Result<(), VoiceError> {
    if !is_valid_voice_session_id(header.session_id.as_str()) {
        return Err(voice_frame_error(
            VoiceErrorKind::InvalidSession,
            "voice session_id has invalid shape",
        ));
    }
    if header.sequence > MAX_GATEWAY_VOICE_CHUNK_SEQUENCE {
        return Err(voice_frame_error(
            VoiceErrorKind::SequenceGap,
            "voice chunk sequence is outside accepted bounds",
        ));
    }
    let format = VoiceAudioFormat {
        sample_rate_hz: header.sample_rate_hz,
        channels: header.channels,
        encoding: header.encoding,
    };
    if let Err(error) = validate_voice_streaming_audio_format(&format) {
        return Err(voice_frame_error(
            VoiceErrorKind::DeviceUnavailable,
            format!(
                "unsupported voice audio format: {error}; expected {VOICE_AUDIO_FORMAT_CONTRACT}"
            ),
        ));
    }
    if let Some(duration_ms) = header.duration_ms
        && duration_ms > VOICE_AUDIO_MAX_CHUNK_DURATION_MS
    {
        return Err(voice_frame_error(
            VoiceErrorKind::StaleChunk,
            format!(
                "voice chunk duration exceeds maximum: {}ms > {}ms",
                duration_ms, VOICE_AUDIO_MAX_CHUNK_DURATION_MS
            ),
        ));
    }

    Ok(())
}

fn is_valid_voice_session_id(session_id: &str) -> bool {
    let len = session_id.len();
    (8..=128).contains(&len)
        && session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn map_voice_frame_decode_error(error: VoiceFrameDecodeError) -> VoiceError {
    match error {
        VoiceFrameDecodeError::PayloadLengthMismatch { .. }
        | VoiceFrameDecodeError::EmptyPayload => {
            voice_frame_error(VoiceErrorKind::StaleChunk, error.to_string())
        }
        VoiceFrameDecodeError::MissingSessionId => {
            voice_frame_error(VoiceErrorKind::InvalidSession, error.to_string())
        }
        VoiceFrameDecodeError::InvalidAudioFormat => {
            voice_frame_error(VoiceErrorKind::DeviceUnavailable, error.to_string())
        }
        VoiceFrameDecodeError::TooShort
        | VoiceFrameDecodeError::InvalidMagic
        | VoiceFrameDecodeError::HeaderOutOfBounds
        | VoiceFrameDecodeError::HeaderDecodeFailed(_) => {
            voice_frame_error(VoiceErrorKind::InvalidSession, error.to_string())
        }
    }
}

fn voice_frame_error(kind: VoiceErrorKind, message: impl Into<String>) -> VoiceError {
    VoiceError {
        kind,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_frame_kind_detects_skill_upload_magic() {
        let mut payload = Vec::new();
        payload.extend_from_slice(SKILL_UPLOAD_CHUNK_FRAME_MAGIC);
        payload.extend_from_slice(&0u32.to_be_bytes());

        let kind = GatewayBinaryFrameKind::from_payload(payload.as_slice());

        assert_eq!(kind, Some(GatewayBinaryFrameKind::SkillUploadChunk));
        assert_eq!(kind.expect("kind").name(), "skills/upload/chunk");
    }

    #[test]
    fn binary_frame_kind_detects_artifact_upload_magic() {
        let mut payload = Vec::new();
        payload.extend_from_slice(ARTIFACT_UPLOAD_CHUNK_FRAME_MAGIC);
        payload.extend_from_slice(&0u32.to_be_bytes());

        let kind = GatewayBinaryFrameKind::from_payload(payload.as_slice());

        assert_eq!(kind, Some(GatewayBinaryFrameKind::ArtifactUploadChunk));
        assert_eq!(kind.expect("kind").name(), "artifact/upload/chunk");
    }

    #[test]
    fn binary_frame_kind_detects_voice_chunk_magic() {
        let frame = voice_frame("voice_session_1", 0, b"\xff\x00").expect("voice frame");

        let kind = GatewayBinaryFrameKind::from_payload(frame.as_slice());

        assert_eq!(kind, Some(GatewayBinaryFrameKind::VoiceChunk));
        assert_eq!(kind.expect("kind").name(), "voice/chunk");
    }

    #[test]
    fn binary_frame_kind_rejects_unknown_magic() {
        assert_eq!(GatewayBinaryFrameKind::from_payload(b"NOPE"), None);
    }

    #[test]
    fn decode_gateway_voice_chunk_frame_accepts_good_non_json_audio_payload() {
        let frame = voice_frame("voice_session_1", 3, b"\xff\x00\x7f\x00").expect("voice frame");

        let decoded =
            decode_gateway_voice_chunk_frame(frame.as_slice()).expect("voice frame should decode");

        assert_eq!(decoded.header.session_id, "voice_session_1");
        assert_eq!(decoded.header.sequence, 3);
        assert_eq!(decoded.header.sample_rate_hz, 16_000);
        assert_eq!(decoded.header.channels, 1);
        assert_eq!(decoded.audio_payload, b"\xff\x00\x7f\x00");
    }

    #[test]
    fn decode_gateway_voice_chunk_frame_rejects_truncated_header() {
        let mut payload = Vec::new();
        payload.extend_from_slice(VOICE_CHUNK_FRAME_MAGIC);
        payload.extend_from_slice(&64u32.to_be_bytes());

        let error = decode_gateway_voice_chunk_frame(payload.as_slice())
            .expect_err("truncated header should fail");

        assert_eq!(error.kind, VoiceErrorKind::InvalidSession);
        assert!(
            error.message.contains("header is out of bounds"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn decode_gateway_voice_chunk_frame_rejects_bad_sequence() {
        let frame = voice_frame(
            "voice_session_1",
            MAX_GATEWAY_VOICE_CHUNK_SEQUENCE + 1,
            b"pcm",
        )
        .expect("voice frame");

        let error = decode_gateway_voice_chunk_frame(frame.as_slice())
            .expect_err("bad sequence should fail");

        assert_eq!(error.kind, VoiceErrorKind::SequenceGap);
    }

    #[test]
    fn decode_gateway_voice_chunk_frame_rejects_oversized_payload() {
        let audio = vec![0u8; MAX_GATEWAY_VOICE_CHUNK_BYTES + 1];
        let frame = voice_frame("voice_session_1", 0, audio.as_slice()).expect("voice frame");

        let error = decode_gateway_voice_chunk_frame(frame.as_slice())
            .expect_err("oversized payload should fail");

        assert_eq!(error.kind, VoiceErrorKind::InvalidSession);
        assert!(
            error.message.contains("exceeds maximum size"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn decode_gateway_voice_chunk_frame_rejects_unsupported_audio_format() {
        let frame = voice_frame_with_format(
            "voice_session_1",
            0,
            pioneer_protocol::VoiceAudioFormat {
                sample_rate_hz: 48_000,
                channels: 1,
                encoding: pioneer_protocol::VoiceAudioEncoding::PcmS16Le,
            },
            b"pcm",
        )
        .expect_err("client encoder should reject unsupported format");

        assert!(frame.contains("voice audio format is invalid"));
    }

    #[test]
    fn decode_gateway_voice_chunk_frame_rejects_too_long_duration() {
        let mut header = voice_header("voice_session_1", 0);
        header.duration_ms = Some(pioneer_protocol::VOICE_AUDIO_MAX_CHUNK_DURATION_MS + 1);
        let frame = pioneer_protocol::encode_voice_chunk_frame(header, b"pcm1")
            .expect("duration is gateway validation");

        let error = decode_gateway_voice_chunk_frame(frame.as_slice())
            .expect_err("long duration should fail");

        assert_eq!(error.kind, VoiceErrorKind::StaleChunk);
        assert!(
            error.message.contains("duration exceeds maximum"),
            "unexpected error message: {}",
            error.message
        );
    }

    fn voice_frame(session_id: &str, sequence: u64, audio: &[u8]) -> Result<Vec<u8>, String> {
        pioneer_protocol::encode_voice_chunk_frame(voice_header(session_id, sequence), audio)
            .map_err(|error| error.to_string())
    }

    fn voice_frame_with_format(
        session_id: &str,
        sequence: u64,
        audio_format: pioneer_protocol::VoiceAudioFormat,
        audio: &[u8],
    ) -> Result<Vec<u8>, String> {
        pioneer_protocol::encode_voice_chunk_frame(
            VoiceChunkFrameHeader {
                sample_rate_hz: audio_format.sample_rate_hz,
                channels: audio_format.channels,
                encoding: audio_format.encoding,
                ..voice_header(session_id, sequence)
            },
            audio,
        )
        .map_err(|error| error.to_string())
    }

    fn voice_header(session_id: &str, sequence: u64) -> VoiceChunkFrameHeader {
        VoiceChunkFrameHeader {
            session_id: session_id.to_owned(),
            sequence,
            sample_rate_hz: pioneer_protocol::VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ,
            channels: pioneer_protocol::VOICE_AUDIO_TARGET_CHANNELS,
            encoding: pioneer_protocol::VoiceAudioEncoding::PcmS16Le,
            payload_len: 0,
            captured_at_unix_ms: None,
            duration_ms: Some(pioneer_protocol::VOICE_AUDIO_TARGET_CHUNK_DURATION_MS),
        }
    }
}
