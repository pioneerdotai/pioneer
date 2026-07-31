use super::*;
use crate::authorization::{
    ActionGateDecision, AuthorizationDecision, AuthorizationResolver, AuthorizationService,
    BinaryIngressKind, DenyReason, DisclosurePolicy, ProofResolution, ResourceAction,
    binary_ingress_entry, record_binary_decision,
};
use crate::message::artifacts::{
    ARTIFACT_UPLOAD_CHUNK_FRAME_MAGIC, upload::parse_artifact_upload_chunk_frame,
};
use crate::message::skills::SKILL_UPLOAD_CHUNK_FRAME_MAGIC;
use pioneer_protocol::{
    VOICE_AUDIO_FORMAT_CONTRACT, VOICE_AUDIO_MAX_CHUNK_BYTES, VOICE_AUDIO_MAX_CHUNK_DURATION_MS,
    VOICE_AUDIO_TARGET_BYTES_PER_SAMPLE, VOICE_CHUNK_FRAME_MAGIC, VoiceAudioFormat,
    VoiceChunkAckNotification, VoiceChunkFrameHeader, VoiceError, VoiceErrorKind,
    VoiceFrameDecodeError, decode_voice_chunk_frame, validate_voice_streaming_audio_format,
};
use tracing::Instrument as _;

pub(in crate::message) const MAX_GATEWAY_VOICE_CHUNK_BYTES: usize = VOICE_AUDIO_MAX_CHUNK_BYTES;
pub(in crate::message) const MAX_GATEWAY_VOICE_CHUNK_SEQUENCE: u64 = 1_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::message) struct GatewayVoiceChunkFrame {
    pub header: VoiceChunkFrameHeader,
    pub audio_payload: Vec<u8>,
}

fn classify_binary_payload(payload: &[u8]) -> Option<BinaryIngressKind> {
    if payload.starts_with(VOICE_CHUNK_FRAME_MAGIC) {
        return Some(BinaryIngressKind::VoiceChunk);
    }
    if payload.starts_with(SKILL_UPLOAD_CHUNK_FRAME_MAGIC) {
        return Some(BinaryIngressKind::SkillUploadChunk);
    }
    if payload.starts_with(ARTIFACT_UPLOAD_CHUNK_FRAME_MAGIC) {
        return Some(BinaryIngressKind::ArtifactUploadChunk);
    }

    None
}

impl MessageProcessor {
    pub(crate) async fn process_binary_frame(
        &self,
        connection: &crate::request_context::ConnectionContext,
        payload: &[u8],
    ) {
        let Some(kind) = classify_binary_payload(payload) else {
            let context = crate::request_context::RequestContext::new(
                connection,
                None,
                crate::request_context::CanonicalMethod::binary("binary/unknown"),
            );
            let span = context.request_span();
            async {
                warn!(
                    rejection_reason = "unknown_binary_frame",
                    frame_len = payload.len(),
                    "unknown gateway binary frame ignored"
                );
            }
            .instrument(span)
            .await;
            return;
        };
        let Ok(registration) = binary_ingress_entry(kind) else {
            let context = crate::request_context::RequestContext::new(
                connection,
                None,
                crate::request_context::CanonicalMethod::binary("binary/unregistered"),
            );
            let span = context.request_span();
            async {
                warn!(
                    rejection_reason = "unregistered_binary_frame",
                    frame_len = payload.len(),
                    "unregistered gateway binary frame ignored"
                );
            }
            .instrument(span)
            .await;
            return;
        };
        let context = crate::request_context::RequestContext::new(
            connection,
            None,
            crate::request_context::CanonicalMethod::binary(registration.kind.safe_name()),
        );
        let span = context.request_span();

        async {
            let decision = match kind {
                BinaryIngressKind::ArtifactUploadChunk => {
                    self.authorize_artifact_upload_chunk(&context, payload)
                        .await
                }
                BinaryIngressKind::VoiceChunk => {
                    self.authorize_voice_chunk(&context, payload).await
                }
                BinaryIngressKind::SkillUploadChunk => {
                    let service = AuthorizationService::new();
                    match service.authorize_action(
                        context.principal().kind,
                        context.role_key(),
                        registration.action,
                    ) {
                        ActionGateDecision::AllowSuperuser => AuthorizationDecision::AllowSuperuser,
                        ActionGateDecision::RequireResource { .. } => AuthorizationDecision::Deny {
                            reason: DenyReason::MissingAuthoritativeResource,
                            disclosure: registration.disclosure,
                        },
                        ActionGateDecision::Deny { reason, disclosure } => {
                            AuthorizationDecision::Deny { reason, disclosure }
                        }
                    }
                }
            };
            record_binary_decision(registration, &decision);
            if !decision.is_allowed() {
                return;
            }

            debug!(frame_len = payload.len(), "processing gateway binary frame");

            match kind {
                BinaryIngressKind::ArtifactUploadChunk => {
                    self.process_artifact_upload_chunk_frame(&context, payload)
                        .await;
                }
                BinaryIngressKind::SkillUploadChunk => {
                    self.process_skill_upload_chunk_frame(&context, payload)
                        .await;
                }
                BinaryIngressKind::VoiceChunk => {
                    self.process_voice_chunk_frame(&context, payload).await;
                }
            }
        }
        .instrument(span)
        .await;
    }

    async fn authorize_artifact_upload_chunk(
        &self,
        request_context: &RequestContext,
        payload: &[u8],
    ) -> AuthorizationDecision {
        let Ok((header, _)) = parse_artifact_upload_chunk_frame(payload) else {
            return missing_binary_resource();
        };
        let owner = AuthenticatedTransferOwner::from_request_context(request_context);
        let Ok(session) = self
            .artifact_uploads
            .lookup_for_owner(
                &owner,
                header.workspace_id.as_str(),
                header.upload_id.as_str(),
                now_timestamp_secs(),
            )
            .await
        else {
            return missing_binary_resource();
        };
        let action = if session.planned_turn_id.is_some() {
            ResourceAction::ThreadWrite
        } else {
            ResourceAction::ArtifactWrite
        };
        let service = AuthorizationService::new();
        let gate = service.authorize_action(
            request_context.principal().kind,
            request_context.role_key(),
            action,
        );
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        if let (Some(thread_id), Some(turn_id)) = (
            session.thread_id.as_deref(),
            session.planned_turn_id.as_deref(),
        ) {
            match resolver
                .authorize_turn(
                    request_context.principal(),
                    &gate,
                    action,
                    turn_id,
                    Some(session.workspace_id.as_str()),
                    Some(thread_id),
                )
                .await
            {
                Ok(ProofResolution::Authorized(proof)) => proof.decision().clone(),
                Ok(ProofResolution::Denied(decision)) => decision,
                Err(_) => missing_binary_resource(),
            }
        } else if let Some(thread_id) = session.thread_id.as_deref() {
            match resolver
                .authorize_thread(
                    request_context.principal(),
                    &gate,
                    action,
                    thread_id,
                    Some(session.workspace_id.as_str()),
                )
                .await
            {
                Ok(ProofResolution::Authorized(proof)) => proof.decision().clone(),
                Ok(ProofResolution::Denied(decision)) => decision,
                Err(_) => missing_binary_resource(),
            }
        } else {
            match resolver
                .authorize_workspace(
                    request_context.principal(),
                    &gate,
                    action,
                    session.workspace_id.as_str(),
                )
                .await
            {
                Ok(ProofResolution::Authorized(proof)) => proof.decision().clone(),
                Ok(ProofResolution::Denied(decision)) => decision,
                Err(_) => missing_binary_resource(),
            }
        }
    }

    async fn authorize_voice_chunk(
        &self,
        request_context: &RequestContext,
        payload: &[u8],
    ) -> AuthorizationDecision {
        let Ok(chunk) = decode_gateway_voice_chunk_frame(payload) else {
            return missing_binary_resource();
        };
        let owner = AuthenticatedTransferOwner::from_request_context(request_context);
        let Ok(session) = self
            .voice_sessions
            .lookup_authenticated_session(chunk.header.session_id.as_str(), &owner)
        else {
            return missing_binary_resource();
        };
        let action = ResourceAction::ThreadWrite;
        let service = AuthorizationService::new();
        let gate = service.authorize_action(
            request_context.principal().kind,
            request_context.role_key(),
            action,
        );
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        match resolver
            .authorize_thread(
                request_context.principal(),
                &gate,
                action,
                session.thread_id.as_str(),
                Some(session.workspace_id.as_str()),
            )
            .await
        {
            Ok(ProofResolution::Authorized(proof)) => proof.decision().clone(),
            Ok(ProofResolution::Denied(decision)) => decision,
            Err(_) => missing_binary_resource(),
        }
    }

    #[cfg(test)]
    pub(crate) async fn process_binary_frame_for_connection(
        &self,
        connection_id: ConnectionId,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        let context = self
            .session_manager
            .connection_context(connection_id)
            .await
            .map_err(|_| {
                anyhow::anyhow!("binary frame rejected for unregistered connection {connection_id}")
            })?;
        self.process_binary_frame(&context, payload).await;
        Ok(())
    }

    pub(crate) async fn process_voice_chunk_frame(
        &self,
        request_context: &RequestContext,
        payload: &[u8],
    ) {
        let connection_id = request_context.connection_id();
        match decode_gateway_voice_chunk_frame(payload) {
            Ok(chunk) => {
                self.forward_decoded_voice_chunk(request_context, chunk)
                    .await;
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
        request_context: &RequestContext,
        chunk: GatewayVoiceChunkFrame,
    ) {
        let connection_id = request_context.connection_id();
        let owner = AuthenticatedTransferOwner::from_request_context(request_context);
        let session = match self
            .voice_sessions
            .lookup_authenticated_session(chunk.header.session_id.as_str(), &owner)
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

fn missing_binary_resource() -> AuthorizationDecision {
    AuthorizationDecision::Deny {
        reason: DenyReason::MissingAuthoritativeResource,
        disclosure: DisclosurePolicy::NotFound,
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

        let kind = classify_binary_payload(payload.as_slice());

        assert_eq!(kind, Some(BinaryIngressKind::SkillUploadChunk));
        assert_eq!(kind.expect("kind").safe_name(), "skills/upload/chunk");
    }

    #[test]
    fn binary_frame_kind_detects_artifact_upload_magic() {
        let mut payload = Vec::new();
        payload.extend_from_slice(ARTIFACT_UPLOAD_CHUNK_FRAME_MAGIC);
        payload.extend_from_slice(&0u32.to_be_bytes());

        let kind = classify_binary_payload(payload.as_slice());

        assert_eq!(kind, Some(BinaryIngressKind::ArtifactUploadChunk));
        assert_eq!(kind.expect("kind").safe_name(), "artifact/upload/chunk");
    }

    #[test]
    fn binary_frame_kind_detects_voice_chunk_magic() {
        let frame = voice_frame("voice_session_1", 0, b"\xff\x00").expect("voice frame");

        let kind = classify_binary_payload(frame.as_slice());

        assert_eq!(kind, Some(BinaryIngressKind::VoiceChunk));
        assert_eq!(kind.expect("kind").safe_name(), "voice/chunk");
    }

    #[test]
    fn binary_frame_kind_rejects_unknown_magic() {
        assert_eq!(classify_binary_payload(b"NOPE"), None);
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
