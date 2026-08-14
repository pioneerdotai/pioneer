use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    AgentExecutionBackend, SandboxPolicy, ThreadMode, TurnCLIRuntimeOptions, TurnCapability,
    TurnPermissionProfileSelection, TurnReasoningSelection, TurnStartParams, UserInput,
};

pub const VOICE_CHUNK_FRAME_MAGIC: &[u8; 4] = b"VOC1";
pub const VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ: u32 = 16_000;
pub const VOICE_AUDIO_TARGET_CHANNELS: u16 = 1;
pub const VOICE_AUDIO_TARGET_BYTES_PER_SAMPLE: usize = 2;
pub const VOICE_AUDIO_TARGET_CHUNK_DURATION_MS: u32 = 20;
pub const VOICE_AUDIO_MAX_CHUNK_DURATION_MS: u32 = 100;
pub const VOICE_AUDIO_TARGET_CHUNK_BYTES: usize = 640;
pub const VOICE_AUDIO_MAX_CHUNK_BYTES: usize = 3_200;
pub const VOICE_AUDIO_FORMAT_CONTRACT: &str =
    "streaming microphone PCM only: pcm_s16le, 16000 Hz, mono, target 20 ms chunks";

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VoiceStatus {
    Disabled,
    Unavailable,
    /// Model download is happening in the gateway background. First pass UI
    /// should not infer or require detailed progress from this status.
    ModelDownloading,
    ModelLoading,
    Ready,
    Busy,
    Recording,
    Transcribing,
    Error,
}

impl VoiceStatus {
    pub const fn voice_entry_available(self) -> bool {
        matches!(self, Self::Ready)
    }

    pub const fn disables_voice_entry(self) -> bool {
        !self.voice_entry_available()
    }

    pub const fn is_model_bootstrap(self) -> bool {
        matches!(self, Self::ModelDownloading | Self::ModelLoading)
    }

    pub const fn is_active_session(self) -> bool {
        matches!(self, Self::Recording | Self::Transcribing)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VoiceErrorKind {
    ModelUnavailable,
    MicrophonePermissionBlocked,
    DeviceUnavailable,
    InvalidSession,
    StaleChunk,
    SequenceGap,
    Cancelled,
    NoSpeech,
    TranscriptionFailed,
    GatewayBusy,
    ModelDownloading,
    Unknown,
}

impl VoiceErrorKind {
    pub const fn is_platform_microphone_permission(self) -> bool {
        matches!(self, Self::MicrophonePermissionBlocked)
    }

    pub const fn is_gateway_model_status(self) -> bool {
        matches!(self, Self::ModelUnavailable | Self::ModelDownloading)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct VoiceError {
    pub kind: VoiceErrorKind,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_error: Option<crate::PublicError>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VoiceAudioEncoding {
    PcmS16Le,
    /// Reserved for future client adapters. The initial gateway intake
    /// contract accepts only `pcm_s16le`.
    PcmF32Le,
}

/// Audio format metadata for a voice session.
///
/// This is control metadata only. It does not carry audio bytes.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VoiceAudioFormat {
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub encoding: VoiceAudioEncoding,
}

impl VoiceAudioFormat {
    pub const fn pioneer_streaming_target() -> Self {
        Self {
            sample_rate_hz: VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ,
            channels: VOICE_AUDIO_TARGET_CHANNELS,
            encoding: VoiceAudioEncoding::PcmS16Le,
        }
    }

    pub const fn matches_pioneer_streaming_target(&self) -> bool {
        self.sample_rate_hz == VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ
            && self.channels == VOICE_AUDIO_TARGET_CHANNELS
            && matches!(self.encoding, VoiceAudioEncoding::PcmS16Le)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoiceAudioFormatValidationError {
    UnsupportedSampleRate {
        expected: u32,
        actual: u32,
    },
    UnsupportedChannels {
        expected: u16,
        actual: u16,
    },
    UnsupportedEncoding {
        expected: VoiceAudioEncoding,
        actual: VoiceAudioEncoding,
    },
}

impl std::fmt::Display for VoiceAudioFormatValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSampleRate { expected, actual } => write!(
                formatter,
                "voice audio sample_rate_hz must be {expected}, got {actual}"
            ),
            Self::UnsupportedChannels { expected, actual } => {
                write!(
                    formatter,
                    "voice audio channels must be {expected}, got {actual}"
                )
            }
            Self::UnsupportedEncoding { expected, actual } => {
                write!(
                    formatter,
                    "voice audio encoding must be {expected:?}, got {actual:?}"
                )
            }
        }
    }
}

impl std::error::Error for VoiceAudioFormatValidationError {}

pub fn validate_voice_streaming_audio_format(
    format: &VoiceAudioFormat,
) -> Result<(), VoiceAudioFormatValidationError> {
    if format.sample_rate_hz != VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ {
        return Err(VoiceAudioFormatValidationError::UnsupportedSampleRate {
            expected: VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ,
            actual: format.sample_rate_hz,
        });
    }
    if format.channels != VOICE_AUDIO_TARGET_CHANNELS {
        return Err(VoiceAudioFormatValidationError::UnsupportedChannels {
            expected: VOICE_AUDIO_TARGET_CHANNELS,
            actual: format.channels,
        });
    }
    if !matches!(format.encoding, VoiceAudioEncoding::PcmS16Le) {
        return Err(VoiceAudioFormatValidationError::UnsupportedEncoding {
            expected: VoiceAudioEncoding::PcmS16Le,
            actual: format.encoding,
        });
    }

    Ok(())
}

/// JSON header carried by a `VOC1` binary voice frame.
///
/// The frame payload following this header is raw audio bytes only. Finalize
/// and cancel are control DTOs, not binary frame markers.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct VoiceChunkFrameHeader {
    pub session_id: String,
    pub sequence: u64,
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub encoding: VoiceAudioEncoding,
    pub payload_len: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u32>,
}

impl VoiceChunkFrameHeader {
    pub fn audio_format(&self) -> VoiceAudioFormat {
        VoiceAudioFormat {
            sample_rate_hz: self.sample_rate_hz,
            channels: self.channels,
            encoding: self.encoding,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedVoiceChunkFrame<'a> {
    pub header: VoiceChunkFrameHeader,
    pub audio_payload: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoiceFrameEncodeError {
    MissingSessionId,
    InvalidAudioFormat,
    EmptyPayload,
    PayloadLengthOverflow,
    HeaderTooLarge,
    HeaderEncodeFailed(String),
}

impl std::fmt::Display for VoiceFrameEncodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingSessionId => formatter.write_str("voice session_id is required"),
            Self::InvalidAudioFormat => formatter.write_str("voice audio format is invalid"),
            Self::EmptyPayload => formatter.write_str("voice audio payload cannot be empty"),
            Self::PayloadLengthOverflow => {
                formatter.write_str("voice audio payload length overflow")
            }
            Self::HeaderTooLarge => formatter.write_str("voice frame header is too large"),
            Self::HeaderEncodeFailed(error) => {
                write!(formatter, "failed to encode voice frame header: {error}")
            }
        }
    }
}

impl std::error::Error for VoiceFrameEncodeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoiceFrameDecodeError {
    TooShort,
    InvalidMagic,
    HeaderOutOfBounds,
    HeaderDecodeFailed(String),
    MissingSessionId,
    InvalidAudioFormat,
    EmptyPayload,
    PayloadLengthMismatch { expected: u64, actual: u64 },
}

impl std::fmt::Display for VoiceFrameDecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort => formatter.write_str("voice frame is too short"),
            Self::InvalidMagic => formatter.write_str("voice frame magic/version is invalid"),
            Self::HeaderOutOfBounds => formatter.write_str("voice frame header is out of bounds"),
            Self::HeaderDecodeFailed(error) => {
                write!(formatter, "failed to decode voice frame header: {error}")
            }
            Self::MissingSessionId => formatter.write_str("voice session_id is required"),
            Self::InvalidAudioFormat => formatter.write_str("voice audio format is invalid"),
            Self::EmptyPayload => formatter.write_str("voice audio payload cannot be empty"),
            Self::PayloadLengthMismatch { expected, actual } => write!(
                formatter,
                "voice audio payload length mismatch: expected {expected}, got {actual}"
            ),
        }
    }
}

impl std::error::Error for VoiceFrameDecodeError {}

pub fn encode_voice_chunk_frame(
    mut header: VoiceChunkFrameHeader,
    audio_payload: &[u8],
) -> Result<Vec<u8>, VoiceFrameEncodeError> {
    validate_voice_chunk_header_common(
        header.session_id.as_str(),
        header.sample_rate_hz,
        header.channels,
        header.encoding,
    )
    .map_err(|error| match error {
        VoiceFrameDecodeError::MissingSessionId => VoiceFrameEncodeError::MissingSessionId,
        VoiceFrameDecodeError::InvalidAudioFormat => VoiceFrameEncodeError::InvalidAudioFormat,
        _ => VoiceFrameEncodeError::InvalidAudioFormat,
    })?;

    if audio_payload.is_empty() {
        return Err(VoiceFrameEncodeError::EmptyPayload);
    }

    header.payload_len = u64::try_from(audio_payload.len())
        .map_err(|_| VoiceFrameEncodeError::PayloadLengthOverflow)?;

    let header_bytes = serde_json::to_vec(&header)
        .map_err(|error| VoiceFrameEncodeError::HeaderEncodeFailed(error.to_string()))?;
    let header_len =
        u32::try_from(header_bytes.len()).map_err(|_| VoiceFrameEncodeError::HeaderTooLarge)?;

    let mut frame = Vec::with_capacity(
        VOICE_CHUNK_FRAME_MAGIC.len() + 4 + header_bytes.len() + audio_payload.len(),
    );
    frame.extend_from_slice(VOICE_CHUNK_FRAME_MAGIC);
    frame.extend_from_slice(&header_len.to_be_bytes());
    frame.extend_from_slice(header_bytes.as_slice());
    frame.extend_from_slice(audio_payload);
    Ok(frame)
}

pub fn decode_voice_chunk_frame(
    frame: &[u8],
) -> Result<DecodedVoiceChunkFrame<'_>, VoiceFrameDecodeError> {
    if frame.len() < 8 {
        return Err(VoiceFrameDecodeError::TooShort);
    }
    if &frame[0..4] != VOICE_CHUNK_FRAME_MAGIC {
        return Err(VoiceFrameDecodeError::InvalidMagic);
    }

    let header_len = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
    let header_start = 8usize;
    let header_end = header_start
        .checked_add(header_len)
        .ok_or(VoiceFrameDecodeError::HeaderOutOfBounds)?;
    if header_end > frame.len() {
        return Err(VoiceFrameDecodeError::HeaderOutOfBounds);
    }

    let header: VoiceChunkFrameHeader = serde_json::from_slice(&frame[header_start..header_end])
        .map_err(|error| VoiceFrameDecodeError::HeaderDecodeFailed(error.to_string()))?;
    validate_voice_chunk_header_common(
        header.session_id.as_str(),
        header.sample_rate_hz,
        header.channels,
        header.encoding,
    )?;

    let audio_payload = &frame[header_end..];
    if audio_payload.is_empty() {
        return Err(VoiceFrameDecodeError::EmptyPayload);
    }
    let actual = u64::try_from(audio_payload.len()).unwrap_or(u64::MAX);
    if header.payload_len != actual {
        return Err(VoiceFrameDecodeError::PayloadLengthMismatch {
            expected: header.payload_len,
            actual,
        });
    }

    Ok(DecodedVoiceChunkFrame {
        header,
        audio_payload,
    })
}

fn validate_voice_chunk_header_common(
    session_id: &str,
    sample_rate_hz: u32,
    channels: u16,
    encoding: VoiceAudioEncoding,
) -> Result<(), VoiceFrameDecodeError> {
    if session_id.trim().is_empty() {
        return Err(VoiceFrameDecodeError::MissingSessionId);
    }
    let format = VoiceAudioFormat {
        sample_rate_hz,
        channels,
        encoding,
    };
    if validate_voice_streaming_audio_format(&format).is_err() {
        return Err(VoiceFrameDecodeError::InvalidAudioFormat);
    }
    Ok(())
}

/// Frozen non-audio composer context for voice turn materialization.
///
/// `prepared_input` is for existing prepared `UserInput` references such as
/// artifacts/local attachment references. It must not contain the future voice
/// transcript; the gateway prepends the transcript as `UserInput::Text` after
/// successful transcription.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct VoiceTurnContext {
    /// Workspace active when the voice session starts.
    ///
    /// `TurnStartParams` is still thread-scoped; the gateway keeps the
    /// workspace here to validate/session-route the frozen voice context.
    pub workspace_id: String,
    /// Thread that receives the gateway-created turn after transcription.
    pub thread_id: String,
    /// Client-planned turn id reserved before any audio chunk is accepted.
    pub turn_id: String,
    /// Existing prepared composer inputs such as artifact/file references.
    ///
    /// This vector must not contain audio bytes or the future transcript. On
    /// cancel, dropping this context must not create a turn; already completed
    /// upload/cache side effects are handled by the existing attachment flow.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prepared_input: Vec<UserInput>,
    /// Selected skills, MCP tools/servers and related turn capabilities.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<TurnCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_policy: Option<SandboxPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ThreadMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_backend: Option<AgentExecutionBackend>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<TurnReasoningSelection>,
    /// Agent permission profile for the eventual turn.
    ///
    /// This is not the platform microphone permission. Microphone permission
    /// stays client/platform-local and is reported through voice status/errors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_profile: Option<TurnPermissionProfileSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_runtime_options: Option<TurnCLIRuntimeOptions>,
}

impl VoiceTurnContext {
    pub fn into_turn_start_params_with_transcript(self, transcript: String) -> TurnStartParams {
        let mut input = Vec::with_capacity(1 + self.prepared_input.len());
        input.push(UserInput::Text {
            text: transcript,
            text_elements: Vec::new(),
        });
        input.extend(self.prepared_input);

        TurnStartParams {
            thread_id: self.thread_id,
            turn_id: self.turn_id,
            input,
            capabilities: self.capabilities,
            model: self.model,
            model_provider: self.model_provider,
            sandbox_policy: self.sandbox_policy,
            mode: self.mode,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: self.execution_backend,
            reasoning: self.reasoning,
            permission_profile: self.permission_profile,
            cli_runtime_options: self.cli_runtime_options,
        }
    }
}

/// Minimal context required to route and own a streaming voice session.
///
/// Full turn materialization context is provided on commit/finalize so clients can
/// start microphone streaming before slower attachment/capability preparation.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct VoiceSessionStartContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct VoiceStatusParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct VoiceStatusResponse {
    pub status: VoiceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<VoiceError>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct VoiceSessionStartParams {
    pub context: VoiceSessionStartContext,
    pub audio_format: VoiceAudioFormat,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct VoiceSessionStartResponse {
    pub session_id: String,
    pub status: VoiceStatus,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct VoiceSessionFinalizeParams {
    pub session_id: String,
    pub context: VoiceTurnContext,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct VoiceSessionFinalizeResponse {
    pub status: VoiceStatus,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct VoiceSessionCancelParams {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct VoiceSessionCancelResponse {
    pub cancelled: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VoiceSessionOutcome {
    TurnStarted,
    Cancelled,
    NoSpeech,
    Failed,
}

/// Voice session terminal notification.
///
/// This never carries transcript text. Successful user-message rendering comes
/// from the normal `turn/started` and timeline notifications.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct VoiceSessionResultNotification {
    pub session_id: String,
    pub outcome: VoiceSessionOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<VoiceError>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct VoiceChunkAckNotification {
    pub session_id: String,
    pub sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentExecutionBackend, ThreadMode, TurnCapability, TurnCapabilityKind, TurnPermissionMode,
        TurnPermissionProfileSelection, TurnReasoningSelection, UserInput,
    };

    fn voice_header() -> VoiceChunkFrameHeader {
        VoiceChunkFrameHeader {
            session_id: "voice_session_1".to_owned(),
            sequence: 7,
            sample_rate_hz: 16_000,
            channels: 1,
            encoding: VoiceAudioEncoding::PcmS16Le,
            payload_len: 0,
            captured_at_unix_ms: Some(1_725_000_000_000),
            duration_ms: Some(20),
        }
    }

    #[test]
    fn voice_chunk_frame_round_trips_header_and_raw_audio_payload() {
        let audio = [1u8, 2, 3, 4, 5, 6];
        let encoded =
            encode_voice_chunk_frame(voice_header(), &audio).expect("voice frame should encode");

        assert_eq!(&encoded[0..4], VOICE_CHUNK_FRAME_MAGIC);

        let decoded =
            decode_voice_chunk_frame(encoded.as_slice()).expect("voice frame should decode");

        assert_eq!(decoded.header.session_id, "voice_session_1");
        assert_eq!(decoded.header.sequence, 7);
        assert_eq!(
            decoded.header.audio_format(),
            VoiceAudioFormat {
                sample_rate_hz: 16_000,
                channels: 1,
                encoding: VoiceAudioEncoding::PcmS16Le,
            }
        );
        assert_eq!(decoded.header.payload_len, audio.len() as u64);
        assert_eq!(decoded.audio_payload, audio);
    }

    #[test]
    fn voice_chunk_frame_header_has_no_turn_context_attachment_or_capability_fields() {
        let value =
            serde_json::to_value(voice_header()).expect("voice header should serialize to json");
        let object = value.as_object().expect("voice header should be an object");

        for forbidden_key in [
            "workspace_id",
            "thread_id",
            "turn_id",
            "prepared_input",
            "capabilities",
            "attachments",
            "input",
            "permission_profile",
        ] {
            assert!(
                !object.contains_key(forbidden_key),
                "voice binary frame header must not carry {forbidden_key}"
            );
        }
    }

    #[test]
    fn voice_chunk_frame_rejects_invalid_magic_version_and_header_bounds() {
        assert_eq!(
            decode_voice_chunk_frame(b"VOC"),
            Err(VoiceFrameDecodeError::TooShort)
        );

        let mut wrong_magic = Vec::new();
        wrong_magic.extend_from_slice(b"VOC2");
        wrong_magic.extend_from_slice(&0u32.to_be_bytes());
        assert_eq!(
            decode_voice_chunk_frame(wrong_magic.as_slice()),
            Err(VoiceFrameDecodeError::InvalidMagic)
        );

        let mut bad_header_len = Vec::new();
        bad_header_len.extend_from_slice(VOICE_CHUNK_FRAME_MAGIC);
        bad_header_len.extend_from_slice(&128u32.to_be_bytes());
        assert_eq!(
            decode_voice_chunk_frame(bad_header_len.as_slice()),
            Err(VoiceFrameDecodeError::HeaderOutOfBounds)
        );
    }

    #[test]
    fn voice_chunk_frame_rejects_invalid_header_and_payload() {
        assert_eq!(
            encode_voice_chunk_frame(voice_header(), b""),
            Err(VoiceFrameEncodeError::EmptyPayload)
        );

        let mut missing_session = voice_header();
        missing_session.session_id = " ".to_owned();
        assert_eq!(
            encode_voice_chunk_frame(missing_session, b"pcm"),
            Err(VoiceFrameEncodeError::MissingSessionId)
        );

        let mut invalid_format = voice_header();
        invalid_format.sample_rate_hz = 0;
        assert_eq!(
            encode_voice_chunk_frame(invalid_format, b"pcm"),
            Err(VoiceFrameEncodeError::InvalidAudioFormat)
        );

        let mut truncated =
            encode_voice_chunk_frame(voice_header(), b"pcm").expect("voice frame should encode");
        truncated.pop();
        assert_eq!(
            decode_voice_chunk_frame(truncated.as_slice()),
            Err(VoiceFrameDecodeError::PayloadLengthMismatch {
                expected: 3,
                actual: 2,
            })
        );
    }

    #[test]
    fn voice_control_dtos_round_trip_through_serde() {
        let context = VoiceTurnContext {
            workspace_id: "workspace_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            prepared_input: vec![UserInput::LocalImage {
                path: "/tmp/image.png".to_owned(),
            }],
            capabilities: vec![TurnCapability {
                id: "skill:mvg02zVNGWuw5z5C4nYDo".to_owned(),
                kind: TurnCapabilityKind::Skill {
                    skill_id: "mvg02zVNGWuw5z5C4nYDo".parse().expect("valid skill id"),
                    pack_id: None,
                },
                label: Some("Demo".to_owned()),
            }],
            model: Some("model_1".to_owned()),
            model_provider: Some("provider_1".to_owned()),
            sandbox_policy: None,
            mode: Some(ThreadMode::Agent),
            execution_backend: Some(AgentExecutionBackend::ApiProvider {
                provider: "provider_1".to_owned(),
            }),
            reasoning: Some(TurnReasoningSelection {
                effort: "medium".to_owned(),
            }),
            permission_profile: Some(TurnPermissionProfileSelection {
                mode: TurnPermissionMode::AutoAcceptEdits,
            }),
            cli_runtime_options: None,
        };
        let start = VoiceSessionStartParams {
            context: VoiceSessionStartContext {
                workspace_id: context.workspace_id.clone(),
                thread_id: context.thread_id.clone(),
                turn_id: context.turn_id.clone(),
            },
            audio_format: VoiceAudioFormat::pioneer_streaming_target(),
        };
        let start_round_trip: VoiceSessionStartParams =
            serde_json::from_value(serde_json::to_value(&start).expect("serialize start"))
                .expect("deserialize start");
        assert_eq!(start_round_trip, start);

        let status = VoiceStatusResponse {
            status: VoiceStatus::ModelLoading,
            active_session_id: None,
            error: Some(VoiceError {
                kind: VoiceErrorKind::ModelDownloading,
                message: "loading".to_owned(),
                public_error: None,
            }),
        };
        let status_round_trip: VoiceStatusResponse =
            serde_json::from_value(serde_json::to_value(&status).expect("serialize status"))
                .expect("deserialize status");
        assert_eq!(status_round_trip, status);

        let finalize = VoiceSessionFinalizeParams {
            session_id: "voice_session_1".to_owned(),
            context,
        };
        let finalize_round_trip: VoiceSessionFinalizeParams =
            serde_json::from_value(serde_json::to_value(&finalize).expect("serialize finalize"))
                .expect("deserialize finalize");
        assert_eq!(finalize_round_trip, finalize);

        let cancel = VoiceSessionCancelParams {
            session_id: "voice_session_1".to_owned(),
            reason: Some("swipe_cancel".to_owned()),
        };
        let cancel_round_trip: VoiceSessionCancelParams =
            serde_json::from_value(serde_json::to_value(&cancel).expect("serialize cancel"))
                .expect("deserialize cancel");
        assert_eq!(cancel_round_trip, cancel);

        let result = VoiceSessionResultNotification {
            session_id: "voice_session_1".to_owned(),
            outcome: VoiceSessionOutcome::TurnStarted,
            turn_id: Some("turn_1".to_owned()),
            error: None,
        };
        let result_round_trip: VoiceSessionResultNotification =
            serde_json::from_value(serde_json::to_value(&result).expect("serialize result"))
                .expect("deserialize result");
        assert_eq!(result_round_trip, result);

        let ack = VoiceChunkAckNotification {
            session_id: "voice_session_1".to_owned(),
            sequence: 42,
        };
        let ack_round_trip: VoiceChunkAckNotification =
            serde_json::from_value(serde_json::to_value(&ack).expect("serialize ack"))
                .expect("deserialize ack");
        assert_eq!(ack_round_trip, ack);
    }

    #[test]
    fn voice_context_prepends_transcript_without_embedding_it_in_frozen_context() {
        let context = VoiceTurnContext {
            workspace_id: "workspace_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            prepared_input: vec![UserInput::Artifact {
                artifact_id: "artifact_1".to_owned(),
                version_id: Some("version_1".to_owned()),
            }],
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        };

        let params = context.into_turn_start_params_with_transcript("hello voice".to_owned());

        assert_eq!(
            params.input,
            vec![
                UserInput::Text {
                    text: "hello voice".to_owned(),
                    text_elements: Vec::new(),
                },
                UserInput::Artifact {
                    artifact_id: "artifact_1".to_owned(),
                    version_id: Some("version_1".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn disabled_voice_status_roundtrips_and_disables_entry() {
        let serialized = serde_json::to_string(&VoiceStatus::Disabled)
            .expect("disabled voice status should serialize");
        assert_eq!(serialized, "\"disabled\"");

        let roundtrip: VoiceStatus =
            serde_json::from_str(&serialized).expect("disabled voice status should deserialize");
        assert_eq!(roundtrip, VoiceStatus::Disabled);
        assert!(roundtrip.disables_voice_entry());
        assert!(!roundtrip.is_model_bootstrap());
        assert!(!roundtrip.is_active_session());
    }
}
