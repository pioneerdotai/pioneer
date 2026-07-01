//! Code-facing product contract for local voice composer input.
//!
//! This module intentionally does not define wire DTOs, WebSocket methods,
//! audio frames, transcription runtime APIs, or UI state. It is a small shared
//! specification for future voice implementation work.
//!
//! Contract summary:
//!
//! - A normal voice release is an explicit send gesture.
//! - The gateway owns transcription and creates the normal turn.
//! - Clients must not insert the transcript into composer draft state.
//! - Clients must not call `turn/start` with a transcript they received from
//!   voice transcription.
//! - Cancelled and no-speech voice sessions create no turn and no timeline user
//!   message.
//! - Clients stream microphone PCM chunks while the user holds the voice
//!   control.
//! - Clients do not store a full local recording for later upload.
//! - Audio chunk transport carries only audio bytes plus audio metadata.
//! - Files, artifacts, skills, MCP selections, model/mode/reasoning, agent
//!   permission, and execution options travel through a frozen voice turn
//!   context and existing turn materialization semantics.
//! - Platform microphone permission is a client-side capture gate. It is not
//!   the same thing as Pioneer agent permission policy.

/// Future voice APIs must use this owner for successful voice-to-turn creation.
pub const VOICE_TURN_OWNER: &str = "gateway";

/// Successful voice transcription must become the first text input of a normal
/// turn created by the gateway.
pub const VOICE_TRANSCRIPT_TURN_INPUT_KIND: &str = "UserInput::Text";

/// Clients must render voice results from the existing turn/timeline event
/// stream instead of inserting the transcript into composer draft state.
pub const VOICE_RESULT_DELIVERY: &str = "existing_turn_and_timeline_notifications";

/// Cancel and no-speech sessions are terminal no-turn outcomes.
pub const VOICE_CANCEL_AND_NO_SPEECH_OUTCOME: &str = "no_turn_no_timeline_user_message";

/// Voice audio transport carries only microphone audio data and audio metadata.
pub const VOICE_AUDIO_TRANSPORT_PAYLOAD: &str = "microphone_pcm_chunks_only";

/// Non-audio composer context belongs to the frozen voice turn context.
pub const VOICE_NON_AUDIO_CONTEXT_LOCATION: &str = "frozen_voice_turn_context";

/// Name used in implementation notes when referring to Pioneer turn execution
/// permission policy. This is distinct from platform microphone permission.
pub const VOICE_AGENT_PERMISSION_CONTEXT: &str = "agent_permission";

/// Name used in implementation notes when referring to OS/platform microphone
/// access. This is a client-side gate before capture and before committed voice
/// session start.
pub const VOICE_PLATFORM_MICROPHONE_PERMISSION_GATE: &str = "platform_microphone_permission";

/// Stable list of non-negotiable voice input invariants.
pub const VOICE_PRODUCT_INVARIANTS: &[&str] = &[
    "normal_release_is_send",
    "gateway_transcribes_and_starts_turn",
    "client_never_inserts_transcript_into_draft",
    "client_never_calls_turn_start_with_voice_transcript",
    "cancel_creates_no_turn",
    "no_speech_creates_no_turn",
    "client_streams_chunks_while_holding",
    "client_does_not_store_full_recording_for_upload",
    "audio_chunks_carry_only_audio",
    "attachments_skills_mcp_use_frozen_turn_context",
    "agent_permission_is_not_microphone_permission",
];

/// Shared semantic state names for voice composer work.
///
/// These states are product states, not transport or runtime DTOs. Future
/// desktop, mobile, client, and gateway code should map local details into
/// these names instead of inventing parallel state machines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VoiceComposerState {
    Idle,
    Typing,
    PermissionRequesting,
    PermissionDenied,
    VoiceReady,
    Recording,
    SendCandidate,
    CancelCandidate,
    Finalizing,
    Error,
}

/// Identifies where a state is owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VoiceStateOwner {
    /// Local composer/capture state owned by desktop or mobile client code.
    ClientOnly,
    /// Gateway voice session/runtime state.
    GatewayOnly,
    /// State must be represented consistently in both client and gateway.
    SharedStatus,
}

/// Product-level transition triggers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VoiceTransitionTrigger {
    TextDraftBecameNonEmpty,
    TextDraftCleared,
    EnterVoiceMode,
    PressHold,
    PermissionGranted,
    PermissionDenied,
    GatewayBusy,
    ModelUnavailable,
    DesktopPointerInsideCircle,
    DesktopPointerOutsideCircle,
    MobileSwipeUpToCancel,
    ReleaseSend,
    ReleaseCancel,
    FinalizeSucceeded,
    NoSpeech,
    Disconnect,
    RecoverableError,
    Reset,
}

/// Platform-specific cancel gestures that map to the shared cancel-candidate
/// and release-cancel semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VoiceCancelGesture {
    /// Mobile uses full hold-surface red/cancel state after swipe up.
    MobileSwipeUpRelease,
    /// Desktop keeps the rail neutral and turns only the mic circle red when
    /// pointer release happens outside the circle.
    DesktopReleaseOutsideCircle,
}

/// One valid state transition in the shared voice state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoiceStateTransition {
    pub from: VoiceComposerState,
    pub trigger: VoiceTransitionTrigger,
    pub to: VoiceComposerState,
    pub owner: VoiceStateOwner,
    pub note: &'static str,
}

/// Stable shared state transition table.
pub const VOICE_STATE_TRANSITIONS: &[VoiceStateTransition] = &[
    VoiceStateTransition {
        from: VoiceComposerState::Idle,
        trigger: VoiceTransitionTrigger::TextDraftBecameNonEmpty,
        to: VoiceComposerState::Typing,
        owner: VoiceStateOwner::ClientOnly,
        note: "text draft shows existing Send instead of mic",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Typing,
        trigger: VoiceTransitionTrigger::TextDraftCleared,
        to: VoiceComposerState::Idle,
        owner: VoiceStateOwner::ClientOnly,
        note: "empty draft can show mic when voice is available",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Idle,
        trigger: VoiceTransitionTrigger::EnterVoiceMode,
        to: VoiceComposerState::VoiceReady,
        owner: VoiceStateOwner::ClientOnly,
        note: "mobile enters hold-to-talk mode; desktop may enter on hold",
    },
    VoiceStateTransition {
        from: VoiceComposerState::VoiceReady,
        trigger: VoiceTransitionTrigger::PressHold,
        to: VoiceComposerState::PermissionRequesting,
        owner: VoiceStateOwner::ClientOnly,
        note: "platform microphone permission/device gate runs before capture",
    },
    VoiceStateTransition {
        from: VoiceComposerState::PermissionRequesting,
        trigger: VoiceTransitionTrigger::PermissionGranted,
        to: VoiceComposerState::Recording,
        owner: VoiceStateOwner::SharedStatus,
        note: "capture starts and a gateway voice session can receive chunks",
    },
    VoiceStateTransition {
        from: VoiceComposerState::PermissionRequesting,
        trigger: VoiceTransitionTrigger::PermissionDenied,
        to: VoiceComposerState::PermissionDenied,
        owner: VoiceStateOwner::ClientOnly,
        note: "no capture, no committed gateway voice session, no turn",
    },
    VoiceStateTransition {
        from: VoiceComposerState::VoiceReady,
        trigger: VoiceTransitionTrigger::GatewayBusy,
        to: VoiceComposerState::Error,
        owner: VoiceStateOwner::SharedStatus,
        note: "mic remains unavailable until gateway can accept voice",
    },
    VoiceStateTransition {
        from: VoiceComposerState::VoiceReady,
        trigger: VoiceTransitionTrigger::ModelUnavailable,
        to: VoiceComposerState::Error,
        owner: VoiceStateOwner::SharedStatus,
        note: "model missing/downloading/loading disables voice entry",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Recording,
        trigger: VoiceTransitionTrigger::DesktopPointerInsideCircle,
        to: VoiceComposerState::SendCandidate,
        owner: VoiceStateOwner::ClientOnly,
        note: "desktop mic circle is blue; rail stays neutral",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Recording,
        trigger: VoiceTransitionTrigger::DesktopPointerOutsideCircle,
        to: VoiceComposerState::CancelCandidate,
        owner: VoiceStateOwner::ClientOnly,
        note: "desktop mic circle is red; rail stays neutral",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Recording,
        trigger: VoiceTransitionTrigger::MobileSwipeUpToCancel,
        to: VoiceComposerState::CancelCandidate,
        owner: VoiceStateOwner::ClientOnly,
        note: "mobile full hold surface becomes red",
    },
    VoiceStateTransition {
        from: VoiceComposerState::SendCandidate,
        trigger: VoiceTransitionTrigger::DesktopPointerOutsideCircle,
        to: VoiceComposerState::CancelCandidate,
        owner: VoiceStateOwner::ClientOnly,
        note: "desktop leaves circle and switches release target to cancel",
    },
    VoiceStateTransition {
        from: VoiceComposerState::CancelCandidate,
        trigger: VoiceTransitionTrigger::DesktopPointerInsideCircle,
        to: VoiceComposerState::SendCandidate,
        owner: VoiceStateOwner::ClientOnly,
        note: "desktop re-enters circle and switches release target to send",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Recording,
        trigger: VoiceTransitionTrigger::ReleaseSend,
        to: VoiceComposerState::Finalizing,
        owner: VoiceStateOwner::SharedStatus,
        note: "normal release finalizes gateway transcription",
    },
    VoiceStateTransition {
        from: VoiceComposerState::SendCandidate,
        trigger: VoiceTransitionTrigger::ReleaseSend,
        to: VoiceComposerState::Finalizing,
        owner: VoiceStateOwner::SharedStatus,
        note: "release sends through gateway-created turn flow",
    },
    VoiceStateTransition {
        from: VoiceComposerState::CancelCandidate,
        trigger: VoiceTransitionTrigger::ReleaseCancel,
        to: VoiceComposerState::Idle,
        owner: VoiceStateOwner::SharedStatus,
        note: "cancel drops audio/session context and creates no turn",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Finalizing,
        trigger: VoiceTransitionTrigger::FinalizeSucceeded,
        to: VoiceComposerState::Idle,
        owner: VoiceStateOwner::GatewayOnly,
        note: "gateway starts normal turn; clients render timeline events",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Finalizing,
        trigger: VoiceTransitionTrigger::NoSpeech,
        to: VoiceComposerState::Idle,
        owner: VoiceStateOwner::GatewayOnly,
        note: "empty/no-speech creates no turn and no draft transcript",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Recording,
        trigger: VoiceTransitionTrigger::Disconnect,
        to: VoiceComposerState::Idle,
        owner: VoiceStateOwner::SharedStatus,
        note: "disconnect cleans up active session without a turn",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Finalizing,
        trigger: VoiceTransitionTrigger::RecoverableError,
        to: VoiceComposerState::Error,
        owner: VoiceStateOwner::SharedStatus,
        note: "transcription/session errors surface without draft insertion",
    },
    VoiceStateTransition {
        from: VoiceComposerState::PermissionDenied,
        trigger: VoiceTransitionTrigger::Reset,
        to: VoiceComposerState::Idle,
        owner: VoiceStateOwner::ClientOnly,
        note: "user can retry after denial/settings change",
    },
    VoiceStateTransition {
        from: VoiceComposerState::Error,
        trigger: VoiceTransitionTrigger::Reset,
        to: VoiceComposerState::Idle,
        owner: VoiceStateOwner::ClientOnly,
        note: "recoverable errors return composer to a stable state",
    },
];

pub const VOICE_CANCEL_GESTURE_CONTRACT: &[VoiceCancelGesture] = &[
    VoiceCancelGesture::MobileSwipeUpRelease,
    VoiceCancelGesture::DesktopReleaseOutsideCircle,
];

pub const fn voice_state_owner(state: VoiceComposerState) -> VoiceStateOwner {
    match state {
        VoiceComposerState::Idle
        | VoiceComposerState::Typing
        | VoiceComposerState::PermissionRequesting
        | VoiceComposerState::PermissionDenied
        | VoiceComposerState::VoiceReady
        | VoiceComposerState::SendCandidate
        | VoiceComposerState::CancelCandidate => VoiceStateOwner::ClientOnly,
        VoiceComposerState::Recording | VoiceComposerState::Finalizing => {
            VoiceStateOwner::SharedStatus
        }
        VoiceComposerState::Error => VoiceStateOwner::SharedStatus,
    }
}

pub fn is_valid_voice_state_transition(
    from: VoiceComposerState,
    trigger: VoiceTransitionTrigger,
    to: VoiceComposerState,
) -> bool {
    VOICE_STATE_TRANSITIONS.iter().any(|transition| {
        transition.from == from && transition.trigger == trigger && transition.to == to
    })
}

/// Client surface covered by the voice UX acceptance matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VoiceUxPlatform {
    Desktop,
    Mobile,
}

/// Product scenarios that every implementation should validate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VoiceUxScenario {
    EmptyComposerShowsMic,
    TypedDraftShowsSend,
    DesktopHoldInsideCircle,
    DesktopPointerOutsideCircle,
    DesktopReleaseInsideCircle,
    DesktopReleaseOutsideCircle,
    DesktopPermissionDenied,
    DesktopModelUnavailable,
    MobileEnterVoiceMode,
    MobileKeyboardBackToTyping,
    MobileHoldSurface,
    MobileSwipeUpCancel,
    MobileReleaseSend,
    MobilePermissionDenied,
    MobileModelUnavailable,
}

/// Expected outcome for one UX scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoiceUxAcceptanceCase {
    pub platform: VoiceUxPlatform,
    pub scenario: VoiceUxScenario,
    pub expected_state: VoiceComposerState,
    pub gateway_voice_session_opened: bool,
    pub timeline_user_message_expected: bool,
    pub note: &'static str,
}

/// Compact desktop/mobile acceptance matrix for Proposal 42 voice input.
pub const VOICE_UX_ACCEPTANCE_MATRIX: &[VoiceUxAcceptanceCase] = &[
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Desktop,
        scenario: VoiceUxScenario::EmptyComposerShowsMic,
        expected_state: VoiceComposerState::Idle,
        gateway_voice_session_opened: false,
        timeline_user_message_expected: false,
        note: "empty desktop composer shows mic in the existing Send slot",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Desktop,
        scenario: VoiceUxScenario::TypedDraftShowsSend,
        expected_state: VoiceComposerState::Typing,
        gateway_voice_session_opened: false,
        timeline_user_message_expected: false,
        note: "typed draft uses existing typed-message Send flow",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Desktop,
        scenario: VoiceUxScenario::DesktopHoldInsideCircle,
        expected_state: VoiceComposerState::SendCandidate,
        gateway_voice_session_opened: true,
        timeline_user_message_expected: false,
        note: "holding inside circle streams chunks; mic circle blue, rail neutral",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Desktop,
        scenario: VoiceUxScenario::DesktopPointerOutsideCircle,
        expected_state: VoiceComposerState::CancelCandidate,
        gateway_voice_session_opened: true,
        timeline_user_message_expected: false,
        note: "pointer outside circle turns only mic circle red; rail stays neutral",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Desktop,
        scenario: VoiceUxScenario::DesktopReleaseInsideCircle,
        expected_state: VoiceComposerState::Finalizing,
        gateway_voice_session_opened: true,
        timeline_user_message_expected: true,
        note: "release inside finalizes and gateway starts normal turn",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Desktop,
        scenario: VoiceUxScenario::DesktopReleaseOutsideCircle,
        expected_state: VoiceComposerState::Idle,
        gateway_voice_session_opened: true,
        timeline_user_message_expected: false,
        note: "release outside cancels active session and creates no turn",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Desktop,
        scenario: VoiceUxScenario::DesktopPermissionDenied,
        expected_state: VoiceComposerState::PermissionDenied,
        gateway_voice_session_opened: false,
        timeline_user_message_expected: false,
        note: "blocked microphone access prevents capture and committed session",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Desktop,
        scenario: VoiceUxScenario::DesktopModelUnavailable,
        expected_state: VoiceComposerState::Error,
        gateway_voice_session_opened: false,
        timeline_user_message_expected: false,
        note: "model missing/downloading/loading disables desktop voice entry",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Mobile,
        scenario: VoiceUxScenario::EmptyComposerShowsMic,
        expected_state: VoiceComposerState::Idle,
        gateway_voice_session_opened: false,
        timeline_user_message_expected: false,
        note: "empty mobile composer shows mic in the existing Send slot",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Mobile,
        scenario: VoiceUxScenario::TypedDraftShowsSend,
        expected_state: VoiceComposerState::Typing,
        gateway_voice_session_opened: false,
        timeline_user_message_expected: false,
        note: "typed draft uses existing typed-message Send flow",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Mobile,
        scenario: VoiceUxScenario::MobileEnterVoiceMode,
        expected_state: VoiceComposerState::VoiceReady,
        gateway_voice_session_opened: false,
        timeline_user_message_expected: false,
        note: "tap mic enters voice mode and shows keyboard/back control",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Mobile,
        scenario: VoiceUxScenario::MobileKeyboardBackToTyping,
        expected_state: VoiceComposerState::Idle,
        gateway_voice_session_opened: false,
        timeline_user_message_expected: false,
        note: "keyboard/back exits voice mode without sending",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Mobile,
        scenario: VoiceUxScenario::MobileHoldSurface,
        expected_state: VoiceComposerState::SendCandidate,
        gateway_voice_session_opened: true,
        timeline_user_message_expected: false,
        note: "holding full surface streams chunks; surface blue with send text",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Mobile,
        scenario: VoiceUxScenario::MobileSwipeUpCancel,
        expected_state: VoiceComposerState::CancelCandidate,
        gateway_voice_session_opened: true,
        timeline_user_message_expected: false,
        note: "swipe up turns full hold surface red and release will cancel",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Mobile,
        scenario: VoiceUxScenario::MobileReleaseSend,
        expected_state: VoiceComposerState::Finalizing,
        gateway_voice_session_opened: true,
        timeline_user_message_expected: true,
        note: "normal release finalizes and gateway starts normal turn",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Mobile,
        scenario: VoiceUxScenario::MobilePermissionDenied,
        expected_state: VoiceComposerState::PermissionDenied,
        gateway_voice_session_opened: false,
        timeline_user_message_expected: false,
        note: "blocked microphone access prevents capture and committed session",
    },
    VoiceUxAcceptanceCase {
        platform: VoiceUxPlatform::Mobile,
        scenario: VoiceUxScenario::MobileModelUnavailable,
        expected_state: VoiceComposerState::Error,
        gateway_voice_session_opened: false,
        timeline_user_message_expected: false,
        note: "model missing/downloading/loading disables mobile voice entry",
    },
];
