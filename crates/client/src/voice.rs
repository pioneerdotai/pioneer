//! Shell-neutral reductions for voice session command responses and events.

use pioneer_protocol::{
    VoiceError, VoiceSessionFinalizeResponse, VoiceSessionOutcome, VoiceSessionResultNotification,
    VoiceStatus,
};
use serde::{Deserialize, Serialize};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VoiceFinalizeUiAction {
    KeepFinalizing,
    ClearFinalizing,
    ShowNoSpeechError,
    ShowFinalizeError,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VoiceFinalizeResponseReduction {
    pub session_id: String,
    pub status: VoiceStatus,
    pub action: VoiceFinalizeUiAction,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VoiceSessionResultReduction {
    pub session_id: String,
    pub outcome: VoiceSessionOutcome,
    pub turn_id: Option<String>,
    pub action: VoiceFinalizeUiAction,
    pub error: Option<VoiceError>,
}

pub fn reduce_voice_session_finalize_response(
    session_id: impl Into<String>,
    response: &VoiceSessionFinalizeResponse,
) -> VoiceFinalizeResponseReduction {
    VoiceFinalizeResponseReduction {
        session_id: session_id.into(),
        status: response.status,
        action: match response.status {
            VoiceStatus::Recording | VoiceStatus::Transcribing | VoiceStatus::Busy => {
                VoiceFinalizeUiAction::KeepFinalizing
            }
            VoiceStatus::Ready
            | VoiceStatus::Unavailable
            | VoiceStatus::ModelDownloading
            | VoiceStatus::ModelLoading
            | VoiceStatus::Error => VoiceFinalizeUiAction::ClearFinalizing,
        },
    }
}

pub fn reduce_voice_session_result_notification(
    notification: &VoiceSessionResultNotification,
) -> VoiceSessionResultReduction {
    VoiceSessionResultReduction {
        session_id: notification.session_id.clone(),
        outcome: notification.outcome,
        turn_id: notification.turn_id.clone(),
        action: match notification.outcome {
            VoiceSessionOutcome::TurnStarted | VoiceSessionOutcome::Cancelled => {
                VoiceFinalizeUiAction::ClearFinalizing
            }
            VoiceSessionOutcome::NoSpeech => VoiceFinalizeUiAction::ShowNoSpeechError,
            VoiceSessionOutcome::Failed => VoiceFinalizeUiAction::ShowFinalizeError,
        },
        error: notification.error.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{VoiceErrorKind, VoiceSessionOutcome};

    #[test]
    fn finalize_transcribing_response_keeps_composer_finalizing() {
        let reduction = reduce_voice_session_finalize_response(
            "voice_session_1",
            &VoiceSessionFinalizeResponse {
                status: VoiceStatus::Transcribing,
            },
        );

        assert_eq!(reduction.session_id, "voice_session_1");
        assert_eq!(reduction.action, VoiceFinalizeUiAction::KeepFinalizing);
    }

    #[test]
    fn turn_started_result_clears_finalizing_composer() {
        let reduction = reduce_voice_session_result_notification(&VoiceSessionResultNotification {
            session_id: "voice_session_1".to_owned(),
            outcome: VoiceSessionOutcome::TurnStarted,
            turn_id: Some("turn_1".to_owned()),
            error: None,
        });

        assert_eq!(reduction.action, VoiceFinalizeUiAction::ClearFinalizing);
        assert_eq!(reduction.turn_id.as_deref(), Some("turn_1"));
    }

    #[test]
    fn no_speech_result_preserves_error_for_shell_presentation() {
        let reduction = reduce_voice_session_result_notification(&VoiceSessionResultNotification {
            session_id: "voice_session_1".to_owned(),
            outcome: VoiceSessionOutcome::NoSpeech,
            turn_id: None,
            error: Some(VoiceError {
                kind: VoiceErrorKind::NoSpeech,
                message: "no speech".to_owned(),
            }),
        });

        assert_eq!(reduction.action, VoiceFinalizeUiAction::ShowNoSpeechError);
        assert_eq!(
            reduction.error.as_ref().map(|error| error.kind),
            Some(VoiceErrorKind::NoSpeech)
        );
    }
}
