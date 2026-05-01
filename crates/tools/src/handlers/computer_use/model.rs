use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::time::Instant;

pub(crate) const DEFAULT_TIMEOUT_MS: u64 = 300_000;
pub(crate) const MAX_TEXT_CHARS: usize = 2_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComputerUseStatus {
    Running,
    Stopped,
}

impl ComputerUseStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComputerUseLoopState {
    Started,
    SnapshotCaptured,
    PlannerRequestBuilt,
    LlmDecisionReceived,
    ActionExecuted,
    PostActionResultReported,
    Completed,
    Failed,
    Stopped,
}

impl ComputerUseLoopState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::SnapshotCaptured => "snapshot_captured",
            Self::PlannerRequestBuilt => "planner_request_built",
            Self::LlmDecisionReceived => "llm_decision_received",
            Self::ActionExecuted => "action_executed",
            Self::PostActionResultReported => "post_action_result_reported",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComputerUseFailureClass {
    AttachmentTransportFailure,
    ProviderTimeout,
    ProviderRateLimit,
    ExpectedEffectMismatch,
    PolicyBlocked,
    RuntimeActionError,
    LoopGuardTriggered,
    RecoveryBudgetExceeded,
}

impl ComputerUseFailureClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::AttachmentTransportFailure => "attachment_transport_failure",
            Self::ProviderTimeout => "provider_timeout",
            Self::ProviderRateLimit => "provider_rate_limit",
            Self::ExpectedEffectMismatch => "expected_effect_mismatch",
            Self::PolicyBlocked => "policy_blocked",
            Self::RuntimeActionError => "runtime_action_error",
            Self::LoopGuardTriggered => "loop_guard_triggered",
            Self::RecoveryBudgetExceeded => "recovery_budget_exceeded",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "attachment_transport_failure" => Some(Self::AttachmentTransportFailure),
            "provider_timeout" => Some(Self::ProviderTimeout),
            "provider_rate_limit" => Some(Self::ProviderRateLimit),
            "expected_effect_mismatch" => Some(Self::ExpectedEffectMismatch),
            "policy_blocked" => Some(Self::PolicyBlocked),
            "runtime_action_error" => Some(Self::RuntimeActionError),
            "loop_guard_triggered" => Some(Self::LoopGuardTriggered),
            "recovery_budget_exceeded" => Some(Self::RecoveryBudgetExceeded),
            _ => None,
        }
    }

    pub(crate) fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::AttachmentTransportFailure
                | Self::ProviderTimeout
                | Self::ProviderRateLimit
                | Self::ExpectedEffectMismatch
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DisplayMeta {
    pub(crate) display_id: u32,
    pub(crate) width_px: u32,
    pub(crate) height_px: u32,
    pub(crate) scale_factor: f32,
    pub(crate) origin_x: i32,
    pub(crate) origin_y: i32,
    #[serde(default)]
    pub(crate) is_primary: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotMeta {
    pub(crate) index: u32,
    pub(crate) path: String,
    pub(crate) width_px: u32,
    pub(crate) height_px: u32,
    pub(crate) scale_factor: f32,
    pub(crate) size_bytes: usize,
    pub(crate) resize_passes: u32,
    pub(crate) captured_at_unix_ms: i64,
    pub(crate) state_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ActionRecord {
    pub(crate) index: u32,
    pub(crate) action_type: String,
    pub(crate) payload: JsonValue,
    pub(crate) executed_at_unix_ms: i64,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotBudget {
    pub(crate) provider_hint: Option<String>,
    pub(crate) model_hint: Option<String>,
    pub(crate) profile: String,
    pub(crate) max_bytes: usize,
    pub(crate) max_side_px: u32,
    pub(crate) min_side_px: u32,
    pub(crate) downscale_factor: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LoopGuardState {
    pub(crate) consecutive_same_snapshot_hash: u32,
    pub(crate) consecutive_same_action_signature: u32,
    pub(crate) consecutive_no_progress_steps: u32,
    pub(crate) max_same_snapshot_hash: u32,
    pub(crate) max_same_action_signature: u32,
    pub(crate) max_no_progress_steps: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct ComputerUseSession {
    pub(crate) session_id: u64,
    pub(crate) goal: String,
    pub(crate) status: ComputerUseStatus,
    pub(crate) loop_state: ComputerUseLoopState,
    pub(crate) updated_at_unix_ms: i64,
    pub(crate) created_at_mono: Instant,
    pub(crate) timeout_ms: u64,
    pub(crate) max_steps: u32,
    pub(crate) step_count: u32,
    pub(crate) snapshot_count: u32,
    pub(crate) display: DisplayMeta,
    pub(crate) last_snapshot: Option<SnapshotMeta>,
    pub(crate) last_action: Option<ActionRecord>,
    pub(crate) stop_reason: Option<String>,
    pub(crate) stop_failure_class: Option<ComputerUseFailureClass>,
    pub(crate) last_action_signature: Option<String>,
    pub(crate) awaiting_post_action_snapshot: bool,
    pub(crate) loop_guard: LoopGuardState,
    pub(crate) recovery_attempts_current_step: u32,
    pub(crate) recovery_attempts_run: u32,
    pub(crate) max_recovery_attempts_per_step: u32,
    pub(crate) max_recovery_attempts_per_run: u32,
    pub(crate) snapshot_budget: SnapshotBudget,
    pub(crate) artifacts_dir: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ComputerUseArgs {
    pub(crate) action: String,
    #[serde(default)]
    pub(crate) session_id: Option<u64>,
    #[serde(default)]
    pub(crate) goal: Option<String>,
    #[serde(default)]
    pub(crate) display_id: Option<u32>,
    #[serde(default)]
    pub(crate) screenshot_path: Option<String>,
    #[serde(default)]
    pub(crate) act: Option<ComputerUseActArgs>,
    #[serde(default)]
    pub(crate) max_steps: Option<u32>,
    #[serde(default)]
    pub(crate) timeout_ms: Option<u64>,
    #[serde(default)]
    pub(crate) planner_provider: Option<String>,
    #[serde(default)]
    pub(crate) planner_model: Option<String>,
    #[serde(default)]
    pub(crate) snapshot_max_bytes: Option<usize>,
    #[serde(default)]
    pub(crate) snapshot_max_side_px: Option<u32>,
    #[serde(default)]
    pub(crate) recovery_attempt: Option<u32>,
    #[serde(default)]
    pub(crate) expected_effect_mismatch: Option<bool>,
    #[serde(default)]
    pub(crate) failure_class: Option<String>,
    #[serde(default)]
    pub(crate) outcome: Option<String>,
    #[serde(default)]
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ComputerUseActArgs {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) x_norm: Option<f64>,
    #[serde(default)]
    pub(crate) y_norm: Option<f64>,
    #[serde(default)]
    pub(crate) button: Option<String>,
    #[serde(default)]
    pub(crate) delta_x: Option<i32>,
    #[serde(default)]
    pub(crate) delta_y: Option<i32>,
    #[serde(default)]
    pub(crate) text: Option<String>,
    #[serde(default)]
    pub(crate) keys: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MouseButtonKind {
    Left,
    Right,
    Middle,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ResolvedAction {
    Move {
        x: i32,
        y: i32,
    },
    Click {
        x: i32,
        y: i32,
        button: MouseButtonKind,
        click_count: u8,
    },
    Scroll {
        delta_x: i32,
        delta_y: i32,
    },
    TypeText {
        text: String,
    },
    Hotkey {
        keys: Vec<String>,
    },
    Wait {
        wait_ms: u64,
    },
}

impl ResolvedAction {
    pub(crate) fn action_type(&self) -> &'static str {
        match self {
            Self::Move { .. } => "move",
            Self::Click { click_count: 2, .. } => "double_click",
            Self::Click {
                button: MouseButtonKind::Right,
                ..
            } => "right_click",
            Self::Click { .. } => "click",
            Self::Scroll { .. } => "scroll",
            Self::TypeText { .. } => "type_text",
            Self::Hotkey { .. } => "hotkey",
            Self::Wait { .. } => "wait",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CapturedFrame {
    pub(crate) width_px: u32,
    pub(crate) height_px: u32,
    pub(crate) scale_factor: f32,
    pub(crate) png_bytes: Vec<u8>,
    pub(crate) resize_passes: u32,
}
