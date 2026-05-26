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
    PermissionDenied,
    AccessibilityUnavailable,
    AccessibilityNotEnabled,
    AppNotFound,
    ElementNotFound,
    ElementStale,
    ActionNotSupported,
    InputSimulationUnavailable,
    ScreenshotUnavailable,
    AttachmentTransportFailure,
    ProviderTimeout,
    ProviderRateLimit,
    LoopGuardTriggered,
    RecoveryBudgetExceeded,
    RuntimeActionError,
}

impl ComputerUseFailureClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::PermissionDenied => "permission_denied",
            Self::AccessibilityUnavailable => "accessibility_unavailable",
            Self::AccessibilityNotEnabled => "accessibility_not_enabled",
            Self::AppNotFound => "app_not_found",
            Self::ElementNotFound => "element_not_found",
            Self::ElementStale => "element_stale",
            Self::ActionNotSupported => "action_not_supported",
            Self::InputSimulationUnavailable => "input_simulation_unavailable",
            Self::ScreenshotUnavailable => "screenshot_unavailable",
            Self::AttachmentTransportFailure => "attachment_transport_failure",
            Self::ProviderTimeout => "provider_timeout",
            Self::ProviderRateLimit => "provider_rate_limit",
            Self::LoopGuardTriggered => "loop_guard_triggered",
            Self::RecoveryBudgetExceeded => "recovery_budget_exceeded",
            Self::RuntimeActionError => "runtime_action_error",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match normalize_failure_class_token(value).as_str() {
            "permissiondenied" => Some(Self::PermissionDenied),
            "accessibilityunavailable" => Some(Self::AccessibilityUnavailable),
            "accessibilitynotenabled" => Some(Self::AccessibilityNotEnabled),
            "appnotfound" => Some(Self::AppNotFound),
            "elementnotfound" => Some(Self::ElementNotFound),
            "elementstale" => Some(Self::ElementStale),
            "actionnotsupported" => Some(Self::ActionNotSupported),
            "inputsimulationunavailable" => Some(Self::InputSimulationUnavailable),
            "screenshotunavailable" => Some(Self::ScreenshotUnavailable),
            "attachmenttransportfailure" => Some(Self::AttachmentTransportFailure),
            "providertimeout" => Some(Self::ProviderTimeout),
            "providerratelimit" => Some(Self::ProviderRateLimit),
            "loopguardtriggered" => Some(Self::LoopGuardTriggered),
            "recoverybudgetexceeded" => Some(Self::RecoveryBudgetExceeded),
            "runtimeactionerror" => Some(Self::RuntimeActionError),
            _ => None,
        }
    }

    pub(crate) fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::AttachmentTransportFailure
                | Self::ProviderTimeout
                | Self::ProviderRateLimit
                | Self::ElementNotFound
                | Self::ElementStale
                | Self::ScreenshotUnavailable
        )
    }

    pub(crate) fn is_fatal_action_failure(self) -> bool {
        matches!(
            self,
            Self::PermissionDenied
                | Self::AccessibilityUnavailable
                | Self::AccessibilityNotEnabled
                | Self::ScreenshotUnavailable
                | Self::AttachmentTransportFailure
                | Self::ProviderTimeout
                | Self::ProviderRateLimit
                | Self::LoopGuardTriggered
                | Self::RecoveryBudgetExceeded
        )
    }
}

fn normalize_failure_class_token(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-')
        .flat_map(char::to_lowercase)
        .collect()
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
    pub(crate) snapshot_id: String,
    pub(crate) path: String,
    pub(crate) width_px: u32,
    pub(crate) height_px: u32,
    pub(crate) transport_width_px: u32,
    pub(crate) transport_height_px: u32,
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
    pub(crate) target: ComputerUseTarget,
    pub(crate) last_snapshot: Option<SnapshotMeta>,
    pub(crate) last_accessibility_tree: Option<AccessibilityTreePayload>,
    pub(crate) last_node_refs: Vec<AccessibilityNodeRef>,
    pub(crate) last_action: Option<ActionRecord>,
    pub(crate) last_verification: Option<VerifyRecord>,
    pub(crate) previous_verification_status: Option<String>,
    pub(crate) last_completion_evidence: Option<CompletionEvidence>,
    pub(crate) last_evidence_at_step: Option<u32>,
    pub(crate) last_progress_signals: Option<ProgressSignals>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VerifyRecord {
    pub(crate) status: String,
    pub(crate) evidence: JsonValue,
    pub(crate) verified_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CompletionEvidence {
    pub(crate) source: String,
    pub(crate) strength: String,
    pub(crate) summary: String,
    pub(crate) evidence: JsonValue,
    pub(crate) recorded_at_unix_ms: i64,
    pub(crate) step_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) snapshot_index: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ProgressSignals {
    pub(crate) screenshot_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_screenshot_hash: Option<String>,
    pub(crate) screenshot_hash_changed: bool,
    pub(crate) tree_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_tree_hash: Option<String>,
    pub(crate) tree_hash_changed: bool,
    pub(crate) target_exists: bool,
    pub(crate) target_disappeared: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) focused_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_focused_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) focused_node_changed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selected_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_selected_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selected_node_changed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) active_app: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_active_app: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) active_app_changed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) window_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_window_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) window_title_changed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target_node_signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_target_node_signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target_node_changed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) verification_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_verification_status: Option<String>,
    pub(crate) verification_failed_to_passed: bool,
    pub(crate) no_progress: bool,
    pub(crate) changed_signals: Vec<String>,
}

impl ProgressSignals {
    pub(crate) fn has_meaningful_progress(&self) -> bool {
        !self.no_progress
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputerUseArgs {
    pub(crate) action: String,
    #[serde(default)]
    pub(crate) session_id: Option<u64>,
    #[serde(default)]
    pub(crate) goal: Option<String>,
    #[serde(default)]
    pub(crate) target: Option<ComputerUseTargetArgs>,
    #[serde(default)]
    pub(crate) display_id: Option<u32>,
    #[serde(default)]
    pub(crate) launch_if_missing: Option<bool>,
    #[serde(default)]
    pub(crate) launch_command: Option<String>,
    #[serde(default)]
    pub(crate) activation_timeout_ms: Option<u64>,
    #[serde(default)]
    pub(crate) tree_max_depth: Option<usize>,
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
    pub(crate) failure_class: Option<String>,
    #[serde(default)]
    pub(crate) outcome: Option<String>,
    #[serde(default)]
    pub(crate) reason: Option<String>,
    #[serde(default)]
    pub(crate) expect: Option<ComputerUseVerifyArgs>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputerUseVerifyArgs {
    #[serde(default)]
    pub(crate) app: Option<String>,
    #[serde(default)]
    pub(crate) window_title: Option<String>,
    #[serde(default)]
    pub(crate) visible_text: Option<String>,
    #[serde(default)]
    pub(crate) node: Option<ComputerUseVerifyNodeArgs>,
    #[serde(default)]
    pub(crate) snapshot_hash_changed: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputerUseVerifyNodeArgs {
    #[serde(default)]
    pub(crate) node_id: Option<String>,
    #[serde(default)]
    pub(crate) selector: Option<String>,
    #[serde(default)]
    pub(crate) role: Option<String>,
    #[serde(default)]
    pub(crate) name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputerUseTargetArgs {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) pid: Option<u32>,
    #[serde(default)]
    pub(crate) identity_key: Option<String>,
    #[serde(default)]
    pub(crate) bundle_id: Option<String>,
    #[serde(default)]
    pub(crate) executable_path: Option<String>,
    #[serde(default)]
    pub(crate) display_id: Option<u32>,
    #[serde(default)]
    pub(crate) launch_if_missing: Option<bool>,
    #[serde(default)]
    pub(crate) launch_command: Option<String>,
    #[serde(default)]
    pub(crate) activation_timeout_ms: Option<u64>,
    #[serde(default)]
    pub(crate) tree_max_depth: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputerUseActArgs {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) target: Option<ActionTarget>,
    #[serde(default)]
    pub(crate) from: Option<ActionTarget>,
    #[serde(default)]
    pub(crate) to: Option<ActionTarget>,
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
    pub(crate) numeric_value: Option<f64>,
    #[serde(default)]
    pub(crate) action_name: Option<String>,
    #[serde(default)]
    pub(crate) condition: Option<String>,
    #[serde(default)]
    pub(crate) wait_ms: Option<u64>,
    #[serde(default)]
    pub(crate) app: Option<String>,
    #[serde(default)]
    pub(crate) path: Option<String>,
    #[serde(default)]
    pub(crate) url: Option<String>,
    #[serde(default)]
    pub(crate) menu_path: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActionTarget {
    #[serde(default)]
    pub(crate) node_id: Option<String>,
    #[serde(default)]
    pub(crate) snapshot_id: Option<String>,
    #[serde(default)]
    pub(crate) selector: Option<String>,
    #[serde(default)]
    pub(crate) role: Option<String>,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) nth: Option<usize>,
    #[serde(default)]
    pub(crate) bounds_anchor: Option<BoundsAnchorTarget>,
    #[serde(default)]
    pub(crate) point: Option<PointTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct BoundsAnchorTarget {
    pub(crate) node_id: String,
    #[serde(default)]
    pub(crate) snapshot_id: Option<String>,
    #[serde(default)]
    pub(crate) anchor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PointTarget {
    pub(crate) x: i32,
    pub(crate) y: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) coordinate_space: Option<CoordinateSpace>,
}

pub(crate) type InputPoint = PointTarget;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CoordinateSpace {
    SourcePixels,
    TransportPixels,
    LogicalScreen,
    NativeInput,
}

impl CoordinateSpace {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SourcePixels => "source_pixels",
            Self::TransportPixels => "transport_pixels",
            Self::LogicalScreen => "logical_screen",
            Self::NativeInput => "native_input",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MouseButtonKind {
    Left,
    Right,
    Middle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ComputerUseAction {
    Os(OsAction),
    Semantic(SemanticAction),
    Input(InputAction),
}

impl ComputerUseAction {
    pub(crate) fn action_type(&self) -> &'static str {
        match self {
            Self::Os(action) => action.action_type.as_str(),
            Self::Semantic(action) => action.action_type.as_str(),
            Self::Input(action) => action.action_type.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct DesktopPreflightReport {
    pub(crate) platform: String,
    pub(crate) status: String,
    pub(crate) message: String,
    pub(crate) capabilities: DesktopPreflightCapabilities,
    pub(crate) blocking_issues: Vec<String>,
    pub(crate) warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DesktopPreflightOptions {
    pub(crate) screenshot_probe_enabled: bool,
    pub(crate) input_simulation_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct DesktopPreflightCapabilities {
    pub(crate) accessibility_tree: String,
    pub(crate) accessibility_actions: String,
    pub(crate) screenshot: String,
    pub(crate) input_simulation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct AppMeta {
    #[serde(default)]
    pub(crate) identity_key: Option<String>,
    pub(crate) name: String,
    pub(crate) pid: Option<u32>,
    #[serde(default)]
    pub(crate) role: Option<String>,
    #[serde(default)]
    pub(crate) window_title: Option<String>,
    #[serde(default)]
    pub(crate) bundle_id: Option<String>,
    #[serde(default)]
    pub(crate) localized_name: Option<String>,
    #[serde(default)]
    pub(crate) executable_path: Option<String>,
    #[serde(default)]
    pub(crate) frontmost: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct AppTarget {
    pub(crate) name: Option<String>,
    pub(crate) pid: Option<u32>,
    #[serde(default)]
    pub(crate) identity_key: Option<String>,
    #[serde(default)]
    pub(crate) bundle_id: Option<String>,
    #[serde(default)]
    pub(crate) executable_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct AppHandle {
    #[serde(default)]
    pub(crate) identity_key: Option<String>,
    pub(crate) name: String,
    pub(crate) pid: Option<u32>,
    #[serde(default)]
    pub(crate) role: Option<String>,
    #[serde(default)]
    pub(crate) window_title: Option<String>,
    #[serde(default)]
    pub(crate) bundle_id: Option<String>,
    #[serde(default)]
    pub(crate) localized_name: Option<String>,
    #[serde(default)]
    pub(crate) executable_path: Option<String>,
    #[serde(default)]
    pub(crate) frontmost: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct AppHandleMeta {
    #[serde(default)]
    pub(crate) identity_key: Option<String>,
    pub(crate) name: String,
    pub(crate) pid: Option<u32>,
    #[serde(default)]
    pub(crate) role: Option<String>,
    #[serde(default)]
    pub(crate) window_title: Option<String>,
    #[serde(default)]
    pub(crate) bundle_id: Option<String>,
    #[serde(default)]
    pub(crate) localized_name: Option<String>,
    #[serde(default)]
    pub(crate) executable_path: Option<String>,
    #[serde(default)]
    pub(crate) frontmost: Option<bool>,
}

impl From<AppHandle> for AppHandleMeta {
    fn from(value: AppHandle) -> Self {
        Self {
            identity_key: value.identity_key,
            name: value.name,
            pid: value.pid,
            role: value.role,
            window_title: value.window_title,
            bundle_id: value.bundle_id,
            localized_name: value.localized_name,
            executable_path: value.executable_path,
            frontmost: value.frontmost,
        }
    }
}

impl From<AppMeta> for AppHandleMeta {
    fn from(value: AppMeta) -> Self {
        Self {
            identity_key: value.identity_key,
            name: value.name,
            pid: value.pid,
            role: value.role,
            window_title: value.window_title,
            bundle_id: value.bundle_id,
            localized_name: value.localized_name,
            executable_path: value.executable_path,
            frontmost: value.frontmost,
        }
    }
}

pub(crate) fn derive_app_identity_key(
    name: &str,
    pid: Option<u32>,
    bundle_id: Option<&str>,
    executable_path: Option<&str>,
) -> String {
    if let Some(bundle_id) = non_empty_identity_value(bundle_id) {
        return format!("bundle:{bundle_id}");
    }
    if let Some(executable_path) = non_empty_identity_value(executable_path) {
        return format!("exe:{executable_path}");
    }
    if let Some(pid) = pid {
        return format!("pid:{pid}");
    }
    format!("name:{}", normalize_identity_name(name))
}

fn non_empty_identity_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn normalize_identity_name(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(dead_code)]
pub(crate) enum ComputerUseTarget {
    Screen {
        display: DisplayMeta,
    },
    App {
        requested: AppTarget,
        app: AppHandleMeta,
        display: DisplayMeta,
        tree_max_depth: usize,
    },
    ActiveApp {
        app: AppHandleMeta,
        display: DisplayMeta,
        tree_max_depth: usize,
    },
}

impl ComputerUseTarget {
    pub(crate) fn display(&self) -> &DisplayMeta {
        match self {
            Self::Screen { display }
            | Self::App { display, .. }
            | Self::ActiveApp { display, .. } => display,
        }
    }

    pub(crate) fn snapshot_target(&self) -> SnapshotTarget {
        match self {
            Self::Screen { display } => SnapshotTarget::Display {
                display_id: display.display_id,
            },
            Self::App { .. } | Self::ActiveApp { .. } => SnapshotTarget::PrimaryScreen,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct DesktopTree {
    pub(crate) payload: AccessibilityTreePayload,
    #[serde(skip)]
    pub(crate) node_refs: Vec<AccessibilityNodeRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct AccessibilityTreePayload {
    pub(crate) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
    pub(crate) nodes: Vec<CompactAccessibilityNode>,
    pub(crate) truncated: bool,
    pub(crate) omitted_count: usize,
    pub(crate) max_depth: usize,
    pub(crate) max_nodes: usize,
    pub(crate) serialized_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CompactAccessibilityNode {
    pub(crate) id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parent_id: Option<String>,
    pub(crate) depth: usize,
    pub(crate) role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bounds: Option<AccessibilityBounds>,
    pub(crate) states: Vec<String>,
    pub(crate) supported_act_types: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) raw_actions: Vec<String>,
    pub(crate) selector_hints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct AccessibilityBounds {
    pub(crate) x: i32,
    pub(crate) y: i32,
    pub(crate) width: u32,
    pub(crate) height: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct AccessibilityNodeRef {
    pub(crate) id: String,
    pub(crate) selector_hints: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stable_id: Option<String>,
    pub(crate) role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bounds: Option<AccessibilityBounds>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) supported_act_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) enum SnapshotTarget {
    PrimaryScreen,
    Display { display_id: u32 },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SemanticActionKind {
    Press,
    Focus,
    Blur,
    Toggle,
    Select,
    Expand,
    Collapse,
    ShowMenu,
    ScrollIntoView,
    SetValue,
    SetNumericValue,
    TypeText,
    SelectText,
    PerformAction,
    WaitFor,
}

impl SemanticActionKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Press => "press",
            Self::Focus => "focus",
            Self::Blur => "blur",
            Self::Toggle => "toggle",
            Self::Select => "select",
            Self::Expand => "expand",
            Self::Collapse => "collapse",
            Self::ShowMenu => "show_menu",
            Self::ScrollIntoView => "scroll_into_view",
            Self::SetValue => "set_value",
            Self::SetNumericValue => "set_numeric_value",
            Self::TypeText => "type_text",
            Self::SelectText => "select_text",
            Self::PerformAction => "perform_action",
            Self::WaitFor => "wait_for",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InputActionKind {
    InputClick,
    InputDoubleClick,
    InputRightClick,
    InputMove,
    InputDrag,
    InputScroll,
    InputKey,
    InputChord,
    InputTypeText,
    Wait,
}

impl InputActionKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::InputClick => "input_click",
            Self::InputDoubleClick => "input_double_click",
            Self::InputRightClick => "input_right_click",
            Self::InputMove => "input_move",
            Self::InputDrag => "input_drag",
            Self::InputScroll => "input_scroll",
            Self::InputKey => "input_key",
            Self::InputChord => "input_chord",
            Self::InputTypeText => "input_type_text",
            Self::Wait => "wait",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OsActionKind {
    OpenApp,
    ActivateApp,
    OpenPath,
    RevealPath,
    OpenUrl,
    SelectMenuItem,
    FocusWindow,
}

impl OsActionKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::OpenApp => "open_app",
            Self::ActivateApp => "activate_app",
            Self::OpenPath => "open_path",
            Self::RevealPath => "reveal_path",
            Self::OpenUrl => "open_url",
            Self::SelectMenuItem => "select_menu_item",
            Self::FocusWindow => "focus_window",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct SemanticAction {
    #[serde(rename = "type")]
    pub(crate) action_type: SemanticActionKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target: Option<ActionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) numeric_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) action_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct InputAction {
    #[serde(rename = "type")]
    pub(crate) action_type: InputActionKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target: Option<ActionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) from: Option<ActionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) to: Option<ActionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) button: Option<MouseButtonKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) delta_x: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) delta_y: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) keys: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct OsAction {
    #[serde(rename = "type")]
    pub(crate) action_type: OsActionKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) app: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) menu_path: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[allow(dead_code)]
pub(crate) struct SuggestedAction {
    #[serde(rename = "type")]
    pub(crate) action_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target: Option<ActionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) numeric_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) action_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) app: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) menu_path: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub(crate) enum ResolvedActionTargetKind {
    Locator,
    Point,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[allow(dead_code)]
pub(crate) struct ResolvedActionTarget {
    pub(crate) kind: ResolvedActionTargetKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selector: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) nth: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bounds: Option<AccessibilityBounds>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) requested_point: Option<PointTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) point: Option<InputPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[allow(dead_code)]
pub(crate) struct ResolvedInputActionTargets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target: Option<ResolvedActionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) from: Option<ResolvedActionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) to: Option<ResolvedActionTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct ActionExecution {
    pub(crate) status: String,
    pub(crate) message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) action_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target: Option<ResolvedActionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) app_before: Option<AppHandleMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) app_after: Option<AppHandleMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) details: Option<JsonValue>,
}

#[derive(Debug, Clone)]
pub(crate) struct CapturedFrame {
    pub(crate) width_px: u32,
    pub(crate) height_px: u32,
    pub(crate) transport_width_px: u32,
    pub(crate) transport_height_px: u32,
    pub(crate) scale_factor: f32,
    pub(crate) png_bytes: Vec<u8>,
    pub(crate) resize_passes: u32,
}
