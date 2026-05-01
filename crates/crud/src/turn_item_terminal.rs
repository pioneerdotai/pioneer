use pioneer_protocol::{
    StorageOutputPolicy, TimelineOutputPolicy, ToolCallStatus, ToolDisplayPayload, ToolErrorClass,
    ToolMetadata, ToolOutcome, ToolOutcomeStatus, ToolStoragePayload, TurnItem,
    TurnItemAttemptStatus, TurnItemTimeoutReason,
};

use crate::convention::{
    TURN_ITEM_STATUS_CANCELLED, TURN_ITEM_STATUS_COMPLETED, TURN_ITEM_STATUS_FAILED,
    TURN_ITEM_STATUS_TIMED_OUT,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnItemTerminalState {
    Completed,
    Failed { reason: Option<String> },
    TimedOut { reason: TurnItemTimeoutReason },
    Cancelled { reason: Option<String> },
}

impl TurnItemTerminalState {
    pub fn to_turn_item_status(&self) -> &'static str {
        match self {
            Self::Completed => TURN_ITEM_STATUS_COMPLETED,
            Self::Failed { .. } => TURN_ITEM_STATUS_FAILED,
            Self::TimedOut { .. } => TURN_ITEM_STATUS_TIMED_OUT,
            Self::Cancelled { .. } => TURN_ITEM_STATUS_CANCELLED,
        }
    }

    fn terminal_tool_status(&self) -> ToolCallStatus {
        match self {
            Self::Completed => ToolCallStatus::Completed,
            Self::Failed { .. } | Self::TimedOut { .. } | Self::Cancelled { .. } => {
                ToolCallStatus::Failed
            }
        }
    }

    fn timed_out(&self) -> bool {
        matches!(self, Self::TimedOut { .. })
    }

    fn outcome(&self, existing: Option<ToolOutcome>) -> ToolOutcome {
        match self {
            Self::Completed => existing.unwrap_or(ToolOutcome {
                status: ToolOutcomeStatus::Ok,
                error_class: None,
                should_retry: false,
                retry_hint: None,
                incomplete: false,
                incomplete_reason: None,
            }),
            Self::Failed { reason } => {
                let mut outcome = existing.unwrap_or(ToolOutcome {
                    status: ToolOutcomeStatus::FatalError,
                    error_class: Some(ToolErrorClass::ExecutionFailed),
                    should_retry: false,
                    retry_hint: None,
                    incomplete: true,
                    incomplete_reason: None,
                });
                outcome.status = ToolOutcomeStatus::FatalError;
                outcome
                    .error_class
                    .get_or_insert(ToolErrorClass::ExecutionFailed);
                outcome.should_retry = false;
                outcome.incomplete = true;
                if outcome.incomplete_reason.is_none() {
                    outcome.incomplete_reason =
                        reason.clone().or_else(|| Some("item_failed".to_owned()));
                }
                outcome
            }
            Self::TimedOut { reason } => {
                let mut outcome = existing.unwrap_or(ToolOutcome {
                    status: ToolOutcomeStatus::RecoverableError,
                    error_class: Some(ToolErrorClass::Timeout),
                    should_retry: true,
                    retry_hint: Some("Tool execution timed out.".to_owned()),
                    incomplete: true,
                    incomplete_reason: None,
                });
                outcome.status = ToolOutcomeStatus::RecoverableError;
                outcome.error_class = Some(ToolErrorClass::Timeout);
                outcome.should_retry = true;
                outcome.incomplete = true;
                outcome.incomplete_reason = Some(timeout_reason_to_string(*reason));
                if outcome.retry_hint.is_none() {
                    outcome.retry_hint = Some("Tool execution timed out.".to_owned());
                }
                outcome
            }
            Self::Cancelled { reason } => {
                let mut outcome = existing.unwrap_or(ToolOutcome {
                    status: ToolOutcomeStatus::FatalError,
                    error_class: Some(ToolErrorClass::Cancelled),
                    should_retry: false,
                    retry_hint: None,
                    incomplete: true,
                    incomplete_reason: None,
                });
                outcome.status = ToolOutcomeStatus::FatalError;
                outcome.error_class = Some(ToolErrorClass::Cancelled);
                outcome.should_retry = false;
                outcome.incomplete = true;
                if outcome.incomplete_reason.is_none() {
                    outcome.incomplete_reason =
                        reason.clone().or_else(|| Some("item_cancelled".to_owned()));
                }
                outcome
            }
        }
    }
}

pub fn tool_call_status(item: &TurnItem) -> Option<ToolCallStatus> {
    match item {
        TurnItem::CommandExecution { status, .. }
        | TurnItem::FileChange { status, .. }
        | TurnItem::WebSearch { status, .. }
        | TurnItem::WebFetch { status, .. }
        | TurnItem::Download { status, .. }
        | TurnItem::DynamicToolCall { status, .. } => Some(*status),
        _ => None,
    }
}

pub fn terminal_turn_item_status_from_payload(item: &TurnItem) -> &'static str {
    match tool_call_status(item) {
        Some(ToolCallStatus::InProgress) => TURN_ITEM_STATUS_IN_PROGRESS,
        Some(ToolCallStatus::Failed) => TURN_ITEM_STATUS_FAILED,
        Some(ToolCallStatus::Completed) => TURN_ITEM_STATUS_COMPLETED,
        None => TURN_ITEM_STATUS_COMPLETED,
    }
}

pub fn attempt_status_from_payload(item: &TurnItem) -> TurnItemAttemptStatus {
    match tool_call_status(item) {
        Some(ToolCallStatus::Failed) => TurnItemAttemptStatus::Failed,
        Some(ToolCallStatus::Completed) => TurnItemAttemptStatus::Completed,
        Some(ToolCallStatus::InProgress) => TurnItemAttemptStatus::Running,
        None => TurnItemAttemptStatus::Completed,
    }
}

pub fn terminalize_turn_item_payload(item: &mut TurnItem, state: TurnItemTerminalState) {
    match item {
        TurnItem::CommandExecution {
            status,
            output_policy,
            display,
            storage,
            success,
            outcome,
            ..
        }
        | TurnItem::FileChange {
            status,
            output_policy,
            display,
            storage,
            success,
            outcome,
            ..
        }
        | TurnItem::WebSearch {
            status,
            output_policy,
            display,
            storage,
            success,
            outcome,
            ..
        }
        | TurnItem::WebFetch {
            status,
            output_policy,
            display,
            storage,
            success,
            outcome,
            ..
        }
        | TurnItem::Download {
            status,
            output_policy,
            display,
            storage,
            success,
            outcome,
            ..
        }
        | TurnItem::DynamicToolCall {
            status,
            output_policy,
            display,
            storage,
            success,
            outcome,
            ..
        } => {
            *status = state.terminal_tool_status();
            *success = Some(matches!(state, TurnItemTerminalState::Completed));
            let existing = outcome.take();
            *outcome = Some(state.outcome(existing));
            *display = normalize_terminal_display(
                std::mem::take(display),
                &output_policy.timeline,
                state.timed_out(),
            );
            *storage = normalize_terminal_storage(
                std::mem::take(storage),
                &output_policy.storage,
                state.timed_out(),
            );
        }
        _ => {}
    }
}

fn normalize_terminal_display(
    display: ToolDisplayPayload,
    policy: &TimelineOutputPolicy,
    timed_out: bool,
) -> ToolDisplayPayload {
    let display = match display {
        ToolDisplayPayload::Progress { .. } => ToolDisplayPayload::Hidden,
        ToolDisplayPayload::Shell {
            stdout,
            stderr,
            aggregated_output,
            exit_code,
            duration_ms,
            timed_out: previous_timed_out,
            truncated,
        } => ToolDisplayPayload::Shell {
            stdout,
            stderr,
            aggregated_output,
            exit_code,
            duration_ms,
            timed_out: Some(previous_timed_out.unwrap_or(false) || timed_out),
            truncated,
        },
        other => other,
    };

    match policy {
        TimelineOutputPolicy::Full { .. } => display,
        TimelineOutputPolicy::Summary { .. } => match display {
            ToolDisplayPayload::Summary(_) | ToolDisplayPayload::Hidden => display,
            _ => ToolDisplayPayload::Hidden,
        },
        TimelineOutputPolicy::MetadataOnly | TimelineOutputPolicy::Hidden => {
            ToolDisplayPayload::Hidden
        }
    }
}

fn normalize_terminal_storage(
    storage: ToolStoragePayload,
    policy: &StorageOutputPolicy,
    timed_out: bool,
) -> ToolStoragePayload {
    let storage = match storage {
        ToolStoragePayload::Shell {
            stdout,
            stderr,
            aggregated_output,
            exit_code,
            duration_ms,
            timed_out: previous_timed_out,
            truncated,
        } => ToolStoragePayload::Shell {
            stdout,
            stderr,
            aggregated_output,
            exit_code,
            duration_ms,
            timed_out: Some(previous_timed_out.unwrap_or(false) || timed_out),
            truncated,
        },
        other => other,
    };

    match policy {
        StorageOutputPolicy::Full { .. } => storage,
        StorageOutputPolicy::Summary { .. } => match storage {
            ToolStoragePayload::Summary(_) | ToolStoragePayload::None => storage,
            _ => ToolStoragePayload::None,
        },
        StorageOutputPolicy::MetadataOnly => match storage {
            ToolStoragePayload::Metadata { .. } | ToolStoragePayload::None => storage,
            _ => ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
        },
        StorageOutputPolicy::None => ToolStoragePayload::None,
    }
}

fn timeout_reason_to_string(reason: TurnItemTimeoutReason) -> String {
    match reason {
        TurnItemTimeoutReason::StartDeadlineExceeded => "start_deadline_exceeded".to_owned(),
        TurnItemTimeoutReason::IdleDeadlineExceeded => "idle_deadline_exceeded".to_owned(),
        TurnItemTimeoutReason::HardDeadlineExceeded => "hard_deadline_exceeded".to_owned(),
        TurnItemTimeoutReason::LeaseExpired => "lease_expired".to_owned(),
    }
}

const TURN_ITEM_STATUS_IN_PROGRESS: &str = "in_progress";

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{ToolOutputPolicySnapshot, ToolStoragePayload};
    use serde_json::json;

    fn in_progress_command(id: &str) -> TurnItem {
        TurnItem::CommandExecution {
            id: id.to_owned(),
            tool_name: "exec_command".to_owned(),
            arguments: json!({"cmd":"echo ok"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
            display: ToolDisplayPayload::Progress {
                stage: "running".to_owned(),
                metadata: ToolMetadata::empty(),
            },
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
            recovery: None,
            command: vec!["sh".to_owned(), "-lc".to_owned(), "echo ok".to_owned()],
            cwd: None,
            success: None,
            outcome: None,
            observation: None,
        }
    }

    fn in_progress_file_change(id: &str) -> TurnItem {
        TurnItem::FileChange {
            id: id.to_owned(),
            tool_name: "apply_patch".to_owned(),
            arguments: json!({"patch":"*** Begin Patch\n*** End Patch\n"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("apply_patch"),
            display: ToolDisplayPayload::Progress {
                stage: "running".to_owned(),
                metadata: ToolMetadata::empty(),
            },
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
            recovery: None,
            changed_files: Vec::new(),
            exit_code: None,
            stdout: None,
            stderr: None,
            success: None,
            outcome: None,
            observation: None,
        }
    }

    fn in_progress_web_search(id: &str) -> TurnItem {
        TurnItem::WebSearch {
            id: id.to_owned(),
            tool_name: "web_search".to_owned(),
            arguments: json!({"q":"test"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_search"),
            display: ToolDisplayPayload::Progress {
                stage: "running".to_owned(),
                metadata: ToolMetadata::empty(),
            },
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
            recovery: None,
            query: None,
            provider: None,
            took_ms: None,
            result_count: None,
            results: Vec::new(),
            success: None,
            outcome: None,
            observation: None,
        }
    }

    fn in_progress_web_fetch(id: &str) -> TurnItem {
        TurnItem::WebFetch {
            id: id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: json!({"url":"https://example.com"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Progress {
                stage: "running".to_owned(),
                metadata: ToolMetadata::empty(),
            },
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
            recovery: None,
            url: None,
            final_url: None,
            status_code: None,
            content_type: None,
            extract_mode: None,
            resolved_mode: None,
            bytes_received: None,
            elapsed_ms: None,
            truncated: None,
            title: None,
            word_count: None,
            links: Vec::new(),
            success: None,
            outcome: None,
            observation: None,
        }
    }

    fn in_progress_download(id: &str) -> TurnItem {
        TurnItem::Download {
            id: id.to_owned(),
            tool_name: "download".to_owned(),
            arguments: json!({"url":"https://example.com/file"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("download"),
            display: ToolDisplayPayload::Progress {
                stage: "running".to_owned(),
                metadata: ToolMetadata::empty(),
            },
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
            recovery: None,
            url: None,
            final_url: None,
            status_code: None,
            path: None,
            bytes_written: None,
            sha256: None,
            content_type: None,
            elapsed_ms: None,
            truncated: None,
            success: None,
            outcome: None,
            observation: None,
        }
    }

    fn in_progress_dynamic(id: &str) -> TurnItem {
        TurnItem::DynamicToolCall {
            id: id.to_owned(),
            tool_name: "grep_files".to_owned(),
            arguments: json!({"pattern":"x"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("grep_files"),
            display: ToolDisplayPayload::Progress {
                stage: "running".to_owned(),
                metadata: ToolMetadata::empty(),
            },
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
            recovery: None,
            success: None,
            outcome: None,
            observation: None,
        }
    }

    #[test]
    fn terminalization_converts_all_tool_variants_from_progress() {
        let mut items = vec![
            in_progress_command("item_command"),
            in_progress_file_change("item_file_change"),
            in_progress_web_search("item_web_search"),
            in_progress_web_fetch("item_web_fetch"),
            in_progress_download("item_download"),
            in_progress_dynamic("item_dynamic"),
        ];

        for item in &mut items {
            terminalize_turn_item_payload(
                item,
                TurnItemTerminalState::TimedOut {
                    reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
                },
            );
            assert_eq!(tool_call_status(item), Some(ToolCallStatus::Failed));

            match item {
                TurnItem::CommandExecution {
                    display,
                    success,
                    outcome,
                    ..
                }
                | TurnItem::FileChange {
                    display,
                    success,
                    outcome,
                    ..
                }
                | TurnItem::WebSearch {
                    display,
                    success,
                    outcome,
                    ..
                }
                | TurnItem::WebFetch {
                    display,
                    success,
                    outcome,
                    ..
                }
                | TurnItem::Download {
                    display,
                    success,
                    outcome,
                    ..
                }
                | TurnItem::DynamicToolCall {
                    display,
                    success,
                    outcome,
                    ..
                } => {
                    assert!(!matches!(display, ToolDisplayPayload::Progress { .. }));
                    assert_eq!(*success, Some(false));
                    assert!(outcome.is_some());
                }
                _ => panic!("expected tool item variant"),
            }
        }
    }

    #[test]
    fn terminalization_converts_dynamic_tool_from_progress() {
        let mut item = in_progress_dynamic("item_dynamic");
        terminalize_turn_item_payload(
            &mut item,
            TurnItemTerminalState::TimedOut {
                reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
            },
        );

        let TurnItem::DynamicToolCall {
            status,
            display,
            success,
            outcome,
            ..
        } = item
        else {
            panic!("expected dynamic tool call");
        };

        assert_eq!(status, ToolCallStatus::Failed);
        assert!(!matches!(display, ToolDisplayPayload::Progress { .. }));
        assert_eq!(success, Some(false));
        assert!(outcome.is_some());
    }
}
