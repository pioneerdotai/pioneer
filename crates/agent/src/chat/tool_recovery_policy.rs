use pioneer_protocol::{
    RecoveryAction, ToolRecoveryIdempotencyMode, ToolRecoveryPolicySnapshot,
    ToolRecoveryRetryClass, TurnItemType,
};
use pioneer_tools::{ToolIdempotencyMode, ToolRecoveryMetadata, ToolRetryClass};

#[derive(Debug, Clone, Copy)]
struct ToolItemBasePolicy {
    max_wall_clock_secs: u64,
    no_progress_limit: i64,
}

pub(super) fn snapshot_for_tool_metadata(
    item_type: TurnItemType,
    metadata: ToolRecoveryMetadata,
) -> ToolRecoveryPolicySnapshot {
    let base = base_policy_for_item_type(item_type);
    let retry_class = protocol_retry_class(metadata.retry_class);
    let idempotency_mode = protocol_idempotency_mode(metadata.idempotency_mode);
    let mut max_attempts = metadata.max_attempts;
    if matches!(metadata.idempotency_mode, ToolIdempotencyMode::None) {
        max_attempts = max_attempts.min(1);
    }

    ToolRecoveryPolicySnapshot {
        retry_class,
        idempotency_mode,
        max_attempts,
        can_resume: metadata.can_resume,
        resolved_action: action_for_retry_class(metadata.retry_class),
        base_backoff_secs: base_backoff_secs_for_retry_class(metadata.retry_class),
        max_wall_clock_secs: metadata
            .max_wall_clock_secs
            .unwrap_or(base.max_wall_clock_secs),
        no_progress_limit: base.no_progress_limit,
    }
}

pub(super) fn conservative_no_recovery_snapshot(
    item_type: TurnItemType,
) -> ToolRecoveryPolicySnapshot {
    let base = base_policy_for_item_type(item_type);
    ToolRecoveryPolicySnapshot {
        retry_class: ToolRecoveryRetryClass::Never,
        idempotency_mode: ToolRecoveryIdempotencyMode::None,
        max_attempts: 1,
        can_resume: false,
        resolved_action: RecoveryAction::MarkFailed,
        base_backoff_secs: 1,
        max_wall_clock_secs: base.max_wall_clock_secs,
        no_progress_limit: 1,
    }
}

fn protocol_retry_class(retry_class: ToolRetryClass) -> ToolRecoveryRetryClass {
    match retry_class {
        ToolRetryClass::Never => ToolRecoveryRetryClass::Never,
        ToolRetryClass::Transient => ToolRecoveryRetryClass::Transient,
        ToolRetryClass::Arguments => ToolRecoveryRetryClass::Arguments,
        ToolRetryClass::Session => ToolRecoveryRetryClass::Session,
        ToolRetryClass::Network => ToolRecoveryRetryClass::Network,
    }
}

fn protocol_idempotency_mode(idempotency_mode: ToolIdempotencyMode) -> ToolRecoveryIdempotencyMode {
    match idempotency_mode {
        ToolIdempotencyMode::None => ToolRecoveryIdempotencyMode::None,
        ToolIdempotencyMode::Safe => ToolRecoveryIdempotencyMode::Safe,
        ToolIdempotencyMode::RequiresKey => ToolRecoveryIdempotencyMode::RequiresKey,
        ToolIdempotencyMode::SessionBound => ToolRecoveryIdempotencyMode::SessionBound,
    }
}

fn action_for_retry_class(retry_class: ToolRetryClass) -> RecoveryAction {
    match retry_class {
        ToolRetryClass::Never => RecoveryAction::MarkFailed,
        ToolRetryClass::Transient | ToolRetryClass::Arguments | ToolRetryClass::Session => {
            RecoveryAction::RetryAttempt
        }
        ToolRetryClass::Network => RecoveryAction::RetryWithBackoff,
    }
}

fn base_backoff_secs_for_retry_class(retry_class: ToolRetryClass) -> u64 {
    match retry_class {
        ToolRetryClass::Network => 3,
        ToolRetryClass::Session | ToolRetryClass::Transient => 2,
        ToolRetryClass::Arguments | ToolRetryClass::Never => 1,
    }
}

fn base_policy_for_item_type(item_type: TurnItemType) -> ToolItemBasePolicy {
    match item_type {
        TurnItemType::CommandExecution => ToolItemBasePolicy {
            max_wall_clock_secs: 300,
            no_progress_limit: 3,
        },
        TurnItemType::FileChange => ToolItemBasePolicy {
            max_wall_clock_secs: 180,
            no_progress_limit: 2,
        },
        TurnItemType::WebSearch => ToolItemBasePolicy {
            max_wall_clock_secs: 180,
            no_progress_limit: 3,
        },
        TurnItemType::WebFetch => ToolItemBasePolicy {
            max_wall_clock_secs: 240,
            no_progress_limit: 3,
        },
        TurnItemType::Download => ToolItemBasePolicy {
            max_wall_clock_secs: 600,
            no_progress_limit: 3,
        },
        TurnItemType::DynamicToolCall => ToolItemBasePolicy {
            max_wall_clock_secs: 300,
            no_progress_limit: 2,
        },
        TurnItemType::UserMessage
        | TurnItemType::AgentMessage
        | TurnItemType::Reasoning
        | TurnItemType::SystemEvent
        | TurnItemType::Task => ToolItemBasePolicy {
            max_wall_clock_secs: 10,
            no_progress_limit: 1,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_retry_metadata_maps_to_gateway_timeout_policy_values() {
        let snapshot = snapshot_for_tool_metadata(
            TurnItemType::WebFetch,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Network,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 9,
                can_resume: true,
                max_wall_clock_secs: None,
            },
        );

        assert_eq!(snapshot.retry_class, ToolRecoveryRetryClass::Network);
        assert_eq!(snapshot.idempotency_mode, ToolRecoveryIdempotencyMode::Safe);
        assert_eq!(snapshot.max_attempts, 9);
        assert!(snapshot.can_resume);
        assert_eq!(snapshot.resolved_action, RecoveryAction::RetryWithBackoff);
        assert_eq!(snapshot.base_backoff_secs, 3);
        assert_eq!(snapshot.max_wall_clock_secs, 240);
        assert_eq!(snapshot.no_progress_limit, 3);
    }

    #[test]
    fn non_idempotent_metadata_caps_attempt_budget() {
        let snapshot = snapshot_for_tool_metadata(
            TurnItemType::CommandExecution,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Session,
                idempotency_mode: ToolIdempotencyMode::None,
                max_attempts: 3,
                can_resume: true,
                max_wall_clock_secs: None,
            },
        );

        assert_eq!(snapshot.max_attempts, 1);
        assert_eq!(snapshot.resolved_action, RecoveryAction::RetryAttempt);
        assert_eq!(snapshot.base_backoff_secs, 2);
        assert_eq!(snapshot.max_wall_clock_secs, 300);
        assert_eq!(snapshot.no_progress_limit, 3);
    }

    #[test]
    fn tool_metadata_can_override_wall_clock_budget() {
        let snapshot = snapshot_for_tool_metadata(
            TurnItemType::DynamicToolCall,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Transient,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 2,
                can_resume: true,
                max_wall_clock_secs: Some(3_600),
            },
        );

        assert_eq!(snapshot.max_wall_clock_secs, 3_600);
    }

    #[test]
    fn unresolved_tool_uses_conservative_no_recovery_snapshot() {
        let snapshot = conservative_no_recovery_snapshot(TurnItemType::DynamicToolCall);

        assert_eq!(snapshot.retry_class, ToolRecoveryRetryClass::Never);
        assert_eq!(snapshot.idempotency_mode, ToolRecoveryIdempotencyMode::None);
        assert_eq!(snapshot.max_attempts, 1);
        assert!(!snapshot.can_resume);
        assert_eq!(snapshot.resolved_action, RecoveryAction::MarkFailed);
        assert_eq!(snapshot.base_backoff_secs, 1);
        assert_eq!(snapshot.max_wall_clock_secs, 300);
        assert_eq!(snapshot.no_progress_limit, 1);
    }

    #[test]
    fn file_change_argument_retry_metadata_matches_apply_patch_recovery_policy() {
        let snapshot = snapshot_for_tool_metadata(
            TurnItemType::FileChange,
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::RequiresKey,
                max_attempts: 2,
                can_resume: false,
                max_wall_clock_secs: None,
            },
        );

        assert_eq!(snapshot.retry_class, ToolRecoveryRetryClass::Arguments);
        assert_eq!(
            snapshot.idempotency_mode,
            ToolRecoveryIdempotencyMode::RequiresKey
        );
        assert_eq!(snapshot.max_attempts, 2);
        assert!(!snapshot.can_resume);
        assert_eq!(snapshot.resolved_action, RecoveryAction::RetryAttempt);
        assert_eq!(snapshot.base_backoff_secs, 1);
        assert_eq!(snapshot.max_wall_clock_secs, 180);
        assert_eq!(snapshot.no_progress_limit, 2);
    }
}
