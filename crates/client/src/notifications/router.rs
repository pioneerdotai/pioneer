//! Gateway notification routing.

use crate::{
    cli_runtime::approvals::{
        PendingRequestsReduction, reduce_pending_request_terminal_turn_cleanup,
        reduce_pending_request_thread_closed_cleanup,
    },
    conversation::ConversationEvent,
};
use pioneer_protocol::{
    ArtifactCreatedNotification, ArtifactDeletedNotification, ArtifactSummary,
    ArtifactUpdatedNotification, CLIRuntimeAccountUpdatedNotification,
    CLIRuntimeAppsChangedNotification, CLIRuntimeRequestOpenedNotification,
    CLIRuntimeRequestResolvedNotification, CLIRuntimeStatusChangedNotification,
    ItemCompletedNotification, ItemDeltaNotification, ItemRecoveryAttachedNotification,
    ItemRecoveryExhaustedNotification, ItemRecoveryOpenedNotification,
    ItemRecoverySucceededNotification, ItemRetryAttemptStartedNotification,
    ItemRetryScheduledNotification, ItemStartedNotification, ItemTimeoutDetectedNotification,
    ItemToolRetryExhaustedNotification, ItemToolRetryResolvedNotification,
    ItemToolRetryScheduledNotification, ItemUpdatedNotification, SkillsChangedNotification, Thread,
    ThreadAgentsDocChangedNotification, ThreadArtifactsChangedNotification,
    ThreadClosedNotification, ThreadStartedNotification, ThreadStatus,
    ThreadTreeChangedNotification, ThreadUpdatedNotification, TurnBlockedNotification,
    TurnCompletedNotification, TurnExecutionWindowBlockedNotification,
    TurnExecutionWindowCheckpointedNotification, TurnExecutionWindowContinuedNotification,
    TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification,
    TurnFailedNotification, TurnStartedNotification, TurnToolLoopBudgetExceededNotification,
};

pub use crate::mcp::notifications::{
    McpRefreshReduction, McpServerCatalogChangedReduction, McpServerStatusChangedReduction,
    apply_mcp_server_catalog_changed_to_catalog, apply_mcp_server_status_changed_to_catalog,
    apply_mcp_server_status_changed_to_details, reduce_mcp_changed_notification,
    reduce_mcp_server_catalog_changed_notification, reduce_mcp_server_status_changed_notification,
};
pub use crate::workspaces::actions::{
    WorkspacePreferenceReduction, apply_workspace_changed_to_catalog,
    reduce_workspace_preference_after_catalog_change,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadStartedContext<'a> {
    pub pending_thread_id: Option<&'a str>,
    pub active_thread_id: Option<&'a str>,
    pub active_workspace_id: Option<&'a str>,
}

#[derive(Clone, Debug)]
pub struct ThreadStartedReduction {
    pub thread: Thread,
    pub thread_id: String,
    pub workspace_id: String,
    pub started_local_pending: bool,
    pub set_draft_thread_id: Option<String>,
    pub set_active_thread_id: Option<String>,
    pub set_preferred_workspace_id: Option<String>,
    pub persist_active_gateway_workspace_id: Option<String>,
    pub reset_thread_start: bool,
    pub clear_thread_start_queue: bool,
    pub queue_thread_list_refresh: bool,
    pub sync_composer_model_selection: bool,
}

#[derive(Clone, Debug)]
pub struct TurnLifecycleReduction {
    pub thread_id: String,
    pub workspace_id: String,
    pub turn_id: String,
    pub promote_thread_from_draft: bool,
    pub queue_thread_list_refresh: bool,
    pub thread_status: Option<ThreadStatus>,
    pub conversation_event: ConversationEvent,
    pub tick_conversation: bool,
    pub reset_thread_resume: bool,
    pub refresh_thread_artifacts: bool,
    pub sync_composer_model_selection: bool,
    pub pending_requests: Option<PendingRequestsReduction>,
}

#[derive(Clone, Debug)]
pub struct ConversationEventReduction {
    pub thread_id: String,
    pub workspace_id: String,
    pub conversation_event: ConversationEvent,
}

#[derive(Clone, Debug)]
pub struct ThreadClosedReduction {
    pub thread_id: String,
    pub workspace_id: String,
    pub matches_thread_workspace: bool,
    pub remove_thread_conversation: bool,
    pub clear_active_thread_if_matches: bool,
    pub queue_thread_list_refresh: bool,
    pub pending_requests: Option<PendingRequestsReduction>,
}

#[derive(Clone, Debug)]
pub struct ThreadUpdatedReduction {
    pub thread: Thread,
    pub thread_id: String,
    pub workspace_id: String,
    pub sync_composer_model_selection: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceRefreshReduction {
    pub workspace_id: String,
    pub queue_thread_list_refresh: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillsRefreshReduction {
    pub workspace_id: String,
    pub queue_skills_refresh: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CLIRuntimeRefreshReduction {
    pub workspace_id: String,
    pub runtime_id: Option<String>,
    pub workspace_matches: bool,
    pub queue_runtime_refresh: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadArtifactsRefreshReduction {
    pub workspace_id: String,
    pub thread_id: String,
    pub matches_thread_workspace: bool,
    pub refresh_thread_artifacts: bool,
    pub force_refresh: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactThreadRefreshReduction {
    pub workspace_id: String,
    pub thread_id: Option<String>,
    pub refresh_thread_artifacts: bool,
    pub force_refresh: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactDeletedRefreshReduction {
    pub workspace_id: String,
    pub artifact_id: String,
    pub active_thread_id: Option<String>,
    pub refresh_thread_artifacts: bool,
    pub force_refresh: bool,
}

pub fn reduce_thread_started_notification(
    notification: ThreadStartedNotification,
    context: ThreadStartedContext<'_>,
) -> ThreadStartedReduction {
    let thread = notification.thread;
    let thread_id = thread.id.clone();
    let workspace_id = thread.workspace_id.clone();
    let started_local_pending = should_accept_thread_started_as_local_pending(
        context.pending_thread_id,
        thread_id.as_str(),
    );
    let queue_thread_list_refresh = started_local_pending
        || should_refresh_workspace_bound_data(context.active_workspace_id, workspace_id.as_str());

    ThreadStartedReduction {
        thread,
        thread_id: thread_id.clone(),
        workspace_id: workspace_id.clone(),
        started_local_pending,
        set_draft_thread_id: started_local_pending.then(|| thread_id.clone()),
        set_active_thread_id: (started_local_pending && context.active_thread_id.is_none())
            .then(|| thread_id.clone()),
        set_preferred_workspace_id: started_local_pending.then(|| workspace_id.clone()),
        persist_active_gateway_workspace_id: started_local_pending.then(|| workspace_id.clone()),
        reset_thread_start: started_local_pending,
        clear_thread_start_queue: started_local_pending,
        queue_thread_list_refresh,
        sync_composer_model_selection: true,
    }
}

pub fn reduce_turn_started_notification(
    notification: TurnStartedNotification,
) -> TurnLifecycleReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();
    let turn_id = notification.turn.id.clone();

    TurnLifecycleReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        turn_id,
        promote_thread_from_draft: true,
        queue_thread_list_refresh: true,
        thread_status: Some(ThreadStatus::Active),
        conversation_event: ConversationEvent::TurnStarted {
            thread_id,
            turn: notification.turn,
        },
        tick_conversation: false,
        reset_thread_resume: true,
        refresh_thread_artifacts: false,
        sync_composer_model_selection: true,
        pending_requests: None,
    }
}

pub fn reduce_turn_completed_notification(
    notification: TurnCompletedNotification,
) -> TurnLifecycleReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();
    let turn_id = notification.turn.id.clone();

    TurnLifecycleReduction {
        thread_id: thread_id.clone(),
        workspace_id: workspace_id.clone(),
        turn_id: turn_id.clone(),
        promote_thread_from_draft: false,
        queue_thread_list_refresh: false,
        thread_status: Some(ThreadStatus::Idle),
        conversation_event: ConversationEvent::TurnCompleted {
            thread_id: thread_id.clone(),
            turn: notification.turn,
        },
        tick_conversation: true,
        reset_thread_resume: true,
        refresh_thread_artifacts: true,
        sync_composer_model_selection: false,
        pending_requests: Some(reduce_pending_request_terminal_turn_cleanup(
            workspace_id,
            thread_id,
            turn_id,
        )),
    }
}

pub fn reduce_turn_failed_notification(
    notification: TurnFailedNotification,
) -> TurnLifecycleReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();
    let turn_id = notification.turn.id.clone();

    TurnLifecycleReduction {
        thread_id: thread_id.clone(),
        workspace_id: workspace_id.clone(),
        turn_id: turn_id.clone(),
        promote_thread_from_draft: false,
        queue_thread_list_refresh: false,
        thread_status: Some(ThreadStatus::Idle),
        conversation_event: ConversationEvent::TurnFailed {
            thread_id: thread_id.clone(),
            turn: notification.turn,
        },
        tick_conversation: false,
        reset_thread_resume: true,
        refresh_thread_artifacts: false,
        sync_composer_model_selection: false,
        pending_requests: Some(reduce_pending_request_terminal_turn_cleanup(
            workspace_id,
            thread_id,
            turn_id,
        )),
    }
}

pub fn reduce_turn_blocked_notification(
    notification: TurnBlockedNotification,
) -> TurnLifecycleReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();
    let turn_id = notification.turn.id.clone();

    TurnLifecycleReduction {
        thread_id: thread_id.clone(),
        workspace_id: workspace_id.clone(),
        turn_id: turn_id.clone(),
        promote_thread_from_draft: false,
        queue_thread_list_refresh: false,
        thread_status: Some(ThreadStatus::Idle),
        conversation_event: ConversationEvent::TurnBlocked {
            thread_id: thread_id.clone(),
            turn: notification.turn,
            resume: notification.resume,
        },
        tick_conversation: false,
        reset_thread_resume: true,
        refresh_thread_artifacts: false,
        sync_composer_model_selection: false,
        pending_requests: Some(reduce_pending_request_terminal_turn_cleanup(
            workspace_id,
            thread_id,
            turn_id,
        )),
    }
}

pub fn reduce_item_started_notification(
    notification: ItemStartedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemStarted {
            thread_id,
            turn_id: notification.turn_id,
            item: notification.item,
        },
    }
}

pub fn reduce_item_delta_notification(
    notification: ItemDeltaNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemDelta {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            delta: notification.delta,
            stream: notification.stream,
            payload: notification.payload,
            markdown: notification.markdown,
            markdown_version: notification.markdown_version,
        },
    }
}

pub fn reduce_item_completed_notification(
    notification: ItemCompletedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemCompleted {
            thread_id,
            turn_id: notification.turn_id,
            item: notification.item,
        },
    }
}

pub fn reduce_item_updated_notification(
    notification: ItemUpdatedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemUpdated {
            thread_id,
            turn_id: notification.turn_id,
            item: notification.item,
        },
    }
}

pub fn reduce_item_timeout_detected_notification(
    notification: ItemTimeoutDetectedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemTimeoutDetected {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            item_type: notification.item_type,
            attempt_number: notification.attempt_number,
            reason: notification.reason,
            recovery_job_id: notification.recovery_job_id,
        },
    }
}

pub fn reduce_item_recovery_opened_notification(
    notification: ItemRecoveryOpenedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemRecoveryOpened {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            item_type: notification.item_type,
            recovery_job_id: notification.recovery_job_id,
            attempt_number: notification.attempt_number,
        },
    }
}

pub fn reduce_item_recovery_attached_notification(
    notification: ItemRecoveryAttachedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemRecoveryAttached {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            item_type: notification.item_type,
            recovery_job_id: notification.recovery_job_id,
            recovery_item_id: notification.recovery_item_id,
            recovery_item_type: notification.recovery_item_type,
            existing_status: notification.existing_status,
            next_attempt_number: notification.next_attempt_number,
        },
    }
}

pub fn reduce_item_retry_scheduled_notification(
    notification: ItemRetryScheduledNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemRetryScheduled {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            item_type: notification.item_type,
            recovery_job_id: notification.recovery_job_id,
            attempt_number: notification.attempt_number,
            next_run_at_unix: notification.next_run_at_unix,
            reason: notification.reason,
        },
    }
}

pub fn reduce_item_retry_attempt_started_notification(
    notification: ItemRetryAttemptStartedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemRetryAttemptStarted {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            item_type: notification.item_type,
            recovery_job_id: notification.recovery_job_id,
            attempt_number: notification.attempt_number,
        },
    }
}

pub fn reduce_item_recovery_succeeded_notification(
    notification: ItemRecoverySucceededNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemRecoverySucceeded {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            item_type: notification.item_type,
            recovery_job_id: notification.recovery_job_id,
            attempt_number: notification.attempt_number,
        },
    }
}

pub fn reduce_item_recovery_exhausted_notification(
    notification: ItemRecoveryExhaustedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemRecoveryExhausted {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            item_type: notification.item_type,
            recovery_job_id: notification.recovery_job_id,
            attempt_number: notification.attempt_number,
            status: notification.status,
            error_message: notification.error_message,
        },
    }
}

pub fn reduce_item_tool_retry_scheduled_notification(
    notification: ItemToolRetryScheduledNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemToolRetryScheduled {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            item_type: notification.item_type,
            tool_retry_episode_id: notification.tool_retry_episode_id,
            tool_name: notification.tool_name,
            attempt_number: notification.attempt_number,
            error_class: notification.error_class,
            retry_hint: notification.retry_hint,
            budgets: notification.budgets,
            failure_signature_fingerprint: notification.failure_signature_fingerprint,
            reason: notification.reason,
        },
    }
}

pub fn reduce_item_tool_retry_resolved_notification(
    notification: ItemToolRetryResolvedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemToolRetryResolved {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            item_type: notification.item_type,
            tool_retry_episode_id: notification.tool_retry_episode_id,
            tool_name: notification.tool_name,
            attempt_number: notification.attempt_number,
            resolution: notification.resolution,
            budgets: notification.budgets,
            reason: notification.reason,
        },
    }
}

pub fn reduce_item_tool_retry_exhausted_notification(
    notification: ItemToolRetryExhaustedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::ItemToolRetryExhausted {
            thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            item_type: notification.item_type,
            tool_retry_episode_id: notification.tool_retry_episode_id,
            tool_name: notification.tool_name,
            attempt_number: notification.attempt_number,
            error_class: notification.error_class,
            exhaustion_kind: notification.exhaustion_kind,
            budgets: notification.budgets,
            failure_signature_fingerprint: notification.failure_signature_fingerprint,
            reason: notification.reason,
        },
    }
}

pub fn reduce_turn_tool_loop_budget_exceeded_notification(
    notification: TurnToolLoopBudgetExceededNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id: thread_id.clone(),
        workspace_id,
        conversation_event: ConversationEvent::TurnToolLoopBudgetExceeded {
            thread_id,
            turn_id: notification.turn_id,
            limit_kind: notification.limit_kind,
            limit: notification.limit,
            observed: notification.observed,
            action: notification.action,
            reason: notification.reason,
        },
    }
}

pub fn reduce_turn_execution_window_started_notification(
    notification: TurnExecutionWindowStartedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id,
        workspace_id,
        conversation_event: ConversationEvent::TurnExecutionWindowStarted { notification },
    }
}

pub fn reduce_turn_execution_window_exhausted_notification(
    notification: TurnExecutionWindowExhaustedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id,
        workspace_id,
        conversation_event: ConversationEvent::TurnExecutionWindowExhausted { notification },
    }
}

pub fn reduce_turn_execution_window_checkpointed_notification(
    notification: TurnExecutionWindowCheckpointedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id,
        workspace_id,
        conversation_event: ConversationEvent::TurnExecutionWindowCheckpointed { notification },
    }
}

pub fn reduce_turn_execution_window_continued_notification(
    notification: TurnExecutionWindowContinuedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id,
        workspace_id,
        conversation_event: ConversationEvent::TurnExecutionWindowContinued { notification },
    }
}

pub fn reduce_turn_execution_window_blocked_notification(
    notification: TurnExecutionWindowBlockedNotification,
) -> ConversationEventReduction {
    let thread_id = notification.thread_id.clone();
    let workspace_id = notification.workspace_id.clone();

    ConversationEventReduction {
        thread_id,
        workspace_id,
        conversation_event: ConversationEvent::TurnExecutionWindowBlocked { notification },
    }
}

pub fn reduce_thread_closed_notification(
    notification: ThreadClosedNotification,
    matches_thread_workspace: bool,
) -> ThreadClosedReduction {
    let workspace_id = notification.workspace_id;
    let thread_id = notification.thread_id;

    ThreadClosedReduction {
        thread_id: thread_id.clone(),
        workspace_id: workspace_id.clone(),
        matches_thread_workspace,
        remove_thread_conversation: matches_thread_workspace,
        clear_active_thread_if_matches: matches_thread_workspace,
        queue_thread_list_refresh: matches_thread_workspace,
        pending_requests: Some(reduce_pending_request_thread_closed_cleanup(
            workspace_id,
            thread_id,
        )),
    }
}

pub fn reduce_thread_tree_changed_notification(
    notification: ThreadTreeChangedNotification,
    active_workspace: Option<&str>,
) -> WorkspaceRefreshReduction {
    let queue_thread_list_refresh =
        should_refresh_workspace_bound_data(active_workspace, notification.workspace_id.as_str());

    WorkspaceRefreshReduction {
        workspace_id: notification.workspace_id,
        queue_thread_list_refresh,
    }
}

pub fn reduce_thread_agents_doc_changed_notification(
    notification: ThreadAgentsDocChangedNotification,
    active_workspace: Option<&str>,
) -> WorkspaceRefreshReduction {
    let queue_thread_list_refresh =
        should_refresh_workspace_bound_data(active_workspace, notification.workspace_id.as_str());

    WorkspaceRefreshReduction {
        workspace_id: notification.workspace_id,
        queue_thread_list_refresh,
    }
}

pub fn reduce_thread_updated_notification(
    notification: ThreadUpdatedNotification,
) -> ThreadUpdatedReduction {
    let thread = notification.thread;
    let thread_id = thread.id.clone();
    let workspace_id = thread.workspace_id.clone();

    ThreadUpdatedReduction {
        thread,
        thread_id,
        workspace_id,
        sync_composer_model_selection: true,
    }
}

pub fn reduce_skills_changed_notification(
    notification: SkillsChangedNotification,
    active_workspace: Option<&str>,
) -> SkillsRefreshReduction {
    let queue_skills_refresh =
        should_refresh_workspace_bound_data(active_workspace, notification.workspace_id.as_str());

    SkillsRefreshReduction {
        workspace_id: notification.workspace_id,
        queue_skills_refresh,
    }
}

pub fn reduce_cli_runtime_status_changed_notification(
    notification: CLIRuntimeStatusChangedNotification,
    active_workspace: Option<&str>,
) -> CLIRuntimeRefreshReduction {
    let runtime_id = notification.runtime.runtime_id;
    reduce_cli_runtime_workspace_notification(
        notification.workspace_id,
        Some(runtime_id),
        active_workspace,
    )
}

pub fn reduce_cli_runtime_account_updated_notification(
    notification: CLIRuntimeAccountUpdatedNotification,
    active_workspace: Option<&str>,
) -> CLIRuntimeRefreshReduction {
    reduce_cli_runtime_workspace_notification(
        notification.workspace_id,
        Some(notification.runtime_id),
        active_workspace,
    )
}

pub fn reduce_cli_runtime_request_opened_notification(
    notification: CLIRuntimeRequestOpenedNotification,
    active_workspace: Option<&str>,
) -> CLIRuntimeRefreshReduction {
    reduce_cli_runtime_workspace_notification(
        notification.workspace_id,
        Some(notification.runtime_id),
        active_workspace,
    )
}

pub fn reduce_cli_runtime_request_resolved_notification(
    notification: CLIRuntimeRequestResolvedNotification,
    active_workspace: Option<&str>,
) -> CLIRuntimeRefreshReduction {
    reduce_cli_runtime_workspace_notification(
        notification.workspace_id,
        Some(notification.runtime_id),
        active_workspace,
    )
}

pub fn reduce_cli_runtime_apps_changed_notification(
    notification: CLIRuntimeAppsChangedNotification,
    active_workspace: Option<&str>,
) -> CLIRuntimeRefreshReduction {
    reduce_cli_runtime_workspace_notification(
        notification.workspace_id,
        Some(notification.runtime_id),
        active_workspace,
    )
}

fn reduce_cli_runtime_workspace_notification(
    workspace_id: String,
    runtime_id: Option<String>,
    active_workspace: Option<&str>,
) -> CLIRuntimeRefreshReduction {
    let workspace_matches =
        should_refresh_workspace_bound_data(active_workspace, workspace_id.as_str());
    CLIRuntimeRefreshReduction {
        workspace_id,
        runtime_id,
        workspace_matches,
        queue_runtime_refresh: workspace_matches,
    }
}

pub fn reduce_thread_artifacts_changed_notification(
    notification: ThreadArtifactsChangedNotification,
    matches_thread_workspace: bool,
) -> ThreadArtifactsRefreshReduction {
    ThreadArtifactsRefreshReduction {
        workspace_id: notification.workspace_id,
        thread_id: notification.thread_id,
        matches_thread_workspace,
        refresh_thread_artifacts: matches_thread_workspace,
        force_refresh: matches_thread_workspace,
    }
}

pub fn reduce_artifact_created_notification(
    notification: ArtifactCreatedNotification,
) -> ArtifactThreadRefreshReduction {
    let thread_id = notification.artifact.primary_thread_id;
    let refresh_thread_artifacts = thread_id.is_some();
    ArtifactThreadRefreshReduction {
        workspace_id: notification.workspace_id,
        thread_id,
        refresh_thread_artifacts,
        force_refresh: refresh_thread_artifacts,
    }
}

pub fn reduce_artifact_updated_notification(
    notification: ArtifactUpdatedNotification,
) -> ArtifactThreadRefreshReduction {
    let thread_id = notification.artifact.primary_thread_id;
    let refresh_thread_artifacts = thread_id.is_some();
    ArtifactThreadRefreshReduction {
        workspace_id: notification.workspace_id,
        thread_id,
        refresh_thread_artifacts,
        force_refresh: refresh_thread_artifacts,
    }
}

pub fn reduce_artifact_deleted_notification(
    notification: ArtifactDeletedNotification,
    active_thread_id: Option<&str>,
    active_thread_artifacts: &[ArtifactSummary],
) -> ArtifactDeletedRefreshReduction {
    let active_thread_contains_artifact = active_thread_artifacts
        .iter()
        .any(|summary| summary.artifact.artifact_id.as_str() == notification.artifact_id.as_str());
    let refresh_thread_artifacts = active_thread_id.is_some() && active_thread_contains_artifact;

    ArtifactDeletedRefreshReduction {
        workspace_id: notification.workspace_id,
        artifact_id: notification.artifact_id,
        active_thread_id: active_thread_id.map(str::to_owned),
        refresh_thread_artifacts,
        force_refresh: refresh_thread_artifacts,
    }
}

pub fn should_refresh_workspace_bound_data(
    active_workspace: Option<&str>,
    notification_workspace: &str,
) -> bool {
    active_workspace == Some(notification_workspace)
}

pub fn should_accept_thread_started_as_local_pending(
    pending_thread_id: Option<&str>,
    started_thread_id: &str,
) -> bool {
    pending_thread_id == Some(started_thread_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ArtifactCreatedByKind, ArtifactCreatedNotification, ArtifactDeletedNotification,
        ArtifactKind, ArtifactRef, ArtifactStatus, ArtifactSummary, ArtifactUpdatedNotification,
        CLIAgentRuntimeKind, CLIRuntimeAccountUpdatedNotification,
        CLIRuntimeAppsChangedNotification, CLIRuntimePendingRequest, CLIRuntimeRequestKind,
        CLIRuntimeRequestOpenedNotification, CLIRuntimeRequestResolution,
        CLIRuntimeRequestResolvedNotification, CLIRuntimeStatusChangedNotification,
        ExecutionWindowStatus, ItemDeltaStream, McpChangedNotification, McpListItem,
        McpPolicyState, McpRuntimeState, McpRuntimeStatus, McpScopeKind,
        McpServerCatalogChangedNotification, McpServerCatalogDetails, McpServerDetailsResponse,
        McpServerHealthDetails, McpServerStatus, McpServerStatusChangedNotification,
        McpServerStatusItem, McpSourceKind, McpTransportSummary, RecoveryAction, RecoveryJobStatus,
        RecoveryTrigger, RuntimeCapabilities, RuntimeStatus, RuntimeSummary,
        SkillsChangedNotification, ThreadArtifactsChangedNotification, ThreadMode,
        ThreadOriginKind, ThreadSidebarVisibility, ToolLoopBudgetAction, ToolLoopBudgetLimitKind,
        ToolRetryErrorClass, ToolRetryResolution, TurnItem, TurnItemTimeoutReason, TurnItemType,
        TurnStatus, Workspace, WorkspaceChangeKind, WorkspaceChangedNotification,
    };
    use std::collections::BTreeMap;

    fn thread(id: &str, workspace_id: &str) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Chat,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: 0,
            updated_at: 0,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        }
    }

    fn workspace(id: &str, is_active: bool, is_current: bool) -> Workspace {
        Workspace {
            id: id.to_owned(),
            name: format!("{id} workspace"),
            is_active,
            is_current,
            created_at: 1,
            updated_at: 2,
        }
    }

    fn mcp_runtime(state: McpRuntimeState, live: bool) -> McpRuntimeStatus {
        McpRuntimeStatus {
            state,
            live,
            last_seen_at: None,
            last_error: None,
        }
    }

    fn mcp_server(id: &str, status: McpServerStatus) -> McpListItem {
        McpListItem {
            id: id.to_owned(),
            name: id.to_owned(),
            display_name: None,
            scope: McpScopeKind::Workspace,
            source_kind: McpSourceKind::Config,
            transport: McpTransportSummary::Stdio {
                command: "server".to_owned(),
            },
            policy: McpPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
            },
            required: false,
            fingerprint: "fingerprint".to_owned(),
            runtime: mcp_runtime(McpRuntimeState::Ready, true),
            tools_count: 1,
            resources_count: 2,
            resource_templates_count: 3,
            prompts_count: 4,
            status,
            status_reason: None,
        }
    }

    fn mcp_details(server: McpListItem) -> McpServerDetailsResponse {
        McpServerDetailsResponse {
            snapshot_version: 1,
            generated_at: 1,
            server,
            catalog: McpServerCatalogDetails {
                catalog_version: None,
                generated_at: None,
                server_info: serde_json::json!({}),
                server_instructions_hash: None,
                tools: Vec::new(),
                resources: Vec::new(),
                resource_templates: Vec::new(),
                prompts: Vec::new(),
            },
            health: McpServerHealthDetails {
                runtime: mcp_runtime(McpRuntimeState::Ready, true),
                status: McpServerStatus::Ready,
                status_reason: None,
                last_error: None,
                retry_attempt: None,
                next_retry_at: None,
                catalog_version: None,
                stderr_tail: None,
            },
            audit: Vec::new(),
            recent_bindings: Vec::new(),
        }
    }

    fn runtime_summary(runtime_id: &str) -> RuntimeSummary {
        RuntimeSummary {
            runtime_id: runtime_id.to_owned(),
            kind: CLIAgentRuntimeKind::Codex,
            display_name: "Codex CLI".to_owned(),
            enabled: true,
            status: RuntimeStatus::Ready,
            capabilities: RuntimeCapabilities::default(),
            account: None,
            version: None,
            binary_path: None,
            home_path: None,
            shadow_home_path: None,
            proxy_url: None,
            debug_native_events_enabled: false,
            models_refreshed_at_unix_ms: None,
            diagnostics: Vec::new(),
            recent_stderr: Vec::new(),
        }
    }

    fn artifact_summary(artifact_id: &str, primary_thread_id: Option<&str>) -> ArtifactSummary {
        ArtifactSummary {
            artifact: ArtifactRef {
                artifact_id: artifact_id.to_owned(),
                version_id: None,
                display_name: format!("{artifact_id}.txt"),
                kind: ArtifactKind::Text,
                mime_type: Some("text/plain".to_owned()),
                size_bytes: Some(10),
                sha256: None,
                status: ArtifactStatus::Ready,
                preview: None,
            },
            workspace_id: "ws_a".to_owned(),
            primary_thread_id: primary_thread_id.map(str::to_owned),
            created_by_kind: ArtifactCreatedByKind::Agent,
            created_by_actor_id: None,
            created_at: 1,
            updated_at: 2,
            bindings: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    fn turn(id: &str, status: TurnStatus) -> pioneer_protocol::Turn {
        pioneer_protocol::Turn {
            id: id.to_owned(),
            status,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        }
    }

    fn agent_item(id: &str, text: &str) -> TurnItem {
        TurnItem::AgentMessage {
            id: id.to_owned(),
            text: text.to_owned(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        }
    }

    #[test]
    fn thread_started_reduction_accepts_local_pending_thread() {
        let reduction = reduce_thread_started_notification(
            ThreadStartedNotification {
                thread: thread("thr_pending", "ws_a"),
            },
            ThreadStartedContext {
                pending_thread_id: Some("thr_pending"),
                active_thread_id: None,
                active_workspace_id: Some("ws_b"),
            },
        );

        assert!(reduction.started_local_pending);
        assert_eq!(
            reduction.set_draft_thread_id.as_deref(),
            Some("thr_pending")
        );
        assert_eq!(
            reduction.set_active_thread_id.as_deref(),
            Some("thr_pending")
        );
        assert_eq!(
            reduction.set_preferred_workspace_id.as_deref(),
            Some("ws_a")
        );
        assert_eq!(
            reduction.persist_active_gateway_workspace_id.as_deref(),
            Some("ws_a")
        );
        assert!(reduction.reset_thread_start);
        assert!(reduction.clear_thread_start_queue);
        assert!(reduction.queue_thread_list_refresh);
        assert!(reduction.sync_composer_model_selection);
    }

    #[test]
    fn thread_started_reduction_refreshes_only_matching_workspace_when_not_pending() {
        let matching = reduce_thread_started_notification(
            ThreadStartedNotification {
                thread: thread("thr_remote", "ws_a"),
            },
            ThreadStartedContext {
                pending_thread_id: Some("thr_other"),
                active_thread_id: Some("thr_active"),
                active_workspace_id: Some("ws_a"),
            },
        );
        assert!(!matching.started_local_pending);
        assert!(matching.set_draft_thread_id.is_none());
        assert!(matching.set_active_thread_id.is_none());
        assert!(matching.queue_thread_list_refresh);

        let foreign = reduce_thread_started_notification(
            ThreadStartedNotification {
                thread: thread("thr_foreign", "ws_b"),
            },
            ThreadStartedContext {
                pending_thread_id: None,
                active_thread_id: Some("thr_active"),
                active_workspace_id: Some("ws_a"),
            },
        );
        assert!(!foreign.queue_thread_list_refresh);
    }

    #[test]
    fn turn_lifecycle_reductions_project_thread_status_and_events() {
        let started = reduce_turn_started_notification(TurnStartedNotification {
            workspace_id: "ws_a".to_owned(),
            thread_id: "thr_a".to_owned(),
            turn: turn("turn_a", TurnStatus::InProgress),
        });
        assert!(started.promote_thread_from_draft);
        assert!(started.queue_thread_list_refresh);
        assert_eq!(started.thread_status, Some(ThreadStatus::Active));
        assert!(started.reset_thread_resume);
        assert!(started.sync_composer_model_selection);
        assert!(matches!(
            started.conversation_event,
            ConversationEvent::TurnStarted { .. }
        ));

        let completed = reduce_turn_completed_notification(TurnCompletedNotification {
            workspace_id: "ws_a".to_owned(),
            thread_id: "thr_a".to_owned(),
            turn: turn("turn_a", TurnStatus::Completed),
        });
        assert_eq!(completed.thread_status, Some(ThreadStatus::Idle));
        assert!(completed.tick_conversation);
        assert!(completed.refresh_thread_artifacts);
        assert!(matches!(
            completed.conversation_event,
            ConversationEvent::TurnCompleted { .. }
        ));

        let failed = reduce_turn_failed_notification(TurnFailedNotification {
            workspace_id: "ws_a".to_owned(),
            thread_id: "thr_a".to_owned(),
            turn: turn("turn_a", TurnStatus::Failed),
        });
        assert_eq!(failed.thread_status, Some(ThreadStatus::Idle));
        assert!(!failed.tick_conversation);
        assert!(!failed.refresh_thread_artifacts);
        assert!(matches!(
            failed.conversation_event,
            ConversationEvent::TurnFailed { .. }
        ));
    }

    #[test]
    fn item_reductions_project_target_and_conversation_events() {
        let started = reduce_item_started_notification(ItemStartedNotification {
            workspace_id: "ws_a".to_owned(),
            thread_id: "thr_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            item: agent_item("item_a", ""),
        });
        assert_eq!(started.thread_id, "thr_a");
        assert_eq!(started.workspace_id, "ws_a");
        assert!(matches!(
            started.conversation_event,
            ConversationEvent::ItemStarted { .. }
        ));

        let delta = reduce_item_delta_notification(ItemDeltaNotification {
            workspace_id: "ws_a".to_owned(),
            thread_id: "thr_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            item_id: "item_a".to_owned(),
            delta: "hello".to_owned(),
            stream: Some(ItemDeltaStream::AgentMessage),
            payload: None,
            markdown: None,
            markdown_version: Some(1),
        });
        match delta.conversation_event {
            ConversationEvent::ItemDelta {
                item_id,
                delta,
                stream,
                markdown_version,
                ..
            } => {
                assert_eq!(item_id, "item_a");
                assert_eq!(delta, "hello");
                assert_eq!(stream, Some(ItemDeltaStream::AgentMessage));
                assert_eq!(markdown_version, Some(1));
            }
            _ => panic!("expected item delta event"),
        }

        let completed = reduce_item_completed_notification(ItemCompletedNotification {
            workspace_id: "ws_a".to_owned(),
            thread_id: "thr_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            item: agent_item("item_a", "done"),
        });
        assert!(matches!(
            completed.conversation_event,
            ConversationEvent::ItemCompleted { .. }
        ));

        let updated = reduce_item_updated_notification(ItemUpdatedNotification {
            workspace_id: "ws_a".to_owned(),
            thread_id: "thr_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            item: agent_item("item_a", "updated"),
        });
        assert!(matches!(
            updated.conversation_event,
            ConversationEvent::ItemUpdated { .. }
        ));
    }

    #[test]
    fn recovery_retry_timeout_reductions_project_conversation_events() {
        let timeout = reduce_item_timeout_detected_notification(ItemTimeoutDetectedNotification {
            workspace_id: "ws_a".to_owned(),
            thread_id: "thr_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            item_id: "item_a".to_owned(),
            item_type: TurnItemType::CommandExecution,
            attempt_number: 2,
            reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
            recovery_job_id: Some("rec_a".to_owned()),
        });
        assert_eq!(timeout.thread_id, "thr_a");
        assert_eq!(timeout.workspace_id, "ws_a");
        assert!(matches!(
            timeout.conversation_event,
            ConversationEvent::ItemTimeoutDetected { .. }
        ));

        let attached =
            reduce_item_recovery_attached_notification(ItemRecoveryAttachedNotification {
                workspace_id: "ws_a".to_owned(),
                thread_id: "thr_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                item_id: "item_a".to_owned(),
                item_type: TurnItemType::CommandExecution,
                recovery_job_id: "rec_a".to_owned(),
                recovery_item_id: "item_retry".to_owned(),
                recovery_item_type: TurnItemType::CommandExecution,
                trigger: RecoveryTrigger::Timeout,
                action: RecoveryAction::RetryAttempt,
                existing_status: RecoveryJobStatus::Active,
                next_attempt_number: 3,
            });
        assert!(matches!(
            attached.conversation_event,
            ConversationEvent::ItemRecoveryAttached { .. }
        ));

        let retry = reduce_item_retry_scheduled_notification(ItemRetryScheduledNotification {
            workspace_id: "ws_a".to_owned(),
            thread_id: "thr_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            item_id: "item_a".to_owned(),
            item_type: TurnItemType::CommandExecution,
            recovery_job_id: "rec_a".to_owned(),
            attempt_number: 3,
            next_run_at_unix: 1_700_000_123,
            reason: Some("backoff".to_owned()),
        });
        assert!(matches!(
            retry.conversation_event,
            ConversationEvent::ItemRetryScheduled { .. }
        ));
    }

    #[test]
    fn tool_retry_loop_and_window_reductions_project_conversation_events() {
        let tool_retry =
            reduce_item_tool_retry_resolved_notification(ItemToolRetryResolvedNotification {
                workspace_id: "ws_a".to_owned(),
                thread_id: "thr_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                item_id: "item_a".to_owned(),
                item_type: TurnItemType::CommandExecution,
                tool_retry_episode_id: "episode_a".to_owned(),
                tool_name: "shell".to_owned(),
                attempt_number: 2,
                resolution: ToolRetryResolution::Succeeded,
                budgets: Vec::new(),
                reason: "ok".to_owned(),
            });
        assert!(matches!(
            tool_retry.conversation_event,
            ConversationEvent::ItemToolRetryResolved { .. }
        ));

        let loop_budget = reduce_turn_tool_loop_budget_exceeded_notification(
            TurnToolLoopBudgetExceededNotification {
                workspace_id: "ws_a".to_owned(),
                thread_id: "thr_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                limit_kind: ToolLoopBudgetLimitKind::ToolCalls,
                limit: 20,
                observed: 21,
                action: ToolLoopBudgetAction::ContinueInNextWindow,
                reason: "too many calls".to_owned(),
            },
        );
        assert!(matches!(
            loop_budget.conversation_event,
            ConversationEvent::TurnToolLoopBudgetExceeded { .. }
        ));

        let window = reduce_turn_execution_window_blocked_notification(
            TurnExecutionWindowBlockedNotification {
                workspace_id: "ws_a".to_owned(),
                thread_id: "thr_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                window_id: "win_a".to_owned(),
                window_index: 1,
                status: ExecutionWindowStatus::Blocked,
                exhaustion_reason: None,
                checkpoint_id: Some("ckpt_a".to_owned()),
                total_windows: 2,
                total_tool_calls: 21,
                reason: "blocked".to_owned(),
                blocked_at_unix_ms: 1_700_000_123_000,
            },
        );
        assert_eq!(window.thread_id, "thr_a");
        assert_eq!(window.workspace_id, "ws_a");
        assert!(matches!(
            window.conversation_event,
            ConversationEvent::TurnExecutionWindowBlocked { .. }
        ));

        let scheduled =
            reduce_item_tool_retry_scheduled_notification(ItemToolRetryScheduledNotification {
                workspace_id: "ws_a".to_owned(),
                thread_id: "thr_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                item_id: "item_a".to_owned(),
                item_type: TurnItemType::CommandExecution,
                tool_retry_episode_id: "episode_a".to_owned(),
                tool_name: "shell".to_owned(),
                attempt_number: 1,
                error_class: ToolRetryErrorClass::Timeout,
                retry_hint: "retry".to_owned(),
                budgets: Vec::new(),
                failure_signature_fingerprint: "fp".to_owned(),
                reason: "timeout".to_owned(),
            });
        assert!(matches!(
            scheduled.conversation_event,
            ConversationEvent::ItemToolRetryScheduled { .. }
        ));
    }

    #[test]
    fn thread_catalog_reductions_preserve_workspace_refresh_decisions() {
        let closed = reduce_thread_closed_notification(
            ThreadClosedNotification {
                workspace_id: "ws_a".to_owned(),
                thread_id: "thr_a".to_owned(),
            },
            true,
        );
        assert!(closed.matches_thread_workspace);
        assert!(closed.remove_thread_conversation);
        assert!(closed.clear_active_thread_if_matches);
        assert!(closed.queue_thread_list_refresh);

        let foreign_closed = reduce_thread_closed_notification(
            ThreadClosedNotification {
                workspace_id: "ws_b".to_owned(),
                thread_id: "thr_b".to_owned(),
            },
            false,
        );
        assert!(!foreign_closed.remove_thread_conversation);
        assert!(!foreign_closed.queue_thread_list_refresh);

        let tree = reduce_thread_tree_changed_notification(
            ThreadTreeChangedNotification {
                workspace_id: "ws_a".to_owned(),
            },
            Some("ws_a"),
        );
        assert!(tree.queue_thread_list_refresh);

        let agents_doc = reduce_thread_agents_doc_changed_notification(
            ThreadAgentsDocChangedNotification {
                workspace_id: "ws_b".to_owned(),
                folder_id: None,
                doc: None,
                effective: None,
                effective_changed: true,
            },
            Some("ws_a"),
        );
        assert!(!agents_doc.queue_thread_list_refresh);
    }

    #[test]
    fn thread_updated_reduction_projects_snapshot_upsert() {
        let reduction = reduce_thread_updated_notification(ThreadUpdatedNotification {
            thread: thread("thr_a", "ws_a"),
        });

        assert_eq!(reduction.thread_id, "thr_a");
        assert_eq!(reduction.workspace_id, "ws_a");
        assert!(reduction.sync_composer_model_selection);
    }

    #[test]
    fn workspace_catalog_mutation_and_preference_fallback_are_shared() {
        let mut catalog = vec![workspace("ws_old", true, true)];
        apply_workspace_changed_to_catalog(
            &mut catalog,
            &WorkspaceChangedNotification {
                kind: WorkspaceChangeKind::CurrentChanged,
                workspace: workspace("ws_new", true, true),
            },
        );

        assert_eq!(catalog.len(), 2);
        assert!(!catalog[0].is_current);
        assert!(catalog[1].is_current);

        let reduction = reduce_workspace_preference_after_catalog_change(
            Some("ws_missing"),
            catalog.as_slice(),
        );
        assert_eq!(
            reduction.set_preferred_workspace_id,
            Some(Some("ws_new".to_owned()))
        );
        assert_eq!(
            reduction.persist_active_gateway_workspace_id.as_deref(),
            Some("ws_new")
        );
        assert!(reduction.queue_thread_list_refresh);

        let unchanged =
            reduce_workspace_preference_after_catalog_change(Some("ws_new"), catalog.as_slice());
        assert_eq!(unchanged.set_preferred_workspace_id, None);
        assert!(!unchanged.queue_thread_list_refresh);
    }

    #[test]
    fn skills_and_mcp_refresh_reductions_match_workspace_scope() {
        let skills = reduce_skills_changed_notification(
            SkillsChangedNotification {
                workspace_id: "ws_a".to_owned(),
                snapshot_version: 1,
                reason: "test".to_owned(),
                changes: Vec::new(),
                pack_changes: Vec::new(),
                created_at: 1,
            },
            Some("ws_a"),
        );
        assert!(skills.queue_skills_refresh);

        let foreign_skills = reduce_skills_changed_notification(
            SkillsChangedNotification {
                workspace_id: "ws_b".to_owned(),
                snapshot_version: 1,
                reason: "test".to_owned(),
                changes: Vec::new(),
                pack_changes: Vec::new(),
                created_at: 1,
            },
            Some("ws_a"),
        );
        assert!(!foreign_skills.queue_skills_refresh);

        let mcp = reduce_mcp_changed_notification(
            McpChangedNotification {
                workspace_id: "ws_a".to_owned(),
                snapshot_version: 1,
                changed: Vec::new(),
            },
            Some("ws_a"),
            Some("server_a"),
        );
        assert!(mcp.workspace_matches);
        assert!(mcp.queue_mcp_refresh);
        assert!(mcp.queue_mcp_details_refresh);
    }

    #[test]
    fn cli_runtime_refresh_reductions_match_workspace_scope() {
        let status = reduce_cli_runtime_status_changed_notification(
            CLIRuntimeStatusChangedNotification {
                workspace_id: "ws_a".to_owned(),
                runtime: runtime_summary("codex_personal"),
            },
            Some("ws_a"),
        );
        assert!(status.workspace_matches);
        assert!(status.queue_runtime_refresh);
        assert_eq!(status.runtime_id.as_deref(), Some("codex_personal"));

        let account = reduce_cli_runtime_account_updated_notification(
            CLIRuntimeAccountUpdatedNotification {
                workspace_id: "ws_a".to_owned(),
                runtime_id: "codex_personal".to_owned(),
                kind: Some(CLIAgentRuntimeKind::Codex),
                account: None,
                status: RuntimeStatus::Ready,
            },
            Some("ws_a"),
        );
        assert!(account.queue_runtime_refresh);

        let opened = reduce_cli_runtime_request_opened_notification(
            CLIRuntimeRequestOpenedNotification {
                workspace_id: "ws_a".to_owned(),
                runtime_id: "codex_personal".to_owned(),
                request_id: "req_1".to_owned(),
                thread_id: Some("thread_1".to_owned()),
                turn_id: Some("turn_1".to_owned()),
                item_id: None,
                request: CLIRuntimePendingRequest {
                    kind: CLIRuntimeRequestKind::CommandApproval,
                    title: Some("Run command".to_owned()),
                    message: None,
                    native_request_id: None,
                    payload: None,
                },
            },
            Some("ws_a"),
        );
        assert!(opened.queue_runtime_refresh);

        let resolved = reduce_cli_runtime_request_resolved_notification(
            CLIRuntimeRequestResolvedNotification {
                workspace_id: "ws_a".to_owned(),
                runtime_id: "codex_personal".to_owned(),
                request_id: "req_1".to_owned(),
                thread_id: Some("thread_1".to_owned()),
                turn_id: Some("turn_1".to_owned()),
                item_id: None,
                resolution: CLIRuntimeRequestResolution::Approved,
            },
            Some("ws_a"),
        );
        assert!(resolved.queue_runtime_refresh);

        let apps = reduce_cli_runtime_apps_changed_notification(
            CLIRuntimeAppsChangedNotification {
                workspace_id: "ws_a".to_owned(),
                runtime_id: "codex_personal".to_owned(),
                apps: Vec::new(),
                refreshed_at_unix_ms: None,
            },
            Some("ws_a"),
        );
        assert!(apps.queue_runtime_refresh);

        let foreign = reduce_cli_runtime_apps_changed_notification(
            CLIRuntimeAppsChangedNotification {
                workspace_id: "ws_b".to_owned(),
                runtime_id: "codex_personal".to_owned(),
                apps: Vec::new(),
                refreshed_at_unix_ms: None,
            },
            Some("ws_a"),
        );
        assert!(!foreign.workspace_matches);
        assert!(!foreign.queue_runtime_refresh);
    }

    #[test]
    fn mcp_status_and_catalog_reductions_patch_local_read_models() {
        let mut servers = vec![mcp_server("server_a", McpServerStatus::Starting)];
        let status_notification = McpServerStatusChangedNotification {
            workspace_id: "ws_a".to_owned(),
            snapshot_version: 2,
            server: McpServerStatusItem {
                id: "server_a".to_owned(),
                name: "server_a".to_owned(),
                scope_kind: McpScopeKind::Workspace,
                runtime: mcp_runtime(McpRuntimeState::Failed, false),
                status: McpServerStatus::Failed,
                status_reason: Some("crashed".to_owned()),
            },
        };

        let status_reduction = reduce_mcp_server_status_changed_notification(
            status_notification.clone(),
            Some("ws_a"),
            Some("server_a"),
            true,
        );
        assert!(status_reduction.workspace_matches);
        assert!(status_reduction.selected_server_matches);
        assert!(status_reduction.update_selected_details);
        assert!(!status_reduction.queue_mcp_details_refresh);

        let mut details = mcp_details(servers[0].clone());
        apply_mcp_server_status_changed_to_catalog(&mut servers, &status_notification);
        apply_mcp_server_status_changed_to_details(&mut details, &status_notification);
        assert_eq!(servers[0].status, McpServerStatus::Failed);
        assert_eq!(details.server.status, McpServerStatus::Failed);
        assert_eq!(details.health.status_reason.as_deref(), Some("crashed"));

        let catalog_notification = McpServerCatalogChangedNotification {
            workspace_id: "ws_a".to_owned(),
            snapshot_version: 3,
            server_id: "server_a".to_owned(),
            name: "server_a".to_owned(),
            catalog_version: "cat_2".to_owned(),
            tools_count: 10,
            resources_count: 11,
            resource_templates_count: 12,
            prompts_count: 13,
        };
        let catalog_reduction = reduce_mcp_server_catalog_changed_notification(
            catalog_notification.clone(),
            Some("ws_a"),
            Some("server_a"),
        );
        assert!(catalog_reduction.queue_mcp_details_refresh);

        apply_mcp_server_catalog_changed_to_catalog(&mut servers, &catalog_notification);
        assert_eq!(servers[0].tools_count, 10);
        assert_eq!(servers[0].resources_count, 11);
        assert_eq!(servers[0].resource_templates_count, 12);
        assert_eq!(servers[0].prompts_count, 13);
    }

    #[test]
    fn artifact_refresh_reductions_preserve_desktop_refresh_decisions() {
        let thread_changed = reduce_thread_artifacts_changed_notification(
            ThreadArtifactsChangedNotification {
                workspace_id: "ws_a".to_owned(),
                thread_id: "thr_a".to_owned(),
                artifact_ids: vec!["artifact_a".to_owned()],
                reason: "test".to_owned(),
                generated_at: 1,
            },
            true,
        );
        assert!(thread_changed.refresh_thread_artifacts);
        assert!(thread_changed.force_refresh);
        assert_eq!(thread_changed.thread_id, "thr_a");

        let foreign_thread_changed = reduce_thread_artifacts_changed_notification(
            ThreadArtifactsChangedNotification {
                workspace_id: "ws_b".to_owned(),
                thread_id: "thr_b".to_owned(),
                artifact_ids: Vec::new(),
                reason: "test".to_owned(),
                generated_at: 1,
            },
            false,
        );
        assert!(!foreign_thread_changed.refresh_thread_artifacts);

        let created = reduce_artifact_created_notification(ArtifactCreatedNotification {
            workspace_id: "ws_a".to_owned(),
            artifact: artifact_summary("artifact_a", Some("thr_a")),
        });
        assert_eq!(created.thread_id.as_deref(), Some("thr_a"));
        assert!(created.refresh_thread_artifacts);
        assert!(created.force_refresh);

        let updated_without_thread =
            reduce_artifact_updated_notification(ArtifactUpdatedNotification {
                workspace_id: "ws_a".to_owned(),
                artifact: artifact_summary("artifact_detached", None),
            });
        assert!(!updated_without_thread.refresh_thread_artifacts);

        let active_artifacts = vec![artifact_summary("artifact_a", Some("thr_a"))];
        let deleted = reduce_artifact_deleted_notification(
            ArtifactDeletedNotification {
                workspace_id: "ws_a".to_owned(),
                artifact_id: "artifact_a".to_owned(),
                status: ArtifactStatus::Deleted,
                deleted_at: 1,
            },
            Some("thr_a"),
            active_artifacts.as_slice(),
        );
        assert_eq!(deleted.active_thread_id.as_deref(), Some("thr_a"));
        assert!(deleted.refresh_thread_artifacts);

        let deleted_missing = reduce_artifact_deleted_notification(
            ArtifactDeletedNotification {
                workspace_id: "ws_a".to_owned(),
                artifact_id: "artifact_other".to_owned(),
                status: ArtifactStatus::Deleted,
                deleted_at: 1,
            },
            Some("thr_a"),
            active_artifacts.as_slice(),
        );
        assert!(!deleted_missing.refresh_thread_artifacts);
    }
}
