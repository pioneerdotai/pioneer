use pioneer_protocol::{
    ProviderFailureClass, ProviderFailureStage, RecoveryAction, RecoveryJobStatus, RecoveryTrigger,
};

pub const RECOVERY_STATUS_PENDING: &str = "pending";
pub const RECOVERY_STATUS_ACTIVE: &str = "active";
pub const RECOVERY_STATUS_SUCCEEDED: &str = "succeeded";
pub const RECOVERY_STATUS_FAILED: &str = "failed";
pub const RECOVERY_STATUS_EXHAUSTED: &str = "exhausted";
pub const RECOVERY_STATUS_BLOCKED: &str = "blocked";
pub const RECOVERY_STATUS_CANCELLED: &str = "cancelled";

pub fn recovery_job_status_to_db(status: RecoveryJobStatus) -> &'static str {
    match status {
        RecoveryJobStatus::Pending => RECOVERY_STATUS_PENDING,
        RecoveryJobStatus::Active => RECOVERY_STATUS_ACTIVE,
        RecoveryJobStatus::Succeeded => RECOVERY_STATUS_SUCCEEDED,
        RecoveryJobStatus::Failed => RECOVERY_STATUS_FAILED,
        RecoveryJobStatus::Exhausted => RECOVERY_STATUS_EXHAUSTED,
        RecoveryJobStatus::Blocked => RECOVERY_STATUS_BLOCKED,
        RecoveryJobStatus::Cancelled => RECOVERY_STATUS_CANCELLED,
    }
}

pub fn recovery_job_status_from_db(value: &str) -> Option<RecoveryJobStatus> {
    match value {
        RECOVERY_STATUS_PENDING => Some(RecoveryJobStatus::Pending),
        RECOVERY_STATUS_ACTIVE => Some(RecoveryJobStatus::Active),
        RECOVERY_STATUS_SUCCEEDED => Some(RecoveryJobStatus::Succeeded),
        RECOVERY_STATUS_FAILED => Some(RecoveryJobStatus::Failed),
        RECOVERY_STATUS_EXHAUSTED => Some(RecoveryJobStatus::Exhausted),
        RECOVERY_STATUS_BLOCKED => Some(RecoveryJobStatus::Blocked),
        RECOVERY_STATUS_CANCELLED => Some(RecoveryJobStatus::Cancelled),
        _ => None,
    }
}

pub fn recovery_trigger_to_db(trigger: RecoveryTrigger) -> &'static str {
    match trigger {
        RecoveryTrigger::Timeout => "timeout",
        RecoveryTrigger::ProviderError => "provider_error",
        RecoveryTrigger::TurnStart => "turn_start",
        RecoveryTrigger::TurnDispatch => "turn_dispatch",
        RecoveryTrigger::ProjectionFailure => "projection_failure",
        RecoveryTrigger::ExecutionWindowContinuation => "execution_window_continuation",
        RecoveryTrigger::ArtifactFinalization => "artifact_finalization",
        RecoveryTrigger::TaskDispatch => "task_dispatch",
        RecoveryTrigger::RuntimeFailure => "runtime_failure",
        RecoveryTrigger::Unknown => "unknown",
    }
}

pub fn recovery_trigger_from_db(value: &str) -> Option<RecoveryTrigger> {
    match value {
        "timeout" => Some(RecoveryTrigger::Timeout),
        "provider_error" => Some(RecoveryTrigger::ProviderError),
        "turn_start" => Some(RecoveryTrigger::TurnStart),
        "turn_dispatch" => Some(RecoveryTrigger::TurnDispatch),
        "projection_failure" => Some(RecoveryTrigger::ProjectionFailure),
        "execution_window_continuation" => Some(RecoveryTrigger::ExecutionWindowContinuation),
        "artifact_finalization" => Some(RecoveryTrigger::ArtifactFinalization),
        "task_dispatch" => Some(RecoveryTrigger::TaskDispatch),
        "runtime_failure" => Some(RecoveryTrigger::RuntimeFailure),
        "unknown" => Some(RecoveryTrigger::Unknown),
        _ => None,
    }
}

pub fn recovery_action_to_db(action: RecoveryAction) -> &'static str {
    match action {
        RecoveryAction::RetryAttempt => "retry_attempt",
        RecoveryAction::RetryWithBackoff => "retry_with_backoff",
        RecoveryAction::RestartTurn => "restart_turn",
        RecoveryAction::ReplayDurableEvent => "replay_durable_event",
        RecoveryAction::RehydrateTurnState => "rehydrate_turn_state",
        RecoveryAction::OpenNextExecutionWindow => "open_next_execution_window",
        RecoveryAction::AdaptProviderRequest => "adapt_provider_request",
        RecoveryAction::RefreshProviderAuth => "refresh_provider_auth",
        RecoveryAction::CompactHistory => "compact_history",
        RecoveryAction::DisableStreaming => "disable_streaming",
        RecoveryAction::DisableUnsupportedCapability => "disable_unsupported_capability",
        RecoveryAction::RepairArtifactFinalization => "repair_artifact_finalization",
        RecoveryAction::RequeueTaskDispatch => "requeue_task_dispatch",
        RecoveryAction::BlockResumable => "block_resumable",
        RecoveryAction::Fallback => "fallback",
        RecoveryAction::MarkFailed => "mark_failed",
    }
}

pub fn recovery_action_from_db(value: &str) -> Option<RecoveryAction> {
    match value {
        "retry_attempt" => Some(RecoveryAction::RetryAttempt),
        "retry_with_backoff" => Some(RecoveryAction::RetryWithBackoff),
        "restart_turn" => Some(RecoveryAction::RestartTurn),
        "replay_durable_event" => Some(RecoveryAction::ReplayDurableEvent),
        "rehydrate_turn_state" => Some(RecoveryAction::RehydrateTurnState),
        "open_next_execution_window" => Some(RecoveryAction::OpenNextExecutionWindow),
        "adapt_provider_request" => Some(RecoveryAction::AdaptProviderRequest),
        "refresh_provider_auth" => Some(RecoveryAction::RefreshProviderAuth),
        "compact_history" => Some(RecoveryAction::CompactHistory),
        "disable_streaming" => Some(RecoveryAction::DisableStreaming),
        "disable_unsupported_capability" => Some(RecoveryAction::DisableUnsupportedCapability),
        "repair_artifact_finalization" => Some(RecoveryAction::RepairArtifactFinalization),
        "requeue_task_dispatch" => Some(RecoveryAction::RequeueTaskDispatch),
        "block_resumable" => Some(RecoveryAction::BlockResumable),
        "fallback" => Some(RecoveryAction::Fallback),
        "mark_failed" => Some(RecoveryAction::MarkFailed),
        _ => None,
    }
}

pub fn provider_failure_class_to_db(class: ProviderFailureClass) -> &'static str {
    match class {
        ProviderFailureClass::NetworkTransient => "network_transient",
        ProviderFailureClass::RateLimit => "rate_limit",
        ProviderFailureClass::Provider5xx => "provider_5xx",
        ProviderFailureClass::AuthExpired => "auth_expired",
        ProviderFailureClass::AuthOrPermission => "auth_or_permission",
        ProviderFailureClass::ModelNotFound => "model_not_found",
        ProviderFailureClass::PromptTooLong => "prompt_too_long",
        ProviderFailureClass::ContextTooLarge => "context_too_large",
        ProviderFailureClass::MaxOutputTokens => "max_output_tokens",
        ProviderFailureClass::StreamStall => "stream_stall",
        ProviderFailureClass::StreamTruncated => "stream_truncated",
        ProviderFailureClass::EmptyResponse => "empty_response",
        ProviderFailureClass::ProviderRejected => "provider_rejected",
        ProviderFailureClass::UnsupportedParameter => "unsupported_parameter",
        ProviderFailureClass::UnsupportedCapability => "unsupported_capability",
        ProviderFailureClass::UnsupportedImageInput => "unsupported_image_input",
        ProviderFailureClass::UnsupportedToolCalling => "unsupported_tool_calling",
        ProviderFailureClass::UnsupportedStreaming => "unsupported_streaming",
        ProviderFailureClass::MalformedProviderRequest => "malformed_provider_request",
        ProviderFailureClass::InvalidRequest => "invalid_request",
        ProviderFailureClass::PermissionDenied => "permission_denied",
        ProviderFailureClass::Unknown => "unknown",
    }
}

pub fn provider_failure_stage_to_db(stage: ProviderFailureStage) -> &'static str {
    match stage {
        ProviderFailureStage::Connect => "connect",
        ProviderFailureStage::FirstChunk => "first_chunk",
        ProviderFailureStage::MidStream => "mid_stream",
        ProviderFailureStage::Finalize => "finalize",
    }
}

pub fn provider_failure_class_from_db(value: &str) -> Option<ProviderFailureClass> {
    match value {
        "network_transient" => Some(ProviderFailureClass::NetworkTransient),
        "rate_limit" => Some(ProviderFailureClass::RateLimit),
        "provider_5xx" => Some(ProviderFailureClass::Provider5xx),
        "auth_expired" => Some(ProviderFailureClass::AuthExpired),
        "auth_or_permission" => Some(ProviderFailureClass::AuthOrPermission),
        "model_not_found" => Some(ProviderFailureClass::ModelNotFound),
        "prompt_too_long" => Some(ProviderFailureClass::PromptTooLong),
        "context_too_large" => Some(ProviderFailureClass::ContextTooLarge),
        "max_output_tokens" => Some(ProviderFailureClass::MaxOutputTokens),
        "stream_stall" => Some(ProviderFailureClass::StreamStall),
        "stream_truncated" => Some(ProviderFailureClass::StreamTruncated),
        "empty_response" => Some(ProviderFailureClass::EmptyResponse),
        "provider_rejected" => Some(ProviderFailureClass::ProviderRejected),
        "unsupported_parameter" => Some(ProviderFailureClass::UnsupportedParameter),
        "unsupported_capability" => Some(ProviderFailureClass::UnsupportedCapability),
        "unsupported_image_input" => Some(ProviderFailureClass::UnsupportedImageInput),
        "unsupported_tool_calling" => Some(ProviderFailureClass::UnsupportedToolCalling),
        "unsupported_streaming" => Some(ProviderFailureClass::UnsupportedStreaming),
        "malformed_provider_request" => Some(ProviderFailureClass::MalformedProviderRequest),
        "invalid_request" => Some(ProviderFailureClass::InvalidRequest),
        "permission_denied" => Some(ProviderFailureClass::PermissionDenied),
        "unknown" => Some(ProviderFailureClass::Unknown),
        _ => None,
    }
}

pub fn provider_failure_stage_from_db(value: &str) -> Option<ProviderFailureStage> {
    match value {
        "connect" => Some(ProviderFailureStage::Connect),
        "first_chunk" => Some(ProviderFailureStage::FirstChunk),
        "mid_stream" => Some(ProviderFailureStage::MidStream),
        "finalize" => Some(ProviderFailureStage::Finalize),
        _ => None,
    }
}
