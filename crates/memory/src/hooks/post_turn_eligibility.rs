use super::*;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum MemoryPostTurnEligibilityPolicy {
    Available(MemoryTurnPolicy),
    Missing,
    Malformed,
}

impl MemoryPostTurnEligibilityPolicy {
    pub(super) fn as_available_policy(&self) -> Option<&MemoryTurnPolicy> {
        match self {
            Self::Available(policy) => Some(policy),
            Self::Missing | Self::Malformed => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct MemoryPostTurnEligibilityInput {
    pub(super) config_enabled: bool,
    pub(super) status: TurnPostTurnStatus,
    pub(super) policy: MemoryPostTurnEligibilityPolicy,
    pub(super) has_user_text: bool,
    pub(super) has_assistant_text: bool,
    pub(super) has_tool_events: bool,
    pub(super) has_domain_events: bool,
    pub(super) source_context_kind: MemorySourceContextKind,
    pub(super) task_runtime_owned: bool,
    pub(super) system_runtime_owned: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MemoryPostTurnEligibilitySkipReason {
    ConfigDisabled,
    NonSuccessTurn,
    MissingPolicy,
    MalformedPolicy,
    PolicyDisabledExtraction,
    NoTranscript,
    NoDirectUserSource,
    TaskRuntimeOwnedSource,
    SystemOrToolOnlySource,
}

impl MemoryPostTurnEligibilitySkipReason {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::ConfigDisabled => "config_disabled",
            Self::NonSuccessTurn => "non_success_status",
            Self::MissingPolicy => "missing_policy",
            Self::MalformedPolicy => "malformed_policy",
            Self::PolicyDisabledExtraction => "policy_disabled",
            Self::NoTranscript => "no_transcript",
            Self::NoDirectUserSource => "no_direct_user_source",
            Self::TaskRuntimeOwnedSource => "task_runtime_owned_source",
            Self::SystemOrToolOnlySource => "system_or_tool_only_source",
        }
    }

    pub(super) fn diagnostic_code(self) -> &'static str {
        match self {
            Self::ConfigDisabled => "memory.post_turn_eligibility.config_disabled",
            Self::NonSuccessTurn => "memory.post_turn_eligibility.non_success_status",
            Self::MissingPolicy => "memory.post_turn_eligibility.missing_policy",
            Self::MalformedPolicy => "memory.post_turn_eligibility.malformed_policy",
            Self::PolicyDisabledExtraction => "memory.post_turn_eligibility.policy_disabled",
            Self::NoTranscript => "memory.post_turn_eligibility.no_transcript",
            Self::NoDirectUserSource => "memory.post_turn_eligibility.no_direct_user_source",
            Self::TaskRuntimeOwnedSource => {
                "memory.post_turn_eligibility.task_runtime_owned_source"
            }
            Self::SystemOrToolOnlySource => {
                "memory.post_turn_eligibility.system_or_tool_only_source"
            }
        }
    }

    pub(super) fn message(self) -> &'static str {
        match self {
            Self::ConfigDisabled => "memory post-turn extraction skipped: config disabled",
            Self::NonSuccessTurn => "memory post-turn extraction skipped: turn did not succeed",
            Self::MissingPolicy => "memory post-turn extraction skipped: missing policy",
            Self::MalformedPolicy => "memory post-turn extraction skipped: malformed policy",
            Self::PolicyDisabledExtraction => {
                "memory post-turn extraction skipped: policy disabled extraction"
            }
            Self::NoTranscript => "memory post-turn extraction skipped: no transcript",
            Self::NoDirectUserSource => {
                "memory post-turn extraction skipped: no direct user source"
            }
            Self::TaskRuntimeOwnedSource => {
                "memory post-turn extraction skipped: task/runtime-owned source"
            }
            Self::SystemOrToolOnlySource => {
                "memory post-turn extraction skipped: system/tool-only source"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MemoryPostTurnEligibilityDecision {
    Eligible,
    Skipped(MemoryPostTurnEligibilitySkipReason),
}

impl MemoryPostTurnEligibilityDecision {
    pub(super) fn is_eligible(self) -> bool {
        self == Self::Eligible
    }
}

pub(super) struct MemoryPostTurnEligibilityGate;

impl MemoryPostTurnEligibilityGate {
    pub(super) fn evaluate(
        input: &MemoryPostTurnEligibilityInput,
    ) -> MemoryPostTurnEligibilityDecision {
        if !input.config_enabled {
            return MemoryPostTurnEligibilityDecision::Skipped(
                MemoryPostTurnEligibilitySkipReason::ConfigDisabled,
            );
        }
        if input.status != TurnPostTurnStatus::Succeeded {
            return MemoryPostTurnEligibilityDecision::Skipped(
                MemoryPostTurnEligibilitySkipReason::NonSuccessTurn,
            );
        }
        let policy = match &input.policy {
            MemoryPostTurnEligibilityPolicy::Available(policy) => policy,
            MemoryPostTurnEligibilityPolicy::Missing => {
                return MemoryPostTurnEligibilityDecision::Skipped(
                    MemoryPostTurnEligibilitySkipReason::MissingPolicy,
                );
            }
            MemoryPostTurnEligibilityPolicy::Malformed => {
                return MemoryPostTurnEligibilityDecision::Skipped(
                    MemoryPostTurnEligibilitySkipReason::MalformedPolicy,
                );
            }
        };
        if !post_turn_policy_allows_any_extraction(policy) {
            return MemoryPostTurnEligibilityDecision::Skipped(
                MemoryPostTurnEligibilitySkipReason::PolicyDisabledExtraction,
            );
        }
        if !input.has_user_text
            && !input.has_assistant_text
            && !input.has_tool_events
            && !input.has_domain_events
        {
            return MemoryPostTurnEligibilityDecision::Skipped(
                MemoryPostTurnEligibilitySkipReason::NoTranscript,
            );
        }
        if input.task_runtime_owned
            || input.source_context_kind == MemorySourceContextKind::TaskRuntime
        {
            return MemoryPostTurnEligibilityDecision::Skipped(
                MemoryPostTurnEligibilitySkipReason::TaskRuntimeOwnedSource,
            );
        }
        if input.system_runtime_owned
            || matches!(
                input.source_context_kind,
                MemorySourceContextKind::ToolResult
                    | MemorySourceContextKind::SystemRuntime
                    | MemorySourceContextKind::GeneratedSummary
            )
        {
            return MemoryPostTurnEligibilityDecision::Skipped(
                MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource,
            );
        }
        if input.source_context_kind != MemorySourceContextKind::DirectUserConversation {
            return MemoryPostTurnEligibilityDecision::Skipped(
                MemoryPostTurnEligibilitySkipReason::NoDirectUserSource,
            );
        }
        MemoryPostTurnEligibilityDecision::Eligible
    }
}

pub(super) fn memory_post_turn_eligibility_input_from_request(
    request: &HookHandlerRequest,
    input: &TurnPostTurnHookInput,
    config: &MemoryPostTurnExtractorConfig,
    policy: MemoryPostTurnEligibilityPolicy,
) -> MemoryPostTurnEligibilityInput {
    let has_user_text = input
        .user_text
        .as_ref()
        .map(|text| !text.text.trim().is_empty())
        .unwrap_or(false);
    let has_assistant_text = input
        .assistant_text
        .as_ref()
        .map(|text| !text.text.trim().is_empty())
        .unwrap_or(false);
    let has_tool_events = !input.tool_events.is_empty();
    let has_domain_events = !input.domain_events.is_empty();
    let task_runtime_owned = post_turn_request_is_task_owned(request, input);
    let system_runtime_owned = post_turn_request_is_system_owned(request);
    let source_context_kind = post_turn_source_context_kind(
        has_user_text,
        has_assistant_text,
        has_tool_events,
        has_domain_events,
        task_runtime_owned,
        system_runtime_owned,
    );

    MemoryPostTurnEligibilityInput {
        config_enabled: config.normalized().enabled,
        status: input.status,
        policy,
        has_user_text,
        has_assistant_text,
        has_tool_events,
        has_domain_events,
        source_context_kind,
        task_runtime_owned,
        system_runtime_owned,
    }
}

fn post_turn_request_is_task_owned(
    request: &HookHandlerRequest,
    input: &TurnPostTurnHookInput,
) -> bool {
    request.context.task_id.is_some()
        || matches!(request.context.mode.as_ref(), Some(HookContextMode::Task))
        || request
            .context
            .actor
            .as_ref()
            .map(|actor| actor.kind == HookActorKind::Task)
            .unwrap_or(false)
        || input
            .domain_events
            .iter()
            .any(|event| event.domain == TurnPostTurnDomain::Task)
}

fn post_turn_request_is_system_owned(request: &HookHandlerRequest) -> bool {
    matches!(request.context.mode.as_ref(), Some(HookContextMode::System))
        || request
            .context
            .actor
            .as_ref()
            .map(|actor| {
                matches!(
                    actor.kind,
                    HookActorKind::System | HookActorKind::Service | HookActorKind::Automation
                )
            })
            .unwrap_or(false)
}

fn post_turn_source_context_kind(
    has_user_text: bool,
    has_assistant_text: bool,
    has_tool_events: bool,
    has_domain_events: bool,
    task_runtime_owned: bool,
    system_runtime_owned: bool,
) -> MemorySourceContextKind {
    if task_runtime_owned {
        return MemorySourceContextKind::TaskRuntime;
    }
    if system_runtime_owned {
        return MemorySourceContextKind::SystemRuntime;
    }
    if has_user_text {
        return MemorySourceContextKind::DirectUserConversation;
    }
    if has_tool_events {
        return MemorySourceContextKind::ToolResult;
    }
    if has_domain_events {
        return MemorySourceContextKind::SystemRuntime;
    }
    if has_assistant_text {
        return MemorySourceContextKind::AssistantResponse;
    }
    MemorySourceContextKind::Unknown
}
