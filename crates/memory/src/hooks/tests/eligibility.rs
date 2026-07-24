use super::*;

fn eligible_input() -> MemoryPostTurnEligibilityInput {
    MemoryPostTurnEligibilityInput {
        config_enabled: true,
        status: TurnPostTurnStatus::Succeeded,
        policy: MemoryPostTurnEligibilityPolicy::Available(MemoryTurnPolicy::normal_default_allow()),
        has_user_text: true,
        has_assistant_text: true,
        has_tool_events: false,
        has_domain_events: false,
        source_context_kind: MemorySourceContextKind::DirectUserConversation,
        task_runtime_owned: false,
        accepted_task_result: false,
        system_runtime_owned: false,
    }
}

fn resolver_request(
    user_text: Option<&str>,
    assistant_text: Option<&str>,
    tool_events: Vec<pioneer_hooks::TurnPostTurnToolEventSummary>,
    domain_events: Vec<pioneer_hooks::TurnPostTurnDomainEventSummary>,
) -> HookHandlerRequest {
    HookHandlerRequest {
        hook_id: HookId::new(MEMORY_POST_TURN_EXTRACTOR_HOOK_ID).expect("static hook id is valid"),
        phase: HookPhase::TurnPostTurn,
        context: HookContext {
            workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
            thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
            turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
            ..HookContext::default()
        },
        input: HookInput::turn_post_turn(TurnPostTurnHookInput::from_parts_with_model(
            TurnPostTurnStatus::Succeeded,
            Some("test-model"),
            Some("test-provider"),
            user_text,
            assistant_text,
            None::<&str>,
            tool_events,
            domain_events,
            pioneer_hooks::TurnPostTurnHookInputLimits::default(),
        )),
        policy_set: HookPolicySet::empty(),
        prompt_context_set: HookPromptContextSet::default(),
    }
}

fn tool_event() -> pioneer_hooks::TurnPostTurnToolEventSummary {
    pioneer_hooks::TurnPostTurnToolEventSummary {
        item_id: "tool-item".to_owned(),
        item_type: "dynamic_tool_call".to_owned(),
        tool_name: "exec".to_owned(),
        attempt_number: 1,
        status: pioneer_hooks::TurnPostTurnToolStatus::Succeeded,
        outcome_status: Some(pioneer_hooks::TurnPostTurnToolOutcomeStatus::Ok),
        error_class: None,
    }
}

fn domain_event(
    domain: pioneer_hooks::TurnPostTurnDomain,
) -> pioneer_hooks::TurnPostTurnDomainEventSummary {
    pioneer_hooks::TurnPostTurnDomainEventSummary {
        domain,
        code: Some("completed".to_owned()),
        item_id: Some("domain-item".to_owned()),
        message: None,
    }
}

fn resolver_input(request: &HookHandlerRequest) -> MemoryPostTurnEligibilityInput {
    memory_post_turn_eligibility_input_from_request(
        request,
        turn_post_turn_input(request).expect("post-turn input exists"),
        &MemoryPostTurnExtractorConfig::default(),
        MemoryPostTurnEligibilityPolicy::Available(MemoryTurnPolicy::normal_default_allow()),
    )
}

#[test]
fn post_turn_eligibility_gate_allows_direct_user_turns() {
    let decision = MemoryPostTurnEligibilityGate::evaluate(&eligible_input());

    assert!(decision.is_eligible());
    assert_eq!(decision, MemoryPostTurnEligibilityDecision::Eligible);
}

#[test]
fn post_turn_eligibility_gate_skips_non_success_turns() {
    let input = MemoryPostTurnEligibilityInput {
        status: TurnPostTurnStatus::ProviderFailure,
        ..eligible_input()
    };

    assert_eq!(
        MemoryPostTurnEligibilityGate::evaluate(&input),
        MemoryPostTurnEligibilityDecision::Skipped(
            MemoryPostTurnEligibilitySkipReason::NonSuccessTurn
        )
    );
}

#[test]
fn post_turn_eligibility_gate_skips_policy_disabled_extraction() {
    let input = MemoryPostTurnEligibilityInput {
        policy: MemoryPostTurnEligibilityPolicy::Available(MemoryTurnPolicy::no_save()),
        ..eligible_input()
    };

    assert_eq!(
        MemoryPostTurnEligibilityGate::evaluate(&input),
        MemoryPostTurnEligibilityDecision::Skipped(
            MemoryPostTurnEligibilitySkipReason::PolicyDisabledExtraction
        )
    );
}

#[test]
fn post_turn_eligibility_gate_skips_missing_and_malformed_policy() {
    for (policy, reason) in [
        (
            MemoryPostTurnEligibilityPolicy::Missing,
            MemoryPostTurnEligibilitySkipReason::MissingPolicy,
        ),
        (
            MemoryPostTurnEligibilityPolicy::Malformed,
            MemoryPostTurnEligibilitySkipReason::MalformedPolicy,
        ),
    ] {
        let input = MemoryPostTurnEligibilityInput {
            policy,
            ..eligible_input()
        };

        assert_eq!(
            MemoryPostTurnEligibilityGate::evaluate(&input),
            MemoryPostTurnEligibilityDecision::Skipped(reason)
        );
        assert!(!reason.as_str().is_empty());
        assert!(
            reason
                .diagnostic_code()
                .starts_with("memory.post_turn_eligibility.")
        );
        assert!(!reason.message().is_empty());
    }
}

#[test]
fn post_turn_eligibility_gate_skips_empty_transcript() {
    let input = MemoryPostTurnEligibilityInput {
        has_user_text: false,
        has_assistant_text: false,
        has_tool_events: false,
        has_domain_events: false,
        ..eligible_input()
    };

    assert_eq!(
        MemoryPostTurnEligibilityGate::evaluate(&input),
        MemoryPostTurnEligibilityDecision::Skipped(
            MemoryPostTurnEligibilitySkipReason::NoTranscript
        )
    );
}

#[test]
fn post_turn_eligibility_gate_skips_task_system_and_tool_sources() {
    let cases = [
        (
            MemoryPostTurnEligibilityInput {
                source_context_kind: MemorySourceContextKind::TaskRuntime,
                task_runtime_owned: true,
                ..eligible_input()
            },
            MemoryPostTurnEligibilitySkipReason::TaskRuntimeOwnedSource,
        ),
        (
            MemoryPostTurnEligibilityInput {
                source_context_kind: MemorySourceContextKind::ToolResult,
                has_user_text: false,
                has_assistant_text: false,
                has_tool_events: true,
                ..eligible_input()
            },
            MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource,
        ),
        (
            MemoryPostTurnEligibilityInput {
                source_context_kind: MemorySourceContextKind::SystemRuntime,
                has_user_text: false,
                has_assistant_text: false,
                has_domain_events: true,
                system_runtime_owned: true,
                ..eligible_input()
            },
            MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource,
        ),
        (
            MemoryPostTurnEligibilityInput {
                source_context_kind: MemorySourceContextKind::AssistantResponse,
                has_user_text: false,
                has_assistant_text: true,
                ..eligible_input()
            },
            MemoryPostTurnEligibilitySkipReason::NoDirectUserSource,
        ),
    ];

    for (input, reason) in cases {
        assert_eq!(
            MemoryPostTurnEligibilityGate::evaluate(&input),
            MemoryPostTurnEligibilityDecision::Skipped(reason)
        );
    }
}

#[test]
fn post_turn_eligibility_resolver_classifies_direct_user_with_events_as_user_source() {
    let request = resolver_request(
        Some("stable user assertion"),
        Some("assistant response"),
        vec![tool_event()],
        vec![domain_event(pioneer_hooks::TurnPostTurnDomain::Custom(
            "test".to_owned(),
        ))],
    );

    let input = resolver_input(&request);

    assert!(input.has_user_text);
    assert!(input.has_tool_events);
    assert!(input.has_domain_events);
    assert_eq!(
        input.source_context_kind,
        MemorySourceContextKind::DirectUserConversation
    );
    assert!(!input.task_runtime_owned);
    assert!(!input.system_runtime_owned);
    assert!(MemoryPostTurnEligibilityGate::evaluate(&input).is_eligible());
}

#[test]
fn post_turn_eligibility_resolver_classifies_assistant_tool_domain_and_task_sources() {
    let assistant_only = resolver_input(&resolver_request(
        None,
        Some("assistant response"),
        Vec::new(),
        Vec::new(),
    ));
    assert_eq!(
        assistant_only.source_context_kind,
        MemorySourceContextKind::AssistantResponse
    );
    assert_eq!(
        MemoryPostTurnEligibilityGate::evaluate(&assistant_only),
        MemoryPostTurnEligibilityDecision::Skipped(
            MemoryPostTurnEligibilitySkipReason::NoDirectUserSource
        )
    );

    let tool_only = resolver_input(&resolver_request(
        None,
        None,
        vec![tool_event()],
        Vec::new(),
    ));
    assert_eq!(
        tool_only.source_context_kind,
        MemorySourceContextKind::ToolResult
    );
    assert_eq!(
        MemoryPostTurnEligibilityGate::evaluate(&tool_only),
        MemoryPostTurnEligibilityDecision::Skipped(
            MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource
        )
    );

    let domain_only = resolver_input(&resolver_request(
        None,
        None,
        Vec::new(),
        vec![domain_event(pioneer_hooks::TurnPostTurnDomain::Memory)],
    ));
    assert_eq!(
        domain_only.source_context_kind,
        MemorySourceContextKind::SystemRuntime
    );
    assert_eq!(
        MemoryPostTurnEligibilityGate::evaluate(&domain_only),
        MemoryPostTurnEligibilityDecision::Skipped(
            MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource
        )
    );

    let mut task_owned = resolver_request(
        Some("user text"),
        Some("assistant response"),
        Vec::new(),
        Vec::new(),
    );
    task_owned.context.task_id =
        Some(pioneer_hooks::HookTaskId::new("task-1").expect("valid task id"));
    let task_owned = resolver_input(&task_owned);
    assert_eq!(
        task_owned.source_context_kind,
        MemorySourceContextKind::TaskRuntime
    );
    assert_eq!(
        MemoryPostTurnEligibilityGate::evaluate(&task_owned),
        MemoryPostTurnEligibilityDecision::Skipped(
            MemoryPostTurnEligibilitySkipReason::TaskRuntimeOwnedSource
        )
    );
}

#[test]
fn post_turn_eligibility_resolver_classifies_system_context_as_system_owned() {
    let mut request = resolver_request(
        Some("user text"),
        Some("assistant response"),
        Vec::new(),
        Vec::new(),
    );
    request.context.mode = Some(pioneer_hooks::HookContextMode::System);

    let input = resolver_input(&request);

    assert_eq!(
        input.source_context_kind,
        MemorySourceContextKind::SystemRuntime
    );
    assert!(input.system_runtime_owned);
    assert_eq!(
        MemoryPostTurnEligibilityGate::evaluate(&input),
        MemoryPostTurnEligibilityDecision::Skipped(
            MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource
        )
    );
}

#[test]
fn post_turn_eligibility_allows_only_accepted_task_results_with_parent_scope() {
    let mut accepted = resolver_request(
        Some("background request"),
        Some("accepted final result"),
        Vec::new(),
        Vec::new(),
    );
    accepted.context.task_id =
        Some(pioneer_hooks::HookTaskId::new("task-accepted").expect("valid task id"));
    accepted.context.mode = Some(pioneer_hooks::HookContextMode::Task);
    accepted.context.conversation_thread_id =
        Some(pioneer_hooks::HookThreadId::new("thread-parent").expect("valid parent id"));
    accepted.context.feature_flags.insert(
        pioneer_hooks::HookFeatureFlag::new(MEMORY_ACCEPTED_TASK_RESULT_POST_TURN_FEATURE_FLAG)
            .expect("valid feature flag"),
        true,
    );

    let accepted_input = resolver_input(&accepted);
    assert!(accepted_input.task_runtime_owned);
    assert!(accepted_input.accepted_task_result);
    assert_eq!(
        accepted_input.source_context_kind,
        MemorySourceContextKind::TaskRuntime
    );
    assert_eq!(
        MemoryPostTurnEligibilityGate::evaluate(&accepted_input),
        MemoryPostTurnEligibilityDecision::Eligible
    );

    accepted.context.conversation_thread_id = None;
    let missing_parent_scope = resolver_input(&accepted);
    assert!(!missing_parent_scope.accepted_task_result);
    assert_eq!(
        MemoryPostTurnEligibilityGate::evaluate(&missing_parent_scope),
        MemoryPostTurnEligibilityDecision::Skipped(
            MemoryPostTurnEligibilitySkipReason::TaskRuntimeOwnedSource
        )
    );
}
