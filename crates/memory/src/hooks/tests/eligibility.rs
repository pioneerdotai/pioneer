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

fn resolver_input(request: &HookHandlerRequest) -> MemoryPostTurnEligibilityInput {
    memory_post_turn_eligibility_input_from_request(
        request,
        turn_post_turn_input(request).expect("post-turn input exists"),
        &MemoryPostTurnExtractorConfig::default(),
        MemoryPostTurnEligibilityPolicy::Available(MemoryTurnPolicy::normal_default_allow()),
    )
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
