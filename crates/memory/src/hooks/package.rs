use super::*;

pub const MEMORY_HOOK_PACKAGE_ID: &str = "pioneer.memory";

pub fn package(
    memory_provider: Arc<dyn AgentMemoryProvider>,
    memory_write_provider: Option<Arc<dyn AgentMemoryWriteProvider>>,
    post_turn_extractor_provider: Option<Arc<dyn AgentMemoryPostTurnExtractorProvider>>,
    policy_provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    tool_bundle_artifacts: Arc<dyn MemoryToolBundleArtifactStore>,
    memory_config: MemoryLoopConfig,
) -> MemoryHookPackage {
    MemoryHookPackage {
        memory_provider,
        memory_write_provider,
        post_turn_extractor_provider,
        policy_provider,
        tool_bundle_artifacts,
        memory_config,
    }
}

pub struct MemoryHookPackage {
    memory_provider: Arc<dyn AgentMemoryProvider>,
    memory_write_provider: Option<Arc<dyn AgentMemoryWriteProvider>>,
    post_turn_extractor_provider: Option<Arc<dyn AgentMemoryPostTurnExtractorProvider>>,
    policy_provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    tool_bundle_artifacts: Arc<dyn MemoryToolBundleArtifactStore>,
    memory_config: MemoryLoopConfig,
}

impl HookPackage for MemoryHookPackage {
    fn id(&self) -> &'static str {
        MEMORY_HOOK_PACKAGE_ID
    }

    fn definitions(&self) -> Result<Vec<HookDefinition>, HookRegistryError> {
        let memory_config = self.memory_config.clone().normalized();
        let active_recall_config = memory_config.active_recall.clone();
        let post_turn_extractor_config = memory_config.post_turn_extractor.clone();
        let state = Arc::new(MemoryHookTurnStateStore::default());

        Ok(vec![
            memory_hook_definition(
                Arc::new(MemoryPolicyClassifierHook {
                    policy_provider: self.policy_provider.clone(),
                    state: state.clone(),
                }),
                MEMORY_POLICY_CLASSIFIER_SUBSCRIPTION_ID,
                HookPhase::TurnPrePolicy,
                0,
            ),
            memory_hook_definition(
                Arc::new(MemoryDeterministicRecallHook {
                    memory_provider: self.memory_provider.clone(),
                }),
                MEMORY_DETERMINISTIC_RECALL_SUBSCRIPTION_ID,
                HookPhase::TurnPrePromptContext,
                0,
            ),
            memory_hook_definition_with_options(
                Arc::new(ActiveMemoryRecallHook {
                    memory_provider: self.memory_provider.clone(),
                    decision_provider: None,
                    config: active_recall_config.clone(),
                }),
                MEMORY_ACTIVE_RECALL_SUBSCRIPTION_ID,
                HookPhase::TurnPrePromptContext,
                -10,
                HookExecutionPolicy {
                    await_policy: HookAwaitPolicy::Deadline,
                    timeout_ms: Some(active_recall_config.timeout_ms),
                    max_parallelism: None,
                },
                HookSubscriptionDependencies::new(
                    [
                        HookSubscriptionId::new(MEMORY_DETERMINISTIC_RECALL_SUBSCRIPTION_ID)
                            .expect("static subscription id is valid"),
                    ],
                    [],
                ),
                HookSubscriptionVisibility::Internal,
            ),
            memory_hook_definition(
                Arc::new(MemoryToolBundleHook {
                    memory_provider: self.memory_provider.clone(),
                    state: state.clone(),
                    tool_bundle_artifacts: self.tool_bundle_artifacts.clone(),
                }),
                MEMORY_TOOL_BUNDLE_SUBSCRIPTION_ID,
                HookPhase::TurnPreToolMaterialization,
                0,
            ),
            memory_hook_definition(
                Arc::new(MemoryPromptContractHook),
                MEMORY_PROMPT_CONTRACT_SUBSCRIPTION_ID,
                HookPhase::TurnPrePromptCompile,
                0,
            ),
            memory_hook_definition_with_options_and_retry(
                Arc::new(MemoryPostTurnExtractorHook {
                    write_provider: self.memory_write_provider.clone(),
                    extractor_provider: self.post_turn_extractor_provider.clone(),
                    config: post_turn_extractor_config.clone(),
                }),
                MEMORY_POST_TURN_EXTRACTOR_SUBSCRIPTION_ID,
                HookPhase::TurnPostTurn,
                0,
                HookExecutionPolicy {
                    await_policy: post_turn_extractor_config.await_policy,
                    timeout_ms: Some(post_turn_extractor_config.timeout_ms),
                    max_parallelism: None,
                },
                HookSubscriptionDependencies::default(),
                HookSubscriptionVisibility::Internal,
                HookRetryPolicy {
                    max_attempts: 2,
                    backoff: HookRetryBackoff::Fixed,
                    initial_delay_ms: Some(1_000),
                    idempotency_required: true,
                },
            ),
        ])
    }
}

pub(super) fn memory_hook_definition(
    handler: Arc<dyn HookHandler>,
    subscription_id: &'static str,
    phase: HookPhase,
    priority: i32,
) -> HookDefinition {
    memory_hook_definition_with_options(
        handler,
        subscription_id,
        phase,
        priority,
        HookExecutionPolicy {
            await_policy: HookAwaitPolicy::Blocking,
            timeout_ms: None,
            max_parallelism: None,
        },
        HookSubscriptionDependencies::default(),
        HookSubscriptionVisibility::Internal,
    )
}

pub(super) fn memory_hook_definition_with_options(
    handler: Arc<dyn HookHandler>,
    subscription_id: &'static str,
    phase: HookPhase,
    priority: i32,
    execution_policy: HookExecutionPolicy,
    dependencies: HookSubscriptionDependencies,
    visibility: HookSubscriptionVisibility,
) -> HookDefinition {
    memory_hook_definition_with_options_and_retry(
        handler,
        subscription_id,
        phase,
        priority,
        execution_policy,
        dependencies,
        visibility,
        HookRetryPolicy::default(),
    )
}

pub(super) fn memory_hook_definition_with_options_and_retry(
    handler: Arc<dyn HookHandler>,
    subscription_id: &'static str,
    phase: HookPhase,
    priority: i32,
    execution_policy: HookExecutionPolicy,
    dependencies: HookSubscriptionDependencies,
    visibility: HookSubscriptionVisibility,
    retry_policy: HookRetryPolicy,
) -> HookDefinition {
    let hook_id = handler.id();
    let subscription_id =
        HookSubscriptionId::new(subscription_id).expect("static subscription id is valid");
    HookDefinition::new(
        handler,
        [HookSubscription::new(subscription_id, hook_id, phase)
            .with_priority(priority)
            .with_dependencies(dependencies)
            .with_execution_policy(execution_policy)
            .with_failure_policy(HookFailurePolicy::BestEffort)
            .with_retry_policy(retry_policy)
            .with_visibility(visibility)],
        MEMORY_HOOK_PACKAGE_ID,
    )
}
