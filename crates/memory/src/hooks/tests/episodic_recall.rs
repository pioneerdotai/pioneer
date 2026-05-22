use super::*;

#[derive(Default)]
struct FakeEpisodicRecallProvider {
    capabilities: MemoryEpisodicRecallCapabilities,
    current_thread: MemoryEpisodicRecallResponse,
    related_threads: MemoryEpisodicRecallResponse,
    current_task: MemoryEpisodicRecallResponse,
    completed_tasks: MemoryEpisodicRecallResponse,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

struct SlowEpisodicRecallProvider;

#[async_trait::async_trait]
impl AgentEpisodicRecallProvider for SlowEpisodicRecallProvider {
    async fn recall_capabilities(
        &self,
        _context: MemoryTurnContext,
    ) -> MemoryEpisodicRecallCapabilities {
        MemoryEpisodicRecallCapabilities {
            current_thread_search: true,
            ..MemoryEpisodicRecallCapabilities::default()
        }
    }

    async fn recall_current_thread(
        &self,
        _request: MemoryCurrentThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok(MemoryEpisodicRecallResponse::default())
    }
}

impl FakeEpisodicRecallProvider {
    fn with_capabilities(capabilities: MemoryEpisodicRecallCapabilities) -> Self {
        Self {
            capabilities,
            ..Self::default()
        }
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().expect("calls lock poisoned").clone()
    }
}

#[async_trait::async_trait]
impl AgentEpisodicRecallProvider for FakeEpisodicRecallProvider {
    async fn recall_capabilities(
        &self,
        _context: MemoryTurnContext,
    ) -> MemoryEpisodicRecallCapabilities {
        self.capabilities.clone()
    }

    async fn recall_current_thread(
        &self,
        _request: MemoryCurrentThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        self.calls
            .lock()
            .expect("calls lock poisoned")
            .push("current_thread");
        Ok(self.current_thread.clone())
    }

    async fn recall_related_threads(
        &self,
        _request: MemoryRelatedThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        self.calls
            .lock()
            .expect("calls lock poisoned")
            .push("related_threads");
        Ok(self.related_threads.clone())
    }

    async fn recall_current_task(
        &self,
        _request: MemoryCurrentTaskRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        self.calls
            .lock()
            .expect("calls lock poisoned")
            .push("current_task");
        Ok(self.current_task.clone())
    }

    async fn recall_completed_tasks(
        &self,
        _request: MemoryCompletedTaskRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        self.calls
            .lock()
            .expect("calls lock poisoned")
            .push("completed_tasks");
        Ok(self.completed_tasks.clone())
    }
}

fn episodic_item(
    id: &str,
    source: MemoryEpisodicRecallSourceKind,
    content: &str,
) -> MemoryEpisodicRecallItem {
    MemoryEpisodicRecallItem {
        id: id.to_owned(),
        content: content.to_owned(),
        title: None,
        provenance: MemoryEpisodicRecallProvenance {
            workspace_id: "ws".to_owned(),
            thread_id: Some(
                match source {
                    MemoryEpisodicRecallSourceKind::RelatedThread => "related_thr",
                    _ => "thr",
                }
                .to_owned(),
            ),
            turn_id: Some("turn_prev".to_owned()),
            task_id: matches!(
                source,
                MemoryEpisodicRecallSourceKind::CurrentTask
                    | MemoryEpisodicRecallSourceKind::CompletedTask
            )
            .then(|| "task_1".to_owned()),
            timestamp_unix: Some(42),
            source,
            retrieval_score: Some(0.8),
            boundary: MemoryEpisodicRecallBoundary::Snippet,
        },
        score: Some(0.8),
        updated_at_unix: Some(42),
        visibility: MemoryEpisodicRecallVisibility::Public,
    }
}

fn task_memory_turn_context() -> MemoryTurnContext {
    MemoryTurnContext {
        task_id: Some("task_1".to_owned()),
        ..test_memory_turn_context()
    }
}

fn episodic_planner_input_for_test() -> ActiveRecallPlannerInput {
    ActiveRecallPlannerInput {
        workspace_id: "ws".to_owned(),
        thread_id: "thr".to_owned(),
        turn_id: "turn".to_owned(),
        task_id: None,
        agent_id: None,
        mode: ThreadMode::Agent,
        input_text_preview: "continue prior context".to_owned(),
        input_text_char_count: 22,
        input_length_bucket: ActiveRecallInputLengthBucket::Short,
        read_allowed: true,
        active_memory_allowed: true,
        explicit_no_memory: false,
        config_mode: MemoryActiveRecallMode::Hybrid,
        deterministic_context_count: 0,
        deterministic_context_chars: 0,
        deterministic_memory_ids: Vec::new(),
        deterministic_sufficient: false,
        deterministic_recall_empty: true,
        deterministic_categories: Vec::new(),
        typed_targets: Vec::new(),
        has_workspace_context: true,
        has_task_context: false,
        episodic_capabilities: MemoryEpisodicRecallCapabilities::default(),
    }
}

#[test]
fn episodic_recall_dtos_are_typed_and_serializable() {
    let item = episodic_item(
        "thread_snippet_1",
        MemoryEpisodicRecallSourceKind::CurrentThread,
        "Earlier in this thread, the user chose hook-based memory recall.",
    );

    let encoded = serde_json::to_value(&item).expect("episodic item serializes");
    assert_eq!(encoded["provenance"]["source"], "current_thread");
    assert_eq!(encoded["provenance"]["boundary"], "snippet");

    let decoded: MemoryEpisodicRecallItem =
        serde_json::from_value(encoded).expect("episodic item deserializes");
    assert_eq!(
        decoded.provenance.source,
        MemoryEpisodicRecallSourceKind::CurrentThread
    );
    assert_eq!(decoded.visibility, MemoryEpisodicRecallVisibility::Public);
}

#[tokio::test]
async fn missing_episodic_capability_drops_provider_selected_mode() {
    let provider = Arc::new(TestRecallMemoryProvider::with_recall(
        MemoryRecallSnapshot::empty(),
    ));
    let hook = ActiveMemoryRecallHook {
        memory_provider: provider.clone(),
        episodic_provider: None,
        config: MemoryActiveRecallConfig::default(),
    };
    let plan = parse_active_memory_decision_json(
        r#"{"status":"run","reasonCode":"provider_run","confidence":0.9,"modes":["current_thread"],"targets":[]}"#,
    )
    .expect("provider-owned preflight plan parses");
    let input = TurnPostPreflightPromptContextHookInput::from_parts(
        "continue what we discussed earlier",
        Some("test-model"),
        Some("test-provider"),
    )
    .with_active_memory_recall_preflight_plan(
        serde_json::to_value(plan).expect("active recall plan serializes"),
    );
    let mut request = test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "continue what we discussed earlier",
    );
    request.input = HookInput::turn_post_preflight_prompt_context(input);

    let response = hook
        .execute(request)
        .await
        .expect("active recall hook executes");

    assert_eq!(provider.recall_call_count(), 0);
    assert_no_prompt_context_contributions(&response);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .as_str()
            .contains("dropped_mode=thread_episodic:capability_unavailable")
    }));
}

#[test]
fn active_recall_planner_parses_new_episodic_modes_and_validates_capabilities() {
    let plan = parse_active_memory_decision_json(
        r#"{"status":"run","reasonCode":"provider_run","confidence":0.9,"modes":["current_thread","related_thread","current_task","completed_task"],"targets":[]}"#,
    )
    .expect("provider plan parses");
    assert_eq!(
        plan.modes,
        vec![
            ActiveRecallMode::CurrentTask,
            ActiveRecallMode::CurrentThread,
            ActiveRecallMode::RelatedThread,
            ActiveRecallMode::CompletedTask
        ]
    );

    let mut input = episodic_planner_input_for_test();
    input.task_id = Some("task_1".to_owned());
    input.has_task_context = true;
    input.episodic_capabilities.current_thread_search = true;
    input.episodic_capabilities.current_task_context = true;
    let normalized = normalize_active_recall_plan_for_input(plan, &input);

    assert_eq!(
        normalized.modes,
        vec![
            ActiveRecallMode::CurrentTask,
            ActiveRecallMode::CurrentThread
        ]
    );
    assert!(
        normalized.diagnostics.iter().any(|diagnostic| {
            diagnostic == "dropped_mode=related_thread:capability_unavailable"
        })
    );
    assert!(
        normalized.diagnostics.iter().any(|diagnostic| {
            diagnostic == "dropped_mode=completed_task:capability_unavailable"
        })
    );
}

#[tokio::test]
async fn current_thread_recall_uses_native_provider_and_separate_prompt_context() {
    let durable_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        MemoryRecallSnapshot::empty(),
    ));
    let mut episodic =
        FakeEpisodicRecallProvider::with_capabilities(MemoryEpisodicRecallCapabilities {
            current_thread_search: true,
            ..MemoryEpisodicRecallCapabilities::default()
        });
    episodic.current_thread = MemoryEpisodicRecallResponse {
        items: vec![episodic_item(
            "thread_snippet_1",
            MemoryEpisodicRecallSourceKind::CurrentThread,
            "Earlier in this thread, the user rejected phrase-list memory policy.",
        )],
        ..MemoryEpisodicRecallResponse::default()
    };
    let episodic = Arc::new(episodic);
    let hook = ActiveMemoryRecallHook {
        memory_provider: durable_provider.clone(),
        episodic_provider: Some(episodic.clone()),
        config: MemoryActiveRecallConfig::default(),
    };
    let plan = parse_active_memory_decision_json(
        r#"{"status":"run","reasonCode":"provider_run","confidence":0.9,"modes":["current_thread"],"targets":[]}"#,
    )
    .expect("provider-owned preflight plan parses");
    let input = TurnPostPreflightPromptContextHookInput::from_parts(
        "continue what we discussed earlier",
        Some("test-model"),
        Some("test-provider"),
    )
    .with_active_memory_recall_preflight_plan(
        serde_json::to_value(plan).expect("active recall plan serializes"),
    );
    let mut request = test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "continue what we discussed earlier",
    );
    request.input = HookInput::turn_post_preflight_prompt_context(input);

    let response = hook
        .execute(request)
        .await
        .expect("active recall hook executes");

    assert_eq!(durable_provider.recall_call_count(), 0);
    assert_eq!(episodic.calls(), vec!["current_thread"]);
    let contributions = prompt_context_contributions(&response);
    assert_eq!(contributions.len(), 1);
    assert_eq!(
        contributions[0].contribution_id.as_str(),
        MEMORY_THREAD_CONTEXT_CONTRIBUTION_ID
    );
    assert!(
        contributions[0]
            .content
            .as_str()
            .contains("current thread snippet")
    );
    assert_eq!(
        contributions[0].source_refs[0].kind.as_str(),
        "thread_context"
    );
}

#[tokio::test]
async fn related_thread_recall_filters_workspace_and_visibility_boundaries() {
    let durable_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        MemoryRecallSnapshot::empty(),
    ));
    let mut hidden = episodic_item(
        "hidden_related",
        MemoryEpisodicRecallSourceKind::RelatedThread,
        "Hidden related thread content.",
    );
    hidden.visibility = MemoryEpisodicRecallVisibility::Hidden;
    let mut other_workspace = episodic_item(
        "other_workspace_related",
        MemoryEpisodicRecallSourceKind::RelatedThread,
        "Other workspace related thread content.",
    );
    other_workspace.provenance.workspace_id = "other_ws".to_owned();
    let visible = episodic_item(
        "visible_related",
        MemoryEpisodicRecallSourceKind::RelatedThread,
        "Related thread decided to keep thread context separate from memory.",
    );
    let mut episodic =
        FakeEpisodicRecallProvider::with_capabilities(MemoryEpisodicRecallCapabilities {
            related_thread_search: true,
            ..MemoryEpisodicRecallCapabilities::default()
        });
    episodic.related_threads = MemoryEpisodicRecallResponse {
        items: vec![hidden, other_workspace, visible],
        ..MemoryEpisodicRecallResponse::default()
    };
    let episodic = Arc::new(episodic);

    let result = execute_active_recall_plan(
        durable_provider.as_ref(),
        ActiveRecallExecutionInput {
            context: test_memory_turn_context(),
            plan: ActiveRecallPlan::run(
                ActiveMemoryDecisionReasonCode::ProviderRun,
                0.9,
                vec![ActiveRecallMode::RelatedThread],
                Vec::new(),
                Vec::new(),
            ),
            deterministic: DeterministicRecallContextSummary::default(),
            config: MemoryActiveRecallConfig::default(),
            episodic_provider: Some(episodic.clone()),
            episodic_capabilities: MemoryEpisodicRecallCapabilities {
                related_thread_search: true,
                ..MemoryEpisodicRecallCapabilities::default()
            },
        },
    )
    .await;

    assert_eq!(durable_provider.recall_call_count(), 0);
    assert_eq!(episodic.calls(), vec!["related_threads"]);
    assert_eq!(result.episodic_items.len(), 1);
    assert_eq!(result.episodic_items[0].id, "visible_related");
    assert!(result.diagnostics.iter().any(|diagnostic| {
        diagnostic.contains("memory.episodic_recall.filtered_count:related_thread:2")
    }));
}

#[tokio::test]
async fn episodic_recall_applies_source_caps_and_deterministic_truncation() {
    let durable_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        MemoryRecallSnapshot::empty(),
    ));
    let mut episodic =
        FakeEpisodicRecallProvider::with_capabilities(MemoryEpisodicRecallCapabilities {
            related_thread_search: true,
            ..MemoryEpisodicRecallCapabilities::default()
        });
    episodic.related_threads = MemoryEpisodicRecallResponse {
        items: vec![
            episodic_item(
                "related_high",
                MemoryEpisodicRecallSourceKind::RelatedThread,
                "High score related thread context.",
            ),
            episodic_item(
                "related_low",
                MemoryEpisodicRecallSourceKind::RelatedThread,
                "Lower score related thread context.",
            ),
        ],
        ..MemoryEpisodicRecallResponse::default()
    };
    episodic.related_threads.items[0].score = Some(0.9);
    episodic.related_threads.items[1].score = Some(0.1);
    let episodic = Arc::new(episodic);

    let result = execute_active_recall_plan(
        durable_provider.as_ref(),
        ActiveRecallExecutionInput {
            context: test_memory_turn_context(),
            plan: ActiveRecallPlan::run(
                ActiveMemoryDecisionReasonCode::ProviderRun,
                0.9,
                vec![ActiveRecallMode::RelatedThread],
                Vec::new(),
                Vec::new(),
            ),
            deterministic: DeterministicRecallContextSummary::default(),
            config: MemoryActiveRecallConfig {
                top_k_per_query: 1,
                max_prompt_chars: 80,
                ..MemoryActiveRecallConfig::default()
            },
            episodic_provider: Some(episodic),
            episodic_capabilities: MemoryEpisodicRecallCapabilities {
                related_thread_search: true,
                ..MemoryEpisodicRecallCapabilities::default()
            },
        },
    )
    .await;

    assert_eq!(result.episodic_items.len(), 1);
    assert_eq!(result.episodic_items[0].id, "related_high");
    assert!(result.truncated);
}

#[tokio::test]
async fn task_context_and_completed_task_modes_use_native_provider() {
    let durable_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        MemoryRecallSnapshot::empty(),
    ));
    let mut episodic =
        FakeEpisodicRecallProvider::with_capabilities(MemoryEpisodicRecallCapabilities {
            current_task_context: true,
            completed_task_summary: true,
            ..MemoryEpisodicRecallCapabilities::default()
        });
    episodic.current_task = MemoryEpisodicRecallResponse {
        items: vec![episodic_item(
            "current_task_summary",
            MemoryEpisodicRecallSourceKind::CurrentTask,
            "Current task is implementing Phase 19 episodic recall.",
        )],
        ..MemoryEpisodicRecallResponse::default()
    };
    episodic.completed_tasks = MemoryEpisodicRecallResponse {
        items: vec![episodic_item(
            "completed_task_summary",
            MemoryEpisodicRecallSourceKind::CompletedTask,
            "Completed task found the prompt separation requirement.",
        )],
        ..MemoryEpisodicRecallResponse::default()
    };
    let episodic = Arc::new(episodic);

    let result = execute_active_recall_plan(
        durable_provider.as_ref(),
        ActiveRecallExecutionInput {
            context: task_memory_turn_context(),
            plan: ActiveRecallPlan::run(
                ActiveMemoryDecisionReasonCode::ProviderRun,
                0.9,
                vec![
                    ActiveRecallMode::CurrentTask,
                    ActiveRecallMode::CompletedTask,
                ],
                Vec::new(),
                Vec::new(),
            ),
            deterministic: DeterministicRecallContextSummary::default(),
            config: MemoryActiveRecallConfig {
                max_queries: 2,
                ..MemoryActiveRecallConfig::default()
            },
            episodic_provider: Some(episodic.clone()),
            episodic_capabilities: MemoryEpisodicRecallCapabilities {
                current_task_context: true,
                completed_task_summary: true,
                ..MemoryEpisodicRecallCapabilities::default()
            },
        },
    )
    .await;

    assert_eq!(durable_provider.recall_call_count(), 0);
    assert_eq!(episodic.calls(), vec!["current_task", "completed_tasks"]);
    assert_eq!(result.episodic_items.len(), 2);
    assert!(
        result
            .episodic_items
            .iter()
            .any(|item| item.provenance.source == MemoryEpisodicRecallSourceKind::CurrentTask)
    );
    assert!(
        result
            .episodic_items
            .iter()
            .any(|item| item.provenance.source == MemoryEpisodicRecallSourceKind::CompletedTask)
    );
}

#[tokio::test]
async fn episodic_provider_timeout_skips_mode_without_turn_failure() {
    let durable_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        MemoryRecallSnapshot::empty(),
    ));

    let result = execute_active_recall_plan(
        durable_provider.as_ref(),
        ActiveRecallExecutionInput {
            context: test_memory_turn_context(),
            plan: ActiveRecallPlan::run(
                ActiveMemoryDecisionReasonCode::ProviderRun,
                0.9,
                vec![ActiveRecallMode::CurrentThread],
                Vec::new(),
                Vec::new(),
            ),
            deterministic: DeterministicRecallContextSummary::default(),
            config: MemoryActiveRecallConfig {
                timeout_ms: 1,
                ..MemoryActiveRecallConfig::default()
            },
            episodic_provider: Some(Arc::new(SlowEpisodicRecallProvider)),
            episodic_capabilities: MemoryEpisodicRecallCapabilities {
                current_thread_search: true,
                ..MemoryEpisodicRecallCapabilities::default()
            },
        },
    )
    .await;

    assert!(result.items.is_empty());
    assert!(result.episodic_items.is_empty());
    assert!(result.diagnostics.iter().any(|diagnostic| {
        diagnostic.contains("memory.episodic_recall.mode_timed_out:current_thread")
    }));
}

#[tokio::test]
async fn prompt_contract_separates_durable_thread_and_task_context() {
    let durable = memory_active_recall_prompt_context_contribution(
        vec![MemoryRecallItem {
            memory_id: "mem_project".to_owned(),
            scope: MemoryScope {
                kind: MemoryScopeKind::Workspace,
                key: "ws".to_owned(),
            },
            category: MemoryCategory::ProjectDecision,
            key: Some("memory_architecture".to_owned()),
            content: "Use hook runtime for memory domains.".to_owned(),
            score: Some(0.9),
            updated_at: 42,
        }],
        false,
        &MemoryActiveRecallConfig::default(),
    )
    .expect("durable active contribution");
    let episodic = memory_episodic_recall_prompt_context_contributions(
        vec![
            episodic_item(
                "thread_snippet_1",
                MemoryEpisodicRecallSourceKind::CurrentThread,
                "Thread context says Proposal 09 owns transcript indexing.",
            ),
            episodic_item(
                "task_summary_1",
                MemoryEpisodicRecallSourceKind::CompletedTask,
                "Task summary says source_kind removal is complete.",
            ),
        ],
        false,
        &MemoryActiveRecallConfig::default(),
    );
    let prompt_context_set = HookPromptContextSet::aggregate_contributions(
        std::iter::once(durable).chain(episodic).collect::<Vec<_>>(),
        HookPromptContextLimits::default(),
    );

    let response = MemoryPromptContractHook
        .execute(test_prompt_compile_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            true,
            &["memory_search"],
            prompt_context_set,
        ))
        .await
        .expect("prompt contract executes");
    let content = prompt_section_content(response).expect("prompt section renders");

    assert!(content.contains("Additional active memory context for this turn:"));
    assert!(content.contains("Relevant thread context for this turn:"));
    assert!(content.contains("Relevant task context for this turn:"));
    assert!(content.contains("Use hook runtime for memory domains."));
    assert!(content.contains("Proposal 09 owns transcript indexing."));
    assert!(content.contains("source_kind removal is complete."));
    assert!(!content.contains("Relevant memory context for this turn:\n- current thread"));
}
