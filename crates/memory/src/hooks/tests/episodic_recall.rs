use super::*;

#[derive(Default)]
struct FakeEpisodicRecallProvider {
    capabilities: MemoryEpisodicRecallCapabilities,
    current_thread: MemoryEpisodicRecallResponse,
    related_threads: MemoryEpisodicRecallResponse,
    workspace_threads: MemoryEpisodicRecallResponse,
    current_task: MemoryEpisodicRecallResponse,
    completed_tasks: MemoryEpisodicRecallResponse,
    calls: Arc<Mutex<Vec<&'static str>>>,
    queries: Arc<Mutex<Vec<String>>>,
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

    fn queries(&self) -> Vec<String> {
        self.queries.lock().expect("queries lock poisoned").clone()
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
        request: MemoryCurrentThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        self.calls
            .lock()
            .expect("calls lock poisoned")
            .push("current_thread");
        self.queries
            .lock()
            .expect("queries lock poisoned")
            .push(request.query);
        Ok(self.current_thread.clone())
    }

    async fn recall_related_threads(
        &self,
        request: MemoryRelatedThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        self.calls
            .lock()
            .expect("calls lock poisoned")
            .push("related_threads");
        self.queries
            .lock()
            .expect("queries lock poisoned")
            .push(request.query);
        Ok(self.related_threads.clone())
    }

    async fn recall_workspace_threads(
        &self,
        request: MemoryWorkspaceThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        self.calls
            .lock()
            .expect("calls lock poisoned")
            .push("workspace_threads");
        self.queries
            .lock()
            .expect("queries lock poisoned")
            .push(request.query);
        Ok(self.workspace_threads.clone())
    }

    async fn recall_current_task(
        &self,
        request: MemoryCurrentTaskRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        self.calls
            .lock()
            .expect("calls lock poisoned")
            .push("current_task");
        self.queries
            .lock()
            .expect("queries lock poisoned")
            .push(request.query);
        Ok(self.current_task.clone())
    }

    async fn recall_completed_tasks(
        &self,
        request: MemoryCompletedTaskRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        self.calls
            .lock()
            .expect("calls lock poisoned")
            .push("completed_tasks");
        self.queries
            .lock()
            .expect("queries lock poisoned")
            .push(request.query);
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
                    MemoryEpisodicRecallSourceKind::WorkspaceThread => "workspace_thr",
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
        deterministic_recall_empty: true,
        deterministic_categories: Vec::new(),
        typed_targets: Vec::new(),
        has_workspace_context: true,
        has_task_context: false,
        episodic_capabilities: MemoryEpisodicRecallCapabilities::default(),
        thread_episodic: MemoryActiveRecallThreadEpisodicSummary::default(),
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
        r#"{"durable":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"modes":[],"targets":[]},"episodic":{"status":"run","reasonCode":"provider_run","confidence":0.9,"queries":[{"mode":"current_thread","query":"continue what we discussed earlier","targets":[]}]}}"#,
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
            .contains("dropped_query=thread_episodic:capability_unavailable")
    }));
}

#[test]
fn active_recall_planner_parses_new_episodic_modes_and_validates_capabilities() {
    let plan = parse_active_memory_decision_json(
        r#"{"durable":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"modes":[],"targets":[]},"episodic":{"status":"run","reasonCode":"provider_run","confidence":0.9,"queries":[{"mode":"current_thread","query":"current thread context","targets":[]},{"mode":"related_threads","query":"related thread context","targets":[]},{"mode":"workspace_threads","query":"workspace thread context","targets":[]},{"mode":"current_task","query":"current task context","targets":[]}]}}"#,
    )
    .expect("provider plan parses");
    assert_eq!(
        plan.selected_modes(),
        vec![
            ActiveRecallMode::CurrentTask,
            ActiveRecallMode::CurrentThread,
            ActiveRecallMode::RelatedThread,
            ActiveRecallMode::WorkspaceThread
        ]
    );
    let completed_task_plan = parse_active_memory_decision_json(
        r#"{"durable":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"modes":[],"targets":[]},"episodic":{"status":"run","reasonCode":"provider_run","confidence":0.9,"queries":[{"mode":"completed_task","query":"completed task context","targets":[]}]}}"#,
    )
    .expect("completed task mode parses");
    assert_eq!(
        completed_task_plan.selected_modes(),
        vec![ActiveRecallMode::CompletedTask]
    );

    let mut input = episodic_planner_input_for_test();
    input.task_id = Some("task_1".to_owned());
    input.has_task_context = true;
    input.episodic_capabilities.current_thread_search = true;
    input.episodic_capabilities.current_task_context = true;
    let normalized = normalize_active_recall_plan_for_input(plan, &input);

    assert_eq!(normalized.durable.modes, Vec::<ActiveRecallMode>::new());
    assert_eq!(
        normalized.selected_modes(),
        vec![
            ActiveRecallMode::CurrentTask,
            ActiveRecallMode::CurrentThread
        ]
    );
    assert_eq!(normalized.episodic.queries.len(), 2);
    assert!(
        normalized.all_diagnostics().iter().any(|diagnostic| {
            diagnostic == "dropped_query=related_thread:capability_unavailable"
        })
    );
    assert!(normalized.all_diagnostics().iter().any(|diagnostic| {
        diagnostic == "dropped_query=workspace_thread:capability_unavailable"
    }));
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
            "thread:turn_1/item_1/chunk_0",
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
        r#"{"durable":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"modes":[],"targets":[]},"episodic":{"status":"run","reasonCode":"provider_run","confidence":0.9,"queries":[{"mode":"current_thread","query":"continue what we discussed earlier","targets":[]}]}}"#,
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
    assert_eq!(contributions[0].domain.as_str(), "current_thread_context");
    assert!(
        contributions[0]
            .content
            .as_str()
            .contains("Earlier in this thread, the user rejected phrase-list memory policy.")
    );
    assert!(
        contributions[0]
            .content
            .as_str()
            .contains("thread:turn_1/item_1/chunk_0")
    );
    assert_eq!(
        contributions[0].source_refs[0].kind.as_str(),
        "current_thread_context"
    );
}

#[tokio::test]
async fn current_thread_recall_uses_envelope_planned_query_instead_of_raw_turn_text() {
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
            "thread:turn_1/item_1/chunk_0",
            MemoryEpisodicRecallSourceKind::CurrentThread,
            "Earlier in this thread, the user asked for tomorrow weather in Moscow.",
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
        r#"{
            "durable": {
                "status": "skip",
                "reasonCode": "provider_skip",
                "confidence": 1.0,
                "modes": [],
                "targets": []
            },
            "episodic": {
                "status": "run",
                "reasonCode": "provider_run",
                "confidence": 0.92,
                "queries": [
                    {
                        "mode": "current_thread",
                        "query": "прогноз погоды в Москве завтра",
                        "targets": [],
                        "topK": 3,
                        "maxChars": 700
                    }
                ]
            },
            "diagnostics": ["continuation_query_planned"]
        }"#,
    )
    .expect("provider-owned envelope plan parses");
    let input = TurnPostPreflightPromptContextHookInput::from_parts(
        "а завтра какая?",
        Some("test-model"),
        Some("test-provider"),
    )
    .with_active_memory_recall_preflight_plan(
        serde_json::to_value(plan).expect("active recall plan serializes"),
    );
    let mut request = test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "а завтра какая?",
    );
    request.input = HookInput::turn_post_preflight_prompt_context(input);

    let response = hook
        .execute(request)
        .await
        .expect("active recall hook executes");

    assert_eq!(durable_provider.recall_call_count(), 0);
    assert_eq!(episodic.calls(), vec!["current_thread"]);
    assert_eq!(episodic.queries(), vec!["прогноз погоды в Москве завтра"]);
    assert!(!prompt_context_contributions(&response).is_empty());
}

#[tokio::test]
async fn active_recall_hook_keeps_cross_thread_prompt_domains_separate() {
    let durable_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        MemoryRecallSnapshot::empty(),
    ));
    let mut episodic =
        FakeEpisodicRecallProvider::with_capabilities(MemoryEpisodicRecallCapabilities {
            current_thread_search: true,
            related_thread_search: true,
            workspace_thread_search: true,
            ..MemoryEpisodicRecallCapabilities::default()
        });
    episodic.current_thread = MemoryEpisodicRecallResponse {
        items: vec![episodic_item(
            "current_thread_item",
            MemoryEpisodicRecallSourceKind::CurrentThread,
            "Current thread says keep the local turn context.",
        )],
        ..MemoryEpisodicRecallResponse::default()
    };
    episodic.related_threads = MemoryEpisodicRecallResponse {
        items: vec![episodic_item(
            "related_thread_item",
            MemoryEpisodicRecallSourceKind::RelatedThread,
            "Related thread says use bounded cross-thread recall.",
        )],
        ..MemoryEpisodicRecallResponse::default()
    };
    episodic.workspace_threads = MemoryEpisodicRecallResponse {
        items: vec![episodic_item(
            "workspace_thread_item",
            MemoryEpisodicRecallSourceKind::WorkspaceThread,
            "Workspace thread says broad recall requires explicit planner intent.",
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
        r#"{"durable":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"modes":[],"targets":[]},"episodic":{"status":"run","reasonCode":"provider_run","confidence":0.9,"queries":[{"mode":"current_thread","query":"current thread context","targets":[]},{"mode":"related_thread","query":"related thread context","targets":[]},{"mode":"workspace_thread","query":"workspace thread context","targets":[]}]}}"#,
    )
    .expect("provider-owned cross-thread plan parses");
    let input = TurnPostPreflightPromptContextHookInput::from_parts(
        "continue using relevant thread context",
        Some("test-model"),
        Some("test-provider"),
    )
    .with_active_memory_recall_preflight_plan(
        serde_json::to_value(plan).expect("active recall plan serializes"),
    );
    let mut request = test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "continue using relevant thread context",
    );
    request.input = HookInput::turn_post_preflight_prompt_context(input);

    let response = hook
        .execute(request)
        .await
        .expect("active recall hook executes");

    assert_eq!(durable_provider.recall_call_count(), 0);
    assert_eq!(
        episodic.calls(),
        vec!["current_thread", "related_threads", "workspace_threads"]
    );
    let contributions = prompt_context_contributions(&response);
    let contribution_ids = contributions
        .iter()
        .map(|contribution| contribution.contribution_id.as_str())
        .collect::<Vec<_>>();
    assert!(contribution_ids.contains(&MEMORY_THREAD_CONTEXT_CONTRIBUTION_ID));
    assert!(contribution_ids.contains(&MEMORY_RELATED_THREAD_CONTEXT_CONTRIBUTION_ID));
    assert!(contribution_ids.contains(&MEMORY_WORKSPACE_THREAD_CONTEXT_CONTRIBUTION_ID));
    let domains = contributions
        .iter()
        .map(|contribution| contribution.domain.as_str())
        .collect::<Vec<_>>();
    assert!(domains.contains(&"current_thread_context"));
    assert!(domains.contains(&"related_thread_context"));
    assert!(domains.contains(&"workspace_thread_context"));
    assert!(contributions.iter().any(|contribution| {
        contribution
            .source_refs
            .iter()
            .any(|source_ref| source_ref.id.as_str() == "related_thread_item")
    }));
    assert!(contributions.iter().any(|contribution| {
        contribution
            .source_refs
            .iter()
            .any(|source_ref| source_ref.id.as_str() == "workspace_thread_item")
    }));
}

#[tokio::test]
async fn invalid_preflight_plan_falls_back_to_current_thread_recall_when_available() {
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
            "thread:turn_1/item_1/chunk_1",
            MemoryEpisodicRecallSourceKind::CurrentThread,
            "Earlier in this thread, the user decided to keep thread context separate.",
        )],
        ..MemoryEpisodicRecallResponse::default()
    };
    let episodic = Arc::new(episodic);
    let hook = ActiveMemoryRecallHook {
        memory_provider: durable_provider,
        episodic_provider: Some(episodic.clone()),
        config: MemoryActiveRecallConfig::default(),
    };
    let input = TurnPostPreflightPromptContextHookInput::from_parts(
        "continue what we discussed earlier",
        Some("test-model"),
        Some("test-provider"),
    )
    .with_active_memory_recall_preflight_plan(json!({
        "durable": {
            "status": "skip",
            "reasonCode": "provider_skip",
            "confidence": 1.0,
            "modes": [],
            "targets": []
        },
        "episodic": {
            "status": "run",
            "reasonCode": "provider_run",
            "confidence": 0.9,
            "queries": []
        }
    }));
    let mut request = test_active_prompt_context_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        HookPromptContextSet::default(),
        "continue what we discussed earlier",
    );
    request.input = HookInput::turn_post_preflight_prompt_context(input);

    let response = hook
        .execute(request)
        .await
        .expect("invalid preflight plan falls back");

    assert_eq!(episodic.calls(), vec!["current_thread"]);
    let contributions = prompt_context_contributions(&response);
    assert_eq!(contributions.len(), 1);
    assert_eq!(
        contributions[0].contribution_id.as_str(),
        MEMORY_THREAD_CONTEXT_CONTRIBUTION_ID
    );
    assert_eq!(contributions[0].domain.as_str(), "current_thread_context");
    assert!(
        contributions[0]
            .content
            .as_str()
            .contains("thread:turn_1/item_1/chunk_1")
    );
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.active_recall.preflight_plan_invalid"
    }));
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
async fn full_input_episodic_recall_uses_turn_input_when_planned_query_is_omitted() {
    let durable_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        MemoryRecallSnapshot::empty(),
    ));
    let episodic = Arc::new(FakeEpisodicRecallProvider::with_capabilities(
        MemoryEpisodicRecallCapabilities {
            current_thread_search: true,
            full_input_query: true,
            ..MemoryEpisodicRecallCapabilities::default()
        },
    ));

    let result = execute_active_recall_plan(
        durable_provider.as_ref(),
        ActiveRecallExecutionInput {
            context: test_memory_turn_context(),
            plan: ActiveMemoryRecallPlan {
                durable: DurableMemoryRecallPlan::skip(
                    ActiveMemoryDecisionReasonCode::ProviderSkip,
                    1.0,
                    Vec::new(),
                ),
                episodic: EpisodicMemoryRecallPlan::run(
                    ActiveMemoryDecisionReasonCode::ProviderRun,
                    0.9,
                    vec![EpisodicMemoryRecallQuery {
                        mode: ActiveRecallMode::CurrentThread,
                        query: None,
                        targets: Vec::new(),
                        top_k: None,
                        max_chars: None,
                    }],
                    Vec::new(),
                ),
                diagnostics: Vec::new(),
            },
            deterministic: DeterministicRecallContextSummary::default(),
            config: MemoryActiveRecallConfig::default(),
            episodic_provider: Some(episodic.clone()),
            episodic_capabilities: MemoryEpisodicRecallCapabilities {
                current_thread_search: true,
                full_input_query: true,
                ..MemoryEpisodicRecallCapabilities::default()
            },
        },
    )
    .await;

    assert_eq!(durable_provider.recall_call_count(), 0);
    assert_eq!(episodic.calls(), vec!["current_thread"]);
    assert_eq!(episodic.queries(), vec!["remember my preference"]);
    assert!(result.items.is_empty());
}

#[tokio::test]
async fn workspace_thread_recall_uses_workspace_provider_mode_and_boundaries() {
    let durable_provider = Arc::new(TestRecallMemoryProvider::with_recall(
        MemoryRecallSnapshot::empty(),
    ));
    let mut hidden = episodic_item(
        "hidden_workspace",
        MemoryEpisodicRecallSourceKind::WorkspaceThread,
        "Hidden workspace thread content.",
    );
    hidden.visibility = MemoryEpisodicRecallVisibility::Hidden;
    let mut other_workspace = episodic_item(
        "other_workspace_thread",
        MemoryEpisodicRecallSourceKind::WorkspaceThread,
        "Other workspace thread content.",
    );
    other_workspace.provenance.workspace_id = "other_ws".to_owned();
    let visible = episodic_item(
        "visible_workspace",
        MemoryEpisodicRecallSourceKind::WorkspaceThread,
        "Workspace thread recall is explicitly planned and bounded.",
    );
    let mut episodic =
        FakeEpisodicRecallProvider::with_capabilities(MemoryEpisodicRecallCapabilities {
            workspace_thread_search: true,
            ..MemoryEpisodicRecallCapabilities::default()
        });
    episodic.workspace_threads = MemoryEpisodicRecallResponse {
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
                vec![ActiveRecallMode::WorkspaceThread],
                Vec::new(),
                Vec::new(),
            ),
            deterministic: DeterministicRecallContextSummary::default(),
            config: MemoryActiveRecallConfig::default(),
            episodic_provider: Some(episodic.clone()),
            episodic_capabilities: MemoryEpisodicRecallCapabilities {
                workspace_thread_search: true,
                ..MemoryEpisodicRecallCapabilities::default()
            },
        },
    )
    .await;

    assert_eq!(durable_provider.recall_call_count(), 0);
    assert_eq!(episodic.calls(), vec!["workspace_threads"]);
    assert_eq!(result.episodic_items.len(), 1);
    assert_eq!(result.episodic_items[0].id, "visible_workspace");
    assert!(result.diagnostics.iter().any(|diagnostic| {
        diagnostic.contains("memory.episodic_recall.filtered_count:workspace_thread:2")
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
                "related_snippet_1",
                MemoryEpisodicRecallSourceKind::RelatedThread,
                "Related thread context says cross-thread recall must stay bounded.",
            ),
            episodic_item(
                "workspace_snippet_1",
                MemoryEpisodicRecallSourceKind::WorkspaceThread,
                "Workspace thread context says broad recall requires explicit intent.",
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
    let episodic_domains = episodic
        .iter()
        .map(|contribution| contribution.domain.as_str())
        .collect::<Vec<_>>();
    assert!(episodic_domains.contains(&"current_thread_context"));
    assert!(episodic_domains.contains(&"related_thread_context"));
    assert!(episodic_domains.contains(&"workspace_thread_context"));
    assert!(episodic_domains.contains(&"task_context"));
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
    let sections = prompt_section_contents(response);
    let memory_content = sections
        .iter()
        .find(|(section_id, _)| section_id == "memory_recall")
        .map(|(_, content)| content.as_str())
        .expect("memory prompt section renders");
    let thread_content = sections
        .iter()
        .find(|(section_id, _)| section_id == "thread_context")
        .map(|(_, content)| content.as_str())
        .expect("thread context prompt section renders");

    assert!(memory_content.contains("Additional active memory context for this turn:"));
    assert!(memory_content.contains("Relevant task context for this turn:"));
    assert!(memory_content.contains("Use hook runtime for memory domains."));
    assert!(memory_content.contains("source_kind removal is complete."));
    assert!(!memory_content.contains("Relevant thread context for this turn:"));
    assert!(!memory_content.contains("Proposal 09 owns transcript indexing."));
    assert!(!memory_content.contains("cross-thread recall must stay bounded."));
    assert!(!memory_content.contains("broad recall requires explicit intent."));
    assert!(thread_content.contains("Thread context is recalled conversation context"));
    assert!(thread_content.contains("Relevant thread context:"));
    assert!(thread_content.contains("Proposal 09 owns transcript indexing."));
    assert!(thread_content.contains("cross-thread recall must stay bounded."));
    assert!(thread_content.contains("broad recall requires explicit intent."));
    assert!(!thread_content.contains("Relevant memory context for this turn:"));
}
