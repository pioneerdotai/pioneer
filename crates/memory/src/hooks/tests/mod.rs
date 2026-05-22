mod classifier_fallback;
mod descriptors;
mod eligibility;
mod episodic_recall;
mod policy_basics;
mod policy_classifier;
mod post_turn;
mod prompt_dto;
mod recall_prompt;
mod tool_bundle;

use super::*;
use pioneer_hooks::{
    HookContext, HookInput, HookPhaseRequest, HookPromptContextLimits, HookPromptContextSet,
    HookRegistry, HookRuntime, HookRuntimeBuilder, HookSubscriptionRegistry, HookThreadId,
    HookToolName, HookTurnId, HookWorkspaceId, TurnPostPreflightPromptContextHookInput,
    TurnPrePromptCompileHookInput, TurnPrePromptContextHookInput,
    TurnPreToolMaterializationHookInput,
};
use pioneer_protocol::{MemoryAttribute, MemoryCanonicalKey, MemoryRecord, MemorySubject};
use pioneer_tools::{ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolSpec};
use serde_json::json;

#[derive(Default)]
struct TestToolBundleArtifactStore;

impl TestToolBundleArtifactStore {
    fn new() -> Self {
        Self
    }
}

impl MemoryToolBundleArtifactStore for TestToolBundleArtifactStore {
    fn insert_tool_bundle_artifact(
        &self,
        _turn_id: &str,
        _bundle_id: HookToolBundleId,
        _bundle: ToolExtensionBundle,
    ) {
    }
}

fn install_memory_hook_package_for_test(
    runtime: &Arc<HookRuntime>,
    memory_provider: Arc<dyn AgentMemoryProvider>,
    memory_write_provider: Option<Arc<dyn AgentMemoryWriteProvider>>,
    post_turn_extractor_provider: Option<Arc<dyn AgentMemoryPostTurnExtractorProvider>>,
    policy_provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    episodic_recall_provider: Option<Arc<dyn AgentEpisodicRecallProvider>>,
    tool_bundle_artifacts: Arc<dyn MemoryToolBundleArtifactStore>,
    memory_config: MemoryLoopConfig,
) -> Result<(), HookRegistryError> {
    let _runtime = HookRuntimeBuilder::from_runtime(runtime.as_ref())
        .install(package(
            memory_provider,
            memory_write_provider,
            post_turn_extractor_provider,
            policy_provider,
            episodic_recall_provider,
            tool_bundle_artifacts,
            memory_config,
        ))?
        .build();
    Ok(())
}

struct SingleDefinitionHookPackage {
    handler: Arc<dyn HookHandler>,
    subscription: HookSubscription,
}

impl HookPackage for SingleDefinitionHookPackage {
    fn id(&self) -> &'static str {
        "test.memory.single_definition"
    }

    fn definitions(&self) -> Result<Vec<HookDefinition>, HookRegistryError> {
        Ok(vec![HookDefinition::new(
            self.handler.clone(),
            [self.subscription.clone()],
            "test.memory",
        )])
    }
}

fn install_single_hook_definition_for_test(
    runtime: &Arc<HookRuntime>,
    handler: Arc<dyn HookHandler>,
    subscription: HookSubscription,
) -> Result<(), HookRegistryError> {
    let _runtime = HookRuntimeBuilder::from_runtime(runtime.as_ref())
        .install(SingleDefinitionHookPackage {
            handler,
            subscription,
        })?
        .build();
    Ok(())
}

fn user_scope() -> MemoryScope {
    MemoryScope {
        kind: MemoryScopeKind::User,
        key: "global".to_owned(),
    }
}

fn test_policy_hook_request(metadata: HookMetadata) -> HookHandlerRequest {
    HookHandlerRequest {
        hook_id: HookId::new(MEMORY_POLICY_CLASSIFIER_HOOK_ID).expect("static hook id is valid"),
        phase: HookPhase::TurnPrePolicy,
        context: HookContext {
            workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
            thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
            turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
            metadata,
            ..HookContext::default()
        },
        input: HookInput::turn_pre_policy(TurnPrePolicyHookInput::from_parts(
            "No guardes esto.",
            Some("test-model"),
            Some("test-provider"),
        )),
        policy_set: HookPolicySet::empty(),
        prompt_context_set: HookPromptContextSet::default(),
    }
}

fn test_tool_bundle_hook_request(
    policy_set: HookPolicySet,
    provider_tool_calling: bool,
) -> HookHandlerRequest {
    HookHandlerRequest {
        hook_id: HookId::new(MEMORY_TOOL_BUNDLE_HOOK_ID).expect("static hook id is valid"),
        phase: HookPhase::TurnPreToolMaterialization,
        context: HookContext {
            workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
            thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
            turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
            ..HookContext::default()
        },
        input: HookInput::turn_pre_tool_materialization(
            TurnPreToolMaterializationHookInput::from_parts(provider_tool_calling, Vec::new()),
        ),
        policy_set,
        prompt_context_set: HookPromptContextSet::default(),
    }
}

fn test_prompt_context_hook_request(policy_set: HookPolicySet) -> HookHandlerRequest {
    HookHandlerRequest {
        hook_id: HookId::new(MEMORY_DETERMINISTIC_RECALL_HOOK_ID).expect("static hook id is valid"),
        phase: HookPhase::TurnPrePromptContext,
        context: HookContext {
            workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
            thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
            turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
            ..HookContext::default()
        },
        input: HookInput::turn_pre_prompt_context(TurnPrePromptContextHookInput::from_parts(
            "what do you remember about my city?",
            Some("test-model"),
            Some("test-provider"),
        )),
        policy_set,
        prompt_context_set: HookPromptContextSet::default(),
    }
}

fn test_active_prompt_context_hook_request(
    policy_set: HookPolicySet,
    prompt_context_set: HookPromptContextSet,
    input_text: &str,
) -> HookHandlerRequest {
    HookHandlerRequest {
        hook_id: HookId::new(MEMORY_ACTIVE_RECALL_HOOK_ID).expect("static hook id is valid"),
        phase: HookPhase::TurnPostPreflightPromptContext,
        context: HookContext {
            workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
            thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
            turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
            ..HookContext::default()
        },
        input: HookInput::turn_post_preflight_prompt_context(
            TurnPostPreflightPromptContextHookInput::from_parts(
                input_text,
                Some("test-model"),
                Some("test-provider"),
            ),
        ),
        policy_set,
        prompt_context_set,
    }
}

fn test_prompt_compile_hook_request(
    policy_set: HookPolicySet,
    provider_tool_calling: bool,
    available_tool_names: &[&str],
    prompt_context_set: HookPromptContextSet,
) -> HookHandlerRequest {
    HookHandlerRequest {
        hook_id: HookId::new(MEMORY_PROMPT_CONTRACT_HOOK_ID).expect("static hook id is valid"),
        phase: HookPhase::TurnPrePromptCompile,
        context: HookContext {
            workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
            thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
            turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
            ..HookContext::default()
        },
        input: HookInput::turn_pre_prompt_compile(TurnPrePromptCompileHookInput::from_parts(
            provider_tool_calling,
            available_tool_names
                .iter()
                .map(|name| HookToolName::new(*name).expect("valid tool name"))
                .collect(),
        )),
        policy_set,
        prompt_context_set,
    }
}

fn test_post_turn_hook_request(
    policy_set: HookPolicySet,
    user_text: &str,
    assistant_text: &str,
) -> HookHandlerRequest {
    test_post_turn_hook_request_with_events(
        policy_set,
        Some(user_text),
        Some(assistant_text),
        Vec::new(),
        Vec::new(),
    )
}

fn test_post_turn_hook_request_with_events(
    policy_set: HookPolicySet,
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
        policy_set,
        prompt_context_set: HookPromptContextSet::default(),
    }
}

fn test_post_turn_tool_event() -> pioneer_hooks::TurnPostTurnToolEventSummary {
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

fn test_post_turn_domain_event(
    domain: pioneer_hooks::TurnPostTurnDomain,
) -> pioneer_hooks::TurnPostTurnDomainEventSummary {
    pioneer_hooks::TurnPostTurnDomainEventSummary {
        domain,
        code: Some("completed".to_owned()),
        item_id: Some("domain-item".to_owned()),
        message: None,
    }
}

fn memory_policy_set(policy: &MemoryTurnPolicy) -> HookPolicySet {
    HookPolicySet::merge_contributions([memory_policy_contribution(policy)])
}

fn malformed_memory_policy_set() -> HookPolicySet {
    HookPolicySet::merge_contributions([PolicyContribution {
        domain: memory_policy_domain(),
        key: memory_turn_policy_key(),
        value: HookValue::Text("memory_no_use".to_owned()),
        priority: 500,
        diagnostics: Vec::new(),
    }])
}

fn recalled_city_snapshot() -> MemoryRecallSnapshot {
    MemoryRecallSnapshot {
        items: vec![MemoryRecallItem {
            memory_id: "mem_city".to_owned(),
            scope: user_scope(),
            category: MemoryCategory::Preference,
            key: Some("city".to_owned()),
            content: "User likes Porto.".to_owned(),
            score: Some(0.91),
            updated_at: 1_714_867_200,
        }],
        diagnostics: Vec::new(),
        truncated: false,
    }
}

fn active_project_snapshot() -> MemoryRecallSnapshot {
    MemoryRecallSnapshot {
        items: vec![MemoryRecallItem {
            memory_id: "mem_active_project".to_owned(),
            scope: user_scope(),
            category: MemoryCategory::ProjectDecision,
            key: Some("hooks".to_owned()),
            content: "Use hooks for memory domains.".to_owned(),
            score: Some(0.88),
            updated_at: 1_714_867_200,
        }],
        diagnostics: Vec::new(),
        truncated: false,
    }
}

fn valid_post_turn_extractor_json() -> String {
    serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "content": "Имя пользователя: Александр",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Меня зовут Александр",
                "extractor_reason": "The user directly stated their name."
            },
            "confidence": 0.98,
            "importance": 0.7
        }]
    })
    .to_string()
}

fn valid_post_turn_extractor_json_with_ontology() -> String {
    serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "ontology": {
                "fact_class": "user_identity",
                "lifetime_class": "long_lived",
                "evidence_class": "direct_user_assertion",
                "proposed_ownership_class": "durable_user_memory"
            },
            "content": "Имя пользователя: Александр",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Меня зовут Александр",
                "extractor_reason": "The user directly stated their name."
            },
            "confidence": 0.98,
            "importance": 0.7
        }]
    })
    .to_string()
}

fn implicit_post_turn_extractor_json() -> String {
    serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "implicit_candidate",
                "explicitness": "implicit",
                "category": "communication_style",
                "subject": "current_user",
                "attribute": "communication_style",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "low",
                "certainty": "high"
            },
            "content": "Пользователь предпочитает лаконичный стиль ответов.",
            "value": "лаконичный стиль ответов",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Мне нравится лаконичный стиль ответов.",
                "extractor_reason": "The user stated a stable communication preference."
            },
            "confidence": 0.9,
            "importance": 0.6
        }]
    })
    .to_string()
}

fn prompt_context_set_from_response(response: HookHandlerResponse) -> HookPromptContextSet {
    HookPromptContextSet::aggregate_hook_contributions(
        response.contributions,
        HookPromptContextLimits::default(),
    )
}

fn prompt_context_contributions(response: &HookHandlerResponse) -> Vec<&PromptContextContribution> {
    response
        .contributions
        .iter()
        .filter_map(|contribution| match contribution {
            HookContribution::PromptContext(context) => Some(context),
            _ => None,
        })
        .collect()
}

fn assert_no_prompt_context_contributions(response: &HookHandlerResponse) {
    assert!(
        prompt_context_contributions(response).is_empty(),
        "expected no prompt context contributions, got {:?}",
        response.contributions
    );
}

fn assert_has_memory_recall_audit(response: &HookHandlerResponse, event_kind: &str) {
    assert!(
        response.contributions.iter().any(|contribution| {
            matches!(
                contribution,
                HookContribution::Audit(audit) if audit.event_kind.as_str() == event_kind
            )
        }),
        "expected memory recall audit contribution `{event_kind}`, got {:?}",
        response.contributions
    );
}

fn prompt_context_set_from_prompt_context_contribution(
    contribution: PromptContextContribution,
) -> HookPromptContextSet {
    HookPromptContextSet::aggregate_contributions(
        [contribution],
        HookPromptContextLimits::default(),
    )
}

fn memory_source_ref(memory_id: &str) -> HookSourceRef {
    HookSourceRef {
        kind: HookSourceKind::Custom("memory".to_owned()),
        id: HookSourceId::new(memory_id.to_owned()).expect("valid memory source id"),
        label: None,
    }
}

fn prompt_section_content(response: HookHandlerResponse) -> Option<String> {
    response.contributions.into_iter().find_map(|contribution| {
        let HookContribution::PromptSection(section) = contribution else {
            return None;
        };
        Some(section.content.as_str().to_owned())
    })
}

fn test_memory_turn_context() -> MemoryTurnContext {
    MemoryTurnContext {
        workspace_id: "ws".to_owned(),
        thread_id: "thr".to_owned(),
        turn_id: "turn".to_owned(),
        mode: ThreadMode::Agent,
        input_text: "remember my preference".to_owned(),
        task_id: None,
        agent_id: None,
    }
}

fn test_tool_spec(name: &str) -> ConfiguredToolSpec {
    ConfiguredToolSpec::new(
        ToolSpec::new(
            name,
            "test memory tool",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            PayloadKind::Function,
        ),
        ExecutionClass::Shared,
        pioneer_tools::dynamic_unknown_output_policy(),
    )
}

fn test_memory_tool_bundle(names: &[&str]) -> ToolExtensionBundle {
    ToolExtensionBundle {
        specs: names.iter().map(|name| test_tool_spec(name)).collect(),
        handlers: names
            .iter()
            .map(|name| {
                (
                    (*name).to_owned(),
                    Arc::new(TestToolHandler) as Arc<dyn pioneer_tools::ToolHandler>,
                )
            })
            .collect(),
    }
}

fn empty_tool_materialization() -> MemoryToolMaterialization {
    MemoryToolMaterialization {
        bundles: Vec::new(),
        diagnostics: Vec::new(),
    }
}

fn standard_tool_materialization() -> MemoryToolMaterialization {
    MemoryToolMaterialization {
        bundles: vec![test_memory_tool_bundle(&[
            MEMORY_SEARCH_TOOL,
            MEMORY_LIST_TOOL,
            MEMORY_GET_TOOL,
            MEMORY_REMEMBER_TOOL,
            MEMORY_FORGET_TOOL,
        ])],
        diagnostics: Vec::new(),
    }
}

fn panicking_handler_tool_materialization() -> MemoryToolMaterialization {
    let handler: Arc<dyn pioneer_tools::ToolHandler> = Arc::new(PanickingToolHandler);
    MemoryToolMaterialization {
        bundles: vec![ToolExtensionBundle {
            specs: [
                MEMORY_SEARCH_TOOL,
                MEMORY_LIST_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_FORGET_TOOL,
            ]
            .into_iter()
            .map(test_tool_spec)
            .collect(),
            handlers: [
                MEMORY_SEARCH_TOOL,
                MEMORY_LIST_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_FORGET_TOOL,
            ]
            .into_iter()
            .map(|name| (name.to_owned(), handler.clone()))
            .collect(),
        }],
        diagnostics: Vec::new(),
    }
}

fn response_tool_names(response: &HookHandlerResponse) -> Vec<&'static str> {
    response
        .contributions
        .iter()
        .flat_map(|contribution| match contribution {
            HookContribution::ToolBundle(bundle) => hook_tool_names_to_static(&bundle.tool_names),
            _ => Vec::new(),
        })
        .collect()
}

fn hook_tool_names_to_static(tool_names: &[HookToolName]) -> Vec<&'static str> {
    tool_names
        .iter()
        .filter_map(|name| match name.as_str() {
            MEMORY_SEARCH_TOOL => Some(MEMORY_SEARCH_TOOL),
            MEMORY_LIST_TOOL => Some(MEMORY_LIST_TOOL),
            MEMORY_GET_TOOL => Some(MEMORY_GET_TOOL),
            MEMORY_REMEMBER_TOOL => Some(MEMORY_REMEMBER_TOOL),
            MEMORY_FORGET_TOOL => Some(MEMORY_FORGET_TOOL),
            _ => None,
        })
        .collect()
}

fn hook_tool_names_to_strings(tool_names: &[HookToolName]) -> Vec<&str> {
    tool_names.iter().map(|name| name.as_str()).collect()
}

#[derive(Default)]
struct TestMemoryWriteProvider {
    manifest_calls: Arc<Mutex<usize>>,
    write_calls: Arc<Mutex<usize>>,
    write_params: Arc<Mutex<Vec<MemorySemanticWriteParams>>>,
    response: Option<MemorySemanticWriteResponse>,
}

impl TestMemoryWriteProvider {
    fn manifest_call_count(&self) -> usize {
        *self
            .manifest_calls
            .lock()
            .expect("manifest call lock poisoned")
    }

    fn write_call_count(&self) -> usize {
        *self.write_calls.lock().expect("write call lock poisoned")
    }

    fn write_params(&self) -> Vec<MemorySemanticWriteParams> {
        self.write_params
            .lock()
            .expect("write params lock poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl AgentMemoryWriteProvider for TestMemoryWriteProvider {
    async fn load_memory_manifest(
        &self,
        _context: MemoryTurnContext,
        _request: MemoryManifestRequest,
    ) -> Result<MemoryManifest, String> {
        *self
            .manifest_calls
            .lock()
            .expect("manifest call lock poisoned") += 1;
        Ok(MemoryManifest::default())
    }

    async fn write_semantic_memory(
        &self,
        _context: MemoryTurnContext,
        params: MemorySemanticWriteParams,
    ) -> Result<MemorySemanticWriteResponse, String> {
        *self.write_calls.lock().expect("write call lock poisoned") += 1;
        self.write_params
            .lock()
            .expect("write params lock poisoned")
            .push(params);
        Ok(self
            .response
            .clone()
            .unwrap_or_else(test_semantic_write_response))
    }
}

struct TestPostTurnExtractorProvider {
    json: String,
    calls: Arc<Mutex<usize>>,
    contexts: Arc<Mutex<Vec<MemoryPostTurnExtractorContext>>>,
    prompts: Arc<Mutex<Vec<String>>>,
}

impl TestPostTurnExtractorProvider {
    fn json(json: impl Into<String>) -> Self {
        Self {
            json: json.into(),
            calls: Arc::new(Mutex::new(0)),
            contexts: Arc::new(Mutex::new(Vec::new())),
            prompts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn call_count(&self) -> usize {
        *self.calls.lock().expect("extractor call lock poisoned")
    }

    fn prompts(&self) -> Vec<String> {
        self.prompts.lock().expect("prompt lock poisoned").clone()
    }

    fn contexts(&self) -> Vec<MemoryPostTurnExtractorContext> {
        self.contexts.lock().expect("context lock poisoned").clone()
    }
}

struct TestSequencedPostTurnExtractorProvider {
    responses: std::sync::Mutex<std::collections::VecDeque<Result<String, String>>>,
    contexts: Arc<Mutex<Vec<MemoryPostTurnExtractorContext>>>,
}

impl TestSequencedPostTurnExtractorProvider {
    fn new(responses: impl IntoIterator<Item = Result<String, String>>) -> Self {
        Self {
            responses: std::sync::Mutex::new(responses.into_iter().collect()),
            contexts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn contexts(&self) -> Vec<MemoryPostTurnExtractorContext> {
        self.contexts.lock().expect("context lock poisoned").clone()
    }
}

#[async_trait::async_trait]
impl AgentMemoryPostTurnExtractorProvider for TestSequencedPostTurnExtractorProvider {
    async fn extract_post_turn_memory_json(
        &self,
        context: MemoryPostTurnExtractorContext,
        _request: MemoryPostTurnExtractorRequest,
    ) -> Result<String, String> {
        self.contexts
            .lock()
            .expect("context lock poisoned")
            .push(context);
        self.responses
            .lock()
            .expect("post-turn sequenced provider responses lock poisoned")
            .pop_front()
            .unwrap_or_else(|| Err("no sequenced provider response".to_owned()))
    }
}

#[async_trait::async_trait]
impl AgentMemoryPostTurnExtractorProvider for TestPostTurnExtractorProvider {
    async fn extract_post_turn_memory_json(
        &self,
        context: MemoryPostTurnExtractorContext,
        request: MemoryPostTurnExtractorRequest,
    ) -> Result<String, String> {
        *self.calls.lock().expect("extractor call lock poisoned") += 1;
        self.contexts
            .lock()
            .expect("context lock poisoned")
            .push(context);
        self.prompts
            .lock()
            .expect("prompt lock poisoned")
            .push(request.render_prompt());
        Ok(self.json.clone())
    }
}

#[derive(Default)]
struct TestFailingPostTurnExtractorProvider {
    calls: Arc<Mutex<usize>>,
}

impl TestFailingPostTurnExtractorProvider {
    fn call_count(&self) -> usize {
        *self.calls.lock().expect("extractor call lock poisoned")
    }
}

#[async_trait::async_trait]
impl AgentMemoryPostTurnExtractorProvider for TestFailingPostTurnExtractorProvider {
    async fn extract_post_turn_memory_json(
        &self,
        _context: MemoryPostTurnExtractorContext,
        _request: MemoryPostTurnExtractorRequest,
    ) -> Result<String, String> {
        *self.calls.lock().expect("extractor call lock poisoned") += 1;
        Err("provider unavailable".to_owned())
    }
}

fn test_semantic_write_response() -> MemorySemanticWriteResponse {
    MemorySemanticWriteResponse {
        relation: MemoryWriteRelation::Novel,
        canonical_key: MemoryCanonicalKey {
            key: "user/global:identity:self:name".to_owned(),
            scope: user_scope(),
            namespace: "identity".to_owned(),
            category: MemoryCategory::Identity,
            cardinality: pioneer_protocol::MemoryAttributeCardinality::SingleValue,
        },
        semantic_fingerprint: "fingerprint".to_owned(),
        record: None,
        candidate: None,
        created: false,
        superseded_memory_id: None,
        evidence_merged: false,
        route: None,
    }
}

fn test_memory_record(id: &str) -> MemoryRecord {
    MemoryRecord {
        id: id.to_owned(),
        scope: user_scope(),
        namespace: Some("identity".to_owned()),
        category: MemoryCategory::Identity,
        key: Some("user/global:identity:self:name".to_owned()),
        content: "Имя пользователя: Александр".to_owned(),
        status: MemoryStatus::Active,
        confidence: 0.98,
        importance: 0.7,
        sensitivity: pioneer_protocol::MemorySensitivity::Personal,
        provenance: MemoryProvenance {
            source_thread_id: Some("thr".to_owned()),
            source_turn_id: Some("turn".to_owned()),
            source_item_id: None,
            created_by: Some(MemoryActor {
                kind: MemoryActorKind::Extractor,
                id: Some(MEMORY_POST_TURN_EXTRACTOR_HOOK_ID.to_owned()),
            }),
        },
        source_context_kind: None,
        created_at: 1,
        updated_at: 1,
        expires_at: None,
        last_accessed_at: None,
        access_count: 0,
        superseded_by: None,
        deleted_at: None,
        delete_reason: None,
        metadata: BTreeMap::new(),
    }
}

struct TestMemoryProvider {
    materialization: Result<MemoryToolMaterialization, String>,
    materialize_calls: Arc<Mutex<usize>>,
}

impl TestMemoryProvider {
    fn with_materialization(materialization: MemoryToolMaterialization) -> Self {
        Self {
            materialization: Ok(materialization),
            materialize_calls: Arc::new(Mutex::new(0)),
        }
    }

    fn failing(error: impl Into<String>) -> Self {
        Self {
            materialization: Err(error.into()),
            materialize_calls: Arc::new(Mutex::new(0)),
        }
    }

    fn materialize_call_count(&self) -> usize {
        *self
            .materialize_calls
            .lock()
            .expect("materialize call count lock poisoned")
    }
}

struct TestRecallMemoryProvider {
    recall_result: Result<MemoryRecallSnapshot, String>,
    recall_calls: Arc<Mutex<usize>>,
    recall_requests: Arc<Mutex<Vec<MemoryRecallRequest>>>,
    mode_recall_requests: Arc<Mutex<Vec<MemoryModeRecallParams>>>,
    materialize_calls: Arc<Mutex<usize>>,
}

impl TestRecallMemoryProvider {
    fn with_recall(recall_result: MemoryRecallSnapshot) -> Self {
        Self {
            recall_result: Ok(recall_result),
            recall_calls: Arc::new(Mutex::new(0)),
            recall_requests: Arc::new(Mutex::new(Vec::new())),
            mode_recall_requests: Arc::new(Mutex::new(Vec::new())),
            materialize_calls: Arc::new(Mutex::new(0)),
        }
    }

    fn failing_recall(error: impl Into<String>) -> Self {
        Self {
            recall_result: Err(error.into()),
            recall_calls: Arc::new(Mutex::new(0)),
            recall_requests: Arc::new(Mutex::new(Vec::new())),
            mode_recall_requests: Arc::new(Mutex::new(Vec::new())),
            materialize_calls: Arc::new(Mutex::new(0)),
        }
    }

    fn recall_call_count(&self) -> usize {
        *self.recall_calls.lock().expect("recall lock poisoned")
    }

    fn materialize_call_count(&self) -> usize {
        *self
            .materialize_calls
            .lock()
            .expect("materialize lock poisoned")
    }

    fn recall_requests(&self) -> Vec<MemoryRecallRequest> {
        self.recall_requests
            .lock()
            .expect("recall request lock poisoned")
            .clone()
    }

    fn mode_recall_requests(&self) -> Vec<MemoryModeRecallParams> {
        self.mode_recall_requests
            .lock()
            .expect("mode recall request lock poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl AgentMemoryProvider for TestRecallMemoryProvider {
    async fn recall_memory(
        &self,
        _context: MemoryTurnContext,
        request: MemoryRecallRequest,
    ) -> Result<MemoryRecallSnapshot, String> {
        *self.recall_calls.lock().expect("recall lock poisoned") += 1;
        self.recall_requests
            .lock()
            .expect("recall request lock poisoned")
            .push(request);
        self.recall_result.clone()
    }

    async fn recall_memory_mode(
        &self,
        _context: MemoryTurnContext,
        request: MemoryModeRecallParams,
    ) -> Result<MemoryRecallSnapshot, String> {
        *self.recall_calls.lock().expect("recall lock poisoned") += 1;
        self.mode_recall_requests
            .lock()
            .expect("mode recall request lock poisoned")
            .push(request);
        self.recall_result.clone()
    }

    async fn materialize_memory_tools(
        &self,
        _context: MemoryTurnContext,
    ) -> Result<MemoryToolMaterialization, String> {
        *self
            .materialize_calls
            .lock()
            .expect("materialize lock poisoned") += 1;
        Ok(empty_tool_materialization())
    }
}

#[async_trait::async_trait]
impl AgentMemoryProvider for TestMemoryProvider {
    async fn recall_memory(
        &self,
        _context: MemoryTurnContext,
        _request: MemoryRecallRequest,
    ) -> Result<MemoryRecallSnapshot, String> {
        Ok(MemoryRecallSnapshot::empty())
    }

    async fn materialize_memory_tools(
        &self,
        _context: MemoryTurnContext,
    ) -> Result<MemoryToolMaterialization, String> {
        *self
            .materialize_calls
            .lock()
            .expect("materialize call count lock poisoned") += 1;
        self.materialization.clone()
    }
}

struct SlowRecallMemoryProvider;

#[async_trait::async_trait]
impl AgentMemoryProvider for SlowRecallMemoryProvider {
    async fn recall_memory(
        &self,
        _context: MemoryTurnContext,
        _request: MemoryRecallRequest,
    ) -> Result<MemoryRecallSnapshot, String> {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok(active_project_snapshot())
    }

    async fn recall_memory_mode(
        &self,
        _context: MemoryTurnContext,
        _request: MemoryModeRecallParams,
    ) -> Result<MemoryRecallSnapshot, String> {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok(active_project_snapshot())
    }

    async fn materialize_memory_tools(
        &self,
        _context: MemoryTurnContext,
    ) -> Result<MemoryToolMaterialization, String> {
        panic!("active memory recall timeout test must not materialize tools")
    }
}

struct PanickingToolHandler;

#[async_trait::async_trait]
impl pioneer_tools::ToolHandler for PanickingToolHandler {
    async fn handle(
        &self,
        _invocation: pioneer_tools::ToolInvocation,
        _trace: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn pioneer_tools::ToolOutput>, pioneer_tools::ToolError> {
        panic!("tool materialization must not execute memory tool handlers")
    }
}

struct TestToolHandler;

#[async_trait::async_trait]
impl pioneer_tools::ToolHandler for TestToolHandler {
    async fn handle(
        &self,
        _invocation: pioneer_tools::ToolInvocation,
        _trace: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn pioneer_tools::ToolOutput>, pioneer_tools::ToolError> {
        Ok(Box::new(pioneer_tools::FunctionToolOutput::new("ok", true)))
    }
}
