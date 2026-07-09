use crate::thread_episodic::{
    ThreadEpisodicRecallService, WorkspaceEpisodicRecallIntentSource, WorkspaceEpisodicRecallMode,
    WorkspaceEpisodicRecallRequest, WorkspaceEpisodicRecallService,
};
use async_trait::async_trait;
use pioneer_crud::{
    ConversationArtifactRef, ConversationArtifactRefLimits, ConversationTurnArtifactRefs, CrudStore,
};
use pioneer_hooks::{
    HookAwaitPolicy, HookCapabilities, HookCapability, HookContribution, HookContributionId,
    HookDefinition, HookDiagnostic, HookDiagnosticCode, HookDiagnosticMessage,
    HookDiagnosticSeverity, HookDomain, HookError, HookExecutionPolicy, HookFailurePolicy,
    HookHandler, HookHandlerRequest, HookHandlerResponse, HookId, HookInputPayload, HookKind,
    HookPackage, HookPhase, HookPromptContent, HookRegistryError, HookResult, HookSourceId,
    HookSourceKind, HookSourceLabel, HookSourceRef, HookSubscription, HookSubscriptionId,
    HookSubscriptionVisibility,
};
use pioneer_memory::hooks::{
    AgentEpisodicRecallProvider, MemoryCurrentThreadRecallRequest, MemoryEpisodicRecallBoundary,
    MemoryEpisodicRecallCapabilities, MemoryEpisodicRecallItem, MemoryEpisodicRecallProvenance,
    MemoryEpisodicRecallResponse, MemoryEpisodicRecallSourceKind, MemoryEpisodicRecallVisibility,
    MemoryLoopConfig, MemoryRecallPolicy, MemoryRelatedThreadRecallRequest,
    MemoryWorkspaceThreadRecallRequest, memory_turn_policy_from_hook_policy_set,
};
use pioneer_protocol::{
    ThreadEpisodicHit, ThreadEpisodicRecallDiagnostic, ThreadEpisodicRecallDiagnosticCode,
    ThreadEpisodicRecallInput, ThreadEpisodicRecallOutput, ThreadEpisodicRecallPolicyContext,
    ThreadEpisodicSourceActorRole, ThreadEpisodicThreadId, ThreadEpisodicTurnId,
    ThreadEpisodicWorkspaceId,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const THREAD_CONTEXT_RECALL_PACKAGE_ID: &str = "pioneer.thread_episodic";
const THREAD_CONTEXT_RECALL_HOOK_ID: &str = "thread_episodic.context_recall";
const THREAD_CONTEXT_RECALL_SUBSCRIPTION_ID: &str = "thread_episodic.context_recall.default";
const THREAD_CONTEXT_CONTRIBUTION_ID: &str = "memory.active_recall.thread_context";
const THREAD_CONTEXT_DOMAIN: &str = "thread_context";
const THREAD_CONTEXT_PROMPT_MAX_CHARS: u32 = 2_400;
const THREAD_CONTEXT_MAX_CANDIDATES: u32 = 40;
const THREAD_CONTEXT_HOOK_TIMEOUT_MS: u64 = 1_500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ThreadContextRecallHookConfig {
    pub enabled: bool,
    pub max_prompt_chars: u32,
    pub max_candidates: u32,
}

impl Default for ThreadContextRecallHookConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_prompt_chars: THREAD_CONTEXT_PROMPT_MAX_CHARS,
            max_candidates: THREAD_CONTEXT_MAX_CANDIDATES,
        }
    }
}

#[async_trait]
pub(crate) trait ThreadContextRecallProvider: Send + Sync {
    async fn search_current_thread(
        &self,
        input: ThreadEpisodicRecallInput,
    ) -> ThreadEpisodicRecallOutput;
}

#[async_trait]
impl ThreadContextRecallProvider for ThreadEpisodicRecallService {
    async fn search_current_thread(
        &self,
        input: ThreadEpisodicRecallInput,
    ) -> ThreadEpisodicRecallOutput {
        ThreadEpisodicRecallService::search_current_thread(self, input, None).await
    }
}

pub(crate) struct ThreadEpisodicMemoryRecallProvider {
    service: Arc<ThreadEpisodicRecallService>,
    workspace_service: Option<Arc<WorkspaceEpisodicRecallService>>,
    crud_store: Arc<CrudStore>,
}

impl ThreadEpisodicMemoryRecallProvider {
    #[allow(dead_code)]
    pub(crate) fn new(
        service: Arc<ThreadEpisodicRecallService>,
        crud_store: Arc<CrudStore>,
    ) -> Self {
        Self {
            service,
            workspace_service: None,
            crud_store,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_workspace_service(
        service: Arc<ThreadEpisodicRecallService>,
        workspace_service: Arc<WorkspaceEpisodicRecallService>,
        crud_store: Arc<CrudStore>,
    ) -> Self {
        Self {
            service,
            workspace_service: Some(workspace_service),
            crud_store,
        }
    }
}

#[async_trait]
impl AgentEpisodicRecallProvider for ThreadEpisodicMemoryRecallProvider {
    async fn recall_capabilities(
        &self,
        context: pioneer_memory::hooks::MemoryTurnContext,
    ) -> MemoryEpisodicRecallCapabilities {
        MemoryEpisodicRecallCapabilities {
            current_thread_search: true,
            related_thread_search: self.workspace_service.is_some(),
            workspace_thread_search: self.workspace_service.is_some(),
            full_input_query: self
                .service
                .full_input_query_enabled_for_workspace(context.workspace_id.as_str()),
            current_task_context: false,
            completed_task_summary: false,
        }
    }

    async fn recall_current_thread(
        &self,
        request: MemoryCurrentThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        let output = self
            .service
            .search_current_thread(
                ThreadEpisodicRecallInput {
                    workspace_id: ThreadEpisodicWorkspaceId(request.workspace_id),
                    thread_id: ThreadEpisodicThreadId(request.thread_id),
                    turn_id: ThreadEpisodicTurnId(request.turn_id),
                    query_text: request.query,
                    recent_context_summary: None,
                    policy_context: ThreadEpisodicRecallPolicyContext {
                        context_recall_allowed: true,
                        include_sensitive_context: false,
                    },
                    max_prompt_chars: Some(request.max_chars.min(u32::MAX as usize) as u32),
                    max_candidates: Some(request.top_k),
                },
                None,
            )
            .await;
        let truncated = output.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == ThreadEpisodicRecallDiagnosticCode::PromptBudgetExceeded
        });
        let hits = enrich_thread_hits_with_artifact_refs(&self.crud_store, output.hits).await;
        Ok(MemoryEpisodicRecallResponse {
            items: hits
                .into_iter()
                .map(|hit| {
                    thread_hit_to_memory_episodic_item(
                        hit,
                        MemoryEpisodicRecallSourceKind::CurrentThread,
                    )
                })
                .collect(),
            diagnostics: output
                .diagnostics
                .into_iter()
                .map(|diagnostic| diagnostic.message)
                .collect(),
            truncated,
        })
    }

    async fn recall_related_threads(
        &self,
        request: MemoryRelatedThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        let Some(service) = self.workspace_service.as_ref() else {
            return Ok(MemoryEpisodicRecallResponse {
                diagnostics: vec!["memory.episodic_recall.related_threads_unavailable".to_owned()],
                ..MemoryEpisodicRecallResponse::default()
            });
        };
        let output = service
            .search_related_threads(WorkspaceEpisodicRecallRequest {
                workspace_id: request.workspace_id,
                current_thread_id: request.current_thread_id,
                turn_id: "related_thread_recall".to_owned(),
                query_text: request.query,
                mode: WorkspaceEpisodicRecallMode::RelatedThreads,
                intent_source: Some(WorkspaceEpisodicRecallIntentSource::Planner),
                task_affinity_json: None,
                project_affinity_json: None,
                max_threads: request.top_k.max(1).min(8),
                max_segments_per_thread: 4,
                max_candidates_per_thread: request.top_k.max(1).min(16),
                max_total_candidates: request.top_k.max(1).min(32),
                max_prompt_chars: request.max_chars.min(u32::MAX as usize) as u32,
                policy_context: ThreadEpisodicRecallPolicyContext {
                    context_recall_allowed: true,
                    include_sensitive_context: false,
                },
            })
            .await;
        let hits = enrich_thread_hits_with_artifact_refs(&self.crud_store, output.hits).await;
        Ok(MemoryEpisodicRecallResponse {
            items: hits
                .into_iter()
                .map(|hit| {
                    thread_hit_to_memory_episodic_item(
                        hit,
                        MemoryEpisodicRecallSourceKind::RelatedThread,
                    )
                })
                .collect(),
            diagnostics: output.diagnostics,
            truncated: output.fallback_used,
        })
    }

    async fn recall_workspace_threads(
        &self,
        request: MemoryWorkspaceThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        let Some(service) = self.workspace_service.as_ref() else {
            return Ok(MemoryEpisodicRecallResponse {
                diagnostics: vec![
                    "memory.episodic_recall.workspace_threads_unavailable".to_owned(),
                ],
                ..MemoryEpisodicRecallResponse::default()
            });
        };
        let output = service
            .search_workspace_threads(WorkspaceEpisodicRecallRequest {
                workspace_id: request.workspace_id,
                current_thread_id: request.current_thread_id,
                turn_id: "workspace_thread_recall".to_owned(),
                query_text: request.query,
                mode: WorkspaceEpisodicRecallMode::WorkspaceThreads,
                intent_source: Some(WorkspaceEpisodicRecallIntentSource::Planner),
                task_affinity_json: None,
                project_affinity_json: None,
                max_threads: request.top_k.max(1).min(8),
                max_segments_per_thread: 4,
                max_candidates_per_thread: request.top_k.max(1).min(16),
                max_total_candidates: request.top_k.max(1).min(32),
                max_prompt_chars: request.max_chars.min(u32::MAX as usize) as u32,
                policy_context: ThreadEpisodicRecallPolicyContext {
                    context_recall_allowed: true,
                    include_sensitive_context: false,
                },
            })
            .await;
        let hits = enrich_thread_hits_with_artifact_refs(&self.crud_store, output.hits).await;
        Ok(MemoryEpisodicRecallResponse {
            items: hits
                .into_iter()
                .map(|hit| {
                    thread_hit_to_memory_episodic_item(
                        hit,
                        MemoryEpisodicRecallSourceKind::WorkspaceThread,
                    )
                })
                .collect(),
            diagnostics: output.diagnostics,
            truncated: output.fallback_used,
        })
    }
}

fn thread_hit_to_memory_episodic_item(
    hit: ThreadEpisodicHit,
    source: MemoryEpisodicRecallSourceKind,
) -> MemoryEpisodicRecallItem {
    MemoryEpisodicRecallItem {
        id: hit.provenance.source_id.clone(),
        content: hit.text,
        title: None,
        provenance: MemoryEpisodicRecallProvenance {
            workspace_id: hit.provenance.workspace_id.0,
            thread_id: Some(hit.provenance.thread_id.0),
            turn_id: Some(hit.provenance.turn_id.0),
            task_id: None,
            timestamp_unix: hit.created_at,
            source,
            retrieval_score: Some(hit.score),
            boundary: MemoryEpisodicRecallBoundary::Snippet,
        },
        score: Some(hit.score),
        updated_at_unix: hit.created_at,
        visibility: MemoryEpisodicRecallVisibility::Public,
    }
}

async fn enrich_thread_hits_with_artifact_refs(
    crud_store: &CrudStore,
    hits: Vec<ThreadEpisodicHit>,
) -> Vec<ThreadEpisodicHit> {
    if hits.is_empty() {
        return hits;
    }
    let refs_by_source_id = conversation_artifact_refs_by_source_id(crud_store, &hits).await;
    if refs_by_source_id.is_empty() {
        return hits;
    }

    hits.into_iter()
        .map(|mut hit| {
            if let Some(refs) = refs_by_source_id.get(hit.provenance.source_id.as_str()) {
                hit.text = crate::artifact_prompt_refs::append_episodic_artifact_refs(
                    hit.text.as_str(),
                    hit.provenance.source_id.as_str(),
                    refs,
                );
            }
            hit
        })
        .collect()
}

async fn conversation_artifact_refs_by_source_id(
    crud_store: &CrudStore,
    hits: &[ThreadEpisodicHit],
) -> BTreeMap<String, Vec<ConversationArtifactRef>> {
    let mut turn_ids_by_thread: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for hit in hits {
        turn_ids_by_thread
            .entry((
                hit.provenance.workspace_id.0.clone(),
                hit.provenance.thread_id.0.clone(),
            ))
            .or_default()
            .insert(hit.provenance.turn_id.0.clone());
    }

    let mut refs_by_turn: BTreeMap<(String, String, String), ConversationTurnArtifactRefs> =
        BTreeMap::new();
    for ((workspace_id, thread_id), turn_ids) in turn_ids_by_thread {
        let turn_ids = turn_ids.into_iter().collect::<Vec<_>>();
        let Ok(grouped) = crud_store
            .list_conversation_artifact_refs(
                workspace_id.as_str(),
                thread_id.as_str(),
                &turn_ids,
                ConversationArtifactRefLimits::default(),
            )
            .await
        else {
            continue;
        };
        for (turn_id, refs) in grouped {
            refs_by_turn.insert((workspace_id.clone(), thread_id.clone(), turn_id), refs);
        }
    }

    let mut refs_by_source_id = BTreeMap::new();
    for hit in hits {
        let key = (
            hit.provenance.workspace_id.0.clone(),
            hit.provenance.thread_id.0.clone(),
            hit.provenance.turn_id.0.clone(),
        );
        let Some(turn_refs) = refs_by_turn.get(&key) else {
            continue;
        };
        let refs = refs_for_thread_hit(hit, turn_refs);
        if !refs.is_empty() {
            refs_by_source_id.insert(hit.provenance.source_id.clone(), refs);
        }
    }
    refs_by_source_id
}

fn refs_for_thread_hit(
    hit: &ThreadEpisodicHit,
    turn_refs: &ConversationTurnArtifactRefs,
) -> Vec<ConversationArtifactRef> {
    let (bucket, allow_bucket_fallback) = match hit.provenance.source_actor_role {
        ThreadEpisodicSourceActorRole::User => (&turn_refs.user, true),
        ThreadEpisodicSourceActorRole::Assistant => (&turn_refs.assistant, true),
        ThreadEpisodicSourceActorRole::TaskSummary
        | ThreadEpisodicSourceActorRole::GeneratedSummary => (&turn_refs.assistant, false),
    };
    if bucket.is_empty() {
        return Vec::new();
    }

    let item_id = hit.provenance.item_id.0.as_str();
    let exact = bucket
        .iter()
        .filter(|artifact_ref| {
            artifact_ref.turn_item_id.as_deref() == Some(item_id)
                || artifact_ref.message_id.as_deref() == Some(item_id)
        })
        .cloned()
        .collect::<Vec<_>>();
    if exact.is_empty() {
        if allow_bucket_fallback {
            bucket.clone()
        } else {
            Vec::new()
        }
    } else {
        exact
    }
}

pub(crate) fn thread_context_recall_hook_package(
    recall_provider: Arc<dyn ThreadContextRecallProvider>,
    artifact_store: Option<Arc<CrudStore>>,
    memory_config: MemoryLoopConfig,
    thread_context_config: ThreadContextRecallHookConfig,
) -> ThreadContextRecallHookPackage {
    ThreadContextRecallHookPackage {
        recall_provider,
        artifact_store,
        config: ThreadContextRecallHookConfig {
            enabled: memory_config.normalized().deterministic_recall_enabled
                && thread_context_config.enabled,
            ..thread_context_config
        },
    }
}

pub(crate) struct ThreadContextRecallHookPackage {
    recall_provider: Arc<dyn ThreadContextRecallProvider>,
    artifact_store: Option<Arc<CrudStore>>,
    config: ThreadContextRecallHookConfig,
}

impl HookPackage for ThreadContextRecallHookPackage {
    fn id(&self) -> &'static str {
        THREAD_CONTEXT_RECALL_PACKAGE_ID
    }

    fn definitions(&self) -> Result<Vec<HookDefinition>, HookRegistryError> {
        let hook = Arc::new(ThreadContextRecallHook {
            recall_provider: self.recall_provider.clone(),
            artifact_store: self.artifact_store.clone(),
            config: self.config,
        });
        let hook_id = hook.id();
        let subscription_id = HookSubscriptionId::new(THREAD_CONTEXT_RECALL_SUBSCRIPTION_ID)
            .expect("static subscription id is valid");
        Ok(vec![HookDefinition::new(
            hook,
            [
                HookSubscription::new(subscription_id, hook_id, HookPhase::TurnPrePromptContext)
                    .with_priority(-10)
                    .with_execution_policy(HookExecutionPolicy {
                        await_policy: HookAwaitPolicy::Deadline,
                        timeout_ms: Some(THREAD_CONTEXT_HOOK_TIMEOUT_MS),
                        max_parallelism: None,
                    })
                    .with_failure_policy(HookFailurePolicy::BestEffort)
                    .with_visibility(HookSubscriptionVisibility::Internal),
            ],
            THREAD_CONTEXT_RECALL_PACKAGE_ID,
        )])
    }
}

pub(crate) struct ThreadContextRecallHook {
    recall_provider: Arc<dyn ThreadContextRecallProvider>,
    artifact_store: Option<Arc<CrudStore>>,
    config: ThreadContextRecallHookConfig,
}

#[async_trait]
impl HookHandler for ThreadContextRecallHook {
    fn id(&self) -> HookId {
        HookId::new(THREAD_CONTEXT_RECALL_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("thread_episodic").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPrePromptContext]
    }

    fn capabilities(&self) -> HookCapabilities {
        HookCapabilities::new([
            HookCapability::new("thread_episodic_recall").expect("static capability is valid"),
            HookCapability::new("contribute_prompt_context").expect("static capability is valid"),
        ])
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        if request.phase != HookPhase::TurnPrePromptContext {
            return Ok(HookHandlerResponse::default());
        }

        let mut response = HookHandlerResponse::default();
        if !self.config.enabled {
            response.diagnostics.push(thread_context_info_diagnostic(
                "thread_context.disabled",
                "thread context recall skipped: disabled",
            ));
            return Ok(response);
        }

        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                response.diagnostics.push(thread_context_warning_diagnostic(
                    "thread_context.policy_decode_failed",
                    format!("thread context recall skipped: policy decode failed: {error}"),
                ));
                return Ok(response);
            }
            None => {
                response.diagnostics.push(thread_context_warning_diagnostic(
                    "thread_context.policy_missing",
                    "thread context recall skipped: memory policy missing",
                ));
                return Ok(response);
            }
        };
        if policy.recall != MemoryRecallPolicy::Allow {
            response.diagnostics.push(thread_context_info_diagnostic(
                "thread_context.policy_skipped",
                format!(
                    "thread context recall skipped: reason={} recall={}",
                    policy.reason_code.as_str(),
                    policy.recall.as_str()
                ),
            ));
            return Ok(response);
        }

        let HookInputPayload::TurnPrePromptContext(input) = &request.input.payload else {
            response.diagnostics.push(thread_context_warning_diagnostic(
                "thread_context.invalid_input",
                "thread context recall skipped: invalid hook input",
            ));
            return Ok(response);
        };

        let Some(workspace_id) = request.context.workspace_id.as_ref().map(|id| id.as_str()) else {
            response.diagnostics.push(thread_context_warning_diagnostic(
                "thread_context.invalid_context",
                "thread context recall skipped: workspace id missing",
            ));
            return Ok(response);
        };
        let Some(thread_id) = request.context.thread_id.as_ref().map(|id| id.as_str()) else {
            response.diagnostics.push(thread_context_warning_diagnostic(
                "thread_context.invalid_context",
                "thread context recall skipped: thread id missing",
            ));
            return Ok(response);
        };
        let Some(turn_id) = request.context.turn_id.as_ref().map(|id| id.as_str()) else {
            response.diagnostics.push(thread_context_warning_diagnostic(
                "thread_context.invalid_context",
                "thread context recall skipped: turn id missing",
            ));
            return Ok(response);
        };

        let recall_output = self
            .recall_provider
            .search_current_thread(ThreadEpisodicRecallInput {
                workspace_id: ThreadEpisodicWorkspaceId(workspace_id.to_owned()),
                thread_id: ThreadEpisodicThreadId(thread_id.to_owned()),
                turn_id: ThreadEpisodicTurnId(turn_id.to_owned()),
                query_text: input.input_text.clone(),
                recent_context_summary: None,
                policy_context: ThreadEpisodicRecallPolicyContext {
                    context_recall_allowed: true,
                    include_sensitive_context: false,
                },
                max_prompt_chars: Some(self.config.max_prompt_chars),
                max_candidates: Some(self.config.max_candidates),
            })
            .await;
        let recall_diagnostics = hook_diagnostics_from_recall(&recall_output.diagnostics);
        response.diagnostics.extend(recall_diagnostics.clone());

        let recall_output = if let Some(artifact_store) = self.artifact_store.as_ref() {
            ThreadEpisodicRecallOutput {
                hits: enrich_thread_hits_with_artifact_refs(artifact_store, recall_output.hits)
                    .await,
                ..recall_output
            }
        } else {
            recall_output
        };

        if let Some(contribution) = thread_context_prompt_contribution(
            &recall_output,
            recall_diagnostics,
            self.config.max_prompt_chars,
        )? {
            response.diagnostics.push(thread_context_info_diagnostic(
                "thread_context.contributed",
                "thread context recall contributed prompt context",
            ));
            response
                .contributions
                .push(HookContribution::PromptContext(contribution));
        }

        Ok(response)
    }
}

fn thread_context_prompt_contribution(
    recall_output: &ThreadEpisodicRecallOutput,
    mut diagnostics: Vec<HookDiagnostic>,
    max_prompt_chars: u32,
) -> HookResult<Option<pioneer_hooks::PromptContextContribution>> {
    if recall_output.hits.is_empty() {
        return Ok(None);
    }
    let content = render_thread_context_hits(&recall_output.hits);
    let content = HookPromptContent::new(content).map_err(|error| {
        thread_context_hook_error(
            "thread_context.invalid_prompt_content",
            format!("invalid thread context prompt content: {error}"),
        )
    })?;
    let source_refs = recall_output
        .hits
        .iter()
        .filter_map(thread_context_source_ref)
        .collect::<Vec<_>>();
    diagnostics.push(thread_context_prompt_consumed_diagnostic(&source_refs));
    let truncated = recall_output.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == ThreadEpisodicRecallDiagnosticCode::PromptBudgetExceeded
    });

    Ok(Some(pioneer_hooks::PromptContextContribution {
        contribution_id: HookContributionId::new(THREAD_CONTEXT_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        domain: HookDomain::new(THREAD_CONTEXT_DOMAIN).expect("static domain is valid"),
        priority: 480,
        content,
        max_chars: Some(max_prompt_chars as usize),
        source_refs,
        diagnostics,
        truncated,
    }))
}

fn thread_context_prompt_consumed_diagnostic(source_refs: &[HookSourceRef]) -> HookDiagnostic {
    let source_ids = source_refs
        .iter()
        .map(|source_ref| source_ref.id.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    thread_context_info_diagnostic(
        "thread_context.prompt_section_consumed",
        format!(
            "thread context prompt section `{}` consumed {} source refs: {}",
            THREAD_CONTEXT_CONTRIBUTION_ID,
            source_refs.len(),
            source_ids
        ),
    )
}

fn render_thread_context_hits(hits: &[ThreadEpisodicHit]) -> String {
    hits.iter()
        .map(|hit| {
            format!(
                "- [{source_id}, role={role}, context={context}, score={score:.2}] {text}",
                source_id = hit.provenance.source_id,
                role = thread_context_actor_role_label(&hit.provenance.source_actor_role),
                context = thread_context_source_context_label(&hit.provenance.source_context),
                score = hit.score,
                text = hit.text.trim()
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn thread_context_source_ref(hit: &ThreadEpisodicHit) -> Option<HookSourceRef> {
    Some(HookSourceRef {
        kind: HookSourceKind::Custom("thread_episodic".to_owned()),
        id: HookSourceId::new(hit.provenance.source_id.clone()).ok()?,
        label: Some(
            HookSourceLabel::new(format!(
                "{} {}",
                thread_context_actor_role_label(&hit.provenance.source_actor_role),
                hit.provenance.source_id
            ))
            .ok()?,
        ),
    })
}

fn thread_context_actor_role_label(
    role: &pioneer_protocol::ThreadEpisodicSourceActorRole,
) -> &'static str {
    match role {
        pioneer_protocol::ThreadEpisodicSourceActorRole::User => "user",
        pioneer_protocol::ThreadEpisodicSourceActorRole::Assistant => "assistant",
        pioneer_protocol::ThreadEpisodicSourceActorRole::TaskSummary => "task_summary",
        pioneer_protocol::ThreadEpisodicSourceActorRole::GeneratedSummary => "generated_summary",
    }
}

fn thread_context_source_context_label(
    context: &pioneer_protocol::ThreadEpisodicSourceContext,
) -> &'static str {
    match context {
        pioneer_protocol::ThreadEpisodicSourceContext::UserVisibleThreadItem => {
            "user_visible_thread_item"
        }
        pioneer_protocol::ThreadEpisodicSourceContext::UserVisibleTaskSummary => {
            "user_visible_task_summary"
        }
        pioneer_protocol::ThreadEpisodicSourceContext::ThreadCompactionSummary => {
            "thread_compaction_summary"
        }
        pioneer_protocol::ThreadEpisodicSourceContext::HiddenPrompt => "hidden_prompt",
        pioneer_protocol::ThreadEpisodicSourceContext::ReasoningTrace => "reasoning_trace",
        pioneer_protocol::ThreadEpisodicSourceContext::RawToolOutput => "raw_tool_output",
        pioneer_protocol::ThreadEpisodicSourceContext::RawTaskRuntime => "raw_task_runtime",
        pioneer_protocol::ThreadEpisodicSourceContext::InternalHookRuntime => {
            "internal_hook_runtime"
        }
        pioneer_protocol::ThreadEpisodicSourceContext::SystemPrompt => "system_prompt",
        pioneer_protocol::ThreadEpisodicSourceContext::DeveloperPrompt => "developer_prompt",
        pioneer_protocol::ThreadEpisodicSourceContext::Unknown => "unknown",
    }
}

fn hook_diagnostics_from_recall(
    diagnostics: &[ThreadEpisodicRecallDiagnostic],
) -> Vec<HookDiagnostic> {
    diagnostics
        .iter()
        .map(|diagnostic| {
            let severity = match diagnostic.code {
                ThreadEpisodicRecallDiagnosticCode::BackendUnavailable
                | ThreadEpisodicRecallDiagnosticCode::InvalidInput => {
                    HookDiagnosticSeverity::Warning
                }
                ThreadEpisodicRecallDiagnosticCode::Completed
                | ThreadEpisodicRecallDiagnosticCode::SkippedByPolicy
                | ThreadEpisodicRecallDiagnosticCode::PromptBudgetExceeded
                | ThreadEpisodicRecallDiagnosticCode::SuppressedByBoundary
                | ThreadEpisodicRecallDiagnosticCode::Unknown => HookDiagnosticSeverity::Info,
            };
            thread_context_diagnostic(
                recall_diagnostic_code(&diagnostic.code),
                diagnostic.message.clone(),
                severity,
            )
        })
        .collect()
}

fn recall_diagnostic_code(code: &ThreadEpisodicRecallDiagnosticCode) -> &'static str {
    match code {
        ThreadEpisodicRecallDiagnosticCode::Completed => "thread_context.completed",
        ThreadEpisodicRecallDiagnosticCode::SkippedByPolicy => "thread_context.skipped_by_policy",
        ThreadEpisodicRecallDiagnosticCode::BackendUnavailable => {
            "thread_context.backend_unavailable"
        }
        ThreadEpisodicRecallDiagnosticCode::InvalidInput => "thread_context.invalid_input",
        ThreadEpisodicRecallDiagnosticCode::PromptBudgetExceeded => {
            "thread_context.prompt_budget_exceeded"
        }
        ThreadEpisodicRecallDiagnosticCode::SuppressedByBoundary => {
            "thread_context.suppressed_by_boundary"
        }
        ThreadEpisodicRecallDiagnosticCode::Unknown => "thread_context.unknown",
    }
}

fn thread_context_info_diagnostic(
    code: &'static str,
    message: impl Into<String>,
) -> HookDiagnostic {
    thread_context_diagnostic(code, message, HookDiagnosticSeverity::Info)
}

fn thread_context_warning_diagnostic(
    code: &'static str,
    message: impl Into<String>,
) -> HookDiagnostic {
    thread_context_diagnostic(code, message, HookDiagnosticSeverity::Warning)
}

fn thread_context_diagnostic(
    code: &'static str,
    message: impl Into<String>,
    severity: HookDiagnosticSeverity,
) -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(message.into())
            .expect("thread context diagnostic message is non-empty"),
        severity,
        safe_for_user: false,
        metadata: Default::default(),
    }
}

fn thread_context_hook_error(code: &'static str, message: impl Into<String>) -> HookError {
    HookError::new(
        HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        HookDiagnosticMessage::new(message.into())
            .expect("thread context hook error message is non-empty"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_hooks::{
        HookContext, HookInput, HookPhaseRequest, HookPolicyKey, HookPolicySet, HookRuntimeBuilder,
        HookValue, HookWorkspaceId, PolicyContribution, TurnPrePromptContextHookInput,
    };
    use pioneer_memory::hooks::{
        MemoryActiveContextPolicy, MemoryExtractionPolicy, MemoryMutationToolPolicy,
        MemoryPolicyReasonCode, MemoryPolicySource, MemoryPromptPolicy, MemoryReadToolPolicy,
        MemoryTurnPolicy,
    };
    use pioneer_protocol::{
        ArtifactBindingDirection, ArtifactBindingKind, ArtifactKind, ArtifactRole,
        ThreadEpisodicItemId, ThreadEpisodicScoreBreakdown, ThreadEpisodicSourceActorRole,
        ThreadEpisodicSourceContext, ThreadEpisodicSourceProvenance,
    };
    use std::collections::BTreeMap;
    use tokio::sync::Mutex;

    struct FakeThreadContextRecallProvider {
        output: Mutex<ThreadEpisodicRecallOutput>,
        inputs: Mutex<Vec<ThreadEpisodicRecallInput>>,
    }

    impl Default for FakeThreadContextRecallProvider {
        fn default() -> Self {
            Self {
                output: Mutex::new(ThreadEpisodicRecallOutput {
                    hits: Vec::new(),
                    diagnostics: Vec::new(),
                    fallback_used: false,
                }),
                inputs: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ThreadContextRecallProvider for FakeThreadContextRecallProvider {
        async fn search_current_thread(
            &self,
            input: ThreadEpisodicRecallInput,
        ) -> ThreadEpisodicRecallOutput {
            self.inputs.lock().await.push(input);
            self.output.lock().await.clone()
        }
    }

    #[tokio::test]
    async fn thread_context_hook_contributes_prompt_context_from_recall() {
        let provider = Arc::new(FakeThreadContextRecallProvider::default());
        *provider.output.lock().await = ThreadEpisodicRecallOutput {
            hits: vec![test_hit(
                "thread:turn_1/item_1/chunk_1",
                "previous decision",
            )],
            diagnostics: vec![ThreadEpisodicRecallDiagnostic {
                code: ThreadEpisodicRecallDiagnosticCode::Completed,
                message: "done".to_owned(),
            }],
            fallback_used: false,
        };
        let hook = ThreadContextRecallHook {
            recall_provider: provider.clone(),
            artifact_store: None,
            config: ThreadContextRecallHookConfig::default(),
        };

        let response = hook
            .execute(test_request(MemoryTurnPolicy::normal_default_allow()))
            .await
            .expect("hook should execute");

        let inputs = provider.inputs.lock().await;
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].workspace_id.0, "ws_1");
        assert_eq!(inputs[0].thread_id.0, "thread_1");
        assert_eq!(inputs[0].turn_id.0, "turn_1");
        assert_eq!(
            inputs[0].max_prompt_chars,
            Some(THREAD_CONTEXT_PROMPT_MAX_CHARS)
        );
        assert_eq!(
            inputs[0].max_candidates,
            Some(THREAD_CONTEXT_MAX_CANDIDATES)
        );
        let contribution = prompt_context_contribution(&response).expect("context contribution");
        assert_eq!(contribution.domain.as_str(), THREAD_CONTEXT_DOMAIN);
        assert_eq!(
            contribution.contribution_id.as_str(),
            THREAD_CONTEXT_CONTRIBUTION_ID
        );
        assert!(contribution.content.as_str().contains("previous decision"));
        assert_eq!(contribution.source_refs.len(), 1);
        assert_eq!(
            contribution.source_refs[0].id.as_str(),
            "thread:turn_1/item_1/chunk_1"
        );
        assert!(contribution.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "thread_context.prompt_section_consumed"
                && diagnostic
                    .message
                    .as_str()
                    .contains("thread:turn_1/item_1/chunk_1")
        }));
    }

    #[test]
    fn thread_hit_maps_to_memory_episodic_item_with_source_provenance() {
        let item = thread_hit_to_memory_episodic_item(
            test_hit("thread:turn_1/item_1/chunk_1", "previous decision"),
            MemoryEpisodicRecallSourceKind::CurrentThread,
        );

        assert_eq!(item.id, "thread:turn_1/item_1/chunk_1");
        assert_eq!(item.content, "previous decision");
        assert_eq!(item.provenance.workspace_id, "ws_1");
        assert_eq!(item.provenance.thread_id.as_deref(), Some("thread_1"));
        assert_eq!(item.provenance.turn_id.as_deref(), Some("turn_1"));
        assert_eq!(
            item.provenance.source,
            MemoryEpisodicRecallSourceKind::CurrentThread
        );
        assert_eq!(
            item.provenance.boundary,
            MemoryEpisodicRecallBoundary::Snippet
        );
        assert_eq!(item.visibility, MemoryEpisodicRecallVisibility::Public);
        assert_eq!(item.score, Some(0.82));
    }

    #[tokio::test]
    async fn thread_context_contribution_appears_through_hook_runtime() {
        let provider = Arc::new(FakeThreadContextRecallProvider::default());
        *provider.output.lock().await = ThreadEpisodicRecallOutput {
            hits: vec![test_hit("thread:turn_1/item_1/chunk_1", "runtime context")],
            diagnostics: Vec::new(),
            fallback_used: false,
        };
        let runtime = HookRuntimeBuilder::new()
            .install(thread_context_recall_hook_package(
                provider,
                None,
                MemoryLoopConfig::default(),
                ThreadContextRecallHookConfig::default(),
            ))
            .expect("package installs")
            .build();

        let response = runtime
            .run_phase(test_phase_request(MemoryTurnPolicy::normal_default_allow()))
            .await
            .expect("phase should run");

        assert!(response.runs.iter().any(|run| {
            run.hook_id.as_str() == THREAD_CONTEXT_RECALL_HOOK_ID
                && run.phase == HookPhase::TurnPrePromptContext
        }));
        assert!(response.contributions.iter().any(|contribution| {
            matches!(
                contribution,
                HookContribution::PromptContext(context)
                    if context.contribution_id.as_str() == THREAD_CONTEXT_CONTRIBUTION_ID
                        && context.domain.as_str() == THREAD_CONTEXT_DOMAIN
                        && context.content.as_str().contains("runtime context")
            )
        }));
    }

    #[tokio::test]
    async fn thread_context_hook_skips_policy_opt_out_without_calling_provider() {
        let provider = Arc::new(FakeThreadContextRecallProvider::default());
        let hook = ThreadContextRecallHook {
            recall_provider: provider.clone(),
            artifact_store: None,
            config: ThreadContextRecallHookConfig::default(),
        };

        let response = hook
            .execute(test_request(MemoryTurnPolicy::no_use()))
            .await
            .expect("hook should execute");

        assert!(provider.inputs.lock().await.is_empty());
        assert!(prompt_context_contribution(&response).is_none());
        assert!(
            response
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code.as_str() == "thread_context.policy_skipped")
        );
    }

    #[tokio::test]
    async fn thread_context_hook_skips_disabled_setting_without_calling_provider() {
        let provider = Arc::new(FakeThreadContextRecallProvider::default());
        let hook = ThreadContextRecallHook {
            recall_provider: provider.clone(),
            artifact_store: None,
            config: ThreadContextRecallHookConfig {
                enabled: false,
                ..ThreadContextRecallHookConfig::default()
            },
        };

        let response = hook
            .execute(test_request(MemoryTurnPolicy::normal_default_allow()))
            .await
            .expect("hook should execute");

        assert!(provider.inputs.lock().await.is_empty());
        assert!(prompt_context_contribution(&response).is_none());
        assert!(
            response
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code.as_str() == "thread_context.disabled")
        );
    }

    #[test]
    fn thread_context_hook_package_registers_on_prompt_context_phase() {
        let provider = Arc::new(FakeThreadContextRecallProvider::default());
        let package = thread_context_recall_hook_package(
            provider,
            None,
            MemoryLoopConfig::default(),
            ThreadContextRecallHookConfig::default(),
        );
        let definitions = package.definitions().expect("definitions");
        assert_eq!(definitions.len(), 1);
        let subscription = definitions[0].subscriptions.first().expect("subscription");
        assert_eq!(subscription.phase, HookPhase::TurnPrePromptContext);
        assert!(subscription.dependencies.after.is_empty());
        assert_eq!(
            subscription.execution_policy.await_policy,
            HookAwaitPolicy::Deadline
        );
    }

    #[test]
    fn agent_loop_does_not_call_thread_context_recall_service_directly() {
        let agent_chat = include_str!("../../agent/src/chat/mod.rs");
        let agent_hooks = include_str!("../../agent/src/hooks.rs");

        assert!(!agent_chat.contains("ThreadEpisodicRecallService"));
        assert!(!agent_chat.contains("thread_context_recall_hook_package"));
        assert!(!agent_hooks.contains("ThreadEpisodicRecallService"));
        assert!(!agent_hooks.contains("thread_context_recall_hook_package"));
    }

    fn test_request(policy: MemoryTurnPolicy) -> HookHandlerRequest {
        let request = test_phase_request(policy);
        HookHandlerRequest {
            hook_id: HookId::new(THREAD_CONTEXT_RECALL_HOOK_ID).expect("valid hook id"),
            phase: request.phase,
            context: request.context,
            input: request.input,
            policy_set: request.policy_set,
            prompt_context_set: request.prompt_context_set,
        }
    }

    fn test_phase_request(policy: MemoryTurnPolicy) -> HookPhaseRequest {
        HookPhaseRequest::new(
            HookPhase::TurnPrePromptContext,
            HookContext {
                workspace_id: Some(HookWorkspaceId::new("ws_1").expect("valid workspace id")),
                thread_id: Some(
                    pioneer_hooks::HookThreadId::new("thread_1").expect("valid thread id"),
                ),
                turn_id: Some(pioneer_hooks::HookTurnId::new("turn_1").expect("valid turn id")),
                ..HookContext::default()
            },
            HookInput::turn_pre_prompt_context(TurnPrePromptContextHookInput::from_parts(
                "continue from previous decision",
                Some("model"),
                Some("provider"),
            )),
        )
        .with_policy_set(memory_policy_set(policy))
    }

    fn memory_policy_set(policy: MemoryTurnPolicy) -> HookPolicySet {
        HookPolicySet::merge_contributions([PolicyContribution {
            domain: HookDomain::new("memory").expect("valid domain"),
            key: HookPolicyKey::new("turn_policy").expect("valid policy key"),
            value: memory_turn_policy_value(policy),
            priority: 500,
            diagnostics: Vec::new(),
        }])
    }

    fn memory_turn_policy_value(policy: MemoryTurnPolicy) -> HookValue {
        let mut object = BTreeMap::new();
        insert_policy_text(&mut object, "recall", policy.recall.as_str());
        insert_policy_text(&mut object, "prompt", policy.prompt.as_str());
        insert_policy_text(&mut object, "read_tools", policy.read_tools.as_str());
        insert_policy_text(&mut object, "remember_tool", policy.remember_tool.as_str());
        insert_policy_text(&mut object, "forget_tool", policy.forget_tool.as_str());
        insert_policy_text(
            &mut object,
            "post_turn_extraction",
            policy.post_turn_extraction.as_str(),
        );
        insert_policy_text(&mut object, "active_memory", policy.active_memory.as_str());
        object.insert(
            pioneer_hooks::HookMetadataKey::new("explicit_remember").expect("valid key"),
            HookValue::Bool(policy.explicit_remember),
        );
        object.insert(
            pioneer_hooks::HookMetadataKey::new("explicit_forget").expect("valid key"),
            HookValue::Bool(policy.explicit_forget),
        );
        object.insert(
            pioneer_hooks::HookMetadataKey::new("reason_code").expect("valid key"),
            HookValue::Text(policy.reason_code.as_str().to_owned()),
        );
        object.insert(
            pioneer_hooks::HookMetadataKey::new("confidence").expect("valid key"),
            HookValue::F64(policy.confidence as f64),
        );
        object.insert(
            pioneer_hooks::HookMetadataKey::new("source").expect("valid key"),
            HookValue::Text(policy.source.as_str().to_owned()),
        );
        if let Some(target) = policy.forget_target_hint {
            object.insert(
                pioneer_hooks::HookMetadataKey::new("forget_target_hint").expect("valid key"),
                HookValue::Text(target),
            );
        }
        if let Some(language) = policy.detected_language {
            object.insert(
                pioneer_hooks::HookMetadataKey::new("detected_language").expect("valid key"),
                HookValue::Text(language),
            );
        }
        HookValue::Object(object)
    }

    fn insert_policy_text(
        object: &mut BTreeMap<pioneer_hooks::HookMetadataKey, HookValue>,
        key: &'static str,
        value: &'static str,
    ) {
        object.insert(
            pioneer_hooks::HookMetadataKey::new(key).expect("valid key"),
            HookValue::Text(value.to_owned()),
        );
    }

    fn test_hit(source_id: &str, text: &str) -> ThreadEpisodicHit {
        ThreadEpisodicHit {
            provenance: ThreadEpisodicSourceProvenance {
                source_id: source_id.to_owned(),
                workspace_id: ThreadEpisodicWorkspaceId("ws_1".to_owned()),
                thread_id: ThreadEpisodicThreadId("thread_1".to_owned()),
                turn_id: ThreadEpisodicTurnId("turn_1".to_owned()),
                item_id: ThreadEpisodicItemId("item_1".to_owned()),
                index_item_id: pioneer_protocol::ThreadEpisodicIndexItemId(
                    "index_item_1".to_owned(),
                ),
                source_actor_role: ThreadEpisodicSourceActorRole::User,
                source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
                created_at: Some(1_700_000_000),
            },
            text: text.to_owned(),
            score: 0.82,
            score_breakdown: ThreadEpisodicScoreBreakdown {
                final_score: 0.82,
                memvid_score: Some(0.8),
                semantic_score: None,
                lexical_score: Some(0.8),
                temporal_score: None,
                exact_source_boost: None,
                recency_boost: Some(0.02),
                source_role_boost: None,
            },
            adaptive_diagnostics: None,
            created_at: Some(1_700_000_000),
        }
    }

    fn test_user_conversation_artifact_ref(turn_item_id: Option<&str>) -> ConversationArtifactRef {
        ConversationArtifactRef {
            artifact_id: "art_car".to_owned(),
            version_id: Some("ver_car".to_owned()),
            display_name: "car.jpg".to_owned(),
            kind: ArtifactKind::Image,
            mime_type: Some("image/jpeg".to_owned()),
            size_bytes: Some(862_208),
            sha256: Some("sha".to_owned()),
            binding_kind: ArtifactBindingKind::UserInput,
            direction: ArtifactBindingDirection::Input,
            role: Some(ArtifactRole::User),
            turn_id: Some("turn_1".to_owned()),
            message_id: turn_item_id.map(ToOwned::to_owned),
            turn_item_id: turn_item_id.map(ToOwned::to_owned),
            item_index: Some(0),
        }
    }

    fn test_assistant_conversation_artifact_ref(
        turn_item_id: Option<&str>,
    ) -> ConversationArtifactRef {
        ConversationArtifactRef {
            artifact_id: "art_report".to_owned(),
            version_id: Some("ver_report".to_owned()),
            display_name: "report.pdf".to_owned(),
            kind: ArtifactKind::File,
            mime_type: Some("application/pdf".to_owned()),
            size_bytes: Some(24_000),
            sha256: Some("sha".to_owned()),
            binding_kind: ArtifactBindingKind::AgentOutput,
            direction: ArtifactBindingDirection::Output,
            role: Some(ArtifactRole::Assistant),
            turn_id: Some("turn_1".to_owned()),
            message_id: turn_item_id.map(ToOwned::to_owned),
            turn_item_id: turn_item_id.map(ToOwned::to_owned),
            item_index: Some(0),
        }
    }

    #[test]
    fn episodic_user_and_assistant_hits_get_matching_artifact_refs() {
        let turn_refs = ConversationTurnArtifactRefs {
            user: vec![test_user_conversation_artifact_ref(Some("item_1"))],
            assistant: vec![test_assistant_conversation_artifact_ref(Some(
                "assistant_item",
            ))],
        };

        let user_refs = refs_for_thread_hit(
            &test_hit("thread:turn_1/item_1/chunk_1", "user"),
            &turn_refs,
        );
        assert_eq!(user_refs.len(), 1);
        assert_eq!(user_refs[0].artifact_id, "art_car");
        assert_eq!(user_refs[0].role, Some(ArtifactRole::User));

        let mut assistant_hit = test_hit("thread:turn_1/assistant_item/chunk_1", "assistant");
        assistant_hit.provenance.source_actor_role = ThreadEpisodicSourceActorRole::Assistant;
        assistant_hit.provenance.item_id = ThreadEpisodicItemId("assistant_item".to_owned());
        let assistant_refs = refs_for_thread_hit(&assistant_hit, &turn_refs);
        assert_eq!(assistant_refs.len(), 1);
        assert_eq!(assistant_refs[0].artifact_id, "art_report");
        assert_eq!(assistant_refs[0].role, Some(ArtifactRole::Assistant));
    }

    #[test]
    fn episodic_summary_hits_do_not_inherit_assistant_artifacts_without_exact_provenance() {
        let turn_refs = ConversationTurnArtifactRefs {
            user: Vec::new(),
            assistant: vec![test_assistant_conversation_artifact_ref(Some(
                "assistant_item",
            ))],
        };
        let mut summary_hit = test_hit("thread:turn_1/summary_item/chunk_1", "summary");
        summary_hit.provenance.source_actor_role = ThreadEpisodicSourceActorRole::GeneratedSummary;
        summary_hit.provenance.item_id = ThreadEpisodicItemId("summary_item".to_owned());

        assert!(refs_for_thread_hit(&summary_hit, &turn_refs).is_empty());

        let mut exact_summary_hit = summary_hit;
        exact_summary_hit.provenance.item_id = ThreadEpisodicItemId("assistant_item".to_owned());
        let refs = refs_for_thread_hit(&exact_summary_hit, &turn_refs);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].artifact_id, "art_report");
    }

    fn prompt_context_contribution(
        response: &HookHandlerResponse,
    ) -> Option<&pioneer_hooks::PromptContextContribution> {
        response.contributions.iter().find_map(|contribution| {
            if let HookContribution::PromptContext(context) = contribution {
                Some(context)
            } else {
                None
            }
        })
    }

    #[allow(dead_code)]
    fn _assert_policy_types_are_public() {
        let _ = MemoryPromptPolicy::Full;
        let _ = MemoryReadToolPolicy::Allow;
        let _ = MemoryMutationToolPolicy::Allow;
        let _ = MemoryExtractionPolicy::Allow;
        let _ = MemoryActiveContextPolicy::Allow;
        let _ = MemoryPolicyReasonCode::DefaultAllowRead;
        let _ = MemoryPolicySource::PreMemoryClassifier;
    }
}
