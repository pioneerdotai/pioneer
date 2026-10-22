//! Shared, authorized retrieval and stateless thread-card selection.
//! See SEARCH_REVIEW.md for the continuation proof and cost model.
use super::*;
use crate::authorization::{
    AuthorizationResolver, AuthorizationService, ExecutionAuthorizationContext, ProofResolution,
    ResourceAction, RuntimePrincipalPolicy, collaboration_authority_principal,
};
use anyhow::{Context, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};

const PAGE_MAX: usize = 20;
const SOURCES_PER_THREAD: usize = 3;
const WORK_QUANTA: usize = 32;
const CURSOR_MAX: usize = 65_536;
const CURSOR_TTL: i64 = 900;

#[derive(Clone)]
pub(crate) struct EpisodicSearchAuthority {
    workspace_id: String,
    execution_thread_id: String,
    turn_id: String,
    principal: crate::auth::AuthenticatedSessionPrincipal,
    root_thread_id: String,
    root_only: bool,
    only_thread_id: Option<String>,
    fingerprint: String,
    policy_generation: u64,
}

impl EpisodicSearchAuthority {
    pub(crate) async fn resolve(
        store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        principal_hint: Option<&str>,
    ) -> Result<Self> {
        let policy_generation = crate::authorization::observed_policy_generation();
        let execution = ExecutionAuthorizationContext::load_for_turn(store, turn_id).await?;
        execution
            .verify_turn_scope(store, workspace_id, thread_id, turn_id)
            .await?;
        if principal_hint.is_none_or(|id| id != execution.initiating_principal_id().as_str()) {
            bail!("episodic search authority unavailable");
        }
        let principal =
            collaboration_authority_principal(store, execution.initiating_principal_id()).await?;
        execution
            .revalidate_root_for_collaborator(
                store,
                &principal,
                ResourceAction::ThreadRead,
                policy_generation,
            )
            .await?;
        let policy = AuthorizationService::new();
        let root_only = execution.runtime_principal_policy()? != RuntimePrincipalPolicy::Absolute
            || policy.runtime_principal_policy(principal.kind, principal.role_key.as_ref())
                != Some(RuntimePrincipalPolicy::Absolute);
        let fingerprint = stable_text_hash(&format!(
            "threads_search:v1:{turn_id}:{}:{:?}:{:?}:{}",
            execution.to_persisted_json()?,
            principal.kind,
            principal.role_key,
            policy_generation
        ));
        Ok(Self {
            workspace_id: workspace_id.to_owned(),
            execution_thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            principal,
            root_thread_id: execution.root_thread_id().to_owned(),
            root_only,
            only_thread_id: None,
            fingerprint,
            policy_generation,
        })
    }

    pub(crate) async fn resolve_for_turn(
        store: &CrudStore,
        workspace_id: &str,
        turn_id: &str,
        principal_hint: Option<&str>,
    ) -> Result<Self> {
        let execution = ExecutionAuthorizationContext::load_for_turn(store, turn_id).await?;
        let principal = execution.initiating_principal_id().as_str();
        if principal_hint.is_some_and(|hint| hint != principal) {
            bail!("episodic search authority unavailable");
        }
        let turn = pioneer_crud::resolve_turn_authorization_scope(
            &store.database_connection(),
            turn_id,
            Some(workspace_id),
            None,
        )
        .await?
        .context("episodic search authority unavailable")?;
        Self::resolve(
            store,
            workspace_id,
            &turn.thread_id,
            turn_id,
            Some(principal),
        )
        .await
    }

    pub(crate) fn current_thread_only(mut self, thread_id: &str) -> Self {
        self.only_thread_id = Some(thread_id.to_owned());
        self
    }

    async fn allows(&self, store: &CrudStore, thread_id: &str) -> Result<bool> {
        if crate::authorization::observed_policy_generation() != self.policy_generation {
            return Ok(false);
        }
        let Ok(current_principal) =
            collaboration_authority_principal(store, &self.principal.principal_id).await
        else {
            return Ok(false);
        };
        if current_principal.kind != self.principal.kind
            || current_principal.role_key != self.principal.role_key
        {
            return Ok(false);
        }
        if self
            .only_thread_id
            .as_deref()
            .is_some_and(|only| only != thread_id)
        {
            return Ok(false);
        }
        let policy = AuthorizationService::new();
        let gate = policy.authorize_action(
            self.principal.kind,
            self.principal.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );
        let resolver = AuthorizationResolver::new(store.clone());
        let Some(thread) = store.get_thread_by_id(thread_id).await? else {
            return Ok(false);
        };
        let proof = if thread.access_class == "internal" {
            resolver
                .authorize_internal_thread_via_root(
                    &self.principal,
                    &gate,
                    ResourceAction::ThreadRead,
                    thread_id,
                    Some(&self.workspace_id),
                )
                .await?
        } else {
            resolver
                .authorize_thread(
                    &self.principal,
                    &gate,
                    ResourceAction::ThreadRead,
                    thread_id,
                    Some(&self.workspace_id),
                )
                .await?
        };
        Ok(matches!(proof, ProofResolution::Authorized(proof)
            if !self.root_only || proof.collaboration_root_thread_id() == self.root_thread_id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ThreadSearchSource {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub source_kind: ThreadEpisodicSourceContext,
    pub text: String,
    pub relevance: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ThreadSearchCard {
    pub thread_id: String,
    pub title: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub relevance: f32,
    pub fragments: Vec<ThreadSearchSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ThreadSearchPage {
    pub threads: Vec<ThreadSearchCard>,
    pub next_cursor: Option<String>,
    /// exhausted, more, work_budget, index_unavailable, or partial.
    pub status: String,
    pub approximate: bool,
    pub incomplete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SourceRef {
    index_item_id: String,
    source_identity: String,
    score: f32,
    excerpt_start: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Selection {
    thread_id: String,
    sources: Vec<SourceRef>,
}
impl Selection {
    fn score(&self) -> f32 {
        self.sources.first().map_or(0.0, |source| source.score)
    }
    fn add(&mut self, source: SourceRef) {
        if let Some(existing) = self.sources.iter_mut().find(|s| {
            s.index_item_id == source.index_item_id || s.source_identity == source.source_identity
        }) {
            if source.score > existing.score
                || (source.score == existing.score && source.index_item_id < existing.index_item_id)
            {
                *existing = source;
            }
        } else {
            self.sources.push(source);
        }
        self.sources.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.index_item_id.cmp(&b.index_item_id))
        });
        self.sources.truncate(SOURCES_PER_THREAD);
    }
}
fn selection_order(a: &Selection, b: &Selection) -> std::cmp::Ordering {
    b.score()
        .total_cmp(&a.score())
        .then_with(|| a.thread_id.cmp(&b.thread_id))
}

#[derive(Clone, Serialize, Deserialize)]
struct SearchCursor {
    binding: String,
    snapshot: String,
    expires: i64,
    after: Option<Selection>,
    completed_thread: Option<String>,
    current: Option<Selection>,
    segment_after: Option<i64>,
    native_cursor: Option<String>,
    best: Vec<Selection>,
    approximate: bool,
    incomplete: bool,
}
fn cursor_key() -> &'static [u8; 32] {
    static KEY: std::sync::OnceLock<[u8; 32]> = std::sync::OnceLock::new();
    KEY.get_or_init(|| {
        let mut key = [0; 32];
        rand::fill(&mut key);
        key
    })
}
fn frontier_token(thread_id: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(cursor_key()).expect("HMAC key length");
    mac.update(b"pioneer.threads_search.frontier.v1\0");
    mac.update(thread_id.as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}
fn encode_cursor(cursor: &SearchCursor) -> Result<String> {
    // The discovery frontier may have crossed denied threads. An authenticated
    // plaintext cursor must never disclose their identifiers. Resolve this
    // non-bearer token against bounded index metadata on continuation instead.
    let mut opaque = cursor.clone();
    opaque.completed_thread = opaque.completed_thread.as_deref().map(frontier_token);
    let payload = serde_json::to_vec(&opaque)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(cursor_key()).expect("HMAC key length");
    mac.update(b"pioneer.threads_search.v1\0");
    mac.update(&payload);
    let mut envelope = payload;
    envelope.extend_from_slice(&mac.finalize().into_bytes());
    let encoded = URL_SAFE_NO_PAD.encode(envelope);
    if encoded.len() > CURSOR_MAX {
        bail!("episodic search continuation unavailable");
    }
    Ok(encoded)
}
fn decode_cursor(encoded: &str, binding: &str, snapshot: &str) -> Result<SearchCursor> {
    if encoded.len() > CURSOR_MAX {
        bail!("invalid or stale search cursor");
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| anyhow::anyhow!("invalid or stale search cursor"))?;
    if bytes.len() < 32 {
        bail!("invalid or stale search cursor");
    }
    let (payload, signature) = bytes.split_at(bytes.len() - 32);
    let mut mac = Hmac::<Sha256>::new_from_slice(cursor_key()).expect("HMAC key length");
    mac.update(b"pioneer.threads_search.v1\0");
    mac.update(payload);
    mac.verify_slice(signature)
        .map_err(|_| anyhow::anyhow!("invalid or stale search cursor"))?;
    let cursor: SearchCursor = serde_json::from_slice(payload)
        .map_err(|_| anyhow::anyhow!("invalid or stale search cursor"))?;
    if cursor.binding != binding
        || cursor.snapshot != snapshot
        || cursor.expires < chrono::Utc::now().timestamp()
    {
        bail!("invalid or stale search cursor; start a new search");
    }
    Ok(cursor)
}

struct PreparedSearch {
    embedder: Option<Arc<ThreadEpisodicMemvidEmbedder>>,
    unavailable: bool,
    degraded: bool,
    config: ThreadEpisodicRecallServiceConfig,
}

impl ThreadEpisodicRecallService {
    async fn prepare_shared_search(
        &self,
        workspace_id: &str,
        query: &str,
    ) -> Result<PreparedSearch> {
        let config = self.config.read().map(|c| c.clone()).unwrap_or_default();
        if !config.enabled {
            return Ok(PreparedSearch {
                embedder: None,
                unavailable: true,
                degraded: false,
                config,
            });
        }
        let vector = self.vector_search_config_for_workspace(workspace_id, &config.vector_search);
        let vector_enabled = config.vector_search_enabled || vector.has_selected_embedding_model();
        let target = self.recall_projection_target(workspace_id, &config);
        let gate = self
            .resolve_recall_projection_gate(workspace_id, vector_enabled, &target)
            .await?;
        let mut prepared = PreparedSearch {
            embedder: None,
            unavailable: !gate.search_allowed,
            degraded: vector_enabled
                && gate.search_path != ThreadEpisodicRecallSearchPath::HybridAsk,
            config,
        };
        if gate.search_allowed && gate.search_path == ThreadEpisodicRecallSearchPath::HybridAsk {
            match self.resolve_hybrid_recall_embedder(workspace_id).await {
                Ok(embedder) => {
                    let query = query.to_owned();
                    match tokio::task::spawn_blocking(move || embedder.prepare_query(&query)).await
                    {
                        Ok(Ok(embedder)) => prepared.embedder = Some(Arc::new(embedder)),
                        _ => prepared.degraded = true,
                    }
                }
                Err(_) => prepared.degraded = true,
            }
        }
        Ok(prepared)
    }

    async fn capsule_after(
        &self,
        workspace_id: &str,
        after: Option<i64>,
    ) -> Result<Option<ThreadEpisodicCapsuleRecord>> {
        Ok(self
            .crud_store
            .list_thread_episodic_workspace_capsules_after(workspace_id, after, 1)
            .await?
            .into_iter()
            .next())
    }

    async fn search_snapshot(
        &self,
        authority: &EpisodicSearchAuthority,
        prepared: &PreparedSearch,
    ) -> Result<(String, bool)> {
        if crate::authorization::observed_policy_generation() != authority.policy_generation {
            bail!("search authority changed; start a new search");
        }
        let mut digest = Sha256::new();
        digest.update(authority.fingerprint.as_bytes());
        let vector = self.vector_search_config_for_workspace(
            &authority.workspace_id,
            &prepared.config.vector_search,
        );
        digest.update(serde_json::to_vec(&vector)?);
        digest.update(format!(
            "{:?}:{:?}",
            prepared.config,
            prepared
                .embedder
                .as_ref()
                .map(|embedder| embedder.identity())
        ));
        if let Some(vector) = prepared
            .embedder
            .as_ref()
            .and_then(|embedder| embedder.prepared_query_vector())
        {
            for coordinate in vector {
                digest.update(coordinate.to_bits().to_le_bytes());
            }
        }
        let mut after = None;
        let mut searchable_capsules = false;
        let mut incomplete = false;
        loop {
            let batch = self
                .crud_store
                .list_thread_episodic_workspace_capsules_after(&authority.workspace_id, after, 64)
                .await?;
            if batch.is_empty() {
                break;
            }
            for capsule in batch {
                after = Some(capsule.segment_index);
                digest.update(format!("{capsule:?}"));
                if capsule_searchable(&capsule) {
                    searchable_capsules = true;
                    let generation = self
                        .backend
                        .segment_generation(segment_from_capsule(&capsule))
                        .await
                        .map_err(|_| anyhow::anyhow!("episodic index unavailable"))?;
                    digest.update(generation.to_le_bytes());
                } else if capsule.status != ThreadEpisodicCapsuleStatus::Deleted {
                    incomplete = true;
                }
            }
        }
        // Include source tombstones/exclusions and projection replacements even
        // when capsule files have not yet caught up with a canonical mutation.
        let mut source_after = None;
        loop {
            let batch = self
                .crud_store
                .list_thread_episodic_search_source_versions(
                    &authority.workspace_id,
                    source_after.as_deref(),
                    128,
                )
                .await?;
            if batch.is_empty() {
                break;
            }
            for item in batch {
                source_after = Some(item.id.clone());
                if matches!(
                    item.status,
                    ThreadEpisodicItemStatus::PendingIndex | ThreadEpisodicItemStatus::IndexFailed
                ) || (item.status == ThreadEpisodicItemStatus::Active
                    && item.capsule_id.is_none())
                {
                    incomplete = true;
                }
                digest.update(format!("{item:?}"));
            }
        }
        if !searchable_capsules && source_after.is_some() {
            bail!("episodic index unavailable");
        }
        Ok((format!("{:x}", digest.finalize()), incomplete))
    }

    async fn restore_frontier(&self, workspace_id: &str, state: &mut SearchCursor) -> Result<()> {
        let Some(token) = state.completed_thread.as_ref() else {
            return Ok(());
        };
        let mut after = None;
        loop {
            let ids = self
                .crud_store
                .list_thread_episodic_search_thread_ids(workspace_id, after.as_deref(), 128)
                .await?;
            if ids.is_empty() {
                bail!("invalid or stale search cursor; start a new search");
            }
            for id in ids {
                if frontier_token(&id) == *token {
                    state.completed_thread = Some(id);
                    return Ok(());
                }
                after = Some(id);
            }
        }
    }

    /// Both workspace recall and threads_search call this retrieval quantum.
    async fn retrieve_authorized_quantum(
        &self,
        authority: &EpisodicSearchAuthority,
        query: &str,
        thread_id: &str,
        capsule: &ThreadEpisodicCapsuleRecord,
        cursor: Option<String>,
        prepared: &PreparedSearch,
        candidate_budget: Option<usize>,
    ) -> Result<(
        Vec<HydratedThreadEpisodicHit>,
        Option<String>,
        bool,
        bool,
        usize,
    )> {
        if !authority.allows(&self.crud_store, thread_id).await? {
            return Ok((Vec::new(), None, false, false, 0));
        }
        if !capsule_searchable(capsule) {
            return Ok((Vec::new(), None, false, true, 0));
        }
        let mut profile = ThreadEpisodicSearchProfile::for_kind(
            ThreadEpisodicSearchProfileKind::HighRecallContinuation,
        );
        profile.max_candidates = prepared
            .config
            .default_max_candidates
            .clamp(1, prepared.config.max_candidate_work.max(1));
        profile.max_segments = 1;
        profile.snippet_chars = prepared.config.snippet_chars;
        let request = ThreadEpisodicMemvidSearchRequest {
            workspace_id: authority.workspace_id.clone(),
            thread_id: thread_id.to_owned(),
            query: query.to_owned(),
            scope: Some(thread_episodic_thread_uri_prefix(
                &authority.workspace_id,
                thread_id,
            )?),
            profile,
            segments: vec![segment_from_capsule(capsule)],
            exact_source: None,
        };
        let output = self
            .backend
            .retrieve_segment(request, cursor, prepared.embedder.clone())
            .await
            .map_err(|_| anyhow::anyhow!("episodic index unavailable"))?;
        let examined = output
            .hits
            .len()
            .min(candidate_budget.unwrap_or(usize::MAX));
        let overflow = examined < output.hits.len();
        let mut hits = Vec::new();
        for ranked in output.hits.iter().take(examined) {
            // Never hydrate or report a candidate from an unauthorized thread.
            if ranked.hit.workspace_id != authority.workspace_id
                || ranked.hit.thread_id != thread_id
            {
                continue;
            }
            match self
                .hydrate_search_source(&authority.workspace_id, thread_id, ranked)
                .await
            {
                Ok(Some(hit)) => hits.push(hit),
                Err(message) if message.starts_with("failed to") => {
                    bail!("episodic source unavailable")
                }
                Ok(None) | Err(_) => {}
            }
        }
        let filtered_all_candidates = !output.hits.is_empty() && hits.is_empty();
        hits.retain(|hit| {
            hit.hit.score.is_finite() && hit.hit.score >= prepared.config.min_relevancy
        });
        Ok((
            deduplicate_thread_episodic_hits(hits).hits,
            output.next_cursor,
            output.approximate,
            output.incomplete || overflow || (output.approximate && filtered_all_candidates),
            examined,
        ))
    }

    async fn scan_shared(
        &self,
        authority: &EpisodicSearchAuthority,
        query: &str,
        limit: usize,
        prepared: &PreparedSearch,
        state: &mut SearchCursor,
        work_quanta: usize,
        recall_budget: Option<(usize, usize)>,
    ) -> Result<(bool, bool)> {
        let mut remaining = recall_budget.map(|(candidates, _)| candidates);
        let mut seen_capsules = BTreeSet::new();
        let mut complete = false;
        for _ in 0..work_quanta {
            if state.current.is_none() {
                let next = if let Some(thread_id) = authority.only_thread_id.as_ref() {
                    if state.completed_thread.is_some() {
                        Vec::new()
                    } else {
                        vec![thread_id.clone()]
                    }
                } else {
                    self.crud_store
                        .list_thread_episodic_search_thread_ids(
                            &authority.workspace_id,
                            state.completed_thread.as_deref(),
                            1,
                        )
                        .await?
                };
                let Some(thread_id) = next.into_iter().next() else {
                    complete = true;
                    break;
                };
                if !authority.allows(&self.crud_store, &thread_id).await? {
                    state.completed_thread = Some(thread_id);
                    continue;
                }
                state.current = Some(Selection {
                    thread_id,
                    sources: Vec::new(),
                });
                state.segment_after = None;
                state.native_cursor = None;
            }
            let thread_id = state
                .current
                .as_ref()
                .expect("current selection")
                .thread_id
                .clone();
            let capsule = self
                .capsule_after(&authority.workspace_id, state.segment_after)
                .await?;
            let Some(capsule) = capsule else {
                if state.segment_after.is_none()
                    && !self
                        .crud_store
                        .list_thread_episodic_items_for_thread(
                            &authority.workspace_id,
                            &thread_id,
                            1,
                        )
                        .await?
                        .is_empty()
                {
                    state.incomplete = true;
                }
                let selection = state.current.take().expect("current selection");
                state.completed_thread = Some(selection.thread_id.clone());
                if !selection.sources.is_empty()
                    && state
                        .after
                        .as_ref()
                        .is_none_or(|after| selection_order(&selection, after).is_gt())
                {
                    state.best.push(selection);
                    state.best.sort_by(selection_order);
                    state.best.truncate(limit + 1);
                }
                continue;
            };
            if let Some((_, max_segments)) = recall_budget {
                if !seen_capsules.contains(&capsule.id) && seen_capsules.len() >= max_segments {
                    state.incomplete = true;
                    return Ok((false, false));
                }
                seen_capsules.insert(capsule.id.clone());
            }
            match self
                .retrieve_authorized_quantum(
                    &authority,
                    query,
                    &thread_id,
                    &capsule,
                    state.native_cursor.clone(),
                    &prepared,
                    remaining,
                )
                .await
            {
                Ok((hits, next, approximate, incomplete, examined)) => {
                    state.approximate |= approximate;
                    state.incomplete |= incomplete;
                    for hit in hits {
                        state
                            .current
                            .as_mut()
                            .expect("current selection")
                            .add(SourceRef {
                                source_identity: format!(
                                    "{}/{}/{}/{}",
                                    hit.hit.provenance.workspace_id.0,
                                    hit.hit.provenance.thread_id.0,
                                    hit.hit.provenance.turn_id.0,
                                    hit.hit.provenance.item_id.0
                                ),
                                index_item_id: hit.hit.provenance.index_item_id.0,
                                score: hit.hit.score,
                                excerpt_start: hit.excerpt_start,
                            });
                    }
                    if let Some(next) = next {
                        state.native_cursor = Some(next);
                    } else {
                        state.segment_after = Some(capsule.segment_index);
                        state.native_cursor = None;
                    }
                    if let Some(budget) = remaining.as_mut() {
                        *budget = budget.saturating_sub(examined);
                        if *budget == 0 {
                            state.incomplete = true;
                            return Ok((false, false));
                        }
                    }
                }
                Err(_) => {
                    // Preserve the frontier: retrying this cursor retries the failed quantum.
                    return Ok((false, true));
                }
            }
        }
        Ok((complete, false))
    }

    /// Recall uses the same retrieval/source checks/selection as the tool, then
    /// applies its own fragment and prompt budgets. No second current-thread scan.
    pub(crate) async fn recall_authorized_workspace(
        &self,
        authority: EpisodicSearchAuthority,
        query: &str,
        max_hits: usize,
        max_chars: usize,
    ) -> Result<ThreadEpisodicRecallOutput> {
        let started_at = Instant::now();
        let input = ThreadEpisodicRecallInput {
            workspace_id: ThreadEpisodicWorkspaceId(authority.workspace_id.clone()),
            thread_id: ThreadEpisodicThreadId(authority.execution_thread_id.clone()),
            turn_id: ThreadEpisodicTurnId(authority.turn_id.clone()),
            query_text: query.to_owned(),
            recent_context_summary: None,
            policy_context: ThreadEpisodicRecallPolicyContext {
                context_recall_allowed: true,
                include_sensitive_context: false,
            },
            max_prompt_chars: Some(max_chars.min(u32::MAX as usize) as u32),
            max_candidates: Some(max_hits.min(u32::MAX as usize) as u32),
        };
        let output = self
            .run_authorized_recall(authority, query, max_hits, max_chars)
            .await
            .unwrap_or_else(|_| ThreadEpisodicRecallOutput {
                hits: Vec::new(),
                diagnostics: vec![recall_diagnostic(
                    ThreadEpisodicRecallDiagnosticCode::BackendUnavailable,
                    "episodic recall is temporarily unavailable",
                )],
                fallback_used: true,
            });
        Ok(self
            .finish_recall(&input, None, None, output, started_at, None)
            .await)
    }

    async fn run_authorized_recall(
        &self,
        authority: EpisodicSearchAuthority,
        query: &str,
        max_hits: usize,
        max_chars: usize,
    ) -> Result<ThreadEpisodicRecallOutput> {
        if query.trim().is_empty() {
            return Ok(ThreadEpisodicRecallOutput {
                hits: Vec::new(),
                diagnostics: vec![recall_diagnostic(
                    ThreadEpisodicRecallDiagnosticCode::InvalidInput,
                    "episodic recall query is required",
                )],
                fallback_used: true,
            });
        }
        let prepared = self
            .prepare_shared_search(&authority.workspace_id, query)
            .await?;
        if prepared.unavailable {
            return Ok(ThreadEpisodicRecallOutput {
                hits: Vec::new(),
                diagnostics: vec![recall_diagnostic(
                    ThreadEpisodicRecallDiagnosticCode::BackendUnavailable,
                    "episodic index is unavailable or still being prepared",
                )],
                fallback_used: true,
            });
        }
        let max_hits = max_hits.clamp(1, 40);
        let mut state = SearchCursor {
            binding: String::new(),
            snapshot: String::new(),
            expires: 0,
            after: None,
            completed_thread: None,
            current: None,
            segment_after: None,
            native_cursor: None,
            best: Vec::new(),
            approximate: false,
            incomplete: prepared.degraded,
        };
        let (complete, unavailable) = self
            .scan_shared(
                &authority,
                query,
                max_hits,
                &prepared,
                &mut state,
                WORK_QUANTA,
                Some((
                    prepared.config.max_candidate_work.max(1) as usize,
                    prepared.config.max_segments.max(1) as usize,
                )),
            )
            .await?;
        if let Some(current) = state
            .current
            .take()
            .filter(|current| !current.sources.is_empty())
        {
            state.best.push(current);
        }
        state.best.sort_by(selection_order);
        let mut hits = Vec::new();
        for selection in state.best {
            if !authority
                .allows(&self.crud_store, &selection.thread_id)
                .await?
            {
                continue;
            }
            for source in &selection.sources {
                if let Some(hit) = self
                    .hydrate_saved_source(&authority, &selection.thread_id, source)
                    .await?
                {
                    hits.push(hit);
                }
            }
        }
        let (mut hits, _) = deduplicate_cross_thread_projection_hits(&self.crud_store, hits).await;
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.provenance.source_id.cmp(&b.provenance.source_id))
        });
        hits.retain(|hit| hit.score.is_finite() && hit.score >= prepared.config.min_relevancy);
        hits.truncate(max_hits);
        let cap = max_chars.min(prepared.config.max_prompt_chars as usize);
        let original_chars: usize = hits.iter().map(|h| h.text.chars().count()).sum();
        hits = cap_workspace_prompt_hits(hits, cap);
        let mut diagnostics = Vec::new();
        if !complete || unavailable || state.incomplete {
            diagnostics.push(recall_diagnostic(
                ThreadEpisodicRecallDiagnosticCode::BackendUnavailable,
                "episodic recall coverage is incomplete",
            ));
        }
        if original_chars > cap {
            diagnostics.push(recall_diagnostic(
                ThreadEpisodicRecallDiagnosticCode::PromptBudgetExceeded,
                "episodic recall was limited by the prompt budget",
            ));
        }
        Ok(ThreadEpisodicRecallOutput {
            hits,
            diagnostics,
            fallback_used: !complete || unavailable || state.incomplete,
        })
    }

    async fn validate_cursor_sources(
        &self,
        authority: &EpisodicSearchAuthority,
        state: &SearchCursor,
    ) -> Result<()> {
        for selection in state
            .best
            .iter()
            .chain(state.current.iter())
            .chain(state.after.iter())
        {
            if !authority
                .allows(&self.crud_store, &selection.thread_id)
                .await?
            {
                bail!("invalid or stale search cursor; start a new search");
            }
            for source in &selection.sources {
                if self
                    .hydrate_saved_source(&authority, &selection.thread_id, source)
                    .await?
                    .is_none()
                {
                    bail!("invalid or stale search cursor; start a new search");
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn search_threads(
        &self,
        authority: EpisodicSearchAuthority,
        query: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<ThreadSearchPage> {
        if query.trim().is_empty()
            || query.chars().count() > 4096
            || !(1..=PAGE_MAX).contains(&limit)
        {
            bail!("query must contain 1..4096 characters and limit must be 1..20");
        }
        let prepared = self
            .prepare_shared_search(&authority.workspace_id, query)
            .await
            .map_err(|_| anyhow::anyhow!("episodic search unavailable"))?;
        if prepared.unavailable {
            return Ok(unavailable_page());
        }
        let (snapshot, snapshot_incomplete) =
            match self.search_snapshot(&authority, &prepared).await {
                Ok(snapshot) => snapshot,
                Err(_) => return Ok(unavailable_page()),
            };
        let binding = stable_text_hash(&format!("{}:{query}:{limit}", authority.fingerprint));
        let mut state = match cursor {
            Some(cursor) => decode_cursor(cursor, &binding, &snapshot)?,
            None => SearchCursor {
                binding,
                snapshot: snapshot.clone(),
                expires: chrono::Utc::now().timestamp() + CURSOR_TTL,
                after: None,
                completed_thread: None,
                current: None,
                segment_after: None,
                native_cursor: None,
                best: Vec::new(),
                approximate: false,
                incomplete: prepared.degraded,
            },
        };
        if cursor.is_some() {
            self.restore_frontier(&authority.workspace_id, &mut state)
                .await?;
        }
        state.incomplete |= snapshot_incomplete;
        self.validate_cursor_sources(&authority, &state).await?;
        let (complete, unavailable) = self
            .scan_shared(
                &authority,
                query,
                limit,
                &prepared,
                &mut state,
                WORK_QUANTA,
                None,
            )
            .await?;
        if self.search_snapshot(&authority, &prepared).await?.0 != snapshot {
            bail!("search index changed; start a new search");
        }
        self.validate_cursor_sources(&authority, &state).await?;
        if unavailable {
            return Ok(ThreadSearchPage {
                threads: Vec::new(),
                next_cursor: Some(encode_cursor(&state)?),
                status: "index_unavailable".into(),
                approximate: state.approximate,
                incomplete: true,
            });
        }
        if !complete {
            return Ok(ThreadSearchPage {
                threads: Vec::new(),
                next_cursor: Some(encode_cursor(&state)?),
                status: "work_budget".into(),
                approximate: state.approximate,
                incomplete: true,
            });
        }
        let more = state.best.len() > limit;
        let selected: Vec<_> = state.best.iter().take(limit).cloned().collect();
        let mut cards = Vec::new();
        for selection in &selected {
            cards.push(
                self.hydrate_card(&authority, selection)
                    .await?
                    .context("search sources changed; start a new search")?,
            );
        }
        let next_cursor = if more {
            state.after = selected.last().cloned();
            state.completed_thread = None;
            state.current = None;
            state.segment_after = None;
            state.native_cursor = None;
            state.best.clear();
            Some(encode_cursor(&state)?)
        } else {
            None
        };
        Ok(ThreadSearchPage {
            threads: cards,
            next_cursor,
            status: if state.incomplete {
                "partial"
            } else if more {
                "more"
            } else {
                "exhausted"
            }
            .into(),
            approximate: state.approximate,
            incomplete: state.incomplete,
        })
    }

    async fn hydrate_saved_source(
        &self,
        authority: &EpisodicSearchAuthority,
        thread_id: &str,
        source: &SourceRef,
    ) -> Result<Option<ThreadEpisodicHit>> {
        let (item, text) = match self
            .validate_search_source(&authority.workspace_id, thread_id, &source.index_item_id)
            .await
        {
            Ok(Some(source)) => source,
            Ok(None) => return Ok(None),
            Err(message) if message.starts_with("failed to") => {
                bail!("episodic source unavailable")
            }
            Err(_) => return Ok(None),
        };
        let Some(excerpt) = text.get(source.excerpt_start..) else {
            return Ok(None);
        };
        let max_hit_chars = self
            .config
            .read()
            .map(|c| c.max_hit_chars.min(1200))
            .unwrap_or(1200);
        let text = cap_string_chars(excerpt, max_hit_chars);
        Ok(Some(ThreadEpisodicHit {
            provenance: provenance_from_item(&item),
            text,
            score: source.score,
            score_breakdown: pioneer_protocol::ThreadEpisodicScoreBreakdown {
                final_score: source.score,
                memvid_score: None,
                semantic_score: None,
                lexical_score: None,
                temporal_score: None,
                exact_source_boost: None,
                recency_boost: None,
                source_role_boost: None,
            },
            adaptive_diagnostics: None,
            created_at: Some(item.created_at.timestamp()),
        }))
    }

    async fn hydrate_card(
        &self,
        authority: &EpisodicSearchAuthority,
        selection: &Selection,
    ) -> Result<Option<ThreadSearchCard>> {
        if !authority
            .allows(&self.crud_store, &selection.thread_id)
            .await?
        {
            return Ok(None);
        }
        let Some(thread) = self
            .crud_store
            .get_thread_by_id(&selection.thread_id)
            .await?
        else {
            return Ok(None);
        };
        let mut fragments = Vec::new();
        for source in &selection.sources {
            if let Some(hit) = self
                .hydrate_saved_source(authority, &selection.thread_id, source)
                .await?
            {
                fragments.push(ThreadSearchSource {
                    workspace_id: hit.provenance.workspace_id.0,
                    thread_id: hit.provenance.thread_id.0,
                    turn_id: hit.provenance.turn_id.0,
                    item_id: hit.provenance.item_id.0,
                    source_kind: hit.provenance.source_context,
                    text: hit.text,
                    relevance: hit.score,
                });
            } else {
                return Ok(None);
            }
        }
        Ok(Some(ThreadSearchCard {
            thread_id: selection.thread_id.clone(),
            title: thread.name.map(|name| cap_string_chars(&name, 256)),
            created_at: thread.created_at.timestamp(),
            updated_at: thread.updated_at.timestamp(),
            relevance: selection.score(),
            fragments,
        }))
    }
}
fn capsule_searchable(capsule: &ThreadEpisodicCapsuleRecord) -> bool {
    capsule.status == ThreadEpisodicCapsuleStatus::Active
        && capsule.repair_status == ThreadEpisodicRepairStatus::Ok
        && matches!(
            capsule.write_state,
            ThreadEpisodicCapsuleWriteState::ActiveWrite
                | ThreadEpisodicCapsuleWriteState::ReadOnly
                | ThreadEpisodicCapsuleWriteState::Full
        )
}
fn segment_from_capsule(
    capsule: &ThreadEpisodicCapsuleRecord,
) -> ThreadEpisodicMemvidSearchSegment {
    ThreadEpisodicMemvidSearchSegment {
        capsule_id: capsule.id.clone(),
        capsule_ref: capsule.capsule_ref.clone(),
        storage_uri: capsule.storage_uri.clone(),
        segment_index: capsule.segment_index,
    }
}
fn unavailable_page() -> ThreadSearchPage {
    ThreadSearchPage {
        threads: Vec::new(),
        next_cursor: None,
        status: "index_unavailable".into(),
        approximate: false,
        incomplete: true,
    }
}

#[cfg(test)]
#[path = "search_tests.rs"]
mod tests;
