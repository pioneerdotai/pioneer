//! Canonical Pioneer retrieval shared by recall and threads_search.
use super::*;
use anyhow::{anyhow, ensure};
use pioneer_memory::episodic_search::{
    EpisodicCandidateProvider, EpisodicChannelCandidates, EpisodicQueryEmbedding,
    EpisodicSearchLimitations, EpisodicSearchScope, EpisodicSearchWork, EpisodicSourceCandidate,
    fuse_episodic_ranks,
};
use std::future::Future;

pub(crate) const SEARCH_RESULTS: usize = 128;
const SEARCH_SEGMENTS: u32 = 16;
const CHANNEL_CANDIDATES: u32 = 128;
const VALIDATION_WORK: usize = 1024;

#[derive(Debug, Clone)]
pub(crate) struct SearchSource {
    pub reference: EpisodicSourceCandidate,
    pub provenance: ThreadEpisodicSourceProvenance,
    pub text: String,
    pub score: f32,
    pub created_at: Option<i64>,
    /// Pioneer causal projection group, validated against the canonical row.
    dedup_key: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SourceSearchOutput {
    pub sources: Vec<SearchSource>,
    pub limitations: EpisodicSearchLimitations,
}

/// Storage discovery and paths are confined to this adapter. The common search
/// below consumes only the independent lexical/semantic candidate contracts.
struct MemvidCandidateAdapter<'a> {
    service: &'a ThreadEpisodicRecallService,
}

impl MemvidCandidateAdapter<'_> {
    async fn channel(
        &self,
        query: &str,
        scope: &EpisodicSearchScope,
        embedding: Option<EpisodicQueryEmbedding>,
        work: EpisodicSearchWork,
    ) -> EpisodicChannelCandidates {
        let capsules = match self
            .service
            .crud_store
            .list_thread_episodic_workspace_capsules(
                &scope.workspace_id,
                u64::from(work.max_partitions.min(SEARCH_SEGMENTS)) + 1,
            )
            .await
        {
            Ok(capsules) => capsules,
            Err(_) => {
                return EpisodicChannelCandidates {
                    unavailable: true,
                    ..Default::default()
                };
            }
        };
        let coverage_limited = capsules.len() > work.max_partitions.min(SEARCH_SEGMENTS) as usize;
        let mut unavailable = false;
        let segments = capsules
            .into_iter()
            .take(work.max_partitions.min(SEARCH_SEGMENTS) as usize)
            .filter_map(|capsule| {
                if capsule.status != ThreadEpisodicCapsuleStatus::Active
                    || capsule.repair_status != ThreadEpisodicRepairStatus::Ok
                    || !matches!(
                        capsule.write_state,
                        ThreadEpisodicCapsuleWriteState::ActiveWrite
                            | ThreadEpisodicCapsuleWriteState::ReadOnly
                            | ThreadEpisodicCapsuleWriteState::Full
                    )
                {
                    unavailable = true;
                    return None;
                }
                Some(ThreadEpisodicMemvidSearchSegment {
                    capsule_id: capsule.id,
                    capsule_ref: capsule.capsule_ref,
                    storage_uri: capsule.storage_uri,
                    segment_index: capsule.segment_index,
                })
            })
            .collect();
        let filter = match scope.thread_id.as_deref() {
            Some(thread) => match thread_episodic_thread_uri_prefix(&scope.workspace_id, thread) {
                Ok(prefix) => Some(prefix),
                Err(_) => {
                    return EpisodicChannelCandidates {
                        unavailable: true,
                        ..Default::default()
                    };
                }
            },
            None => None,
        };
        let mut profile =
            ThreadEpisodicSearchProfile::for_kind(ThreadEpisodicSearchProfileKind::DefaultContext);
        profile.max_segments = work.max_partitions.min(SEARCH_SEGMENTS).max(1);
        profile.max_candidates = work.candidates_per_partition.min(CHANNEL_CANDIDATES).max(1);
        // Direct APIs do not apply profile relevancy, recency or adaptive cutoffs.
        let mut output = self
            .service
            .backend
            .source_candidates(
                ThreadEpisodicMemvidSearchRequest {
                    workspace_id: scope.workspace_id.clone(),
                    thread_id: scope.thread_id.clone().unwrap_or_default(),
                    query: query.to_owned(),
                    scope: filter,
                    profile,
                    segments,
                    exact_source: None,
                },
                embedding,
            )
            .await;
        output.coverage_limited |= coverage_limited;
        output.unavailable |= unavailable;
        output
    }
}

#[async_trait]
impl EpisodicCandidateProvider for MemvidCandidateAdapter<'_> {
    async fn lexical(
        &self,
        query: &str,
        scope: &EpisodicSearchScope,
        work: EpisodicSearchWork,
    ) -> EpisodicChannelCandidates {
        self.channel(query, scope, None, work).await
    }
    async fn semantic(
        &self,
        query: &str,
        scope: &EpisodicSearchScope,
        embedding: &EpisodicQueryEmbedding,
        work: EpisodicSearchWork,
    ) -> EpisodicChannelCandidates {
        self.channel(query, scope, Some(embedding.clone()), work)
            .await
    }
}

impl ThreadEpisodicRecallService {
    pub(crate) async fn search_sources<F, Fut>(
        &self,
        query: &str,
        scope: EpisodicSearchScope,
        readable: F,
    ) -> Result<SourceSearchOutput>
    where
        F: FnMut(String) -> Fut,
        Fut: Future<Output = bool>,
    {
        ensure!(
            !query.trim().is_empty() && query.len() <= 4096,
            "query must contain 1..4096 bytes"
        );
        let permit = Arc::new(
            self.search_capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| anyhow!("thread search is busy; retry later"))?,
        );
        let config = self.config.read().map(|c| c.clone()).unwrap_or_default();
        let vector =
            self.vector_search_config_for_workspace(&scope.workspace_id, &config.vector_search);
        let vector_enabled = config.vector_search_enabled || vector.has_selected_embedding_model();
        let target = self.recall_projection_target(&scope.workspace_id, &config);
        let gate = self
            .resolve_recall_projection_gate(&scope.workspace_id, vector_enabled, &target)
            .await?;
        if !gate.search_allowed {
            return Ok(SourceSearchOutput {
                limitations: EpisodicSearchLimitations {
                    coverage_limited: true,
                    semantic_degraded: vector_enabled,
                    ..Default::default()
                },
                ..Default::default()
            });
        }
        let semantic_ready = gate.search_path != ThreadEpisodicRecallSearchPath::Lexical;
        let mut degraded = vector_enabled && !semantic_ready;
        let embedding = if vector_enabled && semantic_ready {
            let provider = match self.embedding_provider_resolver.as_ref() {
                Some(resolver) => resolver
                    .resolve_active_embedding_provider(&scope.workspace_id)
                    .await
                    .ok()
                    .flatten(),
                None => None,
            };
            match provider {
                Some(provider) => {
                    let query = query.to_owned();
                    // Retain capacity until a synchronous query provider really
                    // finishes, including cancellation of its owning request.
                    let permit = permit.clone();
                    match tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        let identity = provider.identity();
                        provider
                            .embed_query_checked(&query)
                            .map(|vector| EpisodicQueryEmbedding { identity, vector })
                    })
                    .await
                    {
                        Ok(Ok(embedding)) => Some(embedding),
                        _ => {
                            degraded = true;
                            None
                        }
                    }
                }
                None => {
                    degraded = true;
                    None
                }
            }
        } else {
            None
        };
        let adapter = MemvidCandidateAdapter { service: self };
        self.search_sources_from_channels(
            query,
            &scope,
            &adapter,
            &adapter,
            embedding.as_ref(),
            degraded,
            readable,
        )
        .await
    }

    pub(super) async fn search_sources_from_channels<F, Fut>(
        &self,
        query: &str,
        scope: &EpisodicSearchScope,
        lexical: &dyn EpisodicCandidateProvider,
        semantic: &dyn EpisodicCandidateProvider,
        embedding: Option<&EpisodicQueryEmbedding>,
        degraded: bool,
        mut readable: F,
    ) -> Result<SourceSearchOutput>
    where
        F: FnMut(String) -> Fut,
        Fut: Future<Output = bool>,
    {
        let work = EpisodicSearchWork {
            max_partitions: SEARCH_SEGMENTS,
            candidates_per_partition: CHANNEL_CANDIDATES,
        };
        let lexical = lexical.lexical(query, scope, work).await;
        let semantic = match embedding {
            Some(embedding) => semantic.semantic(query, scope, embedding, work).await,
            None => EpisodicChannelCandidates::default(),
        };
        let mut limitations = EpisodicSearchLimitations {
            coverage_limited: lexical.coverage_limited
                || semantic.coverage_limited
                || lexical.unavailable
                || semantic.unavailable,
            semantic_approximate: embedding.is_some(),
            semantic_degraded: degraded || semantic.unavailable,
            ..Default::default()
        };
        let mut validated = BTreeMap::<String, Option<SearchSource>>::new();
        let mut canonical = BTreeMap::<String, SearchSource>::new();
        let mut channels = Vec::new();
        let mut work = 0;
        for channel in [lexical, semantic] {
            let mut partitions = Vec::new();
            let partition_work = (VALIDATION_WORK / 2 / channel.partitions.len().max(1)).max(1);
            for partition in channel.partitions {
                limitations.coverage_limited |= partition.len() > partition_work;
                let mut ids = Vec::new();
                for candidate in partition.into_iter().take(partition_work) {
                    if work == VALIDATION_WORK {
                        limitations.coverage_limited = true;
                        break;
                    }
                    work += 1;
                    if candidate.workspace_id != scope.workspace_id
                        || scope
                            .thread_id
                            .as_ref()
                            .is_some_and(|id| id != &candidate.thread_id)
                    {
                        continue;
                    }
                    // Cache only within this request. Never cache an authorization
                    // grant or trust indexed snippets across requests/pages.
                    let cache_key =
                        serde_json::to_string(&(&candidate.source_id, &candidate.source_version))?;
                    if !validated.contains_key(&cache_key) {
                        let source = if readable(candidate.thread_id.clone()).await {
                            match self.validate_search_source(&candidate).await {
                                Ok(source) => source,
                                Err(_) => {
                                    limitations.coverage_limited = true;
                                    None
                                }
                            }
                        } else {
                            None
                        };
                        validated.insert(cache_key.clone(), source);
                    }
                    if let Some(source) = validated.get(&cache_key).and_then(Option::as_ref) {
                        let key = source.dedup_key.clone();
                        ids.push(key.clone());
                        // Multiple projections cannot multiply a channel's vote.
                        match canonical.get(&key) {
                            Some(old)
                                if (
                                    canonical_source_key(&old.reference),
                                    &old.reference.source_id,
                                ) <= (
                                    canonical_source_key(&source.reference),
                                    &source.reference.source_id,
                                ) => {}
                            _ => {
                                canonical.insert(key, source.clone());
                            }
                        }
                    }
                }
                partitions.push(ids);
            }
            channels.push(partitions);
        }
        // Resolve causal groups to real canonical source identifiers before
        // any tie-breaking. Replicas in different threads remain one vote.
        let names = canonical
            .iter()
            .map(|(group, source)| (group.clone(), canonical_source_key(&source.reference)))
            .collect::<BTreeMap<_, _>>();
        for partitions in &mut channels {
            for partition in partitions {
                for id in partition {
                    *id = names
                        .get(id.as_str())
                        .expect("validated source group")
                        .clone();
                }
            }
        }
        let mut canonical = canonical
            .into_values()
            .map(|source| (canonical_source_key(&source.reference), source))
            .collect::<BTreeMap<_, _>>();
        let ranks = fuse_episodic_ranks(&channels);
        limitations.selection_limited = ranks.len() > SEARCH_RESULTS;
        let sources = ranks
            .into_iter()
            .take(SEARCH_RESULTS)
            .filter_map(|(key, score)| {
                canonical.remove(&key).map(|mut source| {
                    source.score = score;
                    source
                })
            })
            .collect();
        Ok(SourceSearchOutput {
            sources,
            limitations,
        })
    }

    pub(crate) async fn validate_search_source(
        &self,
        reference: &EpisodicSourceCandidate,
    ) -> Result<Option<SearchSource>> {
        let Some(item) = self
            .crud_store
            .find_thread_episodic_item(&reference.source_id)
            .await?
        else {
            return Ok(None);
        };
        if item.workspace_id != reference.workspace_id
            || item.thread_id != reference.thread_id
            || item.turn_id != reference.turn_id
            || item.item_id != reference.item_id
            || item.source_text_hash != reference.source_version
            || item.deleted_at.is_some()
            || item.status != ThreadEpisodicItemStatus::Active
            || item.turn_id.is_empty()
            || !matches!(
                item.visibility,
                ThreadEpisodicItemVisibility::UserVisible
                    | ThreadEpisodicItemVisibility::ParentVisible
            )
            || !thread_episodic_source_context_is_recallable(&item.source_context)
        {
            return Ok(None);
        }
        let Some(thread) = self.crud_store.get_thread_by_id(&item.thread_id).await? else {
            return Ok(None);
        };
        if thread.workspace_id != reference.workspace_id {
            return Ok(None);
        }
        let Some((workspace, turn)) = self
            .crud_store
            .get_turn(&item.thread_id, &item.turn_id)
            .await?
        else {
            return Ok(None);
        };
        if workspace != reference.workspace_id || turn.message_deleted {
            return Ok(None);
        }
        if self
            .crud_store
            .thread_episodic_source_occurrence_is_excluded(
                &item.workspace_id,
                &item.thread_id,
                &item.turn_id,
                &item.item_id,
            )
            .await?
        {
            return Ok(None);
        }
        let text = self
            .reconstruct_item_text(&item)
            .await
            .map_err(|_| anyhow!("source unavailable"))?;
        if text.trim().is_empty() || looks_secret_like(&text) {
            return Ok(None);
        }
        Ok(Some(SearchSource {
            dedup_key: if item.projection_group_id.is_empty() {
                canonical_source_key(reference)
            } else {
                serde_json::to_string(&(&item.workspace_id, &item.projection_group_id))?
            },
            reference: reference.clone(),
            provenance: provenance_from_item(&item),
            text: cap_string_chars(&text, 1200),
            score: 0.0,
            created_at: Some(item.created_at.timestamp()),
        }))
    }
}

fn canonical_source_key(source: &EpisodicSourceCandidate) -> String {
    serde_json::to_string(&(
        &source.workspace_id,
        &source.thread_id,
        &source.turn_id,
        &source.item_id,
    ))
    .expect("string tuple")
}
