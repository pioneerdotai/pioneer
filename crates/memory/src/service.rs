use crate::NoopMemoryBackend;
use crate::backend::{
    BackendDeleteRequest, BackendGetRequest, BackendPayload, BackendPutRequest,
    BackendSearchRequest, BackendSearchScope, MemoryBackend,
};
use crate::config::MemoryServiceConfig;
use crate::context::MemoryOperationContext;
use crate::convert::{
    content_preview, crud_candidate_to_protocol, crud_record_to_protocol, effective_provenance,
    metadata_with_idempotency, protocol_actor_to_crud,
};
use crate::policy::{
    MemoryPolicyEngine, POLICY_ACTION_FORGET, POLICY_ACTION_REMEMBER, POLICY_DECISION_ALLOW,
    POLICY_DECISION_ERROR,
};
use crate::ranking::{MemoryRankingCandidate, rank_memory_search_hits};
use crate::recall::{
    MemoryRecallItem, MemoryRecallParams, MemoryRecallResponse, compact_recall_content,
};
use crate::write::{
    SemanticWritePrepared, merge_metadata, metadata_normalized_value, normalize_semantic_text,
    prepare_semantic_write, semantic_metadata,
};
use anyhow::{Context, Result, bail};
use pioneer_crud::{
    AgentMemoryCandidateDecisionRecord, AgentMemoryCandidateListFilter, AgentMemoryControlRecord,
    AgentMemoryListFilter, AgentMemoryRepairJobRecord, CrudStore, NewAgentMemoryCandidate,
    NewAgentMemoryControlRecord, NewAgentMemoryPolicyDecision, NewAgentMemoryRepairJob,
};
use pioneer_protocol::{
    MemoryCandidate, MemoryCandidateDecision, MemoryCandidateStatus, MemoryCandidatesDecideParams,
    MemoryCandidatesDecideResponse, MemoryCandidatesListParams, MemoryCandidatesListResponse,
    MemoryCategory, MemoryForgetParams, MemoryForgetResponse, MemoryForgetTarget, MemoryGetParams,
    MemoryGetResponse, MemoryListParams, MemoryListResponse, MemoryProvenance, MemoryRecord,
    MemoryRememberParams, MemoryRememberResponse, MemoryScope, MemoryScopeKind, MemorySearchHit,
    MemorySearchParams, MemorySearchResponse, MemorySemanticWriteDisposition,
    MemorySemanticWriteParams, MemorySemanticWriteResponse, MemorySensitivity,
    MemorySensitivityHint, MemorySourceKind, MemoryStatus, MemoryWriteRelation, generate_id,
};
use std::collections::BTreeSet;
use std::sync::Arc;

const ID_LEN: usize = 21;
const REPAIR_STATUS_OK: &str = "ok";
const REPAIR_STATUS_REPAIR_NEEDED: &str = "repair_needed";
const REPAIR_JOB_BACKEND_PAYLOAD_MISSING: &str = "backend_payload_missing";
const REPAIR_JOB_BACKEND_DELETE_FAILED: &str = "backend_delete_failed";
const REPAIR_JOB_BACKEND_STALE_PAYLOAD: &str = "backend_stale_payload";
const REPAIR_PRIORITY_DEFAULT: i64 = 10;
const REPAIR_MAX_ATTEMPTS_DEFAULT: i32 = 3;
const POLICY_ACTION_SEMANTIC_WRITE: &str = "semantic_write";

pub struct MemoryService {
    store: Arc<CrudStore>,
    backend: Arc<dyn MemoryBackend>,
    config: MemoryServiceConfig,
    policy: MemoryPolicyEngine,
}

impl MemoryService {
    pub fn new(
        store: Arc<CrudStore>,
        backend: Arc<dyn MemoryBackend>,
        config: MemoryServiceConfig,
    ) -> Self {
        let policy = MemoryPolicyEngine::new(config.clone());
        Self {
            store,
            backend,
            config,
            policy,
        }
    }

    pub fn with_noop_backend(store: Arc<CrudStore>) -> Self {
        Self::new(
            store,
            Arc::new(NoopMemoryBackend),
            MemoryServiceConfig::default(),
        )
    }

    pub async fn remember(
        &self,
        context: MemoryOperationContext,
        params: MemoryRememberParams,
    ) -> Result<MemoryRememberResponse> {
        let now = context.now_or(current_unix());
        let prepared = self.policy.prepare_remember(&context, &params)?;
        let resolved_scope = self
            .store
            .resolve_memory_scope(params.scope.clone())
            .await
            .with_context(|| {
                format!(
                    "failed to resolve memory scope `{:?}/{}`",
                    params.scope.kind, params.scope.key
                )
            })?;

        let existing = if params.supersedes.is_none() {
            match params.key.as_deref() {
                Some(key) => self
                    .store
                    .get_active_agent_memory_by_key(
                        params.scope.clone(),
                        params.namespace.as_deref(),
                        key,
                        context.workspace_guard(),
                    )
                    .await
                    .with_context(|| format!("failed to find active memory by key `{key}`"))?,
                None => None,
            }
        } else {
            let superseded_id = params.supersedes.as_deref().expect("checked above");
            self.store
                .get_agent_memory_record(superseded_id, false)
                .await
                .with_context(|| format!("failed to load superseded memory `{superseded_id}`"))?
                .with_context(|| format!("superseded memory `{superseded_id}` does not exist"))?;
            None
        };

        let created = existing.is_none();
        let memory_id = params
            .supersedes
            .as_ref()
            .map(|_| generate_id(ID_LEN))
            .or_else(|| existing.as_ref().map(|record| record.id.clone()))
            .unwrap_or_else(|| generate_id(ID_LEN));
        let metadata_json =
            metadata_with_idempotency(&params.metadata, params.idempotency_key.as_deref())?;
        let provenance = effective_provenance(&params, context.actor.clone());

        let backend_request = BackendPutRequest {
            memory_id: memory_id.clone(),
            scope: params.scope.clone(),
            namespace: params.namespace.clone(),
            category: params.category,
            key: params.key.clone(),
            content: prepared.content.clone(),
            sensitivity: prepared.sensitivity,
            metadata_json: metadata_json.clone(),
            source_kind: provenance.source_kind,
            source_thread_id: provenance.source_thread_id.clone(),
            source_turn_id: provenance.source_turn_id.clone(),
            source_item_id: provenance.source_item_id.clone(),
            created_by_kind: provenance.created_by.as_ref().map(|actor| actor.kind),
            created_by_id: provenance
                .created_by
                .as_ref()
                .and_then(|actor| actor.id.clone()),
            policy_version: self.config.policy_version.clone(),
            status: MemoryStatus::Active,
            idempotency_key: params.idempotency_key.clone(),
        };
        let backend_result = match self.backend.put(backend_request).await {
            Ok(result) => result,
            Err(error) => {
                let error_message = error.to_string();
                self.record_policy_decision(
                    POLICY_ACTION_REMEMBER,
                    POLICY_DECISION_ERROR,
                    Some("backend_put_failed"),
                    Some(error_message),
                    &context,
                    Some(memory_id.clone()),
                    resolved_scope.workspace_id.clone(),
                    now,
                )
                .await?;
                return Err(error)
                    .with_context(|| format!("failed to write memory `{memory_id}` to backend"));
            }
        };

        let new_record = NewAgentMemoryControlRecord {
            id: Some(memory_id.clone()),
            scope: params.scope.clone(),
            namespace: params.namespace.clone(),
            category: params.category,
            key: params.key.clone(),
            sensitivity: prepared.sensitivity,
            confidence: prepared.confidence,
            importance: prepared.importance,
            content_preview: content_preview(&prepared.content, self.config.content_preview_chars),
            capsule_id: backend_result.capsule_id,
            capsule_ref: backend_result.capsule_ref,
            frame_id: backend_result.frame_id,
            frame_uri: backend_result.frame_uri,
            frame_version: backend_result.frame_version,
            source_kind: provenance.source_kind,
            source_thread_id: provenance.source_thread_id.clone(),
            source_turn_id: provenance.source_turn_id.clone(),
            source_item_id: provenance.source_item_id.clone(),
            created_by: protocol_actor_to_crud(provenance.created_by.clone()),
            expires_at_unix: None,
            policy_version: Some(self.config.policy_version.clone()),
            metadata_json: metadata_json.or(backend_result.backend_metadata_json),
        };

        if let Some(superseded_id) = params.supersedes.as_deref() {
            self.store
                .mark_agent_memory_superseded(superseded_id, memory_id.as_str(), now)
                .await
                .with_context(|| format!("failed to supersede memory `{superseded_id}`"))?
                .with_context(|| format!("superseded memory `{superseded_id}` disappeared"))?;
        }

        let row = if params.supersedes.is_some() || params.key.is_none() {
            self.store
                .insert_agent_memory_record(new_record, None, now)
                .await
                .with_context(|| format!("failed to insert memory `{memory_id}`"))?
        } else {
            self.store
                .upsert_active_agent_memory_record(new_record, None, now)
                .await
                .with_context(|| format!("failed to upsert memory `{memory_id}`"))?
        };

        self.record_policy_decision(
            POLICY_ACTION_REMEMBER,
            POLICY_DECISION_ALLOW,
            None,
            None,
            &context,
            Some(row.id.clone()),
            row.workspace_id.clone(),
            now,
        )
        .await?;

        let payload = self
            .backend
            .get(backend_get_request(&row))
            .await?
            .with_context(|| format!("backend payload missing after memory `{}` write", row.id))?;
        Ok(MemoryRememberResponse {
            record: crud_record_to_protocol(row, payload)?,
            created,
            superseded_memory_id: params.supersedes,
        })
    }

    pub async fn write_semantic_memory(
        &self,
        context: MemoryOperationContext,
        params: MemorySemanticWriteParams,
    ) -> Result<MemorySemanticWriteResponse> {
        let now = context.now_or(current_unix());
        let content = params.content.trim();
        if content.is_empty() {
            bail!("semantic memory content cannot be empty");
        }
        if params.evidence.is_none() {
            bail!("semantic memory write requires evidence");
        }
        let value = params.value.as_deref().unwrap_or(content);
        let prepared = prepare_semantic_write(&params.scope, &params.semantic, value)?;
        let disposition = params
            .disposition
            .unwrap_or(MemorySemanticWriteDisposition::RouteToCandidatePolicy);
        let sensitivity = sensitivity_from_hint(params.semantic.sensitivity);
        let provenance = semantic_write_provenance(&params, &context);
        let mut base_metadata = params.metadata.clone();
        for (key, value) in semantic_metadata(
            &params.semantic,
            &prepared,
            params.client_provided_key.as_deref(),
            Some("semantic_write"),
        ) {
            base_metadata.insert(key, value);
        }
        let metadata_json = merge_metadata(None, base_metadata, params.evidence.as_ref(), now)?;

        if let Some(existing) = self
            .store
            .get_active_agent_memory_by_key(
                params.scope.clone(),
                Some(prepared.canonical.namespace.as_str()),
                prepared.canonical.key.as_str(),
                context.workspace_guard(),
            )
            .await?
        {
            let existing_value = metadata_normalized_value(existing.metadata_json.as_deref())
                .or_else(|| {
                    existing
                        .content_preview
                        .as_deref()
                        .map(normalize_semantic_text)
                });
            if existing_value.as_deref() == Some(prepared.normalized_value.as_str()) {
                let merged_metadata = merge_metadata(
                    existing.metadata_json.as_deref(),
                    semantic_metadata(
                        &params.semantic,
                        &prepared,
                        params.client_provided_key.as_deref(),
                        Some("semantic_duplicate"),
                    ),
                    params.evidence.as_ref(),
                    now,
                )?;
                let merged = self
                    .store
                    .update_agent_memory_metadata(existing.id.as_str(), Some(merged_metadata), now)
                    .await?
                    .unwrap_or(existing);
                let memory_id = merged.id.clone();
                let workspace_id = merged.workspace_id.clone();
                let record = self
                    .hydrate_visible_row(merged, &context, &[], now, false)
                    .await?;
                self.record_semantic_write_relation(
                    MemoryWriteRelation::Duplicate,
                    "active_duplicate",
                    &context,
                    Some(memory_id),
                    workspace_id,
                    now,
                )
                .await?;
                return Ok(MemorySemanticWriteResponse {
                    relation: MemoryWriteRelation::Duplicate,
                    canonical_key: prepared.canonical,
                    semantic_fingerprint: prepared.semantic_fingerprint,
                    record,
                    candidate: None,
                    created: false,
                    superseded_memory_id: None,
                    evidence_merged: true,
                });
            }

            if disposition == MemorySemanticWriteDisposition::AcceptActive
                && params.semantic.explicitness == pioneer_protocol::MemoryExplicitness::Explicit
            {
                let relation_context = context.clone();
                let superseded_memory_id = existing.id.clone();
                let response = self
                    .remember(
                        context,
                        MemoryRememberParams {
                            scope: params.scope,
                            category: prepared.canonical.category,
                            namespace: Some(prepared.canonical.namespace.clone()),
                            key: Some(prepared.canonical.key.clone()),
                            content: content.to_owned(),
                            sensitivity: Some(sensitivity),
                            confidence: params.confidence,
                            importance: params.importance,
                            provenance: Some(provenance),
                            idempotency_key: None,
                            supersedes: Some(superseded_memory_id),
                            metadata: serde_json::from_str(metadata_json.as_str())
                                .context("semantic metadata must decode")?,
                        },
                    )
                    .await?;
                self.record_semantic_write_relation(
                    MemoryWriteRelation::CompatibleUpdate,
                    "active_supersession",
                    &relation_context,
                    Some(response.record.id.clone()),
                    relation_context.workspace_id.clone(),
                    now,
                )
                .await?;
                return Ok(MemorySemanticWriteResponse {
                    relation: MemoryWriteRelation::CompatibleUpdate,
                    canonical_key: prepared.canonical,
                    semantic_fingerprint: prepared.semantic_fingerprint,
                    record: Some(response.record),
                    candidate: None,
                    created: response.created,
                    superseded_memory_id: response.superseded_memory_id,
                    evidence_merged: false,
                });
            }

            let candidate = self
                .maybe_route_semantic_candidate(
                    &context,
                    &params,
                    &prepared,
                    content,
                    metadata_json,
                    disposition,
                    now,
                    "semantic_contradiction",
                )
                .await?;
            self.record_semantic_write_relation(
                MemoryWriteRelation::Contradiction,
                "same_key_value_conflict",
                &context,
                Some(existing.id.clone()),
                existing.workspace_id.clone(),
                now,
            )
            .await?;
            return Ok(MemorySemanticWriteResponse {
                relation: MemoryWriteRelation::Contradiction,
                canonical_key: prepared.canonical,
                semantic_fingerprint: prepared.semantic_fingerprint,
                record: None,
                candidate,
                created: false,
                superseded_memory_id: None,
                evidence_merged: false,
            });
        }

        if let Some(rejected) = self
            .store
            .get_agent_memory_candidate_by_dedupe(
                params.scope.clone(),
                Some(prepared.canonical.namespace.as_str()),
                prepared.dedupe_key.as_str(),
                vec![
                    MemoryCandidateStatus::Rejected,
                    MemoryCandidateStatus::Expired,
                ],
                context.workspace_guard(),
            )
            .await?
        {
            self.record_semantic_write_relation(
                MemoryWriteRelation::SuppressedByRejection,
                "suppressed_duplicate",
                &context,
                None,
                rejected.workspace_id.clone(),
                now,
            )
            .await?;
            return Ok(MemorySemanticWriteResponse {
                relation: MemoryWriteRelation::SuppressedByRejection,
                canonical_key: prepared.canonical,
                semantic_fingerprint: prepared.semantic_fingerprint,
                record: None,
                candidate: None,
                created: false,
                superseded_memory_id: None,
                evidence_merged: false,
            });
        }

        if let Some(pending) = self
            .store
            .get_agent_memory_candidate_by_dedupe(
                params.scope.clone(),
                Some(prepared.canonical.namespace.as_str()),
                prepared.dedupe_key.as_str(),
                vec![MemoryCandidateStatus::Pending],
                context.workspace_guard(),
            )
            .await?
        {
            let merged_metadata = merge_metadata(
                pending.metadata_json.as_deref(),
                semantic_metadata(
                    &params.semantic,
                    &prepared,
                    params.client_provided_key.as_deref(),
                    Some("semantic_pending_duplicate"),
                ),
                params.evidence.as_ref(),
                now,
            )?;
            let updated = self
                .store
                .update_agent_memory_candidate_metadata(
                    pending.id.as_str(),
                    pending.reason.clone(),
                    Some(merged_metadata),
                    now,
                )
                .await?
                .unwrap_or(pending);
            self.record_semantic_write_relation(
                MemoryWriteRelation::Duplicate,
                "pending_duplicate",
                &context,
                None,
                updated.workspace_id.clone(),
                now,
            )
            .await?;
            return Ok(MemorySemanticWriteResponse {
                relation: MemoryWriteRelation::Duplicate,
                canonical_key: prepared.canonical,
                semantic_fingerprint: prepared.semantic_fingerprint,
                record: None,
                candidate: Some(crud_candidate_to_protocol(updated)?),
                created: false,
                superseded_memory_id: None,
                evidence_merged: true,
            });
        }

        match disposition {
            MemorySemanticWriteDisposition::AcceptActive => {
                let relation_context = context.clone();
                let response = self
                    .remember(
                        context,
                        MemoryRememberParams {
                            scope: params.scope,
                            category: prepared.canonical.category,
                            namespace: Some(prepared.canonical.namespace.clone()),
                            key: Some(prepared.canonical.key.clone()),
                            content: content.to_owned(),
                            sensitivity: Some(sensitivity),
                            confidence: params.confidence,
                            importance: params.importance,
                            provenance: Some(provenance),
                            idempotency_key: None,
                            supersedes: None,
                            metadata: serde_json::from_str(metadata_json.as_str())
                                .context("semantic metadata must decode")?,
                        },
                    )
                    .await?;
                self.record_semantic_write_relation(
                    MemoryWriteRelation::Novel,
                    "active_created",
                    &relation_context,
                    Some(response.record.id.clone()),
                    relation_context.workspace_id.clone(),
                    now,
                )
                .await?;
                Ok(MemorySemanticWriteResponse {
                    relation: MemoryWriteRelation::Novel,
                    canonical_key: prepared.canonical,
                    semantic_fingerprint: prepared.semantic_fingerprint,
                    record: Some(response.record),
                    candidate: None,
                    created: response.created,
                    superseded_memory_id: response.superseded_memory_id,
                    evidence_merged: false,
                })
            }
            MemorySemanticWriteDisposition::CreatePendingCandidate
            | MemorySemanticWriteDisposition::RejectSuppressed => {
                let candidate = self
                    .create_semantic_candidate(
                        &context,
                        &params,
                        &prepared,
                        content,
                        metadata_json,
                        disposition == MemorySemanticWriteDisposition::RejectSuppressed,
                        now,
                        "semantic_candidate",
                    )
                    .await?;
                self.record_semantic_write_relation(
                    MemoryWriteRelation::Novel,
                    if disposition == MemorySemanticWriteDisposition::RejectSuppressed {
                        "suppressed_created"
                    } else {
                        "candidate_created"
                    },
                    &context,
                    None,
                    context.workspace_id.clone(),
                    now,
                )
                .await?;
                Ok(MemorySemanticWriteResponse {
                    relation: MemoryWriteRelation::Novel,
                    canonical_key: prepared.canonical,
                    semantic_fingerprint: prepared.semantic_fingerprint,
                    record: None,
                    candidate: Some(candidate),
                    created: true,
                    superseded_memory_id: None,
                    evidence_merged: false,
                })
            }
            MemorySemanticWriteDisposition::RouteToCandidatePolicy => {
                self.record_semantic_write_relation(
                    MemoryWriteRelation::Novel,
                    "candidate_policy_boundary",
                    &context,
                    None,
                    context.workspace_id.clone(),
                    now,
                )
                .await?;
                Ok(MemorySemanticWriteResponse {
                    relation: MemoryWriteRelation::Novel,
                    canonical_key: prepared.canonical,
                    semantic_fingerprint: prepared.semantic_fingerprint,
                    record: None,
                    candidate: None,
                    created: false,
                    superseded_memory_id: None,
                    evidence_merged: false,
                })
            }
        }
    }

    pub async fn get(
        &self,
        context: MemoryOperationContext,
        params: MemoryGetParams,
    ) -> Result<MemoryGetResponse> {
        let now = context.now_or(current_unix());
        let Some(row) = self
            .store
            .get_agent_memory_record(params.memory_id.as_str(), params.include_deleted)
            .await?
        else {
            return Ok(MemoryGetResponse { record: None });
        };
        let allowed_statuses = if params.include_deleted {
            vec![MemoryStatus::Active, MemoryStatus::Deleted]
        } else {
            Vec::new()
        };
        let Some(record) = self
            .hydrate_visible_row(row, &context, &allowed_statuses, now, true)
            .await?
        else {
            return Ok(MemoryGetResponse { record: None });
        };
        Ok(MemoryGetResponse {
            record: Some(record),
        })
    }

    pub async fn get_by_key(
        &self,
        context: MemoryOperationContext,
        scope: MemoryScope,
        namespace: Option<String>,
        key: String,
    ) -> Result<MemoryGetResponse> {
        let now = context.now_or(current_unix());
        let Some(row) = self
            .store
            .get_active_agent_memory_by_key(
                scope,
                namespace.as_deref(),
                key.as_str(),
                context.workspace_guard(),
            )
            .await?
        else {
            return Ok(MemoryGetResponse { record: None });
        };
        let Some(record) = self
            .hydrate_visible_row(row, &context, &[], now, true)
            .await?
        else {
            return Ok(MemoryGetResponse { record: None });
        };
        Ok(MemoryGetResponse {
            record: Some(record),
        })
    }

    pub async fn list(
        &self,
        context: MemoryOperationContext,
        params: MemoryListParams,
    ) -> Result<MemoryListResponse> {
        if let Some(query) = params.query.as_deref()
            && !query.trim().is_empty()
        {
            let search = self
                .search(
                    context,
                    MemorySearchParams {
                        query: query.to_owned(),
                        scopes: params.scopes,
                        categories: params.categories,
                        statuses: params.statuses,
                        limit: params.limit,
                        cursor: params.cursor,
                        include_provenance: true,
                    },
                )
                .await?;
            return Ok(MemoryListResponse {
                records: search.hits.into_iter().map(|hit| hit.record).collect(),
                next_cursor: search.next_cursor,
            });
        }

        let now = context.now_or(current_unix());
        let scopes = context.effective_scopes(&params.scopes);
        let limit = self.normalized_limit(params.limit);
        let include_deleted = params.statuses.contains(&MemoryStatus::Deleted);
        let include_superseded = params.statuses.contains(&MemoryStatus::Superseded);
        let include_expired = params.statuses.contains(&MemoryStatus::Expired);
        let rows = self
            .store
            .list_agent_memory_records(AgentMemoryListFilter {
                scopes,
                workspace_guard: context.workspace_guard(),
                namespace: None,
                categories: params.categories.clone(),
                statuses: params.statuses.clone(),
                include_expired,
                include_deleted,
                include_superseded,
                limit: Some(u64::from(limit)),
            })
            .await?;

        let mut records = Vec::new();
        for row in rows {
            if let Some(record) = self
                .hydrate_visible_row(row, &context, &params.statuses, now, false)
                .await?
            {
                records.push(record);
            }
        }

        Ok(MemoryListResponse {
            records,
            next_cursor: None,
        })
    }

    pub async fn search(
        &self,
        context: MemoryOperationContext,
        params: MemorySearchParams,
    ) -> Result<MemorySearchResponse> {
        let query = params.query.trim();
        let backend_query = normalize_backend_search_query(query);
        if backend_query.is_empty() {
            bail!("memory search query cannot be empty");
        }

        let now = context.now_or(current_unix());
        let active_scopes = context.active_scopes(&params.scopes);
        let resolved_scopes = self
            .resolve_backend_search_scopes(&active_scopes.scopes)
            .await?;
        let limit = self.normalized_tool_search_limit(params.limit);
        let backend_limit = self.backend_candidate_limit(limit);
        let backend_hits = self
            .backend
            .search(BackendSearchRequest {
                query: backend_query,
                scopes: active_scopes.scopes.clone(),
                resolved_scopes,
                limit: backend_limit,
            })
            .await
            .context("failed to search memory backend")?;

        let backend_returned_any = !backend_hits.is_empty();
        let mut candidates = Vec::new();
        let mut seen_memory_ids = BTreeSet::new();
        for hit in backend_hits {
            seen_memory_ids.insert(hit.memory_id.clone());
            let Some(row) = self
                .store
                .get_agent_memory_record(hit.memory_id.as_str(), true)
                .await?
            else {
                self.enqueue_stale_backend_repair(hit.memory_id.as_str(), now)
                    .await?;
                continue;
            };
            if !scope_matches(&row.scope, &active_scopes.scopes)
                || !category_matches(row.category, &params.categories)
            {
                continue;
            }
            let recency_anchor_unix = recency_anchor_unix(&row);
            let Some(record) = self
                .hydrate_visible_row(row, &context, &params.statuses, now, true)
                .await?
            else {
                continue;
            };
            let backend_score = hit.score;
            candidates.push(MemoryRankingCandidate {
                hit: MemorySearchHit {
                    record,
                    score: backend_score,
                    snippet: hit.snippet,
                    matched_terms: hit.matched_terms,
                },
                backend_score,
                recency_anchor_unix,
            });
        }
        if candidates.is_empty() && !backend_returned_any {
            let fallback_rows = self
                .store
                .list_agent_memory_records(AgentMemoryListFilter {
                    scopes: active_scopes.scopes.clone(),
                    workspace_guard: context.workspace_guard(),
                    namespace: None,
                    categories: params.categories.clone(),
                    statuses: params.statuses.clone(),
                    include_expired: params.statuses.contains(&MemoryStatus::Expired),
                    include_deleted: params.statuses.contains(&MemoryStatus::Deleted),
                    include_superseded: params.statuses.contains(&MemoryStatus::Superseded),
                    limit: Some(backend_limit as u64),
                })
                .await?;
            for row in fallback_rows {
                if seen_memory_ids.contains(row.id.as_str()) {
                    continue;
                }
                let Some(fallback_hit) =
                    control_plane_fallback_hit(&row, query, &params.categories)
                else {
                    continue;
                };
                let recency_anchor_unix = recency_anchor_unix(&row);
                let Some(record) = self
                    .hydrate_visible_row(row, &context, &params.statuses, now, true)
                    .await?
                else {
                    continue;
                };
                let backend_score = fallback_hit.score;
                candidates.push(MemoryRankingCandidate {
                    hit: MemorySearchHit {
                        record,
                        score: backend_score,
                        snippet: fallback_hit.snippet,
                        matched_terms: fallback_hit.matched_terms,
                    },
                    backend_score,
                    recency_anchor_unix,
                });
            }
        }
        let hits = rank_memory_search_hits(
            candidates,
            query,
            &params.categories,
            &active_scopes,
            &self.config.ranking,
            now,
            limit,
        );

        Ok(MemorySearchResponse {
            hits,
            next_cursor: None,
        })
    }

    pub async fn recall_for_prompt(
        &self,
        context: MemoryOperationContext,
        params: MemoryRecallParams,
    ) -> Result<MemoryRecallResponse> {
        let top_k = self.normalized_prompt_top_k(params.top_k);
        let max_chars = params
            .max_chars
            .unwrap_or(self.config.recall.max_prompt_chars)
            .min(self.config.recall.max_prompt_chars);
        let search = self
            .search(
                context,
                MemorySearchParams {
                    query: params.query,
                    scopes: params.scopes,
                    categories: params.categories,
                    statuses: Vec::new(),
                    limit: Some(top_k),
                    cursor: None,
                    include_provenance: false,
                },
            )
            .await?;

        let mut remaining_chars = max_chars;
        let item_max_chars = self.config.recall.max_item_chars.max(1);
        let mut items = Vec::new();
        for hit in search.hits {
            if remaining_chars == 0 {
                break;
            }
            let content =
                compact_recall_content(&hit.record.content, item_max_chars.min(remaining_chars));
            if content.is_empty() {
                continue;
            }
            remaining_chars = remaining_chars.saturating_sub(content.chars().count());
            items.push(MemoryRecallItem {
                memory_id: hit.record.id,
                scope: hit.record.scope,
                category: hit.record.category,
                key: hit.record.key,
                content,
                score: hit.score,
                updated_at: hit.record.updated_at,
            });
        }

        Ok(MemoryRecallResponse { items })
    }

    pub async fn forget(
        &self,
        context: MemoryOperationContext,
        params: MemoryForgetParams,
    ) -> Result<MemoryForgetResponse> {
        let now = context.now_or(current_unix());
        let targets = self.resolve_forget_targets(&context, &params).await?;
        let memory_ids = targets.iter().map(|row| row.id.clone()).collect::<Vec<_>>();
        if params.dry_run {
            return Ok(MemoryForgetResponse {
                forgotten_memory_ids: memory_ids,
                dry_run: true,
            });
        }

        let actor = params.actor.clone().or_else(|| context.actor.clone());
        let crud_actor = protocol_actor_to_crud(actor.clone());
        for row in targets {
            let deleted = self
                .store
                .mark_agent_memory_deleted(
                    row.id.as_str(),
                    crud_actor.clone(),
                    params.reason.clone(),
                    now,
                )
                .await?
                .with_context(|| format!("memory `{}` disappeared during forget", row.id))?;
            match self.backend.delete(backend_delete_request(&deleted)).await {
                Ok(_) => {
                    self.record_policy_decision(
                        POLICY_ACTION_FORGET,
                        POLICY_DECISION_ALLOW,
                        None,
                        params.reason.clone(),
                        &context,
                        Some(deleted.id.clone()),
                        deleted.workspace_id.clone(),
                        now,
                    )
                    .await?;
                }
                Err(error) => {
                    self.enqueue_backend_repair(
                        &deleted,
                        REPAIR_JOB_BACKEND_DELETE_FAILED,
                        "forget",
                        Some(error.to_string()),
                        now,
                    )
                    .await?;
                    self.record_policy_decision(
                        POLICY_ACTION_FORGET,
                        POLICY_DECISION_ERROR,
                        Some("backend_delete_failed"),
                        Some(error.to_string()),
                        &context,
                        Some(deleted.id.clone()),
                        deleted.workspace_id.clone(),
                        now,
                    )
                    .await?;
                }
            }
        }

        Ok(MemoryForgetResponse {
            forgotten_memory_ids: memory_ids,
            dry_run: false,
        })
    }

    pub async fn list_candidates(
        &self,
        context: MemoryOperationContext,
        params: MemoryCandidatesListParams,
    ) -> Result<MemoryCandidatesListResponse> {
        let rows = self
            .store
            .list_agent_memory_candidates(AgentMemoryCandidateListFilter {
                scopes: context.effective_scopes(&params.scopes),
                workspace_guard: context.workspace_guard(),
                statuses: params.statuses.clone(),
                limit: Some(u64::from(self.normalized_limit(params.limit))),
            })
            .await?;

        let candidates = rows
            .into_iter()
            .map(crud_candidate_to_protocol)
            .collect::<Result<Vec<_>>>()?;

        Ok(MemoryCandidatesListResponse {
            candidates,
            next_cursor: None,
        })
    }

    pub async fn decide_candidate(
        &self,
        context: MemoryOperationContext,
        params: MemoryCandidatesDecideParams,
    ) -> Result<MemoryCandidatesDecideResponse> {
        let visible_pending = self
            .store
            .list_agent_memory_candidates(AgentMemoryCandidateListFilter {
                scopes: Vec::new(),
                workspace_guard: context.workspace_guard(),
                statuses: vec![MemoryCandidateStatus::Pending],
                limit: None,
            })
            .await?;
        if !visible_pending
            .iter()
            .any(|candidate| candidate.id == params.candidate_id)
        {
            bail!(
                "memory candidate `{}` was not found or is not pending",
                params.candidate_id
            );
        }

        let now = context.now_or(current_unix());
        let actor = params.actor.clone().or_else(|| context.actor.clone());
        let decided = self
            .store
            .decide_agent_memory_candidate(AgentMemoryCandidateDecisionRecord {
                candidate_id: params.candidate_id.clone(),
                decision: params.decision,
                decided_by: protocol_actor_to_crud(actor),
                decision_reason: params.reason.clone(),
                promoted_memory_id: None,
                decided_at_unix: now,
            })
            .await?
            .with_context(|| {
                format!(
                    "memory candidate `{}` was not found or is not pending",
                    params.candidate_id
                )
            })?;

        Ok(MemoryCandidatesDecideResponse {
            candidate: crud_candidate_to_protocol(decided)?,
            record: None,
        })
    }

    pub async fn enqueue_repair_job(
        &self,
        job: NewAgentMemoryRepairJob,
        now_unix: i64,
    ) -> Result<AgentMemoryRepairJobRecord> {
        self.store
            .enqueue_agent_memory_repair_job(job, now_unix)
            .await
    }

    pub async fn claim_due_repair_jobs(
        &self,
        now_unix: i64,
        lock_ttl_secs: i64,
        locked_by: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryRepairJobRecord>> {
        self.store
            .claim_due_agent_memory_repair_jobs(now_unix, lock_ttl_secs, locked_by, limit)
            .await
    }

    pub async fn complete_repair_job(
        &self,
        job_id: &str,
        locked_by: &str,
        result_json: Option<String>,
        now_unix: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        self.store
            .mark_agent_memory_repair_job_completed(job_id, locked_by, result_json, now_unix)
            .await
    }

    pub async fn fail_repair_job(
        &self,
        job_id: &str,
        locked_by: &str,
        last_error: String,
        retry_at_unix: Option<i64>,
        now_unix: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        self.store
            .mark_agent_memory_repair_job_failed(
                job_id,
                locked_by,
                last_error,
                retry_at_unix,
                now_unix,
            )
            .await
    }

    async fn maybe_route_semantic_candidate(
        &self,
        context: &MemoryOperationContext,
        params: &MemorySemanticWriteParams,
        prepared: &SemanticWritePrepared,
        content: &str,
        metadata_json: String,
        disposition: MemorySemanticWriteDisposition,
        now: i64,
        reason: &str,
    ) -> Result<Option<MemoryCandidate>> {
        match disposition {
            MemorySemanticWriteDisposition::CreatePendingCandidate => self
                .create_semantic_candidate(
                    context,
                    params,
                    prepared,
                    content,
                    metadata_json,
                    false,
                    now,
                    reason,
                )
                .await
                .map(Some),
            MemorySemanticWriteDisposition::AcceptActive
            | MemorySemanticWriteDisposition::RejectSuppressed => self
                .create_semantic_candidate(
                    context,
                    params,
                    prepared,
                    content,
                    metadata_json,
                    true,
                    now,
                    reason,
                )
                .await
                .map(Some),
            MemorySemanticWriteDisposition::RouteToCandidatePolicy => Ok(None),
        }
    }

    async fn create_semantic_candidate(
        &self,
        context: &MemoryOperationContext,
        params: &MemorySemanticWriteParams,
        prepared: &SemanticWritePrepared,
        content: &str,
        metadata_json: String,
        reject: bool,
        now: i64,
        reason: &str,
    ) -> Result<MemoryCandidate> {
        let provenance = semantic_write_provenance(params, context);
        let candidate = self
            .store
            .insert_agent_memory_candidate(
                NewAgentMemoryCandidate {
                    id: None,
                    scope: params.scope.clone(),
                    namespace: Some(prepared.canonical.namespace.clone()),
                    category: prepared.canonical.category,
                    key: Some(prepared.canonical.key.clone()),
                    candidate_text: content.to_owned(),
                    confidence: f64::from(params.confidence.unwrap_or(0.5).clamp(0.0, 1.0)),
                    reason: reason.to_owned(),
                    source_kind: provenance.source_kind,
                    source_thread_id: provenance.source_thread_id.clone(),
                    source_turn_id: provenance.source_turn_id.clone(),
                    source_item_id: provenance.source_item_id.clone(),
                    created_by: protocol_actor_to_crud(provenance.created_by.clone()),
                    dedupe_key: Some(prepared.dedupe_key.clone()),
                    metadata_json: Some(metadata_json),
                },
                now,
            )
            .await?;
        if !reject {
            return crud_candidate_to_protocol(candidate);
        }
        let rejected = self
            .store
            .decide_agent_memory_candidate(AgentMemoryCandidateDecisionRecord {
                candidate_id: candidate.id.clone(),
                decision: MemoryCandidateDecision::Reject,
                decided_by: protocol_actor_to_crud(context.actor.clone()),
                decision_reason: Some("review_disabled_or_suppressed".to_owned()),
                promoted_memory_id: None,
                decided_at_unix: now,
            })
            .await?
            .unwrap_or(candidate);
        crud_candidate_to_protocol(rejected)
    }

    async fn resolve_forget_targets(
        &self,
        context: &MemoryOperationContext,
        params: &MemoryForgetParams,
    ) -> Result<Vec<AgentMemoryControlRecord>> {
        let now = context.now_or(current_unix());
        let row = match &params.target {
            MemoryForgetTarget::Id { memory_id } => {
                self.store
                    .get_agent_memory_record(memory_id.as_str(), false)
                    .await?
            }
            MemoryForgetTarget::ScopedKey {
                scope,
                namespace,
                key,
            } => {
                self.store
                    .get_active_agent_memory_by_key(
                        scope.clone(),
                        namespace.as_deref(),
                        key.as_str(),
                        context.workspace_guard(),
                    )
                    .await?
            }
        };
        let Some(row) = row else {
            return Ok(Vec::new());
        };
        if self.row_visible(&row, context, &[], now) {
            Ok(vec![row])
        } else {
            Ok(Vec::new())
        }
    }

    async fn hydrate_visible_row(
        &self,
        row: AgentMemoryControlRecord,
        context: &MemoryOperationContext,
        allowed_statuses: &[MemoryStatus],
        now: i64,
        record_access: bool,
    ) -> Result<Option<MemoryRecord>> {
        if !self.row_visible(&row, context, allowed_statuses, now) {
            return Ok(None);
        }

        let payload = match self.backend.get(backend_get_request(&row)).await? {
            Some(payload) => payload,
            None if row.status == MemoryStatus::Deleted => BackendPayload {
                memory_id: row.id.clone(),
                content: row.content_preview.clone().unwrap_or_default(),
                snippet: row.content_preview.clone(),
                metadata_json: None,
            },
            None => {
                self.mark_missing_backend_payload(&row, now).await?;
                return Ok(None);
            }
        };

        let row = if record_access && row.status == MemoryStatus::Active {
            self.store
                .record_agent_memory_access(row.id.as_str(), now)
                .await?;
            self.store
                .get_agent_memory_record(row.id.as_str(), true)
                .await?
                .unwrap_or(row)
        } else {
            row
        };

        Ok(Some(crud_record_to_protocol(row, payload)?))
    }

    fn row_visible(
        &self,
        row: &AgentMemoryControlRecord,
        context: &MemoryOperationContext,
        allowed_statuses: &[MemoryStatus],
        now: i64,
    ) -> bool {
        let status_allowed = if allowed_statuses.is_empty() {
            row.status == MemoryStatus::Active
        } else {
            allowed_statuses.contains(&row.status)
        };
        if !status_allowed {
            return false;
        }
        if row.deleted_at_unix.is_some() && !allowed_statuses.contains(&MemoryStatus::Deleted) {
            return false;
        }
        if row.superseded_by.is_some() && !allowed_statuses.contains(&MemoryStatus::Superseded) {
            return false;
        }
        if row
            .expires_at_unix
            .is_some_and(|expires_at| expires_at <= now)
            && !allowed_statuses.contains(&MemoryStatus::Expired)
        {
            return false;
        }
        if row.repair_status != REPAIR_STATUS_OK {
            return false;
        }
        if !self.policy.allows_sensitivity(context, row.sensitivity) {
            return false;
        }
        workspace_visible(row, context)
    }

    async fn mark_missing_backend_payload(
        &self,
        row: &AgentMemoryControlRecord,
        now: i64,
    ) -> Result<()> {
        if row.status == MemoryStatus::Active && row.repair_status == REPAIR_STATUS_OK {
            self.store
                .mark_agent_memory_repair_status(row.id.as_str(), REPAIR_STATUS_REPAIR_NEEDED, now)
                .await?;
            self.enqueue_backend_repair(
                row,
                REPAIR_JOB_BACKEND_PAYLOAD_MISSING,
                "hydrate",
                Some("backend payload missing".to_owned()),
                now,
            )
            .await?;
        }
        Ok(())
    }

    async fn enqueue_backend_repair(
        &self,
        row: &AgentMemoryControlRecord,
        job_kind: &str,
        operation: &str,
        error: Option<String>,
        now: i64,
    ) -> Result<AgentMemoryRepairJobRecord> {
        let payload_json = serde_json::json!({
            "memory_id": row.id,
            "operation": operation,
            "error": error,
            "capsule_ref": row.capsule_ref,
            "frame_uri": row.frame_uri,
        })
        .to_string();
        self.enqueue_repair_job(
            NewAgentMemoryRepairJob {
                job_kind: job_kind.to_owned(),
                workspace_id: row.workspace_id.clone(),
                scope_kind: Some(row.scope.kind),
                scope_key_hash: Some(row.scope_key_hash.clone()),
                memory_id: Some(row.id.clone()),
                capsule_id: row.capsule_id.clone(),
                priority: REPAIR_PRIORITY_DEFAULT,
                max_attempts: REPAIR_MAX_ATTEMPTS_DEFAULT,
                scheduled_at_unix: now,
                payload_json: Some(payload_json),
            },
            now,
        )
        .await
    }

    async fn enqueue_stale_backend_repair(
        &self,
        memory_id: &str,
        now: i64,
    ) -> Result<AgentMemoryRepairJobRecord> {
        let payload_json = serde_json::json!({
            "memory_id": memory_id,
            "operation": "search",
            "reason": "backend hit without active control-plane row",
        })
        .to_string();
        self.enqueue_repair_job(
            NewAgentMemoryRepairJob {
                job_kind: REPAIR_JOB_BACKEND_STALE_PAYLOAD.to_owned(),
                workspace_id: None,
                scope_kind: None,
                scope_key_hash: None,
                memory_id: Some(memory_id.to_owned()),
                capsule_id: None,
                priority: REPAIR_PRIORITY_DEFAULT,
                max_attempts: REPAIR_MAX_ATTEMPTS_DEFAULT,
                scheduled_at_unix: now,
                payload_json: Some(payload_json),
            },
            now,
        )
        .await
    }

    async fn resolve_backend_search_scopes(
        &self,
        scopes: &[MemoryScope],
    ) -> Result<Vec<BackendSearchScope>> {
        let resolved = self.store.resolve_memory_scopes(scopes.to_vec()).await?;
        Ok(resolved
            .into_iter()
            .map(|scope| BackendSearchScope {
                scope: scope.scope,
                scope_key_hash: scope.scope_key_hash,
                workspace_id: scope.workspace_id,
                capsule_ref: None,
            })
            .collect())
    }

    async fn record_policy_decision(
        &self,
        action: &str,
        decision: &str,
        reason_code: Option<&str>,
        reason: Option<String>,
        context: &MemoryOperationContext,
        memory_id: Option<String>,
        workspace_id: Option<String>,
        now: i64,
    ) -> Result<()> {
        self.store
            .insert_agent_memory_policy_decision(NewAgentMemoryPolicyDecision {
                memory_id,
                candidate_id: None,
                workspace_id,
                action: action.to_owned(),
                decision: decision.to_owned(),
                reason_code: reason_code.map(str::to_owned),
                reason,
                policy_version: self.config.policy_version.clone(),
                actor: protocol_actor_to_crud(context.actor.clone()),
                thread_id: context.thread_id.clone(),
                turn_id: None,
                item_id: None,
                details_json: None,
                created_at_unix: now,
            })
            .await?;
        Ok(())
    }

    async fn record_semantic_write_relation(
        &self,
        relation: MemoryWriteRelation,
        reason_code: &'static str,
        context: &MemoryOperationContext,
        memory_id: Option<String>,
        workspace_id: Option<String>,
        now: i64,
    ) -> Result<()> {
        self.record_policy_decision(
            POLICY_ACTION_SEMANTIC_WRITE,
            memory_write_relation_to_policy_decision(relation),
            Some(reason_code),
            None,
            context,
            memory_id,
            workspace_id,
            now,
        )
        .await
    }

    fn normalized_limit(&self, limit: Option<u32>) -> u32 {
        limit
            .unwrap_or(self.config.default_limit)
            .min(self.config.max_limit)
            .max(1)
    }

    fn normalized_tool_search_limit(&self, limit: Option<u32>) -> u32 {
        limit
            .unwrap_or(self.config.recall.tool_search_limit)
            .min(self.config.max_limit)
            .max(1)
    }

    fn normalized_prompt_top_k(&self, top_k: Option<u32>) -> u32 {
        top_k
            .unwrap_or(self.config.recall.prompt_top_k)
            .min(self.config.recall.prompt_top_k)
            .min(self.config.max_limit)
            .max(1)
    }

    fn backend_candidate_limit(&self, final_limit: u32) -> u32 {
        let multiplier = self.config.ranking.backend_candidate_multiplier.max(1);
        final_limit
            .saturating_mul(multiplier)
            .min(self.config.ranking.max_backend_candidates.max(final_limit))
            .min(self.config.max_limit)
            .max(final_limit)
    }
}

fn normalize_backend_search_query(value: &str) -> String {
    let mut normalized = String::new();
    for ch in value.chars() {
        if ch.is_alphanumeric() {
            normalized.push(ch);
        } else {
            normalized.push(' ');
        }
    }
    normalized.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn control_plane_fallback_hit(
    row: &AgentMemoryControlRecord,
    query: &str,
    requested_categories: &[MemoryCategory],
) -> Option<MemorySearchHit> {
    let normalized_query = normalize_lexical_text(query);
    if normalized_query.is_empty() {
        return None;
    }
    let query_terms = normalized_query
        .split_whitespace()
        .filter(|term| !term.is_empty())
        .collect::<Vec<_>>();
    let haystack = normalize_lexical_text(
        format!(
            "{} {} {:?} {:?} {}",
            row.content_preview.as_deref().unwrap_or_default(),
            row.key.as_deref().unwrap_or_default(),
            row.category,
            row.sensitivity,
            row.namespace
        )
        .as_str(),
    );
    let matched_terms = query_terms
        .iter()
        .filter(|term| haystack.contains(**term))
        .map(|term| (*term).to_owned())
        .collect::<Vec<_>>();
    let category_match =
        !requested_categories.is_empty() && requested_categories.contains(&row.category);
    let key_match = row
        .key
        .as_deref()
        .map(normalize_lexical_text)
        .is_some_and(|key| {
            !key.is_empty()
                && (normalized_query == key
                    || normalized_query
                        .split_whitespace()
                        .any(|token| token == key))
        });
    let score = if haystack.contains(normalized_query.as_str()) {
        0.45
    } else if !query_terms.is_empty() && matched_terms.len() == query_terms.len() {
        0.35
    } else if !matched_terms.is_empty() || category_match || key_match {
        0.25
    } else {
        return None;
    };

    Some(MemorySearchHit {
        record: crud_record_to_protocol(
            row.clone(),
            BackendPayload {
                memory_id: row.id.clone(),
                content: row.content_preview.clone().unwrap_or_default(),
                snippet: row.content_preview.clone(),
                metadata_json: None,
            },
        )
        .ok()?,
        score: Some(score),
        snippet: row.content_preview.clone(),
        matched_terms,
    })
}

fn normalize_lexical_text(value: &str) -> String {
    let mut normalized = String::new();
    for ch in value.chars() {
        if ch.is_alphanumeric() {
            for lower in ch.to_lowercase() {
                normalized.push(lower);
            }
        } else {
            normalized.push(' ');
        }
    }
    normalized.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn sensitivity_from_hint(hint: MemorySensitivityHint) -> MemorySensitivity {
    match hint {
        MemorySensitivityHint::None
        | MemorySensitivityHint::Low
        | MemorySensitivityHint::Unknown => MemorySensitivity::Normal,
        MemorySensitivityHint::Personal => MemorySensitivity::Personal,
        MemorySensitivityHint::Regulated => MemorySensitivity::Regulated,
        MemorySensitivityHint::Secret => MemorySensitivity::SecretLike,
    }
}

fn semantic_write_provenance(
    params: &MemorySemanticWriteParams,
    context: &MemoryOperationContext,
) -> MemoryProvenance {
    if let Some(provenance) = &params.provenance {
        return provenance.clone();
    }

    let evidence = params.evidence.as_ref();
    MemoryProvenance {
        source_kind: match params.semantic.intent {
            pioneer_protocol::MemoryIntent::ExplicitStore => MemorySourceKind::ExplicitUserRequest,
            pioneer_protocol::MemoryIntent::ExplicitForget
            | pioneer_protocol::MemoryIntent::ExplicitNoMemory => MemorySourceKind::System,
            pioneer_protocol::MemoryIntent::ImplicitCandidate
            | pioneer_protocol::MemoryIntent::None => MemorySourceKind::BackgroundExtractor,
        },
        source_thread_id: evidence
            .and_then(|evidence| evidence.source_thread_id.clone())
            .or_else(|| context.thread_id.clone()),
        source_turn_id: evidence.and_then(|evidence| evidence.source_turn_id.clone()),
        source_item_id: evidence.and_then(|evidence| evidence.source_item_id.clone()),
        created_by: context.actor.clone(),
    }
}

fn memory_write_relation_to_policy_decision(relation: MemoryWriteRelation) -> &'static str {
    match relation {
        MemoryWriteRelation::Duplicate => "duplicate",
        MemoryWriteRelation::CompatibleUpdate => "compatible_update",
        MemoryWriteRelation::Contradiction => "contradiction",
        MemoryWriteRelation::Novel => "novel",
        MemoryWriteRelation::SuppressedByRejection => "suppressed_by_rejection",
    }
}

fn current_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

fn scope_matches(scope: &MemoryScope, scopes: &[MemoryScope]) -> bool {
    scopes.is_empty() || scopes.iter().any(|candidate| candidate == scope)
}

fn category_matches(
    category: pioneer_protocol::MemoryCategory,
    categories: &[pioneer_protocol::MemoryCategory],
) -> bool {
    categories.is_empty() || categories.contains(&category)
}

fn recency_anchor_unix(row: &AgentMemoryControlRecord) -> i64 {
    row.last_accessed_at_unix
        .unwrap_or(row.updated_at_unix)
        .max(row.updated_at_unix)
        .max(row.created_at_unix)
}

fn backend_get_request(row: &AgentMemoryControlRecord) -> BackendGetRequest {
    BackendGetRequest {
        memory_id: row.id.clone(),
        scope: row.scope.clone(),
        scope_key_hash: Some(row.scope_key_hash.clone()),
        capsule_id: row.capsule_id.clone(),
        capsule_ref: row.capsule_ref.clone(),
        frame_id: row.frame_id,
        frame_uri: row.frame_uri.clone(),
    }
}

fn backend_delete_request(row: &AgentMemoryControlRecord) -> BackendDeleteRequest {
    BackendDeleteRequest {
        memory_id: row.id.clone(),
        scope: row.scope.clone(),
        scope_key_hash: Some(row.scope_key_hash.clone()),
        capsule_id: row.capsule_id.clone(),
        capsule_ref: row.capsule_ref.clone(),
        frame_id: row.frame_id,
        frame_uri: row.frame_uri.clone(),
    }
}

fn workspace_visible(row: &AgentMemoryControlRecord, context: &MemoryOperationContext) -> bool {
    match row.workspace_id.as_deref() {
        Some(row_workspace_id) => context
            .workspace_id
            .as_deref()
            .is_some_and(|context_workspace_id| context_workspace_id == row_workspace_id),
        None => match row.scope.kind {
            MemoryScopeKind::User => context.workspace_id.is_none() || context.allow_global_user,
            MemoryScopeKind::Agent => context.allow_global_agent,
            _ => false,
        },
    }
}
