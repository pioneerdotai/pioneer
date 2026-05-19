use crate::NoopMemoryBackend;
use crate::backend::{
    BackendDeleteRequest, BackendGetRequest, BackendPayload, BackendPutRequest,
    BackendSearchRequest, BackendSearchScope, MemoryBackend,
};
use crate::candidate_policy::MemoryCandidatePolicyEngine;
use crate::config::MemoryServiceConfig;
use crate::context::MemoryOperationContext;
use crate::convert::{
    content_preview, crud_candidate_to_protocol, crud_record_to_protocol, effective_provenance,
    metadata_with_idempotency, protocol_actor_to_crud,
};
use crate::debug::{
    MEMORY_DEBUG_TRACE_MAX_EVENTS, MEMORY_DEBUG_TRACE_MAX_HOOK_RUNS,
    MEMORY_DEBUG_TRACE_MAX_QUALITY_DECISIONS, MEMORY_DEBUG_TRACE_MAX_QUARANTINE_HISTORY,
    MEMORY_DEBUG_TRACE_MAX_REPAIR_JOBS, MemoryDebugMissingData, MemoryDebugMissingDataKind,
    MemoryDebugRecallTrace, MemoryDebugSourceContextTrace, MemoryDebugTrace,
    MemoryDebugTraceTarget, MemoryDebugWriteTrace, memory_debug_event_trace,
    memory_debug_item_from_candidate, memory_debug_item_from_record, memory_debug_quality_trace,
    memory_debug_quarantine_trace, memory_debug_recall_trace_from_hook_run,
    memory_debug_recall_trace_from_hook_runs, memory_debug_repair_trace,
    memory_debug_score_from_metadata, memory_debug_source_context_from_candidate,
    memory_debug_source_context_from_record, semantic_route_from_quality,
    write_outcome_for_candidate, write_outcome_for_memory, write_outcome_for_quality_decision,
};
use crate::lifecycle::{
    MemoryQuarantineRequest, MemoryQuarantineResponse, MemoryRestoreRequest, MemoryRestoreResponse,
};
use crate::ownership_route::{
    MemoryOwnershipRoute, MemoryOwnershipRouteInput, resolve_memory_ownership_route,
};
use crate::policy::{
    MemoryPolicyEngine, POLICY_ACTION_FORGET, POLICY_ACTION_REMEMBER, POLICY_DECISION_ALLOW,
    POLICY_DECISION_ERROR,
};
use crate::quality::{classify_semantic_memory_fact, resolve_semantic_write_source_context};
use crate::quality_gate::{
    MemoryQualityGate, MemoryQualityGateInput, memory_quality_gate_input_from_semantic_write,
};
use crate::ranking::{
    MemoryRankingCandidate, MemoryRankingDiagnostics, rank_memory_search_hits_with_diagnostics,
};
use crate::recall::{
    MemoryModeRecallParams, MemoryModeRecallResponse, MemoryRecallItem, MemoryRecallMode,
    MemoryRecallParams, MemoryRecallResponse, MemoryRecallTarget, compact_recall_content,
};
use crate::recall_visibility::{
    MemoryRecallQualitySignals, MemoryRecallVisibility, decide_memory_recall_visibility,
    memory_recall_quality_signals_for_row, memory_recall_visibility_input_for_row,
};
use crate::write::{
    SemanticWritePrepared, build_memory_canonical_key, merge_metadata, metadata_normalized_value,
    normalize_semantic_text, prepare_semantic_write, semantic_metadata,
};
use anyhow::{Context, Result, bail};
use pioneer_crud::{
    AgentMemoryCandidateDecisionRecord, AgentMemoryCandidateListFilter, AgentMemoryCandidateRecord,
    AgentMemoryCandidateStatusUpdateRecord, AgentMemoryControlRecord, AgentMemoryListFilter,
    AgentMemoryQualityDecisionRecord, AgentMemoryRepairJobRecord, CrudStore,
    MemoryLifecycleActorRecord, NewAgentMemoryCandidate, NewAgentMemoryControlRecord,
    NewAgentMemoryPolicyDecision, NewAgentMemoryQualityDecision, NewAgentMemoryQuarantine,
    NewAgentMemoryRepairJob, ResolveAgentMemoryQuarantine,
};
use pioneer_hooks::{HookPhase, HookRunId};
use pioneer_protocol::{
    MemoryCandidate, MemoryCandidateDecision, MemoryCandidatePolicyDecision,
    MemoryCandidatePolicyInput, MemoryCandidatePolicyOutput, MemoryCandidateStatus,
    MemoryCandidatesApproveParams, MemoryCandidatesApproveResponse, MemoryCandidatesDecideParams,
    MemoryCandidatesDecideResponse, MemoryCandidatesEditAndApproveParams,
    MemoryCandidatesEditAndApproveResponse, MemoryCandidatesGetParams, MemoryCandidatesGetResponse,
    MemoryCandidatesListParams, MemoryCandidatesListResponse, MemoryCandidatesMergeParams,
    MemoryCandidatesMergeResponse, MemoryCandidatesRejectParams, MemoryCandidatesRejectResponse,
    MemoryCandidatesSuppressSimilarParams, MemoryCandidatesSuppressSimilarResponse, MemoryCategory,
    MemoryDurability, MemoryExplicitness, MemoryExtractorCertainty, MemoryForgetParams,
    MemoryForgetResponse, MemoryForgetTarget, MemoryGetParams, MemoryGetResponse, MemoryIntent,
    MemoryLifecycleActor, MemoryLifecycleActorKind, MemoryLifecycleReasonCode, MemoryListParams,
    MemoryListResponse, MemoryProvenance, MemoryQualityDecision, MemoryRecord,
    MemoryRememberParams, MemoryRememberResponse, MemoryScope, MemoryScopeClarity, MemoryScopeHint,
    MemoryScopeKind, MemorySearchHit, MemorySearchParams, MemorySearchResponse,
    MemorySemanticFields, MemorySemanticWriteDisposition, MemorySemanticWriteParams,
    MemorySemanticWriteResponse, MemorySemanticWriteRouteInfo, MemorySensitivity,
    MemorySensitivityHint, MemorySourceContextKind, MemoryStatus, MemoryWriteEvidence,
    MemoryWriteRelation, generate_id,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const ID_LEN: usize = 21;
const REPAIR_STATUS_OK: &str = "ok";
const REPAIR_STATUS_REPAIR_NEEDED: &str = "repair_needed";
const REPAIR_JOB_BACKEND_PAYLOAD_MISSING: &str = "backend_payload_missing";
const REPAIR_JOB_BACKEND_DELETE_FAILED: &str = "backend_delete_failed";
const REPAIR_JOB_BACKEND_STALE_PAYLOAD: &str = "backend_stale_payload";
const REPAIR_JOB_BACKEND_QUARANTINE_CLEANUP: &str = "backend_quarantine_cleanup";
const REPAIR_JOB_BACKEND_REINDEX: &str = "backend_restore_reindex";
const REPAIR_JOB_MEMVID_STALE_VECTOR: &str = "memvid_stale_vector";
const REPAIR_PRIORITY_DEFAULT: i64 = 10;
const REPAIR_MAX_ATTEMPTS_DEFAULT: i32 = 3;
const POLICY_ACTION_SEMANTIC_WRITE: &str = "semantic_write";

#[derive(Debug, Clone)]
struct MemorySearchWithDiagnostics {
    response: MemorySearchResponse,
    diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct MemoryRecallServiceDiagnostics {
    stale_backend_hit_count: usize,
    visibility_suppression_counts: BTreeMap<&'static str, usize>,
    ranking: MemoryRankingDiagnostics,
}

impl MemoryRecallServiceDiagnostics {
    fn record_stale_backend_hit(&mut self) {
        self.stale_backend_hit_count += 1;
    }

    fn record_visibility(&mut self, visibility: MemoryRecallVisibility) {
        if visibility.is_visible() {
            return;
        }
        *self
            .visibility_suppression_counts
            .entry(visibility.as_str())
            .or_insert(0) += 1;
    }

    fn extend_ranking(&mut self, ranking: &MemoryRankingDiagnostics) {
        self.ranking.exact_key_boost_count += ranking.exact_key_boost_count;
        self.ranking.quality_penalty_applied_count += ranking.quality_penalty_applied_count;
        self.ranking.low_source_context_penalty_count += ranking.low_source_context_penalty_count;
        self.ranking.rejected_related_penalty_count += ranking.rejected_related_penalty_count;
    }

    fn into_safe_strings(self) -> Vec<String> {
        let mut diagnostics = Vec::new();
        if self.stale_backend_hit_count > 0 {
            diagnostics.push(format!(
                "memory.recall_visibility.backend_stale_ids:{}",
                self.stale_backend_hit_count
            ));
        }
        let suppressed_count = self
            .visibility_suppression_counts
            .values()
            .copied()
            .sum::<usize>();
        if suppressed_count > 0 {
            diagnostics.push(format!(
                "memory.recall_visibility.suppressed_count:{suppressed_count}"
            ));
            for (reason, count) in self.visibility_suppression_counts {
                diagnostics.push(format!(
                    "memory.recall_visibility.suppressed:{reason}:{count}"
                ));
            }
        }
        if self.ranking.exact_key_boost_count > 0 {
            diagnostics.push(format!(
                "memory.recall_ranking.exact_key_boost_count:{}",
                self.ranking.exact_key_boost_count
            ));
        }
        if self.ranking.quality_penalty_applied_count > 0 {
            diagnostics.push(format!(
                "memory.recall_ranking.quality_penalty_applied_count:{}",
                self.ranking.quality_penalty_applied_count
            ));
        }
        if self.ranking.low_source_context_penalty_count > 0 {
            diagnostics.push(format!(
                "memory.recall_ranking.low_source_context_penalty_count:{}",
                self.ranking.low_source_context_penalty_count
            ));
        }
        if self.ranking.rejected_related_penalty_count > 0 {
            diagnostics.push(format!(
                "memory.recall_ranking.rejected_related_penalty_count:{}",
                self.ranking.rejected_related_penalty_count
            ));
        }
        diagnostics
    }
}

pub struct MemoryService {
    store: Arc<CrudStore>,
    backend: Arc<dyn MemoryBackend>,
    config: MemoryServiceConfig,
    policy: MemoryPolicyEngine,
    candidate_policy: MemoryCandidatePolicyEngine,
}

impl MemoryService {
    pub fn new(
        store: Arc<CrudStore>,
        backend: Arc<dyn MemoryBackend>,
        config: MemoryServiceConfig,
    ) -> Self {
        config
            .candidate_policy
            .validate()
            .expect("invalid memory candidate policy config");
        let policy = MemoryPolicyEngine::new(config.clone());
        let candidate_policy = MemoryCandidatePolicyEngine::new(config.candidate_policy.clone());
        Self {
            store,
            backend,
            config,
            policy,
            candidate_policy,
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
        self.remember_with_source_context(context, params, None)
            .await
    }

    async fn remember_with_source_context(
        &self,
        context: MemoryOperationContext,
        params: MemoryRememberParams,
        source_context_kind: Option<MemorySourceContextKind>,
    ) -> Result<MemoryRememberResponse> {
        let now = context.now_or(current_unix());
        let source_context_kind = source_context_kind
            .or(params.source_context_kind)
            .or(Some(MemorySourceContextKind::DirectUserConversation));
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
            source_context_kind,
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
        let content = params.content.trim().to_owned();
        if content.is_empty() {
            bail!("semantic memory content cannot be empty");
        }
        if params.evidence.is_none() {
            bail!("semantic memory write requires evidence");
        }
        let value = params.value.as_deref().unwrap_or(content.as_str());
        let prepared = prepare_semantic_write(&params.scope, &params.semantic, value)?;
        let disposition = params
            .disposition
            .unwrap_or(MemorySemanticWriteDisposition::RouteToCandidatePolicy);
        let sensitivity = sensitivity_from_hint(params.semantic.sensitivity);
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
                let (quality_input, quality_decision) = self.semantic_quality_decision(
                    &params,
                    &prepared,
                    MemoryWriteRelation::Duplicate,
                    sensitivity,
                );
                let ownership_route =
                    Self::quality_ownership_route(&quality_input, &quality_decision);
                let quality_record = self
                    .record_quality_decision(
                        &quality_input,
                        &quality_decision,
                        &context,
                        Some(memory_id.clone()),
                        None,
                        now,
                    )
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
                let response = MemorySemanticWriteResponse {
                    relation: MemoryWriteRelation::Duplicate,
                    canonical_key: prepared.canonical,
                    semantic_fingerprint: prepared.semantic_fingerprint,
                    record,
                    candidate: None,
                    created: false,
                    superseded_memory_id: None,
                    evidence_merged: true,
                    route: None,
                };
                return Ok(Self::with_quality_route(
                    response,
                    &quality_decision,
                    &ownership_route,
                    Some(quality_record.id),
                ));
            }

            if disposition == MemorySemanticWriteDisposition::AcceptActive
                && params.semantic.explicitness == pioneer_protocol::MemoryExplicitness::Explicit
            {
                let relation_context = context.clone();
                let superseded_memory_id = existing.id.clone();
                let (quality_input, quality_decision) = self.semantic_quality_decision(
                    &params,
                    &prepared,
                    MemoryWriteRelation::CompatibleUpdate,
                    sensitivity,
                );
                let ownership_route =
                    Self::quality_ownership_route(&quality_input, &quality_decision);
                if ownership_route.is_terminal_non_memory_route() {
                    let quality_record = self
                        .record_quality_decision(
                            &quality_input,
                            &quality_decision,
                            &relation_context,
                            Some(superseded_memory_id),
                            None,
                            now,
                        )
                        .await?;
                    let response = Self::quality_suppressed_response(
                        MemoryWriteRelation::CompatibleUpdate,
                        prepared,
                    );
                    return Ok(Self::with_quality_route(
                        response,
                        &quality_decision,
                        &ownership_route,
                        Some(quality_record.id),
                    ));
                }
                let response = self
                    .remember_semantic_active(
                        context,
                        &params,
                        &prepared,
                        content.as_str(),
                        sensitivity,
                        semantic_write_provenance(&params, &relation_context),
                        metadata_json,
                        Some(superseded_memory_id),
                    )
                    .await?;
                let semantic_response = MemorySemanticWriteResponse {
                    relation: MemoryWriteRelation::CompatibleUpdate,
                    canonical_key: prepared.canonical,
                    semantic_fingerprint: prepared.semantic_fingerprint,
                    record: Some(response.record),
                    candidate: None,
                    created: response.created,
                    superseded_memory_id: response.superseded_memory_id,
                    evidence_merged: false,
                    route: None,
                };
                let quality_record = self
                    .record_quality_decision_for_response(
                        &quality_input,
                        &quality_decision,
                        &relation_context,
                        &semantic_response,
                        now,
                    )
                    .await?;
                let semantic_response = Self::with_quality_route(
                    semantic_response,
                    &quality_decision,
                    &ownership_route,
                    Some(quality_record.id),
                );
                self.record_semantic_write_relation(
                    MemoryWriteRelation::CompatibleUpdate,
                    "active_supersession",
                    &relation_context,
                    semantic_response
                        .record
                        .as_ref()
                        .map(|record| record.id.clone()),
                    relation_context.workspace_id.clone(),
                    now,
                )
                .await?;
                return Ok(semantic_response);
            }

            let (quality_input, quality_decision) = self.semantic_quality_decision(
                &params,
                &prepared,
                MemoryWriteRelation::Contradiction,
                sensitivity,
            );
            let ownership_route = Self::quality_ownership_route(&quality_input, &quality_decision);
            let response = if ownership_route.permits_candidate_policy() {
                let response = self
                    .route_semantic_candidate_policy(
                        context.clone(),
                        params,
                        prepared,
                        content.as_str(),
                        metadata_json,
                        MemoryWriteRelation::Contradiction,
                        None,
                        &quality_input,
                        &quality_decision,
                        &ownership_route,
                        now,
                    )
                    .await?;
                let quality_record = self
                    .record_quality_decision_for_response(
                        &quality_input,
                        &quality_decision,
                        &context,
                        &response,
                        now,
                    )
                    .await?;
                Self::with_quality_route(
                    response,
                    &quality_decision,
                    &ownership_route,
                    Some(quality_record.id),
                )
            } else {
                let quality_record = self
                    .record_quality_decision(
                        &quality_input,
                        &quality_decision,
                        &context,
                        Some(existing.id.clone()),
                        None,
                        now,
                    )
                    .await?;
                let response =
                    Self::quality_suppressed_response(MemoryWriteRelation::Contradiction, prepared);
                Self::with_quality_route(
                    response,
                    &quality_decision,
                    &ownership_route,
                    Some(quality_record.id),
                )
            };
            self.record_semantic_write_relation(
                MemoryWriteRelation::Contradiction,
                "same_key_value_conflict",
                &context,
                Some(existing.id.clone()),
                existing.workspace_id.clone(),
                now,
            )
            .await?;
            return Ok(response);
        }

        if disposition != MemorySemanticWriteDisposition::AcceptActive
            && let Some(rejected) = self
                .store
                .get_agent_memory_candidate_by_dedupe(
                    params.scope.clone(),
                    Some(prepared.canonical.namespace.as_str()),
                    prepared.dedupe_key.as_str(),
                    rejected_candidate_statuses(),
                    context.workspace_guard(),
                )
                .await?
        {
            let (quality_input, quality_decision) = self.semantic_quality_decision(
                &params,
                &prepared,
                MemoryWriteRelation::SuppressedByRejection,
                sensitivity,
            );
            let ownership_route = Self::quality_ownership_route(&quality_input, &quality_decision);
            let quality_record = self
                .record_quality_decision(
                    &quality_input,
                    &quality_decision,
                    &context,
                    None,
                    Some(rejected.id.clone()),
                    now,
                )
                .await?;
            self.record_semantic_write_relation(
                MemoryWriteRelation::SuppressedByRejection,
                "suppressed_duplicate",
                &context,
                None,
                rejected.workspace_id.clone(),
                now,
            )
            .await?;
            let response = Self::quality_suppressed_response(
                MemoryWriteRelation::SuppressedByRejection,
                prepared,
            );
            return Ok(Self::with_quality_route(
                response,
                &quality_decision,
                &ownership_route,
                Some(quality_record.id),
            ));
        }

        if disposition != MemorySemanticWriteDisposition::AcceptActive
            && let Some(pending) = self
                .store
                .get_agent_memory_candidate_by_dedupe(
                    params.scope.clone(),
                    Some(prepared.canonical.namespace.as_str()),
                    prepared.dedupe_key.as_str(),
                    pending_candidate_statuses(),
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
            let pending_id = updated.id.clone();
            let pending_workspace_id = updated.workspace_id.clone();
            let (quality_input, quality_decision) = self.semantic_quality_decision(
                &params,
                &prepared,
                MemoryWriteRelation::Duplicate,
                sensitivity,
            );
            let ownership_route = Self::quality_ownership_route(&quality_input, &quality_decision);
            let quality_record = self
                .record_quality_decision(
                    &quality_input,
                    &quality_decision,
                    &context,
                    None,
                    Some(pending_id),
                    now,
                )
                .await?;
            self.record_semantic_write_relation(
                MemoryWriteRelation::Duplicate,
                "pending_duplicate",
                &context,
                None,
                pending_workspace_id,
                now,
            )
            .await?;
            let response = MemorySemanticWriteResponse {
                relation: MemoryWriteRelation::Duplicate,
                canonical_key: prepared.canonical,
                semantic_fingerprint: prepared.semantic_fingerprint,
                record: None,
                candidate: Some(crud_candidate_to_protocol(updated)?),
                created: false,
                superseded_memory_id: None,
                evidence_merged: true,
                route: None,
            };
            return Ok(Self::with_quality_route(
                response,
                &quality_decision,
                &ownership_route,
                Some(quality_record.id),
            ));
        }

        let relation_context = context.clone();
        let (quality_input, quality_decision) = self.semantic_quality_decision(
            &params,
            &prepared,
            MemoryWriteRelation::Novel,
            sensitivity,
        );
        let ownership_route = Self::quality_ownership_route(&quality_input, &quality_decision);
        if ownership_route.is_terminal_non_memory_route() {
            let quality_record = self
                .record_quality_decision(
                    &quality_input,
                    &quality_decision,
                    &relation_context,
                    None,
                    None,
                    now,
                )
                .await?;
            self.record_semantic_write_relation(
                MemoryWriteRelation::Novel,
                "quality_gate_suppressed",
                &relation_context,
                None,
                relation_context.workspace_id.clone(),
                now,
            )
            .await?;
            let response = Self::quality_suppressed_response(MemoryWriteRelation::Novel, prepared);
            return Ok(Self::with_quality_route(
                response,
                &quality_decision,
                &ownership_route,
                Some(quality_record.id),
            ));
        }

        if disposition == MemorySemanticWriteDisposition::AcceptActive {
            let response = self
                .remember_semantic_active(
                    context,
                    &params,
                    &prepared,
                    content.as_str(),
                    sensitivity,
                    semantic_write_provenance(&params, &relation_context),
                    metadata_json,
                    None,
                )
                .await?;
            let semantic_response = MemorySemanticWriteResponse {
                relation: MemoryWriteRelation::Novel,
                canonical_key: prepared.canonical,
                semantic_fingerprint: prepared.semantic_fingerprint,
                record: Some(response.record),
                candidate: None,
                created: response.created,
                superseded_memory_id: response.superseded_memory_id,
                evidence_merged: false,
                route: None,
            };
            let quality_record = self
                .record_quality_decision_for_response(
                    &quality_input,
                    &quality_decision,
                    &relation_context,
                    &semantic_response,
                    now,
                )
                .await?;
            let semantic_response = Self::with_quality_route(
                semantic_response,
                &quality_decision,
                &ownership_route,
                Some(quality_record.id),
            );
            self.record_semantic_write_relation(
                MemoryWriteRelation::Novel,
                "active_created",
                &relation_context,
                semantic_response
                    .record
                    .as_ref()
                    .map(|record| record.id.clone()),
                relation_context.workspace_id.clone(),
                now,
            )
            .await?;
            return Ok(semantic_response);
        }

        if matches!(
            disposition,
            MemorySemanticWriteDisposition::CreatePendingCandidate
                | MemorySemanticWriteDisposition::RejectSuppressed
        ) {
            let candidate = self
                .create_semantic_candidate(
                    &relation_context,
                    &params,
                    &prepared,
                    content.as_str(),
                    metadata_json,
                    if disposition == MemorySemanticWriteDisposition::RejectSuppressed {
                        MemoryCandidateStatus::Rejected
                    } else {
                        MemoryCandidateStatus::Pending
                    },
                    now,
                    "semantic_candidate",
                    None,
                )
                .await?;
            let semantic_response = MemorySemanticWriteResponse {
                relation: MemoryWriteRelation::Novel,
                canonical_key: prepared.canonical,
                semantic_fingerprint: prepared.semantic_fingerprint,
                record: None,
                candidate: Some(candidate),
                created: true,
                superseded_memory_id: None,
                evidence_merged: false,
                route: None,
            };
            let quality_record = self
                .record_quality_decision_for_response(
                    &quality_input,
                    &quality_decision,
                    &relation_context,
                    &semantic_response,
                    now,
                )
                .await?;
            let semantic_response = Self::with_quality_route(
                semantic_response,
                &quality_decision,
                &ownership_route,
                Some(quality_record.id),
            );
            self.record_semantic_write_relation(
                MemoryWriteRelation::Novel,
                if disposition == MemorySemanticWriteDisposition::RejectSuppressed {
                    "suppressed_created"
                } else {
                    "candidate_created"
                },
                &relation_context,
                None,
                relation_context.workspace_id.clone(),
                now,
            )
            .await?;
            return Ok(semantic_response);
        }

        let response = self
            .route_semantic_candidate_policy(
                context,
                params,
                prepared,
                content.as_str(),
                metadata_json,
                MemoryWriteRelation::Novel,
                None,
                &quality_input,
                &quality_decision,
                &ownership_route,
                now,
            )
            .await?;
        let quality_record = self
            .record_quality_decision_for_response(
                &quality_input,
                &quality_decision,
                &relation_context,
                &response,
                now,
            )
            .await?;
        Ok(Self::with_quality_route(
            response,
            &quality_decision,
            &ownership_route,
            Some(quality_record.id),
        ))
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
            .hydrate_control_plane_row(row, &context, &allowed_statuses, now, true)
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
            .hydrate_control_plane_row(row, &context, &[], now, true)
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
                key: None,
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
                .hydrate_control_plane_row(row, &context, &params.statuses, now, false)
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
        self.search_with_diagnostics(context, params)
            .await
            .map(|result| result.response)
    }

    pub async fn inspect_memory_debug(
        &self,
        context: MemoryOperationContext,
        memory_id: &str,
    ) -> Result<MemoryDebugTrace> {
        let target = MemoryDebugTraceTarget::memory(memory_id);
        let Some(row) = self.store.get_agent_memory_record(memory_id, true).await? else {
            return Ok(MemoryDebugTrace::missing(
                target,
                MemoryDebugMissingDataKind::MemoryRecord,
            ));
        };
        if !workspace_visible(&row, &context) {
            return Ok(MemoryDebugTrace::missing(
                target,
                MemoryDebugMissingDataKind::MemoryRecord,
            ));
        }

        let events = self
            .store
            .list_agent_memory_events(memory_id, MEMORY_DEBUG_TRACE_MAX_EVENTS)
            .await?;
        let quality_decisions = self
            .store
            .list_agent_memory_quality_decisions_for_memory(
                memory_id,
                MEMORY_DEBUG_TRACE_MAX_QUALITY_DECISIONS,
            )
            .await?;
        let quarantine_history = self
            .store
            .list_agent_memory_quarantine_history(
                memory_id,
                MEMORY_DEBUG_TRACE_MAX_QUARANTINE_HISTORY,
            )
            .await?;
        let active_quarantine = quarantine_history
            .iter()
            .any(|quarantine| quarantine.resolved_at_unix.is_none());
        let repair_jobs = self
            .store
            .list_agent_memory_repair_jobs_for_memory(memory_id, MEMORY_DEBUG_TRACE_MAX_REPAIR_JOBS)
            .await?;
        let latest_quality = quality_decisions.first();
        let score = memory_debug_score_from_metadata(row.metadata_json.as_deref());
        let write = MemoryDebugWriteTrace {
            outcome: write_outcome_for_memory(&row, latest_quality, active_quarantine, &events),
            relation: latest_quality.map(|quality| quality.relation),
            semantic_route: semantic_route_from_quality(latest_quality),
            latest_quality: latest_quality.map(memory_debug_quality_trace),
            score,
            source_context: Some(memory_debug_source_context_from_record(&row)),
            events: events.iter().map(memory_debug_event_trace).collect(),
            reason: latest_quality.map(|quality| {
                format!(
                    "quality_action={:?} target_ownership={:?}",
                    quality.action, quality.target_ownership
                )
            }),
        };
        let mut missing = Vec::new();
        if latest_quality.is_none() {
            missing.push(MemoryDebugMissingData::new(
                MemoryDebugMissingDataKind::QualityDecision,
                "no quality decision found for memory",
            ));
        }
        if row.source_context_kind.is_none() {
            missing.push(MemoryDebugMissingData::new(
                MemoryDebugMissingDataKind::SourceContext,
                "memory has no source_context_kind",
            ));
        }
        Ok(MemoryDebugTrace {
            target,
            found: true,
            lifecycle_state: memory_debug_item_from_record(&row, active_quarantine).lifecycle_state,
            item: Some(memory_debug_item_from_record(&row, active_quarantine)),
            write: Some(write),
            recall: None,
            quarantine_history: quarantine_history
                .iter()
                .map(memory_debug_quarantine_trace)
                .collect(),
            repair_jobs: repair_jobs.iter().map(memory_debug_repair_trace).collect(),
            missing,
        })
    }

    pub async fn inspect_candidate_debug(
        &self,
        context: MemoryOperationContext,
        candidate_id: &str,
    ) -> Result<MemoryDebugTrace> {
        let target = MemoryDebugTraceTarget::candidate(candidate_id);
        let Some(candidate) = self
            .store
            .get_agent_memory_candidate(candidate_id, context.workspace_guard())
            .await?
        else {
            return Ok(MemoryDebugTrace::missing(
                target,
                MemoryDebugMissingDataKind::CandidateRecord,
            ));
        };
        if !candidate_workspace_visible(&candidate, &context) {
            return Ok(MemoryDebugTrace::missing(
                target,
                MemoryDebugMissingDataKind::CandidateRecord,
            ));
        }

        let events = self
            .store
            .list_agent_memory_candidate_events(candidate_id, MEMORY_DEBUG_TRACE_MAX_EVENTS)
            .await?;
        let quality_decisions = self
            .store
            .list_agent_memory_quality_decisions_for_candidate(
                candidate_id,
                MEMORY_DEBUG_TRACE_MAX_QUALITY_DECISIONS,
            )
            .await?;
        let latest_quality = quality_decisions.first();
        let score = memory_debug_score_from_metadata(candidate.metadata_json.as_deref());
        let mut missing = Vec::new();
        if latest_quality.is_none() {
            missing.push(MemoryDebugMissingData::new(
                MemoryDebugMissingDataKind::QualityDecision,
                "no quality decision found for candidate",
            ));
        }
        if score.is_none() {
            missing.push(MemoryDebugMissingData::new(
                MemoryDebugMissingDataKind::CandidateScore,
                "candidate has no score metadata",
            ));
        }
        if candidate.source_context_kind.is_none() {
            missing.push(MemoryDebugMissingData::new(
                MemoryDebugMissingDataKind::SourceContext,
                "candidate has no source_context_kind",
            ));
        }
        let item = memory_debug_item_from_candidate(&candidate);
        Ok(MemoryDebugTrace {
            target,
            found: true,
            lifecycle_state: item.lifecycle_state,
            item: Some(item),
            write: Some(MemoryDebugWriteTrace {
                outcome: write_outcome_for_candidate(&candidate, latest_quality),
                relation: latest_quality.map(|quality| quality.relation),
                semantic_route: semantic_route_from_quality(latest_quality),
                latest_quality: latest_quality.map(memory_debug_quality_trace),
                score: score.or_else(|| Some(crate::debug::MemoryDebugScoreTrace::missing())),
                source_context: Some(memory_debug_source_context_from_candidate(&candidate)),
                events: events.iter().map(memory_debug_event_trace).collect(),
                reason: latest_quality.map(|quality| {
                    format!(
                        "quality_action={:?} target_ownership={:?}",
                        quality.action, quality.target_ownership
                    )
                }),
            }),
            recall: None,
            quarantine_history: Vec::new(),
            repair_jobs: Vec::new(),
            missing,
        })
    }

    pub async fn inspect_hook_run_memory_debug(
        &self,
        context: MemoryOperationContext,
        hook_run_id: &str,
    ) -> Result<MemoryDebugTrace> {
        let target = MemoryDebugTraceTarget::hook_run(hook_run_id);
        let hook_run_id = HookRunId::new(hook_run_id.to_owned())
            .with_context(|| format!("invalid hook run id `{hook_run_id}`"))?;
        let Some(run) = self.store.find_hook_run(&hook_run_id).await? else {
            return Ok(MemoryDebugTrace::missing(
                target,
                MemoryDebugMissingDataKind::HookRun,
            ));
        };
        if !hook_run_visible(&run, &context) {
            return Ok(MemoryDebugTrace::missing(
                target,
                MemoryDebugMissingDataKind::HookRun,
            ));
        }
        let audit_events = self
            .store
            .list_hook_audit_events_for_run(&hook_run_id)
            .await?;
        Ok(MemoryDebugTrace {
            target,
            found: true,
            lifecycle_state: crate::debug::MemoryDebugLifecycleState::Active,
            item: None,
            write: None,
            recall: Some(memory_debug_recall_trace_from_hook_run(&run, &audit_events)),
            quarantine_history: Vec::new(),
            repair_jobs: Vec::new(),
            missing: Vec::new(),
        })
    }

    pub async fn inspect_turn_memory_debug(
        &self,
        context: MemoryOperationContext,
        turn_id: &str,
        limit: Option<u64>,
    ) -> Result<MemoryDebugTrace> {
        let target = MemoryDebugTraceTarget::turn(turn_id, context.workspace_id.clone());
        let limit = limit
            .unwrap_or(MEMORY_DEBUG_TRACE_MAX_HOOK_RUNS)
            .min(MEMORY_DEBUG_TRACE_MAX_HOOK_RUNS);
        let runs = self
            .store
            .list_hook_runs_for_turn(turn_id, Some(HookPhase::TurnPrePromptContext), limit)
            .await?
            .into_iter()
            .filter(|run| hook_run_visible(run, &context))
            .collect::<Vec<_>>();
        if runs.is_empty() {
            return Ok(MemoryDebugTrace {
                target,
                found: false,
                lifecycle_state: crate::debug::MemoryDebugLifecycleState::Missing,
                item: None,
                write: None,
                recall: Some(MemoryDebugRecallTrace::default()),
                quarantine_history: Vec::new(),
                repair_jobs: Vec::new(),
                missing: vec![MemoryDebugMissingData::new(
                    MemoryDebugMissingDataKind::HookRun,
                    "no memory hook runs found for turn",
                )],
            });
        }
        let mut audit_events_by_run = BTreeMap::new();
        for run in &runs {
            audit_events_by_run.insert(
                run.id.as_str().to_owned(),
                self.store.list_hook_audit_events_for_run(&run.id).await?,
            );
        }
        Ok(MemoryDebugTrace {
            target,
            found: true,
            lifecycle_state: crate::debug::MemoryDebugLifecycleState::Active,
            item: None,
            write: None,
            recall: Some(memory_debug_recall_trace_from_hook_runs(
                &runs,
                &audit_events_by_run,
            )),
            quarantine_history: Vec::new(),
            repair_jobs: Vec::new(),
            missing: Vec::new(),
        })
    }

    pub async fn inspect_turn_memory_write_debug(
        &self,
        context: MemoryOperationContext,
        thread_id: &str,
        turn_id: &str,
        limit: Option<u64>,
    ) -> Result<MemoryDebugTrace> {
        let mut target = MemoryDebugTraceTarget::turn(turn_id, context.workspace_id.clone());
        target.thread_id = Some(thread_id.to_owned());
        let limit = limit
            .unwrap_or(MEMORY_DEBUG_TRACE_MAX_QUALITY_DECISIONS)
            .min(MEMORY_DEBUG_TRACE_MAX_QUALITY_DECISIONS);
        let decisions = self
            .store
            .list_agent_memory_quality_decisions_for_thread(thread_id, limit)
            .await?
            .into_iter()
            .filter(|decision| decision.turn_id.as_deref() == Some(turn_id))
            .filter(|decision| quality_decision_visible(decision, &context))
            .collect::<Vec<_>>();
        let Some(latest_quality) = decisions.first() else {
            return Ok(MemoryDebugTrace {
                target,
                found: false,
                lifecycle_state: crate::debug::MemoryDebugLifecycleState::Missing,
                item: None,
                write: Some(MemoryDebugWriteTrace::default()),
                recall: None,
                quarantine_history: Vec::new(),
                repair_jobs: Vec::new(),
                missing: vec![MemoryDebugMissingData::new(
                    MemoryDebugMissingDataKind::QualityDecision,
                    "no memory quality decisions found for turn",
                )],
            });
        };

        let write = MemoryDebugWriteTrace {
            outcome: write_outcome_for_quality_decision(latest_quality),
            relation: Some(latest_quality.relation),
            semantic_route: semantic_route_from_quality(Some(latest_quality)),
            latest_quality: Some(memory_debug_quality_trace(latest_quality)),
            score: None,
            source_context: Some(MemoryDebugSourceContextTrace {
                source_context_kind: Some(latest_quality.source_context_kind),
                source_thread_id: latest_quality.thread_id.clone(),
                source_turn_id: latest_quality.turn_id.clone(),
                source_item_id: latest_quality.item_id.clone(),
                workspace_id: latest_quality.workspace_id.clone(),
            }),
            events: Vec::new(),
            reason: Some(format!(
                "quality_action={:?} target_ownership={:?}",
                latest_quality.action, latest_quality.target_ownership
            )),
        };

        Ok(MemoryDebugTrace {
            target,
            found: true,
            lifecycle_state: crate::debug::MemoryDebugLifecycleState::Active,
            item: None,
            write: Some(write),
            recall: None,
            quarantine_history: Vec::new(),
            repair_jobs: Vec::new(),
            missing: Vec::new(),
        })
    }

    pub async fn list_workspace_memory_debug_events(
        &self,
        workspace_id: &str,
        limit: Option<u64>,
    ) -> Result<Vec<crate::debug::MemoryDebugEventTrace>> {
        let limit = limit
            .unwrap_or(MEMORY_DEBUG_TRACE_MAX_EVENTS)
            .min(MEMORY_DEBUG_TRACE_MAX_EVENTS);
        self.store
            .list_workspace_agent_memory_events(workspace_id, limit)
            .await
            .map(|events| events.iter().map(memory_debug_event_trace).collect())
    }

    async fn search_with_diagnostics(
        &self,
        context: MemoryOperationContext,
        params: MemorySearchParams,
    ) -> Result<MemorySearchWithDiagnostics> {
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
        let mut diagnostics = MemoryRecallServiceDiagnostics::default();
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
                diagnostics.record_stale_backend_hit();
                continue;
            };
            if !scope_matches(&row.scope, &active_scopes.scopes)
                || !category_matches(row.category, &params.categories)
            {
                continue;
            }
            let recency_anchor_unix = recency_anchor_unix(&row);
            let Some((record, quality)) = self
                .hydrate_visible_row_with_quality_and_diagnostics(
                    row,
                    &context,
                    &params.statuses,
                    now,
                    true,
                    Some(&mut diagnostics),
                )
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
                quality,
            });
        }
        if candidates.is_empty() && !backend_returned_any {
            let fallback_rows = self
                .store
                .list_agent_memory_records(AgentMemoryListFilter {
                    scopes: active_scopes.scopes.clone(),
                    workspace_guard: context.workspace_guard(),
                    namespace: None,
                    key: None,
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
                let Some((record, quality)) = self
                    .hydrate_visible_row_with_quality_and_diagnostics(
                        row,
                        &context,
                        &params.statuses,
                        now,
                        true,
                        Some(&mut diagnostics),
                    )
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
                    quality,
                });
            }
        }
        let ranking = rank_memory_search_hits_with_diagnostics(
            candidates,
            query,
            &params.categories,
            &active_scopes,
            &self.config.ranking,
            now,
            limit,
        );
        diagnostics.extend_ranking(&ranking.diagnostics);

        Ok(MemorySearchWithDiagnostics {
            response: MemorySearchResponse {
                hits: ranking.hits,
                next_cursor: None,
            },
            diagnostics: diagnostics.into_safe_strings(),
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
            .search_with_diagnostics(
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
        for hit in search.response.hits {
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

        Ok(MemoryRecallResponse {
            items,
            diagnostics: search.diagnostics,
        })
    }

    pub async fn recall_mode_for_prompt(
        &self,
        context: MemoryOperationContext,
        params: MemoryModeRecallParams,
    ) -> Result<MemoryModeRecallResponse> {
        let top_k = self.normalized_prompt_top_k(params.top_k);
        let max_chars = params
            .max_chars
            .unwrap_or(self.config.recall.max_prompt_chars)
            .min(self.config.recall.max_prompt_chars);
        if params.mode == MemoryRecallMode::ExactCanonical {
            return self
                .recall_exact_canonical_for_prompt(context, params, top_k, max_chars)
                .await;
        }
        if matches!(
            params.mode,
            MemoryRecallMode::ThreadEpisodic | MemoryRecallMode::TaskContext
        ) {
            return Ok(MemoryModeRecallResponse {
                diagnostics: vec![format!(
                    "memory.active_recall.mode_native_provider_required:{}",
                    params.mode.as_str()
                )],
                skipped_reason: Some(format!(
                    "{}_native_provider_required",
                    params.mode.as_str()
                )),
                ..MemoryModeRecallResponse::default()
            });
        }

        let scopes = memory_recall_mode_scopes(&context, params.mode);
        if scopes.is_empty() {
            return Ok(MemoryModeRecallResponse {
                skipped_reason: Some(format!("{}_scope_unavailable", params.mode.as_str())),
                ..MemoryModeRecallResponse::default()
            });
        }
        let categories = memory_recall_mode_categories(params.mode);
        let rows = match self
            .store
            .list_agent_memory_records(AgentMemoryListFilter {
                scopes,
                workspace_guard: context.workspace_guard(),
                namespace: None,
                key: None,
                categories: categories.clone(),
                statuses: Vec::new(),
                include_expired: false,
                include_deleted: false,
                include_superseded: false,
                limit: Some(u64::from(top_k)),
            })
            .await
        {
            Ok(rows) => rows,
            Err(_)
                if matches!(
                    params.mode,
                    MemoryRecallMode::ThreadEpisodic | MemoryRecallMode::TaskContext
                ) =>
            {
                return Ok(MemoryModeRecallResponse {
                    diagnostics: vec![format!(
                        "memory.active_recall.mode_scope_unavailable:{}",
                        params.mode.as_str()
                    )],
                    skipped_reason: Some(format!("{}_scope_unavailable", params.mode.as_str())),
                    ..MemoryModeRecallResponse::default()
                });
            }
            Err(error) => return Err(error),
        };

        let mode = params.mode;
        self.recall_items_from_control_rows(
            context,
            rows,
            top_k,
            max_chars,
            params.mode.as_str(),
            &categories,
        )
        .await
        .map(|mut response| {
            response.diagnostics.push(format!(
                "memory.active_recall.mode_executed:{}",
                mode.as_str()
            ));
            response
        })
    }

    async fn recall_exact_canonical_for_prompt(
        &self,
        context: MemoryOperationContext,
        params: MemoryModeRecallParams,
        top_k: u32,
        max_chars: usize,
    ) -> Result<MemoryModeRecallResponse> {
        let targets = params
            .targets
            .iter()
            .filter(|target| exact_target_has_lookup_key(&context, target))
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return Ok(MemoryModeRecallResponse {
                skipped_reason: Some("missing_canonical_target".to_owned()),
                ..MemoryModeRecallResponse::default()
            });
        }

        let mut rows = Vec::new();
        let mut seen = BTreeSet::new();
        for target in &targets {
            if rows.len() >= top_k as usize {
                break;
            }
            let scopes = exact_target_scopes(&context, target);
            if scopes.is_empty() {
                continue;
            }
            let categories = target.category.into_iter().collect::<Vec<_>>();
            for key in exact_target_lookup_keys(&context, target) {
                let target_rows = self
                    .store
                    .list_agent_memory_records(AgentMemoryListFilter {
                        scopes: scopes.clone(),
                        workspace_guard: context.workspace_guard(),
                        namespace: None,
                        key: Some(key),
                        categories: categories.clone(),
                        statuses: Vec::new(),
                        include_expired: false,
                        include_deleted: false,
                        include_superseded: false,
                        limit: Some(u64::from(top_k)),
                    })
                    .await?;
                for row in target_rows {
                    if seen.insert(row.id.clone()) {
                        rows.push(row);
                    }
                    if rows.len() >= top_k as usize {
                        break;
                    }
                }
                if rows.len() >= top_k as usize {
                    break;
                }
            }
        }

        let ranking_query = targets
            .iter()
            .flat_map(|target| exact_target_lookup_keys(&context, target))
            .collect::<Vec<_>>()
            .join(" ");
        self.recall_items_from_control_rows(
            context,
            rows,
            top_k,
            max_chars,
            ranking_query.as_str(),
            &[],
        )
        .await
        .map(|mut response| {
            response
                .diagnostics
                .push("memory.active_recall.mode_executed:exact_canonical".to_owned());
            response
        })
    }

    async fn recall_items_from_control_rows(
        &self,
        context: MemoryOperationContext,
        rows: Vec<AgentMemoryControlRecord>,
        top_k: u32,
        max_chars: usize,
        ranking_query: &str,
        requested_categories: &[MemoryCategory],
    ) -> Result<MemoryModeRecallResponse> {
        let now = context.now_or(current_unix());
        let mut remaining_chars = max_chars;
        let item_max_chars = self.config.recall.max_item_chars.max(1);
        let raw_count = rows.len();
        let mut candidates = Vec::new();
        let mut truncated = false;
        let mut diagnostics = MemoryRecallServiceDiagnostics::default();
        for row in rows {
            let recency_anchor_unix = recency_anchor_unix(&row);
            let Some((record, quality)) = self
                .hydrate_visible_row_with_quality_and_diagnostics(
                    row,
                    &context,
                    &[],
                    now,
                    true,
                    Some(&mut diagnostics),
                )
                .await?
            else {
                continue;
            };
            candidates.push(MemoryRankingCandidate {
                hit: MemorySearchHit {
                    record,
                    score: None,
                    snippet: None,
                    matched_terms: Vec::new(),
                },
                backend_score: None,
                recency_anchor_unix,
                quality,
            });
        }
        let active_scopes = context.active_scopes(&[]);
        let ranking = rank_memory_search_hits_with_diagnostics(
            candidates,
            ranking_query,
            requested_categories,
            &active_scopes,
            &self.config.ranking,
            now,
            top_k,
        );
        diagnostics.extend_ranking(&ranking.diagnostics);

        let mut items = Vec::new();
        for hit in ranking.hits {
            if items.len() >= top_k as usize || remaining_chars == 0 {
                truncated = true;
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
        if raw_count > items.len() && items.len() >= top_k as usize {
            truncated = true;
        }

        Ok(MemoryModeRecallResponse {
            items,
            diagnostics: diagnostics.into_safe_strings(),
            truncated,
            skipped_reason: None,
        })
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

    pub async fn quarantine_memory(
        &self,
        context: MemoryOperationContext,
        params: MemoryQuarantineRequest,
    ) -> Result<MemoryQuarantineResponse> {
        let now = context.now_or(current_unix());
        let row = self
            .store
            .get_agent_memory_record(params.memory_id.as_str(), true)
            .await?
            .with_context(|| format!("memory `{}` was not found", params.memory_id))?;
        if row.status != MemoryStatus::Active {
            bail!(
                "memory `{}` cannot be quarantined because it is not active",
                row.id
            );
        }
        if !workspace_visible(&row, &context) {
            bail!(
                "memory `{}` is not visible in this workspace context",
                row.id
            );
        }
        let actor =
            lifecycle_actor_to_crud(params.actor.clone(), MemoryLifecycleActorKind::Service);
        let quarantine = self
            .store
            .create_agent_memory_quarantine_marker(NewAgentMemoryQuarantine {
                id: None,
                memory_id: row.id.clone(),
                workspace_id: row.workspace_id.clone(),
                reason_code: params.reason_code,
                actor,
                details_json: params.details_json.clone(),
                created_at_unix: now,
            })
            .await?;
        let repair_job = if params.schedule_backend_cleanup {
            Some(
                self.enqueue_backend_repair(
                    &row,
                    REPAIR_JOB_BACKEND_QUARANTINE_CLEANUP,
                    "quarantine",
                    None,
                    now,
                )
                .await?,
            )
        } else {
            None
        };
        Ok(MemoryQuarantineResponse {
            quarantine,
            repair_job,
        })
    }

    pub async fn restore_quarantined_memory(
        &self,
        context: MemoryOperationContext,
        params: MemoryRestoreRequest,
    ) -> Result<MemoryRestoreResponse> {
        let now = context.now_or(current_unix());
        let row = self
            .store
            .get_agent_memory_record(params.memory_id.as_str(), true)
            .await?
            .with_context(|| format!("memory `{}` was not found", params.memory_id))?;
        if !workspace_visible(&row, &context) {
            bail!(
                "memory `{}` is not visible in this workspace context",
                row.id
            );
        }
        let actor =
            lifecycle_actor_to_crud(params.actor.clone(), MemoryLifecycleActorKind::Service);
        let quarantine = self
            .store
            .resolve_agent_memory_quarantine(ResolveAgentMemoryQuarantine {
                memory_id: row.id.clone(),
                reason_code: MemoryLifecycleReasonCode::ExplicitRestore,
                actor,
                resolved_at_unix: now,
            })
            .await?;
        let repair_job = if quarantine.is_some()
            && params.schedule_backend_reindex
            && row.status == MemoryStatus::Active
        {
            Some(self.enqueue_backend_reindex(&row, "restore", now).await?)
        } else {
            None
        };
        Ok(MemoryRestoreResponse {
            quarantine,
            repair_job,
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
                categories: params.categories.clone(),
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

    pub async fn get_candidate(
        &self,
        context: MemoryOperationContext,
        params: MemoryCandidatesGetParams,
    ) -> Result<MemoryCandidatesGetResponse> {
        let candidate = self
            .store
            .get_agent_memory_candidate(params.candidate_id.as_str(), context.workspace_guard())
            .await?
            .map(crud_candidate_to_protocol)
            .transpose()?;
        Ok(MemoryCandidatesGetResponse { candidate })
    }

    pub async fn approve_candidate(
        &self,
        context: MemoryOperationContext,
        params: MemoryCandidatesApproveParams,
    ) -> Result<MemoryCandidatesApproveResponse> {
        let candidate = self
            .load_visible_candidate(&context, params.candidate_id.as_str())
            .await?;
        if candidate.status == MemoryCandidateStatus::Approved {
            let record = self
                .load_promoted_candidate_memory(&context, &candidate)
                .await?;
            return Ok(MemoryCandidatesApproveResponse {
                candidate: crud_candidate_to_protocol(candidate)?,
                record,
            });
        }
        ensure_candidate_pending_for_transition(&candidate, "approve")?;
        let mut action_context = context.clone();
        if params.actor.is_some() {
            action_context.actor = params.actor.clone();
        }
        let write_params =
            candidate_semantic_write_params(&candidate, None, None, Some("candidate_approve"))?;
        let response = self
            .write_semantic_memory(action_context.clone(), write_params)
            .await?;
        let record = response.record.with_context(|| {
            format!(
                "candidate `{}` approval did not produce memory",
                candidate.id
            )
        })?;
        let updated = self
            .update_candidate_status(
                &action_context,
                candidate,
                MemoryCandidateStatus::Approved,
                params
                    .reason
                    .clone()
                    .unwrap_or_else(|| "approved".to_owned()),
                Some(record.id.clone()),
                None,
                action_context.now_or(current_unix()),
            )
            .await?;
        Ok(MemoryCandidatesApproveResponse {
            candidate: crud_candidate_to_protocol(updated)?,
            record,
        })
    }

    pub async fn reject_candidate(
        &self,
        context: MemoryOperationContext,
        params: MemoryCandidatesRejectParams,
    ) -> Result<MemoryCandidatesRejectResponse> {
        let candidate = self
            .load_visible_candidate(&context, params.candidate_id.as_str())
            .await?;
        if candidate_is_rejected(candidate.status) {
            return Ok(MemoryCandidatesRejectResponse {
                candidate: crud_candidate_to_protocol(candidate)?,
            });
        }
        ensure_candidate_pending_for_transition(&candidate, "reject")?;
        let mut action_context = context.clone();
        if params.actor.is_some() {
            action_context.actor = params.actor.clone();
        }
        let updated = self
            .update_candidate_status(
                &action_context,
                candidate,
                MemoryCandidateStatus::Rejected,
                params.reason.unwrap_or_else(|| "rejected".to_owned()),
                None,
                None,
                action_context.now_or(current_unix()),
            )
            .await?;
        Ok(MemoryCandidatesRejectResponse {
            candidate: crud_candidate_to_protocol(updated)?,
        })
    }

    pub async fn edit_and_approve_candidate(
        &self,
        context: MemoryOperationContext,
        params: MemoryCandidatesEditAndApproveParams,
    ) -> Result<MemoryCandidatesEditAndApproveResponse> {
        let edited_text = params.edited_text.trim();
        if edited_text.is_empty() {
            bail!("edited memory candidate text cannot be empty");
        }
        let candidate = self
            .load_visible_candidate(&context, params.candidate_id.as_str())
            .await?;
        ensure_candidate_pending_for_transition(&candidate, "edit_and_approve")?;
        let mut action_context = context.clone();
        if params.actor.is_some() {
            action_context.actor = params.actor.clone();
        }
        let write_params = candidate_semantic_write_params(
            &candidate,
            Some(edited_text.to_owned()),
            params.edited_value.clone(),
            Some("candidate_edit_and_approve"),
        )?;
        let response = self
            .write_semantic_memory(action_context.clone(), write_params)
            .await?;
        let record = response.record.with_context(|| {
            format!(
                "candidate `{}` edit-and-approval did not produce memory",
                candidate.id
            )
        })?;
        let metadata_json = candidate_metadata_with_lifecycle(
            candidate.metadata_json.as_deref(),
            serde_json::json!({
                "edited_text": edited_text,
                "edited_value": params.edited_value,
            }),
        )?;
        let updated = self
            .update_candidate_status(
                &action_context,
                candidate,
                MemoryCandidateStatus::Approved,
                params
                    .reason
                    .clone()
                    .unwrap_or_else(|| "edit_and_approved".to_owned()),
                Some(record.id.clone()),
                Some(metadata_json),
                action_context.now_or(current_unix()),
            )
            .await?;
        Ok(MemoryCandidatesEditAndApproveResponse {
            candidate: crud_candidate_to_protocol(updated)?,
            record,
        })
    }

    pub async fn merge_candidate(
        &self,
        context: MemoryOperationContext,
        params: MemoryCandidatesMergeParams,
    ) -> Result<MemoryCandidatesMergeResponse> {
        let candidate = self
            .load_visible_candidate(&context, params.candidate_id.as_str())
            .await?;
        ensure_candidate_pending_for_transition(&candidate, "merge")?;
        self.load_visible_candidate(&context, params.target_candidate_id.as_str())
            .await?;
        let mut action_context = context.clone();
        if params.actor.is_some() {
            action_context.actor = params.actor.clone();
        }
        let metadata_json = candidate_metadata_with_lifecycle(
            candidate.metadata_json.as_deref(),
            serde_json::json!({
                "merged_into_candidate_id": params.target_candidate_id,
            }),
        )?;
        let updated = self
            .update_candidate_status(
                &action_context,
                candidate,
                MemoryCandidateStatus::MergedDuplicate,
                params
                    .reason
                    .unwrap_or_else(|| "merged_duplicate".to_owned()),
                None,
                Some(metadata_json),
                action_context.now_or(current_unix()),
            )
            .await?;
        Ok(MemoryCandidatesMergeResponse {
            candidate: crud_candidate_to_protocol(updated)?,
        })
    }

    pub async fn suppress_similar_candidate(
        &self,
        context: MemoryOperationContext,
        params: MemoryCandidatesSuppressSimilarParams,
    ) -> Result<MemoryCandidatesSuppressSimilarResponse> {
        let candidate = self
            .load_visible_candidate(&context, params.candidate_id.as_str())
            .await?;
        if candidate_is_rejected(candidate.status) {
            return Ok(MemoryCandidatesSuppressSimilarResponse {
                candidate: crud_candidate_to_protocol(candidate)?,
            });
        }
        ensure_candidate_pending_for_transition(&candidate, "suppress_similar")?;
        let mut action_context = context.clone();
        if params.actor.is_some() {
            action_context.actor = params.actor.clone();
        }
        let metadata_json = candidate_metadata_with_lifecycle(
            candidate.metadata_json.as_deref(),
            serde_json::json!({
                "suppress_similar": true,
            }),
        )?;
        let updated = self
            .update_candidate_status(
                &action_context,
                candidate,
                MemoryCandidateStatus::AutoRejected,
                params
                    .reason
                    .unwrap_or_else(|| "suppress_similar".to_owned()),
                None,
                Some(metadata_json),
                action_context.now_or(current_unix()),
            )
            .await?;
        Ok(MemoryCandidatesSuppressSimilarResponse {
            candidate: crud_candidate_to_protocol(updated)?,
        })
    }

    pub async fn decide_candidate(
        &self,
        context: MemoryOperationContext,
        params: MemoryCandidatesDecideParams,
    ) -> Result<MemoryCandidatesDecideResponse> {
        match params.decision {
            MemoryCandidateDecision::Approve => {
                let response = self
                    .approve_candidate(
                        context,
                        MemoryCandidatesApproveParams {
                            candidate_id: params.candidate_id,
                            reason: params.reason,
                            actor: params.actor,
                        },
                    )
                    .await?;
                return Ok(MemoryCandidatesDecideResponse {
                    candidate: response.candidate,
                    record: Some(response.record),
                });
            }
            MemoryCandidateDecision::Reject => {
                let response = self
                    .reject_candidate(
                        context,
                        MemoryCandidatesRejectParams {
                            candidate_id: params.candidate_id,
                            reason: params.reason,
                            actor: params.actor,
                        },
                    )
                    .await?;
                return Ok(MemoryCandidatesDecideResponse {
                    candidate: response.candidate,
                    record: None,
                });
            }
            MemoryCandidateDecision::Expire => {}
        }
        let visible_pending = self
            .store
            .list_agent_memory_candidates(AgentMemoryCandidateListFilter {
                scopes: Vec::new(),
                workspace_guard: context.workspace_guard(),
                categories: Vec::new(),
                statuses: pending_candidate_statuses(),
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

    async fn load_visible_candidate(
        &self,
        context: &MemoryOperationContext,
        candidate_id: &str,
    ) -> Result<AgentMemoryCandidateRecord> {
        self.store
            .get_agent_memory_candidate(candidate_id, context.workspace_guard())
            .await?
            .with_context(|| format!("memory candidate `{candidate_id}` was not found"))
    }

    async fn load_promoted_candidate_memory(
        &self,
        context: &MemoryOperationContext,
        candidate: &AgentMemoryCandidateRecord,
    ) -> Result<MemoryRecord> {
        let memory_id = candidate.promoted_memory_id.as_deref().with_context(|| {
            format!(
                "approved memory candidate `{}` does not reference promoted memory",
                candidate.id
            )
        })?;
        let row = self
            .store
            .get_agent_memory_record(memory_id, false)
            .await?
            .with_context(|| format!("promoted memory `{memory_id}` was not found"))?;
        self.hydrate_visible_row(row, context, &[], context.now_or(current_unix()), false)
            .await?
            .with_context(|| format!("promoted memory `{memory_id}` is not visible"))
    }

    async fn update_candidate_status(
        &self,
        context: &MemoryOperationContext,
        candidate: AgentMemoryCandidateRecord,
        status: MemoryCandidateStatus,
        reason: String,
        promoted_memory_id: Option<String>,
        metadata_json: Option<String>,
        now: i64,
    ) -> Result<AgentMemoryCandidateRecord> {
        let updated = self
            .store
            .update_agent_memory_candidate_status(AgentMemoryCandidateStatusUpdateRecord {
                candidate_id: candidate.id.clone(),
                status,
                decided_by: protocol_actor_to_crud(context.actor.clone()),
                decision_reason: Some(reason.clone()),
                promoted_memory_id: promoted_memory_id.clone(),
                metadata_json,
                decided_at_unix: now,
            })
            .await?
            .with_context(|| format!("memory candidate `{}` disappeared", candidate.id))?;
        self.store
            .insert_agent_memory_policy_decision(NewAgentMemoryPolicyDecision {
                memory_id: promoted_memory_id,
                candidate_id: Some(updated.id.clone()),
                workspace_id: updated.workspace_id.clone(),
                action: "candidate_lifecycle".to_owned(),
                decision: candidate_status_label(status).to_owned(),
                reason_code: Some(reason.clone()),
                reason: Some(reason),
                policy_version: self.config.policy_version.clone(),
                actor: protocol_actor_to_crud(context.actor.clone()),
                thread_id: context.thread_id.clone(),
                turn_id: None,
                item_id: None,
                details_json: updated.metadata_json.clone(),
                created_at_unix: now,
            })
            .await?;
        Ok(updated)
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

    pub async fn process_repair_job(
        &self,
        job_id: &str,
        locked_by: &str,
        now_unix: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        let Some(job) = self.store.get_agent_memory_repair_job(job_id).await? else {
            return Ok(None);
        };
        if job.status != "running" || job.locked_by.as_deref() != Some(locked_by) {
            bail!("memory repair job `{job_id}` is not locked by `{locked_by}`");
        }

        let result = match job.job_kind.as_str() {
            REPAIR_JOB_BACKEND_STALE_PAYLOAD | REPAIR_JOB_MEMVID_STALE_VECTOR => {
                self.process_stale_backend_repair(&job).await
            }
            REPAIR_JOB_BACKEND_DELETE_FAILED | REPAIR_JOB_BACKEND_QUARANTINE_CLEANUP => {
                self.process_backend_delete_repair(&job).await
            }
            REPAIR_JOB_BACKEND_PAYLOAD_MISSING | REPAIR_JOB_BACKEND_REINDEX => {
                self.process_backend_reindex_repair(&job, now_unix).await
            }
            _ => bail!("unknown memory repair job kind `{}`", job.job_kind),
        };

        match result {
            Ok(result_json) => {
                self.complete_repair_job(job_id, locked_by, Some(result_json), now_unix)
                    .await
            }
            Err(error) => {
                self.fail_repair_job(
                    job_id,
                    locked_by,
                    bounded_error_message(error.to_string().as_str()),
                    Some(now_unix.saturating_add(60)),
                    now_unix,
                )
                .await
            }
        }
    }

    async fn remember_semantic_active(
        &self,
        context: MemoryOperationContext,
        params: &MemorySemanticWriteParams,
        prepared: &SemanticWritePrepared,
        content: &str,
        sensitivity: MemorySensitivity,
        provenance: MemoryProvenance,
        metadata_json: String,
        supersedes: Option<String>,
    ) -> Result<MemoryRememberResponse> {
        self.remember_with_source_context(
            context,
            MemoryRememberParams {
                scope: params.scope.clone(),
                category: prepared.canonical.category,
                namespace: Some(prepared.canonical.namespace.clone()),
                key: Some(prepared.canonical.key.clone()),
                content: content.to_owned(),
                sensitivity: Some(sensitivity),
                confidence: params.confidence,
                importance: params.importance,
                provenance: Some(provenance),
                source_context_kind: params.source_context_kind,
                idempotency_key: None,
                supersedes,
                metadata: serde_json::from_str(metadata_json.as_str())
                    .context("semantic metadata must decode")?,
            },
            params.source_context_kind,
        )
        .await
    }

    async fn route_semantic_candidate_policy(
        &self,
        context: MemoryOperationContext,
        params: MemorySemanticWriteParams,
        prepared: SemanticWritePrepared,
        content: &str,
        metadata_json: String,
        relation: MemoryWriteRelation,
        supersedes: Option<String>,
        quality_input: &MemoryQualityGateInput,
        quality_decision: &MemoryQualityDecision,
        ownership_route: &MemoryOwnershipRoute,
        now: i64,
    ) -> Result<MemorySemanticWriteResponse> {
        if !ownership_route.permits_candidate_policy() {
            return Ok(Self::quality_suppressed_response(relation, prepared));
        }

        let sensitivity = sensitivity_from_hint(params.semantic.sensitivity);
        let provenance = semantic_write_provenance(&params, &context);
        let policy_input = MemoryCandidatePolicyInput {
            semantic: params.semantic.clone(),
            relation,
            scope: params.scope.clone(),
            scope_clarity: scope_clarity_from_hint(params.semantic.scope_hint),
            evidence_count: metadata_evidence_count(metadata_json.as_str()).max(1) as u32,
            has_contradiction: relation == MemoryWriteRelation::Contradiction,
            has_duplicate: relation == MemoryWriteRelation::Duplicate,
            has_rejected_duplicate: relation == MemoryWriteRelation::SuppressedByRejection,
            sensitivity,
            active_no_memory_policy: false,
            quality_action: quality_decision.action,
            quality_target_ownership: quality_decision.target_ownership,
            quality_reason_codes: quality_decision.reason_codes.clone(),
            quality_candidate_auto_approve_allowed: quality_decision.candidate_auto_approve_allowed,
            source_context_kind: quality_input.source_context_kind,
            fact_class: quality_input.fact_class,
            lifetime_class: quality_input.lifetime_class,
            ownership_class: quality_input.ownership_class,
            evidence_class: quality_input.evidence_class,
            hook_run_id: params
                .metadata
                .get("hook_run_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
        };
        let policy_output = self.candidate_policy.decide(policy_input);
        let reason_code = policy_output.reason_code.clone();
        let policy_metadata_json =
            metadata_json_with_candidate_policy(metadata_json, &policy_output)?;

        if policy_output.decision == MemoryCandidatePolicyDecision::AutoApprove {
            let relation_context = context.clone();
            let response = self
                .remember_semantic_active(
                    context,
                    &params,
                    &prepared,
                    content,
                    sensitivity,
                    provenance,
                    policy_metadata_json,
                    supersedes,
                )
                .await?;
            self.record_candidate_policy_decision(
                &policy_output,
                &relation_context,
                Some(response.record.id.clone()),
                None,
                relation_context.workspace_id.clone(),
                now,
            )
            .await?;
            self.record_semantic_write_relation(
                relation,
                "candidate_policy_auto_approved",
                &relation_context,
                Some(response.record.id.clone()),
                relation_context.workspace_id.clone(),
                now,
            )
            .await?;
            return Ok(MemorySemanticWriteResponse {
                relation,
                canonical_key: prepared.canonical,
                semantic_fingerprint: prepared.semantic_fingerprint,
                record: Some(response.record),
                candidate: None,
                created: response.created,
                superseded_memory_id: response.superseded_memory_id,
                evidence_merged: false,
                route: None,
            });
        }

        let candidate = self
            .create_semantic_candidate(
                &context,
                &params,
                &prepared,
                content,
                policy_metadata_json,
                policy_output.status,
                now,
                reason_code.as_str(),
                None,
            )
            .await?;
        self.record_candidate_policy_decision(
            &policy_output,
            &context,
            None,
            Some(candidate.id.clone()),
            context.workspace_id.clone(),
            now,
        )
        .await?;
        self.record_semantic_write_relation(
            relation,
            match policy_output.decision {
                MemoryCandidatePolicyDecision::AutoReject => "candidate_policy_auto_rejected",
                MemoryCandidatePolicyDecision::RejectReviewDisabled => {
                    "candidate_policy_review_disabled_rejected"
                }
                MemoryCandidatePolicyDecision::PendingSilent
                | MemoryCandidatePolicyDecision::AskOnUse
                | MemoryCandidatePolicyDecision::NeedsReview => "candidate_policy_review_routed",
                MemoryCandidatePolicyDecision::AutoApprove => "candidate_policy_auto_approved",
            },
            &context,
            None,
            context.workspace_id.clone(),
            now,
        )
        .await?;

        Ok(MemorySemanticWriteResponse {
            relation,
            canonical_key: prepared.canonical,
            semantic_fingerprint: prepared.semantic_fingerprint,
            record: None,
            candidate: Some(candidate),
            created: true,
            superseded_memory_id: None,
            evidence_merged: false,
            route: None,
        })
    }

    async fn create_semantic_candidate(
        &self,
        context: &MemoryOperationContext,
        params: &MemorySemanticWriteParams,
        prepared: &SemanticWritePrepared,
        content: &str,
        metadata_json: String,
        status: MemoryCandidateStatus,
        now: i64,
        reason: &str,
        policy_output: Option<&MemoryCandidatePolicyOutput>,
    ) -> Result<MemoryCandidate> {
        let provenance = semantic_write_provenance(params, context);
        let metadata_json = if let Some(policy_output) = policy_output {
            metadata_json_with_candidate_policy(metadata_json, policy_output)?
        } else {
            metadata_json
        };
        let candidate = self
            .store
            .insert_agent_memory_candidate(
                NewAgentMemoryCandidate {
                    id: None,
                    scope: params.scope.clone(),
                    namespace: Some(prepared.canonical.namespace.clone()),
                    category: prepared.canonical.category,
                    key: Some(prepared.canonical.key.clone()),
                    status: Some(status),
                    candidate_text: content.to_owned(),
                    confidence: f64::from(params.confidence.unwrap_or(0.5).clamp(0.0, 1.0)),
                    reason: reason.to_owned(),
                    source_context_kind: params.source_context_kind,
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
        crud_candidate_to_protocol(candidate)
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
        if self.row_control_plane_visible(&row, context, &[], now) {
            Ok(vec![row])
        } else {
            Ok(Vec::new())
        }
    }

    async fn hydrate_control_plane_row(
        &self,
        row: AgentMemoryControlRecord,
        context: &MemoryOperationContext,
        allowed_statuses: &[MemoryStatus],
        now: i64,
        record_access: bool,
    ) -> Result<Option<MemoryRecord>> {
        if !self.row_control_plane_visible(&row, context, allowed_statuses, now) {
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

    fn row_control_plane_visible(
        &self,
        row: &AgentMemoryControlRecord,
        context: &MemoryOperationContext,
        allowed_statuses: &[MemoryStatus],
        now: i64,
    ) -> bool {
        if !control_plane_status_visible(row, allowed_statuses, now) {
            return false;
        }
        if row.repair_status != REPAIR_STATUS_OK {
            return false;
        }
        if !self.policy.read_policy(context).allows(row.sensitivity) {
            return false;
        }
        workspace_visible(row, context)
    }

    async fn hydrate_visible_row(
        &self,
        row: AgentMemoryControlRecord,
        context: &MemoryOperationContext,
        allowed_statuses: &[MemoryStatus],
        now: i64,
        record_access: bool,
    ) -> Result<Option<MemoryRecord>> {
        Ok(self
            .hydrate_visible_row_with_quality(row, context, allowed_statuses, now, record_access)
            .await?
            .map(|(record, _quality)| record))
    }

    async fn hydrate_visible_row_with_quality(
        &self,
        row: AgentMemoryControlRecord,
        context: &MemoryOperationContext,
        allowed_statuses: &[MemoryStatus],
        now: i64,
        record_access: bool,
    ) -> Result<Option<(MemoryRecord, MemoryRecallQualitySignals)>> {
        self.hydrate_visible_row_with_quality_and_diagnostics(
            row,
            context,
            allowed_statuses,
            now,
            record_access,
            None,
        )
        .await
    }

    async fn hydrate_visible_row_with_quality_and_diagnostics(
        &self,
        row: AgentMemoryControlRecord,
        context: &MemoryOperationContext,
        allowed_statuses: &[MemoryStatus],
        now: i64,
        record_access: bool,
        diagnostics: Option<&mut MemoryRecallServiceDiagnostics>,
    ) -> Result<Option<(MemoryRecord, MemoryRecallQualitySignals)>> {
        let quality = self.recall_quality_signals_for_row(&row).await?;
        let visibility = self
            .row_recall_visibility_with_quality(&row, context, allowed_statuses, now, &quality)
            .await?;
        if let Some(diagnostics) = diagnostics {
            diagnostics.record_visibility(visibility);
        }
        if !visibility.is_visible() {
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

        Ok(Some((crud_record_to_protocol(row, payload)?, quality)))
    }

    async fn row_recall_visibility_with_quality(
        &self,
        row: &AgentMemoryControlRecord,
        context: &MemoryOperationContext,
        allowed_statuses: &[MemoryStatus],
        now: i64,
        quality: &MemoryRecallQualitySignals,
    ) -> Result<MemoryRecallVisibility> {
        let read_policy = self.policy.read_policy(context);
        let quarantined = self.row_is_quarantined(row.id.as_str()).await?;
        let input = memory_recall_visibility_input_for_row(
            row,
            allowed_statuses,
            now,
            row.repair_status == REPAIR_STATUS_OK,
            quarantined,
            &read_policy,
            workspace_visible(row, context),
            quality,
        );
        Ok(decide_memory_recall_visibility(&input))
    }

    async fn recall_quality_signals_for_row(
        &self,
        row: &AgentMemoryControlRecord,
    ) -> Result<MemoryRecallQualitySignals> {
        let quality_decisions = self
            .store
            .list_agent_memory_quality_decisions_for_memory(row.id.as_str(), 1)
            .await?;
        Ok(memory_recall_quality_signals_for_row(
            row,
            quality_decisions.first(),
        ))
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

    async fn row_is_quarantined(&self, memory_id: &str) -> Result<bool> {
        Ok(self
            .store
            .get_active_agent_memory_quarantine(memory_id)
            .await?
            .is_some())
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

    async fn enqueue_backend_reindex(
        &self,
        row: &AgentMemoryControlRecord,
        operation: &str,
        now: i64,
    ) -> Result<AgentMemoryRepairJobRecord> {
        let payload_json = serde_json::json!({
            "memory_id": row.id,
            "operation": operation,
            "capsule_ref": row.capsule_ref,
            "frame_uri": row.frame_uri,
        })
        .to_string();
        self.enqueue_repair_job(
            NewAgentMemoryRepairJob {
                job_kind: REPAIR_JOB_BACKEND_REINDEX.to_owned(),
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

    async fn process_stale_backend_repair(
        &self,
        job: &AgentMemoryRepairJobRecord,
    ) -> Result<String> {
        let memory_id = job
            .memory_id
            .as_deref()
            .context("stale backend repair job missing memory id")?;
        self.backend
            .delete(backend_delete_request_from_repair_job(job, memory_id))
            .await?;
        Ok(serde_json::json!({
            "kind": job.job_kind,
            "memory_id": memory_id,
            "action": "backend_delete_attempted"
        })
        .to_string())
    }

    async fn process_backend_delete_repair(
        &self,
        job: &AgentMemoryRepairJobRecord,
    ) -> Result<String> {
        let memory_id = job
            .memory_id
            .as_deref()
            .context("backend delete repair job missing memory id")?;
        let Some(row) = self.store.get_agent_memory_record(memory_id, true).await? else {
            self.backend
                .delete(backend_delete_request_from_repair_job(job, memory_id))
                .await?;
            return Ok(serde_json::json!({
                "kind": job.job_kind,
                "memory_id": memory_id,
                "action": "backend_delete_attempted_without_control_row"
            })
            .to_string());
        };
        self.backend.delete(backend_delete_request(&row)).await?;
        Ok(serde_json::json!({
            "kind": job.job_kind,
            "memory_id": memory_id,
            "action": "backend_delete_attempted"
        })
        .to_string())
    }

    async fn process_backend_reindex_repair(
        &self,
        job: &AgentMemoryRepairJobRecord,
        now: i64,
    ) -> Result<String> {
        let memory_id = job
            .memory_id
            .as_deref()
            .context("backend reindex repair job missing memory id")?;
        let Some(row) = self.store.get_agent_memory_record(memory_id, true).await? else {
            return Ok(serde_json::json!({
                "kind": job.job_kind,
                "memory_id": memory_id,
                "action": "skipped_missing_control_row"
            })
            .to_string());
        };
        if row.status != MemoryStatus::Active {
            return Ok(serde_json::json!({
                "kind": job.job_kind,
                "memory_id": memory_id,
                "action": "skipped_non_active_control_row"
            })
            .to_string());
        }
        if self.row_is_quarantined(memory_id).await? {
            return Ok(serde_json::json!({
                "kind": job.job_kind,
                "memory_id": memory_id,
                "action": "skipped_quarantined"
            })
            .to_string());
        }

        self.backend.put(backend_put_request_from_row(&row)).await?;
        if row.repair_status != REPAIR_STATUS_OK {
            self.store
                .mark_agent_memory_repair_status(row.id.as_str(), REPAIR_STATUS_OK, now)
                .await?;
        }
        Ok(serde_json::json!({
            "kind": job.job_kind,
            "memory_id": memory_id,
            "action": "backend_reindex_attempted"
        })
        .to_string())
    }

    fn semantic_quality_decision(
        &self,
        params: &MemorySemanticWriteParams,
        prepared: &SemanticWritePrepared,
        relation: MemoryWriteRelation,
        sensitivity: MemorySensitivity,
    ) -> (MemoryQualityGateInput, MemoryQualityDecision) {
        let source_context = resolve_semantic_write_source_context(params);
        let ontology = classify_semantic_memory_fact(&params.semantic, Some(&params.scope));
        let input = memory_quality_gate_input_from_semantic_write(
            params,
            relation,
            &source_context,
            ontology,
            sensitivity,
            Some(prepared.canonical.key.clone()),
            params.semantic.intent == MemoryIntent::ExplicitNoMemory,
        );
        let decision = MemoryQualityGate::decide(&input);
        (input, decision)
    }

    async fn record_quality_decision(
        &self,
        input: &MemoryQualityGateInput,
        decision: &MemoryQualityDecision,
        context: &MemoryOperationContext,
        memory_id: Option<String>,
        candidate_id: Option<String>,
        now: i64,
    ) -> Result<AgentMemoryQualityDecisionRecord> {
        self.store
            .insert_agent_memory_quality_decision(NewAgentMemoryQualityDecision {
                workspace_id: context
                    .workspace_id
                    .clone()
                    .or_else(|| input.workspace_id.clone()),
                thread_id: input
                    .source_thread_id
                    .clone()
                    .or_else(|| context.thread_id.clone()),
                turn_id: input.source_turn_id.clone(),
                item_id: input.source_item_id.clone(),
                task_id: input.task_id.clone(),
                memory_id,
                candidate_id,
                canonical_key: input.canonical_key.clone(),
                action: decision.action,
                target_ownership: decision.target_ownership,
                source_context_kind: input.source_context_kind,
                fact_class: input.fact_class,
                lifetime_class: input.lifetime_class,
                ownership_class: input.ownership_class,
                evidence_class: input.evidence_class,
                relation: input.relation,
                reason_codes: decision.reason_codes.clone(),
                input_snapshot_json: Some(
                    serde_json::to_string(input)
                        .context("failed to encode memory quality input snapshot")?,
                ),
                created_at_unix: now,
                updated_at_unix: now,
            })
            .await
    }

    async fn record_quality_decision_for_response(
        &self,
        input: &MemoryQualityGateInput,
        decision: &MemoryQualityDecision,
        context: &MemoryOperationContext,
        response: &MemorySemanticWriteResponse,
        now: i64,
    ) -> Result<AgentMemoryQualityDecisionRecord> {
        self.record_quality_decision(
            input,
            decision,
            context,
            response.record.as_ref().map(|record| record.id.clone()),
            response
                .candidate
                .as_ref()
                .map(|candidate| candidate.id.clone()),
            now,
        )
        .await
    }

    fn quality_suppressed_response(
        relation: MemoryWriteRelation,
        prepared: SemanticWritePrepared,
    ) -> MemorySemanticWriteResponse {
        MemorySemanticWriteResponse {
            relation,
            canonical_key: prepared.canonical,
            semantic_fingerprint: prepared.semantic_fingerprint,
            record: None,
            candidate: None,
            created: false,
            superseded_memory_id: None,
            evidence_merged: false,
            route: None,
        }
    }

    fn quality_ownership_route(
        quality_input: &MemoryQualityGateInput,
        quality_decision: &MemoryQualityDecision,
    ) -> MemoryOwnershipRoute {
        resolve_memory_ownership_route(MemoryOwnershipRouteInput::from_quality(
            quality_input,
            quality_decision,
        ))
    }

    fn with_quality_route(
        mut response: MemorySemanticWriteResponse,
        quality_decision: &MemoryQualityDecision,
        ownership_route: &MemoryOwnershipRoute,
        quality_decision_id: Option<String>,
    ) -> MemorySemanticWriteResponse {
        response.route = Some(MemorySemanticWriteRouteInfo {
            route: ownership_route.semantic_write_route(),
            quality_action: quality_decision.action,
            target_ownership: quality_decision.target_ownership,
            quality_decision_id,
            thread_id: ownership_route.thread_id.clone(),
            source_turn_id: ownership_route.source_turn_id.clone(),
            source_item_id: ownership_route.source_item_id.clone(),
            canonical_key: ownership_route.canonical_key.clone(),
        });
        response
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

    async fn record_candidate_policy_decision(
        &self,
        output: &MemoryCandidatePolicyOutput,
        context: &MemoryOperationContext,
        memory_id: Option<String>,
        candidate_id: Option<String>,
        workspace_id: Option<String>,
        now: i64,
    ) -> Result<()> {
        self.store
            .insert_agent_memory_policy_decision(NewAgentMemoryPolicyDecision {
                memory_id,
                candidate_id,
                workspace_id,
                action: "candidate_policy".to_owned(),
                decision: candidate_policy_decision_label(output.decision).to_owned(),
                reason_code: Some(output.reason_code.clone()),
                reason: output.reason.clone(),
                policy_version: self.config.policy_version.clone(),
                actor: protocol_actor_to_crud(context.actor.clone()),
                thread_id: context.thread_id.clone(),
                turn_id: None,
                item_id: None,
                details_json: Some(serde_json::to_string(output)?),
                created_at_unix: now,
            })
            .await?;
        Ok(())
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

fn candidate_policy_decision_label(decision: MemoryCandidatePolicyDecision) -> &'static str {
    match decision {
        MemoryCandidatePolicyDecision::AutoApprove => "auto_approve",
        MemoryCandidatePolicyDecision::PendingSilent => "pending_silent",
        MemoryCandidatePolicyDecision::AskOnUse => "ask_on_use",
        MemoryCandidatePolicyDecision::NeedsReview => "needs_review",
        MemoryCandidatePolicyDecision::AutoReject => "auto_reject",
        MemoryCandidatePolicyDecision::RejectReviewDisabled => "reject_review_disabled",
    }
}

fn candidate_status_label(status: MemoryCandidateStatus) -> &'static str {
    match status {
        MemoryCandidateStatus::Pending => "pending",
        MemoryCandidateStatus::PendingSilent => "pending_silent",
        MemoryCandidateStatus::AskOnUse => "ask_on_use",
        MemoryCandidateStatus::NeedsReview => "needs_review",
        MemoryCandidateStatus::Approved => "approved",
        MemoryCandidateStatus::Rejected => "rejected",
        MemoryCandidateStatus::AutoRejected => "auto_rejected",
        MemoryCandidateStatus::ReviewDisabledRejected => "review_disabled_rejected",
        MemoryCandidateStatus::Superseded => "superseded",
        MemoryCandidateStatus::MergedDuplicate => "merged_duplicate",
        MemoryCandidateStatus::Expired => "expired",
    }
}

fn ensure_candidate_pending_for_transition(
    candidate: &AgentMemoryCandidateRecord,
    operation: &str,
) -> Result<()> {
    if candidate_is_pending(candidate.status) {
        return Ok(());
    }
    bail!(
        "memory candidate `{}` cannot be {operation} because it is `{}`",
        candidate.id,
        candidate_status_label(candidate.status)
    )
}

fn candidate_is_pending(status: MemoryCandidateStatus) -> bool {
    matches!(
        status,
        MemoryCandidateStatus::Pending
            | MemoryCandidateStatus::PendingSilent
            | MemoryCandidateStatus::AskOnUse
            | MemoryCandidateStatus::NeedsReview
    )
}

fn candidate_is_rejected(status: MemoryCandidateStatus) -> bool {
    matches!(
        status,
        MemoryCandidateStatus::Rejected
            | MemoryCandidateStatus::AutoRejected
            | MemoryCandidateStatus::ReviewDisabledRejected
    )
}

fn metadata_json_with_candidate_policy(
    metadata_json: String,
    output: &MemoryCandidatePolicyOutput,
) -> Result<String> {
    let mut metadata =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(metadata_json.as_str())
            .with_context(|| format!("invalid semantic metadata JSON `{metadata_json}`"))?;
    metadata.insert(
        "candidate_policy".to_owned(),
        serde_json::to_value(output).context("failed to encode memory candidate policy output")?,
    );
    metadata.insert(
        "candidate_score".to_owned(),
        serde_json::to_value(&output.score).context("failed to encode memory candidate score")?,
    );
    metadata.insert(
        "candidate_score_bucket".to_owned(),
        serde_json::json!(score_bucket_label(output.score.bucket)),
    );
    metadata.insert(
        "candidate_policy_decision".to_owned(),
        serde_json::json!(candidate_policy_decision_label(output.decision)),
    );
    metadata.insert(
        "candidate_policy_reason_code".to_owned(),
        serde_json::json!(output.reason_code.as_str()),
    );
    Ok(serde_json::Value::Object(metadata).to_string())
}

fn candidate_semantic_write_params(
    candidate: &AgentMemoryCandidateRecord,
    content_override: Option<String>,
    value_override: Option<String>,
    lifecycle_source: Option<&str>,
) -> Result<MemorySemanticWriteParams> {
    let mut metadata = candidate_metadata_map(candidate.metadata_json.as_deref())?;
    let semantic_value = metadata.get("semantic").with_context(|| {
        format!(
            "memory candidate `{}` has no semantic metadata",
            candidate.id
        )
    })?;
    let mut semantic = semantic_value
        .get("fields")
        .cloned()
        .map(serde_json::from_value::<MemorySemanticFields>)
        .transpose()
        .context("failed to decode candidate semantic fields")?
        .with_context(|| {
            format!(
                "memory candidate `{}` has no typed semantic fields",
                candidate.id
            )
        })?;
    semantic.intent = MemoryIntent::ExplicitStore;
    semantic.explicitness = MemoryExplicitness::Explicit;

    let content = content_override.unwrap_or_else(|| candidate.candidate_text.clone());
    let value = value_override.or_else(|| {
        semantic_value
            .get("normalized_value")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    });
    let evidence = MemoryWriteEvidence {
        source_thread_id: candidate.source_thread_id.clone(),
        source_turn_id: candidate.source_turn_id.clone(),
        source_item_id: candidate.source_item_id.clone(),
        source_ref: Some(format!("memory_candidate:{}", candidate.id)),
        quote_or_span: Some(candidate.candidate_text.clone()),
        extractor_reason: Some("candidate approved through memory service lifecycle".to_owned()),
    };
    let provenance = MemoryProvenance {
        source_thread_id: candidate.source_thread_id.clone(),
        source_turn_id: candidate.source_turn_id.clone(),
        source_item_id: candidate.source_item_id.clone(),
        created_by: candidate
            .created_by
            .as_ref()
            .map(|actor| pioneer_protocol::MemoryActor {
                kind: actor.kind,
                id: actor.id.clone(),
            }),
    };
    if let Some(lifecycle_source) = lifecycle_source {
        metadata.insert(
            "candidate_lifecycle_source".to_owned(),
            serde_json::json!(lifecycle_source),
        );
        metadata.insert(
            "approved_candidate_id".to_owned(),
            serde_json::json!(candidate.id),
        );
    }

    Ok(MemorySemanticWriteParams {
        scope: candidate.scope.clone(),
        semantic,
        content,
        value,
        evidence: Some(evidence),
        provenance: Some(provenance),
        source_context_kind: Some(MemorySourceContextKind::DirectUserConversation),
        disposition: Some(MemorySemanticWriteDisposition::AcceptActive),
        client_provided_key: None,
        confidence: Some(candidate.confidence.clamp(0.0, 1.0) as f32),
        importance: None,
        metadata,
    })
}

fn candidate_metadata_with_lifecycle(
    metadata_json: Option<&str>,
    lifecycle: serde_json::Value,
) -> Result<String> {
    let mut metadata = candidate_metadata_map(metadata_json)?;
    metadata.insert("candidate_lifecycle".to_owned(), lifecycle);
    Ok(serde_json::to_string(&metadata)?)
}

fn candidate_metadata_map(
    metadata_json: Option<&str>,
) -> Result<std::collections::BTreeMap<String, serde_json::Value>> {
    match metadata_json {
        Some(metadata_json) if !metadata_json.trim().is_empty() => {
            serde_json::from_str(metadata_json).with_context(|| {
                format!("invalid memory candidate metadata JSON `{metadata_json}`")
            })
        }
        _ => Ok(std::collections::BTreeMap::new()),
    }
}

fn score_bucket_label(bucket: pioneer_protocol::MemoryCandidateScoreBucket) -> &'static str {
    match bucket {
        pioneer_protocol::MemoryCandidateScoreBucket::High => "high",
        pioneer_protocol::MemoryCandidateScoreBucket::Middle => "middle",
        pioneer_protocol::MemoryCandidateScoreBucket::ExtremelyLow => "extremely_low",
    }
}

fn metadata_evidence_count(metadata_json: &str) -> u64 {
    serde_json::from_str::<serde_json::Value>(metadata_json)
        .ok()
        .and_then(|metadata| {
            metadata
                .get("evidence")
                .and_then(|evidence| evidence.get("count"))
                .and_then(serde_json::Value::as_u64)
        })
        .unwrap_or(0)
}

fn scope_clarity_from_hint(scope_hint: MemoryScopeHint) -> MemoryScopeClarity {
    match scope_hint {
        MemoryScopeHint::Unknown => MemoryScopeClarity::Unclear,
        MemoryScopeHint::UserGlobal
        | MemoryScopeHint::UserWorkspace
        | MemoryScopeHint::AgentGlobal
        | MemoryScopeHint::AgentWorkspace
        | MemoryScopeHint::ProjectWorkspace => MemoryScopeClarity::Clear,
    }
}

fn pending_candidate_statuses() -> Vec<MemoryCandidateStatus> {
    vec![
        MemoryCandidateStatus::Pending,
        MemoryCandidateStatus::PendingSilent,
        MemoryCandidateStatus::AskOnUse,
        MemoryCandidateStatus::NeedsReview,
    ]
}

fn rejected_candidate_statuses() -> Vec<MemoryCandidateStatus> {
    vec![
        MemoryCandidateStatus::Rejected,
        MemoryCandidateStatus::AutoRejected,
        MemoryCandidateStatus::ReviewDisabledRejected,
        MemoryCandidateStatus::Expired,
    ]
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

fn memory_recall_mode_scopes(
    context: &MemoryOperationContext,
    mode: MemoryRecallMode,
) -> Vec<MemoryScope> {
    let active_scopes = context.effective_scopes(&[]);
    let allowed_kinds: &[MemoryScopeKind] = match mode {
        MemoryRecallMode::Profile => &[MemoryScopeKind::User],
        MemoryRecallMode::Project => &[MemoryScopeKind::Workspace],
        MemoryRecallMode::Durable => &[
            MemoryScopeKind::User,
            MemoryScopeKind::Workspace,
            MemoryScopeKind::Agent,
        ],
        MemoryRecallMode::ThreadEpisodic => &[MemoryScopeKind::Thread],
        MemoryRecallMode::TaskContext => &[MemoryScopeKind::Task],
        MemoryRecallMode::ExactCanonical => &[],
    };
    active_scopes
        .into_iter()
        .filter(|scope| allowed_kinds.contains(&scope.kind))
        .collect()
}

fn exact_target_scopes(
    context: &MemoryOperationContext,
    target: &MemoryRecallTarget,
) -> Vec<MemoryScope> {
    let active_scopes = context.effective_scopes(&[]);
    match target.scope_kind {
        Some(scope_kind) => active_scopes
            .into_iter()
            .filter(|scope| scope.kind == scope_kind)
            .collect(),
        None => active_scopes,
    }
}

fn exact_target_has_lookup_key(
    context: &MemoryOperationContext,
    target: &MemoryRecallTarget,
) -> bool {
    target
        .canonical_key
        .as_ref()
        .is_some_and(|key| !key.trim().is_empty())
        || !exact_target_lookup_keys(context, target).is_empty()
}

fn exact_target_lookup_keys(
    context: &MemoryOperationContext,
    target: &MemoryRecallTarget,
) -> Vec<String> {
    if let Some(key) = target
        .canonical_key
        .as_ref()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty())
    {
        return vec![key.to_owned()];
    }
    let Some(category) = target.category else {
        return Vec::new();
    };
    let Some(subject) = target.subject else {
        return Vec::new();
    };
    let Some(attribute) = target.attribute else {
        return Vec::new();
    };

    exact_target_scopes(context, target)
        .into_iter()
        .filter_map(|scope| {
            let semantic = MemorySemanticFields {
                intent: MemoryIntent::ImplicitCandidate,
                explicitness: MemoryExplicitness::Unclear,
                category,
                subject,
                attribute,
                subject_key: None,
                custom_subject: None,
                custom_attribute: None,
                scope_hint: canonical_scope_hint_for_scope(&scope),
                durability: MemoryDurability::Unknown,
                sensitivity: MemorySensitivityHint::Unknown,
                certainty: MemoryExtractorCertainty::Medium,
            };
            build_memory_canonical_key(&scope, &semantic)
                .ok()
                .map(|canonical| canonical.key)
        })
        .collect()
}

fn canonical_scope_hint_for_scope(scope: &MemoryScope) -> MemoryScopeHint {
    match scope.kind {
        MemoryScopeKind::User => MemoryScopeHint::UserGlobal,
        MemoryScopeKind::Workspace => MemoryScopeHint::ProjectWorkspace,
        MemoryScopeKind::Agent => {
            if scope.key.starts_with("global:agent:") {
                MemoryScopeHint::AgentGlobal
            } else {
                MemoryScopeHint::AgentWorkspace
            }
        }
        MemoryScopeKind::Thread | MemoryScopeKind::Task => MemoryScopeHint::Unknown,
    }
}

fn memory_recall_mode_categories(mode: MemoryRecallMode) -> Vec<MemoryCategory> {
    match mode {
        MemoryRecallMode::Profile => vec![
            MemoryCategory::Identity,
            MemoryCategory::Preference,
            MemoryCategory::Biography,
            MemoryCategory::Relationship,
            MemoryCategory::CommunicationStyle,
            MemoryCategory::RecurringInstruction,
        ],
        MemoryRecallMode::Project => vec![
            MemoryCategory::ProjectDecision,
            MemoryCategory::ProjectPolicy,
            MemoryCategory::ProjectFact,
            MemoryCategory::Procedure,
            MemoryCategory::Constraint,
        ],
        MemoryRecallMode::Durable
        | MemoryRecallMode::ThreadEpisodic
        | MemoryRecallMode::TaskContext
        | MemoryRecallMode::ExactCanonical => Vec::new(),
    }
}

fn recency_anchor_unix(row: &AgentMemoryControlRecord) -> i64 {
    row.last_accessed_at_unix
        .unwrap_or(row.updated_at_unix)
        .max(row.updated_at_unix)
        .max(row.created_at_unix)
}

fn control_plane_status_visible(
    row: &AgentMemoryControlRecord,
    allowed_statuses: &[MemoryStatus],
    now: i64,
) -> bool {
    if allowed_statuses.is_empty() {
        if row.status != MemoryStatus::Active {
            return false;
        }
    } else if !allowed_statuses.contains(&row.status) {
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

    true
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

fn backend_put_request_from_row(row: &AgentMemoryControlRecord) -> BackendPutRequest {
    BackendPutRequest {
        memory_id: row.id.clone(),
        scope: row.scope.clone(),
        namespace: Some(row.namespace.clone()),
        category: row.category,
        key: row.key.clone(),
        content: row.content_preview.clone().unwrap_or_default(),
        sensitivity: row.sensitivity,
        metadata_json: row.metadata_json.clone(),
        source_thread_id: row.source_thread_id.clone(),
        source_turn_id: row.source_turn_id.clone(),
        source_item_id: row.source_item_id.clone(),
        created_by_kind: row.created_by.as_ref().map(|actor| actor.kind),
        created_by_id: row.created_by.as_ref().and_then(|actor| actor.id.clone()),
        policy_version: row.policy_version.clone().unwrap_or_default(),
        status: row.status,
        idempotency_key: None,
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

fn backend_delete_request_from_repair_job(
    job: &AgentMemoryRepairJobRecord,
    memory_id: &str,
) -> BackendDeleteRequest {
    BackendDeleteRequest {
        memory_id: memory_id.to_owned(),
        scope: MemoryScope {
            kind: job.scope_kind.unwrap_or(MemoryScopeKind::User),
            key: "repair".to_owned(),
        },
        scope_key_hash: job.scope_key_hash.clone(),
        capsule_id: job.capsule_id.clone(),
        capsule_ref: None,
        frame_id: None,
        frame_uri: None,
    }
}

fn bounded_error_message(message: &str) -> String {
    message.chars().take(512).collect()
}

fn lifecycle_actor_to_crud(
    actor: Option<MemoryLifecycleActor>,
    default_kind: MemoryLifecycleActorKind,
) -> MemoryLifecycleActorRecord {
    match actor {
        Some(actor) => MemoryLifecycleActorRecord {
            kind: actor.kind,
            id: actor.id,
        },
        None => MemoryLifecycleActorRecord {
            kind: default_kind,
            id: None,
        },
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

fn candidate_workspace_visible(
    candidate: &AgentMemoryCandidateRecord,
    context: &MemoryOperationContext,
) -> bool {
    match candidate.workspace_id.as_deref() {
        Some(candidate_workspace_id) => context
            .workspace_id
            .as_deref()
            .is_some_and(|context_workspace_id| context_workspace_id == candidate_workspace_id),
        None => match candidate.scope.kind {
            MemoryScopeKind::User => context.workspace_id.is_none() || context.allow_global_user,
            MemoryScopeKind::Agent => context.allow_global_agent,
            _ => false,
        },
    }
}

fn hook_run_visible(run: &pioneer_crud::HookRunRecord, context: &MemoryOperationContext) -> bool {
    match run
        .context
        .workspace_id
        .as_ref()
        .map(|workspace_id| workspace_id.as_str())
    {
        Some(run_workspace_id) => context
            .workspace_id
            .as_deref()
            .is_some_and(|context_workspace_id| context_workspace_id == run_workspace_id),
        None => context.workspace_id.is_none(),
    }
}

fn quality_decision_visible(
    decision: &AgentMemoryQualityDecisionRecord,
    context: &MemoryOperationContext,
) -> bool {
    match decision.workspace_id.as_deref() {
        Some(decision_workspace_id) => context
            .workspace_id
            .as_deref()
            .is_some_and(|context_workspace_id| context_workspace_id == decision_workspace_id),
        None => context.workspace_id.is_none() || context.allow_global_user,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryMemoryBackend;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::AgentMemoryListFilter;
    use pioneer_protocol::{
        MemoryAttribute, MemoryDurability, MemoryExtractorCertainty, MemorySubject,
    };
    use sea_orm::Database;

    async fn test_service() -> (MemoryService, Arc<CrudStore>) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory connects");
        Migrator::up(&connection, None)
            .await
            .expect("migrations apply");
        let store = Arc::new(CrudStore::new(connection));
        let service = MemoryService::new(
            store.clone(),
            Arc::new(InMemoryMemoryBackend::default()),
            MemoryServiceConfig::default(),
        );
        (service, store)
    }

    fn duplicate_safe_context() -> MemoryOperationContext {
        MemoryOperationContext {
            allow_global_user: true,
            now_unix: Some(1_771_000_000),
            ..MemoryOperationContext::default()
        }
    }

    fn user_name_semantic_params() -> MemorySemanticWriteParams {
        MemorySemanticWriteParams {
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".to_owned(),
            },
            semantic: MemorySemanticFields {
                intent: MemoryIntent::ExplicitStore,
                explicitness: MemoryExplicitness::Explicit,
                category: MemoryCategory::Identity,
                subject: MemorySubject::CurrentUser,
                attribute: MemoryAttribute::Name,
                subject_key: None,
                custom_subject: None,
                custom_attribute: None,
                scope_hint: MemoryScopeHint::UserGlobal,
                durability: MemoryDurability::LongLived,
                sensitivity: MemorySensitivityHint::Personal,
                certainty: MemoryExtractorCertainty::High,
            },
            content: "Имя пользователя: Александр".to_owned(),
            value: Some("Александр".to_owned()),
            evidence: Some(MemoryWriteEvidence {
                source_thread_id: Some("thread_phase21".to_owned()),
                source_turn_id: Some("turn_phase21".to_owned()),
                source_item_id: Some("user_turn_phase21".to_owned()),
                source_ref: Some("hook:memory.post_turn_extractor".to_owned()),
                quote_or_span: Some("Меня зовут Александр".to_owned()),
                extractor_reason: Some("explicit self-identification".to_owned()),
            }),
            provenance: None,
            source_context_kind: Some(MemorySourceContextKind::DirectUserConversation),
            disposition: Some(MemorySemanticWriteDisposition::AcceptActive),
            client_provided_key: None,
            confidence: Some(0.99),
            importance: Some(0.7),
            metadata: Default::default(),
        }
    }

    #[tokio::test]
    async fn duplicate_semantic_retry_merges_active_memory_instead_of_creating_duplicate() {
        let (service, store) = test_service().await;
        let context = duplicate_safe_context();
        let params = user_name_semantic_params();

        let first = service
            .write_semantic_memory(context.clone(), params.clone())
            .await
            .expect("first semantic write succeeds");
        let second = service
            .write_semantic_memory(context.clone(), params.clone())
            .await
            .expect("retry semantic write succeeds");

        assert_eq!(first.relation, MemoryWriteRelation::Novel);
        assert_eq!(second.relation, MemoryWriteRelation::Duplicate);
        assert!(second.evidence_merged);
        assert_eq!(
            first.record.as_ref().map(|record| record.id.as_str()),
            second.record.as_ref().map(|record| record.id.as_str())
        );

        let records = store
            .list_agent_memory_records(AgentMemoryListFilter {
                scopes: vec![params.scope],
                statuses: vec![MemoryStatus::Active],
                ..AgentMemoryListFilter::default()
            })
            .await
            .expect("memory records list");
        assert_eq!(records.len(), 1);
        assert_eq!(
            metadata_evidence_count(records[0].metadata_json.as_deref().unwrap_or("{}")),
            2
        );
    }
}
