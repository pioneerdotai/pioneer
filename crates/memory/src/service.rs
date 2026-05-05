use crate::NoopMemoryBackend;
use crate::backend::{
    BackendDeleteRequest, BackendGetRequest, BackendPutRequest, BackendSearchRequest,
    BackendSearchScope, MemoryBackend,
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
use anyhow::{Context, Result, bail};
use pioneer_crud::{
    AgentMemoryCandidateDecisionRecord, AgentMemoryCandidateListFilter, AgentMemoryControlRecord,
    AgentMemoryListFilter, AgentMemoryRepairJobRecord, CrudStore, NewAgentMemoryControlRecord,
    NewAgentMemoryPolicyDecision, NewAgentMemoryRepairJob,
};
use pioneer_protocol::{
    MemoryCandidateStatus, MemoryCandidatesDecideParams, MemoryCandidatesDecideResponse,
    MemoryCandidatesListParams, MemoryCandidatesListResponse, MemoryForgetParams,
    MemoryForgetResponse, MemoryForgetTarget, MemoryGetParams, MemoryGetResponse, MemoryListParams,
    MemoryListResponse, MemoryRecord, MemoryRememberParams, MemoryRememberResponse, MemoryScope,
    MemoryScopeKind, MemorySearchHit, MemorySearchParams, MemorySearchResponse, MemoryStatus,
    generate_id,
};
use std::sync::Arc;

const ID_LEN: usize = 21;
const REPAIR_STATUS_OK: &str = "ok";
const REPAIR_STATUS_REPAIR_NEEDED: &str = "repair_needed";
const REPAIR_JOB_BACKEND_PAYLOAD_MISSING: &str = "backend_payload_missing";
const REPAIR_JOB_BACKEND_DELETE_FAILED: &str = "backend_delete_failed";
const REPAIR_JOB_BACKEND_STALE_PAYLOAD: &str = "backend_stale_payload";
const REPAIR_PRIORITY_DEFAULT: i64 = 10;
const REPAIR_MAX_ATTEMPTS_DEFAULT: i32 = 3;

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

        let row = if params.supersedes.is_some() || params.key.is_none() || existing.is_none() {
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
        if query.is_empty() {
            bail!("memory search query cannot be empty");
        }

        let now = context.now_or(current_unix());
        let scopes = context.effective_scopes(&params.scopes);
        let resolved_scopes = self.resolve_backend_search_scopes(&scopes).await?;
        let limit = self.normalized_limit(params.limit);
        let backend_hits = self
            .backend
            .search(BackendSearchRequest {
                query: query.to_owned(),
                scopes: scopes.clone(),
                resolved_scopes,
                limit: self.config.max_limit,
            })
            .await
            .context("failed to search memory backend")?;

        let mut hits = Vec::new();
        for hit in backend_hits {
            if hits.len() >= limit as usize {
                break;
            }
            let Some(row) = self
                .store
                .get_agent_memory_record(hit.memory_id.as_str(), true)
                .await?
            else {
                self.enqueue_stale_backend_repair(hit.memory_id.as_str(), now)
                    .await?;
                continue;
            };
            if !scope_matches(&row.scope, &scopes)
                || !category_matches(row.category, &params.categories)
            {
                continue;
            }
            let Some(record) = self
                .hydrate_visible_row(row, &context, &params.statuses, now, true)
                .await?
            else {
                continue;
            };
            hits.push(MemorySearchHit {
                record,
                score: hit.score,
                snippet: hit.snippet,
                matched_terms: hit.matched_terms,
            });
        }

        Ok(MemorySearchResponse {
            hits,
            next_cursor: None,
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

        let Some(payload) = self.backend.get(backend_get_request(&row)).await? else {
            self.mark_missing_backend_payload(&row, now).await?;
            return Ok(None);
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

    fn normalized_limit(&self, limit: Option<u32>) -> u32 {
        limit
            .unwrap_or(self.config.default_limit)
            .min(self.config.max_limit)
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
