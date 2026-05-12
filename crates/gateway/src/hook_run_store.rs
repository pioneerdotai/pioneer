use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use pioneer_crud::{
    CrudStore, HookAuditEventRecord as CrudHookAuditEventRecord,
    HookRunAttemptCompletionRecord as CrudHookRunAttemptCompletionRecord,
    HookRunAttemptRecord as CrudHookRunAttemptRecord,
    HookRunCompletionRecord as CrudHookRunCompletionRecord, HookRunRecord as CrudHookRunRecord,
    HookRunScope as CrudHookRunScope, HookRunScopeKind as CrudHookRunScopeKind,
    NewHookAuditEventRecord as CrudNewHookAuditEventRecord,
    NewHookRunAttemptRecord as CrudNewHookRunAttemptRecord,
    NewHookRunRecord as CrudNewHookRunRecord,
};
use pioneer_hooks::{
    HookAuditEventStoreRecord, HookRecoverableRunRecord, HookRecoveryScan, HookRetrySchedule,
    HookRunAttemptId, HookRunAttemptStoreCompletion, HookRunAttemptStoreRecord, HookRunId,
    HookRunScope, HookRunScopeKind, HookRunStore, HookRunStoreCompletion, HookRunStoreError,
    HookRunStoreRecord, HookRunStoreResult, NewHookAuditEventStoreRecord,
    NewHookRunAttemptStoreRecord, NewHookRunStoreRecord,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct CrudHookRunStore {
    crud_store: Arc<CrudStore>,
}

impl CrudHookRunStore {
    pub(crate) fn new(crud_store: Arc<CrudStore>) -> Self {
        Self { crud_store }
    }
}

#[async_trait]
impl HookRunStore for CrudHookRunStore {
    async fn create_or_load_run(
        &self,
        run: NewHookRunStoreRecord,
    ) -> HookRunStoreResult<HookRunStoreRecord> {
        if let Some(existing) = self
            .crud_store
            .find_hook_run_by_idempotency_key(&run.idempotency_key)
            .await
            .map_err(|_| HookRunStoreError::internal("failed to read hook run"))?
        {
            return crud_run_to_store_record(existing);
        }

        let now = unix_ms_to_datetime(run.queued_at_unix_ms.unwrap_or_else(current_unix_ms));
        let create = CrudNewHookRunRecord {
            id: None,
            idempotency_key: run.idempotency_key.clone(),
            subscription_id: run.subscription_id,
            hook_id: run.hook_id,
            phase: run.phase,
            status: run.status,
            scope: run.scope.map(crud_scope_from_store_scope),
            context: run.context,
            contribution_hashes: run.contribution_hashes,
            diagnostic_previews: run.diagnostic_previews,
            error: run.error,
            queued_at: run.queued_at_unix_ms.map(unix_ms_to_datetime),
            started_at: run.started_at_unix_ms.map(unix_ms_to_datetime),
            completed_at: run.completed_at_unix_ms.map(unix_ms_to_datetime),
            deadline_at: run.deadline_at_unix_ms.map(unix_ms_to_datetime),
            resume_state: run.resume_state,
        };

        match self.crud_store.create_hook_run(create, now).await {
            Ok(created) => crud_run_to_store_record(created),
            Err(_) => {
                if let Some(existing) = self
                    .crud_store
                    .find_hook_run_by_idempotency_key(&run.idempotency_key)
                    .await
                    .map_err(|_| HookRunStoreError::internal("failed to read hook run"))?
                {
                    return crud_run_to_store_record(existing);
                }
                Err(HookRunStoreError::internal("failed to create hook run"))
            }
        }
    }

    async fn mark_run_running(
        &self,
        run_id: &HookRunId,
        started_at_unix_ms: i64,
    ) -> HookRunStoreResult<HookRunStoreRecord> {
        let Some(record) = self
            .crud_store
            .mark_hook_run_running(run_id, unix_ms_to_datetime(started_at_unix_ms))
            .await
            .map_err(|_| HookRunStoreError::internal("failed to mark hook run running"))?
        else {
            return Err(HookRunStoreError::invalid_record("hook run not found"));
        };
        crud_run_to_store_record(record)
    }

    async fn complete_run(
        &self,
        run_id: &HookRunId,
        completion: HookRunStoreCompletion,
    ) -> HookRunStoreResult<HookRunStoreRecord> {
        let completed_at = unix_ms_to_datetime(completion.completed_at_unix_ms);
        let Some(record) = self
            .crud_store
            .complete_hook_run(
                run_id,
                CrudHookRunCompletionRecord {
                    status: completion.status,
                    contribution_hashes: completion.contribution_hashes,
                    diagnostic_previews: completion.diagnostic_previews,
                    error: completion.error,
                    completed_at: Some(completed_at),
                },
                completed_at,
            )
            .await
            .map_err(|_| HookRunStoreError::internal("failed to complete hook run"))?
        else {
            return Err(HookRunStoreError::invalid_record("hook run not found"));
        };
        crud_run_to_store_record(record)
    }

    async fn append_attempt(
        &self,
        attempt: NewHookRunAttemptStoreRecord,
    ) -> HookRunStoreResult<HookRunAttemptStoreRecord> {
        let now = unix_ms_to_datetime(attempt.started_at_unix_ms.unwrap_or_else(current_unix_ms));
        let record = self
            .crud_store
            .append_hook_run_attempt(
                CrudNewHookRunAttemptRecord {
                    id: None,
                    hook_run_id: attempt.hook_run_id,
                    attempt_number: attempt.attempt_number,
                    status: attempt.status,
                    contribution_hashes: attempt.contribution_hashes,
                    diagnostic_previews: attempt.diagnostic_previews,
                    error: attempt.error,
                    started_at: attempt.started_at_unix_ms.map(unix_ms_to_datetime),
                    completed_at: attempt.completed_at_unix_ms.map(unix_ms_to_datetime),
                    duration_ms: attempt.duration_ms,
                },
                now,
            )
            .await
            .map_err(|_| HookRunStoreError::conflict("failed to append hook run attempt"))?;
        crud_attempt_to_store_record(record)
    }

    async fn complete_attempt(
        &self,
        attempt_id: &HookRunAttemptId,
        completion: HookRunAttemptStoreCompletion,
    ) -> HookRunStoreResult<HookRunAttemptStoreRecord> {
        let completed_at = unix_ms_to_datetime(completion.completed_at_unix_ms);
        let Some(record) = self
            .crud_store
            .complete_hook_run_attempt(
                attempt_id,
                CrudHookRunAttemptCompletionRecord {
                    status: completion.status,
                    contribution_hashes: completion.contribution_hashes,
                    diagnostic_previews: completion.diagnostic_previews,
                    error: completion.error,
                    completed_at: Some(completed_at),
                    duration_ms: completion.duration_ms,
                },
                completed_at,
            )
            .await
            .map_err(|_| HookRunStoreError::internal("failed to complete hook run attempt"))?
        else {
            return Err(HookRunStoreError::invalid_record(
                "hook run attempt not found",
            ));
        };
        crud_attempt_to_store_record(record)
    }

    async fn append_audit_events(
        &self,
        events: Vec<NewHookAuditEventStoreRecord>,
    ) -> HookRunStoreResult<Vec<HookAuditEventStoreRecord>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let now = unix_ms_to_datetime(
            events
                .first()
                .and_then(|event| event.created_at_unix_ms)
                .unwrap_or_else(current_unix_ms),
        );
        let records = events
            .into_iter()
            .map(|event| CrudNewHookAuditEventRecord {
                hook_run_id: event.hook_run_id,
                hook_run_attempt_id: event.hook_run_attempt_id,
                subscription_id: event.subscription_id,
                hook_id: event.hook_id,
                phase: event.phase,
                context: event.context,
                event_kind: event.event_kind,
                contribution_hash: event.contribution_hash,
                details: event.details,
                safe_for_user: event.safe_for_user,
                created_at: event.created_at_unix_ms.map(unix_ms_to_datetime),
            })
            .collect::<Vec<_>>();
        self.crud_store
            .append_hook_audit_events(records, now)
            .await
            .map_err(|_| HookRunStoreError::internal("failed to append hook audit events"))?
            .into_iter()
            .map(crud_audit_to_store_record)
            .collect()
    }

    async fn list_recoverable_runs(
        &self,
        scan: HookRecoveryScan,
    ) -> HookRunStoreResult<Vec<HookRecoverableRunRecord>> {
        self.crud_store
            .list_recoverable_hook_runs(scan)
            .await
            .map_err(|_| HookRunStoreError::internal("failed to list recoverable hook runs"))?
            .into_iter()
            .map(|record| {
                let attempts = record
                    .attempts
                    .into_iter()
                    .map(crud_attempt_to_store_record)
                    .collect::<HookRunStoreResult<Vec<_>>>()?;
                let run = crud_run_to_store_record(record.run)?;
                Ok(HookRecoverableRunRecord {
                    resume_state: record.resume_state,
                    run,
                    attempts,
                })
            })
            .collect()
    }

    async fn schedule_run_retry(
        &self,
        run_id: &HookRunId,
        schedule: HookRetrySchedule,
    ) -> HookRunStoreResult<HookRunStoreRecord> {
        let Some(record) = self
            .crud_store
            .schedule_hook_run_retry(run_id, schedule, unix_ms_to_datetime(current_unix_ms()))
            .await
            .map_err(|_| HookRunStoreError::internal("failed to schedule hook run retry"))?
        else {
            return Err(HookRunStoreError::invalid_record("hook run not found"));
        };
        crud_run_to_store_record(record)
    }

    async fn mark_stale_run_timed_out(
        &self,
        run_id: &HookRunId,
        completion: HookRunStoreCompletion,
    ) -> HookRunStoreResult<HookRunStoreRecord> {
        let completed_at = unix_ms_to_datetime(completion.completed_at_unix_ms);
        let Some(record) = self
            .crud_store
            .mark_stale_hook_run_timed_out(
                run_id,
                CrudHookRunCompletionRecord {
                    status: completion.status,
                    contribution_hashes: completion.contribution_hashes,
                    diagnostic_previews: completion.diagnostic_previews,
                    error: completion.error,
                    completed_at: Some(completed_at),
                },
                completed_at,
            )
            .await
            .map_err(|_| HookRunStoreError::internal("failed to mark stale hook run timed out"))?
        else {
            return Err(HookRunStoreError::invalid_record("hook run not found"));
        };
        crud_run_to_store_record(record)
    }

    async fn mark_run_unrecoverable(
        &self,
        run_id: &HookRunId,
        completion: HookRunStoreCompletion,
    ) -> HookRunStoreResult<HookRunStoreRecord> {
        let completed_at = unix_ms_to_datetime(completion.completed_at_unix_ms);
        let Some(record) = self
            .crud_store
            .mark_hook_run_unrecoverable(
                run_id,
                CrudHookRunCompletionRecord {
                    status: completion.status,
                    contribution_hashes: completion.contribution_hashes,
                    diagnostic_previews: completion.diagnostic_previews,
                    error: completion.error,
                    completed_at: Some(completed_at),
                },
                completed_at,
            )
            .await
            .map_err(|_| HookRunStoreError::internal("failed to mark hook run unrecoverable"))?
        else {
            return Err(HookRunStoreError::invalid_record("hook run not found"));
        };
        crud_run_to_store_record(record)
    }
}

fn crud_run_to_store_record(record: CrudHookRunRecord) -> HookRunStoreResult<HookRunStoreRecord> {
    Ok(HookRunStoreRecord {
        id: record.id,
        idempotency_key: record.idempotency_key,
        subscription_id: record.subscription_id,
        hook_id: record.hook_id,
        phase: record.phase,
        status: record.status,
        scope: record.scope.map(store_scope_from_crud_scope),
        context: record.context,
        attempt_count: record.attempt_count,
        contribution_count: record.contribution_count,
        diagnostic_count: record.diagnostic_count,
        contribution_hashes: record.contribution_hashes,
        diagnostic_previews: record.diagnostic_previews,
        error: record.error,
        queued_at_unix_ms: record.queued_at.map(datetime_to_unix_ms),
        started_at_unix_ms: record.started_at.map(datetime_to_unix_ms),
        completed_at_unix_ms: record.completed_at.map(datetime_to_unix_ms),
        deadline_at_unix_ms: record.deadline_at.map(datetime_to_unix_ms),
        resume_state: record.resume_state,
    })
}

fn crud_attempt_to_store_record(
    record: CrudHookRunAttemptRecord,
) -> HookRunStoreResult<HookRunAttemptStoreRecord> {
    Ok(HookRunAttemptStoreRecord {
        id: record.id,
        hook_run_id: record.hook_run_id,
        attempt_number: record.attempt_number,
        status: record.status,
        contribution_count: record.contribution_count,
        diagnostic_count: record.diagnostic_count,
        contribution_hashes: record.contribution_hashes,
        diagnostic_previews: record.diagnostic_previews,
        error: record.error,
        started_at_unix_ms: record.started_at.map(datetime_to_unix_ms),
        completed_at_unix_ms: record.completed_at.map(datetime_to_unix_ms),
        duration_ms: record.duration_ms,
    })
}

fn crud_audit_to_store_record(
    record: CrudHookAuditEventRecord,
) -> HookRunStoreResult<HookAuditEventStoreRecord> {
    Ok(HookAuditEventStoreRecord {
        id: record.id,
        hook_run_id: record.hook_run_id,
        hook_run_attempt_id: record.hook_run_attempt_id,
        subscription_id: record.subscription_id,
        hook_id: record.hook_id,
        phase: record.phase,
        context: record.context,
        event_kind: record.event_kind,
        contribution_hash: record.contribution_hash,
        details: record.details,
        safe_for_user: record.safe_for_user,
        created_at_unix_ms: record.created_at.timestamp_millis(),
    })
}

fn crud_scope_from_store_scope(scope: HookRunScope) -> CrudHookRunScope {
    CrudHookRunScope {
        kind: crud_scope_kind_from_store_scope_kind(scope.kind),
        id: scope.id,
    }
}

fn store_scope_from_crud_scope(scope: CrudHookRunScope) -> HookRunScope {
    HookRunScope {
        kind: store_scope_kind_from_crud_scope_kind(scope.kind),
        id: scope.id,
    }
}

fn crud_scope_kind_from_store_scope_kind(kind: HookRunScopeKind) -> CrudHookRunScopeKind {
    match kind {
        HookRunScopeKind::Workspace => CrudHookRunScopeKind::Workspace,
        HookRunScopeKind::Thread => CrudHookRunScopeKind::Thread,
        HookRunScopeKind::Turn => CrudHookRunScopeKind::Turn,
        HookRunScopeKind::Task => CrudHookRunScopeKind::Task,
        HookRunScopeKind::Agent => CrudHookRunScopeKind::Agent,
        HookRunScopeKind::Hook => CrudHookRunScopeKind::Hook,
        HookRunScopeKind::Custom(kind) => CrudHookRunScopeKind::Custom(kind),
    }
}

fn store_scope_kind_from_crud_scope_kind(kind: CrudHookRunScopeKind) -> HookRunScopeKind {
    match kind {
        CrudHookRunScopeKind::Workspace => HookRunScopeKind::Workspace,
        CrudHookRunScopeKind::Thread => HookRunScopeKind::Thread,
        CrudHookRunScopeKind::Turn => HookRunScopeKind::Turn,
        CrudHookRunScopeKind::Task => HookRunScopeKind::Task,
        CrudHookRunScopeKind::Agent => HookRunScopeKind::Agent,
        CrudHookRunScopeKind::Hook => HookRunScopeKind::Hook,
        CrudHookRunScopeKind::Custom(kind) => HookRunScopeKind::Custom(kind),
    }
}

fn unix_ms_to_datetime(value: i64) -> DateTimeWithTimeZone {
    Utc.timestamp_millis_opt(value)
        .single()
        .unwrap_or_else(Utc::now)
        .fixed_offset()
}

fn datetime_to_unix_ms(value: DateTimeWithTimeZone) -> i64 {
    value.timestamp_millis()
}

fn current_unix_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_entity::{hook_audit_event, hook_run, hook_run_attempt};
    use pioneer_hooks::{
        AuditContribution, HookAuditEventKind, HookAwaitPolicy, HookCapabilities, HookCapability,
        HookContext, HookContribution, HookDiagnosticCode, HookDiagnosticMessage, HookDomain,
        HookError, HookExecutionPolicy, HookFailurePolicy, HookHandler, HookHandlerRequest,
        HookHandlerResponse, HookId, HookInput, HookInputKind, HookKind, HookPhase,
        HookPhaseRequest, HookPolicySet, HookPromptContent, HookPromptContextSet, HookRecoveryScan,
        HookRegistry, HookRetryPolicy, HookRetrySchedule, HookRunIdempotencyKey,
        HookRunInputSnapshot, HookRunResumeState, HookRunScopeId, HookRunStatus, HookRuntime,
        HookRuntimeOptions, HookSectionId, HookSubscription, HookSubscriptionId,
        HookSubscriptionRegistry, HookValue, PromptSectionContribution,
    };
    use sea_orm::{Database, DatabaseConnection, EntityTrait};
    use std::time::Duration;

    struct PromptContributionHandler {
        id: HookId,
        secret: String,
    }

    #[async_trait]
    impl HookHandler for PromptContributionHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            vec![HookPhase::TurnPrePromptCompile]
        }

        fn capabilities(&self) -> HookCapabilities {
            HookCapabilities::new([
                HookCapability::new("contribute_prompt_section").expect("valid capability")
            ])
        }

        async fn execute(
            &self,
            _request: HookHandlerRequest,
        ) -> pioneer_hooks::HookResult<HookHandlerResponse> {
            Ok(HookHandlerResponse {
                contributions: vec![HookContribution::PromptSection(PromptSectionContribution {
                    contribution_id: pioneer_hooks::HookContributionId::new(
                        "gateway.phase15.secret",
                    )
                    .expect("valid contribution id"),
                    section_id: HookSectionId::new("gateway.phase15.secret")
                        .expect("valid section id"),
                    title: None,
                    domain: HookDomain::new("gateway.phase15").expect("valid domain"),
                    priority: 0,
                    content: HookPromptContent::new(self.secret.clone()).expect("valid content"),
                    max_chars: None,
                    source_refs: Vec::new(),
                    diagnostics: Vec::new(),
                    truncated: false,
                })],
                diagnostics: Vec::new(),
                metadata: pioneer_hooks::HookMetadata::default(),
            })
        }
    }

    struct AuditContributionHandler {
        id: HookId,
    }

    #[async_trait]
    impl HookHandler for AuditContributionHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            vec![HookPhase::TurnPrePromptCompile]
        }

        fn capabilities(&self) -> HookCapabilities {
            HookCapabilities::new([HookCapability::new("emit_audit").expect("valid capability")])
        }

        async fn execute(
            &self,
            _request: HookHandlerRequest,
        ) -> pioneer_hooks::HookResult<HookHandlerResponse> {
            Ok(HookHandlerResponse {
                contributions: vec![HookContribution::Audit(AuditContribution {
                    event_kind: HookAuditEventKind::new("test.gateway_hook_audit")
                        .expect("valid audit event kind"),
                    details: HookValue::Text("gateway audit detail".to_owned()),
                    safe_for_user: false,
                })],
                ..HookHandlerResponse::default()
            })
        }
    }

    struct FailingHandler {
        id: HookId,
    }

    #[async_trait]
    impl HookHandler for FailingHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            vec![HookPhase::TurnPrePromptCompile]
        }

        async fn execute(
            &self,
            _request: HookHandlerRequest,
        ) -> pioneer_hooks::HookResult<HookHandlerResponse> {
            Err(HookError::new(
                HookDiagnosticCode::new("gateway.phase15.failed").expect("valid diagnostic code"),
                HookDiagnosticMessage::new("phase 15 failure").expect("valid diagnostic message"),
            ))
        }
    }

    struct SlowHandler {
        id: HookId,
    }

    #[async_trait]
    impl HookHandler for SlowHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            vec![HookPhase::TurnPrePromptCompile]
        }

        async fn execute(
            &self,
            _request: HookHandlerRequest,
        ) -> pioneer_hooks::HookResult<HookHandlerResponse> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(HookHandlerResponse::default())
        }
    }

    async fn migrated_store() -> (DatabaseConnection, Arc<CrudStore>, CrudHookRunStore) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        let crud_store = Arc::new(CrudStore::new(connection.clone()));
        let hook_store = CrudHookRunStore::new(crud_store.clone());
        (connection, crud_store, hook_store)
    }

    fn new_store_run(key: &str) -> NewHookRunStoreRecord {
        NewHookRunStoreRecord {
            idempotency_key: HookRunIdempotencyKey::new(key).expect("valid idempotency key"),
            subscription_id: HookSubscriptionId::new("sub.gateway").expect("valid id"),
            hook_id: HookId::new("hook.gateway").expect("valid id"),
            phase: HookPhase::TurnPrePromptCompile,
            status: HookRunStatus::Queued,
            scope: Some(HookRunScope {
                kind: HookRunScopeKind::Turn,
                id: HookRunScopeId::new("turn_gateway").expect("valid scope id"),
            }),
            context: HookContext::default(),
            contribution_hashes: Vec::new(),
            diagnostic_previews: Vec::new(),
            error: None,
            queued_at_unix_ms: Some(1_700_000_000_000),
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
            deadline_at_unix_ms: None,
            resume_state: None,
        }
    }

    fn phase_15_request() -> HookPhaseRequest {
        HookPhaseRequest::new(
            HookPhase::TurnPrePromptCompile,
            HookContext::default(),
            HookInput::empty(HookInputKind::TurnPrePromptCompile),
        )
    }

    fn phase_21_resume_state() -> HookRunResumeState {
        let execution_policy = HookExecutionPolicy {
            await_policy: HookAwaitPolicy::Background,
            timeout_ms: Some(10_000),
            max_parallelism: None,
        };
        let input = HookInput::empty(HookInputKind::TurnPostTurn);
        HookRunResumeState::input_snapshot(
            execution_policy,
            HookFailurePolicy::BestEffort,
            HookRetryPolicy::default(),
            1,
            1,
            1,
            HookRunInputSnapshot::new(
                HookPhase::TurnPostTurn,
                HookContext::default(),
                input,
                HookPolicySet::empty(),
                HookPromptContextSet::empty(),
            ),
        )
    }

    async fn one_run_row(connection: &DatabaseConnection) -> hook_run::Model {
        let rows = hook_run::Entity::find()
            .all(connection)
            .await
            .expect("hook run rows should query");
        assert_eq!(rows.len(), 1);
        rows.into_iter().next().expect("one hook run row")
    }

    async fn one_attempt_row(connection: &DatabaseConnection) -> hook_run_attempt::Model {
        let rows = hook_run_attempt::Entity::find()
            .all(connection)
            .await
            .expect("hook run attempt rows should query");
        assert_eq!(rows.len(), 1);
        rows.into_iter().next().expect("one hook run attempt row")
    }

    async fn one_audit_row(connection: &DatabaseConnection) -> hook_audit_event::Model {
        let rows = hook_audit_event::Entity::find()
            .all(connection)
            .await
            .expect("hook audit rows should query");
        assert_eq!(rows.len(), 1);
        rows.into_iter().next().expect("one hook audit row")
    }

    #[tokio::test]
    async fn crud_hook_run_store_persists_success() {
        let (_connection, _crud_store, hook_store) = migrated_store().await;
        let run = hook_store
            .create_or_load_run(new_store_run("gateway:hook:success"))
            .await
            .expect("run should create");
        let running = hook_store
            .mark_run_running(&run.id, 1_700_000_000_010)
            .await
            .expect("run should mark running");
        assert_eq!(running.status, HookRunStatus::Running);

        let attempt = hook_store
            .append_attempt(NewHookRunAttemptStoreRecord {
                hook_run_id: run.id.clone(),
                attempt_number: 1,
                status: HookRunStatus::Running,
                contribution_hashes: Vec::new(),
                diagnostic_previews: Vec::new(),
                error: None,
                started_at_unix_ms: Some(1_700_000_000_010),
                completed_at_unix_ms: None,
                duration_ms: None,
            })
            .await
            .expect("attempt should append");
        let completed_attempt = hook_store
            .complete_attempt(
                &attempt.id,
                HookRunAttemptStoreCompletion {
                    status: HookRunStatus::Succeeded,
                    contribution_hashes: Vec::new(),
                    diagnostic_previews: Vec::new(),
                    error: None,
                    completed_at_unix_ms: 1_700_000_000_050,
                    duration_ms: Some(40),
                },
            )
            .await
            .expect("attempt should complete");
        assert_eq!(completed_attempt.status, HookRunStatus::Succeeded);

        let completed = hook_store
            .complete_run(
                &run.id,
                HookRunStoreCompletion {
                    status: HookRunStatus::Succeeded,
                    contribution_hashes: Vec::new(),
                    diagnostic_previews: Vec::new(),
                    error: None,
                    completed_at_unix_ms: 1_700_000_000_050,
                },
            )
            .await
            .expect("run should complete");
        assert_eq!(completed.status, HookRunStatus::Succeeded);
        assert_eq!(completed.attempt_count, 1);
    }

    #[tokio::test]
    async fn crud_hook_run_store_maps_duplicate_create_to_existing_run() {
        let (_connection, _crud_store, hook_store) = migrated_store().await;
        let first = hook_store
            .create_or_load_run(new_store_run("gateway:hook:duplicate"))
            .await
            .expect("first create should succeed");
        let second = hook_store
            .create_or_load_run(new_store_run("gateway:hook:duplicate"))
            .await
            .expect("second create should load");

        assert_eq!(first.id, second.id);
    }

    #[tokio::test]
    async fn phase_21_crud_hook_run_store_lists_due_recoverable_queued_runs() {
        let (_connection, _crud_store, hook_store) = migrated_store().await;
        let mut run = new_store_run("gateway:hook:recoverable:queued");
        run.phase = HookPhase::TurnPostTurn;
        run.queued_at_unix_ms = Some(1_000);
        run.resume_state = Some(phase_21_resume_state());
        let created = hook_store
            .create_or_load_run(run)
            .await
            .expect("run should create");

        let records = hook_store
            .list_recoverable_runs(HookRecoveryScan {
                now_unix_ms: 1_001,
                batch_size: 10,
                stale_running_after_ms: 1_000,
                phases: Some(vec![HookPhase::TurnPostTurn]),
            })
            .await
            .expect("recoverable runs should list");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run.id, created.id);
        assert!(records[0].resume_state.is_some());
        assert!(records[0].attempts.is_empty());
    }

    #[tokio::test]
    async fn phase_21_crud_hook_run_store_does_not_list_future_retry() {
        let (_connection, _crud_store, hook_store) = migrated_store().await;
        let mut run = new_store_run("gateway:hook:recoverable:future");
        run.phase = HookPhase::TurnPostTurn;
        run.queued_at_unix_ms = Some(5_000);
        run.resume_state = Some(phase_21_resume_state());
        hook_store
            .create_or_load_run(run)
            .await
            .expect("run should create");

        let records = hook_store
            .list_recoverable_runs(HookRecoveryScan {
                now_unix_ms: 4_999,
                batch_size: 10,
                stale_running_after_ms: 1_000,
                phases: Some(vec![HookPhase::TurnPostTurn]),
            })
            .await
            .expect("recoverable runs should list");

        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn phase_21_crud_hook_run_store_schedules_retry_and_lists_when_due() {
        let (_connection, _crud_store, hook_store) = migrated_store().await;
        let mut run = new_store_run("gateway:hook:recoverable:retry");
        run.phase = HookPhase::TurnPostTurn;
        run.queued_at_unix_ms = Some(1_000);
        run.resume_state = Some(phase_21_resume_state());
        let created = hook_store
            .create_or_load_run(run)
            .await
            .expect("run should create");

        let scheduled = hook_store
            .schedule_run_retry(
                &created.id,
                HookRetrySchedule {
                    queued_at_unix_ms: 3_000,
                    deadline_at_unix_ms: None,
                    diagnostic_previews: Vec::new(),
                },
            )
            .await
            .expect("retry should schedule");
        assert_eq!(scheduled.status, HookRunStatus::Queued);

        let before_due = hook_store
            .list_recoverable_runs(HookRecoveryScan {
                now_unix_ms: 2_999,
                batch_size: 10,
                stale_running_after_ms: 1_000,
                phases: Some(vec![HookPhase::TurnPostTurn]),
            })
            .await
            .expect("recoverable runs should list");
        assert!(before_due.is_empty());

        let after_due = hook_store
            .list_recoverable_runs(HookRecoveryScan {
                now_unix_ms: 3_000,
                batch_size: 10,
                stale_running_after_ms: 1_000,
                phases: Some(vec![HookPhase::TurnPostTurn]),
            })
            .await
            .expect("recoverable runs should list");
        assert_eq!(after_due.len(), 1);
        assert_eq!(after_due[0].run.id, created.id);
    }

    #[tokio::test]
    async fn phase_21_crud_hook_run_store_marks_stale_running_timed_out() {
        let (_connection, _crud_store, hook_store) = migrated_store().await;
        let mut run = new_store_run("gateway:hook:recoverable:stale");
        run.phase = HookPhase::TurnPostTurn;
        run.resume_state = Some(phase_21_resume_state());
        let created = hook_store
            .create_or_load_run(run)
            .await
            .expect("run should create");
        hook_store
            .mark_run_running(&created.id, 1_000)
            .await
            .expect("run should mark running");
        let attempt = hook_store
            .append_attempt(NewHookRunAttemptStoreRecord {
                hook_run_id: created.id.clone(),
                attempt_number: 1,
                status: HookRunStatus::Running,
                contribution_hashes: Vec::new(),
                diagnostic_previews: Vec::new(),
                error: None,
                started_at_unix_ms: Some(1_000),
                completed_at_unix_ms: None,
                duration_ms: None,
            })
            .await
            .expect("attempt should append");

        let records = hook_store
            .list_recoverable_runs(HookRecoveryScan {
                now_unix_ms: 3_000,
                batch_size: 10,
                stale_running_after_ms: 1_000,
                phases: Some(vec![HookPhase::TurnPostTurn]),
            })
            .await
            .expect("recoverable runs should list");
        assert_eq!(records.len(), 1);

        let completed = hook_store
            .mark_stale_run_timed_out(
                &created.id,
                HookRunStoreCompletion {
                    status: HookRunStatus::TimedOut,
                    contribution_hashes: Vec::new(),
                    diagnostic_previews: Vec::new(),
                    error: None,
                    completed_at_unix_ms: 3_000,
                },
            )
            .await
            .expect("stale run should mark timed out");
        assert_eq!(completed.status, HookRunStatus::TimedOut);

        let attempts = _crud_store
            .list_hook_run_attempts(&created.id)
            .await
            .expect("attempts should list");
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].id, attempt.id);
        assert_eq!(attempts[0].status, HookRunStatus::TimedOut);
    }

    #[tokio::test]
    async fn crud_hook_run_store_persists_failure() {
        let (connection, _crud_store, hook_store) = migrated_store().await;
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let hook_id = HookId::new("gateway.phase15.failure").expect("valid hook id");
        handlers
            .register_handler(Arc::new(FailingHandler {
                id: hook_id.clone(),
            }))
            .expect("handler registers");
        subscriptions
            .register_subscription(
                handlers.as_ref(),
                HookSubscription::new(
                    HookSubscriptionId::new("gateway.phase15.failure").expect("valid sub id"),
                    hook_id,
                    HookPhase::TurnPrePromptCompile,
                )
                .with_failure_policy(HookFailurePolicy::BestEffort),
            )
            .expect("subscription registers");

        let runtime = HookRuntime::with_run_store(handlers, subscriptions, Arc::new(hook_store));
        let response = runtime
            .run_phase(phase_15_request())
            .await
            .expect("best-effort hook failure should not fail phase");
        assert_eq!(response.runs[0].status, HookRunStatus::Failed);

        let run = one_run_row(&connection).await;
        let attempt = one_attempt_row(&connection).await;
        assert_eq!(run.status, "failed");
        assert_eq!(run.attempt_count, 1);
        assert_eq!(run.error_code.as_deref(), Some("gateway.phase15.failed"));
        assert_eq!(attempt.status, "failed");
        assert_eq!(
            attempt.error_code.as_deref(),
            Some("gateway.phase15.failed")
        );
    }

    #[tokio::test]
    async fn crud_hook_run_store_persists_timeout() {
        let (connection, _crud_store, hook_store) = migrated_store().await;
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let hook_id = HookId::new("gateway.phase15.timeout").expect("valid hook id");
        handlers
            .register_handler(Arc::new(SlowHandler {
                id: hook_id.clone(),
            }))
            .expect("handler registers");
        subscriptions
            .register_subscription(
                handlers.as_ref(),
                HookSubscription::new(
                    HookSubscriptionId::new("gateway.phase15.timeout").expect("valid sub id"),
                    hook_id,
                    HookPhase::TurnPrePromptCompile,
                )
                .with_execution_policy(HookExecutionPolicy {
                    await_policy: HookAwaitPolicy::Deadline,
                    timeout_ms: Some(1),
                    max_parallelism: None,
                })
                .with_failure_policy(HookFailurePolicy::BestEffort),
            )
            .expect("subscription registers");

        let runtime = HookRuntime::with_options_and_run_store(
            handlers,
            subscriptions,
            HookRuntimeOptions {
                default_deadline_timeout_ms: 1,
                ..HookRuntimeOptions::default()
            },
            Arc::new(hook_store),
        );
        let response = runtime
            .run_phase(phase_15_request())
            .await
            .expect("best-effort hook timeout should not fail phase");
        assert_eq!(response.runs[0].status, HookRunStatus::TimedOut);

        let run = one_run_row(&connection).await;
        let attempt = one_attempt_row(&connection).await;
        assert_eq!(run.status, "timed_out");
        assert_eq!(run.attempt_count, 1);
        assert_eq!(run.error_code.as_deref(), Some("hook.timeout"));
        assert_eq!(attempt.status, "timed_out");
        assert_eq!(attempt.error_code.as_deref(), Some("hook.timeout"));
        assert!(run.deadline_at.is_some());
    }

    #[tokio::test]
    async fn crud_hook_run_store_runtime_does_not_store_raw_prompt_contribution() {
        let (connection, _crud_store, hook_store) = migrated_store().await;
        let secret = "SECRET_PROMPT_SECTION_SHOULD_NOT_BE_STORED";
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(PromptContributionHandler {
                id: HookId::new("gateway.phase15.prompt").expect("valid hook id"),
                secret: secret.to_owned(),
            }))
            .expect("handler registers");
        subscriptions
            .register_subscription(
                handlers.as_ref(),
                HookSubscription::new(
                    HookSubscriptionId::new("gateway.phase15.prompt").expect("valid sub id"),
                    HookId::new("gateway.phase15.prompt").expect("valid hook id"),
                    HookPhase::TurnPrePromptCompile,
                ),
            )
            .expect("subscription registers");

        let runtime = HookRuntime::with_run_store(handlers, subscriptions, Arc::new(hook_store));
        let response = runtime
            .run_phase(phase_15_request())
            .await
            .expect("hook runtime should succeed");
        assert_eq!(response.runs[0].status, HookRunStatus::Succeeded);

        let run_rows = hook_run::Entity::find()
            .all(&connection)
            .await
            .expect("hook run rows should query");
        let attempt_rows = hook_run_attempt::Entity::find()
            .all(&connection)
            .await
            .expect("hook run attempt rows should query");
        let persisted_text = format!("{run_rows:?}{attempt_rows:?}");
        assert!(
            !persisted_text.contains(secret),
            "hook run persistence must not store raw prompt contribution"
        );
        assert!(persisted_text.contains("sha256:"));
    }

    #[tokio::test]
    async fn crud_hook_run_store_persists_audit_contribution_rows() {
        let (connection, crud_store, hook_store) = migrated_store().await;
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let hook_id = HookId::new("gateway.phase15.audit").expect("valid hook id");
        handlers
            .register_handler(Arc::new(AuditContributionHandler {
                id: hook_id.clone(),
            }))
            .expect("handler registers");
        subscriptions
            .register_subscription(
                handlers.as_ref(),
                HookSubscription::new(
                    HookSubscriptionId::new("gateway.phase15.audit").expect("valid sub id"),
                    hook_id,
                    HookPhase::TurnPrePromptCompile,
                ),
            )
            .expect("subscription registers");

        let runtime = HookRuntime::with_run_store(handlers, subscriptions, Arc::new(hook_store));
        let response = runtime
            .run_phase(phase_15_request())
            .await
            .expect("hook runtime should succeed");
        assert_eq!(response.runs[0].status, HookRunStatus::Succeeded);

        let run = one_run_row(&connection).await;
        let audit_row = one_audit_row(&connection).await;
        assert_eq!(audit_row.hook_run_id, run.id);
        assert_eq!(audit_row.event_kind, "test.gateway_hook_audit");
        assert_eq!(audit_row.safe_for_user, false);
        assert!(
            audit_row
                .contribution_hash
                .as_deref()
                .unwrap_or("")
                .starts_with("sha256:")
        );
        let records = crud_store
            .list_hook_audit_events_for_run(&HookRunId::new(run.id).expect("valid run id"))
            .await
            .expect("hook audit event list should succeed");
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].details,
            HookValue::Text("gateway audit detail".to_owned())
        );
    }
}
