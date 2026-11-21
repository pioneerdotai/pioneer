use crate::store::events::event_preparation::{
    PreparedProjectedTurnEvent, TURN_EVENT_PROJECTION_LEASE_SECS,
    prepare_projected_turn_event_for_permanent_storage,
};
use crate::{CanonicalTurnEventPayload, CrudStore};
use anyhow::{Result, ensure};
use pioneer_crud_compaction::background::CompletedHistoryCheck;
use pioneer_crud_compaction::lifecycle::{LifecycleAdmission, PreparedCompactionLifecycle};
use pioneer_crud_conversations::turn_event_reads::find_event_model_by_idempotency_key;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_entity::turn_event;
use sea_orm::TransactionTrait;

impl CrudStore {
    pub async fn compaction_enqueue_native_history_check(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        descriptor: &str,
    ) -> Result<()> {
        self.compaction_store()
            .compaction_enqueue_native_history_check(workspace, thread, turn, descriptor)
            .await
    }

    /// Bookkeeping has Maintenance reads and writes even on a request handle.
    /// Call seed and ordinary scopes at most once each per shared quantum.
    pub async fn compaction_expand_lifecycle_scope(
        &self,
        seed: bool,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<u64> {
        self.compaction_store()
            .compaction_expand_lifecycle_scope(seed, clock)
            .await
    }

    pub async fn compaction_due_lifecycle(
        &self,
        now_ms: i64,
    ) -> Result<Vec<pioneer_crud_compaction::lifecycle_pending::CompactionLifecycleCandidate>> {
        self.compaction_store()
            .compaction_due_lifecycle(now_ms)
            .await
    }

    pub async fn compaction_claim_lifecycle(
        &self,
        candidate: &pioneer_crud_compaction::lifecycle_pending::CompactionLifecycleCandidate,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<Option<pioneer_crud_compaction::lifecycle_pending::CompactionLifecycleClaim>> {
        self.compaction_store()
            .compaction_claim_lifecycle(candidate, clock)
            .await
    }

    pub async fn compaction_prepare_lifecycle(
        &self,
        claim: &pioneer_crud_compaction::lifecycle_pending::CompactionLifecycleClaim,
        now_ms: i64,
    ) -> Result<Option<pioneer_crud_compaction::lifecycle::PreparedCompactionLifecycle>> {
        self.compaction_store()
            .compaction_prepare_lifecycle(claim, now_ms)
            .await
    }

    pub async fn compaction_repair_lifecycle(
        &self,
        prepared: pioneer_crud_compaction::lifecycle::PreparedCompactionLifecycle,
        event: Option<CanonicalTurnEventPayload>,
        timestamp_secs: i64,
    ) -> Result<bool> {
        let cleanup_turns = prepared
            .scope()
            .map(|(_, _, turn)| turn.to_owned())
            .into_iter()
            .chain(event.iter().map(|event| event.turn_id().to_owned()))
            .collect::<Vec<_>>();
        let cleanup_change = crate::terminal_cleanup::Change::events(&self.connection);
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let frozen_keys = vec![format!("o:{}", prepared.operation_id())];
        let changes = [
            "gateway.control",
            "projection",
            "compaction",
            "maintenance.html",
        ]
        .map(|domain| {
            self.control_work_change(
                domain,
                &cleanup_turns
                    .iter()
                    .cloned()
                    .map(crate::GatewayControlWake::Turn)
                    .collect::<Vec<_>>(),
            )
        });
        let maintenance = self.with_maintenance_access();
        let result = crate::store::compaction::lifecycle::repair_lifecycle(
            &maintenance,
            prepared,
            event,
            timestamp_secs,
        )
        .await;
        if let Ok((_, changed)) = &result {
            cleanup_change
                .complete(&self.connection, *changed, &cleanup_turns)
                .await;
            frozen_change.completed_keys(if *changed { &frozen_keys } else { &[] });
            for change in changes {
                change.completed(*changed);
            }
        }
        result.map(|(acknowledged, _)| acknowledged)
    }

    /// A newer execution invalidates an optional older check. Inspect one
    /// locator at a time rather than bulk-updating a thread's retained history.
    pub async fn compaction_history_check_is_current(&self, turn: &str) -> Result<bool> {
        self.compaction_store()
            .compaction_history_check_is_current(turn)
            .await
    }

    pub async fn compaction_history_check_revision_is_current(
        &self,
        turn: &str,
        revision: i64,
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_history_check_revision_is_current(turn, revision)
            .await
    }

    pub async fn compaction_root_history_activity(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<i64>> {
        self.compaction_store()
            .compaction_root_history_activity(workspace, thread, turn)
            .await
    }

    pub async fn compaction_root_history_busy(
        &self,
        workspace: &str,
        thread: &str,
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_root_history_busy(workspace, thread)
            .await
    }

    pub async fn compaction_root_for_execution(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<String>> {
        self.compaction_store()
            .compaction_root_for_execution(workspace, thread, turn)
            .await
    }

    /// One bounded metadata page; caller releases reader capacity before decoding.
    pub async fn compaction_pending_history_checks(&self) -> Result<Vec<CompletedHistoryCheck>> {
        self.compaction_store()
            .compaction_pending_history_checks()
            .await
    }

    /// Persist the captured settings/deadline before service admission; restart
    /// reuses this exact descriptor. It contains model metadata, never messages.
    pub async fn compaction_capture_history_check(
        &self,
        turn: &str,
        descriptor: &str,
    ) -> Result<Option<String>> {
        self.compaction_store()
            .compaction_capture_history_check(turn, descriptor)
            .await
    }

    pub async fn compaction_finish_history_check(&self, turn: &str, outcome: &str) -> Result<()> {
        self.compaction_store()
            .compaction_finish_history_check(turn, outcome)
            .await
    }

    /// Constant-size control-plane fence. The owning runtime awaits this write
    /// before cancelling service work. It survives worker loss without changing
    /// the completed user Turn or granting a new compaction attempt.
    pub async fn compaction_stop_execution(
        &self,
        workspace: &str,
        thread: &str,
        owner: &str,
        turn: &str,
    ) -> Result<()> {
        self.compaction_store()
            .compaction_stop_execution(workspace, thread, owner, turn)
            .await
    }

    pub async fn compaction_materialize_lifecycle(
        &self,
        operation: &str,
        generation: u64,
        event: CanonicalTurnEventPayload,
        timestamp_secs: i64,
    ) -> Result<()> {
        let cleanup_turns = vec![event.turn_id().to_owned()];
        let cleanup_change = crate::terminal_cleanup::Change::events(&self.connection);
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let frozen_keys = vec![format!("o:{operation}")];
        let changes = [
            "gateway.control",
            "projection",
            "compaction",
            "maintenance.html",
        ]
        .map(|domain| {
            self.control_work_change(
                domain,
                &cleanup_turns
                    .iter()
                    .cloned()
                    .map(crate::GatewayControlWake::Turn)
                    .collect::<Vec<_>>(),
            )
        });
        let result = crate::store::compaction::lifecycle::compaction_materialize_lifecycle(
            self,
            operation,
            generation,
            event,
            timestamp_secs,
        )
        .await;
        if let Ok(changed) = &result {
            cleanup_change
                .complete(&self.connection, *changed, &cleanup_turns)
                .await;
            frozen_change.completed_keys(if *changed { &frozen_keys } else { &[] });
            for change in changes {
                change.completed(*changed);
            }
        }
        result.map(|_| ())
    }

    /// Manifest admission contains metadata only and resumes in bounded batches.
    /// No provider may run until activate_runner has checked the whole manifest.
    /// The execution turn is an immutable admission boundary. Its terminal
    /// interruption fences checkpoint publication even before service cleanup.
    pub async fn compaction_execution_cancelled(&self, operation: &str) -> Result<bool> {
        self.compaction_store()
            .compaction_execution_cancelled(operation)
            .await
    }

    pub async fn compaction_bind_execution_turn(&self, operation: &str, turn: &str) -> Result<()> {
        self.compaction_store()
            .compaction_bind_execution_turn(operation, turn)
            .await
    }

    /// Reuse saved progress when a later request admits the same source plan.
    pub async fn compaction_resume_deadline(
        &self,
        operation: &str,
        execution_turn: &str,
        deadline_ms: u64,
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_resume_deadline(operation, execution_turn, deadline_ms)
            .await
    }

    /// Called only by foreground admission for a distinct user execution.
    pub async fn compaction_resume_invalid_completion(
        &self,
        operation: &str,
        execution_turn: &str,
        deadline_ms: u64,
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_resume_invalid_completion(operation, execution_turn, deadline_ms)
            .await
    }

    pub async fn compaction_finish_execution(
        &self,
        operation: &str,
        execution_turn: Option<&str>,
        deadline_ms: u64,
        status: &str,
        outcome: &str,
    ) -> Result<()> {
        self.compaction_store()
            .compaction_finish_execution(operation, execution_turn, deadline_ms, status, outcome)
            .await
    }

    pub async fn next_compaction_background_deadline(
        &self,
        owned: &[(String, String)],
    ) -> Result<Option<i64>> {
        self.compaction_store()
            .next_compaction_background_deadline(owned)
            .await
    }

    pub async fn compaction_due_history_checks_excluding(
        &self,
        now: i64,
        owned: &[(String, String)],
    ) -> Result<Vec<CompletedHistoryCheck>> {
        self.compaction_store()
            .compaction_due_history_checks_excluding(now, owned)
            .await
    }

    pub async fn compaction_due_history_checks(
        &self,
        now: i64,
    ) -> Result<Vec<CompletedHistoryCheck>> {
        self.compaction_store()
            .compaction_due_history_checks(now)
            .await
    }

    pub async fn compaction_claim_history_check(
        &self,
        id: &str,
        revision: i64,
        now: i64,
    ) -> Result<Option<pioneer_entity::compaction_history_check::Model>> {
        self.compaction_store()
            .compaction_claim_history_check(id, revision, now)
            .await
    }
}

// Atomic canonical publication and acknowledgement of compaction lifecycle work.

// The item must use the domain event's time, not the repair/claim time.
// Read the exact idempotency key and prepare JSON/projection outside capacity;
// the writer compares the full immutable event snapshot before applying it.
struct PreparedLifecycleEvent {
    projected: PreparedProjectedTurnEvent,
    existing: Option<turn_event::Model>,
    idempotency_key: String,
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}
impl PreparedLifecycleEvent {
    async fn prepare(
        store: &CrudStore,
        event: CanonicalTurnEventPayload,
        created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<Self> {
        let idempotency_key = event.idempotency_key()?;
        let existing = find_event_model_by_idempotency_key(
            &store.connection,
            event.turn_id(),
            &idempotency_key,
        )
        .await?;
        let created_at = existing.as_ref().map_or(created_at, |e| e.created_at);
        let projected = prepare_projected_turn_event_for_permanent_storage(
            &store.connection,
            event,
            created_at,
        )
        .await?;
        Ok(Self {
            projected,
            existing,
            idempotency_key,
            created_at,
        })
    }

    async fn apply(
        self,
        store: &CrudStore,
        tx: &sea_orm::DatabaseTransaction,
        expires: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<bool> {
        ensure!(
            self.existing
                == find_event_model_by_idempotency_key(
                    tx,
                    self.projected.event.payload().turn_id(),
                    &self.idempotency_key
                )
                .await?,
            "compaction lifecycle event changed after preparation"
        );
        store
            .append_and_project_compaction_event_in_transaction(
                tx,
                self.projected,
                self.created_at,
                expires,
            )
            .await
    }
}

pub(crate) async fn repair_lifecycle(
    store: &CrudStore,
    mut prepared: PreparedCompactionLifecycle,
    event: Option<CanonicalTurnEventPayload>,
    timestamp_secs: i64,
) -> Result<(bool, bool)> {
    ensure!(
        event.is_some() == prepared.needs_publication(),
        "lifecycle publication preparation mismatch"
    );
    let Some(event) = event else {
        return store
            .compaction_store()
            .repair_lifecycle_without_publication(prepared)
            .await;
    };
    let created_at = unix_to_datetime(timestamp_secs);
    let expires = unix_to_datetime(timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS));
    prepared.validate_publication(&event)?;
    let projected = PreparedLifecycleEvent::prepare(store, event, created_at).await?;
    // No operation-wide retry after a potentially committed claim.
    let tx = store.connection.begin().await?;

    let Some(mut changed) = prepared.repair_in_connection(&tx).await? else {
        tx.rollback().await?;
        return Ok((false, false));
    };
    changed |= projected.apply(store, &tx, expires).await?;
    prepared.acknowledge(&tx).await?;
    tx.commit().await?;
    Ok((true, changed))
}

pub(crate) async fn compaction_materialize_lifecycle(
    store: &CrudStore,
    operation: &str,
    generation: u64,
    event: CanonicalTurnEventPayload,
    timestamp_secs: i64,
) -> Result<bool> {
    let admission =
        LifecycleAdmission::prepare(&store.connection, operation, generation, &event).await?;
    let created_at = unix_to_datetime(timestamp_secs);
    let expires = unix_to_datetime(timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS));
    let projected = PreparedLifecycleEvent::prepare(store, event, created_at).await?;
    let tx = store.connection.begin().await?;
    admission.revalidate(&tx).await?;
    let changed = projected.apply(store, &tx, expires).await?;
    admission.acknowledge(&tx).await?;
    tx.commit().await?;
    Ok(changed)
}
