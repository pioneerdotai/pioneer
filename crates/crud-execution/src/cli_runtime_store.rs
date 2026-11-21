//! Execution-owned CLI control transitions, terminal fences, and logical binding reads.

use crate::cli_runtime_binding::{self, *};
use crate::gateway_control::{GatewayControlWake, control_work_change};
use crate::turn_cli_runtime_instruction::{
    self, CliRuntimeInstructionProjectionRecord, NewCliRuntimeInstructionProjection,
};
use crate::turn_execution::{TurnExecutionRecord, TurnExecutorKind};
use crate::{native_cancellation_context, recovery_job, turn_execution};
use anyhow::{Context, Result, bail};
use pioneer_crud_access::turn_execution::turn_status_from_db;
use pioneer_crud_conversations::projection_receipts::has_accepted_terminal;
use pioneer_protocol::TurnStatus;
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, TransactionTrait,
};
use std::{future::Future, time::Duration};

const DB_ID_LEN: usize = 21;
/// Facts used by terminal projection repair. Captured in one reader snapshot;
/// validated again in the binding/attempt/projection writer transaction.
#[derive(Debug, Clone)]
pub struct CliRuntimeTurnTerminalGuard {
    /// Control guards omit mapping bytes. Ordinary terminal guards retain their
    /// logical read API; both fence the same durable identity/revision.
    pub binding: CliRuntimeTurnBindingRecord,
    pub attempt: Option<CliRuntimeTurnAttemptRecord>,
    pub segment: Option<CliRuntimeExecutionSegmentRecord>,
    pub execution_owner: Option<(String, u64)>,
    pub turn_status: TurnStatus,
    mapping_loaded: bool,
}

impl PartialEq for CliRuntimeTurnTerminalGuard {
    fn eq(&self, other: &Self) -> bool {
        cli_runtime_binding::same_binding_version(&self.binding, &other.binding)
            && self.attempt == other.attempt
            && self.segment == other.segment
            && self.execution_owner == other.execution_owner
            && self.turn_status == other.turn_status
    }
}
impl Eq for CliRuntimeTurnTerminalGuard {}

impl CliRuntimeTurnTerminalGuard {
    pub fn control(
        binding: CliRuntimeTurnBindingRecord,
        attempt: Option<CliRuntimeTurnAttemptRecord>,
        segment: Option<CliRuntimeExecutionSegmentRecord>,
        execution_owner: Option<(String, u64)>,
        turn_status: TurnStatus,
    ) -> Self {
        Self {
            binding,
            attempt,
            segment,
            execution_owner,
            turn_status,
            mapping_loaded: false,
        }
    }

    pub fn has_mapping(&self) -> bool {
        self.mapping_loaded
    }
    pub fn terminal_event_source(&self) -> Option<pioneer_protocol::CliRuntimeBlockedTurnGuard> {
        cli_runtime_event_guard_from_snapshot(self)
    }
    /// Provider events waiting behind admission may observe its own activation.
    /// Metadata/status changes do not replace the selected physical execution.
    pub fn same_execution_after_admission(&self, current: &Self) -> bool {
        let (Some(mut selected), Some(actual)) = (
            cli_runtime_event_guard_from_snapshot(self),
            cli_runtime_event_guard_from_snapshot(current),
        ) else {
            return false;
        };
        if selected.attempt_status == "starting" && selected.attempt_native_turn_id.is_none() {
            selected.binding_native_turn_id = actual.binding_native_turn_id.clone();
            selected.attempt_native_turn_id = actual.attempt_native_turn_id.clone();
            // Activation registers the initial segment for this same attempt.
            if selected.segment.is_none()
                && actual.segment.as_ref().is_none_or(|(_, native, _)| {
                    Some(native) == actual.attempt_native_turn_id.as_ref()
                })
            {
                selected.segment = actual.segment.clone();
            }
        }
        selected.same_execution(&actual)
    }
}

// These typed boundaries run only after repository reads have released reader
// capacity or writer closures have committed. Repository records inside a
// transaction retain the SQL projection and storage provenance.
pub async fn hydrate_cli_binding_optional(
    db: &SqliteDatabase,
    record: Option<CliRuntimeTurnBindingRecord>,
) -> Result<Option<CliRuntimeTurnBindingRecord>> {
    match record {
        Some(record) => Ok(Some(cli_runtime_binding::hydrate_record(db, record).await?)),
        None => Ok(None),
    }
}
pub async fn hydrate_cli_binding_list(
    db: &SqliteDatabase,
    records: Vec<CliRuntimeTurnBindingRecord>,
) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        output.push(cli_runtime_binding::hydrate_record(db, record).await?);
    }
    Ok(output)
}
pub async fn hydrate_cli_binding_pair(
    db: &SqliteDatabase,
    (binding, attempt): (CliRuntimeTurnBindingRecord, CliRuntimeTurnAttemptRecord),
) -> Result<(CliRuntimeTurnBindingRecord, CliRuntimeTurnAttemptRecord)> {
    Ok((
        cli_runtime_binding::hydrate_record(db, binding).await?,
        attempt,
    ))
}
pub async fn hydrate_cli_binding_triple(
    db: &SqliteDatabase,
    (binding, attempt, segment): (
        CliRuntimeTurnBindingRecord,
        CliRuntimeTurnAttemptRecord,
        CliRuntimeExecutionSegmentRecord,
    ),
) -> Result<(
    CliRuntimeTurnBindingRecord,
    CliRuntimeTurnAttemptRecord,
    CliRuntimeExecutionSegmentRecord,
)> {
    Ok((
        cli_runtime_binding::hydrate_record(db, binding).await?,
        attempt,
        segment,
    ))
}
pub async fn hydrate_cli_binding_optional_triple(
    db: &SqliteDatabase,
    record: Option<(
        CliRuntimeTurnBindingRecord,
        CliRuntimeTurnAttemptRecord,
        CliRuntimeExecutionSegmentRecord,
    )>,
) -> Result<
    Option<(
        CliRuntimeTurnBindingRecord,
        CliRuntimeTurnAttemptRecord,
        CliRuntimeExecutionSegmentRecord,
    )>,
> {
    match record {
        Some(record) => Ok(Some(hydrate_cli_binding_triple(db, record).await?)),
        None => Ok(None),
    }
}
pub async fn hydrate_cli_binding_owner(
    db: &SqliteDatabase,
    record: Option<CliRuntimeNativeTurnOwner>,
) -> Result<Option<CliRuntimeNativeTurnOwner>> {
    if let Some(mut owner) = record {
        owner.binding = cli_runtime_binding::hydrate_record(db, owner.binding).await?;
        Ok(Some(owner))
    } else {
        Ok(None)
    }
}

pub async fn cli_runtime_turn_terminal_snapshot<C: ConnectionTrait>(
    connection: &C,
    turn_id: &str,
) -> Result<Option<CliRuntimeTurnTerminalGuard>> {
    let Some(binding) = cli_runtime_binding::find_turn_binding_control(connection, turn_id).await?
    else {
        return Ok(None);
    };
    let Some(status) = pioneer_entity::turn::Entity::find_by_id(turn_id.to_owned())
        .select_only()
        .column(pioneer_entity::turn::Column::Status)
        .filter(pioneer_entity::turn::Column::ThreadId.eq(&binding.thread_id))
        .into_tuple::<String>()
        .one(connection)
        .await?
    else {
        return Ok(None);
    };
    let attempt = cli_runtime_binding::latest_turn_attempt(connection, turn_id).await?;
    let segment = if let Some(attempt) = &attempt {
        cli_runtime_binding::latest_execution_segment_for_attempt(connection, &attempt.id).await?
    } else {
        None
    };
    let execution_owner = turn_execution::find(connection, turn_id)
        .await?
        .map(|execution| (execution.owner_id, execution.owner_generation));
    Ok(Some(CliRuntimeTurnTerminalGuard {
        binding,
        attempt,
        segment,
        execution_owner,
        turn_status: turn_status_from_db(&status).context("unknown canonical Turn status")?,
        mapping_loaded: false,
    }))
}

// Recovery acceptance and invalidation serialize through the same writer.
// Confirmed/Succeeded recovery has already handed authority back to execution.
pub async fn cli_runtime_recovery_authority_is_current<C: ConnectionTrait>(
    connection: &C,
    source: &pioneer_protocol::CliRuntimeBlockedTurnGuard,
) -> Result<bool> {
    if source.recovery_confirmed {
        return Ok(true);
    }
    match (&source.recovery_job_id, &source.recovery_attempt_id) {
        (None, None) => Ok(true),
        (Some(id), Some(attempt_id)) => Ok(recovery_job::find_job_by_id(connection, id)
            .await?
            .is_some_and(|job| {
                job.turn_id == source.turn_id
                    && (job.status == "succeeded"
                        || (job.status == "active"
                            && job.active_attempt_id.as_ref() == Some(attempt_id)))
            })),
        _ => Ok(false),
    }
}

pub async fn cli_runtime_terminal_outcome_pending<C: ConnectionTrait>(
    connection: &C,
    snapshot: &CliRuntimeTurnTerminalGuard,
    id: &str,
) -> Result<bool> {
    if cli_runtime_binding::find_native_event(connection, id)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    let Some(ack) =
        cli_runtime_binding::find_native_event(connection, &format!("{id}:ack")).await?
    else {
        return Ok(true);
    };
    // This ACK atomically transferred the failure obligation to the existing
    // Pending job / terminalization outbox. It is not a completed Goal segment.
    if ack.native_method == "gateway/terminal_recovery_ack" {
        return Ok(false);
    }
    Ok(snapshot
        .segment
        .as_ref()
        .is_some_and(|segment| segment.status == CliRuntimeExecutionSegmentStatus::Completed)
        && snapshot.binding.native_goal_observed_at.is_some()
        && !pioneer_protocol::CliRuntimeBlockedTurnGuard::goal_keeps_turn_open(
            true,
            snapshot.binding.native_goal_status.as_deref(),
        ))
}

pub async fn ack_cli_runtime_recovery_outcome<C: ConnectionTrait>(
    connection: &C,
    source: Option<&pioneer_protocol::CliRuntimeBlockedTurnGuard>,
) -> Result<()> {
    ack_cli_runtime_outcome(connection, source, "gateway/terminal_recovery_ack").await
}

pub async fn ack_cli_runtime_outcome<C: ConnectionTrait>(
    connection: &C,
    source: Option<&pioneer_protocol::CliRuntimeBlockedTurnGuard>,
    method: &str,
) -> Result<()> {
    let Some(id) = source.and_then(|source| source.terminal_delivery_id.as_deref()) else {
        return Ok(());
    };
    let saved = cli_runtime_binding::find_native_event(connection, id)
        .await?
        .context("accepted recovery outcome disappeared")?;
    cli_runtime_binding::append_native_event_if_absent(
        connection,
        NewCliRuntimeNativeEvent {
            id: format!("{id}:ack"),
            runtime_id: saved.runtime_id,
            runtime_kind: saved.runtime_kind,
            workspace_id: saved.workspace_id,
            thread_id: saved.thread_id,
            turn_id: saved.turn_id,
            native_thread_id: saved.native_thread_id,
            native_turn_id: saved.native_turn_id,
            native_method: method.into(),
            payload_redacted_json: "{}".into(),
            sequence: saved.sequence,
            created_at: saved.created_at,
        },
    )
    .await?;
    Ok(())
}

pub async fn cli_runtime_recovery_transition_allowed<C: ConnectionTrait>(
    connection: &C,
    job_id: &str,
    prepared: Option<&pioneer_protocol::CliRuntimeBlockedTurnGuard>,
    accepted: Option<&pioneer_protocol::CliRuntimeBlockedTurnGuard>,
) -> Result<bool> {
    let Some(job) = recovery_job::find_job_by_id(connection, job_id).await? else {
        return Ok(false);
    };
    let snapshot = cli_runtime_turn_terminal_snapshot(connection, &job.turn_id).await?;
    let actual = snapshot
        .as_ref()
        .and_then(cli_runtime_event_guard_from_snapshot);
    match (prepared, actual.as_ref()) {
        (None, None) => return Ok(accepted.is_none()),
        (Some(prepared), Some(actual)) if actual.same_execution(prepared) => {}
        _ => return Ok(false), // a newly admitted execution needs fresh preparation
    }
    let prepared = prepared.expect("source pair checked");
    let actual = actual.as_ref().expect("source pair checked");
    let id = prepared
        .terminal_delivery_id
        .as_deref()
        .context("prepared CLI outcome identity missing")?;
    if let Some(accepted) = accepted {
        if accepted.terminal_delivery_id.as_deref() != Some(id)
            || !actual.same_execution(accepted)
            || actual.recovery_job_id.as_deref() != Some(job_id)
            || actual.recovery_attempt_id != job.active_attempt_id
            || job.status != "active"
            || cli_runtime_binding::find_native_event(connection, id)
                .await?
                .is_none()
        {
            return Ok(false);
        }
        if let Some(owner) = turn_execution::find(connection, &job.turn_id).await?
            && (!owner.status.is_active() || owner.lease_until <= chrono::Utc::now().fixed_offset())
        {
            return Ok(false);
        }
        return Ok(true);
    }
    Ok(!cli_runtime_terminal_outcome_pending(
        connection,
        snapshot.as_ref().expect("source pair checked"),
        id,
    )
    .await?)
}

pub async fn native_recovery_start_transition_allowed<C: ConnectionTrait>(
    db: &C,
    expected: &TurnExecutionRecord,
    recovery: &pioneer_protocol::RecoveryAttemptContext,
    now_unix: i64,
) -> Result<bool> {
    use pioneer_entity::{thread, turn};
    let Some(job) = recovery_job::find_job_by_id(db, &recovery.job_id).await? else {
        return Ok(false);
    };
    if job.turn_id != expected.turn_id
        || job.status != "active"
        || job.active_attempt_id.as_deref() != Some(recovery.attempt_id.as_str())
    {
        return Ok(false);
    }
    let Some(current) = turn_execution::find(db, &expected.turn_id).await? else {
        return Ok(false);
    };
    if current.owner_id != expected.owner_id
        || current.owner_generation != expected.owner_generation
        || current.thread_id != expected.thread_id
        || current.workspace_id != expected.workspace_id
        || current.executor_kind != expected.executor_kind
        || !current.status.is_active()
        || current.lease_until.timestamp() <= now_unix
        || !matches!(
            current.executor_kind,
            TurnExecutorKind::NativeAgent | TurnExecutorKind::ApiProvider
        )
    {
        return Ok(false);
    }
    let turn = turn::Entity::find_by_id(expected.turn_id.clone())
        .filter(turn::Column::ThreadId.eq(expected.thread_id.clone()))
        .filter(turn::Column::Status.eq("in_progress"))
        .select_only()
        .column(turn::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?;
    let thread = thread::Entity::find_by_id(expected.thread_id.clone())
        .filter(thread::Column::WorkspaceId.eq(expected.workspace_id.clone()))
        .select_only()
        .column(thread::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?;
    Ok(turn.is_some()
        && thread.is_some()
        && !has_accepted_terminal(db, &expected.turn_id).await?
        && !native_cancellation_context::has_accepted(db, &expected.turn_id).await?)
}

pub fn cli_runtime_blocked_guard_from_snapshot(
    snapshot: &CliRuntimeTurnTerminalGuard,
) -> Option<pioneer_protocol::CliRuntimeBlockedTurnGuard> {
    if snapshot
        .segment
        .as_ref()
        .is_some_and(|segment| segment.status != CliRuntimeExecutionSegmentStatus::Running)
    {
        return None;
    }
    cli_runtime_event_guard_from_snapshot(snapshot)
}

pub fn cli_runtime_event_guard_from_snapshot(
    snapshot: &CliRuntimeTurnTerminalGuard,
) -> Option<pioneer_protocol::CliRuntimeBlockedTurnGuard> {
    cli_runtime_continuation_guard_from_snapshot(snapshot, false)
}

pub fn cli_runtime_continuation_guard_from_snapshot(
    snapshot: &CliRuntimeTurnTerminalGuard,
    allow_completed: bool,
) -> Option<pioneer_protocol::CliRuntimeBlockedTurnGuard> {
    let binding = &snapshot.binding;
    let attempt = snapshot.attempt.as_ref()?;
    if snapshot.turn_status != TurnStatus::InProgress
        || !(attempt.status.is_active()
            || (allow_completed && attempt.status == CliRuntimeTurnAttemptStatus::Completed))
        || attempt.turn_id != binding.turn_id
        || attempt.runtime_id != binding.runtime_id
        || attempt.runtime_kind != binding.runtime_kind
        || attempt.native_thread_id != binding.native_thread_id
    {
        return None;
    }
    Some(pioneer_protocol::CliRuntimeBlockedTurnGuard {
        terminal_delivery_id: None,
        turn_id: binding.turn_id.clone(),
        thread_id: binding.thread_id.clone(),
        workspace_id: binding.workspace_id.clone(),
        continuation_thread_id: binding.continuation_thread_id.clone(),
        runtime_id: binding.runtime_id.clone(),
        runtime_kind: binding.runtime_kind.clone(),
        native_thread_id: binding.native_thread_id.clone(),
        binding_native_turn_id: binding.native_turn_id.clone(),
        binding_status: binding.status.clone(),
        native_goal_status: binding.native_goal_status.clone(),
        native_goal_turn_id: binding.native_goal_turn_id.clone(),
        attempt_id: attempt.id.clone(),
        attempt_status: attempt.status.as_str().into(),
        attempt_native_turn_id: attempt.native_turn_id.clone(),
        recovery_job_id: attempt.recovery_job_id.clone(),
        recovery_attempt_id: attempt.recovery_attempt_id.clone(),
        recovery_confirmed: attempt.recovery_confirmed_at.is_some(),
        segment: snapshot.segment.as_ref().map(|segment| {
            (
                segment.id.clone(),
                segment.native_turn_id.clone(),
                segment.status.as_str().into(),
            )
        }),
        execution_owner: snapshot.execution_owner.clone(),
    })
}

#[derive(Clone)]
pub struct CliRuntimeStore {
    connection: SqliteDatabase,
}

impl CliRuntimeStore {
    pub fn new(connection: SqliteDatabase) -> Self {
        Self { connection }
    }

    pub fn database_connection(&self) -> SqliteDatabase {
        self.connection.clone()
    }

    pub fn maintenance(&self) -> Self {
        Self::new(self.connection.maintenance())
    }

    async fn retry_operation<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_with_backoff(
            operation,
            is_retryable_database_conflict,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }
    pub async fn insert_cli_runtime_instruction_projection_if_absent(
        &self,
        projection: NewCliRuntimeInstructionProjection,
    ) -> Result<CliRuntimeInstructionProjectionRecord> {
        self.retry_operation(|| async {
            turn_cli_runtime_instruction::insert_cli_runtime_instruction_projection_if_absent(
                &self.connection,
                projection.clone(),
            )
            .await
        })
        .await
    }

    pub async fn get_cli_runtime_instruction_projection(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeInstructionProjectionRecord>> {
        turn_cli_runtime_instruction::find_cli_runtime_instruction_projection(
            &self.connection,
            turn_id,
        )
        .await
    }

    pub async fn verify_claude_provider_session_binding(
        &self,
        thread_id: &str,
        expected_provider_session_id: &str,
        emitted_provider_session_id: Option<&str>,
        process_generation: i64,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        let thread_id = thread_id.to_owned();
        let expected_provider_session_id = expected_provider_session_id.to_owned();
        let emitted_provider_session_id = emitted_provider_session_id.map(str::to_owned);
        self.retry_operation(|| async {
            cli_runtime_binding::verify_claude_provider_session_binding(
                &self.connection,
                thread_id.as_str(),
                expected_provider_session_id.as_str(),
                emitted_provider_session_id.as_deref(),
                process_generation,
            )
            .await
        })
        .await
    }

    pub async fn set_cli_runtime_thread_mcp_metadata(
        &self,
        thread_id: &str,
        metadata: Option<CliRuntimeThreadMcpMetadata>,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        let thread_id = thread_id.to_owned();
        self.retry_operation(|| async {
            cli_runtime_binding::set_thread_mcp_metadata(
                &self.connection,
                thread_id.as_str(),
                metadata.clone(),
            )
            .await
        })
        .await
    }

    pub async fn get_cli_runtime_thread_binding_by_native_thread(
        &self,
        runtime_id: &str,
        native_thread_id: &str,
    ) -> Result<Option<CliRuntimeThreadBindingRecord>> {
        let runtime_id = runtime_id.to_owned();
        let native_thread_id = native_thread_id.to_owned();
        cli_runtime_binding::find_thread_binding_by_native_thread(
            &self.connection,
            runtime_id.as_str(),
            native_thread_id.as_str(),
        )
        .await
    }

    pub async fn list_cli_runtime_thread_bindings_for_runtime(
        &self,
        workspace_id: &str,
        runtime_id: &str,
    ) -> Result<Vec<CliRuntimeThreadBindingRecord>> {
        let workspace_id = workspace_id.to_owned();
        let runtime_id = runtime_id.to_owned();
        cli_runtime_binding::list_thread_bindings_for_runtime(
            &self.connection,
            workspace_id.as_str(),
            runtime_id.as_str(),
        )
        .await
    }

    pub async fn get_cli_runtime_turn_binding(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.retry_operation(|| async {
            let turn_id = turn_id.to_owned();
            let result =
                cli_runtime_binding::find_turn_binding(&self.connection, turn_id.as_str()).await?;
            hydrate_cli_binding_optional(&self.connection, result).await
        })
        .await
    }

    pub async fn has_cli_runtime_dispatch_at_or_after(
        &self,
        source: &CliRuntimeTurnBindingRecord,
    ) -> Result<bool> {
        cli_runtime_binding::has_dispatch_at_or_after(&self.connection, source).await
    }

    pub async fn set_cli_runtime_turn_mcp_metadata(
        &self,
        turn_id: &str,
        metadata: Option<CliRuntimeTurnMcpMetadata>,
    ) -> Result<CliRuntimeTurnBindingRecord> {
        let turn_id = turn_id.to_owned();
        let result = self
            .retry_operation(|| async {
                cli_runtime_binding::set_turn_mcp_metadata(
                    &self.connection,
                    turn_id.as_str(),
                    metadata.clone(),
                )
                .await
            })
            .await?;
        cli_runtime_binding::hydrate_record(&self.connection, result).await
    }

    pub async fn set_cli_runtime_turn_native_goal_state(
        &self,
        turn_id: &str,
        status: Option<String>,
        native_goal_turn_id: Option<String>,
        observed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        expected: Option<CliRuntimeTurnTerminalGuard>,
        execution_owner_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.set_cli_runtime_turn_native_goal_state_projection(
            turn_id,
            status,
            native_goal_turn_id,
            observed_at,
            expected,
            execution_owner_id,
            true,
        )
        .await
    }

    pub async fn set_cli_runtime_turn_native_goal_control_state(
        &self,
        turn_id: &str,
        status: Option<String>,
        native_goal_turn_id: Option<String>,
        observed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        expected: Option<CliRuntimeTurnTerminalGuard>,
        execution_owner_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.set_cli_runtime_turn_native_goal_state_projection(
            turn_id,
            status,
            native_goal_turn_id,
            observed_at,
            expected,
            execution_owner_id,
            false,
        )
        .await
    }

    async fn set_cli_runtime_turn_native_goal_state_projection(
        &self,
        turn_id: &str,
        status: Option<String>,
        native_goal_turn_id: Option<String>,
        observed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        expected: Option<CliRuntimeTurnTerminalGuard>,
        execution_owner_id: &str,
        capture_mapping: bool,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        let control_wakes = vec![
            GatewayControlWake::Turn(turn_id.to_owned()),
            GatewayControlWake::CommandTurn(turn_id.to_owned()),
        ];
        let cli_changes = [
            control_work_change(&self.connection, "gateway.control", &control_wakes),
            control_work_change(&self.connection, "gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            let turn_id = turn_id.to_owned();
            let result = self
                .retry_operation(|| async {
                    let transaction = self.connection.begin().await?;
                    if let Some(expected) = &expected {
                        let current =
                            cli_runtime_turn_terminal_snapshot(&transaction, &turn_id).await?;
                        if let Some(owner) = turn_execution::find(&transaction, &turn_id).await?
                            && (owner.owner_id != execution_owner_id
                                || !owner.status.is_active()
                                || owner.lease_until <= chrono::Utc::now().fixed_offset())
                        {
                            transaction.rollback().await?;
                            return Ok(None);
                        }
                        if !current.as_ref().is_some_and(|current| {
                            expected
                                .terminal_event_source()
                                .zip(current.terminal_event_source())
                                .is_some_and(|(selected, actual)| selected.same_execution(&actual))
                        }) {
                            transaction.rollback().await?;
                            return Ok(None);
                        }
                    }
                    let binding = cli_runtime_binding::set_turn_native_goal_state_projection(
                        &transaction,
                        turn_id.as_str(),
                        status.clone(),
                        native_goal_turn_id.clone(),
                        observed_at,
                        capture_mapping,
                    )
                    .await?;
                    transaction.commit().await?;
                    Ok(Some(binding))
                })
                .await?;
            if capture_mapping {
                hydrate_cli_binding_optional(&self.connection, result).await
            } else {
                Ok(result)
            }
        }
        .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.as_ref().is_ok_and(|changed| changed.is_some()));
            }
        }
        cli_result
    }

    pub async fn get_cli_runtime_turn_binding_by_request(
        &self,
        request_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.retry_operation(|| async {
            let request_id = request_id.to_owned();
            let result = cli_runtime_binding::find_turn_binding_by_request(
                &self.connection,
                request_id.as_str(),
            )
            .await?;
            hydrate_cli_binding_optional(&self.connection, result).await
        })
        .await
    }

    pub async fn get_cli_runtime_turn_binding_by_native_turn(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.retry_operation(|| async {
            let runtime_id = runtime_id.to_owned();
            let native_turn_id = native_turn_id.to_owned();
            let result = cli_runtime_binding::find_turn_binding_by_native_turn(
                &self.connection,
                runtime_id.as_str(),
                native_turn_id.as_str(),
            )
            .await?;
            hydrate_cli_binding_optional(&self.connection, result).await
        })
        .await
    }

    pub async fn list_cli_runtime_turn_bindings_for_thread(
        &self,
        thread_id: &str,
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.retry_operation(|| async {
            let thread_id = thread_id.to_owned();
            let result = cli_runtime_binding::list_turn_bindings_for_thread(
                &self.connection,
                thread_id.as_str(),
            )
            .await?;
            hydrate_cli_binding_list(&self.connection, result).await
        })
        .await
    }

    pub async fn list_active_cli_runtime_turn_binding_page(
        &self,
        status: CliRuntimeActiveTurnBindingStatus,
        after: Option<&(sea_orm::entity::prelude::DateTimeWithTimeZone, String)>,
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.retry_operation(|| async {
            let result =
                cli_runtime_binding::list_active_turn_binding_page(&self.connection, status, after)
                    .await?;
            hydrate_cli_binding_list(&self.connection, result).await
        })
        .await
    }

    pub async fn active_cli_runtime_turn_binding_frontier(
        &self,
        status: CliRuntimeActiveTurnBindingStatus,
    ) -> Result<Option<(sea_orm::entity::prelude::DateTimeWithTimeZone, String)>> {
        cli_runtime_binding::active_turn_binding_frontier(&self.connection, status).await
    }

    pub async fn list_active_cli_runtime_turn_binding_page_through(
        &self,
        status: CliRuntimeActiveTurnBindingStatus,
        after: Option<&(sea_orm::entity::prelude::DateTimeWithTimeZone, String)>,
        through: &(sea_orm::entity::prelude::DateTimeWithTimeZone, String),
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.retry_operation(|| async {
            let result = cli_runtime_binding::list_active_turn_binding_page_through(
                &self.connection,
                status,
                after,
                through,
            )
            .await?;
            hydrate_cli_binding_list(&self.connection, result).await
        })
        .await
    }

    pub async fn list_active_cli_runtime_turn_binding_control_page_through(
        &self,
        status: CliRuntimeActiveTurnBindingStatus,
        after: Option<&(DateTimeWithTimeZone, String)>,
        through: &(DateTimeWithTimeZone, String),
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.retry_operation(|| async {
            cli_runtime_binding::list_active_turn_binding_control_page_through(
                &self.connection,
                status,
                after,
                through,
            )
            .await
        })
        .await
    }

    pub async fn get_cli_runtime_turn_binding_control(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.retry_operation(|| async {
            cli_runtime_binding::find_turn_binding_control(&self.connection, turn_id).await
        })
        .await
    }

    pub async fn list_cli_runtime_turn_bindings(
        &self,
        filter: CliRuntimeTurnBindingListFilter,
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.retry_operation(|| async {
            let filter = filter.clone();
            let result = cli_runtime_binding::list_turn_bindings(&self.connection, filter).await?;
            hydrate_cli_binding_list(&self.connection, result).await
        })
        .await
    }

    pub async fn list_cli_runtime_turn_binding_controls(
        &self,
        filter: CliRuntimeTurnBindingListFilter,
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.retry_operation(|| async {
            cli_runtime_binding::list_turn_binding_controls(&self.connection, filter.clone()).await
        })
        .await
    }

    pub async fn get_cli_runtime_turn_attempt(
        &self,
        id: &str,
    ) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
        cli_runtime_binding::find_turn_attempt(&self.connection, id).await
    }

    pub async fn get_cli_runtime_turn_attempt_by_native_turn(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
        cli_runtime_binding::find_turn_attempt_by_native_turn(
            &self.connection,
            runtime_id,
            native_turn_id,
        )
        .await
    }

    pub async fn get_cli_runtime_turn_attempt_by_recovery_attempt(
        &self,
        recovery_attempt_id: &str,
    ) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
        cli_runtime_binding::find_turn_attempt_by_recovery_attempt(
            &self.connection,
            recovery_attempt_id,
        )
        .await
    }

    pub async fn latest_cli_runtime_turn_attempt(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
        cli_runtime_binding::latest_turn_attempt(&self.connection, turn_id).await
    }

    pub async fn get_cli_runtime_execution_segment_by_native_turn(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
        cli_runtime_binding::find_execution_segment_by_native_turn(
            &self.connection,
            runtime_id,
            native_turn_id,
        )
        .await
    }

    pub async fn resolve_cli_runtime_native_turn_owner(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeNativeTurnOwner>> {
        self.retry_operation(|| async {
            let result = cli_runtime_binding::resolve_native_turn_owner(
                &self.connection,
                runtime_id,
                native_turn_id,
            )
            .await?;
            hydrate_cli_binding_owner(&self.connection, result).await
        })
        .await
    }

    pub async fn resolve_cli_runtime_native_turn_control_owner(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeNativeTurnOwner>> {
        self.retry_operation(|| async {
            cli_runtime_binding::resolve_native_turn_control_owner(
                &self.connection,
                runtime_id,
                native_turn_id,
            )
            .await
        })
        .await
    }

    pub async fn latest_cli_runtime_execution_segment_for_attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
        cli_runtime_binding::latest_execution_segment_for_attempt(&self.connection, attempt_id)
            .await
    }

    pub async fn latest_running_cli_runtime_execution_segment_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
        cli_runtime_binding::latest_running_execution_segment_for_turn(&self.connection, turn_id)
            .await
    }

    pub async fn register_cli_runtime_execution_segment(
        &self,
        turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
        started_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<(
        CliRuntimeTurnBindingRecord,
        CliRuntimeTurnAttemptRecord,
        CliRuntimeExecutionSegmentRecord,
    )> {
        self.register_cli_runtime_execution_segment_projection(
            turn_id,
            native_thread_id,
            native_turn_id,
            started_at,
            true,
        )
        .await
    }

    pub async fn register_cli_runtime_control_execution_segment(
        &self,
        turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
        started_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<(
        CliRuntimeTurnBindingRecord,
        CliRuntimeTurnAttemptRecord,
        CliRuntimeExecutionSegmentRecord,
    )> {
        self.register_cli_runtime_execution_segment_projection(
            turn_id,
            native_thread_id,
            native_turn_id,
            started_at,
            false,
        )
        .await
    }

    async fn register_cli_runtime_execution_segment_projection(
        &self,
        turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
        started_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        capture_mapping: bool,
    ) -> Result<(
        CliRuntimeTurnBindingRecord,
        CliRuntimeTurnAttemptRecord,
        CliRuntimeExecutionSegmentRecord,
    )> {
        let control_wakes = vec![
            GatewayControlWake::Turn(turn_id.to_owned()),
            GatewayControlWake::CommandTurn(turn_id.to_owned()),
        ];
        let cli_changes = [
            control_work_change(&self.connection, "gateway.control", &control_wakes),
            control_work_change(&self.connection, "gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            let turn_id = turn_id.to_owned();
            let native_thread_id = native_thread_id.to_owned();
            let native_turn_id = native_turn_id.to_owned();
            let segment_id = pioneer_protocol::generate_id(DB_ID_LEN);
            let result = self.retry_operation(|| {
            let segment_id = segment_id.clone();
            let turn_id = turn_id.clone();
            let native_thread_id = native_thread_id.clone();
            let native_turn_id = native_turn_id.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin CLI runtime execution segment transaction")?;
                let binding =
                    cli_runtime_binding::find_turn_binding_projection(&transaction, turn_id.as_str(), capture_mapping)
                        .await?
                        .context("CLI runtime turn binding is missing for execution segment")?;
                if binding.status != "running" || binding.native_thread_id != native_thread_id {
                    transaction.rollback().await.ok();
                    bail!("CLI runtime turn binding cannot accept this execution segment");
                }
                let attempt =
                    cli_runtime_binding::latest_turn_attempt(&transaction, turn_id.as_str())
                        .await?
                        .context("CLI runtime turn has no attempt for execution segment")?;
                if !attempt.status.is_active()
                    || attempt.runtime_id != binding.runtime_id
                    || attempt.native_thread_id != native_thread_id
                {
                    transaction.rollback().await.ok();
                    bail!("CLI runtime execution segment has no active owning attempt");
                }

                if let Some(existing) = cli_runtime_binding::find_execution_segment_by_native_turn(
                    &transaction,
                    binding.runtime_id.as_str(),
                    native_turn_id.as_str(),
                )
                .await?
                {
                    if existing.attempt_id != attempt.id
                        || existing.turn_id != binding.turn_id
                        || existing.native_thread_id != native_thread_id
                    {
                        transaction.rollback().await.ok();
                        bail!("native CLI runtime execution segment already has a different owner");
                    }
                    transaction
                        .commit()
                        .await
                        .context("failed to commit idempotent execution segment transaction")?;
                    return Ok((binding, attempt, existing));
                }

                let segment_index = match cli_runtime_binding::latest_execution_segment_for_attempt(
                    &transaction,
                    attempt.id.as_str(),
                )
                .await?
                {
                    Some(previous) => {
                        if previous.status == CliRuntimeExecutionSegmentStatus::Running {
                            transaction.rollback().await.ok();
                            bail!(
                                "CLI runtime attempt already has active execution segment `{}`",
                                previous.native_turn_id
                            );
                        }
                        previous
                            .segment_index
                            .checked_add(1)
                            .context("CLI runtime execution segment index overflow")?
                    }
                    None => 1,
                };
                let segment = cli_runtime_binding::create_execution_segment(
                    &transaction,
                    NewCliRuntimeExecutionSegment {
                        id: segment_id,
                        attempt_id: attempt.id.clone(),
                        turn_id: binding.turn_id.clone(),
                        segment_index,
                        runtime_id: binding.runtime_id.clone(),
                        native_thread_id: native_thread_id.clone(),
                        native_turn_id: native_turn_id.clone(),
                        status: CliRuntimeExecutionSegmentStatus::Running,
                        failure_reason: None,
                        started_at,
                        completed_at: None,
                        created_at: started_at,
                        updated_at: started_at,
                    },
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit CLI runtime execution segment transaction")?;
                Ok((binding, attempt, segment))
            }
        })
        .await?;
            if capture_mapping { hydrate_cli_binding_triple(&self.connection, result).await } else { Ok(result) }
        }
        .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.is_ok());
            }
        }
        cli_result
    }

    pub async fn terminalize_cli_runtime_execution_segment(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
        status: CliRuntimeExecutionSegmentStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<
        Option<(
            CliRuntimeTurnBindingRecord,
            CliRuntimeTurnAttemptRecord,
            CliRuntimeExecutionSegmentRecord,
        )>,
    > {
        self.terminalize_cli_runtime_execution_segment_projection(
            runtime_id,
            native_turn_id,
            status,
            failure_reason,
            completed_at,
            true,
        )
        .await
    }

    pub async fn terminalize_cli_runtime_control_execution_segment(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
        status: CliRuntimeExecutionSegmentStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<
        Option<(
            CliRuntimeTurnBindingRecord,
            CliRuntimeTurnAttemptRecord,
            CliRuntimeExecutionSegmentRecord,
        )>,
    > {
        self.terminalize_cli_runtime_execution_segment_projection(
            runtime_id,
            native_turn_id,
            status,
            failure_reason,
            completed_at,
            false,
        )
        .await
    }

    async fn terminalize_cli_runtime_execution_segment_projection(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
        status: CliRuntimeExecutionSegmentStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        capture_mapping: bool,
    ) -> Result<
        Option<(
            CliRuntimeTurnBindingRecord,
            CliRuntimeTurnAttemptRecord,
            CliRuntimeExecutionSegmentRecord,
        )>,
    > {
        let control_wakes = vec![GatewayControlWake::Native(
            runtime_id.to_owned(),
            native_turn_id.to_owned(),
        )];
        let changes = [
            "gateway.control",
            "gateway.cli",
            "tasks.lifecycle",
            "maintenance.native",
        ]
        .map(|domain| control_work_change(&self.connection, domain, &control_wakes));
        let result = async {
            let runtime_id = runtime_id.to_owned();
            let native_turn_id = native_turn_id.to_owned();
            self.retry_operation(|| async {
                let transaction = self.connection.begin().await.context(
                    "failed to begin CLI runtime execution segment terminal transaction",
                )?;
                let Some(segment) = cli_runtime_binding::find_execution_segment_by_native_turn(
                    &transaction,
                    runtime_id.as_str(),
                    native_turn_id.as_str(),
                )
                .await?
                else {
                    transaction.rollback().await.ok();
                    return Ok((None, false));
                };
                let binding = cli_runtime_binding::find_turn_binding_projection(
                    &transaction,
                    segment.turn_id.as_str(),
                    capture_mapping,
                )
                .await?
                .context("execution segment turn binding is missing")?;
                let attempt = cli_runtime_binding::find_turn_attempt(
                    &transaction,
                    segment.attempt_id.as_str(),
                )
                .await?
                .context("execution segment owning attempt is missing")?;
                if attempt.id != segment.attempt_id || !attempt.status.is_active() {
                    transaction.rollback().await.ok();
                    bail!("execution segment does not belong to the active CLI runtime attempt");
                }
                let changed = segment.status == CliRuntimeExecutionSegmentStatus::Running;
                if changed {
                    if !cli_runtime_binding::mark_execution_segment_terminal(
                        &transaction,
                        segment.id.as_str(),
                        status,
                        failure_reason.clone(),
                        completed_at,
                    )
                    .await?
                    {
                        transaction.rollback().await.ok();
                        bail!("CLI runtime execution segment changed during terminalization");
                    }
                } else if segment.status != status {
                    transaction.rollback().await.ok();
                    bail!("CLI runtime execution segment received conflicting terminal states");
                }
                let stored =
                    cli_runtime_binding::find_execution_segment(&transaction, segment.id.as_str())
                        .await?
                        .context("terminalized CLI runtime execution segment is missing")?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit execution segment terminal transaction")?;
                Ok((Some((binding, attempt, stored)), changed))
            })
            .await
        }
        .await;
        if let Ok((_, changed)) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        let (binding, _) = result?;
        if capture_mapping {
            hydrate_cli_binding_optional_triple(&self.connection, binding).await
        } else {
            Ok(binding)
        }
    }

    pub async fn mark_cli_runtime_turn_attempt_terminal(
        &self,
        id: &str,
        status: CliRuntimeTurnAttemptStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<bool> {
        let control_wakes = vec![GatewayControlWake::Attempt(id.to_owned())];
        let changes = [
            "gateway.control",
            "gateway.cli",
            "maintenance.native",
            "tasks.lifecycle",
        ]
        .map(|domain| control_work_change(&self.connection, domain, &control_wakes));
        let result = async {
            self.retry_operation(|| async {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin CLI runtime attempt terminal transaction")?;
                let changed = cli_runtime_binding::mark_turn_attempt_terminal(
                    &transaction,
                    id,
                    status,
                    failure_reason.clone(),
                    completed_at,
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit CLI runtime attempt terminal transaction")?;
                Ok(changed)
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            let changed = *value;
            for change in changes {
                change.completed(changed);
            }
        }
        result
    }

    pub async fn mark_cli_runtime_turn_attempt_recovery_confirmed(
        &self,
        id: &str,
        recovery_attempt_id: &str,
        confirmed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<bool> {
        let control_wakes = vec![GatewayControlWake::Attempt(id.to_owned())];
        let changes = ["gateway.control", "gateway.cli", "maintenance.native"]
            .map(|domain| control_work_change(&self.connection, domain, &control_wakes));
        let result = async {
            self.retry_operation(|| async {
                cli_runtime_binding::mark_turn_attempt_recovery_confirmed(
                    &self.connection,
                    id,
                    recovery_attempt_id,
                    confirmed_at,
                )
                .await
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            let changed = *value;
            for change in changes {
                change.completed(changed);
            }
        }
        result
    }

    pub async fn cli_runtime_turn_terminal_guard(
        &self,
        expected_binding: &CliRuntimeTurnBindingRecord,
    ) -> Result<Option<CliRuntimeTurnTerminalGuard>> {
        Ok(self
            .cli_runtime_turn_terminal_guard_by_id(&expected_binding.turn_id)
            .await?
            .filter(|snapshot| {
                cli_runtime_binding::same_binding_version(&snapshot.binding, expected_binding)
                    && snapshot.binding.input_mapping_json == expected_binding.input_mapping_json
            }))
    }

    pub async fn cli_runtime_turn_terminal_guard_by_id(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnTerminalGuard>> {
        self.retry_operation(|| async {
            let snapshot = self.connection.begin_read().await?;
            let mut result = cli_runtime_turn_terminal_snapshot(&snapshot, turn_id).await?;
            // This API promises logical JSON. Capture its full source in the same
            // read snapshot, then decode only after releasing reader capacity.
            if let Some(guard) = result.as_mut() {
                guard.binding = cli_runtime_binding::find_turn_binding(&snapshot, turn_id)
                    .await?
                    .context("CLI terminal binding disappeared inside its snapshot")?;
            }
            snapshot.commit().await?;
            if let Some(mut guard) = result {
                guard.binding =
                    cli_runtime_binding::hydrate_record(&self.connection, guard.binding).await?;
                guard.mapping_loaded = true;
                Ok(Some(guard))
            } else {
                Ok(None)
            }
        })
        .await
    }

    pub async fn cli_runtime_turn_terminal_control_guard_by_id(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnTerminalGuard>> {
        self.retry_operation(|| async {
            let snapshot = self.connection.begin_read().await?;
            let result = cli_runtime_turn_terminal_snapshot(&snapshot, turn_id).await?;
            snapshot.commit().await?;
            Ok(result)
        })
        .await
    }

    pub async fn cli_runtime_turn_terminal_control_guard(
        &self,
        expected: &CliRuntimeTurnBindingRecord,
    ) -> Result<Option<CliRuntimeTurnTerminalGuard>> {
        Ok(self
            .cli_runtime_turn_terminal_control_guard_by_id(&expected.turn_id)
            .await?
            .filter(|guard| cli_runtime_binding::same_binding_version(&guard.binding, expected)))
    }

    pub async fn load_cli_runtime_control_mapping(
        &self,
        expected: &CliRuntimeTurnBindingRecord,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        Ok(self
            .get_cli_runtime_turn_binding(&expected.turn_id)
            .await?
            .filter(|binding| cli_runtime_binding::same_binding_version(binding, expected)))
    }

    pub async fn cli_runtime_completed_task_continuation_source(
        &self,
        binding: &CliRuntimeTurnBindingRecord,
    ) -> Result<Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>> {
        Ok(self
            .cli_runtime_turn_terminal_control_guard(binding)
            .await?
            .as_ref()
            .filter(|snapshot| {
                snapshot.attempt.as_ref().is_some_and(|attempt| {
                    attempt.status == CliRuntimeTurnAttemptStatus::Completed
                        && attempt.native_turn_id.is_some()
                        && attempt.native_turn_id == snapshot.binding.native_turn_id
                })
            })
            .and_then(|snapshot| cli_runtime_continuation_guard_from_snapshot(snapshot, true)))
    }

    pub async fn cli_runtime_terminal_event_source(
        &self,
        binding: &CliRuntimeTurnBindingRecord,
        native_turn_id: &str,
    ) -> Result<Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>> {
        Ok(self
            .cli_runtime_turn_terminal_control_guard(binding)
            .await?
            .as_ref()
            .and_then(cli_runtime_event_guard_from_snapshot)
            .filter(|source| {
                source
                    .segment
                    .as_ref()
                    .map(|(_, native, _)| native.as_str())
                    .or(source.attempt_native_turn_id.as_deref())
                    == Some(native_turn_id)
            }))
    }
}

impl CliRuntimeStore {
    pub async fn has_pending_cli_runtime_terminal_event(&self, turn_id: &str) -> Result<bool> {
        let Some(snapshot) = self
            .cli_runtime_turn_terminal_control_guard_by_id(turn_id)
            .await?
        else {
            return Ok(false);
        };
        let Some(source) = cli_runtime_event_guard_from_snapshot(&snapshot) else {
            return Ok(false);
        };
        let id = source.terminal_delivery_id();
        cli_runtime_terminal_outcome_pending(&self.connection, &snapshot, &id).await
    }

    pub async fn authorize_cli_runtime_terminal_event(
        &self,
        record: &CliRuntimeNativeEventRecord,
        accepted: &pioneer_protocol::CliRuntimeBlockedTurnGuard,
        expected: &pioneer_protocol::CliRuntimeBlockedTurnGuard,
        owner_id: &str,
    ) -> Result<Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>> {
        self.retry_operation(|| async {
            let transaction = self.connection.begin().await?;
            let stored = cli_runtime_binding::find_native_event(&transaction, &record.id).await?;
            let snapshot =
                cli_runtime_turn_terminal_snapshot(&transaction, &accepted.turn_id).await?;
            let actual = snapshot
                .as_ref()
                .and_then(cli_runtime_event_guard_from_snapshot);
            let owner = turn_execution::find(&transaction, &accepted.turn_id).await?;
            let owns = match &owner {
                Some(owner) => {
                    owner.owner_id == owner_id
                        && owner.status.is_active()
                        && owner.executor_kind == TurnExecutorKind::CliRuntime
                        && owner.executor_key.as_deref() == Some(expected.runtime_id.as_str())
                        && owner.lease_until > chrono::Utc::now().fixed_offset()
                }
                None => accepted.execution_owner.is_none() && expected.execution_owner.is_none(),
            };
            if !owns
                || stored.as_ref() != Some(record)
                || record.native_method != "gateway/terminal_delivery"
                || !actual.as_ref().is_some_and(|actual| {
                    actual.same_execution(expected) && actual.same_native_execution(accepted)
                })
            {
                transaction.rollback().await?;
                return Ok(None);
            }
            if !cli_runtime_recovery_authority_is_current(
                &transaction,
                actual.as_ref().expect("source checked"),
            )
            .await?
            {
                transaction.rollback().await?;
                return Ok(None);
            }
            transaction.commit().await?;
            Ok(actual)
        })
        .await
    }

    pub async fn persist_cli_runtime_terminal_event(
        &self,
        event: NewCliRuntimeNativeEvent,
        expected: &pioneer_protocol::CliRuntimeBlockedTurnGuard,
        owner_id: &str,
    ) -> Result<bool> {
        let control_wakes = event
            .turn_id
            .as_ref()
            .map(|id| vec![GatewayControlWake::Turn(id.clone())])
            .unwrap_or_default();
        let changes = ["gateway.control", "gateway.cli", "maintenance.native"]
            .map(|domain| control_work_change(&self.connection, domain, &control_wakes));
        let result = async {
            anyhow::ensure!(
                event.runtime_id == expected.runtime_id
                    && event.runtime_kind == expected.runtime_kind
                    && event.turn_id.as_deref() == Some(expected.turn_id.as_str())
                    && event.thread_id.as_deref() == Some(expected.thread_id.as_str())
                    && event.workspace_id.as_deref() == Some(expected.workspace_id.as_str())
                    && event.native_thread_id.as_deref()
                        == Some(expected.native_thread_id.as_str())
                    && event.native_turn_id.as_deref()
                        == expected
                            .segment
                            .as_ref()
                            .map(|(_, native, _)| native.as_str())
                            .or(expected.attempt_native_turn_id.as_deref())
                    && event.native_method == "gateway/terminal_delivery",
                "native terminal delivery does not match its selected source"
            );
            self.retry_operation(|| async {
                let transaction = self.connection.begin().await?;
                if let Some(owner) = turn_execution::find(&transaction, &expected.turn_id).await?
                    && (owner.owner_id != owner_id
                        || !owner.status.is_active()
                        || owner.lease_until <= chrono::Utc::now().fixed_offset())
                {
                    transaction.rollback().await?;
                    return Ok((false, false));
                }
                let Some(binding) =
                    cli_runtime_binding::find_turn_binding(&transaction, &expected.turn_id).await?
                else {
                    transaction.rollback().await?;
                    return Ok((false, false));
                };
                let turn = pioneer_crud_conversations::turn_reads::find_turn_by_thread_and_id(
                    &transaction,
                    &expected.thread_id,
                    &expected.turn_id,
                )
                .await?;
                let attempt =
                    cli_runtime_binding::latest_turn_attempt(&transaction, &expected.turn_id)
                        .await?;
                let segment = if let Some(attempt) = attempt.as_ref() {
                    cli_runtime_binding::latest_execution_segment_for_attempt(
                        &transaction,
                        &attempt.id,
                    )
                    .await?
                } else {
                    None
                };
                let execution_owner = turn_execution::find(&transaction, &expected.turn_id)
                    .await?
                    .map(|owner| (owner.owner_id, owner.owner_generation));
                let actual = if let Some(turn) = turn {
                    Some(CliRuntimeTurnTerminalGuard::control(
                        binding,
                        attempt,
                        segment,
                        execution_owner,
                        turn_status_from_db(&turn.status)
                            .context("unknown canonical Turn status")?,
                    ))
                } else {
                    None
                };
                let actual = actual
                    .as_ref()
                    .and_then(cli_runtime_event_guard_from_snapshot);
                if !actual
                    .as_ref()
                    .is_some_and(|actual| actual.same_execution(expected))
                {
                    transaction.rollback().await?;
                    return Ok((false, false));
                }
                if !cli_runtime_recovery_authority_is_current(
                    &transaction,
                    actual.as_ref().expect("source checked"),
                )
                .await?
                {
                    transaction.rollback().await?;
                    return Ok((false, false));
                }
                let inserted = if cli_runtime_binding::find_native_event(&transaction, &event.id)
                    .await?
                    .is_none()
                {
                    cli_runtime_binding::append_native_event_if_absent(&transaction, event.clone())
                        .await?;
                    true
                } else {
                    false
                };
                transaction.commit().await?;
                Ok((true, inserted))
            })
            .await
        }
        .await;
        if let Ok((_, changed)) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result.map(|(accepted, _)| accepted)
    }
}

impl CliRuntimeStore {
    pub async fn open_native_human_interaction_request_with_authorization(
        &self,
        request: NewCliRuntimePendingRequest,
        authorization: CliRuntimeRequestAuthorizationBinding,
        max_pending_requests: usize,
    ) -> Result<CliRuntimePendingRequestRecord> {
        self.retry_operation(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin native human interaction transaction")?;
            let turn_id = request
                .turn_id
                .as_deref()
                .context("authorized native human interaction is missing its execution turn")?;
            cli_runtime_binding::ensure_pending_request_capacity(
                &transaction,
                request.request_id.as_str(),
                turn_id,
                max_pending_requests,
            )
            .await?;
            let opened =
                cli_runtime_binding::open_pending_request(&transaction, request.clone()).await?;
            let record = if opened.status == CliRuntimePendingRequestStatus::Pending {
                cli_runtime_binding::bind_pending_request_authorization(
                    &transaction,
                    opened.request_id.as_str(),
                    &authorization,
                )
                .await?
            } else {
                // A recovered native executor reopens the same deterministic
                // request after the response may already have been accepted.
                // Preserve that durable delivery state, but only when it is
                // still bound to the exact initiating authority.
                if opened.authorization_binding.as_ref() != Some(&authorization) {
                    return Err(anyhow::anyhow!(
                        "native human interaction request is bound to different initiating authority"
                    ));
                }
                opened
            };
            transaction
                .commit()
                .await
                .context("failed to commit native human interaction")?;
            Ok(record)
        })
        .await
    }

    pub async fn resolve_native_human_interaction_request(
        &self,
        resolution: ResolveCliRuntimePendingRequest,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        self.retry_operation(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin native human interaction resolution")?;
            let record =
                cli_runtime_binding::resolve_pending_request(&transaction, resolution.clone())
                    .await?;
            transaction
                .commit()
                .await
                .context("failed to commit native human interaction resolution")?;
            Ok(record)
        })
        .await
    }

    pub async fn accept_native_human_interaction_response(
        &self,
        response: AcceptCliRuntimePendingRequestResponse,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        self.retry_operation(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin native human interaction acceptance")?;
            let record = cli_runtime_binding::accept_pending_request_response(
                &transaction,
                response.clone(),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit native human interaction acceptance")?;
            Ok(record)
        })
        .await
    }

    pub async fn transition_native_human_interaction_delivery(
        &self,
        transition: TransitionCliRuntimePendingRequestDelivery,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        self.retry_operation(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin native human interaction delivery transition")?;
            let record = cli_runtime_binding::transition_pending_request_delivery(
                &transaction,
                transition.clone(),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit native human interaction delivery transition")?;
            Ok(record)
        })
        .await
    }
}
