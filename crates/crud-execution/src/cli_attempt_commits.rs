//! Durable CLI attempt transitions in the caller's projection transaction.
use crate::cli_runtime_binding::{self, *};
use crate::cli_runtime_store::{
    cli_runtime_continuation_guard_from_snapshot, cli_runtime_event_guard_from_snapshot,
    cli_runtime_terminal_outcome_pending, cli_runtime_turn_terminal_snapshot,
};
use crate::input_mapping::InputMapping;
use crate::turn_execution::{self, TurnExecutionStatus};
use anyhow::{Context, Result, bail};
use pioneer_crud_access::turn_execution::turn_status_from_db;
use pioneer_crud_conversations::{projection_receipts::has_accepted_terminal, turn_reads as turn};
use pioneer_protocol::{CliRuntimeBlockedTurnGuard, TurnStatus};
use sea_orm::{DatabaseTransaction, prelude::DateTimeWithTimeZone};

type AttemptPair = (CliRuntimeTurnBindingRecord, CliRuntimeTurnAttemptRecord);

pub async fn prepare_initial_attempt(
    transaction: &DatabaseTransaction,
    binding: &NewCliRuntimeTurnBinding,
    attempt_id: &str,
    execution_window_index: u32,
    mapping: InputMapping,
    root: &impl FrozenBindingRoot,
) -> Result<(AttemptPair, bool)> {
    anyhow::ensure!(
        !has_accepted_terminal(transaction, &binding.turn_id).await?,
        "CLI initial attempt cannot activate an accepted terminal Turn"
    );
    let existing_binding =
        cli_runtime_binding::find_turn_binding(transaction, binding.turn_id.as_str()).await?;
    let attempt =
        match cli_runtime_binding::latest_turn_attempt(transaction, binding.turn_id.as_str())
            .await?
        {
            Some(existing)
                if existing.recovery_attempt_id.is_none() && existing.status.is_active() =>
            {
                let Some(stored_binding) = existing_binding else {
                    bail!(
                        "active CLI runtime attempt for turn `{}` has no turn binding",
                        binding.turn_id
                    );
                };
                if stored_binding.thread_id != binding.thread_id
                    || stored_binding.workspace_id != binding.workspace_id
                    || stored_binding.runtime_id != binding.runtime_id
                    || stored_binding.runtime_kind != binding.runtime_kind
                    || stored_binding.native_thread_id != binding.native_thread_id
                {
                    bail!(
                        "turn `{}` already has a different active CLI runtime owner",
                        binding.turn_id
                    );
                }

                return Ok(((stored_binding, existing), false));
            }
            Some(existing) => {
                bail!(
                    "turn `{}` already has CLI runtime attempt {} in status `{}`",
                    binding.turn_id,
                    existing.attempt_index,
                    existing.status.as_str()
                );
            }
            None => {
                if existing_binding.is_some() {
                    bail!(
                        "turn `{}` already has a CLI runtime binding without a durable attempt",
                        binding.turn_id
                    );
                }
                let stored_binding = cli_runtime_binding::upsert_turn_binding(
                    transaction,
                    binding.clone(),
                    mapping.clone(),
                    root,
                )
                .await?;
                cli_runtime_binding::create_turn_attempt(
                    transaction,
                    NewCliRuntimeTurnAttempt {
                        id: attempt_id.to_owned(),
                        turn_id: stored_binding.turn_id.clone(),
                        attempt_index: 1,
                        runtime_id: stored_binding.runtime_id.clone(),
                        runtime_kind: stored_binding.runtime_kind.clone(),
                        native_thread_id: stored_binding.native_thread_id.clone(),
                        native_turn_id: None,
                        recovery_job_id: None,
                        recovery_attempt_id: None,
                        execution_window_index: Some(execution_window_index),
                        status: CliRuntimeTurnAttemptStatus::Starting,
                        failure_reason: None,
                        started_at: None,
                        completed_at: None,
                        created_at: stored_binding.updated_at,
                        updated_at: stored_binding.updated_at,
                    },
                )
                .await?
            }
        };
    let stored_binding =
        cli_runtime_binding::find_turn_binding(transaction, binding.turn_id.as_str())
            .await?
            .context("initial CLI runtime turn binding is missing")?;

    Ok(((stored_binding, attempt), true))
}

pub async fn prepare_task_continuation(
    tx: &DatabaseTransaction,
    source: CliRuntimeBlockedTurnGuard,
    owner_id: String,
    attempt_id: String,
    continuation: NewCliRuntimeNativeEvent,
    ack: NewCliRuntimeNativeEvent,
    source_terminal_id: String,
    expected_ack_id: String,
    expected_continuation_id: String,
) -> Result<(AttemptPair, bool)> {
    crate::execution_reads::lock_cli_turn_binding(tx, &source.turn_id).await?;
    let now = continuation.created_at;
    let binding = cli_runtime_binding::find_turn_binding(tx, &source.turn_id)
        .await?
        .context("task continuation binding disappeared")?;
    let latest = cli_runtime_binding::latest_turn_attempt(tx, &source.turn_id)
        .await?
        .context("task continuation attempt disappeared")?;
    let owner = turn_execution::find(tx, &source.turn_id)
        .await?
        .context("task continuation owner disappeared")?;
    anyhow::ensure!(
        owner.owner_id == owner_id
            && owner
                .executor_kind
                .matches_cli_runtime_kind(&source.runtime_kind)
            && owner.executor_key.as_deref() == Some(source.runtime_id.as_str())
            && owner.status.is_active()
            && owner.lease_until > chrono::Utc::now().fixed_offset(),
        "task continuation execution authority unavailable"
    );
    let turn = turn::find_turn_by_thread_and_id(tx, &binding.thread_id, &source.turn_id)
        .await?
        .context("task continuation Turn disappeared")?;
    anyhow::ensure!(
        turn_status_from_db(&turn.status) == Some(TurnStatus::InProgress)
            && binding.workspace_id == source.workspace_id
            && binding.thread_id == source.thread_id
            && binding.continuation_thread_id == source.continuation_thread_id
            && binding.runtime_id == source.runtime_id
            && binding.runtime_kind == source.runtime_kind
            && binding.native_thread_id == source.native_thread_id,
        "task continuation execution is no longer current"
    );
    if latest.id == attempt_id {
        return Ok(((binding, latest), false));
    }
    let snapshot = cli_runtime_turn_terminal_snapshot(tx, &source.turn_id).await?;
    let current = snapshot
        .as_ref()
        .and_then(|snapshot| cli_runtime_continuation_guard_from_snapshot(snapshot, true));
    anyhow::ensure!(
        current
            .as_ref()
            .is_some_and(|current| current.same_execution(&source)),
        "CLI source changed before task continuation"
    );
    let snapshot = snapshot
        .as_ref()
        .context("task continuation snapshot disappeared")?;
    anyhow::ensure!(
        snapshot.turn_status == TurnStatus::InProgress,
        "task continuation Turn is no longer active"
    );
    anyhow::ensure!(
        latest.status == CliRuntimeTurnAttemptStatus::Completed
            || cli_runtime_terminal_outcome_pending(tx, snapshot, &source_terminal_id).await?,
        "task continuation has no pending accepted terminal outcome"
    );
    anyhow::ensure!(
        latest.id == source.attempt_id,
        "task continuation source attempt changed"
    );
    anyhow::ensure!(
        ack.id == expected_ack_id
            && ack.native_method == "gateway/terminal_ack"
            && continuation.id == expected_continuation_id
            && continuation.turn_id.as_deref() == Some(source.turn_id.as_str()),
        "invalid task continuation identity"
    );
    // The saved terminal outcome remains durable, but this ACK
    // records that its effect is a continuation, never TurnCompleted.
    cli_runtime_binding::append_native_event_if_absent(tx, continuation).await?;
    if cli_runtime_binding::find_native_event(tx, &source_terminal_id)
        .await?
        .is_some()
        && cli_runtime_binding::find_native_event(tx, &expected_ack_id)
            .await?
            .is_none()
    {
        cli_runtime_binding::append_native_event_if_absent(tx, ack).await?;
    }
    if latest.status.is_active() {
        anyhow::ensure!(
            cli_runtime_binding::mark_turn_attempt_terminal(
                tx,
                &latest.id,
                CliRuntimeTurnAttemptStatus::Completed,
                None,
                now
            )
            .await?,
            "source attempt changed"
        );
    } else {
        anyhow::ensure!(
            latest.status == CliRuntimeTurnAttemptStatus::Completed,
            "task continuation requires a completed attempt"
        );
    }
    let next_index = latest
        .attempt_index
        .checked_add(1)
        .context("task continuation attempt overflow")?;
    let attempt = cli_runtime_binding::create_turn_attempt(
        tx,
        NewCliRuntimeTurnAttempt {
            id: attempt_id,
            turn_id: source.turn_id.clone(),
            attempt_index: next_index,
            runtime_id: binding.runtime_id.clone(),
            runtime_kind: binding.runtime_kind.clone(),
            native_thread_id: binding.native_thread_id.clone(),
            native_turn_id: None,
            recovery_job_id: None,
            recovery_attempt_id: None,
            // Orchestration continues the current execution window;
            // waiting for children is not budget exhaustion or failure.
            execution_window_index: latest.execution_window_index,
            status: CliRuntimeTurnAttemptStatus::Starting,
            failure_reason: None,
            started_at: None,
            completed_at: None,
            created_at: now,
            updated_at: now,
        },
    )
    .await?;
    cli_runtime_binding::update_turn_control(
        tx,
        NewCliRuntimeTurnBinding {
            turn_id: binding.turn_id.clone(),
            thread_id: binding.thread_id,
            continuation_thread_id: binding.continuation_thread_id,
            workspace_id: binding.workspace_id,
            runtime_id: binding.runtime_id,
            runtime_kind: binding.runtime_kind,
            native_thread_id: binding.native_thread_id,
            native_turn_id: None,
            request_id: binding.request_id,
            status: "starting".to_owned(),
            model: binding.model,
            cwd: binding.cwd,
            sandbox_json: binding.sandbox_json,
            approval_policy: binding.approval_policy,
            input_mapping_json: binding.input_mapping_json,
            created_at: binding.created_at,
            updated_at: now,
        },
    )
    .await?;
    let binding =
        cli_runtime_binding::clear_turn_native_goal_state(tx, &source.turn_id, now).await?;

    Ok(((binding, attempt), true))
}

pub async fn prepare_recovery_attempt(
    transaction: &DatabaseTransaction,
    turn_id: String,
    attempt_id: String,
    recovery_job_id: String,
    recovery_attempt_id: String,
    execution_window_index: u32,
    previous_failure_reason: String,
    prepared_at: DateTimeWithTimeZone,
    terminal_source: Option<(CliRuntimeBlockedTurnGuard, String, String)>,
    legacy_attempt_id: String,
) -> Result<(AttemptPair, bool)> {
    let Some(mut binding) =
        cli_runtime_binding::find_turn_binding(transaction, turn_id.as_str()).await?
    else {
        bail!("CLI runtime turn binding `{turn_id}` is missing");
    };

    if let Some((expected, id, owner_id)) = terminal_source.as_ref() {
        let snapshot = cli_runtime_turn_terminal_snapshot(transaction, &turn_id).await?;
        let actual = snapshot
            .as_ref()
            .and_then(cli_runtime_event_guard_from_snapshot);
        anyhow::ensure!(
            actual
                .as_ref()
                .is_some_and(|actual| actual.same_execution(expected)),
            "CLI execution changed before recovery preparation"
        );
        if let Some(owner) = turn_execution::find(transaction, &turn_id).await? {
            anyhow::ensure!(
                owner.owner_id == *owner_id
                    && owner.status.is_active()
                    && owner.lease_until > chrono::Utc::now().fixed_offset(),
                "CLI recovery execution authority unavailable"
            );
        }
        anyhow::ensure!(
            !cli_runtime_terminal_outcome_pending(
                transaction,
                snapshot.as_ref().expect("source checked"),
                id,
            )
            .await?,
            "accepted CLI terminal outcome must be delivered before recovery preparation"
        );
    }

    if let Some(existing) = cli_runtime_binding::find_turn_attempt_by_recovery_attempt(
        transaction,
        recovery_attempt_id.as_str(),
    )
    .await?
    {
        let latest =
            cli_runtime_binding::latest_turn_attempt(transaction, turn_id.as_str()).await?;
        if existing.turn_id != turn_id
            || existing.recovery_job_id.as_deref() != Some(recovery_job_id.as_str())
            || existing.execution_window_index != Some(execution_window_index)
            || existing.runtime_id != binding.runtime_id
            || existing.runtime_kind != binding.runtime_kind
            || existing.native_thread_id != binding.native_thread_id
            || latest.as_ref().map(|attempt| attempt.id.as_str()) != Some(existing.id.as_str())
        {
            bail!(
                "recovery attempt `{recovery_attempt_id}` is already bound to a different CLI runtime recovery"
            );
        }

        return Ok(((binding, existing), false));
    }

    let mut latest =
        cli_runtime_binding::latest_turn_attempt(transaction, turn_id.as_str()).await?;
    if latest.is_none()
        && let Some(native_turn_id) = binding.native_turn_id.clone()
    {
        latest = Some(
            cli_runtime_binding::create_turn_attempt(
                transaction,
                NewCliRuntimeTurnAttempt {
                    id: legacy_attempt_id,
                    turn_id: turn_id.clone(),
                    attempt_index: 1,
                    runtime_id: binding.runtime_id.clone(),
                    runtime_kind: binding.runtime_kind.clone(),
                    native_thread_id: binding.native_thread_id.clone(),
                    native_turn_id: Some(native_turn_id),
                    recovery_job_id: None,
                    recovery_attempt_id: None,
                    execution_window_index: execution_window_index
                        .checked_sub(1)
                        .filter(|index| *index > 0),
                    status: CliRuntimeTurnAttemptStatus::Failed,
                    failure_reason: Some(previous_failure_reason.clone()),
                    started_at: Some(binding.updated_at),
                    completed_at: Some(prepared_at),
                    created_at: binding.created_at,
                    updated_at: prepared_at,
                },
            )
            .await?,
        );
    }
    if let Some(active) = latest.as_ref().filter(|attempt| attempt.status.is_active())
        && !cli_runtime_binding::mark_turn_attempt_terminal(
            transaction,
            active.id.as_str(),
            CliRuntimeTurnAttemptStatus::Failed,
            Some(previous_failure_reason.clone()),
            prepared_at,
        )
        .await?
    {
        bail!(
            "CLI runtime turn attempt `{}` changed while recovery was being prepared",
            active.id
        );
    }
    let attempt_index = latest
        .as_ref()
        .map(|attempt| {
            attempt
                .attempt_index
                .checked_add(1)
                .context("CLI runtime turn attempt index overflow")
        })
        .transpose()?
        .unwrap_or(1);
    let attempt = cli_runtime_binding::create_turn_attempt(
        transaction,
        NewCliRuntimeTurnAttempt {
            id: attempt_id.clone(),
            turn_id: turn_id.clone(),
            attempt_index,
            runtime_id: binding.runtime_id.clone(),
            runtime_kind: binding.runtime_kind.clone(),
            native_thread_id: binding.native_thread_id.clone(),
            native_turn_id: None,
            recovery_job_id: Some(recovery_job_id.clone()),
            recovery_attempt_id: Some(recovery_attempt_id.clone()),
            execution_window_index: Some(execution_window_index),
            status: CliRuntimeTurnAttemptStatus::Starting,
            failure_reason: None,
            started_at: None,
            completed_at: None,
            created_at: prepared_at,
            updated_at: prepared_at,
        },
    )
    .await?;

    binding.native_turn_id = None;
    binding.status = "starting".to_owned();
    binding.updated_at = prepared_at;
    let stored_binding = cli_runtime_binding::update_turn_control(
        transaction,
        NewCliRuntimeTurnBinding {
            turn_id: binding.turn_id,
            thread_id: binding.thread_id,
            continuation_thread_id: binding.continuation_thread_id,
            workspace_id: binding.workspace_id,
            runtime_id: binding.runtime_id,
            runtime_kind: binding.runtime_kind,
            native_thread_id: binding.native_thread_id,
            native_turn_id: binding.native_turn_id,
            request_id: binding.request_id,
            status: binding.status,
            model: binding.model,
            cwd: binding.cwd,
            sandbox_json: binding.sandbox_json,
            approval_policy: binding.approval_policy,
            input_mapping_json: binding.input_mapping_json,
            created_at: binding.created_at,
            updated_at: binding.updated_at,
        },
    )
    .await?;
    let stored_binding = cli_runtime_binding::clear_turn_native_goal_state(
        transaction,
        stored_binding.turn_id.as_str(),
        prepared_at,
    )
    .await?;

    Ok(((stored_binding, attempt), true))
}

pub async fn activate_attempt(
    transaction: &DatabaseTransaction,
    turn_id: &str,
    attempt_id: &str,
    native_turn_id: &str,
    request_id: Option<String>,
    started_at: DateTimeWithTimeZone,
    execution_owner_id: Option<&str>,
    execution_lease_until: Option<DateTimeWithTimeZone>,
) -> Result<(AttemptPair, bool, bool, DateTimeWithTimeZone)> {
    let Some(binding) = cli_runtime_binding::find_turn_binding(transaction, turn_id).await? else {
        bail!("CLI runtime turn binding `{turn_id}` is missing");
    };
    let Some(attempt) = cli_runtime_binding::find_turn_attempt(transaction, attempt_id).await?
    else {
        bail!("CLI runtime turn attempt `{attempt_id}` is missing");
    };
    if attempt.turn_id != turn_id || !attempt.status.is_active() {
        bail!("CLI runtime turn attempt `{attempt_id}` cannot own native turn for `{turn_id}`");
    }
    if attempt.runtime_id != binding.runtime_id
        || attempt.runtime_kind != binding.runtime_kind
        || attempt.native_thread_id != binding.native_thread_id
    {
        bail!("CLI runtime turn attempt `{attempt_id}` does not match its turn binding");
    }
    let latest = cli_runtime_binding::latest_turn_attempt(transaction, turn_id)
        .await?
        .context("CLI runtime turn has no latest attempt during activation")?;
    if latest.id != attempt.id {
        bail!("CLI runtime turn attempt `{attempt_id}` is not the latest attempt for `{turn_id}`");
    }
    if attempt.status == CliRuntimeTurnAttemptStatus::Running {
        if attempt.native_turn_id.as_deref() == Some(native_turn_id)
            && binding.native_turn_id.as_deref() == Some(native_turn_id)
            && binding.status == "running"
        {
            let mut owner_changed = false;
            if let Some(owner) = turn_execution::find(transaction, turn_id).await? {
                let owner_id = execution_owner_id
                    .context("durable CLI Turn execution requires an owner during activation")?;
                let lease_until = execution_lease_until
                    .context("durable CLI Turn execution requires a lease during activation")?;
                owner_changed = owner.status != TurnExecutionStatus::Running
                    || owner.started_at != Some(started_at)
                    || owner.heartbeat_at != started_at
                    || owner.lease_until != lease_until;
                if !turn_execution::mark_running_owned(
                    transaction,
                    turn_id,
                    owner_id,
                    started_at,
                    lease_until,
                )
                .await?
                {
                    bail!("CLI Turn execution ownership changed before activation");
                }
            }

            let projection_at = binding.updated_at;
            return Ok(((binding, attempt), owner_changed, false, projection_at));
        }

        bail!(
            "CLI runtime turn attempt `{attempt_id}` is already running with a different native owner"
        );
    }
    if binding.status != "starting" || binding.native_turn_id.is_some() {
        bail!(
            "CLI runtime turn binding `{turn_id}` is not ready to activate attempt `{attempt_id}`"
        );
    }
    if !cli_runtime_binding::mark_turn_attempt_running(
        transaction,
        attempt_id,
        native_turn_id,
        started_at,
    )
    .await?
    {
        bail!("CLI runtime turn attempt `{attempt_id}` is no longer starting");
    }
    let stored_binding = cli_runtime_binding::update_turn_control(
        transaction,
        NewCliRuntimeTurnBinding {
            turn_id: binding.turn_id,
            thread_id: binding.thread_id,
            continuation_thread_id: binding.continuation_thread_id,
            workspace_id: binding.workspace_id,
            runtime_id: binding.runtime_id,
            runtime_kind: binding.runtime_kind,
            native_thread_id: binding.native_thread_id,
            native_turn_id: Some(native_turn_id.to_owned()),
            request_id: request_id.clone().or(binding.request_id),
            status: "running".to_owned(),
            model: binding.model,
            cwd: binding.cwd,
            sandbox_json: binding.sandbox_json,
            approval_policy: binding.approval_policy,
            input_mapping_json: binding.input_mapping_json,
            created_at: binding.created_at,
            updated_at: started_at,
        },
    )
    .await?;
    let stored_attempt = cli_runtime_binding::find_turn_attempt(transaction, attempt_id)
        .await?
        .context("activated CLI runtime turn attempt is missing")?;
    if turn_execution::find(transaction, turn_id).await?.is_some() {
        let owner_id = execution_owner_id
            .as_deref()
            .context("durable CLI Turn execution requires an owner during activation")?;
        let lease_until = execution_lease_until
            .context("durable CLI Turn execution requires a lease during activation")?;
        if !turn_execution::mark_running_owned(
            transaction,
            turn_id,
            owner_id,
            started_at,
            lease_until,
        )
        .await?
        {
            bail!("CLI Turn execution ownership changed before activation");
        }
    }

    Ok(((stored_binding, stored_attempt), true, true, started_at))
}

#[cfg(test)]
#[path = "cli_native_delivery_tests.rs"]
mod tests;
