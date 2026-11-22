use crate::DB_ID_LEN;
use crate::events::TurnEventPayload;
use crate::store::events::event_preparation::{
    TURN_EVENT_PROJECTION_LEASE_SECS, TurnEventProjectionContext,
    prepare_projected_turn_event_from_envelope, prepare_turn_event_envelope_for_permanent_storage,
    serialize_turn_event_projection_context,
};
use crate::{
    CliRuntimeNativeEventRecord, CliRuntimeTurnAttemptRecord, CliRuntimeTurnAttemptStatus,
    CliRuntimeTurnBindingRecord, CliRuntimeTurnTerminalGuard, CrudStore, GatewayControlWake,
    NewCliRuntimeNativeEvent, NewCliRuntimeTurnBinding, compaction, terminal_cleanup,
};
use anyhow::{Context, Result, bail};
use pioneer_crud_access::turn_execution::turn_status_from_db;
use pioneer_crud_compaction::frozen_lifetime;
use pioneer_crud_conversations::turn_reads;
use pioneer_crud_conversations::values::turn_status_to_db;
use pioneer_crud_execution::cli_runtime_store::{
    ack_cli_runtime_outcome, cli_runtime_blocked_guard_from_snapshot,
    cli_runtime_event_guard_from_snapshot, cli_runtime_terminal_outcome_pending,
    hydrate_cli_binding_optional, hydrate_cli_binding_pair,
};
use pioneer_crud_execution::event_storage_validation::validate_turn_event_execution_owner;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_execution::{cli_runtime_binding, recovery_job, turn_execution};
use pioneer_crud_timeline::timeline_live_projection::project_cli_runtime_turn_binding_state;
use pioneer_protocol::{Task, Turn, TurnStatus, generate_id};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, TransactionTrait};

impl CrudStore {
    pub async fn prepare_cli_runtime_initial_turn_attempt(
        &self,
        binding: NewCliRuntimeTurnBinding,
        attempt_id: String,
        execution_window_index: u32,
    ) -> Result<(CliRuntimeTurnBindingRecord, CliRuntimeTurnAttemptRecord)> {
        let control_wakes = vec![
            GatewayControlWake::Turn(binding.turn_id.clone()),
            GatewayControlWake::CommandTurn(binding.turn_id.clone()),
        ];
        let html_change = self.connection.work_signal("maintenance.html").changing();
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let changes = ["gateway.control", "gateway.cli", "maintenance.portable"]
            .map(|domain| self.control_work_change(domain, &control_wakes));
        let result = async {
            let root =
                frozen_lifetime::PreparedFrozenRoot::cli(&binding.input_mapping_json, false)?;
            let mapping = pioneer_crud_execution::input_mapping::InputMapping::prepare(
                &binding.input_mapping_json,
                root.locator().is_some(),
            )?;
            self.retry_write_on_conflict(|| async {
                let prepared_binding = binding.clone();
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin CLI runtime initial attempt transaction")?;

                let ((stored_binding, attempt), inserted) =
                    pioneer_crud_execution::cli_attempt_commits::prepare_initial_attempt(
                        &transaction,
                        &binding,
                        &attempt_id,
                        execution_window_index,
                        mapping.clone(),
                        &root,
                    )
                    .await?;
                if inserted {
                    project_cli_runtime_turn_binding_state(
                        &*transaction,
                        &stored_binding.turn_id,
                        stored_binding.updated_at,
                    )
                    .await?;
                }
                transaction.commit().await.context(if inserted {
                    "failed to commit CLI runtime initial attempt transaction"
                } else {
                    "failed to commit idempotent CLI runtime initial attempt"
                })?;
                Ok(((stored_binding, attempt), inserted))
            })
            .await
        }
        .await;
        if let Ok((_, inserted)) = &result {
            html_change.completed(
                *inserted
                    && frozen_lifetime::html_cli_root_changed(
                        None,
                        &binding.workspace_id,
                        &binding.input_mapping_json,
                        false,
                    ),
            );
            let keys = frozen_lifetime::PreparedFrozenRoot::cli(&binding.input_mapping_json, false)
                .ok()
                .map(|root| {
                    root.locator()
                        .into_iter()
                        .map(|id| format!("m:{id}"))
                        .collect::<Vec<_>>()
                });
            frozen_lifetime::complete_keys(frozen_change, &keys, *inserted);
            for change in changes {
                change.completed(*inserted);
            }
        }
        let (binding, _) = result?;
        hydrate_cli_binding_pair(&self.connection, binding).await
    }

    /// Transfer a completed native attempt to task orchestration without
    /// completing the logical Turn. The next attempt and source ACK are atomic.
    /// All input serialization and task snapshot work happens before this call.
    pub async fn prepare_cli_runtime_task_continuation(
        &self,
        source: pioneer_protocol::CliRuntimeBlockedTurnGuard,
        owner_id: String,
        attempt_id: String,
        continuation: NewCliRuntimeNativeEvent,
        ack: NewCliRuntimeNativeEvent,
    ) -> Result<(CliRuntimeTurnBindingRecord, CliRuntimeTurnAttemptRecord)> {
        let control_wakes = vec![
            GatewayControlWake::Turn(source.turn_id.clone()),
            GatewayControlWake::CommandTurn(source.turn_id.clone()),
        ];
        let cli_changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            let source_terminal_id = source.terminal_delivery_id();
            let expected_ack_id = format!("{source_terminal_id}:ack");
            let expected_continuation_id = format!("{attempt_id}:task-continuation");
            let result = self
                .retry_write_on_conflict(|| {
                    let source = source.clone();
                    let source_terminal_id = source_terminal_id.clone();
                    let expected_ack_id = expected_ack_id.clone();
                    let expected_continuation_id = expected_continuation_id.clone();
                    let owner_id = owner_id.clone();
                    let attempt_id = attempt_id.clone();
                    let continuation = continuation.clone();
                    let ack = ack.clone();
                    async move {
                        let tx = self.connection.begin().await?;

                        let (binding, inserted) =
                            pioneer_crud_execution::cli_attempt_commits::prepare_task_continuation(
                                &tx,
                                source,
                                owner_id,
                                attempt_id,
                                continuation,
                                ack,
                                source_terminal_id,
                                expected_ack_id,
                                expected_continuation_id,
                            )
                            .await?;
                        if inserted {
                            project_cli_runtime_turn_binding_state(
                                &*tx,
                                &binding.0.turn_id,
                                binding.0.updated_at,
                            )
                            .await?;
                        }
                        tx.commit().await?;
                        Ok(binding)
                    }
                })
                .await?;
            hydrate_cli_binding_pair(&self.connection, result).await
        }
        .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.is_ok());
            }
        }
        cli_result
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_cli_runtime_recovery_turn_attempt(
        &self,
        turn_id: &str,
        attempt_id: String,
        recovery_job_id: String,
        recovery_attempt_id: String,
        execution_window_index: u32,
        previous_failure_reason: String,
        prepared_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        terminal_source: Option<(pioneer_protocol::CliRuntimeBlockedTurnGuard, String, String)>,
    ) -> Result<(CliRuntimeTurnBindingRecord, CliRuntimeTurnAttemptRecord)> {
        let control_wakes = vec![
            GatewayControlWake::Turn(turn_id.to_owned()),
            GatewayControlWake::CommandTurn(turn_id.to_owned()),
        ];
        let cli_changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            if execution_window_index == 0 {
                bail!("CLI runtime recovery execution window index must be positive");
            }
            let turn_id = turn_id.to_owned();
            let legacy_attempt_id = pioneer_protocol::generate_id(DB_ID_LEN);
            let result = self
                .retry_write_on_conflict(|| {
                    let legacy_attempt_id = legacy_attempt_id.clone();
                    let turn_id = turn_id.clone();
                    let attempt_id = attempt_id.clone();
                    let recovery_job_id = recovery_job_id.clone();
                    let recovery_attempt_id = recovery_attempt_id.clone();
                    let previous_failure_reason = previous_failure_reason.clone();
                    let terminal_source = terminal_source.clone();
                    async move {
                        let transaction =
                            self.connection.begin().await.context(
                                "failed to begin CLI runtime recovery attempt transaction",
                            )?;

                        let (binding, inserted) =
                            pioneer_crud_execution::cli_attempt_commits::prepare_recovery_attempt(
                                &transaction,
                                turn_id,
                                attempt_id,
                                recovery_job_id,
                                recovery_attempt_id,
                                execution_window_index,
                                previous_failure_reason,
                                prepared_at,
                                terminal_source,
                                legacy_attempt_id,
                            )
                            .await?;
                        if inserted {
                            project_cli_runtime_turn_binding_state(
                                &*transaction,
                                &binding.0.turn_id,
                                prepared_at,
                            )
                            .await?;
                        }
                        transaction.commit().await.context(if inserted {
                            "failed to commit CLI runtime recovery attempt transaction"
                        } else {
                            "failed to commit idempotent CLI runtime recovery attempt"
                        })?;
                        Ok(binding)
                    }
                })
                .await?;
            hydrate_cli_binding_pair(&self.connection, result).await
        }
        .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.is_ok());
            }
        }
        cli_result
    }

    pub async fn activate_cli_runtime_turn_attempt(
        &self,
        turn_id: &str,
        attempt_id: &str,
        native_turn_id: &str,
        request_id: Option<String>,
        started_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<(CliRuntimeTurnBindingRecord, CliRuntimeTurnAttemptRecord)> {
        self.activate_cli_runtime_turn_attempt_with_owner(
            turn_id,
            attempt_id,
            native_turn_id,
            request_id,
            started_at,
            None,
            None,
        )
        .await
    }

    pub async fn activate_cli_runtime_turn_attempt_owned(
        &self,
        turn_id: &str,
        attempt_id: &str,
        native_turn_id: &str,
        request_id: Option<String>,
        started_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        execution_owner_id: &str,
        execution_lease_until: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<(CliRuntimeTurnBindingRecord, CliRuntimeTurnAttemptRecord)> {
        self.activate_cli_runtime_turn_attempt_with_owner(
            turn_id,
            attempt_id,
            native_turn_id,
            request_id,
            started_at,
            Some(execution_owner_id),
            Some(execution_lease_until),
        )
        .await
    }

    async fn activate_cli_runtime_turn_attempt_with_owner(
        &self,
        turn_id: &str,
        attempt_id: &str,
        native_turn_id: &str,
        request_id: Option<String>,
        started_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        execution_owner_id: Option<&str>,
        execution_lease_until: Option<sea_orm::entity::prelude::DateTimeWithTimeZone>,
    ) -> Result<(CliRuntimeTurnBindingRecord, CliRuntimeTurnAttemptRecord)> {
        let control_wakes = vec![
            GatewayControlWake::Turn(turn_id.to_owned()),
            GatewayControlWake::CommandTurn(turn_id.to_owned()),
        ];
        let changes = ["gateway.control", "gateway.cli", "maintenance.native"]
            .map(|domain| self.control_work_change(domain, &control_wakes));
        let task_change = self.connection.work_signal("tasks.lifecycle").changing();
        let turn_id = turn_id.to_owned();
        let attempt_id = attempt_id.to_owned();
        let native_turn_id = native_turn_id.to_owned();
        let execution_owner_id = execution_owner_id.map(str::to_owned);
        let result = self
            .retry_write_on_conflict(|| async {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin CLI runtime attempt activation transaction")?;

                let (binding, changed, task_changed, projection_at) =
                    pioneer_crud_execution::cli_attempt_commits::activate_attempt(
                        &transaction,
                        &turn_id,
                        &attempt_id,
                        &native_turn_id,
                        request_id.clone(),
                        started_at,
                        execution_owner_id.as_deref(),
                        execution_lease_until,
                    )
                    .await?;
                project_cli_runtime_turn_binding_state(
                    &*transaction,
                    &binding.0.turn_id,
                    projection_at,
                )
                .await?;
                transaction.commit().await.context(if task_changed {
                    "failed to commit CLI runtime attempt activation transaction"
                } else {
                    "failed to commit idempotent CLI runtime attempt activation"
                })?;
                Ok((binding, changed, task_changed))
            })
            .await;
        if let Ok((_, changed, task_changed)) = &result {
            for change in changes {
                change.completed(*changed);
            }
            task_change.completed(*task_changed);
        }
        let (binding, _, _) = result?;
        hydrate_cli_binding_pair(&self.connection, binding).await
    }

    pub async fn cli_runtime_turn_terminal_guard(
        &self,
        expected_binding: &CliRuntimeTurnBindingRecord,
    ) -> Result<Option<CliRuntimeTurnTerminalGuard>> {
        self.cli_runtime_store()
            .cli_runtime_turn_terminal_guard(expected_binding)
            .await
    }

    pub async fn cli_runtime_turn_terminal_guard_by_id(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnTerminalGuard>> {
        self.cli_runtime_store()
            .cli_runtime_turn_terminal_guard_by_id(turn_id)
            .await
    }

    /// State/version authority only: mapping JSON is intentionally absent.
    /// A content/recovery reader must use the ordinary logical binding API.
    pub async fn cli_runtime_turn_terminal_control_guard_by_id(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnTerminalGuard>> {
        self.cli_runtime_store()
            .cli_runtime_turn_terminal_control_guard_by_id(turn_id)
            .await
    }

    pub async fn cli_runtime_turn_terminal_control_guard(
        &self,
        expected: &CliRuntimeTurnBindingRecord,
    ) -> Result<Option<CliRuntimeTurnTerminalGuard>> {
        self.cli_runtime_store()
            .cli_runtime_turn_terminal_control_guard(expected)
            .await
    }

    /// Reload actual bytes only for a content consumer, keeping the selected
    /// logical binding identity and version authoritative across the handoff.
    pub async fn load_cli_runtime_control_mapping(
        &self,
        expected: &CliRuntimeTurnBindingRecord,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .load_cli_runtime_control_mapping(expected)
            .await
    }

    /// A previously completed native attempt can still belong to an unfinished
    /// logical Turn. Only Task orchestration uses this source; ordinary terminal
    /// event admission remains restricted to active attempts.
    pub async fn cli_runtime_completed_task_continuation_source(
        &self,
        binding: &CliRuntimeTurnBindingRecord,
    ) -> Result<Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>> {
        self.cli_runtime_store()
            .cli_runtime_completed_task_continuation_source(binding)
            .await
    }

    /// Coherent provenance of an ordinary native terminal event. Unlike a
    /// destructive Blocked observation, a Goal completion may already have
    /// completed its segment while its owning attempt remains active.
    pub async fn cli_runtime_terminal_event_source(
        &self,
        binding: &CliRuntimeTurnBindingRecord,
        native_turn_id: &str,
    ) -> Result<Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>> {
        self.cli_runtime_store()
            .cli_runtime_terminal_event_source(binding, native_turn_id)
            .await
    }

    pub async fn get_cli_runtime_native_event(
        &self,
        id: &str,
    ) -> Result<Option<CliRuntimeNativeEventRecord>> {
        cli_runtime_binding::find_native_event(&self.connection, id).await
    }

    /// A point seek for current physical execution; never a journal/history scan.
    /// Recovery must let an accepted outcome run before replacing that execution.
    pub async fn has_pending_cli_runtime_terminal_event(&self, turn_id: &str) -> Result<bool> {
        self.cli_runtime_store()
            .has_pending_cli_runtime_terminal_event(turn_id)
            .await
    }

    /// Re-authorize an immutable accepted outcome under the current process
    /// lease. An unsaved producer still uses the strict source CAS below.
    /// JSON/hash are prepared before this short validation writer transaction.
    pub async fn authorize_cli_runtime_terminal_event(
        &self,
        record: &CliRuntimeNativeEventRecord,
        accepted: &pioneer_protocol::CliRuntimeBlockedTurnGuard,
        expected: &pioneer_protocol::CliRuntimeBlockedTurnGuard,
        owner_id: &str,
    ) -> Result<Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>> {
        self.cli_runtime_store()
            .authorize_cli_runtime_terminal_event(record, accepted, expected, owner_id)
            .await
    }

    /// Persist the accepted native outcome before waiting for transition
    /// ownership. Payload preparation is outside capacity; the source CAS and
    /// insert share this writer commit. First outcome of an execution wins.
    pub async fn persist_cli_runtime_terminal_event(
        &self,
        event: NewCliRuntimeNativeEvent,
        expected: &pioneer_protocol::CliRuntimeBlockedTurnGuard,
        owner_id: &str,
    ) -> Result<bool> {
        self.cli_runtime_store()
            .persist_cli_runtime_terminal_event(event, expected, owner_id)
            .await
    }

    pub async fn cli_runtime_blocked_turn_guard(
        &self,
        binding: &CliRuntimeTurnBindingRecord,
        native_turn_id: &str,
    ) -> Result<Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>> {
        let Some(snapshot) = self
            .cli_runtime_turn_terminal_control_guard(binding)
            .await?
        else {
            return Ok(None);
        };
        Ok(
            cli_runtime_blocked_guard_from_snapshot(&snapshot).filter(|guard| {
                guard
                    .segment
                    .as_ref()
                    .map(|(_, native, _)| native.as_str())
                    .or(guard.attempt_native_turn_id.as_deref())
                    == Some(native_turn_id)
            }),
        )
    }

    /// The attempt/segments and canonical Blocked event have one commit. Before
    /// enqueue nothing has terminalized; a failed projection rolls everything back.
    pub async fn materialize_cli_runtime_blocked_turn_guarded(
        &self,
        notification: pioneer_protocol::TurnBlockedNotification,
        expected: &pioneer_protocol::CliRuntimeBlockedTurnGuard,
        execution_owner_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        let payload_change = terminal_cleanup::Change::payload(&self.connection);
        let control_wakes = vec![
            GatewayControlWake::Turn(notification.turn.id.clone()),
            GatewayControlWake::Resources,
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let receipt_keys = vec![notification.turn.id.clone()];
        let receipt_change = self
            .connection
            .work_signal("maintenance.receipts")
            .changing();
        let changes = [
            "gateway.control",
            "gateway.cli",
            "projection",
            "compaction",
            "maintenance.native",
            "maintenance.html",
            "tasks.lifecycle",
        ]
        .map(|domain| self.control_work_change(domain, &control_wakes));
        let result = async {
            let _release_wake = self.execution_heartbeats.release_wake();
            anyhow::ensure!(
                notification.turn.id == expected.turn_id
                    && notification.thread_id == expected.thread_id
                    && notification.workspace_id == expected.workspace_id
                    && notification.turn.status == TurnStatus::Blocked,
                "Blocked event does not match its native source"
            );
            let outcome_id = expected.terminal_delivery_id();
            let accepted_outcome = expected.terminal_delivery_id.is_some();
            if let Some(id) = expected.terminal_delivery_id.as_deref() {
                anyhow::ensure!(id == outcome_id, "Blocked source outcome identity changed");
            }
            let mut expected_execution = expected.clone();
            expected_execution.terminal_delivery_id = None;
            let event = TurnEventPayload::TurnBlocked(notification);
            let mut envelope =
                prepare_turn_event_envelope_for_permanent_storage(&self.connection, event).await?;
            envelope.projection_context_json = serialize_turn_event_projection_context(
                &TurnEventProjectionContext {
                    enqueue_optional_deliveries: true,
                    ..Default::default()
                },
                envelope.event.id(),
            )?;
            let created_at = unix_to_datetime(event_timestamp_secs);
            let expires_at = unix_to_datetime(
                event_timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS),
            );
            // These existing preparation APIs parse JSON and build terminal-effect
            // plans without an open transaction. Their apply methods fence mutable
            // item/effect sources; the canonical source row is also checked below.
            let prepared_turn_source = turn_reads::find_turn_by_thread_and_id(
                &self.connection,
                &expected.thread_id,
                &expected.turn_id,
            )
            .await?;
            let prepared = prepare_projected_turn_event_from_envelope(
                &self.connection,
                envelope.clone(),
                created_at,
            )
            .await?;
            self.retry_write_on_conflict(|| async {
                let prepared = prepared.clone();
                let transaction = self.connection.begin().await?;
                let Some(binding) =
                    cli_runtime_binding::find_turn_binding(&transaction, &expected.turn_id).await?
                else {
                    transaction.rollback().await?;
                    return Ok(false);
                };
                let turn = turn_reads::find_turn_by_thread_and_id(
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
                if turn != prepared_turn_source {
                    transaction.rollback().await?;
                    return Ok(false);
                }
                let actual_snapshot =
                    turn.and_then(|turn| turn_status_from_db(&turn.status))
                        .map(|turn_status| {
                            CliRuntimeTurnTerminalGuard::control(
                                binding,
                                attempt,
                                segment,
                                execution_owner,
                                turn_status,
                            )
                        });
                let actual = actual_snapshot.as_ref().and_then(|snapshot| {
                    if accepted_outcome {
                        cli_runtime_event_guard_from_snapshot(snapshot)
                    } else {
                        cli_runtime_blocked_guard_from_snapshot(snapshot)
                    }
                });
                let recovery_is_current = if expected.recovery_confirmed {
                    // Existing durable confirmation ends recovery authority. The
                    // attempt identity and confirmation are still checked above.
                    true
                } else if let (Some(job_id), Some(attempt_id)) =
                    (&expected.recovery_job_id, &expected.recovery_attempt_id)
                {
                    let job = recovery_job::find_job_by_id(&transaction, job_id).await?;
                    job.is_some_and(|job| {
                        job.turn_id == expected.turn_id
                            && job.status == "active"
                            && job.active_attempt_id.as_ref() == Some(attempt_id)
                    })
                } else {
                    expected.recovery_job_id.is_none() && expected.recovery_attempt_id.is_none()
                };
                if actual.as_ref() != Some(&expected_execution) || !recovery_is_current {
                    transaction.rollback().await?;
                    return Ok(false);
                }
                let outcome_pending = cli_runtime_terminal_outcome_pending(
                    &transaction,
                    actual_snapshot.as_ref().expect("source checked"),
                    &outcome_id,
                )
                .await?;
                // Observational Blocked cannot consume a saved outcome. An explicit
                // accepted-outcome delivery must still own that exact pending row;
                // ACK and Blocked commit together, preserving restart/replay fencing.
                if outcome_pending != accepted_outcome {
                    transaction.rollback().await?;
                    return Ok(false);
                }
                validate_turn_event_execution_owner(
                    &transaction,
                    envelope.event.payload(),
                    Some(execution_owner_id),
                )
                .await?;
                if !cli_runtime_binding::mark_turn_attempt_terminal(
                    &transaction,
                    &expected.attempt_id,
                    CliRuntimeTurnAttemptStatus::Interrupted,
                    match prepared.event.payload() {
                        TurnEventPayload::TurnBlocked(notification) => {
                            notification.turn.error.clone()
                        }
                        _ => unreachable!(),
                    },
                    created_at,
                )
                .await?
                {
                    transaction.rollback().await?;
                    return Ok(false);
                }
                if accepted_outcome {
                    ack_cli_runtime_outcome(&transaction, Some(expected), "gateway/terminal_ack")
                        .await?;
                }
                self.append_and_project_turn_event_in_transaction(
                    &transaction,
                    prepared,
                    created_at,
                    expires_at,
                    true,
                )
                .await?;
                // Binding remains discoverable until the shared terminal cleanup.
                // If its consumer is cancelled after commit, canonical status plus
                // this same terminal attempt lets the next active round repair it.
                project_cli_runtime_turn_binding_state(
                    &*transaction,
                    &expected.turn_id,
                    created_at,
                )
                .await?;
                transaction.commit().await?;
                Ok(true)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            payload_change
                .complete(&self.connection, *changed, &receipt_keys)
                .await;
            receipt_change.completed_keys(if *changed { &receipt_keys } else { &[] });
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn terminalize_cli_runtime_turn_binding(
        &self,
        turn_id: &str,
        binding_status: &str,
        attempt_status: CliRuntimeTurnAttemptStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.terminalize_cli_runtime_turn_binding_guarded(
            turn_id,
            binding_status,
            attempt_status,
            failure_reason,
            completed_at,
            None,
        )
        .await
    }

    /// None means superseded (or missing), and must not authorize terminal effects.
    pub async fn terminalize_cli_runtime_turn_binding_guarded(
        &self,
        turn_id: &str,
        binding_status: &str,
        attempt_status: CliRuntimeTurnAttemptStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        expected: Option<&CliRuntimeTurnTerminalGuard>,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.terminalize_cli_runtime_turn_binding_guarded_projection(
            turn_id,
            binding_status,
            attempt_status,
            failure_reason,
            completed_at,
            expected,
            true,
        )
        .await
    }

    /// Control transition: the returned binding omits mapping bytes and read holds.
    pub async fn terminalize_cli_runtime_turn_control_binding_guarded(
        &self,
        turn_id: &str,
        binding_status: &str,
        attempt_status: CliRuntimeTurnAttemptStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        expected: Option<&CliRuntimeTurnTerminalGuard>,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.terminalize_cli_runtime_turn_binding_guarded_projection(
            turn_id,
            binding_status,
            attempt_status,
            failure_reason,
            completed_at,
            expected,
            false,
        )
        .await
    }

    async fn terminalize_cli_runtime_turn_binding_guarded_projection(
        &self,
        turn_id: &str,
        binding_status: &str,
        attempt_status: CliRuntimeTurnAttemptStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        expected: Option<&CliRuntimeTurnTerminalGuard>,
        capture_mapping: bool,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        let control_wakes = vec![
            GatewayControlWake::Turn(turn_id.to_owned()),
            GatewayControlWake::CommandTurn(turn_id.to_owned()),
        ];
        let changes = [
            "gateway.control",
            "gateway.cli",
            "maintenance.native",
            "maintenance.portable",
            "maintenance.html",
        ]
        .map(|domain| self.control_work_change(domain, &control_wakes));
        let task_change = self.connection.work_signal("tasks.lifecycle").changing();
        let result = async {
            if attempt_status.is_active() {
                bail!(
                    "CLI runtime terminal binding cannot use active attempt status `{}`",
                    attempt_status.as_str()
                );
            }
            if let Some(expected) = expected
                && binding_status != turn_status_to_db(expected.turn_status)
            {
                bail!("terminal binding projection does not match its canonical Turn decision");
            }
            // Logical guards preserve their byte comparison outside writer capacity.
            // Control guards use only the durable identity/revision, revalidated
            // with all execution facts in the writer below. Logical updates
            // increment that revision; physical compression leaves it intact.
            if let Some(expected) = expected.filter(|guard| guard.has_mapping()) {
                let Some(current) = self.get_cli_runtime_turn_binding(turn_id).await? else {
                    return Ok((None, false, false));
                };
                if !cli_runtime_binding::same_binding_version(&current, &expected.binding)
                    || current.input_mapping_json != expected.binding.input_mapping_json
                {
                    return Ok((None, false, false));
                }
            }
            let turn_id = turn_id.to_owned();
            let binding_status = binding_status.to_owned();
            self.retry_write_on_conflict(|| async {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin CLI runtime terminal binding transaction")?;
                let Some(binding) = cli_runtime_binding::find_turn_binding_projection(
                    &transaction,
                    turn_id.as_str(),
                    capture_mapping,
                )
                .await?
                else {
                    transaction.rollback().await.ok();
                    return Ok((None, false, false));
                };
                let attempt =
                    cli_runtime_binding::latest_turn_attempt(&transaction, turn_id.as_str())
                        .await?;
                if let Some(expected) = expected {
                    let turn = turn_reads::find_turn_by_thread_and_id(
                        &transaction,
                        expected.binding.thread_id.as_str(),
                        turn_id.as_str(),
                    )
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
                    let execution_owner = turn_execution::find(&transaction, turn_id.as_str())
                        .await?
                        .map(|execution| (execution.owner_id, execution.owner_generation));
                    if expected.turn_status == TurnStatus::InProgress
                        || !cli_runtime_binding::same_binding_version(&binding, &expected.binding)
                        || attempt != expected.attempt
                        || segment != expected.segment
                        || execution_owner != expected.execution_owner
                        || turn
                            .as_ref()
                            .and_then(|turn| turn_status_from_db(&turn.status))
                            != Some(expected.turn_status)
                    {
                        transaction.rollback().await?;
                        return Ok((None, false, false));
                    }
                }
                let attempt_changed = attempt
                    .as_ref()
                    .is_some_and(|attempt| attempt.status.is_active());
                if let Some(attempt) = attempt
                    && attempt.status.is_active()
                    && !cli_runtime_binding::mark_turn_attempt_terminal(
                        &transaction,
                        attempt.id.as_str(),
                        attempt_status,
                        failure_reason.clone(),
                        completed_at,
                    )
                    .await?
                {
                    transaction.rollback().await.ok();
                    bail!(
                        "CLI runtime turn attempt `{}` changed while its binding was terminalized",
                        attempt.id
                    );
                }
                let task_changed = attempt_changed || binding.status != binding_status;
                let changed = task_changed || binding.updated_at != completed_at;
                let stored_binding = cli_runtime_binding::update_turn_control_projection(
                    &transaction,
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
                        status: binding_status.clone(),
                        model: binding.model,
                        cwd: binding.cwd,
                        sandbox_json: binding.sandbox_json,
                        approval_policy: binding.approval_policy,
                        input_mapping_json: binding.input_mapping_json,
                        created_at: binding.created_at,
                        updated_at: completed_at,
                    },
                    capture_mapping,
                )
                .await?;
                project_cli_runtime_turn_binding_state(
                    &*transaction,
                    stored_binding.turn_id.as_str(),
                    completed_at,
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit CLI runtime terminal binding transaction")?;
                Ok((Some(stored_binding), changed, task_changed))
            })
            .await
        }
        .await;
        if let Ok((_, changed, task_changed)) = &result {
            for change in changes {
                change.completed(*changed);
            }
            task_change.completed(*task_changed);
        }
        let (binding, _, _) = result?;
        if capture_mapping {
            hydrate_cli_binding_optional(&self.connection, binding).await
        } else {
            Ok(binding)
        }
    }
}
