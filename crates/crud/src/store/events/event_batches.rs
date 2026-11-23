use crate::events::TurnEventPayload;
use crate::store::events::event_preparation::{
    PreparedProjectedTurnEvent, TURN_EVENT_PROJECTION_LEASE_SECS,
    prepare_projected_turn_event_from_envelope, prepare_turn_event_envelope_for_permanent_storage,
};
use crate::{
    AgentCommitInput, AgentExecutionGraphCommitInput, AgentExecutionGraphCommitResult,
    AgentTurnResponseInput, CrudStore, GatewayControlWake, NewThreadMembership, NewTurnAdmission,
    NewTurnExecution, PersistedThreadAccessClass, insert_thread_membership, terminal_cleanup,
};
use anyhow::{Context, Result, bail};
use pioneer_crud_conversations::thread_creation::{
    NewUserThreadCreation, lock_and_validate_initial_turn,
};
use pioneer_crud_execution::gateway_control::event_wakes;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_execution::turn_execution::terminal_turn_execution_status_for_event;
use pioneer_crud_execution::{execution_heartbeat, turn_admission, turn_execution};
use pioneer_crud_timeline::timeline_live_projection::project_cli_runtime_turn_binding_state;
use pioneer_protocol::{PersistedActorRef, TurnExecutionSecuritySnapshot, TurnItem};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{DatabaseTransaction, TransactionTrait};
use std::collections::HashSet;

pub(crate) async fn insert_new_user_thread_for_initial_turn(
    transaction: &DatabaseTransaction,
    events: &[TurnEventPayload],
    creation: &NewUserThreadCreation,
) -> Result<bool> {
    let Some(started) = lock_and_validate_initial_turn(transaction, events, creation).await? else {
        return Ok(false);
    };
    let thread_created_at = unix_to_datetime(started.thread.created_at);
    let thread_updated_at = unix_to_datetime(started.thread.updated_at);
    crate::store::conversations::thread_writes::insert_user_thread_with_creator(
        transaction,
        &started.thread,
        &creation.creator,
        creation.access_class,
        thread_created_at,
        thread_updated_at,
    )
    .await?;

    // Explicit membership is the ACL for a private thread. A workspace-visible
    // thread is authorized exclusively through workspace membership, so
    // persisting the creator into `thread_membership` would create an invalid
    // mixed access model and make the first Turn fail atomically.
    if creation.access_class == PersistedThreadAccessClass::Private
        && let Some((gateway_id, principal_id)) = creation.member.as_ref()
    {
        insert_thread_membership(
            transaction,
            &NewThreadMembership {
                gateway_id: gateway_id.clone(),
                principal_id: principal_id.clone(),
                workspace_id: started.thread.workspace_id.clone(),
                thread_id: started.thread.id.clone(),
                added_by: creation.creator.clone(),
                now: thread_updated_at,
            },
        )
        .await?;
    }
    Ok(true)
}

pub(crate) const MAX_ATOMIC_TURN_EVENT_BATCH_SIZE: usize = 64;

impl CrudStore {
    pub(crate) async fn materialize_turn_events_atomically_with_optional_admission(
        &self,
        events: Vec<TurnEventPayload>,
        event_timestamp_secs: i64,
        creation: Option<NewUserThreadCreation>,
        admission: Option<NewTurnAdmission>,
        authority_envelope: Option<(String, String)>,
        execution: Option<NewTurnExecution>,
        execution_security_snapshot: Option<(String, TurnExecutionSecuritySnapshot)>,
        agent_action: Option<AgentCommitInput>,
        execution_graph: Option<AgentExecutionGraphCommitInput>,
        agent_turn_response: Option<AgentTurnResponseInput>,
    ) -> Result<Option<AgentExecutionGraphCommitResult>> {
        let _release_wake = (events
            .iter()
            .any(|event| terminal_turn_execution_status_for_event(event).is_some()))
        .then(|| self.execution_heartbeats.release_wake());
        let _ownership_change = execution
            .as_ref()
            .map(|execution| self.execution_heartbeats.changing(&execution.owner_id));
        let created_at = unix_to_datetime(event_timestamp_secs);
        let claim_expires_at =
            unix_to_datetime(event_timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS));

        self.retry_write_on_conflict(|| {
            self.materialize_turn_events_atomically_once(
                _ownership_change.as_ref(),
                events.clone(),
                created_at,
                claim_expires_at,
                creation.clone(),
                admission.clone(),
                authority_envelope.clone(),
                execution.clone(),
                execution_security_snapshot.clone(),
                agent_action.clone(),
                execution_graph.clone(),
                agent_turn_response.clone(),
            )
        })
        .await
    }

    pub(crate) async fn materialize_turn_events_atomically_with_creation(
        &self,
        events: Vec<TurnEventPayload>,
        event_timestamp_secs: i64,
        creation: NewUserThreadCreation,
    ) -> Result<()> {
        self.materialize_turn_events_atomically_with_optional_admission(
            events,
            event_timestamp_secs,
            Some(creation),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .map(|_| ())
    }

    fn materialize_turn_events_atomically_once<'a>(
        &'a self,
        ownership_change: Option<&'a execution_heartbeat::OwnershipChange>,
        events: Vec<TurnEventPayload>,
        created_at: DateTimeWithTimeZone,
        claim_expires_at: DateTimeWithTimeZone,
        creation: Option<NewUserThreadCreation>,
        admission: Option<NewTurnAdmission>,
        authority_envelope: Option<(String, String)>,
        execution: Option<NewTurnExecution>,
        execution_security_snapshot: Option<(String, TurnExecutionSecuritySnapshot)>,
        agent_action: Option<AgentCommitInput>,
        execution_graph: Option<AgentExecutionGraphCommitInput>,
        agent_turn_response: Option<AgentTurnResponseInput>,
    ) -> futures_util::future::BoxFuture<'a, Result<Option<AgentExecutionGraphCommitResult>>> {
        Box::pin(async move {
            let control_wakes = {
                let mut wakes = event_wakes(&events);
                if agent_action.is_some() {
                    wakes.push(GatewayControlWake::Actions);
                }
                if execution_graph.is_some() || admission.is_some() || execution.is_some() {
                    wakes.push(GatewayControlWake::Resources);
                    wakes.push(GatewayControlWake::Supervision);
                }
                wakes
            };
            // Optional graph/action receipts may change without a new event.
            let mut changed = false;
            let cleanup_turns = events
                .iter()
                .map(|event| event.turn_id().to_owned())
                .collect::<Vec<_>>();
            // The entire write set below (events, admission and Agent receipts/graph)
            // does not mutate frozen roots, operation bindings or checkpoint edges.
            // Those owners publish their own committed identity deltas.
            let html_turns = events
                .iter()
                .filter_map(terminal_cleanup::html_lifetime_turn)
                .map(str::to_owned)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let html_change = (!html_turns.is_empty())
                .then(|| self.connection.work_signal("maintenance.html").changing());
            let mut html_changed = false;
            let cleanup_change = terminal_cleanup::Change::events(&self.connection);
            let changes = ["gateway.control", "projection", "compaction"]
                .map(|domain| self.control_work_change(domain, &control_wakes));
            let result = async {
        let task_turns = events.iter().map(|event| event.turn_id().to_owned()).collect::<Vec<_>>();
        let task_change = if task_turns.is_empty() { None } else {
            Some(pioneer_crud_tasks::work_change::TaskTurnChange::begin(&self.connection, task_turns).await?)
        };
        let result = async {
        if events.len() > MAX_ATOMIC_TURN_EVENT_BATCH_SIZE {
            bail!("atomic Turn event batch exceeds {MAX_ATOMIC_TURN_EVENT_BATCH_SIZE} events");
        }
        let agent_action_timeline_target = if let Some(action) = agent_action.as_ref() {
            let matching_turns = events
                .iter()
                .filter_map(|event| match event {
                    TurnEventPayload::TurnStarted(started)
                        if matches!(
                            started.actor.as_ref(),
                            Some(PersistedActorRef::AgentExecution(execution_id))
                                if execution_id.as_str() == action.action.execution_id
                        ) =>
                    {
                        Some(started.turn.id.clone())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            if matching_turns.len() > 1 {
                bail!("canonical agent action batch contains multiple authored Turn starts");
            }
            let item_targets = events
                .iter()
                .filter_map(|event| match event {
                    TurnEventPayload::ItemCompleted(completed) => match &completed.item {
                        TurnItem::AgentMessage { id, .. } => {
                            Some((completed.turn_id.clone(), id.clone()))
                        }
                        TurnItem::SystemEvent { id, .. }
                            if action.action.action_kind == "deliver_result"
                                && action.route_receipt_json.is_some() =>
                        {
                            Some((completed.turn_id.clone(), id.clone()))
                        }
                        _ => None,
                    },
                    _ => None,
                })
                .collect::<HashSet<_>>();
            if item_targets.len() > 1 {
                bail!("canonical agent action batch contains multiple Agent message targets");
            }
            match action.action.action_kind.as_str() {
                "deliver_result" => {
                    let (turn_id, item_id) = item_targets.into_iter().next().context(
                        "canonical delivery action batch has no exact result item target",
                    )?;
                    let result_reference =
                        pioneer_crud_execution::agent_action_commit::agent_delivery_result_reference(action)?;
                    if item_id != result_reference {
                        bail!(
                            "canonical delivery action target differs from its immutable result reference"
                        );
                    }
                    if matching_turns
                        .first()
                        .is_some_and(|started_turn_id| started_turn_id != &turn_id)
                    {
                        bail!("agent action item target differs from its exact delivery mutation");
                    }
                    Some((turn_id, Some(item_id)))
                }
                "send_message" | "start_agent" if matching_turns.len() == 1 => {
                    Some((matching_turns[0].clone(), None))
                }
                _ => {
                    bail!("canonical agent action batch has no exact authored timeline target");
                }
            }
        } else {
            None
        };
        let security_snapshot_json = execution_security_snapshot.as_ref()
            .map(|(_, snapshot)| serde_json::to_string(snapshot)).transpose()?;
        let mut prepared_events = Vec::with_capacity(events.len());
        for event in events.iter().cloned() {
            prepared_events.push(
                prepare_turn_event_envelope_for_permanent_storage(&self.connection, event).await?,
            );
        }
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin turn event batch materialization transaction")?;

        let html_before = terminal_cleanup::html_turn_lifetimes(&transaction, &html_turns).await?;

        if let Some(response) = agent_turn_response.as_ref() {
            let matching_start = events.iter().any(|event| {
                matches!(
                    event,
                    TurnEventPayload::TurnStarted(started)
                        if started.turn.id == response.turn_id
                            && started.turn.mode == pioneer_protocol::ThreadMode::Agent
                )
            });
            if !matching_start {
                let _ = transaction.rollback().await;
                bail!("responding AgentExecution is not bound to this Agent Turn batch");
            }
        }

        if let Some(creation) = creation {
            match insert_new_user_thread_for_initial_turn(&transaction, &events, &creation).await {
                Ok(inserted) => changed |= inserted,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            }
        }

        for event in prepared_events {
            let event =
                match prepare_projected_turn_event_from_envelope(&transaction, event, created_at)
                    .await
                {
                    Ok(event) => event,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                };
            match self
                .append_and_project_turn_event_with_replay_in_transaction(
                    &transaction,
                    event,
                    created_at,
                    claim_expires_at,
                    false,
                    false,
                    false,
                )
                .await
            {
                Ok(event_changed) => changed |= event_changed,
                Err(error) => return Err(crate::store::connection::rollback_preserving_error(
                    transaction,
                    error,
                )
                .await),
            }
        }

        // Optional receipts/ownership can be persisted without a new event.
        // Read their identities under the same writer transaction; receipt
        // presence in the returned DTO does not say whether they were inserted.
        changed |= admission.is_some(); // insert is strict: duplicates fail.
        if !changed {
            if let Some(execution) = execution.as_ref() {
                changed |= turn_execution::find(&transaction, &execution.turn_id).await?.is_none();
            }
            if let Some(graph) = execution_graph.as_ref() {
                changed |= pioneer_crud_execution::agent_execution::load_agent_execution(
                    &transaction, &graph.child_execution.id,
                ).await?.is_none();
            }
            if let Some(response) = agent_turn_response.as_ref() {
                changed |= pioneer_crud_execution::execution_reads::load_agent_turn_response(
                    &transaction, &response.turn_id,
                ).await?.is_none();
            }
            if let Some(action) = agent_action.as_ref() {
                changed |= !pioneer_crud_execution::agent_action_commit::agent_action_commit_exists(
                    &transaction, action,
                ).await?;
                if agent_action_timeline_target.is_some() {
                    changed |= !pioneer_crud_execution::agent_action_timeline::action_has_timeline_target(
                        &transaction, &action.action.id,
                    ).await?;
                }
            }
            if let Some((turn_id, context)) = authority_envelope.as_ref() {
                changed |= pioneer_crud_access::turn_execution::find_turn_execution_authorization_context(&transaction, turn_id)
                    .await?.as_deref() != Some(context.trim());
            }
            if let Some((turn_id, snapshot)) = execution_security_snapshot.as_ref() {
                let current = pioneer_crud_conversations::turn_reads::find_turn_by_id(&transaction, turn_id).await?;
                changed |= current.as_ref().and_then(|row| row.execution_security_snapshot_json.as_deref())
                    != Some(security_snapshot_json.as_ref().expect("prepared snapshot").as_str());
                changed |= current.as_ref().and_then(|row| row.execution_security_snapshot_version)
                    != Some(i64::try_from(snapshot.version)?);
            }
        }

        if let Some(admission) = admission
            && let Err(error) = turn_admission::insert(&transaction, admission, created_at).await
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }

        let mut acquired_execution = None;
        if let Some(execution) = execution {
            let execution = match turn_execution::insert_immutable(&transaction, execution).await {
                Ok(execution) => execution,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };
            // TurnStarted was projected before the ownership receipt existed.
            // Refresh within this same atomic admission so a visible CLI child
            // already shows its queued state, even without a runtime binding.
            if execution.executor_kind.is_cli_runtime() {
                project_cli_runtime_turn_binding_state(
                    &*transaction,
                    &execution.turn_id,
                    created_at,
                )
                .await?;
            }
            acquired_execution = Some(execution);
        }

        if let Some((turn_id, authority_envelope_json)) = authority_envelope
            && !pioneer_crud_execution::turn_envelope::set_turn_execution_authorization_context(
                &transaction,
                turn_id.as_str(),
                authority_envelope_json.as_str(),
            )
            .await?
        {
            let _ = transaction.rollback().await;
            bail!("execution Turn disappeared while persisting its authority envelope");
        }

        if let Some((turn_id, snapshot)) = execution_security_snapshot
            && !pioneer_crud_execution::turn_envelope::set_turn_execution_security_snapshot(
                &transaction,
                turn_id.as_str(),
                &snapshot,
            )
            .await?
        {
            let _ = transaction.rollback().await;
            bail!("execution Turn disappeared while persisting its security snapshot");
        }

        let execution_graph_result = if let Some(execution_graph) = execution_graph {
            let before = pioneer_crud_execution::agent_graph_admission::graph_scheduling_facts(
                &transaction, &execution_graph,
            ).await?;
            match crate::store::execution::agent_transactions::commit_agent_execution_graph(
                &transaction,
                &execution_graph,
            )
            .await
            {
                Ok(result) => {
                    changed |= before != pioneer_crud_execution::agent_graph_admission::graph_scheduling_facts(
                        &transaction, &execution_graph,
                    ).await?;
                    Some(result)
                },
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            }
        } else {
            None
        };

        if let Some(response) = agent_turn_response
            && let Err(error) =
                pioneer_crud_execution::agent_identity::insert_agent_turn_response(&transaction, &response)
                    .await
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }

        if let Some(agent_action) = agent_action.as_ref() {
            if let Err(error) =
                crate::store::execution::agent_transactions::commit_agent_action(&transaction, agent_action).await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            if let Some((turn_id, item_id)) = agent_action_timeline_target.as_ref()
                && let Err(error) = pioneer_crud_execution::agent_action_timeline::bind_agent_action_timeline_target(
                    &transaction,
                    turn_id,
                    item_id.as_deref(),
                    agent_action.action.id.as_str(),
                    created_at,
                )
                .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        }

        html_changed = html_before != terminal_cleanup::html_turn_lifetimes(&transaction, &html_turns).await?;

        transaction
            .commit()
            .await
            .context("failed to commit turn event batch materialization transaction")?;
        if let (Some(change), Some(execution)) = (ownership_change, acquired_execution.as_ref()) {
            change.register(execution);
        }

        Ok(execution_graph_result)
        }.await;
        if result.is_ok() { if let Some(change) = task_change { change.completed(&self.connection).await?; } }
        result
        }.await;
            if result.is_ok() {
                cleanup_change
                    .complete(&self.connection, changed, &cleanup_turns)
                    .await;
                if let Some(change) = html_change {
                    change.completed(html_changed);
                }
                for change in changes {
                    change.completed(changed);
                }
            }
            result
        })
    }

    pub(crate) async fn append_and_project_turn_event_in_transaction(
        &self,
        transaction: &DatabaseTransaction,
        prepared: PreparedProjectedTurnEvent,
        created_at: DateTimeWithTimeZone,
        claim_expires_at: DateTimeWithTimeZone,
        enqueue_optional_deliveries: bool,
    ) -> Result<()> {
        self.append_and_project_turn_event_with_replay_in_transaction(
            transaction,
            prepared,
            created_at,
            claim_expires_at,
            enqueue_optional_deliveries,
            false,
            false,
        )
        .await
        .map(|_| ())
    }

    pub(crate) async fn append_and_project_compaction_event_in_transaction(
        &self,
        transaction: &DatabaseTransaction,
        prepared: PreparedProjectedTurnEvent,
        created_at: DateTimeWithTimeZone,
        claim_expires_at: DateTimeWithTimeZone,
    ) -> Result<bool> {
        // Replaying ItemStarted would allocate another running attempt. Only
        // the terminal service item may restore an already projected event.
        let repair_terminal_projection =
            matches!(prepared.event.payload(), TurnEventPayload::ItemCompleted(_));
        self.append_and_project_turn_event_with_replay_in_transaction(
            transaction,
            prepared,
            created_at,
            claim_expires_at,
            true,
            repair_terminal_projection,
            false,
        )
        .await
    }
}
