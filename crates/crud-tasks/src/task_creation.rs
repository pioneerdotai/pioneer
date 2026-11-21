//! Task-owned write set for the atomic Task creation transaction.
use crate::conversation_snapshot::{
    self, NewTaskRunConversationSnapshot, TaskRunConversationSnapshotRecord,
};
use crate::event_pipeline::{
    append_task_events_in_connection_with_delivery_policy, prepare_task_events_for_write,
};
use crate::event_records::DeferredAppendedTaskEvent;
use crate::event_store::TaskEventStore;
use crate::task_actor_contract;
use crate::task_event::PreparedTaskEvent;
use crate::task_execution_admission::{self, NewTaskExecutionAdmission};
use crate::task_projector::TaskOutputBinder;
use anyhow::{Context, Result, bail};
use pioneer_crud_conversations::turn_reads as turn;
use pioneer_crud_execution::{
    cli_runtime_binding::FrozenBindingRoot,
    turn_execution::{self, TurnExecutionStatus},
};
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_protocol::{
    TaskActorContract, TaskEventPayload, TaskOccurrenceContract, TurnWorkOwner,
};
use sea_orm::DatabaseTransaction;

pub struct TaskCreation {
    pub events: Vec<TaskEventPayload>,
    pub event_timestamp_secs: i64,
    pub execution_admission: Option<NewTaskExecutionAdmission>,
    pub conversation_snapshot: Option<NewTaskRunConversationSnapshot>,
    pub actor_contract: TaskActorContract,
    pub occurrence_contract: Option<TaskOccurrenceContract>,
}

struct ComposerAdmission {
    workspace_id: String,
    owner_id: Option<String>,
    creator_thread: Option<String>,
    creator_turn: Option<String>,
    thread_id: String,
    turn_id: String,
}

pub struct PreparedTaskCreation {
    at: i64,
    events: Vec<PreparedTaskEvent>,
    composer_admissions: Vec<ComposerAdmission>,
    snapshot: Option<NewTaskRunConversationSnapshot>,
    snapshot_preflight: Option<TaskRunConversationSnapshotRecord>,
    snapshot_payload: Option<PreparedPayload>,
    execution_admission: Option<NewTaskExecutionAdmission>,
    actor_contract: TaskActorContract,
    occurrence_contract: Option<TaskOccurrenceContract>,
}

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    pub async fn prepare_task_creation(
        &self,
        input: TaskCreation,
        root: Option<&impl FrozenBindingRoot>,
    ) -> Result<PreparedTaskCreation> {
        // Prepare the bounded admission binding outside writer capacity. The
        // status is revalidated inside the Task/action transaction below: Stop
        // or recovery must win atomically against acceptance of new work.
        let composer_admissions = input
            .events
            .iter()
            .filter_map(|event| {
                let TaskEventPayload::TaskCreated { task } = event else {
                    return None;
                };
                let work = task.metadata.as_ref()?.composer_work.as_ref()?;
                Some(ComposerAdmission {
                    workspace_id: task.workspace_id.clone(),
                    owner_id: task.owner_id.clone(),
                    creator_thread: task.created_by_thread_id.clone(),
                    creator_turn: task.created_by_turn_id.clone(),
                    thread_id: work.launch.thread_id.clone(),
                    turn_id: work.launch.turn_id.clone(),
                })
            })
            .collect::<Vec<_>>();
        let prepared_events = prepare_task_events_for_write(&self.connection, input.events).await?;
        // Capture and compare the immutable winner before reserving the writer.
        // Inside the writer, identity/revision/domain CAS revalidates this exact
        // capture. A winner arriving with the Task events forces a full retry.
        let prepared_snapshot = match (&input.conversation_snapshot, root) {
            (Some(snapshot), Some(root)) if root.locator().is_none() => Some(
                pioneer_crud_payloads::storage::PreparedPayload::new(&snapshot.history_json)?,
            ),
            _ => None,
        };
        let snapshot_preflight = if let Some(expected) = &input.conversation_snapshot {
            let winner =
                conversation_snapshot::get_scoped(&self.connection, &expected.run_id).await?;
            if let Some(persisted) = &winner {
                if persisted.run_id != expected.run_id
                    || persisted.task_id != expected.task_id
                    || persisted.workspace_id != expected.workspace_id
                    || persisted.conversation_thread_id != expected.conversation_thread_id
                    || persisted.source_turn_id != expected.source_turn_id
                    || persisted.history_json != expected.history_json
                {
                    bail!(
                        "task run `{}` conversation snapshot conflicts with Task creation",
                        expected.run_id
                    );
                }
            }
            winner
        } else {
            None
        };
        Ok(PreparedTaskCreation {
            at: input.event_timestamp_secs,
            events: prepared_events,
            composer_admissions,
            snapshot: input.conversation_snapshot,
            snapshot_preflight,
            snapshot_payload: prepared_snapshot,
            execution_admission: input.execution_admission,
            actor_contract: input.actor_contract,
            occurrence_contract: input.occurrence_contract,
        })
    }

    pub async fn apply_task_creation(
        &self,
        transaction: &DatabaseTransaction,
        prepared: PreparedTaskCreation,
        root: Option<&impl FrozenBindingRoot>,
    ) -> Result<Vec<DeferredAppendedTaskEvent>> {
        for ComposerAdmission {
            workspace_id,
            owner_id,
            creator_thread,
            creator_turn,
            thread_id,
            turn_id,
        } in &prepared.composer_admissions
        {
            let (Some(creator_thread), Some(creator_turn)) = (creator_thread, creator_turn) else {
                continue;
            };
            let Some(turn) =
                turn::find_turn_by_thread_and_id(transaction, creator_thread, creator_turn).await?
            else {
                continue;
            };
            // Composer payloads also occur in historical/ordinary Task
            // requests. Only a saved detached admission owns this fence.
            if turn.work_owner != TurnWorkOwner::DetachedTask.as_db_str() {
                continue;
            }
            anyhow::ensure!(
                owner_id.as_ref() == Some(thread_id)
                    && creator_thread == thread_id
                    && creator_turn == turn_id,
                "invalid composer Task admission binding"
            );
            let execution = turn_execution::find(transaction, turn_id)
                .await?
                .context("composer Task admission has no execution owner")?;
            anyhow::ensure!(
                turn.work_owner == TurnWorkOwner::DetachedTask.as_db_str()
                    && turn.status == "in_progress"
                    && execution.workspace_id == *workspace_id
                    && execution.thread_id == *thread_id
                    && matches!(
                        execution.status,
                        TurnExecutionStatus::Queued
                            | TurnExecutionStatus::Starting
                            | TurnExecutionStatus::Running
                    ),
                "composer Task admission was stopped or taken over before Task acceptance"
            );
        }
        let appended_events = append_task_events_in_connection_with_delivery_policy(
            transaction,
            prepared.events,
            prepared.at,
            false,
            &self.outputs,
        )
        .await?;

        if let Some(snapshot) = prepared.snapshot {
            let current = conversation_snapshot::find_by_run(transaction, &snapshot.run_id).await?;
            match (&current, &prepared.snapshot_preflight) {
                (None, None) => {}
                (Some(stored), Some(captured))
                    if conversation_snapshot::same_version(stored, captured) => {}
                _ => return Err(pioneer_crud_conversations::turn_payload::PreflightRace.into()),
            }
            if current.is_none() {
                let root = root.context("missing prepared Task root")?;
                root.verify(transaction, &snapshot.workspace_id).await?;
                let (_, inserted) = conversation_snapshot::insert_if_absent(
                    transaction,
                    &snapshot,
                    root.locator(),
                    prepared.snapshot_payload.as_ref(),
                )
                .await?;
                if !inserted {
                    return Err(pioneer_crud_conversations::turn_payload::PreflightRace.into());
                }
                crate::task_child_producers::run(transaction, &snapshot.run_id).await?;
            }
        }
        if let Some(admission) = prepared.execution_admission {
            task_execution_admission::insert_immutable(transaction, admission).await?;
        }
        task_actor_contract::upsert_task_actor_contract(
            transaction,
            &prepared.actor_contract,
            prepared.at,
        )
        .await?;
        if let Some(occurrence) = prepared.occurrence_contract {
            task_actor_contract::upsert_task_occurrence_contract(
                transaction,
                &occurrence,
                prepared.at,
            )
            .await?;
        }
        Ok(appended_events)
    }
}
