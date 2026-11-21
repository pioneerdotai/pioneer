use crate::store::execution::agent_transactions;
use crate::{
    AgentCommitInput, AgentDelegationRouteInput, AgentThreadCreationCommitInput, AppendedTaskEvent,
    CrudStore, DueTaskTriggerReconciliation, GatewayControlWake, NewTaskExecutionAdmission,
    NewTaskRunConversationSnapshot, NewUserNotificationOutbox, TaskEventPayload,
    commit_agent_action, commit_agent_delivery_action, commit_agent_thread_creation,
    insert_task_notification_idempotent,
};
use anyhow::{Context, Result, bail};
use pioneer_crud_compaction::frozen_lifetime;
use pioneer_crud_conversations::events;
use pioneer_crud_tasks::event_records::{DeferredAppendedTaskEvent, materialize_task_events};
use pioneer_crud_tasks::event_store::TaskEventStore;
use pioneer_crud_tasks::task_projector::TaskOutputBinder;
use pioneer_crud_tasks::{task_event, task_execution_admission, user_notification_outbox};
use pioneer_protocol::{
    Task, TaskDelivery, TaskError, TaskExecutorKind, TaskThreadLineage, TaskTrigger, Thread,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[derive(Clone, Copy)]
pub(crate) struct CompactionTaskOutputs;

impl TaskOutputBinder for CompactionTaskOutputs {
    async fn bind<'a, C: ConnectionTrait>(
        &'a self,
        db: &'a C,
        delivery: &'a TaskDelivery,
    ) -> Result<()> {
        pioneer_crud_compaction::task_output::bind_queued_task_output(db, delivery).await
    }
}

/// Complete durable write-set for one Task creation.  Task events, frozen
/// conversation input, execution admission, agent domain actor contracts and
/// an optional execution-bound AgentAction are deliberately committed by one
/// owner.  Keeping this type in CRUD prevents Task Service and model-facing
/// adapters from growing competing partial transaction paths.
#[derive(Debug, Clone)]
pub struct TaskCreationCommitInput {
    pub events: Vec<TaskEventPayload>,
    pub event_timestamp_secs: i64,
    pub execution_admission: Option<NewTaskExecutionAdmission>,
    pub conversation_snapshot: Option<NewTaskRunConversationSnapshot>,
    pub actor_contract: pioneer_protocol::TaskActorContract,
    pub occurrence_contract: Option<pioneer_protocol::TaskOccurrenceContract>,
    pub agent_action: Option<AgentCommitInput>,
}

impl CrudStore {
    pub(crate) fn task_event_store(&self) -> TaskEventStore<CompactionTaskOutputs> {
        let store = TaskEventStore::new(self.connection.clone(), CompactionTaskOutputs);
        #[cfg(any(test, feature = "test-support"))]
        let store = store.with_delivery_commit_test_hooks(&self.delivery_commit_test_hooks);
        store
    }

    pub async fn append_task_event(
        &self,
        event: TaskEventPayload,
        event_timestamp_secs: i64,
    ) -> Result<AppendedTaskEvent> {
        self.task_event_store()
            .append_task_event(event, event_timestamp_secs)
            .await
    }

    pub async fn append_task_events(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        self.task_event_store()
            .append_task_events(events, event_timestamp_secs)
            .await
    }

    pub async fn append_task_events_with_agent_action(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        agent_action: AgentCommitInput,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let change = pioneer_crud_tasks::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            events.iter().map(|event| event.task_id().to_owned()),
        );
        let result = async {
            self.run_serialized_write(|| {
                self.append_task_events_with_agent_action_once(
                    events.clone(),
                    event_timestamp_secs,
                    agent_action.clone(),
                )
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(pioneer_crud_tasks::work_change::inserted_task_events(value));
        }
        result
    }

    pub async fn append_task_events_with_execution_admission(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        admission: NewTaskExecutionAdmission,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let change = pioneer_crud_tasks::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            events.iter().map(|event| event.task_id().to_owned()),
        );
        let result = async {
            self.run_serialized_write(|| {
                self.append_task_events_with_execution_admission_once(
                    events.clone(),
                    event_timestamp_secs,
                    admission.clone(),
                )
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(pioneer_crud_tasks::work_change::inserted_task_events(value));
        }
        result
    }

    pub async fn append_task_events_with_execution_readmission(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        admission: NewTaskExecutionAdmission,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let change = pioneer_crud_tasks::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            events.iter().map(|event| event.task_id().to_owned()),
        );
        let result = async {
            self.run_serialized_write(|| {
                self.append_task_events_with_execution_readmission_once(
                    events.clone(),
                    event_timestamp_secs,
                    admission.clone(),
                )
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(pioneer_crud_tasks::work_change::inserted_task_events(value));
        }
        result
    }

    pub async fn append_task_events_with_execution_readmission_and_agent_action(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        admission: NewTaskExecutionAdmission,
        agent_action: AgentCommitInput,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let change = pioneer_crud_tasks::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            events.iter().map(|event| event.task_id().to_owned()),
        );
        let result = async {
            self.run_serialized_write(|| {
                self.append_task_events_with_execution_readmission_and_agent_action_once(
                    events.clone(),
                    event_timestamp_secs,
                    admission.clone(),
                    agent_action.clone(),
                )
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(pioneer_crud_tasks::work_change::inserted_task_events(value));
        }
        result
    }

    /// Commits every row that makes a newly created Task runnable and
    /// attributable.  In particular, an agent-authored Task cannot become
    /// visible without its exact actor/occurrence contract or its canonical
    /// action receipt, and a failed receipt/outbox write cannot leave a ghost
    /// Task behind.
    pub async fn commit_task_creation(
        &self,
        input: TaskCreationCommitInput,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let change = pioneer_crud_tasks::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            input.events.iter().map(|event| event.task_id().to_owned()),
        );
        let result = async {
            let root = input
                .conversation_snapshot
                .as_ref()
                .map(|s| frozen_lifetime::PreparedFrozenRoot::history(&s.history_json))
                .transpose()?;
            self.run_serialized_write(|| {
                self.commit_task_creation_once(input.clone(), root.clone())
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(pioneer_crud_tasks::work_change::inserted_task_events(value));
        }
        result
    }

    /// Creates an agent-owned Thread, its execution-scoped continuation route
    /// and the originating action receipt/outbox as one durable mutation.
    pub async fn commit_agent_thread_creation_with_action(
        &self,
        thread: Thread,
        execution_id: pioneer_protocol::AgentExecutionId,
        route: Option<AgentDelegationRouteInput>,
        lineage: Option<TaskThreadLineage>,
        created_at: DateTimeWithTimeZone,
        updated_at: DateTimeWithTimeZone,
        agent_action: AgentCommitInput,
    ) -> Result<()> {
        self.run_serialized_write(|| {
            self.commit_agent_thread_creation_with_action_once(
                thread.clone(),
                execution_id.clone(),
                route.clone(),
                lineage.clone(),
                created_at.clone(),
                updated_at.clone(),
                agent_action.clone(),
            )
        })
        .await
    }

    pub async fn append_due_trigger_task_events(
        &self,
        trigger: &TaskTrigger,
        now: i64,
        events: Vec<TaskEventPayload>,
        occurrence_contracts: Vec<pioneer_protocol::TaskOccurrenceContract>,
        reserve_executions: Vec<(String, TaskExecutorKind)>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        self.task_event_store()
            .append_due_trigger_task_events(
                trigger,
                now,
                events,
                occurrence_contracts,
                reserve_executions,
            )
            .await
    }

    /// Quarantine a Task only if the failed schedule is still current. The
    /// caller calculates outside database capacity; a user can repair the
    /// spec without changing its next fire time before this commit. Prepare
    /// the repository's canonical spec JSON here, then revalidate it and the
    /// Task identity inside the existing atomic due-trigger transaction.
    pub async fn block_task_for_due_trigger_failure(
        &self,
        trigger: &TaskTrigger,
        error: TaskError,
        now: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        self.task_event_store()
            .block_task_for_due_trigger_failure(trigger, error, now)
            .await
    }

    /// Atomically removes a due trigger from the scheduler when its parent
    /// Task cannot execute. A concurrent Task resume/reschedule wins by
    /// changing either the Task status or the expected fire time, in which
    /// case this method leaves the trigger untouched.
    pub async fn reconcile_due_trigger_for_nonexecuting_task(
        &self,
        trigger_id: &str,
        expected_next_fire_at: i64,
        now: i64,
    ) -> Result<Option<DueTaskTriggerReconciliation>> {
        self.task_event_store()
            .reconcile_due_trigger_for_nonexecuting_task(trigger_id, expected_next_fire_at, now)
            .await
    }

    pub(crate) async fn append_task_events_once(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        self.task_event_store()
            .append_task_events_once(events, event_timestamp_secs)
            .await
    }

    async fn append_task_events_with_agent_action_once(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        agent_action: AgentCommitInput,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let control_wakes = vec![GatewayControlWake::Resources, GatewayControlWake::Actions];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let mut action_changed = false;
        let control_result = async {
            let events = self.prepare_task_events_for_write(events).await?;
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin Task action materialization transaction")?;
            let result = async {
                let appended = self
                    .append_task_events_in_connection(&transaction, events, event_timestamp_secs)
                    .await?;
                action_changed =
                    !pioneer_crud_execution::agent_action_commit::agent_action_commit_exists(
                        &transaction,
                        &agent_action,
                    )
                    .await?;
                agent_transactions::commit_agent_action(&transaction, &agent_action).await?;
                Ok(appended)
            }
            .await;
            match result {
                Ok(appended) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit Task action materialization transaction")?;
                    Ok(materialize_task_events(appended))
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    Err(error)
                }
            }
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(
                action_changed || pioneer_crud_tasks::work_change::inserted_task_events(value),
            );
        }
        control_result
    }

    pub(crate) async fn materialize_task_notification_once(
        &self,
        notification: NewUserNotificationOutbox,
        agent_action: Option<AgentCommitInput>,
    ) -> Result<pioneer_entity::user_notification_outbox::Model> {
        let control_wakes = vec![GatewayControlWake::Resources, GatewayControlWake::Actions];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let mut action_changed = false;
        let control_result = async {
        let payload: pioneer_protocol::TaskUserNotificationDeliveredNotification =
            serde_json::from_str(notification.payload_json.as_str())
                .context("durable Task notification payload is invalid")?;
        if payload.notification_id != notification.id
            || payload.workspace_id != notification.workspace_id
            || payload.recipient_principal_id != notification.recipient_principal_id
            || payload.task_id != notification.task_id
            || payload.run_id != notification.run_id
            || payload.delivery_id != notification.task_delivery_id
        {
            bail!("durable Task notification payload differs from its exact recipient envelope");
        }
        match agent_action.as_ref() {
            Some(action) => {
                let result_reference =
                    pioneer_crud_execution::agent_action_commit::agent_delivery_result_reference(action)?;
                let delivery_id = pioneer_protocol::task_delivery_id_from_result_item_id(
                    result_reference.as_str(),
                )
                .context("Task notification action has an invalid delivery reference")?;
                if delivery_id != notification.task_delivery_id
                    || payload.delivery_action_receipt_id.as_deref()
                        != Some(action.receipt_id.as_str())
                    || payload.author.as_ref().is_none_or(|author| {
                        author.agent_identity_id.as_str() != action.actor_identity_id
                            || author.agent_execution_id.as_str() != action.action.execution_id
                    })
                {
                    bail!(
                        "Agent Task notification differs from its exact delivery action and author"
                    );
                }
            }
            None => {
                if payload.author.is_some() || payload.delivery_action_receipt_id.is_some() {
                    bail!("System Task notification cannot claim an Agent delivery action");
                }
            }
        }
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin Task notification transaction")?;
        let result = async {
            if let (Some(action), Some(author)) = (agent_action.as_ref(), payload.author.as_ref()) {
                pioneer_crud_execution::agent_identity::revalidate_agent_presentation_for_execution(
                    &transaction,
                    action.action.execution_id.as_str(),
                    author,
                )
                .await?;
            }
            let persisted = user_notification_outbox::insert_task_notification_idempotent(
                &transaction,
                notification,
            )
            .await?;
            if let Some(agent_action) = agent_action {
                action_changed = !pioneer_crud_execution::agent_action_commit::agent_action_commit_exists(
                    &transaction, &agent_action,
                ).await?;
                agent_transactions::commit_agent_delivery_action(
                    &transaction,
                    &agent_action,
                )
                .await?;
            }
            Ok(persisted)
        }
        .await;
        match result {
            Ok(persisted) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit Task notification transaction")?;
                Ok(persisted)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
        }.await;
        if control_result.is_ok() {
            control_change.completed(action_changed);
        }
        control_result
    }

    async fn append_task_events_with_execution_admission_once(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        admission: NewTaskExecutionAdmission,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let events = self.prepare_task_events_for_write(events).await?;
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin Task creation admission transaction")?;
        let result = async {
            let appended_events = self
                .append_task_events_in_connection(&transaction, events, event_timestamp_secs)
                .await?;
            task_execution_admission::insert_immutable(&transaction, admission).await?;
            Ok(appended_events)
        }
        .await;

        match result {
            Ok(appended_events) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit Task creation admission transaction")?;
                Ok(materialize_task_events(appended_events))
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn append_task_events_with_execution_readmission_once(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        admission: NewTaskExecutionAdmission,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let events = self.prepare_task_events_for_write(events).await?;
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin blocked Task readmission transaction")?;
        let result = async {
            let appended_events = self
                .append_task_events_in_connection(&transaction, events, event_timestamp_secs)
                .await?;
            task_execution_admission::readmit_immutable(&transaction, admission).await?;
            Ok(appended_events)
        }
        .await;

        match result {
            Ok(appended_events) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit blocked Task readmission transaction")?;
                Ok(materialize_task_events(appended_events))
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn append_task_events_with_execution_readmission_and_agent_action_once(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        admission: NewTaskExecutionAdmission,
        agent_action: AgentCommitInput,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let control_wakes = vec![GatewayControlWake::Resources, GatewayControlWake::Actions];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let mut action_changed = false;
        let control_result = async {
            let events = self.prepare_task_events_for_write(events).await?;
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin Task action readmission transaction")?;
            let result = async {
                let appended = self
                    .append_task_events_in_connection(&transaction, events, event_timestamp_secs)
                    .await?;
                task_execution_admission::readmit_immutable(&transaction, admission).await?;
                action_changed =
                    !pioneer_crud_execution::agent_action_commit::agent_action_commit_exists(
                        &transaction,
                        &agent_action,
                    )
                    .await?;
                agent_transactions::commit_agent_action(&transaction, &agent_action).await?;
                Ok(appended)
            }
            .await;
            match result {
                Ok(appended) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit Task action readmission transaction")?;
                    Ok(materialize_task_events(appended))
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    Err(error)
                }
            }
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(
                action_changed || pioneer_crud_tasks::work_change::inserted_task_events(value),
            );
        }
        control_result
    }

    async fn commit_task_creation_once(
        &self,
        input: TaskCreationCommitInput,
        root: Option<frozen_lifetime::PreparedFrozenRoot>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let control_wakes = vec![GatewayControlWake::Resources, GatewayControlWake::Actions];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let mut action_changed = false;
        let control_result = async {
            let task_store = self.task_event_store();
            let prepared = task_store
                .prepare_task_creation(
                    pioneer_crud_tasks::task_creation::TaskCreation {
                        events: input.events,
                        event_timestamp_secs: input.event_timestamp_secs,
                        execution_admission: input.execution_admission,
                        conversation_snapshot: input.conversation_snapshot,
                        actor_contract: input.actor_contract,
                        occurrence_contract: input.occurrence_contract,
                    },
                    root.as_ref(),
                )
                .await?;
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin atomic Task creation transaction")?;
            let result = async {
                let appended_events = task_store
                    .apply_task_creation(&transaction, prepared, root.as_ref())
                    .await?;
                if let Some(agent_action) = input.agent_action {
                    action_changed =
                        !pioneer_crud_execution::agent_action_commit::agent_action_commit_exists(
                            &transaction,
                            &agent_action,
                        )
                        .await?;
                    agent_transactions::commit_agent_action(&transaction, &agent_action).await?;
                }
                Ok(appended_events)
            }
            .await;

            match result {
                Ok(appended_events) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit atomic Task creation transaction")?;
                    Ok(materialize_task_events(appended_events))
                }
                Err(error) => Err(crate::store::connection::rollback_preserving_error(
                    transaction,
                    error,
                )
                .await),
            }
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(
                action_changed || pioneer_crud_tasks::work_change::inserted_task_events(value),
            );
        }
        control_result
    }

    async fn commit_agent_thread_creation_with_action_once(
        &self,
        thread: Thread,
        execution_id: pioneer_protocol::AgentExecutionId,
        route: Option<AgentDelegationRouteInput>,
        lineage: Option<TaskThreadLineage>,
        created_at: DateTimeWithTimeZone,
        updated_at: DateTimeWithTimeZone,
        agent_action: AgentCommitInput,
    ) -> Result<()> {
        let control_wakes = vec![GatewayControlWake::Resources, GatewayControlWake::Actions];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let mut action_changed = false;
        let control_result = async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin agent Thread creation transaction")?;
            let result = async {
                agent_transactions::commit_agent_thread_creation(
                    &transaction,
                    &AgentThreadCreationCommitInput {
                        thread: &thread,
                        execution_id,
                        route,
                        lineage,
                        created_at,
                        updated_at,
                    },
                )
                .await?;
                action_changed =
                    !pioneer_crud_execution::agent_action_commit::agent_action_commit_exists(
                        &transaction,
                        &agent_action,
                    )
                    .await?;
                agent_transactions::commit_agent_action(&transaction, &agent_action).await?;
                Ok(())
            }
            .await;
            match result {
                Ok(()) => transaction
                    .commit()
                    .await
                    .context("failed to commit agent Thread creation transaction"),
                Err(error) => {
                    let _ = transaction.rollback().await;
                    Err(error)
                }
            }
        }
        .await;
        if control_result.is_ok() {
            control_change.completed(action_changed);
        }
        control_result
    }

    pub(crate) async fn prepare_task_events_for_write(
        &self,
        events: Vec<TaskEventPayload>,
    ) -> Result<Vec<task_event::PreparedTaskEvent>> {
        pioneer_crud_tasks::event_pipeline::prepare_task_events_for_write(&self.connection, events)
            .await
    }

    /// Refresh only portable/idempotency facts. Preserve every domain fence,
    /// projection and delivery boundary of a caller-owned prepared transition.
    async fn refresh_task_event_preflight(
        &self,
        events: Vec<task_event::PreparedTaskEvent>,
    ) -> Result<Vec<task_event::PreparedTaskEvent>> {
        pioneer_crud_tasks::event_pipeline::refresh_task_event_preflight(&self.connection, events)
            .await
    }

    async fn append_task_events_in_connection<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        events: Vec<task_event::PreparedTaskEvent>,
        event_timestamp_secs: i64,
    ) -> Result<Vec<DeferredAppendedTaskEvent>> {
        self.append_task_events_in_connection_with_delivery_policy(
            db,
            events,
            event_timestamp_secs,
            false,
        )
        .await
    }

    fn append_task_events_in_connection_with_delivery_policy<'a, C: ConnectionTrait + Sync>(
        &'a self,
        db: &'a C,
        events: Vec<task_event::PreparedTaskEvent>,
        event_timestamp_secs: i64,
        delivery_cancellation: bool,
    ) -> futures_util::future::BoxFuture<'a, Result<Vec<DeferredAppendedTaskEvent>>> {
        pioneer_crud_tasks::event_pipeline::append_task_events_in_connection_with_delivery_policy(
            db,
            events,
            event_timestamp_secs,
            delivery_cancellation,
            &CompactionTaskOutputs,
        )
    }
}
