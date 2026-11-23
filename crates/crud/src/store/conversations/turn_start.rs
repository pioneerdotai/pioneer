use crate::events::{TurnEventPayload, TurnStartedEventPayload};
use crate::{
    AgentCommitInput, AgentExecutionGraphCommitInput, AgentExecutionGraphCommitResult,
    AgentTurnResponseInput, CompletedMessageTurnWrite, CrudStore, NewTurnAdmission,
    NewTurnExecution, PersistedThreadAccessClass, TurnWorkOwner,
};
use anyhow::{Context, Result, bail};
use pioneer_crud_conversations::message_completion::completed_message_turn_events;
use pioneer_crud_conversations::thread_creation::NewUserThreadCreation;
use pioneer_protocol::{
    GatewayId, PersistedActorRef, PrincipalId, SandboxMode, Thread, Turn,
    TurnExecutionSecuritySnapshot, TurnItem, TurnStatus, UserInput,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizedTurnRuntimeDraft {
    ScopedPrincipal {
        gateway_id: GatewayId,
        principal_id: PrincipalId,
        access_class: PersistedThreadAccessClass,
    },
    Absolute {
        access_class: PersistedThreadAccessClass,
    },
}

/// One immutable Task result projection.  Delivery owns a single transaction
/// containing the conversation events and, for agent-produced results, the
/// canonical DeliverResult action/receipt/outbox.  This prevents both a
/// timeline ghost before the receipt and a receipt without destination data.
pub struct CompletedTaskDeliveryTurnWrite<'a> {
    pub thread: &'a Thread,
    pub sandbox_mode: SandboxMode,
    pub started_turn: &'a Turn,
    pub actor: PersistedActorRef,
    pub audit_event: pioneer_protocol::TurnPermissionAuditEvent,
    pub item_started: pioneer_protocol::ItemStartedNotification,
    pub item_completed: pioneer_protocol::ItemCompletedNotification,
    pub completed: pioneer_protocol::TurnCompletedNotification,
    pub agent_action: Option<AgentCommitInput>,
}

/// One immutable failed Task delivery projection. The failed Turn and an
/// optional routed DeliverResult receipt commit together, while the public
/// terminal error remains System-authored and contains no work item.
pub struct FailedTaskDeliveryTurnWrite<'a> {
    pub thread: &'a Thread,
    pub sandbox_mode: SandboxMode,
    pub started_turn: &'a Turn,
    pub actor: PersistedActorRef,
    pub audit_event: pioneer_protocol::TurnPermissionAuditEvent,
    pub failed: pioneer_protocol::TurnFailedNotification,
    pub agent_action: Option<AgentCommitInput>,
}

impl CrudStore {
    /// Persists the full turn/start write-set atomically through append-only events + projection.
    pub async fn materialize_turn_start(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        actor: PersistedActorRef,
    ) -> Result<()> {
        self.materialize_turn_start_with_reasoning_effort(
            thread_model,
            sandbox_mode,
            turn_model,
            input,
            None,
            actor,
        )
        .await
    }

    /// Test-support entry point for exercising the same projection replacement
    /// that a canonical message edit uses. New input IDs are generated and the
    /// durable message revision advances atomically.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn replace_turn_input_projection_for_test(
        &self,
        turn_id: &str,
        input: &[UserInput],
        message_revision: i64,
        changed_at_unix: i64,
    ) -> Result<()> {
        use pioneer_crud_execution::time::unix_to_datetime;
        use sea_orm::sea_query::Expr;
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, TransactionTrait};

        let turn_id = turn_id.to_owned();
        let input = input.to_vec();
        self.retry_write_on_conflict(|| {
            let turn_id = turn_id.clone();
            let input = input.clone();
            async move {
                let transaction = self.connection.begin().await?;
                crate::store::conversations::turn_writes::replace_turn_input(
                    &transaction,
                    turn_id.as_str(),
                    input.as_slice(),
                    unix_to_datetime(changed_at_unix),
                )
                .await?;
                let updated = pioneer_entity::turn::Entity::update_many()
                    .col_expr(
                        pioneer_entity::turn::Column::MessageRevision,
                        Expr::value(message_revision),
                    )
                    .filter(pioneer_entity::turn::Column::Id.eq(turn_id))
                    .exec(&transaction)
                    .await?;
                anyhow::ensure!(updated.rows_affected == 1, "replacement Turn is missing");
                transaction.commit().await?;
                Ok(())
            }
        })
        .await
    }

    /// Persists the full turn/start write-set plus explicit reasoning effort.
    pub async fn materialize_turn_start_with_reasoning_effort(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
        actor: PersistedActorRef,
    ) -> Result<()> {
        let event = TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread: thread_model.clone(),
            sandbox_mode,
            turn: turn_model.clone(),
            input: input.to_vec(),
            actor: Some(actor),
            reasoning_effort: reasoning_effort.map(str::to_owned),
            work_owner: Default::default(),
        });

        self.materialize_turn_event(event, thread_model.updated_at)
            .await
    }

    /// Persists an execution-free System projection Turn. This deliberately
    /// narrow API cannot be used for agent work: executable Turns must use the
    /// authority-envelope method below.
    pub async fn materialize_non_executable_system_turn_start_with_permission_audit(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
    ) -> Result<()> {
        self.materialize_non_executable_turn_start_with_permission_audit(
            thread_model,
            sandbox_mode,
            turn_model,
            input,
            PersistedActorRef::System,
            audit_event,
        )
        .await
    }

    /// Persists an execution-free projection Turn with the exact conversation
    /// actor selected by the Gateway.  Delivery and other internal projections
    /// may be non-executable without being System-authored: the actor is still
    /// part of the durable conversation history and must match the prepared
    /// Turn's immutable author snapshot.
    pub async fn materialize_non_executable_turn_start_with_permission_audit(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        actor: PersistedActorRef,
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
    ) -> Result<()> {
        if turn_model.mode != pioneer_protocol::ThreadMode::Chat
            || turn_model.turn_kind != pioneer_protocol::TurnKind::Conversation
            || !input.is_empty()
            || turn_model.prompt_manifest.is_some()
        {
            bail!("non-executable projection Turn violates the projection-only contract");
        }
        match (turn_model.author.as_ref(), &actor) {
            (Some(author), actor) if &author.actor == actor => {}
            (None, PersistedActorRef::System) => {}
            _ => bail!("projection actor does not match Turn author snapshot"),
        }
        let started_event = TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread: thread_model.clone(),
            sandbox_mode,
            turn: turn_model.clone(),
            input: Vec::new(),
            actor: Some(actor),
            reasoning_effort: None,
            work_owner: Default::default(),
        });
        self.materialize_turn_events_atomically_with_optional_admission(
            vec![
                started_event,
                TurnEventPayload::TurnPermissionAudit(audit_event),
            ],
            thread_model.updated_at,
            None,
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

    /// Persists an execution-capable Turn and its mandatory authority envelope
    /// in the same database transaction. Runtime draft creation and native
    /// admission are optional parts of that same write-set.
    #[allow(clippy::too_many_arguments)]
    pub async fn materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
        work_owner: TurnWorkOwner,
        actor: PersistedActorRef,
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
        authority_envelope_json: &str,
        runtime_draft: Option<AuthorizedTurnRuntimeDraft>,
        admission: Option<NewTurnAdmission>,
        execution: Option<NewTurnExecution>,
        execution_security_snapshot: &TurnExecutionSecuritySnapshot,
        security_audit_events: Vec<pioneer_protocol::TurnPermissionAuditEvent>,
        execution_graph: Option<AgentExecutionGraphCommitInput>,
        response: Option<AgentTurnResponseInput>,
    ) -> Result<Option<AgentExecutionGraphCommitResult>> {
        let creation = match runtime_draft {
            Some(AuthorizedTurnRuntimeDraft::ScopedPrincipal {
                gateway_id,
                principal_id,
                access_class,
            }) => Some(NewUserThreadCreation {
                creator: actor.clone(),
                access_class,
                member: Some((gateway_id, principal_id)),
            }),
            Some(AuthorizedTurnRuntimeDraft::Absolute { access_class }) => {
                if access_class == PersistedThreadAccessClass::Internal {
                    bail!("new user execution thread cannot use internal access class");
                }
                Some(NewUserThreadCreation {
                    creator: actor.clone(),
                    access_class,
                    member: None,
                })
            }
            None => None,
        };
        self.materialize_admitted_thread_turn(
            thread_model,
            sandbox_mode,
            turn_model,
            input,
            reasoning_effort,
            work_owner,
            actor,
            audit_event,
            authority_envelope_json,
            creation,
            admission,
            execution,
            execution_security_snapshot,
            security_audit_events,
            execution_graph,
            response,
            None,
        )
        .await
    }

    /// One canonical Turn admission write path for interactive and agent
    /// entries. The adapters only supply their authorization/atomic action facts.
    #[allow(clippy::too_many_arguments)]
    async fn materialize_admitted_thread_turn(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
        work_owner: TurnWorkOwner,
        actor: PersistedActorRef,
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
        authority_envelope_json: &str,
        creation: Option<NewUserThreadCreation>,
        admission: Option<NewTurnAdmission>,
        execution: Option<NewTurnExecution>,
        execution_security_snapshot: &TurnExecutionSecuritySnapshot,
        security_audit_events: Vec<pioneer_protocol::TurnPermissionAuditEvent>,
        execution_graph: Option<AgentExecutionGraphCommitInput>,
        response: Option<AgentTurnResponseInput>,
        agent_action: Option<AgentCommitInput>,
    ) -> Result<Option<AgentExecutionGraphCommitResult>> {
        let authority_envelope_json = authority_envelope_json.trim();
        if authority_envelope_json.is_empty() {
            bail!("execution authority envelope must not be empty");
        }
        match (turn_model.author.as_ref(), &actor) {
            (Some(author), actor) if &author.actor == actor => {}
            (None, PersistedActorRef::System) => {}
            _ => bail!("executable Turn actor does not match its immutable author snapshot"),
        }
        let started_event = TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread: thread_model.clone(),
            sandbox_mode,
            turn: Turn {
                work_owner,
                ..turn_model.clone()
            },
            input: input.to_vec(),
            actor: Some(actor),
            reasoning_effort: reasoning_effort.map(str::to_owned),
            work_owner,
        });
        let mut events = Vec::with_capacity(2usize.saturating_add(security_audit_events.len()));
        events.push(started_event);
        events.push(TurnEventPayload::TurnPermissionAudit(audit_event));
        events.extend(
            security_audit_events
                .into_iter()
                .map(TurnEventPayload::TurnPermissionAudit),
        );
        self.materialize_turn_events_atomically_with_optional_admission(
            events,
            thread_model.updated_at,
            creation,
            admission,
            Some((turn_model.id.clone(), authority_envelope_json.to_owned())),
            execution,
            Some((turn_model.id.clone(), execution_security_snapshot.clone())),
            agent_action,
            execution_graph,
            response,
        )
        .await
    }

    /// Persists an ordinary Message as one Started + immediately Completed
    /// canonical Turn batch. Both projections become visible in one commit.
    pub async fn materialize_completed_message_turn_with_permission_audit(
        &self,
        write: CompletedMessageTurnWrite<'_>,
    ) -> Result<()> {
        let updated_at = write.thread.updated_at;
        let events = completed_message_turn_events(write)?;
        self.materialize_turn_events_atomically(events, updated_at)
            .await
    }

    /// Commits an agent-authored executable Turn, its exact child execution
    /// graph/resource placement and the originating action in one transaction.
    #[allow(clippy::too_many_arguments)]
    pub async fn materialize_agent_turn_start_with_graph_and_action(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
        actor: PersistedActorRef,
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
        authority_envelope_json: &str,
        admission: NewTurnAdmission,
        execution: NewTurnExecution,
        execution_security_snapshot: &TurnExecutionSecuritySnapshot,
        security_audit_events: Vec<pioneer_protocol::TurnPermissionAuditEvent>,
        execution_graph: AgentExecutionGraphCommitInput,
        agent_action: AgentCommitInput,
    ) -> Result<AgentExecutionGraphCommitResult> {
        let input_execution_id = match &actor {
            PersistedActorRef::AgentExecution(execution_id) => execution_id,
            _ => bail!("agent Turn transaction requires an exact execution actor"),
        };
        let graph_response = execution_graph
            .response
            .as_ref()
            .context("agent Turn transaction requires an exact response binding")?;
        let response_execution = &execution_graph.child_execution;
        let independent_root = response_execution.parent_execution_id.is_none()
            && response_execution.id == response_execution.work_graph_root_execution_id
            && execution_graph
                .root_execution
                .as_ref()
                .is_some_and(|root| root.id == response_execution.id)
            && response_execution.home_root_thread_id == thread_model.id;
        let grant: serde_json::Value = serde_json::from_str(&execution_graph.grant.grant_json)?;
        let input_bound = if independent_root {
            grant
                .get("source_execution_id")
                .and_then(serde_json::Value::as_str)
                == Some(input_execution_id.as_str())
                && grant
                    .get("source_action_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(agent_action.action.id.as_str())
        } else {
            response_execution.parent_execution_id.as_deref() == Some(input_execution_id.as_str())
        };
        if !input_bound
            || agent_action.action.execution_id != input_execution_id.as_str()
            || agent_action.action.action_kind != "start_agent"
            || response_execution.parent_thread_id.as_deref() != Some(thread_model.id.as_str())
            || graph_response.turn_id != turn_model.id
            || graph_response.execution_id != response_execution.id
        {
            bail!("agent Turn input/response actors differ from its execution graph and action");
        }
        self.materialize_admitted_thread_turn(
            thread_model,
            sandbox_mode,
            turn_model,
            input,
            reasoning_effort,
            TurnWorkOwner::Turn,
            actor,
            audit_event,
            authority_envelope_json,
            None,
            Some(admission),
            Some(execution),
            execution_security_snapshot,
            security_audit_events,
            Some(execution_graph),
            None,
            Some(agent_action),
        )
        .await?
        .context("agent Turn transaction did not commit its execution graph")
    }

    /// Agent-authored Message variant of the same canonical event batch. The
    /// exact Turn actor and AgentAction receipt/outbox commit together; there
    /// is no interval in which either side can exist as a ghost.
    pub async fn materialize_completed_agent_message_turn_with_action(
        &self,
        write: CompletedMessageTurnWrite<'_>,
        agent_action: AgentCommitInput,
    ) -> Result<()> {
        if !matches!(write.actor, PersistedActorRef::AgentExecution(_)) {
            bail!("agent Message transaction requires an exact execution actor");
        }
        let updated_at = write.thread.updated_at;
        let events = completed_message_turn_events(write)?;
        self.materialize_turn_events_atomically_with_optional_admission(
            events,
            updated_at,
            None,
            None,
            None,
            None,
            None,
            Some(agent_action),
            None,
            None,
        )
        .await
        .map(|_| ())
    }

    /// Commits a projection-only Task delivery Turn as one canonical event
    /// batch. Agent output must bring its DeliverResult action. A routed
    /// failure remains visibly System-authored but still carries the exact
    /// source execution's route action so revocation cannot be bypassed by a
    /// lifecycle diagnostic.
    pub async fn materialize_completed_task_delivery_turn(
        &self,
        write: CompletedTaskDeliveryTurnWrite<'_>,
    ) -> Result<()> {
        let agent_authored = matches!(write.actor, PersistedActorRef::AgentExecution(_));
        let routed_system_diagnostic = matches!(write.actor, PersistedActorRef::System)
            && matches!(&write.item_completed.item, TurnItem::SystemEvent { .. })
            && write.agent_action.as_ref().is_some_and(|action| {
                action.mutation_kind == "deliver_result"
                    && action.action.action_kind == "deliver_result"
                    && action.route_receipt_json.is_some()
            });
        if (agent_authored && write.agent_action.is_none())
            || (!agent_authored && write.agent_action.is_some() && !routed_system_diagnostic)
        {
            bail!("Task delivery actor and canonical agent action differ");
        }
        if write.started_turn.mode != pioneer_protocol::ThreadMode::Chat
            || write.started_turn.status != TurnStatus::InProgress
            || write.completed.turn.status != TurnStatus::Completed
            || write.completed.turn.id != write.started_turn.id
            || write.completed.thread_id != write.thread.id
            || write.completed.workspace_id != write.thread.workspace_id
            || write.completed.turn.author != write.started_turn.author
            || write.audit_event.workspace_id != write.thread.workspace_id
            || write.audit_event.thread_id != write.thread.id
            || write.audit_event.turn_id != write.started_turn.id
            || write.item_started.workspace_id != write.thread.workspace_id
            || write.item_started.thread_id != write.thread.id
            || write.item_started.turn_id != write.started_turn.id
            || write.item_completed.workspace_id != write.thread.workspace_id
            || write.item_completed.thread_id != write.thread.id
            || write.item_completed.turn_id != write.started_turn.id
            || write.item_started.item != write.item_completed.item
        {
            bail!("Task delivery Turn batch contains inconsistent immutable facts");
        }
        match (write.started_turn.author.as_ref(), &write.actor) {
            (Some(author), actor) if &author.actor == actor => {}
            (None, PersistedActorRef::System) => {}
            _ => bail!("Task delivery Turn author differs from its event actor"),
        }
        let events = vec![
            TurnEventPayload::TurnStarted(TurnStartedEventPayload {
                thread: write.thread.clone(),
                sandbox_mode: write.sandbox_mode,
                turn: write.started_turn.clone(),
                input: Vec::new(),
                actor: Some(write.actor),
                reasoning_effort: None,
                work_owner: Default::default(),
            }),
            TurnEventPayload::TurnPermissionAudit(write.audit_event),
            TurnEventPayload::ItemStarted(write.item_started),
            TurnEventPayload::ItemCompleted(write.item_completed),
            TurnEventPayload::TurnCompleted(write.completed),
        ];
        self.materialize_turn_events_atomically_with_optional_admission(
            events,
            write.thread.updated_at,
            None,
            None,
            None,
            None,
            None,
            write.agent_action,
            None,
            None,
        )
        .await
        .map(|_| ())
    }

    /// Commits a failed projection-only Task delivery Turn as one canonical
    /// event batch. A routed failure may carry the source Agent's exact route
    /// receipt, but the visible Turn remains System-authored and contains no
    /// Agent work item or internal diagnostic text.
    pub async fn materialize_failed_task_delivery_turn(
        &self,
        write: FailedTaskDeliveryTurnWrite<'_>,
    ) -> Result<()> {
        if !matches!(write.actor, PersistedActorRef::System) {
            bail!("failed Task delivery Turn must be System-authored");
        }
        if write.agent_action.as_ref().is_some_and(|action| {
            action.mutation_kind != "deliver_result"
                || action.action.action_kind != "deliver_result"
                || action.route_receipt_json.is_none()
        }) {
            bail!("failed Task delivery action must be an exact routed DeliverResult receipt");
        }
        if write.started_turn.mode != pioneer_protocol::ThreadMode::Chat
            || write.started_turn.status != TurnStatus::InProgress
            || write.started_turn.author.is_some()
            || write.failed.turn.status != TurnStatus::Failed
            || write.failed.turn.id != write.started_turn.id
            || write.failed.turn.author != write.started_turn.author
            || write.failed.thread_id != write.thread.id
            || write.failed.workspace_id != write.thread.workspace_id
            || write.audit_event.workspace_id != write.thread.workspace_id
            || write.audit_event.thread_id != write.thread.id
            || write.audit_event.turn_id != write.started_turn.id
        {
            bail!("failed Task delivery Turn batch contains inconsistent immutable facts");
        }
        let events = vec![
            TurnEventPayload::TurnStarted(TurnStartedEventPayload {
                thread: write.thread.clone(),
                sandbox_mode: write.sandbox_mode,
                turn: write.started_turn.clone(),
                input: Vec::new(),
                actor: Some(write.actor),
                reasoning_effort: None,
                work_owner: Default::default(),
            }),
            TurnEventPayload::TurnPermissionAudit(write.audit_event),
            TurnEventPayload::TurnFailed(write.failed),
        ];
        self.materialize_turn_events_atomically_with_optional_admission(
            events,
            write.thread.updated_at,
            None,
            None,
            None,
            None,
            None,
            write.agent_action,
            None,
            None,
        )
        .await
        .map(|_| ())
    }

    /// Projects a Task result into an existing origin Turn. The two item
    /// lifecycle events and optional DeliverResult receipt are inseparable.
    pub async fn materialize_task_delivery_item(
        &self,
        started: pioneer_protocol::ItemStartedNotification,
        completed: pioneer_protocol::ItemCompletedNotification,
        event_timestamp_secs: i64,
        agent_action: Option<AgentCommitInput>,
    ) -> Result<()> {
        if started.workspace_id != completed.workspace_id
            || started.thread_id != completed.thread_id
            || started.turn_id != completed.turn_id
            || started.item != completed.item
        {
            bail!("Task delivery item lifecycle contains inconsistent immutable facts");
        }
        self.materialize_turn_events_atomically_with_optional_admission(
            vec![
                TurnEventPayload::ItemStarted(started),
                TurnEventPayload::ItemCompleted(completed),
            ],
            event_timestamp_secs,
            None,
            None,
            None,
            None,
            None,
            agent_action,
            None,
            None,
        )
        .await
        .map(|_| ())
    }

    /// Atomically creates a Member runtime draft and its immediately
    /// completed first Message Turn.
    pub async fn materialize_new_member_completed_message_turn_with_permission_audit(
        &self,
        write: CompletedMessageTurnWrite<'_>,
        gateway_id: &GatewayId,
        principal_id: &PrincipalId,
        access_class: PersistedThreadAccessClass,
    ) -> Result<()> {
        let updated_at = write.thread.updated_at;
        let actor = write.actor.clone();
        let events = completed_message_turn_events(write)?;
        self.materialize_turn_events_atomically_with_creation(
            events,
            updated_at,
            NewUserThreadCreation {
                creator: actor,
                access_class,
                member: Some((gateway_id.clone(), principal_id.clone())),
            },
        )
        .await
    }

    pub async fn materialize_new_member_native_turn_start_with_reasoning_effort_and_permission_audit(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
        actor: PersistedActorRef,
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
        gateway_id: &GatewayId,
        principal_id: &PrincipalId,
        access_class: PersistedThreadAccessClass,
        admission: NewTurnAdmission,
    ) -> Result<()> {
        self.materialize_new_user_turn_start_with_reasoning_effort_and_permission_audit(
            thread_model,
            sandbox_mode,
            turn_model,
            input,
            reasoning_effort,
            actor.clone(),
            audit_event,
            NewUserThreadCreation {
                creator: actor,
                access_class,
                member: Some((gateway_id.clone(), principal_id.clone())),
            },
            Some(admission),
        )
        .await
    }

    /// Atomically creates a Superuser runtime draft and its immediately
    /// completed first Message Turn.
    pub async fn materialize_new_superuser_completed_message_turn_with_permission_audit(
        &self,
        write: CompletedMessageTurnWrite<'_>,
        access_class: PersistedThreadAccessClass,
    ) -> Result<()> {
        if access_class == PersistedThreadAccessClass::Internal {
            bail!("new user Message thread cannot use internal access class");
        }
        let updated_at = write.thread.updated_at;
        let actor = write.actor.clone();
        let events = completed_message_turn_events(write)?;
        self.materialize_turn_events_atomically_with_creation(
            events,
            updated_at,
            NewUserThreadCreation {
                creator: actor,
                access_class,
                member: None,
            },
        )
        .await
    }

    pub async fn materialize_new_superuser_native_turn_start_with_reasoning_effort_and_permission_audit(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
        actor: PersistedActorRef,
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
        access_class: PersistedThreadAccessClass,
        admission: NewTurnAdmission,
    ) -> Result<()> {
        if access_class == PersistedThreadAccessClass::Internal {
            bail!("new user native thread cannot use internal access class");
        }
        self.materialize_new_user_turn_start_with_reasoning_effort_and_permission_audit(
            thread_model,
            sandbox_mode,
            turn_model,
            input,
            reasoning_effort,
            actor.clone(),
            audit_event,
            NewUserThreadCreation {
                creator: actor,
                access_class,
                member: None,
            },
            Some(admission),
        )
        .await
    }

    /// Atomically creates a Member-owned user thread, its private-thread
    /// creator membership when required, the first turn, and the permission
    /// audit. No empty thread row is visible if first-turn materialization
    /// fails.
    pub async fn materialize_new_member_turn_start_with_reasoning_effort_and_permission_audit(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
        actor: PersistedActorRef,
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
        gateway_id: &GatewayId,
        principal_id: &PrincipalId,
        access_class: PersistedThreadAccessClass,
    ) -> Result<()> {
        self.materialize_new_user_turn_start_with_reasoning_effort_and_permission_audit(
            thread_model,
            sandbox_mode,
            turn_model,
            input,
            reasoning_effort,
            actor.clone(),
            audit_event,
            NewUserThreadCreation {
                creator: actor,
                access_class,
                member: Some((gateway_id.clone(), principal_id.clone())),
            },
            None,
        )
        .await
    }

    /// Atomically creates a Superuser-owned thread and its first turn. The
    /// caller may choose only a user-addressable access class.
    pub async fn materialize_new_superuser_turn_start_with_reasoning_effort_and_permission_audit(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
        actor: PersistedActorRef,
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
        access_class: PersistedThreadAccessClass,
    ) -> Result<()> {
        self.materialize_new_user_turn_start_with_reasoning_effort_and_permission_audit(
            thread_model,
            sandbox_mode,
            turn_model,
            input,
            reasoning_effort,
            actor.clone(),
            audit_event,
            NewUserThreadCreation {
                creator: actor,
                access_class,
                member: None,
            },
            None,
        )
        .await
    }

    async fn materialize_new_user_turn_start_with_reasoning_effort_and_permission_audit(
        &self,
        thread_model: &Thread,
        sandbox_mode: SandboxMode,
        turn_model: &Turn,
        input: &[UserInput],
        reasoning_effort: Option<&str>,
        actor: PersistedActorRef,
        audit_event: pioneer_protocol::TurnPermissionAuditEvent,
        creation: NewUserThreadCreation,
        admission: Option<NewTurnAdmission>,
    ) -> Result<()> {
        let started_event = TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread: thread_model.clone(),
            sandbox_mode,
            turn: turn_model.clone(),
            input: input.to_vec(),
            actor: Some(actor),
            reasoning_effort: reasoning_effort.map(str::to_owned),
            work_owner: Default::default(),
        });
        self.materialize_turn_events_atomically_with_optional_admission(
            vec![
                started_event,
                TurnEventPayload::TurnPermissionAudit(audit_event),
            ],
            thread_model.updated_at,
            Some(creation),
            admission,
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
}
