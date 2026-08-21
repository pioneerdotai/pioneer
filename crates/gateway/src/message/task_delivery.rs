use super::*;
use crate::authorization::AuthorizationExternalError;
use anyhow::{Result, anyhow, bail};
use pioneer_protocol::{TaskDeliveryStatus, TaskGetResponse, TaskRunStatus, ThreadMode};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

struct PreparedTaskDeliveryAgentAction {
    presentation: pioneer_protocol::AgentPresentationSnapshot,
    author: pioneer_protocol::TurnAuthorSnapshot,
    actor: pioneer_protocol::PersistedActorRef,
    plan: crate::authorization::AgentActionCommitPlan,
}

impl MessageProcessor {
    pub(super) async fn task_user_notification_list(
        &self,
        request_context: &RequestContext,
        workspace: &crate::authorization::AuthorizedWorkspace,
        request_id: RequestId,
        params: pioneer_protocol::TaskUserNotificationListParams,
    ) {
        let connection_id = request_context.connection_id();
        if params.workspace_id.trim().is_empty()
            || params.workspace_id.trim() != workspace.workspace_id()
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    "invalid task notification workspace",
                ),
            )
            .await;
            return;
        }
        const DEFAULT_LIMIT: usize = 50;
        const HARD_LIMIT: usize = 100;
        let limit = params
            .limit
            .map(|value| value as usize)
            .unwrap_or(DEFAULT_LIMIT)
            .clamp(1, HARD_LIMIT);
        let principal_id = request_context.principal().principal_id.as_str();
        let before = match params.cursor.as_deref() {
            Some(cursor) if cursor.trim().is_empty() => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        "invalid task notification cursor",
                    ),
                )
                .await;
                return;
            }
            Some(cursor) => match pioneer_crud::find_user_notification_for_recipient(
                &self.crud_store.database_connection(),
                workspace.workspace_id(),
                principal_id,
                cursor,
            )
            .await
            {
                Ok(Some(row)) => Some((row.created_at.timestamp(), row.id)),
                Ok(None) => {
                    self.send_error(
                        connection_id,
                        AuthorizationExternalError::NotFound.response(request_id),
                    )
                    .await;
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        failure_class = "task_notification_cursor_resolution_failed",
                        "failed to resolve task notification cursor"
                    );
                    let _ = error;
                    self.send_error(
                        connection_id,
                        AuthorizationExternalError::Unavailable.response(request_id),
                    )
                    .await;
                    return;
                }
            },
            None => None,
        };
        let rows = match pioneer_crud::list_user_notifications_for_recipient(
            &self.crud_store.database_connection(),
            workspace.workspace_id(),
            principal_id,
            before
                .as_ref()
                .map(|(created_at, id)| (*created_at, id.as_str())),
            limit.saturating_add(1),
        )
        .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(
                    failure_class = "task_notification_list_failed",
                    "failed to list task notifications"
                );
                let _ = error;
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };
        let has_more = rows.len() > limit;
        let mut notifications = Vec::with_capacity(rows.len().min(limit));
        for row in rows.into_iter().take(limit) {
            match task_user_notification_from_row(row) {
                Ok(notification) => notifications.push(notification),
                Err(error) => {
                    tracing::warn!(
                        failure_class = "task_notification_projection_invalid",
                        "durable task notification is invalid"
                    );
                    let _ = error;
                    self.send_error(
                        connection_id,
                        AuthorizationExternalError::Unavailable.response(request_id),
                    )
                    .await;
                    return;
                }
            }
        }
        let next_cursor = has_more
            .then(|| {
                notifications
                    .last()
                    .map(|item| item.notification_id.clone())
            })
            .flatten();
        let response = pioneer_protocol::TaskUserNotificationListResponse {
            notifications,
            next_cursor,
        };
        match JsonRpcResponse::from_result(request_id.clone(), &response) {
            Ok(response) => {
                if let Err(error) = self.send_json(connection_id, &response).await {
                    tracing::warn!(
                        failure_class = "task_notification_response_delivery_failed",
                        "failed to send task notification inbox"
                    );
                    let _ = error;
                }
            }
            Err(error) => {
                tracing::warn!(
                    failure_class = "task_notification_response_encoding_failed",
                    "failed to encode task notification inbox"
                );
                let _ = error;
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_user_notification_acknowledge(
        &self,
        request_context: &RequestContext,
        workspace: &crate::authorization::AuthorizedWorkspace,
        request_id: RequestId,
        params: pioneer_protocol::TaskUserNotificationAcknowledgeParams,
    ) {
        let connection_id = request_context.connection_id();
        if params.workspace_id.trim().is_empty()
            || params.workspace_id.trim() != workspace.workspace_id()
            || params.notification_id.trim().is_empty()
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    "invalid task notification acknowledgement",
                ),
            )
            .await;
            return;
        }
        let row = match pioneer_crud::acknowledge_user_notification(
            &self.crud_store.database_connection(),
            workspace.workspace_id(),
            request_context.principal().principal_id.as_str(),
            params.notification_id.trim(),
            now_timestamp_secs(),
        )
        .await
        {
            Ok(Some(row)) => row,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            Err(error) => {
                tracing::warn!(
                    failure_class = "task_notification_acknowledgement_failed",
                    "failed to acknowledge task notification"
                );
                let _ = error;
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };
        let notification = match task_user_notification_from_row(row) {
            Ok(notification) => notification,
            Err(error) => {
                tracing::warn!(
                    failure_class = "task_notification_projection_invalid",
                    "acknowledged task notification is invalid"
                );
                let _ = error;
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };
        let response = pioneer_protocol::TaskUserNotificationAcknowledgeResponse { notification };
        match JsonRpcResponse::from_result(request_id.clone(), &response) {
            Ok(response) => {
                if let Err(error) = self.send_json(connection_id, &response).await {
                    tracing::warn!(
                        failure_class = "task_notification_response_delivery_failed",
                        "failed to send task notification acknowledgement"
                    );
                    let _ = error;
                }
            }
            Err(error) => {
                tracing::warn!(
                    failure_class = "task_notification_response_encoding_failed",
                    "failed to encode task notification acknowledgement"
                );
                let _ = error;
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
            }
        }
    }

    pub(super) async fn task_run_awaits_origin_thread_delivery(
        &self,
        task_response: &TaskGetResponse,
        run_id: &str,
    ) -> Result<bool> {
        let Some(current) = self
            .crud_store
            .get_task(task_response.task.id.as_str())
            .await?
        else {
            return Ok(false);
        };
        if !current
            .runs
            .iter()
            .any(|run| run.id == run_id && run.status == TaskRunStatus::Succeeded)
        {
            return Ok(false);
        }
        let deliveries = self
            .crud_store
            .list_task_deliveries(pioneer_protocol::TaskDeliveriesParams {
                workspace_id: current.task.workspace_id.clone(),
                task_id: Some(current.task.id.clone()),
                run_id: Some(run_id.to_owned()),
                statuses: vec![TaskDeliveryStatus::Pending, TaskDeliveryStatus::Delivering],
                limit: Some(100),
            })
            .await?;
        Ok(deliveries.deliveries.iter().any(|delivery| {
            delivery.mode == TaskDeliveryMode::Thread
                && delivery.thread_target
                    == Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread)
        }))
    }

    pub(super) async fn process_due_task_deliveries(&self, now: i64, limit: u64) -> Result<()> {
        self.task_runtime
            .service()
            .recover_stuck_deliveries(now, limit)
            .await
            .map_err(|error| anyhow!("{error:#}"))?;
        let deliveries = self.crud_store.list_due_task_deliveries(now, limit).await?;
        for delivery in deliveries {
            let Some((delivery, attempt)) = self
                .task_runtime
                .service()
                .start_delivery(delivery.id.as_str(), now)
                .await
                .map_err(|error| anyhow!("{error:#}"))?
            else {
                continue;
            };
            // Delivery can enter a deep CRUD projection chain. Run one
            // attempt from a fresh standard Tokio task so that chain does not
            // inherit the resilience worker or request-handler poll stack.
            let processor = self.clone();
            let delivery_execution = delivery.clone();
            let attempt_execution = attempt.clone();
            let execution_result = tokio::time::timeout(
                Duration::from_secs(60),
                message_fresh_task(async move {
                    processor
                        .execute_task_delivery(delivery_execution, attempt_execution)
                        .await
                }),
            )
            .await;
            let execution_result = match execution_result {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => Err(anyhow!("task delivery execution task failed: {error}")),
                Err(_) => Err(anyhow!(
                    "task delivery execution exceeded its 60 second lease"
                )),
            };
            if let Err(error) = execution_result {
                let failed_at = now_timestamp_secs();
                let _ = error;
                self.task_runtime
                    .service()
                    .fail_delivery(
                        delivery,
                        attempt,
                        "task_delivery_execution_failed".to_owned(),
                        None,
                        None,
                        failed_at,
                    )
                    .await
                    .map_err(|error| anyhow!("{error:#}"))?;
            }
        }
        Ok(())
    }

    async fn execute_task_delivery(
        &self,
        delivery: TaskDelivery,
        attempt: TaskDeliveryAttempt,
    ) -> Result<()> {
        if delivery.mode != TaskDeliveryMode::None {
            self.ensure_task_delivery_still_authorized(&delivery)
                .await?;
        }
        match delivery.mode {
            TaskDeliveryMode::None => {
                self.complete_delivery(delivery, attempt, None, None, None, None)
                    .await
            }
            TaskDeliveryMode::Thread => {
                let turn_id = match delivery
                    .thread_target
                    .context("thread delivery has no thread_target")?
                {
                    pioneer_protocol::TaskDeliveryThreadTarget::OriginThread => {
                        self.deliver_to_origin_thread(&delivery).await?
                    }
                    pioneer_protocol::TaskDeliveryThreadTarget::CurrentThread
                    | pioneer_protocol::TaskDeliveryThreadTarget::CollaborationRoot
                    | pioneer_protocol::TaskDeliveryThreadTarget::ExactThread => {
                        self.deliver_to_thread(&delivery).await?
                    }
                };
                self.complete_delivery(delivery, attempt, Some(turn_id), None, None, None)
                    .await
            }
            TaskDeliveryMode::UserNotification => {
                let notification = self.deliver_user_notification(&delivery).await?;
                let notification_id = notification.notification_id.clone();
                // The committed exact-recipient inbox row above is the
                // delivery receipt. Live fanout is deliberately best-effort:
                // an offline user recovers the same deterministic receipt,
                // and a crash/retry cannot create a second notification.
                let _ = self
                    .send_task_user_notification(
                        delivery.workspace_id.as_str(),
                        delivery
                            .target_user_id
                            .as_deref()
                            .context("user notification delivery has no recipient")?,
                        events::TASK_USER_NOTIFICATION_DELIVERED,
                        &notification,
                    )
                    .await;
                self.complete_delivery(delivery, attempt, None, Some(notification_id), None, None)
                    .await
            }
            TaskDeliveryMode::Webhook => self.deliver_webhook(delivery, attempt).await,
        }
    }

    async fn ensure_task_delivery_still_authorized(&self, delivery: &TaskDelivery) -> Result<()> {
        let Some(task_response) = self.crud_store.get_task(delivery.task_id.as_str()).await? else {
            bail!("task delivery root task is unavailable");
        };
        let task = task_response.task;
        if task.executor_kind == pioneer_protocol::TaskExecutorKind::System {
            return ensure_system_task_delivery_boundary(&task, delivery);
        }
        let admission = self
            .crud_store
            .get_task_execution_admission(task.id.as_str())
            .await?
            .context("Agent Task delivery has no durable execution admission")?;
        let context = crate::authorization::ExecutionAuthorizationContext::load_for_task_admission(
            self.crud_store.as_ref(),
            &admission,
        )
        .await
        .context("agent Task delivery execution admission is invalid")?;
        if admission.workspace_id != task.workspace_id
            || admission.workspace_id != delivery.workspace_id
            || admission.workspace_id != context.workspace_id()
            || admission.root_thread_id != context.root_thread_id()
            || admission.initiating_principal_id != context.initiating_principal_id().as_str()
        {
            bail!("task delivery differs from its durable execution boundary");
        }
        let current = self
            .execution_leases
            .revalidate_context(
                self.crud_store.as_ref(),
                &context,
                crate::authorization::ResourceAction::MessageCreate,
                self.current_authorization_revision().await?,
            )
            .await
            .context("task delivery has no current collaboration authority")?;
        let actor_contract = self
            .crud_store
            .get_task_actor_contract(task.id.as_str())
            .await?
            .context("Agent Task delivery has no actor contract")?;
        actor_contract
            .validate()
            .map_err(|error| anyhow!("Agent Task delivery actor contract is invalid: {error:?}"))?;

        match delivery.mode {
            TaskDeliveryMode::None => {}
            TaskDeliveryMode::UserNotification => {
                if delivery.target_user_id.as_deref()
                    != Some(context.initiating_principal_id().as_str())
                {
                    bail!("task delivery target does not match the initiating principal");
                }
            }
            TaskDeliveryMode::Webhook => {
                let role_key = pioneer_protocol::RoleKey::new(context.role_key().to_owned())
                    .context("task delivery execution role is invalid")?;
                let definition = crate::authorization::RoleDefinitionRegistry::new()
                    .resolve_key(&role_key)
                    .context("task delivery execution role is not registered")?;
                if definition.runtime_principal
                    != crate::authorization::RuntimePrincipalPolicy::Absolute
                {
                    bail!("webhook task delivery is unavailable to a scoped execution");
                }
            }
            TaskDeliveryMode::Thread => {
                let target_thread_id = delivery
                    .target_thread_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("task delivery has no target thread"))?;
                // A durable cross-capsule route is the destination authority.
                // Do not accidentally add the personal target access of one
                // currently selected source collaborator as a second gate:
                // the canonical DeliverResult plan below validates the exact
                // route/target/disclosure and the commit transaction repeats
                // its generation, expiry and destination checks.  Unrouted
                // same-capsule delivery still needs a current direct proof.
                if actor_contract.delivery.route_id.is_some() {
                    return Ok(());
                }
                let action = crate::authorization::ResourceAction::MessageCreate;
                let gate = crate::authorization::AuthorizationService::new().authorize_action(
                    current.principal().kind,
                    current.principal().role_key.as_ref(),
                    action,
                );
                let resolver = crate::authorization::AuthorizationResolver::new(
                    self.crud_store.as_ref().clone(),
                );
                let mut resolution = resolver
                    .authorize_thread(
                        current.principal(),
                        &gate,
                        action,
                        target_thread_id,
                        Some(delivery.workspace_id.as_str()),
                    )
                    .await?;
                if matches!(
                    resolution.denial(),
                    Some(crate::authorization::AuthorizationDecision::Deny {
                        reason: crate::authorization::DenyReason::MissingAuthoritativeResource,
                        ..
                    })
                ) {
                    resolution = resolver
                        .authorize_internal_thread_via_root(
                            current.principal(),
                            &gate,
                            action,
                            target_thread_id,
                            Some(delivery.workspace_id.as_str()),
                        )
                        .await?;
                }
                match resolution {
                    crate::authorization::ProofResolution::Authorized(proof) => {
                        crate::authorization::record_task_tool_decision(action, proof.decision());
                    }
                    crate::authorization::ProofResolution::Denied(decision) => {
                        crate::authorization::record_task_tool_decision(action, &decision);
                        bail!("task delivery target is no longer authorized");
                    }
                }
            }
        }
        Ok(())
    }

    async fn complete_delivery(
        &self,
        delivery: TaskDelivery,
        attempt: TaskDeliveryAttempt,
        delivered_turn_id: Option<String>,
        delivered_notification_id: Option<String>,
        http_status: Option<u16>,
        response_fingerprint: Option<String>,
    ) -> Result<()> {
        self.task_runtime
            .service()
            .complete_delivery(
                delivery,
                attempt,
                delivered_turn_id,
                delivered_notification_id,
                http_status,
                response_fingerprint,
                now_timestamp_secs(),
            )
            .await
            .map_err(|error| anyhow!("{error:#}"))?;
        Ok(())
    }

    async fn prepare_task_delivery_agent_action(
        &self,
        delivery: &TaskDelivery,
        target_thread_id: Option<&str>,
    ) -> Result<Option<PreparedTaskDeliveryAgentAction>> {
        let task_response = self
            .crud_store
            .get_task(delivery.task_id.as_str())
            .await?
            .context("Task delivery root is unavailable")?;
        if task_response.task.executor_kind == pioneer_protocol::TaskExecutorKind::System {
            return Ok(None);
        }
        let actor_contract = self
            .crud_store
            .get_task_actor_contract(delivery.task_id.as_str())
            .await?
            .context("Agent Task delivery has no actor contract")?;
        // Failed/cancelled/blocked runs remain visibly System-authored.  A
        // routed diagnostic nevertheless needs the same exact route action
        // and transactional receipt as successful cross-capsule delivery;
        // otherwise a revoke between execution and delivery could be
        // bypassed by the lifecycle path.
        if delivery.error_snapshot.is_some() && actor_contract.delivery.route_id.is_none() {
            return Ok(None);
        }
        let occurrence = self
            .crud_store
            .get_task_occurrence_contract_by_run(delivery.run_id.as_str())
            .await?
            .context("Agent Task delivery has no durable occurrence")?;
        let execution_id = occurrence
            .agent_execution_id
            .as_deref()
            .context("Agent Task result has no exact source execution")?;
        let database = self.crud_store.database_connection();
        let execution = pioneer_crud::load_agent_execution(&database, execution_id)
            .await?
            .context("Agent Task delivery source execution is unavailable")?;
        if execution.workspace_id != delivery.workspace_id
            || execution.parent_task_id.as_deref() != Some(delivery.task_id.as_str())
            || execution.work_graph_root_execution_id
                != occurrence
                    .work_graph_root_execution_id
                    .as_deref()
                    .context("Agent Task occurrence has no work-graph root")?
        {
            bail!("Agent Task delivery source differs from its occurrence boundary");
        }
        let source_fence =
            super::agent_action_tools::current_agent_identity_source_fence(self, execution_id)
                .await?;
        let agent_spec =
            super::task_agent_executor::select_agent_spec(&task_response, delivery.run_id.as_str())
                .context("Agent Task delivery has no execution specification")?;
        let policy_generation = self.current_authorization_revision().await?;
        let (mut adapter, _, _) =
            super::task_agent_executor::materialize_task_agent_action_binding_for_execution(
                self,
                &task_response.task,
                &agent_spec,
                execution_id,
                delivery.run_id.as_str(),
                execution.home_root_thread_id.as_str(),
                policy_generation,
                true,
            )
            .await
            .context("failed to restore exact Task delivery actor")?;

        let route_id = actor_contract.delivery.route_id.as_deref();
        let target = if let Some(route_id) = route_id {
            let route = pioneer_crud::load_agent_delegation_route(&database, route_id)
                .await?
                .context("Task delivery route is unavailable")?;
            let projection = pioneer_crud::agent_delegation_route_projection(&route)?;
            let route_facts = crate::authorization::AgentRouteFacts::from_projection(&projection)
                .map_err(|message| anyhow!(message))?;
            let exact_surface_target = match delivery.mode {
                TaskDeliveryMode::Thread => {
                    target_thread_id == Some(projection.destination_thread_id.as_str())
                }
                TaskDeliveryMode::UserNotification => target_thread_id.is_none(),
                TaskDeliveryMode::None | TaskDeliveryMode::Webhook => false,
            };
            if !exact_surface_target {
                bail!("Task delivery route target differs from the immutable destination");
            }
            adapter
                .bind_persisted_route(route_facts)
                .map_err(|error| anyhow!("Task delivery route is invalid: {error:?}"))?;
            pioneer_protocol::AgentStartTarget::RoutedThread {
                route_id: projection.id,
                thread_id: projection.destination_thread_id,
            }
        } else {
            let target_thread_id =
                target_thread_id.unwrap_or(execution.home_root_thread_id.as_str());
            if target_thread_id == execution.home_root_thread_id {
                pioneer_protocol::AgentStartTarget::SameCapsuleThread {
                    thread_id: target_thread_id.to_owned(),
                }
            } else if execution.parent_thread_id.as_deref() == Some(target_thread_id) {
                pioneer_protocol::AgentStartTarget::CurrentThread
            } else {
                bail!("Task result delivery outside its source capsule requires a durable route");
            }
        };
        let action_id = pioneer_protocol::AgentActionId::new(pioneer_crud::canonical_agent_id(
            'A',
            &format!("task-delivery-action\0{}", delivery.id),
        ))
        .map_err(|error| anyhow!("Task delivery action id is invalid: {error:?}"))?;
        let intent = pioneer_protocol::AgentActionIntent::DeliverResult {
            action_id,
            execution_id: pioneer_protocol::AgentExecutionId::new(execution_id.to_owned())
                .map_err(|error| anyhow!("Task delivery execution id is invalid: {error:?}"))?,
            route_id: route_id
                .map(|route_id| pioneer_protocol::AgentDelegationRouteId::new(route_id.to_owned()))
                .transpose()
                .map_err(|error| anyhow!("Task delivery route id is invalid: {error:?}"))?,
            target,
            result_reference: pioneer_protocol::task_delivery_result_item_id(delivery.id.as_str()),
            idempotency_key: delivery.delivery_key.clone(),
        };
        let prepared = adapter
            .prepare(&intent)
            .map_err(|error| anyhow!("Task delivery action was denied: {error:?}"))?;
        let mut plan = adapter
            .prepare_commit(
                &prepared,
                Some(
                    json!({
                        "deliveryId": delivery.id,
                        "taskId": delivery.task_id,
                        "runId": delivery.run_id,
                    })
                    .to_string(),
                ),
                adapter.policy_fingerprint(),
                policy_generation,
            )
            .map_err(|error| anyhow!("Task delivery action commit was denied: {error:?}"))?;
        if route_id.is_some()
            && task_response
                .task
                .delivery_policy
                .as_ref()
                .is_some_and(|policy| {
                    !policy.include_result
                        || policy.format == pioneer_protocol::TaskDeliveryFormat::Summary
                })
        {
            plan.input.disclosure_class = "routed_result_summary".to_owned();
        }
        super::agent_action_tools::apply_current_identity_source_fence(&mut plan, &source_fence);
        let presentation = adapter.presentation_snapshot();
        let author = presentation.to_turn_author_snapshot();
        let actor = author.actor.clone();
        Ok(Some(PreparedTaskDeliveryAgentAction {
            presentation,
            author,
            actor,
            plan,
        }))
    }

    async fn deliver_user_notification(
        &self,
        delivery: &TaskDelivery,
    ) -> Result<pioneer_protocol::TaskUserNotificationDeliveredNotification> {
        let recipient_principal_id = delivery
            .target_user_id
            .as_deref()
            .context("user notification delivery has no recipient")?;
        let agent_action = self
            .prepare_task_delivery_agent_action(delivery, None)
            .await?;
        let notification_id = task_user_notification_id(delivery.id.as_str());
        let notification = pioneer_protocol::TaskUserNotificationDeliveredNotification {
            notification_id: notification_id.clone(),
            workspace_id: delivery.workspace_id.clone(),
            recipient_principal_id: recipient_principal_id.to_owned(),
            task_id: delivery.task_id.clone(),
            run_id: delivery.run_id.clone(),
            delivery_id: delivery.id.clone(),
            author: agent_action
                .as_ref()
                .filter(|_| delivery.error_snapshot.is_none())
                .map(|prepared| prepared.presentation.clone()),
            delivery_action_receipt_id: agent_action
                .as_ref()
                .map(|prepared| prepared.plan.projection.receipt_id.clone()),
            result: delivery
                .result_snapshot
                .as_ref()
                .map(crate::task_projection::project_result),
            error: delivery
                .error_snapshot
                .as_ref()
                .map(crate::task_projection::project_error),
            created_at: delivery.created_at,
        };
        let payload_json = serde_json::to_string(&notification)
            .context("failed to encode durable user notification")?;
        let persisted = self
            .crud_store
            .materialize_task_notification(
                pioneer_crud::NewUserNotificationOutbox {
                    id: notification_id,
                    task_delivery_id: delivery.id.clone(),
                    workspace_id: delivery.workspace_id.clone(),
                    recipient_principal_id: recipient_principal_id.to_owned(),
                    task_id: delivery.task_id.clone(),
                    run_id: delivery.run_id.clone(),
                    payload_json,
                    created_at_unix: now_timestamp_secs(),
                },
                agent_action.map(|prepared| prepared.plan.input),
            )
            .await?;
        let notification = serde_json::from_str(persisted.payload_json.as_str())
            .context("durable user notification payload is invalid")?;
        if persisted.status != "delivered" {
            bail!("durable user notification receipt is not delivered");
        }
        Ok(notification)
    }

    async fn deliver_to_origin_thread(&self, delivery: &TaskDelivery) -> Result<String> {
        let thread_id = delivery
            .target_thread_id
            .as_deref()
            .ok_or_else(|| anyhow!("origin thread delivery has no target_thread_id"))?;

        if let Some(parent_turn_id) = self
            .lineage_parent_turn_for_origin_delivery(delivery, thread_id)
            .await?
        {
            let agent_action = self
                .prepare_task_delivery_agent_action(delivery, Some(thread_id))
                .await?;
            self.ensure_thread_loaded(thread_id, delivery.workspace_id.as_str())
                .await?;
            // The task card on the occurrence Turn is already the canonical
            // failed-state presentation. Do not duplicate its internal error
            // as an agent work item beside the card.
            if delivery.error_snapshot.is_none() {
                self.persist_delivery_item(
                    delivery.workspace_id.as_str(),
                    thread_id,
                    parent_turn_id.as_str(),
                    delivery_result_item(delivery),
                    agent_action.map(|prepared| prepared.plan.input),
                )
                .await?;
            }
            return Ok(parent_turn_id);
        }

        self.deliver_to_thread(delivery).await
    }

    async fn lineage_parent_turn_for_origin_delivery(
        &self,
        delivery: &TaskDelivery,
        target_thread_id: &str,
    ) -> Result<Option<String>> {
        if let Some(binding) = self
            .crud_store
            .get_task_run_primary_thread_binding(delivery.run_id.as_str())
            .await?
            && let Some(lineage) = self
                .crud_store
                .get_task_thread_lineage(binding.thread_id.as_str())
                .await?
        {
            let parent_thread_id = lineage
                .created_by_thread_id
                .as_deref()
                .unwrap_or(lineage.parent_thread_id.as_str());
            if parent_thread_id == target_thread_id {
                return Ok(lineage.created_by_turn_id);
            }
        }

        Ok(None)
    }

    async fn deliver_to_thread(&self, delivery: &TaskDelivery) -> Result<String> {
        let thread_id = delivery
            .target_thread_id
            .as_deref()
            .ok_or_else(|| anyhow!("thread delivery has no target_thread_id"))?;
        let agent_action = self
            .prepare_task_delivery_agent_action(delivery, Some(thread_id))
            .await?;
        self.ensure_thread_loaded(thread_id, delivery.workspace_id.as_str())
            .await?;
        let turn_id =
            pioneer_crud::canonical_agent_id('T', &format!("task-delivery-turn\0{}", delivery.id));
        let public_failure = delivery
            .error_snapshot
            .as_ref()
            .map(task_delivery_failure_message);
        let item = delivery
            .error_snapshot
            .is_none()
            .then(|| delivery_result_item(delivery));
        // A crash after the atomic destination commit but before Task delivery
        // acknowledgement must reuse the same Turn and unread projection.
        if let Some((workspace_id, existing_turn)) = self
            .crud_store
            .get_turn(thread_id, turn_id.as_str())
            .await?
        {
            let existing_item = self
                .crud_store
                .get_turn_item(
                    turn_id.as_str(),
                    pioneer_protocol::task_delivery_result_item_id(delivery.id.as_str()).as_str(),
                )
                .await?;
            if workspace_id != delivery.workspace_id
                || match (&public_failure, &item) {
                    (Some(error), None) => {
                        existing_turn.status != pioneer_protocol::TurnStatus::Failed
                            || existing_turn.error.as_deref() != Some(error.as_str())
                            || existing_item.is_some()
                    }
                    (None, Some(item)) => {
                        existing_turn.status != pioneer_protocol::TurnStatus::Completed
                            || existing_turn.error.is_some()
                            || existing_item.as_ref() != Some(item)
                    }
                    _ => true,
                }
            {
                bail!("deterministic Task delivery Turn conflicts with its immutable result");
            }
            if let Some(prepared) = agent_action.as_ref() {
                let action = pioneer_crud::load_agent_action(
                    &self.crud_store.database_connection(),
                    prepared.plan.projection.action_id.as_str(),
                )
                .await?;
                if action.as_ref().is_none_or(|action| {
                    action.status != "committed"
                        || action.action_kind != "deliver_result"
                        || action.execution_id != prepared.plan.input.action.execution_id
                        || action.idempotency_key != delivery.delivery_key
                }) {
                    bail!("agent-authored Task delivery Turn has no canonical action receipt");
                }
            }
            return Ok(turn_id);
        }
        let turn_params = TurnStartParams {
            agent_delegation_routes: Vec::new(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.clone(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            // Delivery is an internal system projection, not a user-authored ordinary
            // Message. Keep the pre-Epic-6 execution-free Chat semantics explicit so a
            // target thread whose composer default is Message cannot reclassify it.
            mode: Some(ThreadMode::Chat),
            agent_launch: None,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        };
        let permission_profile = pioneer_protocol::system_turn_permission_profile_snapshot(
            pioneer_protocol::TurnPermissionMode::FullAccess,
        );
        let visibly_agent_authored = agent_action.is_some() && delivery.error_snapshot.is_none();
        let turn_outcome = match agent_action.as_ref().filter(|_| visibly_agent_authored) {
            Some(prepared) => {
                self.thread_manager
                    .agent_turn_start_with_permission_profile_and_origin(
                        turn_params,
                        permission_profile,
                        prepared.author.clone(),
                        pioneer_protocol::TurnOrigin::TaskDelivery,
                    )
                    .await?
            }
            None => {
                self.thread_manager
                    .system_turn_start_with_permission_profile_and_origin(
                        turn_params,
                        permission_profile,
                        pioneer_protocol::TurnOrigin::TaskDelivery,
                    )
                    .await?
            }
        };
        let profile_selected_audit = match self.turn_profile_selected_audit_event(&turn_outcome) {
            Ok(event) => event,
            Err(error) => {
                self.thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                return Err(anyhow!(
                    "failed to resolve delivery turn permission profile: {error:#}"
                ));
            }
        };
        let projection_actor = match turn_outcome.materialization.turn.author.as_ref() {
            Some(author) => author.actor.clone(),
            None if visibly_agent_authored => {
                self.thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                bail!("Agent Task delivery Turn has no exact author snapshot");
            }
            None => pioneer_protocol::PersistedActorRef::System,
        };
        if visibly_agent_authored
            && agent_action
                .as_ref()
                .is_some_and(|prepared| prepared.actor != projection_actor)
        {
            self.thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            bail!("Task delivery projection actor differs from its canonical action");
        }
        let finish = match self
            .thread_manager
            .turn_finish(
                thread_id,
                turn_id.as_str(),
                if public_failure.is_some() {
                    pioneer_protocol::TurnStatus::Failed
                } else {
                    pioneer_protocol::TurnStatus::Completed
                },
                public_failure.clone(),
            )
            .await
        {
            Ok(finish) => finish,
            Err(error) => {
                self.thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                return Err(error);
            }
        };
        let started_notification = turn_outcome.started_notification.clone();
        match item {
            Some(item) => {
                let item_started = pioneer_protocol::ItemStartedNotification {
                    workspace_id: delivery.workspace_id.clone(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.clone(),
                    item: item.clone(),
                };
                let item_completed = pioneer_protocol::ItemCompletedNotification {
                    workspace_id: delivery.workspace_id.clone(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.clone(),
                    item,
                };
                let completed = pioneer_protocol::TurnCompletedNotification {
                    workspace_id: finish.workspace_id.clone(),
                    thread_id: finish.thread_id.clone(),
                    turn: finish.turn.clone(),
                };
                if let Err(error) = self
                    .crud_store
                    .materialize_completed_task_delivery_turn(
                        pioneer_crud::CompletedTaskDeliveryTurnWrite {
                            thread: &turn_outcome.materialization.thread,
                            sandbox_mode: turn_outcome.materialization.sandbox_mode,
                            started_turn: &turn_outcome.materialization.turn,
                            actor: projection_actor,
                            audit_event: profile_selected_audit,
                            item_started: item_started.clone(),
                            item_completed: item_completed.clone(),
                            completed: completed.clone(),
                            agent_action: agent_action.map(|prepared| prepared.plan.input),
                        },
                    )
                    .await
                {
                    self.thread_manager
                        .rollback_turn_finish(finish.rollback_context)
                        .await;
                    self.thread_manager
                        .rollback_turn_start(turn_outcome.rollback_context)
                        .await;
                    return Err(error);
                }
                self.send_notification_to_authorized_thread_connections(
                    thread_id,
                    events::TURN_STARTED,
                    &started_notification,
                    turn_outcome.started_notification_connection_ids.clone(),
                )
                .await;
                self.send_notification_to_thread_subscribers(
                    thread_id,
                    events::ITEM_STARTED,
                    &item_started,
                )
                .await;
                self.notify_semantic_timeline_item_changed(
                    item_started.workspace_id.as_str(),
                    item_started.thread_id.as_str(),
                    item_started.turn_id.as_str(),
                    &item_started.item,
                    Some("in_progress"),
                )
                .await;
                self.ingest_committed_thread_item(&item_completed).await;
                self.send_notification_to_thread_subscribers(
                    thread_id,
                    events::ITEM_COMPLETED,
                    &item_completed,
                )
                .await;
                self.notify_semantic_timeline_item_changed(
                    item_completed.workspace_id.as_str(),
                    item_completed.thread_id.as_str(),
                    item_completed.turn_id.as_str(),
                    &item_completed.item,
                    None,
                )
                .await;
                self.send_notification_to_thread_subscribers(
                    thread_id,
                    events::TURN_COMPLETED,
                    &completed,
                )
                .await;
            }
            None => {
                let failed = pioneer_protocol::TurnFailedNotification {
                    workspace_id: finish.workspace_id.clone(),
                    thread_id: finish.thread_id.clone(),
                    turn: finish.turn.clone(),
                };
                if let Err(error) = self
                    .crud_store
                    .materialize_failed_task_delivery_turn(
                        pioneer_crud::FailedTaskDeliveryTurnWrite {
                            thread: &turn_outcome.materialization.thread,
                            sandbox_mode: turn_outcome.materialization.sandbox_mode,
                            started_turn: &turn_outcome.materialization.turn,
                            actor: projection_actor,
                            audit_event: profile_selected_audit,
                            failed: failed.clone(),
                            agent_action: agent_action.map(|prepared| prepared.plan.input),
                        },
                    )
                    .await
                {
                    self.thread_manager
                        .rollback_turn_finish(finish.rollback_context)
                        .await;
                    self.thread_manager
                        .rollback_turn_start(turn_outcome.rollback_context)
                        .await;
                    return Err(error);
                }
                self.send_notification_to_authorized_thread_connections(
                    thread_id,
                    events::TURN_STARTED,
                    &started_notification,
                    turn_outcome.started_notification_connection_ids.clone(),
                )
                .await;
                self.send_notification_to_thread_subscribers(
                    thread_id,
                    events::TURN_FAILED,
                    &failed,
                )
                .await;
                self.notify_semantic_timeline_turn_state_changed(
                    failed.workspace_id.as_str(),
                    failed.thread_id.as_str(),
                    failed.turn.id.as_str(),
                )
                .await;
            }
        }
        self.notify_thread_tree_changed(delivery.workspace_id.clone())
            .await;
        Ok(turn_id)
    }

    pub(super) async fn ensure_thread_loaded(
        &self,
        thread_id: &str,
        workspace_id: &str,
    ) -> Result<()> {
        if self.thread_manager.thread_get(thread_id).await.is_some() {
            return Ok(());
        }
        let thread = self
            .crud_store
            .get_thread_model(thread_id)
            .await?
            .ok_or_else(|| anyhow!("delivery target thread `{thread_id}` not found"))?;
        let sandbox = self.crud_store.get_thread_sandbox_mode(thread_id).await?;
        self.thread_manager
            .system_thread_start_seeded(
                workspace_id.to_owned(),
                ThreadStartParams {
                    thread_id: thread.id.clone(),
                    workspace_id: workspace_id.to_owned(),
                    name: thread.name.clone(),
                    model: Some(thread.model.clone()),
                    model_provider: Some(thread.model_provider.clone()),
                    sandbox,
                    mode: Some(thread.mode),
                    origin_kind: Some(thread.origin_kind),
                    sidebar_visibility: Some(thread.sidebar_visibility),
                    visibility: None,
                    agent_nickname: thread.agent_nickname.clone(),
                    agent_role: thread.agent_role.clone(),
                },
                Some(thread),
                sandbox,
            )
            .await?;
        Ok(())
    }

    async fn persist_delivery_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item: TurnItem,
        agent_action: Option<pioneer_crud::AgentCommitInput>,
    ) -> Result<()> {
        let started = pioneer_protocol::ItemStartedNotification {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item: item.clone(),
        };
        let completed = pioneer_protocol::ItemCompletedNotification {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item,
        };
        let now = now_timestamp_secs();
        self.crud_store
            .materialize_task_delivery_item(started.clone(), completed.clone(), now, agent_action)
            .await?;
        self.send_notification_to_thread_subscribers(thread_id, events::ITEM_STARTED, &started)
            .await;
        self.notify_semantic_timeline_item_changed(
            started.workspace_id.as_str(),
            started.thread_id.as_str(),
            started.turn_id.as_str(),
            &started.item,
            Some("in_progress"),
        )
        .await;

        // Delivery projects an already-produced result into the target conversation. Index the
        // committed projection directly; do not run another turn or post-turn memory extractor.
        self.ingest_committed_thread_item(&completed).await;
        self.send_notification_to_thread_subscribers(thread_id, events::ITEM_COMPLETED, &completed)
            .await;
        self.notify_semantic_timeline_item_changed(
            completed.workspace_id.as_str(),
            completed.thread_id.as_str(),
            completed.turn_id.as_str(),
            &completed.item,
            None,
        )
        .await;
        Ok(())
    }

    async fn deliver_webhook(
        &self,
        delivery: TaskDelivery,
        attempt: TaskDeliveryAttempt,
    ) -> Result<()> {
        let webhook_url = delivery
            .webhook_url
            .as_deref()
            .ok_or_else(|| anyhow!("webhook delivery has no webhook_url"))?;
        validate_webhook_url(webhook_url)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let payload = self.webhook_payload(&delivery).await?;
        let response = client
            .post(webhook_url)
            .header("idempotency-key", delivery.delivery_key.as_str())
            .header("x-pioneer-delivery-id", delivery.id.as_str())
            .json(&payload)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            self.complete_delivery(delivery, attempt, None, None, Some(status.as_u16()), None)
                .await
        } else {
            self.task_runtime
                .service()
                .fail_delivery(
                    delivery,
                    attempt,
                    "task_webhook_delivery_rejected".to_owned(),
                    Some(status.as_u16()),
                    None,
                    now_timestamp_secs(),
                )
                .await
                .map_err(|error| anyhow!("{error:#}"))?;
            Ok(())
        }
    }

    async fn webhook_payload(&self, delivery: &TaskDelivery) -> Result<JsonValue> {
        let child_anchor = self
            .crud_store
            .get_task_run_child_anchor(delivery.run_id.as_str())
            .await?;
        Ok(json!({
            "event": if delivery.error_snapshot.is_some() { "task.run.failed" } else { "task.run.completed" },
            "deliveryId": delivery.id.clone(),
            "taskId": delivery.task_id.clone(),
            "runId": delivery.run_id.clone(),
            "status": if delivery.error_snapshot.is_some() { "failed" } else { "completed" },
            "result": delivery.result_snapshot.clone(),
            "error": delivery.error_snapshot.clone(),
            "childThreadId": child_anchor.child_thread_id,
            "childTurnId": child_anchor.child_turn_id,
            "occurredAt": delivery.updated_at,
        }))
    }
}

fn task_user_notification_from_row(
    row: pioneer_entity::user_notification_outbox::Model,
) -> Result<pioneer_protocol::TaskUserNotification> {
    let payload: pioneer_protocol::TaskUserNotificationDeliveredNotification =
        serde_json::from_str(row.payload_json.as_str())
            .context("durable user notification payload is invalid")?;
    if payload.notification_id != row.id
        || payload.workspace_id != row.workspace_id
        || payload.task_id != row.task_id
        || payload.run_id != row.run_id
        || payload.delivery_id != row.task_delivery_id
        || payload.recipient_principal_id != row.recipient_principal_id
    {
        bail!("durable user notification payload differs from its recipient envelope");
    }
    Ok(pioneer_protocol::TaskUserNotification {
        notification_id: payload.notification_id,
        workspace_id: payload.workspace_id,
        task_id: payload.task_id,
        run_id: payload.run_id,
        delivery_id: payload.delivery_id,
        author: payload.author,
        delivery_action_receipt_id: payload.delivery_action_receipt_id,
        result: payload.result,
        error: payload.error,
        created_at: payload.created_at,
        acknowledged_at: row.acknowledged_at.map(|value| value.timestamp()),
    })
}

/// System tasks are a Gateway-owned execution class, not a principal grant.
/// They cannot carry an Agent execution admission (enforced by `pioneer-tasks`)
/// and collaborative RPC callers cannot select this executor. Revalidate the
/// immutable delivery boundary here so an internal worker cannot redirect a
/// persisted System task to another workspace, owner, or thread.
fn ensure_system_task_delivery_boundary(
    task: &pioneer_protocol::Task,
    delivery: &TaskDelivery,
) -> Result<()> {
    if task.workspace_id != delivery.workspace_id {
        bail!("System Task delivery belongs to another workspace");
    }
    let policy = task
        .delivery_policy
        .as_ref()
        .context("System Task has no durable delivery policy")?;
    if policy.mode != delivery.mode {
        bail!("System Task delivery mode differs from its durable policy");
    }
    if policy.thread_target != delivery.thread_target {
        bail!("System Task delivery thread target differs from its durable policy");
    }
    match delivery.mode {
        TaskDeliveryMode::None => {}
        TaskDeliveryMode::Thread => {
            if policy.thread_id.as_deref() != delivery.target_thread_id.as_deref() {
                bail!("System Task thread delivery target is invalid");
            }
        }
        TaskDeliveryMode::UserNotification => {
            let expected = (task.owner_kind == pioneer_protocol::TaskOwnerKind::User)
                .then_some(task.owner_id.as_deref())
                .flatten();
            if expected != delivery.target_user_id.as_deref() {
                bail!("System Task notification recipient is invalid");
            }
        }
        TaskDeliveryMode::Webhook => {
            if policy.webhook_url.as_deref() != delivery.webhook_url.as_deref() {
                bail!("System Task webhook delivery target is invalid");
            }
        }
    }
    Ok(())
}

fn task_user_notification_id(delivery_id: &str) -> String {
    let digest = Sha256::digest(delivery_id.as_bytes());
    format!("un_{}", hex::encode(&digest[..9]))
}

fn delivery_result_item(delivery: &TaskDelivery) -> TurnItem {
    debug_assert!(delivery.error_snapshot.is_none());
    let text = delivery
        .result_snapshot
        .as_ref()
        .map(delivery_result_display_text)
        .filter(|text| !text.trim().is_empty())
        .unwrap_or_else(|| "Task completed.".to_owned());
    TurnItem::AgentMessage {
        id: pioneer_protocol::task_delivery_result_item_id(delivery.id.as_str()),
        phase: Default::default(),
        markdown: Some(super::markdown::parse_markdown_document(text.as_str())),
        markdown_version: Some(MARKDOWN_AST_VERSION),
        text,
    }
}

fn task_delivery_failure_message(error: &pioneer_protocol::TaskError) -> String {
    match error.code.as_str() {
        "task_executor_start_failed" => "Scheduled task could not start.".to_owned(),
        _ => "Scheduled task failed.".to_owned(),
    }
}

fn delivery_result_display_text(result: &pioneer_protocol::TaskResult) -> String {
    result
        .data
        .as_ref()
        .and_then(task_value_raw_text)
        .or(result.summary.as_deref())
        .unwrap_or("Task completed.")
        .to_owned()
}

fn task_value_raw_text(value: &pioneer_protocol::TaskValue) -> Option<&str> {
    match value {
        pioneer_protocol::TaskValue::String(text) => Some(text.as_str()),
        pioneer_protocol::TaskValue::Object(values) => values.get("rawText").and_then(|value| {
            if let pioneer_protocol::TaskValue::String(text) = value {
                Some(text.as_str())
            } else {
                None
            }
        }),
        _ => None,
    }
}

fn validate_webhook_url(url: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url)?;
    if parsed.scheme() != "https" {
        bail!("webhook URL must use https");
    }
    let Some(host) = parsed.host_str() else {
        bail!("webhook URL must include a host");
    };
    let normalized = host.trim_matches(['[', ']']).to_ascii_lowercase();
    if normalized == "localhost" || normalized.ends_with(".localhost") {
        bail!("webhook URL targets localhost");
    }
    if let Ok(ip) = normalized.parse::<IpAddr>()
        && is_private_ip(ip)
    {
        bail!("webhook URL targets a private network");
    }
    Ok(())
}

fn is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip == Ipv4Addr::UNSPECIFIED
                || ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1])
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || matches!(ip.segments()[0] & 0xfe00, 0xfc00)
                || matches!(ip.segments()[0] & 0xffc0, 0xfe80)
                || ip == Ipv6Addr::LOCALHOST
        }
    }
}
