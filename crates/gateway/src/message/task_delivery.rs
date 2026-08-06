use super::*;
use anyhow::{Result, anyhow, bail};
use pioneer_protocol::{TaskDeliveryStatus, TaskGetResponse, TaskRunStatus, ThreadMode};
use serde_json::json;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

const DELIVERY_TURN_ID_LEN: usize = 21;

impl MessageProcessor {
    pub(super) async fn task_run_awaits_owner_thread_delivery(
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
        Ok(deliveries
            .deliveries
            .iter()
            .any(|delivery| delivery.mode == TaskDeliveryMode::OwnerThread))
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
            let execution_result = message_fresh_task(async move {
                processor
                    .execute_task_delivery(delivery_execution, attempt_execution)
                    .await
            })
            .await;
            let execution_result = match execution_result {
                Ok(result) => result,
                Err(error) => Err(anyhow!("task delivery execution task failed: {error}")),
            };
            if let Err(error) = execution_result {
                let failed_at = now_timestamp_secs();
                self.task_runtime
                    .service()
                    .fail_delivery(
                        delivery,
                        attempt,
                        format!("{error:#}"),
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
            TaskDeliveryMode::OwnerThread => {
                let turn_id = self.deliver_to_owner_thread(&delivery).await?;
                self.complete_delivery(delivery, attempt, Some(turn_id), None, None, None)
                    .await
            }
            TaskDeliveryMode::Thread => {
                let turn_id = self.deliver_to_thread(&delivery).await?;
                self.complete_delivery(delivery, attempt, Some(turn_id), None, None, None)
                    .await
            }
            TaskDeliveryMode::UserNotification => {
                let notification_id = pioneer_protocol::generate_id(DELIVERY_TURN_ID_LEN);
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
        let root_task = match task_response.task.root_task_id.as_deref() {
            Some(root_task_id) => self
                .crud_store
                .get_task(root_task_id)
                .await?
                .map(|response| response.task)
                .ok_or_else(|| anyhow!("task delivery root task is unavailable"))?,
            None => task_response.task,
        };
        if root_task.owner_kind != pioneer_protocol::TaskOwnerKind::User {
            return Ok(());
        }
        let owner_id = root_task
            .owner_id
            .as_deref()
            .ok_or_else(|| anyhow!("user-owned task delivery has no initiating principal"))?;
        let root_thread_id = root_task
            .created_by_thread_id
            .as_deref()
            .or_else(|| {
                (root_task.owner_kind == pioneer_protocol::TaskOwnerKind::Thread)
                    .then_some(root_task.owner_id.as_deref())
                    .flatten()
            })
            .ok_or_else(|| anyhow!("user-owned task delivery has no authoritative root thread"))?;
        let root_turn_id = root_task
            .created_by_turn_id
            .as_deref()
            .ok_or_else(|| anyhow!("user-owned task delivery has no initiating turn"))?;
        let context = self
            .revalidate_execution_authorization_for_turn(
                root_task.workspace_id.as_str(),
                root_thread_id,
                root_turn_id,
                crate::authorization::ResourceAction::ThreadWrite,
            )
            .await
            .map_err(|_| {
                anyhow!("task delivery withheld because initiating authority is no longer active")
            })?;
        if context.initiating_principal_id().as_str() != owner_id {
            bail!("task delivery withheld because initiating principal lost root-thread access");
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

    async fn deliver_to_owner_thread(&self, delivery: &TaskDelivery) -> Result<String> {
        let thread_id = delivery
            .target_thread_id
            .as_deref()
            .ok_or_else(|| anyhow!("owner thread delivery has no target_thread_id"))?;

        if let Some(parent_turn_id) = self
            .lineage_parent_turn_for_owner_delivery(delivery, thread_id)
            .await?
        {
            self.ensure_delivery_thread_loaded(thread_id, delivery.workspace_id.as_str())
                .await?;
            self.persist_delivery_item(
                delivery.workspace_id.as_str(),
                thread_id,
                parent_turn_id.as_str(),
                delivery_summary_item(delivery),
            )
            .await?;
            return Ok(parent_turn_id);
        }

        self.deliver_to_thread(delivery).await
    }

    async fn lineage_parent_turn_for_owner_delivery(
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
        self.ensure_delivery_thread_loaded(thread_id, delivery.workspace_id.as_str())
            .await?;

        let turn_id = pioneer_protocol::generate_id(DELIVERY_TURN_ID_LEN);
        let turn_outcome = self
            .thread_manager
            .system_turn_start_with_permission_profile(
                TurnStartParams {
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
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
                },
                pioneer_protocol::system_turn_permission_profile_snapshot(
                    pioneer_protocol::TurnPermissionMode::FullAccess,
                ),
            )
            .await?;
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
        if let Err(error) = self
            .crud_store
            .materialize_turn_start_with_permission_audit(
                &turn_outcome.materialization.thread,
                turn_outcome.materialization.sandbox_mode,
                &turn_outcome.materialization.turn,
                &turn_outcome.materialization.input,
                pioneer_protocol::PersistedActorRef::System,
                profile_selected_audit,
            )
            .await
        {
            self.thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).map_err(|error| anyhow!("{error:#}"));
        }
        self.send_notification_to_authorized_thread_connections(
            thread_id,
            events::TURN_STARTED,
            &turn_outcome.started_notification,
            turn_outcome.started_notification_connection_ids.clone(),
        )
        .await;

        if let Err(error) = self
            .persist_delivery_item(
                delivery.workspace_id.as_str(),
                thread_id,
                turn_id.as_str(),
                delivery_summary_item(delivery),
            )
            .await
        {
            let reason = format!("failed to persist task delivery item: {error:#}");
            if !self
                .mark_turn_blocked(thread_id.to_owned(), turn_id.clone(), reason.clone())
                .await
            {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to durably close task delivery turn after item persistence failure"
                );
            }
            return Err(error);
        }
        if !self
            .complete_turn(thread_id.to_owned(), turn_id.clone(), None)
            .await
        {
            let reason = "failed to durably complete task delivery turn".to_owned();
            let _ = self
                .mark_turn_blocked(thread_id.to_owned(), turn_id.clone(), reason.clone())
                .await;
            bail!(reason);
        }
        Ok(turn_id)
    }

    async fn ensure_delivery_thread_loaded(
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
            .materialize_item_started(started.clone(), now)
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

        self.crud_store
            .materialize_item_completed(completed.clone(), now)
            .await?;
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
                    format!("webhook returned HTTP {}", status.as_u16()),
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

fn delivery_summary_item(delivery: &TaskDelivery) -> TurnItem {
    if let Some(error) = delivery.error_snapshot.as_ref() {
        return TurnItem::SystemEvent {
            id: pioneer_protocol::task_delivery_result_item_id(delivery.id.as_str()),
            level: SystemEventLevel::Error,
            message: error.message.clone(),
            code: Some(error.code.clone()),
            details: Some(json!({
                "taskId": delivery.task_id.clone(),
                "runId": delivery.run_id.clone(),
                "deliveryId": delivery.id.clone(),
            })),
        };
    }
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
