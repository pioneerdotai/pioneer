use super::*;
use anyhow::{Result, anyhow, bail};
use serde_json::json;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

const DELIVERY_TURN_ID_LEN: usize = 21;

impl MessageProcessor {
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
            if let Err(error) = self
                .execute_task_delivery(delivery.clone(), attempt.clone())
                .await
            {
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
        match delivery.mode {
            TaskDeliveryMode::None => {
                self.complete_delivery(delivery, attempt, None, None, None, None)
                    .await
            }
            TaskDeliveryMode::OwnerThread | TaskDeliveryMode::Thread => {
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
            .system_turn_start(TurnStartParams {
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.clone(),
                input: Vec::new(),
                model: None,
                model_provider: None,
                sandbox_policy: None,
                mode: Some(pioneer_protocol::ThreadMode::Chat),
            })
            .await?;
        if let Err(error) = self
            .crud_store
            .materialize_turn_start(
                &turn_outcome.materialization.thread,
                turn_outcome.materialization.sandbox_mode,
                &turn_outcome.materialization.turn,
                &turn_outcome.materialization.input,
            )
            .await
        {
            self.thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).map_err(|error| anyhow!("{error:#}"));
        }

        let task_item = self.task_turn_item_for_delivery(delivery).await?;
        self.persist_delivery_item(
            delivery.workspace_id.as_str(),
            thread_id,
            turn_id.as_str(),
            TurnItem::Task { item: task_item },
        )
        .await?;
        self.persist_delivery_item(
            delivery.workspace_id.as_str(),
            thread_id,
            turn_id.as_str(),
            delivery_summary_item(delivery),
        )
        .await?;
        self.complete_turn(thread_id.to_owned(), turn_id.clone(), None)
            .await;
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
        let notification = pioneer_protocol::ItemCompletedNotification {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item,
        };
        let now = now_timestamp_secs();
        self.crud_store
            .materialize_item_completed(notification.clone(), now)
            .await?;
        self.send_notification_to_thread_subscribers(
            thread_id,
            events::ITEM_COMPLETED,
            &notification,
        )
        .await;
        Ok(())
    }

    async fn task_turn_item_for_delivery(&self, delivery: &TaskDelivery) -> Result<TaskTurnItem> {
        let response = self
            .crud_store
            .get_task(delivery.task_id.as_str())
            .await?
            .ok_or_else(|| anyhow!("delivery task `{}` not found", delivery.task_id))?;
        let run = response
            .runs
            .iter()
            .rev()
            .find(|run| run.id == delivery.run_id);
        let trigger = response.triggers.iter().rev().next();
        let agent_spec = run
            .and_then(|run| {
                response
                    .agent_specs
                    .iter()
                    .rev()
                    .find(|spec| spec.run_id.as_deref() == Some(run.id.as_str()))
            })
            .or_else(|| response.agent_specs.iter().rev().next());
        let lineage = self
            .crud_store
            .list_thread_lineage_for_run(delivery.run_id.as_str())
            .await?
            .into_iter()
            .last();
        Ok(TaskTurnItem {
            id: format!("task_delivery_item_{}", delivery.id),
            task_id: response.task.id.clone(),
            run_id: Some(delivery.run_id.clone()),
            parent_task_id: response.task.parent_task_id.clone(),
            root_task_id: response.task.root_task_id.clone(),
            title: response.task.title.clone(),
            status: response.task.status,
            trigger_kind: trigger
                .map(TaskTrigger::kind)
                .unwrap_or(pioneer_protocol::TaskTriggerKind::Manual),
            executor_kind: response.task.executor_kind,
            child_thread_id: lineage
                .as_ref()
                .map(|lineage| lineage.child_thread_id.clone()),
            child_turn_id: lineage
                .as_ref()
                .map(|lineage| lineage.child_turn_id.clone()),
            agent_role: agent_spec.and_then(|spec| spec.agent_role.clone()),
            depth: agent_spec.map(|spec| spec.depth).unwrap_or(0),
            max_depth: agent_spec.map(|spec| spec.max_depth).unwrap_or(3),
            next_fire_at: trigger.and_then(|trigger| trigger.next_fire_at),
            result_preview: delivery
                .result_snapshot
                .as_ref()
                .and_then(|result| result.summary.clone()),
            error_preview: delivery
                .error_snapshot
                .as_ref()
                .map(|error| error.message.clone()),
            created_at: response.task.created_at,
            updated_at: response.task.updated_at,
        })
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
        let lineage = self
            .crud_store
            .list_thread_lineage_for_run(delivery.run_id.as_str())
            .await?
            .into_iter()
            .last();
        Ok(json!({
            "event": if delivery.error_snapshot.is_some() { "task.run.failed" } else { "task.run.completed" },
            "deliveryId": delivery.id.clone(),
            "taskId": delivery.task_id.clone(),
            "runId": delivery.run_id.clone(),
            "status": if delivery.error_snapshot.is_some() { "failed" } else { "completed" },
            "result": delivery.result_snapshot.clone(),
            "error": delivery.error_snapshot.clone(),
            "childThreadId": lineage.as_ref().map(|lineage| lineage.child_thread_id.clone()),
            "childTurnId": lineage.as_ref().map(|lineage| lineage.child_turn_id.clone()),
            "occurredAt": delivery.updated_at,
        }))
    }
}

fn delivery_summary_item(delivery: &TaskDelivery) -> TurnItem {
    if let Some(error) = delivery.error_snapshot.as_ref() {
        return TurnItem::SystemEvent {
            id: format!("task_delivery_result_{}", delivery.id),
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
        .and_then(|result| result.summary.clone())
        .unwrap_or_else(|| "Task completed.".to_owned());
    TurnItem::AgentMessage {
        id: format!("task_delivery_result_{}", delivery.id),
        markdown: Some(super::markdown::parse_markdown_document(text.as_str())),
        markdown_version: Some(MARKDOWN_AST_VERSION),
        text,
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
