//! Prepare terminal Task delivery targets and frozen result snapshots.
use pioneer_protocol::{
    TaskDeliveryMode, TaskDeliveryStatus, TaskError, TaskGetResponse, TaskResult, generate_id,
};
use sha2::{Digest, Sha256};

const ID_LEN: usize = 21;

fn delivery_target_fingerprint(target: &str) -> String {
    hex::encode(Sha256::digest(target.as_bytes()))
}

pub fn delivery_for_terminal_run(
    task_response: &TaskGetResponse,
    policy: Option<&pioneer_protocol::TaskDeliveryPolicy>,
    run_id: &str,
    event_timestamp_secs: i64,
    result_snapshot: Option<TaskResult>,
    error_snapshot: Option<TaskError>,
    exact_notification_recipient: Option<&str>,
) -> Option<pioneer_protocol::TaskDelivery> {
    let policy = policy?;
    if policy.mode == TaskDeliveryMode::None {
        return None;
    }
    let thread_target = (policy.mode == TaskDeliveryMode::Thread)
        .then_some(policy.thread_target)
        .flatten();
    if policy.mode == TaskDeliveryMode::Thread && thread_target.is_none() {
        return None;
    }
    let target_thread_id = match policy.mode {
        TaskDeliveryMode::Thread => policy.thread_id.clone(),
        _ => None,
    };
    let target_user_id = (policy.mode == TaskDeliveryMode::UserNotification)
        .then(|| {
            exact_notification_recipient.map(str::to_owned).or_else(|| {
                (task_response.task.owner_kind == pioneer_protocol::TaskOwnerKind::User)
                    .then(|| task_response.task.owner_id.clone())
                    .flatten()
            })
        })
        .flatten();
    let webhook_url = (policy.mode == TaskDeliveryMode::Webhook)
        .then(|| policy.webhook_url.clone())
        .flatten();
    if policy.mode == TaskDeliveryMode::Thread && target_thread_id.is_none() {
        return None;
    }
    if policy.mode == TaskDeliveryMode::UserNotification && target_user_id.is_none() {
        return None;
    }
    if policy.mode == TaskDeliveryMode::Webhook && webhook_url.is_none() {
        return None;
    }
    let run = task_response.runs.iter().rev().find(|run| run.id == run_id);
    let result_snapshot = if policy.include_result {
        result_snapshot
            .or_else(|| run.and_then(|run| run.result.clone()))
            .or_else(|| task_response.task.result.clone())
            .map(|result| task_delivery_result_snapshot(result, policy.format))
    } else {
        None
    };
    let error_snapshot = error_snapshot.or_else(|| {
        run.and_then(|run| run.error.clone())
            .or_else(|| task_response.task.error.clone())
    });
    let target = target_thread_id
        .clone()
        .or_else(|| target_user_id.clone())
        .or_else(|| webhook_url.clone())
        .unwrap_or_else(|| "none".to_owned());
    let delivery_key = match thread_target {
        Some(thread_target) => format!(
            "{}:{}:{}:{}:{}",
            task_response.task.id,
            run_id,
            delivery_mode_key(policy.mode),
            delivery_thread_target_key(thread_target),
            target
        ),
        None => format!(
            "{}:{}:{}:{}",
            task_response.task.id,
            run_id,
            delivery_mode_key(policy.mode),
            target
        ),
    };
    Some(pioneer_protocol::TaskDelivery {
        id: generate_id(ID_LEN),
        workspace_id: task_response.task.workspace_id.clone(),
        task_id: task_response.task.id.clone(),
        run_id: run_id.to_owned(),
        delivery_key,
        mode: policy.mode,
        thread_target,
        target_thread_id,
        target_user_id,
        webhook_url: webhook_url.clone(),
        webhook_url_fingerprint: webhook_url.as_deref().map(delivery_target_fingerprint),
        status: TaskDeliveryStatus::Pending,
        next_attempt_at: Some(event_timestamp_secs),
        attempt_count: 0,
        // Every surface is an idempotent outbox delivery. A crash after the
        // destination commit but before acknowledgement must retry the same
        // deterministic receipt instead of stranding it after one attempt.
        max_attempts: 3,
        result_snapshot,
        error_snapshot,
        delivered_turn_id: None,
        delivered_notification_id: None,
        delivered_at: None,
        last_error: None,
        created_at: event_timestamp_secs,
        updated_at: event_timestamp_secs,
    })
}

pub fn task_delivery_result_snapshot(
    result: TaskResult,
    format: pioneer_protocol::TaskDeliveryFormat,
) -> TaskResult {
    match format {
        pioneer_protocol::TaskDeliveryFormat::FullResult => result,
        pioneer_protocol::TaskDeliveryFormat::Summary => TaskResult {
            summary: result.summary,
            data: None,
            artifacts: Vec::new(),
            completed_by_run_id: result.completed_by_run_id,
        },
    }
}

fn delivery_mode_key(mode: TaskDeliveryMode) -> &'static str {
    match mode {
        TaskDeliveryMode::None => "none",
        TaskDeliveryMode::Thread => "thread",
        TaskDeliveryMode::UserNotification => "user_notification",
        TaskDeliveryMode::Webhook => "webhook",
    }
}

fn delivery_thread_target_key(target: pioneer_protocol::TaskDeliveryThreadTarget) -> &'static str {
    match target {
        pioneer_protocol::TaskDeliveryThreadTarget::OriginThread => "origin_thread",
        pioneer_protocol::TaskDeliveryThreadTarget::CurrentThread => "current_thread",
        pioneer_protocol::TaskDeliveryThreadTarget::CollaborationRoot => "collaboration_root",
        pioneer_protocol::TaskDeliveryThreadTarget::ExactThread => "exact_thread",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{TaskArtifact, TaskDeliveryFormat, TaskValue};
    use std::collections::BTreeMap;

    #[test]
    fn summary_delivery_snapshot_drops_full_data_and_artifacts() {
        let result = TaskResult {
            summary: Some("safe summary".to_owned()),
            data: Some(TaskValue::Object(BTreeMap::from([(
                "secret".to_owned(),
                TaskValue::String("must-not-cross".to_owned()),
            )]))),
            artifacts: vec![TaskArtifact {
                artifact_id: Some("artifact-1".to_owned()),
                version_id: Some("version-1".to_owned()),
                path: None,
                url: None,
                mime_type: None,
                metadata: None,
            }],
            completed_by_run_id: Some("run-1".to_owned()),
        };

        let projected = task_delivery_result_snapshot(result, TaskDeliveryFormat::Summary);
        assert_eq!(projected.summary.as_deref(), Some("safe summary"));
        assert!(projected.data.is_none());
        assert!(projected.artifacts.is_empty());
        assert_eq!(projected.completed_by_run_id.as_deref(), Some("run-1"));
    }
}
