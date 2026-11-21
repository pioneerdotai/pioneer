use pioneer_protocol::{
    Task, TaskExecutorKind, TaskMetadata, TaskOwnerKind, TaskStatus, TaskTrigger, TaskTriggerSpec,
    TaskTriggerStatus, TaskValue,
};
use std::collections::BTreeMap;

pub fn sample_task(timestamp: i64) -> Task {
    Task {
        id: "task_0000000000000001".to_owned(),
        workspace_id: "ws_task".to_owned(),
        owner_kind: TaskOwnerKind::Thread,
        owner_id: Some("thr_task".to_owned()),
        created_by_thread_id: Some("thr_task".to_owned()),
        created_by_turn_id: Some("turn_task".to_owned()),
        root_task_id: None,
        parent_task_id: None,
        executor_kind: TaskExecutorKind::Agent,
        status: TaskStatus::Scheduled,
        title: "Check weather".to_owned(),
        goal: "Send the daily weather summary".to_owned(),
        priority: 10,
        lifecycle_policy: None,
        delivery_policy: None,
        retry_policy: None,
        timeout_policy: None,
        concurrency_policy: None,
        metadata: Some(TaskMetadata {
            labels: vec!["weather".to_owned()],
            data: Some(TaskValue::Object(BTreeMap::from([(
                "city".to_owned(),
                TaskValue::String("Berlin".to_owned()),
            )]))),
            finish_request: None,
            composer_work: None,
        }),
        result: None,
        error: None,
        revision: 0,
        created_at: timestamp,
        updated_at: timestamp,
        completed_at: None,
    }
}

pub fn sample_task_trigger(timestamp: i64) -> TaskTrigger {
    TaskTrigger {
        webhook_secret_ref: None,
        id: "trg_00000000000000001".to_owned(),
        task_id: "task_0000000000000001".to_owned(),
        status: TaskTriggerStatus::Active,
        spec: TaskTriggerSpec::ScheduledAt {
            scheduled_at: timestamp + 3600,
            timezone: Some("Europe/Berlin".to_owned()),
            catch_up_policy: None,
        },
        next_fire_at: Some(timestamp + 3600),
        last_fire_at: None,
        created_at: timestamp,
        updated_at: timestamp,
    }
}
