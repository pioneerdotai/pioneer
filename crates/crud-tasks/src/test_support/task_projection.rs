use pioneer_protocol::{
    Task, TaskExecutorKind, TaskMetadata, TaskOwnerKind, TaskStatus, TaskTrigger, TaskTriggerSpec,
    TaskTriggerStatus, TaskValue,
};
use pioneer_protocol::{
    TaskAgentPrompt, TaskAgentResultContract, TaskAgentResultFormat, TaskAgentSpec, TaskRun,
    TaskRunStatus, TaskSchema,
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

pub fn sample_task_run(timestamp: i64) -> TaskRun {
    TaskRun {
        trigger_input: None,
        id: "run_00000000000000001".to_owned(),
        task_id: "task_0000000000000001".to_owned(),
        trigger_id: Some("trg_00000000000000001".to_owned()),
        parent_run_id: None,
        run_group_id: "run_00000000000000001".to_owned(),
        attempt_number: 1,
        retry_of_run_id: None,
        ready_at: Some(timestamp),
        run_number: 1,
        status: TaskRunStatus::Queued,
        executor_kind: TaskExecutorKind::Agent,
        started_at: None,
        completed_at: None,
        heartbeat_at: None,
        locked_by: None,
        lock_expires_at: None,
        result: None,
        error: None,
        created_at: timestamp,
        updated_at: timestamp,
    }
}

pub fn sample_task_agent_spec(timestamp: i64) -> TaskAgentSpec {
    TaskAgentSpec {
        id: "ags_00000000000000001".to_owned(),
        task_id: "task_0000000000000001".to_owned(),
        run_id: Some("run_00000000000000001".to_owned()),
        agent_role: Some("worker".to_owned()),
        agent_nickname: Some("Weather worker".to_owned()),
        model: Some("gpt-5.4".to_owned()),
        model_provider: Some("openai".to_owned()),
        prompt: TaskAgentPrompt {
            goal: "Check weather".to_owned(),
            instructions: Vec::new(),
            input: None,
            output_instructions: None,
        },
        context_policy: None,
        tool_policy: None,
        permission_cap: None,
        security_cap: None,
        result_contract: Some(TaskAgentResultContract {
            format: TaskAgentResultFormat::Json,
            required: true,
            schema: Some(TaskSchema {
                name: Some("weather_summary".to_owned()),
                description: None,
                schema: TaskValue::Object(BTreeMap::from([
                    ("type".to_owned(), TaskValue::String("object".to_owned())),
                    (
                        "required".to_owned(),
                        TaskValue::List(vec![TaskValue::String("summary".to_owned())]),
                    ),
                ])),
            }),
        }),
        review_policy: None,
        depth: 0,
        max_depth: 3,
        created_at: timestamp,
        updated_at: timestamp,
    }
}

use anyhow::Result;
use pioneer_protocol::TaskDelivery;
use sea_orm::ConnectionTrait;

#[derive(Clone, Copy)]
pub struct NoFrozenOutput;

impl crate::task_projector::TaskOutputBinder for NoFrozenOutput {
    async fn bind<'a, C: ConnectionTrait>(&'a self, _: &'a C, _: &'a TaskDelivery) -> Result<()> {
        anyhow::bail!("test does not queue a delivery")
    }
}
