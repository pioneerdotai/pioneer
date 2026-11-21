//! Task terminal event and retry policy, prepared before acquiring a writer.
use anyhow::{Context, Result, bail};
use pioneer_protocol::{
    TaskError, TaskErrorClass, TaskEventPayload, TaskGetResponse, TaskResult, TaskRetryBackoffKind,
    TaskRun, TaskRunStatus, TaskWriteLockStatus, generate_id,
};
const ID_LEN: usize = 21;

pub fn terminal_facts(
    event: &TaskEventPayload,
) -> Result<(
    i64,
    Option<TaskResult>,
    Option<TaskError>,
    TaskWriteLockStatus,
    Option<String>,
)> {
    Ok(match event {
        TaskEventPayload::RunCompleted {
            result,
            completed_at,
            ..
        } => (
            *completed_at,
            result.clone(),
            None,
            TaskWriteLockStatus::Released,
            Some("run completed".into()),
        ),
        TaskEventPayload::RunFailed {
            error,
            completed_at,
            ..
        } => (
            *completed_at,
            None,
            error.clone(),
            TaskWriteLockStatus::Released,
            Some("run failed".into()),
        ),
        TaskEventPayload::RunBlocked {
            error, blocked_at, ..
        } => (
            *blocked_at,
            None,
            error.clone(),
            TaskWriteLockStatus::Released,
            Some("run blocked".into()),
        ),
        TaskEventPayload::RunCancelled {
            run_id,
            reason,
            cancelled_at,
            ..
        } => (
            *cancelled_at,
            None,
            reason.as_ref().map(|message| TaskError {
                recovery_diagnostic: None,
                code: "task_run_cancelled".into(),
                message: message.clone(),
                class: TaskErrorClass::Cancelled,
                details: None,
                failed_run_id: Some(run_id.clone()),
            }),
            TaskWriteLockStatus::Cancelled,
            reason.clone(),
        ),
        _ => bail!("expected a terminal Task run event"),
    })
}

pub fn prepare_retry(
    task_response: &TaskGetResponse,
    latest_run_number: i64,
    events: &mut Vec<TaskEventPayload>,
    error: Option<TaskError>,
    completed_at: i64,
) -> Result<bool> {
    let failed_run = task_response
        .runs
        .first()
        .context("missing prepared run")?
        .clone();
    let Some(policy) = task_response.task.retry_policy.as_ref() else {
        return Ok(false);
    };
    let error_class = error
        .as_ref()
        .map(|error| error.class)
        .unwrap_or(pioneer_protocol::TaskErrorClass::Unknown);
    if !policy.retry_on.iter().any(|class| *class == error_class) {
        return Ok(false);
    }
    if policy.max_attempts <= failed_run.attempt_number {
        events.push(TaskEventPayload::RunRetryExhausted {
            task_id: task_response.task.id.clone(),
            run_group_id: failed_run.run_group_id.clone(),
            final_run_id: failed_run.id.clone(),
            error,
            exhausted_at: completed_at,
        });
        return Ok(false);
    }

    let next_attempt = failed_run.attempt_number.saturating_add(1);
    let delay_seconds = retry_delay_seconds(policy, next_attempt)?;
    let ready_at = completed_at.saturating_add(delay_seconds);
    let retry_run = TaskRun {
        trigger_input: failed_run.trigger_input.clone(),
        id: generate_id(ID_LEN),
        task_id: task_response.task.id.clone(),
        trigger_id: failed_run.trigger_id.clone(),
        parent_run_id: Some(failed_run.id.clone()),
        run_group_id: failed_run.run_group_id.clone(),
        attempt_number: next_attempt,
        retry_of_run_id: Some(failed_run.id.clone()),
        ready_at: Some(ready_at),
        run_number: latest_run_number.saturating_add(1),
        status: TaskRunStatus::Queued,
        executor_kind: failed_run.executor_kind,
        started_at: None,
        completed_at: None,
        heartbeat_at: None,
        locked_by: None,
        lock_expires_at: None,
        result: None,
        error: None,
        created_at: completed_at,
        updated_at: completed_at,
    };
    let agent_spec = task_response
        .agent_specs
        .iter()
        .rev()
        .find(|spec| spec.run_id.as_deref() == Some(failed_run.id.as_str()))
        .or_else(|| {
            task_response
                .agent_specs
                .iter()
                .rev()
                .find(|spec| spec.run_id.is_none())
        })
        .cloned()
        .map(|mut spec| {
            spec.id = generate_id(ID_LEN);
            spec.run_id = Some(retry_run.id.clone());
            spec.updated_at = completed_at;
            spec
        });
    events.push(TaskEventPayload::TaskQueued {
        task_id: task_response.task.id.clone(),
        run_id: Some(retry_run.id.clone()),
    });
    events.push(TaskEventPayload::RunRetryScheduled {
        task_id: task_response.task.id.clone(),
        failed_run_id: failed_run.id,
        retry_run: retry_run.clone(),
        next_attempt_at: ready_at,
        reason: error,
    });
    events.push(TaskEventPayload::RunCreated {
        run: retry_run,
        agent_spec,
    });
    Ok(true)
}
fn retry_delay_seconds(
    policy: &pioneer_protocol::TaskRetryPolicy,
    next_attempt_number: u32,
) -> Result<i64> {
    let delay = match policy.backoff {
        TaskRetryBackoffKind::None => 0,
        TaskRetryBackoffKind::Fixed => policy.initial_delay_seconds.unwrap_or(0),
        TaskRetryBackoffKind::Exponential => {
            let initial = policy.initial_delay_seconds.unwrap_or(1).max(1);
            let exponent = next_attempt_number.saturating_sub(2).min(30);
            let multiplier = 1_i64.checked_shl(exponent).unwrap_or(i64::MAX);
            initial.saturating_mul(multiplier)
        }
    };
    let capped = policy
        .max_delay_seconds
        .map(|max_delay| delay.min(max_delay.max(0)))
        .unwrap_or(delay);
    Ok(capped.max(0))
}
