# Durable Task Troubleshooting

Use this reference after confusing durable task behavior, missing delivered results, schema errors, noisy recurring tasks, or runtime surprises.

## Contents

- Result Exists But Is Not In The Main Thread
- `task_wait` Says There Is Nothing To Wait For
- Task Tool Schema Error
- Existing Scheduled Task Needs Editing
- Duplicate Or Noisy Scheduled Messages
- Delivery Update Did Not Affect Previous Run
- Stuck Or Failed Task
- Finish Accepted But Work Or Delivery Is Pending

## Result Exists But Is Not In The Main Thread

Likely causes:

- delivery mode is `user_notification`, `webhook`, or `none`;
- the task result is stored in task state but not delivered to a thread;
- delivery happened while the UI did not surface the notification;
- `includeResult:false` produced only a generic completion message.

What to inspect:

1. `task_get` for `deliveryPolicy`.
2. Latest task run status and result snapshot.
3. Delivery record, if available: mode, target thread, delivered turn id, notification id.
4. Parent thread timeline only after delivery state is understood.

Fix for future runs:

```json
{
  "taskId": "TASK_ID",
  "deliveryPolicy": {
    "mode": "thread",
    "threadTarget": "exact_thread",
    "threadId": "CURRENT_THREAD_ID",
    "includeResult": true,
    "format": "full_result"
  }
}
```

Tell the user this is not retroactive.

## `task_wait` Says There Is Nothing To Wait For

Scheduled, interval, and cron tasks often return `waitable:false` or `runId:null` at creation. There is no active run yet.

Do not keep calling `task_wait`. Confirm the schedule and next fire time.

## Task Tool Schema Error

Common causes:

- wrapping `trigger` in `spec`;
- using snake_case instead of camelCase;
- passing runtime-owned fields;
- using `delivery_policy` instead of `deliveryPolicy`;
- omitting `threadId` for `mode:"thread"`;
- omitting `webhookUrl` for `mode:"webhook"`;
- clearing scheduled task `outputInstructions`;
- trying to wait on a future non-waitable scheduled task.

Read `tool-schemas.md`, fix the payload, and retry only if the operation is safe or idempotent.

## Existing Scheduled Task Needs Editing

Use `task_get` first. Preserve fields the user did not ask to change.

For delivery-only changes, patch only `deliveryPolicy`.

For prompt changes on scheduled tasks, ensure `instructions` and `outputInstructions` remain self-contained for future runs.

## Duplicate Or Noisy Scheduled Messages

If a recurring task posts too much:

- adjust instructions so no-op runs return a compact no-op result;
- change delivery to notification or none if chat history should stay clean;
- post to a dedicated thread with `mode:"thread"`;
- keep full thread delivery only for meaningful changes.

## Delivery Update Did Not Affect Previous Run

This is expected. Delivery updates affect future terminal runs.

To show an old result, retrieve the run result and summarize it in the current answer or use a supported explicit posting path.

## Stuck Or Failed Task

Inspect:

- task status;
- latest run status;
- execution status, heartbeat, activity, and observation time from `task_get`;
- error snapshot;
- task events;
- delivery attempts;
- schedule and next run state;
- write locks and concurrency policy when relevant.

Do not infer a stall from an unchanged `running` run, no result, stale `updatedAt`, or missing activity data. A heartbeat is periodic liveness, and recorded activity does not promise a result. For active attached work, `task_wait.timedOut` only ends that wait window; the run may still be working and can be waited on again. Use the run status and error to identify an actual run timeout or failure before choosing a recovery action.

Then choose:

- retry/reschedule if the failure is transient and supported;
- update instructions if the future run prompt is wrong;
- pause for a temporary stop, finish for a user-requested permanent stop that lets current work finish, or cancel for interruption under existing rules;
- explain clearly if the runtime lacks the required capability.

## Finish Accepted But Work Or Delivery Is Pending

`task_finish` returning `alreadyFinished:false` confirms that the permanent stop request is accepted. It does not say that every active run has ended or that a result was delivered. A repeated request while work is finishing can return the same value. `alreadyFinished:true` means the task is already terminal; inspect its actual status rather than reporting success automatically.

Inspect these separately, using `task_get` for task/run state and the existing delivery records or destination when available:

1. `task.finishRequest`: the saved stop time, optional reason, and run boundary. It survives Gateway restart; future runs and queued work that has not started must not launch.
2. Task and run state: all already admitted parallel runs, review and required revisions may still be active. A normal finish becomes `completed`; an error, cancellation or review rejection remains an honest unsuccessful outcome. No new task attempt is started after finish.
3. Result and delivery state: preserved results follow configured delivery. Pending delivery does not mean computation is still running; a delivery error does not revive the task.

If there was no active work, a completed task without a new run or result is expected. Finish stops further work; it does not prove the original goal was achieved or invent a successful result.

Do not repeatedly finish, create a replacement task, or independently post to the delivery thread merely because work or delivery is pending. A repeated finish is safe, but it does not force completion or re-delivery. Resume, update and reschedule after finish are rejected. Use explicit cancel under its existing permissions if the user now wants remaining work interrupted.

A permitted execution can finish its own task. `task_cancel` still rejects self-cancellation and ancestor cancellation; finish does not remove ancestor or scope protections. For `task_access_denied`, inspect the current permissions and scope. For `task_state_conflict`, reload state and preserve the permanent stop decision.
