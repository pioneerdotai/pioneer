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
- error snapshot;
- task events;
- delivery attempts;
- schedule and next run state;
- write locks and concurrency policy when relevant.

Then choose:

- retry/reschedule if the failure is transient and supported;
- update instructions if the future run prompt is wrong;
- cancel or pause if the task is no longer useful;
- explain clearly if the runtime lacks the required capability.
