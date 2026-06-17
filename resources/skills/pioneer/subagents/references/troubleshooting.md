# Task Troubleshooting

Use this reference after confusing task behavior, missing results, schema errors, or runtime surprises.

## Contents

- Result Exists But Is Not In The Main Thread
- `task_wait` Says There Is Nothing To Wait For
- Review Required Is Not Completion
- Child Result Is Bad Or Missing Evidence
- Task Tool Schema Error
- Existing Scheduled Task Needs Editing
- Duplicate Or Noisy Scheduled Messages
- Parent Finished Too Early
- Delivery Update Did Not Affect Previous Run
- Stuck Or Failed Task

## Result Exists But Is Not In The Main Thread

Likely causes:

- delivery mode is `user_notification`, `webhook`, or `none`;
- task result is in a hidden child thread and task state only;
- the parent did not wait/review/accept an attached candidate;
- delivery happened while the UI did not surface the notification;
- `includeResult:false` produced only a generic completion message.

What to inspect:

1. `task_get` for `deliveryPolicy`.
2. Latest task run status and result snapshot.
3. Result candidates and whether one is accepted.
4. Parent thread item: task anchor may have only `resultPreview`.
5. Delivery record, if available: mode, target thread, delivered turn id, notification id.

Fix for future runs:

```json
{
  "taskId": "TASK_ID",
  "deliveryPolicy": {
    "mode": "thread",
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

## Review Required Is Not Completion

If `task_wait` returns `reviewRequired`, the child is asking the parent to decide.

Do not treat the candidate as accepted. Inspect it and then:

- `task_accept` if it is good;
- `task_revise` if it needs focused correction;
- `task_cancel` if it should not be used.

## Child Result Is Bad Or Missing Evidence

Use `task_revise` with specific feedback. Avoid vague prompts.

Bad:

```text
Improve this.
```

Good:

```text
Add file:line evidence for the claim that user_notification does not persist an AgentMessage. Include the exact delivery mode behavior and keep the rest of the answer concise.
```

## Task Tool Schema Error

Common causes:

- using `taskId` instead of `taskIds` in `task_wait`;
- wrapping `trigger` in `spec`;
- using snake_case instead of camelCase;
- passing runtime-owned fields;
- using `delivery_policy` instead of `deliveryPolicy`;
- omitting `threadId` for `mode:"thread"`;
- omitting `webhookUrl` for `mode:"webhook"`;
- clearing scheduled task `outputInstructions`.

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

## Parent Finished Too Early

If the parent answered while attached tasks were still active or waiting for review, the answer is incomplete.

Recover by:

1. waiting on the active runs;
2. reviewing candidates;
3. accepting/revising/cancelling;
4. sending a corrected synthesis.

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
- write locks and concurrency policy when relevant.

Then choose:

- retry/reschedule if the failure is transient and supported;
- update instructions if the future run prompt is wrong;
- cancel if the task is no longer useful;
- explain clearly if the runtime lacks the required capability.
