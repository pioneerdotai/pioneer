# Delivery And Scheduled Tasks

Use this reference when creating future work, recurring tasks, interval tasks, cron tasks, or changing where task results appear.

## Contents

- Product Meaning
- Delivery Modes
- Choosing Summary Vs Full Result
- Scheduled Task Instructions
- No-Op Runs
- Updating Delivery
- Existing Results Are Not Retroactive
- Confirmation Message

## Product Meaning

Task result production and task result delivery are separate.

The task run can finish successfully, create a result candidate, and store a result snapshot. That does not mean the main thread gets an assistant message. Delivery decides the user-facing surface.

Use this mental model:

- **Task state** is the durable source of truth for what ran and what result was produced.
- **Thread delivery** turns the result into a chat message.
- **Notification delivery** alerts the user but may not create durable chat history.
- **Webhook delivery** sends the result outside Pioneer.

## Delivery Modes

### thread + origin_thread

Use when the user says:

```text
Send the answer back here.
Post the report in this conversation.
Every morning, check this and tell me here.
```

`origin_thread` targets the visible thread where the work originated. It is the default for scheduled tasks created from a thread.

```json
{
  "deliveryPolicy": {
    "mode": "thread",
    "threadTarget": "origin_thread",
    "includeResult": true,
    "format": "full_result"
  }
}
```

### thread

Use `exact_thread` when you know the exact target thread id or are updating an existing task to point to a specific thread.

```json
{
  "deliveryPolicy": {
    "mode": "thread",
    "threadTarget": "exact_thread",
    "threadId": "THREAD_ID",
    "includeResult": true,
    "format": "full_result"
  }
}
```

This is explicit and good for "change this task so answers come to this thread."

### user_notification

Use for lightweight alerts where the user does not need a durable chat message.

Examples:

```text
Notify me if the build breaks.
Ping me when the export is done.
Alert me if there is a new incident.
```

Avoid `user_notification` when the user asks for a report, answer, summary, or "send it here." A notification may be transient or displayed in a different UI surface.

### webhook

Use when the result belongs in an external system.

### none

Use for background state, task chains, or internal bookkeeping where no user-facing delivery is needed.

## Choosing Summary Vs Full Result

Use `includeResult:true` when the user expects text in the delivery.

Use `format:"full_result"` when markdown structure, sections, citations, or artifacts matter.

Use `format:"summary"` for compact notifications, dashboards, or routine status where a short preview is enough.

If `includeResult:false`, the delivered thread message may only say that the task completed. Do not use it for "send me the answer."

## Scheduled Task Instructions

Scheduled tasks run in the future. They may not have the creation-time chat context, exact tool list, skills, MCP servers, or current web state.

Good scheduled instructions:

- say what to do step by step;
- include the target URLs, repos, paths, ids, dates, and timezones;
- say how to choose tools by capability;
- define no-op behavior;
- define failure behavior;
- define output language and format;
- avoid relying on hidden parent reasoning.

Bad scheduled instructions:

```text
Do the same as above every day.
Use the tool I just used.
Keep track of it and continue.
Send it to me.
```

Better:

```text
Each run, fetch the latest GitHub release for owner/repo through any available web or HTTP tool. Compare it with the configured baseline or available durable state. If the release is unchanged, return "No new release." If it changed, summarize the changelog in Russian Markdown for product readers. If release data cannot be fetched, fail clearly with the URL and error.
```

## No-Op Runs

Decide whether no-op runs should be visible.

For monitors, users often want:

- no chat message when nothing changed;
- a chat message only when something important happened;
- optional task state showing that the check ran.

If the system cannot suppress no-op delivery, instruct the task to return a very compact no-op result and choose delivery accordingly. For high-noise recurring checks, prefer notification/dashboard surfaces over thread delivery unless the user explicitly wants every run in the thread.

## Updating Delivery

When the user says a result did not appear in the main thread:

1. Inspect the task with `task_get`.
2. Check `deliveryPolicy.mode`.
3. If it is `user_notification`, `webhook`, or `none`, explain that it did not write a chat message.
4. Update future delivery to `thread` with the appropriate `threadTarget`.
5. Verify the updated policy.
6. Tell the user the change applies to future runs.

Example update:

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

## Existing Results Are Not Retroactive

Changing delivery does not automatically insert old results into the parent thread.

If the user wants a prior run result posted, inspect the run/result snapshot and either:

- summarize it in the current parent answer; or
- create a new explicit delivery/posting action if the tools support it.

Do not imply that `task_update` will rewrite prior turns.

## Confirmation Message

After creating or updating scheduled work, tell the user:

- task title and id;
- schedule and timezone;
- next fire time when available;
- delivery destination;
- whether no-op runs will be visible.

Good confirmation:

```text
Done. The Hermes Agent release monitor now runs daily at 05:00 Europe/Moscow. Future results will be posted to this thread as full Markdown, not just as a user notification. The change applies to the next run; today's earlier run was not retroactively posted.
```
