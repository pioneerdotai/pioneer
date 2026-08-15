---
name: tasks
slug: tasks
owner: pioneer
description: "Use this skill when Pioneer should create, inspect, update, reschedule, pause, resume, troubleshoot, or configure delivery for durable tasks, scheduled tasks, recurring tasks, interval tasks, cron jobs, background work, or existing task state."
version: "0.1.0"
user-invocable: true
disable-model-invocation: false
implicit-invocation: required
catalog-hide: true
---

# Pioneer Durable Tasks

Durable tasks are product objects that can outlive the current turn. They have task state, run history, result snapshots, attempts, diagnostics, schedules, and delivery behavior.

Use this skill for future, recurring, background, or already-existing task work. For attached child agents created to help with the current turn, read `system:pioneer/subagents` instead.

## Product Model

Think in three surfaces:

- **Task state**: durable source of truth for what should run, what did run, results, attempts, errors, and diagnostics.
- **Run execution**: a concrete attempt to perform the task now or later.
- **Delivery**: the policy that decides whether a terminal task result becomes a thread message, user notification, webhook call, or no visible output.

Do not confuse "the task produced a result" with "the user saw the result in this thread." Execution and delivery are separate.

## Quick Start

Before using durable task tools, decide what kind of task work this is.

1. If work should run later or repeatedly, create a scheduled, interval, or cron task with self-contained future-run instructions.
2. If existing task behavior must change, inspect it first with `task_get` or `task_list`.
3. If only the schedule changes, use `task_reschedule` when available.
4. If delivery changes, patch only delivery and verify it.
5. If the task should be temporarily stopped or restarted, use pause/resume tools when visible.
6. If a result should appear in this chat later, configure thread delivery deliberately.
7. If the result should only alert the user, use notification delivery only when a durable chat message is not required.

For future scheduled work, do not call `task_wait` after creation unless an active waitable run exists. Confirm the task id, schedule, timezone, delivery behavior, and where results will appear.

## Load References When Needed

Keep this file in context for normal durable task work. Load detailed references only for the matching situation:

- Read `references/tool-schemas.md` before constructing non-trivial durable task payloads, after schema errors, or when exact field names matter.
- Read `references/delivery-and-scheduled.md` before creating or updating scheduled/interval/cron tasks, changing delivery, choosing notification vs thread delivery, or diagnosing why a result did not appear in the main thread.
- Read `references/troubleshooting.md` after task tool failures, missing final answers, hidden results, noisy recurring messages, delivery confusion, or stuck/stale tasks.

For current-turn attached child agents, read `system:pioneer/subagents`.

## Tool Visibility

Only call task tools that are visible in the current turn.

If a needed task tool is hidden and `request_tools` is visible, request the task domain:

```json
{
  "domains": ["task"],
  "reason": "Need task tools to create, inspect, update, reschedule, pause, resume, or troubleshoot durable task work."
}
```

Do not request individual task tool names. Request the `task` domain.

If the task domain cannot be opened, do not pretend the task was created, updated, rescheduled, paused, resumed, or inspected.

## Creating Durable Future Work

Scheduled task instructions must be self-contained because future runs may not have the current chat context, exact tool list, skills, MCP servers, or web state.

Good scheduled instructions:

- say what to do step by step;
- include target URLs, repos, paths, ids, dates, and timezones;
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

## Delivery Rules

Use delivery to match the user experience:

- `thread` + `origin_thread`: write the result to the visible thread where the work originated. Best default for "send the answer back here".
- `thread` + `current_thread` or `collaboration_root`: target the current execution thread or collaboration root without supplying an id.
- `thread` + `exact_thread`: write the result to an explicitly supplied `threadId`.
- `user_notification`: notify the user without necessarily writing a durable chat message. Good for lightweight alerts, but bad when the user expects the full answer in the main thread.
- `webhook`: send the result outside Pioneer.
- `none`: keep the result in task state only.

If the user says "send it here", "post the report in this thread", "make the answer appear in this chat", or "not just a notification", do not use `user_notification`. Use `thread` with `threadTarget:"origin_thread"`.

Changing delivery affects future runs. It does not retroactively materialize already delivered results in the parent thread.

## Updating Existing Tasks

Use `task_list` or `task_get` to inspect before patching. Patch only fields that should change.

For delivery fixes, preserve schedule, goal, instructions, and output instructions unless the user asked to change them.

For prompt changes on scheduled tasks, ensure `instructions` and `outputInstructions` remain self-contained for future runs.

After an update, verify the current task state with the tool result or `task_get`. Tell the user exactly what changed and what will happen on the next run.

## Before Finalizing

Validate task state before answering:

- task id is known;
- schedule/timezone/next fire time are known or explicitly unavailable;
- delivery destination is known;
- no-op visibility is clear when relevant;
- updated fields were verified;
- the user understands whether changes affect past runs or only future runs.

Do not invent a run result for a scheduled task that has not run yet.

## Gotchas

- Scheduled tasks are not attached subagents.
- Do not call `task_wait` for scheduled future work with `waitable:false` or `runId:null`.
- `user_notification` is not the same as "write the answer in this thread".
- `includeResult:false` can produce only a generic delivery message.
- Delivery changes affect future runs, not past runs.
- Scheduled task instructions must be self-contained.
- Do not claim a task was created, updated, paused, resumed, rescheduled, or delivered unless the relevant tool succeeded.
