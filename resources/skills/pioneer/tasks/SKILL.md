---
name: tasks
slug: tasks
owner: pioneer
description: "Use this skill when Pioneer should create, inspect, update, reschedule, pause, resume, finish, cancel, troubleshoot, or configure delivery for durable tasks, scheduled tasks, recurring tasks, interval tasks, cron jobs, background work, or existing task state."
version: "0.1.3"
user-invocable: true
disable-model-invocation: false
implicit-invocation: required
catalog-hide: true
---

# Pioneer Durable Tasks

Durable tasks are product objects that can outlive the current turn. They have task state, run history, result snapshots, attempts, diagnostics, schedules, and delivery behavior.

When observing a task with `task_get`, distinguish task, run, and execution status. A `running` run is unfinished; no result or unchanged `updatedAt` does not prove a stall. The execution heartbeat is periodic liveness, while `lastActivityAt` is confirmed activity only when recorded. Missing activity is unknown. Do not cancel or repeat work solely because it is taking time or repeated observations are unchanged. A `task_wait` timeout ends only that wait window and permits another wait for active attached work.

Use this skill for future, recurring, background, or already-existing task work. For attached child agents created to help with the current turn, read `system:pioneer/subagents` instead.

## Product Model

Think in three surfaces:

- **Task state**: durable source of truth for what should run, what did run, results, attempts, errors, and diagnostics.
- **Run execution**: a concrete attempt to perform the task now or later.
- **Delivery**: the policy that decides whether a terminal task result becomes a thread message, user notification, webhook call, or no visible output.

Do not confuse "the task produced a result" with "the user saw the result in this thread." Execution and delivery are separate.

Return the run result in the execution thread and let the configured delivery policy publish it. Do not use `thread_message_send` or `threads_turn_start` for inter-agent coordination or to announce or duplicate a task result. For attached children, use the result and review flow in `system:pioneer/subagents`.

## Quick Start

Before using durable task tools, decide what kind of task work this is.

1. If work should run later or repeatedly, use `task_create` to create a scheduled, interval, or cron task with self-contained future-run instructions.
2. If existing task behavior must change, inspect it first with `task_get` or `task_list`.
3. If only the schedule changes, use `task_reschedule` when available.
4. If delivery changes, patch only delivery and verify it.
5. If the task should be temporarily stopped or restarted, use pause/resume tools when visible.
6. If the user asks to stop permanently while letting current work finish, use `task_finish` when visible. Use `task_cancel` when the user wants cancellation under its existing interruption rules.
7. If a result should appear in this chat later, configure thread delivery deliberately.
8. If the result should only alert the user, use notification delivery only when a durable chat message is not required.

For future scheduled work, do not call `task_wait` after creation unless an active waitable run exists. Confirm the task id, schedule, timezone, delivery behavior, and where results will appear.

For an inbound HTTP webhook, create an agent task with `trigger: {"kind":"external","source":"webhook"}` and self-contained instructions for interpreting each request body. Creation waits for requests and does not launch an agent. `task_create` and authorized `task_get` return the full current `webhookUrl` and `webhookScope` (`public`, `lan`, or `loopback`); use the returned URL without assembling host or path yourself. The URL is a secret capability: share it only with the intended sender. Each accepted POST creates a separate durable run, including identical bodies. No JSON envelope or event fields are required. The body and Content-Type are run-specific external data; they never change saved instructions or permissions. Binary bodies are provided as base64 with an explicit encoding. Omit `eventType` and `filter`. Do not call `task_wait` while the task is idle; results use ordinary delivery, defaulting to the initiating conversation. Pause stops incoming work until resume; finish permanently stops admission and follows the existing completion rules.

## Load References When Needed

Keep this file in context for normal durable task work. Load detailed references only for the matching situation:

- Read `references/tool-schemas.md` before constructing non-trivial durable task payloads, after schema errors, or when exact field names matter.
- Read `references/delivery-and-scheduled.md` before creating or updating scheduled/interval/cron tasks, changing delivery, choosing notification vs thread delivery, or diagnosing why a result did not appear in the main thread.
- Read `references/troubleshooting.md` after task tool failures, missing final answers, hidden results, noisy recurring messages, delivery confusion, or stuck/stale tasks.

For current-turn attached child agents, read `system:pioneer/subagents`.

## CLI Runtime Binding

When Pioneer provides these operations through its CLI MCP facade, use the callable names in the current CLI tool catalog. The turn instructions bind the canonical task names to those callables. The catalog is fixed for that turn; use only the operations it contains. No `request_tools` call is needed in this environment.

In CLI runtimes, `system:pioneer/tasks` means the exported `pioneer-system-tasks` skill and `system:pioneer/subagents` means the exported `pioneer-system-subagents` skill. Load them using the runtime's skill mechanism or read their exported `SKILL.md` files. Resolve `references/` relative to the loaded skill directory. Other system skill references do not imply that a CLI export or tool is available. Use artifact tools only when the current catalog provides them.

## Tool Visibility

Only call task tools that are visible in the current turn.

If a needed task tool is hidden and `request_tools` is visible, request the task domain:

```json
{
  "domains": ["task"],
  "reason": "Need task tools to create, inspect, update, reschedule, pause, resume, finish, cancel, or troubleshoot durable task work."
}
```

Do not request individual task tool names. Request the `task` domain.

The `task` domain also exposes `threads_start_options`; opening `threads` separately is unnecessary to inspect task launch options. Call it when you need to choose an identity, execution profile, or permitted destination explicitly. For a task that inherits the parent's identity and profile in the current execution, omit `launch` and `targetOptionId`; no preliminary options call is required. Options do not grant permission to create or run a task. See `references/tool-schemas.md` for the exact fields.

If the task domain cannot be opened, do not pretend the task was created, updated, rescheduled, paused, resumed, finished, cancelled, or inspected.

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

The initiating thread is the original user-facing conversation. The execution thread is where the agent runs and may be an internal child. Choose delivery by the intended recipient, not by the execution location.

Use delivery to match the user experience:

- `thread` + `origin_thread`: deliver to the initiating conversation, without supplying `threadId`.
- `thread` + `current_thread`: deliver to the current execution thread, without supplying `threadId`.
- `thread` + `collaboration_root`: deliver to the root conversation of the collaboration capsule, without supplying `threadId`.
- `thread` + `exact_thread`: write the result to an explicitly supplied `threadId`.
- `user_notification`: notify the user without necessarily writing a durable chat message. Good for lightweight alerts, but bad when the user expects the full answer in the main thread.
- `webhook`: send the result outside Pioneer.
- `none`: keep the result in task state only.

Changing delivery affects future runs. It does not retroactively materialize already delivered results in the parent thread.

## Updating Existing Tasks

Task access belongs to the authorized root-thread capsule, not to the turn/run or hidden execution thread that created the task. A later authorized execution in the same capsule can inspect and manage the existing task by its stable `taskId`, within both its admitted action grants and the initiating user's current permissions. A user's broader rights do not widen an execution's restricted grants. Knowing an id does not grant access to another capsule.

`task_access_denied` means this execution cannot perform the operation; it does not mean the task is absent, broken, cancelled, or no longer scheduled. Do not create a replacement merely because inspection or cancellation failed: the original schedule may still fire. Report the blocker and possible duplicate risk. For `task_state_conflict`, reload the task and reconcile the requested change. For `task_operation_failed` (or older `agent_runtime_integrity_lost`), report an unconfirmed operation, not a dead task.

Use `task_list` or `task_get` to inspect before patching. Patch only fields that should change.

For delivery fixes, preserve schedule, goal, instructions, and output instructions unless the user asked to change them.

For prompt changes on scheduled tasks, ensure `instructions` and `outputInstructions` remain self-contained for future runs.

After an update, verify the current task state with the tool result or `task_get`. Tell the user exactly what changed and what will happen on the next run.

## Stopping A Task

Choose the operation from the user's intent:

- `task_pause` temporarily stops future runs so the task can be resumed.
- `task_cancel` cancels under the existing rules, including interruption of current work and restrictions on cancelling the current execution's own task or its ancestors.
- `task_finish` permanently stops a task at the user's request. It prevents new runs and lets already started work finish with its results preserved and configured delivery. Use it when the user asks to stop the task while allowing current work to finish.

Call `task_finish` only for that instruction, including an explicit stop condition the user defined in advance. An ordinary final answer, a completed run, or a successful recurring check does not itself authorize finish. Recurring runs otherwise continue under their existing rules.

Finish stops schedules and queued work that has not started. All already admitted work, including parallel runs, review and necessary revisions, can finish normally. It does not assert that the original goal was achieved, turn a failed run into success, or start a new agent to produce a result when no work is active. Failures remain failures; no new task attempt is started after finish.

The stop decision is durable and survives Gateway restart. `task_get` and `task_list` expose `finishRequest`; inspect run and delivery state separately. An accepted request does not mean computation has finished or delivery has succeeded. A task that finishes normally becomes `completed`; delivery can still be pending or failed without restarting the task. Repeating finish is safe and does not duplicate work or publication. Existing terminal outcomes are preserved.

After finish, resume, update and reschedule are rejected. Explicit cancel remains available under its existing permissions and restrictions if the user wants to interrupt remaining work. A task with the required permissions can finish itself; this does not authorize finishing ancestors or tasks outside its allowed scope.

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
- `user_notification` does not guarantee a durable thread message.
- `includeResult:false` can produce only a generic delivery message.
- Delivery changes affect future runs, not past runs.
- Scheduled task instructions must be self-contained.
- Do not claim a task was created, updated, paused, resumed, rescheduled, cancelled, or delivered unless the relevant tool succeeded.
- A successful `task_finish` confirms the stop decision; report work completion and delivery only from their own observed state.
