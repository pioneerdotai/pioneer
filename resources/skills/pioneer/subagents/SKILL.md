---
name: subagents
slug: subagents
owner: pioneer
description: "Use this skill when Pioneer should delegate work to task-backed subagents, create scheduled/interval/cron tasks, wait for or review task results, revise/accept/cancel/detach task work, update task delivery, or decide where task results should appear."
version: "0.1.0"
user-invocable: true
disable-model-invocation: false
implicit-invocation: required
catalog-hide: true
---

# Pioneer Subagents And Tasks

Tasks are for durable work. Subagents are task-backed child agents that do focused work in hidden child threads. Scheduled tasks are future or recurring runs that may execute long after the creation chat is gone.

The parent agent owns the user outcome. A child result is evidence until the parent accepts it, integrates it, or deliberately routes it somewhere else.

## Product Model

Think in three surfaces:

- **Parent thread**: the user-facing conversation history. Write here when the user expects an answer in this chat.
- **Task state**: durable run history, candidates, result snapshots, attempts, and diagnostics. Every task result should be recoverable here even when it is not shown as a chat message.
- **Delivery**: the policy that decides whether a terminal task result becomes a parent/thread message, a user notification, a webhook, or no visible message.

Do not confuse "the child agent produced a final answer" with "the main thread received a final answer." Those are separate product events.

## Quick Start

Before delegating, decide what kind of work this is.

1. If the user needs one direct answer and the work is small or tightly coupled, do it in the parent turn.
2. If independent focused work would improve speed, coverage, verification, or auditability, create attached subagents.
3. If work should run later or repeatedly, create a scheduled, interval, or cron task with self-contained future-run instructions.
4. If a task result should appear in this chat later, use `deliveryPolicy.mode:"owner_thread"` or `deliveryPolicy.mode:"thread"` with the current thread id.
5. If the result should only alert the user, use `user_notification`, but remember this may not create durable chat history.
6. If the result should go outside Pioneer, use `webhook`.
7. If the task is background state only, use no delivery or `mode:"none"`.

For attached subagents, create all independent tasks first, wait once, review candidates, accept/revise/cancel, then synthesize the parent answer from accepted results.

For scheduled tasks, do not call `task_wait` after creation unless an active waitable run exists. Confirm the schedule, task id, delivery behavior, and next fire time.

## Load References When Needed

Keep this file in context for the normal flow. Load detailed references only for the matching situation:

- Read `references/tool-schemas.md` before constructing non-trivial `task_*` payloads, after schema errors, or when exact field names matter.
- Read `references/delivery-and-scheduled.md` before creating or updating scheduled/interval/cron tasks, changing delivery, or diagnosing why a result did not appear in the main thread.
- Read `references/workflows-and-examples.md` for concrete examples: parallel research, code review, release-monitor cron tasks, delivery updates, and parent synthesis.
- Read `references/troubleshooting.md` after task tool failures, missing final answers, hidden child results, review confusion, or stuck/stale tasks.

## Tool Visibility

Only call task tools that are visible in the current turn.

If a needed task tool is hidden and `request_tools` is visible, request the task domain:

```json
{
  "domains": ["task"],
  "reason": "Need task tools to create, wait for, review, update, cancel, detach, or inspect task-backed work."
}
```

Do not request individual task tool names. Request the `task` domain.

If the task domain cannot be opened, do not pretend the task was created or updated. Continue with visible context and say which task operation is unavailable.

## When To Delegate

Create attached subagents when at least one is true:

- the user explicitly asks for subagents, parallel work, independent workers, or review;
- the task naturally splits into independent parts;
- separate agents can inspect different files, systems, tools, or hypotheses in parallel;
- the parent needs independent verification before committing to an answer;
- the work is large enough that one parent timeline would be hard to audit.

Prefer parent-only work when:

- the task is small, obvious, or single-step;
- each next step depends on one previous result;
- delegation would add coordination overhead without improving quality;
- the user asks not to delegate.

## Create Good Subagents

For immediate attached subagents, omit `trigger`.

Good child tasks are narrow and auditable:

- `title`: short label visible in the parent timeline.
- `goal`: one concrete outcome.
- `instructions`: behavior, constraints, and what not to do.
- `inputText` or `input`: data, paths, ids, dates, examples, and scope.
- `outputInstructions`: exact result shape, evidence requirements, and language.

Use self-contained wording. Do not rely on hidden parent reasoning, invisible local assumptions, or tool names that might not exist in the child turn.

Ask for file:line references when code claims matter. Ask children to register user-visible files with `artifact_register` when they create files.

Do not pass runtime-owned fields such as `workspaceId`, `ownerKind`, `parentTaskId`, `rootTaskId`, `depth`, `model`, `modelProvider`, or `trigger.spec`.

Read `references/tool-schemas.md` for exact examples.

## Wait And Review

For independent attached tasks, create them all first, then call `task_wait` once with `taskIds` or `runIds`. Prefer `runIds` when available.

`task_wait` can return terminal results, pending work, or `reviewRequired`. A review-required candidate is not final. Inspect every candidate and accept only when it satisfies:

- the original user request;
- the child task goal;
- the requested output shape;
- evidence and artifact requirements;
- safety, scope, and write constraints.

Use `task_revise` when the result is close but incomplete or incorrectly shaped. Give concrete feedback: what is wrong, what to add/remove/correct, and what the revised output must look like.

Use `task_accept` only after the candidate is good enough to use. Use accepted child results in the parent synthesis.

Do not finish the parent turn while attached tasks are active, waiting for review, or producing unreviewed candidates.

## Scheduled And Recurring Tasks

Scheduled tasks are not attached subagents. They are durable future work.

For scheduled, interval, and cron tasks:

- include self-contained `instructions`;
- include explicit `outputInstructions`;
- choose delivery deliberately;
- include timezone and exact schedule when relevant;
- do not call `task_wait` unless the returned run is active and waitable;
- tell the user task id, schedule, next fire time, and where results will appear.

Future task runs may not have the tool set, MCP servers, skills, or thread context that existed during task creation. Instruct future agents to choose tools by capability, fail clearly when required data is unavailable, and avoid relying on creation-time chat context.

Read `references/delivery-and-scheduled.md` before creating recurring tasks that must report to the user.

## Delivery Rules

Use delivery to match the user experience:

- `owner_thread`: write the result to the task owner's thread. Best default for "send the answer back here" when the task is owned by the current thread.
- `thread`: write the result to a specific `threadId`. Best when updating an existing task or targeting a known thread explicitly.
- `user_notification`: notify the user without necessarily writing a durable chat message. Good for lightweight alerts, but bad when the user expects the full answer in the main thread.
- `webhook`: send the result outside Pioneer.
- `none`: keep the result in task state only.

If the user says "send it here", "post the report in this thread", "make the answer appear in this chat", or "not just a notification", do not use `user_notification`. Use `owner_thread` or `thread`.

Changing delivery affects future runs. It does not retroactively materialize already delivered results in the parent thread.

## Updating Existing Tasks

Use `task_list` or `task_get` to inspect before patching. Patch only fields that should change.

For delivery fixes, preserve schedule, goal, instructions, and output instructions unless the user asked to change them.

After `task_update`, verify the task's current delivery policy with `task_get` or the tool result. Tell the user exactly what changed and what will happen on the next run.

## Before Finalizing

Validate the task state before answering the user.

- For attached subagents: every relevant child is terminal, accepted, revised, cancelled, or deliberately detached.
- For review-required results: accepted candidates are the only candidates treated as final evidence.
- For scheduled tasks: schedule, timezone, next fire time, and delivery destination are known or explicitly unavailable.
- For task updates: the tool result or `task_get` confirms the changed fields.
- For delivery issues: explain whether the change affects past runs or only future runs.

## Final Parent Answer

For attached subagents, the final user-facing answer comes from the parent. Include only what matters:

- which subagents ran, when useful;
- which results were accepted or revised, when relevant;
- any failures, cancellations, or detached work;
- the integrated answer;
- links or artifacts created by parent or children.

For scheduled tasks, the creation/update answer should not invent a run result. Confirm the schedule and delivery behavior instead.

## Common Patterns

If the user asks for parallel investigation, create focused child tasks for independent parts, wait once, review, accept, and synthesize.

If the user asks to "check every morning and post here", create a cron task with `deliveryPolicy.mode:"owner_thread"` or `mode:"thread"` and `includeResult:true`.

If a scheduled run completed but the answer did not appear in the chat, inspect the task's delivery mode. `user_notification`, `webhook`, and `none` do not guarantee a durable parent-thread message.

For full examples, read `references/workflows-and-examples.md`.

## Gotchas

- `task_wait` uses arrays: `taskIds` or `runIds`, not singular `taskId` or `runId`.
- Do not call `task_wait` for scheduled future work with `waitable:false` or `runId:null`.
- A `pending_review` candidate is not accepted work.
- Do not quote rejected candidates as final results.
- `user_notification` is not the same as "write the answer in this thread".
- `includeResult:false` can produce only a generic delivery message.
- Delivery changes affect future runs, not past runs.
- Scheduled task instructions must be self-contained.
- Do not claim a task was created, updated, accepted, cancelled, or delivered unless the relevant tool succeeded.
