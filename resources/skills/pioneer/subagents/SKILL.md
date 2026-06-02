---
name: subagents
slug: subagents
owner: pioneer
description: "Task-backed subagent orchestration: when to delegate, how to create attached tasks, wait for them, review result candidates, request revisions, accept results, cancel/detach work, and synthesize the parent answer."
version: "0.1.0"
user-invocable: true
disable-model-invocation: false
implicit-invocation: required
catalog-hide: true
---

# Subagent Orchestration

Use this skill when you are deciding whether to split a user task into subagents, create subagents, coordinate multi-agent work, or review subagent results.

The parent agent owns the outcome. Subagents do focused work in hidden child threads, but the parent must decide whether their result is acceptable. Do not treat a child result candidate as final until it has been accepted.

## Delegation Stance

For every non-trivial user task, first look for a useful split into meaningful independent subtasks. Default to attached subagents when independent work can improve speed, coverage, verification, or auditability.

Do not create subagents just to create them. If the task is tiny, obvious, tightly coupled, or single-step, do it yourself in the parent turn.

## When To Create Subagents

Create subagents when at least one of these is true:

- The user explicitly asks for subagents, multi-agent work, parallel investigation, or separate workers.
- The task naturally splits into independent parts that can run concurrently.
- Parallel subagents can materially speed up the work without reducing quality or making coordination harder.
- Different subtasks need different scopes, paths, tools, roles, or output contracts.
- The parent needs independent verification before producing a final answer.
- The work is large enough that doing every part in one parent context would make the timeline hard to audit.

Prefer doing the work yourself when:

- The task is small or tightly coupled.
- The next step depends on one specific result.
- The user asked you not to delegate.
- Creating subagents would add overhead without improving speed, correctness, coverage, verification, or auditability.

## Parent Responsibilities

The parent must:

- create clear, bounded child tasks;
- start independent child tasks before waiting;
- wait for active attached tasks;
- inspect every review-required candidate;
- accept only results that satisfy the user request and child task goal;
- request revision with concrete feedback when the result is incomplete, wrong, unsafe, or poorly formatted;
- synthesize only accepted child results into the final parent answer;
- never finish while attached tasks are still active or waiting for review.

## Ensure Task Tools Are Visible

Before using task orchestration, check whether the needed `task_*` tools are currently visible.

If `task_create` or other required `task_*` tools are not visible, call `request_tools` for the `task` domain first:

```json
{
  "domains": ["task"],
  "reason": "Need task tools to create, wait for, review, revise, accept, cancel, or detach subagent work."
}
```

After `request_tools` succeeds, use the newly visible task tools according to the sections below.

Do not call `request_tools` for individual tool names such as `task_create`; request the whole `task` domain.

## Create Attached Subagents

Use `task_create` with a structured JSON object. Do not wrap the payload in a raw JSON string.

For immediate attached subagents, omit `trigger`; immediate is the default.

Basic payload:

```json
{
  "title": "Subagent 1: Inspect task service",
  "goal": "Find where TaskService is implemented and summarize the retry/cancel logic.",
  "agentRole": "researcher",
  "agentNickname": "TaskService researcher",
  "instructions": [
    "Inspect /path/to/project.",
    "Use search/read tools before answering.",
    "Return exact file:line references for every important claim.",
    "Do not modify files."
  ],
  "inputText": "Focus on TaskService, retry handling, cancellation handling, and task state transitions.",
  "outputInstructions": "Return Markdown with sections: files inspected, findings, file:line links, and conclusion."
}
```

Task quality rules:

- `title` should identify the subtask in the parent timeline.
- `goal` should be short and concrete.
- `instructions` should describe behavior and constraints.
- `inputText` should carry data and scope, not behavior.
- `outputInstructions` should define the result shape.
- Include relevant paths, ids, dates, and limits.
- Ask for file:line references when code claims matter.
- Ask child agents to register user-visible files with `artifact_register`.

Do not pass runtime-owned fields such as `workspaceId`, `ownerKind`, `parentTaskId`, `rootTaskId`, `depth`, `model`, `modelProvider`, or `trigger.spec`.

Omit advanced fields unless needed:

- `toolPolicy` only when restricting tools, writes, paths, or network.
- `contextPolicy` only when custom context behavior matters.
- `resultContract` only when a strict result type/schema is required.
- `maxDepth` only when nested delegation must be constrained.
- `lifecyclePolicy`, `deliveryPolicy`, `retryPolicy`, `timeoutPolicy`, and `concurrencyPolicy` only for deliberate advanced workflows.

## Start Tasks Before Waiting

If several subtasks are independent, call `task_create` for all of them first. Then call `task_wait` once for the created set.

Use the `taskId` or `runId` values returned by `task_create`. Prefer `runIds` when available.

```json
{
  "runIds": [
    "RUN_ID_1",
    "RUN_ID_2",
    "RUN_ID_3"
  ],
  "timeoutMs": 120000,
  "returnCompleted": true,
  "returnPending": true
}
```

`task_wait` defaults to review-aware waiting: it returns when all targets are terminal, or when candidates need parent review. A wait timeout does not cancel child work. If `timedOut` is true and work is still pending, wait again or report progress only when that is useful to the user.

Do not repeatedly call `task_wait` for the same set unless the prior wait timed out or a revision has been requested.

Use `taskIds` and `runIds` arrays. Do not use singular `taskId` or `runId` in `task_wait`.

## Review Result Candidates

When `task_wait` returns `reviewRequired`, inspect every item. A review item contains the task/run, child thread/turn, candidate, review policy, remaining revision rounds, and allowed actions.

Accept a candidate only when it satisfies:

- the user's original request;
- the child task goal;
- the required output format;
- the evidence requirements, such as file:line links;
- safety and scope constraints;
- artifact registration requirements, if the child created files.

Do not use a `pending_review` candidate as final work until `task_accept` succeeds.

## Accept A Candidate

Use `task_accept` only with a candidate id returned by `task_wait.reviewRequired`.

```json
{
  "taskId": "TASK_ID",
  "runId": "RUN_ID",
  "candidateId": "CANDIDATE_ID",
  "reason": "The result covers the requested files, includes exact file:line references, and matches the requested Markdown structure."
}
```

After accepting, use the accepted result in the parent synthesis. If other attached tasks are still active or waiting for review, continue waiting/reviewing before final answer.

## Request A Revision

Use `task_revise` when a candidate is close enough to fix in the same child thread, but not good enough to accept.

```json
{
  "taskId": "TASK_ID",
  "runId": "RUN_ID",
  "candidateId": "CANDIDATE_ID",
  "feedback": "The result is missing the Desktop timeline path. Add a section named \"Desktop timeline verification\" with three exact file:line references and a short conclusion.",
  "additionalInstructions": [
    "Keep the correct existing findings.",
    "Do not redo unrelated research.",
    "Return only the revised final answer."
  ]
}
```

Good revision feedback is specific:

- name what is missing or wrong;
- say what to add, remove, or correct;
- preserve useful work when possible;
- define the expected output shape;
- avoid vague feedback such as "improve this" or "try harder".

After `task_revise` succeeds, call `task_wait` again for that task or run. Review the new candidate the same way. Continue until the result is accepted, cancelled, or revision is no longer allowed.

If `remainingRevisionRounds` is zero or `allowedActions` does not include `task_revise`, do not request another revision. Accept the candidate, cancel the task, or create a separate follow-up task.

## Cancel Or Detach

Use `task_cancel` when the child result should not be used or the task should stop.

```json
{
  "taskId": "TASK_ID",
  "reason": "The task is no longer relevant after the parent changed direction.",
  "scope": "attached_subtree"
}
```

Use `task_detach` only when the work should continue in the background and should no longer block the parent turn.

```json
{
  "taskId": "TASK_ID"
}
```

Do not detach a task that is waiting for review. Accept, revise, or cancel the active candidate first.

## Scheduled, Interval, And Cron Tasks

Scheduled future work is different from attached subagent work. If `task_create` returns `waitable=false` or `runId=null`, there is no active run to wait for.

For scheduled, interval, and cron tasks:

- include self-contained `instructions`;
- include explicit `outputInstructions`;
- do not rely on the current chat context being available later;
- do not call `task_wait` unless there is an active waitable run;
- after creation, tell the user the task id and schedule/next fire time.

Example cron trigger:

```json
{
  "title": "Daily repository health check",
  "goal": "Inspect the repository every weekday morning and summarize issues.",
  "trigger": {
    "kind": "cron",
    "cronExpr": "0 9 * * 1-5",
    "timezone": "Europe/Moscow"
  },
  "instructions": [
    "Run a read-only repository health check.",
    "Use available search and file-reading tools.",
    "Fail clearly if the repository path is unavailable."
  ],
  "inputText": "Repository: /path/to/project",
  "outputInstructions": "Return Markdown with findings, evidence links, and recommended next actions."
}
```

## Final Parent Answer

After all attached work is accepted, terminal, cancelled, or detached, synthesize the parent answer.

Include:

- which subagents ran;
- which results were accepted on the first candidate;
- which results required revision;
- any cancelled, failed, or detached work;
- the final integrated answer;
- registered artifacts created by parent or children, when relevant.

Do not hide review history when it matters. If a child needed revision, mention that the parent requested changes and accepted the revised candidate.

## Common Mistakes

- Creating one child, waiting, then creating the next child even though tasks were independent.
- Using `task_wait` with `taskId` instead of `taskIds`.
- Calling `task_wait` for scheduled future work with no active run.
- Treating `reviewRequired` as completion.
- Quoting a rejected candidate in the final answer as if it were accepted.
- Calling `task_revise` without actionable feedback.
- Finishing the parent turn while attached tasks are still active.
- Forgetting to ask subagents to register user-visible files with `artifact_register`.
