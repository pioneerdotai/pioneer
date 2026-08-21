---
name: subagents
slug: subagents
owner: pioneer
description: "Use this skill when Pioneer should delegate current-turn work to attached task-backed subagents, coordinate parallel child agents, wait for child results, review/revise/accept/cancel/detach attached work, or synthesize accepted child results into the parent answer."
version: "0.2.0"
user-invocable: true
disable-model-invocation: false
implicit-invocation: required
catalog-hide: true
---

# Pioneer Attached Subagents

Attached subagents are task-backed child agents created to help with the current user turn. They do focused work in hidden child threads, but the parent agent owns the user-facing outcome.

A child result is evidence, not the final user answer, until the parent reviews it, accepts it, integrates it, or deliberately rejects/routes it elsewhere.

## Product Model

Use this skill for immediate delegation only:

- parallel investigation;
- independent verification;
- focused review;
- independent file/system/hypothesis inspection;
- work that should block the parent answer until reviewed.

Do not use this skill for scheduled, recurring, future, or long-running background work. For durable future/background tasks, read `system:pioneer/tasks`.

## Quick Start

Before delegating, decide whether attached subagents improve the current answer.

1. If the user needs one direct answer and the work is small, tightly coupled, or sequential, do it in the parent turn.
2. If independent focused work improves speed, coverage, verification, or auditability, create attached subagents.
3. Create all independent child tasks first.
4. Call `task_wait` until the next child result is terminal or requires review.
5. Review every returned terminal or review-required result immediately.
6. Accept good candidates, revise close-but-incomplete candidates, cancel irrelevant or unsafe candidates, or detach work that should no longer block the parent.
7. Call `task_wait` again for the remaining active runs and repeat the review cycle until all child work is resolved.
8. Synthesize the parent answer from accepted results and direct parent work.

Do not finish the parent turn while attached subagents created by this turn are pending, running, waiting for review, or producing unreviewed candidates.

## Load References When Needed

Keep this file in context for the normal attached-subagent flow. Load detailed references only when the matching situation appears:

- Read `references/tool-schemas.md` before constructing non-trivial attached-subagent `task_*` payloads, after schema errors, or when exact field names matter.
- Read `references/workflows-and-examples.md` for concrete attached-subagent examples: parallel investigation, independent review, parent synthesis, revision, cancel, and detach.
- Read `references/troubleshooting.md` after bad child results, missing evidence, review confusion, parent finished too early, schema errors, or stuck attached work.

For scheduled task delivery, cron, recurring runs, task updates, or delivery troubleshooting, read `system:pioneer/tasks` instead.

## Tool Visibility

Only call task tools that are visible in the current turn.

If a needed task tool is hidden and `request_tools` is visible, request the task domain:

```json
{
  "domains": ["task"],
  "reason": "Need task tools to create, wait for, review, revise, accept, cancel, detach, or inspect attached subagent work."
}
```

Do not request individual task tool names. Request the `task` domain.

If the task domain cannot be opened, do not pretend a subagent was created, waited on, accepted, revised, cancelled, or detached.

## When To Delegate

Create attached subagents when at least one is true:

- the user explicitly asks for subagents, agents, parallel work, independent workers, or review;
- the task naturally splits into independent parts;
- separate agents can inspect different files, systems, tools, or hypotheses in parallel;
- the parent needs independent verification before committing to an answer;
- the work is large enough that one parent timeline would be hard to audit.

Prefer parent-only work when:

- the task is small, obvious, or single-step;
- each next step depends on one previous result;
- delegation would add coordination overhead without improving quality;
- the user asks not to delegate.

## Create Good Attached Subagents

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

Read `references/tool-schemas.md` for exact payload examples.

## Wait And Review

For independent attached subagents, create them all first, then call `task_wait` with `taskIds` or `runIds`. Prefer `runIds` when available. By default, `task_wait` returns as soon as any target is terminal or requires review. Handle every returned result before calling `task_wait` again for the remaining active runs.

`task_wait` can return terminal results, pending work, or `reviewRequired`. A review-required candidate is not final. Inspect every candidate and accept only when it satisfies:

- the original user request;
- the child task goal;
- the requested output shape;
- evidence and artifact requirements;
- safety, scope, and write constraints.

Use `task_revise` when the result is close but incomplete or incorrectly shaped. Give concrete feedback: what is wrong, what to add/remove/correct, and what the revised output must look like.

Use `task_accept` only after the candidate is good enough to use. Use accepted child results in the parent synthesis.

After accepting, revising, or cancelling the returned candidates, call `task_wait` again for unresolved active runs. Continue this wait-review cycle until no attached child work remains unresolved.

Use `task_cancel` when the child is irrelevant, unsafe, duplicate, or no longer needed.

Use `task_detach` only when the work should continue in the background and no longer block the parent turn. Do not detach review-required work; accept, revise, or cancel the active candidate first.

## Before Finalizing

Validate the attached-subagent state before answering the user:

- every relevant child is terminal, accepted, revised, cancelled, or deliberately detached;
- review-required candidates are inspected;
- accepted candidates are the only candidates treated as final evidence;
- failures, cancellations, detached work, and uncertainty are accounted for.

## Final Parent Answer

The final user-facing answer comes from the parent.

Include only what matters:

- which subagents ran, when useful;
- which results were accepted or revised, when relevant;
- any failures, cancellations, or detached work;
- the integrated answer;
- links or artifacts created by parent or children.

Do not paste raw child outputs blindly. Combine duplicate findings, preserve important evidence, and write one coherent parent response.

## Gotchas

- Attached subagents are for current-turn delegation, not scheduled future work.
- `task_wait` uses arrays: `taskIds` or `runIds`, not singular `taskId` or `runId`.
- A `pending_review` candidate is not accepted work.
- Do not quote rejected candidates as final results.
- Do not finish while attached child work from this turn is unresolved.
- Do not claim a child was created, accepted, revised, cancelled, or detached unless the relevant tool succeeded.
