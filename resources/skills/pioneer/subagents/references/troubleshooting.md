# Attached Subagent Troubleshooting

Use this reference after confusing attached-subagent behavior, bad child results, schema errors, or parent/child coordination problems.

## Contents

- Review Required Is Not Completion
- Child Result Is Bad Or Missing Evidence
- Task Tool Schema Error
- Parent Finished Too Early
- Stuck Or Failed Attached Work
- Detached Work

## Review Required Is Not Completion

If `task_wait` returns `reviewRequired`, the child is asking the parent to decide.

Do not treat the candidate as accepted. Inspect it and then:

- `task_accept` if it is good;
- `task_revise` if it needs focused correction;
- `task_cancel` if it should not be used.

Inspect `reviewContent`, not only `candidate.result.summary`. Summary is deliberately short. If the content is truncated or was lost after recovery/compaction, call `task_result` with the exact `candidateId` and continue with `nextCursor` until the result is complete. Never inspect a different "latest" task result when deciding an immutable candidate round.

## Child Result Is Bad Or Missing Evidence

Use `task_revise` with specific feedback. Avoid vague prompts.

Bad:

```text
Improve this.
```

Good:

```text
Add file:line evidence for the claim that weak extractor candidates are suppressed before durable write. Include the exact quality gate behavior and keep the rest of the answer concise.
```

## Task Tool Schema Error

Common causes:

- using `taskId` instead of `taskIds` in `task_wait`;
- using snake_case instead of camelCase;
- passing runtime-owned fields;
- passing scheduled-task fields to an attached subagent.

Read `tool-schemas.md`, fix the payload, and retry only if the operation is safe or idempotent.

## Parent Finished Too Early

If the parent answered while attached tasks were still active or waiting for review, the answer is incomplete.

Recover by:

1. waiting on the active runs;
2. reviewing candidates;
3. accepting/revising/cancelling;
4. sending a corrected synthesis.

## Stuck Or Failed Attached Work

Inspect:

- task status;
- latest run status;
- error snapshot;
- result candidates;
- child thread evidence when available.

Use `task_result(candidateId)` before looking for child-thread or database workarounds. Direct database access is not part of the review protocol and must not be required to accept or revise a candidate.

Then choose:

- revise if the candidate is close;
- cancel if the child is wrong, unsafe, or no longer useful;
- detach if the parent can continue and the work should keep running;
- explain clearly if the runtime lacks the required capability.

## Detached Work

Detaching means the parent turn no longer waits for that work. It does not mean the work is accepted.

After detaching:

- do not use detached output as evidence in the current parent answer;
- tell the user that the work continues separately when relevant;
- inspect or resume it later through task tools if needed.
