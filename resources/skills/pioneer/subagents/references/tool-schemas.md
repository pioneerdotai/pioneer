# Attached Subagent Tool Schemas

Use this reference when exact task tool arguments matter for immediate attached subagents. Task tools use strict function arguments: pass fields at the top level, use camelCase field names, and do not add wrappers such as `task`, `spec`, `schedule`, or `triggerInput`.

## Contents

- Tool Visibility
- task_create for attached subagents
- task_wait
- task_result
- task_accept
- task_revise
- task_cancel
- task_detach
- task_list and task_get for attached work inspection

## Tool Visibility

Only call tools visible in the current turn. If a needed task tool is hidden and `request_tools` is visible, request the `task` domain first:

```json
{
  "domains": ["task"],
  "reason": "Need task tools to create, wait for, review, revise, accept, cancel, detach, or inspect attached subagent work."
}
```

If the tool remains unavailable, do not fake the operation.

### Task launch catalogs

Both tools are available through the `task` domain when permitted. Use the callable names from the current catalog in CLI runtimes.

Call `task_start_options` with `{}` to choose an identity, an exact execution profile, or a destination. It accepts optional `limit` (1–100, default 100) and `cursor`.

- Use `identities[].id` in `launch.identity` as `{"kind":"exact","id":"IDENTITY_ID"}`.
- For a ready-made profile, use `{"kind":"exact","id":"PROFILE_ID"}` in `launch.profile`. Select a returned profile whose `compatibleIdentityIds` includes the identity. Honor its `allowedReasoning` and `allowedPermissionProfiles` and the response's `maxPermissionProfile`.
- Choose `launch.skillIds` and `launch.mcpServerIds` from `allowedSkillIds` and `allowedMcpServerIds`.
- For a permitted alternate destination, use the current execution's opaque `targetOptions[].id` as `task_create.targetOptionId`.

For direct selection, call `task_identity_models_list` with `{"identityId":"IDENTITY_ID"}`. Use an identity admitted by `task_start_options`. The model catalog also accepts `limit` (1–100, default 100) and `cursor`. Each returned executor identifies the configured provider or runtime via `providerId` and its `backend`. Use that exact `providerId` and an associated `models[].id` in `launch.profile` as `{"kind":"model","providerId":"PROVIDER_ID","modelId":"MODEL_ID"}`. For a CLI executor, `providerId` identifies the configured runtime instance.

Use the selected model's `reasoning.effort_options` for explicit `launch.reasoning` as `{"effort":"EFFORT"}`. Use an explicit effort only when the catalog lists it for the selected model. Missing metadata or an empty effort list leaves available efforts unknown. Set `launch.useDefaultReasoning:true` to use the model's native default. Leave `launch.reasoning` unset in that case.

For either catalog, follow `nextCursor` with the original arguments and the same `limit` (and `identityId` for the model catalog) until `eof`. Reassemble any oversized entry from `fragments` by entry and offsets, then decode its JSON. Restart from the first page if the cursor becomes stale.

`inheritParentIdentityAvailable` and `inheritParentProfileAvailable` indicate whether inheritance can be selected. `defaultPioneerIdentityAvailable` and `derivedEphemeralIdentityAvailable` indicate whether `launch.identity.kind` can be `default_pioneer` or `server_derived_ephemeral`. To inherit launch settings without choosing an explicit destination, omit `launch` and `targetOptionId` and skip both catalogs. Launch requires separate authorization, which `task_create` rechecks along with selection compatibility.

## task_create For Attached Subagents

Use `task_create` without `trigger` for immediate attached subagents.

```json
{
  "title": "Inspect memory extraction",
  "requireReview": false,
  "goal": "Find why explicit user identity facts are not being extracted into durable memory.",
  "agentRole": "researcher",
  "agentNickname": "Memory researcher",
  "instructions": [
    "Inspect the repository read-only.",
    "Use search and file-reading tools before answering.",
    "Return exact file:line references for every claim.",
    "Do not modify files."
  ],
  "inputText": "Focus on post-turn memory extraction, quality gates, write provider calls, and diagnostics.",
  "outputInstructions": "Return Markdown with sections: inspected files, findings, evidence, and conclusion."
}
```

Important fields:

- `title`: human-visible label and child thread title.
- `goal`: short objective.
- `instructions`: behavior and constraints.
- `inputText`: simple task data and scope.
- `input`: structured data, variables, references, or attachments.
- `outputInstructions`: final shape and evidence requirements.
- `toolPolicy`, `contextPolicy`, `resultContract`: advanced controls; omit unless required.

For attached subagents, omit `trigger` and usually omit `deliveryPolicy`.

Do not pass:

- `workspaceId`
- `ownerKind`
- `parentTaskId`
- `rootTaskId`
- `depth`
- `model`
- `modelProvider`
- `trigger`
- `trigger.spec`

## task_wait

Use `task_wait` for active attached runs only.

```json
{
  "runIds": ["RUN_ID_1", "RUN_ID_2"],
  "timeoutMs": 120000,
  "returnCompleted": true,
  "returnPending": true
}
```

Rules:

- Use arrays: `taskIds` or `runIds`.
- Prefer `runIds` when `task_create` returned run ids.
- Do not use singular `taskId` or `runId`.
- A timeout does not cancel child work.
- `timedOut:true` means this wait window expired. Active `taskIds` or `runIds` can be waited on again even when no counts changed. A run timeout appears separately in its run status and error.
- Authorized result readers receive `reviewContent` for each `reviewRequired` candidate. `summary` alone is not the full result.

## task_result

Use `task_result` to re-read the reviewer-safe content of the exact immutable candidate, or to continue a truncated result.

```json
{
  "candidateId": "CANDIDATE_ID"
}
```

When the response has `truncated: true`, continue with the returned cursor:

```json
{
  "candidateId": "CANDIDATE_ID",
  "cursor": "NEXT_CURSOR"
}
```

Do not substitute `taskId` or a different revision candidate. The review decision must target the same `candidateId` whose content was inspected.

## task_accept

Use after reviewing a pending candidate returned by `task_get.reviewRequired` or `task_wait.reviewRequired` for work created with requireReview:true.

```json
{
  "taskId": "TASK_ID",
  "runId": "RUN_ID",
  "candidateId": "CANDIDATE_ID",
  "reason": "The result satisfies the child goal, includes required evidence, and matches the requested output shape."
}
```

Do not accept candidates you have not inspected.

## task_revise

Use when a candidate is close enough to fix in the same child thread.

```json
{
  "taskId": "TASK_ID",
  "runId": "RUN_ID",
  "candidateId": "CANDIDATE_ID",
  "feedback": "The result identifies the extractor hook but does not explain the quality gate path. Add file:line evidence for parser validation, quality scoring, and write suppression.",
  "additionalInstructions": [
    "Keep the correct findings.",
    "Do not redo unrelated research.",
    "Return only the revised final answer."
  ]
}
```

Good feedback names the exact missing or wrong part and the expected replacement.

## task_cancel

Use when the child should stop or its result should not be used.

```json
{
  "taskId": "TASK_ID",
  "reason": "The parent changed direction and this result is no longer relevant.",
  "scope": "attached_subtree"
}
```

## task_detach

Use when work should continue in the background and should no longer block the parent turn.

```json
{
  "taskId": "TASK_ID"
}
```

Do not detach a task waiting for review. Accept, revise, or cancel the active candidate first.

## task_list And task_get

Use `task_list` or `task_get` when attached work must be inspected, recovered, or audited before deciding what to do next.

```json
{
  "limit": 50
}
```

```json
{
  "taskId": "TASK_ID"
}
```

Each `task_get.runs[]` includes `execution` with `status`, `heartbeatAt`, `lastActivityAt`, and `observedAt` (Unix seconds). `status:null` means no execution row was observed. `heartbeatAt` is periodic liveness, not proof of meaningful progress. `lastActivityAt` is present only when the runtime recorded task activity for the current execution attempt; `null` means activity is unknown. `updatedAt` on a task or run is not a substitute for either timestamp.

### Review option

`requireReview` defaults to false for every task type and attachment mode.
Normally pass false. Set true explicitly when each run result requires acceptance
or same-worker revision; this also applies to scheduled, interval, cron and
webhook tasks. True has no timeout acceptance. Review must be enabled for this
built-in choice. Arbitrary advanced policies retain their existing admission gate.

At creation, omit `maxRevisionRounds` to use the site default, which ships
unlimited. An explicit `null` selects unlimited revisions even on a site with a
finite default. A non-negative integer sets a cap. Zero permits zero requests
for changes. Round 0 is the initial candidate, so a cap of 5 permits requests
for changes from rounds 0 through 4.

Inspect `task_get.runs[].reviewBudget.maxRevisionRounds` for the effective cap of
a reviewed run. In this nested budget, `maxRevisionRounds:null` means unlimited.
In `task_get.reviewRequired` and `task_wait.reviewRequired`, check
`unlimitedRevisionRounds` first. True means unlimited. If false or absent, use
the numeric fields. Zero remaining means the finite budget is exhausted.
The numeric fields are retained for older clients, so a large number alone
does not indicate an unlimited budget. Check `allowedActions` before revising.

When reading a saved `reviewPolicy`, `runRevisionBudget` takes precedence if
present. Otherwise `unlimitedRevisionRounds:true` means the configured cap is
unlimited, and the numeric `maxRevisionRounds` is authoritative when the flag
is false or absent.

Read exact candidate references through task_get.reviewRequired or task_wait,
then page task_result by candidateId. Accept/revise the exact run/candidate under
current capsule permissions. False completes automatically; task_revise does not
reopen a completed result. For reviewed background, omitted deliveryPolicy means
none; explicit permitted policy overrides it for results and terminal errors.
Background without review and scheduled tasks keep their existing delivery defaults.


### Change or remove an active run's revision cap

Use `task_update` with task management and review authority. Pass the exact
`runId` from `task_get` and supply the cap explicitly. This is a standalone patch.

```json
{
  "taskId": "TASK_ID",
  "revisionBudget": {
    "runId": "RUN_ID",
    "budget": { "maxRevisionRounds": null }
  }
}
```

Use a non-negative integer to set or change the cap. Omit `revisionBudget`
from ordinary task updates to leave it alone. The patch applies only to this
active reviewed run, including a run waiting for review at an exhausted cap.
It preserves the current candidate and round count. Future occurrences and new
run attempts use their own snapshots. A retry of an already-admitted revision
turn uses the existing execution retry rules. After updating, read the candidate
and `allowedActions` again, then call `task_revise` with concrete feedback if
allowed. Accepted or otherwise terminal runs cannot be reopened.
