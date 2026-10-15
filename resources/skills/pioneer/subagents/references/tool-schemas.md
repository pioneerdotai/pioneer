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

`threads_start_options` is available through the `task` domain when permitted. Call it with `{}` only when you need to select an identity, execution profile, or destination explicitly:

- Use `identities[].id` in `task_create.launch.identity` as `{"kind":"exact","id":"IDENTITY_ID"}`.
- Use a compatible `profiles[].id` in `launch.profile` as `{"kind":"exact","id":"PROFILE_ID"}`.
- If setting `launch.reasoning` or `launch.permissionProfile`, stay within the selected profile's `allowedReasoning` and `allowedPermissionProfiles` and the response's `maxPermissionProfile`.
- If setting `launch.skillIds` or `launch.mcpServerIds`, choose from `allowedSkillIds` or `allowedMcpServerIds`.
- Use `targetOptions[].id` as `task_create.targetOptionId` for a permitted destination.

`inheritParentIdentityAvailable` and `inheritParentProfileAvailable` indicate whether inheritance can be selected. `defaultPioneerIdentityAvailable` and `derivedEphemeralIdentityAvailable` indicate whether `launch.identity.kind` can be `default_pioneer` or `server_derived_ephemeral`. For normal attached tasks, omit both `launch` and `targetOptionId`; no options lookup is required. The server rechecks authorization when `task_create` runs.

## task_create For Attached Subagents

Use `task_create` without `trigger` for immediate attached subagents.

```json
{
  "title": "Inspect memory extraction",
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
- Authorized reviewers receive `reviewContent` for each `reviewRequired` candidate. `summary` alone is not the full result.

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

Use after reviewing a candidate returned by `task_wait.reviewRequired`.

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
