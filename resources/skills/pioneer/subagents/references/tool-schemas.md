# Attached Subagent Tool Schemas

Use this reference when exact task tool arguments matter for immediate attached subagents. Task tools use strict function arguments: pass fields at the top level, use camelCase field names, and do not add wrappers such as `task`, `spec`, `schedule`, or `triggerInput`.

## Contents

- Tool Visibility
- task_create for attached subagents
- task_wait
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
