# Task Tool Schemas

Use this reference when exact task tool arguments matter. Task tools use strict function arguments: pass fields at the top level, use camelCase field names, and do not add wrappers such as `task`, `spec`, `schedule`, or `triggerInput`.

## Contents

- Tool Visibility
- task_create
- trigger
- task_wait
- task_accept
- task_revise
- task_cancel
- task_detach
- task_list
- task_get
- task_update
- task_reschedule
- Delivery Policy

## Tool Visibility

Only call tools visible in the current turn. If a needed task tool is hidden and `request_tools` is visible, request the `task` domain first:

```json
{
  "domains": ["task"],
  "reason": "Need task tools to create, wait for, update, review, cancel, detach, or inspect task-backed work."
}
```

If the tool remains unavailable, do not fake the operation.

## task_create

Use `task_create` for immediate attached subagents and durable scheduled tasks.

Common attached subagent:

```json
{
  "title": "Inspect task delivery",
  "goal": "Find why completed scheduled task results do not appear in the parent thread.",
  "agentRole": "researcher",
  "agentNickname": "Delivery researcher",
  "instructions": [
    "Inspect the repository read-only.",
    "Use search and file-reading tools before answering.",
    "Return exact file:line references for every claim.",
    "Do not modify files."
  ],
  "inputText": "Focus on task delivery, result candidates, and parent timeline materialization.",
  "outputInstructions": "Return Markdown with sections: inspected files, findings, evidence, and conclusion."
}
```

Common cron task:

```json
{
  "title": "Hermes Agent release monitor",
  "goal": "Check for new Hermes Agent releases and send a product-facing summary when there is a new release.",
  "trigger": {
    "kind": "cron",
    "cronExpr": "0 5 * * *",
    "timezone": "Europe/Moscow"
  },
  "instructions": [
    "Use currently available tools by capability, not by creation-time assumptions.",
    "Fetch the latest release from https://api.github.com/repos/NousResearch/hermes-agent/releases/latest.",
    "Compare the latest tag with the configured baseline or available durable state.",
    "If there is no new release, return a concise no-op result.",
    "If there is a new release, analyze the changelog from a product perspective.",
    "Fail clearly if GitHub cannot be reached or release data is unavailable."
  ],
  "outputInstructions": "Return Russian Markdown. If there is a new release, include a product summary with concise sections. If there is no new release, return 'Новых релизов нет.'",
  "deliveryPolicy": {
    "mode": "owner_thread",
    "includeResult": true,
    "format": "full_result"
  }
}
```

Important fields:

- `title`: human-visible label and child thread title.
- `goal`: short objective.
- `trigger`: omit for immediate attached work; use directly for scheduled work.
- `instructions`: behavior and constraints.
- `inputText`: simple task data and scope.
- `input`: structured data, variables, references, or attachments.
- `outputInstructions`: final shape and delivery contract.
- `deliveryPolicy`: scheduled/detached delivery behavior; omit for normal attached subagents unless needed.
- `toolPolicy`, `contextPolicy`, `resultContract`: advanced controls; omit unless required.

Do not pass:

- `workspaceId`
- `ownerKind`
- `parentTaskId`
- `rootTaskId`
- `depth`
- `model`
- `modelProvider`
- `trigger.spec`

## trigger

Use trigger objects directly.

One-time future run:

```json
{
  "trigger": {
    "kind": "scheduled_at",
    "scheduledAt": 1893456000,
    "timezone": "UTC"
  }
}
```

Cron:

```json
{
  "trigger": {
    "kind": "cron",
    "cronExpr": "0 9 * * 1-5",
    "timezone": "Europe/Moscow"
  }
}
```

Do not use:

```json
{
  "trigger": {
    "spec": {
      "kind": "cron"
    }
  }
}
```

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
- Do not wait on scheduled future tasks with `waitable:false` or `runId:null`.
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
  "feedback": "The result misses deliveryPolicy.thread mode. Add a section explaining owner_thread vs thread vs user_notification and include file:line evidence.",
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

## task_list

Use to find existing tasks before updating or auditing.

```json
{
  "limit": 50
}
```

Filter when available and useful. After finding a likely task, use `task_get` for exact details before patching important fields.

## task_get

Use for exact current task state.

```json
{
  "taskId": "TASK_ID"
}
```

Use before `task_update` when the update depends on preserving existing schedule, instructions, delivery, or revision.

## task_update

Patch only fields that should change. Omitted fields keep their current value.

Change delivery to this thread:

```json
{
  "taskId": "TASK_ID",
  "deliveryPolicy": {
    "mode": "thread",
    "threadId": "CURRENT_THREAD_ID",
    "includeResult": true,
    "format": "full_result"
  }
}
```

Change delivery to owner thread:

```json
{
  "taskId": "TASK_ID",
  "deliveryPolicy": {
    "mode": "owner_thread",
    "includeResult": true,
    "format": "full_result"
  }
}
```

For scheduled, interval, and cron agent tasks, do not clear `instructions` or `outputInstructions` unless replacing them with valid self-contained versions.

## task_reschedule

Use to change only the schedule.

```json
{
  "taskId": "TASK_ID",
  "trigger": {
    "kind": "cron",
    "cronExpr": "0 7 * * *",
    "timezone": "Europe/Moscow"
  }
}
```

Use `task_update` when changing schedule plus instructions, delivery, or other task fields.

## Delivery Policy

```json
{
  "mode": "none | owner_thread | thread | user_notification | webhook",
  "threadId": "THREAD_ID",
  "webhookUrl": "https://example.com/hook",
  "includeResult": true,
  "format": "summary | full_result"
}
```

Rules:

- `threadId` is required for `mode:"thread"`.
- `webhookUrl` is required for `mode:"webhook"`.
- `owner_thread`, `user_notification`, and `none` do not need `threadId`.
- Use `includeResult:true` when the user expects the answer text.
- Use `format:"full_result"` when preserving the child markdown matters.
