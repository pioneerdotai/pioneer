# Durable Task Tool Schemas

Use this reference when exact task tool arguments matter for durable, scheduled, recurring, background, or existing task work. Task tools use strict function arguments: pass fields at the top level, use camelCase field names, and do not add wrappers such as `task`, `spec`, `schedule`, or `triggerInput`.

## Contents

- Tool Visibility
- task_create for scheduled/background tasks
- trigger
- task_list
- task_get
- task_update
- task_reschedule
- task_pause and task_resume
- Delivery Policy
- task_wait caveat

## Tool Visibility

Only call tools visible in the current turn. If a needed task tool is hidden and `request_tools` is visible, request the `task` domain first:

```json
{
  "domains": ["task"],
  "reason": "Need task tools to create, inspect, update, reschedule, pause, resume, or troubleshoot durable task work."
}
```

If the tool remains unavailable, do not fake the operation.

## task_create For Scheduled Or Background Tasks

Use `task_create` with `trigger` for scheduled, interval, or cron work.

Common cron task:

```json
{
  "title": "Repository release monitor",
  "goal": "Check for new releases and post a product-facing summary when there is a new release.",
  "trigger": {
    "kind": "cron",
    "cronExpr": "0 5 * * *",
    "timezone": "Europe/Moscow"
  },
  "instructions": [
    "Each run, fetch the latest release for OWNER/REPO using any available web or HTTP tool.",
    "Compare the latest tag with the configured baseline or available durable state.",
    "If there is no new release, return a concise no-op result.",
    "If there is a new release, analyze the changelog from a product perspective.",
    "Fail clearly if release data is unavailable."
  ],
  "inputText": "Repository: https://github.com/OWNER/REPO. Baseline release tag: vX.Y.Z.",
  "outputInstructions": "Return Russian Markdown. If a new release exists, include a product summary with concise sections. If no release changed, return 'Новых релизов нет.'",
  "deliveryPolicy": {
    "mode": "thread",
    "threadTarget": "origin_thread",
    "includeResult": true,
    "format": "full_result"
  }
}
```

Important fields:

- `title`: human-visible label.
- `goal`: durable objective.
- `trigger`: schedule definition.
- `instructions`: self-contained future-run behavior.
- `inputText` or `input`: data, paths, URLs, ids, baselines, and scope.
- `outputInstructions`: result shape, language, no-op behavior, and failure behavior.
- `deliveryPolicy`: where terminal results appear.
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
    "threadTarget": "exact_thread",
    "threadId": "CURRENT_THREAD_ID",
    "includeResult": true,
    "format": "full_result"
  }
}
```

Change delivery to the origin thread:

```json
{
  "taskId": "TASK_ID",
  "deliveryPolicy": {
    "mode": "thread",
    "threadTarget": "origin_thread",
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

## task_pause And task_resume

Use pause/resume only when those tools are visible.

Pause when future runs should stop temporarily but the task should remain configured.

Resume when a paused task should run according to its configured trigger again.

After pausing or resuming, verify the current task status with the tool result or `task_get`.

## Delivery Policy

```json
{
  "mode": "none | thread | user_notification | webhook",
  "threadTarget": "origin_thread | current_thread | collaboration_root | exact_thread",
  "threadId": "THREAD_ID",
  "webhookUrl": "https://example.com/hook",
  "includeResult": true,
  "format": "summary | full_result"
}
```

Rules:

- `threadTarget` is required for `mode:"thread"`.
- `threadId` is supplied by the caller only for `threadTarget:"exact_thread"`; Gateway resolves the other targets.
- `webhookUrl` is required for `mode:"webhook"`.
- `user_notification` and `none` do not use `threadTarget` or `threadId`.
- Use `includeResult:true` when the user expects the answer text.
- Use `format:"full_result"` when preserving markdown, citations, sections, or artifacts matters.

## task_wait Caveat

Do not call `task_wait` after creating scheduled future work unless the returned run is active and waitable.

Scheduled, interval, and cron tasks often return `waitable:false` or `runId:null` at creation. Confirm the schedule instead of waiting.
