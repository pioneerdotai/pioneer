# Task Workflows And Examples

Use this reference for common multi-step task and subagent workflows.

## Contents

- Parallel Code Investigation
- Independent Review Subagent
- Scheduled Release Monitor
- Fix Delivery From Notification To Thread
- Parent Synthesis After Subagents
- Revision Workflow
- Cancel Or Detach

## Parallel Code Investigation

User:

```text
Find why scheduled task results are not visible in the parent thread.
```

Good split:

- Subagent A: inspect task delivery service and persistence.
- Subagent B: inspect desktop/client notification handling.
- Subagent C: inspect task creation/update schemas and defaults.

Create all three first, then wait once:

```json
{
  "title": "Delivery service investigation",
  "goal": "Find how task delivery turns result snapshots into thread items or notifications.",
  "agentRole": "researcher",
  "instructions": [
    "Inspect the repository read-only.",
    "Return exact file:line references.",
    "Do not modify files."
  ],
  "inputText": "Focus on task delivery execution, owner_thread, thread, user_notification, webhook, and delivered_turn_id.",
  "outputInstructions": "Return findings with evidence and a product-level explanation."
}
```

Then:

```json
{
  "runIds": ["RUN_A", "RUN_B", "RUN_C"],
  "timeoutMs": 120000,
  "returnCompleted": true,
  "returnPending": true
}
```

Review each candidate. Accept only evidence-backed results. Synthesize one parent answer.

## Independent Review Subagent

Use when you made a change and want independent verification.

Child task:

```json
{
  "title": "Review subagents skill update",
  "goal": "Review the edited Pioneer subagents skill for incorrect task semantics or missing delivery guidance.",
  "agentRole": "reviewer",
  "instructions": [
    "Read the changed skill files.",
    "Look for factual errors, missing warnings, and confusing examples.",
    "Do not edit files."
  ],
  "inputText": "Changed files: resources/skills/pioneer/subagents/SKILL.md and references.",
  "outputInstructions": "Return a code-review-style list of findings with file:line references. If no issues, say so and list residual risks."
}
```

Do not pass your intended conclusion. The point is independent validation.

## Scheduled Release Monitor

User:

```text
Every day at 5 Moscow time, check whether this repo has a new release. If yes, send me a product summary here in the same format.
```

Good product choices:

- Use cron trigger with `timezone:"Europe/Moscow"`.
- Make instructions self-contained.
- Use `owner_thread` or `thread` delivery, not `user_notification`.
- Define no-op behavior.
- Include a clear baseline or state comparison rule.

Payload shape:

```json
{
  "title": "Repository release monitor",
  "goal": "Check for new releases and post a product summary when there is a new release.",
  "trigger": {
    "kind": "cron",
    "cronExpr": "0 5 * * *",
    "timezone": "Europe/Moscow"
  },
  "instructions": [
    "Each run, fetch the latest release for OWNER/REPO using any available web or HTTP tool.",
    "Compare the latest tag with the configured baseline or available durable state.",
    "If there is no new release, return a concise no-op result.",
    "If there is a new release, fetch the release body or page and summarize the changelog from a product perspective.",
    "Use Russian Markdown.",
    "Fail clearly if release data is unavailable."
  ],
  "inputText": "Repository: https://github.com/OWNER/REPO. Baseline release tag: vX.Y.Z.",
  "outputInstructions": "If a new release exists, return a Markdown product summary with sections and concise explanations of why each change matters to users. If no new release exists, return 'Новых релизов нет.'",
  "deliveryPolicy": {
    "mode": "owner_thread",
    "includeResult": true,
    "format": "full_result"
  }
}
```

Confirmation should say the result will be posted in the thread and whether no-op runs will be visible.

## Fix Delivery From Notification To Thread

User:

```text
Change this task so answers come here. You configured it as a user notification.
```

Workflow:

1. Use `task_list` or known task id to identify the task.
2. Use `task_get` to inspect current delivery.
3. Patch only `deliveryPolicy`.
4. Verify the update.
5. Explain that future runs will post to this thread; prior runs are not retroactive.

Patch:

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

Good final answer:

```text
Done. The task now delivers full results to this thread instead of user notifications. This applies to future runs; the run that already completed will not be reposted automatically.
```

## Parent Synthesis After Subagents

When all attached child work is accepted, do not paste raw child outputs blindly. Integrate them.

Good synthesis:

- start with the direct answer;
- mention child work only if useful;
- combine duplicate findings;
- keep evidence from children when important;
- call out uncertainty and failures.

Bad synthesis:

```text
Subagent A said...
Subagent B said...
Subagent C said...
```

unless the user asked for an audit trail.

## Revision Workflow

When a candidate is close but incomplete:

1. Identify the missing requirement.
2. Request revision with precise feedback.
3. Wait again.
4. Review the revised candidate.
5. Accept only the revised satisfactory candidate.

Example feedback:

```json
{
  "feedback": "The answer explains user_notification but does not say whether previous runs are retroactive. Add a section 'Retroactivity' explaining that delivery changes affect future runs only, and include evidence from the task update behavior.",
  "additionalInstructions": [
    "Keep the existing correct delivery-mode explanation.",
    "Return the complete revised answer, not a diff."
  ]
}
```

## Cancel Or Detach

Cancel when:

- the user's goal changed;
- the child is working on the wrong thing;
- the result would be unsafe or irrelevant;
- a duplicate child is no longer needed.

Detach when:

- the task should keep running in the background;
- the parent can answer now without waiting;
- the user explicitly wants background work.

Do not detach review-required work. Resolve the candidate first.
