# Memory Tool Schemas

Use this reference when exact memory tool arguments matter. Memory tools use strict function arguments: pass fields at the top level, use the documented casing, and do not add wrappers or unknown fields.

## Tool Visibility

Only call tools visible in the current turn. If a needed memory tool is hidden and `request_tools` is visible, request the `memory` domain first:

```json
{
  "domains": ["memory"],
  "reason": "Need memory tools to read, list, save, or forget durable memory for this turn."
}
```

If the tool remains unavailable, do not fake the operation. Answer from visible context and explain the limitation.

## memory_search

Use `memory_search` for semantic relevance lookup across durable memory. Use it proactively when remembered context may improve the turn; do not wait for the user to explicitly ask about memory.

```json
{
  "query": "string",
  "scopes": ["user", "workspace", "thread", "agent", "task"],
  "categories": [
    "identity",
    "preference",
    "biography",
    "relationship",
    "recurring_instruction",
    "project_policy",
    "project_fact",
    "project_decision",
    "procedure",
    "todo",
    "constraint",
    "communication_style",
    "custom"
  ],
  "limit": 5,
  "includeProvenance": false
}
```

Required field:

- `query`

Use it proactively for:

- identity, biography, preferences, relationships;
- recurring instructions and communication style;
- project rules, facts, decisions, procedures, and constraints;
- stable todos, baselines, and known durable context.
- continuation-heavy work where prior durable decisions or preferences may matter.

Do not use it for full inventory. Semantic search can miss memories that are stored but not semantically close to the query.

Good query style:

- same language as the user when practical;
- concrete nouns over vague terms;
- include project names, dates, versions, paths, people, and keys when relevant;
- use category/scope filters only when they are actually known.

Example:

```json
{
  "query": "what is the user's name",
  "scopes": ["user"],
  "categories": ["identity"],
  "limit": 5,
  "includeProvenance": true
}
```

## memory_list

Use `memory_list` for inventory, audits, review, cleanup, and broad deletion planning.

```json
{
  "scopes": ["user", "workspace", "thread", "agent", "task"],
  "categories": [
    "identity",
    "preference",
    "biography",
    "relationship",
    "recurring_instruction",
    "project_policy",
    "project_fact",
    "project_decision",
    "procedure",
    "todo",
    "constraint",
    "communication_style",
    "custom"
  ],
  "statuses": ["active", "superseded", "deleted", "expired"],
  "limit": 100,
  "cursor": "string",
  "includeProvenance": false
}
```

Common payload:

```json
{
  "statuses": ["active"],
  "limit": 100,
  "includeProvenance": true
}
```

Rules:

- Use `statuses:["active"]` for normal user-facing inventory.
- Include deleted/superseded only when the user asks for history, provenance, or audit details.
- If a cursor is returned, paginate until enough records are collected for the user's request.
- Use this before broad delete/cleanup operations.

## memory_get

Use `memory_get` for exact lookup by memory id or stable scoped key.

```json
{
  "memoryId": "string",
  "key": "string",
  "scope": "user | workspace | thread | agent | task"
}
```

Use `memoryId` when it came from `memory_search` or `memory_list`.

Use `key` plus `scope` when the key is known and stable:

```json
{
  "key": "hermes_agent_release_tracker_last_checked_release",
  "scope": "workspace"
}
```

Use this after search/list when exact details, provenance, or stable-key lookup matter.

## memory_remember

Use `memory_remember` for direct durable writes. Use it proactively when the user provides clearly durable, future-useful information, even if the user did not explicitly say "remember this".

```json
{
  "content": "string",
  "category": "identity | preference | biography | relationship | recurring_instruction | project_policy | project_fact | project_decision | procedure | todo | constraint | communication_style | custom",
  "key": "string",
  "scope": "user | workspace | thread | agent | task",
  "sensitivity": "normal | personal | secret_like | regulated",
  "confidence": 0.9,
  "importance": 0.7,
  "source_context": "direct_user_conversation | assistant_response",
  "idempotency_key": "string"
}
```

Required fields:

- `content`
- `category`

Good proactive write candidates:

- stable user preferences and communication style;
- durable identity or biography facts;
- recurring instructions;
- project rules, constraints, decisions, and procedures;
- operational baselines that future turns should update.

Bad write candidates:

- temporary task progress;
- one-off commands;
- raw logs;
- guesses or uncertain inferences;
- assistant-only claims without user confirmation.

Good payload:

```json
{
  "content": "User prefers concise answers.",
  "category": "communication_style",
  "scope": "user",
  "key": "preferred_answer_length",
  "sensitivity": "normal",
  "source_context": "direct_user_conversation"
}
```

Good operational baseline:

```json
{
  "content": "Hermes Agent release tracker baseline: last checked release tag is v2026.6.5, published 2026-06-06.",
  "category": "procedure",
  "scope": "workspace",
  "key": "hermes_agent_release_tracker_last_checked_release",
  "sensitivity": "normal",
  "source_context": "direct_user_conversation"
}
```

Bad payload:

```json
{
  "memory": {
    "categories": ["procedure"],
    "content": "Last checked release tag is v2026.6.5.",
    "provenance": "Set during task setup.",
    "scope": "user"
  }
}
```

Why it is bad:

- `memory` wrapper is invalid;
- `categories` is invalid for writes, use singular `category`;
- `provenance` is not a tool argument;
- scope is probably wrong for a project baseline.

## memory_forget

Use `memory_forget` to suppress/delete durable memory by id or scoped key.

```json
{
  "memoryId": "string",
  "key": "string",
  "scope": "user | workspace | thread | agent | task",
  "reason": "string",
  "dryRun": false,
  "idempotency_key": "string"
}
```

Prefer `memoryId` after `memory_list` or `memory_search`.

Use `key` plus `scope` only when exact.

Use `dryRun:true` when the user expects confirmation before deletion:

```json
{
  "memoryId": "mem_123",
  "reason": "User asked to forget this stored preference.",
  "dryRun": true
}
```

After confirmation:

```json
{
  "memoryId": "mem_123",
  "reason": "User confirmed deletion of this stored preference.",
  "dryRun": false
}
```
