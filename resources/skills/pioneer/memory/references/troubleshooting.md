# Memory Troubleshooting

Use this reference after a memory tool failure, an empty result that looks suspicious, an incomplete inventory, or a confusing recalled context block.

## ToolNotVisible

Symptoms:

- a memory tool call fails because the tool is hidden;
- the tool you need is not available in the tool list.

Do this:

1. Do not retry the hidden tool directly.
2. If `request_tools` is visible, request the memory domain.
3. If memory tools remain unavailable, answer from visible context and tell the user memory tools are unavailable in this turn.

Good response:

```text
I cannot check memory in this turn because memory tools are unavailable. From the visible context ...
```

Bad response:

```text
I saved that to memory.
```

when no write tool succeeded.

## Invalid memory_remember Arguments

Common causes:

- wrapping arguments inside `memory`;
- using `categories` instead of `category`;
- sending `provenance`;
- using a category not supported by the tool;
- wrong casing such as `sourceContext` instead of `source_context`;
- writing a huge blob instead of a compact fact.

Correct minimum payload:

```json
{
  "content": "User prefers short answers.",
  "category": "communication_style"
}
```

Better payload:

```json
{
  "content": "User prefers short answers.",
  "category": "communication_style",
  "scope": "user",
  "key": "preferred_answer_length",
  "sensitivity": "normal",
  "source_context": "direct_user_conversation"
}
```

## Search Misses A Known Memory

Semantic search is not guaranteed to find every record.

Try this sequence:

1. Rephrase the query in the user's language.
2. Add concrete names, project names, dates, versions, or identifiers.
3. Remove category/scope filters if they might be wrong.
4. If inventory matters, switch to `memory_list`.
5. If an id/key is known, use `memory_get`.

Do not conclude that memory is empty just because one semantic search returned no hits.

## Inventory Seems Incomplete

Use `memory_list`, not `memory_search`.

Check:

- `statuses:["active"]` for normal user-facing inventory;
- pagination cursor;
- scope filters that might be too narrow;
- category filters that might exclude relevant records.

If the user asks "delete all", list enough records to resolve exact targets before forgetting.

## Conflicting Memories

If two active memories conflict:

1. Prefer newer records only when provenance and content support that.
2. If the user gives a current correction, follow the current user instruction.
3. If the user asks to clean it up, list/get the conflicting records and forget or update the stale one.
4. If unsure, explain the conflict and ask which one should remain.

Do not silently merge conflicting facts.

## User Says You Should Know Something

Check injected memory and visible tools. If the fact is unavailable, be direct:

```text
I do not see that record in the memory available for this turn. I can search further if memory tools are available, or save it now if you want.
```

Do not pretend to remember.

## Memory Was Saved But Should Be Changed

If a stable key exists, write the replacement with the same key. If stale records remain and the user asked for cleanup, forget the stale ids.

If no stable key exists, search/list first, then write the corrected memory and delete the wrong one only after the target is clear.

## Recalled Thread Context Looks Relevant But Insufficient

Thread context snippets are compact. They may omit surrounding conversation or artifact content.

If the snippet includes artifact refs and the answer depends on the artifact, use `artifact_read`.

If no tool can retrieve more context, say what is visible and avoid overclaiming.
