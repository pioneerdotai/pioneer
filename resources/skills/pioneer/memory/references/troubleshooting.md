# Memory Troubleshooting

Use this reference after a memory tool failure, an empty result that looks suspicious, an incomplete inventory, or a confusing recalled context block.

## Contents

- ToolNotVisible
- Invalid memory_remember Arguments
- Search Misses A Known Memory
- Inventory Seems Incomplete
- Conflicting Memories
- User Says You Should Know Something
- Memory Was Saved But Should Be Changed
- Retention And Forget Guarantees
- Recalled Thread Context Looks Relevant But Insufficient

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

### Scope denial or null exact result

`scope_not_authorized` explains execution policy without looking up the requested
record. It does not imply that a hidden record exists. The mutation error
`this execution may mutate only thread-capsule or active-task memory` means that
user/workspace/agent writes are outside this execution's grant, not malformed JSON.
Do not retry with a different scope silently; report the limit and preserve the
user's requested ownership.

Even exact `memory_get` can return `record:null` because a record is absent,
expired, under repair, outside record-level authorization, or blocked by
sensitivity policy. An empty inventory is only empty within this execution's
visible area and supplied filters. Say "not visible in memory available to this
execution", not "the database is empty". Check the tool's runtime scope contract
and the full scope/namespace/key address before changing the request.

If an inventory/get record has `recallEligibility.eligible:false`, its reason
explains why it is audit-only. It may be inspected or forgotten when authorized,
but must not be promoted to a trusted answer merely because search omitted it.

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

Read the record first. A write to the same scope/namespace/key without `memoryId`
updates in place and retains the ID: never forget that "old" ID afterward.
Alternatively supply the existing `memoryId` to correct it; supersession is
atomic and the returned ID becomes the current one. No follow-up deletion of
the superseded ID is needed. This also works for generated or missing keys.

If the user requested duplicate cleanup, re-read other candidate IDs after the
update. Delete only separately verified stale active duplicates, never the ID
returned by the update. A different ID or similar text alone is not proof of a
duplicate: compare scope, namespace, meaning and provenance.

## Retention And Forget Guarantees

Apply the lifecycle contract in `SKILL.md`; explain only what the observed
operation supports. Examples:

- "How long will you remember this?" — "The normal memory write has no automatic
  expiry, so it can be used in later authorized turns. I cannot guarantee it will
  appear in every answer or survive deletion of the associated data."
- "Why didn't you remember?" — "Not appearing in this answer does not mean the
  record was deleted. I can check the exact record or the available inventory."
  Check tools and `recallEligibility`; do not assert a particular ranking, access
  or repair cause unless evidence identifies it.
- "Forget this fact." — After a non-dry-run result confirms the target ID:
  "I've removed that record from active durable memory and won't use it as
  remembered context. This does not erase the original conversation."
  If asked whether every copy is gone, explain that tombstones/previews, history,
  logs, backups and the episodic index are outside that guarantee. Tool success
  alone does not establish completion of physical backend cleanup.

If forget errors or returns no target IDs, do not claim a deletion succeeded.
Resolve the authorized target or report uncertainty; a read miss alone cannot
prove physical erasure. Do not recreate the record to test whether deletion worked.

## Recalled Thread Context Looks Relevant But Insufficient

Thread context snippets are compact. They may omit surrounding conversation or artifact content.

If the snippet includes artifact refs and the answer depends on the artifact, use `artifact_read`.

If no tool can retrieve more context, say what is visible and avoid overclaiming.
