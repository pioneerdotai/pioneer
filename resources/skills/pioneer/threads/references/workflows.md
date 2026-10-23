# Threads Workflows

Examples use canonical tool names. Bind them to the actual current-turn CLI callables when running in CLI. Symbolic strings below stand for values returned by the preceding call; do not send the symbols literally or fabricate opaque IDs.

## Find a Conversation and Open the Exact Source

Call `threads_search`:

```json
{"query":"release approval discussion","limit":5}
```

Suppose a returned card contains this fragment (other fragment fields omitted here):

```json
{"threadId":"saved-thread","turnId":"saved-turn","itemId":"saved-item"}
```

Call `threads_history_read` with those exact source IDs:

```json
{"threadId":"saved-thread","limit":20,"anchor":{"turnId":"saved-turn","itemId":"saved-item"},"direction":"older"}
```

When a source has only `threadId`/`turnId`, use `anchor:{"turnId":"saved-turn"}`. There is no separate search-reference argument. A deleted/inaccessible anchor cannot be replaced by a guessed adjacent ID.

The anchored page begins near the source and includes bounded neighboring context. A turn-only anchor chooses the saved turn boundary, not every item of that turn. One page may not yet include a large anchor event: continue reading. Never combine `anchor` and `cursor`.

To continue older, copy that page's `nextCursor`:

```json
{"threadId":"saved-thread","limit":20,"direction":"older","cursor":"returned-nextCursor"}
```

To read toward the newer side of the original page, save its `reverseCursor` and `reverseDirection`. If the returned direction is `newer`, call:

```json
{"threadId":"saved-thread","limit":20,"direction":"newer","cursor":"returned-reverseCursor"}
```

Use the direction actually returned, including when reversing from a newer traversal back to older. Events within every page remain newest first. `eof` completes only the requested direction. The reverse cursor excludes the page edge it leaves; it is not a live-history poll and does not include later appends beyond the snapshot. Save cursors by thread and direction, rather than overwriting the parent's position while visiting another source.

If a record has `fragment.eof:false`, follow the current same-direction `nextCursor` first. Match `eventId` and `fragment.version`; concatenate `fragment.text` in ascending byte-offset order, checking `offset` against the preceding `nextOffset`. Parse only after the record's fragment `eof:true`. The event can span many pages. Do not switch to `reverseCursor` mid-record; it is a traversal edge, not a fragment continuation. Once complete, process later events on the page and use the remaining directional cursor normally.

## List Threads or Descendants

```json
{"limit":25}
```

Use that payload with `threads_list`. Continue with the same limit and returned string cursor until page `eof:true`, even after an empty page. This is a list of accessible workspace conversations, not a full-text search.

When history supplies `childReference.threadId`, open that child directly:

```json
{"threadId":"referenced-child","limit":20}
```

Call `threads_history_read` with this payload and retain the parent's continuation. To enumerate descendants separately, use `threads_children_list`:

```json
{"threadId":"parent-thread","recursive":true,"limit":25}
```

`recursive:false` lists only direct children. Neither setting loads child history or bypasses access checks; read only relevant children and follow each list's own cursor.

## Read a Large Saved Tool Result

Copy the exact supplied workspace/thread/turn/item reference into `threads_tools_result_read`:

```json
{"workspace_id":"source-workspace","thread_id":"source-thread","turn_id":"source-turn","item_id":"source-tool-item","max_tokens":4096,"max_bytes":32768}
```

For a response `next:{"version":"v","offset":2048}`, continue:

```json
{"workspace_id":"source-workspace","thread_id":"source-thread","turn_id":"source-turn","item_id":"source-tool-item","cursor":{"version":"v","offset":2048},"max_tokens":4096,"max_bytes":32768}
```

Those offsets count Unicode scalars. They are not history fragment byte offsets. Retain `output_log:true` across pages when selecting saved logs. Log chunks are separate JSON objects; a returned cursor can advance to another chunk/version. Read the retained stream accordingly instead of parsing all chunks as one JSON object. For normal results, join the canonical JSON text pages. Respect `fullResultSaved:false`, upstream truncation, and attachment references rather than claiming recovered bytes that were never saved.

## Create a Thread Without Running a Model

1. Call `threads_start_options` with `{}`.
2. Choose `threadCreationOptions` according to the intended surface. `New delegated root` creates a visible workspace conversation when permitted. `Current collaboration` creates a hidden internal child.
3. Call `thread_create` using the selected object's actual ID:

```json
{"optionId":"selected-threadCreationOptions-id"}
```

4. Check the `status`/`outcome`. Successful creation returns refreshed `targetOptions`; compare with the previous list to identify the newly available destination. No model has run. The result does not promise a `threadId` field or accept title/sidebar customizations.

## Add a Message Without Running a Model

Use a currently permitted `targetOptions[].id` from options or successful creation. Call `thread_message_send`:

```json
{"targetOptionId":"selected-targetOptions-id","input":[{"type":"text","text":"Here are the findings requested for this conversation."}]}
```

Inspect the operation result before claiming the message was added. Sending records an authored message without triggering a model. A read reference alone is not a valid destination option.

## Create a Thread and Then Start an Agent There

Perform the creation workflow above. Use the relevant refreshed target from the creation result, then call `threads_turn_start`:

```json
{"targetOptionId":"new-targetOptions-id-from-create","input":[{"type":"text","text":"Review the release proposal at the supplied URL. Compare its decisions with the saved approval discussion and return a concise report with evidence."}]}
```

Supply concrete URLs/source references/context in the text for real work. Omit `launch` for inheritance. No extra catalog lookup is required just to inherit current settings. `targetOptionId` is still required. The input belongs to the turn being started, so sending the same prompt first is unnecessary. Starting creates no Task and requires no Task heartbeat/wait/review/accept protocol. Do not claim its analysis has completed merely because the start was accepted or queued.

## Choose Identity, Executor, Model, and Reasoning Explicitly

1. Call `threads_start_options` with `{}` when an explicit identity or destination is needed. Select a returned permitted identity `id`.
2. Call `threads_identity_odels_list`:

```json
{"identityId":"returned-identity-id"}
```

3. Select an available returned executor and model. Use the executor's `providerId` and model's `id`, not display labels. If requesting an explicit effort, choose an allowed string from its `reasoning.effort_options`. Unknown metadata cannot justify inventing an effort.
4. Call `threads_turn_start` with the selected target:

```json
{
  "targetOptionId":"returned-target-id",
  "input":[{"type":"text","text":"Investigate the supplied proposal and report the result."}],
  "launch":{
    "identity":{"kind":"exact","id":"returned-identity-id"},
    "profile":{"kind":"model","providerId":"returned-executor-providerId","modelId":"returned-model-id"},
    "reasoning":{"effort":"returned-advertised-effort"}
  }
}
```

All placeholder strings must come from actual catalog selections, including the effort. For a compatible exact profile, replace `profile` with `{"kind":"exact","id":"returned-profile-id"}` and choose from its `allowedReasoning` when needed.

To use the selected model's native reasoning default instead, omit `reasoning` and set `launch.useDefaultReasoning:true`. Never combine both. Leaving reasoning unspecified (including legacy null) inherits; it does not reset reasoning. An executor can advertise `defaultModelId`; do not infer a default when absent. Omitted `providerId` inherits a compatible current executor, or must resolve uniquely; use a returned provider ID to disambiguate an explicit selection.
