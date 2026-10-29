# Threads Workflows

Values in uppercase below stand for exact values returned by tools, not IDs to invent. Use only current callable names and permitted tools.

## Find Evidence And Open It

1. Search:

```json
{"query":"decision about the database writer queue","limit":25}
```

2. Take a selected `threads[].fragments[].source`. For a source with threadId `THREAD_FROM_SOURCE`, turnId `TURN_FROM_SOURCE`, itemId `ITEM_FROM_SOURCE`, call `threads_history_read` exactly as follows:

```json
{"threadId":"THREAD_FROM_SOURCE","anchor":{"turnId":"TURN_FROM_SOURCE","itemId":"ITEM_FROM_SOURCE"},"direction":"older","limit":25}
```

If the source has no itemId, omit that key:

```json
{"threadId":"THREAD_FROM_SOURCE","anchor":{"turnId":"TURN_FROM_SOURCE"},"direction":"older","limit":25}
```

3. Complete any event fragments. Continue history with its `nextCursor`, omitting anchor:

```json
{"threadId":"THREAD_FROM_SOURCE","cursor":"HISTORY_NEXT_CURSOR","direction":"older","limit":25}
```

For the opposite side, use `cursor: reverseCursor` and `direction: reverseDirection`, retaining limit. The snapshot's end in one direction is not the end in both.

4. Search continuation is separate:

```json
{"query":"decision about the database writer queue","limit":25,"cursor":"SEARCH_NEXT_CURSOR"}
```

A next page advances matching results over the searched accessible corpus. It is not a request to scan unvisited index files. If a response contains `pageFragment`, assemble that card through `complete:true` first, then continue results.

## Browse And Follow A Child

Call `threads_list` with `{"limit":50}` and follow nextCursor through eof, including empty nonterminal pages. The list starts with the newest creation dates, breaking ties by descending ID. Retain limit and the issued cursor; unrelated activity does not restart the walk. Start a fresh first page to see new threads before the current position. Finish any metadata fragments for the same card/version before decoding it. Select a thread and read its history. A childReference in that history can be opened directly using its threadId. Save the parent cursor before switching. To browse descendants separately:

```json
{"threadId":"SELECTED_PARENT_THREAD","recursive":true,"limit":50}
```

Follow pagination as needed and open only relevant histories. Descendant discovery does not require reading every descendant. Lost access to a child does not authorize another tool to bypass it.

## Read A Saved Search Page Or Tool Result

Use the response's actual full-result reference as `threads_tools_result_read` input. Preserve snake_case fields and optional `output_log`; pass the reader's `next` as the next reader `cursor`. With `output_log:false` or omitted, append exact response `text` until `eof:true`, then decode the assembled saved JSON, respecting its actual wrapper. With `output_log:true`, read separate JSON log records: assemble any fragments of one record before parsing it, and parse each record separately rather than the whole log as one JSON document. EOF refers to the currently saved log and does not itself confirm command completion. See [Saved Tool Result Reader](tool-schemas.md#saved-tool-result-reader). If the decoded saved result contains a search page with `nextCursor`, call `threads_search` with that cursor and the original query/limit for the next search page. The reader's `next` continues saved text; the search page's `nextCursor` continues search results. Do not confuse reading the rest of one saved page with requesting more search results.

## Create A Conversation

1. Call `threads_start_options` with `{}`; paginate if the needed option is on another page.
2. For a visible workspace conversation choose the server's delegated-root creation option. For an internal conversation choose home-capsule only if that is the requested scope.
3. Call `thread_create`:

```json
{"optionId":"SELECTED_THREAD_CREATION_OPTION_ID"}
```

4. Inspect status/outcome and retain the refreshed returned `targetOptions`. Creation alone runs no model. Do not claim a title was assigned or return an invented threadId.

## Add A Message Without Starting A Model

Choose an existing destination from `threads_start_options.targetOptions`, or refreshed creation `targetOptions`, whose label identifies the intended permitted destination. Call `thread_message_send`:

```json
{"targetOptionId":"SELECTED_MESSAGE_TARGET_ID","input":[{"type":"text","text":"The requested source is now available for review."}]}
```

Confirm the action result. Do not start a turn unless requested. A source threadId from search is not a message route.

## Create And Run With Inherited Settings

Create using the sequence above. From that confirmed creation result select the returned target option for starting the new conversation. Call `threads_turn_start`:

```json
{"targetOptionId":"CREATION_RESULT_START_TARGET_ID","input":[{"type":"text","text":"Review the supplied question and explain the findings in this conversation."}]}
```

No launch catalog is needed solely for inheritance. `targetOptionId` is still required. The input itself carries the task text; do not add Task fields such as goal or instructions. Accepted/queued start confirms admission/queuing, not a completed answer. The server follows the target thread’s execution rules. Collaborative conversations use an internal detached Task with normal result delivery; an executable TaskRun child continues in that same child without another Task or child thread. This creates no attached review candidate. Do not call task_create yourself or add Task goal/instructions fields to the Threads payload.

## Explicit Identity, Executor, Model, And Reasoning

1. From start options select an admitted identities[].id.
2. Call `threads_identity_models_list` with `{"identityId":"SELECTED_IDENTITY_ID"}`; follow nextCursor and assemble catalog fragments if necessary.
3. Select an available executor and its model. Use actual executor.providerId and model.id, not display names. Check reasoning metadata and compatible-profile restrictions; unknown metadata is not a capability assertion.
4. Start on a permitted target using the selected values:

```json
{"targetOptionId":"SELECTED_START_TARGET_ID","input":[{"type":"text","text":"Investigate the requested question."}],"launch":{"identity":{"kind":"exact","id":"SELECTED_IDENTITY_ID"},"profile":{"kind":"model","providerId":"SELECTED_EXECUTOR_PROVIDER_ID","modelId":"SELECTED_MODEL_ID"},"reasoning":{"effort":"SELECTED_ADMITTED_EFFORT"}}}
```

Only include reasoning when that effort is admitted. To deliberately choose the selected model's native default, replace the reasoning field with `"useDefaultReasoning":true`. Do not send both. To inherit reasoning, omit both; reasoning:null also inherits and is not a reset.

Alternatively use `profile:{"kind":"exact","id":"COMPATIBLE_PROFILE_ID"}` with its compatible identity and allowed selections. Do not guess models or defaults when no catalog is available; use permitted inheritance or explain the unavailable explicit selection.
