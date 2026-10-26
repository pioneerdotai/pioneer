# Threads Tool Contracts

Use current callable schemas as the authority. Canonical names below are mapped to provider callables by the current CLI turn; no provider name is hardcoded into this skill. All tools are subject to the available subset and current authorization. Do not add runtime-owned workspace, principal, route, or execution fields to tools that do not declare them.

## Observation Tools

| Tool | Input | Continuation and result |
| --- | --- | --- |
| `threads_list` | Required `limit` (1–100); optional string/null `cursor` | `threads`, `nextCursor`, `eof`; immutable thread-ID order, including accessible internal/archived threads |
| `threads_history_read` | Required `threadId`, `limit` (1–100); optional `anchor:{turnId,itemId?}`, `direction:"older"` or `"newer"` (default older), string/null `cursor` | `events`, `nextCursor`, `eof`, `reverseCursor`, `reverseDirection`, `direction` in a saved snapshot |
| `threads_children_list` | Required `threadId`, `recursive`, `limit` (1–100); optional string/null `cursor` | `threads`, `nextCursor`, `eof`; direct children or recursive descendants |
| `threads_search` | Required nonblank `query`, `limit` (1–100); optional string/null `cursor` | `threads`, `nextCursor`, `eof`, `coverageIncomplete`, `warnings`, optional `pageFragment` |

No maximum query length is imposed. Listing and children may return an empty nonterminal page. Repeat with the issued cursor, original limit, and original recursive setting when applicable. A changed listing source can invalidate the cursor; restart that listing rather than editing the cursor. Children cursors track traversed edges/returned metadata, not unrelated workspace activity.

Large list/children metadata uses JSON fragments (`encoding`, `version`, `offset`, `nextOffset`, `eof`, `text`); concatenate exact text by UTF-8 byte offsets and decode only at EOF. Titles are not shortened to fit a card.

### Search Sources And Large Cards

Each `threads[]` card has `threadId`, nullable `title`, and `fragments[]`. A fragment has `text`, optional/nullable `score`, and `source`:

```json
{"workspaceId":"WORKSPACE_FROM_SOURCE","threadId":"THREAD_FROM_SOURCE","turnId":"TURN_FROM_SOURCE","itemId":"ITEM_IF_PRESENT","sourceKind":"SOURCE_KIND_FROM_RESULT","indexItemId":"INDEX_ITEM_FROM_RESULT","sourceVersion":"VERSION_FROM_RESULT"}
```

`itemId` may be absent. These references identify saved sources, not write routes. Read the actual history; snippets do not replace it.

The server chooses lexical/hybrid retrieval over the accessible indexed corpus. Page limits of 100 fragments and 2 MiB do not cap the searched threads or files. Approximate semantic ordering can vary between pages; no fixed search session, complete vector scan, or absolute no-overlap guarantee is implied. `nextCursor` advances results, not a deferred partial corpus scan.

`coverageIncomplete:true` and `warnings` report unavailable indexes or semantic retrieval. Threads without an index have no searchable indexed content; search is not proof of absence in their saved histories. Access/source changes may invalidate continuation. Do not hide warnings or describe index failures as missing history.

An oversized card is returned as `pageFragment:{text,source,version,complete}`. Follow the response `nextCursor`, keeping query and limit. Concatenate exact `text` for the same source/version until `complete:true`, decode that JSON card, then continue global results using the next response cursor. Search `eof` remains distinct from card `complete`.

### History Direction And Fragment Continuation

An initial unanchored read must use `older`. An anchored read may use either direction. `anchor` and `cursor` are mutually exclusive. Omitting anchor `itemId` opens the turn; specifying it opens the saved item. Neighboring context is bounded and can span pages. Every page is displayed newest to oldest, including `newer` reads.

Continue with `nextCursor` in the same direction; `eof` ends only that direction. Use `reverseCursor` with the returned `reverseDirection` to read the opposite side of the same snapshot. Omit anchor on all cursor requests, preserve threadId and limit. Do not invent a cursor or alter it. New activity does not itself add events to a captured snapshot; edits/deletions affecting its source can invalidate it.

An oversized event has a `fragment` with `encoding:"json"`, `version`, byte `offset`, `nextOffset`, `eof`, and `text`. Preserve text exactly, assembling offset-contiguous fragments through `fragment.eof:true` before decoding the event. Follow `nextCursor` to finish fragments before moving to another event or reversing direction. A fragment can carry `sourceThreadReference` or `childReference`; do not treat those as a fully decoded event. A `childReference:{threadId,parentThreadId}` points to a separate history; read selectively and retain the parent cursor.

### Saved Tool Result Reader

`threads_tools_result_read` uses a different casing and cursor contract:

```json
{"workspace_id":"WORKSPACE_FROM_REFERENCE","thread_id":"THREAD_FROM_REFERENCE","turn_id":"TURN_FROM_REFERENCE","item_id":"ITEM_FROM_REFERENCE","output_log":false}
```

Optional fields: `cursor`, `max_tokens`, `max_bytes`. Normal full reading can omit both size preferences; existing server page policies still apply. Result:

```json
{"text":"EXACT_SAVED_TEXT_FRAGMENT","next":{"version":"OPAQUE_AUTHENTICATED_TOKEN","offset":123},"eof":false}
```

Pass the whole returned `next` as `cursor` with the same source fields and `output_log`; keep any supplied `max_tokens` and `max_bytes` unchanged as well. `offset` is a Unicode scalar offset, not bytes. Keep `version` untouched.

- `output_log:false`: concatenate exact `text` fragments until `eof:true`, then parse the complete saved JSON.
- `output_log:true`: read the saved output/progress log as a sequence of separate JSON records. A single record can span pages; assemble its fragments before parsing that record. Parse each complete record separately, not the whole log as one JSON document. `eof:true` refers to the currently saved log and does not itself confirm that the command has finished.

Source deletion, modification, or lost access can stop a previously valid read.

Native full-result references can point to a saved complete search page while the working context shows an excerpt. The reader's `next` continues that saved page's text. Once decoded, the search response's `nextCursor` continues search with the original query/limit. Do not pass a search/history string cursor to the result reader. A `kind:"saved_item"` fallback with `fullResultSaved:false` represents only the retained item; upstream truncation markers remain truthful.

## Options And Identity/Models

`threads_start_options`: `{}` or `{ "limit":100, "cursor":"RETURNED_CURSOR" }`. Limit defaults to 100, range 1–100. Returns `identities`, `profiles`, inheritance flags, `allowedSkillIds`, `allowedMcpServerIds`, `maxPermissionProfile`, `threadCreationOptions`, `targetOptions`, `generationFingerprint`, `nextCursor`, `eof`, and `fragments`.

- `threadCreationOptions[]`: `{id,label}`. Pass the selected `id` as create `optionId`.
- `targetOptions[]`: `{id,label}`. Pass a destination permitting the intended send/start as `targetOptionId`.
- `identities[]`: `id`, `displayName`, `nickname`, optional `avatarRevision`, `roleLabel`, and `sourceKind`.
- `profiles[]`: `id`, `compatibleIdentityIds`, `backend`, `allowedReasoning`, `allowedPermissionProfiles`.
- Flags: `inheritParentIdentityAvailable`, `defaultPioneerIdentityAvailable`, `derivedEphemeralIdentityAvailable`, `inheritParentProfileAvailable`.

`threads_identity_models_list`: required `identityId`, optional `limit` (default 100, 1–100), string/null `cursor`. Returns `identityId`, nullable `identity`, `executors`, `nextCursor`, `eof`, `fragments`. Each executor gives `providerId`, `displayName`, `backend` (`api`, `cli`, `acp`), `models`, optional `defaultModelId`, and optional `unavailableReason`. A model gives `id`, `displayName`, nullable `reasoning`. Use actual returned IDs. A CLI executor's providerId names its configured instance, never a session or credential.

Both catalogs paginate. Reassemble entries in `fragments` by `collection`, `index`, `modelIndex`, `version`, `encoding`, `offset`, `nextOffset`, `eof`, and exact `text` before using the decoded JSON. Do not treat a missing entry on one page as absent from the entire catalog.

Reasoning metadata uses its own snake_case fields: `supported`, `effort_options`, `default_effort`, `mandatory`, `supports_token_budget`, `source`. Missing/null facts or an empty effort list are unknown; they do not prove no reasoning support. Exact profiles also constrain allowed reasoning. Never manufacture an effort or default.

## Mutation Payloads

`thread_create`:

```json
{"optionId":"RETURNED_CREATION_OPTION_ID"}
```

Only the option is accepted. No title/sidebar/threadId/input/launch fields. The server's home-capsule option creates a hidden internal conversation; delegated-root creation creates a visible collaborative workspace conversation. Select by the returned option's meaning, not by guessed opaque ID contents.

`thread_message_send`:

```json
{"targetOptionId":"RETURNED_SEND_TARGET_ID","input":[{"type":"text","text":"Here is the requested information."}]}
```

`threads_turn_start`:

```json
{"targetOptionId":"RETURNED_START_TARGET_ID","input":[{"type":"text","text":"Investigate this question and report findings here."}]}
```

Both use `AgentAuthoredInput`, an array of clean `UserInput` entries. Text can include `textElements` per the live schema. Exact versioned artifacts use `{"type":"artifact","artifactId":"RETURNED_ARTIFACT_ID","versionId":"RETURNED_VERSION_ID"}`. Local paths, URL attachments, unresolved mentions, and runtime-owned inputs are not allowed authored content.

Start optionally accepts `launch`:

- `identity`: `{"kind":"inherit_parent"}` (default), `{"kind":"default_pioneer"}`, `{"kind":"exact","id":"ADMITTED_ID"}`, or `{"kind":"server_derived_ephemeral","display_name_hint":"OPTIONAL_HINT","role_label":"OPTIONAL_ROLE"}` when available.
- `profile`: `{"kind":"inherit_parent"}` (default), `{"kind":"exact","id":"COMPATIBLE_PROFILE_ID"}`, or `{"kind":"model","providerId":"RETURNED_PROVIDER_ID","modelId":"RETURNED_MODEL_ID"}`. An omitted providerId inherits the compatible executor, otherwise selection must be unique.
- `reasoning`: `{"effort":"CATALOG_ADMITTED_EFFORT"}`.
- `useDefaultReasoning:true`: choose the selected model's native default, mutually exclusive with explicit reasoning. Omission/false and legacy `reasoning:null` preserve inheritance.
- Optional `permissionProfile`, `skillIds`, `mcpServerIds`: only selections admitted by the current options and live schemas. They cannot widen the execution's rights.

Omitted launch inherits current identity, provider/executor, model, reasoning under server rules, without a preliminary catalog lookup. A targetOptionId remains mandatory.

Mutation results: `status` (`accepted`, `queued`, `completed`, `denied`, `failed`), optional `outcome`, `receiptId`, `outboxId`, `publicMessage`, and `targetOptions`. Creation refreshes permitted targets, including on replay. Do not expect a result `threadId`. Existing call identity and Agent ledger/outbox handle replay; these JSON schemas declare no idempotencyKey.
