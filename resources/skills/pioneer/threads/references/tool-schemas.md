# Threads Tool Contracts

Canonical names below are domain names. In CLI, use the corresponding callable actually supplied in the current turn catalog. These tools may be only partly available. Current runtime schemas and permissions remain authoritative.

## Observation Tools

| Tool | Input | Result and continuation |
| --- | --- | --- |
| `threads_list` | `limit` (1–100), optional string/null `cursor` | `threads`, `nextCursor`, `eof`; immutable thread-ID order; empty nonterminal pages possible |
| `threads_history_read` | `threadId`, `limit` (1–100), optional `anchor:{turnId,itemId?}`, `direction` (`older` default or `newer`), string/null `cursor` | `threadId`, `events`, `nextCursor`, `eof`, `reverseCursor`, `reverseDirection`, `direction` |
| `threads_children_list` | `threadId`, `recursive` (required boolean), `limit` (1–100), optional string/null `cursor` | `threads`, `nextCursor`, `eof`; selected descendant histories are read separately |
| `threads_search` | `query` (nonblank, at most 4096 characters), `limit` (1–20), optional string/null `cursor` (nonempty, at most 65536 characters) | `threads`, `nextCursor`, `status`, `approximate`, `incomplete` |

No model-supplied workspace/user selector exists on these four tools. Listing/children cursors are bound to the operation, source, user/workspace scope, recursion choice where applicable, and limit. History cursors additionally bind the snapshot, direction, and source revision. Do not decode or edit them. A history continuation omits `anchor` and retains the thread, limit, and direction encoded by the selected cursor. `direction:newer` on a fresh read requires an anchor.

Search cards contain `threadId`, optional `title`, `createdAt`, `updatedAt`, `relevance`, and `fragments`. Each fragment contains `workspaceId`, `threadId`, `turnId`, `itemId`, `sourceKind`, `text`, and `relevance`. Use those exact source references to read history. Search has no `eof`: inspect `nextCursor`, `status`, `approximate`, and `incomplete`. `more` means continuation; `work_budget` can return no cards and still require continuation. `partial` or `index_unavailable` means incomplete coverage. Even `exhausted` describes only the current retrieval, not proof of exhaustive semantic absence when coverage is incomplete or approximate. No search mode or embedding is accepted.

History events can expose `childReference`, `sourceThreadReference`, saved tool-result references, or `fragment` instead of a complete record. For oversized records, `fragment.encoding` is `json`; `version` identifies the record, `offset`/`nextOffset` are byte positions, `text` is a UTF-8-safe slice, and fragment `eof` means that record is complete. Finish the same-direction continuation before changing direction. A complete page is not the full conversation. Reverse cursors leave the page's opposite edge and preserve its snapshot; save the appropriate cursor before further traversal.

## Saved Tool Result Reader

`threads_tools_result_read` deliberately has a different casing and cursor contract:

```json
{
  "workspace_id":"workspace-from-reference",
  "thread_id":"thread-from-reference",
  "turn_id":"turn-from-reference",
  "item_id":"tool-item-from-reference",
  "output_log":false,
  "max_tokens":4096,
  "max_bytes":32768
}
```

Only the four IDs are required. `output_log` defaults to false; true selects the retained progress/output log; each chunk is a separate JSON object. Optional `max_tokens` and `max_bytes` are constrained to 1–4096 and 1–32768 by the tool schema. Response:

```json
{"text":"a fragment of retained canonical result JSON","next":{"version":"source-version","offset":120},"eof":false}
```

Pass `next` unchanged as the next call's `cursor`, retaining the IDs and log selection. Offset is a Unicode scalar offset, not bytes. Append the `text` fragments in sequence and parse the assembled normal result if needed. For logs, retain separate chunk objects; the cursor may advance to another chunk/version. Never substitute a history/search cursor. `eof:true` completes the retained source, not data already truncated by its original producer. References do not override current access, deletion, or source-version checks.

## Options and Mutations

`threads_start_options` accepts `{}`. Relevant result fields:

- `threadCreationOptions:[{id,label}]` and `targetOptions:[{id,label}]`: opaque permitted choices.
- `identities`: admitted identities (`id`, `displayName`, `nickname`, `sourceKind`, optional presentation fields).
- `profiles`: exact execution profile choices with `id`, `compatibleIdentityIds`, `backend`, `allowedReasoning`, `allowedPermissionProfiles`.
- `inheritParentIdentityAvailable`, `defaultPioneerIdentityAvailable`, `derivedEphemeralIdentityAvailable`, `inheritParentProfileAvailable`.
- `allowedSkillIds`, `allowedMcpServerIds`, `maxPermissionProfile`, `generationFingerprint`.

Do not synthesize option IDs from infrastructure IDs or use stale options across execution/source changes. Options do not grant mutation rights. The existing creation options have server semantics: `Current collaboration` creates a hidden internal child; `New delegated root`, when admitted, creates a visible workspace conversation. Labels are for selecting the intended option; copy its actual ID.

| Tool | Exact required input | Optional input |
| --- | --- | --- |
| `thread_create` | `optionId` | none |
| `thread_message_send` | `targetOptionId`, `input` | none |
| `threads_turn_start` | `targetOptionId`, `input` | `launch` |
| `threads_identity_odels_list` | `identityId` | none |

Unknown payload fields are rejected. Do not add `title`, `threadId`, `sidebar`, Task `goal`/`instructions`, or JSON `idempotencyKey` to these mutation payloads.

`input` is a transparent array of `UserInput` records constrained to clean authored content: text (`type:text`, `text`, optional `textElements`) or resolved versioned artifacts (`type:artifact`, `artifactId`, `versionId`). Server-side validation still applies. Common text input:

```json
{"targetOptionId":"returned-target-id","input":[{"type":"text","text":"Compare the proposals and report the evidence."}]}
```

Mutation results use `AgentToolSafeResult`: `status` (`accepted`, `queued`, `completed`, `denied`, `failed`), optional `outcome`, `receiptId`, `outboxId`, `publicMessage`, and nonempty `targetOptions` after successful thread creation (including replay). There is no promised `threadId` in this result. The response may also carry runtime transport metadata. A queued/accepted operation is not proof that the launched model finished. Inspect the returned status/outcome and relevant saved history before claiming completion.

## Launch Selection

Omitted `launch` inherits current identity and execution settings. Its optional fields are:

- `identity`: `{kind:"inherit_parent"}` (default), `{kind:"default_pioneer"}`, `{kind:"exact",id}`, or `{kind:"server_derived_ephemeral",display_name_hint?,role_label?}` when admitted. Identity choice variant fields use snake_case.
- `profile`: `{kind:"inherit_parent"}` (default), `{kind:"exact",id}`, or `{kind:"model",modelId,providerId?}`. `providerId` selects the returned executor, including a configured CLI instance; it is not a runtime session ID. Omitted provider inherits the compatible executor, otherwise resolution must be unique.
- `reasoning:{effort:string}`: select an advertised allowed effort.
- `useDefaultReasoning:true`: use the selected model's native default, mutually exclusive with explicit reasoning. Omitted/false or legacy `reasoning:null` retains inheritance.
- `permissionProfile`, `skillIds`, `mcpServerIds`: only permitted selections under the current envelope. Omit them unless needed; choices cannot widen rights.

`threads_identity_odels_list({identityId})` returns `identity` and `executors`. An executor has `providerId`, `displayName`, `backend` (`api`, `cli`, `acp`), `models`, optional `defaultModelId` and `unavailableReason`. A model has `id`, `displayName`, and nullable `reasoning`. Reasoning metadata uses snake_case: `supported`, `effort_options`, `default_effort`, `mandatory`, `supports_token_budget`, `source`. Unknown/null support or an empty effort list does not prove either support or lack of it. Skip unavailable executors; do not invent choices. For a compatible exact profile, copy its returned ID and use its actual `allowedReasoning` choices.

No discovery is necessary just to inherit. Selecting a launch never replaces the required target route or current authorization checks.
