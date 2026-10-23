# Threads Troubleshooting

## Tool Availability

A skill is documentation, not a grant. Native agents request `domains:["threads"]` when hidden operations are needed and `request_tools` is available. CLI agents use the fixed current-turn catalog and supplied callable mapping; do not attempt to open additional tools. An options-only catalog does not imply read/create/send/start rights. Never substitute Task tools for a denied Threads operation.

## Access and Sources

Access denial means this execution cannot perform the operation. It does not prove the source never existed or was deleted. Read errors can deliberately combine missing and denied sources as `source unavailable or access denied`; preserve that uncertainty. Every page, child transition, source/tool-output reference, and mutation is subject to the initiating user's current rights and the execution's admitted scope. Broader current user rights cannot expand that scope.

Do not retry via another thread, tool, raw database path, or guessed opaque route to bypass a refusal. A known readable `threadId` is not authority to write or start a turn there.

## Stale Options or Cursor

For revoked/expired/unavailable routes or changed identity/profile options, refresh through `threads_start_options` (and the identity catalog if selecting explicitly), then reconcile the requested action. Do not guess replacement IDs. A new options list still does not grant a mutation capability.

For a stale history/source revision, restart a read or exact anchor with fresh state. For an invalid cursor/scope mismatch, check operation, thread, limit, direction, query/recursive parameters, and cursor provenance. Do not alter opaque cursors. Keep reader structured `{version,offset}` cursors separate from all opaque string cursors. Re-read a changed tool-result source from its beginning if authorized; do not concatenate bytes from different revisions.

An anchor must identify an accessible saved record in the selected thread. A disappeared source may be deleted, superseded, or inaccessible; do not claim an unrelated record is the requested source.

## Incomplete Reads and Search

An empty list page with `eof:false` requires continuation. History `eof` is directional and snapshot-bound; a reverse side can still have more events. Fragment `eof` concerns one oversized event only. Finish that event in the same direction before changing direction, matching its version and offsets. If continuation fails, report partial reading rather than claiming the record/history is complete.

Search `status:work_budget` is an unfinished search; follow `nextCursor` even if no cards were returned. `partial`, `index_unavailable`, `incomplete:true`, and `approximate:true` limit what the results establish. Do not report proven absence of matches from those conditions. The server owns lexical/hybrid choice; do not retry with invented mode or embedding arguments. Read relevant source history before relying on a search excerpt as full context.

Saved tool results can contain upstream truncation or `fullResultSaved:false`. Reading until `eof` recovers only retained content. Saved logs use separate JSON objects per chunk and `eof` describes the currently retained log, not future output from a still-running command.

## Mutation Outcomes and Replay

Read `status`, `outcome`, `receiptId`, `outboxId`, `publicMessage`, and returned target options as provided. `denied` is not a successful operation; `failed` is not proof a destination is absent. `accepted`/`queued` does not mean the launched model finished. `agent_action_already_committed` during replay indicates an existing action; reconcile it rather than launching another action.

Transport interruption or an execution error can leave an uncertain result. Do not blindly repeat create/send/start with a fresh call identity. Existing transport/recovery identity, the Agent action ledger, receipts, and outbox own replay. CLI supplies stable logical call identity even though these tools have no JSON `idempotencyKey` field; absence of that field does not prohibit first delivery. Preserve the existing invocation through runtime recovery and inspect confirmed results/history where authorized. A genuinely new call may be a new effect. Do not invent a new idempotency mechanism or add Task error envelopes, wait/review, or heartbeat behavior.

If execution reports `agent_runtime_integrity_lost`, a stale execution, recovery quarantine, or another unconfirmed outcome, report exactly what remains unknown. Do not claim a create/send/start occurred without confirmation, and do not automatically create replacement work.
