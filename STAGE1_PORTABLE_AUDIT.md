# Stage 1 portable turn payload: source audit (blocked implementation)

This is an implementation prerequisite audit, not an implemented storage switch.
No schema, Entity, runtime, codec or test behavior changes are included.
Base: `0691788deb0ee6ef3bf8b7b0bc5e5655b7178801` (origin/main, 2026-11-14).

## Generation prerequisite

`crates/entity/AGENTS.md` requires the normal Migrator to create an isolated
SQLite database before SeaORM CLI generates Entity. The task forbids compiling
that Migrator, substituting DDL, and editing generated Entity manually.
`sea-orm-cli` exists at `/Users/alexander/.cargo/bin/sea-orm-cli`.
Neither this worktree nor the main checkout has a target directory containing a
ready Migrator. A source checkout and SeaORM CLI alone cannot execute newly
written Rust migrations. Even an older prebuilt Migrator would need to contain
the new migration to produce its schema; version provenance matters.

The required schema-to-Entity dependency cannot currently be completed under
these constraints. No unregistered draft migration or half-enabled discovery
has been installed. Resume with a ready normal Migrator containing the reviewed
new migration, or a separately revised execution constraint. This document does
not request an exception or authorize compilation.

## Current storage and candidate boundary

`crates/gateway/src/database/startup/zstd_payload_compression.rs` calls
`ensure_compression_schema`; `zstd_column.rs::enable_transparent_compression`
invokes `zstd_enable_transparent` on the writer. These targets may therefore be
plain tables or writable views over `_turn_event_zstd` / `_turn_item_zstd`.
The normal schema Migrator does not itself describe every runtime view shape.
Existing migrations, notably `m20261024_000001_root_conversation_compaction`,
choose physical backing tables when present. Preserve that compatibility.

Candidate minimal durable admission: reuse `portable_identity` with an empty
schema default for existing rows and an INSERT trigger assigning a random
incarnation identity only to new physical rows. Do not populate it on upgrade
or ordinary UPDATE. Discovery, source fragment capture, and publication must
all require nonempty identity, null reference, and existing domain eligibility.
Identity alone can encode admission: a separate eligibility queue/table is
unnecessary. This is a proposed boundary, not installed schema.

An UPDATE of a legacy row must leave identity empty, even when changing payload.
An UPDATE of an admitted row must retain identity, bump revision for logical or
ownership changes, clear/retire the old reference for logical replacement, and
wake the existing portable owner. Physical publication must change reference
atomically with source representation so logical-update triggers do not
invalidate their own publication. Delete/reinsert must allocate a fresh identity
regardless of key, timestamp, or rowid. The publication CAS must include identity,
revision, owner stamp, eligibility and null reference.

Do not call `portable_payload_target::up` unchanged: its pending index and generic
discovery currently admit historical null-reference values. Do not register the
two families before complete readers and legacy exclusion exist.

An index with a partial predicate including nonempty identity avoids indexing
historical payloads. SQLite may still scan existing rows to construct it; call
that O(N) metadata installation cost explicitly. It must not decompress, hash,
copy or rewrite historical payload values. Backing-table/view trigger rebuilding
and preservation of existing domain triggers remain unresolved implementation
work and require fresh/upgrade fixtures before acceptance.

## Coverage map

Paths below are relative to `crates/`; line anchors refer to the recorded base.
These are source findings and required regressions, not successful checks.

| Path | Existing behavior / risk | Required change and static proof | Regression to extend |
| --- | --- | --- | --- |
| `crud/src/repositories/turn_event.rs:87,127,169,187,214` | Prepared append serializes before transaction; preflight semantically decodes historical values; replay compares `Model.payload` with canonical JSON or preflight ID. | Hydrate outside writer; carry logical identity/revision/reference fences into atomic replay validation. A previously matching ID alone must not accept a new incarnation or changed payload. Preserve canonical/legacy idempotency keys. | Same-key retry before/after publication, conflicting bytes, transaction rollback, delete/reinsert after preflight. |
| `crud/src/repositories/turn_event.rs:318,338,351,378,514,558,576` | Sync decoder plus single/batch/latest/history reads assume logical TEXT. Ownership repair parses and CAS-compares payload. | Capture holds while reading bounded rows, hydrate after release, propagate corruption; compare physical revision/reference fences when logical JSON no longer equals stored placeholder. Keep sequence ordering and ownership repair semantics. | Mixed plain/Zstd/pending/PPAY history, repair with stale preparation, corruption, ordering. |
| `crud/src/repositories/canonical_turn_event.rs:749,803,875,943` | Terminal boundary, direct event and paged canonical history use Entity then sync `decode_event`; decoder validates payload owner and type. | Hydrate before decode; retain workspace/thread/turn/type validation and effective actor behavior. Follow all callers, not just public append. | Replay/rebuild and boundary selection across formats. |
| `crud/src/repositories/turn.rs` | Item upsert/read/list, prompt/history and tool result paths use payload; `post_turn_tool_summaries:45` extracts JSON1 fields and filters before LIMIT. | Preserve logical item API and same-transaction state; replace JSON-dependent SQL with equivalent bounded logical processing or explicit validated projection. LIMIT must apply after the same logical filters. | Real upsert and projection producers, mixed tool summaries including missing/invalid field types. |
| `crud/src/timeline_projection.rs:156,280,287,310`, `timeline_projection_model.rs`, `repositories/thread_timeline_projection.rs` | Projection classification and model mapping parse item payload directly; projection preparation and replay assume logical rows. | Hydrate before synchronous classification, outside database capacity; preserve watermark, receipts, event order and read-your-own-write in batches. | Two dependent events in one atomic batch; rebuild after published item/event. |
| `crud/src/repositories/turn_item_attempt.rs:545,592,779,828,987,1101` | Timeout, reactivation and terminal attempt closure prepare JSON and CAS payload (`:705`); status-only heartbeat updates also touch item rows. | Prepare logical bytes outside writer, revalidate physical fences inside; preserve attempt/status predicates. Metadata updates must not admit legacy rows or unnecessarily retire unchanged portable bytes. | Timeout/reactivation race with publish, terminal closure rollback, heartbeat without payload replacement. |
| `crud/src/lib.rs:20376,28824,29064,29148,29370,29568,29587` | Terminal item scans and incremental payload repair directly read/update Entity; some paths return updated rows. | Route through hydration and existing bounded repair state; preserve physical CAS and returned domain payload. Do not reset repair completion markers to convert history. | Historical repair stays legacy; admitted repair invalidates old reference; restart checkpoint. |
| `crud/src/lib.rs:36008,36194,36293` | Atomic prepared publishers call replay checks and append under one transaction. | Keep event ordering and atomicity; no compression/hash/JSON parsing or hydration I/O while holding writer. Check every custom publisher notification boundary. | Cancellation with unknown commit, ordered batch dependencies, retry after portable publication. |
| `crud/src/lib.rs:30526` | Blocked recovery joins `json_extract(e.payload, '$.payload.resume.blocked_recovery_job_id')` then parses canonical event; current parse error can return None. | Select latest canonical transition with unchanged ordering, hydrate outside capacity, validate associated job/outbox fences; portable corruption must escape as error rather than None. | Published blocked event with matching and unrelated newer recovery jobs; missing/corrupt blocks. |
| `crud/src/repositories/compaction.rs:3099` | Tool-name/arguments JSON1 extraction assumes full JSON. | Preserve tool whitelist and argument selection using hydrated values without exposing placeholders or adding SQL portable decoder. | Compaction with published threads tool calls and Unicode/large arguments. |
| `crud/src/repositories/compaction_preparation.rs:96,141,437,512`, `compaction_history.rs`, `compaction_runner.rs` | Whole/batched event/item source loading and revision alias guards feed history. | Reuse holds and bounded hydration; physical publication must not change logical source revisions or invalidate compaction references. | Extend `crud/tests/compaction_payload_compression.rs` whole/batch, Unicode, stale reference/restart cases. |
| `crud/src/repositories/compaction_lifecycle.rs:228,414`, `compaction_task_output.rs:829,849,866`, `compaction_frozen_import.rs:235,1011,1026` | Lifecycle completion, acknowledgement and source joins depend on event/item identity, revision and delivery provenance; some joins are metadata-only. | Keep metadata-only queries payload-free; hydrate payload users; preserve root readiness and source lifetime fences. | Root compaction and frozen import existing suites plus portable publication race. |
| `crud/src/repositories/thread_episodic.rs:1339,1400,2625`, `self_improvement_history.rs`, `membership.rs` | History/source materialization and owner verification have direct Entity consumers. | Distinguish metadata-only checks from consumers of logical payload; preserve access and source ownership. | Saved-history, episodic delivery and self-improvement canonical history suites. |
| `crud/src/repositories/turn_event_delivery.rs`, `turn_finalization.rs`, `task_completion_inbox.rs` | Delivery/finalization consume canonical appended events and atomic domain transitions. | Keep delivery payload logical; hydrate before preparation where needed, preserve finalization/outbox atomicity. | Existing delivery cancellation and task completion suites, published retry. |
| `crud/src/repositories/task_child_cleanup.rs`, `projection_receipt_cleanup.rs`, native cleanup/turn deletion paths | FK is disabled; source removal and metadata cleanup cannot rely on cascades. Receipt cleanup intentionally reads metadata only. | Ensure physical DELETE retires references in same transaction, retains held immutable bytes, and wakes cleanup after hold release. Leave receipt watermark semantics intact. | Native cleanup, frozen lifetime proof, delete/reinsert, concurrent read hold. |
| `gateway/src/database/zstd_column.rs:993,1001,1076,1207,1309,1538` | Dictionary samples, pending discovery, source CAS and pending counts share legacy eligibility. No portable admission exclusion exists. | Exclude admitted rows consistently, including final writer CAS; do not remove decode registration or dictionaries. Preserve legacy projection watermark checks. | Pending admitted row survives worker/restart; old compressed row reads with original dictionary. |
| `crud/src/repositories/portable_payload.rs:314,337,490,598,1735` | Five typed families, fixed-size cursor state, bounded staging/fragment reads, retired-first headers, publication CAS and read holds. Non-raw default source projection is an empty string. | Extend typed target dispatch and every fixed-size array together; reuse cleanup and cancellation. Include admission at every phase; prevent empty string reaching JSON users. Preserve PPAY v1/64 KiB/raw/hash/UTF-8 checks. | Existing reservation, refusal, hot append, resumed turn, raw threshold and staging restart suites plus turn-specific incarnations. |
| `gateway/src/database/maintenance/portable_payload.rs` | Finite passes, WorkSignal begin-pass, pacing, EOF wait and cancellation already exist. | Reuse worker; expand family assumptions in idle tests; mutations and terminal eligibility changes need scoped WorkChange guards before dispatch, quiet no-op completion, late-commit settlement. | Real worker EOF, invalidation during pass/rest, continuation, cancellation. |
| `observability/src/portable_payload.rs`, `sqlite/src/database.rs:188` | Low-cardinality families/outcomes exist; native read attribution recognizes five table/column pairs. | Add only two family labels and corresponding native attribution as needed; audit dimension/array derivations and legacy read outcome visibility. No identifiers, payloads, SQL, paths or dictionaries in telemetry. | Existing observability suites with new families; no new dashboard. |

The search also included Gateway startup projection/permission backfills,
message history/tools, task agent delivery, self-improvement callers, and
`tasks/tests/invariant_sqlite_path.rs`. This map is a starting audit, not a claim
that every transitive call site has been adapted or finally reviewed.

## Storage review findings

1. Generic discovery admits legacy null references: require durable insertion
   admission at frontier, page, fragment and publish. Finding remains open in
   implementation; no discovery changes were made.
2. SQLite-Zstd installation changes physical schema after Migrator. A migration
   must handle both forms and startup must not later replace portable-aware
   triggers with older writable-view behavior. Merely ALTERing logical tables
   fails for views. Open.
3. Portable default compressed projection is empty string; SQL JSON1 consumers,
   synchronous model decoders, idempotency and payload equality cannot consume
   it. Full reader/CAS integration must precede registering the families. Open.
4. Existing compaction revision triggers watch payload. Physical publication
   must preserve logical revisions; deleting/recreating triggers indiscriminately
   loses domain contracts. Open.
5. New pending rows require both legacy discovery and final CAS exclusion.
   Filtering only portable discovery leaves a compression race. Open.
6. Native portable source reads currently recognize only five source pairs.
   Runtime physical backing-table names and portable source blob capture require
   explicit reconciliation rather than assuming logical views are blob tables.
   Open.
7. Down cannot safely erase PPAY references for an old binary. Refuse unsafe
   downgrade after first portable publication; preserve references/data. A
   pre-switch verified backup plus matching old binary is the minimal recovery
   boundary. No downgrade compatibility is claimed.

## Regression allocation after unblocking

Reuse `crud/tests/compaction_payload_compression.rs`, repository-local portable
lifecycle tests, Gateway worker/zstd tests and existing canonical/projection,
root/frozen-history and cancellation suites. Existing codec verification,
reservation collision, cleanup holds, fairness and error-backoff tests provide
shared-mechanism coverage, but do not prove admission of the two new families.

New focused fixtures must cover: fresh install and upgrade with plain and old
Zstd data; real producers and mixed hydration; legacy UPDATE/read/restart never
admitted; new pending recovery/legacy exclusion; idempotency and ordered atomic
projection; publication versus logical UPDATE/delete/reinsert; owner/root CAS;
rollback and read-hold cleanup; corrupt/missing portable data propagated as an
error. Do not construct a cross product of every format and every domain test.
No tests were added or executed in this blocked audit.

## Rollout evidence required for the completed implementation

Only after review and a separately authorized rollout: per-family published/raw
outcomes increase for new writes; pending drains to EOF and worker parks;
legacy and portable reads succeed; errors remain visible; pre-switch empty
identities remain empty and historical reference counts stay zero. Use bounded
samples or existing diagnostics, not full-history decompress/scan at startup.
Current code has not gained these stage-1 capabilities.
