# Portable payload compression — implementation record

## Baseline and boundaries

Base: `dcc63b7142bf681cba96de8267a15e8383e82b11` (local main, 2026-10-27).
Initial main checkout: clean (`## main...origin/main`); no fetch was performed.
Branch: `feature/portable-payload-compression`.
Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/portable-payload-compression`.
The existing `zstd-payload-level-6` worktree is untouched. Only sample Git hooks
exist; commits use a command-local empty hooks directory as additional protection.

Scope: `tool_output_chunk.text`, `.metadata`,
`turn_cli_runtime_binding.input_mapping_json`,
`task_run_conversation_snapshot.history_json`, `task_event.payload_json`.
No vector bytes, turn_event/turn_item format migration, patch snapshots, retention,
index redesign, server database adapters or production database operations.
Audit report.md and usage.md were read; their vector recommendation is superseded.
The measured gains are orientation only, not a promised physical file reduction.

## Source map before implementation

* Tool chunks: repositories/tool_output.rs record_tool_output (writer with scope,
  ID/text/stream/nullable metadata equality), tool_output_page (ordinal cursor,
  ordering and LIMIT 128); src/tool_output.rs splits UTF-8 deltas at 128 KiB.
  Migration m20260914_000002 owns INTEGER AUTOINCREMENT ordinal and unique ID.
  Turn/thread deletion and native cleanup delete chunks through existing SQL.
* CLI: repositories/cli_runtime_binding.rs upsert_turn_binding,
  active_turn_binding_from_new, find_turn_binding and request/native/list reads,
  set_turn_mcp_metadata/native_goal and update_turn_control. CrudStore initial,
  recovery, continuation, activation and terminal transactions use those records;
  gateway/cli_runtime/turn_binding.rs persists and recovers sent mapping.
  html_preview_retention_queries.rs opaque_history inspects the legacy nested
  history string using bounded SQL predicates. frozen_storage_lifetime.rs reads
  unknown locators in 64 KiB BLOB fragments. Frozen verified insert/update triggers
  validate readiness and workspace. Control updates must retain the root and bytes.
* Task snapshot: immutable repository insert_if_absent/find_by_run/delete_by_run;
  CrudStore standalone insert and commit_task_creation_once, Gateway
  publish_prepared_task_snapshot accept the immutable winner. compaction_history
  reads metadata then 64 KiB fragments with run/workspace/thread/time/length guards.
  compaction.rs exposes character fragments and full reference reads;
  compaction_live_sources and compaction_runner/frozen_import inspect '[' prefixes.
  compaction_frozen_import validates accepted history equality in writer SQL.
  compaction_task_basis_update currently fires on every UPDATE and increments
  revision/epoch; physical cutover must be explicitly excluded without weakening
  logical updates. Frozen verified triggers must still execute at cutover.
  Frozen lifetime handles locator registration, owner expiry and cleanup.
* Task events: repositories/task_event.rs prepare/preflight/append, model readers,
  list task/run/thread, fanout_metadata_query/fanout_page; task_event_fanout owns
  byte budgets and durable dispatch. Preflight semantic retry is outside writer;
  writer validates the selected event identity. fanout length is original UTF-8
  bytes before payload admission. task_event_fanout_append fires on real INSERT.
  Physical UPDATE must not append events or change pending generation.
* Gateway startup installs schema through the normal Migrator; database/maintenance
  owns ongoing bounded work, Maintenance reads/writes, cancellation and retries.
  SQLite readers stay physically query_only; there is one serialized writer.

## Storage contract and design decisions

The common codec lives in CRUD as a byte-only module, usable by future adapters.
Versioned envelopes explicitly carry codec, original byte length, dictionary
content digest and original-byte digest. Zstd level is 6. Dictionaries are immutable
and separate for each of the five field families. NULL stays SQL NULL; empty and raw
are explicit completed values, never inferred from string prefixes.

SQLite keeps an explicit nullable payload reference in each target row. NULL
reference means legacy/pending. A published value records original byte/character
lengths and digest. Raw fallback keeps the original TEXT with completed metadata;
compressed values replace TEXT with only a small SQL projection and reference
independent UTF-8 blocks of at most 64 KiB. Each block has its own envelope. Range
reads decode only overlapping blocks; no database capacity survives decoding.
Existing logical CRUD records are hydrated at read boundaries, after transactions.
SQL safety/shape predicates use small projections; byte budgets use metadata.

Dictionary, value and block tables have primary-key seeks. Per-target pending
indexes bound discovery by NULL reference and owner key; building them scans the
existing target table, but schema installation never compresses history. Owner
cleanup triggers explicitly remove auxiliary data with foreign_keys=OFF.
Preparation is outside DB resources, publication is a short guarded transaction.
The service rechecks pending work after EOF, so new writes are admitted without a
fixed historical watermark or operator activation.

Future PostgreSQL adapters can use bytea; MySQL/MariaDB require a BLOB type large
enough for their values (not the 64 KiB base BLOB type). No server decoder is needed.

## Verification policy

No tests, project execution, check/build/clippy/nextest, Migrator, Entity codegen,
production migration, VACUUM, push, merge, deploy or service restart are authorized.
Only source inspection, text edits, Git diff and cargo fmt are used. Entity changes
will use the user's narrow preliminary manual-sync exception, then require normal
Migrator-backed regeneration after complete external acceptance.

## Stages and review log

Implementation in progress. Each stage will record its commit and static audit.

Stages 1–2 are combined as the common storage's first complete integration.
Static review found and fixed cross-family dictionary-ID collisions (storage keys
now include family/version plus content digest), ambiguous staging-insert retry
(ON CONFLICT), and poison-source starvation (bounded pass cursors revisit failures
only after giving later sources a chance). Raw fallback compares the incremental
cost of block storage, rather than charging the shared header twice.
Tool retry decoding runs in reader preflight; the writer revalidates ordinal and
logical revision. Existing scope guards, AUTOINCREMENT insert and LIMIT remain.
The existing large-Unicode retry/page regression now crosses interrupted then
restarted maintenance and verifies compressed physical storage. Codec tests own
independent wire fixtures, bad/missing dictionaries, corruption/version/window/
length limits and exact UTF-8 bytes. No tests were executed.
