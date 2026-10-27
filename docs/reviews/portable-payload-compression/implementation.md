# Portable payload compression — implementation record

Review status (2026-10-27): candidate `f4c09602973c82d555e1286bf6e5efb8b5ccbcae`
was **not accepted** by external review. The repeat-review corrections and their
limits are recorded at the end of this document. Internal review or a local commit
does not constitute external acceptance; execution remains prohibited.

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
  No dedicated production physical chunk deleter was found. Message soft-delete
  retains history; target-row DELETE is handled by the new retirement triggers.
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
  byte budgets and durable dispatch. The original semantic preflight was called from inside the writer; stage 5
  moves this work to reader preparation and retains a small writer identity fence. fanout length is original UTF-8
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
cleanup triggers retire auxiliary headers with foreign_keys=OFF; maintenance
reclaims blocks in batches of at most 16 and then removes the retired header.
Preparation is outside DB resources, publication is a short guarded transaction.
The service rechecks pending work after EOF, so new writes are admitted without a
permanent historical watermark or operator activation.

Future PostgreSQL adapters can use bytea; MySQL/MariaDB require a BLOB type large
enough for their values (not the 64 KiB base BLOB type). No server decoder is needed.

## Verification policy

No tests, project execution, check/build/clippy/nextest, Migrator, Entity codegen,
production migration, VACUUM, push, merge, deploy or service restart are authorized.
Only source inspection, text edits, Git diff and cargo fmt are used. Entity changes
will use the user's narrow preliminary manual-sync exception, then require normal
Migrator-backed regeneration after complete external acceptance.

## Stages and review log

Stage commits and static review results are recorded below. Internal review does
not constitute the requested external acceptance.

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

After stages 1–2, expanded static review also identified delete/recreate ABA:
revision 0 alone can recur. Source reads and final publication now carry a small
insert-identity token generated by an INSERT trigger, as well as the logical
revision. Upgraded legacy rows use the default token until replaced; every new
incarnation receives a new token. Target ownership changes fence preparation while
retaining unchanged published content. Database phases now use the existing
maintenance quantum/retry boundary. Publication also checks current frozen-root
availability for both raw and compressed outcomes. Auxiliary CHECKs bound stored
block/dictionary sizes before reads. These are unpublished migration drafts, not
changes to any historical applied migration.

Stage 3: CLI mapping is connected. Every public CRUD binding/list/tuple/owner/guard
exit hydrates after release/commit. Internal transactions carry explicit storage
provenance; guarded terminalization compares original bytes in reader preflight
and revalidates the insert identity/logical revision/control facts in writer.
Public record equality still compares logical fields. The tiny legacy SQL
projection retains the exact opaque-history predicate outcome (including invalid
JSON and absent/null/empty history). Unknown locator rows remain plain until verified lifetime reconciliation.
The final review additionally identified its old 8 MiB cap as an incomplete
backlog path; the bounded root preparation is described in the final review log.

Stage review covered all CLI reader/writer returns, control-only updates, UPSERT,
root triggers, the accumulated storage diff and three affected test constructors.
The physical-reader integration suite now includes restart/staging/cleanup/new
post-EOF records/NULL and CLI sent bytes/control updates/replacement UPSERT.
`ad3c3d70` is the common+tool integration commit. No dynamic validation performed.

Stage 4 review: the first snapshot draft attempted to touch retired publication
triggers. The current chain's 20261008_000009 migration already removed them;
this was rejected and corrected. Only the surviving task-basis trigger changes.
Snapshot writer reads now use typed metadata projections, with bounded hydration
outside the transaction. Full and fragment compaction reads preserve source
identity/revision/length and scope; accepted-import CAS uses prepared original
SHA/length where TEXT has been released.

The expanded common-storage review found seven issues. Corrections include
partial pending indexes (excluding NULL metadata and unknown frozen locators),
metadata-only tool writer CAS, raw-header retirement on schema rollback, and
streaming preparation for every value size. The old 32 MiB fallback is removed.
CLI's opaque predicate is now prepared by a bounded streaming strict-JSON
projection outside database capacity, retaining at most two short labels and the
empty-history marker; SQLite's first duplicate-label behavior is preserved.
Embedded-NUL legacy input is explicitly retained raw and diagnosed, preserving
SQLite's original predicate. The integration suite compares the real SQLite
predicate before/after cutover for null, arrays, escaped keys/values, duplicate
labels and invalid JSON across block boundaries. These checks are written only.

Optional audit schema/tables/ranking artifacts were unavailable when checked
later in this run; no audit scripts or production payload/dictionary data were
read or executed. report.md and usage.md were read at the beginning.

Captured compressed payloads and staging now own memory-only read holds, never
DB capacity. Cleanup tries an exclusive hold without waiting, marks reclamation
before deleting <=16 blocks and skips held values through bounded ID pages. A
late capture retries the complete source read; public reader boundaries preserve
their scheduling class. Range metadata is checked against ordinal bounds, origin
and predecessor, with checked arithmetic and byte/character continuity. The
active-staging regression interleaves two real Progress owners and verifies exact
metadata and absence of orphan blocks. Existing snapshot insert winners are read
before writer admission; newly inserted plain bytes come from prepared input after
commit, closing the raw/plain deletion gap without replaying committed writes.

The stage-4 pass fixed sent-basis test fixtures missing required pending
Turn/execution identity and moved large CLI input clones before begin. The
ambiguous manual Cargo.lock dependency was corrected in the final stage-5 pass
(`zstd 0.14.3`, already locked). Only
static source/API inspection and cargo fmt were used. The codec's standalone
formatting by its child was also a formatting operation, not project execution.

Current limitations: unavailable or semantically invalid frozen roots stay plain;
unsupported legacy control identities/numbers retain original bytes and are
diagnosed/revisited without completion claims. The old total-payload 8 MiB
locator limit is removed by streaming preparation, as described below.
No readiness gate waits for historical conversion. Dictionary/header/block/index
costs differ from the audit's compact SQLite format; no achieved size reduction or
physical file shrinking is claimed.


Stage 5 connects Task events. Semantic idempotency preparation now hydrates and
compares original payloads before writer admission. The short writer uses typed
metadata, rechecks id/insert identity/logical revision, resolves terminal keys
sequentially against the projected batch state, and returns the captured original
winner. Prepared intra-batch equivalence proofs bind to the actual earlier winner
and distinguish new inserts from persisted winners. Terminal and webhook retries
refresh these caches before begin. The existing fanout INSERT trigger is unchanged;
physical storage UPDATEs do not allocate sequence or notify fanout. Metadata
admission selects the original header byte length for published storage, retaining
direct octet_length(payload_json) for plain rows. Only the admitted prefix hydrates;
its retry cannot charge the caller's byte budget twice.

The final review tightened two common paths: range reads now use at most three
composite-PK block lookups rather than an ordinal scan with a residual offset
filter; rollback refuses missing referenced headers as well as live compressed
headers. Frozen-header owner, identity, counts and readiness/expiry facts are part
of the publication owner stamp. Common target discovery, byte fragment queries
and physical UPDATEs now dispatch through the five existing Entities; there is no
raw-SQL table-name query layer. Fixed correlated metadata expressions remain local
to this SQLite adapter. Diagnostics log only the fixed failed family, never source
IDs, source JSON, dictionary bytes or driver error text.


## Schema objects and installation cost

| Object | Purpose and cost |
| --- | --- |
| `portable_payload_dictionary` | Immutable family/version dictionary content. One unique family/version index; inserted before block references, retained while any old data may need it. Five version-1 dictionaries are created lazily, never copied from production into Git. |
| `portable_payload` | Explicit raw/compressed/completed metadata, original byte/character lengths, SHA-256 and retirement cursor. One `(retired,id)` index makes bounded reclamation independent of target PK shape. |
| `portable_payload_block` | Independent UTF-8 blocks, ordinal and original byte/character offsets. Composite PK plus byte/character seek indexes support bounded random fragments and ordered whole-value reads. |
| Target references (five) | A nullable TEXT payload ID per target field selects legacy versus published storage without sniffing data. No full TEXT duplicate remains after compressed publication. |
| `portable_revision`, `portable_identity` (four tables) | Logical source CAS and insert incarnation, including delete/recreate ABA. Existing rows receive metadata defaults; an INSERT trigger assigns new identities. |
| Five partial pending indexes | Discover non-NULL source fields with NULL payload reference, excluding unknown frozen locators until verified. Building each index scans its existing target table; the two tool fields require two index builds. No payload compression/training runs during schema installation. |
| Target triggers | INSERT identity; field-update invalidation; reference replacement/delete retirement; ownership revision. Explicit triggers work with `foreign_keys=OFF`; no reliance on declarative cascades. |
| Snapshot basis UPDATE trigger replacement | Excludes storage-only changes while retaining domain column updates and the original revision/epoch body. Verified-root triggers remain installed. Previously retired publication triggers remain absent. |

The three helper tables and their indexes start empty. ALTERs install metadata
columns; target index construction is the material upgrade scan and may affect
startup on large databases. No VACUUM, file shrink, measured latency, achieved
compression ratio or production migration is claimed.

## Local exceptions and changed surfaces

Manual Entity synchronization uses exactly the user's preliminary exception:
new `portable_payload`, `portable_payload_block`, `portable_payload_dictionary`
models and lib/prelude exports; three physical columns on CLI, snapshot and Task
event models, four on tool chunks (two references plus revision/identity).
Existing generated dense-model style and column annotations are followed.
No codegen, model repair, generated database or novel codegen attribute was used.
A subsequent normal isolated-Migrator generation and exact comparison are mandatory.

New migration files `m20261027_000001` through `000005` and their private
`portable_payload_target` helper use builders for tables/columns/indexes. Trigger
DDL uses bound/fixed whitelisted schema identifiers because the installed builder
has no trigger/OLD/NEW API. Rollback guards inspect explicit storage state before
dropping the only reference. Historical migration files are unchanged.

CRUD changes live in the existing target repositories, `lib.rs` logical record
boundaries, `compaction_store`, `compaction`, `compaction_history`,
`compaction_frozen_import`, tool paging and the existing Task terminal/webhook
append owners. Local custom SQLite expressions project correlated immutable
header lengths/digests and ownership facts or byte/character substrings; ordinary
owner queries/mutations use Entity selectors/updates. The fanout CASE retains the
installed SQLite direct-column octet_length optimization. Existing accepted-import
raw SQL gains header-original SHA/length guards; it remains in its existing
repository exception, with bound values. No adapter/server-decoder requirement is
added to domain callers.

Gateway adds one sibling `database/maintenance/portable_payload` loop and registers
it with the existing maintenance lifecycle. Three CLI test constructors carry the
new hidden provenance defaults. Cargo adds only the existing locked Zstd 0.14.3
dependency to CRUD. The codec module itself imports no Entity, SQLite API, rowid,
SQL, writer executor or DB identifier.


Deletion map: snapshot `delete_by_run` remains the explicit repository removal
boundary. CLI UPSERT replaces its mapping through the logical-invalidation and
reference-retirement triggers; control-only updates retain the reference. Tool
chunks and Task events have no new retention/deletion API. Any actual target-row
DELETE, including existing direct fixture/admin paths with FK OFF, retires its
references in the same SQLite statement. Cleanup uses the durable retired flag,
source read holds and bounded block deletion. Parent soft-deletion/lifetime policy
is not changed and no new domain history retention is introduced.


## Large unknown roots and bounded reads

The final review found that the existing 8 MiB locator cap could leave otherwise
valid CLI/history values permanently unknown. `frozen_root_stream.rs` now validates
legacy JSON incrementally in the existing locator lifecycle. Ignored fields and
inline message content are discarded; nested historyJson strings are unescaped
straight into a history parser. Message/delivered-item arrays validate one item
at a time, then collapse to a small semantic projection. The original
PreparedFrozenRoot parser validates that projection, and its original writer
verification/CAS still checks readiness, ownership, identity/counts and expiry.
No verified-root trigger is disabled. Last duplicate wins for this serde-based
root contract; the separate SQLite predicate projection intentionally keeps first
label semantics. Unicode, surrogate pairs, syntax, descriptor unknown fields and
integer types remain checked. Fragment state is memory-only and restartable;
prepared root state survives cancellation while waiting for the final writer.

Source fragments are <=64 KiB, prepared blocks buffer <=16 entries, dictionary
training is <=1 MiB/1024 samples (this adapter currently supplies a first fragment),
prepared dictionary retention <=16 entries/16 MiB of native footprint. Indexed
published reads load <=3 overlapping blocks, one origin and one predecessor
metadata record; they never inflate a full compressed value for a small fragment.
Task basis fragments preserve the 16384-character contract and 64 KiB byte reads
preserve original byte budgets. Whole-value APIs necessarily allocate the returned
logical value, with bounded backing reads and CPU outside DB capacity.

The legacy root parser retains <=127 open containers, <=64-byte pending labels,
<=64 KiB per selected control/identity string and <=1024-byte numeric tokens.
Values exceeding these selected-token policies remain unknown with original bytes
intact; total content/history size has no such cap. This is a documented supported
legacy-parser bound, not evidence that a larger identity is semantically invalid.
The repeat-review corrections replace legacy substr/CAST fragments with native
incremental TEXT/BLOB byte reads through the existing scoped SQLite reader. This
removes full TEXT materialization, including the new legacy-root preparation path
and the modified Task snapshot readers. Native SQLite still allocates an overflow
page-number map and may revisit overflow prefixes for a new cursor. Constant native
workspace and linear total I/O are not asserted; see the detailed limits below.

## Written or changed tests — none executed

The test-audit skill and project reference were applied: existing owners were
extended at the affected contract, with no source-grep proof, duplicate codec
implementation or production test hook. Dynamic pre-fix/baseline proof is absent.

* `portable_payload_codec` owns six tests: `v1_raw_wire_bytes_and_present_empty_are_stable`,
  `standard_zstd_frame_decodes_with_explicit_dictionary_identity`,
  `compression_preserves_multibyte_utf8_bytes_at_the_block_limit`,
  `corruption_and_extra_frames_never_return_successful_data`,
  `unsupported_headers_and_size_or_window_limits_are_rejected`, and
  `bounded_training_has_usable_fallback_and_rejects_invalid_dictionaries`.
  Independent wire/known SHA fixtures catch format drift, loss, wrong/missing
  dictionaries, malformed/trailing frames and allocation/window-limit bypass.
* `compaction_payload_compression` adds seven `portable_*` integration contracts:
  tool staging/restart/retry/NULL/ordinal and post-EOF work; hot higher-key append
  streams cannot starve lower-key logical updates; CLI exact sent mapping,
  control update and UPSERT (including replacement during real staging); immutable
  snapshot winner/root/revision preservation;
  independent first/distant fragments with position/content corruption; original
  SQLite JSON predicate results across block boundaries; and simultaneous progress
  owners protecting active staged blocks against cleanup. These use the real
  serialized writer, physically read-only reader and FK OFF fixture.
* `compaction_preparation` extends its existing large Unicode tool-output retry
  through interrupted/restarted portable conversion, catching physical-storage
  retry conflicts without another duplicate paging suite.
* `task_event_fanout`: `utf8_metadata_bytecode_does_not_fetch_overflow_payload_before_admission`
  now crosses actual compressed publication and completed raw fallback, preserving
  original byte admission, existing EXPLAIN negative control and durable fanout
  generation/cursor. `compressed_duplicate_append_preserves_winner_reservation_and_later_insert`
  catches retry identity/reservation loss and missing new appends after EOF.
  `atomic_cancelled_replays_keep_first_uncommitted_payload_and_sequence` catches
  reader-preflight invisibility of an earlier equivalent event in the same batch.
* `frozen_root_stream` adds five focused contracts for last-wins duplicates,
  discarded-subtree syntax/Unicode, descriptor fields/integer types, nested CLI
  histories/delivered arrays, receipt owner fallback and >8 MiB streaming content.
* `frozen_storage_lifetime` integration adds
  `portable_schema_upgrade_and_raw_rollback_reapply_preserve_values_and_reclaim_headers`
  and `portable_schema_rollback_refuses_live_compressed_values_without_losing_reads`.
  Actual pre-portable rows exercise all five fields; raw rollback makes headers
  reclaimable, and destructive compressed rollback errors while readers still work.
  Direct schema down/up is covered; Migrator::down receipt bookkeeping is not.
  `oversized_unknown_cli_and_task_history_restart_then_publish_portable_storage`
  crosses pre-frozen upgrade, interrupted locator preparation, successful verified
  reconciliation and release of both >8 MiB original TEXT copies after compression.
  The old cap refusal test is replaced by
  `oversized_legacy_locator_validates_descriptor_in_tail_without_starving_other_workspace`:
  a descriptor after 9 MiB padding must be found/verified and protect its root;
  unrelated cleanup still progresses.
* Existing locator portion/restart/cancellation/poison tests use streaming offsets;
  oversized poison is malformed independently of size. The new
  `locator_final_writer_cancel_preserves_prepared_root_for_retry` catches consuming
  stream.finish twice after cancellation while queued for writer capacity.
* Historical fanout/Task-run/Task-occurrence, native-cancellation and root-conversation
  fixtures gain only current storage projections required by today's Entity/CRUD.
  Exact upgrade prefixes and historical rollback targets remain separate; existing
  assertions and negative controls remain. Three Gateway CLI constructors receive
  hidden physical-provenance defaults. No new behavior test is added for those
  mechanical constructor/fixture changes.


Final cross-table review additionally found that a permanently busy higher-key
append stream could prevent EOF and starve a changed lower-key source. Each family
now captures a finite upper key for one pass only, then resets/re-discovers at EOF;
there is no durable historical-complete watermark. The new
`portable_hot_append_stream_does_not_starve_updates_below_cursor` regression keeps
appending higher keys while an earlier completed source is invalidated and requires
that source to reach completed storage again. CLI UPSERT is also interleaved with
actual staged blocks, preserving even changed JSON whitespace. A publication CAS
miss is now an explicit diagnosed refusal after the empty transaction commits;
it does not mark completion, and the existing worker applies its 60-second backoff.
This avoids silently cycling through unavailable roots as successful publications.

The last static pass corrected the actual lockfile dependency to `zstd 0.14.3`
(two Zstd versions are locked) and replaced quadratic full payload copies in
Task-event batch proofs with shared typed payloads. Candidate payloads are copied
once lazily per participating input, not once per proof. Persisted preflight
winners are shared as well. A unique single-event winner moves without a copy;
only a selected shared winner is materialized for its returned owned outcome.
The repeat-review correction defers that materialization until after commit releases
the writer (the original candidate still copied shared winners inside the writer).
Proof comparison remains quadratic in the existing maximum of 64 events, but
retained payload bytes are linear. Heap/copy latency has not been measured.

## Checks performed and review boundary

Performed: source and exact installed dependency-API inspection; stage and
accumulated Git diffs, including newly added/untracked files; `git diff --check`;
targeted `cargo fmt` and `cargo fmt --check` for changed Rust files, with child
module traversal disabled to avoid unrelated formatting. Git hooks are bypassed
only for these commits using command-local `core.hooksPath=/dev/null`; no global
Git configuration or shared hook is edited. Subagent/internal review findings
were read in full and corrected; they do not constitute external acceptance.

Not performed: dynamic baseline proof, tests at any level, cargo check/build/
clippy/nextest/bench/run, rustc, Migrator execution, codegen, project reproducers,
production database operations, VACUUM, push/merge/deploy/PR or service restart.
No runtime success, compilation, codegen equivalence or achieved compression ratio
is asserted. Remaining supported-token/legacy-TEXT limits are stated above.

## Verification only after full external review and explicit acceptance

1. Build the ordinary Migrator and create an isolated disposable database using
   its full registered chain; verify fresh install and upgrade from the recorded
   base. No user's database is an acceptable fixture.
2. Set DATABASE_URL explicitly to that isolated database and use exactly the
   Entity AGENTS options: `sea-orm-cli generate entity --lib --with-serde both
   --output-dir crates/entity/src --entity-format dense --ignore-tables seaql_migrations`.
   Compare all manually synchronized models/exports with normal generation.
3. Run the applicable compilation and project-required checks, then nextest for
   CRUD codec/root-stream tests, physical compression/preparation fixtures, frozen
   lifetime/schema fixtures, Task fanout/terminal/idempotency, historical tracker
   and cancellation fixtures, and the touched Gateway CLI/maintenance owners.
   Validate real Migrator rollback/reapply bookkeeping separately from the direct
   schema-step tests. Retain physical read-only readers and FK OFF writer settings.
4. Measure large legacy TEXT fragment I/O, published block seek plans/decoded work,
   cancellation/ownership races, worker backoff/fairness, heap use for large batch
   retries, startup index-build cost and real aggregate storage overhead. No exact
   audit compact-format savings are promised.
5. If normal generation or any subsequent check requires a code change, that
   change again goes through external review before resuming restricted checks.

## Complete changed-file inventory

```text
Cargo.lock
crates/crud/Cargo.toml
crates/crud/src/compaction_store.rs
crates/crud/src/frozen_lifetime.rs
crates/crud/src/frozen_root_stream.rs
crates/crud/src/lib.rs
crates/crud/src/native_cancellation_tests.rs
crates/crud/src/portable_payload_codec.rs
crates/crud/src/repositories/cli_runtime_binding.rs
crates/crud/src/repositories/compaction.rs
crates/crud/src/repositories/compaction_frozen_import.rs
crates/crud/src/repositories/compaction_history.rs
crates/crud/src/repositories/frozen_storage_lifetime.rs
crates/crud/src/repositories/mod.rs
crates/crud/src/repositories/portable_payload.rs
crates/crud/src/repositories/portable_payload_json.rs
crates/crud/src/repositories/task_event.rs
crates/crud/src/repositories/task_run_conversation_snapshot.rs
crates/crud/src/repositories/tool_output.rs
crates/crud/src/task_terminal.rs
crates/crud/src/task_webhook.rs
crates/crud/src/tests/task_event_fanout.rs
crates/crud/src/tests/task_occurrence_reconcile.rs
crates/crud/src/tests/task_run_occurrence.rs
crates/crud/src/tool_output.rs
crates/crud/tests/compaction_payload_compression.rs
crates/crud/tests/compaction_preparation.rs
crates/crud/tests/frozen_storage_lifetime.rs
crates/crud/tests/root_conversation_compaction.rs
crates/entity/src/lib.rs
crates/entity/src/portable_payload.rs
crates/entity/src/portable_payload_block.rs
crates/entity/src/portable_payload_dictionary.rs
crates/entity/src/prelude.rs
crates/entity/src/task_event.rs
crates/entity/src/task_run_conversation_snapshot.rs
crates/entity/src/tool_output_chunk.rs
crates/entity/src/turn_cli_runtime_binding.rs
crates/gateway/src/cli_runtime/command_heartbeat.rs
crates/gateway/src/cli_runtime/mcp/recovery.rs
crates/gateway/src/cli_runtime/thread_binding.rs
crates/gateway/src/database/maintenance/mod.rs
crates/gateway/src/database/maintenance/portable_payload.rs
crates/migration/src/lib.rs
crates/migration/src/m20261027_000001_portable_payload.rs
crates/migration/src/m20261027_000002_portable_tool_output.rs
crates/migration/src/m20261027_000003_portable_cli_mapping.rs
crates/migration/src/m20261027_000004_portable_task_history.rs
crates/migration/src/m20261027_000005_portable_task_event.rs
crates/migration/src/portable_payload_target.rs
docs/reviews/portable-payload-compression/implementation.md
```


## Commit ledger and delivery checkpoint

| Stage | Commit |
| --- | --- |
| 1–2: byte codec, SQLite lifecycle/storage and complete tool chunks | `ad3c3d703a802cdc5f5ec4b74743f2433f619d1f` |
| 3: CLI mapping, exact sent bytes and SQL projection | `8c65b37a90516a18d1187007738acfcb1e712b0c` |
| 4: immutable Task history, root/revision and bounded reference reads | `689641bc3f5e4baa6c308737c0bb2a2ca58f3c64` |
| 5: Task events/fanout, streaming legacy roots and final cross-table corrections | `5c5ab9c6b2375f3f9c2a1dd3e72c46467ea8615b` |

After the stage-5 commit, `git status --short` produced no output: the worktree
was clean. The subsequent documentation-only delivery commit records this ledger
and checkpoint; its exact SHA is supplied in the final handoff. All changes are
local to the one named branch/worktree. The main checkout and adjacent worktree
were not modified, and no fetch/merge/rebase/push/PR/deploy was performed.

The final review traversed the complete base-to-stage-5 changed-file inventory,
including untracked files before staging, dependencies/exports, all new schema
objects and triggers, manual Entity synchronization, production readers/writers,
maintenance cleanup/publication, tests and this record. It did not stop at the
first issue. Corrected areas were re-read, formatted and checked with Git diff;
that initial static pass missed the four subsequent external findings. Their
corrections are recorded below. Residual supported bounds, native SQLite costs and
all missing dynamic evidence remain explicit. This is preparation for external
review, not external acceptance.

Тесты, cargo check, сборки, новый Migrator и генерация Entity не запускались.
Выполнены только разрешённые статические проверки и cargo fmt. Реализация
подготовлена к внешнему ревью; компилируемость, штатная генерация Entity и
прохождение тестов пока не подтверждены.


## Repeat-review revision, 2026-10-27

The starting HEAD was exactly `f4c09602973c82d555e1286bf6e5efb8b5ccbcae`; staged,
unstaged and untracked state was clean. Base remains
`dcc63b7142bf681cba96de8267a15e8383e82b11`. Work continued in the same branch
`feature/portable-payload-compression` and absolute worktree
`/Users/alexander/Code/pioneer/pioneer/.worktrees/portable-payload-compression`.
No new branch/worktree, fetch, merge, rebase or reset was used. Main and sibling
worktrees were not modified. There were no foreign edits to preserve.

| External finding | Correction commit |
| --- | --- |
| P1 missing AcceptedImportBasis fields; P2 independent oracle bind contract | `6ae5c8230820a0b28e0605fa1954ab188054fbc7` |
| P2 TaskEventPayload deep copy inside writer | `b0e1430fd3b5c8cb6f460a8bc3b4eec438ff0c89` |
| P1 full legacy TEXT materialization | `4457db231211294ef034ab6876985120d61bc8ec` |

The following documentation-only commit records this audit; its exact SHA is in
the final handoff. Hooks and signing were disabled only on each commit command
(`-c core.hooksPath=/dev/null -c commit.gpgsign=false`), with no shared configuration
or hook modification. The earlier candidate remains an unaccepted historical
checkpoint.

### 1. Legacy range reads: actual mechanism and limits

Source inspection covered SQLx 0.9, rusqlite 0.39 and their existing shared
libsqlite3-sys 0.37 dependency. SQLx exposes `lock_handle` / `as_raw_handle`, but no
public incremental blob reader. rusqlite `Connection::from_handle` borrows that
same connection without opening or closing it. `blob_open(..., read_only=true)`
accepts ordinary rowid-table TEXT and BLOB, including indexed TEXT columns. It
cannot read NULL/numeric values, views, virtual tables or WITHOUT ROWID tables.
All five target columns are ordinary rowid-table TEXT. Nullable metadata is
excluded from pending discovery and remains NULL; no blob handle is opened for it.
Physical UTF-8 encoding is checked rather than guessing offsets or converting
UTF-16 storage.

The narrow runtime owner is `pioneer-sqlite::SqliteDatabase::read_blob_range`.
There is no new connection, pool, service or orchestration framework. It inherits
the scoped read class and existing maintenance limiter/read observer. Values are
bound before admission; only the necessary SQLite operations run with capacity:

1. Acquire the existing reader and SQLx native-handle lock.
2. Run a parameterized, readonly, single-row rowid guard. Keep it on SQLITE_ROW
   to pin the guard and incremental read to the same snapshot.
3. Open the read-only incremental handle, verify exact physical byte length,
   and read at most 65536 bytes at the requested byte offset.
4. Close the blob and drop rows, statement, borrowed wrapper, SQLx lock, pooled
   connection and permit before returning the owned bytes.

RAII also releases those resources on errors. No await occurs after borrowing the
native handle. Waiting for admission/pool/handle is cancellable; the synchronous
native call finishes before cancellation can next be observed. Nothing detached
continues after cancellation. Busy/locked errors remain recognizable by existing
retry classification; other errors are explicit and contain no payload or SQL
values. A guard miss alone returns None.

`Preparation::step` now builds the same typed owner/revision/incarnation/reference,
logical-length and owner/root stamp guard and obtains bytes through this owner.
The five families share the implementation. Dictionary training, hashing, UTF-8
carry handling and Zstd 6 compression remain after reader release. Publication,
staging cleanup, raw completion and writer/root guards are unchanged.

`LocatorRead::step` in frozen_storage_lifetime uses the same byte owner and retains
its existing scope/sentinel/length and final verified-root guards. Discovery already
uses direct `octet_length(field)`. This removes the repeated full TEXT load before
CLI/history root preparation as well. The helper is also used for its existing
other root kinds; no additional payload becomes a compression target.

The two modified whole-snapshot readers (snapshot `hydrate_record` and
`compaction_task_basis_snapshot`) now separate small reference metadata from the
native legacy/raw byte range. They revalidate captured reference plus identity,
revision, scope and original length; physical cutover races retry instead of
reading the small SQL projection as legacy bytes. Compressed immutable blocks and
read holds retain their prior path. Whole-value public results still necessarily
allocate the logical result outside database capacity.

Task-basis character fragments needed an additional fix: their old join to
compaction_live_sources evaluated `ltrim(history_json)` before the fragment.
That join is replaced locally by its exact Task relational predicates: run,
workspace, conversation thread, matching thread workspace, exact scope and logical
basis revision. The array predicate still means ASCII-space ltrim followed by `[`,
but executes over released native byte ranges. The compressed branch checks its
tiny array/descriptor projection. Legacy/raw metadata never selects the large TEXT
through a CASE branch. Direct-column octet_length keeps SQLite's no-content-load
optimization; no CAST wraps that argument.

A store-local continuation (maximum 16 entries, at most 4096 bytes of identifiers
per entry) retains only run/incarnation/revision and byte/character offsets, never
payload bytes or database resources. Every native range rechecks scope/version.
Sequential 16384-character reads reuse their UTF-8 position; backward/cold random
seeks can scan the prefix. An incomplete scalar rereads at most three bytes; byte
and character offsets, ASCII spaces and SQLite NUL termination retain their
meaning. Cache entries are independent for separate CrudStore instances/databases,
and shared only by scoped clones of that store. Logical edits/recreated rows
cannot reuse an old incarnation/revision entry.

Self-audit also corrected the snapshot created_at fence: capture the original
SQLite TEXT spelling alongside the decoded timestamp and compare that spelling.
A legacy CURRENT_TIMESTAMP value uses a space, whereas a decoded timestamp bound
back through SQLx uses RFC3339. Comparing the latter to the former could reject
an unchanged legacy row. This changes an internal query projection only, with no
schema, Entity, timestamp value or public protocol change. Constructor searches
found no hand-built StoredTaskRunConversationSnapshot to update.

The installed SQLite source establishes what this change does and does not prove:
OP_Column under the former substr(CAST(TEXT AS BLOB)) can allocate len+3 and read
all len bytes before substrFunc. Incremental blob_read instead requests only the
range from the B-tree payload; no full TEXT Mem value is constructed. Direct
length/typeof/octet_length column arguments carry SQLite's skip-load flags.
However, accessPayload allocates aOverflow at up to
`2 * sizeof(Pgno) * overflow_page_count` for the **whole record**, including an
adjacent large field, and clears/populates that page map. Closing a cursor loses
its map; the next range can walk an overflow prefix again. Guard metadata stored
after a large column can itself require overflow traversal. This is neither a
constant-native-workspace proof nor a linear-total-I/O proof. Those stronger
properties cannot be inferred from the <=64 KiB Vec, and are not claimed here.
Keeping the map in a native cursor across CPU work would violate the resource
lifetime requirement. A different substrate for stronger native bounds would
require a further design/review decision; no such architecture or total-size cap
was introduced. UTF-16 legacy databases explicitly fail this reader rather than
silently altering original UTF-8 bytes.

### 2–3. Accepted import fixture and independent legacy oracle

The existing AcceptedImportBasis fixture now supplies SHA-256 and i64 byte length
from the exact UTF-8 bytes of `["accepted"]`. Production fields remain required;
no default or cfg change was introduced. Static search found only the production
constructor and this test constructor, and both supply the required fields.

The old SQL text and all oracle comparisons/negative controls remain unchanged.
Its statement is now constructed entirely in test code, independently of the
production twelve-bind helper. The ten binds remain manifest, ordinal,
destination, turn, workspace, history_json, digest, proof_json, imports_digest,
and checked i64(import_count), in that order. The suite was repaired in place;
no duplicate suite or replacement oracle was added.

### 4. Event result materialization after writer release

An internal DeferredAppendedTaskEvent carries the selected typed payload in Arc.
The repository moves a new payload into Arc or retains the actual fenced preflight
winner; it does not unwrap/clone the shared winner inside append. Database-dependent
key selection, actual batch-winner identity, persisted revision/incarnation,
semantic timestamp/cancelled equivalence and projection ordering remain unchanged.
Projectors borrow that payload. Public AppendedTaskEvent still owns TaskEventPayload.

Callers materialize the owned public result after successful commit: single/batch
append, agent action, Task creation/readmission, trigger admission/reconciliation,
dispatch claim, run-start, terminal transition, delivery transition/cancellation,
finish and webhook admission. The nontransactional repository wrapper accepts the
concrete SqliteDatabase, so its DB operation has returned before conversion. The
resume path that discards events does not materialize unused public results.
Internal terminal outcomes defer their event vector; public outcome types and
fanout interfaces are unchanged. Static search traced every changed helper caller
and conversion site, including existing direct repository test callers.

A shared winner may still require one copy per returned public event, as required
by the existing owned API, but those copies occur after writer release. Proofs
remain Arc-shared: there is no per-pair full payload copy and no new N-squared
retained body set. Commit/rollback, sequence allocation, inserted-only projections
and real fanout work remain on their former paths. No physical backfill creates
an event or notification. Copy latency and memory have not been measured.

### Test changes and evidence boundary

The test-audit skill and Pioneer reference were used for test-only edits, separately
from the authorized production repair. Existing assertions and oracle independence
were retained; no production hook, source-grep test or duplicate business suite was
added. The parent test-only edit checked its protected source hashes before/after.

* New SQLite owner `legacy_text_ranges_avoid_full_allocations_and_release_maintenance_reader`:
  16 MiB Unicode TEXT plus an adjacent large TEXT, a far misaligned byte range,
  physical mode=ro/query_only reader, and real SQLITE_STATUS_MALLOC_SIZE high-water
  measurement. It rejects a full-payload-sized allocation, not merely a large
  result Vec. An independent former substr SQL negative control must report the
  full allocation while returning the same bytes. Existing read observability
  and a subsequent operation using the sole reader check capacity release.
  Revision miss, byte bounds, physical-length mismatch and empty end range are
  exercised. The global allocation counter requires nextest process isolation;
  this is not a peak-RSS or total-I/O benchmark.
* Existing Task snapshot immutable/restart/revision owner now uses the same
  independent expected Unicode fragments before legacy cutover and after compressed
  restart. Backward seek and EOF exercise the new legacy continuation behavior;
  its immutable-winner, epoch and revision assertions remain in place.
* Existing compressed duplicate append owner now returns two large persisted
  replays and retains its winner ID/sequence/timestamp/payload and fanout assertions.
  Existing atomic cancelled replay owner now uses a large first candidate and
  three equivalent inputs, retaining first-winner/sequence/one-real-append checks.
  These behavioral owners do not dynamically prove CPU placement; the boundary
  evidence for that property is the reviewed commit-before-conversion control flow.
* Existing accepted-import oracle suite was repaired as described above; all
  comparisons and negative controls remain its proof owner.

None of these tests was run. Pre-fix dynamic baseline proof, compilation, allocation
thresholds on real platforms, timing and test success are unconfirmed.

### Final static self-audit and exact revision inventory

Each logical repair's diff and its callers were re-read before commit, followed by
the full revision inventory (including both newly added files) and accumulated
base-to-HEAD inventory. Review covered error/retry and cancellation/drop boundaries,
legacy/raw/compressed and NULL/empty/Unicode interpretation, restart/staging/cleanup
against read holds, CAS/ABA and source ownership, SQL projections, immutable roots,
and event ordering. Codec/wire format, dictionary identity/lifetime, all migration
DDL/triggers, Entity, dependencies, retention and publication/cleanup algorithms
remain unchanged from the candidate. The new read mechanism does not hold a blob
or reader through hashing/parsing/training/compression. The original root and
physical-publication fences still decide commits. No outside payload was added.

Additional self-audit findings corrected here were the Task live-source view's
full ltrim materialization and the legacy created_at TEXT spelling fence. No
remaining concrete defect was identified by this static pass. That statement is
limited to source inspection; the native resource and platform limits above and
all missing dynamic evidence remain review items, not accepted properties.

Files changed from the rejected candidate (new files included):

```text
crates/crud/src/lib.rs
crates/crud/src/repositories/compaction.rs
crates/crud/src/repositories/compaction_frozen_import_tests.rs
crates/crud/src/repositories/compaction_history.rs
crates/crud/src/repositories/frozen_storage_lifetime.rs
crates/crud/src/repositories/mod.rs
crates/crud/src/repositories/portable_payload.rs
crates/crud/src/repositories/portable_payload_legacy_text.rs
crates/crud/src/repositories/task_event.rs
crates/crud/src/repositories/task_run_conversation_snapshot.rs
crates/crud/src/task_delivery_lifecycle.rs
crates/crud/src/task_events.rs
crates/crud/src/task_finish.rs
crates/crud/src/task_projector.rs
crates/crud/src/task_terminal.rs
crates/crud/src/task_webhook.rs
crates/crud/src/tests/task_event_fanout.rs
crates/crud/tests/compaction_payload_compression.rs
crates/sqlite/src/blob_range.rs
crates/sqlite/src/database.rs
crates/sqlite/src/lib.rs
docs/reviews/portable-payload-compression/implementation.md
```

Local low-level exceptions: the native-handle bridge is necessary because SQLx
has no public incremental blob wrapper; rusqlite borrows the existing SQLx physical
handle under its documented exclusive lock. Repository guards stay typed Entity
queries except LocatorRead's existing fixed-schema metadata SQL, now selecting
rowid. Task fragment scalar header/CASE/version expressions fill gaps in the ORM
column API without replacing ordinary owner queries. There are no schema additions
or new installation costs in this revision. Entity was not edited; the earlier
manual-generation exception and required later regeneration comparison still apply.

Actually performed: source/dependency reads and static caller/constructor searches;
full changed-file and Git diff inspection, including staged and new files;
`git diff --check` (working and staged/accumulated changes); scoped cargo fmt and
cargo fmt --check for pioneer-crud/pioneer-sqlite with child traversal disabled.
The first formatting check reported only the edited Task test formatting; after
formatting, the same check passed. Formatting status inventory contained only task
files. Local Git commits ran with hooks disabled as described above. After the
code commits the worktree was clean; only this report was then edited. The final
handoff reports the post-documentation-commit status. Delivery checkpoint: all
task changes are committed; the final porcelain status must be empty (no staged,
unstaged or untracked changes), and is checked after the documentation commit.

Still prohibited and NOT performed: any tests/reproducer, compilation, cargo check,
build/clippy/nextest/bench/run, rustc, new Migrator, Entity codegen, application,
production database access or mutation, VACUUM, CI, push/merge/deploy/PR or restart.

After complete external review and explicit acceptance only: perform the previously
listed normal Entity generation/diff and migration/fresh-install/upgrade/rollback
verification, then appropriate crate/workspace compilation and nextest owners.
Run the SQLite allocation owner in its isolated nextest process; measure peak RSS,
SQLite allocations and native page I/O/CPU for increasing values and far offsets,
including adjacent large fields and scoped guard columns after the payload. Verify
reader release and cancellation under admission/handle contention; distinguish
remaining overflow-map/prefix traversal from full TEXT materialization. Exercise
legacy/plain/raw/compressed, root preparation, concurrent edit/delete/recreate,
restart and cleanup read holds through the existing owners. Any resulting code
changes require another full external review before continuing dynamic checks.

Код подготовлен к повторному внешнему ревью; тесты, сборки, cargo check, Migrator
и codegen не запускались. После любых исправлений снова требуется полное внешнее
ревью. Саморевью и локальные коммиты не являются внешним принятием реализации.
