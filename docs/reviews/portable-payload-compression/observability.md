# Portable payload observability

## Review boundary and source state

Base: `0c597c1e0659598b82b93be43403e43cbad8f9ba` (actual HEAD, as specified).
Branch: `feature/portable-payload-compression`.
Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/portable-payload-compression`.
Initial porcelain status was empty.
Final implementation SHA: `428ab9e4936df0dedd25d7c643e40d085c83c2e2`
(`feat(observability): instrument portable payload operations`). This includes
all source and test changes. The report is a separate documentation commit;
its HEAD hash is supplied in the final handoff to avoid a self-referential hash.

The audit and independent review in
`/Users/alexander/Downloads/pioneer-portable-payload-observability-20261027/`
were read and checked against this source. Root, CRUD and Gateway database
instructions and `resources/skills/pioneer/runtime-diagnostics/SKILL.md` were
applied. This is a new review gate. No dynamic checks, CI, push, merge, runtime
captures, database access, deployment, service changes or exporter/consent
configuration changes were performed. The existing PR #203 monitor
`6FzHYJ35PPALBq8JWUayk` was inspected as paused and remains paused.

This change instruments exactly these existing portable targets:

| Family label | Table / field |
| --- | --- |
| `tool.text` | `tool_output_chunk.text` |
| `tool.metadata` | `tool_output_chunk.metadata` |
| `cli.mapping` | `turn_cli_runtime_binding.input_mapping_json` |
| `task.history` | `task_run_conversation_snapshot.history_json` |
| `task.event` | `task_event.payload_json` |
| `mixed` | Common discovery/cleanup, or genuinely unavailable family; never inferred from identifiers |

No schema, migration, Entity, format, Zstd level, dictionary-selection rule,
cache/storage bound, block size, SQL projection/predicate, retry policy,
publication write set, read hold or lifetime fence changes are intended.
Existing `turn_event`/`turn_item`, other frozen-root kinds, vectors and
`patch_snapshot` are outside this instrumentation.

## Signal map established before edits

| Existing owner | Reused signal / facility | Gap and minimal connection |
| --- | --- | --- |
| Gateway portable maintenance worker | Maintenance-scoped store, cancellation, quantum/retry/pause policy | One portable workload/site per quantum, retired before sleep |
| `database_workload`, `work`, `live` | Caller lineage, scoped database recorder, bounded active work/waits/journal | Portable child operation with fixed detail and coalesced phases |
| Metrics registry / telemetry | Existing OTEL meter, resource identity, consent generation and exporters | Portable instruments registered in existing `GatewayMetrics` |
| CRUD preparation | Source/version/root guards, released read phases, staged publication | Source/dictionary/codec/verify timing, preparation versus committed volumes/outcomes |
| CRUD hydration | Logical read holds, bounded indexed ranges, caller context | Full/range scopes, shared nested aggregate, decode/assembly volumes |
| Private byte codec cache | Bounded immutable dictionaries, existing cache/preparation/training | Caller-owned neutral diagnostics because cache hit/miss/native preparation are private |
| SQLite native reader | Admission/read/pool accounting and existing `query_cpu` scope | Separate handle wait/native elapsed execution; no synthetic SQL callbacks |

The map guided the edits below. Existing reader/admission/pool/query instruments
were not duplicated as new portable metrics.

## Operation and context model

`DatabaseWorkload::PortablePayloadMaintenance` is
`portable_payload.maintenance`; `DatabaseOperationSite::PortablePayloadQuantum`
is `portable_payload_quantum`. Gateway uses the existing attribution result
scope around each `PortablePayloadProgress::quantum`, inside the existing
cancellation select. There is no work/span covering the infinite worker loop.
Scheduling remains Maintenance; no new connections, executors or permits exist.

`portable_payload::scope_result` creates one bounded child execution using the
captured `WorkCarrier`. It retains the caller's database recorder, workload,
site, executor, trigger and parent; it does not replace foreground attribution
with maintenance attribution. TLS and OTEL attachment last only for a single
poll. Nested same-family reads share the enclosing aggregate, including full
hydration's internal ranges. They do not add a second operation denominator or
double-count phase samples. Distinct tool text and metadata are separate
operations. A failed nested read stays an error even if an outer caller returns
`Ok`; known source rejection stays `stale`, without Display-string parsing.

The selected family can change from `mixed` after discovery. Phase/byte/block/
dictionary samples retain the family at the point measured. The terminal
operation label identifies the selected family; earlier cleanup remains mixed.
The frozen-root prerequisite only wraps existing locator kinds 0 and 3, retaining
the existing frozen-storage workload rather than reclassifying that worker.

A future drop first destroys its timers and resource-owning future under the
captured context, then emits accumulated metrics and retires local work as
`cancelled`. It cannot report success. Consent-generation changes suppress old
observations. Work/journal overflow uses existing bounded loss accounting.

After the quantum completes, existing long idle/error sleeps use a **separate**
Maintenance child, `live::sleeping`, and `live::retry` for backoff. Detail is
`mixed/background/idle` or `mixed/background/backoff`. They create no portable
operation metrics or summary spans. The existing 1-ms progress pacing is left
unregistered in the portable journal. All durations and retry policies remain
unchanged. Explicit cancel completes the sleep future; dropping it retires its
work as cancelled through existing WorkCarrier RAII.

Local `ActiveWork.portable_payload` is optional, serde-defaulted and omitted for
other work. It contains only fixed `family`, `mode`, `stage`, `outcome`. New CPU
phase transitions are coalesced to the existing operation's 250-ms interval;
short CPU phases are available in aggregate metrics rather than one journal
event per block/cache probe. First I/O phases and terminal state are visible
immediately. Long synchronous CPU work cannot refresh local state while that
thread is executing; the last coalesced phase is not a CPU sampling profiler.

## Integration map

All rows use the instruments described below and optional anomaly summary
`gateway.portable_payload.summary`; `ActiveWork.portable_payload` provides the
local operation/family/mode. Existing DB waits and query callbacks remain.

| Operation / phase | Real code owner | New signal and accounting boundary |
| --- | --- | --- |
| Full tool text / metadata | `repositories/tool_output.rs::hydrate` → `portable_payload::hydrate_with_hold` | Separate `tool.text/full` and present `tool.metadata/full`; NULL metadata has no invented payload operation |
| Full CLI mapping | `cli_runtime_binding.rs::hydrate_record` → `hydrate_with_hold` | `cli.mapping/full`, header/read/dictionary/decode/verification; nested byte ranges shared |
| Full Task event / fanout hydration | `task_event.rs::hydrate_rows` → `hydrate_with_hold` | `task.event/full`; original fanout budgets and sequence/idempotency paths unchanged |
| Full Task history | `task_run_conversation_snapshot::hydrate_record`, `compaction_history::compaction_task_basis_snapshot` | `task.history/full`; legacy/raw native fragments or shared compressed ranges; final hash/UTF-8/assembly |
| Public Task character range | Task-basis branch of `compaction::compaction_reference_fragment` → `portable_payload_legacy_text::fragment` / `portable_payload::read_range` | `task.history/range`; native reads/prefix scan or compressed range, source read and verification/assembly |
| Internal compressed byte/character ranges, all five families | `portable_payload::read_range` | `range` only when standalone, otherwise enclosing `full`; block fetch + dictionary + decode + slice/UTF-8/assembly |
| Background discovery, all five families | `PortablePayloadProgress::quantum`, `pending_source` | `background`; common cleanup mixed, then fixed selected family; `source_read`, real `no_work` branches |
| Background legacy reads and preparation, all five families | `Preparation::step` | `source_read` actual native bytes; verification/hash/JSON projection/UTF-8; observed codec; encoded bytes/blocks after successful encoding |
| CLI/history legacy-root prerequisite | `frozen_storage_lifetime::LocatorRead::step` kinds 3 / 0 only | `background` under original frozen workload; source bytes, incremental parsing/finalization, locator publication wall time; no compressed-publication counters |
| Dictionary load | `dictionary_row`, dictionary query in `read_range` | `dictionary_load` wall time for DB portion; no native preparation hidden within this timer |
| Train / prepare / cache | Codec observed entry points → private `prepared_dictionary`, `train_impl` | Separate `dictionary_train`, `dictionary_prepare`, `dictionary_lookup` elapsed; hit/miss/attempt/fallback events drained by CRUD adapter |
| Compress / decompress | Observed `encode_impl` / `decode_impl` | Actual native elapsed phases, aggregated without a block span |
| Staging / dictionary / locator writes | Existing preparation/header/block/dictionary/locator write points | `publish` elapsed; staging alone is not a `published` outcome or committed byte count |
| Atomic source cutover | `portable_payload::publish` | `publish`; published/raw outcome and volumes only after successful commit and affected-row check; rejection `stale` |
| Cleanup | `cleanup`, raw staged-block discard → `clear_blocks` | `cleanup` elapsed, `cleanup` outcome on real reclamation progress; cleaned blocks from confirmed delete result |
| Native read for exact five table/column pairs | `sqlite::SqliteReadPool::read_blob_range` | `native.reads`, `native.duration`, `native.bytes`; existing admission/pool/read observer and coverage scope preserved |

CLI has no separate new public character-range API here. Full hydration uses
its existing internal bounded compressed ranges; legacy locator preparation is
background native range work. Tool output/events likewise expose full hydration,
not a new product range API. Other old fragment readers remain unchanged.

## Instruments, dimensions and elapsed-time semantics

All instruments use the existing Gateway meter/resource/exporter. Labels are
closed enums, never data-derived strings. Units are OTEL units shown below.

| Exact instrument | Kind / unit | Labels | Meaning |
| --- | --- | --- | --- |
| `pioneer.gateway.portable.operations` | Counter / `{operation}` | family, mode, outcome | One logical full/range operation or background quantum, including dropped operations |
| `pioneer.gateway.portable.duration` | Histogram / `ms` | family, mode, outcome | Inclusive operation elapsed wall duration |
| `pioneer.gateway.portable.phase.duration` | Histogram / `ms` | family, mode, stage | Sum of observed elapsed durations of that phase within one operation/family, recorded once at completion/drop |
| `pioneer.gateway.portable.bytes` | Counter / `By` | family, mode, kind | Actual read, encoded or confirmed published bytes, as defined below |
| `pioneer.gateway.portable.blocks` | Counter / `{block}` | family, mode, kind | Actual block reads, encodings, publication or cleanup |
| `pioneer.gateway.portable.dictionary.events` | Counter / `{event}` | family, mode, event | Initial cache hit/miss, train/prepare attempts and handled native training fallback |
| `pioneer.gateway.portable.native.reads` | Counter / `{read}` | family, outcome | One validated native guard/blob operation, not a count of ordinary SQL queries |
| `pioneer.gateway.portable.native.duration` | Histogram / `ms` | family, stage | Handle-wait or native execution elapsed; distinct from admission/pool wait |
| `pioneer.gateway.portable.native.bytes` | Counter / `By` | family | Bytes returned by successful native operations |

`mode`: `full`, `range`, `background`.
Operation `outcome`: `success`, `no_work`, `raw`, `published`, `stale`, `error`,
`cleanup`, `cancelled`. `success` means the operation/quantum returned normally;
it does not mean a compressed value was published (for example staging or a
plain read). `no_work` is an actual discovery EOF/no eligible source;
`cleanup` means real reclamation progress; cursor-only progress is not cleanup.
`raw` is confirmed terminal whole-value raw publication or validated raw full
hydration in the common helper. Snapshot native legacy/raw hydration can end
with ordinary read `success`; that does not claim compressed publication.
`stale` is marked at known version/ownership/hold/missing-source rejection
points, not inferred from raw error text. Other errors safely use `error`.
Drop overrides terminal outcome with `cancelled`, retaining accumulated work.
The generic pre-existing work/database workload Result APIs may still record a
rejected `Err` as Failed/Error; the new portable outcome distinguishes `stale`.
A stale summary has OTEL status Unset, not Error. Native outcomes are `ok`,
`missing`, `error`, `cancelled`; invalid bounds/bind conversion before the native
recorder are not native attempts (an enclosing operation can still be Error).

Phase `stage`: `source_read`, `block_read`, `dictionary_load`,
`dictionary_train`, `dictionary_prepare`, `dictionary_lookup`, `compress`,
`decompress`, `verify_assemble`, `publish`, `cleanup`. `idle` and `backoff` are
reserved enum labels used only in local sleeping-work detail, not phase metrics.
Native `stage` has only `handle_wait` and `execute`.

These are elapsed/wall durations, **not CPU time**. Async DB phases include
existing queue/admission/pool waits and lock retries where enclosed. Native
handle/execute measurements split part of source-read wall time; existing DB
admission/pool/read observations also overlap that phase. Operation duration
contains all phases plus uninstrumented glue. Never add overlapping parent,
child, native or common DB durations as independent components of total time.
Codec's six internal timing buckets do not overlap each other; they cover
existing work without repeating hashes, encode or decode. Cache lookup includes
mutex waits, LRU lookup/content checks and insertion recheck. Prepare is native
encoder/decoder dictionary construction outside the cache lock. Compression
includes compressor setup/configuration/destruction; decompression includes
bounded frame completion/destruction. Verification/assembly includes existing
codec header/hash/raw copy/envelope/sample assembly plus adapter incremental
hash/JSON projection/UTF-8 checks, slicing and output assembly. Small metadata
validation/glue and eviction destruction are not exhaustively phase-timed.

Dictionary `event`: `hit`, `miss`, `train`, `prepare`, `train_fallback`.
Only the initial cache probe increments hit/miss, not the insertion-race recheck.
Train/prepare count attempts before native execution, not successful creations.
Native training failure handled by the pre-existing raw-content dictionary
fallback increments `train_fallback` even when the outer Result succeeds. Small
samples that never attempt native training do not report a training failure.
Preparation errors propagate to the operation and remain errors. No errors
are classified using Display strings; no raw errors are added to telemetry.

Byte `kind`:

- `read_original`: actual original bytes fetched from native sources, decoded
  blocks, or supplied plain/raw full-hydration inputs. Counts work, including
  overread/prefix scans/repeated block reads; not unique stored bytes or bytes
  returned to a product caller. Plain inputs can have been fetched before the
  portable scope begins; their earlier SQL elapsed is in existing query signals.
- `read_encoded`: actual PPAY bytes fetched from block rows, recorded before
  dictionary lookup/decode so later errors/cancellation do not erase the reads.
- `encoded_original`, `encoded`: successful block-encoder input and output bytes,
  even if subsequently discarded/stale/raw-selected; codec ratio uses this pair.
- `published_original`, `published_encoded`: one confirmed whole-value cutover,
  after commit and successful source affected-row validation. Encoded is the
  actual stored PPAY bytes for block-backed values, removing the existing 48-byte
  per-row benefit estimate; raw uses original byte length. They do not include
  dictionary rows, payload headers, SQL projections, row/index/B-tree/WAL costs.
  A storage-content ratio uses this pair, **not** unrelated averaged ratios.

PPAY encoded bytes include its 78-byte envelope and frame overhead. These ratios
are not savings of the entire SQLite file. Raw whole values retain plain bytes;
individual PPAY blocks can also be raw inside a block-backed value. Null metadata
is never converted to empty data. Empty successful operations still count even
though zero volume counters are not emitted. Header/dictionary overhead and
cleanup byte volume are unavailable, not fabricated as measured zero.

Block `kind`: `read`, `encoded`, `published`, `cleaned`. Read counts actual data
block fetches, including repeats; metadata-only anchor reads are not data blocks.
Encoded is successful block output. Published uses the committed block count
(0 for terminal raw). Cleaned uses confirmed affected rows, including raw staged
block discard; cleanup errors/cancellation retain previously confirmed progress.
Dictionary/header/locator staging never increments published payload counters.

## Native reader and coverage

The new native timer begins **after** existing maintenance admission and pool
acquisition, immediately before `connection.lock_handle()`. Only an actual
`Poll::Pending` creates the existing local Lock wait guard; ready locks add no
per-block wait-journal transition. Execution starts after handle acquisition.
It includes the existing native PRAGMA/guard/snapshot/blob work and small
resource/future teardown, ending after release of borrowed wrapper, handle,
reader and permit. Cancellation drops resource-owning inner future before the
outer native recorder exports. No SQL/transaction/blob algorithm is changed.
The former TEXT substr path is not reintroduced.

No SeaORM callback/fingerprint or invented SQL count is emitted. Existing
`query_cpu::observe` and reader role span remain: when a SQL-CPU capture is
explicitly active, its `scopes_without_callback`/incomplete/unknown coverage
still describes this path. CPU samples target named SQLite worker threads;
native rusqlite/blob calls run on the calling runtime thread and do not gain
CPU coverage from that scope. Unknown CPU is not written as zero. The summary's
fixed coverage attribute is
`elapsed_only;ordinary_queries_partial;native_sql_cpu_unmeasured` and native
instrument descriptions state the boundary. Existing native CPU captures with
exact build symbols are the later way to inspect these functions. No new
per-thread CPU sampling/profiler was added. SQLite overflow-map and cold-prefix
traversal costs remain for later measurement; this change makes elapsed work
visible, not faster or provably linear in physical I/O.

## Export, sampling, privacy and bounds

Registry chain inspected: `portable_payload::Metrics::new` →
`GatewayMetrics::new` → Gateway meter from existing `SdkMeterProvider` →
existing `PeriodicReader` / `ConsentGatedMetricExporter` → existing OTLP exporter.
Summary spans use `TelemetryState.tracer.build_with_context`, existing batch
span processor and `ConsentGatedSpanExporter`, not a tracing-only span assumed
to export. Local detail goes through `live::portable_progress` into the existing
active registry/journal used by diagnostics snapshots. No exporter, consent,
transport or backend configuration changes exist. CRUD has no Axiom dependency
or network await. Backend retention is not proven by transport health.

Metrics are emitted independently of trace sampling. A summary is eligible
only for elapsed >=500 ms or error/stale, and shares the existing 30-second
portable-workload anomaly limiter with generic database anomaly spans. It has
fixed family/mode/outcome/coverage plus aggregate phase/byte/block totals,
without IDs/SQL/payload/dictionary/digest/path/credential/error text. It is a
single operation summary; no block/cache span/log is exported. Summary parent
uses a valid current OTEL context, otherwise restores only known WorkContext
trace/span IDs. WorkCarrier does not retain sampling flags/tracestate; unknown
flags default to unsampled and state empty, so a parent-based sampler can omit
these summaries. This preserves known causality without inventing flags; metrics
and local work do not depend on summary sampling.

Worst-case attribute sets **per resource**, before histogram representation:

| Instrument group | Maximum |
| --- | ---: |
| operations / duration | 144 each (6 × 3 × 8) |
| phase.duration | 234 (6 × 3 × 13, includes unused reserved local stages) |
| bytes | 108 (6 × 3 × 6) |
| blocks | 72 (6 × 3 × 4) |
| dictionary.events | 90 (6 × 3 × 5) |
| native.reads / native.duration / native.bytes | 24 / 12 / 6 |
| Total conservative label sets | 834 |

`MAX_METRIC_SERIES` statically bounds that total below 2,000. It is not a bound
on every backend's histogram bucket/temporality/resource expansion. No metric
crosses every stage with every outcome. Existing workload cardinality grows
21→22 with explicit index/name/site, maintenance classification, anomaly array
and test inventory updated. Existing compile-time workload budget is
22×3×5×6=1,980, anomaly budget 22×4×5=440. Existing SQL metric budget becomes
1,024+22×2×9×2=1,816, both below existing per-instrument 2,000 assertions.

The logical aggregate uses only fixed atomic arrays (6 families; 13 elapsed
phase totals and call counts, 6 byte kinds, 4 block kinds, 5 dictionary events:
1,968 bytes of u64 slots plus fixed context/Arc/timer bookkeeping). No collection
grows with payload length. Small bounded summary attribute vectors are built
only after operation resources release and anomaly eligibility/rate limiting.
No new global per-block mutex exists; the existing cache/local-registry locks
remain. Clocks and atomics, TLS/Arc handling, one boxed future per scoped helper invocation (including
nested helpers that share the aggregate), metric SDK
updates and coalesced live updates have real unmeasured overhead. Observed codec
calls read clocks even if consent later prevents exporting; existing public
unobserved codec entry points do not read the clock. No claim of zero overhead.

## Static self-review and tests (not executed)

Review covered all modified/untracked source, tests and this report against the
base, including the codec diff, full/range callers, worker sleep/cancellation,
native resource destruction, exporter and local registry wiring. Findings fixed
before handoff:

- separated actual successful encoding from committed publication and removed
  the row-size estimate from published PPAY byte counts;
- recorded fetched block volumes before a subsequent dictionary/decode failure;
- retained internal native training fallback as a dictionary event under outer Ok;
- reused nested full/range ownership rather than duplicate read denominators;
- restored known OTEL parent IDs across WorkCarrier-only boundaries;
- made handle wait registration lazy and CPU live stages coalesced, excluding
  short progress sleeps from extra portable journal work;
- kept minute sleep ownership separate from completed quantum, and stale summary
  status separate from actual Error;
- checked legacy UTF-8 scan timer ends before its possible asynchronous EOF probe.

The new observability test owner uses `test-audit`; no storage suite is copied.
The test-only follow-up preserved protected file hashes and the mixed-file
production prefix. No production hook, payload fixture, test dependency or
compile gate was introduced. Tests are written/updated but **not run**:

| Test / owner | Regression it targets |
| --- | --- |
| `nested_range_keeps_caller_database_trace_and_failure_under_outer_ok` | Loss of caller/workload/trace across attached or WorkCarrier-only contexts, duplicate nested operation, swallowed nested failure |
| `family_selection_preserves_mixed_cleanup_and_known_rejection` | Misattributed discovery samples, lost accumulated phase/byte work, training fallback misclassified as error, stale incorrectly normalized to generic error |
| `cancellation_drops_phase_before_terminal_and_retires_live_work` | Dropped future loses partial timings/volumes, reports success, leaks active work or TLS |
| `local_operation_detail_preserves_diagnostic_wire_contract` | Unstable fixed diagnostic field/label spellings; independent expected JSON |
| `native_lock_journals_only_actual_pending_and_releases_on_drop` | Ready-lock journal churn, repeated Pending creates extra waits, cancellation leaves wait active |
| `pause_owns_separate_sleep_work_preserves_database_and_cancels_cleanly` | Sleep inherits wrong owner/database, becomes a portable logical quantum, short pacing adds portable work, cancellation leaks sleeping work |
| Existing `database_workload::workload_names_are_stable_and_bounded` owner | Missing new workload index/name/cardinality consistency |

These tests do not establish compilation, exported metric observations, commit
boundary behavior in a running DB or performance. Existing storage assertions,
fixtures, independent SQL oracle, idempotency/materialization tests and negative
controls are unchanged. No pre-fix/post-fix dynamic baseline proof is available.

## Requirement disposition

| Requirement | Status / evidence or limit |
| --- | --- |
| Foreground versus background; five fixed families | Implemented: full/range/background scopes, exact mapping; mixed for common work |
| Dedicated background workload/site, no loop span | Implemented in existing attribution and work APIs; quantum ends before sleep |
| Caller trace/database/scheduling preservation | Implemented by child carrier; sampling flags absent from a saved carrier remain a documented limit |
| Work/no-work/backoff/local phases | Implemented via outcome and bounded detail; short CPU phases aggregated; long sleeps separate |
| Source/block/dictionary/train/prepare/cache/codec/verify/publish/cleanup elapsed | Implemented at real execution points; uninstrumented glue and overlap explicitly documented |
| Read/encoded/published bytes and blocks | Implemented with separate kinds; confirmed cutover only after commit; storage overhead excluded |
| Raw/stale/no-work/error/cancellation/cleanup distinctions | Implemented at known typed branches; generic safe error elsewhere; training fallback not lost |
| Cancellation and bounded aggregation | Fixed arrays + drop-before-summary; no block-sized diagnostic retention; runtime still unverified |
| Native handle wait versus execution | Implemented after existing pool/admission wait, with lazy Lock wait and no SQL callback fabrication |
| Honest CPU/query coverage | Existing coverage retained; native CPU unknown, wall timers only; profile symbols plan below |
| Existing exporter/consent only | Registry and consent-gated OTEL path inspected; no transport/backend settings changed |
| Cardinality/privacy/hot-path cost | Fixed enums, conservative 834 sets, no raw content or extra DB/hash/codec work; overhead unmeasured |
| Storage compatibility/NULL/empty/Unicode/CAS/ABA/restart/cleanup holds/order | Algorithms/SQL/guards unchanged by diff inspection, not dynamically re-proven by this instrumentation |
| Schema/Entity/new adapter/new targets/profiler/UI/retention | Not applicable; no such changes |
| Actual runtime/export/backend retention evidence | Deferred under explicit review gate; not confirmed |

## Changed files and performed checks

Changed source files (all intentional):

- `crates/observability/src/portable_payload.rs` (new bounded observations/tests),
  `database_workload.rs`, `lib.rs`, `live.rs`, `metrics.rs`, `work.rs`.
- `crates/crud/src/portable_payload_codec.rs` (private neutral diagnostics),
  `repositories/portable_payload.rs`, `portable_payload_legacy_text.rs`,
  `task_run_conversation_snapshot.rs`, `compaction_history.rs`, `compaction.rs`,
  `frozen_storage_lifetime.rs` (real scoped integrations).
- `crates/sqlite/src/database.rs` (native timing after pool/admission).
- `crates/gateway/src/database/maintenance/portable_payload.rs` (quantum scope/sleep).
- This report, `docs/reviews/portable-payload-compression/observability.md`.

No dependency/exporter manifest changes, new raw SQL, migrations or Entity edits.
The codec retains its public byte API and database neutrality; only private
observed entry points and a fixed Duration/counter carrier are added because the
existing calling layer cannot see private cache/native training work.

Performed: source/dependency/instruction/audit reads, targeted rg searches,
Git status/diff (including whitespace-insensitive review of wrapped Task range),
scoped `cargo fmt` and `cargo fmt --check`, `git diff --check`, test-only text
integrity comparison. Formatting uses `skip_children=true` with the task file
list, and the final diff excludes unrelated formatting. Commits use per-command
`core.hooksPath=/dev/null` and disabled commit signing; no hooks or global config
are changed. Final check/commit/status ledger is below. No tests, builds,
cargo check/clippy, project execution, codegen, Migrator or CI were run.

## Verification after external acceptance only

1. Obtain full external review/explicit acceptance of this entire diff. If later
   verification changes code, review it again. Do not resume the PR monitor or
   publish this unaccepted diff as an implicit way to run CI.
2. Run the focused observability owner with nextest and relevant compilation
   checks, then existing portable storage regressions in an isolated disposable
   DB. Confirm all five families, full/range/background, mixed cleanup/no-work,
   native pending/ready/error/missing, cancellation, raw, stale publication and
   training fallback. Observe prepared counters before cutover and published
   counters only after confirmed commit. Do not use the production DB.
3. With an approved new build/run identity, inspect local diagnostics snapshots:
   foreground parent remains the request, background quantum uses portable
   workload, no completed work remains active over minute sleeps, waits release,
   journal cardinality/loss bounds hold. Check actual clock/SDK overhead and
   large-value native allocation/I/O behavior under the already authorized
   storage regression owner; no such measurements were performed here.
4. For unresolved CPU latency, use an approved bounded native capture and exact
   matching Mach-O UUID / ELF build ID / PE GUID+age symbols. Separate runtime
   native work from SQLite worker-thread query CPU and profiler cost. Do not
   restart the hosting Gateway/Desktop from this agent.
5. Only with authorized delivery/backend access, inspect the real Axiom dataset
   schema first, then query actual instrument/attribute columns for this exact
   new service/build/run/resource and narrow time window. Check dimensions,
   outcomes, units, paired byte sums, drop/cancellation, summary limiter/parent
   sampling and absence of sensitive values. Never assume an Axiom column name
   or reuse old installed-binary data as evidence for this diff. Check exporter
   health separately: successful transport does not guarantee backend retention.

Compilability, runtime behavior, actual signal delivery and measured overhead
remain **unconfirmed**. The implementation is prepared for external review;
self-review and local commits do not lift the dynamic verification gate.

## Local handoff ledger

- Base: `0c597c1e0659598b82b93be43403e43cbad8f9ba`.
- Final source/test commit: `428ab9e4936df0dedd25d7c643e40d085c83c2e2`.
- Documentation commit: `docs(observability): document portable payload diagnostics`;
  its full SHA and post-commit status are provided in the external-review handoff.
- Final scoped formatting check completed with exit 0:

  ```text
  cargo fmt -p pioneer-observability -p pioneer-sqlite -p pioneer-crud -p pioneer-gateway --check -- --config skip_children=true <the 14 explicitly listed touched non-root Rust files>
  ```

  The explicit list was: observability portable_payload/database_workload/live/metrics/work,
  SQLite database, CRUD portable_payload_codec and repositories
  portable_payload/portable_payload_legacy_text/compaction/compaction_history/
  task_run_conversation_snapshot/frozen_storage_lifetime, Gateway maintenance/portable_payload.
  Crate root formatting was checked by these package invocations. A final
  compaction-only formatting check after whitespace cleanup also completed with exit 0.
- `git diff --check` and `git diff --cached --check` completed without findings.
- Source commit executed with hooks disabled per command; only this report remained
  untracked afterward. No unrelated files were staged or committed.
- No test/build/check/runtime/export measurements or dynamic baseline proof.
  No push, CI invocation, new PR, monitor resume or process changes.

Code is prepared for external review. Compilation, runtime behavior and actual
export delivery are not confirmed; external acceptance is required before any
dynamic verification of this new diff.
