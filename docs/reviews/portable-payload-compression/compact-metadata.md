# Compact portable payload metadata and deferred refusal diagnostics

Starting HEAD: `dd023d428f3573cb34a861637df0a9187b995507`, clean.
Original observability base: `0c597c1e0659598b82b93be43403e43cbad8f9ba`.
Branch: `feature/portable-payload-compression`.
Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/portable-payload-compression`.
Review gate: source editing/static review/formatting only. No project execution,
CI/push/monitor/service/production-data operations. The commit/check ledger at the end records the prepared review state.

## Owner map before edits

| Contract | Existing owner / required change |
| --- | --- |
| Early refusal | PayloadReadHold::capture → observability hold_rejected; preserve early capture and typed PreflightRace, defer unscoped receipt until DB owners released |
| Transaction release/retry | CrudStore serialized/scoped quantum and actual snapshot transaction callers; inspect rollback/drop before diagnostic flush |
| Payload ID reservation | PortablePayloadProgress discovery → Preparation::step header INSERT; require actual affected count, bounded regeneration before any blocks |
| ID generator | Existing protocol::generate_id and its 62 ASCII alphanumeric alphabet; local length 12 only for new payload IDs |
| Header checksum schema | Branch-owned draft m20261027_000001, BLOB32 / empty retired-only BLOB marker; preserve five migration names/order |
| Entity | Minimal manual portable_payload.sha256 synchronization to existing generated binary style; generation deferred by explicit user exception |
| Checksum consumers | Header staging/finalize/cleanup/read; snapshot, compaction_history, accepted-import SQL bind and fixture |
| Tests | Existing portable storage/migration fixture and hold-observation owner; no executed checks or RNG-only test |

Planned reservation: one INSERT attempt per bounded quantum, up to a small fixed
number of candidates per preparation. Only affected=1 establishes ownership.
Affected=0 from targeted ON CONFLICT(id) DO NOTHING is collision, never adoption.
Generation/hold replacement occurs after releasing DB resources. Confirmed IDs
persist for block retries and publication; ambiguous/error outcomes do not claim
an existing row. Completed SHA values are the full 32 digest bytes. Empty BLOB
is a retired-only reclaiming state, never a reserved 32-byte hash value.

## Implemented reservation and identity contract

Only new `portable_payload.id` candidates use local `PAYLOAD_ID_LEN = 12` and
existing `pioneer_protocol::generate_id`. Its existing alphabet is 62 ASCII
letters/digits (`A-Z`, `a-z`, `1-9`, `0`). No RNG, dependency, global DB_ID_LEN,
domain ID, source incarnation, routing token, filename or dictionary identity
changes. IDs remain ordinary TEXT without a length CHECK, so older UUID-shaped
references are still read without rewriting them.

`Preparation::step` attempts one header INSERT per quantum, bounded by
`RESERVATION_ATTEMPTS = 8` candidate attempts per preparation. Discovery and every
replacement pair the same candidate with its memory-only read hold before the
INSERT. The targeted `ON CONFLICT(id) DO NOTHING` returns its actual affected row
count via the installed SeaORM 2.0.4 `exec_insert_without_returning` implementation:
it returns `db.execute(...).rows_affected()`, including zero. The existing
`on_conflict` builder preserves Insert (not the separate TryInsert adapter).
No existence query,
record-content ownership inference or driver-error-string classification is used.

- Affected=1 confirms this preparation's reservation; `installed=true` prevents
  further allocation during block writes, repeated batches and publication.
- Affected=0 confirms only that the targeted ID conflicted. The foreign hold is
  released immediately. If attempts remain, a fresh candidate and its hold replace
  both header ID and hold outside DB capacity; no source bytes/blocks were processed
  before reservation. The next quantum attempts it. Foreign headers/blocks are
  neither updated nor adopted, even when all placeholder metadata matches.
- Other DB/constraint errors propagate normally; affected counts outside 0/1 are
  explicit errors. If a result was ambiguous, the existing same-statement retry
  may observe a conflict and abandon its own retired staging, never adopt a row.
  Cancellation drops preparation/hold; existing bounded cleanup can reclaim any
  committed retired staging after release.
- Exhaustion fails this preparation under the unchanged worker error/backoff
  policy. The original source/reference is unchanged. A replacement hold failure
  cannot leave a usable unheld reservation: the next step refuses a missing hold.

No SELECT-before-INSERT race, new allocator table, persistent queue, timeout or
priority change is introduced. All later blocks and cutover references take the
confirmed header ID. Existing source revision/incarnation/root/ownership CAS,
read-only readers, serialized writer and atomic publication remain in place.

## Full binary checksum and reclaiming state

The only checksum field converted is `portable_payload.sha256`:

| State | Stored representation / invariant |
| --- | --- |
| Preparing/staging | Retired header, 32-byte zero placeholder, protected by preparation's read hold; never a logical source |
| Completed raw/compressed | Full SHA-256 digest bytes, exactly 32 bytes, SQLite BLOB, no hex conversion or truncation |
| Reclaiming | Empty BLOB, permitted only with retired=1; durable marker precedes bounded block deletion |

Draft schema CHECK is exactly
`typeof(sha256)='blob' AND (length(sha256)=32 OR (retired=1 AND length(sha256)=0))`.
The existing NOT NULL and retired 0/1 constraints remain. TEXT64, BLOB ASCII hex,
31/33/64-byte blobs and active empty blobs are rejected. Every possible 32-byte
hash (including all zeros) remains a real hash value; none is reserved as marker.

The Entity field becomes `Vec<u8>` using the current generated `Binary(1)` style
already present on `portable_payload_block.data` and dictionary data. Finalize
uses full digest bytes; raw/full reads and snapshot/Task basis assembly compare
bytes. Accepted-import preflight binds its digest as binary to the existing
portable-header predicate; its independent ten-bind legacy SQL oracle is retained.
Frozen-history hashes, SQL ownership identities and full-hex deterministic
`family:version:digest` dictionary keys are unchanged. PPAY v1 remains the same
78-byte envelope with its two existing binary digests, same Zstd 6/dictionaries,
block size, raw selection and validation/cache policy.

Cleanup still acquires its exclusive memory hold, marks the retired header, then
removes at most 16 blocks. A prior captured read/preparation hold prevents marking.
After marking, a new reader returns typed retryable PreflightRace, including a
reader attempting to use a stale captured reference; it cannot decode surviving
blocks as a complete value. Captured valid retired backing remains readable while
its earlier hold prevents cleanup. No new cleanup, retention or lifetime policy
was added, and FK OFF remains supported without relying on cascades.

## R1 release boundary

See [observability.md](observability.md#r1-deferred-refusal-correction-current-review)
for the complete signal/lifetime contract. In brief: early capture preserves its
static family, original WorkContext and consent epoch as a typed single-claim
receipt on the unchanged PreflightRace error. It does not touch SDK/live registry.
Scoped refusals still only change bounded Aggregate state. Released attempt owners
and the actual awaited-rollback arms drain once; a consumed record is not Clone.
The rollback-local guard survives cancellation until the transaction-owning future
has dropped. Both shared retry wrappers include final failed attempts, before the
original WorkCarrier retires. Direct compaction lifecycle entry points drain at
their existing boundary without acquiring new retry behavior. No fictitious full
read is created, and a consent cycle suppresses old receipts at flush.

The audit found indirect CLI capture through semantic timeline/work-summary
projection, not just the two Task snapshot examples. All 17 relevant awaited error
rollback arms were adapted (16 lib.rs, one task_run_occurrence.rs), along with the
two common attempt wrappers and two direct compaction lifecycle boundaries. SQL,
transaction success paths, rollback-error policy, retry predicates and guards were
preserved. DB-owner/admission release is distinct from synchronous completion of
SeaORM's queued implicit rollback; only the former is established by source order.

## Draft migration status and Entity exception

The supplied round-two review reports that the parent's read-only local production
migration ledger contained no portable migrations. We did not open that database
or any other user DB. This is evidence about that environment only. The feature
remains unreleased on its development branch; earlier implementation notes call
these migrations drafts, while later PR publication is not a release. No available
source/review evidence establishes a supported nondisposable installation of this
portable draft. Under the user's explicit authorization, only branch-owned
`m20261027_000001_portable_payload.rs` was amended. All five portable migration
names/order and historical unrelated migrations are unchanged. No new schema
object/index/scan/backfill is added; fresh installation uses the final BLOB CHECK.

There is **no supported upgrade from an already installed old TEXT64 portable
draft** in this correction. Older payload ID strings are supported under the final
schema; that is a separate compatibility contract. If release/applied-environment
evidence later appears, an ordinary data-preserving upgrade must be designed and
reviewed before using this schema there. No mass production rewrite/converter or
claim about every installation is made.

Order followed: migration schema, then the one portable Entity field, then CRUD.
The user's narrow preliminary manual-Entity exception was applied because running
Migrator/codegen is prohibited at this gate. Only `portable_payload.sha256` was
synchronized, matching neighboring generated binary fields exactly; no invented
attribute, model repair, export or other Entity change. Normal isolated full-
Migrator-backed generation and exact diff remain mandatory after acceptance.

## Prepared tests and their independent contracts — none executed

`test-audit` and its Pioneer reference were applied in separate test-only phases.
Protected hashes for 3981 tracked files and the two mixed production prefixes were
saved and compared in the first two phases; the final test-only correction also
protected the new report (3982 files/prefixes). Formatting output was applied
only to identified test regions or wholly test files. A production move-out-of-Drop issue discovered by static
review was fixed in a separate production phase between the two freezes; the
second test phase used a new baseline. Final Entity review also corrected three
test lookups/deletes to use the unique tool-output Id column, since ordinal is
its actual primary key; this was a third, strictly test-only phase. No temporary
production mutation, new production hook, manifest change or executed pre-fix/
post-fix baseline was used.

| Existing owner / changed test | Regression detected / limits |
| --- | --- |
| portable_payload unit owner: `initial_capture_rejection_records_only_after_transaction_release` (replaces prior F6 test) | Actual transaction and exclusive cleanup hold; real writer Released observer catches eager recording; implicit release and explicit rollback under another context retain one original-family receipt; cancellation and consent-cycle fencing; successful capture creates no read/receipt. This checks the resource boundary that the prior capture-only test missed |
| Same private owner: `reservation_collision_retries_own_blocks_and_abandons_unconfirmed_candidates` | Forces candidate collision by private test fixture state before reservation, then real INSERT; matching foreign header/block unchanged, new 12-char reservation, own committed batch replay idempotent, real compressed Unicode publication/read, full digest corruption rejection, captured read blocks cleanup, durable empty marker with surviving blocks causes PreflightRace, eventual bounded reclaim, old UUID-shaped raw reference with independent SHA256(abc), exhaustion and queued cancellation preserve source/foreign data |
| Existing `compaction_payload_compression::portable_tool_storage_survives_staging_restart_and_preserves_retry_and_null` | Existing restart/ordinal/NULL/Unicode/raw-empty assertions retained; actual Migrator fixture checks BLOB32/type/length constraints, known SHA256(empty) fixture, wrong full-size raw digest rejection and successful restoration. No schema-source grep |
| `compaction_frozen_import_tests` existing AcceptedImportBasis fixture/oracle suite | Digest fixture follows the binary metadata contract for exact UTF-8 `["accepted"]`; independent legacy SQL text, ten binds and comparisons/negative controls unchanged |
| observability existing `family_selection_preserves_mixed_cleanup_and_known_rejection` | Scoped refusal returns no unscoped receipt; existing stale-outcome and aggregate assertions retained |

The private reservation owner is needed for deterministic candidate collision and
exact batch replay without a public RNG/allocation hook. Its narrow migrated
fixture uses real SqliteDatabase/read-only pool/serialized writer/FK OFF and the
existing progress/publish/hydrate/cleanup paths, not a second business suite. The
integration owner remains the primary public restart/raw/NULL/schema assertion.
Cancelled unconfirmed reservation is exercised; a lost commit acknowledgement is
not dynamically fault-injected. Abandon/no-adoption behavior there is source-
reviewed, with retained existing restart coverage. Tests are prepared, not proof
of compilation, runtime behavior or measured allocation/latency.

## Self-review, files and checks

Production and test diffs were reviewed after each coherent change and together
with the full cumulative observability inventory from `0c597c1e`. Checks included:
claim/epoch/context retention, cancellation/drop order, original retry recognition,
foreign-ID ownership, affected-row semantics, block replay, source CAS/ABA and root
guards, raw/NULL/empty/Unicode, dictionary identity, event order/fanout, marker versus
all 32-byte digests, binary SQL binds and absence of target-checksum hex copies.
F1–F5 remain as reviewed: dependency/lock, exporter epoch attribute, bounded first
CPU-phase entry, native handle lifetime and aggregate native flush semantics.
No new label or signal changes the conservative 840-set budget. No target payload,
ID, SQL, digest, path or raw driver error was added to diagnostics.

Self-review corrected an attempted move of a field from the new Drop-bearing
accumulator: assignment now transfers the whole empty guard, preserving single-
claim ownership without moving out of Drop. Final test review corrected use of
find/delete-by-primary-key with a string tool-output ID to explicit unique-column
filters, retaining ordinal as the real PK. No other concrete issue was left
unresolved by this static pass; this is not a runtime correctness assertion.

Exact correction file inventory (relative to starting HEAD):

```text
crates/crud/src/compaction_store.rs
crates/crud/src/lib.rs
crates/crud/src/repositories/compaction_frozen_import.rs
crates/crud/src/repositories/compaction_frozen_import_tests.rs
crates/crud/src/repositories/compaction_history.rs
crates/crud/src/repositories/portable_payload.rs
crates/crud/src/repositories/task_run_conversation_snapshot.rs
crates/crud/src/task_run_occurrence.rs
crates/crud/tests/compaction_payload_compression.rs
crates/entity/src/portable_payload.rs
crates/migration/src/m20261027_000001_portable_payload.rs
crates/observability/src/portable_payload.rs
docs/reviews/portable-payload-compression/observability.md
docs/reviews/portable-payload-compression/compact-metadata.md
```

Performed only: source/instruction/report/dependency reads and searches, Git status/
diff/full inventory and whitespace checks, allowed scoped cargo fmt/fmt --check,
text editing and byte-integrity comparisons. Rust formatting used skip_children;
formatter stdout was applied only to tests during test-audit. Final check/commit
ledger follows below. Commits disable hooks/signing only per command; no shared
configuration/hook changes. No tests, compilation, cargo check/build/clippy/nextest/
run/bench/metadata, Migrator, Entity/codegen, project program, CI/push/PR, monitor
resume, database access, service/process change, profiler/load or VACUUM occurred.

## Size estimates and remaining verification

36→12 bytes is a factor of three for that ID value only; 64→32 is a factor of two
for that checksum only; together those two values are 100→44 bytes. Repeated
references/index entries may benefit differently. Dictionary keys and the PPAY
envelope do not shrink. SQLite record/index/page overhead, blocks, dictionaries,
free pages, compression and workload are separate costs. These are field-size
arithmetic, not measured database-file savings, codec ratio, immediate file shrink
or runtime-overhead evidence. No VACUUM or production conversion was performed.

After complete external review and explicit acceptance only: ordinary full-
Migrator-backed isolated DB/codegen comparison, focused compilation/nextest owners
above and existing cross-table storage/immutable basis/fanout suites; verify real
collision/error/cancellation/cleanup and fresh/upgrade/rollback behavior. Then
inspect local diagnostic release order and consent generation on the approved
build; for transport/backend verification use the existing schema-first Axiom
plan with actual new build/run identity in observability.md. Existing native
SQLite overflow-map/prefix traversal costs, query/CPU coverage and unmeasured
instrumentation overhead remain limits, not grounds for a redesign here. Any
code change from later verification requires full review again. Compilability,
normal generated Entity equivalence, test results, runtime behavior and actual
new-signal delivery remain unconfirmed.


A read-only attached caller audit was coordinated with the
[pioneer-system-subagents skill](/Users/alexander/.codex/skills/pioneer-system-subagents/SKILL.md)
(task `pMKA370oEUI7d64TjzSeE`). Its first result was revised to cover indirect CLI
transactional callers; the complete revised result was read and source-checked.
The attached task completed; it made no code edits or prohibited checks. This
material supports the internal self-audit and does not constitute external
acceptance. Runtime diagnostics and test-audit skills were also applied with the
user's execution prohibition taking precedence over their dynamic workflows.


## Current commit and verification ledger

| Revision | SHA / content |
| --- | --- |
| Starting clean HEAD | `dd023d428f3573cb34a861637df0a9187b995507` |
| Original observability base | `0c597c1e0659598b82b93be43403e43cbad8f9ba` |
| Production/schema/Entity/tests | `bc2d6e64c8772507132ddac62fbe7cd22bbc4cd0` — `fix(storage): reserve compact payload metadata and defer refusals` |
| Final test-only correction | `fc577b77900c0638eb81314a8763c798a9f70a25` — `test(storage): target tool payload fixtures by unique id` |
| Reports | Following `docs(storage): record compact metadata review handoff`; exact final SHA is in the handoff to avoid a self-referential commit hash |

After the source/test commits only these two review reports remained uncommitted;
no foreign edits were found. Final post-report status is verified in the handoff.
All changes remain local in the original branch/worktree. Hooks and signing were
disabled per commit (`core.hooksPath=/dev/null`, `commit.gpgsign=false`).

Completed with exit 0: correction working/staged diff whitespace checks,
cumulative `git diff 0c597c1e0659598b82b93be43403e43cbad8f9ba --check`, and scoped
`cargo fmt --check` for pioneer-crud/observability/entity/migration/sqlite/gateway,
with skip_children and 23 explicit changed/cumulative Rust files. The explicit
paths were CRUD repositories portable_payload/compaction_frozen_import_tests/
compaction_frozen_import/compaction_history/task_run_conversation_snapshot plus
CRUD task_run_occurrence/compaction_store; Entity portable_payload; migration
m20261027_000001_portable_payload; observability portable_payload/live/telemetry/
database_workload/metrics/work; CRUD portable_payload_codec and repositories
cli_runtime_binding/task_event/compaction/portable_payload_legacy_text/
frozen_storage_lifetime; SQLite database; Gateway maintenance/portable_payload.
Package-root/target formatting was checked by the same invocation, including
the changed integration test. The final unique-ID test-only correction then passed
the CRUD formatting check again. Test-audit hash/prefix comparisons passed after
all three test phases. These are formatting/text/source checks only.

Prepared for full external review. Tests, builds, cargo check, Migrator and
Entity/codegen were not run. Runtime correctness, generated equivalence,
compilability, signal delivery, storage savings and overhead are unconfirmed.
Self-review and these local commits do not permit dynamic verification.
