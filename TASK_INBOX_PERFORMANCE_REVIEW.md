# Task inbox performance corrections for external review

Branch: `fix/task-inbox-performance` in both repositories.

- Rust worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-inbox-perf-fix`.
  Exact parent/base: `38d543f5ee87205f73fd4a9fd91ce64d9512d381`.
- Mobile worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-inbox-perf-fix-app`.
  Exact parent/base: `bf25a98cbfb73d36ec51e6c7eded0fa30b27c1d6`.

No main updates, merge, push, deployment, changes to the accepted worktrees, or
writes to the production database. Benchmark data/selectors and the backup are
private and excluded from Git. Local raw evidence, reproducible standalone
release profiling sources and the performance report are under
`/tmp/pioneer-inbox-perf-20261018/fix-implementation`; the parent REPORT.md links
this implementation's measurements separately from the earlier experiment.

## Changes and invariants

1. Exact inbox relations fence `task_run -> task` with CROSS JOIN and the original
   equality in WHERE. Mass list/count/read-all retain `workspace task -> run`
   order and their narrow, fail-closed INDEXED BY adapter. The full receipt,
   terminal outcome, saved item/delivery, accepted candidate/frontier, root ACL,
   membership and FK OFF belonging predicates remain in the positive path.

   An immutable set of at most 100 requested Turn IDs is prepared before writer
   acquisition, only if there are no direct run IDs, canonical delivery items or
   dedicated status links. Its existence probe is deliberately broader than the
   full proof query and runs inside the same transaction after actual viewport
   thread/workspace validation. A negative result commits no receipts; positive
   or ambiguous results run the unchanged full resolver. No evidence cache or
   new transaction boundary is introduced.

   Dedicated status links first use one bounded reader-side existence probe on
   workspace/target thread/all requested delivered Turn IDs joined to actual
   `Turn.origin=task_delivery`. Absence is a strict superset-negative shortcut;
   presence retains the previous canonical per-Turn validation and ambiguity
   limit. At most one row is returned by the probe. Every positive link is still
   revalidated by the original writer path.

2. New migration `m20261018_000002_task_inbox_query_indexes` adds only
   `task(workspace_id,id)` and the full covering
   `task_run(task_id,CAST(strftime('%s',completed_at) AS INTEGER),id,status,completed_at)`.
   No existing index is dropped. Normal names are `idx_task_inbox_workspace_id`
   and `idx_task_run_inbox_epoch`. Eligibility still normalizes timestamps to
   epoch seconds and the activation baseline is unchanged.

   Identifiers, Func/CAST and IndexColumn expressions use SeaQuery builders,
   independent of entity. Installed SeaQuery 1.0.2 exposes IndexColumn::from(Expr),
   but its SQLite IndexBuilder inherits a panic for that variant in
   backend/index_builder.rs. The expression index's restricted typed AST is
   rendered with its Postgres builder, whose CREATE INDEX/CAST/strftime syntax
   here is SQLite-compatible; there is no handwritten DDL or SQL replacement.
   Task index and index drops use SchemaManager normally. This is index-only:
   there are no new entities to generate or schema DTOs to change.

3. The internal worker result can be `Ok(None)` after an empty successful viewed
   response. Successful negative evidence is recorded before returning it.
   Completion releases the matching pending operation and preserves items,
   cursor/count and their revisions. Already queued actions and independent
   refreshes continue through the existing queue. Err, session/request/epoch
   fencing and real-viewport retry behavior are unchanged. There is no unread=0
   shortcut for concrete new/late evidence.

4. Only positive `task/inbox/viewed` fanout excludes its authenticated source
   connection. The supported desktop and mobile caller is the shared Client
   worker, which already reloads the current range after a positive response.
   Viewed is native-only, reached from shared timeline planning, not a public
   mobile intent. Repository-wide search found no other supported application
   caller; direct protocol regression calls are included in review. Earlier
   clients also unconditionally reload after viewed. Reload failure remains a
   visible error/Retry. An RPC timeout after a durable commit can leave an
   explicitly failed local view until the normal real-view retry, Retry or
   reconnect observes that receipt; no successful local zero is fabricated. A
   lost connection is fenced by the existing identity and reconnect/bootstrap
   path.

   Other sessions of the principal receive the personal invalidation. Other
   mutation paths keep their existing fanout. No pending-window hint suppression,
   timers or debounce is used: an independent invalidation still requests its own
   authoritative refresh even when it overlaps a viewed request or range load.

Thread unread/cursors, scheduling classes, SQLite settings/readers/writer,
FK/query_only/durability, popup/scroll/footer/lazy loading, native notifications,
activation baseline and personal receipt semantics are untouched.

## Validation and limits

Completed compiler/static checks:

- `cargo check -j 1 -p pioneer-crud -p pioneer-client -p pioneer-gateway --tests --features pioneer-client/test-support` passed; the final repeat also passed.
  Logs: `/tmp/pioneer-inbox-perf-20261018/fix-check.log` and `fix-final-check.log`.
  Actual bound-parameter EXPLAIN also confirmed that an exact owning-task seek
  may use the new `(workspace_id,id)` index. The plan regression permits this
  bounded seek while rejecting workspace-only enumeration; it matches the ID
  term itself rather than the suffix of workspace_id/task_id.
- After the bound-plan regression refinement,
  `cargo check -j 1 -p pioneer-crud --tests --features test-support` passed
  (`fix-plan-regression-check-with-support.log`). An initial narrower command
  omitted test-support and failed in the unchanged compaction_preparation
  integration fixture: compaction_finish_frozen_history has the same feature
  gate in the exact base. No unrelated source was changed.
- `cargo fmt --all -- --check` and both worktrees' `git diff --check` passed.
  Observed warnings in artifact.rs, compaction_preparation.rs and
  thread_episodic_workspace_capsule_refill.rs were compared with the exact base;
  those files are unchanged.
- The stock source-contract generator and `check-source` passed with explicit
  `PIONEER_RUST_ROOT` pointing to this Rust worktree. Public DTO/schema/type
  files are unchanged; mobile retains only the generated source hash.
- The full normal migration chain succeeded on disposable snapshot copies and
  an empty database. The index-only upgrade was also applied to three fresh
  copies; its actual SQLite index definitions were retained for review. No
  entity generation is applicable to this index-only change.
- The standalone worker profiler exposed an invalid synthetic workspace
  resource fingerprint before its first RPC. The inbox regression fixture now
  supplies a coherent nonempty resource projection and checks acceptance.
  `cargo check -j 1 -p pioneer-crud -p pioneer-client -p pioneer-gateway --tests --features pioneer-client/test-support`
  passed again after that cfg(test)-only correction (`fix-fixture-check.log`).
  No functional regression was executed by the profiler.
- Full mobile TypeScript, Clippy and application native builds were not rerun.
  No historical diagnostics are presented as a newly executed check.

See the local performance report for completed compiler/static checks, original
backup hash, three-repeat raw measurements, result and SQL/RPC counts, separate
cold-copy/warm-metadata/dense-history profiles, index storage/build cost and
explicit boundaries of the worker-only controlled-transport experiment. These
measurements do not establish end-to-end Gateway/desktop/mobile acceptance.

Added/updated regression sources cover exact run-first and mass workspace-first
query plans; negative batches with real viewport validation; direct run and
existing exact delivery/candidate/status positive paths; unchanged publication
and pending independent Refresh after negative viewed; zero-unread late proof;
Err/negative dedupe; single positive range refresh; and personal fanout to another
session with the independent source skipped. They are compiled, never executed.

## Expression-index correction after external review

External review baseline: Rust `84008ea3ceb9d29322e01803f85e8ecc28e9dd43` and
mobile `929610362ddea75842eb5943356293eb112a602c`. The original cumulative bases
above remain unchanged. Final runtime/test source commit for this correction:
`b03bcb398139108dde3a9a4f29bdd3445838b6bd`. The final review HEAD including this
documentation commit and clean status are recorded after committing in the new
local REPORT.md/provenance.json. Mobile remains at its reviewed SHA with no new diff.

The previous runtime `Expr::val("%s")` emitted `strftime(?, ...)` in the actual
bound Statement. SQLite could use the covering index only for task_id; the
expression did not structurally match its literal format. `unix_seconds` now
uses `Expr::Constant("%s".into())` from installed SeaQuery 1.0.2. Func/CAST, epoch
normalization and every caller expression are preserved. Workspace, principal,
IDs and timestamps remain bound. The existing migration/index is unchanged;
there is no new schema/entity, SQL rewriting, query inlining or transaction change.

The existing count/page/keyset regression still passes the real Statement with
its original Values to EXPLAIN. It now requires `<expr>>?` on the task-local
run index as well as workspace-first order and bound workspace/principal Values.
The bounded owning-task seek and FK OFF/query_only setup remain. The regression
was compiled, never executed.

New release profiling evidence is separate from all earlier series:
`/tmp/pioneer-inbox-perf-20261018/epoch-range-constant/REPORT.md`,
`measurements.jsonl`, `summary.csv`, `summary.json`, `*.plan.jsonl`,
`result-parity.json`, `logical-statement-parity.json` and `build-provenance.json`.
Baseline is the provided bound-format native binary, whose production body at
84008ea3 matches its original build; its SHA and that equivalence are retained.
Both variants were measured anew, three processes per group, alternating order.
This compares the reviewed performance code with this correction, not the
original 38d543f5 base or the reviewer's SQL-only diagnostic.

| New paired series | CPU/op bound format, ms | CPU/op structural constant, ms |
|---|---:|---:|
| current-count | 4.914 | 2.973 |
| current-page | 5.445 | 3.884 |
| current-rpc_page | 10.649 | 6.641 |
| dense-count | 83.886 | 78.708 |
| dense-page | 87.171 | 81.833 |
| dense-rpc_page | 152.512 | 145.477 |
| read-all-unloaded-warm | 5.128 | 3.672 |

Actual bound count/page plans in both current and dense profiles now show
`SEARCH r USING COVERING INDEX idx_task_run_inbox_epoch (task_id=? AND <expr>>?)`
after the workspace task seek. The actual read-all INSERT SELECT has the same
range, with the operation timestamp still bound. Count stayed 102/current and
6485/dense; bounded metadata/summary/error projections matched. Read-all on six
fresh write copies changed 102 to zero without loading pages. Untimed Display
SQL/Values equality for count/page/page+count/read-all independently confirmed
the logical predicates and values; Display SQL was never used for EXPLAIN.

CPU is the median of three process CPU/op values; wall median/P95 and peak/current
RSS are separate in the new report/raw evidence. Dense CPU is 4–6% lower in this
pair, but wall tails are noisy (including a worse dense-count P95); eliminating
the earlier dense regression against 38d543f5 is not established. Current-page
peak RSS increased in this series; no universal memory reduction is claimed.
These are standalone CRUD timings, not full Gateway/desktop/mobile measurements.
Worker/100-proof/concurrency scenarios were not repeated because they did not change.

Checks executed for this correction:

- `cargo check -j 1 -p pioneer-crud --lib --tests --features test-support` passed
  (18.58s, `epoch-range-check.log`). Warnings in unchanged artifact.rs and
  compaction_preparation.rs were compared with the reviewed baseline.
- `cargo fmt --all -- --check`, both cumulative `git diff --check` and the stock
  `check-source` with explicit `PIONEER_RUST_ROOT` for this Rust worktree passed.
  Boundary inputs did not change, so no generated/mobile hash update was needed.
- Offline release harness built with `--locked --offline --release -j 1` before
  the sequential measurements. No functional tests or native apps were launched.
  All writes were on new disposable copies; the pristine backup was only hashed.
- Intermediate audits covered the structural constant and actual bound plans;
  the final audit covered the whole cumulative diff of both repositories.

Commands reserved exclusively for after full external review and explicit code
acceptance by the original reviewer:

```sh
cargo test -j 1 -p pioneer-crud --test task_completion_inbox
cargo test -j 1 -p pioneer-crud --lib repositories::task_completion_inbox::tests
cargo test -j 1 -p pioneer-client --features test-support tasks::notifications
cargo test -j 1 -p pioneer-gateway --features pioneer-client/test-support task_completion_inbox_rpc_uses_root_acl_and_syncs_personal_reads_without_timeline
```

Функциональные тесты не запускались. Требуется повторное внешнее ревью кода и производительности
