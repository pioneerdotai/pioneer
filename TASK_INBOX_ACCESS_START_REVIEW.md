# Inbox access start: implementation review

Bases: Rust ebbf69941eb92a4496343fe2dacc0efb04ff0f18; mobile
929610362ddea75842eb5943356293eb112a602c. Branches fix/task-inbox-access-start,
worktrees task-inbox-access-start and task-inbox-access-start-app. Sources were clean.

Implementation commit: `f42ecda8a2ca269e09574a17b87873bac1e058fb`.
Rust worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-inbox-access-start`.
Mobile worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-inbox-access-start-app`.
Mobile HEAD remains its base with zero diff; no DTO/FFI/mobile generation was needed.
The final report-only commit is a descendant of the implementation commit; the
implementation crates tree and profiled code remain unchanged.

## Model audit before implementation

Invitation acceptance creates the principal and workspace memberships in one writer
transaction. Other grants use membership repository inserts with ON CONFLICT DO
NOTHING; created_at is immutable for an existing grant. Revocation physically
removes workspace membership and its private memberships, or the exact private
membership. Regrant inserts a fresh row. Ordinary workspace-visible access needs
workspace membership only; private access also needs the exact root thread grant.
Superuser kind is fixed at principal creation (no supported promotion/demotion),
and bootstrap reuses the principal. Suspension does not revoke its membership.

Task terminal projections persist domain completed_at with second precision.
Membership dates can have fractional seconds, but callers sample their time before
writer acquisition. Thus neither dates nor per-task event sequence alone recover
global commit order within a second. Historical ordering is ambiguous within a
second and during unrecorded writer queue delay. Include the entire saved grant second, avoiding lost post-grant
notifications. This can retain pre-grant completions within that second or between
the sampled request time and its eventual commit. Audit timestamps share that
request clock; per-task event ordering and reusable SQLite rowids cannot recover
the missing global order. No invented first-view date, repair-time
baseline, fake receipt, or read_at backfill is used.

For new grants and terminal transitions, use an inbox-only completion sequence in
the existing singleton activation row. A new membership or absolute principal
captures the current sequence in its existing writer transaction. Terminal transition increments
it and stores the resulting sequence on the exact run after transition validation,
in that same transaction. Grant must precede completion strictly by sequence;
recorded completion seconds must also reach the grant second. Historical NULL
watermarks use dates. A new grant excludes all already terminal legacy runs.
No users x runs state, event bus, cache or new query per card is needed.

Visibility changes preserve the existing ACL. A private -> workspace transition
records a workspace visibility date/watermark on the thread; existing explicit
participants keep their continuous route. Workspace -> private removes the public
route; re-publication establishes a new public interval. Historical transitions
have no durable date: current workspace visibility uses thread creation as a
conservative lower bound, rather than unrelated updated_at or repair time.

Atomicity: no transaction boundary moves. Writer serialization orders grant,
visibility and terminal updates; reads validate persisted current membership.
Sequence increment and run update roll back together, replay does not increment.
Read-all remains one INSERT SELECT snapshot; later completions cannot join it,
even with identical timestamps. Restart/reconnect/devices use the same durable
state. Deleting grants removes their watermark; regrant captures a fresh one.

Gateway TaskRead/root authorization, exact viewed resolver, per-actor Stop receipts,
worker negative/positive refresh handling, UI and thread unread remain unchanged.

## Validation / evidence

Functional tests must not be executed. Compilation alone is not runtime
correctness evidence. The former /tmp/pioneer-inbox-perf-20261018 directory is
absent; profiling uses the fresh consistent backup described below.

Intermediate source review found and fixed a timestamp trap: thread management
keeps updated_at monotonic using max(old, operation time). A future/unrelated
metadata timestamp must not become the access start. Publication now stores the
original operation timestamp separately; a regression covers future updated_at,
republication and retained private participants.

Schema state is seven fields on six existing entities (no new table/index):
activation.completion_sequence; run.inbox_sequence; membership and absolute
principal inbox_after_sequence; thread inbox_workspace_started_at and
inbox_workspace_after_sequence. Existing rows remain NULL except the singleton
counter initialized to zero. Ordinary principal construction and private thread-row
insertion add no direct clock read; a private creator membership captures its own
clock. Absolute principal and public thread creation read it directly.

Full stock migrations succeeded on isolated entity.sqlite; the stock SeaORM CLI
then generated all entities into a private directory. Only six generated files
were copied; their only changes are these seven columns. Source contract check
passed with explicit PIONEER_RUST_ROOT pointing to this new Rust worktree, without
a generated/mobile diff.

Private performance artifacts: /Users/alexander/Library/Caches/pioneer-inbox-access-1uwxjrv6.
The previous /tmp snapshot no longer exists. A SQLite online backup from a pinned
read-only transaction produced production-backup-pinned.sqlite, 9,317,789,696 bytes,
SHA256 00e626293305b874c8ed27413695668b189eb9ec2e4cb15753815c1b1578fc25.
The source was closed after backup, with no source migration or write. Of 6,781
saved terminal timestamps, none contains nonzero subsecond time. The domain's
terminal event contract is i64 Unix seconds. Grant timestamps are not presented
as a precise historical ordering against those lossy completion times.

## Migration compatibility and generated entities

The unified source is
`crates/migration/src/m20261016_000001_task_completion_inbox.rs`; the other
inbox source files are removed. Explicit `MigrationName` implementations retain
all five inbox identities. The surrounding registrations remain in this order:

1. `m20261016_000001_task_completion_inbox`
2. existing `m20261015_000001_html_preview_retention`
3. existing `m20261018_000001_html_preview_work_selection`
4. `m20261017_000001_task_inbox_activation`
5. `m20261018_000001_task_cancellation_actor`
6. `m20261018_000002_task_inbox_query_indexes`
7. `m20261018_000003_task_inbox_access_start`
8. existing `m20261018_000001_frozen_sweep_work`

A static comparison against the exact base confirms that released up/down and
transaction bodies differ only by module wrapping, explicit historical names and
rustfmt. See private `released-migration-source-parity.json`. No migration log
rows are rewritten. Already applied versions therefore skip their original DDL,
acknowledgement backfill and index creation. New installations retain the same
operations and their reverse down order. Activation still copies the first
version's original applied_at. The new migration only adds nullable access/run
facts and the singleton counter; it does not initialize personal dates to now.

The actual snapshot contained only the first inbox migration. Disposable copies
were upgraded through the normal chain before the paired profiles. A separate
empty entity.sqlite also ran the complete normal chain. Stock SeaORM CLI
2.0.0-rc.35 generated the full schema with dense entities and serde both into
`generated-full`; only the six affected files were copied. Their only schema
changes are the seven generated columns. No entity was handwritten. Logs:
`entity-migrations.log`, `entity-generation-full.log`.

The regression suite covers all four released prefixes (none, first, first two,
all three), exact names/interleaving, schema parity, activation and receipt
preservation, no repeated legacy backfill, and an idempotent second up. These
regressions were compiled, not executed. Running normal migrations for generation
and profiling is separate from executing this suite.

## Production paths and review invariants

- Invitation acceptance and member-service grants use membership inserts inside
  their existing transactions. Invitation coverage now checks the actual saved
  grant clock/date alongside principal/session creation. Repeated inserts retain
  the prior interval. Revoke/regrant is a new interval, without old-run receipts.
- Bootstrap creates an absolute principal only if it is genuinely absent. Its
  clock is captured in that transaction; ordinary principals use workspace facts.
  Absolute access is the existing canonical TaskRead gate, not a guessed owner.
- User-thread creation/publication capture the publication clock in the existing
  writer transaction. Generic projection upserts do not overwrite access class or
  its interval. The actual operation time is separate from monotonic updated_at.
- RunCompleted, RunFailed, RunBlocked, RunCancelled and the depth-limit terminal
  projection use the common result/error write path. Its guard precedes sequence
  allocation. Duplicate/rejected transitions do not advance it. Existing Stop
  actor receipts remain in the same commit; receipt failure rolls back the clock.
- list/count/keyset/read-all share the same unread eligibility. Exact get/read/
  viewed retain TaskRead history access; a real read of historical content remains
  possible and cannot make it unread. Missing expected access dates fail closed.
- Root/turn/thread/workspace/admission checks with FK OFF are unchanged. Mass
  lookup stays workspace-first; point lookup stays run-first. Batch negative probes
  and precise accepted-candidate/delivery proofs are unchanged.
- The worker/controller and public DTOs are unchanged. Empty viewed retains its
  publication, successful negative dedupe and pending independent refresh; positive
  viewed reloads its range while other-session hints retain the base behavior.
  Thread unread, UI, sidebar indicators and mobile screens have no changes.

A second source review moved the complete workspace/absolute epoch maximum inside
its uncorrelated access subquery. This removes repeated normalization of a saved
constant for every task seek. It changes neither values nor authorization, keeps
`Expr::Constant("%s")`, and avoids a per-card query. The run scan remains covering:
non-NULL grant watermarks use an exact secondary run PK probe instead of forcing
an uncovered sequence column into every legacy scan.


## Final performance evidence

Artifact root (private, not committed):
`/Users/alexander/Library/Caches/pioneer-inbox-access-1uwxjrv6`.
`PROVENANCE.json` contains exact source trees, all binary/source/lock hashes,
compiler version and selected build environment. The native before/after harness
sources and dependency locks are byte-identical; dependency paths select the
specified base versus the implementation crates tree. Binaries were built before
timing, release opt-level 3/debug 0, offline, one Cargo job. No user tasks,
providers, native application or Gateway scheduler were started.

Binary SHA-256:

- baseline: `d924dc03ddaf89a72e022702ba4b1e5056a0539b0f0f69bd771bc96e857bd853`
- implementation: `d03d7ab420b4f60587115b844a3d03a0903deb81302dd990012be35252423f64`
- shared-worker profiler: `d43ae6800527625395f3a8222bb7b3ae294639abd503abee3cbf0bfe05bc2301`
- grant baseline: `7b9da67585f4f74dc303981593d743fff0d4c15588d0df83df714f7233699ecc`
- grant implementation: `89e7f7209a68ab4dd58f51ba20b337878739ac66800862f084d650872e7d78dc`

Final cohorts: `measurements-access-clean-final` (270 records),
`measurements-worker-final` (48), `measurements-grants-final` (24). Each includes
raw JSONL, CSV, stdout/stderr and result guards. Native records also retain actual
bound Statement SQL/Values and EXPLAIN plans. Every cell has three independent
process repetitions; write cases use fresh copies. A compiler watcher rejected
interference and paused between calls. Earlier candidate/ambient-build directories
are exploratory/rejected timing evidence, not part of these results.

Current data: 210 unread before/after; dense: 6593 before/after. Dense is a separate
fixture with original history enabled for an existing absolute principal; its
numbers are not mixed into the normal profile. Synthetic independent expected
sets (all five terminal statuses in the historical tail, actual terminal commits
for new results) give:

| Access case | Before | After |
|---|---:|---:|
| Existing member / dense 100-task fixture | 10100 | 10100 |
| Existing late grant with NULL watermark | 10100 | 100 |
| New workspace grant | 10100 | 100 |
| New private grant, older workspace grant | 4222 | 222 |
| Mixed late workspace/private grants | 4222 | 22 |
| Empty workspace | 0 | 0 |

The private case retains 202 already-accessible public results plus 20 new private
results; the mixed case retains 22 post-grant results. Page membership/projection
and exact read-all receipt sets are checked against independently enumerated
fixture IDs, including unloaded runs. Current/dense projections match baseline.
Exclusion creates no receipts; read-all remains one INSERT SELECT and subsequent
count, rather than one RPC/write per run.

Actual count/page/read-all bound plans retain:

```text
SEARCH t USING INDEX idx_task_inbox_workspace_id (workspace_id=?)
SEARCH r USING COVERING INDEX idx_task_run_inbox_epoch (task_id=? AND <expr>>?)
```

Point plans retain an exact run ID seek before its owning-task seek. New sequence
probes use the run primary key; no run/task/sequence_run table scan appears. The
format is structural `%s`, while principal/workspace/status/cursor/time values
remain bound. Empty workspace remains a bounded workspace seek, not a scan of
foreign history. Raw plans, including their original Values, are in each native
cohort's `*-bound-plan.json` files.

### Comparable measured operations

Three independent processes per cell. CPU is getrusage user+system/op. Wall median is the median of each process median (20 warm calls; negatives/exact 100; mutations 1). Raw per-process upper p95 and min/max remain in CSV. RSS includes startup, warmup and untimed plan capture; not application/Gateway RSS. Ambient compiler-overlap attempts are excluded.

| Scenario / operation | CPU before → after, ms/op | Wall before → after, ms | max peak RSS before → after, MiB | SELECT/write per op before → after |
|---|---:|---:|---:|---|
| current / count | 4.898 → 4.901 | 4.228 → 4.305 | 35.5 → 35.6 | 1/0 → 1/0 |
| current / page | 5.751 → 5.805 | 5.133 → 5.177 | 36.2 → 36.2 | 1/0 → 1/0 |
| current / page_count | 10.103 → 9.934 | 9.747 → 9.442 | 36.3 → 36.4 | 2/0 → 2/0 |
| current / read_all | 16.106 → 15.922 | 16.092 → 16.205 | 32.9 → 32.9 | 1/1 → 1/1 |
| dense / count | 70.304 → 66.349 | 69.869 → 66.363 | 35.6 → 35.7 | 1/0 → 1/0 |
| dense / page | 74.367 → 70.759 | 74.092 → 70.709 | 36.4 → 36.4 | 1/0 → 1/0 |
| dense / page_count | 145.040 → 136.792 | 144.367 → 136.487 | 36.4 → 36.3 | 2/0 → 2/0 |
| dense / read_all | 147.725 → 150.567 | 149.446 → 152.520 | 34.5 → 34.4 | 1/1 → 1/1 |
| existing-member / count | 16.185 → 17.776 | 15.639 → 17.089 | 29.4 → 29.5 | 1/0 → 1/0 |
| existing-member / page | 21.235 → 22.591 | 20.561 → 21.863 | 31.3 → 31.4 | 1/0 → 1/0 |
| existing-member / page_count | 35.738 → 39.404 | 35.393 → 39.035 | 30.8 → 30.9 | 2/0 → 2/0 |
| existing-member / read_all | 54.076 → 57.074 | 59.110 → 60.889 | 31.0 → 30.9 | 1/1 → 1/1 |
| legacy-late-member / count | 16.612 → 0.987 | 15.934 → 0.329 | 29.4 → 26.7 | 1/0 → 1/0 |
| legacy-late-member / page | 21.208 → 1.560 | 20.467 → 0.924 | 31.3 → 28.5 | 1/0 → 1/0 |
| legacy-late-member / page_count | 36.860 → 1.597 | 36.428 → 1.231 | 30.7 → 28.0 | 2/0 → 2/0 |
| legacy-late-member / read_all | 54.330 → 7.400 | 58.025 → 7.944 | 30.9 → 26.8 | 1/1 → 1/1 |
| new-workspace / count | 16.721 → 1.042 | 16.062 → 0.408 | 29.4 → 30.6 | 1/0 → 1/0 |
| new-workspace / page | 22.123 → 1.669 | 21.125 → 0.985 | 31.3 → 31.2 | 1/0 → 1/0 |
| new-workspace / page_count | 40.595 → 1.796 | 40.179 → 1.407 | 30.8 → 31.3 | 2/0 → 2/0 |
| new-workspace / read_all | 57.704 → 8.000 | 59.868 → 8.275 | 30.9 → 30.7 | 1/1 → 1/1 |
| new-private / count | 8.694 → 7.434 | 8.043 → 6.794 | 26.6 → 27.6 | 1/0 → 1/0 |
| new-private / page | 12.248 → 8.150 | 11.640 → 7.491 | 28.3 → 28.4 | 1/0 → 1/0 |
| new-private / page_count | 17.540 → 14.860 | 17.187 → 14.391 | 27.8 → 28.3 | 2/0 → 2/0 |
| new-private / read_all | 26.470 → 20.642 | 28.255 → 20.875 | 29.0 → 27.8 | 1/1 → 1/1 |
| mixed / count | 7.542 → 0.783 | 6.949 → 0.187 | 26.6 → 26.0 | 1/0 → 1/0 |
| mixed / page | 10.720 → 1.301 | 10.103 → 0.675 | 28.3 → 26.5 | 1/0 → 1/0 |
| mixed / page_count | 17.420 → 1.221 | 17.003 → 0.868 | 27.8 → 26.6 | 2/0 → 2/0 |
| mixed / read_all | 26.200 → 6.796 | 27.262 → 7.046 | 28.9 → 26.0 | 1/1 → 1/1 |
| empty / count | 0.656 → 0.674 | 0.087 → 0.099 | 23.9 → 23.9 | 1/0 → 1/0 |
| empty / page | 0.960 → 1.051 | 0.367 → 0.456 | 24.4 → 24.4 | 1/0 → 1/0 |
| empty / page_count | 0.764 → 0.903 | 0.447 → 0.574 | 24.5 → 24.6 | 2/0 → 2/0 |
| empty / read_all | 6.021 → 6.124 | 5.967 → 6.064 | 24.0 → 24.0 | 1/1 → 1/1 |

### Point, viewport and mutation paths

| Scenario / operation / batch | CPU before → after, ms/op | SELECT/write before → after |
|---|---:|---|
| new-workspace / exact / - | 0.459 → 0.455 | 1/0 → 1/0 |
| new-workspace / ordinary / 1 | 0.064 → 0.061 | 2/0 → 2/0 |
| new-workspace / ordinary / 5 | 0.078 → 0.070 | 2/0 → 2/0 |
| new-workspace / ordinary / 100 | 0.153 → 0.142 | 2/0 → 2/0 |
| new-workspace / status / 1 | 0.236 → 0.225 | 3/0 → 3/0 |
| new-workspace / status / 5 | 0.258 → 0.235 | 3/0 → 3/0 |
| new-workspace / status / 100 | 0.370 → 0.341 | 3/0 → 3/0 |
| existing-member / positive / - | 130.492 → 136.242 | 8/1 → 8/1 |
| new-workspace / positive / - | 129.839 → 20.454 | 8/1 → 8/1 |
| grant / grant / - | 1.006 → 1.129 | 5/1 → 6/1 |
| regrant / grant / - | 0.978 → 0.908 | 7/2 → 8/2 |
| terminal / terminal / - | 3.774 → 3.706 | 6/2 → 6/3 |

### Shared worker controlled transport

| Fixture / scenario | component CPU before → after, ms | wall before → after, ms | viewed/list/count | actual SELECT/write before → after |
|---|---:|---:|---|---|
| existing-member / negative | 1.073 → 1.057 | 10.166 → 10.170 | 1/0/0 | 2/0 → 2/0 |
| existing-member / negative-independent | 108.972 → 120.144 | 118.106 → 128.999 | 1/3/3 | 8/0 → 8/0 |
| existing-member / positive | 112.246 → 123.621 | 125.122 → 134.962 | 1/3/3 | 8/1 → 8/1 |
| existing-member / positive-independent | 220.839 → 243.362 | 233.705 → 255.940 | 1/6/6 | 14/1 → 14/1 |
| new-workspace / negative | 0.989 → 1.067 | 10.062 → 9.321 | 1/0/0 | 2/0 → 2/0 |
| new-workspace / negative-independent | 109.036 → 7.731 | 118.031 → 12.640 | 1/3/3 | 8/0 → 8/0 |
| new-workspace / positive | 111.139 → 9.389 | 123.997 → 22.653 | 1/3/3 | 8/1 → 8/1 |
| new-workspace / positive-independent | 220.970 → 16.344 | 233.778 → 27.943 | 1/6/6 | 14/1 → 14/1 |

Component CPU is parent client CPU + sum of native handler CPU, not complete child process CPU. Controlled wall includes stdio transport and up to 1 ms idle detection. Count calls equal the observed list RPC calls: the dispatcher always executes one actual count per successful list. SELECT/write counts are the real metric callbacks; write counts cover non-SELECT statements seen by SeaORM, including no-op DDL; Connection API BEGIN/COMMIT and connection PRAGMAs are not emitted by this callback and are not counted. No Gateway auth, fanout, native UI, scheduler or providers were started.

### Schema initialization

- base: CPU 0.581 ms, wall 0.728 ms, growth [0, 0, 0], SELECT/write 1/4. Activation, receipts and existing index rootpages unchanged.
- after: CPU 37.362 ms, wall 37.817 ms, growth [0, 0, 0], SELECT/write 1/12. Activation, receipts and existing index rootpages unchanged.

### Additional access lifecycle methods

| Operation | CPU before → after, ms | wall before → after, ms | SELECT/write before → after |
|---|---:|---:|---|
| private-grant | 0.918 → 0.971 | 1.056 → 1.112 | 7/2 → 8/2 |
| private-regrant | 0.901 → 1.018 | 1.438 → 1.428 | 7/2 → 8/2 |
| publication | 0.613 → 0.670 | 1.071 → 1.083 | 1/1 → 2/1 |
| owner_init | 2.607 → 2.698 | 3.658 → 3.759 | 1/2 → 2/2 |

Private grant/regrant and publication use new clones of the same prepared corpus. Absolute principal initialization uses clones of the fully migrated empty entity DB for both implementations (no tasks or sessions). These are the actual CUD helpers inside their normal transactions, not complete Gateway authorization/bootstrap workflows. Three fresh write copies per cell; no receipts are manufactured.


The normal current profile is stable: count 4.898 -> 4.901 ms CPU/op, page+count
10.103 -> 9.934. The 10100-unread synthetic existing-member stress case costs
about 10% more CPU for count/page+count because the required thread access checks
still apply to every eligible run. This is an explicit cost of the rule, not a
universal speedup claim. Its controlled positive worker profile also grows about
10%. Late workspace grants skip their historical tail through the epoch range;
private-only grants retain the workspace-first plan and filter their thread
interval locally. No priority, pool, cache, SQLite pragma or ACL adjustment masks
these costs.

CPU is process user+system, not SQL-only CPU. A fresh process and one metadata
warmup are used per cell, so tiny operations include pool/runtime background work
and measurement noise. Wall percentiles are per-process upper order statistics;
the raw min/max/p95 and current/peak RSS remain in CSV. Mutation cells have one
operation per fresh copy; three copies provide their spread. RSS is the complete
short-lived harness lifetime, including setup and untimed plan capture, not full
Gateway/desktop/mobile RSS or isolated hot-query memory. All four physical reader
connections use mode=ro/query_only ON; the serialized writer uses FK OFF.
SeaORM callbacks count observed SELECT/non-SELECT statements; Connection API
transaction controls and startup PRAGMAs are not included. No-op migration DDL can
still count as a write statement. Subqueries are part of their enclosing SQL.

The worker profiler uses the real unchanged ClientCore controller, execute,
completion and range-loader bodies. A private feature-only copy enables existing
controlled transport hooks, adds a request-activity marker, and feeds synthetic
authenticated viewport intents. It does not enable cfg(test) or call a regression
factory/suite. A native child executes real CRUD list/count/viewed methods; an
independent hint enters the actual observer while viewed is pending. The exact
private instrumentation patch and its hash are retained. This proves observed
RPC behavior for that controlled path, not native UI timing, real Gateway
authorization/fanout or complete multi-device runtime acceptance.

New-schema initialization adds no pages on these three 9.3 GB copies after
checkpoint and preserves activation, receipts and index rootpages. This does not
mean future fields consume no space: new/rewritten grant/run/thread rows store
their facts. There are no new indexes or users-times-runs state. Terminal commit
adds one UPDATE; each new access fact adds one scalar SELECT. The measured rare
publication/bootstrap operations use the actual existing CUD transactions.

## Reproduction and retained evidence

Use the private artifact root on this host; DBs/selectors/user history must not be
committed or published. `METRICS.md`, `native-aggregate.csv`, `PROVENANCE.json`,
`backup-final-verification.json` and the three final cohorts are the evidence.
The backup hash was rechecked after profiling and exactly matches creation.
Only disposable copies were migrated/written; the live source was opened read-only
for the pinned online backup and then closed.

The measured collector files remain unchanged. Separate `_repro.py` copies only
add output/corpus path selection, and are statically parsed, not newly executed.
`prepare_corpora_repro.py` prepares a fresh corpus and checkpoints disposable WALs
before APFS cloning; it never checkpoints or writes the pristine backup. Original
setup logs remain private, including the discarded incomplete-copy attempt.

Example offline reproduction (fresh names; run sequentially after builds finish):

```sh
INBOX_PROFILE_ROOT=/Users/alexander/Library/Caches/pioneer-inbox-access-1uwxjrv6
python3 "$INBOX_PROFILE_ROOT/prepare_corpora_repro.py" "$INBOX_PROFILE_ROOT/corpora-repro-NEW"
INBOX_PROFILE_CORPUS="$INBOX_PROFILE_ROOT/corpora-repro-NEW" INBOX_PROFILE_OUTPUT="$INBOX_PROFILE_ROOT/native-repro-NEW" python3 "$INBOX_PROFILE_ROOT/run_native_profiles_clean_repro.py"
INBOX_PROFILE_CORPUS="$INBOX_PROFILE_ROOT/corpora-repro-NEW" INBOX_PROFILE_OUTPUT="$INBOX_PROFILE_ROOT/worker-repro-NEW" python3 "$INBOX_PROFILE_ROOT/run_worker_profiles_repro.py"
INBOX_PROFILE_CORPUS="$INBOX_PROFILE_ROOT/corpora-repro-NEW" INBOX_PROFILE_OUTPUT="$INBOX_PROFILE_ROOT/grants-repro-NEW" python3 "$INBOX_PROFILE_ROOT/run_grant_profiles_repro.py"
```

Build sources: `harness-base`, `harness`, `worker-parent`,
`harness-grants-base`, `harness-grants-after`. Their Cargo locks and release logs
are retained. Build with `CARGO_TARGET_DIR="$INBOX_PROFILE_ROOT/target-release"
cargo build --locked --offline --release -j 1 --manifest-path <project>/Cargo.toml`,
then copy the resulting executable to a new uniquely named binary path. Match
PROVENANCE hashes when reproducing the exact measured build; do not overwrite the
retained measured binaries/evidence. This is a local offline harness, not a
functional runner or full application benchmark.

## Checks and final self-review

Executed, sequentially with one Rust job:

- `cargo check --locked --offline -j 1 -p pioneer-crud --lib --tests --features test-support`
  passed after the final epoch-subquery refinement (`check-epoch-hoist-crud.log`).
- `cargo check --locked --offline -j 1 -p pioneer-gateway --lib --tests --features pioneer-client/test-support`
  passed for schema/fixture/invitation changes (`check-final-gateway.log`). It was
  not repeated after the private CRUD expression-only hoist; the final CRUD test
  targets were recompiled. No public signature changed in that hoist.
- Migration build, full normal chain on empty disposable entity.sqlite, stock
  entity generation, and byte-for-byte comparison of all six copied files passed.
- `cargo fmt --all -- --check`, staged/cumulative `git diff --check` and the
  source-contract check passed. The latter used explicit
  `PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/task-inbox-access-start`
  with `node modules/pioneer-client-nitro/scripts/boundary-integrity.mjs check-source`.
  The contract/mobile diff is zero. No unrelated TypeScript/native build was run.
- Release standalone profiles completed with their dataset/projection/plan/
  receipt guards. These are profiling evidence, not functional-suite execution.

Existing warnings in artifact.rs, compaction_preparation.rs and the unused Gateway
capsule-refill test helper are from unchanged base code; no unrelated fixes were
made. Early nullable-model compilation errors in this diff were corrected before
the successful checks. Debug checks reused the existing ignored target cache;
source checkout files and Cargo locks remained unchanged.

Intermediate audits covered sources/lifecycle/order before implementation, then
atomic terminal/receipt writes, public-operation time versus metadata time,
covering/bound plans, and generated entities. Final review read the entire
cumulative source/migration/generated/test diff against the specified Rust base,
plus this report and the zero mobile diff. No UI, thread unread, scheduling,
connection configuration, .claude or bun.lock changes are included. Original
source worktrees remain Git-clean at the specified bases. No merge/rebase/push/
deploy or user-DB migration was performed.

Limits: historical dates cannot recover subsecond or queued grant commit order;
unrecorded historical private-to-public changes use thread creation as a lower
bound and can retain pre-publication history. New facts order durable commits,
not an external provider's unrecorded nanosecond finish time. Existing membership
survives suspension; a session lifecycle event is not a new workspace grant.
Functional race/restart/upgrade/ACL/viewability regressions have been written and
compiled, not executed. Native apps and full Gateway runtime were not launched.
Self-review and profiling do not constitute external acceptance.

Future functional commands — **только после полного внешнего ревью и явной
приёмки этой доработки**:

```sh
cargo test --locked -j 1 -p pioneer-crud --features test-support --test task_completion_inbox
cargo test --locked -j 1 -p pioneer-crud --features test-support --lib repositories::task_completion_inbox
cargo test --locked -j 1 -p pioneer-gateway --features pioneer-client/test-support --lib invitation::service::tests::accept_atomically_creates_exact_registered_role_grants_session_and_audit
```

Функциональные тесты не запускались. Реализация подготовлена к внешнему ревью.


## Migration source naming follow-up

All five inbox-related implementations, including cancellation attribution, now
live in `crates/migration/src/m20261016_000001_task_completion_inbox.rs`.
The filename follows the existing migration naming convention. Every implementation
has an explicit `MigrationName`; the cancellation actor retains
`m20261018_000001_task_cancellation_actor` despite its new module path.
Registration order, up/down bodies, and transaction settings remain unchanged.
This source-only move neither reapplies DDL nor changes existing migration records.

Follow-up checks: `cargo check --locked --offline -j 1 -p pioneer-crud --lib
--test task_completion_inbox --features test-support` passed. Source contract
check with `PIONEER_RUST_ROOT` pointing to the publication worktree passed.
Static normalized comparison confirmed unchanged migration bodies and transaction
settings. The existing compatibility regression also checks uniqueness of all
five historical names. Functional tests were not executed for this follow-up.


## CI historical-fixture follow-up

Run `39190635852` on `20536da5` failed six CRUD library tests. All six reported
`no such column: thread.inbox_workspace_after_sequence`; Windows and macOS jobs
passed. These failures came from current Entity/projector use against deliberately
older test migration prefixes, before the inbox access columns exist.

The native-cancellation production upgrade fixture still uses its exact historical
schema. Its preservation query now selects all historical thread columns and
projects NULL for the two later optional fields, following the existing stream
marker projection in the same test. An explicit schema assertion protects this
fixture from accidentally receiving the runtime compatibility fields.

Three cancellation rollback-guard tests and two occurrence-tracker migration tests
need today's projector on their older, reversible prefixes. Like the existing CLI
locator compatibility column, a test-only helper provides the nullable thread/run
fields and the sequence singleton required by that projector. It does not record
inbox migration versions, add grants/receipts, change the production migration
registry, or run on the pure production-upgrade fixture. Existing rollback,
marker preservation, trigger-installation and repair assertions remain in place.

Only fixture code and this report changed. Local `cargo check` and functional
tests were not run, as requested. Formatting, diff checks and source-contract
validation are the available local checks; runtime confirmation belongs to the
new GitHub CI run after publication.


## CI compaction legacy-summary follow-up

Run `39199100814` on `9bb64667` passed all 702 CRUD library tests, including the
six historical fixtures above. The next failure was
`compaction_migration_resumes_partial_ddl_and_preserves_legacy_data_on_reapply`:
it called current Thread summary CRUD before installing later inbox columns.

The partial-DDL fixture now seeds and reads only the historical `summary` and
`summary_turn_count` columns with typed SeaQuery statements. It still re-executes
the actual compaction migration twice and checks the retained metadata after each
application. After the full production migration chain, it additionally verifies
the same summary through the current CRUD. Production code, migration registration
and the historical schema boundary are unchanged.

Local `cargo check` and tests were not run, as requested. Validation for this
follow-up consists of source review, formatting and diff checks; the next GitHub
CI run will compile and execute it.


## Proactive audit of historical schema fixtures

This audit follows the seven new fields across the six changed entities:
`thread` (two fields), `task_run`, `gateway_principal`, `workspace_membership`,
`thread_membership` and `task_inbox_activation`. It is a source audit, not an
execution of fixtures. A repository-wide search found 335 migration-call/registry
references in 104 Rust files; most install the complete production registry.
Historical prefixes, selective registries, rollback helpers and direct DDL were
reviewed for reads through current Entities or CRUD before the relevant upgrade.

| Fixture family | Schema/use reviewed | Result |
| --- | --- | --- |
| CRUD cancellation upgrade snapshots | Production prefix before/through native cancellation; historical thread and stream projections | Retains historical fields and schema; prior repair applies |
| CRUD cancellation and run-occurrence rollback guards | Current projector on a reversible old prefix | Explicit test-only runtime fields/counter added in prior repair; pure upgrade fixture is excluded |
| CRUD native delivery, terminal effects, atomic terminalization, execution window and initial schema | Named prefix registries; actual up/down and historical table projections | No additional current inbox Entity reads identified; these library suites passed CI run `39199100814` |
| CRUD action outbox, task occurrence and task event fanout cutovers | Prefixes seed outbox/Task/cursor data and use narrow repository projections | No additional affected thread/principal/full-run reads identified; library suites passed the same CI run |
| Compaction partial-DDL and independent-summary upgrades | Raw historical seeds; full chain before current runtime reads | Legacy summary access repaired in `b2d7c4b1`; other paths upgrade before runtime access |
| Compaction preparation, item schema and compression | Historical raw seeds and compression, then complete production chain | No additional affected reads before upgrade identified |
| Compaction lifecycle rollback | Historical context/operation/runner metadata, explicit projection queries | No full current thread/run/principal access at the old boundary identified |
| Frozen storage/lifetime and durable sweep upgrades | Historical Task/locator/header seeds | Later CI exposed an indirect current TaskRun read inside the snapshot helper; corrected in the follow-up below |
| Inbox upgrade compatibility | Raw historical Task/Run/ACL seeds, new DTO queries after full chain | Existing stage/activation/receipt checks remain applicable |
| Gateway Epic 2 and refresh credential upgrades | Historical identity/session SQL, snapshot DTOs; current auth after full chain | No new principal-column reads before upgrade identified |
| Gateway fanout cutover | Cancellation-era prefix followed by current Gateway bootstrap, task context and receiver authorization | Additional incompatible fixture found and repaired in this follow-up |
| Task service and tools patch-history fixtures | Complete `Migrator::up(..., None)` before current CRUD | No historical-schema adaptation required |
| Shared client/FFI/mobile boundary | DTO/store fixtures, no Gateway SQLite entity-schema initialization | No affected database fixture to migrate |

The additional Gateway issue was in
`cutover_quantum_never_reads_history_and_only_delivers_post_install_events`.
Its old prefix omitted the inbox columns even though the test used current Task
context and authorization paths, including full principal Entity reads. The
fixture now starts with the complete production migration chain and current
Gateway setup. A rollback-only test registry places the real fanout migration
last so `down(1)` removes only fanout. An assertion using the actual production
registry requires that fanout be the sole pending version. Reinstallation uses
`Migrator::up`, then asserts that no version remains pending. The existing checks
for no historical reads/delivery, post-install sequence bounds, maintenance
routing and read-only readers remain unchanged.

The generic partial-schema Gateway setup helper was removed: its remaining
ordinary helper always applies the full production chain before current
bootstrap/authorization. This prevents another call site from choosing an old
prefix while silently using today's Gateway setup. Production migration names,
registration order, DDL, ACL and runtime code are unchanged; no fixture shim or
new notification framework was introduced for Gateway.

Validation: full follow-up diff review, formatting, `git diff --check` and the
mobile source-contract check with the publication Rust worktree. No local
`cargo check`, builds, migrations or functional tests were executed. The
identified Gateway failure is inferred from source, not observed in a local run.
The audit reduces this known risk; compilation and runtime correctness of the
new fixture still require GitHub CI, including any suites blocked by an earlier
failure.


## CI frozen-storage historical snapshot follow-up

Run `39206770994` on `6b016c02` passed all 702 CRUD library tests and all 76
compaction tests. It then failed
`released_schema_upgrade_matches_fresh_triggers_and_releases_existing_inputs`
with `no such column: task_run.inbox_sequence`. The earlier static audit missed
an indirect dependency: `task_input` inserts the run with SQL, then calls current
`insert_task_run_conversation_snapshot_if_absent`. That repository validates the
run through the complete current TaskRun Entity before inserting the snapshot.
The earlier frozen-storage audit row above has been corrected accordingly.

This upgrade fixture now seeds the run and accepted snapshot with typed SeaQuery
inserts using only historical columns. It preserves the exact Task/run/workspace/
thread link, frozen manifest locator and descriptor used by the prior helper.
An assertion requires `inbox_sequence` to be absent before seeding, so the test
continues to exercise the true historical schema. After the complete normal
migration chain, the existing expiry/physical cleanup/retained-descriptor checks
remain. Additional current-CRUD reads verify the retained snapshot's identity and
manifest after cleanup.

All other callers of the shared `task_input` helper were traced to the complete
`fixture()` schema. The direct snapshot calls in the retry-conflict scenario
also use that complete schema. The other historical `db_fixture(true)` paths
seed SQL and complete migrations before current CRUD. The Gateway HTML-retention
snapshot helper uses the ordinary fully migrated compaction fixture. No runtime
validation, root verification, released migration or generated Entity was
weakened or modified.

Local `cargo check`, builds, migrations and tests were not run. Validation here
is source review, formatting and diff checks. Compilation and execution of this
fix remain for GitHub CI.


## CI inbox fixture constraint follow-up

Run `39211958092` on `dbf678bb` passed the CRUD library (702), compaction (76)
and frozen-storage lifetime (25) tests. The inbox suite then passed 27 tests and
failed two while creating fixture rows, before their behavior assertions:

- The consolidated-migration compatibility fixture omitted the required
  `user_notification_outbox.delivered_at`. Its delivery/outbox rows now record
  deterministic creation, delivery and acknowledgement times in causal order.
  The acknowledgement value used by the receipt backfill is unchanged.
- The legacy timezone fixture moved membership `created_at` past its existing
  `updated_at`, violating `ck_workspace_membership_timestamps`. Both timestamps
  now use the same intended grant value, retaining all nine fractional digits
  and the `+03:00` offset. Expected tie/pre/post-grant unread counts are unchanged.

Other raw outbox inserts and membership date rewrites were inspected for the
same omissions. No production constraint, migration, timestamp normalization or
assertion was relaxed. Only fixture data and explanatory comments changed.
Local `cargo check`, builds, migrations and tests were not run; source review,
formatting and diff checks are the local validation for this follow-up.

## Merge of main after sidebar PR #142

The merge uses PR #143 head `2c265b600da21e75f49c5a23f9156f0dc08e587a`
and main `f6aa881dbbb3197db47cabb0a71b6988fdc10a1d`, including sidebar
PR #142 (`afc7b381188607bd351a10bf4c7de62760eaf32c`). It also retains
main's CLI task orchestration changes. Work was confined to the publication
worktrees; this section describes the merge, not a new performance acceptance.

Seven conflicting paths were reconciled:

- The migration registry retains every accepted inbox name and its registration
  order, adding main's thread-unread migration at the tail. The consolidated
  inbox source and the sidebar migration are unchanged from their own parents.
  No new migration, reordered released identity, or repeated backfill was added.
- Thread Entity is generated from the complete merged migration chain. It has
  both inbox access fields and sidebar `workspace_unread_started_at`.
- The startup historical Thread insert leaves all three later fields `NotSet`.
- The two occurrence-projector fixtures retain both branches' metadata helpers;
  neither helper changes the historical migration ledger or rollback target.
- Native cancellation's historical Thread query excludes all three later
  columns, using decoding-only placeholders. Historical insertion avoids a
  current-schema RETURNING read. Existing data-preservation assertions remain.
- Compaction seeds and reads historical summary columns directly and retains
  main's affected-row assertion. Current CRUD is used after the full upgrade.
- Fanout cutover retains the complete current bootstrap schema and PR #143's
  rollback-only registry. Its assertions still require only fanout to be pending
  before upgrade and no pending migrations afterward; main's cutover, bounded
  reads, routing and no-history assertions remain intact.

Shared production paths were reviewed against both parents. Membership inserts
retain their immutable creation dates and inbox sequence capture; duplicate
grants still preserve existing rows. Private-to-workspace transitions retain
both independently persisted access boundaries. Sidebar publication positions
and thread read cursors remain separate from inbox eligibility and receipts.
Authenticated Stop attribution, native/CLI reconciliation and atomic terminal
receipt commits remain alongside main's CLI task continuation and action receipts.

Preservation checks compare 94 paths changed by only one parent byte for byte
with that parent. Additional comparisons protect the main sidebar controller,
directory, coordinator registry, navigation, unread repository, timeline
projection, thread summary, authorization and migration paths, and PR #143's
inbox repository, worker, viewport, Gateway handler, consolidated migration and
popup files. All 116 comparisons passed. The existing review is preserved with
this section appended. The 24 paths changed by both parents
were reviewed as diffs against both sides, including fixtures, generated modules,
authorization, membership, cancellation and migration registration.

In particular, no inbox query or index was rewritten: workspace-first bulk
queries, bounded run-first seeks, structural `Expr::Constant("%s")`, negative
batch probes, unloaded-page read-all, empty-viewed refresh suppression and
source-connection hint exclusion remain byte-identical to the accepted PR.
Main's compact sidebar summaries, bounded unread reads, directory baseline merge
and existing-coordinator updates are likewise byte-identical. No cache, polling,
scheduling change or additional hot-path query was introduced by conflict repair.

Validation for this merge:

- `rustfmt` on resolved Rust paths; staged and unstaged `git diff --check`.
- Full stock migration CLI `up` on a newly created disposable SQLite database,
  built offline with `-j 1`; stock `sea-orm-cli generate entity --entity-format
  dense --with-serde both -l`. Only the conflicting generated Thread was copied
  back; unrelated generator differences were excluded.
- Static entity-literal review for combined fields and review of all merge
  conflicts and shared production hunks. Parent comparisons are repeated before
  commit and before publication.
- Mobile main's sole divergent source-contract change is merged and regenerated
  with the stock integrity script using explicit `PIONEER_RUST_ROOT` pointing to
  this publication worktree; `check-source` passes. DTOs, generated types,
  schemas, mobile code and lockfiles are unchanged by this merge.

Private generation logs, generated entities and preservation manifests are in
`/tmp/pioneer-inbox-merge-20261019.XNbLd6`. No user database or production backup
was opened. Local `cargo check` and functional tests were not run, as instructed.
There are no new CPU/RSS measurements: code equality supports preservation of
the accepted optimization paths, not a claim of measured runtime equivalence.
Compilation and execution of the combined code remain for GitHub CI.

## CI cancellation follow-up

The completed second attempt of run `39217624711` exposes two failures:
the method-registry test compares the correct 158-method set to a stale literal
156, and the pending CLI heartbeat test times out joining cancellation cleanup.
Three CLI Stop tests never report completion (history transfer, coordinator
wait, and selected-turn restore). The Gateway binary remains alive until the
job is cancelled at its three-hour limit. Other `panicked at` lines in that log
include successful intentional panic-containment tests and were not treated as
failures. Run `39248717445` still has an active workspace step at this inspection;
its active Linux log was unavailable, so it is not evidence of an identical result.

The registry regression now retains its exact protocol-set comparison, duplicate
checks and per-entry validation without hardcoding the number of public RPCs.
No authorization entry or protocol method changed.

Source review found that the CLI history monitor waits for `get_turn` before
inspecting authenticated Stop intent or shutdown. A queued read therefore blocks
the local cancellation which must drop capture/progress work before joining its
cleanup. The monitor now retains one state-read future and races it against
shutdown and the existing 50 ms intent check. A local Stop drops the pending read;
it does not imply a terminal commit or acknowledge cancellation. The existing
durable graph fence, initiator persistence, terminal receipt transaction, CLI
transition gate and awaited runner cleanup are unchanged. There are no extra
state queries, detached tasks, larger pools or changed scheduling classes.

A new regression uses the real pooled fixture and production capture path with
all four read-only connections occupied. It checks Stop and shutdown before any
reader is released, rejects another thread's intent, and checks reader usability
after cancellation. Existing Stop RPC assertions for the three stalled scenarios
now have a 30-second bound around the request itself, while the paused work is
still held. Their terminal-state, no-provider-start, cleanup and session-reuse
assertions remain. The pending heartbeat test's existing failure oracle and
timeouts are unchanged; neither failing tests nor scenarios were disabled.

Local validation is `rustfmt --check`, `git diff --check`, full diff review and
the stock mobile `check-source` with this Rust publication worktree explicitly
selected. The source contract is unchanged. Local `cargo check`, compilation and
functional tests were not run, per the user's instruction. Runtime confirmation
that the read cancellation also resolves each CI stall remains for the next run;
these changes are not reported as a passing test result or performance measurement.

## Stop intent mutex deadlock: follow-up to run 39264248928

The captured workspace log for `68060f8e` contains three actual `FAILED` results:
`cancelling_cli_history_transfer_stops_before_provider_start_and_releases_session`,
`cancelling_cli_transfer_while_waiting_for_coordinator_releases_session`, and
`cancelling_selected_turn_restore_releases_cli_session`. All time out inside the
Stop RPC. The method-registry regression and the new pending-reader cancellation
regression pass in that CI log. The reader fix therefore did not resolve the
remaining RPC deadlock. The run was manually cancelled to retrieve its logs;
the log ends at 15:06:30 UTC, before the pending-heartbeat test and the remaining
Gateway tests. No results are inferred for that unexecuted remainder.

The concrete lock cycle is in `mark_turn_blocked_with_task_start_failure`:
the `if let` scrutinee acquires `user_turn_cancel_intents` and clones an intent,
but its temporary `MutexGuard` lives through the successful branch. That branch
awaits `mark_turn_interrupted_with_recovery_disposition`. The terminal path
reaches `materialize_native_agent_turn_event_with_cli_blocked_guard`, which
acquires the same mutex to obtain the authenticated actor before committing the
Interrupted event. Cleanup waits for itself; the cancel handler also needs the
same map or waits for the preparation's CLI transition gate.

The fix clones the existing intent in an explicit block, then branches on the
owned value after the guard is dropped. Every other production access to this
map was checked: cloned values or booleans end their guard at a statement or
expression block; the reference-count release helper performs no subsequent
await while locked. No attribution, reference count, transition guard, database
transaction, task/run scope, receipt or notification ordering was changed.
The fix adds no query, cache, timer, task or allocation beyond the existing clone.

The three existing bounded RPC scenarios and the pending-heartbeat scenario
remain the regressions for the lock cycle. The selected-restore scenario now
also requires the exact run to be Cancelled rather than any terminal status,
and checks its persisted initiator and that principal's receipt before releasing
the paused restore. Its later session-reuse assertions are retained.

Validation: source audit of all intent-map uses, full change review, rustfmt,
diff checks in both repositories and stock `check-source` against the explicit
Rust publication worktree. The contract remains unchanged. Local `cargo check`,
compilation and functional tests were not run. The next automatically triggered
CI run must confirm the fix; no run is cancelled or manually restarted by this
follow-up.

## CI terminal replay fixture: run 39276004866

The run for `9397b0ca` completed normally with a test failure, rather than
stalling. Windows and macOS checks passed. The Gateway library reported 2913
passed, zero failed and eight ignored; all three formerly stalled CLI Stop
scenarios and `pending_cli_heartbeat_writer_is_dropped_before_compaction_cleanup`
passed. This is GitHub CI evidence for the previous lock-lifetime fix, not a
local execution or evidence that subsequent workspace targets ran.

There is exactly one `FAILED` result in the full downloaded log:
`tests::cancelled_terminal_commit_uses_saved_stop_actor_and_rolls_back_receipt_failure`
in pioneer-tasks (227 passed, one failed). Its receipt-failure rollback and
successful cancellation steps complete. It then calls the low-level terminal
handle again with reason `replay` and a different timestamp, expecting success.
The saved cancellation used reason `saved Stop`; the production terminal fence
correctly rejects a different committed outcome. This is the same contract
already covered in the adjacent terminal-delivery conflict regressions.

The fixture now repeats the exact accepted reason and time for the idempotency
check. It separately requires the changed-reason attempt to return the typed
`TaskTerminalConflict`, checks that the exact replay created only one canonical
cancellation event, and compares the journal before/after the rejected attempt.
Its original rollback, retained initiator and one personal receipt assertions
remain. No production terminal comparison, TaskService cancellation semantics,
receipt logic, migration, query plan, client or scheduling code changed.

Validation: full log scan for actual test results and failures; review against
the terminal-commit predicate, handle implementation and neighboring regressions;
rustfmt and diff checks. Local tests, compilation and `cargo check` were not run.
The corrected fixture still needs execution in the next automatic CI run. No CI
run was cancelled or manually restarted for this follow-up.
