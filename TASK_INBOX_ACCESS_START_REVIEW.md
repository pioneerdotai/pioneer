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
