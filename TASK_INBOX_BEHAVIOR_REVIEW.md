# Task completion inbox: implementation for external review

## Isolation

Both branches are `fix/task-inbox-behavior`.

| Repository | Worktree | Local main at branch creation |
| --- | --- | --- |
| Rust | `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-inbox-behavior` | `f67cc163e43ce53d97643ac66badefb338523f52` |
| Mobile | `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-inbox-behavior-app` | `1e2df533fd9924bca4c2f9a117e55fdf9757d688` |

The original checkouts were clean during the initial implementation inspection and its final verification. No old inbox worktree was used. No merge, push, deployment, publication, or user database access is part of this change.

During the initial six-fix implementation the main refs changed externally: its final Rust main was `33a827bbaaf457f342d9eac15311285e5984bb08`, and its final mobile main was `0513f50618a94eaba21e54156d344b13e361ca5c`. Reflog records external Rust fast-forwards and a mobile reset to origin/main. The implementation commits' direct parents are exactly the two branch-creation bases in the table. The branches were deliberately not merged or rebased onto concurrently changed main. Compilation, generation and baseline comparisons reported below apply to these recorded bases and their implementation diffs; integration with the newer main refs requires the reviewer's subsequent review.

## Existing paths and changes

1. **Historical completions.** CRUD still reads terminal `task_run` records. A new migration copies the original inbox migration's durable activation fact into `task_inbox_activation`. The same eligibility predicate now drives unread list filtering and count, before keyset pagination. Historical results remain available through the existing exact-entry API; no personal viewing timestamp or historical receipt is fabricated.
2. **Actual viewing.** Shared Client resolves only `TimelineDemand.viewed_row_ids` against the current immutable semantic snapshot. Terminal task rows contribute their exact `run_id`; final messages contribute their exact Turn and item ID, while terminal status rows contribute their Turn. Gateway resolves child results using exact accepted candidate bindings, or the matching terminal work frontier for paths without a candidate. Delivered messages use the existing canonical result item ID to select one exact delivery, plus its `delivered_turn_id`; a status-only delivery uses its canonical dedicated delivery Turn identity. Two deliveries sharing a parent Turn cannot read one another. Gateway applies explicit run/task/thread/workspace existence and belonging checks. Canonical TaskRead/root ACL/membership remain in the receipt path. Desktop's active-window/connected/authenticated gates and mobile's active-screen/foreground/connected gates remain in their existing viewport paths. Open thread only navigates. Manual Mark as read remains on every unread row.
3. **Own cancellation.** Authenticated `TaskMutationContext.actor_id` passes through Service and Projector to the existing cancellation commit. The writer revalidates which runs are nonterminal immediately before the event batch. Only newly inserted cancellation events for those runs can suppress unread for that actor. The receipt, terminal projection and durable fanout journal commit together; receipt failure rolls back that quantum. The internal receipt validates the exact cancelled run/task/workspace even when no presentation root exists yet. It grants no TaskRead access. Other principals, old terminal runs and future repeats are unaffected.
4. **Scrollbar.** Popover's default padding is explicitly removed, as in Gateway switcher. The virtual viewport uses the list width and the same standard `vertical_scrollbar` composition as Tree. Insets belong to rows/header/footer; rows reserve a scrollbar gutter. The retained scroll handle and run-derived row IDs remain. Row dimensions are recomputed from rem size.
5. **Lazy loading.** The Load more button is removed. VirtualList's resolved range produces a cursor-deduplicated deferred demand through shared LoadMore. Wheels, thumb movement, keyboard selection and resize all update that range. Only one page request is active; demand during a different operation is coalesced in the existing queue. Underfilled sequential loading is capped at three pages. Errors stop automatic demand and retain the loaded prefix, cursor and selection; Retry remains visible for the failed cursor even through an unrelated successful refresh. Explicit Retry permits another attempt. Workspace/session fences reject stale work.
6. **Read all.** A fixed footer follows a Separator, with the Gateway switcher's ghost/xsmall/compact treatment. All eight desktop locales include its label, including “Прочитать всё” and “Mark all as read”. It is disabled without unread or while loading. Errors remain visible without an optimistic permanent zero.

## Activation evidence and its limits

The released `m20261016_000001_task_completion_inbox` already has a `seaql_migrations.applied_at` record. The installed SeaORM migration implementation saves Unix seconds after a successful migration. Gateway uses the standard migration table. The original migration copied legacy acknowledgement receipts, so an earliest receipt is not evidence of a user's first inbox visit. No durable personal first-view/activation fact exists in this implementation.

The deterministic fallback is **Gateway activation**, not installation time of this repair: completions whose stored completion second is at least that original migration's applied second are eligible. Results with eligible saved completion times survive the repair, including those saved before this fix was installed. The whole activation second is retained to avoid dropping genuinely new results at the precision boundary. This can retain pre-activation completions from that same second. It is not a reconstructed per-user activation time, and relies on the stored completion/migration clock facts. Eligibility follows saved completion time: a historical record incorrectly dated after activation can remain eligible, and a later commit with a backdated completion timestamp can be excluded. This fact cannot reconstruct exact historical commit order or a personal first-view time. A principal joining later can still have accessible, unacknowledged post-activation results; the baseline does not invent a personal joining/viewing cutoff. Missing original migration evidence fails migration instead of silently using repair time. This is server state, independent of client reinstall, reconnect, device or process restart.

Suppression is a query eligibility rule. `read_at` continues to mean an actual personal receipt/legacy acknowledgement. No history/result payloads are loaded to compute activation or count, and first opening does not backfill receipts.

## Personal reads, concurrency and invalidation

`task_run_read(principal_id, run_id)` remains the durable personal key. OnConflict preserves idempotency. Large viewports are split into bounded batches without dropping evidence. Shared Client caches successful viewport proofs within the authenticated connection/authorization identity and fences worker results before updating state. Terminal/delivery commit notifications retry negative evidence only for still-active, revision-matching viewed rows; an epoch prevents an older in-flight negative response from suppressing that retry. Hidden/prefetched rows and a thread cursor alone produce no inbox receipt. Existing thread unread/cursor rules are unchanged.

`task/inbox/read_all` uses the existing workspace list request/response shapes and a single typed INSERT SELECT over authorized eligible unread runs. That serialized writer statement defines the exact set: a later committed completion remains unread even if it has the identical completion timestamp or a lower run ID. Task creation time and wall-clock cutoffs do not decide membership. The explicit bulk command can write one receipt per included run; ordinary opening never does. Its cost scales with the active workspace's eligible metadata/receipts, which is necessary for its ACL-specific atomic set.

The writer is released before personal invalidation fanout. Bulk invalidation is sent after the durable mutation even if fetching the response page subsequently fails. List and count share a short read snapshot; result/error projection stays bounded to the page. Existing personal invalidations reach all sessions of the principal, with workspace scope. Cancellation continues to use the existing durable task-event fanout.

## Storage and generation

New migration: `m20261017_000001_task_inbox_activation`. It uses its own DeriveIden identifiers and SeaQuery/SchemaManager builders, without depending on entity. It adds the activation table and a workspace/thread/result-Turn index for delivery proof lookup. The released migration is unchanged.

Entity was generated by installed `sea-orm-cli 2.0.0-rc.35` in dense/serde-both/chrono mode from a disposable SQLite database after the full normal migration chain. Only the new entity and generated registration lines are retained; unrelated generator output is excluded. Runtime keeps foreign_keys OFF and continues through SqliteDatabase/CrudStore and its serialized writer/readers. The only runtime SQL-rendering adapter is the existing fixed INDEXED BY workaround for SeaQuery's missing SQLite hint API, now also applied to the typed bulk statement.

Protocol, Client and FFI schemas and mobile types use the normal generators, with explicit `PIONEER_RUST_ROOT` for mobile generation and contract verification. Generated files and source contract are included in review.

## Initial implementation validation (before this review follow-up)

Tests are deliberately not executed. Compiling test targets is not external acceptance.

Completed storage/generation and static checks:

- The full normal migration chain completed on disposable databases, including the final activation/index migration. The final generation database was `/tmp/pioneer-task-inbox-behavior-final-20261017.sqlite`; the full-chain log is `/tmp/pioneer-inbox-final-migrations.log`.
- `sea-orm-cli generate entity` completed from that database. The retained activation entity is byte-identical to the CLI output (`cmp` passed). Generated registration lines were taken from that output.
- Protocol schemas were exported with `PIONEER_RUST_ROOT` set to the Rust worktree and `cargo run -j 1 -p pioneer-protocol --bin schema -- schemas`.
- With the same explicit Rust root and `CARGO_BUILD_JOBS=1`, `bun run client` and `bun run client:check` succeeded. The latter independently reproduced all **552 schemas and 553 generated TypeScript files**. No unrelated generator churn was retained. Native-only Viewed evidence is omitted from the public mobile intent schema.
- `boundary-integrity.mjs check-source` matched the source contract against that Rust worktree, including after dependency-symlink removal.
- `cargo fmt --all -- --check`, both repositories' `git diff --check`, targeted mobile ESLint and Prettier checks passed. All eight desktop locale TOML files parsed with the new footer label.
- Full mobile `tsc --noEmit --incremental false --pretty false` exits 2 with **42 existing diagnostics**. Its final output is byte-identical to the check of a separate `git archive` of base `1e2df533fd9924bca4c2f9a117e55fdf9757d688` using the same installed dependencies. Logs: `/tmp/pioneer-task-inbox-app-tsc-final.log` and `/tmp/pioneer-task-inbox-app-base-tsc.log`. No new TypeScript diagnostic was introduced.
- The six-package Rust check covering CRUD, Tasks, Client, Gateway, desktop inbox and Client FFI, including test targets with `pioneer-client/test-support`, passed. A final check including the complete desktop shell is recorded below.

Final Rust check passed (16m 40s), including the complete desktop shell and all selected test targets:

```sh
cargo check -j 1 -p pioneer-crud -p pioneer-tasks -p pioneer-client -p pioneer-gateway -p pioneer-desktop-task-notifications -p pioneer-client-ffi -p pioneer-desktop --tests --features pioneer-client/test-support
```

Log: `/tmp/pioneer-task-inbox-check-reviewed-final.log`. This includes the final viewport batching, scope fencing, lazy Retry preservation and footer changes. No test executable was run. The complete final diffs of both repositories, including new migration/entity/schema files, locales and regressions, were reviewed; issues found during review were corrected before this final compile.

The initial Client test-target check without `test-support` failed in the unchanged base `composer/workflow.rs` because `ClientMutationAuthority::for_test()` is gated by that feature. The corrected checks include the feature. Existing warnings in unchanged files are not treated as inbox regressions.

New/updated regressions cover upgrade activation and reopen, no fabricated viewing date/backfill, exact visible run/result evidence, prefetch and mobile lifecycle, receipt synchronization, cancellation actor/replay/rollback/future runs, unread filtering before limit/cursor, unloaded bulk pages/ACL/workspace/concurrent equal-time completion, lazy error/retry/fencing, and fixed popup geometry/footer during keyboard scrolling.

## External review follow-up: four corrections

This follow-up started from the reviewer's exact clean commits, without updating
from main: Rust `a7925640a561bb5a742e7caaba97c2a163277b86`, mobile
`ddd8d5b65e406bdf5f00df98279a9794370067d6`. Both worktree paths and branch names
above are retained. The cumulative diffs are reviewed against the two original
branch-creation bases, including the previous six fixes, generated files,
locales, and regressions. Main checkouts and unrelated files are not modified. Read-only inspection in
this follow-up found both main checkouts clean, at Rust
`42c7533818f71d592e0d30fbf885625c97ccf388` and mobile
`0513f50618a94eaba21e54156d344b13e361ca5c`; these advancing refs are not the
implementation or verification bases.

1. **Composer Stop attribution.** Desktop `stop_active_turn` and the shared
   `request_turn_cancellation` issue `turn/cancel`. `turn_handlers.rs` now retains
   the authenticated principal with the live Stop intent. Concurrent requests
   share that first intent until all requests finish; one failed duplicate
   cannot remove another request's actor. Native durable callbacks, CLI Stop,
   and interruption commits use that intent. The canonical append transaction
   saves attribution for the exact nonterminal run joined through its actual
   task/turn/thread/workspace records. Replayed canonical events cannot rebind
   the actor. Root `cancel_root_agent_work_graph_for_turn` passes the actor into
   TaskMutationContext instead of defaulting it away. Its graph-fence transaction
   saves attribution for actual active graph occurrences and attached tasks in
   the existing exact originating-turn pre-admission scope. Already terminal
   runs and future repeat IDs are excluded.

   `TaskExecutionHandle::cancel_run` reaches the existing terminal transaction;
   it now writes the actor's personal receipt with RunCancelled, finalization,
   and the task-event fanout journal. Independent TaskService system/agent
   cancellation does not inherit a saved Stop actor; its existing explicit
   mutation context identifies that separate operation. Deferred child terminal
   reconciliation uses the saved attribution. A failed receipt rolls back
   the terminal transition; fanout remains outside the writer. Actor intent
   itself grants no TaskRead or root ACL access. Other principals' receipts are
   untouched. A partly reconciled cancelled task_run_turn is now discoverable
   while its run remains active, and reconciliation retries the terminal commit
   without rewriting that already recorded per-turn failure. An actual atomic
   blocked-run resume retires pending attribution for that run: a Stop that lost
   to Blocked cannot attribute a later cancellation of the reopened execution.
   Existing read receipts are preserved.

2. **Ready delivery inside a running parent.** The exact canonical delivery
   result item requires a completed saved agent_message plus the real delivery
   ID, target thread, delivered turn, workspace, task, and terminal source run.
   The containing parent Turn may still be in_progress. Status-only evidence
   retains the terminal Turn requirement and canonical dedicated-delivery role.
   A result item never falls back to the parent's own task_run_turn binding.
   The regression includes two children and a parent sharing the same Turn:
   only the viewed child's receipt is saved and survives later parent completion.

3. **Actual accepted outcome.** Successful child-work evidence requires an
   initial/revision/recovery task_run_turn in candidate_created state, with an
   accepted candidate matching run, task, binding ID, thread, and turn. A later
   reviewer does not displace the accepted source; a rejected/superseded source
   or an intermediate reviewer cannot acknowledge the run. Legacy no-candidate
   success uses candidate_created at the durable ordered work frontier. Error,
   blocked, cancelled and timeout paths require corresponding terminal work at
   that frontier; Interrupted work can represent the existing system-failure
   path. Sequence is the existing causal work order (unique per run), not a
   timestamp heuristic. Unknown roles and inconsistent candidate bindings fail
   closed. Direct visible terminal run cards and exact deliveries retain their
   separate proof paths. No result/HTML payload is parsed for these decisions.

4. **RPC failure is not negative evidence.** record_viewed returns without
   populating either server-evidence cache on Err. A separate transient failure
   gate allows one failed attempt per real view/domain epoch. Source revision
   updates and row reordering alone do not renew it. A changed actually visible
   row set or consumer generation renews failures after releasing the registry
   lock; confirmed and successful negative evidence remain deduplicated. A fresh
   real revisit can retry, including after an ordinary successful inbox refresh.
   Ok([]) remains negative evidence until a relevant domain epoch change;
   confirmed proof keys remain deduplicated. Request/identity/epoch fencing is
   retained. No timer, polling, or render-triggered retry was added: an unchanged
   viewport alone does not repeatedly issue RPCs. Worker regressions exercise
   ClientCore, its real request queue, typed RPC and completion path with a test
   transport: temporary transport/server failure, refresh between attempts,
   no retry loop from repeated unchanged proofs, successful confirmation dedupe,
   successful negative evidence, and an old confirmation crossing a connection
   replacement. A planner regression distinguishes streaming/source updates from
   real visibility renewal. The existing transport test
   seam is also available to the direct RPC path used by the inbox worker.

New migration `m20261018_000001_task_cancellation_actor` adds only the per-run
attribution fact. It uses local DeriveIden and SchemaManager builders; no
released migration is edited. The full normal migration chain was applied only
to `/tmp/pioneer-inbox-review-20261018.sqlite`; log:
`/tmp/pioneer-inbox-review-migration.log`. Installed SeaORM CLI generated the
entity into `/tmp/pioneer-inbox-review-entities-20261018` with dense/serde-both/
chrono options. Only the new entity and its generated registry entries are
retained. `cmp` verifies the retained entity against CLI output. Runtime FK OFF,
SqliteDatabase readers, serialized writer, and post-commit fanout are preserved.
The updated bounded reconciliation discovery uses typed SeaQuery instead of
extending its former raw SQL.

Follow-up checks:

- The final five-package check, including test targets, passed (2m 09s):
  `cargo check -j 1 -p pioneer-crud -p pioneer-tasks -p pioneer-client -p pioneer-gateway -p pioneer-client-ffi --tests --features pioneer-client/test-support`.
  Log: `/tmp/pioneer-inbox-review-complete-check.log`. This includes the final
  transient failure gate and viewport renewal regression. Earlier passes of the
  same command also succeeded (6m 46s and 13m 28s), before the last refinements;
  logs: `/tmp/pioneer-inbox-review-reviewed-diff-check.log` and
  `/tmp/pioneer-inbox-review-final-check.log`. The initial exploratory check found
  a new numeric `min` / ExprTrait ambiguity; it was corrected to `std::cmp::min`,
  and the subsequent successful checks contain no such error.
- New/updated regressions compile through these targets. They cover actual
  native API and CLI child Stop, native parent graph Stop, canonical actor
  acceptance/replay, terminal receipt rollback, independent system cancellation,
  blocked-run reopen attribution, two exact deliveries in a running parent,
  rejected/superseded and reviewer evidence, terminal errors without candidates,
  and ClientCore/worker RPC recovery and fencing.
- `cargo fmt --all -- --check`, Rust and mobile `git diff --check`, and the
  generated entity `cmp` passed. Warnings observed in the successful Rust check
  are in artifact.rs, compaction_preparation.rs and
  thread_episodic_workspace_capsule_refill.rs; those files were explicitly
  compared with the review-start commit and have no diff.
- With explicit `PIONEER_RUST_ROOT` pointing to this Rust worktree and
  `CARGO_BUILD_JOBS=1`, the normal `bun run client` generator passed. Its log is
  `/tmp/pioneer-inbox-review-final-client-generation.log`. Independent
  `bun run client:check` reproduced all **552 schemas and 553 TypeScript files**;
  log: `/tmp/pioneer-inbox-review-final-client-repro.log`. The public DTO/schema
  outputs are unchanged from the review-start commits. Mobile retains only the
  generated Rust source hash update:
  `929628f07fa33e7920e294e5d69d9f7a8e03681d26034a9d3c3b42708247f930`.
  `boundary-integrity.mjs check-source` matched before and after removal of our
  temporary dependency symlink. No unrelated generator churn is retained.

Applications and test executables are not launched. Compilation is not runtime
validation or external acceptance. Full mobile tsc and Clippy are not rerun in
this follow-up; the TypeScript diagnostics above belong to the initial stage.
Native mobile binaries and interactive/visual workflows still require the
reviewer's subsequent validation.

Limitations: pre-fix interruptions without a saved authenticated cancellation
actor cannot safely be retroactively attributed; creator/owner identity is not
used to fabricate those receipts. The activation baseline and its second-level
historical uncertainty described above are unchanged. A failed viewed RPC can
retry on a renewed real viewport or a relevant completion/delivery event;
there is intentionally no continuous retry for an unchanged viewport. Ambiguous
legacy outcome evidence stays unread until a real run card, exact delivery, or
manual read supplies a valid acknowledgement.

## Commands reserved for after external acceptance

Do not execute these until the reviewer has completed review and explicitly accepted the implementation in code.

```sh
cargo test -j 1 -p pioneer-crud --test task_completion_inbox
cargo test -j 1 -p pioneer-client --features test-support tasks::notifications
cargo test -j 1 -p pioneer-client --features test-support timeline::controller
cargo test -j 1 -p pioneer-tasks
cargo test -j 1 -p pioneer-crud --lib duplicate_interruption_during_projection_preserves_canonical_acceptance
cargo test -j 1 -p pioneer-crud --lib task_owned_blocked_turn_resume_reopens_the_full_aggregate
cargo test -j 1 -p pioneer-gateway --features pioneer-client/test-support cancelling_detached_native_child_turn_cancels_parent_task_anchor
cargo test -j 1 -p pioneer-gateway --features pioneer-client/test-support composer_stop_native_api_child_saves_personal_cancellation_receipt
cargo test -j 1 -p pioneer-gateway --features pioneer-client/test-support parent_turn_cancel_cancels_attached_child_tasks_through_service
cargo test -j 1 -p pioneer-gateway --features pioneer-client/test-support task_completion_inbox_rpc_uses_root_acl_and_syncs_personal_reads_without_timeline
cargo test -j 1 -p pioneer-desktop-task-notifications --features pioneer-client/test-support
```

From the mobile worktree, after dependency setup and external acceptance:

```sh
PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/task-inbox-behavior bunx jest src/client/timeline-demand.test.tsx --runInBand
```

Applications were not launched and mobile native binaries were not rebuilt. Visual/runtime behavior, native app integration and regression execution require the reviewer's subsequent validation. The temporary dependency symlink in the app worktree was removed; original checkout contents remain untouched.

Тесты не запускались. Требуется повторное внешнее ревью
