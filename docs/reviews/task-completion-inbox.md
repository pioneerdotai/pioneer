# Task completion inbox — external review handoff

Current status: the previous external acceptance is withdrawn. The storage architecture revision below requires a new external review and explicit acceptance. Earlier CI results in this document describe earlier commits, not this unexecuted revision.

Implementation branch: `feature/task-completion-inbox` in both repositories.

- Rust worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-completion-inbox`.
- Rust base: local `main`, `7f76de76292e2f5180ffb8307ec7ca8f8e6e29ea`.
- Mobile contract worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-completion-inbox-app`.
- Mobile base: local `main`, `b8c3e8bd1e06b9804d8c4849bb8a12aa3f0608ab`.
- Main checkouts, other worktrees and the pre-existing mobile `bun.lock` change were not edited. Publication and later main synchronization are recorded below; no worktree was removed.

## Main synchronization after the storage revision

At the user's request, the storage revision was committed as `3debeeea` and the latest fetched `origin/main` was merged into both feature branches: Rust `0160b2aedbd0929641993350b7598d7a48e96979`, mobile `04d479c`. Rust merged without conflicts. The mobile conflict was confined to `boundary-source.json`; the standard `client:contract` generator rebuilt it with the explicit Rust worktree path. Contract versions, package selection and verification logic remain unchanged.

Static review checked the overlapping authorization, Gateway task event, protocol export and generated schema changes. Main's running-task scope/activity changes and the inbox's typed scope, workspace-first selection, personal receipts and loaded-history refresh remain present. `check-source` matched after generation, and `git diff --check` passed in both worktrees. No local compilation, tests or migrations were run during this synchronization. Earlier compilation/typecheck limitations below remain applicable; the merged revision still requires external review.

## Data path and invariants

Saved terminal TaskRun → canonical task scope and collaboration root → current TaskRead/root ACL → bounded Gateway page + total unread count → client TaskInbox publication → desktop virtual list → authorized context lookup/navigation → personal read receipt → invalidation of the user's devices.

One row is one `run_id`. Delivery policy, executor, attachment, schedule and online/timeline subscription state are not selection predicates. Recurring occurrences and retries have separate run IDs. Task/TaskRun events and deliveries do not insert inbox rows. A changed run is re-read under the same ID.

The list includes saved `succeeded`, `failed`, `blocked`, `cancelled`, `timed_out` rows with `completed_at`. All other statuses are excluded, even if a stale timestamp remains. Completion time comes from `task_run.completed_at`: the existing projector writes completion/failure time, blocked time, or cancellation time there. `updated_at` is not used for completion ordering. Existing executors and launch mechanisms are unchanged.

The desktop displays task title, user thread, terminal status, local completion time, bounded summary/public error text, read state, Open thread and Mark as read. Opening the bell does not read entries. Read history remains available. Rows are virtualized; loading, empty and retry states and all eight desktop locales are covered.

## Authorization and navigation

`authorization_scope.rs` owns one typed SeaQuery scope for exact task scope resolution and inbox set queries. It validates the root Task, creator-turn consistency, internal-thread lineage, workspace, actor envelopes and execution admission root. It does not substitute a child's owner or creator for its root ACL.

Gateway admission retains own-notification permissions and additionally evaluates the canonical TaskRead action gate. Scoped users require the active workspace membership and, for private roots, exact thread membership. Absolute access comes only from the canonical TaskRead gate. Service Tasks without a user collaboration root are excluded, including for absolute principals; System executor Tasks with a valid root are included. Sidebar visibility does not restrict accessible history.

List, count, get and read use the same relation and ACL predicate. Read commits through the serialized writer in a single `INSERT … SELECT … ON CONFLICT DO NOTHING`, rechecking access in the mutating statement. Reads inherit the request's interactive database scope; no connection is opened outside the existing database/repository boundary.

Open first fetches the exact authorized run context, then fetches its user/root thread. The client checks workspace/task/run identity and navigation/request generations before committing navigation. Successful navigation schedules the separate server read operation. Inbox read does not call thread/read or mutate thread read cursors.

Existing authorization invalidation evicts protected client publications and in-flight results. Closing/switching a workspace drops its retained inbox binding/state; request identity fences reject stale responses.

## Migration and old inbox

`m20261016_000001_task_completion_inbox` adds:

- `task_run_read(principal_id, run_id, read_at)`, primary key `(principal_id, run_id)`;
- partial legacy acknowledged-receipt lookup index on recipient/run/workspace/task.

The migration copies only exact, existing run/task/workspace legacy acknowledgements to that recipient's read state. It preserves the outbox, payloads and delivery receipts. A runtime legacy-ack fallback also accounts for acknowledgements made by older clients after migration. Old acknowledgements emit a read invalidation. No fake delivery is created and no result is copied per collaborator.

Saved accessible terminal runs without a receipt are unread, including pre-existing history. There is no attempt to reconstruct unknown historical viewing.

## Pagination, count and synchronization

`task/inbox/list` uses descending `(completed_at, run_id)` keyset pagination, default 30 and maximum 100. Its cursor contains the exact saved timestamp and ID and does not require the cursor row to remain accessible. The page and total unread count are read in one short reader snapshot. Projection/serialization and notification fanout happen after the snapshot is released.

The count is an aggregate over the entire authorized terminal set and personal/legacy receipts, independent of page size. It uses metadata and indexed joins, not full result loading or per-row application queries. Page and count start at `task INDEXED BY idx_task_workspace_status`, constrained by workspace ID, then `CROSS JOIN task_run INDEXED BY idx_task_run_task_id` constrained by each task ID. The keyset predicate and metadata sort operate within that workspace. A materialized metadata page bounds result/error extraction to at most 101 runs. No offset-based history walk or client-side authorization filtering is used.

`task/inbox/changed` is a payload-free invalidation on the existing websocket event path. Persisted terminal/run-state transitions notify authorized workspace connections before timeline/delivery work. This also avoids depending on the old richer Task notification payload decoder. Read changes notify only the current principal's connections. The client coalesces hints and reloads authoritative state; it does not append event-shaped cards. Reconnection and authorization/workspace changes use existing client/session fencing and the desktop binding lifecycle. No polling or new event bus is added.

## Contracts

New shared DTOs: TaskCompletionInboxEntry, ListParams/ListResponse, EntryParams/EntryResponse, ChangedNotification. New RPCs: `task/inbox/list`, `task/inbox/get`, `task/inbox/read`; new invalidation: `task/inbox/changed`.

TaskInboxPublication now carries completed-run entries and authoritative `unread_count`; TaskNotificationIntent adds LoadMore. The existing client-owned inbox controller remains shell neutral. Desktop consumes typed client publications directly. FFI remains a boundary adapter; its generated schemas include the shared changes. Protocol/client/FFI schema generators and mobile generated TypeScript are the contract sources. No new mobile screen or hand-authored generated types.

The existing mobile notification card required a small consumer update from outbox fields to completed-run fields (`runId`, `readAt`, title, summary and public error text). The bridge envelope and C ABI are unchanged. Required aggregate Rust schemas also refresh already-main HTML-publication/artifact-version and SettingsPatch definitions that were stale in tracked generated snapshots; no corresponding feature source was changed, and unrelated generated files were excluded from this diff.

## Regression coverage prepared

- CRUD integration suite `task_completion_inbox`: status/policy/executor/attachment matrix; service vs thread-bound System Tasks; recurring occurrences; scheduled/other trigger history; private participants and independent reads; durable reopen; root ACL and internal lineage; creator-turn and admission mismatches; access removal for list/count/get/read; stable equal-time pages; global count; workspace isolation; migration restart/idempotency and late legacy acknowledgements.
- Gateway test `task_completion_inbox_rpc_uses_root_acl_and_syncs_personal_reads_without_timeline`: real RPC admission, two devices of one principal and another collaborator, read hints, terminal Task/TaskRun replay without duplicate cards, root context and revoked access.
- Client notification tests: pagination/total/merge identity, exact run/user context checks, read-history reopening, principal/workspace/connection/request fencing and authorization eviction. Existing inbox tests updated for run entries.
- Desktop feature tests: workspace changes clear retained cards/count; keyboard selection reaches virtualized rows without reading them and Escape closes the popup; existing notification lifecycle test updated for the shared contract.

## Storage architecture revision (current, awaiting external review)

Pre-change audit covered `m20260804_000001_shared_messages_authorship`, `m20261008_000001_provider_usage_observation`, the `thread_read_cursor`/`user_notification_outbox` entities, the outbox and TaskRun repositories, and the installed SeaORM 2.0.4 / SeaQuery 1.0.2 sources. The fixed invariants are the original three TEXT columns, `(principal_id,run_id)` primary key, run FK with `ON DELETE CASCADE`, exact legacy recipient/run/task/workspace backfill, shared TaskRead/root ACL, workspace-first seeks, exact timestamp/ID pagination and bounded payload extraction. There is no new migration/version or data conversion.

Replaced SQL:

- Migration `up`/`down` use `SchemaManager` with `Table`, `Index` and `ForeignKey` builders. The acknowledgement copy uses typed entity columns, joins, `MIN`, grouping and `INSERT SELECT`. The partial legacy index keeps its original name, column order and non-null acknowledgement predicate. `use_transaction` and drop order remain unchanged. No raw SQL remains in this migration.
- `task_completion_inbox.rs` uses entities/Column identifiers for the source tables, typed aliases/CTE fields, joins, membership `EXISTS`, terminal conditions, receipt/legacy `COALESCE`, tuple keyset ordering and limits. `FromQueryResult` projections decode the bounded entry/count results. `INSERT SELECT` plus `OnConflict` still performs access validation and receipt insertion in one serialized writer statement. Get/read retain exact run lookup sources. A materialized metadata CTE bounds JSON extraction for both list and exact get.
- The task scope joins/validation in `authorization_scope.rs` now use entity columns and typed CASE/COALESCE/EXISTS/GLOB expressions. CROSS JOIN equalities are WHERE predicates because SeaQuery deliberately discards ON conditions for CROSS JOIN. Exact task scope uses a typed projection. Other authorization repositories/policies are unchanged; inbox rows never call the exact resolver per row.

Generated entity:

`pioneer-entity::task_run_read` contains String fields with `column_type = "Text"`, both primary-key annotations, and the generated `HasOne<task_run>` relationship. `read_at` deliberately stays String: the existing schema declares TEXT and preserves the exact saved timestamp representation. The module, prelude export and inverse `task_run_reads` relation were updated. CRUD uses this entity for joins, writes and receipt lookup coverage.

Generation used the installed SeaORM CLI, `generate entity --entity-format dense --with-serde both --lib --tables task_run,task_run_read`, with a disposable schema-only SQLite input. That input declares the unchanged receipt DDL and a parent task_run PK for relation discovery. Only the new receipt entity and corresponding module/prelude/inverse-relation entries were applied; existing TaskRun fields and other generated entities were preserved. No migration or fixture/data statements ran during generation, and no user database was opened.

Remaining narrow SQL exceptions:

1. SQLite `INDEXED BY` has no node/API in the installed SeaQuery version. Its `index_hints` implementation is MySQL-only, and SQLite's table rendering does not emit it. `workspace_statement` first builds the complete typed query, then annotates only the fixed `task AS t` and `task_run AS r` sources with the existing indexes. Values remain bound. It rejects missing/ambiguous source matches instead of falling back to an unhinted scan. The task -> run CROSS JOIN and both explicit hints preserve the existing empty/small-workspace guarantee. No full query is hidden inside `Expr::cust`; neither rewritten repository path uses that API.
2. The existing query-plan regression prefixes the final driver statement with `EXPLAIN QUERY PLAN` so it inspects the exact hinted SQL. SeaQuery's Explain AST cannot represent the missing SQLite index-hint nodes either. This is test-only and has not been executed in this revision.

SQLite `strftime`, `substr`, `json_extract` and `json_object` use SeaQuery's supported `Func::cust` named-function builder with typed/bound arguments, not raw expressions or queries. Schema construction, ACL, pagination, projections and INSERT/OnConflict need no raw exception.

Regression additions (written only): a typed source with missing/ambiguous index targets fails closed; the new receipt entity supports the composite PK, repeated personal read and run-FK cascade without deleting another run's receipt. Existing migrated-file ACL, legacy backfill, bounded projection, workspace-isolation and page/count plan suites remain in place. Reader handles stay physically query-only and all writes inherit their scoped serialized writer. The large-history cost within the active workspace and refresh cost described below remain unchanged.

Source contract was regenerated from the mobile worktree using `PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/task-completion-inbox bun run client:contract`; `check-source` returned `Client source contract: matched`. The hashes/file are unchanged because CRUD/entity/migration are outside the client-FFI source dependency closure. No RPC/UI or mobile schema/TypeScript regeneration was needed.

Current verification completed without executing tests:

- `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo check --locked -p pioneer-crud --features test-support --tests` completed successfully. It compiled the changed entity, migration, production queries and regression code. Only existing warnings in artifact helpers and compaction preparation tests remain. Gateway/full-workspace compilation was not repeated in this pass; the public CRUD result/argument shapes are unchanged.
- `rustfmt --edition 2024 --check` on every changed Rust file and `git diff --check` in both worktrees. The new receipt entity matches the dense CLI output, and its generated registration/inverse relation entries were inspected against that output.
- Complete final diff read against the pre-change implementation, including the new entity and regression bodies. Static review checked each root/actor/admission predicate, personal/legacy receipts, all terminal statuses, exact cursor comparison, CTE limits, CREATE/DROP and FK/PK/index semantics, serialized writer routing and read-only reader scope. No UI/RPC/contract input or unrelated repository source changed.
- Final `check-source` with the explicit Rust worktree returned `Client source contract: matched`; the mobile worktree has no diff.

No tests, query plans, fixture behavior or migration logic have been executed in this revision. The CLI only introspected a disposable schema created for entity generation. Runtime equivalence, physical plans and migration execution await external review/acceptance and subsequent authorized execution. No migration was applied to a user database. This storage pass did not merge or push; the subsequent user-authorized main synchronization is recorded above.

## Corrections after external review (earlier revision)

Only the two requested review issues are changed in this pass. Public DTOs, RPC/event wire shapes, generated schemas/types, mobile, desktop UI, authorization policy, delivery routes and read semantics are unchanged.

### Revalidate the loaded history range

Each refresh captures the previous continuation frontier. The existing worker replays `task/inbox/list` from the head through that exact `(saved timestamp, run ID)` cursor, following the newly returned cursors. If all history was already loaded it refreshes to the end. Initial load and LoadMore remain single-page operations. The client compares the existing Gateway JSON cursor's exact raw timestamp/ID, rather than reconstructing a cursor from public epoch seconds.

The complete authoritative prefix replaces the publication atomically. It therefore includes new completions, current read state, changed rows and the latest total unread count; rows missing from revalidation are removed. A deleted or nonterminal frontier row does not prevent completion: the cursor remains a valid boundary without that row. Refresh may fetch one partial extra page beyond the old boundary; its final returned cursor becomes the LoadMore continuation, so no part of that page is skipped. IDs are merged uniquely and equal cards retain their item revisions. The existing scroll handle and run-ID selection are not reset; an unchanged history stays at the same row positions.

Identity, connection, workspace/request and authorization fences are checked before every page and again by the existing completion path. Access invalidation/closing the binding still evicts protected data and rejects staged stale pages. Intermediate pages never publish a mix with unrevalidated old cards. The existing failure path clears the retained publication; coalesced invalidations request another authoritative refresh. No separate history store, polling or per-card RPC is added.

Refresh work grows with the loaded range (including new records), uses the existing 30-row list requests, each with its existing eight-second end-to-end RPC timeout. A failed/timed-out replay clears entries and exposes Retry rather than leaving unverified cards. Each page uses the existing short server snapshot; there is no long transaction spanning network calls. Concurrent changes use the existing coalesced follow-up invalidation.

Prepared client regressions cover two loaded pages plus an own read/invalidation, remote read/title changes, revoked and nonterminal rows including a removed frontier, more than a page of new completions, continued pagination without gaps/duplicates, full-history refresh, exact fractional cursor keys and fencing/nonadvancing cursors between pages.

### Seek the requested workspace before its runs

Both page and count force the existing workspace-prefix task index before task-local run seeks. An empty workspace yields no task rows and thus no run lookups; a workspace with a small history visits only its own tasks/runs, regardless of the volume/newness of foreign completions. There is no forced traversal of the global completion index. Scoped/absolute TaskRead, root/lineage ACL, terminal predicates and personal/legacy receipts remain the same; exact get/read retain their run lookup path.

This trades the previous globally ordered walk for sorting only the requested workspace's eligible metadata. The existing `(task_id,id)` index does not provide completion-time ordering; large histories within the active workspace can therefore still cost more per page/count. No additional index is justified by the two reported cases. The unused global terminal index was removed from the still-unreleased inbox migration (both CREATE and DROP); read-table creation, legacy acknowledgement backfill and the legacy read index are unchanged. No migration was applied.

Private statement builders let the existing CRUD unit suite inspect the exact production page (head and continuation) and count plans. The prepared regression requires indexed `SEARCH t` by workspace followed by indexed `SEARCH r` by task and forbids global/base-table scans, while allowing a workspace metadata sort. An integration regression adds 4,096 newer foreign completions, checks an empty requested workspace and then a five-run local workspace, and checks authorized continuation after ACL revocation/read/resume. These tests are written only; neither EXPLAIN nor their fixtures have been executed.

Correction-pass verification without executing tests:

- `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo check --locked -p pioneer-client --features test-support --tests` completed successfully, compiling the new client regressions without executing them.
- `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo check --locked -p pioneer-crud --features test-support --tests` completed successfully, including the production-query plan unit regression, inbox integration regressions and pending migration. Existing warnings in artifact helpers and compaction preparation tests remain outside the changed inbox code. No test body or migration SQL was executed by these checks.
- `rustfmt --edition 2024 --check` on the four Rust files changed in this pass; `git diff --check` in both worktrees; static JSON/TOML parsing of all changed documents (26 Rust-repository documents and 11 mobile documents).
- Complete final diff review, including existing tests, migration and generated files. Comparison with the pre-correction diff snapshots confirms exactly five changed files in this pass: client inbox, CRUD repository, CRUD inbox integration suite, pending inbox migration and this report. The mobile diff is byte-identical to its pre-correction snapshot; public contracts and generated files were not changed or regenerated in this pass.
- Initial feature-free `cargo check --locked -p pioneer-client -p pioneer-crud --tests` encounters a baseline client composer test calling `ClientMutationAuthority::for_test()`, which is gated behind the existing `test-support` feature. Standalone feature-free CRUD `--tests` also encounters baseline compaction integration tests calling `compaction_finish_frozen_history`, gated behind `test-support` for a dependency library. Both call sites/gates are present in the base SHA; those unrelated sources were not edited. Reviewer commands below enable the existing feature explicitly.

The earlier verification record below describes the initial implementation, before these corrections.

## Source contract correction after repeated external review

This final correction changes only the mobile `modules/pioneer-client-nitro/boundary-source.json` and this report. The reviewed Refresh/SQL implementation and all generator inputs remain unchanged.

Before generation, the existing `client:contract` package script and `boundary-integrity.mjs` were read in full. Source mode reads the selected local Rust dependency sources using offline, locked `cargo metadata` and hashes mobile boundary inputs; it does not compile or execute tests. With `PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/task-completion-inbox`, the initial `check-source` reproduced `Client source contract is stale`.

From `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-completion-inbox-app`, completed successfully:

```sh
PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/task-completion-inbox bun run client:contract
PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/task-completion-inbox node modules/pioneer-client-nitro/scripts/boundary-integrity.mjs check-source
```

The generated contract matches this exact worktree pair: `rust_source_sha256` is `840f978697edeb723f3ce72b43bcf19efd0eccbc24fa1bd4ba3f9421a6432bcf`; `mobile_boundary_sha256` is `807ec83e55135c97d9f92219e83d748a2c18269725b0ca8466f9e2e76e87ace3`. Only those two hashes changed. Boundary/schema versions, Rust package selection, platform paths and verification mechanisms remain unchanged.

Post-generation static review includes both complete repository diffs and comparison against snapshots taken before this correction, plus `git diff --check` in both worktrees. The final `check-source` was repeated after all edits and returned `Client source contract: matched`. No generator input files were edited. No tests, typecheck, heavy compilation or migrations were run in this pass; previous compilation feature requirements, baseline TypeScript diagnostics and performance/runtime verification limitations documented elsewhere in this report still apply. Source-contract matching does not constitute runtime testing or external acceptance.

## CI fixture follow-up

The Linux [CI run for PR #107](https://github.com/pioneerdotai/pioneer/actions/runs/38905121168/job/116986642517) stopped in all nine CRUD inbox integration cases at the common fixture setup: its short workspace ID violated `ck_workspace_membership_ids`. No inbox assertions ran in those cases. The same remote run completed the client refresh regressions and the CRUD page/count query-plan regression successfully.

The CRUD fixture now uses 21-character alphanumeric workspace/thread IDs throughout its related rows, scope lookups and assertions. Static schema review also found a nonexistent `workspace_id` column in the `thread_membership` inserts in both the CRUD fixture and the Gateway inbox fixture; those inserts now use the actual membership columns. Production queries, authorization, migrations and delivery behavior are unchanged. The existing migrated CHECK constraints remain intact.

Local follow-up checks are limited to Rust formatting, diff review/`git diff --check` and source-contract matching. Neither local `cargo check` nor local tests or SQL fixture execution were run for this fix, as requested. The updated cases still need the next remote CI run.

The subsequent [Linux CI run](https://github.com/pioneerdotai/pioneer/actions/runs/38915036253/job/117020205443) passed all nine CRUD inbox integration cases and the Gateway inbox RPC scenario. Its only Gateway failure was the authorization registry's fixed expected method count: 153 predates the three new inbox RPCs, while the actual count is 156. The registry/protocol set equality and duplicate checks already passed. The expected count is now 156; all coverage assertions remain in place. This follow-up changes no production behavior or contract inputs. Local verification is limited to formatting and `git diff --check`; no local `cargo check` or tests were run.

## Reviewer-only test commands

Run only after full external review and explicit acceptance by the originating-thread reviewer:

```sh
cd /Users/alexander/Code/pioneer/pioneer/.worktrees/task-completion-inbox
cargo test --locked -p pioneer-crud --features test-support --test task_completion_inbox
cargo test --locked -p pioneer-crud --features test-support --lib repositories::task_completion_inbox::tests
cargo test --locked -p pioneer-gateway --lib task_completion_inbox
cargo test --locked -p pioneer-gateway --lib authorization::resolver::tests
cargo test --locked -p pioneer-client --features test-support --lib tasks::notifications::tests
cargo test --locked -p pioneer-client --features schema,test-support --lib
cargo test --locked -p pioneer-desktop-task-notifications
cargo test --locked -p pioneer-client-ffi
```

External review should also exercise the real desktop popup, keyboard/focus, long translations/zoom, two devices, disconnected completion, access revocation, workspace switching and reconnect. No UI smoke or e2e was executed during implementation.

## Initial implementation verification record

Completed without executing tests:

- `git diff --check` in both worktrees; rustfmt on changed Rust files; static JSON/TOML parsing; lexical migration SQL and shared scope predicate review. No migration SQL was executed.
- `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo check --locked -p pioneer-desktop` completed successfully, including the desktop shell and reused existing local timestamp presentation. The existing `block v0.1.6` future-compatibility warning remains.
- Protocol schemas: `cargo run --locked -p pioneer-protocol --bin schema -- schemas`.
- Client schemas: `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo run --locked -p pioneer-client --features schema --bin schema -- schemas/client`.
- FFI schemas: `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo run --locked -p pioneer-client-ffi --features schema --bin schema -- schemas/client`.
- Mobile consumer workflow: `PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/task-completion-inbox CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 bun ./scripts/export-client-schema.ts`, then the same environment with `bun ./scripts/generate-client-types.ts`. Both scripts were read before execution; neither runs tests.
- Changed mobile schema documents match the Rust client/FFI generator output. The six new protocol/client DTO schemas also agree. Generated TypeScript was inspected with its schemas. `bun.lock` is unchanged in the app worktree.
- Mobile `./node_modules/.bin/tsc --noEmit` reports exactly the same four diagnostics as the baseline captured before changes: TS2448/TS2454 for `visibleThreadId` at `src/screens/thread/index.tsx:212:34` and `:213:29`. The baseline and final logs are byte-identical; that unrelated screen was not edited.
- Full final source/generated diff reviewed statically against the requested invariants, including authorization eviction, duplicate hints, root navigation, workspace switching and reconnection. Runtime/UI, migration and test execution remain for external review after acceptance.

Completed successfully: `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo check --locked -p pioneer-client -p pioneer-client-ffi -p pioneer-crud -p pioneer-gateway -p pioneer-protocol -p pioneer-desktop-task-notifications --tests`. This compiled the test code without executing any tests. Existing warnings remain in artifact helpers, a Gateway capsule test helper, CRUD compaction preparation tests, and `block v0.1.6`; none are in the new inbox code.

Тесты не запускались. Требуется повторное внешнее ревью.
Миграции к пользовательской базе не применялись.
