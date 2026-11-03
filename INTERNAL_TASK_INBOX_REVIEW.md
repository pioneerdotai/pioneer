# Internal task completion inbox — external review handoff

## Checkout

- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/internal-task-inbox`
- Branch: `fix/internal-task-inbox`
- Local main base and current HEAD: `cfb509afd24da8bfca3254cd8a4e351baba22626`
- Uncommitted review diff. No fetch/pull, merge/rebase, commit, push, deployment, mobile worktree or changes to the primary checkout.

## Cause and durable chain

Agent creation enters `crates/gateway/src/task_tools/mod.rs:3096` (`create_params`); `current_parent_task_id` at 4904 obtains lineage from the executor binding. Immediate attached defaults (`crates/tasks/src/policy.rs:143,226`) use delivery none. Attachment is mutable and is not a notification classification.

Creation materializes the exact authenticated creator (`crates/tasks/src/actor_contract.rs:17,144`) and occurrence-specific normalized delivery (`actor_contract.rs:201,230`), through `crates/tasks/src/service.rs:2330` and scheduler (`crates/tasks/src/scheduler.rs:952,1125`). Actor persistence checks immutable replay (`crates/crud/src/repositories/task_actor_contract.rs:18,67`); occurrence update rejects changed delivery plan (`task_actor_contract.rs:859`). Protocol plan routingVersion=1 and presentation destination validation live in `crates/protocol/src/task_actor.rs:539`. Retry clones that plan (`task_actor.rs:652`).

Terminal run -> completion inbox `relation_with_access` (`crates/crud/src/repositories/task_completion_inbox.rs:538`) -> list/count/exact/receipt queries -> scoped push. Previously this relation required terminal status, a presentation root, workspace and ACL but did not distinguish user results from internal results. `task_scope_query` (`crates/crud/src/repositories/authorization_scope.rs:624`) correctly permits reading internal work through the authorized root; permission to read is not an intent to notify.

## P1 retry chain corrected

External review identified an error in the initial implementation and its stated legacy limitation: direct run_id alone is insufficient after normal retry. `crates/crud/src/task_terminal.rs:1345` creates retries in the same group with incremented attempt and both source links. `crates/tasks/src/scheduler.rs:1145,1177,1180` reads the previous occurrence, clones it through retry(), changes run_id, and dispatches it. `crates/protocol/src/task_actor.rs:652` preserves occurrence_id and delivery plan. `crates/crud/src/lib.rs:18510,18612` claims and upserts the same occurrence in a transaction; `crates/crud/src/repositories/task_actor_contract.rs:123,180,848` persists by occurrence_id and rejects changed immutable plan/key/generation. Inbox now follows that logical identity with bounded SQL lookups. No scheduler/retry lifecycle code changed.

## Predicate

`internal_completion` (`task_completion_inbox.rs:413`) is a correlated EXISTS, negated in the shared relation. It requires:

1. Actor task_id and workspace_id match the current Task; creator JSON identifies agent_execution with a valid 21-character ASCII alphanumeric exact ID.
2. Occurrence task_id matches the Task. `completion_occurrence_id` (task_completion_inbox.rs:317) first resolves an exact UNIQUE(run_id) match scoped to Task. If absent, it resolves the first attempt of the candidate's (task_id, run_group_id) with attempt_number=1 and no retry_of_run_id; that run's ID is the immutable occurrence_id. The occurrence must still bind a later attempt in the same Task/group, without comparing occurrence retry generation to TaskRun attempt_number. Both candidate (when attempt>1) and bound retry must point retry_of_run_id and parent_run_id to an existing same-Task/group predecessor with attempt_number exactly one lower and a smaller run_number. A first attempt's parent_run_id may legitimately point to another recurrence, so it is not constrained. The run itself is already joined to its Task. Occurrence has no workspace column: its workspace derives from that validated Task.
3. Recognized saved delivery plan: routingVersion integer 1; valid policy includeResult boolean, format, optional fields and known semantic thread target; no unknown top-level plan fields.
4. Either mode none with absent/null presentationThreadId, or mode thread whose policy.threadId equals presentationThreadId and resolves by PK to a same-workspace thread with server access_class internal.

Malformed JSON is guarded inside CASE before every JSON extraction, independent of WHERE evaluation order. NULL/unknown conditions cannot make EXISTS succeed. The current Task delivery policy, attachment, owner/executor kind, labels and ancestry do not classify output.

| Creator and saved occurrence plan | Inbox eligibility (existing ACL/boundaries still apply) |
| --- | --- |
| Agent + none | Excluded |
| Agent + thread to same-workspace internal target, matching presentation | Excluded |
| Agent + user-facing private/workspace thread | Retained |
| Agent + user_notification | Retained |
| Agent + webhook | Retained |
| Human/system + any mode | Retained |
| Missing contract/plan, malformed JSON, unknown version/format/types, mismatched task/workspace/target | Retained |

All five terminal statuses use the same rule. Existing receipts/results/history are not deleted or rewritten. Older internal runs disappear when sufficient durable evidence exists. Legacy runs lacking it may remain noisy; there is no backfill or inference from current policy. The query validates the fields needed for classification, not unrelated reviewer/launch/actor snapshot JSON. Internal thread classification is read from current server thread metadata, not sidebar visibility. Lost/deleted targets cannot prove internal delivery. Normal retry rebinding retains classification of earlier attempts through the proven logical occurrence. Missing origin/source/bound rows, a noncanonical legacy occurrence ID without a direct match, or inconsistent group/attempt/Task links cannot prove this fallback and retain legacy eligibility. No arbitrary contract of the Task is used.

## Query and publication paths

All use the same relation:

- list/keyset: `task_completion_inbox.rs:754`, page predicate before LIMIT, payload projection after limited metadata CTE;
- full unread count: 777;
- addressed unread entries: 791;
- exact history get: 814;
- read-all: 832, source at 841;
- viewed proofs: 1050, including run, turn and delivery evidence;
- manual read: 1333.

`CrudStore::task_completion_inbox_patch` (`crates/crud/src/lib.rs:23848`) reads total and addressed membership in one existing reader snapshot. `crates/gateway/src/message/tasks.rs:600` supplies dirty hints, not membership decisions. `scoped_push.rs:963,985,1010,1026` uses the same repository for addressed refresh and bootstrap/recovery, derives removes from absent IDs, and excludes these runs from upserts. No new live-client upgrade protocol is introduced. A separate existing cancellation-suppression receipt path is deliberately unchanged; it is not inbox admission or task-read authorization.

## Cost and invariants

Before editing, the selected approach was SQL filtering before pagination with bounded indexed existence checks. After editing, statically: each candidate adds an actor PK(task_id) probe and a UNIQUE occurrence(run_id) probe to obtain the occurrence ID, followed by the outer occurrence PK(occurrence_id) probe. COALESCE uses the bounded retry fallback only when no direct ID exists. Fallback reads the candidate by run PK, origin through `uidx_task_run_group_attempt(task_id, run_group_id, attempt_number)` (schema: crates/migration/src/m20260313_125253_create_workspace_table.rs:408), occurrence by PK(origin.id), bound run by PK(occurrence.run_id), and immediate predecessor by PK for candidate (if retry) and bound. CROSS JOIN fences this candidate/origin/occurrence/bound order. Scalar results are unique; no LIMIT or DISTINCT masks ambiguous cardinality. Outer `r` still contributes only its existing covered ID, not new group/attempt columns. Thread delivery can add thread PK(id) lookup. PK/UNIQUE are real constraints even with FK OFF (`crates/migration/src/m20260820_000001_agent_domain_foundation.rs:1216`). EXISTS cannot multiply outer rows and needs no DISTINCT. Existing task workspace index hints, run epoch ranges, sequence boundaries, ACL joins and result metadata CTE remain unchanged.

JSON validity/extraction/type checks and json_remove have nonzero CPU cost; safe CASE expressions may be evaluated repeatedly by SQLite. No measured speed or zero-cost claim is made. No result payload is loaded for classification; no Rust history collection, cache, per-row RPC/SQL round trip, tree traversal, fanout receipt write, new index/table/schema or counter. Push retains its existing bounded batch and snapshot logic. The scoped SqliteDatabase/CrudStore and serialized writer routes are unchanged. New expressions use SeaQuery within the existing repository; no raw SQL adapter was added to production.

Ordinary task read/review/accept/revise/wait access is untouched. Composer creates a detached Task with thread delivery to the user's thread (`crates/gateway/src/message/turn_handlers.rs:1827,1833`), which remains eligible. Background/scheduled user destinations likewise remain eligible, independently of trigger or parent relationships.

Thread unread is separate: completed agent_message assistant blocks count (`crates/crud/src/repositories/thread_timeline_projection.rs:446`); final messages become top-level assistant blocks (`crates/crud/src/timeline_projection.rs:368`); delivery intentionally creates AgentMessage (`crates/gateway/src/message/task_delivery.rs:1579`). Task cards alone do not satisfy this predicate. No separate thread-unread defect was established. No cursor, folder, message classification or delivery changes.

## Regressions authored, not executed

Existing owner: `crates/crud/tests/task_completion_inbox.rs`. Runtime-style FK OFF file-backed fixture with query-only readers and writer executor retained. The original two repository regressions (task_completion_inbox.rs:2769,2910) cover:

- exact typed creator and normalized plan JSON; agent none/internal and detached cases across succeeded/failed/blocked/cancelled/timed_out;
- retained user-thread output (Composer/background/scheduled destination shape), notifications, webhook, human/system none and internal delivery;
- current policy update does not alter historical classification; several runs of one Task carry different plans;
- keyset pages filled before LIMIT; list/count/addressed patch/exact/read/viewed/read-all agree; no hidden-run receipts and personal unread isolation;
- missing actor, NULL/malformed/incomplete plan, unknown version/format, wrong boolean, mismatched presentation, invalid creator, foreign actor workspace/occurrence task/target workspace.

The P1 retry regression (`crates/crud/tests/task_completion_inbox.rs:3016`) adds both none/internal-thread cases with one logical occurrence rebinding to second and third attempts. Storage fixtures reproduce queued retry rows with the same group, incremented attempt/run number and both predecessor links; `TaskOccurrenceContract::retry` and `CrudStore::upsert_task_occurrence_contract` perform the actual saved contract transition, followed by failure. There is no contract row per attempt. A different user-delivered occurrence of the same Task stays visible despite its parent_run_id referring to the internal group. Current Task policy is changed to user delivery. At every stage list (LIMIT 1), count, addressed patch, exact get, manual read and read-all remain consistent; only the visible occurrence receives a receipt. Corruption controls cover bound group/Task, occurrence ID/Task, predecessor links, attempt numbering and missing durable occurrence. These controls are read through the real repository, with damage restored between assertions. The test is expected to fail the pre-P1 implementation immediately after the first rebinding; that expectation has not been executed.

Fixtures seed durable rows directly to exercise legacy/corrupt FK-OFF data and invoke actual repository/CrudStore paths; they do not claim to exercise full Composer/scheduler admission. Existing suite retains independent ACL revocation, activation/grant sequence, legacy missing contracts and read persistence coverage. No duplicate Gateway/UI test layer or new harness was added. Before the fix these new regressions are expected to fail because internal rows are admitted; that expectation was not executed.

## Self-review and command record

1. Before edit: checked product cases, NULL/unknown semantics, binding integrity, immutable run policy, cost.
2. After wiring: traced all relation callers and scoped push snapshot/remove behavior.
3. Final: read full diff against base, including regressions; corrected temporary plan lifetime in fixture, strengthened unknown-format/version/type examples and mismatched-target negative control; re-read changed sections. Formatting and whitespace diff checks completed. This is self-review, not external acceptance.

Commands actually used: `pwd`; `rg --files`, `rg -n`; `cat`, `sed`, `head` for source/instruction inspection; `git status --short`, `git worktree list`, `git show-ref --heads`; `git worktree add -b fix/internal-task-inbox /Users/alexander/Code/pioneer/pioneer/.worktrees/internal-task-inbox main`; `git rev-parse HEAD`; `git diff --stat`, full `git diff cfb509afd24da8bfca3254cd8a4e351baba22626 -- ...`; `git diff --check`; Python heredocs only to edit source/test text; `cargo fmt -p pioneer-crud` (three runs before this report). Some exploratory globs had no matches; no fallback executed project code. Report written with a shell heredoc. Subsequent final static reads/status/diff-check are included in the handoff. No tests, EXPLAIN, benchmarks, application, database inspection, migrations, schema generation, compilation or typechecking executed.

Test-audit authoring guidance was read for regression ownership and meaningful failure cases. Its production-read-only and validation-execution restrictions conflict with the explicit implementation task; the user's scope and no-execution instructions take precedence. Provided Tasks/Subagents/Threads/HTML skills require no task/thread mutation or HTML artifact for this source change; no child agents were created.

## P1 pass: self-review and commands

Read applicable root/CRUD AGENTS.md, current HEAD/status/stat and existing diff, actual retry creation, scheduler dispatch, repository claim/upsert, protocol retry and schema indexes. Intermediate review covered identity binding, group isolation, FK-OFF integrity, uniqueness and bounded lookup cost. Final review reread the full cumulative production diff, both original regressions, the new retry regression and this report against the recorded base. Corrected the fixture's initial occurrence status and queued-to-failed attempt representation during static review. All prior actor/JSON/ACL/read-time filters and shared callers remain unchanged. Self-review does not constitute external acceptance.

Commands in this pass: `cat`, `sed`, `head`, `rg --files`, `rg -n`; `git rev-parse HEAD`, `git branch --show-current`, `git status --short`, `git diff --stat`, cumulative `git diff cfb509afd24da8bfca3254cd8a4e351baba22626 -- ...`, `git diff --check`; Python heredocs strictly for editing files; `cargo fmt -p pioneer-crud` (two runs). No tests, compilation, typecheck, SQL/EXPLAIN experiments, migrations, application or measurement were run. The existing uncommitted work was preserved; only the same two Rust files and this report are changed.

## P1 same-run recovery correction

External review found that the prior fallback incorrectly equated occurrence.retry_attempt with TaskRun.attempt_number. The equality is removed locally in completion_occurrence_id; it is not replaced with a numeric inequality. The durable identity proof remains origin ID = occurrence_id plus same Task/group and validated immediate retry source links. The occurrence counter is an execution/recovery generation, not identity evidence.

Producer audit:

- First-run builders initialize TaskRun.attempt_number=1 (service.rs:2244, scheduler.rs:894,1056) and occurrence.retry_attempt=0 (tasks/src/actor_contract.rs:226).
- Automatic retry creation increments TaskRun.attempt_number and preserves group/source links (crud/src/task_terminal.rs:1343,1352). Scheduler clones the original logical contract via retry(run.attempt_number), then rebinds run_id (tasks/src/scheduler.rs:1177,1180).
- `CrudStore::resume_task_owned_turn` (crud/src/lib.rs:30424) instead increments occurrence.retry_attempt with checked_add(1), calls retry() and sets Recovering (30582–30594). It reopens the SAME blocked TaskRun to Running without changing attempt_number (30796–30835), then applies the prepared occurrence transition (30932).
- prepare_task_occurrence_retry (crud/src/repositories/task_actor_contract.rs:752–766) validates immutable facts and explicitly rejects a changed run_id. apply_prepared_task_occurrence_retry (793–837) fences occurrence/task/run/key/status/generation and updates generation/status, not identity or plan. Protocol retry() clones the plan and ID (protocol/src/task_actor.rs:652); next_generation() creates a distinct recurrence identity and resets the counter (667).

Thus equality is not an invariant across normal producers. No execution, scheduler, resume or recovery code is changed. Existing Task/group/attempt relationships still prove which immutable plan belongs to a previous completed run; they survive blocked -> resumed -> completed lifecycle changes. Malformed/missing/unknown facts and wrong bindings still fail to prove internal output. The existing invalid attempt_number controls continue to reject broken immediate predecessor relationships independently of the removed equality.

Regression `same_run_recovery_preserves_internal_retry_history_membership` (crud/tests/task_completion_inbox.rs:3225) covers none and internal-thread plans. First attempt fails; the same occurrence rebinds to blocked attempt 2. At three observation points (blocked, resumed/running, succeeded) it verifies the previous run stays excluded, current run ID/attempt_number remain fixed, occurrence retry generation changes 2 -> 3, plan stays frozen and only a separate user-delivered occurrence of the same Task appears in list/count/addressed patch. Manual reads/read-all never create receipts for internal attempts. One internal logical occurrence row is used throughout.

The test reproduces the persisted recovery transition with public protocol retry() and CrudStore upsert, including Recovering and same run ID, and storage fixtures for run lifecycle fields. It does not invoke the full turn/execution resume aggregate or directly call the private prepare/apply helpers; no public production API or test seam was added. Existing three-attempt/corruption/legacy regressions are preserved.

Static cost: only a scalar equality predicate was removed. Existing PK/UNIQUE probes, CROSS JOIN ordering, bounded scalar cardinality, before-LIMIT filtering and payload loading remain as described above. No additional query, row fanout, history walk, cache, schema or receipt writes were introduced. No measured latency/CPU/memory or executed query-plan claim is made.

This pass: read root/scoped instructions, HEAD/branch/status, producer and recovery code, full cumulative production diff and all four regressions, plus this report. Intermediate self-review separated identity from execution generation and checked membership/cost. Final static review corrected the modeled reopened run state to Running (matching resume_task_owned_turn). Self-review is not external acceptance.

Commands used in this pass: cat, sed, rg (including --files), git rev-parse HEAD, git branch --show-current, git status --short, git diff --stat/--numstat and full cumulative diff against cfb509afd24da8bfca3254cd8a4e351baba22626, git diff --check; Python heredocs and shell heredoc for source/report edits; cargo fmt -p pioneer-crud (two runs). No tests/compilation/typecheck, SQL experiments/EXPLAIN, migrations, generators, application or benchmarks. Remaining unverified properties and external acceptance gate below still apply.

## Remaining unverified and future commands

Compilation, SQL rendering/execution, query plans, assertions, runtime push/reconnect and performance remain unverified. External review requested P1 retry correction; the revised cumulative implementation has not received repeat full external acceptance.

Only after complete external review and explicit acceptance, run focused checks per AGENTS.md:

```sh
cargo nextest run --locked -p pioneer-crud --test task_completion_inbox --test-threads 2 --no-fail-fast --retries 0
cargo nextest run --locked -p pioneer-crud --lib --test-threads 2 --no-fail-fast --retries 0 task_completion_inbox
```

The second command includes the existing real-statement EXPLAIN regression. Broader relevant workspace tests required by root AGENTS.md are a later validation step, after the same acceptance gate; these commands are documentation only, not executed.
