# CLI task orchestration parity — external review notes

Original base: `41d777637f187bd076df1866d445bd84d0b3c756`.
Revision starting HEAD: `5a049097cda491aac6a1f3afdf8621f700c230ff`.
Previous code: `0e1749f6037fa2b8d0c6f845c47368247dd493c2`.
Corrected implementation head: `3bcdf61e9160bf01e1a6e893ee3dd4464248940a`.
Branch: `fix/cli-task-orchestration-parity`.
Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/cli-task-orchestration-parity`.

The implementation head identifies the accepted code/regressions before
publication. The following documentation commit was
`f54844a175463efd504cd22277889b5701de8260`. After acceptance, publication consolidates
these local commits into one `fix(tasks)` commit with identical Rust sources.
The published HEAD is reported with the pull request. The existing untracked
`implementation-notes/cli-task-orchestration-parity-audit.md` belongs to the audit;
it is preserved and excluded from these commits.

The new revision addresses only the two findings in
`/tmp/pioneer-cli-task-parity-audit-20261020/external-review-5a049097.md`, against
`implementation-prompt-v2.md` in the same directory. They are source findings,
not newly reproduced production incidents. F3 is retained without code changes.
Tests remain forbidden until an external reviewer accepts this corrected code.

## Boundary matrix

| Boundary | Evidence | Loss/restart behavior |
| --- | --- | --- |
| Native input → invocation | admitted native dispatch, or frozen CLI first-party catalog and validated invocation/native correlation | foreign names, arguments and arbitrary text are insufficient |
| Invocation → backend commit | TaskService mutation and existing idempotent receipt | proves commit, not delivery/review |
| Backend → model output | successful ExecutedToolResult for API; received correlated native ItemCompleted with successful stored lifecycle for CLI | failed/cancelled/lost response remains unobserved despite commit |
| Output → durable knowledge | existing completed item `taskResult` metadata and semantic `task.terminal.observed` markers | replay restores the particular outcome without another mutation |
| Knowledge → current gate | per-child task revision, or exact immutable accepted candidate of current completed run | resume invalidates old outcome even in the same run; B changes do not invalidate unchanged A |
| Receipt → marker → provider start | persisted input plus its exact bounded terminal outcome keys; common pre-start marker path | receipt alone is not observation; marker does not discharge owed input; either crash window replays both |
| Final → extracted result | canonical committed finalization item owned by the completed turn/thread/workspace | commentary/demoted finals/prepared intent cannot supply results; unique legacy FinalAnswer only |

The pre-change audit recorded before editing is retained below. No Task mutation,
ACK transaction, finalization transaction or scheduling class has changed.

## F1/F2 — observe a particular current terminal outcome

`crates/agent/src/task_observation.rs:9` defines the small internal
`ObservedTaskOutcome` value. The shared parser at line 54 reads current public
DTOs: wait's completed/failed/blocked/cancelled arrays → `item.task.id/revision/status`;
get/cancel → `task.id/revision/status`; accept requires `taskTerminal=true`, `status=completed`, and
uses its public `taskId/runId/candidateId`. Public task schemas are unchanged.

Task revision is existing durable child-local evidence. Both
`TaskService::resume_task` and `CrudStore::resume_task_owned_turn` increment it;
the latter can reopen the same run. Terminal result/error projection increments
it again. Thus task/run/status identity alone cannot cover another outcome.
Changing another child's revision or heartbeat cannot invalidate A's key.
Revision-based observations conservatively require renewed observation when
A's own revision changes.

Accept has no public task revision. Its immutable candidate/run identity is
compared only against the accepted candidate of the current **completed** run.
`GatewayTaskToolProvider::attached_task_finalization_snapshot`
(`crates/gateway/src/task_tools/mod.rs:313`) uses the existing narrow
`get_accepted_task_result_candidate` reader, verifies task/run ownership and
includes it in the existing before/after aggregate revision fence. It does not
infer delivery from the lookup: it only tests a key from an already delivered
successful completed accept. A blocked accept response is not retained as a dormant
candidate key that could match a later completion after resume. Wrong candidates,
other runs/tasks and blocked snapshots do not match. Both resume paths reject
completed tasks; a blocked accept without public task revision conservatively
requires another observation. `finalAnswerAllowed` does not clear any other child.

The CLI observer (`task_tools/mod.rs:4711`) reads successful completed dynamic
items of this logical turn, with stored lifecycle, exact Tasks binding,
canonical/raw/server identities, provider-call identity, session generation and
invocation correlation. Earlier attempts remain eligible, but their keys must
match the current child's outcome. Failed items, mutation receipts, backend audit
success, arguments and references-only `alreadyCommitted` results cannot count.

The durable source remains `taskResult` metadata normalized from actual native
output before raw metadata redaction. The earlier normalization code is unchanged:
Codex/OpenCode use `cli_runtime/projector.rs:948`; Claude's earlier typed conversion
uses `cli_runtime/claude_session.rs:3242`. Their regression expectations now retain
candidate identity. Complete canonical structuredContent or actual MCP text content
is interpreted; display summaries and arbitrary strings are never scanned.

Provider boundary inspection: Codex frozen binding → callable/argument correlation
ledger → validated facade invocation → received MCP envelope → projector → stored
ItemCompleted; Claude exact tool_use binding/ledger → matching tool_result →
normalization before typed redaction → durable item; OpenCode activated native
scope/frozen namespace → native correlation → received V2 completed content →
projector → durable item. No adapter lifecycle/correlation rules changed here.

New semantic system markers carry `details.outcomes`. Legacy ID-only markers
cannot identify a terminal generation and conservatively require re-observation;
they cannot bypass the new gate. No history is rewritten. The API loop now tracks
the same outcome keys, renders those exact keys in durable markers and compares
fresh snapshots using `was_observed` (`agent/src/chat/mod.rs:3233,3407,4494,4954,5366`).
The alternative terminal-observation provider path also filters by outcome.
Unchanged semantic observations survive CrudStore reconstruction; resumed tasks
cannot inherit them. Pending/review/revise/nonwaitable/timeout-only payloads remain
nonterminal, and mixed waits observe only the terminal items actually returned.

## F3 — retained accepted final source

`CrudStore::completed_turn_final_answer` (`crates/crud/src/lib.rs:19365`) requires
the expected completed turn/thread/workspace and a committed finalization intent
with matching exact item_id/item_json and FinalAnswer phase. Prepared intent does
not fall through to legacy. The existing migration/worker legacy path explicitly
accepts only a unique completed FinalAnswer; ambiguity or absence yields no item.

Both consumers are retained: reviewer advisory (`message/task_agent_executor.rs:10031`)
and task result extraction (line 10436), preserving abstain/missing-result behavior.
Timeline timestamp/ID order, commentary, text length and old demoted partial finals
cannot select a result. This selector does not hide F1's later ACK FinalAnswer.

## F4 — one owed payload and matching recovery observation

`CliTaskFinalizationState::continuation_terminal` / `continuation_outcomes`
(`cli_runtime/handlers.rs:247,260`) select exactly the same bounded batch as the
renderer. The existing continuation receipt stores `terminalObservations` beside
`input` (`handlers.rs:6798`). Preparation replay reads both from the committed
receipt, rather than combining its input with keys from a newer snapshot.

Both actual call sites of `start_prepared_cli_runtime_attempt` were checked.
The task-continuation call supplies the persisted input and `Some(saved outcomes)`;
ordinary recovery supplies `None`. This keeps the continuation distinction and
carries its delivery facts explicitly. The common start path
(`message/turn_handlers.rs:8505,8673`) publishes the exact semantic marker before
provider start for both paths. Publication failure takes the existing attempt-start
failure path; it does not continue with undurable knowledge.

Recovery with a saved receipt appends that one saved block and restores its
semantic marker, including when the original process stopped after receipt commit
but before marker. A preexisting marker never removes saved input. The marker
remains before provider start, so a crash there still owes redelivery.

Recovery without a receipt freezes one fresh block and its semantic keys in the
existing native-event input receipt lane (`turn_handlers.rs:8563`). This small
additional write is necessary: otherwise a fresh recovery could write a marker,
crash before provider start, then omit owed facts from a newly filtered snapshot.
It is not a new table/journal/ACK protocol. Its deterministic attempt-owned receipt
is idempotent. It is created only when there was no prior task input receipt and
has no empty-completion counters; the existing no-progress baseline and budget
are preserved. Only the orchestration block is saved, so generic recovery text
and created-task facts do not accumulate recursively.

Created-task anti-dup facts are still appended separately, including scheduled and
detached work. The fallback terminal renderer remains. No arbitrary prompt parser
was added and no terminal fact is removed merely because a marker exists.
A saved snapshot may be stale: its keys can observe only that historical outcome;
TaskService and the unchanged finalization revision fence decide current state.

All added reads/writes use existing scoped CrudStore/repositories. Snapshot reads
release DB capacity before parsing/rendering/serialization/publication/provider
work. The accepted-candidate read is covered by the existing snapshot seqlock.
Receipt preparation is historical data, not a current-state decision: concurrent
Task changes are allowed, and commit-time finalization revalidates the aggregate.
Existing ACK/continuation atomic write sets are unchanged. Fresh recovery saves
its input before its marker without holding DB capacity between operations.

## Regressions written, not run

- `message/tests/cli_task_tools.rs:645`: three providers × actual task_get/task_wait
  blocked payload, B remains active and changes independently, semantic + legacy
  markers, CrudStore reconstruction, actual same-run `resume_task_owned_turn`,
  second terminal outcome with identical run/status, gate before new delivery,
  failed delivery, successful new delivery, substantive final without extra attempt.
- `agent/src/chat/mod.rs:9314`: native observer with a serialized current PublicTask
  DTO, same run/new revision, failed result and renewed terminal rendering.
- `message/tests/cli_task_tools.rs:996`: three providers × fresh recovery,
  committed receipt before marker, marker before provider start. The pre-marker
  crash fixture persists the real renderer's input/keys through CrudStore; the
  post-marker fixture injects recording-session resume failure. Each reconstructs
  CrudStore, requires one block, retained saved input, restored observations,
  scheduled-task anti-dup facts, substantive FinalAnswer + completed logical turn,
  no ACK attempt, and exact reviewContent at the upper parent.
- `cli_runtime/handlers.rs:14459`: more than 64 terminal facts, subset selection
  and fallback redelivery; marker keys match exactly the rendered bounded input.
- Existing public DTO projection fixtures now assert revision retention through
  metadata redaction. Existing actual accept output fixture verifies current
  candidate matching and rejects other candidates/blocked snapshots/missing proof.
  Adapter fixtures were updated to retain real accept candidate/run fields.

Earlier regressions remain: nested create → full reviewContent → accept → final
for all three providers; terminal wait/get/cancel; failed/foreign/replay outputs;
multiple-child gate; old candidate rejection; lost wait response; no-progress,
duplicate terminal events, concurrent detach, scheduled/background semantics,
commit-time revision fence; committed/legacy final selection. They are unrun.

## Full self-review and limits

The complete revision diff, all changed branches and regression bodies were read.
The broader original F1–F4 owners (snapshot, native correlation/delivery, receipts,
resume, finalization and extraction) were rechecked. Incidental edits in review
rendering found during self-review were removed. No F3, scheduler, heartbeat,
deadline, UI, public schema, permission, exact reviewer authorization, required
skills/order, lazy visibility or frozen catalog change is included.

Evidence is static source reasoning and written regression code. The original
production evidence establishes F1 and duplicate F4 payload only; F2/F3 and the
two new review findings are not claimed as separately reproduced incidents.
No production DB/tasks/subagents, historical messages or live provider were used.
No delegation, merge, rebase, push, deploy, install or restart occurred.

Compatibility is conservative: old redacted output lacking enough semantic facts,
ID-only markers, and old receipts without outcome keys cannot fabricate an
observation. Existing receipt input is still replayed; the gate may require
renewed observation. Ambiguous legacy finals still yield missing-result/abstain.
Saved facts can be stale or redelivered; freshness checks never depend on prompt
text. Malformed receipt input fails recovery rather than discarding owed facts.

## Checks actually performed in this revision

- Read-only source/fixture inspection, both boundary audits, and full revision/
  staged diff self-review, including negative branches and regression bodies.
- Direct stable `rustfmt --edition 2024 --config skip_children=true` on the ten
  affected Rust files, followed by `--check` on the final source: exit 0.
- `git diff --check`, `git diff --cached --check`, and
  `git diff --check 41d777637f187bd076df1866d445bd84d0b3c756..HEAD`: exit 0.
- `cargo check -j 1 -p pioneer-agent -p pioneer-gateway --tests`: exit 0,
  `Finished dev profile` in 35m 49s. Log:
  `/tmp/cli-task-parity-review-fixes-check-review-source.log`. Existing dead-code
  warnings: CRUD library (1), Gateway library (8), Gateway test target (1).
  No test binary was executed.
- The first compilation reported an error converting ExecutionEventHubError to
  anyhow::Error in the new publication helper; it was corrected. An intermediate
  check was superseded after self-review added the completed-status requirement
  for accept. Only verified workers of these own obsolete checks were stopped;
  no result is claimed for interrupted checks. No test runner was involved.
- Only sample Git hooks were present. Local commits use the command-local
  `-c core.hooksPath=/dev/null`; no persistent configuration changes or
  hooks/wrappers that run tests were used.

No tests, doctests, other test runners, application, live CLI providers or manual
runtime substitute were executed. `cargo check --tests` compiles/checks test code;
it is not a passed test suite or runtime confirmation. Tests may run only after
an external reviewer explicitly accepts this corrected implementation.

Тесты не запускались; исправленный код ожидает внешнего ревью.

Contract sources: root `AGENTS.md` (no local instructions under changed crates),
[pioneer-system-tasks SKILL.md](/Users/alexander/.codex/skills/pioneer-system-tasks/SKILL.md),
[pioneer-system-subagents SKILL.md](/Users/alexander/.codex/skills/pioneer-system-subagents/SKILL.md),
and their applicable schema/troubleshooting references. They were applied solely
as tool semantics, without delegation or task creation.

## Revision after external review (pre-change boundary audit)

Starting HEAD: `5a049097cda491aac6a1f3afdf8621f700c230ff`; original base unchanged.
The two review findings are statically confirmed; F3 is retained.

| Boundary | Evidence and freshness | Failure/restart |
| --- | --- | --- |
| terminal tool output → observation | successful correlated durable output; public per-child task revision for wait/get/cancel, immutable accepted candidate + run for accept | no receipt/arguments inference; failed output supplies nothing |
| observation → resume → terminal | snapshot compares the same child's revision (resume increments it even with the same run); accepted candidate is checked against the current completed run's accepted candidate | old ID-only markers lack outcome proof and conservatively require re-observation; B changes do not alter A's revision |
| receipt → marker | receipt stores exact bounded terminal outcome keys beside its input | committed receipt alone does not count as observation; recovery replays input AND restores its semantic marker |
| marker → provider start | existing pre-start observation boundary; saved input remains owed after marker | both pre-marker and post-marker crashes retain facts; fresh recovery renders and marks the identical bounded set |
| provider final → gate | pending/review/unobserved outcomes and existing commit-time aggregate fence | stale saved payload cannot observe a resumed child's new outcome |

No transaction, ACK or scheduling boundary changes are planned. Existing
repositories supply task revisions and accepted candidates. These reads finish
before parsing/rendering/publication/provider calls. The snapshot's existing
before/after aggregate revision fence covers the accepted-candidate lookup;
commit-time finalization revalidation remains unchanged. Semantic receipt fields
are prepared outside DB capacity and travel in the existing atomic write set.

## CI repair — 21 October 2026

Starting published HEAD: `eb2dd66b037b7984d6b1cf51419864a9f72e6f0d`.
Evidence: the three attempts of Actions run `39445886024`, including Ubuntu job
`118991938083`. All three hit the three-hour job limit. Windows/macOS checks
succeeded. This repair changes no workflow, CI steps, dependency versions or
timeouts; it does not skip tests or weaken their assertions.

The lasting hang is the pre-existing
`cancellation_and_repeated_drop_release_only_the_target_stream` test in
`transport/http/streams.rs`. Its `read_u8` future retries `Interrupted` in a loop
inside one poll in locked Tokio 1.54.0, while the cancelled reader always returns
that error. Consequently the future never returns to its executor. The test now
polls `AsyncRead` directly and checks immediate `Interrupted` twice, an empty
buffer, and the original independent-stream/drop accounting. The Tokio test
runtime is retained for the reader's idle timer. Production reader behavior is
unchanged.

Two new CLI regressions also failed before the suite timed out:

- `cli_observation_tracks_the_child_outcome_across_same_run_resume` reached an
  actual terminal-event collision after legal same-run resume. Persisted run,
  turn and task terminal keys now include the existing occurrence retry number
  when it is nonzero. Initial/legacy keys stay unchanged. This permits the new
  projection while retaining deduplication within that retry. The terminal replay
  lookup uses the identical key rule. Deliveries similarly receive a retry prefix,
  and replay examines at most two delivery rows for that exact run/retry, rather
  than confusing an earlier outcome's delivery with the current one.
- `cli_recovery_delivers_exact_task_facts_and_preserves_substantive_final` expected
  a recovery event while advancing ten seconds beyond job creation, under a
  fifteen-second process-owner lease. Fixture setup consumes part of that lease.
  The production coordinator correctly defers a lease-expired active execution.
  The fixture now waits for the terminal consumer/ACK transition, heartbeats its
  real process owner through CrudStore, and runs recovery at current time after
  checking the job is due. The complete payload, substantive final and upper-parent
  result assertions remain in place for all providers and crash windows.

Database boundary: key selection reads the persisted occurrence and, only for
task-level terminal payloads, the latest numbered run. These bounded reads remain
inside the existing serialized append transaction, so preceding events in the
same batch are visible. Delivery preparation reads occurrence state outside the
writer as before; terminal commit still compares the exact occurrence, actor,
policy and execution fence before writing. No transaction/ACK boundary or
scheduling class changed, and no external work was added under DB capacity.
The same-run regression additionally rejects a terminal transition prepared
before resume, verifies rollback leaves the run running, checks that the new
blocked turn is actually projected, and repeats the new terminal outcome without
another task revision change. Existing stale-observation/restart checks remain.

Validation performed: targeted rustfmt and `git diff --check`; static review of
the changed paths, terminal replay and occurrence fences. No local build,
compilation, test runner or runtime scenario was executed. CI must validate the
repair after push; runtime success is not claimed here.

## CI duration repair — head `48a109b3`

Actions run `39565257931` reached the 180-minute job limit while still
executing Tasks tests. It did not stall on the old stream test. The complete
Gateway suite passed (2932 passed, 8 ignored) in 4579.19 seconds; CRUD unit tests
passed (713 passed) in 1899.90 seconds. The same-run observation and all recovery
payload regressions passed. Test completions continued until cancellation.
Successful main run `39556987142` already spent 4045.83 seconds in Gateway and
1686.43 seconds in CRUD, with a total workflow duration near 163 minutes.
The PR runner took longer across these existing suites, exhausting the margin.

The shared CRUD, Gateway and Tasks fixtures repeatedly build the full schema
from an empty database. Many migrations use SQLite's default non-transactional
SeaORM path, so each schema operation reacquires a pooled connection. The fixture
helpers now run initial migration within one transaction through SqliteDatabase,
pinning the physical connection for that chain. Migrations which request their
own transaction use SeaORM's existing nested SAVEPOINT support. All migration
steps, seed data, fixture isolation and test assertions remain in place.

This atomic boundary is limited to unpublished fixture initialization: the
database is private and no fixture consumer exists until commit. No domain state
can become stale through a concurrent application writer at this point. The
transaction is committed before workspace bootstrap, seeding, provider work or
the actual test scenario. Migration/rollback tests which construct their own
connections keep their existing setup. Production storage and migration entry
points, dependencies and CI configuration are unchanged.

Targeted rustfmt and `git diff --check` passed. No local build, compilation or
tests were run. Removing repeated pool acquisition is established by source
inspection; the time saved and completion within the existing CI limit require
the next remote run and are not claimed as measured results.

## CI fixture isolation — head `7bc28097`

Run `39589873110` finished with a test failure rather than the job timeout:
Gateway reported 2931 passed, 1 failed, 8 ignored. The only failure was
`production_settings_recovery_transitions_projection_before_new_source_capsule_write`,
at its five-second wait for the recovered source's completed index job.

The fixture bootstrapped the fixed default workspace `000000000000000000000`.
Memvid ownership is process-wide and keyed only by workspace ID
(`memory/src/thread_episodic.rs::workspace_lock`); separate databases and temporary
storage roots therefore still shared this lock with concurrent Gateway tests.
The ordinary runner and projection refill both use this ownership boundary.
That source-level isolation defect permits unrelated work to delay this test;
the log establishes the elapsed wait but does not identify the competing owner.

The test now creates a unique workspace through WorkspaceManager, which also
seeds its reserved identity, and uses that ID throughout the existing scenario.
It explicitly shuts down its executor after its assertions. Both model/dimension
transitions, provider-unavailable nonmutation checks, retry accounting and capsule
metadata checks are retained. No timeout, assertion, production lock or CI setting
changed. Targeted rustfmt and `git diff --check` passed; local builds/tests remain
unrun. The next remote run must validate the repair.
