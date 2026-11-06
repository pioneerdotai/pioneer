# Durable task scheduling: external static review handoff

Status: READY_FOR_EXTERNAL_STATIC_REVIEW. External acceptance has NOT occurred.

## Revision and isolation

- Branch: `fix/task-monitor-scheduling`
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-monitor-scheduling`
- BASE_SHA: `cae4a50892990abc9d8f3afa07be933d6ef648cc`
- Frozen implementation candidate HEAD: `734c010ff69fb95ab4ec4c134d58b7fe7111dfdc`
- Previously externally reviewed HEAD: `c502d35eec98ee17359be3ea0498de441970b8fe`
- Final review HEAD: the commit containing this handoff, whose exact SHA is
  supplied in the final delivery message. This documentation-only commit cannot
  embed its own SHA. Resolve with `git rev-parse fix/task-monitor-scheduling`;
  review its entire diff against BASE_SHA, including this file.

`origin/main` was fetched before branching and matched BASE_SHA. Root and CRUD
AGENTS.md were read. The main checkout was clean; existing neighboring worktrees
were inspected, some had unrelated changes, and none were edited or cleaned.
No name collision required a fallback. Hooks were disabled for commits.

## Problem and minimal solution

An extra check previously replaced recurrence with a one-shot. Historical runs
then prevented that one-shot from firing because the trigger ID was reused.
Pausing an exhausted schedule returned no explicit explanation of that state.

No existing model-facing extra-run operation was found. Added `task_run_now`:
Gateway task domain -> exact TaskScheduleManage / TaskControl authorization ->
canonical ControlTask/RunNow action -> TaskService -> shared queued-occurrence
writer -> RunCreated + TaskQueued + immutable occurrence + action ledger commit.
The normal scheduler queue, dispatch, execution admission, recovery, retry,
review and delivery paths continue to own execution. No model is started directly.
Native tools and the CLI MCP facade share the registered specs and handler;
there is no separate unauthenticated RPC launch endpoint.

The new run has no trigger ID, so it cannot consume a planned schedule fire or
coalesce with a queued scheduled occurrence. The existing task actor, base agent
spec, lifecycle, delivery, concurrency and security policies are reused. The
trigger is not updated by admission. Manual completion consults the current
schedule when refreshing aggregate task status. With accepted additional runs,
automatic CompleteOnTerminalRun completion waits for all accepted queued/active
runs and for the absence of a future scheduled fire, regardless of which run
finishes first. Without additional runs, ordinary completion is unchanged.
Explicit finish/cancel and already-terminal task protection retain priority.
Bounded repository lookups serve preparation and terminal refresh.

The durable Agent action receipt atomically stores taskRunNow.taskId/runId.
Gateway replay reads that receipt even after loss of the response; runtime and
writer replay also check the canonical action identity and fingerprint. A race
at the writer returns the already committed reference. No extra ledger, table,
request identity scheme or migration was introduced. Cancellation before commit
leaves no partial admission; cancellation after commit leaves a discoverable
ordinary queued occurrence. Publishing is after commit.

For explicit schedule replacement, trigger ID remains stable, preserving run,
occurrence, webhook endpoint and delivery references. The current assignment's
last_fire_at is cleared in reschedule and changed-trigger update. One-shot
consumption checks that cursor, rather than all historical runs. The writer
atomically compares trigger state and consumes next_fire_at with RunCreated.
Historical cursor event keys no longer suppress explicit replacements or fresh
one-shot consumption, including assigning the same timestamp twice. Recurring
fire keys retain their existing behavior. A queued older assignment cannot
swallow a new one-shot. Scheduler allocation rechecks next run number and
occurrence generation inside the writer, so a concurrent manual admission leaves
a stale schedule plan due for a fresh pass instead of colliding with its run.

## Tool contracts

- `task_run_now {taskId, idempotencyKey?}` returns durable `taskId`, `runId`;
  receipt replay may also return `alreadyCommitted`. Normal calls omit the key
  and use the runtime's durable call identity. A different identity intentionally
  requests another run. A missing canonical action identity is refused.
- Active or exhausted triggers may admit an extra occurrence only while the task
  remains executable. Paused/cancelled triggers, blocked/terminal task state and
  finish requests are not reopened. Current work is never cancelled for a run.
- `task_reschedule` remains an explicit replacement; `task_update` changes a
  trigger only when its patch changes the current assignment. Neither is an
  alias for an extra run. Resume retains the saved interval/cron spec.
- `task_pause` adds optional, backward-decodable `scheduleOutcome`: `paused`,
  `no_future_firings`, or `not_applied`. Current servers populate it. Terminal
  no-ops report not_applied; exhausted schedules stay exhausted and report
  no_future_firings. Errors do not claim an applied operation. This is a snapshot
  of schedule control, not queue disposal, running-work cancellation or a ban on
  later rescheduling. An already-paused schedule reports paused.

Domain visibility, capability gating, authorization catalog, permission intent,
protocol exports/schema registration and CLI/native replay are aligned. No role
or executor permission was widened. Skill changes are confined to the canonical
`resources/skills/pioneer/tasks/` sources, not exported home-directory copies.

## External review corrections (2026-11-05)

P2 was reachable with the default lifecycle of a future scheduled_at task:
admission preserved trigger fields, but the additional run's completion emitted
TaskCompleted and made the planned fire unexecutable. The previous self-audit
missed this lifecycle interaction; the original trigger-field assertions were
insufficient evidence.

The first correction added a guard in the existing terminal transition: only a
run with a trigger ID could apply ordinary automatic CompleteOnTerminalRun at
task level. That guard alone was insufficient for the reverse execution order;
it is superseded by the drain condition described below. No saved lifecycle
policy is changed and scheduled_at tasks remain supported. RunFailed/RunBlocked/RunCancelled
outcomes, retry exhaustion and configured delivery remain durable run outcomes;
they are not rewritten as success merely to preserve the schedule. Explicit task
finish/cancel still stop planned work, and finish draining can terminalize the
task after an additional run completes. A terminal task is never reopened.

Preparation already reads the exact run, occurrence, task policy and trigger;
the terminal writer revalidates these facts through its existing snapshot and
attempt fences. That first correction did not alter transaction boundaries or
add queries, authority, schemas, migrations or infrastructure. Terminal replay, occurrence
finalization, delivery deduplication, finish-race reconciliation and retry keep
their existing atomic paths.

The blocked/finish negative tests previously reused a paused task, so pause
alone could satisfy their assertions. Each guard now has its own fresh task.
Guard-specific states use the existing TaskUpdated event fixture: blocked has an
active trigger and no finish request; finish-request has an active trigger and
a nonterminal task. This deliberately isolates those guards from secondary effects
of full block/finish transitions. Each refusal must leave the run history unchanged.
The new execution matrix separately exercises actual task_finish/task_cancel.

## Follow-up: primary run finishes before the accepted extra run

The review of c502d35 found the reverse ordering: an already-running planned
one-shot could complete its task while an accepted extra run waited behind it.
The former trigger-ID-only completion guard also could not close a task when an
extra run was the last remaining work. These are now one drain decision in the
existing terminal transition, without allowing dispatch of terminal tasks.

For CompleteOnTerminalRun tasks, a repository query first checks whether any
additional run exists (trigger_id IS NULL). Without one, the old completion
behavior remains unchanged. With one, preparation reads a pending sibling ID
(including queued/waiting/retry work) and an unsuperseded failed terminal outcome,
if any. Each lookup returns at most one row; no run history is materialized.
Task/trigger facts and these exact bounded facts are reread inside the terminal
writer before any projection. A future active or paused schedule still prevents
automatic closure; after the final accepted run and no future fire, the same
terminal batch closes the task. The saved lifecycle policy is not rewritten.

Sibling admission/terminal progress invalidates the prepared decision and uses
the existing bounded terminal commit retry loop to prepare again. Monotonic
planned-fire consumption of the same trigger identity/spec is also reconciled,
so a scheduled sibling admitted between preparation and commit is preserved.
Policy, actor, current run/attempt, locks, task revision, trigger identity/spec,
pause/resume and explicit finish/cancel fences remain protected. This is a narrow
terminal reconciliation, not a scheduler, versioning layer or new worker.

A successful last sibling does not erase an earlier unsuperseded failure,
blocking or cancellation: the final task outcome uses that recorded outcome.
A failed attempt with a retry successor is excluded from that decision; the
retry's own final outcome remains authoritative. Existing run outcomes and their
delivery snapshots remain intact. The prior extra-failure scenario now asserts
TaskFailed with the original error after all work drains, in addition to its
existing failed-run and failed-delivery assertions. Explicit finish/cancel still
win and use their existing draining/disposal semantics. Terminal/action replay
continues through the same durable events, ledger and delivery deduplication.

The extended existing CLI/System fixture dispatches a real planned run, holds
its execution handle, admits an extra through task_run_now, and proves that the
extra stays queued with maxParallelRuns=1. It prepares the primary terminal batch
before extra admission to protect the writer recheck. After the primary completes,
the extra must actually dispatch and finish, the task must become terminal, and
command plus both terminal replays must leave exactly two runs and deliveries.
Both primary-success and primary-failure variants are included. The opposite-order
queued scenario now also prepares before planned-fire admission, exercising
revalidation of the schedule cursor. Future-fire, extra-error, extra-retry,
finish/cancel and independent blocked/finish-refusal scenarios remain covered.
All of these tests are authored only and remain NOT_RUN.

No schema, migration, role, tool registration, queue policy, application process
or production task was changed by this follow-up. The accumulated package still
requires the deferred schema-emitter comparison and all executable validation.

## Authored regressions — all NOT_RUN

- Existing CLI fixture across providers: cron and interval extra runs preserve
  the saved trigger and next fire; receipt replay after discarding the response
  returns the same run; a durable occurrence exists. Foreign task, paused,
  blocked, finish and cancellation cases refuse additional work.
- Service one-shot matrix through reschedule and update: two assignments of the
  identical time produce two historical runs; reprocessing each assignment
  produces no extra run; exhausted pause reports no_future_firings.
- Running interval pause/resume: the current run remains running, can complete,
  and resume retains repetition.
- Ordinary manual queue fixture: waits behind a running scheduled occurrence,
  then completes through dispatch without losing recurrence or next fire. A
  stale scheduler writer plan after manual queue growth cannot reuse allocation
  or consume the scheduled fire. The fixture does not fabricate action receipts;
  API/ledger admission is covered at the CLI boundary.
- New one-shot execution matrix in the existing CLI fixture: real task_run_now
  admission, queue dispatch through CompletingSystemExecutor, terminal writer,
  receipt/terminal replay, and eventual execution of the original scheduled_at
  run. A queued planned sibling remains executable after the extra completes;
  the saved lifecycle stays CompleteOnTerminalRun. Failed extra runs preserve
  their error and delivery snapshot; a retried extra uses its existing occurrence
  and does not complete the task instead of the original fire. Repeated due scans
  and reconstructed-handle terminal replay must not duplicate runs or deliveries.
- The same compact matrix covers actual explicit finish/cancel during an active
  additional run: planned work stays stopped and completion cannot reopen it.
  The future case uses ordinary queue dispatch end to end; queued/error/stop
  cases use existing execution handles to control the precise terminal boundary.
  Primary-first success/failure cases additionally prove real extra dispatch
  after primary completion and final task closure, including stale-preparation
  reconciliation. No provider/CLI process or live task was executed while
  authoring these tests.
- Existing permission/capability and domain/prompt inventory fixtures include
  the new operation. These cover independent visibility/consent contracts.

## Static self-audit and deferred validation

The accumulated diff from BASE_SHA, affected callers, tool registrations,
permissions, event keys, queue admission, terminal refresh, replay and skill
text were reread. No new scheduler architecture, task cloning mechanism,
intent/text heuristics, authorization model, event system, tables or migrations
were added. No production database or real task was accessed. Static findings
about stale allocation, event-key reuse and terminal refresh were repaired within
scope. After the external-review corrections the full accumulated diff was
reread again: run_now/ledger replay, reschedule/update assignment consumption,
pause outcomes, queue/dispatch/retry/terminal paths, permissions/registrations,
schemas, skill text and regressions. The latest review correction adds only
bounded terminal drain facts, their writer reconciliation, and regression
coverage. Callers for admission, scheduled/retry dispatch, terminal replay and
explicit stop paths were reread after each logical change. This self-audit is not
external acceptance or evidence of test success.

Checked-in schema edits were made statically: RunNow params/response, pause
outcome, and the embedded AgentTaskControl enums. Protocol schema registrations
were updated. The authoritative `pioneer-protocol` schema emitter and comparison
remain deferred; no generator or migration was executed. Compile/type correctness,
behavioral execution, database rollback/cancellation fault injection, cross-process
race/restart validation, and generated-schema equivalence require later authorized
validation after external acceptance. No live monitor reproduction was attempted.

LOCAL_TESTS: NOT_RUN
TEST_COMPILATION: NOT_RUN
CARGO_CHECK_AND_BUILDS: NOT_RUN
MIGRATION_EXECUTION: NOT_RUN
CODEGEN: NOT_RUN
RUNTIME_REPRODUCTION: NOT_RUN
FORMATTING: PASS (`cargo fmt --all --check`)
DIFF_CHECK: PASS (`git diff --check`, and accumulated diff against BASE_SHA)

The final candidate is frozen for external static review. Do not modify it until
reviewer feedback. No push, PR, CI, merge or deployment was performed.
