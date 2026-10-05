# C1 native stop prerequisite handoff

Status: **READY_FOR_C1_STOP_REVIEW**. Full C1 **NOT_COMPLETE**; lifecycle/UI,
C2 providers/continuations, D mobile and E separately authorized testing remain.
This is source delivery, not coordinator acceptance or behavior verification.

- Branch: `feature/agent-plugins-simple`.
- Worktree/cwd for all checks: `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple`.
- Base: `8edfcc23bd6c914510dc2ad5eb5953d138a37653`; initial tree clean.
- Final implementation HEAD: `e7777f346fd6051e61e9cc4ea6daf3fff433bc6d`; tree clean.
- Delivery HEAD is the subsequent handoff-only commit; exact SHA accompanies delivery.
- Commits: `0615968a` shell ownership; `5d8fa294` native Agent completion;
  `473a6921` MCP completion; `e7777f34` native graph await.

## Actual ownership chain

`agent/src/lib.rs:3878`: `cancel_turn_and_wait` captures a `NativeTurnStopOwner`
before signalling; Gateway can capture first through `capture_turn_stop_owner`
and then call `cancel_captured_turn_and_wait`. The handle binds the actual native
control/run and original actor mailbox, not a turn-ID lookup after cancellation.
An internal CancelTurn carries the captured run; legacy public cancellation
keeps its fast admission ACK, cached admission semantics and unrestricted legacy
branch. A late internal command cannot cancel a replacement run or its context
fence. Completion does not wait for that ACK or depend on actor/event-consumer
progress: a full/closed mailbox still leaves the exact run drain available.

`agent/src/native_completion.rs:111`: the current native run retains root and
nested **actual JoinHandles**, their join results and its existing shell handlers.
Cancellation preserves the existing root grace/abort policy, then joins root,
joins parallel nested tasks, and awaits native shell cleanup before publishing
an outcome. A consumed join failure is cached, not lost to a concurrent observer.
`chat/mod.rs::AbortOnDropJoinHandle` retains task ownership in that same run;
Drop requests abort, while the native owner still joins it. Tools remain parallel.

`agent_loop.rs::TurnTaskFinished`, cancel, recovery handoff, shutdown and clear
use the same completion drain. Normal completion also drains persistent shells.
Active control clear retains the latest owner. Native thread retirement retains
only outstanding cleanup in the existing manager state, drains after actor join,
and releases confirmed results; no permanent turn history. Its bounded retirement
cleanup uses the existing 2.5-second retirement budget scale; timeout keeps the
owner reachable. Captured handles remain valid after retirement/release. Recovery
retains unresolved preceding native cleanup, rather than silently dropping it.

`tools/src/handlers/shell.rs:246`: `UnifiedExecHandler::stop_and_wait` reuses
`terminate_child_process`/process-group policy and `child.wait`, then joins reader
tasks. One-shot Child, reader handles and runtime temp directory are published
synchronously into this **existing shell handler** before any await; persistent
sessions retain their readers too. A dropped dispatch/reader waiter leaves the
real handles reachable. Consumed reader panic is sticky. `BuiltinTools.shell`
is the same handler registered for exec/write_stdin and retained by the run.
`runtime.rs` dispatch cancellation grace is unchanged: dropping dispatch now
cannot lose the only Child owner. No plugin IDs, shell runner, permission bypass,
new OAuth implementation or tool-call serialization were added.

`gateway/src/mcp_service.rs:2042`: Result-returning `stop_task_result` is available
to lifecycle callers **under the existing installation lifecycle guard**. The
original task map retains shared join outcome and actual session; cancellation
of a waiter does not consume either. Initial missing owner and replacement are
errors; a cloned confirmed join cannot lose its error to another observer.
Legacy best-effort `stop_task` remains; native reload/manual restart propagate
stop errors before replacing the runtime, and acknowledged obsolete owners are
released. Stopping actor admission exclusions from the accepted checkpoint remain.

`mcp::McpRuntimeSession::shutdown_result` adds native acknowledgement; the legacy
trait default fails closed. `RmcpRuntimeSession` awaits rmcp close, actual native
stdio wrapper kill/wait and stderr-reader join. `ManagedChildStdout::Drop` only
signals; it no longer spawns detached cleanup. Connector shutdown cannot discard
a connect future already owning a child. Failed startup cleanup retains a failed
session, publishes no usable catalog, and cannot return successful stop. Native
HTTP OAuth/header behavior is unchanged. Fake sessions acknowledge their own
existing cleanup; the OAuth proxy fixture forwards the real native result.

`message/agent_runtime.rs:7449`: the await variant shares existing graph fencing,
Task cancellation and native cancellation. Root and descendants are captured
before native Task cleanup; every descendant error propagates. The bounded native
Crud/repository variant collects already-fenced Turn bindings on retry inside
that **same serialized graph transaction**, revalidating scope/node bounds; it
preserves the old active-only standalone branch. No binding under that fence
means no admitted native Turn; an unknown bound owner fails closed. DB status is
never used as process acknowledgement. Request scope remains Interactive/inherited.

Registry/map locks only snapshot/update handles. Joins, process/network waits
occur outside them and outside DB capacity. Per-owner cleanup/join/session locks
serialize observers of that exact owner; installation guard remains outermost
for MCP mutation. No new tables, jobs, leases, operation polling, receipts,
plugin coordinator, durable cleanup worker or protocol/generated DTO changes.

## Outcomes and limits

Success confirms owned root/tool tasks and native shell/process cleanup. A single
request deadline spans capture/drain and graph work; timeout keeps unresolved
owners. Unknown owner, ambiguity, root/tool panic, MCP join/cleanup failure and
replacement remain errors; admission ACK, cancellation token, Drop and terminal
DB state are not success proofs. Released historical owners return UnknownOwner;
callers needing their result must retain the captured handle before cancellation.
No package-file mutation was added. Full C1 must consume these results under its
parent gate before file changes, and use the request deadline around MCP waiting.

This covers Pioneer-owned tasks/process handles under the existing process-tree
policy. Escaped daemons, completed remote effects and rollback of executed calls
are outside that guarantee. B-rejected CLI/provider paths remain unsupported;
the await wrapper fails closed for them. Failed/panicked MCP cleanup deliberately
keeps its failed result even if a subsequent cleanup attempt succeeds; it cannot
be interpreted as successful lifecycle completion without explicit reconciliation.
There is no added automatic recovery framework.

## Checks and regression sources

All source tests **NOT_RUN**, all own test targets **NOT_COMPILED**. No app,
provider/fixture/process scenario, migration, device or functional run performed.

At final implementation HEAD `e7777f34`:

- `CARGO_INCREMENTAL=0 cargo check -p pioneer-gateway --lib`: **exit 0**, 1m37s;
  `/tmp/pioneer-c1-stop-final-gateway-check.log`. Two dead-code warnings:
  future C1 graph await entrypoint and previously unused `set_mcp_policy`.
- `rustfmt --edition 2024 --config skip_children=true --check` on all 17 changed
  Rust files: **exit 0**. This formats/parses sources, not test compilation.
- `git diff --check 8edfcc23bd6c914510dc2ad5eb5953d138a37653..HEAD`: **exit 0**.

Intermediate dirty-tree library checks: Agent `/tmp/pioneer-c1-stop-agent-check-1.log`
**101** (unavailable derive dependency; replaced with standard Error/Display).
Gateway `/tmp/pioneer-c1-stop-gateway-check-{1..7}.log` exits respectively
**0, 101, 101, 0, 0, 0, 0**. Failures were missing shell destructuring and duplicate
MCP session field; corrected before final check. Intermediate successes are not
behavior evidence. Hooks disabled on commits. Prior handoff's historical external
test-compilation UNKNOWN_EXTERNAL_ACTIVITY is not this task's check/result.

Regression sources cover real public admission before delayed native completion;
root versus delayed nested join; cancelled waiter/retry; timeout, panic and unknown
owner; cleared/retired owner; recovery run identity; parallel tools; aborted
one-shot dispatch and persistent Child wait; reader error retention; shared MCP
join error/panic, cancellation/repeat and replacement; graph-fence retry and
queued nodes; descendant deadline propagation/retry through the Gateway wrapper.
Sources live in native_completion, manager_tests, chat, shell, mcp_service,
agent_domain repository tests and message/tests. All statuses remain NOT_RUN /
NOT_COMPILED. No behavior or source-test compilation success is claimed.

No known unresolved source compilation error. Coordinator must review lifetime,
legacy timing and failure behavior before full C1 resumes; full lifecycle/UI,
C2/D/E and testing remain explicitly unimplemented/unapproved.
