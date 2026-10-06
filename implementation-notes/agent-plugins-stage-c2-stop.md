# C2 native stop prerequisite

Status: **READY_FOR_C2_STOP_REVIEW**. Source implementation only; coordinator
acceptance and behavioral validation are pending. **C2 NOT_COMPLETE**. CLI plugin
selection/aliases/continuations, graph CLI integration, mobile and testing remain
closed. The previous `agent-plugins-stage-c2.md` provider inventory is preserved.

## Snapshot

- Branch: `feature/agent-plugins-simple`.
- Worktree / command cwd:
  `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple`.
- Base: `4db792ff335907ed6e1a93f0e39dda60c59300a1`, verified initially clean.
  Recovery resumed one untracked, unfinished native ownership helper; no external
  changes were overwritten.
- Final implementation/code HEAD: `73684baf629be48735e13107fc62c3afd9886922`.
  Initial native implementation: `f3142450631e2d789437b569d01e2f2c8e687fbe`;
  the second small commit closes startup publication/prepared-root seams.
- Delivery HEAD is the documentation commit containing this file:
  `git log -1 --format=%H -- implementation-notes/agent-plugins-stage-c2-stop.md`.
  The final response also supplies its hash. Code commit was clean afterwards;
  delivery adds only this handoff, with final status checked after commit.
- Root AGENTS applies; no more-specific AGENTS exists in the three changed crates.
  Current prerequisite prompt, C2 proposal/inventory and accepted reviews read.
  Main, archived branches/worktrees and mobile unchanged. No push, deployment,
  merge/rebase/reset/clean, credentials access, app, provider or fixture run.

## Concrete implementation and contracts

`cli_runtime/manager.rs` retains starting/closing ownership in the **existing**
session map. Registration and factory task publication precede the first
cancellable factory await. The startup completion mutex is acquired **before**
spawn, so even immediate failed-start cleanup cannot observe an unpublished handle. `manager_ownership.rs` is a private helper for that
entry, not another registry or plugin operation engine. Existing identities and
generation allocator are reused. A caller dropping startup closes admission,
signals the same startup context and starts retained native cleanup. A failed
factory does the same. Preparation before process spawn is distinguished from a
retained live process. CAS conflict is rejected before starting a factory, so
there is no unpublished native process needing a second inventory.

Factory contract adds `&CLIAgentRuntimeSessionStartup`: dispatch and both real
factories share it; each retains prepared managed files even before spawn, synchronously retains
the spawned process before its next await, then retains the complete session /
transport before initialize. Native factories transfer guard cleanup ownership to
that same entry; standalone/probe guards retain their former Drop behavior. A
pre-spawn error cannot delete a root still referenced by strict grant cleanup. Cancellation competes with initialize, rather than dropping owned
workers. Native `process` is shared with this temporary startup owner; the same
Arc/identity is used for the complete session. The previous Claude failed-verify
write is retained. No Skills/MCP installer, OAuth, launch isolation, projection,
attestation or configured Codex recovery budget was replaced.

Native session contract separates `stop_and_wait` and `cleanup_after_stop`;
`close` remains their result-returning wrapper. Manager waits startup, native
process/workers/server, then Gateway callbacks, then the strict lifecycle hook
and managed-root cleanup. Only successful completion permits exact-identity
registry removal. Both transports drain even when a completed bridge returns an
error; no managed root is released on that error. Stderr consumed join failure is
sticky. Codex reuses the accepted three-worker helper. Claude retains its actual
reader and `OrderedEventIngress::spawn_owned` worker, fails pending senders,
closes request/emission admission and joins workers with sticky panic evidence.

`capture_stop_owner`, `stop_inventory(workspace)` and `stop_and_wait(owner,
deadline)` expose actual ownership, including startup/closing. The strict API
never treats unknown owner, missing map entry or bool false as completion. An
already captured, genuinely stopped old owner can acknowledge its own completion
without touching the replacement. Terminal Gateway cleanup now captures once
before async work and closes that exact instance. Existing void/logging wrappers
remain best effort; future plugin graph stop must use the strict owner API.

Both required bridges retain server handles and consumed outcomes in Stopping.
Their `request_stop` closes facade admission immediately and requests normal
`server.run` shutdown. Server closes outbound admission, waits facade and actual
call JoinSet, and returns its connection to the **same** supervisor entry.
There is no abort/drop shortcut for persistent required bridges. Facade
cancellation signals the existing invoker and **awaits its completion**; the old
outer select could drop resource-owning Gateway execution while clearing the
ledger. An undrained ledger is retained. This shared CLI facade change deliberately
makes an uncooperative invocation delay the result instead of pretending it ended.
No alternate invoker, executor, permission path or OAuth implementation was added.

`revoke_session_result` is the strict variant of existing supervisor cleanup.
Opaque socket/artifact/directory resources move to a cleanup holder in the same
session entry and survive cancellation/error. Successful individual steps consume
only their own reference. Grant revocation and completed bridge cleanup are cached
in their real owners; directory failure can be repaired and retried. TransportOwned
rejects early revoke; retirement validates its actual supervisor entry. Legacy
`build` and standalone/probe transport cleanup retain their best-effort branch;
required sessions use `build_owned`, the same builder with retained cleanup.

Gateway canonical/Codex notification/request/diagnostic pumps, durable listener
and fresh-task steer publication register their actual handles with the instance.
Closing rejects new registration and stops receives; in-flight handlers finish
before join. Request pump finalizes pending/buffered requests for its exact instance.
Listeners retain their hub strongly through cleanup, including poisoned-hub
replacement. Only CLI hubs opt into owned progress: one retained flush worker uses
the same coalescer, with no recursive queue; strict shutdown rejects queued durable
waiters and joins that worker. Legacy hub constructors / flush-only shutdown stay
unchanged. The extra hub change repairs a concrete detached-publication/queued-wait
hole in pump drain. Current-instance fencing remains; closing is never current.

## Resources and lock order

| Resource | Existing owner; publication | Drain / cancellation and retry | Release |
| --- | --- | --- | --- |
| Factory, child, stderr, managed descriptor | Same manager entry before await; raw process immediately after spawn; complete native session before handshake | Startup cancellation retained; native cleanup handle survives waiter deadline; process terminate/wait + stderr join; consumed panic remains failed | Root only after native and callback completion; entry only after all cleanup succeeds |
| Codex transport | Complete native session, before initialize await | Accepted owned reader/RPC/ingress abort-and-join by reference; sticky errors | Before overlay release |
| Claude stream | Client owns ingress at construction and reader at synchronous spawn, then same session | Fail pending senders, close admission, actual joins; cancelled waiter retains worker; consumed panic sticky | Before managed config release |
| Required MCP facade / calls | Existing bridge Serving/Ready handle; supervisor owns grant and opaque artifacts | Normal stop signal, invoker completion, facade/JoinSet, actual server join; error remains failed; same retained cleanup resources retried | After server join; successful strict revoke; no active transport cleanup |
| Gateway pumps / listener / fresh publication | Actual instance entry, synchronous task registration; listener retains hub | Receive cancellation plus completion of in-flight work; no abort; panic/flush failure persists | Join before root cleanup and strict ACK |
| CLI progress / queued publishers | Existing hub/coalescer, optional one-worker ownership | Close admission, reject queued waiters, join actual flush by reference; legacy branch unchanged | Before listener/pump completion |

Map and lock-directory mutexes are short, synchronous inventory work (lock-directory
uses Tokio mutex). No map guard spans filesystem/network/process/join or database
capacity. Normal acquisition/drain serializes with the existing logical key lock.
Its directory stores Weak references; every active/waiting guard retains the same
Arc, and idle cleanup prunes only dead entries. No split lock is created for a
waiting predecessor. Root cleanup does not acquire the manager key lock. Callback
self-stop only requests retained cleanup and returns a pending/error result, not
ACK; own-key acquisition fails before waiting. This avoids self-join and reentrant
key-lock deadlock. Native startup/completion, bridge state and cleanup mutexes own
only their respective handles/resources; supervisor inventory is released before
socket shutdown, grant operations, filesystem cleanup and joins. No DB contracts
or scheduling classes changed; request scopes and Maintenance idle reconciliation
remain existing caller operations, without DB capacity held over new native waits.

Source seams: cancelled startup (`StartupWaitGuard` + factory context), cancelled
close/deadline (retained close task), retry/error/panic (cached join outcomes), EOF
(`remove_if_generation` only requests cleanup), restart (prepare/checkpoint + drain
before replacement), idle (key-lock revalidation), shutdown (all instance admission closed and owned cleanup initiated
before awaited shutdown hooks; fresh lookup/reuse denied), stale captured owner (release revalidates ID).
All manager removal goes through `release_stopped`; EOF is not process proof and a
closing predecessor blocks reuse. Bridge shutdown waits a call that remains active
after observing cancellation; no grant/artifact release while TransportOwned.

## Actual checks

All commands below ran in the worktree above, with tests/hooks disabled.
Production checks were `CARGO_INCREMENTAL=0 cargo check -p pioneer-gateway --lib`.
Intermediate checks used successive dirty snapshots over the base (then over the
initial code commit). Only the last prepared-root check claims the final production
source bytes committed at the final code HEAD. Test bodies were subsequently parsed
by scoped rustfmt, never compiled. Concurrent cargo/rustc processes were observed
by process name only; their targets were not inspected or claimed as evidence.

| Evidence | Actual exit / result |
| --- | --- |
| `target/plugin-c2-stop-check-1.log` | 0, 2m33s; intermediate before owned hub and later fixes |
| `target/plugin-c2-stop-check-2.log` | 0, 1m59s; intermediate |
| `target/plugin-c2-stop-check-3.log` | 0, 1m28s; intermediate |
| `target/plugin-c2-stop-check-4.log` | 0, 1m29s; intermediate |
| `target/plugin-c2-stop-check-final.log` | 0, 1m24s; superseded by final bounded flush / drain changes |
| `target/plugin-c2-stop-check-delivery.log` | 0, 1m28s; initial code HEAD, superseded by startup fixes |
| `target/plugin-c2-stop-start-publication-check.log` | 0, 1m50s; intermediate startup publication fix |
| `target/plugin-c2-stop-final-publication-check.log` | 0, 5m14s; intermediate, before prepared-root retention |
| **`target/plugin-c2-stop-prepared-root-check.log`** | **0, 9m01s; final production source bytes at `73684baf`** |
| Scoped `rustfmt --edition 2024 --config skip_children=true --check` | 0, all 13 changed Rust files including source test files; parsing is not test compilation |
| `git diff --check`; `git diff --check 4db792ff335907ed6e1a93f0e39dda60c59300a1 HEAD` at code HEAD | 0 / 0 |

Final compile has only existing unused `set_mcp_policy` warning. Logs remain local
under ignored target; no command journal committed. No generated/wire/FFI contract
changed or generation required. Commits use `-c core.hooksPath=/dev/null`.

## Regression sources — NOT_RUN / NOT_COMPILED

- `manager_ownership.rs`: cleanup initiated inside a factory waits its actual
  published handle; prepared root without a process survives cancelled waiter until
  strict lifecycle cleanup completes; cancelled startup and close retain actual native task;
  repeated close cleans once; in-flight callback blocks root cleanup; late callback
  registration denied; consumed callback panic remains failed on repeat.
- `manager.rs`: strict stop deadline retains closing inventory; retry waits actual
  completion; captured stopped owner cannot stop replacement; shutdown closes start
  admission; existing EOF source updated to require retained inventory + actual drain.
- `claude_session.rs`: actual worker handles survive cancelled join waiter and retain
  consumed panic across repeat, using an in-memory delayed task (no child fixture).
- `mcp/server.rs`: existing helper/facade source extended with a call delayed **after**
  observing cancellation; normal stop keeps server pending and rejects premature
  revoke until genuine completion. This source fixture was **not executed**.
- `mcp/supervisor.rs`: failed directory cleanup retains the same native cleanup
  owner and retries successfully after removing its injected blocker; source only.
- `runtime-events/src/tests.rs`: strict owned CLI hub rejects queued durable waiters,
  joins buffered progress and refuses late progress; legacy constructors unchanged.
- Existing test factory signature adapted; accepted Codex transport sources preserved.

No test runner, test-target compilation, app/provider/process/fixture, migration,
functional/smoke/browser/device run. Types/behavior of test sources are unvalidated.
Historical external test compilation remains **UNKNOWN_EXTERNAL_ACTIVITY**, not
part of this task's evidence or a passed check.

## Remaining boundaries

Configured Codex nested `spawn_blocking` spool decoders remain **unjoined** by the
three-worker helper. They perform bounded pure decoding from anonymous spool,
without package/data/tool/publication effects after their owning transport ends;
no recovery budget was reduced. This delivery claims the actual resource owners
listed above, not arbitrary descendants or 100% cross-process isolation.

A native/protocol/join panic remains a failed closing instance; retry cannot turn
empty handles into success. An invocation ignoring cancellation can outlive the
request deadline, with actual cleanup retained and no ACK/root release. Runtime
or host failure cannot manufacture completion evidence. Production compilation is
for the current host, not behavioral or cross-platform verification.

No v2 plugin architecture deviation: no new tables, jobs/generations/leases,
installer, OAuth engine, UI, apps runtime or generic operation framework. Native
startup factory context, strict supervisor variant and optional owned CLI hub are
local stop-contract adaptations. Full C2 still must connect the accepted parent
selection snapshot/aliases/assets/ready/continuation/task/recovery gates and strict
CLI inventory into the existing graph stop. Explicit CLI plugin rejection,
detached-plugin rejection and graph CLI-binding failure are preserved; these are
pending C2 work, not completed plugin support. Stop here for coordinator review.
