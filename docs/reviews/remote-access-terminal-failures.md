# Remote access terminal failures: review notes

This change addresses client resilience on subsequent authentication/configuration
rejections. The original Relay/Gateway incompatibility was already resolved by
Gateway 0.55.0. It does not change production relay configuration or Sentry
filtering policy.

## Review locations and dependency state

- Pioneer branch: `fix/remote-access-terminal-failures`.
- Pioneer worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/remote-access-terminal-failures`.
- Pioneer base: `ddcc02057c76f7b44cce86c420c4596de414c1bb` (local `main`).
- Relay branch: `fix/remote-access-terminal-failures` in the separate relay repository.
- Relay worktree: `/Users/alexander/Code/pioneer/pioneer-relay/.worktrees/remote-access-terminal-failures`.
- Relay base and original pinned dependency: `93e0ddccab7cb6c255b417c391ef649a4f7554a7`.

Relay 0.6.1 is committed and pushed at `686680a37689bf0abea13f8e37340a588003da50` on the fix branch:
https://github.com/pioneerdotai/relay/pull/1
The GitHub branch ref was checked against the local commit before updating Pioneer.

Pioneer pins that exact published Git commit in the workspace dependency.
`Cargo.lock` records rathole 0.6.1 with the matching Git source. The local path patch
has been removed, so the dependency no longer requires a neighboring relay worktree.
The Cargo checkout is untouched.

The Relay and Pioneer PRs still need review and merge. Publishing the branch does
not merge it or create a release. No release tags or deployments were created.

## Lifecycle invariants

1. Relay turns `Ack::AuthFailed` and `Ack::ServiceNotExist` into typed errors and
   retains the corresponding typed events. Its own retry loop detects the error
   type, records one static rejection diagnostic, and exits without backoff or a
   reconnect event. This also works when no Pioneer event consumer is attached.
2. Network errors retain the existing exponential backoff, jitter and unlimited
   elapsed time. Neither the backoff configuration nor `max_restarts` is changed.
3. Control handles own their tasks. Shutdown selects against the entire control
   future (including DNS/connect/handshake) and against backoff. The client awaits
   each control handle; hot reload awaits a removed/replaced service before
   starting its replacement.
4. Each control channel owns its data tasks in a `JoinSet`. On disconnection or
   shutdown, it cancels and joins them. Data tasks select cancellation against
   handshake/connection/forwarding, and abort and join their nested UDP tasks.
   Completed tasks are reaped while the control channel is active. Drop guards
   abort owned tasks if an owning future is canceled unexpectedly.
5. `run_config_with_events` owns the instance future directly, instead of spawning
   a detached instance. Pioneer also owns the relay future directly. Normal stop,
   disable and reconfiguration use cooperative shutdown and await cleanup rather
   than timeout-and-detach fallbacks.
6. Pioneer latches terminal status from typed events and returns a terminal run
   outcome. Cleanup events and late events cannot overwrite that failure; the
   outer supervisor returns without restarting. Authentication remains
   `Failed/TunnelAuthFailed`; missing service is `Failed/InvalidSettings`.
7. `apply` serializes stop/join/start under the supervisor state lock. A new
   generation starts only after the previous supervisor and its relay children
   finish. Join handles stay in state across awaits, so cancellation of an `apply`
   does not lose ownership. Dropping the supervisor signals shutdown too.
8. Each new `apply` starts a fresh event state. The existing settings switch
   (disable then enable), saving a corrected key, and explicitly applying the same
   enabled configuration can retry. Static review verified the settings update
   sets `changes.remote_access.changed` and the Gateway dispatch calls `apply`.
   No new UI or protocol action is required.
9. Rejection diagnostics contain static reasons, without raw dependency errors,
   tokens or authentication verifiers. The former startup routing-digest log is
   removed. Sentry's authentication-event policy remains intact.

## Review follow-up: partial control reads and heartbeat

`ControlChannel::run` now creates one pinned heartbeat timer per established
session. It calls `next_control_command`, which pins the production
`read_control_cmd` future once for each command and keeps polling that same future
while reaping data-task completions. The `read_exact` buffer and partially consumed
bytes survive those completions. A fresh read starts only after a complete command.
The read can be dropped when the session ends (timeout, read error or shutdown).
No reader task is spawned and data/UDP ownership and cleanup remain unchanged.

Data-task completion never resets the timer. Any correctly parsed command resets
the deadline to now plus the configured interval, including `CreateDataChannel`;
there is no HeartBeat-only activity policy. A zero interval disables the timeout
branch. A new control session creates a new timer; an expired timer returns an
error and exits the session, rather than leaving a ready timer in a busy loop.
Wire serialization, token routing, authentication, terminal failure classification,
backoff and restart budgets are unchanged by this follow-up.

Four additional Relay tests in `src/client/control_session_tests.rs` are written:

- A duplex-stream reader reports cumulative byte consumption when its underlying
  read returns Pending. The test waits for exactly two consumed command bytes and
  a pending read before releasing an existing data-task gate. It then waits for
  the reader to be polled again, sends the remainder and the next command, and
  checks HeartBeat followed by CreateDataChannel on the same stream, with the task
  reaped. This uses read barriers, not sleeps or TCP packet timing.
- With Tokio time paused, a data task completes at 35 seconds in a silent session
  with a 40-second timeout. The written assertion requires timeout at 40 seconds.
- Both valid command variants at 35 seconds refresh the deadline to 75 seconds.
- Timeout zero remains disabled even after 4000 virtual seconds and a data-task
  completion; the following control command is still read.

Pioneer's `canceled_apply_keeps_old_task_owned_until_followup_joins_it` test uses
existing private supervisor state and oneshot gates, without production hooks.
It installs a controlled old-generation task, polls the actual apply into Pending
while that task cannot finish, then drops apply. It checks the old task identity
and shutdown sender are still in state. Both subsequent apply and shutdown are
polled and required to remain Pending while the gate is closed; a listener accept
probe also checks no new client connected. Only then is old cleanup released. The
old task publishes its final status and completes before a real new client receives
AuthFailed from a local fixture. Joining its completion leaves the new terminal
status intact. No production Pioneer code was changed for this test.

Existing terminal-rejection and shutdown tests in Relay are retained. Static review
also checked Pioneer's existing written tests for internal/external retry cessation,
sticky failure, corrected keys, disable/enable retry, handshake/backoff cancellation,
TCP forwarding socket closure and local secret-free Sentry capture. Their runtime
behavior has not been verified, and no UDP runtime claim follows from these TCP tests.

## Regression coverage added (not executed)

`crates/tunnel/tests/remote_access_lifecycle.rs` uses a local TCP wire fixture,
loopback sockets and test keys, exercising the actual embedded relay client:

- Authentication rejection stops connection counts and preserves the entire
  failure snapshot under unlimited and bounded outer restart policies.
- Missing service is terminal as well.
- Correcting a key creates a connected generation.
- Explicit disable/enable retry recovers with the same key for both rejections;
  fixing the server alone does not silently retry a rejected configuration.
- Transient handshake/network failure retries automatically; loss of an
  established control channel also reconnects after the listener returns.
- Disable and replacement during handshake/backoff close old sockets, stop old
  attempts and preserve the new status.
- Shutdown closes a live TCP forwarding connection and a pending data handshake.
- Dropping the supervisor stops handshake/backoff tasks.
- Sentry's local test transport captures exactly one secret-free error per
  rejected generation, with no retry errors and no network export.

A tunnel unit test verifies the terminal latch ignores cleanup/late events and
never publishes raw rejection diagnostics. The existing event test is updated for
sticky authentication failure and a fresh next-generation event state.

An observability unit test preserves the new single rejection diagnostic as a
Sentry event. Existing network-demotion/authentication-preservation checks remain.

Relay unit tests independently exercise both rejected Acks without any owner
shutdown, awaiting the control task's natural completion and observing no further
connections. Another test verifies its public API awaits a pending handshake's
shutdown and socket closure. Existing `relay_token_routing.rs` checks are unchanged.

## Validation limits

The initial implementation used static review, Rust formatting, `git diff --check`,
offline Cargo metadata without dependency traversal/builds, and TOML/lockfile/path
consistency checks. During this review follow-up, only source/history/diff review,
source formatting and whitespace checks were performed; no Cargo command was run.
Both accumulated diffs were reviewed for unrelated changes and lifecycle invariants.

No compilation, cargo check, application, Gateway, relay, tests, test wrappers,
`cargo test --no-run`, CI, deployment, publication, merge or Sentry issue mutation
was performed during implementation and review. The written tests and runtime behavior still require independent
execution/verification. UDP task ownership is covered only by static review;
Pioneer supports TCP and the runtime socket regression tests written here exercise
TCP. After acceptance, Relay was committed and pushed with a version bump to 0.6.1,
and Pioneer was updated to the verified published commit. GitHub CI was not disabled
or skipped for these pushes. Local tests, builds and applications remain unexecuted.

Тесты не запускались по указанию пользователя.
