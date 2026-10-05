# Stage C1 handoff

Status: **READY_FOR_STAGE_C1_REVIEW — partial delivery; C1 NOT_COMPLETE**.
The package mutation / execution acknowledgement boundary is blocked below.
This is not acceptance, a behavioral check, or permission to start C2/testing.

- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple`.
- Branch: `feature/agent-plugins-simple`.
- Base: `39eb03b79eb8685002c00db5ee51120528d0edfa` (accepted B); initial tree clean.
- Final implementation HEAD: `a312c27e86f1ca87f531099766a77292407f95ae`.
- Dirty state: initial and final implementation trees clean; delivery status
  is checked after the handoff commit and reported with delivery.
- Delivery HEAD: the subsequent handoff-only commit on this branch; its exact SHA
  is reported with delivery. No implementation changes after the implementation HEAD.
- Commits: `392d1b00` native policy operation; `38a2761b` cancellable native
  runtime stop ownership; `a312c27e` stopping-actor admission.

## Completed native changes

`message/mcp/policy.rs`: `set_mcp_policy` returns a typed native response without
an internal websocket request. The RPC and business entrypoint share the same
`prepare_mcp_policy_change` and `finish_mcp_policy_change`: original workspace
validation, native lifecycle lock, native policy/audit write, changed publication,
and runtime reload. The RPC retains response-before-publication ordering and
holds its native lifecycle guard until the former release point. Admission is
still the caller's normal authorization boundary. No owned-policy bypass added.

`mcp_service.rs`: shutdown requests consume their sender once, but keep the
original handle in the **existing native task map** until its task ends. A caller
cancelled while waiting leaves a subsequent stop able to observe that owner and
join it. Taking the handle after the wait rechecks its completion identity; a
replacement runtime with the same installation ID is not removed. Join failures
retain the legacy warning behavior. Stopping handles cannot receive new tool
calls/OAuth recovery and cannot satisfy the same-configuration runtime reuse
shortcut. This fixes native stop ownership; it is **not** a complete plugin
execution stop/ack contract.

No public DTO/schema/storage contract changed; no schema generation required.
No new tables, parent pending phases, OAuth engine, installers, UI, client intents,
or provider support were introduced. Existing Skills/MCP installations and
accepted A/B package/composer paths remain authoritative.

Native lock order remains installation lifecycle guard -> brief task-map lock;
completion/join happen outside that map lock and outside DB capacity. Policy
preparation holds its native lifecycle guard through the ordinary DB write;
publication and reload run after the write has returned. A plugin parent guard
and atomic owned user-override/removal contracts are **not implemented** yet.

## Concrete blocker and minimal next contract

[C1 prompt, section 6](/Users/alexander/Code/pioneer/pioneer-proposal/research-04/plugin-implementation-proposal-v2/stage-c1-implementation-prompt.md)
permits stopping the affected part when its API requires a larger mechanism.
An await on current cancellation or status cannot authorize package replacement:

- `agent/src/agent_loop.rs:975`: CancelTurn acknowledges admission **before**
  waiting for the root task. Its grace fallback joins an aborted root task.
- `agent/src/chat/mod.rs:306`: dropping a model tool join wrapper requests abort
  without awaiting the nested task; joining the root does not join that task.
- `tools/src/runtime.rs:404`: dispatch cancellation waits a one-second grace,
  then drops dispatch. The one-shot `Child` in `handlers/shell.rs:586` is local to
  that future; its later process wait cannot be recovered by a Gateway await.
- `handlers/shell.rs:98`: persistent-session Drop uses try-lock and kill signalling,
  not an acknowledged async process wait. Root graph cancellation at
  `message/agent_runtime.rs:7437` also signals descendants and tolerates cleanup
  errors; a durable cancelled graph is not proof of execution completion.

Minimal next step: extend the **existing native execution owner** to provide an
explicit stop-and-wait result for host-derived selected-plugin execution. Preserve
standalone cancellation's legacy branch. Retain ownership through dispatch grace
expiry/forced root retirement/recovery handoff; await nested tool and shell
process cleanup (including persistent sessions), then expose actual completion.
Timeout/owner loss must fail closed rather than treating NoActiveTurn, a terminal
DB status, or a cancellation count as success. Reuse native graph cancellation
and require its descendants' acknowledgements. No plugin jobs, leases, operation
polling, or new coordinator should be needed. The attempted broader Agent/shell
tracking prototype was removed; only the completed MCP changes above are delivered.

Consequently parent disable/update/remove, stable-root swap, bounded package
pending/replay, retry/repair, native owned overrides/OAuth actions, client intents
and desktop management remain **C1 work**, not deferred to C2. No package files
were changed via an unconfirmed stop. Their source-tracing acceptance scenarios
are NOT_IMPLEMENTED here. After that contract, implement the C1 orchestration and
UI against genuine A installers and current OAuth. C2 providers/continuations,
D mobile/native contracts, and E separately authorized testing remain outstanding.

## Actual checks

All commands ran in the worktree above. Own test targets **NOT_COMPILED**;
all source tests **NOT_RUN**. No app, MCP fixture, migration or functional/device
scenario was executed.

- Stable final implementation `a312c27e`: `CARGO_INCREMENTAL=0 cargo check -p pioneer-gateway --lib`:
  exit **0**, 7m57s; log `/tmp/pioneer-c1-a312c27e-gateway-check.log`.
  One dead-code warning: `set_mcp_policy` is not yet consumed by C1 orchestration;
  the RPC already shares its underlying native preparation/publication operations.
- `rustfmt --edition 2024 --check crates/gateway/src/message/mcp/policy.rs crates/gateway/src/mcp_service.rs`:
  exit **0**, `a312c27e`.
- `rustfmt --edition 2024 --config skip_children=true --check crates/gateway/src/message/tests.rs`:
  exit **0**, `a312c27e`.
- `git diff --check 39eb03b79eb8685002c00db5ee51120528d0edfa..HEAD`:
  exit **0**, `a312c27e`.
- Earlier same non-test cargo command: `/tmp/pioneer-c1-native-policy-check.log`
  exit **101** (discarded prototype referenced a private UnifiedExecHandler path);
  `/tmp/pioneer-c1-native-check-2.log` and `/tmp/pioneer-c1-native-stop-check.log`
  exit **0** on intermediate trees containing that subsequently removed prototype.
  `/tmp/pioneer-c1-final-gateway-check.log`, `/tmp/pioneer-c1-final-gateway-check-2.log`
  and `/tmp/pioneer-c1-38a2761b-gateway-check.log` exit **0** on subsequent intermediate
  trees. These are compilation observations, not final behavior evidence.

Regression sources: expanded `mcp_list_empty_then_install_stdio_persists_redacts_and_notifies`
compares direct business/RPC native policy and ID/audit preservation;
`cancelled_stop_retains_native_handle_until_a_retry_observes_completion` covers
caller cancellation, OAuth recovery exclusion and a still-pending retry;
`delayed_stop_does_not_remove_a_replacement_runtime_with_the_same_installation_id`
covers stale stop identity. All **NOT_RUN / NOT_COMPILED**.

Known external activity: the B handoff's historical UNKNOWN_EXTERNAL_ACTIVITY
(`cargo check --tests`, unknown owner/outcome) is external evidence, not a C1
command or a testing result. No conclusion is drawn from it.
