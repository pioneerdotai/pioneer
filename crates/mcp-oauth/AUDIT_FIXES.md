# MCP OAuth audit corrections — 2026-10-01

Working tree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/mcp-oauth`,
branch `feature/mcp-oauth`. Implementation base:
`ae814a827ef8b9fb0a4a932a26fbfe39d14f36dc`. Existing documentation-only HEAD
`38ef8dc0a2942df3364c0b2aedcfd7f226f47932` is preserved. These corrections
are uncommitted. No tests have been executed for this correction pass.

## Findings and concrete changes

| Finding | Correction | Principal files |
| --- | --- | --- |
| F1 | Callback validates owner/state/issuer/deadline and consumes the code once, then accepts immediately. An OAuth-service-owned task performs exchange/storage. RPC acceptance is not durable authorization. Exchanging and terminal notifications report completion; the browser returns 202 with acceptance copy. Cancel on the same sequential WebSocket reader interrupts the owned task. | `src/service.rs`, protocol `mcp.rs`, Desktop `platform/mcp_oauth.rs`, eight Desktop locales, `schemas/mcp_oauth_response.json` |
| F2 | The actual blocking mutation owns a strong refresh/file lease, its shared IO owner and a completion signal. Independently created persistence adapters share ownership by runtime home/store. Cancellation of an async waiter cannot detach completion from the registry. Cleanup waits the lease and rereads under it; GC drains writes before enumeration; shutdown drains owned tasks and IO. | `src/store.rs`, `src/service.rs` |
| F3 | Full registration retains the assigned DCR auth method. Per-manager SDK HTTP adaptation applies client_secret_post at the exact discovered token endpoint. Metadata is not rewritten; grants, PKCE, resource binding, redirects, exchanges and refresh remain rmcp 3.5.0. Pre-registered confidential clients can explicitly configure token_endpoint_auth_method. Unsupported/inconsistent methods have safe diagnostics. | `src/network.rs`, `src/store.rs`, `src/service.rs`, MCP `domain.rs`, `config.rs`, `oauth.rs` |
| F4 | Configure carries a failed listener preparation as a safe install flag. Public/stdio installs still proceed; an actual protected challenge produces an actionable terminal preparation failure, without a URL. Sign-in prepares again. | Client `mcp/operations.rs`, protocol `mcp.rs`, Gateway `message/mcp/install.rs`, `src/service.rs`, Desktop OAuth copy and install schema |
| F5 | Operation UUID, initiating client, deadlines and cancellation exist before Preparing/discovery. One operation monitor covers discovery, registration, backoff, callback and exchange. Cancel does not wait for the actor/network. Expiry/disappearance produce terminal events; no late browser event survives cancellation. Callback TTL never extends the original operation deadline. | `src/service.rs`, Client/Desktop existing operation consumers |
| F6 | Config Update no longer releases a relay by name. Equal OAuth identity (including timeout-only updates) keeps operation/listener/dedup state. Backend identity replacement retires the old operation with its UUID. Client fences retirement events against a newer operation; uninstall and session/epoch fencing still release relays. | Client `mcp/oauth.rs`, `src/service.rs` |
| F7 | The OAuth event boundary creates a Maintenance-scoped service handle. Restart, runtime actor, catalog persistence, audit, notification lease checks and shutdown carry that scope. Existing foreground handles and global sequential RPC ordering are unchanged. | Gateway `mcp_oauth.rs`, `mcp_service.rs`, `auth/service.rs` |
| F8 | Transient transport refresh failures are reported to the shared service manager. Recovery uses that manager/SDK refresh and emits a distinct Recovered transition only after failure. Normal rotation emits no restart event. Gateway serializes recovery through the live actor and restores only its OAuth degradation, retaining unrelated reasons/generation. Management/details projections refresh; failed tools are not replayed. | `src/service.rs`, MCP `rmcp_adapter.rs`, `oauth.rs`, Gateway `mcp_oauth.rs`, `mcp_service.rs`, Client `mcp/notifications.rs` |
| F9 | Clear sign-in/account is available in non-active failed/denied/timed-out/AuthRequired states, including saved redirect mismatch. Clearing uses the authorized disconnect/cleanup path; fresh sign-in can register again. Temporary recovery and terminal failures have different copy. | Desktop-MCP `src/oauth.rs`, eight Desktop-MCP locales |

All paths in the table are relative to their named crate. `src/` without a
crate prefix means `crates/mcp-oauth/src/`. No primary SQLite migration was
introduced. No tokens, client secrets or verifier fields were added to management
DTOs, bridge configuration or FFI projections.

## Cancellation and actual IO ordering

A callback operation first validates and marks its code consumed under the entry
actor. Its accepted RPC releases the production connection queue. Exchange then
holds the shared rmcp manager plus refresh lease, rereads persisted credentials
**after** acquiring the lease, and snapshots the prior atomic record. Cancel
fences the entry immediately and queues service-owned cleanup, without awaiting
network or actor completion in the RPC.

If cancellation happens while `put_string` is already executing, the blocking
closure still owns the lease. It cannot be undone by dropping its async receiver.
The exchange owner drains actual writes and restores the pre-exchange record
under the same lease before releasing it. A concurrent rotation cannot slip
between this snapshot and rollback. Replacement/disconnect wait for the retired
actor and the per-installation lease, then reread/delete; independent GC drains
the shared completion registry before enumerating records and deletes under the
same lease. Thus neither an absent pre-commit read nor cancellation of an earlier
drain lets an old put run after final cleanup and resurrect its identity.

Guards span network/keystore ownership, but no SQLite permit or transaction does.
An OS/keystore blocking call cannot safely be force-killed: its owned completion
must return before final cleanup/shutdown can complete. Cancellation still frees
the RPC queue and suppresses Authorized/browser effects immediately. This is an
explicit ownership guarantee, not a promise to interrupt arbitrary blocking IO.

Shutdown fences entry tokens, prevents registration of new tasks under the task
registry mutex, joins owned work and drains the shared IO owner. Registration,
callback and late completion cannot authorize a retired installation.

## Prepared regressions — NOT RUN

- Production sequential WebSocket: accepted callback at blocked token endpoint,
  same-connection Cancel, terminal cancellation and no persisted token.
- Barrier **inside actual put**: async caller abort followed by replacement,
  independent delete/GC and independent GC enumeration before commit.
- AS advertising Basic and POST, assigned DCR POST: endpoint enforces method for
  first exchange and restored refresh; explicit pre-registered POST also restores.
- Configure with listener preparation failure: install remains valid, protected
  challenge surfaces the error, public/stdio have no OAuth presentation or URL.
- Cancel during actual registration; operation UUID already in Preparing;
  deadline and disappearing initiator publish terminal events without a browser.
- Equal config/timeout update keeps callback and one browser effect; retired
  operation cannot cancel a replacement client presentation.
- Maintenance-scoped restart through the actual actor/catalog/audit paths, with
  query_only reader pool, read-class observations and writer-class observations.
- Failed token endpoint on a live tool call → successful background recovery,
  credentials retained and no tool replay/additional browser; recovery event once
  and none for normal rotation; Gateway generation and unrelated degradation kept.
- Saved redirect mismatch → clear persisted record → fresh registration/sign-in.
- Browser socket response acknowledges callback acceptance (202), not saved login.
- Successful public connection retires the volatile initial intent without later
  false OAuth timeout UI; client auth method configuration validation/redaction.

The prior audit fixtures were updated for asynchronous acceptance: assertions on
durable success/failure wait for terminal events, never equate an accepted RPC
with a saved token. The token endpoint now verifies assigned client authentication;
its old incorrect Basic expectation was removed rather than blessed as behavior.

## Extra corrections within OAuth scope

A successful public MCP connection now retires an unused volatile install intent
without a terminal OAuth failure later. Successful catalog reconciliation also
supersedes the saved OAuth degradation snapshot, so a delayed recovery cannot
overwrite a newer runtime status. Consent now uses a separate temporary SDK state store; early exchange failures
delete known PKCE state and discard the failed manager, so the SDK cannot offer a
freshly cached but unsaved grant to a new MCP connection. Shutdown checks the
task registry and binding boundary to prevent new operation tasks from appearing after it has drained work.

## Deferred commands — only after acceptance AND separate permission

These commands are proposals, not executed validation. Each selects an OAuth
integration target/module or a concrete Gateway scenario; verify nonzero test
counts at execution time. No workspace-wide suite is proposed.

```sh
cargo test -p pioneer-mcp-oauth --test oauth_lifecycle
cargo test -p pioneer-mcp-oauth --lib store::tests
cargo test -p pioneer-mcp --lib config::tests::native_oauth_client_auth_method_is_preserved_and_validated -- --exact
cargo test -p pioneer-mcp --lib config::tests::mcp_remote_commands_and_legacy_http_auth_keep_existing_configuration -- --exact
cargo test -p pioneer-mcp --lib oauth::tests
cargo test -p pioneer-client --lib mcp::oauth::tests
cargo test -p pioneer-client --lib mcp::operations::tests::configure_preparation_failure_survives_install_until_protected_challenge -- --exact
cargo test -p pioneer-client --lib mcp::notifications::tests
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::callback_acceptance_frees_same_websocket_for_cancel_during_exchange -- --exact
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::oauth_scoped_restart_keeps_catalog_and_audit_on_maintenance_writer -- --exact
cargo test -p pioneer-gateway --lib mcp_service::tests::oauth_recovery_keeps_session_generation_and_unrelated_degraded_reason -- --exact
cargo test -p pioneer-desktop --bin pioneer-app platform::mcp_oauth::tests
cargo test -p pioneer-gateway --lib mcp_secrets::tests
cargo test -p pioneer-gateway --lib authorization::registry::tests::oauth_rpc_requires_installation_management_and_keeps_capability_disclosure -- --exact
```

See [VALIDATION.md](VALIDATION.md) for checks actually performed on the correction
pass. Earlier test results are historical and do not validate this corrected diff.

## Second audit R1–R6 — unaccepted, uncommitted corrections

The initial F1–F9 changes above are preserved. This section describes the later
corrections against the 97-path reaudit snapshot, not behavioral test results.

| Finding | Correction | Principal files |
| --- | --- | --- |
| R1 | ActiveFlow has one terminal decision shared by callback completion and Cancel. Success retires cancellation admission before any await or refresh-lease release. Accepted cancellation/expiry drains the actual put and rolls back the prior atomic registration/grant under that same lease. SDK readers see the previous consent baseline until success; independent adapters wait the refresh/file lease. | `src/service.rs`, `src/store.rs`, `tests/oauth_lifecycle.rs`, Gateway `message/tests/mcp_oauth_rpc_queue.rs` |
| R2 | A short observable projection holds state/operation UUID separately from the network actor. Details and event validation never wait for discovery, registration or exchange. Validation still checks durable identity, entry generation, operation UUID, owner, cancellation and both deadlines; retired terminal events require a recorded envelope. | `src/service.rs`, Gateway queue regressions |
| R3 | Explicit consent intent records its own retry reason, independent of a received MCP challenge. Discovery/DCR retries keep the original UUID, deadlines, cancellation and backoff. No intent is reconstructed on startup. | `src/service.rs`, `tests/oauth_lifecycle.rs` |
| R4 | Scoped shutdown requests govern all old actor branches (startup, live, AuthRequired and backoff), including status/notification lease reads. OAuthRecovered carries the Maintenance store and an acknowledged, cancellable completion. Foreground handles remain Interactive. | Gateway `mcp_service.rs`, `mcp_oauth.rs`, queue/scope regressions |
| R5 | A per-scope/name lifecycle admission gate serializes install/policy/uninstall/reload and OAuth effects. After notification awaits, the effect rereads the durable row and verifies UUID, OAuth identity/generation and operation. Stop/start revalidates after join; recovery holds admission through the actor acknowledgement. | Gateway `mcp_service.rs`, `mcp_oauth.rs`, `message/mcp/{install,policy,uninstall}.rs`, queue regressions |
| R6 | Default browser opening uses existing pinned hardened webbrowser. The launcher runs on bounded, tracked Client browser workers, outside the event dispatcher/action queue and their observable/listener mutexes. Atomic browser admission fences retirement before launch; failed launch keeps an existing relay for explicit retry. URL-bearing library logging is suppressed. | Desktop `platform/mcp_oauth.rs`, Client `mcp/{oauth,operations}.rs`, Desktop-MCP `{oauth,catalog}.rs`, Client action schema |

### Terminal decision and grant visibility

The exchange owns the manager mutex and refresh/file lease, rereads the atomic
record and stages its previous credentials as the consent baseline. rmcp performs
exchange and durable save. A successful put alone does not authorize the operation.
Under the short ActiveFlow decision mutex, success checks monotonic and wall-clock
deadlines, respects an already accepted Cancel/timeout and removes cancellation
admission. Under the still-held lease, a separate atomic commit then promotes the
durable pending candidate and clears baseline visibility; only after that succeeds
is Authorized emitted. This is the success linearization point.
The later PKCE deletion/event awaits cannot reopen Cancel admission.

If Cancel or timeout wins instead, the exchange drains actual blocking I/O and
restores the complete prior record before releasing its still-owned lease. Scope
upgrade cancellation therefore retains the previous grant. The baseline also
prevents an existing SDK session manager from reading the newly written grant
before that decision. Independently created adapters acquire the same refresh/file
lease for credential loads. If rollback fails, the operation reports safe storage
failure and discards its manager. The atomic record still contains a durable
pending candidate and the complete previous record; credential readers use only
the previous record, including independent adapters and restart. Bind retries
pending rollback under the file lease and reports failure if storage still fails; it does not
report durable success. Replacement, disconnect and GC keep the F2 actual-I/O
ownership/drain rules. No SQLite capacity spans these operations.

### Observation and runtime fencing

Preparing is published with its operation UUID without holding the observable
projection lock over discovery/registration. Event validation reads that short
projection and ActiveFlow instead of the long actor. Details remains available
during preparation and exchange, so the sequential WebSocket can subsequently
process Cancel. Recorded retirement envelopes are bounded (1024), contain no
credentials, and only authorize terminal relay-release notifications.

Runtime admission occurs after recipient authorization/notification awaits.
Its lifecycle gate owns no database permits. Durable mutations use that same
keyed gate, so UUID/resource replacement cannot cross an admitted stop/join/start.
The effect rereads the row and rechecks entry identity/generation/operation; after
joining an old actor it rechecks OAuth currency again before starting/publishing.
Notification delivery also admits and rereads under this gate, preventing an old
URL from crossing durable replacement during recipient authorization/fanout.
Recovery carries a scoped Maintenance store and transferable lifecycle admission
to the existing actor, and awaits completion (bounded to 30 seconds). Waiter drop
or timeout releases unclaimed admission; the actor-owned coalesced mailbox retains
recovery and reacquires admission with shutdown fencing if needed. An actor that
already took it retains ownership across its actual status/fanout awaits.
An unrelated degradation is preserved and a healthy refresh causes no restart.

Browser admission is an atomic claim, not a lock held around an OS launcher.
Epoch/operation retirement remains immediate even if that launcher is slow.
Retirement before the claim prevents launch; an already admitted OS launch cannot
be recalled. Its late result cannot recreate a retired Client presentation or
listener. OS waiting runs on dedicated tracked workers, never the shared Client
event dispatcher or MCP action queue. Manual retry requires the existing live pending relay; it never opens a
fallback URL without a listener or prepares a replacement redirect implicitly.

### R regressions — NOT RUN

- Successful-put-before-decision barrier: Cancel wins, wall timeout wins and
  success wins; same sequential WebSocket acceptance/status, repeat bind/session,
  scope-upgrade prior-grant rollback and old/independent SDK reader visibility.
- Production event consumer: registration stalls, Preparing UUID reaches the
  authenticated wire client; Details then Cancel complete before provider release.
  Details-before-Cancel is also covered while exchange stalls.
- Fresh and post-Cancel explicit SignIn with no challenge: temporary discovery/DCR
  failure recovers with the original UUID and one URL; registration-only restore,
  Cancel and timeout during retry backoff are included.
- Real AuthService with finite authenticated management leases: old Interactive
  actor shutdown in live/startup/backoff/AuthRequired and live recovery use
  Maintenance physical reader/query_only and catalog/audit writer routes.
- Production consumer paused after notification: late Authorized, AuthRequired
  and Recovered versus same-name new UUID or same-UUID resource replacement must
  preserve the new runtime generation/state/reason. Equal-identity updates keep
  the prior F6 relay behavior.
- Injectable browser failure, dedup, actionable manual retry, expired/retired
  operation rejection; a blocked launcher cannot block observable projection or
  epoch retirement, and retirement before launch prevents OS dispatch.

Additional proposed exact commands, **not executed**:

```sh
cargo test -p pioneer-mcp-oauth --lib store::tests::pending_consent_is_hidden_from_existing_and_independent_sdk_readers -- --exact
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::preparing_is_delivered_and_details_does_not_block_cancel_on_same_websocket -- --exact
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::same_websocket_terminal_decision_after_durable_put_preserves_cancel_timeout_and_success -- --exact
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::authorized_event_cannot_restart_reinstalled_or_replaced_identity_after_notification_await -- --exact
```

The existing exact scope command and OAuth integration/client/Desktop module
commands above include the remaining R scenarios. Execute only after code
acceptance and separate permission; verify nonzero selected test counts then.


## Third audit S1–S5 — unaccepted corrections, tests NOT RUN

The checked 98-path R snapshot matched exactly before this pass. F1–F9 and R1–R6
are retained. The changes below correct the remaining interleavings.

| Finding | Concrete correction | Files |
| --- | --- | --- |
| S1 | OAuth RPC captures the initial configuration, acquires the existing lifecycle gate, rereads the durable row and requires the same UUID and OAuth configuration identity. It uses the freshly admitted row. Callback/Cancel only inspect an existing binding and never synchronize/rebind; SignIn creates owned consent under admission. Disconnect clears and restarts the exact admitted row without nested gate acquisition. | Gateway `message/mcp/oauth.rs`, `mcp_service.rs`; OAuth `src/service.rs` |
| S2A | Ordinary CredentialStore clones have independent lease ownership; only the exchange-specific deadline adapter explicitly receives its owner's lease. Reads recheck baseline after actual async record read and sanitize durable pending records. A pre-stage reader cannot borrow another SDK manager's Weak lease to see a candidate. | OAuth `src/store.rs`, `tests/oauth_lifecycle.rs` |
| S2B | SDK consent save writes an atomic pending candidate plus full previous registration/grant. The candidate becomes ordinary credentials only after terminal success wins and an owned atomic commit succeeds. Failed rollback leaves the persistent pending marker, so independent readers and restart see only the previous grant. Bind performs lease-protected pending recovery; permanent storage failure remains a failure. | OAuth `src/store.rs`, `src/service.rs`; Gateway secret fixture |
| S3 | Automatic opening and manual retry enqueue bounded tracked browser workers. Dispatcher/action worker return after admission; effects reduce later with operation/epoch fences. Workers retain the shell/relay, are tracked across epoch retirement, reaped after completion and joined at owner destruction. Busy/capacity admission prevents competing retries; hardened webbrowser remains unchanged. | Client `mcp/oauth.rs`, `core.rs`; test-only ingress/RPC hooks in `runtime.rs`, `transport/ws/runtime*` |
| S4 | OAuth failure/recovery revisions travel in internal events; new failures invalidate old recovery currency. The queued actor command also captures its actor failure revision and rechecks both revisions before changing status. Transferable lifecycle admission, Maintenance store and timeout cancellation remain owned. | OAuth `src/service.rs`; Gateway `mcp_service.rs` |
| S5 | Replacement/RPC-race scenarios install enabled queue and verify Ready before targeting the barrier. Disabled cancellation controls still assert Disabled with no runtime generation. A separate successful consent case confirms a disabled server is never started. Release guards unblock controlled token/effect/clock waits on fixture exit. | Gateway `message/tests/mcp_oauth_rpc_queue.rs` |

### Persistent pending/commit ordering

The service snapshots the prior record after acquiring refresh ownership and
stages its in-memory baseline. SDK token save writes **pending_consent** in that
same atomic secret record: full prior record plus candidate token response.
Ordinary credentials still represent the prior grant. Record readers sanitize
pending data, and load rechecks baseline after asynchronous I/O. Cloning a store
for another SDK manager does not clone lease reentrancy.

Success wins the short terminal decision before cancellation admission closes.
The exchange owner then commits the candidate under its retained refresh/file
lease, clears the baseline after actual write completion, releases ownership and
emits Authorized. Commit failure reports Failed and leaves the pending candidate
unusable. Cancel/timeout winning the decision drains actual writes and attempts
prior-record rollback under the same lease. If rollback fails, independent readers
still see the prior record; restart/bind must recover pending state under the file
lease and cannot promote its candidate. No promise is made that permanently
failed storage can restore the prior record or that the provider still accepts
that old grant. Fresh bind owns cleanup retries; ordinary runtime retry supplies
bounded backoff without a browser. No keystore redesign or DB migration is added.

### Queue and recovery ownership

At most eight browser effects can be outstanding. Their handles remain owned
through epoch fencing; fencing retires admission and releases relays without
waiting for the OS. The shared event dispatcher and MCP action controller never
join a launcher. A short result reduction verifies epoch/flow/current phase;
late results cannot restore terminal state. Owner destruction joins outstanding
actual effects, which cannot be forcibly stopped once OS launch is admitted.

Recovery is associated with the OAuth revision observed by the event and the
actor failure revision captured before command enqueue. A new failure from tool
B invalidates queued recovery A even while the installation UUID/identity stays
unchanged. Currency is checked at actual command execution; only recovery B can
clear failure B. Normal rotation causes neither restart nor tool replay.

### Additional confirmed correction within OAuth scope

Rollback failure after accepted Cancel previously emitted Failed but the canceled
entry fence discarded that terminal event. Failed consent envelopes now use the
same bounded recorded terminal-delivery mechanism as cancellation; Client fences
all terminal flow IDs against newer consent. This delivers honest storage failure
and releases the old relay without authorizing runtime effects for a retired
installation.

### New/updated regressions — NOT RUN

- Two authenticated WebSocket connections; stale SignIn/Callback/Cancel/Disconnect
  blocked after first row read versus same-UUID resource replacement or reinstall.
  Current grant/operation/runtime remain intact and the deleted UUID has no record.
- Actual shared-adapter read begins before staging and completes after pending put;
  cloned/independent SDK readers cannot expose a candidate. The real prior SDK
  manager is started during a pre-stage store barrier and remains fenced through
  the post-put decision barrier for success/Cancel/timeout and scope upgrade.
- Rollback write failure after pending successful put and accepted Cancel/timeout:
  independent adapter, same-identity bind and service restart cannot use candidate;
  restoring storage recovers the complete prior record.
- Real Client dispatcher/action controller with blocked fake launcher: terminal
  and connection retirement, dedup, manual retry followed by Cancel, honest failure
  projection, relay cleanup and rejection of late completion before launcher exits.
- Real actor queue: recovery A behind blocked tool B; fresh failure B retains
  Degraded/error/generation until recovery B. Separate service revision coverage
  checks currency change after recovery admission.
- All six stale-event state × replacement cases have enabled queue prerequisites;
  disabled consent commits its account without starting runtime. Controlled awaits
  have fixture release guards.

Proposed exact commands after acceptance AND separate permission, **not run**:

```sh
cargo test -p pioneer-mcp-oauth --lib store::tests::pre_stage_actual_read_cannot_expose_candidate_before_terminal_commit -- --exact
cargo test -p pioneer-mcp-oauth --lib store::tests::failed_rollback_pending_record_survives_adapter_rebind_and_storage_recovery -- --exact
cargo test -p pioneer-mcp-oauth --test oauth_lifecycle terminal_decision_after_successful_put_linearizes_cancel_timeout_and_success -- --exact
cargo test -p pioneer-mcp-oauth --test oauth_lifecycle cancelled_or_timed_out_consent_with_failed_rollback_cannot_authorize_after_restart -- --exact
cargo test -p pioneer-mcp-oauth --test oauth_lifecycle recovery_revision_becomes_stale_when_a_new_session_refresh_failure_arrives -- --exact
cargo test -p pioneer-client --lib mcp::oauth::tests::production_dispatcher_and_action_queue_retire_while_browser_is_blocked -- --exact
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::two_connections_stale_oauth_rpc_cannot_rebind_or_clear_replacement -- --exact
cargo test -p pioneer-gateway --lib mcp_service::tests::queued_oauth_recovery_cannot_clear_a_newer_blocked_tool_failure -- --exact
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::disabled_consent_commits_account_without_starting_runtime -- --exact
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::authorized_event_cannot_restart_reinstalled_or_replaced_identity_after_notification_await -- --exact
```

Check nonzero selected counts only when separately permitted to execute. Compile
checks are not behavioral evidence. See VALIDATION.md for actual non-test checks.


## Fourth audit T1–T4

These corrections preserve F/R/S and the doc-only HEAD. **All regressions below
are NOT RUN.** Compile checks are not behavioral evidence.

| Finding | Correction | Files |
| --- | --- | --- |
| T1 | Short registry lookup/publication, owned weak-keyed bind gate per installation; old retirement/data/refresh/file/I/O waits occur outside the map. Suspend/disconnect use the same gate; publication rechecks shutdown. | OAuth `src/service.rs`; Gateway `message/tests/mcp_oauth_rpc_queue.rs` |
| T2 | OAuth owner returns a typed failure generation/revision before transport error delivery; rmcp error classification retains it. Actor recovery compares the actual degradation cause, accepting late delivery of the same failure and rejecting a newer one. Untagged errors retain the conservative counter fallback. | MCP `src/oauth.rs`, `runtime/mod.rs`, `client/rmcp_adapter.rs`, `lib.rs`; OAuth `src/service.rs`; Gateway `mcp_service.rs` |
| T3 | One pending recovery mailbox belongs to each runtime actor. Ack expiry releases unclaimed lifecycle admission, not recovery. Actor reclaims/reacquires interruptible admission, rereads durable UUID/configuration/event currency, then applies scoped Maintenance status. Stop/replacement/shutdown clear pending envelopes. | Gateway `mcp_service.rs` |
| T4 | Confirm mutation errors by exact value readback. Install a durable non-secret promotion fence before final mutation; retain previous account in the promoted record. Readers ignore a fenced candidate across adapters/restart. Known durable promotion plus confirmed fence removal is Authorized; unknown promotion remains protected and Failed. Unknown fence deletion is owned reconciliation, never falsely Failed for a potentially committed grant. | OAuth `src/store.rs`, `src/service.rs`, `tests/oauth_lifecycle.rs` |

### Ownership and linearization

Registry map ownership ends before any await on a per-installation actor, file
lease, storage or network. The per-ID bind owner fences retirement, drains the
old actor/real mutations, rereads identity and initializes projection before the
new generation is published. Other IDs remain observable/cancelable. Canceled
requests cannot release an actual write's retained lease; shutdown cannot publish
new entries. Failed-consent cleanup on same-identity bind can restore prior
credentials without canceling the old healthy SDK manager.

Recovery A is tagged by the OAuth failure it resolved, not by when rmcp delivered
its error to the actor. Cause generation/revision, event currency and durable
installation admission are checked at effect time. An actor counter is still
used when no protocol cause is available. A mailbox holds at most one pending
recovery and one actor-owned effect; newer admission coalesces older pending
work. No detached retry task or OAuth supervisor was added to Gateway. The
waiter's deadline is still 30 seconds. Its cleanup drops only unclaimed admission;
late delivery reacquires the gate with shutdown selection and reads through the
Maintenance handle. No DB capacity spans waiting, network, shutdown or fanout.

The terminal consent decision remains before promotion and closes Cancel
admission. Actual put ownership and the refresh/file lease cover both the account
and its non-secret `::promotion` fence. A returned write error proves neither
rollback nor non-mutation: exact readback can confirm the intended value. Before
promotion, the fence must be acknowledged/read-confirmed. Promotion retains the
previous complete record in `pending_consent`, with a backward-compatible
`committed` flag. While fenced, readers receive the old grant (or a storage error),
never the candidate. A known promotion and confirmed fence deletion produce
Authorized even if a preceding API call returned a post-mutation error. If
promotion cannot be confirmed, Failed leaves the fence intact; bind/restart
restore the prior record before removing it. If deletion itself has unknown
outcome, the owned exchange retries with cancellation-aware backoff and cannot
publish a false Failed. Retirement/shutdown may interrupt that reconciliation
without an authorization-success/failure notification for a retired operation.
A crash after confirmed promotion and actual fence removal restores that grant;
a remaining fence restores the prior grant. Permanent storage failure cannot
promise cleanup or that the provider still accepts the prior grant.

The account/registration remains one atomic record. The additional fence contains
no credentials, token fingerprint, code or verifier. GC maps the auxiliary key
back to its installation; disconnect/replacement remove both under the lease.
No primary DB migration, dependency upgrade, public DTO/schema/UI text change or
general keystore redesign was introduced. rmcp remains 3.5.0.

### Added regressions — NOT RUN

- T1: physical blocking read of installation A while B has accepted callback;
  production sequential WebSocket B delivers Details, accepts Cancel and delivers
  cancellation before A is released. B does not persist a usable grant. Fixture
  barriers are released on unwind.
- T2: real fake OAuth provider, lifecycle worker and Gateway event consumer enqueue
  recovery before a controlled error reaches the actor. Same generation and
  unrelated degradation survive; no tool replay/restart. A lower-layer transport
  fixture checks the cause survives delayed, sanitized error delivery. The prior
  recovery A/new failure B regression remains unchanged.
- T3: controlled ack deadline expires behind a successful blocked tool. Actor
  mailbox retains recovery, admission is available, delivery restores projection
  after the tool. Actor replacement/shutdown clear pending work; the replacement
  receives a new generation and no old error. No test clock dependency was added.
- T4: fake stores write through the delegate before returning Err; pending save
  and final promotion cover before-mutation, after-mutation and readback failure.
  Independent CredentialStore load, same-identity bind, restart and storage
  restoration expose only a proved committed grant or the prior grant/None.
  Initial consent and an existing account are both covered. A delegate delete
  followed by Err plus failed fence readback verifies owned uncertainty resolution. Successful
  final readback/fence cleanup and terminal state agree. Earlier Cancel/timeout,
  scope-upgrade rollback and real blocking-write regressions remain selected for
  post-review execution, without weakened assertions.

Exact proposed commands, **only after acceptance and separate permission**:

```sh
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::other_installation_blocking_read_cannot_delay_same_websocket_details_and_cancel -- --exact
cargo test -p pioneer-gateway --lib mcp_service::tests::real_oauth_recovery_before_actor_receives_notified_error_restores_projection -- --exact
cargo test -p pioneer-gateway --lib mcp_service::tests::recovery_mailbox_accepts_late_delivery_of_the_same_failure_cause -- --exact
cargo test -p pioneer-gateway --lib mcp_service::tests::recovery_mailbox_survives_ack_deadline_and_retires_on_replacement_or_shutdown -- --exact
cargo test -p pioneer-gateway --lib mcp_service::tests::queued_oauth_recovery_cannot_clear_a_newer_blocked_tool_failure -- --exact
cargo test -p pioneer-mcp --lib client::rmcp_adapter::tests::oauth_failure_cause_survives_sdk_transport_error_classification -- --exact
cargo test -p pioneer-mcp --lib oauth::tests::notified_failure_cause_survives_delayed_transport_error_delivery -- --exact
cargo test -p pioneer-mcp-oauth --lib store::tests::uncertain_post_mutation_fence_delete_is_owned_until_readback_resolves -- --exact
cargo test -p pioneer-mcp-oauth --lib store::tests::post_mutation_errors_cannot_expose_failed_consent_to_independent_sdk_readers -- --exact
cargo test -p pioneer-mcp-oauth --test oauth_lifecycle consent_pending_and_promotion_post_mutation_errors_match_durable_outcome -- --exact
cargo test -p pioneer-mcp-oauth --test oauth_lifecycle terminal_decision_after_successful_put_linearizes_cancel_timeout_and_success -- --exact
cargo test -p pioneer-mcp-oauth --test oauth_lifecycle cancelled_or_timed_out_consent_with_failed_rollback_cannot_authorize_after_restart -- --exact
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::authorized_event_cannot_restart_reinstalled_or_replaced_identity_after_notification_await -- --exact
```

## Fifth audit U1–U2

These changes retain F/R/S/T, the doc-only HEAD and the test-execution prohibition.

- **U1 — external lifecycle ownership:** `service.rs` admits bind, provider.client,
  suspend, disconnect and GC through a short synchronous admission counter. Nested
  callers do not contend with a queued global writer. Admission closes before
  root cancellation; shutdown waits accepted callers, then owned tasks, then the
  actual persistence mutation registry. A serialized shutdown owner also prevents
  a second shutdown from returning while the first owns a batch of joins. No
  entries-map lock spans these waits. A cleanup paused in physical read can finish
  its uncancelled rollback/delete only before its caller releases admission and
  shutdown reaches the final drain. Dropping its future prevents subsequent async
  mutation admission; any already-started blocking write remains in the IO registry.
  Gateway actor connect now participates through provider.client, so the existing
  OAuth-shutdown → actor-stop/join ordering cannot strand a later cleanup write.
- **U2 — honest resolution phase:** protocol and service add `Resolving` after the
  consent terminal decision wins and before promotion. ActiveFlow retains that
  decision while reconciliation runs. Cancel reports that the decision already
  completed; Desktop exposes Clear sign-in instead of an ineffective Cancel or
  another SignIn. Client retires browser admission and releases the callback relay
  immediately on Resolving. No URL/secrets are added to this state.
  The operation monitor retains the original wall/monotonic deadline and initiating
  client availability; it rechecks the consent winner at the admission mutex,
  including a decision racing with the availability await. Expiry/client loss
  retires the resolution owner, not the successful consent decision: no false
  Failed/TimedOut/Cancelled or late Authorized is emitted. Temporary SDK state,
  manager and flow are released on retirement. The last observable state remains
  Resolving and keeps Clear sign-in available. Explicit clear/replacement cancels
  the owner before waiting for its data/lease; shutdown joins it. Readback recovery
  either confirms promotion and emits Authorized for a current operation, or bind/
  restart resolves the existing durable fence/envelope without replaying consent.

The UI uses the existing inline label and named GPUI Button, with no modal,
layout/token or browser changes. Resolving copy says sign-in remains unconfirmed
and offers clearing it to try again; all eight Desktop MCP locales are
updated. Shared protocol/Client schemas include the new state. Existing FFI
consumers deserialize the shared enum; no separate OAuth FFI enum is introduced.
Schema generation is isolated and unrelated pre-existing schema drift is excluded.

### New regressions — NOT RUN

- Physical pending-record read is blocked before first cleanup mutation; shutdown
  cannot finish, new admission is refused, release lets the accepted caller drain
  rollback/delete before final shutdown, and no mutation can start after return.
- Production Gateway actor connect invokes the actual provider.client; OAuth
  shutdown remains pending with that actor registered until physical read releases,
  then completes actor stop/join and publishes Stopped.
- Actual fence delete mutates storage then returns Err; marker readback stays
  unavailable. Resolving is visible and Cancel cannot reverse the decision.
  Independent reads cannot use an unconfirmed grant. Coverage includes wall-clock
  expiry, initiating-client disappearance, management clear while reads fail,
  resource replacement, shutdown, storage restoration and restart. No old flow
  emits a terminal failure/success after retirement.
- Client Resolving retires browser admission, releases the relay, preserves a
  URL-free actionable projection, rejects browser retry and fences on disconnect.

Exact deferred commands — only after code acceptance and separate permission:

```sh
cargo test -p pioneer-mcp-oauth --test oauth_lifecycle shutdown_waits_for_admitted_bind_cleanup_before_final_mutation_drain -- --exact
cargo test -p pioneer-gateway --lib mcp_service::tests::gateway_shutdown_waits_for_oauth_connect_caller_before_actor_stop_join -- --exact
cargo test -p pioneer-mcp-oauth --features test-support --test oauth_lifecycle uncertain_promotion_is_manageable_and_retired_without_false_terminal_outcome -- --exact
cargo test -p pioneer-client --lib mcp::oauth::tests::resolving_releases_relay_and_preserves_actionable_uncertain_projection -- --exact
```

Permanent storage failure cannot guarantee clear completion or validity of a prior
provider grant. Actual blocking IO cannot be forcibly interrupted; shutdown/clear
wait its owned completion. Expired/disconnected uncertain resolution leaves an
honest management state until clear, rebind or restart can reconcile readable
storage; it never invents a terminal authorization failure for a possibly committed
grant. None of these scenarios has been executed.

## Sixth audit V1–V3

**Code remains unaccepted. Every regression in this pass is NOT RUN.** Earlier
F/R/S/T/U behavior and doc-only HEAD are preserved.

- **V1:** `service.rs` sets the short observable Resolving projection while
  reserving the Authorized consent decision under the same active-flow mutex.
  There is no await between decision and projection. The monitor can retire only
  after that publication; later Resolving event emission explicitly permits a
  cancelled resolution owner. Gateway still validates durable UUID/configuration,
  entry generation, revision and flow at delivery. Replacement cannot deliver a
  stale Resolving. The test barrier is after this atomic linearization and before
  event emission: that is the remaining scheduler boundary, not an artificial
  await inside the decision lock.
- **V2:** a separate addressed `Retired` notification resets presentation without
  changing the consent outcome. The old entry retains safe presentation owner
  metadata (flow/client/workspace), so reset still reaches its initiating client
  after reconciliation has cleared temporary intent. Genuine bind replacement,
  suspend and disconnect emit reset, including public/header/stdio transitions.
  Equal identity/timeout-only live updates use the existing bind fast path and
  emit no reset. Known retired-event proof allows delivery after same-UUID resource
  replacement; Gateway revalidates durable UUID and management rights and sends
  only to the old initiator. No URL, code, token or verifier is attached.
  Client removes only the exact matching flow, retains a bounded session-scoped
  retirement fence, rejects late old Resolving, and preserves an already-started
  new presentation. Removing the old presentation restores Desktop precedence to
  current management details. Details rejects OAuth state from a binding with a
  different current configuration. Retired is a control signal, never rendered
  or used as a runtime effect; no new user-facing copy/locales are required.
- **V3:** the six uncertain-outcome scenarios wait actual exchange completion,
  cleared intent/flow/managers/active operation, released local refresh gate and
  actual shared file lease. `bound_to_installation` keeps its configuration-only
  meaning. The completion guard drops after exchange resources and retains the
  exact flow ID. No arbitrary sleep substitutes for the deadline/client-loss
  ownership assertion; all terminal-state and durable-recovery assertions remain.

Test-only ownership/publication hooks are behind OAuth `test-support`; Gateway
`oauth-test-support` enables that feature for its production-consumer fixture.
They are absent from default production builds. These features add no dependency,
OAuth algorithm, secret projection or Gateway supervisor. Regression selection
below explicitly enables them; the six-case uncertain-promotion target requires
`test-support` so its package/name-filtered command must include that flag.

### Regressions — NOT RUN

- Winner reserved/projection committed → notification barrier → original deadline
  or initiator disappears → monitor retires owner → Resolving is still observable
  and emitted → actual exchange/temporary-state/leases finish → no late consent
  terminal outcome. Clear remains usable.
- Real Gateway event consumer with local fake OAuth AS delivers current Resolving
  to a live deadline-expired initiator; a disconnected initiator receives no new
  effects, while current management state remains Resolving. Two registered
  management clients then replace auth identity: A receives only its addressed
  reset; B receives its own new authorization URL.
- Backend reset covers private replacement, header auth, stdio/mcp-remote, public
  resource replacement, suspend and clear, with unchanged timeout identity control.
- Client reducer clears old Resolving, rejects delayed old signals, preserves new
  flow on delayed retirement, and frees presentation precedence/relay. Existing
  equal-identity Update listener/dedup regression remains selected.
- All six existing uncertain-promotion cases remain: normal recovery, deadline,
  initiating-client loss, clear, replacement and shutdown, with real ownership and
  lease observations for the deadline/client-loss cases.

Exact deferred commands — **only after acceptance and separate permission**:

```sh
cargo test -p pioneer-mcp-oauth --features test-support --test oauth_lifecycle winner_retirement_before_notification_preserves_resolving_projection -- --exact
cargo test -p pioneer-mcp-oauth --features test-support --test oauth_lifecycle resolving_retirement_targets_old_client_and_preserves_equal_identity_flow -- --exact
cargo test -p pioneer-mcp-oauth --features test-support --test oauth_lifecycle uncertain_promotion_is_manageable_and_retired_without_false_terminal_outcome -- --exact
cargo test -p pioneer-gateway --features oauth-test-support --lib mcp_service::tests::retired_winner_delivers_resolving_and_addressed_reset_through_gateway_consumer -- --exact
cargo test -p pioneer-client --lib mcp::oauth::tests::addressed_retirement_clears_old_presentation_without_touching_new_flow -- --exact
cargo test -p pioneer-client --lib mcp::oauth::tests::config_update_preserves_listener_and_does_not_repeat_browser_effect -- --exact
```

An unavailable initiator cannot receive a notification; connection-epoch retirement
and current authorized management details remain the recovery route. Permanent
storage failure still cannot guarantee cleanup, and actual blocking IO cannot be
forcibly cancelled. Retired-event/presentation fences are bounded to 1024 entries
per service/client lifetime; generation, durable identity and session epoch checks
remain independent of those caches. Real providers/browser/UI interaction were not
executed.

A related retirement boundary was tightened: a background poll that won its
outer select before cancellation and then waited for EntryData now rechecks
entry currency after acquiring it. It cannot recreate a manager or overwrite the
retired Resolving projection after exchange cleanup. This is scoped to OAuth
ownership; no generic Gateway worker behavior changed.

## Seventh audit W1 — management cleanup survives a failed Clear

**Code is not accepted. Every regression below is NOT RUN.**

Retiring consent and removing its account now have separate outcomes. Disconnect
keeps the cancelled entry under per-installation admission until account/fence
deletion succeeds. It drains the old exchange through EntryData, clears temporary
state/managers, retires its addressed presentation, and acquires local refresh and
shared file/write ownership. A delete error (including a post-mutation error)
leaves a non-usable `CleanupRequired` projection bound to the current configuration.
A managed clear without an existing runtime entry can create only this cancelled
management holder, without restoring credentials or registering a client. Shutdown
caller admission remains owned; no new holder is published after shutdown admission
closes. Equal-configuration bind rejects this holder until Clear succeeds; a real
identity replacement still drains/cleans the old identity before publication.

The error notification uses a new management presentation UUID, distinct from the
retired consent flow; it has no callback, browser URL or secret. Details can read
this short projection without storage IO, including for header-auth cleanup. Client
keeps it actionable, fences late old retirement, and refreshes Details after failed
Disconnect even if no notification arrived. A subsequent Clear retires precisely
this management presentation. Only successful deletion removes the holder. New
consent is not resumed and no consent terminal outcome is invented.

OAuth delete reinstalls its durable promotion fence inside the owned blocking
write before deleting the account. A leftover promoted pending record therefore
exposes only the previous grant to independent readers when storage becomes
readable; an absent account exposes none. Failure installing the fence, deleting
the account, or deleting the fence remains an error, never acknowledged cleanup.
The actual closure owns the refresh/file lease until completion; SQLite capacity
is not held across IO, waits or notifications. Permanent inability to write/read
storage cannot guarantee cleanup or repair an already-uncertain durable outcome.

Desktop renders localized `CleanupRequired` guidance and offers Clear; SignIn and
Cancel are unavailable in this cleanup phase. All eight Desktop MCP languages and
the eight shared schema snapshots were updated. DTOs contain only the new safe
enum value and management presentation UUID; existing FFI consumes the shared DTO.

### Regressions — NOT RUN

- Actual Gateway sequential WebSocket: unresolved promotion → Clear → account
  delete failure before mutation, partial account/fence deletion, or error after
  actual mutation → old Retired plus RPC error → fresh CleanupRequired and Details
  → storage recovery → second Clear on the same installation. Checks no browser,
  no late Authorized, distinct management UUID, account/fence removal and disabled
  runtime generation remaining zero. Fixture exit releases provider/storage barriers.
- Independent persistence/restart reader sees prior grant behind the reinstalled
  fence after failed account deletion; retry deletes account and fence.
- Client retirement/failure presentation stays visible, rejects browser retry, and
  old cleanup signals cannot remove or replace a new consent presentation.
- Desktop management action policy offers repeat Clear in Ready/AuthRequired/Degraded
  cleanup states without offering unusable Cancel or another SignIn. Existing
  identity/update/permissions regressions remain unchanged.

Deferred commands, **only after acceptance and separate user permission**:

```sh
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::resolving_clear_failure_preserves_details_and_retry_on_same_websocket -- --exact
cargo test -p pioneer-mcp-oauth --lib store::tests::failed_clear_reinstates_candidate_quarantine_for_independent_restart_reader -- --exact
cargo test -p pioneer-client --lib mcp::oauth::tests::failed_clear_retains_new_cleanup_presentation_and_retires_only_matching_flow -- --exact
cargo test -p pioneer-desktop-mcp --lib oauth::tests::failed_clear_remains_actionable_without_restarting_or_signing_in -- --exact
```

## Eighth audit X1–X3

**Code is unaccepted. All scenarios below are NOT RUN.**

- X1: bind admission distinguishes management synchronization from obtaining
  an OAuth client. Under the same per-ID gate, equal-configuration CleanupRequired
  admits synchronization without secret reads, restoration, client creation or
  consent. The transport/SignIn bind still fails closed. Suspend retains that
  cancelled management holder, so disabled rows keep repeat Clear. Non-HTTP
  synchronization similarly retains equal-configuration cleanup rather than
  deleting it. Genuine identity replacement and arbitrary errors retain their
  previous handling; Gateway's reload error propagation is not weakened.
- X2: current addressed cleanup can replace completed consent presentation such
  as old Denied, but not a distinct live operation or newer cleanup. Callback
  preparation failure with a still-active relay is not treated as terminal.
  Superseded cleanup UUIDs, and stale cleanup observed behind another current
  operation, enter the bounded session retirement fence; later consent failure
  does not reopen their admission. Known Retired still removes only its exact
  flow. This reduction uses the ordered authenticated Gateway event consumer
  and its existing durable/current-event validation, not arbitrary untrusted
  event ordering. Shared management precedence lets current Details CleanupRequired
  override a terminal presentation; Desktop then suppresses its stale label/URL
  and offers Clear, without SignIn/Cancel. A new live flow retains precedence
  over stale Details. No DTO/schema or locale key changes were necessary.
- X3: the uncertain-promotion fixture waits a bounded Notify raised inside actual
  promotion readback failure, after the injected delete failure. It then asserts
  the physically present account and fence. The fault stays active through Clear
  retirement and its RPC error, so the exchange cannot slip through a retry between
  notification and Clear. Existing three failure modes and exit guards remain.

### Regressions — NOT RUN

- Main OAuth target: failed Clear -> synchronize/suspend retain current cleanup;
  client acquisition and consent remain blocked; recovered storage permits Clear
  without another exchange/browser.
- Production Gateway reload: row A sorts before enabled B; failed Clear A does not
  abort reload; B starts an actual fake session/catalog (setup has no B actor).
  Disable A stops its actor and preserves cleanup; repeat Clear leaves B's runtime
  generation unchanged. Fault injection targets only A, not B's unrelated cleanup.
- Two authenticated management WebSockets: B Denied -> A new flow -> B failed Clear
  -> A addressed Retired and B distinct CleanupRequired -> current Details/shared
  UI selector -> same-installation retry. Client reducer separately verifies the
  stale terminal replacement and late cleanup/Retired protection, including a newer
  flow that has already become Denied. Desktop action policy checks cleanup guidance,
  repeat Clear, and no SignIn/Cancel over stale Denied.
- All three uncertain-promotion Clear modes retain their physical-record, no-late-
  Authorized, retry and lease-owned reconciliation assertions, now with a real
  storage-phase prerequisite rather than only a Resolving notification.

Deferred exact commands — only after acceptance and separate user permission:

```sh
cargo test -p pioneer-mcp-oauth --test oauth_lifecycle cleanup_synchronization_and_suspend_preserve_management_without_restoring_consent -- --exact
cargo test -p pioneer-gateway --lib mcp_service::tests::cleanup_holder_does_not_abort_later_enabled_rows_or_disappear_on_disable -- --exact
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::two_management_clients_denied_then_other_consent_clear_delivers_current_cleanup -- --exact
cargo test -p pioneer-gateway --lib message::tests::mcp_oauth_rpc_queue::resolving_clear_failure_preserves_details_and_retry_on_same_websocket -- --exact
cargo test -p pioneer-client --lib mcp::oauth::tests::current_cleanup_replaces_old_denied_but_not_new_consent -- --exact
cargo test -p pioneer-desktop-mcp --lib oauth::tests::current_details_cleanup_overrides_stale_denied_copy_and_actions -- --exact
```

No behavioral result is claimed. Persistent storage outage/actual blocked IO retain
the existing limitations; cleanup remains a management state, not authorization.
