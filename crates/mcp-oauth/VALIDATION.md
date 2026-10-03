# Validation of audit corrections — 2026-10-01

Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/mcp-oauth`.
Branch: `feature/mcp-oauth`. Base: `ae814a827ef8b9fb0a4a932a26fbfe39d14f36dc`.
Preserved documentation-only HEAD: `38ef8dc0a2942df3364c0b2aedcfd7f226f47932`.
No commit, push, merge or main-checkout change was made.

**Tests executed for these corrections: 0.** The user prohibits execution until
code acceptance and separate permission. No cargo test, doctest, test binary,
testing script or CI workflow was invoked. Prior claims of 102 passing tests
were historical pre-audit results; they do not validate this corrected diff.

## Historical F1–F9 non-test checks

Commands ran from this worktree using the pinned lockfile and rmcp 3.5.0:

```sh
export CARGO_TARGET_DIR=/tmp/pioneer-mcp-oauth-validation-ae814a827ef8
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
```

Successful checks recorded for the preceding F1–F9 pass (not validation of R1–R6):

```sh
cargo check -p pioneer-mcp-oauth --lib
cargo check -p pioneer-mcp-oauth --test oauth_lifecycle
cargo check -p pioneer-mcp-oauth --lib --tests
cargo check -p pioneer-mcp-oauth -p pioneer-mcp --lib --tests
cargo check -p pioneer-client --lib
cargo check -p pioneer-client --lib --tests
cargo check -p pioneer-gateway --lib
cargo check -p pioneer-gateway --lib --tests
cargo check -p pioneer-gateway -p pioneer-mcp-oauth --lib --tests
cargo check -p pioneer-desktop-mcp --lib
cargo check -p pioneer-desktop --bin pioneer-app
cargo check -p pioneer-desktop --bin pioneer-app --tests
cargo run -p pioneer-protocol --bin schema -- /tmp/pioneer-oauth-audit-schemas
git diff --check
```

The preceding F1–F9 combined Gateway/OAuth check completed after its cleanup
changes (11m 56s). The R1–R6 pass is recorded separately below.

`cargo check --tests` type-checks test targets and does **not** execute them.
Intermediate compile errors were corrected before the successful checks above.
The schema exporter was inspected: it writes JSON schemas and does not run tests.
Only the changed install and callback-response schemas were copied from its output;
unrelated generated-schema drift was preserved.

A Python subprocess invoked `rustfmt --edition 2024 --check` on the 25 corrected
Rust files identified against the audit snapshot plus new auth/transport/test
files. Exit code 0. Formatting was applied directly to the corresponding files;
no workspace-wide format/check/test command was run.

Static Python checks using `tomllib` parsed all 16 affected locale files and
verified the new OAuth keys in all eight languages for each shell/UI crate.
JSON checks confirmed that both changed protocol schemas match the export and
that the OAuth schema documents parse. These are static contract checks, not
execution of regression scenarios.

The desktop dependency `block 0.1.6` can emit its existing future-incompatibility
warning. No unrelated dependency upgrade was made.

## Deferred behavioral checks

All newly added and modified regressions are **NOT RUN**. Their scenarios and
exact, package/target-filtered proposed commands are listed in
[AUDIT_FIXES.md](AUDIT_FIXES.md). Compilation is not evidence that they pass.
No assertion is made about the whole workspace.

Real providers, browser/UI interaction, cross-device networking, OS sleep,
Windows/Linux builds and fault interleavings have not been exercised for these
corrections. They require post-review validation. The persistence adapter still
uses permissions-restricted `keystore.db` with `encryption_opts: None`.

A blocking keystore operation already started cannot safely be force-cancelled;
its retained IO owner/leases make cleanup wait for actual completion. Revoked
refresh tokens and a provider rotation followed by local persistence failure
may require fresh consent. Registration can remain orphaned at the provider if
its external side effect precedes a local failure. The callback port must be
available and allowed by the registered client. These are not hidden test results
or guarantees of universal provider compatibility.

## Exact formatting command

```sh
rustfmt --edition 2024 --check \
  crates/client/src/mcp/actions.rs \
  crates/client/src/mcp/notifications.rs \
  crates/client/src/mcp/oauth.rs \
  crates/client/src/mcp/operations.rs \
  crates/client/src/transport/ws/command_sender.rs \
  crates/desktop-mcp/src/oauth.rs \
  crates/desktop/src/platform/mcp_oauth.rs \
  crates/gateway/src/auth/service.rs \
  crates/gateway/src/mcp_oauth.rs \
  crates/gateway/src/mcp_secrets.rs \
  crates/gateway/src/mcp_service.rs \
  crates/gateway/src/message/mcp/install.rs \
  crates/gateway/src/message/tests.rs \
  crates/gateway/src/message/tests/mcp_oauth_rpc_queue.rs \
  crates/gateway/src/transport/mod.rs \
  crates/gateway/src/transport/server.rs \
  crates/mcp-oauth/src/network.rs \
  crates/mcp-oauth/src/service.rs \
  crates/mcp-oauth/src/store.rs \
  crates/mcp-oauth/tests/oauth_lifecycle.rs \
  crates/mcp/src/client/rmcp_adapter.rs \
  crates/mcp/src/config.rs \
  crates/mcp/src/domain.rs \
  crates/mcp/src/oauth.rs \
  crates/protocol/src/mcp.rs
```


## R1–R6 non-test checks

These corrections remain unaccepted and uncommitted. **Behavioral regressions
executed: 0.** No tests/CI/browser/provider interaction were run. Commands use the
same three environment variables recorded above and the existing lockfile.

Successful R-pass commands:

```sh
cargo check -p pioneer-mcp-oauth --test oauth_lifecycle
cargo check -p pioneer-mcp-oauth --lib --tests
cargo check -p pioneer-gateway -p pioneer-mcp-oauth -p pioneer-client --lib --tests
cargo check -p pioneer-desktop --bin pioneer-app --tests
cargo run -p pioneer-client --features schema --bin schema -- /tmp/pioneer-oauth-reaudit-client-schemas
```

The schema binary was read before execution: it exports JSON schemas and does
not run tests. Only `mcp_action_publication.json` was copied; the new browser retry
action is shell-neutral, with no URL/token/PKCE payload in the action contract.
No unrelated schema drift was copied. All eight Desktop-MCP locales reuse their
existing localized `mcp.oauth.open_link` label for the retry button.

`cargo check --tests` compiles targets only. Several intermediate compile errors
in updated test fixtures were corrected; interrupted checks are not counted as
successful validation. The existing `block 0.1.6` future incompatibility warning
appeared during Desktop checking; dependencies were not upgraded.

Final formatting/diff/static contract commands and final compile completion are
recorded at the end of this section. Deferred test commands/scenarios remain in
[AUDIT_FIXES.md](AUDIT_FIXES.md), explicitly **NOT RUN**. Real providers, browser
interaction, OS sleep, cross-device networking, Windows/Linux and deterministic
interleavings have not been behaviorally validated. An already admitted OS
browser launch cannot be recalled; its late result cannot revive retired UI.

Final static checks completed successfully:

```sh
rustfmt --edition 2024 --check \
  crates/client/src/mcp/oauth.rs \
  crates/client/src/mcp/operations.rs \
  crates/desktop-mcp/src/catalog.rs \
  crates/desktop-mcp/src/oauth.rs \
  crates/desktop/src/platform/mcp_oauth.rs \
  crates/gateway/src/mcp_oauth.rs \
  crates/gateway/src/mcp_service.rs \
  crates/gateway/src/message/mcp/install.rs \
  crates/gateway/src/message/mcp/policy.rs \
  crates/gateway/src/message/mcp/uninstall.rs \
  crates/gateway/src/message/tests/mcp_oauth_rpc_queue.rs \
  crates/mcp-oauth/src/service.rs \
  crates/mcp-oauth/src/store.rs \
  crates/mcp-oauth/tests/oauth_lifecycle.rs
git diff --check
```

The rustfmt command was invoked by Python with that exact argument list.
A static Python check read reaudit FILES.json without modifying it, identified
18 R-pass changed paths (14 Rust), parsed OAuth JSON schemas and all 16 locale
TOML documents, checked `open_link` in all eight Desktop-MCP languages, compared
the changed Client schema structurally with exporter output, and verified
Cargo.toml/Cargo.lock hashes remain identical to the reaudit snapshot. Exit 0.
Branch and HEAD were reread as `feature/mcp-oauth` and the preserved
`38ef8dc0a2942df3364c0b2aedcfd7f226f47932`. No external audit artifacts were changed.

Final combined Gateway/OAuth/Client check completed successfully after terminal
ownership, delivery admission, transferable recovery admission and scope fixture
corrections (4m 32s). The final short Client epoch-fence adjustment was separately
compiled successfully with:

```sh
cargo check -p pioneer-client --lib --tests
```

That check completed in 56.20s including build-lock waiting. Desktop checking
including its fake-browser fixtures and the admission API completed successfully
(3m 23s). These compile results execute **zero tests**. Final `git diff --check`
passed; all 98 pre-existing/current implementation paths remain uncommitted,
including the preserved F1–F9 work. The R pass changes 18 paths relative to the
reaudit snapshot. No test count other than zero is asserted.


## S1–S5 non-test checks

The third audit snapshot (98 paths) was read and compared by SHA-256 before
editing: every recorded path matched. Branch/HEAD were verified against the
user's specified worktree. Earlier F/R changes and the doc-only HEAD remain.
This section concerns the S pass only; historical checks above are not validation
of its new scenarios. **Tests executed: 0.** No browser or CI was started.

Successful intermediate compilation commands in this pass:

```sh
cargo check -p pioneer-client --lib --tests
cargo check -p pioneer-gateway -p pioneer-client -p pioneer-mcp-oauth --lib --tests
cargo check -p pioneer-client -p pioneer-mcp-oauth --lib --tests
```

Intermediate fixture compilation errors (record initializer and endpoint API
paths) were corrected. Failed checks are not counted as successful validation.
The final compilation results for the completed source are recorded below.
All commands use the three Cargo environment variables recorded above; none
execute test targets. Compilation cannot prove that a selected regression passes.

Static checks parse the existing JSON schema documents and all affected locale
TOML documents. Their SHA-256 hashes and all Cargo manifest/lock hashes still
match the third audit snapshot: the internal event revision and durable pending
secret record add no public DTO/schema/localization/dependency changes.
Targeted rustfmt covers the changed Rust paths, including three newly touched
Client transport files used solely for cfg(test) ingress/RPC injection. No broad
workspace check, test script, or schema exporter with unrelated drift was run.

New and modified regression scenarios are **NOT RUN**. Exact proposed filtered
commands are recorded in AUDIT_FIXES.md and require code acceptance followed by
separate permission. The persistence pending marker prevents candidate adoption
through independent readers, same-identity binding and restart if rollback fails;
permanently failed storage still cannot promise successful prior-record recovery.
A browser call admitted to the OS cannot be revoked; its tracked owner joins
actual completion at destruction, while event/action retirement never joins it.
Real providers, real browser interaction and cross-platform OS behavior remain
unverified.

Final S-pass checks completed successfully:

```sh
cargo check -p pioneer-gateway -p pioneer-client -p pioneer-mcp-oauth --lib --tests
cargo check -p pioneer-desktop --bin pioneer-app --tests
```

The combined check completed in 4m 21s; Desktop completed in 2m 14s including
build-lock waiting. Desktop reports the existing `block 0.1.6` future-compatibility
warning. Its hardened webbrowser dependency is unchanged. After those checks,
test-only cleanup guards and the pre-stage independent-reader fixture were
refined to use the actual per-installation file lease, without adding a dependency.
A temporary fixture reference to unavailable tempfile failed compilation and was
replaced with an owned UUID-named temporary directory; that failed check is not
behavioral evidence or a successful validation result.

The final OAuth source and its test targets (including those fixture refinements)
compiled successfully, executing zero tests:

```sh
cargo check -p pioneer-mcp-oauth --lib --tests
```

Exit 0, 2.26s. The final static Python check used the union of git diff paths and
snapshot paths, so it included all three newly touched cfg(test) transport files.
It ran the following exact formatting command successfully, parsed unchanged
JSON/TOML contracts, verified dependency/schema/locale hashes, and ran
`git diff --check` (all exit 0):

```sh
rustfmt --edition 2024 --check crates/client/src/core.rs crates/client/src/mcp/oauth.rs crates/client/src/runtime.rs crates/client/src/transport/ws/runtime.rs crates/client/src/transport/ws/runtime/client.rs crates/client/src/transport/ws/runtime/command_sender.rs crates/gateway/src/mcp_secrets.rs crates/gateway/src/mcp_service.rs crates/gateway/src/message/mcp/oauth.rs crates/gateway/src/message/tests/mcp_oauth_rpc_queue.rs crates/mcp-oauth/src/service.rs crates/mcp-oauth/src/store.rs crates/mcp-oauth/tests/oauth_lifecycle.rs
git diff --check
```

The S pass changes 16 paths relative to the third audit snapshot, including
13 Rust paths. The complete implementation diff contains 101 paths relative to
HEAD; it retains the earlier F/R implementation. All regressions remain NOT RUN.


## T1–T4 non-test checks

Before editing, branch/HEAD and all 101 snapshot SHA-256 values were verified;
there were no content differences. The previous F/R/S implementation and doc-only
HEAD remain in this worktree. **Tests executed: 0.** No cargo test, test binary,
doctest, testing script, browser, CI, commit, push or merge was executed.

Successful completed-source checks, using the same three Cargo environment
variables documented above:

```sh
cargo check -p pioneer-mcp -p pioneer-mcp-oauth -p pioneer-gateway --lib --tests
cargo check -p pioneer-desktop --bin pioneer-app --tests
```

The final combined check, including the last typed SDK classification and durable
installation fixture changes, completed in 4m 37s (exit 0). The earlier combined
check completed in 3m 45s. The final Desktop check completed in 2.34s (exit 0) and
reported the existing `block 0.1.6` future-compatibility warning. These commands
only compile targets; they are not behavioral regression results. Intermediate
checks caught missing internal error fields, fence helper ownership/types and
fixture time/WebSocket message types; those were corrected. Failed checks are
not counted as successful validation. Other intermediate successful checks are
superseded by the final combined result. The test-only initial-consent extension was also compiled with
`cargo check -p pioneer-mcp-oauth --test oauth_lifecycle` (exit 0, 6.74s).
The final combined check above includes the durable-installation queue fixture
and the narrowly typed SDK error-classification regression.

A static Python check identified nine changed Rust paths against the fourth
audit snapshot and ran this exact command successfully:

```sh
rustfmt --edition 2024 --check crates/gateway/src/mcp_service.rs crates/gateway/src/message/tests/mcp_oauth_rpc_queue.rs crates/mcp-oauth/src/service.rs crates/mcp-oauth/src/store.rs crates/mcp-oauth/tests/oauth_lifecycle.rs crates/mcp/src/client/rmcp_adapter.rs crates/mcp/src/lib.rs crates/mcp/src/oauth.rs crates/mcp/src/runtime/mod.rs
git diff --check
```

It also parsed the existing JSON schemas and affected locale TOML files and
verified their hashes, together with all Cargo manifests and lockfile, against
the snapshot. There are no changes to public DTO/schema/locales/dependencies in
the T pass. The typed OAuth failure cause is an internal transport/runtime seam,
not a secret-bearing management or bridge contract. The promotion fence contains
only a non-secret marker in the existing OAuth namespace; no primary DB migration
or general keystore change was introduced.

New regression scenarios and exact proposed package/target/name-filtered commands
are listed in AUDIT_FIXES.md. **All are NOT RUN**, including the real WebSocket
queue and real worker/event-consumer scenarios. Acceptance and separate permission
are still required. No assertion is made that the whole workspace was checked.

Permanent storage failure can prevent cleanup and cannot guarantee provider
acceptance of the prior account. A mutation error is not proof of rollback.
Unknown promotion is fenced; unknown fence deletion remains an owned, cancelable
reconciliation without falsely reporting Failed for a potentially confirmed grant.
Actual blocking I/O retains its file/refresh lease through completion. The actor
mailbox is bounded/coalesced and retired with its actor; the ack deadline does not
bound the duration of a healthy tool or discard recovery. Real providers, browser
interaction, OS sleep and cross-platform behavior remain unexecuted.

The final static pass compared all 101 snapshot paths: this T pass changes 12
paths (nine Rust sources and three documentation files). Targeted rustfmt,
`git diff --check`, JSON/TOML parsing and unchanged schema/locale/dependency hashes
all completed with exit 0. Branch `feature/mcp-oauth` and HEAD
`38ef8dc0a2942df3364c0b2aedcfd7f226f47932` remain unchanged.

## U1–U2 non-test validation

Tests executed in this pass: **0**. New admission/shutdown, uncertain-outcome,
Gateway actor-ordering and Client relay regressions are **NOT RUN**. Historical
F/R/S/T check or test results are not behavioral evidence for this diff.

Branch and doc-only HEAD were checked before editing and remain unchanged.
Applicable root and Client instructions were read. The GPUI Kit Design Guides
were applied to the narrow state/action/copy change; no layout or browser API
was changed. Real-window, keyboard interaction and provider scenarios were not
executed under the test prohibition.

The schema binaries were inspected: they call serialization writers only and
execute no tests. Isolated generation commands:

```sh
cargo run -p pioneer-protocol --bin schema -- /tmp/pioneer-u-protocol-schemas
cargo run -p pioneer-client --features schema --bin schema -- /tmp/pioneer-u-client-schemas
```

Both exited 0. Only the new shared OAuth `resolving` enum value was propagated to
eight existing protocol/Client snapshots. Unrelated generator drift was excluded.
The standalone OAuth, notification, management/details and Client MCP snapshots
were compared semantically to generated output; enum definitions in the larger
notification/event snapshots match the generators. All eight Desktop MCP locale
files parse and contain resolving copy. Existing FFI uses shared protocol state;
its schema export defines no separate OAuth state contract.

A targeted rustfmt check and `git diff --check` exited 0:

```sh
rustfmt --edition 2024 --check crates/mcp-oauth/src/service.rs crates/mcp-oauth/tests/oauth_lifecycle.rs crates/gateway/src/mcp_oauth.rs crates/gateway/src/mcp_service.rs crates/protocol/src/mcp.rs crates/client/src/mcp/oauth.rs crates/desktop-mcp/src/oauth.rs
git diff --check
```

An intermediate check caught a Result/Option mistake in the new Client fixture;
it was corrected to assert rejected browser retry. That failed check is not a
successful validation result. Addressed compile commands use the same target
and Cargo environment documented above; `--tests` compiles targets without running
regression behavior. Exact deferred test commands are in AUDIT_FIXES.md.

Shutdown now waits admitted external callers before the final mutation drain.
Actual blocking IO cannot be forcibly interrupted; an indefinitely blocked store
can prevent clear/shutdown completion. An expired/disconnected resolution leaves
an honest, clearable Resolving projection, without a false terminal outcome.
Persistent unreadable storage cannot prove a grant or guarantee cleanup; later
clear/rebind/restart must reconcile readable durable state. No code acceptance,
behavioral test result, commit, push, merge or CI execution is claimed.

Final addressed compile checks, executing no tests:

```sh
cargo check -p pioneer-mcp-oauth -p pioneer-client -p pioneer-gateway --lib --tests
```

Exit 0, 5m 23s including schema-generation build-lock waiting. This check includes
both new OAuth lifecycle regressions, the production Gateway actor-ordering fixture
and Client relay fixture. The existing FFI dependency consumer compiled in this
graph. The earlier successful 6m 08s check preceded the final monitor-race and
shutdown-owner cleanup refinements and is superseded by this result.

```sh
cargo check -p pioneer-desktop --bin pioneer-app --tests
```

Exit 0, 4m 34s including build-lock waiting. Desktop and its MCP UI consumer
compile with the new state and locale keys. The existing `block 0.1.6`
future-compatibility warning remains. This is compile coverage only; no GPUI
interaction or regression behavior was executed. Final dependency-hash checks,
changed JSON parsing and `git diff --check` also exited 0; rmcp/webbrowser and
all dependency manifests/lockfile are unchanged in the U pass.

## V1–V3 static-review pass (2026-10-01)

**Code remains unaccepted. Tests executed: 0.** New winner-publication,
addressed-retirement, production Gateway consumer and Client reducer scenarios
are **NOT RUN**. All six existing uncertain-promotion scenarios are preserved;
their corrected completion/lease observation is compiled but not exercised.
No actual browser, provider account, GPUI interaction or CI was used.

Addressed compile commands (same target directory/environment documented above):

```sh
cargo check -p pioneer-mcp-oauth -p pioneer-client -p pioneer-gateway --features pioneer-gateway/oauth-test-support --lib --tests
cargo check -p pioneer-mcp-oauth --features test-support --lib --tests
cargo check -p pioneer-mcp-oauth --lib
cargo check -p pioneer-desktop --bin pioneer-app --tests
```

All exited 0. The combined check took 3m 35s and includes the new Gateway and
Client fixtures. The subsequent OAuth check (7.68s) includes the final placement
of the outer owned-task completion guard and post-lock worker retirement check.
The default OAuth build (4.37s) verifies production compilation without optional
publication/completion hooks. Desktop compiled in 48.59s including lock waiting.
The existing `block 0.1.6` future-compatibility warning remains. `--tests` compiles
regression targets; it does not demonstrate their behavior. An intermediate
Gateway fixture compile caught an accidentally copied reference to `unrelated`;
the unused block was removed and the corrected combined check passed.

The serialization-only schema binary sources were inspected before execution:

```sh
cargo run -p pioneer-protocol --bin schema -- /tmp/pioneer-v-protocol-schemas
cargo run -p pioneer-client --features schema --bin schema -- /tmp/pioneer-v-client-schemas
```

Both exited 0. Only the shared `retired` enum member was applied to eight existing
OAuth/management/notification/Client schema snapshots. Standalone relevant
snapshots and shared enum definitions in the larger snapshots match generated
output semantically; unrelated generator drift was excluded. JSON snapshots and
all eight Desktop MCP locale TOMLs parse. Retired is an addressed presentation
control signal, not rendered UI copy; no locale strings were added in this pass.
The existing FFI consumer compiled in the addressed Client/Gateway graph.

Targeted Rust formatting and `git diff --check` passed. Cargo.lock and dependency
versions remain unchanged from the audited snapshot; only optional test-support
features were added to the OAuth/Gateway package manifests. Branch and doc-only
HEAD remain unchanged. No commit, push, merge or test execution occurred.

The decision and short Resolving projection share one linearization lock. The
notification can follow retirement, but only a current UUID/config/generation/flow
may deliver Resolving. Genuine replacement/clear/suspend sends a separately
addressed presentation reset; equal-identity updates preserve consent. The six-case
fixture observes actual owned completion, temporary-state cleanup and shared
refresh/file-lease release rather than a configuration predicate. Optional hooks
are excluded from default builds. Exact deferred commands are in AUDIT_FIXES.md.

A disconnected initiator cannot receive an event; session epoch retirement and
current authorized management details remain its recovery route. Persistent
unreadable storage cannot establish durable success or guarantee cleanup. Actual
blocking IO may delay shutdown/clear until its owned completion. Retirement caches
are bounded; independent durable identity/generation/epoch checks remain in place.
Compile and static checks do not replace the deferred behavioral regressions.

## W1 static-review pass (2026-10-01)

**Code remains unaccepted. Tests executed: 0. All W1 regressions are NOT RUN.**
No browser, real provider, GPUI interaction or CI was executed. The exact deferred
scenarios/commands are documented in AUDIT_FIXES.md; compile coverage does not
demonstrate their behavior.

Addressed compile checks, using the existing /tmp target directory and
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0`:

```sh
cargo check -p pioneer-mcp-oauth -p pioneer-gateway -p pioneer-client --features pioneer-gateway/oauth-test-support --lib --tests
cargo check -p pioneer-mcp-oauth --lib --tests
cargo check -p pioneer-desktop --bin pioneer-app --tests
cargo check -p pioneer-desktop-mcp --lib --tests
```

All corrected commands exited 0. The combined final check took 10m 13s including
build-lock waiting; the final OAuth default-feature check took 1m 06s and includes
the last consent-cancellation compatibility branch. Desktop app check took
2m 45s; the subsequent Desktop MCP check (3m 47s) includes the extracted action
policy and its regression target. Existing FFI dependencies compiled in the
combined graph. Existing `block 0.1.6` future-compatibility warning remains.

Intermediate fixture compilation caught a sea-query `is_null` trait collision,
incorrect boxing of PendingConsent, and a GPUI test-macro glob import. These were
corrected (Value::Null comparison, boxed envelope, explicit type imports). Those
failed checks are not successful validation evidence. No assertions were weakened.
The Gateway wire fixture tests pre-account mutation, partial deletion and
post-account-mutation failures. It explicitly starts with both the account and
promotion fence present in Resolving, and observes management Details before retry.

Serialization-only schema sources were inspected before executing:

```sh
cargo run -p pioneer-protocol --bin schema -- /tmp/pioneer-w-protocol-schemas
cargo run -p pioneer-client --features schema --bin schema -- /tmp/pioneer-w-client-schemas
```

Both exited 0 (5m 15s and 5m 14s including lock waiting). Only the new shared
`cleanup_required` enum value was propagated to eight existing snapshots; the
relevant snapshots/shared definitions match generated output semantically. Eight
Desktop MCP locale TOMLs parse and provide localized cleanup guidance. No secret
fields or browser effects were added to the contract.

Targeted rustfmt, `git diff --check`, schema/locale parsing and Cargo.lock snapshot
hash comparison passed. Branch and doc-only HEAD remain unchanged. Previous
F/R/S/T/U/V work is retained, without commit, push or merge.

A Clear error retains a cancelled, non-usable management holder until an explicit
retry succeeds; it does not revive the old exchange. Fresh cleanup presentation
identity avoids the old flow's retirement fence. Cleanup blocks consent/restoration
but offers Clear, and failed Client Disconnect refreshes Details. The actual blocking
delete retains file/write ownership and reinstalls the durable fence first. Partial
mutation or unavailable readback never acknowledges successful cleanup. Permanent
failed storage can still prevent cleanup or repair of an already-uncertain outcome;
actual blocking IO cannot be forcibly interrupted. This pass makes no behavioral
acceptance claim.

Final Gateway fixture check after explicitly asserting both saved account and
fence at the Resolving prerequisite:

```sh
cargo check -p pioneer-gateway --lib --tests
```

The pre-interruption process result was unavailable after recovery, so it is not
claimed as successful evidence. The final addressed check returned exit 0 in
3.15s and covers the current wire fixture. Branch/HEAD, diff whitespace, localized
uncertain-outcome copy and schema contract comparisons were reconciled afterward.
No test command or test binary was executed.

## X1–X3 static-review pass (2026-10-01)

**Code remains unaccepted. Tests executed: 0. All X regressions are NOT RUN.**
No provider/browser/GPUI interaction or CI was executed. The exact deferred
commands are in AUDIT_FIXES.md. Compiling targets is not behavioral evidence.

Addressed checks, with the existing target directory and Cargo environment:

```sh
cargo check -p pioneer-mcp-oauth -p pioneer-client -p pioneer-gateway --lib --tests
cargo check -p pioneer-client -p pioneer-gateway --lib --tests
cargo check -p pioneer-mcp-oauth --test oauth_lifecycle
cargo check -p pioneer-desktop-mcp --lib --tests
```

All exited 0. The final Client/Gateway check (5m 41s including lock waiting)
covers the installation-specific deletion fault, ordered-row enabled-runtime
regression, two-management-client wire fixture, and terminal/live cleanup reduction.
The OAuth integration target check (3m 58s including waiting) compiles the new
package-local cleanup synchronization/suspend scenario without Gateway in its
dependency graph. The final Desktop MCP check (2m 12s including waiting) covers
the shared state-selector API and stale-Denied action-policy regression. Earlier
combined checks (4m 17s / 4m 12s) preceded final fixture refinements and are not
claimed as verification of those refinements. The existing block 0.1.6 future-
compatibility warning remains. A misspelled fake-server field in the newly written
OAuth fixture was corrected from state to data before target compilation.

Targeted rustfmt checks of six affected Rust files, git diff --check, dependency
lock snapshot hash comparison and JSON schema parsing exited 0. No DTO/schema or
locale changes were needed: the helper is shell-neutral state precedence and
Desktop uses existing localized cleanup copy. Root/Client AGENTS and applicable
GPUI design guidance were applied. Existing CRUD access/scoped handles and
transaction boundaries are unchanged; no DB capacity is held over OAuth IO.

Static inspection checks the per-ID management admission, disabled-holder
retention, transport/consent failure closure, exact-flow/session retirement
fences and physical readback prerequisite. The uncertain fault remains active
until Clear has drained the exchange and failed its deletion; an enum notification
alone cannot release it. Exit guards still retire/release fixture faults. The
Gateway row-order fixture injects deletion failure only for A so unrelated B does
not fail from an accidental global storage outage.

Branch feature/mcp-oauth and doc-only HEAD remain unchanged; previous F/R/S/T/U/V/W
edits are retained. No commit, push, merge or test execution occurred. Persistent
storage failure and blocked actual IO retain their documented limitations. Real
UI state/interaction and regression behavior still require acceptance followed by
separate permission to run the exact tests.
