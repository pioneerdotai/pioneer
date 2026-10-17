# Desktop startup: composer boundary

`desktop.startup` ends after the active desktop composer's content-independent
Send prerequisites are available in its native inputs and a corresponding GPUI
frame has been drawn. It is not a first-window metric and does not measure the
latency of a submitted turn. This corrects a measurement boundary; it does not
establish a physical startup speedup or reconstruct historical composer latency.

| Dependency | Desktop startup contract |
| --- | --- |
| Transport / scope | Connected, verified identity, current workspace and restored thread or supported workspace draft. Existing workspace/tree restoration gates remain. |
| Authorization | Current identity, workspace authorization, thread permissions (workspace create permission for a draft), and reconciled execution policy. |
| Chat / Agent | Selected provider and model; the same `has_complete_model_selection` and selected-provider readiness used by Send. A CLI selection must belong to the current draft/provider/workspace. |
| Message | No model or CLI runtime requirement. |
| User work | Empty text is ready. Uploads, attachments, Send clicks and an already executing turn are not startup prerequisites. Their existing Send gates remain unchanged. |
| Display metadata | Model label, full model lists, optional capability catalogs and opening the picker are not prerequisites. |
| Full provider catalog | `ProviderCollection::Catalog` is optional, including absent, idle, loading and failed states. |

Desktop filters the optional Provider milestone locally. Tree restoration is
classified from the current workspace directory publication, rather than an
unscoped ThreadTree milestone. The shared `StartupCoordinator::has_failed` and
provider/tree-owner milestone semantics remain unchanged. No catalog is requested
to complete telemetry. The existing background/demand-driven loading order remains
unchanged.

Unconfigured selection or a completed runtime discovery with an unavailable
selected runtime ends as `setup_required`. Successful runtime discovery only
means the list was received: an enabled selected row with `Initializing` remains
pending, even before its capabilities are known. A subsequent matching Ready row
must agree with the composer projection and reach native inputs. A missing,
disabled or otherwise unavailable selected runtime is terminal setup; failed
required discovery remains degraded. The existing native model-selection
pending predicate keeps queued thread creation/restoration pending before that
classification; startup does not wait for model-label discovery. Missing/revoked
authentication uses `authentication_required`. Required read/runtime/subscription errors and denied
scoped write/start permissions use `degraded`; terminal secure-storage/identity
failures are not relabeled as authentication setup. Pending reads/runtime
preparation remain pending. A missing right to create the first dialog is
terminal after the workspace tree has resolved, rather than an indefinite wait
for a draft the principal cannot create. Completed workspace bootstrap without
an active workspace ends as setup; its terminal error ends as degraded.

An initial directory error for the current workspace is degraded before waiting
for an active thread, including principals who can create threads. A read still
loading remains pending. A failed background refresh after a thread in that
workspace has been restored is not a required startup failure; its diagnostic
wait cancels and is never recorded as a successful tree load. Directory action
errors are separate from the read error, and errors in other workspaces are not
used for this classification.

## Publication and frame boundary

The observer uses `ClientCore::watch_publications` without a feature subscription.
Selected-runtime changes publish Composer through the existing owner and wake
that signal. GPUI observations of the shell and composer separately wake it when
a mount or asynchronous native-input application completes. They do not create
or retain feature demand.

The native composer checks the same scoped permission/model helpers as Send.
Only action prerequisites are compared with current publications; metadata or
user-content revisions do not delay acknowledgement. Policy fingerprints must
match the current reconciled policy, rather than merely exist.

In the pinned GPUI implementation, next-frame callbacks are drained before the
frame is drawn. The operational-frame acknowledgement therefore uses two phases:
the first validates readiness and refreshes the window, the next acknowledges
that draw. Both validate navigation identity, draft, mode, selected model/runtime
prerequisites, policy and current native availability. Navigation away and back
invalidates the old candidate. A stale candidate cancels its frame guard and
waits for existing publication/application notifications; it does not poll.
Cancellation also retires queued callbacks. Completion is once only and releases
all observers. No test or actual Send is used to probe readiness.

## Stages

Existing startup trace and metric instruments, consent-generation fencing and
monotonic stage timers are reused. No identifiers or raw errors are added.

- `composer.prepare`: observed wait for the current composer's native action
  prerequisites, starting with the coordinator; a changed workspace/thread/draft
  cancels the old observation. Ready succeeds, a required composer failure errors,
  setup cancels the wait. This is an overlapping wait, not exclusive loading CPU.
- `composer.runtime.wait`: starts only when an observed Chat/Agent CLI selection
  still needs runtime readiness. Ready discovery with an Initializing selected
  row leaves this wait open. Selected readiness observed in native inputs
  succeeds, failed discovery errors, unavailable/setup/cancellation or changed
  selection cancels. A runtime already ready, a normal provider, or Message creates no such wait stage.
- `ui.operational_frame`: the acknowledged draw described above. Stale/cancelled
  candidates record cancellation, not successful frames.
- Desktop no longer synthesizes `providers.load` from coordinator start until
  someone opens a lazy catalog. The enum/shared milestone remains available to
  other consumers. An unexecuted catalog produces no desktop startup stage;
  skipping it is never recorded as a successful load.

Unfinished diagnostic guards cancel at completion/cancellation. Required
milestone/read failures finish their relevant active guards as errors. Stages
that were never observed/executed are absent, distinct from cancelled waits.
The shared session's already measured connect/identity timings are still linked
to this `desktop.startup` using the existing mechanism.

## Regression coverage and validation gate

Regressions use existing ComposerView and desktop shell/runtime fixtures with
controlled publications. Runtime coverage drives the existing runtime owner with
actual Initializing → Ready summaries/status notifications, rather than only a
synthetic ready boolean. The successful Ready regression mounts DesktopShellView
with its native composer, uses GPUI `window.draw(cx).clear(cx)` to draw that root,
and confirms a `thread-composer` rendered bound before acknowledging the frame.
`simulate_next_frame` is used only to deliver callbacks; it does not draw. The
callback-only degraded/cancellation tests remain as separate lifecycle checks.
A test-only watch-sender probe verifies that the finished coordinator releases
its receiver and all captured observer senders, including after later
publications. They cover optional catalog states, CLI pending/failed/unavailable/ready, empty text, Message without selection,
Chat/Agent during an active turn, denied/missing authorization, native application,
scope changes between frame phases, once-only acknowledgement, cancellation,
initial directory errors with create permission, scoped/background directory
outcomes, metadata-independent prerequisites and absence of telemetry-driven catalog demand.

**Do not run tests until the external reviewer in the initiating thread has
accepted the implementation.** Compilation with `cargo check --tests` does not
execute tests. After that acceptance, focused commands are:

```sh
cargo test -p pioneer-client composer::
cargo test -p pioneer-desktop-thread composer::
cargo test -p pioneer-desktop --bin pioneer-app client_runtime::startup::
cargo test -p pioneer-observability startup::tests
```

No real application/smoke run or production telemetry measurement is included in
this change. Runtime test results and physical startup timings remain unverified.
