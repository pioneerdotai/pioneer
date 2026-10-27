# Gateway implementation notes

Base: dcc63b7142bf681cba96de8267a15e8383e82b11; initial worktree clean.

Before changes: `session_buffer` owns normalized PCM and raw packets in its
bounded inbox. Append and close serialize on the same store mutex. A single
tracked blocking worker per session drains that inbox; incremental owns VAD
residual, context window and text. The lifetime byte budget counts accepted
raw plus normalized bytes even after drain; no mode gets a rolling allowance.

Start holds the gateway settings update lock, validates Ready and freezes engine
generation, cancellation token and admission lease. Commit closes input and
takes the completion receiver. No turn is created by the worker: existing
authenticated finalization claims terminal authority and starts the turn.
Cancel/disconnect/revocation remove session input and cancel its token; shutdown
cancels all and joins tracked workers. The lease travels through completion and
remains on the worker stack during native calls. Static inspection shows the
supervisor checks generation and cancellation after acquiring its engine mutex.

Planned buffered path: freeze `transcribe_while_recording` at start alongside the
generation. Wait for close, then move the entire inbox out under the same lock;
perform VAD and speech concatenation outside the lock. Reuse current VAD frame
and residual methods, concatenate selected speech into one prepared segment,
and call the existing session transcriber once. Keep source counters separate
from adapter padding. The cumulative ingress limit bounds all retained PCM;
buffered has no incremental 18-second window limit. No engine lock is held
while recording or waiting for Commit, and no new job is created per chunk.

History inspected: pre-incremental 4c56de1 `finalize_voice_session_audio`,
`take_session_audio`/`into_session_audio`, segmentation and one-shot prepared
transcription; b0866e15 introduces current worker/lease ownership; 5ede0866
retains contextual incremental inference and hypothesis-aware engine fences.
The old whole-recording VAD -> concatenated speech -> one adapter call principle
is restored, without restoring old handlers, lock-held flattening, or
`chunks_exact` residual loss.

Coordination: mode is deliberately outside supervisor desired identity. Start
uses `apply_persisted_voice_input_mode` under the settings lock, then passes the
captured boolean to the session worker. Supervisor readiness-only snapshots use
the protocol's default policy value; gateway management publication overlays
persisted mode before sending these snapshots (settings owner implements this).
Thus mode-only saves never enter `apply_desired` or alter model generation.

No tests, inference, builds, checks or generators are authorized to run.

## Implemented and statically reviewed

- `voice_handlers::voice_session_start` reads persisted mode under the existing
  settings-update lock, using the settings owner's
  `apply_persisted_voice_input_mode`; any read error fails admission. The bool
  moves into `incremental::start`'s one tracked worker closure and never changes
  for that session. Model supervisor desired state stays unchanged.
- `session_buffer::take_closed_processing_chunks` waits for the existing close
  boundary, then moves the `VecDeque` out in O(1). Cancel/removal/shutdown retain
  existing wakeup semantics. No full concatenation occurs under the mutex.
- `process_buffered` segments the full accepted PCM using current
  `push_incremental_frame` and `finish_incremental_tail` mechanics, joins all
  emitted speech and calls the existing session adapter once. It never calls
  incremental text/window planning. Source counters precede native padding.
- An explicit additional memory bound is one full normalized recording during
  preparation (plus bounded VAD prefill/residual). Capacity is reserved once
  from the accepted sample count; chunks drop as consumed. Raw+normalized input
  is still bounded by `VOICE_SESSION_MAX_BUFFERED_BYTES`. The adapter's existing
  flattened native input is made after the source queue has been consumed.
  With current 3200 * 1920 = 6,144,000-byte admission budget, normalized sample
  storage is at most 4,096,000 bytes and the additional reserved speech storage
  has that same bound (plus at most 1600 samples of short native padding).
  Existing per-chunk Vec/metadata overhead remains; buffered retains it until
  Commit rather than draining during recording. Accepted nonempty PCM chunks
  consume at least six budget bytes each; zero-payload network chunks must
  continue to be rejected by the existing protocol ingress validation.
- Static code inspection found that the post-engine-mutex fence preceded
  potentially large input flattening/validation. `LoadedVoiceEngine::transcribe_hypothesis`
  now takes an optional session cancellation token and rechecks directly after
  preparation and before the selected adapter. A cancelled entry returns None;
  supervisor converts it to a session error without invalidating the model.
  True adapter failures and panics retain their existing engine-failure path.
- Generation is still checked while holding the same engine mutex; no settings
  update can replace that generation until native returns. The worker's lease
  remains owned through actual return, even if cancellation drops completion.

New, unrun coverage in `incremental` exercises real normalization/inbox/VAD:
one exact full-recording ASR call over 20 seconds with 137 residual samples and
four packet sizes; speech concatenation across a long pause; short voiced
inputs/onsets and Moonshine padding without source-counter growth; silence,
empty transcript and genuine inference error; gaps/duplicates/order/late
packets; worker cancellation before Commit; real tracked worker cancellation
and shutdown while a blocking fake native call retains all capacity.

The written, unrun runtime test
`pre_cancelled_call_returns_none_and_engine_remains_usable` supplies an already
cancelled token, asserts `Ok(None)`, and then asserts a successful call on the
same stub engine without a token. It does not exercise cancellation during
input preparation or any supervisor state transition.

The written, unrun supervisor test
`voice_supervisor_session_generation_and_cancellation_fence_native_entry`
contains stale-generation rejection, pre-cancelled-call rejection, a successful
valid call and engine-retention assertions. Its additional concurrent scenario
cancels while the parent holds the engine mutex and asserts a rejected worker
call and a retained engine. The worker signals before `transcribe_for_session`,
so receipt of the signal does not establish that it has entered the method or
is waiting for the mutex. Cancellation may precede method entry or occur while
the worker waits; the test does not deterministically select between them.

The locations of the production checks after mutex acquisition and after input
preparation, and the supervisor's separate `None` handling, are visible by static
code reading. Neither of these two tests deterministically reproduces a token
being cancelled during input preparation or while the worker is already blocked
on the engine mutex. Their assertions have been retained, not executed; they
must not be described as proof of those precise race timings.

The session-buffer harness races append against Commit and checks that the
ownership transfer contains exactly those chunks accepted before close, once,
in sequence order and with literal PCM values.
Existing incremental tests and expected values remain unchanged.

## Revision: authenticated message coverage

The existing `owned_voice_ingress_case` harness now accepts an explicit buffered
case parameter. Its original incremental cases retain their assertions and
call shapes. All cases own a temporary runtime home and seed enabled/provider/
model/mode through the real gateway settings update path. The same ready fake
engine, owned dispatcher, real PCM frame ingress, VAD, terminal claim and CRUD
turn/history boundary are reused; no separate message fixtures were added.

New named message cases:

- `owned_voice_buffered_full_recording_one_authorized_turn`: zero ASR calls and
  no turn while recording; one exact 320,007-sample prepared PCM call after
  Commit (over 18 seconds, duplicate packet ignored and seven-sample residual);
  one successful session result and exactly one durable TurnStarted event.
- `owned_voice_buffered_cancel_during_inference`,
  `owned_voice_buffered_disconnect_during_inference`,
  `owned_voice_buffered_revocation_during_inference`,
  `owned_voice_buffered_shutdown_during_inference`: block the existing fake
  native boundary after Commit, exercise authenticated cleanup, preserve all
  original no-late-result/no-turn/history assertions, reserve the other three
  slots and prove capacity remains full until native release. Shutdown must
  remain pending until release.
- `owned_voice_buffered_empty_transcript`,
  `owned_voice_buffered_native_error_no_partial_turn`,
  `owned_voice_buffered_silence_no_inference_or_turn`: existing terminal-result
  and turn checks plus equality with the original durable history; silence has
  no ASR call at all.
- `owned_voice_incremental_session_pins_mode_and_peer_next_session_buffers` and
  `owned_voice_buffered_session_pins_mode_and_peer_next_session_is_incremental`:
  a second authenticated client changes the saved mode during the first active
  recording, the owner receives the authoritative notification, generation and
  loaded engine stay unchanged, and first-session call shape remains pinned.
  After its authorized turn completes, the peer starts another recording and
  proves the new behavior: one full call only after Commit when buffered, or
  a real ASR call before Commit when incremental. The latter is then cancelled
  and must not create a turn. Generation and loaded engine remain unchanged.

Static concurrency review: existing `PendingVoiceRelease` releases the fake on
unwind as well as success; extra capacity leases drop before awaiting the task
tracker. All native-entry and tracker waits are bounded; the second session's
native gate is already released. First turn completion and voice result are
collected together without assuming arrival order before starting the peer
recording. Silence collects ACK and terminal result together because no native
gate delays it. Duplicate terminal events are inspected after joined completion
before a later RPC response helper can discard them. Mutex guards around
captured PCM end before async waits.

The readiness-only supervisor snapshot now uses
`..GatewayVoiceInputSettings::default()` rather than repeating the mode default.
Its enabled/provider/model/runtime fields remain explicit.

The added message cases were statically reviewed but have not been compiled or
executed. They extend the requested authenticated matrix; native SDK behavior
and runtime race outcomes still require the deferred external validation.

Intermediate self-audit corrected a proposed coupling of the new bool to model
desired state: the final implementation reads saved session policy directly and
does not use mode changes to reconcile/reload models. Static reading shows that
the post-preparation cancellation return is distinct from engine failure; the
precise cancellation timing is not reproduced by the runtime test. Static
inspection also restricted new packet fixtures to the protocol's 1600-sample chunk cap.

Commands executed: source/Git reads, editing, `cargo fmt --package
pioneer-gateway -- --check` (reported formatting differences in owned changes),
then `cargo fmt --package pioneer-gateway` (completed). No compilation, tests,
inference, executables or generators were run. Formatting is not evidence of
compilation or correctness. Explicit repeat external acceptance remains required
before executing deferred validation.

Revision formatting commands: `cargo fmt --package pioneer-gateway -- --check`
followed by `cargo fmt --package pioneer-gateway`, then the same `-- --check`
again; these only concern formatting. The final check completed without output.

## Clarification after the reported external static audit

The user reports that the full external static audit covered all 92 changed/new
files and found no confirmed production defects. This clarification changes only
the runtime test name, test comments and the description of cancellation evidence;
all assertions and production code are preserved. No race hook, sleep, polling
or additional test was added. During this pass, only reads, temporary baseline
copies, edits and Git inspection were performed, plus
`cargo fmt --package pioneer-gateway -- --check` (exit 0, no output).
No tests or other executable checks were run. Precise cancellation during input
preparation and after the worker starts waiting for the engine mutex remain
undemonstrated by these tests. Repeat external acceptance is pending; the
implementation is not accepted and the execution ban remains in force.
