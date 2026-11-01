# macOS voice Xrun review

- Base and implementation-review HEAD: `0b8be31f0e97e3db4dccfcc57fa6a343f2fff8a5`.
- Branch: `fix/macos-voice-xrun`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/macos-voice-xrun`.
- The user accepted the implementation and authorized committing, pushing, and opening a PR.

After e8ab1da3a, Xrun set fatal `CallbackState.error`, so stop/wait blocked
finalization. CPAL 0.18.2 on macOS emits Xrun for the device-global
`kAudioDeviceProcessorOverload` notification. Its ErrorKind documentation says
"potential audio glitch"; the notification gives no lost-sample count for this
capture operation.

Changed files:

- `crates/desktop/src/audio/capture.rs`: route macOS Xrun through the existing
  ordered Discontinuity path, update comments, and adapt regression coverage.
- `VOICE_MACOS_XRUN_REVIEW.md`: this review record, translated into English for publication.

## Static review

The new match arm is gated to macOS. Other platforms retain fatal Xrun handling.
The callback uses `deliver` and the existing bounded queue without DSP or
transport work. The shared callback mutex and FIFO preserve accepted event order.
The worker finishes the old resampler segment and starts fresh on subsequent
audio. It does not replay PCM. Failed discontinuity admission remains sticky in
the callback gate and queue and blocks Commit.

Admission fences, the callback barrier, readiness from valid nonempty audio,
Stop/drain, Cancel/Drop, single tail flush, and cleanup remain intact. Worker
errors/panics, PCM or tail delivery failures, native stop failures, unsupported
formats, and device removal still prevent successful finalization.

Reviewed callers in thread_port.rs and client composer/voice.rs retain operation,
generation, and authority checks, audio_failed/audio_closed guards, terminal
claims, and integrity sealing before finalize. Gateway verified finalization
remains unchanged, including integrity rejection.

The final review covered the entire diff from the base, this document, the
callback/worker/flow paths, and affected callers. It added Xrun tail-failure
coverage and found no further issues within scope. Production changes consist
only of macOS classification and two comments. The main checkout retained its
four unrelated modified files; neighboring worktrees were left alone.

## Regression coverage

Written or adapted, without execution during implementation:

- `macos_xrun_drains_ordered_segments_and_finalizes_once`: real queue and worker,
  PCM followed by two short 48 kHz segments separated by Xrun, Stop/drain,
  exact PCM/timestamps/sequences, one finalize/stop, repeated finalize, and late
  notifications/audio after Stop.
- `macos_xrun_without_audio_never_confirms_readiness`: Xrun and an empty buffer,
  worker join, then late Xrun/audio with no readiness confirmation.
- `full_capture_queue_rejects_commit_prefix_and_cleans_up_once`: macOS Xrun
  triggers queue overflow, which remains sticky and blocks Commit.
- `flush_failure_prevents_finalize_and_releases_session_once`: adds macOS Xrun
  as a segment boundary whose tail delivery fails.
- `cancelled_or_stale_recorder_cannot_confirm_a_new_capture`: adds late Xrun
  from a cancelled recorder.
- Renames `xrun_known_source_loss_cannot_finalize_successfully` to
  `xrun_remains_fatal_outside_macos`, gated to non-macOS.

Existing callback barrier, route/filter history, worker failure/panic, and native
stop failure coverage remains. New scenarios use existing FakeBackend/FakeGateway,
channel barriers, and joins without sleeps or new production APIs. Expected PCM
is literal: identity [-1, 0.5] plus separate zero tails. Two short tails distinguish
separate segments from one continuous resampler. Static inspection indicates the
old fatal classification would fail the successful macOS scenario; that baseline
was not executed.

## Command log

Repeated read-only inspections are grouped:

- `pwd`; `rg --files -g AGENTS.md -g '!node_modules' -g '!target' /Users/alexander/Code/pioneer`;
  ancestor AGENTS discovery with shell `for`, `test -f`, and `cat`.
- `cat` and `sed -n` on root/desktop/client AGENTS, test-audit skill references,
  capture.rs, thread_port.rs, capture_worker.rs, resampling.rs, composer/voice.rs,
  gateway voice_handlers.rs, manifests, Cargo.lock, CI, and local CPAL sources.
- `rg --files /Users/alexander/.cargo/registry/src` filtered for CPAL 0.18.2;
  `rg -n`/`rg -n -C 7` for Xrun, ProcessorOverload, lifecycle, and integrity.
  A gateway glob search hit zsh's no-matches error; a directory search with
  `--glob '*.rs'` replaced it.
- `git status --short` in the main checkout and isolated worktree;
  `git cat-file -t 0b8be31f0e97e3db4dccfcc57fa6a343f2fff8a5` confirmed a local commit;
  `git worktree list`; `git branch --list 'fix/macos-voice-xrun*'`.
- `git worktree add -b fix/macos-voice-xrun /Users/alexander/Code/pioneer/pioneer/.worktrees/macos-voice-xrun 0b8be31f0e97e3db4dccfcc57fa6a343f2fff8a5`.
- `git show e8ab1da3a -- crates/desktop/src/audio/capture.rs`; `git diff`, including
  comparison to the base; `git diff --stat`; `git diff --name-only`;
  `git diff --cached --stat`; `git rev-parse HEAD`; `git branch --show-current`.
- Implementation edits used `apply_patch`.
- Scoped cargo fmt with `--bin pioneer-app` returned exit 2 because cargo fmt
  does not support `--bin`.
- `cargo fmt --manifest-path crates/desktop/Cargo.toml -- --config skip_children=true crates/desktop/src/audio/capture.rs`
  returned exit 0, as did the same command with `--check` after `skip_children=true`.
  No other files changed.
- Publication preparation: read the humanizer skill, inspected `git remote -v`,
  `git log -1 --oneline`, hooks configuration, and `gh auth status`; translated
  this record and edited the PR description using the humanizer workflow.

## Follow-up validation

Implementation acceptance has now been given. Remaining validation includes
focused desktop audio tests on macOS and non-macOS with cargo nextest using
`--locked --test-threads 2 --no-fail-fast --retries 0`, compilation and clippy under
the project workflow, and separate hardware checks of first/repeated capture,
Stop/Cancel, and transport failure.

The implementation phase did not run tests, builds, applications, microphones,
inference, or models, or install dependencies. The reported hardware scenario
was not reproduced. The overload cause, timing relative to the first callback,
and physical audio loss remain unknown. This fixes fatal classification of the
notification without claiming to fix every CoreAudio overload.
