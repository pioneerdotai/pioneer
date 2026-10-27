# Video recording

<!-- Modified for Pioneer. -->
Bare commands assume the task's named session is set for this execution.

## Contents

- [Requirements](#requirements)
- [Record the active page](#record-the-active-page)
- [Frame rate and cursor](#frame-rate-and-cursor)
- [Contact sheets](#contact-sheets)
- [Reproduction and cleanup](#reproduction-and-cleanup)
- [Limits and delivery](#limits-and-delivery)

## Requirements

Recording needs `ffmpeg` on PATH with the matching encoder: `libvpx` for
`.webm` (VP8), or `libx264` for `.mp4` (H.264). A path with no extension is
rejected. Other extensions are handed to ffmpeg with H.264; prefer an explicitly
supported format.

For missing ffmpeg, use the host's existing package manager when installation
is within scope (for example `brew install ffmpeg` on macOS or
`sudo apt install ffmpeg` on Debian/Ubuntu). `agent-browser doctor` reports
recording dependencies. Other browser operations do not need ffmpeg.

## Record the active page

`record start` records the active tab as it exists: no new context, no
navigation and no loss of hydration/form state when no URL is supplied.

```bash
agent-browser open https://app.example.com
agent-browser snapshot -i
agent-browser record start ./flow.webm
# Perform the requested flow with observed refs and result-specific waits.
agent-browser record stop
```

A URL argument navigates that same active tab before recording. For a separate
recording tab, create one deliberately:

```bash
agent-browser tab new https://app.example.com
agent-browser record start ./flow.webm
agent-browser record stop
agent-browser record restart ./take2.webm --fps 60
```

`record restart` stops the current take and starts a new file. The example
commands describe alternatives; do not restart a recording the task did not own.

## Frame rate and cursor

| Rate | Suitable use |
| --- | --- |
| 60 fps | Short animation, drag and scroll-motion checks |
| 30 fps (default) | Walkthroughs and interaction evidence |
| 1–15 fps | Long timeline/soak captures where motion detail is secondary |

`--fps` accepts 1–60. The recording holds the most recent Chrome frame
between repaints. `record stop --json` distinguishes written `frames` from
distinct `capturedFrames`; a 60 fps container does not prove the page rendered
60 distinct frames per second.

```bash
agent-browser record start ./walkthrough.webm --fps 30 --cursor
```

Chrome screencasts do not include the native cursor. `--cursor` renders a
temporary inert pointer/click overlay with the page, synchronizing drags and
captured motion. It is absent from accessibility snapshots, present in
screenshots taken during recording, and removed on stop.

## Contact sheets

```bash
agent-browser record start ./checkout.webm --contact-sheet
agent-browser record start ./checkout.webm --contact-sheet-threshold 0.02
```

These are alternative starts. A contact sheet is a timestamped PNG summary with
changed-region highlights, capped at 100 selected frames. The change threshold
is 0–1, defaults to 0.05, and implies `--contact-sheet` when supplied.

## Reproduction and cleanup

Explore enough to identify a repeatable failure, then begin recording before
the relevant actions. Capture the minimum clear reproduction. Brief pauses can
make a demonstration readable, but still wait for real UI conditions before
claiming success. Do not substitute a fixed sleep for a missing readiness signal.

For error cleanup, keep ownership explicit: stop a recording this task started,
and close only a session it launched. A reusable shell script should not close
a caller-supplied session or stop someone else's existing recording. The
[session guide](session-management.md#cleanup-and-recovery) covers attached CDP
and user-owned apps.

Combine a recording with selected screenshots when the frames make an issue
easier to inspect. A screenshot and steps may be sufficient for a static typo
or layout defect; recording is most useful for timing, motion or multi-step
behavior. See [dogfood](dogfood.md) for issue reports.

For CI evidence, finish/flush the recording on both success and failure and
preserve the test's original exit status. Use task-specific output paths rather
than a shared temporary filename. Avoid recording real passwords or sensitive
authenticated screens when a demo account or redacted capture serves the task.

## Limits and delivery

Higher frame rates add overhead, bandwidth and disk usage. A slowly repainting
page limits distinct captured frames regardless of the requested rate.
Headless hosts can have codec/capture limits; a black WebGPU canvas needs the
specific [WebGPU workflow](webgpu.md), not just another recording attempt.

Inspect the relevant images or video with a supported viewer before making
visual claims. Static contact sheets cannot prove an interaction was tested.
Deliver only intended public artifacts through the current runtime's tools;
see [tools and runtime](tools-and-runtime.md#images-files-and-delivery).
