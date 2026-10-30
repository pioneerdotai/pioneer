# Silero VAD v4 model

Copyright (c) 2020-present Silero Team. Distributed under the MIT license in
the adjacent LICENSE file, copied verbatim from the same upstream revision.
The upstream repository publishes its pretrained VAD model under MIT; its
README explicitly describes the model as MIT licensed (no separate weights
license). No conversion or modification of the model bytes was performed.

- Upstream: https://github.com/snakers4/silero-vad
- Tag observed: v4.0; annotated tag object `7a176cc294a2c40615458e50895ed9703782638d`
- Immutable commit: `915dd3d639b8333a52e001af095f87c5b7f1e0ac`
- Original path: `files/silero_vad.onnx`
- Model: https://raw.githubusercontent.com/snakers4/silero-vad/915dd3d639b8333a52e001af095f87c5b7f1e0ac/files/silero_vad.onnx
- License: https://raw.githubusercontent.com/snakers4/silero-vad/915dd3d639b8333a52e001af095f87c5b7f1e0ac/LICENSE
- License statement: https://github.com/snakers4/silero-vad/blob/915dd3d639b8333a52e001af095f87c5b7f1e0ac/README.md
- Size: 1,807,522 bytes
- SHA256: `a35ebf52fd3ce5f1469b2a36158dba761bc47b973ea3382b3186ca15b1f5af28`
- LICENSE SHA256: `2e63e9a38b6e8fc0c7bc37ce174caca1862870856c6daf5697cfb785e925520b`

Gateway embeds this model, this notice, and LICENSE. Its existing gzip/ZIP and
computer-use gateway packaging therefore carries them on Linux x86_64/aarch64,
macOS x86_64/aarch64, and Windows x86_64/aarch64. The ONNX loader uses a filesystem
path: the blocking gateway lifecycle stages a private temporary copy with the
notices, verifies the bytes, and loads one Session per recording. There is no
runtime download, user-supplied model path, or mobile resource copy.

Consumer: gateway-local `SileroV4Session` in `voice/silero.rs`, using the
already locked `ort 2.0.0-rc.12` ONNX runtime. No ASR SDK fork is involved.
Inputs are `input/sr/h/c`, outputs `output/hn/cn`, with independent h/c tensors
of shape `[2,1,64]`. The sample rate is scalar 16000, as in the pinned Python
helper. Gateway feeds sequential 512-sample (32 ms) frames. This matches v4
`utils_vad.py`'s minimum accepted 16 kHz frame and default `audio_forward` /
`get_speech_timestamps` frame size:
https://github.com/snakers4/silero-vad/blob/915dd3d639b8333a52e001af095f87c5b7f1e0ac/utils_vad.py

The locked transcribe-rs 0.3.12 VAD wrapper instead requires 480 samples.
That contract difference is the reason for the narrow local adapter, not a
claim of measured quality degradation. Only the final partial native frame
is zero padded; real PCM/time never includes that padding. Sessions retain
recurrent state through silence and are never shared between recordings.
Native load/inference verification is deferred to external acceptance; the
ignored smoke checks isolation but is not an independent quality oracle.
Model bytes, license, integrity checks and packaging are unchanged. This is
not a v5/v6 model with combined recurrent state.
