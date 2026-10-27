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
macOS x86_64/aarch64, and Windows x86_64/aarch64. The SDK requires a filesystem
path: the blocking gateway lifecycle stages a private temporary copy with the
notices, verifies the bytes, and loads one Session per recording. There is no
runtime download, user-supplied model path, or mobile resource copy.

Consumer: locked `transcribe-rs 0.3.12`, feature `vad-silero`. Its Silero v4
wrapper takes 480 samples at 16 kHz, inputs `input/sr/h/c`, outputs
`output/hn/cn`, and independent h/c tensors of shape `[2,1,64]`. Upstream v4's
Python helper rejects frames below 512, while the model has dynamic input
length and the upstream README says it was trained on 30 ms. Pioneer uses the
locked Rust wrapper's 480-sample contract; native load/inference verification
is deferred to external acceptance, with a prepared smoke test. This is not
a v5/v6 model with combined recurrent state.
