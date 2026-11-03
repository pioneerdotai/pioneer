# transcribe-rs 0.3.12, minimal local patch

Upstream: <https://github.com/cjpais/transcribe-rs>.
Exact source revision recorded in the published crate's `.cargo_vcs_info.json`:
`abc6f582967c40839c1d05ec5acc98d1068d9d58`.
License: MIT, copyright (c) 2025 Ilya Stupakov; original license is retained at
`patched/LICENSE` and inside the immutable upstream archive.

`upstream/transcribe-rs-0.3.12.crate` is copied byte-for-byte from the existing
Cargo cache, not downloaded/resolved or rebuilt in this task. The expected
registry checksum recorded by Pioneer's base Cargo.lock is
`96f6b5ff70237570619b5fc53f5a9bf9b8f20dd54197756328a22d0df1982b0a`.
This is a provenance value from the base, not a newly computed integrity seal.

`patched/` contains the upstream `src`, `Cargo.toml`, `Cargo.toml.orig`,
`build.rs`, `LICENSE`, `README.md`, `.cargo_vcs_info.json`, `examples` and `tests`,
extracted from that archive, followed by the small patch in `pioneer.patch`.
Large sample audio, upstream CI, and upstream's standalone lock are left in the
archive; they are not needed by the gateway dependency. No dependency versions,
features, license or build script in the SDK manifest are changed.

Reconstruct from `vendor/transcribe-rs` into a new empty directory (instructions
only; these commands were not run as validation):

```sh
mkdir reconstructed
cd reconstructed
tar -xzf ../upstream/transcribe-rs-0.3.12.crate --strip-components=1 \
  transcribe-rs-0.3.12/src transcribe-rs-0.3.12/Cargo.toml \
  transcribe-rs-0.3.12/Cargo.toml.orig transcribe-rs-0.3.12/build.rs \
  transcribe-rs-0.3.12/LICENSE transcribe-rs-0.3.12/README.md \
  transcribe-rs-0.3.12/.cargo_vcs_info.json transcribe-rs-0.3.12/examples \
  transcribe-rs-0.3.12/tests
mkdir -p src/onnx/moonshine/streaming
patch -p1 < ../pioneer.patch
```

Own delta: additive `DecodingCompletion`/`CompletedTranscription`; greedy stop
metadata; completion-returning methods for Moonshine Base/Streaming, Cohere,
Canary; Canary context clamp (1024 including its prompt); bounded recording-owned
Moonshine Streaming begin/feed/hypothesis/finish; deterministic decoder unit
coverage and explicitly ignored real-export session coverage. Legacy result-only
methods retain their signatures. Other backends and VAD are unchanged.

The native API uses the existing five-graph SDK implementation, not the model
name or `supports_streaming` flag. Decoder reset on changed acoustic context is
also consistent with Moonshine's reference implementation (`decoder_reset` and
`decode_full`):
<https://github.com/moonshine-ai/moonshine/blob/234f60faa0eb388b01cdf7e60aca232af37aefda/core/moonshine-streaming-model.cpp>.
This reference is supporting source inspection, not a dependency or proof of
runtime equivalence. Native export behavior remains unexecuted until acceptance.
