# Pinned media fixture provenance

The MP3 and MP4 bytes are copied unchanged from the local opencode reference at
`0112a92c416f5ad833d96e7a8308441f0a875d94`:

- `opencode-yup-06.mp3`: `packages/ui/src/assets/audio/yup-06.mp3`
- `opencode-tabs.mp4`: `packages/app/src/assets/help/introducing-tabs.mp4`

`OPENCODE-LICENSE` preserves its MIT license. The regression helpers generate
valid PNG/JPEG/GIF, structured PDFs and PCM WAV fixtures when tests are eventually
authorized. The fixtures and tests were not executed during this revision.

`attachment-conflicts.json` pins real source rows from the existing catalog
source fixture; it is evidence for source semantics, not a second model registry.
`anthropic-parallel-results.json` describes the complete parallel tool-round wire
shape; the test separately compares native data to the budget-pinned bytes.

Round 3: `hermes-feature-connect.webp` copies
`apps/desktop/src/assets/tiers/feature-connect.webp` at Hermes
`a3b56cac95488242856b6fb1f121842a38c3e391`; `HERMES-LICENSE` preserves MIT.
Future Rust builders extract the unchanged VP8 keyframe into bounded WebM
containers with TrackType/DocType metadata, paired with RFC 7845 Opus silence
packets. No fixture construction, parsing, playback or transcoding was executed.
`bedrock-claude-source.json` is the existing pinned models.dev direct Claude row;
`bedrock-summary.json` and `openrouter-models.json` are schema-shaped native
discovery fixtures, not live responses or a new model registry.
