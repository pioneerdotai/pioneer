# G05 synthetic history fixtures

These source-derived shapes are not recorded vendor acceptances and have never been executed in this change.

- Anthropic interleaved JSON/SSE deliberately exercises original block order, signed thinking, redaction, text between parallel tool uses. Ordinary thinking → text → tools is unchanged. Whether a specific selected model returns the unusual within-response sequence still needs an allowed recorded/live conformance run.
- Gemini JSON covers thought/text/function/media signatures; SSE covers two id-less calls in separate frames, final metadata without content, and an empty signed text part. Partial function-argument streaming and built-in server tools are separate unconfirmed profiles.
- DeepSeek mode-switch fixture documents G04 reasoning-control dependency. It must not be treated as a passing G05 runtime test.

Sources: https://platform.claude.com/docs/en/build-with-claude/thinking-tool-workflows ; https://ai.google.dev/gemini-api/docs/generate-content/thinking ; https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures ; https://ai.google.dev/api/generate-content ; https://api-docs.deepseek.com/guides/thinking_mode/
