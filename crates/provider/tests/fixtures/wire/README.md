# Wire contract fixtures

Hand-authored canonical examples, not recorded inference or generated snapshots.
They cover supported text/function subsets; they do not certify all model capabilities.

- Gemini: https://ai.google.dev/api/generate-content#Part and #FunctionCall.
- Bedrock: https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ContentBlock.html
  and API_runtime_ReasoningContentBlock.html.
- Ollama: https://docs.ollama.com/api/chat and https://docs.ollama.com/capabilities/tool-calling.
- Chat: https://platform.openai.com/docs/api-reference/chat/create.

Nested arguments retain objects, integers and booleans. The second Gemini call
intentionally omits optional args. Signatures are synthetic opaque strings;
byte fields use base64 strings. No credentials or live payloads are included.

Ollama explicit-ID fixtures retain the native result ID supported by Message at
`ollama/ollama` revision `b3f78b77328bccc4f5af336d16efee69407dbcb1` (`api/types.go`).
The original no-native-ID fixtures remain; older server/model versions are not
certified by either case. Anthropic object inputs follow ToolUseBlockParam and
ToolUseBlock at `anthropics/anthropic-sdk-python` revision
`7a94250411e45e875f9f58303145c90b9acc769e` (`src/anthropic/types/`).
