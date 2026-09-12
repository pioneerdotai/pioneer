//! Static allow/exclude lists from the pinned reference; transformations live in Rust.
pub(super) const TOGETHER_REASONING_ONLY_MODELS: &[&str] =
    &["deepseek-ai/DeepSeek-R1", "MiniMaxAI/MiniMax-M2.7"];
pub(super) const TOGETHER_REASONING_EFFORT_MODELS: &[&str] =
    &["openai/gpt-oss-20b", "openai/gpt-oss-120b"];
pub(super) const TOGETHER_TOGGLE_REASONING_EFFORT_MODELS: &[&str] =
    &["deepseek-ai/DeepSeek-V4-Pro"];
pub(super) const NVIDIA_NIM_UNSUPPORTED_MODELS: &[&str] = &[
    "abacusai/dracarys-llama-3.1-70b-instruct",
    "bytedance/seed-oss-36b-instruct",
    "deepseek-ai/deepseek-v4-flash",
    "deepseek-ai/deepseek-v4-pro",
    "google/gemma-2-2b-it",
    "google/gemma-3n-e2b-it",
    "google/gemma-3n-e4b-it",
    "google/gemma-4-31b-it",
    "meta/llama-3.2-1b-instruct",
    "meta/llama-4-maverick-17b-128e-instruct",
    "microsoft/phi-4-mini-instruct",
    "minimaxai/minimax-m2.7",
    "mistralai/mistral-nemotron",
    "nvidia/nemotron-mini-4b-instruct",
    "qwen/qwen3-next-80b-a3b-instruct",
    "qwen/qwen3.5-397b-a17b",
    "sarvamai/sarvam-m",
    "upstage/solar-10.7b-instruct",
];
pub(super) const ZAI_TOOL_STREAM_UNSUPPORTED_MODELS: &[&str] =
    &["glm-4.5", "glm-4.5-air", "glm-4.5-flash", "glm-4.5v"];
pub(super) const EAGER_TOOL_INPUT_STREAMING_UNSUPPORTED_ANTHROPIC_MODELS: &[&str] = &[
    "github-copilot:claude-haiku-4.5",
    "github-copilot:claude-sonnet-4",
    "github-copilot:claude-sonnet-4.5",
];
pub(super) const QWEN_TOKEN_PLAN_REASONING_EFFORT_FALLBACK_MODEL_IDS: &[&str] =
    &["glm-5", "glm-5.1"];
pub(super) const QWEN_TOKEN_PLAN_EXCLUDED_MODEL_IDS: &[&str] = &["qwen3.8-max-preview"];
pub(super) const QWEN_TOKEN_PLAN_PROVIDER_IDS: &[&str] = &[
    "qwen-token-plan",
    "qwen-token-plan-cn",
    "qwen-token-plan-individual",
];
pub(super) const QWEN_TOKEN_PLAN_INDIVIDUAL_MODEL_IDS: &[&str] = &[
    "deepseek-v4-flash-0731",
    "deepseek-v4-pro",
    "deepseek-v4-pro-0813",
    "glm-5.2",
    "qwen3.6-flash",
    "qwen3.7-max",
    "qwen3.7-plus",
    "qwen3.8-flash",
    "qwen3.8-max",
];
pub(super) const OPENROUTER_KIMI_K3_MODEL_IDS: &[&str] =
    &["moonshotai/kimi-k3", "~moonshotai/kimi-latest"];
pub(super) const BEDROCK_INFERENCE_PROFILE_ONLY_MODEL_IDS: &[&str] = &["anthropic.claude-opus-5"];
pub(super) const MODELS_DEV_OPENAI_UNSUPPORTED_MODEL_IDS: &[&str] = &["gpt-5.6"];
pub(super) const OPENAI_TOOL_SEARCH_MODEL_IDS: &[&str] = &[
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.4-pro",
    "gpt-5.5",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
    "gpt-6-astra",
];
pub(super) const OPENAI_CODEX_ADDITIONAL_TOOLS_MODEL_IDS: &[&str] = &[
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
    "gpt-6-astra",
];
pub(super) const OPENAI_SHORT_CONTEXT_CAPPED_MODEL_IDS: &[&str] = &[
    "gpt-5.4",
    "gpt-5.5",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
    "gpt-6-astra",
];
pub(super) const OPENAI_LONG_CONTEXT_PRICING_MODEL_IDS: &[&str] = &[
    "gpt-5.4",
    "gpt-5.4-pro",
    "gpt-5.5",
    "gpt-5.5-pro",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
    "gpt-6-astra",
];
pub(super) const OPENAI_RESPONSES_NONE_REASONING_MODELS: &[&str] = &[
    "gpt-5.1",
    "gpt-5.2",
    "gpt-5.3-codex",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.4-nano",
    "gpt-5.5",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
];
pub(super) const XAI_BUILTIN_EXCLUDED_MODEL_IDS: &[&str] = &[
    "grok-3",
    "grok-3-fast",
    "grok-4.20-0309-non-reasoning",
    "grok-4.20-0309-reasoning",
    "grok-build-0.1",
    "grok-code-fast-1",
];
pub(super) const OPENCODE_OPENAI_COMPLETIONS_LONG_CACHE_RETENTION_UNSUPPORTED_MODELS: &[&str] = &[
    "opencode:deepseek-v4-flash",
    "opencode:deepseek-v4-pro",
    "opencode:kimi-k2.5",
    "opencode:kimi-k2.6",
    "opencode:minimax-m2.7",
    "opencode-go:kimi-k2.6",
];
pub(super) const GITHUB_COPILOT_EXTENDED_CONTEXT_MODELS: &[&str] = &[
    "claude-fable-5",
    "claude-opus-4.6",
    "claude-opus-4.7",
    "claude-opus-4.8",
    "claude-opus-5",
    "claude-sonnet-4.6",
    "claude-sonnet-5",
    "gpt-5.3-codex",
    "gpt-5.4",
    "gpt-5.5",
];
pub(super) const VERIFIED_ANTHROPIC_MID_CONVO_EFFORT_PROVIDERS: &[&str] =
    &["anthropic", "openrouter"];
pub(super) const OPENAI_GRAMMAR_TOOL_PROVIDERS: &[&str] = &[
    "openai",
    "openai-codex",
    "azure-openai-responses",
    "github-copilot",
    "opencode",
    "cloudflare-ai-gateway",
];
pub(super) const OPENAI_GRAMMAR_TOOL_APIS: &[&str] = &[
    "openai-responses",
    "azure-openai-responses",
    "openai-codex-responses",
];
pub(super) const MINIMAX_DIRECT_SUPPORTED_IDS: &[&str] =
    &["MiniMax-M2.7", "MiniMax-M2.7-highspeed", "MiniMax-M3"];
