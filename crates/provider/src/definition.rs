//! Endpoint definitions available before a runtime provider is constructed.
//! Adapter constants are referenced here so the endpoint used for requests and
//! the endpoint advertised by the gateway have the same owner.
use crate::providers::{
    anthropic, copilot, deepseek, gemini, glm, ollama, openai, openrouter, telnyx,
};
use anyhow::{Result, bail};

/// RFC 3986 segment encoding for deployment names and AWS model IDs/ARNs.
/// Unlike form encoding, spaces become %20, `*` is escaped and `~` is retained.
pub(crate) fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(&mut encoded, "%{byte:02X}").expect("write to String");
        }
    }
    encoded
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderEndpointDefinition {
    pub name: &'static str,
    pub default_base_url: Option<&'static str>,
    pub supports_base_url_override: bool,
    /// A locally hosted compatible endpoint can be used without an API key.
    pub base_url_without_key: bool,
}

impl ProviderEndpointDefinition {
    /// Retirement applies to this public product, including its saved aliases.
    /// A private replacement must be configured explicitly as `custom`.
    pub fn retirement_reason(self) -> Option<&'static str> {
        match self.name {
            "yi" => Some(
                "Yi public API was retired on 2026-09-03. Saved credentials and history are retained; choose another provider explicitly.",
            ),
            "hyperbolic" => Some(
                "Hyperbolic serverless inference API was retired. Saved credentials and history are retained; the GPU rental product is not an automatic replacement.",
            ),
            _ => None,
        }
    }
}

impl ProviderEndpointDefinition {
    const fn fixed(name: &'static str, url: &'static str, supports_override: bool) -> Self {
        Self {
            name,
            default_base_url: Some(url),
            supports_base_url_override: supports_override,
            base_url_without_key: false,
        }
    }
    const fn compatible(name: &'static str, url: &'static str, base_url_without_key: bool) -> Self {
        Self {
            name,
            default_base_url: Some(url),
            supports_base_url_override: true,
            base_url_without_key,
        }
    }
    const fn without_endpoint(name: &'static str) -> Self {
        Self {
            name,
            default_base_url: None,
            supports_base_url_override: false,
            base_url_without_key: false,
        }
    }
    const fn overridable_without_endpoint(name: &'static str) -> Self {
        Self {
            name,
            default_base_url: None,
            supports_base_url_override: true,
            base_url_without_key: false,
        }
    }
}

// These entries are also consumed by the compatible-provider factory. Its
// default endpoint must never be supplied from a second URL table.
const COMPATIBLE: &[ProviderEndpointDefinition] = &[
    ProviderEndpointDefinition::compatible("groq", "https://api.groq.com/openai/v1", false),
    ProviderEndpointDefinition::compatible("mistral", "https://api.mistral.ai/v1", false),
    ProviderEndpointDefinition::compatible("xai", "https://api.x.ai/v1", false),
    ProviderEndpointDefinition::compatible("together", "https://api.together.ai/v1", false),
    ProviderEndpointDefinition::compatible(
        "fireworks",
        "https://api.fireworks.ai/inference/v1",
        false,
    ),
    ProviderEndpointDefinition::compatible("novita", "https://api.novita.ai/openai/v1", false),
    ProviderEndpointDefinition::compatible("perplexity", "https://api.perplexity.ai", false),
    ProviderEndpointDefinition::compatible(
        "cohere",
        "https://api.cohere.ai/compatibility/v1",
        false,
    ),
    ProviderEndpointDefinition::compatible("venice", "https://api.venice.ai/api/v1", false),
    ProviderEndpointDefinition::compatible("cerebras", "https://api.cerebras.ai/v1", false),
    ProviderEndpointDefinition::compatible("sambanova", "https://api.sambanova.ai/v1", false),
    ProviderEndpointDefinition::compatible("hyperbolic", "https://api.hyperbolic.xyz/v1", false),
    ProviderEndpointDefinition::compatible(
        "deepinfra",
        "https://api.deepinfra.com/v1/openai",
        false,
    ),
    ProviderEndpointDefinition::compatible(
        "huggingface",
        "https://router.huggingface.co/v1",
        false,
    ),
    ProviderEndpointDefinition::compatible("ai21", "https://api.ai21.com/studio/v1", false),
    ProviderEndpointDefinition::compatible("reka", "https://api.reka.ai/v1", false),
    ProviderEndpointDefinition::compatible("baseten", "https://inference.baseten.co/v1", false),
    ProviderEndpointDefinition::compatible("nscale", "https://inference.api.nscale.com/v1", false),
    ProviderEndpointDefinition::compatible(
        "anyscale",
        "https://api.endpoints.anyscale.com/v1",
        false,
    ),
    ProviderEndpointDefinition::compatible("nebius", "https://api.studio.nebius.ai/v1", false),
    ProviderEndpointDefinition::compatible(
        "friendli",
        "https://api.friendli.ai/serverless/v1",
        false,
    ),
    ProviderEndpointDefinition::compatible(
        "lepton",
        "https://llama3-1-405b.lepton.run/api/v1",
        false,
    ),
    ProviderEndpointDefinition::compatible("siliconflow", "https://api.siliconflow.cn/v1", false),
    ProviderEndpointDefinition::compatible("aihubmix", "https://aihubmix.com/v1", false),
    ProviderEndpointDefinition::compatible("astrai", "https://as-trai.com/v1", false),
    ProviderEndpointDefinition::compatible("stepfun", "https://api.stepfun.com/v1", false),
    ProviderEndpointDefinition::compatible("baichuan", "https://api.baichuan-ai.com/v1", false),
    ProviderEndpointDefinition::compatible("yi", "https://api.lingyiwanwu.com/v1", false),
    ProviderEndpointDefinition::compatible(
        "hunyuan",
        "https://api.hunyuan.cloud.tencent.com/v1",
        false,
    ),
    ProviderEndpointDefinition::compatible("ovhcloud", "https://api.ai.cloud.ovh.net/v1", false),
    ProviderEndpointDefinition::compatible("nvidia", "https://integrate.api.nvidia.com/v1", false),
    ProviderEndpointDefinition::compatible(
        "synthetic",
        "https://api.synthetic.new/openai/v1",
        false,
    ),
    ProviderEndpointDefinition::compatible(
        "doubao",
        "https://ark.cn-beijing.volces.com/api/v3",
        false,
    ),
    // V2 uses a supplied Bearer API key; the legacy aip host uses a different
    // RPC/access-token contract and cannot be served by this Chat transport.
    ProviderEndpointDefinition::compatible("qianfan", "https://qianfan.baidubce.com/v2", false),
    ProviderEndpointDefinition::compatible("lmstudio", "http://localhost:1234/v1", true),
    ProviderEndpointDefinition::compatible("llamacpp", "http://localhost:8080/v1", true),
    ProviderEndpointDefinition::compatible("sglang", "http://localhost:30000/v1", true),
    ProviderEndpointDefinition::compatible("vllm", "http://localhost:8000/v1", true),
    ProviderEndpointDefinition::compatible("osaurus", "http://localhost:1337/v1", true),
    ProviderEndpointDefinition::compatible("litellm", "http://localhost:4000/v1", true),
    ProviderEndpointDefinition::compatible("custom", "http://localhost:8000/v1", true),
];

const SPECIAL: &[ProviderEndpointDefinition] = &[
    ProviderEndpointDefinition::fixed("openai", openai::BASE_URL, true),
    ProviderEndpointDefinition::fixed("anthropic", anthropic::BASE_URL, true),
    ProviderEndpointDefinition::fixed("openrouter", openrouter::BASE_URL, true),
    ProviderEndpointDefinition::fixed("deepseek", deepseek::BASE_URL, true),
    ProviderEndpointDefinition::fixed("gemini", gemini::BASE_URL, true),
    ProviderEndpointDefinition {
        name: "ollama",
        default_base_url: Some(ollama::DEFAULT_BASE_URL),
        supports_base_url_override: true,
        base_url_without_key: true,
    },
    ProviderEndpointDefinition::fixed("telnyx", telnyx::BASE_URL, true),
    ProviderEndpointDefinition::fixed("copilot", copilot::BASE_URL, true),
    ProviderEndpointDefinition::fixed("glm", glm::DEFAULT_BASE_URL, true),
    ProviderEndpointDefinition::fixed("zai", glm::GLOBAL_BASE_URL, true),
    ProviderEndpointDefinition::fixed("glm-coding", glm::CN_CODING_BASE_URL, true),
    ProviderEndpointDefinition::fixed("zai-coding", glm::GLOBAL_CODING_BASE_URL, true),
    ProviderEndpointDefinition::without_endpoint("local"),
    ProviderEndpointDefinition::without_endpoint("bedrock"),
    ProviderEndpointDefinition::overridable_without_endpoint("azure-openai"),
];

pub fn provider_definitions() -> impl Iterator<Item = ProviderEndpointDefinition> {
    SPECIAL.iter().chain(COMPATIBLE.iter()).copied()
}

pub fn provider_definition(name: &str) -> Option<ProviderEndpointDefinition> {
    let name = name.trim().to_ascii_lowercase();
    let canonical = match name.as_str() {
        "google" | "google-gemini" => "gemini",
        "github-copilot" => "copilot",
        "zhipu" | "bigmodel" | "glm-cn" | "zhipu-cn" => "glm",
        "glm-global" | "zhipu-global" | "z.ai" | "z-ai" => "zai",
        "glm-coding-cn" | "zhipu-coding" | "zai-coding-cn" => "glm-coding",
        "glm-coding-global" | "zai-coding-plan" => "zai-coding",
        "aws-bedrock" => "bedrock",
        "azure_openai" | "azure" => "azure-openai",
        "grok" => "xai",
        "together-ai" => "together",
        "fireworks-ai" => "fireworks",
        "deep-infra" => "deepinfra",
        "hf" => "huggingface",
        "ai21-labs" => "ai21",
        "friendliai" => "friendli",
        "lepton-ai" => "lepton",
        "silicon-flow" => "siliconflow",
        "step" => "stepfun",
        "01ai" | "lingyiwanwu" => "yi",
        "tencent" => "hunyuan",
        "ovh" => "ovhcloud",
        "nvidia-nim" => "nvidia",
        "volcengine" | "ark" => "doubao",
        "baidu" => "qianfan",
        "lm-studio" => "lmstudio",
        "llama.cpp" => "llamacpp",
        "lite-llm" => "litellm",
        "compatible" | "openai-compatible" => "custom",
        other => other,
    };
    provider_definitions().find(|definition| definition.name == canonical)
}

/// HTTP(S) endpoint with an optional path prefix. URI credentials, query and
/// fragment are disallowed because adapters append request paths to this URL.
pub fn validate_provider_base_url(provider: &str, value: &str) -> Result<String> {
    let definition =
        provider_definition(provider).ok_or_else(|| anyhow::anyhow!("unknown provider"))?;
    if !definition.supports_base_url_override {
        bail!("provider does not support an API base URL override");
    }
    let value = value.trim();
    let parsed = url::Url::parse(value).map_err(|_| anyhow::anyhow!("invalid API base URL"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!(
            "API base URL must be an HTTP(S) URL with a host and without credentials, query, or fragment"
        );
    }
    if definition.name == "qianfan" && parsed.host_str() == Some("aip.baidubce.com") {
        bail!(
            "Qianfan legacy aip RPC/access-token API is not supported by this Chat adapter; use the V2 compatible API with its Bearer credentials"
        );
    }
    if definition.name == "azure-openai" {
        let path = parsed.path().trim_end_matches('/');
        if path.contains("/openai/deployments/")
            || path.ends_with("/chat/completions")
            || path.ends_with("/responses")
        {
            bail!(
                "Azure Chat base URL must be a resource/gateway root or openai/v1 prefix, not a final request URL"
            );
        }
    }
    Ok(value.trim_end_matches('/').to_owned())
}

pub fn provider_is_available(
    api_key: bool,
    proxy: bool,
    base_url: bool,
    definition: ProviderEndpointDefinition,
) -> bool {
    if definition.name == "bedrock" {
        // This adapter supports environment SigV4 credentials, including a
        // session token. A saved generic API key/proxy is not an AWS identity.
        return crate::providers::BedrockProvider::environment_is_configured();
    }
    definition.retirement_reason().is_none()
        && (definition.name == "local"
            || api_key
            || proxy
            || (base_url && definition.base_url_without_key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glm_aliases_keep_cn_credentials_separate_from_global_and_coding() {
        for (alias, canonical, endpoint) in [
            ("glm", "glm", glm::DEFAULT_BASE_URL),
            ("zhipu", "glm", glm::DEFAULT_BASE_URL),
            ("bigmodel", "glm", glm::DEFAULT_BASE_URL),
            ("glm-cn", "glm", glm::DEFAULT_BASE_URL),
            ("zhipu-cn", "glm", glm::DEFAULT_BASE_URL),
            ("glm-global", "zai", glm::GLOBAL_BASE_URL),
            ("zhipu-global", "zai", glm::GLOBAL_BASE_URL),
            ("z.ai", "zai", glm::GLOBAL_BASE_URL),
            ("glm-coding-cn", "glm-coding", glm::CN_CODING_BASE_URL),
            ("zai-coding-plan", "zai-coding", glm::GLOBAL_CODING_BASE_URL),
        ] {
            let profile = provider_definition(alias).unwrap();
            assert_eq!(profile.name, canonical, "{alias}");
            assert_eq!(profile.default_base_url, Some(endpoint), "{alias}");
            assert_eq!(
                validate_provider_base_url(alias, "https://example.test/private/v7/").unwrap(),
                "https://example.test/private/v7"
            );
        }
        assert_eq!(provider_definitions().count(), 56);
    }

    #[test]
    fn retirement_is_terminal_with_saved_keys_proxy_or_endpoint_override() {
        for alias in ["yi", "01ai", "lingyiwanwu", "hyperbolic"] {
            let profile = provider_definition(alias).unwrap();
            assert!(profile.retirement_reason().unwrap().contains("retired"));
            assert!(!provider_is_available(true, true, true, profile));
            // Configuration stays parseable; lifecycle gates operations.
            assert!(validate_provider_base_url(alias, "https://example.test/v1").is_ok());
        }
        for legacy in ["nebius", "anyscale", "ovhcloud", "lepton"] {
            assert!(
                provider_definition(legacy)
                    .unwrap()
                    .retirement_reason()
                    .is_none()
            );
        }
    }

    #[test]
    fn path_segments_encode_reserved_bytes_without_form_encoding() {
        assert_eq!(encode_path_segment("a b/+?%*~"), "a%20b%2F%2B%3F%25%2A~");
    }

    #[test]
    fn incompatible_legacy_qianfan_and_azure_protocol_overrides_are_explicit() {
        assert!(
            validate_provider_base_url("baidu", "https://aip.baidubce.com")
                .unwrap_err()
                .to_string()
                .contains("legacy")
        );
        assert!(validate_provider_base_url("qianfan", "https://example.test/private/v2/").is_ok());
        for base in [
            "https://example.test/openai/v1/responses",
            "https://example.test/openai/deployments/name/chat/completions",
        ] {
            assert!(
                validate_provider_base_url("azure", base)
                    .unwrap_err()
                    .to_string()
                    .contains("resource/gateway root")
            );
        }
    }

    #[test]
    fn metadata_is_available_without_credentials_or_runtime_instances() {
        let openai = provider_definition("OpenAI").unwrap();
        assert_eq!(openai.default_base_url, Some(openai::BASE_URL));
        assert!(openai.supports_base_url_override);
        let bedrock = provider_definition("aws-bedrock").unwrap();
        assert_eq!(bedrock.default_base_url, None);
        assert!(!bedrock.supports_base_url_override);
        let vllm = provider_definition("vllm").unwrap();
        assert!(vllm.base_url_without_key);
        assert!(provider_is_available(false, false, true, vllm));
        assert!(!provider_is_available(false, false, true, openai));
        for name in [
            "ollama",
            "glm",
            "deepseek",
            "gemini",
            "telnyx",
            "copilot",
            "azure-openai",
        ] {
            let definition = provider_definition(name).unwrap();
            assert!(definition.supports_base_url_override, "{name}");
            assert_eq!(definition.base_url_without_key, name == "ollama", "{name}");
        }
        let azure = provider_definition("azure").unwrap();
        assert_eq!(azure.default_base_url, None);
        assert!(azure.supports_base_url_override);
        assert!(provider_is_available(
            false,
            false,
            true,
            provider_definition("ollama").unwrap()
        ));
        assert!(!provider_is_available(false, false, true, azure));
        assert!(
            !provider_definition("local")
                .unwrap()
                .supports_base_url_override
        );
    }

    #[test]
    fn base_url_validation_keeps_path_prefix_and_rejects_unsafe_components() {
        assert_eq!(
            validate_provider_base_url("openai", " https://example.test/api/prefix/ ").unwrap(),
            "https://example.test/api/prefix"
        );
        assert_eq!(
            validate_provider_base_url("azure", "https://gateway.example/team/").unwrap(),
            "https://gateway.example/team"
        );
        for value in [
            "ftp://example.test",
            "https://",
            "https://user:pass@example.test",
            "https://example.test/v1?token=secret",
            "https://example.test/v1#fragment",
        ] {
            assert!(validate_provider_base_url("openai", value).is_err());
        }
        assert!(validate_provider_base_url("bedrock", "https://example.test").is_err());
    }
}
