//! Endpoint definitions available before a runtime provider is constructed.
//! Adapter constants are referenced here so the endpoint used for requests and
//! the endpoint advertised by the gateway have the same owner.
use crate::providers::{
    anthropic, copilot, deepseek, gemini, glm, ollama, openai, openrouter, telnyx,
};
use anyhow::{Result, bail};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderEndpointDefinition {
    pub name: &'static str,
    pub default_base_url: Option<&'static str>,
    pub supports_base_url_override: bool,
    /// A locally hosted compatible endpoint can be used without an API key.
    pub base_url_without_key: bool,
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
    ProviderEndpointDefinition::compatible("xai", "https://api.x.ai", false),
    ProviderEndpointDefinition::compatible("together", "https://api.together.xyz", false),
    ProviderEndpointDefinition::compatible(
        "fireworks",
        "https://api.fireworks.ai/inference/v1",
        false,
    ),
    ProviderEndpointDefinition::compatible("novita", "https://api.novita.ai/openai", false),
    ProviderEndpointDefinition::compatible("perplexity", "https://api.perplexity.ai", false),
    ProviderEndpointDefinition::compatible("cohere", "https://api.cohere.com/compatibility", false),
    ProviderEndpointDefinition::compatible("venice", "https://api.venice.ai", false),
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
    ProviderEndpointDefinition::compatible("qianfan", "https://aip.baidubce.com", false),
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
        "zhipu" | "bigmodel" | "glm-global" | "zhipu-global" | "glm-cn" | "zhipu-cn" => "glm",
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
    Ok(value.trim_end_matches('/').to_owned())
}

pub fn provider_is_available(
    api_key: bool,
    proxy: bool,
    base_url: bool,
    definition: ProviderEndpointDefinition,
) -> bool {
    definition.name == "local" || api_key || proxy || (base_url && definition.base_url_without_key)
}

#[cfg(test)]
mod tests {
    use super::*;

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
