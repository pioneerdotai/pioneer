//! Provider catalog helpers.

use super::selectors::{ProviderAliasEntry, canonical_provider_id as canonical_provider_id_with};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderCatalogEntry {
    pub id: &'static str,
    pub aliases: &'static [&'static str],
}

impl ProviderCatalogEntry {
    pub const fn new(id: &'static str, aliases: &'static [&'static str]) -> Self {
        Self { id, aliases }
    }

    pub const fn alias_entry(&self) -> ProviderAliasEntry<'static> {
        ProviderAliasEntry {
            id: self.id,
            aliases: self.aliases,
        }
    }
}

pub const PROVIDER_CATALOG: &[ProviderCatalogEntry] = &[
    ProviderCatalogEntry::new("anthropic", &[]),
    ProviderCatalogEntry::new("bedrock", &["aws-bedrock"]),
    ProviderCatalogEntry::new("azure-openai", &["azure_openai", "azure"]),
    ProviderCatalogEntry::new("copilot", &["github-copilot"]),
    ProviderCatalogEntry::new("deepseek", &[]),
    ProviderCatalogEntry::new("gemini", &["google", "google-gemini"]),
    ProviderCatalogEntry::new(
        "glm",
        &[
            "zhipu",
            "bigmodel",
            "glm-global",
            "zhipu-global",
            "glm-cn",
            "zhipu-cn",
        ],
    ),
    ProviderCatalogEntry::new("groq", &[]),
    ProviderCatalogEntry::new("litellm", &["lite-llm"]),
    ProviderCatalogEntry::new("mistral", &[]),
    ProviderCatalogEntry::new("ollama", &[]),
    ProviderCatalogEntry::new("openai", &[]),
    ProviderCatalogEntry::new("openrouter", &[]),
    ProviderCatalogEntry::new("telnyx", &[]),
    ProviderCatalogEntry::new("xai", &["grok"]),
    ProviderCatalogEntry::new("together", &["together-ai"]),
    ProviderCatalogEntry::new("fireworks", &["fireworks-ai"]),
    ProviderCatalogEntry::new("novita", &[]),
    ProviderCatalogEntry::new("perplexity", &[]),
    ProviderCatalogEntry::new("cohere", &[]),
    ProviderCatalogEntry::new("venice", &[]),
    ProviderCatalogEntry::new("cerebras", &[]),
    ProviderCatalogEntry::new("sambanova", &[]),
    ProviderCatalogEntry::new("hyperbolic", &[]),
    ProviderCatalogEntry::new("deepinfra", &["deep-infra"]),
    ProviderCatalogEntry::new("huggingface", &["hf"]),
    ProviderCatalogEntry::new("ai21", &["ai21-labs"]),
    ProviderCatalogEntry::new("reka", &[]),
    ProviderCatalogEntry::new("baseten", &[]),
    ProviderCatalogEntry::new("nscale", &[]),
    ProviderCatalogEntry::new("anyscale", &[]),
    ProviderCatalogEntry::new("nebius", &[]),
    ProviderCatalogEntry::new("friendli", &["friendliai"]),
    ProviderCatalogEntry::new("lepton", &["lepton-ai"]),
    ProviderCatalogEntry::new("siliconflow", &["silicon-flow"]),
    ProviderCatalogEntry::new("aihubmix", &[]),
    ProviderCatalogEntry::new("astrai", &[]),
    ProviderCatalogEntry::new("stepfun", &["step"]),
    ProviderCatalogEntry::new("baichuan", &[]),
    ProviderCatalogEntry::new("yi", &["01ai", "lingyiwanwu"]),
    ProviderCatalogEntry::new("hunyuan", &["tencent"]),
    ProviderCatalogEntry::new("ovhcloud", &["ovh"]),
    ProviderCatalogEntry::new("nvidia", &["nvidia-nim"]),
    ProviderCatalogEntry::new("synthetic", &[]),
    ProviderCatalogEntry::new("doubao", &["volcengine", "ark"]),
    ProviderCatalogEntry::new("qianfan", &["baidu"]),
    ProviderCatalogEntry::new("lmstudio", &["lm-studio"]),
    ProviderCatalogEntry::new("llamacpp", &["llama.cpp"]),
    ProviderCatalogEntry::new("sglang", &[]),
    ProviderCatalogEntry::new("vllm", &[]),
    ProviderCatalogEntry::new("osaurus", &[]),
];

pub fn canonical_provider_id(raw: &str) -> String {
    canonical_provider_id_with(
        raw,
        PROVIDER_CATALOG.iter().map(|entry| entry.alias_entry()),
    )
}

pub fn provider_catalog_entry(provider_id: &str) -> Option<&'static ProviderCatalogEntry> {
    let canonical = canonical_provider_id(provider_id);
    PROVIDER_CATALOG
        .iter()
        .find(|provider| provider.id == canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_catalog_resolves_aliases_to_canonical_ids() {
        assert_eq!(canonical_provider_id("AWS_Bedrock"), "bedrock");
        assert_eq!(canonical_provider_id("azure openai"), "azure-openai");
        assert_eq!(canonical_provider_id("google-gemini"), "gemini");
        assert_eq!(canonical_provider_id("lm_studio"), "lmstudio");
        assert_eq!(canonical_provider_id("custom provider"), "customprovider");
    }

    #[test]
    fn provider_catalog_entry_uses_aliases() {
        let entry = provider_catalog_entry("deep-infra").expect("deepinfra entry");

        assert_eq!(entry.id, "deepinfra");
        assert!(entry.aliases.contains(&"deep-infra"));
    }
}
