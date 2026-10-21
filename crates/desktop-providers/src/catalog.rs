use pioneer_client::providers::catalog as client_provider_catalog;

#[derive(Clone, Copy)]
pub(super) struct ProviderCatalogEntry {
    pub(super) id: &'static str,
    pub(super) logo_path: &'static str,
}

impl ProviderCatalogEntry {
    const fn new(id: &'static str, logo_path: &'static str) -> Self {
        Self { id, logo_path }
    }

    pub(super) fn title(&self) -> String {
        t!(format!("providers.catalog.{}.title", self.id)).to_string()
    }

    pub(super) fn description(&self) -> String {
        t!(format!("providers.catalog.{}.description", self.id)).to_string()
    }
}

pub(super) fn provider_catalog_entries() -> impl Iterator<Item = ProviderCatalogEntry> {
    client_provider_catalog::PROVIDER_CATALOG
        .iter()
        .map(|provider| ProviderCatalogEntry::new(provider.id, provider_logo_path(provider.id)))
}

pub(super) fn provider_logo_path(provider_id: &str) -> &'static str {
    match provider_id {
        "anthropic" => "logos/providers/anthropic.svg",
        "bedrock" => "logos/providers/bedrock.svg",
        "azure-openai" => "logos/providers/azure_openai.svg",
        "copilot" => "logos/providers/copilot.svg",
        "deepseek" => "logos/providers/deepseek.svg",
        "gemini" => "logos/providers/gemini.svg",
        "glm" => "logos/providers/glm.svg",
        "groq" => "logos/providers/groq.svg",
        "litellm" => "logos/providers/litellm.svg",
        "mistral" => "logos/providers/mistral.svg",
        "ollama" => "logos/providers/ollama.svg",
        "openai" => "logos/providers/openai.svg",
        "openrouter" => "logos/providers/openrouter.svg",
        "telnyx" => "logos/providers/telnyx.svg",
        "xai" => "logos/providers/xai.svg",
        "together" => "logos/providers/together.svg",
        "fireworks" => "logos/providers/fireworks.svg",
        "novita" => "logos/providers/novita.svg",
        "perplexity" => "logos/providers/perplexity.svg",
        "cohere" => "logos/providers/cohere.svg",
        "venice" => "logos/providers/venice.svg",
        "cerebras" => "logos/providers/cerebras.svg",
        "sambanova" => "logos/providers/sambanova.svg",
        "hyperbolic" => "logos/providers/hyperbolic.svg",
        "deepinfra" => "logos/providers/deepinfra.svg",
        "huggingface" => "logos/providers/huggingface.svg",
        "ai21" => "logos/providers/ai21.svg",
        "reka" => "logos/providers/reka.svg",
        "baseten" => "logos/providers/baseten.svg",
        "nscale" => "logos/providers/nscale.svg",
        "anyscale" => "logos/providers/anyscale.svg",
        "nebius" => "logos/providers/nebius.svg",
        "friendli" => "logos/providers/friendli.svg",
        "lepton" => "logos/providers/lepton.svg",
        "siliconflow" => "logos/providers/siliconflow.svg",
        "aihubmix" => "logos/providers/aihubmix.svg",
        "astrai" => "logos/providers/astrai.svg",
        "stepfun" => "logos/providers/stepfun.svg",
        "baichuan" => "logos/providers/baichuan.svg",
        "yi" => "logos/providers/yi.svg",
        "hunyuan" => "logos/providers/hunyuan.svg",
        "ovhcloud" => "logos/providers/ovhcloud.svg",
        "nvidia" => "logos/providers/nvidia.svg",
        "synthetic" => "logos/providers/synthetic.svg",
        "doubao" => "logos/providers/doubao.svg",
        "qianfan" => "logos/providers/qianfan.svg",
        "lmstudio" => "logos/providers/lmstudio.svg",
        "llamacpp" => "logos/providers/llamacpp.svg",
        "sglang" => "logos/providers/sglang.svg",
        "vllm" => "logos/providers/vllm.svg",
        "osaurus" => "logos/providers/osaurus.svg",
        _ => "logos/providers/synthetic.svg",
    }
}
