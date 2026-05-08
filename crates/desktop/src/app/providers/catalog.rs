#[derive(Clone, Copy)]
pub(super) struct ProviderCatalogEntry {
    pub(super) id: &'static str,
    pub(super) logo_path: &'static str,
    pub(super) aliases: &'static [&'static str],
}

impl ProviderCatalogEntry {
    const fn new(
        id: &'static str,
        logo_path: &'static str,
        aliases: &'static [&'static str],
    ) -> Self {
        Self {
            id,
            logo_path,
            aliases,
        }
    }

    pub(super) fn title(&self) -> String {
        t!(format!("providers.catalog.{}.title", self.id)).to_string()
    }

    pub(super) fn description(&self) -> String {
        t!(format!("providers.catalog.{}.description", self.id)).to_string()
    }
}

pub(super) const PROVIDER_CATALOG: &[ProviderCatalogEntry] = &[
    ProviderCatalogEntry::new("anthropic", "logos/providers/anthropic.svg", &[]),
    ProviderCatalogEntry::new("bedrock", "logos/providers/bedrock.svg", &["aws-bedrock"]),
    ProviderCatalogEntry::new(
        "azure-openai",
        "logos/providers/azure_openai.svg",
        &["azure_openai", "azure"],
    ),
    ProviderCatalogEntry::new(
        "copilot",
        "logos/providers/copilot.svg",
        &["github-copilot"],
    ),
    ProviderCatalogEntry::new("deepseek", "logos/providers/deepseek.svg", &[]),
    ProviderCatalogEntry::new(
        "gemini",
        "logos/providers/gemini.svg",
        &["google", "google-gemini"],
    ),
    ProviderCatalogEntry::new(
        "glm",
        "logos/providers/glm.svg",
        &[
            "zhipu",
            "bigmodel",
            "glm-global",
            "zhipu-global",
            "glm-cn",
            "zhipu-cn",
        ],
    ),
    ProviderCatalogEntry::new("groq", "logos/providers/groq.svg", &[]),
    ProviderCatalogEntry::new("litellm", "logos/providers/litellm.svg", &["lite-llm"]),
    ProviderCatalogEntry::new("mistral", "logos/providers/mistral.svg", &[]),
    ProviderCatalogEntry::new("ollama", "logos/providers/ollama.svg", &[]),
    ProviderCatalogEntry::new("openai", "logos/providers/openai.svg", &[]),
    ProviderCatalogEntry::new("openrouter", "logos/providers/openrouter.svg", &[]),
    ProviderCatalogEntry::new("telnyx", "logos/providers/telnyx.svg", &[]),
    ProviderCatalogEntry::new("xai", "logos/providers/xai.svg", &["grok"]),
    ProviderCatalogEntry::new("together", "logos/providers/together.svg", &["together-ai"]),
    ProviderCatalogEntry::new(
        "fireworks",
        "logos/providers/fireworks.svg",
        &["fireworks-ai"],
    ),
    ProviderCatalogEntry::new("novita", "logos/providers/novita.svg", &[]),
    ProviderCatalogEntry::new("perplexity", "logos/providers/perplexity.svg", &[]),
    ProviderCatalogEntry::new("cohere", "logos/providers/cohere.svg", &[]),
    ProviderCatalogEntry::new("venice", "logos/providers/venice.svg", &[]),
    ProviderCatalogEntry::new("cerebras", "logos/providers/cerebras.svg", &[]),
    ProviderCatalogEntry::new("sambanova", "logos/providers/sambanova.svg", &[]),
    ProviderCatalogEntry::new("hyperbolic", "logos/providers/hyperbolic.svg", &[]),
    ProviderCatalogEntry::new(
        "deepinfra",
        "logos/providers/deepinfra.svg",
        &["deep-infra"],
    ),
    ProviderCatalogEntry::new("huggingface", "logos/providers/huggingface.svg", &["hf"]),
    ProviderCatalogEntry::new("ai21", "logos/providers/ai21.svg", &["ai21-labs"]),
    ProviderCatalogEntry::new("reka", "logos/providers/reka.svg", &[]),
    ProviderCatalogEntry::new("baseten", "logos/providers/baseten.svg", &[]),
    ProviderCatalogEntry::new("nscale", "logos/providers/nscale.svg", &[]),
    ProviderCatalogEntry::new("anyscale", "logos/providers/anyscale.svg", &[]),
    ProviderCatalogEntry::new("nebius", "logos/providers/nebius.svg", &[]),
    ProviderCatalogEntry::new("friendli", "logos/providers/friendli.svg", &["friendliai"]),
    ProviderCatalogEntry::new("lepton", "logos/providers/lepton.svg", &["lepton-ai"]),
    ProviderCatalogEntry::new(
        "siliconflow",
        "logos/providers/siliconflow.svg",
        &["silicon-flow"],
    ),
    ProviderCatalogEntry::new("aihubmix", "logos/providers/aihubmix.svg", &[]),
    ProviderCatalogEntry::new("astrai", "logos/providers/astrai.svg", &[]),
    ProviderCatalogEntry::new("stepfun", "logos/providers/stepfun.svg", &["step"]),
    ProviderCatalogEntry::new("baichuan", "logos/providers/baichuan.svg", &[]),
    ProviderCatalogEntry::new("yi", "logos/providers/yi.svg", &["01ai", "lingyiwanwu"]),
    ProviderCatalogEntry::new("hunyuan", "logos/providers/hunyuan.svg", &["tencent"]),
    ProviderCatalogEntry::new("ovhcloud", "logos/providers/ovhcloud.svg", &["ovh"]),
    ProviderCatalogEntry::new("nvidia", "logos/providers/nvidia.svg", &["nvidia-nim"]),
    ProviderCatalogEntry::new("synthetic", "logos/providers/synthetic.svg", &[]),
    ProviderCatalogEntry::new(
        "doubao",
        "logos/providers/doubao.svg",
        &["volcengine", "ark"],
    ),
    ProviderCatalogEntry::new("qianfan", "logos/providers/qianfan.svg", &["baidu"]),
    ProviderCatalogEntry::new("lmstudio", "logos/providers/lmstudio.svg", &["lm-studio"]),
    ProviderCatalogEntry::new("llamacpp", "logos/providers/llamacpp.svg", &["llama.cpp"]),
    ProviderCatalogEntry::new("sglang", "logos/providers/sglang.svg", &[]),
    ProviderCatalogEntry::new("vllm", "logos/providers/vllm.svg", &[]),
    ProviderCatalogEntry::new("osaurus", "logos/providers/osaurus.svg", &[]),
];
