use crate::providers::{
    AnthropicProvider, AuthStyle, AzureOpenAiProvider, BedrockProvider, ClaudeCodeProvider,
    CopilotProvider, GeminiCliProvider, GeminiProvider, GlmProvider, KiloCliProvider,
    OllamaProvider, OpenAiCompatibleProvider, OpenAiProvider, OpenRouterProvider, TelnyxProvider,
};
use crate::traits::Provider;
use crate::types::{InputTypeSupport, ProviderInputCapabilities};
use anyhow::{Result, bail};

pub fn create_provider(provider_name: &str, api_key: &str) -> Result<Box<dyn Provider>> {
    match provider_name {
        // ── Primary providers with custom implementations ────────────────
        "openrouter" => Ok(Box::new(OpenRouterProvider::new(api_key))),
        "anthropic" => Ok(Box::new(AnthropicProvider::new(api_key))),
        "openai" => Ok(Box::new(OpenAiProvider::new(api_key))),
        "gemini" | "google" | "google-gemini" => Ok(Box::new(GeminiProvider::new(api_key))),
        "ollama" => Ok(Box::new(OllamaProvider::new())),
        "telnyx" => Ok(Box::new(TelnyxProvider::new(api_key))),
        "copilot" | "github-copilot" => Ok(Box::new(CopilotProvider::new(api_key))),
        "bedrock" | "aws-bedrock" => {
            Ok(Box::new(BedrockProvider::from_env().unwrap_or_else(|_| {
                BedrockProvider::new(api_key, "", "us-east-1")
            })))
        }

        // ── GLM / Zhipu ─────────────────────────────────────────────────
        "glm" | "zhipu" | "bigmodel" | "glm-global" | "zhipu-global" | "glm-cn" | "zhipu-cn" => {
            Ok(Box::new(GlmProvider::new(api_key)))
        }

        // ── Azure OpenAI ────────────────────────────────────────────────
        "azure_openai" | "azure-openai" | "azure" => {
            // Azure requires resource_name and deployment_name; for the
            // simple factory we pass api_key as the key and expect env
            // configuration for the rest.
            let resource = std::env::var("AZURE_OPENAI_RESOURCE").unwrap_or_default();
            let deployment = std::env::var("AZURE_OPENAI_DEPLOYMENT").unwrap_or_default();
            Ok(Box::new(AzureOpenAiProvider::new(
                api_key, resource, deployment,
            )))
        }

        // ── CLI providers ───────────────────────────────────────────────
        "claude-code" => Ok(Box::new(ClaudeCodeProvider::new())),
        "gemini-cli" => Ok(Box::new(GeminiCliProvider::new())),
        "kilocli" | "kilo" => Ok(Box::new(KiloCliProvider::new())),

        // ── OpenAI-compatible providers ─────────────────────────────────
        "groq" => Ok(Box::new(compat(
            "groq",
            "https://api.groq.com/openai/v1",
            api_key,
        ))),
        "mistral" => Ok(Box::new(compat(
            "mistral",
            "https://api.mistral.ai/v1",
            api_key,
        ))),
        "xai" | "grok" => Ok(Box::new(compat("xai", "https://api.x.ai", api_key))),
        "deepseek" => Ok(Box::new(compat(
            "deepseek",
            "https://api.deepseek.com",
            api_key,
        ))),
        "together" | "together-ai" => Ok(Box::new(compat(
            "together",
            "https://api.together.xyz",
            api_key,
        ))),
        "fireworks" | "fireworks-ai" => Ok(Box::new(compat(
            "fireworks",
            "https://api.fireworks.ai/inference/v1",
            api_key,
        ))),
        "novita" => Ok(Box::new(compat(
            "novita",
            "https://api.novita.ai/openai",
            api_key,
        ))),
        "perplexity" => Ok(Box::new(compat(
            "perplexity",
            "https://api.perplexity.ai",
            api_key,
        ))),
        "cohere" => Ok(Box::new(compat(
            "cohere",
            "https://api.cohere.com/compatibility",
            api_key,
        ))),
        "venice" => Ok(Box::new(compat("venice", "https://api.venice.ai", api_key))),
        "cerebras" => Ok(Box::new(compat(
            "cerebras",
            "https://api.cerebras.ai/v1",
            api_key,
        ))),
        "sambanova" => Ok(Box::new(compat(
            "sambanova",
            "https://api.sambanova.ai/v1",
            api_key,
        ))),
        "hyperbolic" => Ok(Box::new(compat(
            "hyperbolic",
            "https://api.hyperbolic.xyz/v1",
            api_key,
        ))),
        "deepinfra" | "deep-infra" => Ok(Box::new(compat(
            "deepinfra",
            "https://api.deepinfra.com/v1/openai",
            api_key,
        ))),
        "huggingface" | "hf" => Ok(Box::new(compat(
            "huggingface",
            "https://router.huggingface.co/v1",
            api_key,
        ))),
        "ai21" | "ai21-labs" => Ok(Box::new(compat(
            "ai21",
            "https://api.ai21.com/studio/v1",
            api_key,
        ))),
        "reka" => Ok(Box::new(compat("reka", "https://api.reka.ai/v1", api_key))),
        "baseten" => Ok(Box::new(compat(
            "baseten",
            "https://inference.baseten.co/v1",
            api_key,
        ))),
        "nscale" => Ok(Box::new(compat(
            "nscale",
            "https://inference.api.nscale.com/v1",
            api_key,
        ))),
        "anyscale" => Ok(Box::new(compat(
            "anyscale",
            "https://api.endpoints.anyscale.com/v1",
            api_key,
        ))),
        "nebius" => Ok(Box::new(compat(
            "nebius",
            "https://api.studio.nebius.ai/v1",
            api_key,
        ))),
        "friendli" | "friendliai" => Ok(Box::new(compat(
            "friendli",
            "https://api.friendli.ai/serverless/v1",
            api_key,
        ))),
        "lepton" | "lepton-ai" => Ok(Box::new(compat(
            "lepton",
            "https://llama3-1-405b.lepton.run/api/v1",
            api_key,
        ))),
        "siliconflow" | "silicon-flow" => Ok(Box::new(compat(
            "siliconflow",
            "https://api.siliconflow.cn/v1",
            api_key,
        ))),
        "aihubmix" => Ok(Box::new(compat(
            "aihubmix",
            "https://aihubmix.com/v1",
            api_key,
        ))),
        "astrai" => Ok(Box::new(compat(
            "astrai",
            "https://as-trai.com/v1",
            api_key,
        ))),
        "stepfun" | "step" => Ok(Box::new(compat(
            "stepfun",
            "https://api.stepfun.com/v1",
            api_key,
        ))),
        "baichuan" => Ok(Box::new(compat(
            "baichuan",
            "https://api.baichuan-ai.com/v1",
            api_key,
        ))),
        "yi" | "01ai" | "lingyiwanwu" => Ok(Box::new(compat(
            "yi",
            "https://api.lingyiwanwu.com/v1",
            api_key,
        ))),
        "hunyuan" | "tencent" => Ok(Box::new(compat(
            "hunyuan",
            "https://api.hunyuan.cloud.tencent.com/v1",
            api_key,
        ))),
        "ovhcloud" | "ovh" => Ok(Box::new(compat(
            "ovhcloud",
            "https://api.ai.cloud.ovh.net/v1",
            api_key,
        ))),
        "nvidia" | "nvidia-nim" => Ok(Box::new(compat(
            "nvidia",
            "https://integrate.api.nvidia.com/v1",
            api_key,
        ))),
        "synthetic" => Ok(Box::new(compat(
            "synthetic",
            "https://api.synthetic.new/openai/v1",
            api_key,
        ))),
        "doubao" | "volcengine" | "ark" => Ok(Box::new(compat(
            "doubao",
            "https://ark.cn-beijing.volces.com/api/v3",
            api_key,
        ))),
        "qianfan" | "baidu" => Ok(Box::new(compat(
            "qianfan",
            "https://aip.baidubce.com",
            api_key,
        ))),

        // ── Local inference servers ─────────────────────────────────────
        "lmstudio" | "lm-studio" => Ok(Box::new(compat(
            "lmstudio",
            "http://localhost:1234/v1",
            api_key,
        ))),
        "llamacpp" | "llama.cpp" => Ok(Box::new(compat(
            "llamacpp",
            "http://localhost:8080/v1",
            api_key,
        ))),
        "sglang" => Ok(Box::new(compat(
            "sglang",
            "http://localhost:30000/v1",
            api_key,
        ))),
        "vllm" => Ok(Box::new(compat(
            "vllm",
            "http://localhost:8000/v1",
            api_key,
        ))),
        "osaurus" => Ok(Box::new(compat(
            "osaurus",
            "http://localhost:1337/v1",
            api_key,
        ))),
        "litellm" | "lite-llm" => Ok(Box::new(compat(
            "litellm",
            "http://localhost:4000/v1",
            api_key,
        ))),

        _ => bail!("unknown provider: {provider_name}"),
    }
}

/// Shorthand to create an OpenAI-compatible provider with Bearer auth.
fn compat(name: &str, base_url: &str, api_key: &str) -> OpenAiCompatibleProvider {
    OpenAiCompatibleProvider::new(name, base_url, api_key, AuthStyle::Bearer)
        .with_input_capabilities(compat_input_capabilities())
}

fn compat_input_capabilities() -> ProviderInputCapabilities {
    // Compatibility-first contract: all OpenAI-compatible adapters expose the
    // same multimodal surface as our OpenAI-compatible renderer.
    ProviderInputCapabilities {
        text: true,
        file: InputTypeSupport {
            native: true,
            file_upload: false,
            data_url_inline: true,
            text_fallback: false,
        },
        image: InputTypeSupport {
            native: true,
            file_upload: false,
            data_url_inline: true,
            text_fallback: false,
        },
        audio: InputTypeSupport::native_inline_only(),
        video: InputTypeSupport {
            native: true,
            file_upload: false,
            data_url_inline: true,
            text_fallback: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachments::prepare_messages_for_provider;
    use crate::types::{AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart};
    use base64::Engine;

    #[test]
    fn creates_openrouter_provider() {
        let provider = create_provider("openrouter", "sk-or-test").unwrap();
        assert_eq!(provider.name(), "openrouter");
    }

    #[test]
    fn creates_anthropic_provider() {
        let provider = create_provider("anthropic", "sk-ant-test").unwrap();
        assert_eq!(provider.name(), "anthropic");
    }

    #[test]
    fn creates_openai_provider() {
        let provider = create_provider("openai", "sk-test").unwrap();
        assert_eq!(provider.name(), "openai");
    }

    #[test]
    fn creates_gemini_aliases() {
        for name in &["gemini", "google", "google-gemini"] {
            let provider = create_provider(name, "key").unwrap();
            assert_eq!(provider.name(), "gemini");
        }
    }

    #[test]
    fn creates_groq_provider() {
        let provider = create_provider("groq", "gsk-test").unwrap();
        assert_eq!(provider.name(), "groq");
    }

    #[test]
    fn creates_xai_aliases() {
        for name in &["xai", "grok"] {
            let provider = create_provider(name, "key").unwrap();
            assert_eq!(provider.name(), "xai");
        }
    }

    #[test]
    fn creates_ollama_provider() {
        let provider = create_provider("ollama", "").unwrap();
        assert_eq!(provider.name(), "ollama");
    }

    #[test]
    fn creates_deepseek_provider() {
        let provider = create_provider("deepseek", "key").unwrap();
        assert_eq!(provider.name(), "deepseek");
    }

    #[test]
    fn creates_together_aliases() {
        for name in &["together", "together-ai"] {
            let provider = create_provider(name, "key").unwrap();
            assert_eq!(provider.name(), "together");
        }
    }

    #[test]
    fn creates_cli_providers() {
        let provider = create_provider("claude-code", "").unwrap();
        assert_eq!(provider.name(), "claude_code");

        let provider = create_provider("gemini-cli", "").unwrap();
        assert_eq!(provider.name(), "gemini_cli");

        let provider = create_provider("kilo", "").unwrap();
        assert_eq!(provider.name(), "kilo_cli");
    }

    #[test]
    fn rejects_unknown_provider() {
        let result = create_provider("nonexistent", "key");
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("should reject unknown provider"),
        };
        assert!(err.to_string().contains("unknown provider"));
    }

    #[test]
    fn phase_b_registry_capabilities_and_attachment_contracts() {
        let providers = [
            "openrouter",
            "anthropic",
            "openai",
            "gemini",
            "google",
            "google-gemini",
            "ollama",
            "telnyx",
            "copilot",
            "github-copilot",
            "bedrock",
            "aws-bedrock",
            "glm",
            "zhipu",
            "bigmodel",
            "glm-global",
            "zhipu-global",
            "glm-cn",
            "zhipu-cn",
            "azure_openai",
            "azure-openai",
            "azure",
            "claude-code",
            "gemini-cli",
            "kilocli",
            "kilo",
            "groq",
            "mistral",
            "xai",
            "grok",
            "deepseek",
            "together",
            "together-ai",
            "fireworks",
            "fireworks-ai",
            "novita",
            "perplexity",
            "cohere",
            "venice",
            "cerebras",
            "sambanova",
            "hyperbolic",
            "deepinfra",
            "deep-infra",
            "huggingface",
            "hf",
            "ai21",
            "ai21-labs",
            "reka",
            "baseten",
            "nscale",
            "anyscale",
            "nebius",
            "friendli",
            "friendliai",
            "lepton",
            "lepton-ai",
            "siliconflow",
            "silicon-flow",
            "aihubmix",
            "astrai",
            "stepfun",
            "step",
            "baichuan",
            "yi",
            "01ai",
            "lingyiwanwu",
            "hunyuan",
            "tencent",
            "ovhcloud",
            "ovh",
            "nvidia",
            "nvidia-nim",
            "synthetic",
            "doubao",
            "volcengine",
            "ark",
            "qianfan",
            "baidu",
            "lmstudio",
            "lm-studio",
            "llamacpp",
            "llama.cpp",
            "sglang",
            "vllm",
            "osaurus",
            "litellm",
            "lite-llm",
        ];

        let build_part = |part_type: &str| -> MessageContentPart {
            match part_type {
                "file" => MessageContentPart::file(MessageAttachment {
                    mime_type: "application/pdf".to_owned(),
                    name: Some("doc.pdf".to_owned()),
                    size_bytes: Some(4),
                    sha256: None,
                    source: AttachmentDataSource::Bytes {
                        base64_data: base64::engine::general_purpose::STANDARD
                            .encode([1u8, 2, 3, 4]),
                    },
                }),
                "image" => MessageContentPart::image(MessageAttachment {
                    mime_type: "image/png".to_owned(),
                    name: Some("img.png".to_owned()),
                    size_bytes: Some(4),
                    sha256: None,
                    source: AttachmentDataSource::Bytes {
                        base64_data: base64::engine::general_purpose::STANDARD
                            .encode([1u8, 2, 3, 4]),
                    },
                }),
                "audio" => MessageContentPart::audio(MessageAttachment {
                    mime_type: "audio/wav".to_owned(),
                    name: Some("a.wav".to_owned()),
                    size_bytes: Some(4),
                    sha256: None,
                    source: AttachmentDataSource::Bytes {
                        base64_data: base64::engine::general_purpose::STANDARD
                            .encode([1u8, 2, 3, 4]),
                    },
                }),
                "video" => MessageContentPart::video(MessageAttachment {
                    mime_type: "video/mp4".to_owned(),
                    name: Some("v.mp4".to_owned()),
                    size_bytes: Some(4),
                    sha256: None,
                    source: AttachmentDataSource::Bytes {
                        base64_data: base64::engine::general_purpose::STANDARD
                            .encode([1u8, 2, 3, 4]),
                    },
                }),
                other => panic!("unknown part type: {other}"),
            }
        };

        for name in providers {
            let provider = create_provider(name, "test-key").expect("provider should initialize");
            let caps = provider.capabilities();

            for (part_type, support) in [
                ("file", caps.input_types.file),
                ("image", caps.input_types.image),
                ("audio", caps.input_types.audio),
                ("video", caps.input_types.video),
            ] {
                assert!(
                    !support.text_fallback,
                    "phase B: provider `{name}` must not use legacy text_fallback for {part_type}"
                );

                let part = build_part(part_type);
                let result = prepare_messages_for_provider(
                    name,
                    &caps,
                    &[ChatMessage::user_parts(vec![part])],
                );

                if support.is_supported() {
                    assert!(
                        result.is_ok(),
                        "phase B: provider `{name}` advertises {part_type} support but pipeline failed: {:?}",
                        result.err()
                    );
                } else {
                    let err =
                        result.expect_err("phase B: unsupported kind must fail with a typed error");
                    assert!(
                        err.to_string()
                            .contains("ATTACHMENT_PIPELINE_CONTRACT_VIOLATION"),
                        "phase B: provider `{name}` must fail explicitly for unsupported {part_type}"
                    );
                }
            }
        }
    }

    #[test]
    fn compat_profiles_match_openai_compatible_contract() {
        let openai_compatible_aliases = [
            "groq",
            "mistral",
            "xai",
            "grok",
            "deepseek",
            "together",
            "together-ai",
            "fireworks",
            "fireworks-ai",
            "novita",
            "perplexity",
            "cohere",
            "venice",
            "cerebras",
            "sambanova",
            "hyperbolic",
            "deepinfra",
            "deep-infra",
            "huggingface",
            "hf",
            "ai21",
            "ai21-labs",
            "reka",
            "baseten",
            "nscale",
            "anyscale",
            "nebius",
            "friendli",
            "friendliai",
            "lepton",
            "lepton-ai",
            "siliconflow",
            "silicon-flow",
            "aihubmix",
            "astrai",
            "stepfun",
            "step",
            "baichuan",
            "yi",
            "01ai",
            "lingyiwanwu",
            "hunyuan",
            "tencent",
            "ovhcloud",
            "ovh",
            "nvidia",
            "nvidia-nim",
            "synthetic",
            "doubao",
            "volcengine",
            "ark",
            "qianfan",
            "baidu",
            "lmstudio",
            "lm-studio",
            "llamacpp",
            "llama.cpp",
            "sglang",
            "vllm",
            "osaurus",
            "litellm",
            "lite-llm",
        ];
        let expected = compat_input_capabilities();

        for alias in openai_compatible_aliases {
            let provider = create_provider(alias, "test-key")
                .unwrap_or_else(|err| panic!("failed to construct provider `{alias}`: {err}"));
            let caps = provider.capabilities().input_types;
            assert_eq!(
                caps, expected,
                "openai-compatible provider `{alias}` must expose full OpenAI-compatible input contract"
            );
        }
    }
}
