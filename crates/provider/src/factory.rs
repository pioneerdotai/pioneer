use crate::providers::{
    AnthropicProvider, AuthStyle, AzureOpenAiProvider, BedrockProvider, CopilotProvider,
    DeepSeekProvider, GeminiProvider, GlmProvider, LocalProvider, OllamaProvider,
    OpenAiCompatibleProvider, OpenAiProvider, OpenRouterProvider, TelnyxProvider,
};
use crate::traits::Provider;
use crate::types::{InputTypeSupport, ProviderInputCapabilities, ProviderTimeoutPolicy};
use anyhow::{Result, bail};
use sha2::{Digest, Sha256};

pub fn create_provider(provider_name: &str, api_key: &str) -> Result<Box<dyn Provider>> {
    create_provider_with_timeout_policy(provider_name, api_key, ProviderTimeoutPolicy::default())
}

pub fn create_provider_with_timeout_policy(
    provider_name: &str,
    api_key: &str,
    timeout_policy: ProviderTimeoutPolicy,
) -> Result<Box<dyn Provider>> {
    create_provider_with_timeout_policy_and_proxy(provider_name, api_key, timeout_policy, None)
}

pub fn create_provider_with_timeout_policy_and_proxy(
    provider_name: &str,
    api_key: &str,
    timeout_policy: ProviderTimeoutPolicy,
    proxy_url: Option<&str>,
) -> Result<Box<dyn Provider>> {
    let mut digest = Sha256::new();
    digest.update(b"pioneer-provider-authority-v1");
    digest.update([0]);
    digest.update(b"<direct-factory>");
    digest.update([0]);
    digest.update(provider_name.trim().to_ascii_lowercase().as_bytes());
    digest.update([0]);
    digest.update(api_key.as_bytes());
    digest.update([0]);
    digest.update(proxy_url.unwrap_or("<direct>").as_bytes());
    let authority_fingerprint = hex::encode(digest.finalize());
    create_provider_with_timeout_policy_and_proxy_and_authority(
        provider_name,
        api_key,
        timeout_policy,
        proxy_url,
        authority_fingerprint.as_str(),
    )
}

pub(crate) fn create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority(
    provider_name: &str,
    api_key: &str,
    timeout_policy: ProviderTimeoutPolicy,
    proxy_url: Option<&str>,
    base_url: Option<&str>,
    authority_fingerprint: &str,
) -> Result<Box<dyn Provider>> {
    let proxy_url = proxy_url.map(crate::http::validate_proxy_url).transpose()?;
    crate::http::with_provider_proxy(proxy_url.as_deref(), || {
        create_provider_with_timeout_policy_inner(
            provider_name,
            api_key,
            timeout_policy,
            base_url,
            authority_fingerprint,
        )
    })
}

pub(crate) fn create_provider_with_timeout_policy_and_proxy_and_authority(
    provider_name: &str,
    api_key: &str,
    timeout_policy: ProviderTimeoutPolicy,
    proxy_url: Option<&str>,
    authority_fingerprint: &str,
) -> Result<Box<dyn Provider>> {
    create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority(
        provider_name,
        api_key,
        timeout_policy,
        proxy_url,
        None,
        authority_fingerprint,
    )
}

/// Returns the authoritative built-in default base URL for the given provider,
/// or `None` if the provider has no static URL endpoint.
pub fn default_provider_base_url(provider_name: &str) -> Option<&'static str> {
    pioneer_protocol::default_provider_base_url(provider_name)
}

fn create_provider_with_timeout_policy_inner(
    provider_name: &str,
    api_key: &str,
    timeout_policy: ProviderTimeoutPolicy,
    base_url: Option<&str>,
    authority_fingerprint: &str,
) -> Result<Box<dyn Provider>> {
    let compat = |name: &str, api_key: &str| {
        let default_url = default_provider_base_url(name).unwrap_or("http://localhost:8000/v1");
        let effective_base_url = base_url.unwrap_or(default_url);
        compat_provider(name, effective_base_url, api_key).with_timeout_policy(timeout_policy)
    };

    match provider_name {
        // ── Primary providers with custom implementations ────────────────
        "openrouter" => {
            if let Some(base_url) = base_url {
                Ok(Box::new(
                    OpenRouterProvider::with_base_url_and_timeout_policy(
                        api_key,
                        base_url,
                        timeout_policy,
                    ),
                ))
            } else {
                Ok(Box::new(OpenRouterProvider::with_timeout_policy(
                    api_key,
                    timeout_policy,
                )))
            }
        }
        "anthropic" => {
            if let Some(base_url) = base_url {
                Ok(Box::new(
                    AnthropicProvider::with_base_url_and_timeout_policy(
                        api_key,
                        base_url,
                        timeout_policy,
                    ),
                ))
            } else {
                Ok(Box::new(AnthropicProvider::with_timeout_policy(
                    api_key,
                    timeout_policy,
                )))
            }
        }
        "openai" => {
            if let Some(base_url) = base_url {
                Ok(Box::new(
                    OpenAiProvider::with_base_url_timeout_policy_and_authority(
                        api_key,
                        base_url,
                        timeout_policy,
                        authority_fingerprint,
                    ),
                ))
            } else {
                Ok(Box::new(OpenAiProvider::with_timeout_policy_and_authority(
                    api_key,
                    timeout_policy,
                    authority_fingerprint,
                )))
            }
        }
        "local" => Ok(Box::new(LocalProvider::new())),
        "gemini" | "google" | "google-gemini" => Ok(Box::new(GeminiProvider::with_timeout_policy(
            api_key,
            timeout_policy,
        ))),
        "ollama" => Ok(Box::new(OllamaProvider::with_timeout_policy(
            timeout_policy,
        ))),
        "telnyx" => Ok(Box::new(TelnyxProvider::with_timeout_policy(
            api_key,
            timeout_policy,
        ))),
        "copilot" | "github-copilot" => Ok(Box::new(CopilotProvider::with_timeout_policy(
            api_key,
            timeout_policy,
        ))),
        "bedrock" | "aws-bedrock" => Ok(Box::new(
            BedrockProvider::from_env_with_timeout_policy(timeout_policy).unwrap_or_else(|_| {
                BedrockProvider::with_timeout_policy(api_key, "", "us-east-1", timeout_policy)
            }),
        )),

        // ── GLM / Zhipu ─────────────────────────────────────────────────
        "glm" | "zhipu" | "bigmodel" | "glm-global" | "zhipu-global" | "glm-cn" | "zhipu-cn" => Ok(
            Box::new(GlmProvider::with_timeout_policy(api_key, timeout_policy)),
        ),

        // ── Azure OpenAI ────────────────────────────────────────────────
        "azure_openai" | "azure-openai" | "azure" => {
            // Azure requires resource_name and deployment_name; for the
            // simple factory we pass api_key as the key and expect env
            // configuration for the rest.
            let resource = std::env::var("AZURE_OPENAI_RESOURCE").unwrap_or_default();
            let deployment = std::env::var("AZURE_OPENAI_DEPLOYMENT").unwrap_or_default();
            Ok(Box::new(AzureOpenAiProvider::with_timeout_policy(
                api_key,
                resource,
                deployment,
                timeout_policy,
            )))
        }

        // ── OpenAI-compatible providers ─────────────────────────────────
        "groq" => Ok(Box::new(compat("groq", api_key))),
        "mistral" => Ok(Box::new(compat("mistral", api_key))),
        "xai" | "grok" => Ok(Box::new(compat("xai", api_key))),
        "deepseek" => Ok(Box::new(
            DeepSeekProvider::with_timeout_policy(api_key, timeout_policy)
                .with_input_capabilities(compat_input_capabilities()),
        )),
        "together" | "together-ai" => Ok(Box::new(compat("together", api_key))),
        "fireworks" | "fireworks-ai" => Ok(Box::new(compat("fireworks", api_key))),
        "novita" => Ok(Box::new(compat("novita", api_key))),
        "perplexity" => Ok(Box::new(compat("perplexity", api_key))),
        "cohere" => Ok(Box::new(compat("cohere", api_key))),
        "venice" => Ok(Box::new(compat("venice", api_key))),
        "cerebras" => Ok(Box::new(compat("cerebras", api_key))),
        "sambanova" => Ok(Box::new(compat("sambanova", api_key))),
        "hyperbolic" => Ok(Box::new(compat("hyperbolic", api_key))),
        "deepinfra" | "deep-infra" => Ok(Box::new(compat("deepinfra", api_key))),
        "huggingface" | "hf" => Ok(Box::new(compat("huggingface", api_key))),
        "ai21" | "ai21-labs" => Ok(Box::new(compat("ai21", api_key))),
        "reka" => Ok(Box::new(compat("reka", api_key))),
        "baseten" => Ok(Box::new(compat("baseten", api_key))),
        "nscale" => Ok(Box::new(compat("nscale", api_key))),
        "anyscale" => Ok(Box::new(compat("anyscale", api_key))),
        "nebius" => Ok(Box::new(compat("nebius", api_key))),
        "friendli" | "friendliai" => Ok(Box::new(compat("friendli", api_key))),
        "lepton" | "lepton-ai" => Ok(Box::new(compat("lepton", api_key))),
        "siliconflow" | "silicon-flow" => Ok(Box::new(compat("siliconflow", api_key))),
        "aihubmix" => Ok(Box::new(compat("aihubmix", api_key))),
        "astrai" => Ok(Box::new(compat("astrai", api_key))),
        "stepfun" | "step" => Ok(Box::new(compat("stepfun", api_key))),
        "baichuan" => Ok(Box::new(compat("baichuan", api_key))),
        "yi" | "01ai" | "lingyiwanwu" => Ok(Box::new(compat("yi", api_key))),
        "hunyuan" | "tencent" => Ok(Box::new(compat("hunyuan", api_key))),
        "ovhcloud" | "ovh" => Ok(Box::new(compat("ovhcloud", api_key))),
        "nvidia" | "nvidia-nim" => Ok(Box::new(compat("nvidia", api_key))),
        "synthetic" => Ok(Box::new(compat("synthetic", api_key))),
        "doubao" | "volcengine" | "ark" => Ok(Box::new(compat("doubao", api_key))),
        "qianfan" | "baidu" => Ok(Box::new(compat("qianfan", api_key))),

        // ── Local inference servers ─────────────────────────────────────
        "lmstudio" | "lm-studio" => Ok(Box::new(compat("lmstudio", api_key))),
        "llamacpp" | "llama.cpp" => Ok(Box::new(compat("llamacpp", api_key))),
        "sglang" => Ok(Box::new(compat("sglang", api_key))),
        "vllm" => Ok(Box::new(compat("vllm", api_key))),
        "osaurus" => Ok(Box::new(compat("osaurus", api_key))),
        "litellm" | "lite-llm" => Ok(Box::new(compat("litellm", api_key))),
        "custom" | "compatible" | "openai-compatible" => {
            Ok(Box::new(compat(provider_name, api_key)))
        }

        _ => bail!("unknown provider: {provider_name}"),
    }
}

/// Shorthand to create an OpenAI-compatible provider with Bearer auth.
fn compat_provider(name: &str, base_url: &str, api_key: &str) -> OpenAiCompatibleProvider {
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
    fn creates_local_provider() {
        let provider = create_provider("local", "").unwrap();
        assert_eq!(provider.name(), "local");
        assert!(provider.capabilities().embeddings);
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
    fn rejects_unused_cli_providers() {
        for name in ["claude-code", "gemini-cli", "kilocli", "kilo"] {
            let err = match create_provider(name, "") {
                Err(error) => error,
                Ok(_) => panic!("CLI providers are not factory-backed"),
            };
            assert!(
                err.to_string().contains("unknown provider"),
                "unexpected error for {name}: {err}"
            );
        }
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
                    artifact: None,
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
                    artifact: None,
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
                    artifact: None,
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
                    artifact: None,
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

    #[test]
    fn factory_supports_custom_base_url_for_openai_and_compatible() {
        let custom_openai =
            create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority(
                "openai",
                "sk-custom",
                ProviderTimeoutPolicy::default(),
                None,
                Some("https://api.example.com/v1/"),
                "fp-test-1",
            )
            .expect("create custom openai provider");
        assert_eq!(custom_openai.name(), "openai");

        let custom_compat =
            create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority(
                "custom",
                "sk-custom",
                ProviderTimeoutPolicy::default(),
                None,
                Some("https://custom.endpoint.com/v1"),
                "fp-test-2",
            )
            .expect("create custom-compatible provider");
        assert_eq!(custom_compat.name(), "custom");
    }
}
