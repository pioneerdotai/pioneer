use crate::definition::{provider_definition, validate_provider_base_url};
use crate::providers::{
    AnthropicProvider, AuthStyle, AzureOpenAiProvider, BedrockProvider, CopilotProvider,
    DeepSeekProvider, GeminiProvider, GlmProvider, LocalProvider, OllamaProvider,
    OpenAiCompatibleProvider, OpenAiProvider, OpenRouterProvider, TelnyxProvider,
};
use crate::traits::Provider;
use crate::types::{InputTypeSupport, ProviderInputCapabilities, ProviderTimeoutPolicy};
use anyhow::Result;
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
    let base_url = base_url
        .map(|url| validate_provider_base_url(provider_name, url))
        .transpose()?;
    crate::http::with_provider_proxy(proxy_url.as_deref(), || {
        create_provider_with_timeout_policy_inner(
            provider_name,
            api_key,
            timeout_policy,
            base_url.as_deref(),
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

fn create_provider_with_timeout_policy_inner(
    provider_name: &str,
    api_key: &str,
    timeout_policy: ProviderTimeoutPolicy,
    base_url: Option<&str>,
    authority_fingerprint: &str,
) -> Result<Box<dyn Provider>> {
    let definition =
        provider_definition(provider_name).ok_or_else(|| anyhow::anyhow!("unknown provider"))?;
    let endpoint = base_url.or(definition.default_base_url);
    let compat = |api_key: &str| {
        let effective_base_url = endpoint.expect("compatible provider endpoint definition");
        compat_provider(definition.name, effective_base_url, api_key)
            .with_timeout_policy(timeout_policy)
    };

    match definition.name {
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
        "gemini" => Ok(Box::new(GeminiProvider::with_base_url_and_timeout_policy(
            api_key,
            endpoint.expect("Gemini endpoint definition"),
            timeout_policy,
        ))),
        "ollama" => Ok(Box::new(OllamaProvider::with_base_url_and_timeout_policy(
            endpoint.expect("Ollama endpoint definition"),
            timeout_policy,
        ))),
        "telnyx" => Ok(Box::new(TelnyxProvider::with_base_url_and_timeout_policy(
            api_key,
            endpoint.expect("Telnyx endpoint definition"),
            timeout_policy,
        ))),
        "copilot" => Ok(Box::new(CopilotProvider::with_base_url_and_timeout_policy(
            api_key,
            endpoint.expect("Copilot endpoint definition"),
            timeout_policy,
        ))),
        "bedrock" => Ok(Box::new(
            BedrockProvider::from_env_with_timeout_policy(timeout_policy).unwrap_or_else(|_| {
                BedrockProvider::with_timeout_policy(api_key, "", "us-east-1", timeout_policy)
            }),
        )),

        // ── GLM / Zhipu ─────────────────────────────────────────────────
        "glm" => Ok(Box::new(GlmProvider::with_base_url_and_timeout_policy(
            api_key,
            endpoint.expect("GLM endpoint definition"),
            timeout_policy,
        ))),

        // ── Azure OpenAI ────────────────────────────────────────────────
        "azure-openai" => {
            // Azure requires resource_name and deployment_name; for the
            // simple factory we pass api_key as the key and expect env
            // configuration for the rest.
            let resource = std::env::var("AZURE_OPENAI_RESOURCE").unwrap_or_default();
            let deployment = std::env::var("AZURE_OPENAI_DEPLOYMENT").unwrap_or_default();
            if let Some(base_url) = base_url {
                Ok(Box::new(
                    AzureOpenAiProvider::with_base_url_and_timeout_policy(
                        api_key,
                        resource,
                        deployment,
                        base_url,
                        timeout_policy,
                    ),
                ))
            } else {
                Ok(Box::new(AzureOpenAiProvider::with_timeout_policy(
                    api_key,
                    resource,
                    deployment,
                    timeout_policy,
                )))
            }
        }

        "deepseek" => Ok(Box::new(
            DeepSeekProvider::with_base_url_and_timeout_policy(
                api_key,
                endpoint.expect("DeepSeek endpoint definition"),
                timeout_policy,
            )
            .with_input_capabilities(compat_input_capabilities()),
        )),
        _ => Ok(Box::new(compat(api_key))),
    }
}

/// Shorthand to create an OpenAI-compatible provider with Bearer auth.
fn compat_provider(name: &str, base_url: &str, api_key: &str) -> OpenAiCompatibleProvider {
    OpenAiCompatibleProvider::new(name, base_url, api_key, AuthStyle::Bearer)
        .with_input_capabilities(compat_input_capabilities())
}

fn compat_input_capabilities() -> ProviderInputCapabilities {
    // Adapter ceiling, not a promise about any selected model. Chat's
    // image_url renderer is shared; file/audio/video are endpoint-specific.
    ProviderInputCapabilities {
        text: true,
        image: InputTypeSupport::data_url_inline_only(),
        ..ProviderInputCapabilities::disabled_for_all_file_types()
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
                    size_bytes: None,
                    sha256: None,
                    source: AttachmentDataSource::Bytes {
                        base64_data: base64::engine::general_purpose::STANDARD
                            .encode(crate::attachments::regression::pdf(1)),
                    },
                    artifact: None,
                }),
                "image" => MessageContentPart::image(MessageAttachment {
                    mime_type: "image/png".to_owned(),
                    name: Some("img.png".to_owned()),
                    size_bytes: None,
                    sha256: None,
                    source: AttachmentDataSource::Bytes {
                        base64_data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a8Z0AAAAASUVORK5CYII=".to_owned(),
                    },
                    artifact: None,
                }),
                "audio" => MessageContentPart::audio(MessageAttachment {
                    mime_type: "audio/wav".to_owned(),
                    name: Some("a.wav".to_owned()),
                    size_bytes: None,
                    sha256: None,
                    source: AttachmentDataSource::Bytes {
                        base64_data: base64::engine::general_purpose::STANDARD
                            .encode(crate::attachments::regression::wav()),
                    },
                    artifact: None,
                }),
                "video" => MessageContentPart::video(MessageAttachment {
                    mime_type: "video/mp4".to_owned(),
                    name: Some("v.mp4".to_owned()),
                    size_bytes: None,
                    sha256: None,
                    source: AttachmentDataSource::Bytes {
                        base64_data: base64::engine::general_purpose::STANDARD
                            .encode(crate::attachments::regression::video()),
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
                    &[{
                        let mut message = ChatMessage::user_parts(vec![part]);
                        message.content = "analyze".to_owned();
                        message
                    }],
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
                "openai-compatible provider `{alias}` must expose only the compatible renderer ceiling"
            );
        }
    }

    #[tokio::test]
    async fn factory_supports_custom_base_url_for_openai_and_compatible() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        async fn server() -> (String, tokio::task::JoinHandle<String>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/gateway/v1/", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = [0u8; 8192];
                let count = stream.read(&mut bytes).await.unwrap();
                let request = String::from_utf8_lossy(&bytes[..count])
                    .lines()
                    .next()
                    .unwrap()
                    .to_owned();
                let body = r#"{"data":[{"id":"fixture-model"}]}"#;
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                request
            });
            (url, task)
        }
        let (openai_url, openai_server) = server().await;
        let (compat_url, compat_server) = server().await;
        let custom_openai =
            create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority(
                "openai",
                "sk-custom",
                ProviderTimeoutPolicy::default(),
                None,
                Some(&openai_url),
                "fp-test-1",
            )
            .expect("create custom openai provider");
        assert_eq!(custom_openai.name(), "openai");
        assert_eq!(
            custom_openai.list_models().await.unwrap()[0].id,
            "fixture-model"
        );
        assert_eq!(
            openai_server.await.unwrap(),
            "GET /gateway/v1/models HTTP/1.1"
        );

        let custom_compat =
            create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority(
                "custom",
                "sk-custom",
                ProviderTimeoutPolicy::default(),
                None,
                Some(&compat_url),
                "fp-test-2",
            )
            .expect("create custom-compatible provider");
        assert_eq!(custom_compat.name(), "custom");
        assert_eq!(
            custom_compat.list_models().await.unwrap()[0].id,
            "fixture-model"
        );
        assert_eq!(
            compat_server.await.unwrap(),
            "GET /gateway/v1/models HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn factory_routes_special_provider_discovery_to_custom_endpoints() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        for (name, expected_path, body) in [
            (
                "ollama",
                "/gateway/api/tags",
                r#"{"models":[{"name":"fixture"}]}"#,
            ),
            (
                "glm",
                "/gateway/api/models",
                r#"{"data":[{"id":"fixture"}]}"#,
            ),
            (
                "deepseek",
                "/gateway/api/models",
                r#"{"data":[{"id":"fixture"}]}"#,
            ),
            (
                "gemini",
                "/gateway/api/models?key=key",
                r#"{"models":[{"name":"models/fixture"}]}"#,
            ),
            (
                "telnyx",
                "/gateway/api/models",
                r#"{"data":[{"id":"fixture"}]}"#,
            ),
            (
                "copilot",
                "/gateway/api/models",
                r#"{"data":[{"id":"fixture"}]}"#,
            ),
            (
                "azure-openai",
                "/gateway/api/openai/models?api-version=2024-08-01-preview",
                r#"{"data":[{"id":"fixture"}]}"#,
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base_url = format!("http://{}/gateway/api/", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = [0u8; 8192];
                let count = stream.read(&mut bytes).await.unwrap();
                let request_line = String::from_utf8_lossy(&bytes[..count])
                    .lines()
                    .next()
                    .unwrap()
                    .to_owned();
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                request_line
            });
            let provider =
                create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority(
                    name,
                    if name == "ollama" { "" } else { "key" },
                    ProviderTimeoutPolicy::default(),
                    None,
                    Some(&base_url),
                    "fixture-authority",
                )
                .unwrap();
            let models = provider.list_models().await.unwrap();
            assert!(!models.is_empty(), "{name}");
            assert_eq!(
                server.await.unwrap(),
                format!("GET {expected_path} HTTP/1.1"),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn special_provider_overrides_route_chat_and_complete_streams() {
        use crate::types::ChatRequest;
        use futures_util::StreamExt;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let openai_reply =
            r#"{"choices":[{"message":{"content":"fixture reply"},"finish_reason":"stop"}]}"#;
        let openai_stream = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"fixture reply\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let gemini_reply = r#"{"candidates":[{"content":{"role":"model","parts":[{"text":"fixture reply"}]},"finishReason":"STOP"}]}"#;
        let gemini_stream = concat!(
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"fixture reply\"}]},\"finishReason\":\"STOP\"}]}\n\n"
        );

        for name in [
            "ollama",
            "glm",
            "deepseek",
            "gemini",
            "telnyx",
            "copilot",
            "azure-openai",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base_url = format!(
                "http://{}/secret-gateway/api/",
                listener.local_addr().unwrap()
            );
            let (chat_reply, stream_reply) = match name {
                "ollama" => (
                    r#"{"message":{"content":"fixture reply"},"done":true}"#,
                    concat!(
                        "{\"message\":{\"content\":\"fixture reply\"},\"done\":false}\n",
                        "{\"message\":{},\"done\":true}\n"
                    ),
                ),
                "gemini" => (gemini_reply, gemini_stream),
                _ => (openai_reply, openai_stream),
            };
            let server = tokio::spawn(async move {
                let mut requests = Vec::new();
                for reply in [chat_reply, stream_reply] {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut bytes = Vec::new();
                    let mut buffer = [0u8; 4096];
                    loop {
                        let count = socket.read(&mut buffer).await.unwrap();
                        assert!(count > 0, "request ended before its body");
                        bytes.extend_from_slice(&buffer[..count]);
                        if let Some(header_end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&bytes[..header_end]);
                            let length = headers
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|value| value.trim().parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            if bytes.len() >= header_end + 4 + length {
                                break;
                            }
                        }
                    }
                    requests.push(String::from_utf8(bytes).unwrap());
                    let content_type = if reply.starts_with("data:") {
                        "text/event-stream"
                    } else {
                        "application/json"
                    };
                    socket.write_all(format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}", reply.len()
                    ).as_bytes()).await.unwrap();
                }
                requests
            });
            // The simple factory gets Azure's deployment from the process environment.
            // Use the same adapter constructor with an explicit fixture deployment here.
            let provider: Box<dyn Provider> = if name == "azure-openai" {
                Box::new(AzureOpenAiProvider::with_base_url_and_timeout_policy(
                    "key",
                    "unused-resource",
                    "fixture-deployment",
                    &base_url,
                    ProviderTimeoutPolicy::default(),
                ))
            } else {
                create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority(
                    name,
                    if name == "ollama" { "" } else { "key" },
                    ProviderTimeoutPolicy::default(),
                    None,
                    Some(&base_url),
                    "fixture-authority",
                )
                .unwrap()
            };
            let request = || ChatRequest {
                model: "fixture".to_owned(),
                messages: vec![ChatMessage::user("hello")],
                temperature: None,
                max_tokens: None,
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                reasoning: None,
                compiled_prompt: None,
            };
            let (chat, chunks) = crate::attachments::runtime::with_async_authority_scope(
                "fixture-authority".to_owned(),
                async {
                    let chat = provider.chat(request()).await;
                    let chunks = provider
                        .stream_chat(request())
                        .await?
                        .collect::<Vec<_>>()
                        .await;
                    Ok::<_, anyhow::Error>((chat?, chunks))
                },
            )
            .await
            .unwrap();
            assert_eq!(chat.text, "fixture reply", "{name}");
            let chunks: Vec<_> = chunks.into_iter().collect::<Result<_, _>>().unwrap();
            assert_eq!(
                chunks.iter().map(|c| c.delta.as_str()).collect::<String>(),
                "fixture reply",
                "{name}"
            );
            assert!(
                chunks.iter().any(|c| c.is_final),
                "{name} stream never finished"
            );

            let requests = server.await.unwrap();
            let expected_path = match name {
                "ollama" => "/secret-gateway/api/chat".to_owned(),
                "gemini" => "/secret-gateway/api/models/fixture:generateContent?key=key".to_owned(),
                "azure-openai" => "/secret-gateway/api/openai/deployments/fixture-deployment/chat/completions?api-version=2024-08-01-preview".to_owned(),
                _ => "/secret-gateway/api/chat/completions".to_owned(),
            };
            let expected_stream_path = if name == "gemini" {
                "/secret-gateway/api/models/fixture:streamGenerateContent?alt=sse&key=key"
                    .to_owned()
            } else {
                expected_path.clone()
            };
            for (index, expected) in [expected_path, expected_stream_path].iter().enumerate() {
                assert!(
                    requests[index].starts_with(&format!("POST {expected} HTTP/1.1")),
                    "{name}: {}",
                    requests[index].lines().next().unwrap()
                );
                let lower = requests[index].to_ascii_lowercase();
                if name == "azure-openai" {
                    assert!(lower.contains("api-key: key\r\n"), "{name}");
                } else if !matches!(name, "ollama" | "gemini") {
                    assert!(lower.contains("authorization: bearer key\r\n"), "{name}");
                } else {
                    assert!(!lower.contains("authorization:"), "{name}");
                }
                if name == "copilot" {
                    assert!(lower.contains("editor-version: pioneer/0.1.0\r\n"));
                }
                let body = requests[index].split_once("\r\n\r\n").unwrap().1;
                let json: serde_json::Value = serde_json::from_str(body).unwrap();
                if name != "gemini" {
                    assert_eq!(json["stream"], index == 1, "{name}");
                }
            }
        }
    }
}
