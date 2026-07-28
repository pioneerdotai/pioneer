mod budget;
mod errors;
mod normalize;
mod observability;
mod plan;
mod registry;
mod resolve;
pub(crate) mod runtime;
mod security;
mod types;

use crate::attachments::errors::AttachmentPipelineError;
use crate::attachments::normalize::{normalize_attachment_name, reconcile_mime};
use crate::attachments::resolve::{resolve_attachment_source, resolve_sha256};
use crate::types::{
    ChatMessage, InputContentType, MessageAttachment, MessageContentPart, ProviderCapabilities,
};
use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use std::sync::{OnceLock, RwLock};

pub use normalize::infer_mime_from_reference;
pub use registry::{
    ArtifactExternalRefCacheBackend, ArtifactExternalRefLookupRequest,
    ArtifactExternalRefStoreRequest, lookup_uploaded_reference_with_artifact,
    model_family_for_model, set_artifact_external_ref_cache_backend, store_uploaded_reference,
    upload_registry_key,
};
pub use runtime::AttachmentOperationError;
pub use types::{
    ArtifactExternalRefCachePolicy, AttachmentBudgetReport, AttachmentCircuitBreakerPolicy,
    AttachmentNormalizationPolicy, AttachmentPipelineConfig, AttachmentRetryPolicy,
    AttachmentRuntimePolicy, AttachmentSecurityPolicy, AttachmentTransportKind,
    AttachmentTransportPlan, PreparedAttachment, PreparedAttachmentSource,
    PreparedProviderMessages,
};

static PIPELINE_CONFIG: OnceLock<RwLock<AttachmentPipelineConfig>> = OnceLock::new();

fn pipeline_config_store() -> &'static RwLock<AttachmentPipelineConfig> {
    PIPELINE_CONFIG.get_or_init(|| RwLock::new(AttachmentPipelineConfig::default()))
}

pub fn set_default_attachment_pipeline_config(config: AttachmentPipelineConfig) {
    let mut guard = pipeline_config_store()
        .write()
        .expect("attachment pipeline config lock poisoned");
    *guard = config;
}

pub fn default_attachment_pipeline_config() -> AttachmentPipelineConfig {
    pipeline_config_store()
        .read()
        .expect("attachment pipeline config lock poisoned")
        .clone()
}

pub fn attachment_data_url(attachment: &PreparedAttachment) -> Result<String> {
    let bytes = attachment_bytes(attachment)?;
    Ok(format!(
        "data:{};base64,{}",
        attachment.mime_type,
        BASE64.encode(bytes)
    ))
}

pub fn attachment_bytes(attachment: &PreparedAttachment) -> Result<&[u8]> {
    attachment.bytes.as_deref().ok_or_else(|| {
        AttachmentPipelineError::contract_violation(format!(
            "attachment `{}` (kind={:?}, mime={}) has no materialized bytes",
            attachment.name, attachment.kind, attachment.mime_type
        ))
        .into()
    })
}

pub fn prepare_messages_for_provider(
    provider_name: &str,
    capabilities: &ProviderCapabilities,
    messages: &[ChatMessage],
) -> Result<PreparedProviderMessages> {
    let config = default_attachment_pipeline_config();
    prepare_messages_for_provider_with_config(provider_name, capabilities, messages, &config)
}

pub fn prepare_messages_for_provider_with_config(
    provider_name: &str,
    capabilities: &ProviderCapabilities,
    messages: &[ChatMessage],
    config: &AttachmentPipelineConfig,
) -> Result<PreparedProviderMessages> {
    let attachment_count_hint = messages
        .iter()
        .map(|message| message.content_parts.len())
        .sum::<usize>();
    observability::emit_preflight_start(provider_name, messages.len(), attachment_count_hint);

    let result = prepare_messages_impl(provider_name, capabilities, messages, config);
    match &result {
        Ok(prepared) => {
            observability::emit_preflight_ok(provider_name, prepared.budget_report);
            observability::emit_request_materialized(
                provider_name,
                prepared.budget_report.attachment_count,
                prepared.budget_report.total_bytes,
            );
        }
        Err(error) => {
            let (code, message) = error_code_and_message(error);
            observability::emit_preflight_fail(provider_name, code, message.as_str());
        }
    }

    result
}

fn prepare_messages_impl(
    provider_name: &str,
    capabilities: &ProviderCapabilities,
    messages: &[ChatMessage],
    config: &AttachmentPipelineConfig,
) -> Result<PreparedProviderMessages> {
    let mut prepared_messages = Vec::with_capacity(messages.len());
    let mut attachments = Vec::new();

    for (message_index, message) in messages.iter().enumerate() {
        let mut rendered_parts = Vec::new();
        if !message.content.trim().is_empty() {
            rendered_parts.push(message.content.clone());
        }

        for (part_index, part) in message.content_parts.iter().enumerate() {
            match part {
                MessageContentPart::Text { text } => {
                    if !text.trim().is_empty() {
                        rendered_parts.push(text.clone());
                    }
                }
                MessageContentPart::File { file } => {
                    attachments.push(resolve_attachment(
                        provider_name,
                        capabilities,
                        message_index,
                        part_index,
                        InputContentType::File,
                        file,
                        config,
                    )?);
                }
                MessageContentPart::Image { image } => {
                    attachments.push(resolve_attachment(
                        provider_name,
                        capabilities,
                        message_index,
                        part_index,
                        InputContentType::Image,
                        image,
                        config,
                    )?);
                }
                MessageContentPart::Audio { audio } => {
                    attachments.push(resolve_attachment(
                        provider_name,
                        capabilities,
                        message_index,
                        part_index,
                        InputContentType::Audio,
                        audio,
                        config,
                    )?);
                }
                MessageContentPart::Video { video } => {
                    attachments.push(resolve_attachment(
                        provider_name,
                        capabilities,
                        message_index,
                        part_index,
                        InputContentType::Video,
                        video,
                        config,
                    )?);
                }
            }
        }

        let mut normalized = message.clone();
        normalized.content = rendered_parts.join("\n\n");
        normalized.content_parts.clear();
        prepared_messages.push(normalized);
    }

    plan::assign_transport_plans(
        provider_name,
        capabilities,
        config,
        attachments.as_mut_slice(),
    )?;

    let budget_report = budget::validate_budget(config, attachments.as_slice())?;

    Ok(PreparedProviderMessages {
        messages: prepared_messages,
        attachments,
        budget_report,
    })
}

pub fn ensure_no_unrendered_attachments(
    provider_name: &str,
    prepared: &PreparedProviderMessages,
) -> Result<()> {
    if let Some(first_unsupported) = prepared.attachments.iter().find(|attachment| {
        matches!(
            attachment.transport_plan.kind,
            AttachmentTransportKind::Unsupported
        )
    }) {
        return Err(AttachmentPipelineError::contract_violation(format!(
            "provider `{provider_name}` has unsupported attachment plan for `{}` ({:?}, mime={})",
            first_unsupported.name, first_unsupported.kind, first_unsupported.mime_type
        ))
        .into());
    }
    Ok(())
}

fn resolve_attachment(
    provider_name: &str,
    capabilities: &ProviderCapabilities,
    message_index: usize,
    part_index: usize,
    kind: InputContentType,
    attachment: &MessageAttachment,
    config: &AttachmentPipelineConfig,
) -> Result<PreparedAttachment> {
    let support = capabilities.input_types.support_for(kind);
    if !support.is_supported() {
        return Err(AttachmentPipelineError::contract_violation(format!(
            "provider `{provider_name}` does not declare support for `{}` attachments",
            content_kind_label(kind)
        ))
        .into());
    }

    let resolved_source = resolve_attachment_source(provider_name, attachment, kind, config)?;
    let mime = reconcile_mime(
        attachment.mime_type.as_str(),
        resolved_source.bytes.as_deref(),
        &config.normalization,
    )?;
    let name = normalize_attachment_name(
        attachment.name.as_deref(),
        resolved_source.source_name.as_deref(),
        kind,
        mime.as_str(),
        &config.normalization,
    )?;

    let size_bytes = attachment
        .size_bytes
        .and_then(|value| usize::try_from(value).ok())
        .or_else(|| resolved_source.bytes.as_ref().map(Vec::len))
        .unwrap_or_default();

    let sha256 = resolve_sha256(
        attachment.sha256.as_deref(),
        resolved_source.bytes.as_deref(),
        resolved_source.source_label.as_str(),
    )?;

    Ok(PreparedAttachment {
        message_index,
        part_index,
        kind,
        mime_type: mime,
        name,
        size_bytes,
        sha256,
        source: resolved_source.source,
        bytes: resolved_source.bytes,
        transport_plan: AttachmentTransportPlan {
            kind: AttachmentTransportKind::Unsupported,
            reason: String::new(),
        },
        artifact: attachment.artifact.clone(),
    })
}

fn content_kind_label(kind: InputContentType) -> &'static str {
    match kind {
        InputContentType::Text => "text",
        InputContentType::File => "file",
        InputContentType::Image => "image",
        InputContentType::Audio => "audio",
        InputContentType::Video => "video",
    }
}

fn error_code_and_message(error: &anyhow::Error) -> (&'static str, String) {
    if let Some(pipeline) = error.downcast_ref::<AttachmentPipelineError>() {
        return (pipeline.code(), pipeline.to_string());
    }
    ("ATTACHMENT_PIPELINE_UNKNOWN_ERROR", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AttachmentDataSource, InputTypeSupport, ProviderInputCapabilities, Role};
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;

    fn caps_with_native_declared() -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: false,
            tool_calling: true,
            embeddings: false,
            transcription: false,
            input_types: ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport::native_inline_only(),
                image: InputTypeSupport::native_inline_only(),
                audio: InputTypeSupport::native_inline_only(),
                video: InputTypeSupport::native_inline_only(),
            },
        }
    }

    #[test]
    fn prepares_text_and_attachment_context_without_inlining_binary() {
        let message = ChatMessage {
            role: Role::User,
            content: "analyze".to_owned(),
            reasoning_content: None,
            content_parts: vec![
                MessageContentPart::text("extra context"),
                MessageContentPart::image(MessageAttachment {
                    mime_type: "image/png".to_owned(),
                    name: Some("screen.png".to_owned()),
                    size_bytes: None,
                    sha256: None,
                    source: AttachmentDataSource::Bytes {
                        base64_data: BASE64.encode([1u8, 2, 3, 4]),
                    },
                    artifact: None,
                }),
            ],
            tool_call_id: None,
            name: None,
            tool_calls: None,
            provider_replay_state: None,
        };

        let prepared =
            prepare_messages_for_provider("mock", &caps_with_native_declared(), &[message])
                .expect("pipeline should succeed");

        assert_eq!(prepared.messages.len(), 1);
        assert_eq!(prepared.messages[0].content, "analyze\n\nextra context");
        assert!(prepared.messages[0].content_parts.is_empty());
        assert!(
            !prepared.messages[0]
                .content
                .contains("data:image/png;base64,")
        );

        assert_eq!(prepared.attachments.len(), 1);
        assert_eq!(prepared.attachments[0].mime_type, "image/png");
        assert_eq!(prepared.budget_report.attachment_count, 1);
    }

    #[test]
    fn fails_when_provider_does_not_declare_attachment_support() {
        let caps = ProviderCapabilities {
            streaming: true,
            vision: false,
            tool_calling: true,
            embeddings: false,
            transcription: false,
            input_types: ProviderInputCapabilities::disabled_for_all_file_types(),
        };

        let message = ChatMessage::user_parts(vec![MessageContentPart::file(MessageAttachment {
            mime_type: "application/pdf".to_owned(),
            name: Some("doc.pdf".to_owned()),
            size_bytes: Some(10),
            sha256: None,
            source: AttachmentDataSource::Reference {
                reference: "file://artifact/1".to_owned(),
            },
            artifact: None,
        })]);

        let err = prepare_messages_for_provider("mock", &caps, &[message])
            .expect_err("attachments should fail when support is not declared");
        assert!(
            err.to_string()
                .contains("ATTACHMENT_PIPELINE_CONTRACT_VIOLATION")
        );
    }

    #[test]
    fn allows_messages_when_transport_plan_is_native() {
        let message = ChatMessage::user_parts(vec![MessageContentPart::image(MessageAttachment {
            mime_type: "image/png".to_owned(),
            name: Some("screen.png".to_owned()),
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data: BASE64.encode([1u8, 2, 3]),
            },
            artifact: None,
        })]);

        let prepared =
            prepare_messages_for_provider("mock", &caps_with_native_declared(), &[message])
                .expect("prepare should succeed");

        ensure_no_unrendered_attachments("mock", &prepared)
            .expect("native transport plan should pass");
    }

    #[test]
    fn budget_limits_are_enforced() {
        let config = AttachmentPipelineConfig {
            max_bytes_per_attachment: 4,
            max_total_bytes_per_request: 6,
            max_attachments_per_request: 2,
            upload_preferred_min_bytes: 1024,
            ..AttachmentPipelineConfig::default()
        };

        let first = MessageContentPart::image(MessageAttachment {
            mime_type: "image/png".to_owned(),
            name: Some("a.png".to_owned()),
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data: BASE64.encode([1u8, 2, 3, 4]),
            },
            artifact: None,
        });
        let second = MessageContentPart::image(MessageAttachment {
            mime_type: "image/png".to_owned(),
            name: Some("b.png".to_owned()),
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data: BASE64.encode([5u8, 6, 7]),
            },
            artifact: None,
        });

        let message = ChatMessage::user_parts(vec![first, second]);

        let err = prepare_messages_for_provider_with_config(
            "mock",
            &caps_with_native_declared(),
            &[message],
            &config,
        )
        .expect_err("total budget should fail");

        assert!(err.to_string().contains("ATTACHMENT_TOTAL_BUDGET_EXCEEDED"));
    }

    #[test]
    fn planner_prefers_upload_for_large_attachments_when_supported() {
        let caps = ProviderCapabilities {
            streaming: true,
            vision: true,
            tool_calling: true,
            embeddings: false,
            transcription: false,
            input_types: ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport {
                    native: true,
                    file_upload: true,
                    data_url_inline: false,
                    text_fallback: false,
                },
                image: InputTypeSupport::disabled(),
                audio: InputTypeSupport::disabled(),
                video: InputTypeSupport::disabled(),
            },
        };

        let config = AttachmentPipelineConfig {
            max_bytes_per_attachment: 1024 * 1024,
            max_total_bytes_per_request: 1024 * 1024,
            max_attachments_per_request: 8,
            upload_preferred_min_bytes: 16,
            ..AttachmentPipelineConfig::default()
        };

        let message = ChatMessage::user_parts(vec![MessageContentPart::file(MessageAttachment {
            mime_type: "application/pdf".to_owned(),
            name: Some("doc.pdf".to_owned()),
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data: BASE64.encode(vec![7u8; 32]),
            },
            artifact: None,
        })]);

        let prepared =
            prepare_messages_for_provider_with_config("mock", &caps, &[message], &config)
                .expect("prepare should succeed");
        assert_eq!(
            prepared.attachments[0].transport_plan.kind,
            AttachmentTransportKind::Upload
        );
    }

    #[test]
    fn url_source_blocked_by_security_policy() {
        let mut config = AttachmentPipelineConfig::default();
        config.security.allow_http = true;
        config.security.allow_private_network = false;

        let message = ChatMessage::user_parts(vec![MessageContentPart::image(MessageAttachment {
            mime_type: "image/png".to_owned(),
            name: Some("screen.png".to_owned()),
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Url {
                url: "http://127.0.0.1/screen.png".to_owned(),
            },
            artifact: None,
        })]);

        let err = prepare_messages_for_provider_with_config(
            "mock",
            &caps_with_native_declared(),
            &[message],
            &config,
        )
        .expect_err("private URL source must be blocked");
        assert!(err.to_string().contains("URL_SOURCE_BLOCKED"));
    }

    #[test]
    fn path_source_blocked_outside_allowlist() {
        let temp_dir = std::env::temp_dir().join("pioneer-attachments-path-allowlist");
        let _ = std::fs::create_dir_all(temp_dir.as_path());
        let target = temp_dir.join("example.bin");
        std::fs::write(target.as_path(), [1u8, 2, 3, 4]).expect("write temp file");

        let mut config = AttachmentPipelineConfig::default();
        config.security.enforce_path_allowlist = true;
        config.security.allowed_path_roots = vec![std::env::temp_dir().join("not-this-root")];

        let message = ChatMessage::user_parts(vec![MessageContentPart::file(MessageAttachment {
            mime_type: "application/octet-stream".to_owned(),
            name: None,
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Path {
                path: target.display().to_string(),
            },
            artifact: None,
        })]);

        let err = prepare_messages_for_provider_with_config(
            "mock",
            &caps_with_native_declared(),
            &[message],
            &config,
        )
        .expect_err("path outside allowlist must be blocked");
        assert!(err.to_string().contains("UNSUPPORTED_ATTACHMENT_SOURCE"));
    }
}
