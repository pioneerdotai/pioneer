pub(crate) mod admission;
mod audio_timing;
mod budget;
mod contracts;
mod errors;
pub(crate) mod input_estimate;
#[cfg(test)]
pub(crate) mod media_fixtures;
mod mp4_timing;
mod normalize;
mod observability;
mod plan;
mod registry;
#[cfg(test)]
pub(crate) mod regression;
mod resolve;
pub(crate) mod runtime;
mod security;
mod types;
mod webm;

use crate::attachments::errors::AttachmentPipelineError;
use crate::attachments::normalize::{normalize_attachment_name, reconcile_mime};
use crate::attachments::resolve::{resolve_attachment_source, resolve_sha256};
use crate::types::{
    ChatMessage, ChatRequest, InputContentType, MessageAttachment, MessageContentPart,
    ProviderCapabilities,
};
use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
pub(crate) use contracts::MediaInputRejection;
pub(crate) use normalize::canonical_mime as canonical_media_mime;
pub(crate) use normalize::normalize_mime as normalized_media_mime;
use std::sync::Arc;
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

const ATTACHMENT_BLOCKING_MAX_CONCURRENCY: usize = 16;
const ATTACHMENT_BLOCKING_QUEUE_TIMEOUT: Duration = Duration::from_secs(30);

static ATTACHMENT_BLOCKING_GOVERNOR: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

fn attachment_blocking_governor() -> Arc<tokio::sync::Semaphore> {
    ATTACHMENT_BLOCKING_GOVERNOR
        .get_or_init(|| {
            Arc::new(tokio::sync::Semaphore::new(
                ATTACHMENT_BLOCKING_MAX_CONCURRENCY,
            ))
        })
        .clone()
}

pub use input_estimate::{MediaInputEstimate, PreparedInputBudget, image_tokens};
pub use normalize::infer_mime_from_reference;
pub use registry::{
    ArtifactExternalRefCacheBackend, ArtifactExternalRefLookupRequest,
    ArtifactExternalRefStoreRequest, lookup_uploaded_reference_with_artifact_for_authority,
    model_family_for_model, set_artifact_external_ref_cache_backend,
    store_uploaded_reference_for_authority, upload_registry_key_for_authority,
};
pub use runtime::AttachmentOperationAuthority;
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

/// Synchronous request preflight with a concrete replay compatibility target.
/// Production adapters use the async equivalent; this entry point lets local
/// wire-builder tests exercise the identical projection without transport.
#[cfg(test)]
pub fn prepare_messages_for_provider_model(
    provider_name: &str,
    model: &str,
    capabilities: &ProviderCapabilities,
    messages: &[ChatMessage],
) -> Result<PreparedProviderMessages> {
    let config = default_attachment_pipeline_config();
    prepare_messages_for_provider_target(
        provider_name,
        Some(model),
        None,
        capabilities,
        messages,
        &config,
    )
}

/// Runs filesystem, DNS, blocking HTTP and retry materialization outside the
/// async worker pool. The semaphore bounds concurrent blocking work; the
/// durable execution governor bounds how many Turns may wait to enter this
/// stage.
pub async fn prepare_messages_for_provider_async(
    provider_name: &str,
    model: &str,
    capabilities: &ProviderCapabilities,
    messages: &[ChatMessage],
) -> Result<PreparedProviderMessages> {
    prepare_messages_async(provider_name, model, None, capabilities, messages).await
}

/// Request-aware replay projection for generation consumers. Message-only
/// callers retain their legacy policy; requests supply their current mode.
/// `messages` can be rendered prompt messages or the budget's canonical copy.
pub(crate) async fn prepare_messages_for_request_async(
    provider_name: &str,
    capabilities: &ProviderCapabilities,
    request: &ChatRequest,
    messages: &[ChatMessage],
) -> Result<PreparedProviderMessages> {
    prepare_messages_async(
        provider_name,
        &request.model,
        crate::history::request_thinking_override(provider_name, request),
        capabilities,
        messages,
    )
    .await
}

#[cfg(test)]
pub(crate) fn prepare_messages_for_request(
    provider_name: &str,
    capabilities: &ProviderCapabilities,
    request: &ChatRequest,
    messages: &[ChatMessage],
) -> Result<PreparedProviderMessages> {
    prepare_messages_for_provider_target(
        provider_name,
        Some(&request.model),
        crate::history::request_thinking_override(provider_name, request),
        capabilities,
        messages,
        &default_attachment_pipeline_config(),
    )
}

async fn prepare_messages_async(
    provider_name: &str,
    model: &str,
    thinking_override: Option<bool>,
    capabilities: &ProviderCapabilities,
    messages: &[ChatMessage],
) -> Result<PreparedProviderMessages> {
    // Reject mismatches before I/O and retain this snapshot through materialization.
    let state = admission::current();
    let entry = if messages.iter().any(ChatMessage::has_attachments)
        && crate::definition::provider_definition(provider_name).is_some()
    {
        #[cfg(test)]
        let fixture_catalog = state.as_ref().and_then(|s| s.catalog.clone());
        #[cfg(not(test))]
        let fixture_catalog: Option<Arc<crate::catalog::ModelCatalog>> = None;
        let catalog = fixture_catalog
            .map(Ok)
            .unwrap_or_else(crate::catalog::model_catalog)
            .context(MediaInputRejection(
                "model catalog is unavailable; retry after catalog refresh",
            ))?;
        let entry = admission::effective_model(provider_name, model, &catalog);
        contracts::validate_effective_model_inputs(
            provider_name,
            capabilities,
            messages,
            entry.as_ref(),
        )?;
        entry
    } else {
        None
    };
    let authority_fingerprint = runtime::current_authority_fingerprint()?;
    let permit = tokio::time::timeout(
        ATTACHMENT_BLOCKING_QUEUE_TIMEOUT,
        attachment_blocking_governor().acquire_owned(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("attachment materialization capacity wait timed out"))?
    .map_err(|_| anyhow::anyhow!("attachment materialization governor is closed"))?;
    let provider_name = provider_name.to_owned();
    let model = model.to_owned();
    let capabilities = capabilities.clone();
    let messages = messages.to_vec();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime::with_blocking_authority_scope(authority_fingerprint, || {
            admission::blocking_scope(state, || {
                #[cfg(test)]
                let config = admission::current()
                    .and_then(|s| s.pipeline_config.clone())
                    .unwrap_or_else(default_attachment_pipeline_config);
                #[cfg(not(test))]
                let config = default_attachment_pipeline_config();
                let prepared = prepare_messages_for_provider_target(
                    provider_name.as_str(),
                    Some(model.as_str()),
                    thinking_override,
                    &capabilities,
                    messages.as_slice(),
                    &config,
                )
                .context(MediaInputRejection(
                    "media input could not be materialized under the selected attachment policy",
                ))?;
                if let Some(entry) = entry.as_ref() {
                    for attachment in &prepared.attachments {
                        contracts::validate_materialized_constraints(entry, attachment).context(
                            MediaInputRejection(
                                "materialized media violates catalog size/duration/MIME limits",
                            ),
                        )?;
                    }
                    contracts::validate_model_media_limits(
                        &provider_name,
                        entry,
                        &prepared.attachments,
                    )
                    .context(MediaInputRejection(
                        "media input exceeds the native model PDF/media limit",
                    ))?;
                }
                Ok(prepared)
            })
        })
    })
    .await
    .map_err(|error| anyhow::anyhow!("attachment materialization worker failed: {error}"))?
}

pub fn prepare_messages_for_provider_with_config(
    provider_name: &str,
    capabilities: &ProviderCapabilities,
    messages: &[ChatMessage],
    config: &AttachmentPipelineConfig,
) -> Result<PreparedProviderMessages> {
    prepare_messages_for_provider_target(provider_name, None, None, capabilities, messages, config)
}

fn prepare_messages_for_provider_target(
    provider_name: &str,
    model: Option<&str>,
    thinking_override: Option<bool>,
    capabilities: &ProviderCapabilities,
    messages: &[ChatMessage],
    config: &AttachmentPipelineConfig,
) -> Result<PreparedProviderMessages> {
    let attachment_count_hint = messages
        .iter()
        .map(|message| message.content_parts.len())
        .sum::<usize>();
    observability::emit_preflight_start(provider_name, messages.len(), attachment_count_hint);

    let result = prepare_messages_impl(
        provider_name,
        model,
        thinking_override,
        capabilities,
        messages,
        config,
    );
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
    model: Option<&str>,
    thinking_override: Option<bool>,
    capabilities: &ProviderCapabilities,
    messages: &[ChatMessage],
    config: &AttachmentPipelineConfig,
) -> Result<PreparedProviderMessages> {
    let projected_messages = model
        .map(|model| {
            crate::history::project_messages(provider_name, model, thinking_override, messages)
        })
        .transpose()?;
    let messages = projected_messages.as_deref().unwrap_or(messages);
    let attachment_count = messages
        .iter()
        .flat_map(|message| message.content_parts.iter())
        .filter(|part| !matches!(part, MessageContentPart::Text { .. }))
        .count();
    if attachment_count > config.max_attachments_per_request {
        return Err(AttachmentPipelineError::attachment_count_exceeded(
            attachment_count,
            config.max_attachments_per_request,
        )
        .into());
    }
    let mut prepared_messages = Vec::with_capacity(messages.len());
    let mut attachments = Vec::with_capacity(attachment_count);
    let mut materialized_total_bytes = 0usize;

    for (message_index, message) in messages.iter().enumerate() {
        let mut rendered_parts = Vec::new();
        if !message.content.trim().is_empty() {
            rendered_parts.push(message.content.clone());
        }

        for (part_index, part) in message.content_parts.iter().enumerate() {
            if let Some((kind, attachment)) = match part {
                MessageContentPart::Text { .. } => None,
                MessageContentPart::File { file } => Some((InputContentType::File, file)),
                MessageContentPart::Image { image } => Some((InputContentType::Image, image)),
                MessageContentPart::Audio { audio } => Some((InputContentType::Audio, audio)),
                MessageContentPart::Video { video } => Some((InputContentType::Video, video)),
            } {
                contracts::validate_representation(
                    provider_name,
                    kind,
                    message.role.clone(),
                    &attachment.mime_type,
                    &attachment.source,
                ).context(MediaInputRejection("media MIME/source/role is unsupported; external references require owned materialized bytes"))?;
            }
            let attachment_count_before = attachments.len();
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
                        materialized_total_bytes,
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
                        materialized_total_bytes,
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
                        materialized_total_bytes,
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
                        materialized_total_bytes,
                    )?);
                }
            }
            if attachments.len() > attachment_count_before {
                let latest = attachments
                    .last()
                    .map(|attachment| attachment.size_bytes)
                    .unwrap_or_default();
                materialized_total_bytes = materialized_total_bytes.saturating_add(latest);
                if materialized_total_bytes > config.max_total_bytes_per_request {
                    return Err(AttachmentPipelineError::attachment_total_budget_exceeded(
                        materialized_total_bytes,
                        config.max_total_bytes_per_request,
                    )
                    .into());
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

    contracts::validate_prepared(provider_name, &prepared_messages, &attachments).context(
        MediaInputRejection("media input violates native MIME/role/count/size/text requirements"),
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
    materialized_total_bytes: usize,
) -> Result<PreparedAttachment> {
    let support = capabilities.input_types.support_for(kind);
    if !support.is_supported() {
        return Err(AttachmentPipelineError::contract_violation(format!(
            "provider `{provider_name}` does not declare support for `{}` attachments",
            content_kind_label(kind)
        ))
        .into());
    }

    let remaining_total_bytes = config
        .max_total_bytes_per_request
        .saturating_sub(materialized_total_bytes);
    let source_limit = config.max_bytes_per_attachment.min(remaining_total_bytes);
    let resolved_source =
        match resolve_attachment_source(provider_name, attachment, kind, config, source_limit) {
            Ok(source) => source,
            Err(error)
                if source_limit < config.max_bytes_per_attachment
                    && error
                        .downcast_ref::<AttachmentPipelineError>()
                        .is_some_and(|error| error.code() == "ATTACHMENT_TOO_LARGE") =>
            {
                return Err(AttachmentPipelineError::attachment_total_budget_exceeded(
                    materialized_total_bytes
                        .saturating_add(source_limit)
                        .saturating_add(1),
                    config.max_total_bytes_per_request,
                )
                .into());
            }
            Err(error) => return Err(error),
        };
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

    let size_bytes = if let Some(bytes) = resolved_source.bytes.as_ref() {
        let actual_size = bytes.len();
        if let Some(declared_size) = attachment.size_bytes
            && usize::try_from(declared_size).ok() != Some(actual_size)
        {
            return Err(AttachmentPipelineError::declared_size_mismatch(
                declared_size,
                actual_size,
                resolved_source.source_label.as_str(),
            )
            .into());
        }
        actual_size
    } else {
        attachment
            .size_bytes
            .map(|value| usize::try_from(value).unwrap_or(usize::MAX))
            .unwrap_or_default()
    };

    if size_bytes > config.max_bytes_per_attachment {
        return Err(AttachmentPipelineError::attachment_too_large(
            size_bytes,
            config.max_bytes_per_attachment,
            resolved_source.source_label.as_str(),
        )
        .into());
    }
    if size_bytes > remaining_total_bytes {
        return Err(AttachmentPipelineError::attachment_total_budget_exceeded(
            materialized_total_bytes.saturating_add(size_bytes),
            config.max_total_bytes_per_request,
        )
        .into());
    }

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
            provenance: None,
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
                        base64_data: BASE64.encode(super::regression::image(
                            image::ImageFormat::Png,
                            1,
                            1,
                        )),
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
                base64_data: BASE64.encode(super::regression::image(image::ImageFormat::Png, 1, 1)),
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
        let png = super::regression::image(image::ImageFormat::Png, 1, 1);
        let config = AttachmentPipelineConfig {
            max_bytes_per_attachment: png.len(),
            max_total_bytes_per_request: 2 * png.len() - 1,
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
                base64_data: BASE64.encode(&png),
            },
            artifact: None,
        });
        let second = MessageContentPart::image(MessageAttachment {
            mime_type: "image/png".to_owned(),
            name: Some("b.png".to_owned()),
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data: BASE64.encode(&png),
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
    fn declared_attachment_size_must_match_materialized_bytes() {
        let message = ChatMessage::user_parts(vec![MessageContentPart::file(MessageAttachment {
            mime_type: "application/octet-stream".to_owned(),
            name: Some("payload.bin".to_owned()),
            size_bytes: Some(99),
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data: BASE64.encode([1_u8, 2, 3]),
            },
            artifact: None,
        })]);

        let error = prepare_messages_for_provider("mock", &caps_with_native_declared(), &[message])
            .expect_err("a false declared size must be rejected");

        assert!(error.to_string().contains("ATTACHMENT_SIZE_MISMATCH"));
    }

    #[test]
    fn sparse_path_is_rejected_by_metadata_before_reading_content() {
        let root =
            std::env::temp_dir().join(format!("pioneer-attachment-sparse-{}", std::process::id()));
        std::fs::create_dir(&root).expect("create sparse attachment test directory");
        let path = root.join("oversized.bin");
        let result = (|| {
            let file = std::fs::File::create(&path).expect("create sparse file");
            file.set_len(65).expect("extend sparse file");

            let message =
                ChatMessage::user_parts(vec![MessageContentPart::file(MessageAttachment {
                    mime_type: "application/octet-stream".to_owned(),
                    name: Some("oversized.bin".to_owned()),
                    size_bytes: None,
                    sha256: None,
                    source: AttachmentDataSource::Path {
                        path: path.display().to_string(),
                    },
                    artifact: None,
                })]);
            let mut config = AttachmentPipelineConfig::default();
            config.max_bytes_per_attachment = 64;
            config.max_total_bytes_per_request = 128;

            let error = prepare_messages_for_provider_with_config(
                "mock",
                &caps_with_native_declared(),
                &[message],
                &config,
            )
            .expect_err("sparse path beyond the hard limit must be rejected");
            assert!(error.to_string().contains("ATTACHMENT_TOO_LARGE"));
            Ok::<(), anyhow::Error>(())
        })();
        let _ = std::fs::remove_dir_all(&root);
        result.expect("sparse attachment regression");
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

/// Count the complete serialized body before transport. The endpoint limit
/// includes binary base64 expansion, text, tools and all structural overhead.
/// Sources: Claude PDF support (32 MB); Gemini file input methods (100 MB).
pub(crate) fn validate_inline_payload(provider: &str, value: &impl serde::Serialize) -> Result<()> {
    let limit = match provider {
        "anthropic" => 32_000_000,
        "gemini" => 100_000_000,
        "groq" => 20_000_000,
        _ => return Ok(()),
    };
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self.bytes.saturating_add(bytes.len());
            if self.bytes > self.limit {
                return Err(std::io::Error::other(
                    "native request payload exceeds endpoint byte limit",
                ));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter { bytes: 0, limit }, value).map_err(|_| {
        MediaInputRejection(
            "native request exceeds its inline payload byte limit or cannot be serialized",
        )
        .into()
    })
}

#[cfg(test)]
mod payload_limit_tests {
    use super::*;
    #[test]
    fn native_payload_limit_counts_json_escaping_and_structural_overhead() {
        // Raw text is below 32 MB, but serialized quotes expand past the limit.
        let payload = serde_json::json!({"content":"\"".repeat(16_000_001)});
        assert!(validate_inline_payload("anthropic", &payload).is_err());
        assert!(validate_inline_payload("gemini", &payload).is_ok());
    }
}
