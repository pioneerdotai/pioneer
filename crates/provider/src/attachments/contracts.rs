//! Media contracts intersect the refreshed model catalog with the selected
//! adapter. These are protocol rules, not a second model registry.
#[cfg(test)]
use crate::catalog::ModelCatalog;
use crate::catalog::{CatalogModel, InputCapabilityState};
use crate::{
    AttachmentDataSource, ChatMessage, InputContentType, MessageContentPart, ProviderCapabilities,
    Role,
};
use anyhow::{Context, Result, ensure};

#[cfg(test)]
pub(super) fn validate_model_inputs(
    provider: &str,
    model: &str,
    adapter: &ProviderCapabilities,
    messages: &[ChatMessage],
    catalog: &ModelCatalog,
) -> Result<()> {
    validate_effective_model_inputs(provider, adapter, messages, catalog.model(provider, model))
}

pub(super) fn validate_effective_model_inputs(
    provider: &str,
    adapter: &ProviderCapabilities,
    messages: &[ChatMessage],
    entry: Option<&CatalogModel>,
) -> Result<()> {
    let provider = crate::definition::provider_definition(provider)
        .map(|d| d.name)
        .unwrap_or(provider);

    for message in messages {
        for part in &message.content_parts {
            let (kind, attachment) = match part {
                MessageContentPart::Text { .. } => continue,
                MessageContentPart::Image { image } => (InputContentType::Image, image),
                MessageContentPart::File { file } => (InputContentType::File, file),
                MessageContentPart::Audio { audio } => (InputContentType::Audio, audio),
                MessageContentPart::Video { video } => (InputContentType::Video, video),
            };
            let Some(entry) = entry else {
                return Err(MediaInputRejection(
                    "unknown input capabilities; refresh the catalog or select a documented model",
                )
                .into());
            };
            let mime = super::normalize::normalize_mime(&attachment.mime_type)?;
            let mut state = entry.input_capability(kind);
            if kind == InputContentType::File
                && state == InputCapabilityState::Supported
                && mime != "application/pdf"
                && !entry
                    .input
                    .iter()
                    .any(|v| v.eq_ignore_ascii_case("file") || v.eq_ignore_ascii_case("document"))
            {
                // A pdf modality is not a promise about every document MIME.
                state = InputCapabilityState::Unsupported;
            }
            // Native PDF rendering is documented for vision models on these
            // endpoints. It is not generic text extraction or an audio promise.
            // https://platform.claude.com/docs/en/build-with-claude/pdf-support
            // https://developers.openai.com/api/docs/guides/pdf-files
            if kind == InputContentType::File
                && mime == "application/pdf"
                && matches!(provider, "openai" | "anthropic")
                && entry.input_capability(InputContentType::Image)
                    == InputCapabilityState::Supported
            {
                state = InputCapabilityState::Supported;
            }
            match state {
                InputCapabilityState::Unknown => {
                    return Err(MediaInputRejection(
                        "unknown input capabilities for this media type; refresh the catalog",
                    )
                    .into());
                }
                InputCapabilityState::Unsupported => {
                    return Err(MediaInputRejection(
                        "selected model does not support this media input",
                    )
                    .into());
                }
                InputCapabilityState::Supported => {}
            }
            if !adapter.input_types.support_for(kind).is_supported() {
                return Err(MediaInputRejection(
                    "selected adapter does not render this media type on its protocol endpoint",
                )
                .into());
            }
            validate_endpoint(entry, provider, kind).context(MediaInputRejection(
                "model media metadata does not establish support on the selected protocol endpoint",
            ))?;
            validate_representation(
                provider,
                kind,
                message.role.clone(),
                &mime,
                &attachment.source,
            ).context(MediaInputRejection("media MIME, source or role is unsupported by the selected adapter; opaque references require owned materialized bytes"))?;
            validate_catalog_constraints(
                entry,
                kind,
                message.role.clone(),
                &mime,
                &attachment.source,
                attachment.size_bytes,
            )
            .context(MediaInputRejection(
                "media input violates catalog MIME/source/role/size constraints",
            ))?;
        }
    }
    Ok(())
}

pub(super) fn validate_representation(
    provider: &str,
    kind: InputContentType,
    role: Role,
    mime: &str,
    source: &AttachmentDataSource,
) -> Result<()> {
    let normalized = super::normalize::normalize_mime(mime)?;
    let mime = super::normalize::canonical_mime(&normalized);
    let Some(definition) = crate::definition::provider_definition(provider) else {
        // Unregistered test doubles exercise materialization independently of
        // provider contracts. Production custom endpoints have canonical IDs.
        #[cfg(test)]
        return Ok(());
        #[cfg(not(test))]
        anyhow::bail!("unknown adapter media contract for `{provider}`");
    };
    let provider = definition.name;
    // Tool media is explicitly projected by existing adapters to a follow-up
    // user message, preserving its tool result text/call ID. System/assistant
    // binary parts have no common safe projection and must not be dropped.
    ensure!(
        matches!(role, Role::User | Role::Tool)
            && !(role == Role::Tool && matches!(provider, "copilot" | "ollama")),
        "adapter `{provider}` cannot render {kind:?} input in role {role:?}"
    );
    // Caller-supplied IDs/URIs have no verified account/endpoint ownership or
    // materialized bytes for budgeting. Internal OpenAI uploads happen AFTER
    // this boundary and use the authority-scoped registry, unchanged.
    ensure!(
        !matches!(source, AttachmentDataSource::Reference { .. }),
        "external media reference requires verified ownership and materialized original bytes; use Bytes, Path or an explicitly allowed Url source"
    );
    ensure!(
        !(provider == "bedrock" && kind == InputContentType::File && role == Role::Tool),
        "Converse Tool document lacks a sibling text block; nested result text is insufficient"
    );
    let image = matches!(
        mime,
        "image/png" | "image/jpeg" | "image/webp" | "image/gif"
    );
    let allowed = match (provider, kind) {
        ("gemini", InputContentType::Image) => matches!(
            mime,
            "image/png" | "image/jpeg" | "image/webp" | "image/heic" | "image/heif"
        ),
        (_, InputContentType::Image) => image,
        ("openai", InputContentType::Audio) => matches!(
            mime,
            "audio/wav" | "audio/x-wav" | "audio/mpeg" | "audio/mp3"
        ),
        ("openrouter", InputContentType::Audio) => matches!(
            mime,
            "audio/wav"
                | "audio/x-wav"
                | "audio/mpeg"
                | "audio/mp3"
                | "audio/flac"
                | "audio/ogg"
                | "audio/aac"
                | "audio/mp4"
                | "audio/x-m4a"
                | "audio/aiff"
                | "audio/x-aiff"
        ),
        ("gemini", InputContentType::Audio) => matches!(
            mime,
            "audio/wav"
                | "audio/x-wav"
                | "audio/mpeg"
                | "audio/mp3"
                | "audio/flac"
                | "audio/ogg"
                | "audio/aac"
                | "audio/mp4"
                | "audio/aiff"
                | "audio/x-aiff"
        ),
        ("openrouter" | "gemini" | "bedrock", InputContentType::Video) => {
            matches!(mime, "video/mp4" | "video/mpeg" | "video/webm")
        }
        ("openai" | "anthropic" | "openrouter" | "gemini", InputContentType::File) => {
            mime == "application/pdf"
        }
        ("bedrock", InputContentType::File) => matches!(
            mime,
            "application/pdf" | "text/plain" | "text/csv" | "text/html"
        ),
        // https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_AudioBlock.html
        ("bedrock", InputContentType::Audio) => matches!(
            mime,
            "audio/wav"
                | "audio/x-wav"
                | "audio/mp3"
                | "audio/mpeg"
                | "audio/flac"
                | "audio/aac"
                | "audio/ogg"
                | "audio/mp4"
                | "audio/x-m4a"
                | "audio/webm"
                | "audio/opus"
        ),
        (_, InputContentType::Text) => true,
        _ => false,
    };
    ensure!(
        allowed,
        "adapter `{provider}` has no documented renderer for {kind:?} with MIME `{mime}` on the selected endpoint"
    );
    Ok(())
}

fn validate_catalog_constraints(
    entry: &CatalogModel,
    kind: InputContentType,
    role: Role,
    mime: &str,
    source: &AttachmentDataSource,
    size: Option<u64>,
) -> Result<()> {
    let key = match kind {
        InputContentType::Text => "text",
        InputContentType::Image => "image",
        InputContentType::File => "file",
        InputContentType::Audio => "audio",
        InputContentType::Video => "video",
    };
    let Some(constraints) = entry
        .metadata
        .get("inputConstraints")
        .and_then(|v| v.get(key))
    else {
        return Ok(());
    };
    for (field, actual) in [
        ("mimeTypes", mime),
        (
            "roles",
            match role {
                Role::User => "user",
                Role::Tool => "tool",
                Role::Assistant => "assistant",
                Role::System => "system",
            },
        ),
        (
            "sources",
            match source {
                AttachmentDataSource::Bytes { .. } => "bytes",
                AttachmentDataSource::Path { .. } => "path",
                AttachmentDataSource::Url { .. } => "url",
                AttachmentDataSource::Reference { .. } => "reference",
            },
        ),
    ] {
        if field == "mimeTypes" {
            continue;
        }
        if let Some(values) = constraints.get(field) {
            let values = values
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("invalid catalog input constraint `{field}`"))?;
            if field == "sources" && matches!(source, AttachmentDataSource::Bytes { .. }) {
                continue;
            }
            ensure!(
                values.iter().any(|v| v.as_str() == Some(actual)),
                "model input constraint `{field}` rejects `{actual}`"
            );
        }
    }
    if let Some(limit) = constraints.get("maxBytes") {
        let limit = limit
            .as_u64()
            .filter(|v| *v > 0)
            .ok_or_else(|| anyhow::anyhow!("invalid catalog maxBytes"))?;
        ensure!(
            size.is_none_or(|size| size <= limit),
            "attachment exceeds model maxBytes"
        );
    }
    Ok(())
}

pub(super) fn validate_materialized_constraints(
    entry: &CatalogModel,
    attachment: &super::PreparedAttachment,
) -> Result<()> {
    let key = match attachment.kind {
        InputContentType::Text => "text",
        InputContentType::Image => "image",
        InputContentType::File => "file",
        InputContentType::Audio => "audio",
        InputContentType::Video => "video",
    };
    let Some(constraints) = entry
        .metadata
        .get("inputConstraints")
        .and_then(|v| v.get(key))
    else {
        return Ok(());
    };
    if let Some(values) = constraints.get("sources") {
        let values = values
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("invalid catalog ingress sources"))?;
        let source = super::admission::source_name(&attachment.source);
        let permitted = values.iter().any(|v| v.as_str() == Some(source));
        // This hash comes from freshly decoded bytes, not the caller's sha256.
        // Receipt state is held by the same authority instance as discovery.
        let witnessed = source == "bytes"
            && super::admission::current().is_some_and(|s| s.permits_ingress(attachment, values));
        ensure!(
            permitted || witnessed,
            "unverified ingress source violates catalog constraint"
        );
    }
    if let Some(limit) = constraints.get("maxBytes") {
        let limit = limit
            .as_u64()
            .filter(|v| *v > 0)
            .ok_or_else(|| anyhow::anyhow!("invalid catalog maxBytes"))?;
        ensure!(
            attachment.size_bytes as u64 <= limit,
            "materialized input exceeds model maxBytes"
        );
    }
    if let Some(limit) = constraints.get("maxDurationMillis") {
        let limit = limit
            .as_u64()
            .filter(|v| *v > 0)
            .ok_or_else(|| anyhow::anyhow!("invalid catalog maxDurationMillis"))?;
        ensure!(
            matches!(
                attachment.kind,
                InputContentType::Audio | InputContentType::Video
            ),
            "duration constraint requires audio/video input"
        );
        let duration = super::input_estimate::native_duration(
            super::attachment_bytes(attachment)?,
            &attachment.mime_type,
        )?;
        ensure!(
            duration.within_millis(limit)?,
            "media duration exceeds model maxDurationMillis"
        );
    }
    if let Some(values) = constraints.get("mimeTypes") {
        let values = values
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("invalid catalog mimeTypes"))?;
        ensure!(
            values.iter().any(|v| v
                .as_str()
                .and_then(|v| super::normalize::normalize_mime(v).ok())
                .is_some_and(|v| super::normalize::canonical_mime(&v)
                    == super::normalize::canonical_mime(&attachment.mime_type))),
            "normalized MIME violates model input constraint"
        );
    }
    Ok(())
}

pub(super) fn validate_prepared(
    provider: &str,
    messages: &[ChatMessage],
    attachments: &[super::PreparedAttachment],
) -> Result<()> {
    // Text-only adapters have no native media contract to validate. Keep this
    // separate from unknown adapters with actual binary input, which fail closed.
    if attachments.is_empty() {
        return Ok(());
    }
    let Some(definition) = crate::definition::provider_definition(provider) else {
        // Unregistered test doubles exercise materialization independently of
        // provider contracts. Production custom endpoints have canonical IDs.
        #[cfg(test)]
        return Ok(());
        #[cfg(not(test))]
        anyhow::bail!("unknown adapter media contract for `{provider}`");
    };
    let provider = definition.name;
    for attachment in attachments {
        // Revalidate MIME after content sniffing; declared MIME is not evidence.
        validate_representation(
            provider,
            attachment.kind,
            messages[attachment.message_index].role.clone(),
            &attachment.mime_type,
            &AttachmentDataSource::Bytes {
                base64_data: String::new(),
            },
        )?;
    }
    // All image renderers send the reconciled encoded format. Parse actual
    // dimensions here even when the caller did not request budget preparation.
    for image in attachments
        .iter()
        .filter(|a| a.kind == InputContentType::Image)
    {
        image_dimensions(image)?;
    }
    for attachment in attachments {
        let bytes = super::attachment_bytes(attachment)?;
        match attachment.kind {
            InputContentType::File if attachment.mime_type == "application/pdf" => {
                let document = lopdf::Document::load_mem(bytes)?;
                ensure!(
                    !document.is_encrypted() && !document.get_pages().is_empty(),
                    "PDF input must have readable unencrypted pages"
                );
            }
            InputContentType::Audio | InputContentType::Video => {
                super::input_estimate::duration_millis(bytes, &attachment.mime_type)?;
            }
            _ => {}
        }
    }
    if provider == "groq" {
        // Current Groq vision Chat profile: all resent history counts.
        // https://console.groq.com/docs/vision (3 images, 20 MB request).
        ensure!(
            attachments
                .iter()
                .filter(|a| a.kind == InputContentType::Image)
                .count()
                <= 3,
            "Groq vision allows at most 3 images per request"
        );
    }
    if provider == "openai" {
        // Chat accepts only PDFs; <50 MB each, <=50 MB total (file-inputs).
        let files = attachments
            .iter()
            .filter(|a| a.kind == InputContentType::File)
            .collect::<Vec<_>>();
        let total = files.iter().try_fold(0usize, |total, file| {
            ensure!(
                file.size_bytes < 50_000_000,
                "OpenAI file must be under 50 MB"
            );
            Ok::<_, anyhow::Error>(total.saturating_add(file.size_bytes))
        })?;
        ensure!(
            total <= 50_000_000,
            "OpenAI combined file input exceeds 50 MB"
        );
    }
    if provider == "gemini" {
        // https://ai.google.dev/gemini-api/docs/file-input-methods
        for pdf in attachments
            .iter()
            .filter(|a| a.kind == InputContentType::File)
        {
            ensure!(
                pdf.size_bytes <= 50_000_000,
                "Gemini inline PDF exceeds 50 MB"
            );
        }
    }
    if provider == "anthropic" {
        validate_claude_image_dimensions(attachments, false)?;
        // Direct Claude image size is 10 MB base64-encoded, not raw bytes.
        for attachment in attachments
            .iter()
            .filter(|a| a.kind == InputContentType::Image)
        {
            ensure!(
                attachment.size_bytes.div_ceil(3).saturating_mul(4) <= 10_000_000,
                "Claude base64 image exceeds 10 MB"
            );
        }
    }
    if provider == "bedrock" {
        // https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Message.html
        for (index, message) in messages.iter().enumerate() {
            let images = attachments
                .iter()
                .filter(|a| a.message_index == index && a.kind == InputContentType::Image)
                .collect::<Vec<_>>();
            let documents = attachments
                .iter()
                .filter(|a| a.message_index == index && a.kind == InputContentType::File)
                .collect::<Vec<_>>();
            ensure!(
                images.len() <= 20 && documents.len() <= 5,
                "Converse allows at most 20 images and 5 documents per message"
            );
            ensure!(
                documents.is_empty()
                    || (message.role == Role::User && !message.content.trim().is_empty()),
                "Converse document input requires a User sibling text block; Tool text is nested and does not satisfy it"
            );
            for image in images {
                ensure!(
                    image.size_bytes <= 3_750_000,
                    "Converse image exceeds 3.75 MB"
                );
                let (width, height) =
                    image::ImageReader::new(std::io::Cursor::new(super::attachment_bytes(image)?))
                        .with_guessed_format()?
                        .into_dimensions()?;
                ensure!(
                    width <= 8000 && height <= 8000,
                    "Converse image dimensions exceed 8000 pixels"
                );
            }
            for document in documents {
                ensure!(
                    document.size_bytes <= 4_500_000,
                    "Converse document exceeds 4.5 MB"
                );
            }
        }
    }
    Ok(())
}

pub(super) fn validate_model_media_limits(
    provider: &str,
    entry: &CatalogModel,
    attachments: &[super::PreparedAttachment],
) -> Result<()> {
    if provider == "gemini" {
        // https://ai.google.dev/gemini-api/docs/document-processing
        ensure!(
            pdf_pages(attachments)? <= 1000,
            "Gemini PDF input exceeds 1000 pages per request"
        );
        // https://ai.google.dev/gemini-api/docs/audio: maximum per prompt.
        let mut duration = super::input_estimate::NativeDuration::ZERO;
        for audio in attachments
            .iter()
            .filter(|a| a.kind == InputContentType::Audio)
        {
            duration = duration.add(super::input_estimate::native_duration(
                super::attachment_bytes(audio)?,
                &audio.mime_type,
            )?)?;
        }
        ensure!(
            duration.within_millis(34_200_000)?,
            "Gemini audio exceeds 9.5 hours per prompt"
        );
    }
    let claude = provider == "anthropic"
        || (provider == "bedrock"
            && entry.metadata.get("sourceMetadata").is_some_and(|v| {
                v["family"]
                    .as_str()
                    .is_some_and(|f| f.starts_with("claude"))
            }));
    if claude {
        // https://platform.claude.com/docs/en/build-with-claude/vision
        let count = attachments
            .iter()
            .filter(|a| a.kind == InputContentType::Image)
            .count();
        let limit = if entry.context_window == 200_000 || entry.context_window == 0 {
            100
        } else {
            600
        };
        ensure!(
            count <= limit,
            "Claude image count exceeds the catalog context-dependent request limit"
        );
        validate_claude_image_dimensions(attachments, provider == "bedrock")?;
        if provider == "bedrock" {
            for image in attachments
                .iter()
                .filter(|a| a.kind == InputContentType::Image)
            {
                ensure!(
                    image.size_bytes.div_ceil(3).saturating_mul(4) <= 5_000_000,
                    "Bedrock Claude base64 image exceeds 5 MB"
                );
            }
        }
    }
    if provider == "anthropic" {
        // PDF request page cap depends on catalog context, never a model-name table.
        // https://platform.claude.com/docs/en/build-with-claude/pdf-support
        let limit = if entry.context_window >= 1_000_000 {
            600
        } else {
            100
        };
        let mut pages = 0usize;
        for attachment in attachments
            .iter()
            .filter(|a| a.mime_type == "application/pdf")
        {
            let document = lopdf::Document::load_mem(super::attachment_bytes(attachment)?)?;
            ensure!(
                !document.is_encrypted(),
                "Claude PDF input must not be encrypted"
            );
            pages = pages.saturating_add(document.get_pages().len());
        }
        ensure!(
            pages <= limit,
            "Claude PDF input exceeds the catalog context-dependent request page limit ({limit})"
        );
    }
    Ok(())
}

fn image_dimensions(image: &super::PreparedAttachment) -> Result<(u32, u32)> {
    image::ImageReader::new(std::io::Cursor::new(super::attachment_bytes(image)?))
        .with_guessed_format()?
        .into_dimensions()
        .map_err(Into::into)
}
fn validate_claude_image_dimensions(
    attachments: &[super::PreparedAttachment],
    partner: bool,
) -> Result<()> {
    // All nested tool results and replayed turns count. Partner PDFs also
    // count toward many-image threshold, unlike direct Messages PDFs.
    let count = attachments
        .iter()
        .filter(|a| {
            a.kind == InputContentType::Image || (partner && a.kind == InputContentType::File)
        })
        .count();
    let maximum = if count > 20 { 2000 } else { 8000 };
    for image in attachments
        .iter()
        .filter(|a| a.kind == InputContentType::Image)
    {
        let (width, height) = image_dimensions(image)?;
        ensure!(
            width <= maximum && height <= maximum,
            "Claude image exceeds request-count-dependent dimensions ({maximum}px)"
        );
    }
    Ok(())
}
fn pdf_pages(attachments: &[super::PreparedAttachment]) -> Result<usize> {
    attachments
        .iter()
        .filter(|a| a.mime_type == "application/pdf")
        .try_fold(0usize, |total, a| {
            let document = lopdf::Document::load_mem(super::attachment_bytes(a)?)?;
            ensure!(!document.is_encrypted(), "PDF input must be decryptable");
            Ok(total.saturating_add(document.get_pages().len()))
        })
}

fn validate_endpoint(entry: &CatalogModel, provider: &str, kind: InputContentType) -> Result<()> {
    let provider = crate::definition::provider_definition(provider)
        .map(|d| d.name)
        .unwrap_or(provider);
    // Models routed to distinct official products do not inherit GenerateContent
    // or Chat media merely because their source lists multimodal inputs.
    // https://ai.google.dev/gemini-api/docs/live-guide
    // https://ai.google.dev/gemini-api/docs/deep-research
    // https://developers.openai.com/api/docs/guides/realtime
    ensure!(
        !(provider == "gemini"
            && (entry.id.contains("-live") || entry.id.starts_with("deep-research")))
            && !(provider == "openai" && entry.id.contains("realtime")),
        "model requires a separate live/realtime/interactions endpoint"
    );
    let supported = match provider {
        "anthropic" => entry.api == "anthropic-messages",
        "gemini" => entry.api == "google-generative-ai",
        "bedrock" => entry.api == "bedrock-converse-stream",
        // OpenRouter's Chat gateway routes these native model profiles itself.
        "openrouter" => matches!(
            entry.api.as_str(),
            "openai-completions" | "anthropic-messages"
        ),
        // Chat vision/PDF are documented on OpenAI and Azure; Responses catalog
        // membership alone does not establish Chat audio/realtime compatibility.
        "openai" if kind == InputContentType::Audio => entry.api == "openai-completions",
        "openai" | "azure-openai" => {
            matches!(kind, InputContentType::Image | InputContentType::File)
                && matches!(
                    entry.api.as_str(),
                    "openai-completions" | "openai-responses" | "azure-openai-responses"
                )
        }
        "copilot" => entry.api == "openai-completions",
        "ollama" => entry.api == "ollama-chat",
        "mistral" => {
            kind == InputContentType::Image
                && matches!(
                    entry.api.as_str(),
                    "openai-completions" | "mistral-conversations"
                )
        }
        "xai" => {
            kind == InputContentType::Image
                && matches!(
                    entry.api.as_str(),
                    "openai-completions" | "openai-responses"
                )
        }
        _ => entry.api == "openai-completions",
    };
    ensure!(
        supported,
        "model media metadata for API `{}` does not establish support on the selected `{provider}` endpoint",
        entry.api
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InputTypeSupport, MessageAttachment, ProviderInputCapabilities};
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde_json::json;

    fn catalog() -> ModelCatalog {
        ModelCatalog::parse(
            include_str!("../../tests/fixtures/capabilities/models.json"),
            include_str!("../../tests/fixtures/capabilities/provenance.json"),
        )
        .unwrap()
    }
    fn ceiling() -> ProviderCapabilities {
        ProviderCapabilities {
            input_types: ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport::native_inline_only(),
                image: InputTypeSupport::native_inline_only(),
                audio: InputTypeSupport::native_inline_only(),
                video: InputTypeSupport::native_inline_only(),
            },
            ..ProviderCapabilities::default()
        }
    }
    fn attachment(mime: &str) -> MessageAttachment {
        MessageAttachment {
            mime_type: mime.into(),
            name: None,
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data: STANDARD.encode([1, 2, 3, 4]),
            },
            artifact: None,
        }
    }
    fn validate(provider: &str, model: &str, part: MessageContentPart) -> Result<()> {
        validate_model_inputs(
            provider,
            model,
            &ceiling(),
            &[ChatMessage::user_parts(vec![part])],
            &catalog(),
        )
    }
    #[test]
    fn text_and_image_only_models_reject_audio_video_despite_adapter_override() {
        for model in ["text", "vision"] {
            for part in [
                MessageContentPart::audio(attachment("audio/wav")),
                MessageContentPart::video(attachment("video/mp4")),
            ] {
                assert!(
                    validate("openrouter", model, part)
                        .unwrap_err()
                        .root_cause()
                        .to_string()
                        .contains("does not support")
                );
            }
        }
    }
    #[test]
    fn unknown_model_and_unknown_modality_metadata_remain_distinct_from_unsupported() {
        assert!(
            validate(
                "groq",
                "missing",
                MessageContentPart::image(attachment("image/png"))
            )
            .unwrap_err()
            .root_cause()
            .to_string()
            .contains("unknown input capabilities")
        );
        assert!(
            validate(
                "groq",
                "unknown",
                MessageContentPart::image(attachment("image/png"))
            )
            .unwrap_err()
            .root_cause()
            .to_string()
            .contains("unknown input capabilities")
        );
        assert!(
            validate(
                "groq",
                "text",
                MessageContentPart::image(attachment("image/png"))
            )
            .unwrap_err()
            .root_cause()
            .to_string()
            .contains("does not support")
        );
    }
    #[test]
    fn model_adapter_and_endpoint_intersection_rejects_generic_media_and_responses_only_copilot() {
        for part in [
            MessageContentPart::audio(attachment("audio/wav")),
            MessageContentPart::video(attachment("video/mp4")),
            MessageContentPart::file(attachment("application/pdf")),
        ] {
            assert!(validate("groq", "media", part).is_err());
        }
        let message =
            ChatMessage::user_parts(vec![MessageContentPart::image(attachment("image/png"))]);
        assert!(
            validate_model_inputs(
                "groq",
                "vision",
                &ProviderCapabilities::default(),
                &[message],
                &catalog()
            )
            .is_err()
        );
        assert!(
            validate(
                "copilot",
                "vision",
                MessageContentPart::image(attachment("image/png"))
            )
            .unwrap_err()
            .root_cause()
            .to_string()
            .contains("selected")
        );
    }
    #[test]
    fn native_pdf_rule_is_vision_dependent_and_does_not_enable_text_only_files() {
        for provider in ["openai", "anthropic"] {
            assert!(
                validate(
                    provider,
                    "vision",
                    MessageContentPart::file(attachment("application/pdf"))
                )
                .is_ok()
            );
            assert!(
                validate(
                    provider,
                    "text",
                    MessageContentPart::file(attachment("application/pdf"))
                )
                .is_err()
            );
            assert!(
                validate(
                    provider,
                    "media",
                    MessageContentPart::file(attachment("text/plain"))
                )
                .is_err()
            );
        }
    }
    #[test]
    fn mime_roles_sources_and_declared_limits_are_validated_before_materialization() {
        for role in [Role::System, Role::Assistant] {
            assert!(
                validate_representation(
                    "groq",
                    InputContentType::Image,
                    role,
                    "image/png",
                    &attachment("image/png").source
                )
                .is_err()
            );
        }
        assert!(
            validate_representation(
                "gemini",
                InputContentType::Image,
                Role::User,
                "image/gif",
                &attachment("image/gif").source
            )
            .is_err()
        );
        assert!(
            validate_representation(
                "openai",
                InputContentType::Audio,
                Role::User,
                "audio/ogg",
                &attachment("audio/ogg").source
            )
            .is_err()
        );
        for source in [
            AttachmentDataSource::Reference {
                reference: "file-other-account".into(),
            },
            AttachmentDataSource::Reference {
                reference: "https://example.invalid/image.png".into(),
            },
            AttachmentDataSource::Reference {
                reference: "gs://other-project/media".into(),
            },
        ] {
            assert!(
                validate_representation(
                    "openai",
                    InputContentType::File,
                    Role::User,
                    "application/pdf",
                    &source
                )
                .unwrap_err()
                .root_cause()
                .to_string()
                .contains("ownership")
            );
        }
        let mut audio = attachment("audio/wav");
        audio.size_bytes = Some(5);
        assert!(
            validate("openrouter", "media", MessageContentPart::audio(audio))
                .unwrap_err()
                .root_cause()
                .to_string()
                .contains("maxBytes")
        );
        let mut audio = attachment("audio/wav");
        audio.source = AttachmentDataSource::Path {
            path: "/unread-path.wav".into(),
        };
        assert!(
            validate("openrouter", "media", MessageContentPart::audio(audio))
                .unwrap_err()
                .root_cause()
                .to_string()
                .contains("sources")
        );
        // Format constraints are evaluated on reconciled bytes, not a raw
        // declaration that the permitted normalization policy can correct.
        assert!(
            validate(
                "openrouter",
                "media",
                MessageContentPart::audio(attachment("audio/mp3"))
            )
            .is_ok()
        );
    }
    #[test]
    fn bytes_path_and_policy_controlled_url_are_materializable_not_opaque_references() {
        for source in [
            attachment("image/png").source,
            AttachmentDataSource::Path {
                path: "/image.png".into(),
            },
            AttachmentDataSource::Url {
                url: "https://example.invalid/image.png".into(),
            },
        ] {
            assert!(
                validate_representation(
                    "groq",
                    InputContentType::Image,
                    Role::User,
                    "image/png",
                    &source
                )
                .is_ok()
            );
        }
        // Url admission above still requires the existing security opt-in/domain
        // allowlist at resolve; no URL fetch is performed by this fixture.
        let mut image = attachment("image/png");
        image.source = AttachmentDataSource::Url {
            url: "https://example.invalid/image.png".into(),
        };
        assert!(
            super::super::prepare_messages_for_provider(
                "groq",
                &ceiling(),
                &[ChatMessage::user_parts(vec![MessageContentPart::image(
                    image
                )])]
            )
            .is_err()
        );
    }
    #[test]
    fn materialized_size_cannot_bypass_catalog_limit_with_missing_declared_size() {
        let mut file = attachment("audio/wav");
        file.source = AttachmentDataSource::Bytes {
            base64_data: STANDARD.encode([0u8; 5]),
        };
        let prepared = super::super::prepare_messages_for_provider(
            "mock",
            &ceiling(),
            &[ChatMessage::user_parts(vec![MessageContentPart::audio(
                file,
            )])],
        )
        .unwrap();
        assert!(
            validate_materialized_constraints(
                catalog().model("openrouter", "media").unwrap(),
                &prepared.attachments[0]
            )
            .unwrap_err()
            .root_cause()
            .to_string()
            .contains("maxBytes")
        );
    }
    #[test]
    fn duration_constraint_uses_media_timing_and_fails_closed_on_unknown_encoding() {
        let prepared = super::super::prepare_messages_for_provider(
            "mock",
            &ceiling(),
            &[ChatMessage::user_parts(vec![MessageContentPart::audio(
                attachment("audio/wav"),
            )])],
        )
        .unwrap();
        let mut entry = catalog().model("openrouter", "media").unwrap().clone();
        entry.metadata.insert(
            "inputConstraints".into(),
            json!({"audio":{"maxDurationMillis":1000}}),
        );
        assert!(validate_materialized_constraints(&entry, &prepared.attachments[0]).is_err());
        entry.metadata.insert(
            "inputConstraints".into(),
            json!({"audio":{"maxBytes":"untrusted"}}),
        );
        assert!(validate_materialized_constraints(&entry, &prepared.attachments[0]).is_err());
    }
    #[test]
    fn separate_live_realtime_and_interactions_products_do_not_inherit_chat_inputs() {
        let mut entry = catalog().model("gemini", "media").unwrap().clone();
        for id in ["gemini-fixture-live-preview", "deep-research-fixture"] {
            entry.id = id.into();
            assert!(validate_endpoint(&entry, "gemini", InputContentType::Audio).is_err());
        }
        let mut entry = catalog().model("openai", "vision").unwrap().clone();
        entry.id = "gpt-realtime-fixture".into();
        assert!(validate_endpoint(&entry, "openai", InputContentType::Image).is_err());
        let mut entry = catalog().model("openai", "media").unwrap().clone();
        assert!(validate_endpoint(&entry, "openai", InputContentType::Audio).is_err());
        entry.api = "openai-completions".into();
        assert!(validate_endpoint(&entry, "openai", InputContentType::Audio).is_ok());
    }
    #[test]
    fn explicit_modalities_win_over_file_attachment_flag_without_enabling_unknown_formats() {
        let mut entry = catalog().model("openrouter", "media").unwrap().clone();
        entry
            .metadata
            .insert("sourceMetadata".into(), json!({"attachment":false}));
        assert_eq!(
            entry.input_capability(InputContentType::Audio),
            InputCapabilityState::Supported
        );
        assert_eq!(
            entry.input_capability(InputContentType::Image),
            InputCapabilityState::Supported
        );
        assert!(
            validate_representation(
                "groq",
                InputContentType::Video,
                Role::User,
                "video/mp4",
                &attachment("video/mp4").source
            )
            .is_err()
        );
    }
    #[test]
    fn converse_document_requires_text_and_enforces_real_byte_limit() {
        let prepared = super::super::prepare_messages_for_provider(
            "mock",
            &ceiling(),
            &[ChatMessage::user_parts(vec![
                super::super::regression::part(
                    InputContentType::File,
                    "application/pdf",
                    &super::super::regression::pdf(1),
                ),
            ])],
        )
        .unwrap();
        assert!(
            validate_prepared("bedrock", &prepared.messages, &prepared.attachments)
                .unwrap_err()
                .root_cause()
                .to_string()
                .contains("sibling text")
        );
        let mut messages = prepared.messages;
        messages[0].content = "analyze".into();
        let mut files = prepared.attachments;
        files[0].size_bytes = 4_500_001;
        assert!(
            validate_prepared("bedrock", &messages, &files)
                .unwrap_err()
                .root_cause()
                .to_string()
                .contains("4.5 MB")
        );
    }
}

/// Controlled local diagnostics can cross endpoint redaction without exposing
/// a private host/path, external reference, model ID or raw provider response.
#[derive(Debug, Clone)]
pub(crate) struct MediaInputRejection(pub &'static str);
impl std::fmt::Display for MediaInputRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for MediaInputRejection {}
