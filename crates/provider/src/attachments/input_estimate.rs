//! Local media accounting after the existing attachment pipeline materializes
//! and verifies bytes. No inference, OCR, upload, or token-count API is used.
use super::{PreparedAttachment, attachment_bytes};
use crate::{AttachmentDataSource, ChatRequest, InputContentType, MessageContentPart};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{collections::BTreeMap, io::Cursor};

#[derive(Clone, Debug)]
pub struct MediaInputEstimate {
    pub message: usize,
    pub part: usize,
    pub input_tokens: u64,
}

pub struct PreparedInputBudget {
    pub request: ChatRequest,
    pub media: Vec<MediaInputEstimate>,
}

pub(crate) async fn prepare(
    provider: &str,
    capabilities: &crate::ProviderCapabilities,
    mut request: ChatRequest,
) -> Result<PreparedInputBudget> {
    if !request
        .messages
        .iter()
        .any(|message| message.has_attachments())
    {
        return Ok(PreparedInputBudget {
            request,
            media: vec![],
        });
    }
    let prepared =
        super::prepare_messages_for_provider_async(provider, capabilities, &request.messages)
            .await?;
    super::ensure_no_unrendered_attachments(provider, &prepared)?;
    let provider = provider.to_owned();
    // Header/document parsing and base64 encoding remain outside async workers
    // and outside every DB scope. The pipeline bounds both count and bytes.
    tokio::task::spawn_blocking(move || {
        let mut media = Vec::with_capacity(prepared.attachments.len());
        for attachment in prepared.attachments {
            let tokens = estimate(&provider, &request.model, &attachment)?;
            let bytes = attachment_bytes(&attachment)?;
            let part = request
                .messages
                .get_mut(attachment.message_index)
                .and_then(|message| message.content_parts.get_mut(attachment.part_index))
                .context("materialized media lost its request position")?;
            let target = match part {
                MessageContentPart::Image { image } => image,
                MessageContentPart::Audio { audio } => audio,
                MessageContentPart::Video { video } => video,
                MessageContentPart::File { file } => file,
                MessageContentPart::Text { .. } => {
                    anyhow::bail!("materialized media points to text")
                }
            };
            // Pin the bytes that were actually estimated. A later provider pass
            // can neither re-fetch an edited URL nor reopen a changed file.
            // Exact artifact identity is preserved for the existing upload cache.
            target.source = AttachmentDataSource::Bytes {
                base64_data: STANDARD.encode(bytes),
            };
            target.mime_type = attachment.mime_type;
            target.name = Some(attachment.name);
            target.size_bytes = Some(attachment.size_bytes as u64);
            target.sha256 = Some(attachment.sha256);
            media.push(MediaInputEstimate {
                message: attachment.message_index,
                part: attachment.part_index,
                input_tokens: tokens.max(1),
            });
        }
        Ok(PreparedInputBudget { request, media })
    })
    .await
    .context("media accounting worker failed")?
}

fn text_tokens(text: &str) -> u64 {
    tiktoken_rs::cl100k_base_singleton()
        .encode_with_special_tokens(text)
        .len() as u64
}

fn estimate(provider: &str, model: &str, attachment: &PreparedAttachment) -> Result<u64> {
    let bytes = attachment_bytes(attachment)?;
    ensure!(!bytes.is_empty(), "media input is empty");
    match attachment.kind {
        InputContentType::Image => {
            let (width, height) = image::ImageReader::new(Cursor::new(bytes))
                .with_guessed_format()?
                .into_dimensions()
                .context("image dimensions are unavailable")?;
            image_tokens(provider, model, width, height)
        }
        InputContentType::Audio => Ok(duration_millis(bytes, &attachment.mime_type)?
            .saturating_mul(50)
            .div_ceil(1000)),
        InputContentType::Video => Ok(duration_millis(bytes, &attachment.mime_type)?
            .saturating_mul(350)
            .div_ceil(1000)),
        InputContentType::File if attachment.mime_type == "application/pdf" => {
            let document = lopdf::Document::load_mem(bytes)
                .context("PDF structure is unavailable for input accounting")?;
            let pages = document.get_pages();
            ensure!(!pages.is_empty(), "PDF has no readable pages");
            // PDFs contribute both extracted text and a visual representation.
            // Missing text extraction is not OCR: page images remain budgeted.
            let mut total = 0_u64;
            for (number, page) in &pages {
                let text = document
                    .extract_text(&[*number])
                    .context("PDF text cannot be accounted for")?;
                let (width, height) = pdf_page_dimensions(&document, *page)?;
                total = total
                    .saturating_add(text_tokens(&text))
                    .saturating_add(image_tokens(provider, model, width, height)?);
            }
            Ok(total)
        }
        InputContentType::File | InputContentType::Text => {
            let text = std::str::from_utf8(bytes)
                .context("file input requires readable text or supported document metadata")?;
            Ok(text_tokens(text))
        }
    }
}

/// Use the actual inherited page box at 144 dpi. The baseline covers normal
/// document rasterization; unusual page sizes must not collapse to one tile.
/// This is a local estimate, not a promise about a provider's PDF renderer.
fn pdf_page_dimensions(
    document: &lopdf::Document,
    mut page: lopdf::ObjectId,
) -> Result<(u32, u32)> {
    for _ in 0..64 {
        let dictionary = document.get_dictionary(page)?;
        if let Ok(value) = dictionary.get(b"MediaBox") {
            let (_, value) = document.dereference(value)?;
            let values = value.as_array()?;
            ensure!(values.len() == 4, "PDF page box is malformed");
            let numbers = values
                .iter()
                .map(|value| value.as_float().map(f64::from))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let width = (numbers[2] - numbers[0]).abs() * 2.0;
            let height = (numbers[3] - numbers[1]).abs() * 2.0;
            ensure!(
                width.is_finite() && height.is_finite() && width > 0.0 && height > 0.0,
                "PDF page dimensions are unavailable"
            );
            return Ok((
                (width.ceil() as u32).max(1536),
                (height.ceil() as u32).max(2048),
            ));
        }
        page = dictionary.get(b"Parent")?.as_reference()?;
    }
    anyhow::bail!("PDF page inheritance exceeds the bounded depth")
}

/// Provider-specific local estimates, before the common N2 safety margin.
/// Sources: https://developers.openai.com/api/docs/guides/images-vision
/// https://platform.claude.com/docs/en/build-with-claude/vision
/// https://ai.google.dev/gemini-api/docs/tokens
fn image_tokens(provider: &str, model: &str, width: u32, height: u32) -> Result<u64> {
    ensure!(width > 0 && height > 0, "image has empty dimensions");
    let model = model.to_ascii_lowercase();
    let family = model.rsplit('/').next().unwrap_or(&model);
    if family.contains("claude") || provider == "anthropic" {
        // Taking the high-resolution tier also bounds unknown Claude aliases.
        return Ok(u64::from(width.div_ceil(28))
            .saturating_mul(u64::from(height.div_ceil(28)))
            .min(4784));
    }
    if family.contains("gemini") || provider == "gemini" {
        // Half-size tiles intentionally bound resolution-dependent subdivision.
        return Ok(u64::from(width.div_ceil(384))
            .saturating_mul(u64::from(height.div_ceil(384)))
            .saturating_mul(258));
    }
    let tile = if family.starts_with("gpt-4o-mini") {
        Some((2833_u64, 5667_u64))
    } else if family.starts_with("gpt-4o")
        || family == "gpt-4.1"
        || family.starts_with("gpt-4.1-20")
    {
        Some((85, 170))
    } else if family == "gpt-5" || family.starts_with("gpt-5-20") || family.starts_with("gpt-5.1") {
        Some((70, 140))
    } else if family.starts_with("o1") || family.starts_with("o3") {
        Some((75, 150))
    } else {
        None
    };
    if let Some((base, tile)) = tile {
        let scale = (2048.0 / f64::from(width.max(height)))
            .min(768.0 / f64::from(width.min(height)))
            .min(1.0);
        let w = (f64::from(width) * scale).ceil() as u64;
        let h = (f64::from(height) * scale).ceil() as u64;
        return Ok(base.saturating_add(
            w.div_ceil(512)
                .saturating_mul(h.div_ceil(512))
                .saturating_mul(tile),
        ));
    }
    let multiplier = if family.starts_with("gpt-4.1-nano") {
        246
    } else if family.starts_with("gpt-4.1-mini") {
        162
    } else if family.starts_with("o4-mini") {
        172
    } else if family.starts_with("gpt-5-nano") {
        150
    } else if family.starts_with("gpt-5") || family.starts_with("gpt-6") {
        120
    } else {
        400
    };
    // Count original patches without assuming an undocumented resizing cap.
    // Other vision models use this explicit local approximation; provider
    // overflow recovery remains necessary, as for text tokenization.
    Ok(u64::from(width.div_ceil(32))
        .saturating_mul(u64::from(height.div_ceil(32)))
        .saturating_mul(multiplier)
        .div_ceil(100))
}

fn duration_millis(bytes: &[u8], mime: &str) -> Result<u64> {
    if matches!(
        mime,
        "video/mp4" | "audio/mp4" | "video/quicktime" | "audio/x-m4a"
    ) {
        let context =
            mp4parse::read_mp4(&mut Cursor::new(bytes)).context("MP4 timing is unavailable")?;
        let mut duration = 0;
        for track in &context.tracks {
            let ticks = track
                .duration
                .context("MP4 track duration is unavailable")?;
            let scale = track
                .timescale
                .context("MP4 track time base is unavailable")?;
            ensure!(scale.0 > 0, "MP4 track time base is zero");
            duration = duration.max(
                ((u128::from(ticks.0) * 1000).div_ceil(u128::from(scale.0)))
                    .min(u128::from(u64::MAX)) as u64,
            );
        }
        ensure!(duration > 0, "MP4 duration is unavailable");
        return Ok(duration);
    }
    use symphonia::core::{
        formats::FormatOptions,
        io::{MediaSourceStream, MediaSourceStreamOptions},
        meta::MetadataOptions,
        probe::Hint,
    };
    let source = MediaSourceStream::new(
        Box::new(Cursor::new(bytes.to_vec())),
        MediaSourceStreamOptions::default(),
    );
    let mut hint = Hint::new();
    hint.mime_type(mime);
    let mut probe = symphonia::default::get_probe().format(
        &hint,
        source,
        &FormatOptions::default(),
        &MetadataOptions::default(),
    )?;
    let mut bases = BTreeMap::new();
    let mut duration = 0_u64;
    let mut all_known = !probe.format.tracks().is_empty();
    for track in probe.format.tracks() {
        let params = &track.codec_params;
        let base = params.time_base.or_else(|| {
            params
                .sample_rate
                .filter(|rate| *rate > 0)
                .map(|rate| symphonia::core::units::TimeBase::new(1, rate))
        });
        if let Some(base) = base {
            bases.insert(track.id, base);
            if let Some(frames) = params.n_frames {
                duration = duration.max(ticks_millis(frames.saturating_add(1), base));
            } else {
                all_known = false;
            }
        } else {
            anyhow::bail!("media track time base is unavailable");
        }
    }
    if !all_known {
        // Demux packet timestamps without decoding or transcribing audio/video.
        let mut complete = false;
        for _ in 0..1_000_000 {
            match probe.format.next_packet() {
                Ok(packet) => {
                    if let Some(base) = bases.get(&packet.track_id()) {
                        duration = duration.max(ticks_millis(
                            packet.ts.saturating_add(packet.dur).saturating_add(1),
                            *base,
                        ));
                    }
                }
                Err(symphonia::core::errors::Error::IoError(error))
                    if error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    complete = true;
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }
        ensure!(
            complete,
            "media timing scan exceeded its bounded packet quantum"
        );
    }
    ensure!(duration > 0, "media duration is unavailable");
    Ok(duration)
}
fn ticks_millis(ticks: u64, base: symphonia::core::units::TimeBase) -> u64 {
    (u128::from(ticks)
        .saturating_mul(u128::from(base.numer))
        .saturating_mul(1000)
        .div_ceil(u128::from(base.denom)))
    .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachments::{
        AttachmentTransportKind, AttachmentTransportPlan, PreparedAttachmentSource,
    };

    fn attachment(kind: InputContentType, mime: &str, bytes: Vec<u8>) -> PreparedAttachment {
        PreparedAttachment {
            message_index: 0,
            part_index: 0,
            kind,
            mime_type: mime.into(),
            name: "fixture".into(),
            size_bytes: bytes.len(),
            sha256: String::new(),
            source: PreparedAttachmentSource::Bytes,
            bytes: Some(bytes),
            artifact: None,
            transport_plan: AttachmentTransportPlan {
                kind: AttachmentTransportKind::Inline,
                reason: "fixture".into(),
            },
        }
    }
    fn png(width: u32, height: u32) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(width, height)
            .write_to(&mut output, image::ImageFormat::Png)
            .unwrap();
        output.into_inner()
    }
    fn wav() -> Vec<u8> {
        let length = 16_000_u32;
        let mut out = Vec::new();
        out.extend(b"RIFF");
        out.extend((36 + length).to_le_bytes());
        out.extend(b"WAVEfmt ");
        out.extend(16_u32.to_le_bytes());
        out.extend(1_u16.to_le_bytes());
        out.extend(1_u16.to_le_bytes());
        out.extend(8000_u32.to_le_bytes());
        out.extend(16000_u32.to_le_bytes());
        out.extend(2_u16.to_le_bytes());
        out.extend(16_u16.to_le_bytes());
        out.extend(b"data");
        out.extend(length.to_le_bytes());
        out.resize(44 + length as usize, 0);
        out
    }
    fn mp4(duration: u32) -> Vec<u8> {
        fn atom(name: &[u8; 4], bytes: &[u8]) -> Vec<u8> {
            let mut out = ((bytes.len() + 8) as u32).to_be_bytes().to_vec();
            out.extend(name);
            out.extend(bytes);
            out
        }
        let mut timing = vec![0; 12];
        timing.extend(1000_u32.to_be_bytes());
        timing.extend(duration.to_be_bytes());
        timing.extend([0; 4]);
        let track = atom(b"trak", &atom(b"mdia", &atom(b"mdhd", &timing)));
        atom(b"moov", &track)
    }
    #[test]
    fn input_estimate_uses_decoded_dimensions_and_container_time() {
        let small = attachment(InputContentType::Image, "image/png", png(512, 512));
        let big = attachment(InputContentType::Image, "image/png", png(1536, 1024));
        assert_eq!(estimate("openai", "gpt-4o", &small).unwrap(), 255);
        assert!(estimate("openai", "gpt-4o", &big).unwrap() > 255);
        let audio = attachment(InputContentType::Audio, "audio/wav", wav());
        assert_eq!(
            duration_millis(audio.bytes.as_ref().unwrap(), "audio/wav").unwrap(),
            1001
        );
        assert_eq!(estimate("gemini", "gemini-fixture", &audio).unwrap(), 51);
        let video = attachment(InputContentType::Video, "video/mp4", mp4(2500));
        assert_eq!(estimate("gemini", "gemini-fixture", &video).unwrap(), 875);
        assert!(duration_millis(&mp4(u32::MAX), "video/mp4").is_err());
        let mut mixed = mp4(2500);
        mixed.extend(&mp4(u32::MAX)[8..]);
        let length = (mixed.len() as u32).to_be_bytes();
        mixed[..4].copy_from_slice(&length);
        assert!(
            duration_millis(&mixed, "video/mp4").is_err(),
            "a known audio track must not hide an unknown video duration"
        );

        assert!(duration_millis(b"unreadable", "video/mp4").is_err());
        assert!(
            estimate(
                "openai",
                "gpt-4o",
                &attachment(InputContentType::Image, "image/png", vec![0; 20])
            )
            .is_err()
        );
    }
    #[test]
    fn input_estimate_text_is_unicode_and_unknown_input_is_not_zero() {
        let text = "История работы 🦀 ".repeat(30);
        assert_eq!(
            estimate(
                "fixture",
                "fixture",
                &attachment(
                    InputContentType::File,
                    "text/plain",
                    text.as_bytes().to_vec()
                )
            )
            .unwrap(),
            text_tokens(&text)
        );
        assert!(
            estimate(
                "fixture",
                "fixture",
                &attachment(
                    InputContentType::File,
                    "application/octet-stream",
                    vec![255; 20]
                )
            )
            .is_err()
        );
    }
    #[test]
    fn input_estimate_pdf_counts_every_page_and_extracted_text() {
        use lopdf::{Object, Stream, dictionary};
        let mut document = lopdf::Document::with_version("1.5");
        let pages_id = document.new_object_id();
        let font = document.add_object(
            dictionary! { "Type" => "Font", "Subtype" => "Type1", "BaseFont" => "Helvetica" },
        );
        let resources = document.add_object(dictionary! { "Font" => dictionary! { "F1" => font } });
        let content = document.add_object(Stream::new(
            dictionary! {},
            b"BT /F1 12 Tf (Preserve the evidence) Tj ET".to_vec(),
        ));
        let mut children = Vec::new();
        for _ in 0..2 {
            children.push(Object::Reference(document.add_object(
                dictionary! { "Type" => "Page", "Parent" => pages_id, "Contents" => content },
            )));
        }
        document.objects.insert(pages_id, dictionary! { "Type" => "Pages", "Kids" => children, "Count" => 2, "Resources" => resources, "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()] }.into());
        let catalog = document.add_object(dictionary! { "Type" => "Catalog", "Pages" => pages_id });
        document.trailer.set("Root", catalog);
        let mut bytes = Vec::new();
        document.save_to(&mut bytes).unwrap();
        let pages = attachment(InputContentType::File, "application/pdf", bytes);
        let total = estimate("openai", "gpt-4o", &pages).unwrap();
        assert!(total > 2 * image_tokens("openai", "gpt-4o", 1536, 2048).unwrap());
        assert!(
            estimate(
                "openai",
                "gpt-4o",
                &attachment(
                    InputContentType::File,
                    "application/pdf",
                    b"bad pdf".to_vec()
                )
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn input_estimate_pins_materialized_bytes_before_source_changes() {
        use crate::{
            ChatMessage, InputTypeSupport, MessageAttachment, ProviderCapabilities,
            ProviderInputCapabilities,
        };
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("image.png");
        let bytes = png(512, 512);
        std::fs::write(&path, &bytes).unwrap();
        let caps = ProviderCapabilities {
            streaming: true,
            vision: true,
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
        };
        let mut message = ChatMessage::user("inspect");
        message
            .content_parts
            .push(MessageContentPart::image(MessageAttachment::from_path(
                path.to_str().unwrap(),
                "image/png",
            )));
        let request = ChatRequest {
            model: "gpt-4o".into(),
            messages: vec![message],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        let prepared = super::super::runtime::with_async_authority_scope(
            "fixture-media-authority".into(),
            prepare("openai", &caps, request),
        )
        .await
        .unwrap();
        std::fs::write(&path, b"changed after accounting").unwrap();
        assert_eq!(prepared.media[0].input_tokens, 255);
        let MessageContentPart::Image { image } = &prepared.request.messages[0].content_parts[0]
        else {
            panic!("lost image")
        };
        let AttachmentDataSource::Bytes { base64_data } = &image.source else {
            panic!("unpinned image")
        };
        assert_eq!(STANDARD.decode(base64_data).unwrap(), bytes);
        let next = super::super::runtime::with_async_authority_scope(
            "fixture-media-authority".into(),
            super::super::prepare_messages_for_provider_async(
                "openai",
                &caps,
                &prepared.request.messages,
            ),
        )
        .await
        .unwrap();
        assert_eq!(attachment_bytes(&next.attachments[0]).unwrap(), bytes);
        assert_eq!(
            next.attachments[0].sha256,
            image.sha256.as_ref().unwrap().as_str()
        );
    }
}
