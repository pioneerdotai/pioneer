//! Unexecuted regression fixtures exercise the production async admission and
//! budget pipeline. Only docs/catalog inputs and HTTP delivery are fixtures.
use super::{
    admission::{self, AdmissionState},
    runtime,
};
use crate::{
    AttachmentDataSource, ChatMessage, ChatRequest, InputContentType, MessageAttachment,
    MessageContentPart, ProviderCapabilities,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{io::Cursor, sync::Arc};

pub(crate) fn image(format: image::ImageFormat, width: u32, height: u32) -> Vec<u8> {
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(width, height));
    let mut bytes = Cursor::new(Vec::new());
    image.write_to(&mut bytes, format).unwrap();
    bytes.into_inner()
}
pub(crate) fn pdf(pages: usize) -> Vec<u8> {
    use lopdf::{Object, Stream, dictionary};
    let mut d = lopdf::Document::with_version("1.5");
    let pages_id = d.new_object_id();
    let font =
        d.add_object(dictionary! {"Type"=>"Font","Subtype"=>"Type1","BaseFont"=>"Helvetica"});
    let resources = d.add_object(dictionary! {"Font"=>dictionary!{"F1"=>font}});
    let content = d.add_object(Stream::new(
        dictionary! {},
        b"BT /F1 12 Tf (Evidence) Tj ET".to_vec(),
    ));
    let mut children = Vec::new();
    for _ in 0..pages {
        children.push(Object::Reference(d.add_object(
            dictionary! {"Type"=>"Page","Parent"=>pages_id,"Contents"=>content},
        )));
    }
    d.objects.insert(pages_id,dictionary!{"Type"=>"Pages","Kids"=>children,"Count"=>pages as i64,"Resources"=>resources,"MediaBox"=>vec![0.into(),0.into(),612.into(),792.into()]}.into());
    let catalog = d.add_object(dictionary! {"Type"=>"Catalog","Pages"=>pages_id});
    d.trailer.set("Root", catalog);
    let mut bytes = vec![];
    d.save_to(&mut bytes).unwrap();
    bytes
}
pub(crate) fn wav() -> Vec<u8> {
    // Valid 16-bit mono PCM, 8000 Hz, one second; not a truncated magic header.
    wav_frames(8000, 8000)
}
pub(crate) fn wav_frames(frames: u32, rate: u32) -> Vec<u8> {
    let data = frames * 2;
    let mut b = Vec::new();
    b.extend(b"RIFF");
    b.extend((36 + data).to_le_bytes());
    b.extend(b"WAVEfmt ");
    b.extend(16u32.to_le_bytes());
    b.extend(1u16.to_le_bytes());
    b.extend(1u16.to_le_bytes());
    b.extend(rate.to_le_bytes());
    b.extend((rate * 2).to_le_bytes());
    b.extend(2u16.to_le_bytes());
    b.extend(16u16.to_le_bytes());
    b.extend(b"data");
    b.extend(data.to_le_bytes());
    b.resize(44 + data as usize, 0);
    b
}
pub(crate) fn mp3() -> &'static [u8] {
    include_bytes!("../../tests/fixtures/capabilities/opencode-yup-06.mp3")
}
pub(crate) fn video() -> Vec<u8> {
    // Keep the encoded samples and their offsets, but use the independently
    // covered no-edit timeline instead of the pinned asset's nonidentity edits.
    super::media_fixtures::encoded_video_mp4(None)
}
pub(crate) fn part(kind: InputContentType, mime: &str, bytes: &[u8]) -> MessageContentPart {
    let a = MessageAttachment {
        mime_type: mime.into(),
        name: None,
        size_bytes: None,
        sha256: None,
        source: AttachmentDataSource::Bytes {
            base64_data: STANDARD.encode(bytes),
        },
        artifact: None,
    };
    match kind {
        InputContentType::Image => MessageContentPart::image(a),
        InputContentType::File => MessageContentPart::file(a),
        InputContentType::Audio => MessageContentPart::audio(a),
        InputContentType::Video => MessageContentPart::video(a),
        InputContentType::Text => unreachable!(),
    }
}
pub(crate) fn attachment(part: &MessageContentPart) -> &MessageAttachment {
    match part {
        MessageContentPart::Image { image } => image,
        MessageContentPart::File { file } => file,
        MessageContentPart::Audio { audio } => audio,
        MessageContentPart::Video { video } => video,
        _ => panic!("expected attachment"),
    }
}
pub(crate) fn request(model: &str, parts: Vec<MessageContentPart>) -> ChatRequest {
    let mut message = ChatMessage::user_parts(parts);
    message.content = "analyze".into();
    ChatRequest {
        model: model.into(),
        messages: vec![message],
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    }
}
pub(crate) fn state(provider: &str, model: &str, constraints: Value) -> AdmissionState {
    let mut models: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/capabilities/models.json"
    ))
    .unwrap();
    let key = match provider {
        "gemini" => "google",
        "bedrock" => "amazon-bedrock",
        _ => provider,
    };
    if !models[key][model].is_null() {
        models[key][model]["inputConstraints"] = constraints;
    }
    if provider == "openai" && model == "media" {
        // These fixtures exercise the admitted Chat audio profile. The source
        // fixture's Responses membership does not establish Chat audio support.
        models[key][model]["api"] = json!("openai-completions");
    }
    let c = crate::catalog::ModelCatalog::parse(
        &serde_json::to_string(&models).unwrap(),
        include_str!("../../tests/fixtures/capabilities/provenance.json"),
    )
    .unwrap();
    AdmissionState::for_test(Arc::new(c))
}
pub(crate) async fn scoped<T>(
    state: Arc<AdmissionState>,
    operation: impl std::future::Future<Output = T>,
) -> T {
    // The tests use registered providers and the identical scope path as the
    // AuthorityBoundProvider. No unknown-provider test-double bypass applies.
    admission::scope(
        state,
        runtime::with_async_authority_scope("regression-authority".into(), operation),
    )
    .await
}
pub(crate) fn capabilities() -> ProviderCapabilities {
    use crate::{InputTypeSupport, ProviderInputCapabilities};
    ProviderCapabilities {
        input_types: ProviderInputCapabilities {
            text: true,
            image: InputTypeSupport::data_url_inline_only(),
            file: InputTypeSupport::native_inline_only(),
            audio: InputTypeSupport::native_inline_only(),
            video: InputTypeSupport::data_url_inline_only(),
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn effective_discovery_narrows_catalog_and_instance_evidence_does_not_leak() {
    let png = image(image::ImageFormat::Png, 1, 1);
    let caps = capabilities();
    let narrowed_state = Arc::new(state("groq", "vision", json!({})));
    let discovered=serde_json::from_value(json!({"id":"vision","provider":"groq","limits":{},"capabilities":{"input_modalities":["text"],"vision":true}})).unwrap();
    narrowed_state.replace_discovery(vec![discovered]);
    assert!(
        scoped(
            narrowed_state,
            super::input_estimate::prepare(
                "groq",
                &caps,
                request(
                    "vision",
                    vec![part(InputContentType::Image, "image/png", &png)]
                )
            )
        )
        .await
        .is_err()
    );
    let local = Arc::new(state("groq", "media", json!({})));
    local.replace_discovery(vec![serde_json::from_value(json!({"id":"deployment-one","provider":"custom","limits":{},"capabilities":{"input_modalities":["text","image"]}})).unwrap()]);
    let prepared = scoped(
        local.clone(),
        super::input_estimate::prepare(
            "custom",
            &caps,
            request(
                "deployment-one",
                vec![part(InputContentType::Image, "image/png", &png)],
            ),
        ),
    )
    .await
    .unwrap();
    assert_eq!(prepared.media.len(), 1);
    let other = Arc::new(state("groq", "media", json!({})));
    assert!(
        scoped(
            other,
            super::prepare_messages_for_provider_async(
                "custom",
                "deployment-one",
                &caps,
                &prepared.request.messages
            )
        )
        .await
        .is_err()
    );
    let unknown = Arc::new(state("groq", "media", json!({})));
    unknown.replace_discovery(vec![
        serde_json::from_value(
            json!({"id":"deployment-unknown","provider":"custom","limits":{},"capabilities":{}}),
        )
        .unwrap(),
    ]);
    assert!(
        scoped(
            unknown,
            super::input_estimate::prepare(
                "custom",
                &caps,
                request(
                    "deployment-unknown",
                    vec![part(InputContentType::Image, "image/png", &png)]
                )
            )
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn effective_mime_actual_bytes_aliases_and_strict_policy_reach_admission() {
    let caps = capabilities();
    let png = image(image::ImageFormat::Png, 1, 1);
    let jpeg = image(image::ImageFormat::Jpeg, 1, 1);
    let gif = image(image::ImageFormat::Gif, 1, 1);
    let s = Arc::new(state("groq", "vision", json!({})));
    let p = scoped(
        s.clone(),
        super::input_estimate::prepare(
            "groq",
            &caps,
            request(
                "vision",
                vec![part(InputContentType::Image, " IMAGE/PNG ", &png)],
            ),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        attachment(&p.request.messages[0].content_parts[0]).mime_type,
        "image/png"
    );
    let p = scoped(
        s,
        super::input_estimate::prepare(
            "groq",
            &caps,
            request(
                "vision",
                vec![part(InputContentType::Image, "image/png", &jpeg)],
            ),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        attachment(&p.request.messages[0].content_parts[0]).mime_type,
        "image/jpeg"
    );
    let narrowed = Arc::new(state(
        "groq",
        "vision",
        json!({"image":{"mimeTypes":["image/png"]}}),
    ));
    assert!(
        scoped(
            narrowed,
            super::input_estimate::prepare(
                "groq",
                &caps,
                request(
                    "vision",
                    vec![part(InputContentType::Image, "image/png", &jpeg)]
                )
            )
        )
        .await
        .is_err()
    );
    let gemini = Arc::new(state("gemini", "media", json!({})));
    assert!(
        scoped(
            gemini,
            super::input_estimate::prepare(
                "gemini",
                &caps,
                request(
                    "media",
                    vec![part(InputContentType::Image, "image/png", &gif)]
                )
            )
        )
        .await
        .is_err()
    );
    let mut strict = state("groq", "vision", json!({}));
    let mut config = super::default_attachment_pipeline_config();
    config.normalization.strict_mime_match = true;
    strict.pipeline_config = Some(config);
    assert!(
        scoped(
            Arc::new(strict),
            super::input_estimate::prepare(
                "groq",
                &caps,
                request(
                    "vision",
                    vec![part(InputContentType::Image, "image/png", &jpeg)]
                )
            )
        )
        .await
        .is_err()
    );
    for mime in ["audio/wav", " AUDIO/X-WAV "] {
        let s = Arc::new(state("openrouter", "media", json!({})));
        let p = scoped(
            s,
            super::input_estimate::prepare(
                "openrouter",
                &caps,
                request("media", vec![part(InputContentType::Audio, mime, &wav())]),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            attachment(&p.request.messages[0].content_parts[0]).mime_type,
            "audio/wav"
        );
    }
}

#[tokio::test]
async fn authorized_path_and_url_pins_survive_budget_renderer_and_replay_without_io() {
    use crate::providers::OpenAiCompatibleProvider;
    use crate::{InputTypeSupport, Provider};
    for ingress in ["path", "url"] {
        let bytes = image(image::ImageFormat::Png, 1, 1);
        let mut state = state("groq", "vision", json!({"image":{"sources":[ingress]}}));
        let mut config = super::default_attachment_pipeline_config();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), &bytes).unwrap();
        config.security.enforce_path_allowlist = true;
        config.security.allowed_path_roots = vec![file.path().parent().unwrap().into()];
        config.security.allow_url_sources = true;
        config.security.url_allowed_domains = vec!["example.com".into()];
        state.pipeline_config = Some(config);
        let url = "https://example.com/evidence.png";
        state
            .url_fixtures
            .write()
            .unwrap()
            .insert(url.into(), bytes.clone());
        let state = Arc::new(state);
        let mut req = request(
            "vision",
            vec![part(InputContentType::Image, "image/png", &bytes)],
        );
        if let MessageContentPart::Image { image } = &mut req.messages[0].content_parts[0] {
            image.source = if ingress == "path" {
                AttachmentDataSource::Path {
                    path: file.path().display().to_string(),
                }
            } else {
                AttachmentDataSource::Url { url: url.into() }
            };
        }
        let provider = OpenAiCompatibleProvider::new(
            "groq",
            "https://example.com/v1",
            "unused",
            crate::providers::AuthStyle::Bearer,
        )
        .with_input_capabilities(crate::ProviderInputCapabilities {
            text: true,
            image: InputTypeSupport::data_url_inline_only(),
            file: InputTypeSupport::disabled(),
            audio: InputTypeSupport::disabled(),
            video: InputTypeSupport::disabled(),
        });
        let budget = scoped(state.clone(), provider.prepare_input_budget(req))
            .await
            .unwrap();
        assert_eq!(budget.media.len(), 1);
        std::fs::write(file.path(), b"changed file").unwrap();
        state.url_fixtures.write().unwrap().clear();
        let expected = attachment(&budget.request.messages[0].content_parts[0])
            .sha256
            .clone();
        for stream in [false, true, false] {
            let wire = scoped(
                state.clone(),
                provider.render_chat_request_async_for_test(budget.request.clone(), stream),
            )
            .await
            .unwrap();
            assert_eq!(
                wire["messages"][0]["content"][1]["image_url"]["url"],
                format!("data:image/png;base64,{}", STANDARD.encode(&bytes))
            );
            let replay = scoped(
                state.clone(),
                provider.prepare_input_budget(budget.request.clone()),
            )
            .await
            .unwrap();
            assert_eq!(
                attachment(&replay.request.messages[0].content_parts[0]).sha256,
                expected
            );
        }
        assert_eq!(
            state.url_reads.load(std::sync::atomic::Ordering::Relaxed),
            usize::from(ingress == "url")
        );
        let other = Arc::new(self::state(
            "groq",
            "vision",
            json!({"image":{"sources":[ingress]}}),
        ));
        assert!(
            scoped(other, provider.prepare_input_budget(budget.request.clone()))
                .await
                .is_err()
        );
        let mut tampered = budget.request.clone();
        if let MessageContentPart::Image { image } = &mut tampered.messages[0].content_parts[0] {
            image.source = AttachmentDataSource::Bytes {
                base64_data: STANDARD.encode(self::image(image::ImageFormat::Png, 2, 1)),
            };
            image.sha256 = None;
            image.size_bytes = None;
        }
        assert!(
            scoped(state.clone(), provider.prepare_input_budget(tampered))
                .await
                .is_err()
        );
        let denied = request(
            "vision",
            vec![part(
                InputContentType::Image,
                "image/png",
                &self::image(image::ImageFormat::Png, 3, 1),
            )],
        );
        assert!(
            scoped(state, provider.prepare_input_budget(denied))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn native_limit_boundaries_use_all_history_and_actual_structure() {
    let caps = capabilities();
    for (provider, n, accepted) in [
        ("groq", 3, true),
        ("groq", 4, false),
        ("anthropic", 20, true),
        ("anthropic", 21, true),
    ] {
        let s = Arc::new(state(provider, "media", json!({})));
        let png = image(image::ImageFormat::Png, 1, 1);
        let req = request(
            "media",
            (0..n)
                .map(|_| part(InputContentType::Image, "image/png", &png))
                .collect(),
        );
        assert_eq!(
            scoped(s, super::input_estimate::prepare(provider, &caps, req))
                .await
                .is_ok(),
            accepted
        );
    }
    for (n, width, accepted) in [
        (20, 8000, true),
        (20, 8001, false),
        (21, 2000, true),
        (21, 2001, false),
    ] {
        let s = Arc::new(state("anthropic", "media", json!({})));
        let png = image(image::ImageFormat::Png, width, 1);
        let mut req = request(
            "media",
            vec![part(InputContentType::Image, "image/png", &png)],
        );
        // Resent earlier turns count too, not just the latest turn.
        req.messages = (0..n).map(|_| req.messages[0].clone()).collect();
        assert_eq!(
            scoped(s, super::input_estimate::prepare("anthropic", &caps, req))
                .await
                .is_ok(),
            accepted
        );
    }
    for (pages, accepted) in [(1000, true), (1001, false)] {
        let s = Arc::new(state("gemini", "media", json!({})));
        let req = request(
            "media",
            vec![part(InputContentType::File, "application/pdf", &pdf(pages))],
        );
        assert_eq!(
            scoped(s, super::input_estimate::prepare("gemini", &caps, req))
                .await
                .is_ok(),
            accepted
        );
    }
}

pub(crate) fn representation(
    provider: &str,
    kind: InputContentType,
    mime: &str,
) -> anyhow::Result<()> {
    super::contracts::validate_representation(
        provider,
        kind,
        crate::Role::User,
        mime,
        &crate::AttachmentDataSource::Bytes {
            base64_data: String::new(),
        },
    )
}

#[tokio::test]
async fn count_and_duration_boundary_constraints_are_native_admission_not_token_billing() {
    let caps = capabilities();
    let png = image(image::ImageFormat::Png, 1, 1);
    for (context, count, accepted) in [
        (200_000, 100, true),
        (200_000, 101, false),
        (1_000_000, 600, true),
        (1_000_000, 601, false),
    ] {
        let mut s = state("anthropic", "media", json!({}));
        let mut config = super::default_attachment_pipeline_config();
        config.max_attachments_per_request = 700;
        s.pipeline_config = Some(config);
        s.replace_discovery(vec![serde_json::from_value(json!({"id":"media","provider":"anthropic","limits":{"context_window":context},"capabilities":{}})).unwrap()]);
        let req = request(
            "media",
            (0..count)
                .map(|_| part(InputContentType::Image, "image/png", &png))
                .collect(),
        );
        assert_eq!(
            scoped(
                Arc::new(s),
                super::input_estimate::prepare("anthropic", &caps, req)
            )
            .await
            .is_ok(),
            accepted
        );
    }
    // Independent native boundary: 8000 frames / 8000 Hz = exactly 1000ms.
    assert_eq!(
        super::input_estimate::duration_millis(&wav(), "audio/wav").unwrap(),
        1000
    );
    for (limit, accepted) in [(1000, true), (999, false)] {
        let s = Arc::new(state(
            "openrouter",
            "media",
            json!({"audio":{"maxDurationMillis":limit}}),
        ));
        assert_eq!(
            scoped(
                s,
                super::input_estimate::prepare(
                    "openrouter",
                    &caps,
                    request(
                        "media",
                        vec![part(InputContentType::Audio, "audio/wav", &wav())]
                    )
                )
            )
            .await
            .is_ok(),
            accepted
        );
    }
    for (declared, actual, strict, accepted) in [
        (
            "audio/wav",
            super::media_fixtures::vbr_mp3(100),
            false,
            true,
        ),
        (
            "audio/wav",
            super::media_fixtures::vbr_mp3(100),
            true,
            false,
        ),
        ("audio/mp3", wav(), false, true),
        ("audio/mp3", wav(), true, false),
    ] {
        let mut s = state("openrouter", "media", json!({}));
        let mut config = super::default_attachment_pipeline_config();
        config.normalization.strict_mime_match = strict;
        s.pipeline_config = Some(config);
        let result = scoped(
            Arc::new(s),
            super::input_estimate::prepare(
                "openrouter",
                &caps,
                request(
                    "media",
                    vec![part(InputContentType::Audio, declared, &actual)],
                ),
            ),
        )
        .await;
        assert_eq!(result.is_ok(), accepted);
        if let Ok(p) = result {
            assert_eq!(
                attachment(&p.request.messages[0].content_parts[0]).mime_type,
                super::normalize::sniff_mime_from_bytes(&actual).unwrap()
            );
        }
    }
}

#[tokio::test]
async fn groq_payload_cap_counts_the_entire_normal_and_stream_json() {
    use crate::{InputTypeSupport, Provider};
    let provider = crate::providers::OpenAiCompatibleProvider::new(
        "groq",
        "https://example.com/v1",
        "unused",
        crate::providers::AuthStyle::Bearer,
    )
    .with_input_capabilities(crate::ProviderInputCapabilities {
        text: true,
        image: InputTypeSupport::data_url_inline_only(),
        file: InputTypeSupport::disabled(),
        audio: InputTypeSupport::disabled(),
        video: InputTypeSupport::disabled(),
    });
    let s = Arc::new(state("groq", "vision", json!({})));
    let base = request(
        "vision",
        vec![part(
            InputContentType::Image,
            "image/png",
            &image(image::ImageFormat::Png, 1, 1),
        )],
    );
    let budget = scoped(s.clone(), provider.prepare_input_budget(base))
        .await
        .unwrap();
    for stream in [false, true] {
        let wire = scoped(
            s.clone(),
            provider.render_chat_request_async_for_test(budget.request.clone(), stream),
        )
        .await
        .unwrap();
        let overhead = serde_json::to_vec(&wire).unwrap().len() - "analyze".len();
        for (size, accepted) in [
            (20_000_000 - overhead, true),
            (20_000_001 - overhead, false),
        ] {
            let mut req = budget.request.clone();
            req.messages[0].content = "a".repeat(size);
            assert_eq!(
                scoped(
                    s.clone(),
                    provider.render_chat_request_async_for_test(req, stream)
                )
                .await
                .is_ok(),
                accepted
            );
        }
    }
}

#[tokio::test]
async fn pinned_conflicts_accept_images_through_registered_async_model_admission() {
    let cases: Vec<Value> = serde_json::from_str(include_str!(
        "../../tests/fixtures/capabilities/attachment-conflicts.json"
    ))
    .unwrap();
    let original: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/capabilities/models.json"
    ))
    .unwrap();
    let origins: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/capabilities/provenance.json"
    ))
    .unwrap();
    let caps = capabilities();
    let png = image(image::ImageFormat::Png, 1, 1);
    for case in cases {
        let provider = match case["provider"].as_str().unwrap() {
            "amazon-bedrock" => "bedrock",
            "togetherai" => "together",
            other => other,
        };
        let key = if provider == "bedrock" {
            "amazon-bedrock"
        } else {
            provider
        };
        let id = case["id"].as_str().unwrap();
        let mut model = original["groq"]["vision"].clone();
        model["id"] = json!(id);
        model["provider"] = json!(key);
        model["api"] = json!(if provider == "bedrock" {
            "bedrock-converse-stream"
        } else {
            "openai-completions"
        });
        model["input"] = case["source"]["modalities"]["input"].clone();
        model["sourceMetadata"] = case["source"].clone();
        model["inputOrigin"] = json!({"kind":"source"});
        let c = crate::catalog::ModelCatalog::parse(
            &json!({key:{id:model}}).to_string(),
            &json!({key:{id:origins["groq"]["vision"]}}).to_string(),
        )
        .unwrap();
        let state = Arc::new(AdmissionState::for_test(Arc::new(c)));
        assert!(
            scoped(
                state,
                super::input_estimate::prepare(
                    provider,
                    &caps,
                    request(id, vec![part(InputContentType::Image, "image/png", &png)])
                )
            )
            .await
            .is_ok(),
            "{provider}/{id}"
        );
    }
}

#[tokio::test]
async fn effective_audio_mime_restriction_rejects_actual_mp3_despite_wav_declaration() {
    let caps = capabilities();
    let s = Arc::new(state(
        "openrouter",
        "media",
        json!({"audio":{"mimeTypes":["audio/wav"]}}),
    ));
    assert!(
        scoped(
            s,
            super::input_estimate::prepare(
                "openrouter",
                &caps,
                request(
                    "media",
                    vec![part(
                        InputContentType::Audio,
                        "audio/wav",
                        &super::media_fixtures::vbr_mp3(100)
                    )]
                )
            )
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn webm_actual_tracks_restrict_declarations_without_changing_kind_or_bytes() {
    use super::media_fixtures::webm;
    for provider in ["bedrock", "gemini", "openrouter"] {
        for (audio, video) in [(true, false), (false, true), (true, true), (false, false)] {
            let bytes = webm(audio, video, "webm");
            for (kind, mime) in [
                (InputContentType::Audio, "audio/webm"),
                (InputContentType::Video, "video/webm"),
            ] {
                for strict in [false, true] {
                    let mut s = state(provider, "media", json!({}));
                    let mut config = super::default_attachment_pipeline_config();
                    config.normalization.strict_mime_match = strict;
                    s.pipeline_config = Some(config);
                    let caps = capabilities();
                    let expected = if video {
                        kind == InputContentType::Video
                    } else {
                        audio && kind == InputContentType::Audio && provider == "bedrock"
                    };
                    let result = scoped(
                        Arc::new(s),
                        super::prepare_messages_for_provider_async(
                            provider,
                            "media",
                            &caps,
                            &request("media", vec![part(kind, mime, &bytes)]).messages,
                        ),
                    )
                    .await;
                    assert_eq!(
                        result.is_ok(),
                        expected,
                        "{provider} audio={audio} video={video} {mime} strict={strict}"
                    );
                    if let Ok(prepared) = result {
                        assert_eq!(prepared.attachments[0].mime_type, mime);
                        assert_eq!(
                            super::attachment_bytes(&prepared.attachments[0]).unwrap(),
                            bytes
                        );
                    }
                }
            }
        }
        for bytes in [
            webm(true, false, "matroska"),
            vec![0x1a, 0x45, 0xdf, 0xa3],
            webm(false, false, "webm"),
        ] {
            assert!(
                scoped(
                    Arc::new(state(provider, "media", json!({}))),
                    super::prepare_messages_for_provider_async(
                        provider,
                        "media",
                        &capabilities(),
                        &request(
                            "media",
                            vec![part(InputContentType::Video, "video/webm", &bytes)]
                        )
                        .messages
                    )
                )
                .await
                .is_err()
            );
        }
    }
}

#[tokio::test]
async fn native_duration_exact_metadata_and_packet_scan_boundaries_are_independent() {
    use super::{input_estimate::duration_millis, media_fixtures::webm};
    assert_eq!(duration_millis(&wav(), "audio/wav").unwrap(), 1000);
    // Packet scan has 50 x 20ms Opus frames and one 1000ms VP8 display span.
    for bytes in [
        webm(true, false, "webm"),
        webm(false, true, "webm"),
        webm(true, true, "webm"),
    ] {
        let mime = super::webm::actual_mime(&bytes).unwrap();
        assert_eq!(duration_millis(&bytes, mime).unwrap(), 1000);
        for limit in [999, 1000] {
            let kind = if mime.starts_with("audio") {
                InputContentType::Audio
            } else {
                InputContentType::Video
            };
            let key = admission::input_key(kind);
            let req = request("media", vec![part(kind, mime, &bytes)]);
            assert_eq!(
                scoped(
                    Arc::new(state(
                        "bedrock",
                        "media",
                        json!({key:{"maxDurationMillis":limit}})
                    )),
                    super::input_estimate::prepare("bedrock", &capabilities(), req)
                )
                .await
                .is_ok(),
                limit == 1000
            );
        }
    }
    // Actual one-sample overrun at 8kHz = 1000.125ms, rounded up to 1001ms.
    let mut over = wav();
    over.extend([0, 0]);
    let riff = (over.len() - 8) as u32;
    let data = (over.len() - 44) as u32;
    over[4..8].copy_from_slice(&riff.to_le_bytes());
    over[40..44].copy_from_slice(&data.to_le_bytes());
    assert_eq!(duration_millis(&over, "audio/wav").unwrap(), 1001);
    assert!(
        scoped(
            Arc::new(state(
                "openrouter",
                "media",
                json!({"audio":{"maxDurationMillis":1000}})
            )),
            super::input_estimate::prepare(
                "openrouter",
                &capabilities(),
                request(
                    "media",
                    vec![part(InputContentType::Audio, "audio/wav", &over)]
                )
            )
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn gemini_native_audio_duration_aggregates_spans_before_millisecond_rounding() {
    for (seconds, accepted) in [
        (vec![34200], true),
        (vec![17100, 17100], true),
        (vec![17100, 17101], false),
    ] {
        // 1 Hz PCM is a valid small WAVE fixture: independent exact frame count.
        let parts = seconds
            .iter()
            .map(|s| part(InputContentType::Audio, "audio/wav", &wav_frames(*s, 1)))
            .collect();
        let req = request("media", parts);
        let result = scoped(
            Arc::new(state("gemini", "media", json!({}))),
            super::prepare_messages_for_provider_async(
                "gemini",
                "media",
                &capabilities(),
                &req.messages,
            ),
        )
        .await;
        assert_eq!(result.is_ok(), accepted);
    }
    let half_ms = wav_frames(1, 2000);
    assert_eq!(
        super::input_estimate::duration_nanos(&half_ms, "audio/wav").unwrap(),
        500_000
    );
    assert_eq!(
        super::input_estimate::duration_millis(&half_ms, "audio/wav").unwrap(),
        1
    );
    // Exact metadata spans combine without per-input millisecond overhead.
    let sum = 2 * super::input_estimate::duration_nanos(&half_ms, "audio/wav").unwrap();
    assert_eq!(sum, 1_000_000);
}

#[tokio::test]
async fn webm_unknown_track_and_mime_only_constraint_fail_before_projection() {
    let mut unknown = super::media_fixtures::webm(true, false, "webm");
    // Change a valid TrackType integer from audio (2) to subtitle (17), without
    // changing the EBML structure or encoded packet bytes.
    let pos = unknown
        .windows(7)
        .position(|w| w == [0x83, 0x40, 0x04, 0, 0, 0, 2])
        .unwrap();
    unknown[pos + 6] = 17;
    for strict in [false, true] {
        let mut s = state(
            "bedrock",
            "media",
            json!({"video":{"mimeTypes":["video/webm"],"sources":["bytes"]}}),
        );
        let mut config = super::default_attachment_pipeline_config();
        config.normalization.strict_mime_match = strict;
        s.pipeline_config = Some(config);
        let s = Arc::new(s);
        for bytes in [
            unknown.clone(),
            super::media_fixtures::webm(true, false, "webm"),
        ] {
            let req = request(
                "media",
                vec![part(InputContentType::Video, "video/webm", &bytes)],
            );
            assert!(
                scoped(
                    s.clone(),
                    super::input_estimate::prepare("bedrock", &capabilities(), req)
                )
                .await
                .is_err()
            );
        }
    }
}

#[tokio::test]
async fn native_gemini_aggregate_keeps_exact_thirds_at_the_audio_boundary() {
    let mut parts = vec![part(
        InputContentType::Audio,
        "audio/wav",
        &wav_frames(34199, 1),
    )];
    for _ in 0..3 {
        parts.push(part(
            InputContentType::Audio,
            "audio/wav",
            &wav_frames(1, 3),
        ));
    }
    // 34199 seconds + 3*(1/3 second) = exactly 34200 seconds, independently.
    let req = request("media", parts.clone());
    assert!(
        scoped(
            Arc::new(state("gemini", "media", json!({}))),
            super::prepare_messages_for_provider_async(
                "gemini",
                "media",
                &capabilities(),
                &req.messages
            )
        )
        .await
        .is_ok()
    );
    parts.push(part(
        InputContentType::Audio,
        "audio/wav",
        &wav_frames(1, 3),
    ));
    let req = request("media", parts);
    assert!(
        scoped(
            Arc::new(state("gemini", "media", json!({}))),
            super::prepare_messages_for_provider_async(
                "gemini",
                "media",
                &capabilities(),
                &req.messages
            )
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn container_timeline_limits_and_end_overflow_reject_before_native_projection() {
    use super::media_fixtures::{TimingFixture, webm_timeline};
    use crate::Provider;
    for name in ["bedrock", "gemini", "openrouter"] {
        let provider: Arc<dyn Provider> = match name {
            "bedrock" => Arc::new(crate::providers::BedrockProvider::new(
                "unused",
                "unused",
                "us-east-1",
            )),
            "gemini" => Arc::new(crate::providers::GeminiProvider::new("unused")),
            _ => Arc::new(crate::providers::OpenRouterProvider::new("unused")),
        };
        for declared_duration in [None, Some(11000.0)] {
            let bytes = webm_timeline(
                true,
                true,
                "webm",
                TimingFixture {
                    video_start: 10000,
                    declared_duration,
                    ..Default::default()
                },
            );
            for (limit, allowed) in [(1000, false), (10999, false), (11000, true)] {
                let s = Arc::new(state(
                    name,
                    "media",
                    json!({"video":{"maxDurationMillis":limit}}),
                ));
                let req = request(
                    "media",
                    vec![part(InputContentType::Video, "video/webm", &bytes)],
                );
                let budget = scoped(s.clone(), provider.prepare_input_budget(req.clone())).await;
                assert_eq!(budget.is_ok(), allowed, "{name}, limit={limit}");
                for _stream in [false, true] {
                    let admission = scoped(
                        s.clone(),
                        super::prepare_messages_for_provider_async(
                            name,
                            "media",
                            &provider.capabilities(),
                            &req.messages,
                        ),
                    )
                    .await;
                    assert_eq!(admission.is_ok(), allowed);
                }
                if let Ok(budget) = budget {
                    assert_eq!(budget.media[0].input_tokens, 3850); // 11 seconds × local 350/sec heuristic
                    let replay = scoped(s, provider.prepare_input_budget(budget.request))
                        .await
                        .unwrap();
                    assert_eq!(replay.media[0].input_tokens, 3850);
                }
            }
        }
        for timing in [
            TimingFixture {
                cluster_timestamp: (i64::MAX - 1000) as u64,
                video_duration: 2000,
                ..Default::default()
            },
            TimingFixture {
                video_start: -1,
                ..Default::default()
            },
            TimingFixture {
                track_scale: 2.0,
                ..Default::default()
            },
        ] {
            let bytes = webm_timeline(false, true, "webm", timing);
            let s = Arc::new(state(
                name,
                "media",
                json!({"video":{"maxDurationMillis":1000}}),
            ));
            let req = request(
                "media",
                vec![part(InputContentType::Video, "video/webm", &bytes)],
            );
            assert!(
                scoped(s.clone(), provider.prepare_input_budget(req.clone()))
                    .await
                    .is_err()
            );
            for _stream in [false, true] {
                let error = scoped(
                    s.clone(),
                    super::prepare_messages_for_provider_async(
                        name,
                        "media",
                        &provider.capabilities(),
                        &req.messages,
                    ),
                )
                .await
                .unwrap_err();
                // anyhow's downcast traverses typed contexts; std Error::source
                // exposes the ContextError wrapper rather than its context type.
                assert!(error.downcast_ref::<super::MediaInputRejection>().is_some());
                assert!(!error.to_string().contains(&STANDARD.encode(&bytes)));
            }
        }
    }
}

#[tokio::test]
async fn confirmed_elementary_audio_limits_and_mp4_edit_admission() {
    use super::media_fixtures::{Mp4Edit, encoded_audio_mp4, encoded_video_mp4, vbr_adts, vbr_mp3};
    use crate::Provider;
    for name in ["openai", "bedrock", "gemini", "openrouter"] {
        let provider: Arc<dyn Provider> = match name {
            "openai" => Arc::new(crate::providers::OpenAiProvider::new("unused")),
            "bedrock" => Arc::new(crate::providers::BedrockProvider::new(
                "unused",
                "unused",
                "us-east-1",
            )),
            "gemini" => Arc::new(crate::providers::GeminiProvider::new("unused")),
            _ => Arc::new(crate::providers::OpenRouterProvider::new("unused")),
        };
        let mut cases = vec![
            (
                InputContentType::Audio,
                "audio/mpeg",
                vbr_mp3(100),
                2400,
                true,
            ),
            (
                InputContentType::Audio,
                "audio/mpeg",
                vbr_mp3(100),
                2399,
                false,
            ),
            (
                InputContentType::Audio,
                "audio/mpeg",
                vbr_mp3(101),
                2400,
                false,
            ),
        ];
        // 1152 samples/frame at 48000Hz: these independent bounds do not
        // consult the scanner. Ignored private bits leave coded lengths zero.
        for magic in [b"Info", b"Xing", b"VBRI"] {
            for at in [0, 37] {
                for (frames, limit, allowed) in
                    [(100, 2400, true), (100, 2399, false), (101, 2400, false)]
                {
                    cases.push((
                        InputContentType::Audio,
                        "audio/mpeg",
                        super::media_fixtures::ordinary_mp3_collision(frames, at, magic),
                        limit,
                        allowed,
                    ));
                }
            }
        }
        for magic in [b"Info", b"Xing"] {
            for (bytes, _samples, _rate, bound) in super::media_fixtures::confirmed_mp3_trims(magic)
            {
                for (limit, allowed) in [(bound, true), (bound - 1, false)] {
                    cases.push((
                        InputContentType::Audio,
                        "audio/mpeg",
                        bytes.clone(),
                        limit,
                        allowed,
                    ));
                }
            }
            // A real additional frame adds24ms, rather than changing a bound.
            cases.push((
                InputContentType::Audio,
                "audio/mpeg",
                super::media_fixtures::encoder_trim_mp3(magic, b"LAME3.100", 101, 576, 576),
                2376,
                false,
            ));
            for bytes in super::media_fixtures::unproven_mp3_trims(magic)
                .into_iter()
                .chain([super::media_fixtures::encoder_trim_mp3_mpeg2(
                    magic,
                    b"LAME3.100",
                    576,
                    1152,
                )])
            {
                for (limit, allowed) in [(2400, true), (2399, false)] {
                    cases.push((
                        InputContentType::Audio,
                        "audio/mpeg",
                        bytes.clone(),
                        limit,
                        allowed,
                    ));
                }
            }
        }
        cases.push((
            InputContentType::Audio,
            "audio/mpeg",
            mp3().to_vec(),
            60_000,
            true,
        ));
        for encoder in [b"Lavf62.11", b"Lavc62.11"] {
            let mut bytes =
                super::media_fixtures::encoder_trim_mp3(b"Info", encoder, 100, 576, 576);
            bytes[33 + 34] ^= 1;
            cases.push((
                InputContentType::Audio,
                "audio/mpeg",
                bytes.clone(),
                2400,
                true,
            ));
            cases.push((InputContentType::Audio, "audio/mpeg", bytes, 2399, false));
        }
        if name != "openai" {
            cases.extend([
                (
                    InputContentType::Audio,
                    "audio/aac",
                    vbr_adts(441),
                    10240,
                    true,
                ),
                (
                    InputContentType::Audio,
                    "audio/aac",
                    vbr_adts(441),
                    10239,
                    false,
                ),
                (
                    InputContentType::Audio,
                    "audio/aac",
                    vbr_adts(442),
                    10240,
                    false,
                ),
                (
                    InputContentType::Audio,
                    "audio/mp4",
                    encoded_audio_mp4(None),
                    10240,
                    true,
                ),
                (
                    InputContentType::Audio,
                    "audio/mp4",
                    encoded_audio_mp4(Some(&[Mp4Edit {
                        duration: 10240,
                        start: 0,
                        rate: 0x10000,
                    }])),
                    10240,
                    true,
                ),
                (
                    InputContentType::Audio,
                    "audio/mp4",
                    encoded_audio_mp4(None),
                    10239,
                    false,
                ),
            ]);
            for edits in [
                vec![
                    Mp4Edit {
                        duration: 1000,
                        start: 0,
                        rate: 0x10000
                    };
                    2
                ],
                vec![Mp4Edit {
                    duration: 1000,
                    start: 500,
                    rate: 0x10000,
                }],
                vec![Mp4Edit {
                    duration: 1000,
                    start: 0,
                    rate: 0x20000,
                }],
            ] {
                cases.push((
                    InputContentType::Audio,
                    "audio/mp4",
                    encoded_audio_mp4(Some(&edits)),
                    1000,
                    false,
                ));
                cases.push((
                    InputContentType::Video,
                    "video/mp4",
                    encoded_video_mp4(Some(&edits)),
                    1000,
                    false,
                ));
            }
        } else {
            // The timing prerequisite must not enable OpenAI MP4 or AAC Chat.
            cases.push((
                InputContentType::Audio,
                "audio/mp4",
                encoded_audio_mp4(None),
                10240,
                false,
            ));
            cases.push((
                InputContentType::Audio,
                "audio/aac",
                vbr_adts(441),
                10240,
                false,
            ));
        }
        for (kind, mime, bytes, limit, allowed) in cases {
            let key = if kind == InputContentType::Audio {
                "audio"
            } else {
                "video"
            };
            let s = Arc::new(state(
                name,
                "media",
                json!({key:{"maxDurationMillis":limit}}),
            ));
            let req = request("media", vec![part(kind, mime, &bytes)]);
            let budget = scoped(s.clone(), provider.prepare_input_budget(req.clone())).await;
            assert_eq!(budget.is_ok(), allowed, "{name} {mime} {limit}");
            let admission = scoped(
                s.clone(),
                super::prepare_messages_for_provider_async(
                    name,
                    "media",
                    &provider.capabilities(),
                    &req.messages,
                ),
            )
            .await;
            assert_eq!(admission.is_ok(), allowed);
            if let Err(error) = admission {
                assert!(error.downcast_ref::<super::MediaInputRejection>().is_some());
                assert!(!error.to_string().contains(&STANDARD.encode(&bytes)));
            }
            if let Ok(budget) = budget {
                let replay = scoped(s, provider.prepare_input_budget(budget.request))
                    .await
                    .unwrap();
                assert_eq!(replay.media.len(), 1);
            }
        }
        let mut bad_crc = super::media_fixtures::trimmed_xing_mp3(b"Info");
        bad_crc[33 + 10] ^= 1;
        for bytes in super::media_fixtures::rejected_mp3_tag_candidates()
            .into_iter()
            .chain([bad_crc])
            .chain(super::media_fixtures::invalid_mp3_trims(b"Info"))
            .chain(super::media_fixtures::invalid_mp3_trims(b"Xing"))
        {
            let req = request(
                "media",
                vec![part(InputContentType::Audio, "audio/mpeg", &bytes)],
            );
            let s = Arc::new(state(name, "media", json!({})));
            assert!(
                scoped(s.clone(), provider.prepare_input_budget(req.clone()))
                    .await
                    .is_err()
            );
            let error = scoped(
                s,
                super::prepare_messages_for_provider_async(
                    name,
                    "media",
                    &provider.capabilities(),
                    &req.messages,
                ),
            )
            .await
            .unwrap_err();
            assert!(error.downcast_ref::<super::MediaInputRejection>().is_some());
            assert!(!error.to_string().contains(&STANDARD.encode(&bytes)));
        }
        for mime in ["audio/mpeg", "audio/aac"] {
            if mime == "audio/aac" && name == "openai" {
                continue;
            }
            let mut truncated = if mime == "audio/mpeg" {
                vbr_mp3(100)
            } else {
                vbr_adts(441)
            };
            truncated.pop();
            let req = request(
                "media",
                vec![part(InputContentType::Audio, mime, &truncated)],
            );
            assert!(
                scoped(
                    Arc::new(state(name, "media", json!({}))),
                    provider.prepare_input_budget(req)
                )
                .await
                .is_err()
            );
        }
    }
}

#[tokio::test]
async fn gemini_aggregate_uses_confirmed_vbr_frames_not_bitrate_estimates() {
    use super::media_fixtures::{vbr_adts, vbr_mp3};
    use crate::Provider;
    let provider = crate::providers::GeminiProvider::new("unused");
    // 34187.36 + 2.4 + 10.24 =34200 seconds exactly, independently.
    // 34187.36*100 is an integral PCM sample count at 100Hz.
    for (mp3_frames, allowed) in [(100, true), (101, false)] {
        let req = request(
            "media",
            vec![
                part(
                    InputContentType::Audio,
                    "audio/wav",
                    &wav_frames(3_418_736, 100),
                ),
                part(InputContentType::Audio, "audio/mpeg", &vbr_mp3(mp3_frames)),
                part(InputContentType::Audio, "audio/aac", &vbr_adts(441)),
            ],
        );
        let s = Arc::new(state("gemini", "media", json!({})));
        assert_eq!(
            scoped(s.clone(), provider.prepare_input_budget(req.clone()))
                .await
                .is_ok(),
            allowed
        );
        assert_eq!(
            scoped(
                s,
                super::prepare_messages_for_provider_async(
                    "gemini",
                    "media",
                    &provider.capabilities(),
                    &req.messages
                )
            )
            .await
            .is_ok(),
            allowed
        );
    }
}

pub(crate) fn confirmed_wire_inputs(
    provider: &str,
) -> Vec<(InputContentType, &'static str, Vec<u8>)> {
    use super::media_fixtures::{encoded_audio_mp4, encoded_video_mp4, vbr_adts, vbr_mp3};
    let mut cases = vec![(InputContentType::Audio, "audio/mpeg", vbr_mp3(100))];
    // All four native builder tests consume this collection after production
    // async budget/admission and replay, comparing the complete encoded bytes.
    for magic in [b"Info", b"Xing", b"VBRI"] {
        for at in [0, 37] {
            cases.push((
                InputContentType::Audio,
                "audio/mpeg",
                super::media_fixtures::ordinary_mp3_collision(100, at, magic),
            ));
        }
    }
    for magic in [b"Info", b"Xing"] {
        let mut bytes = super::media_fixtures::xing_mp3(100, 100);
        bytes[21..25].copy_from_slice(magic);
        cases.push((InputContentType::Audio, "audio/mpeg", bytes));
        for (bytes, _samples, _rate, _millis) in super::media_fixtures::confirmed_mp3_trims(magic) {
            cases.push((InputContentType::Audio, "audio/mpeg", bytes));
        }
    }
    if provider != "openai" {
        cases.extend([
            (InputContentType::Audio, "audio/aac", vbr_adts(441)),
            (
                InputContentType::Audio,
                "audio/mp4",
                encoded_audio_mp4(None),
            ),
            (
                InputContentType::Audio,
                "audio/mp4",
                encoded_audio_mp4(Some(&[super::media_fixtures::Mp4Edit {
                    duration: 10240,
                    start: 0,
                    rate: 0x10000,
                }])),
            ),
            (
                InputContentType::Video,
                "video/mp4",
                encoded_video_mp4(None),
            ),
        ]);
    }
    cases
}

#[tokio::test]
async fn gemini_aggregate_counts_ordinary_ancillary_collisions_as_audio() {
    use crate::Provider;
    let provider = crate::providers::GeminiProvider::new("unused");
    // Independent PCM + frame clock: 34197.6s + 100*1152/48000s =34200s.
    // One additional ordinary frame exceeds the native bound by exactly24ms.
    for magic in [b"Info", b"Xing", b"VBRI"] {
        for at in [0, 37] {
            for (frames, allowed) in [(100, true), (101, false)] {
                let bytes = super::media_fixtures::ordinary_mp3_collision(frames, at, magic);
                let req = request(
                    "media",
                    vec![
                        part(
                            InputContentType::Audio,
                            "audio/wav",
                            &wav_frames(3_419_760, 100),
                        ),
                        part(InputContentType::Audio, "audio/mpeg", &bytes),
                    ],
                );
                let s = Arc::new(state("gemini", "media", json!({})));
                assert_eq!(
                    scoped(s.clone(), provider.prepare_input_budget(req.clone()))
                        .await
                        .is_ok(),
                    allowed
                );
                assert_eq!(
                    scoped(
                        s,
                        super::prepare_messages_for_provider_async(
                            "gemini",
                            "media",
                            &provider.capabilities(),
                            &req.messages
                        )
                    )
                    .await
                    .is_ok(),
                    allowed
                );
            }
        }
    }
}

#[tokio::test]
async fn gemini_aggregate_uses_confirmed_trim_or_scanned_upper_bound() {
    use crate::Provider;
    let provider = crate::providers::GeminiProvider::new("unused");
    for magic in [b"Info", b"Xing"] {
        for encoder in [b"LAME3.100", b"Lavf62.11", b"Lavc62.11"] {
            // Independent exact span: 4_274_703 PCM samples/125Hz=34197.624s;
            // (100*1152 -576-576)/48000=2.376s; sum34200s. +1frame=+24ms.
            for (frames, allowed) in [(100, true), (101, false)] {
                let bytes =
                    super::media_fixtures::encoder_trim_mp3(magic, encoder, frames, 576, 576);
                let req = request(
                    "media",
                    vec![
                        part(
                            InputContentType::Audio,
                            "audio/wav",
                            &wav_frames(4_274_703, 125),
                        ),
                        part(InputContentType::Audio, "audio/mpeg", &bytes),
                    ],
                );
                let s = Arc::new(state("gemini", "media", json!({})));
                assert_eq!(
                    scoped(s.clone(), provider.prepare_input_budget(req.clone()))
                        .await
                        .is_ok(),
                    allowed
                );
                assert_eq!(
                    scoped(
                        s,
                        super::prepare_messages_for_provider_async(
                            "gemini",
                            "media",
                            &provider.capabilities(),
                            &req.messages
                        )
                    )
                    .await
                    .is_ok(),
                    allowed
                );
            }
            for padding in [0, 200, 528] {
                let bytes =
                    super::media_fixtures::encoder_trim_mp3(magic, encoder, 100, 100, padding);
                // Unproven trim retains all 100*1152/48000=2.4s of audio.
                // 4_274_700/125 +2.4 =34200s; one PCM sample exceeds the
                // aggregate limit by 8ms. Trusting the trim would admit both.
                for (pcm_frames, allowed) in [(4_274_700, true), (4_274_701, false)] {
                    let req = request(
                        "media",
                        vec![
                            part(
                                InputContentType::Audio,
                                "audio/wav",
                                &wav_frames(pcm_frames, 125),
                            ),
                            part(InputContentType::Audio, "audio/mpeg", &bytes),
                        ],
                    );
                    let s = Arc::new(state("gemini", "media", json!({})));
                    assert_eq!(
                        scoped(s.clone(), provider.prepare_input_budget(req.clone()))
                            .await
                            .is_ok(),
                        allowed,
                        "budget: {magic:?}, {encoder:?}, padding={padding}, pcm={pcm_frames}"
                    );
                    let admission = scoped(
                        s,
                        super::prepare_messages_for_provider_async(
                            "gemini",
                            "media",
                            &provider.capabilities(),
                            &req.messages,
                        ),
                    )
                    .await;
                    assert_eq!(
                        admission.is_ok(),
                        allowed,
                        "admission: {magic:?}, {encoder:?}, padding={padding}, pcm={pcm_frames}"
                    );
                    if !allowed {
                        let error = admission.unwrap_err();
                        assert!(error.downcast_ref::<super::MediaInputRejection>().is_some());
                        assert!(!error.to_string().contains(&STANDARD.encode(&bytes)));
                    }
                }
            }
        }
    }
}
