//! Picker-only purpose policy. Discovery owns availability; this module never
//! adds catalog rows or changes admission/execution of a saved model.
use crate::{
    ProviderCapabilities,
    catalog::{CatalogModel, ModelCatalog},
};
use pioneer_protocol::{ProviderModelCapabilities, ProviderModelInfo};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelPurpose {
    Chat,
    Embeddings,
    Transcription,
}

/// `None` is unknown, including absent/null/malformed source values. An
/// explicit negative is final; the catalog fills only missing evidence.
fn prefer_native(native: Option<bool>, catalog: impl FnOnce() -> Option<bool>) -> Option<bool> {
    native.or_else(catalog)
}

fn contains(values: &[String], marker: &str) -> bool {
    values
        .iter()
        .any(|value| value.eq_ignore_ascii_case(marker))
}

fn strings(value: &Value) -> Option<Vec<String>> {
    value
        .as_array()?
        .iter()
        .map(|v| v.as_str().map(str::to_owned))
        .collect()
}

fn embedding_output_support(output: &[String]) -> Option<bool> {
    if contains(output, "embeddings") || contains(output, "embedding") {
        return Some(true);
    }
    // Only a recognized closed modality list proves absence. A future value
    // does not become a negative operation fact.
    output
        .iter()
        .all(|kind| {
            matches!(
                kind.to_ascii_lowercase().as_str(),
                "text" | "image" | "audio" | "video"
            )
        })
        .then_some(false)
}

fn source_chat_api(adapter_api: &str) -> Option<&'static str> {
    match adapter_api {
        "chat_completions" => Some("chat-completions"),
        "responses" => Some("responses"),
        "messages" => Some("messages"),
        "generate_content" => Some("generate-content"),
        "bedrock_converse" => Some("converse"),
        // Deployment-scoped Azure and native Ollama require their own route
        // evidence; a generic public OpenAI API declaration does not attest it.
        _ => None,
    }
}

fn text_chat_modalities(
    native: &ProviderModelCapabilities,
    entry: Option<&CatalogModel>,
) -> Option<bool> {
    let source = entry.and_then(|entry| entry.metadata.get("sourceMetadata"));
    let input = native
        .input_modalities
        .clone()
        .or_else(|| strings(&source?["modalities"]["input"]))?;
    let output = native
        .output_modalities
        .clone()
        .or_else(|| strings(&source?["modalities"]["output"]))?;
    Some(contains(&input, "text") && contains(&output, "text"))
}

fn catalog_support(
    purpose: ModelPurpose,
    adapter_api: &str,
    native: &ProviderModelCapabilities,
    entry: &CatalogModel,
) -> Option<bool> {
    match purpose {
        ModelPurpose::Chat => {
            let source = entry.metadata.get("sourceMetadata")?.as_object()?;
            // The existing models.dev v1 source defaults an omitted type to
            // chat and has no per-model API map. Apply that source contract
            // only to rows carrying its existing provenance, not arbitrary
            // metadata or the catalog's generated provider-wide API default.
            let models_dev = entry
                .metadata
                .get("pricingSource")
                .and_then(|s| s["url"].as_str())
                == Some("https://models.dev/api.json");
            let chat = match source.get("type") {
                None => models_dev,
                Some(kind) => kind.as_str() == Some("chat"),
            };
            if !chat {
                return None;
            }
            if let Some(api) = source.get("api") {
                if !api
                    .as_object()?
                    .get(source_chat_api(adapter_api)?)
                    .is_some_and(Value::is_object)
                {
                    return None;
                }
            } else if !models_dev {
                return None;
            }
            text_chat_modalities(native, Some(entry))
        }
        ModelPurpose::Embeddings => {
            let source = entry.metadata.get("sourceMetadata")?;
            let output = native
                .output_modalities
                .clone()
                .or_else(|| strings(&source["modalities"]["output"]))?;
            // A vector output identifies this operation, unlike text output.
            embedding_output_support(&output)
        }
        // Current catalog sources have no operation-specific transcription
        // evidence. Audio input + text output can also describe chat.
        ModelPurpose::Transcription => None,
    }
}

pub(crate) fn filter_models(
    provider: &str,
    adapter_api: &str,
    capabilities: &ProviderCapabilities,
    purpose: ModelPurpose,
    mut models: Vec<ProviderModelInfo>,
    catalog: Option<&ModelCatalog>,
) -> Vec<ProviderModelInfo> {
    let adapter_supports = match purpose {
        ModelPurpose::Chat => true,
        ModelPurpose::Embeddings => capabilities.embeddings,
        ModelPurpose::Transcription => capabilities.transcription,
    };
    models.retain(|model| {
        let native = match purpose {
            ModelPurpose::Chat => {
                model
                    .capabilities
                    .chat
                    .or_else(|| match model.capabilities.chat_operation? {
                        false => Some(false),
                        true => text_chat_modalities(
                            &model.capabilities,
                            catalog.and_then(|catalog| catalog.model(provider, &model.id)),
                        ),
                    })
            }
            ModelPurpose::Embeddings => model.capabilities.embeddings.or_else(|| {
                embedding_output_support(model.capabilities.output_modalities.as_ref()?)
            }),
            ModelPurpose::Transcription => model.capabilities.transcription,
        };
        adapter_supports
            && prefer_native(native, || {
                catalog_support(
                    purpose,
                    adapter_api,
                    &model.capabilities,
                    catalog?.model(provider, &model.id)?,
                )
            }) == Some(true)
    });
    models
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn model(
        id: &str,
        chat: Option<bool>,
        embeddings: Option<bool>,
        transcription: Option<bool>,
    ) -> ProviderModelInfo {
        let mut model: ProviderModelInfo = serde_json::from_value(json!({
            "id":id, "provider":"openai", "limits":{}, "capabilities":{}
        }))
        .unwrap();
        model.capabilities.chat = chat;
        model.capabilities.embeddings = embeddings;
        model.capabilities.transcription = transcription;
        model
    }

    fn test_catalog(source: Value) -> ModelCatalog {
        ModelCatalog::parse(
            &json!({"openai":{"listed":{
                "id":"listed", "name":"Listed", "provider":"openai",
                "api":"openai-responses", "baseUrl":"https://api.openai.com/v1",
                "contextWindow":4096, "maxTokens":1024, "reasoning":false,
                "input":["text"], "output":["text"], "cost":{},
                "sourceMetadata":source
            }}})
            .to_string(),
            &json!({"openai":{"listed":{
                "contextWindow":{"kind":"source", "expression":"fixture"},
                "maxTokens":{"kind":"source", "expression":"fixture"}
            }}})
            .to_string(),
        )
        .unwrap()
    }

    fn chat_source() -> Value {
        json!({"type":"chat", "api":{"responses":{}},
            "modalities":{"input":["text"], "output":["text"]}})
    }

    fn select(
        purpose: ModelPurpose,
        models: Vec<ProviderModelInfo>,
        catalog: Option<&ModelCatalog>,
    ) -> Vec<String> {
        let capabilities = ProviderCapabilities {
            embeddings: true,
            transcription: true,
            ..Default::default()
        };
        filter_models(
            "openai",
            "responses",
            &capabilities,
            purpose,
            models,
            catalog,
        )
        .into_iter()
        .map(|m| m.id)
        .collect()
    }

    #[test]
    fn models_dev_v1_catalog_rows_without_type_or_api_are_selectable() {
        use crate::catalog::generator::{SourceSnapshot, generate};
        let snapshot: SourceSnapshot =
            serde_json::from_str(include_str!("../tests/fixtures/catalog/sources.json")).unwrap();
        let generated = generate(&snapshot, false).unwrap();
        let catalog_from = |models: &Value| {
            ModelCatalog::parse(
                &models.to_string(),
                &serde_json::to_string(&generated.provenance).unwrap(),
            )
            .unwrap()
        };
        let models = serde_json::to_value(&generated.models).unwrap();
        let catalog = catalog_from(&models);
        for (provider, id, api) in [
            ("openai", "gpt-5-nano", "responses"),
            ("deepseek", "deepseek-v4-flash", "chat_completions"),
            ("groq", "llama-3.3-70b-versatile", "chat_completions"),
            ("gemini", "gemini-2.5-flash", "generate_content"),
            ("anthropic", "claude-sonnet-4-6", "messages"),
        ] {
            let entry = catalog.model(provider, id).unwrap();
            let source = &entry.metadata["sourceMetadata"];
            assert!(source.get("type").is_none());
            assert!(source.get("api").is_none());
            let mut candidate = model(id, None, None, None);
            candidate.provider = provider.to_owned();
            let selected = filter_models(
                provider,
                api,
                &ProviderCapabilities::default(),
                ModelPurpose::Chat,
                vec![candidate.clone()],
                Some(&catalog),
            );
            assert_eq!(
                selected.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
                [id]
            );
            candidate.capabilities.chat = Some(false);
            assert!(
                filter_models(
                    provider,
                    api,
                    &ProviderCapabilities::default(),
                    ModelPurpose::Chat,
                    vec![candidate],
                    Some(&catalog),
                )
                .is_empty()
            );
        }
        // Omitted type means chat only under the actual source contract.
        // Explicit other operations, malformed fields and missing text I/O
        // must not be admitted merely because the model has a catalog row.
        for (field, value) in [
            ("type", json!("image")),
            ("type", json!("embedding")),
            ("type", json!("transcription")),
            ("type", json!("realtime")),
            ("type", Value::Null),
            ("api", Value::Null),
            ("api", json!({"realtime":{}})),
            ("modalities", json!({"input":["audio"],"output":["text"]})),
            ("modalities", json!({"input":["text"],"output":["image"]})),
            ("modalities", Value::Null),
        ] {
            let mut changed = models.clone();
            changed["openai"]["gpt-5-nano"]["sourceMetadata"][field] = value;
            assert!(
                select(
                    ModelPurpose::Chat,
                    vec![model("gpt-5-nano", None, None, None)],
                    Some(&catalog_from(&changed)),
                )
                .is_empty(),
                "{field}"
            );
        }
        let candidate = model("gpt-5-nano", None, None, None);
        assert!(select(ModelPurpose::Chat, vec![candidate], None).is_empty());
        for source in [Value::Null, json!("malformed source metadata")] {
            let mut changed = models.clone();
            changed["openai"]["gpt-5-nano"]["sourceMetadata"] = source;
            let mut candidate = model("gpt-5-nano", None, None, None);
            candidate.capabilities.input_modalities = Some(vec!["text".into()]);
            candidate.capabilities.output_modalities = Some(vec!["text".into()]);
            assert!(
                select(
                    ModelPurpose::Chat,
                    vec![candidate],
                    Some(&catalog_from(&changed)),
                )
                .is_empty()
            );
        }
        assert!(
            select(
                ModelPurpose::Chat,
                vec![model("not-in-catalog", None, None, None)],
                Some(&catalog),
            )
            .is_empty()
        );
        let mut unknown_source = models;
        unknown_source["openai"]["gpt-5-nano"]["pricingSource"]["url"] =
            json!("https://example.invalid/models");
        assert!(
            select(
                ModelPurpose::Chat,
                vec![model("gpt-5-nano", None, None, None)],
                Some(&catalog_from(&unknown_source)),
            )
            .is_empty()
        );
    }

    #[test]
    fn native_chat_operation_combines_with_real_models_dev_modalities() {
        let sources: Value =
            serde_json::from_str(include_str!("../tests/fixtures/catalog/sources.json")).unwrap();
        for id in [
            "gemini-2.5-flash",
            "gemini-2.5-pro",
            "gemini-3-flash-preview",
        ] {
            let source =
                sources["sources"]["https://models.dev/api.json"]["body"]["google"]["models"][id]
                    .clone();
            assert!(source.is_object());
            assert!(source.get("type").is_none());
            assert!(source.get("api").is_none());
            let catalog = test_catalog(source.clone());
            let mut candidate = model("listed", None, None, None);
            candidate.capabilities.chat_operation = Some(true);
            assert_eq!(
                select(ModelPurpose::Chat, vec![candidate.clone()], Some(&catalog)),
                vec!["listed"]
            );
            assert!(select(ModelPurpose::Chat, vec![candidate.clone()], None).is_empty());
            candidate.capabilities.chat_operation = Some(false);
            assert!(select(ModelPurpose::Chat, vec![candidate.clone()], Some(&catalog)).is_empty());
            candidate.capabilities.chat_operation = None;
            assert!(select(ModelPurpose::Chat, vec![candidate.clone()], Some(&catalog)).is_empty());
            candidate.capabilities.chat_operation = Some(true);
            let mut missing_input = source.clone();
            missing_input["modalities"]["input"] = Value::Null;
            assert!(
                select(
                    ModelPurpose::Chat,
                    vec![candidate.clone()],
                    Some(&test_catalog(missing_input))
                )
                .is_empty()
            );
            for output in [json!(["image"]), Value::Null] {
                let mut incomplete = source.clone();
                incomplete["modalities"]["output"] = output;
                assert!(
                    select(
                        ModelPurpose::Chat,
                        vec![candidate.clone()],
                        Some(&test_catalog(incomplete))
                    )
                    .is_empty()
                );
            }
            candidate.capabilities.output_modalities = Some(vec!["image".into()]);
            assert!(select(ModelPurpose::Chat, vec![candidate.clone()], Some(&catalog)).is_empty());
            candidate.capabilities.input_modalities = Some(vec!["text".into()]);
            candidate.capabilities.output_modalities = Some(vec!["text".into()]);
            assert_eq!(
                select(ModelPurpose::Chat, vec![candidate], None),
                vec!["listed"]
            );
        }
    }

    #[test]
    fn picker_purpose_uses_native_precedence_and_only_missing_catalog_facts() {
        let catalog = test_catalog(chat_source());
        for (id, native, expected) in [
            ("absent", Some(true), true),
            ("listed", Some(false), false),
            ("listed", None, true),
            ("absent", None, false),
        ] {
            let mut candidate = model(id, native, None, None);
            candidate.capabilities.tool_calling = Some(false);
            // Neither unrelated unknown metadata nor tools are chat requirements.
            assert!(candidate.pricing.is_none());
            assert!(candidate.capabilities.reasoning.is_none());
            let selected = select(ModelPurpose::Chat, vec![candidate], Some(&catalog));
            assert_eq!(!selected.is_empty(), expected, "{id}: {native:?}");
        }
        assert_eq!(
            select(
                ModelPurpose::Chat,
                vec![model("private", Some(true), None, None)],
                None
            ),
            ["private"]
        );
    }

    #[test]
    fn modality_and_generated_api_defaults_do_not_prove_chat_or_transcription() {
        let mut sources = vec![
            Value::Null,
            json!({"tool_call":true}),
            json!({"modalities":{"input":["text","image"],"output":["text"]}}),
            json!({"type":"chat", "api":null,"modalities":{"input":["text"],"output":["text"]}}),
            json!({"type":"chat", "api":{"realtime":{}},"modalities":{"input":["text"],"output":["text"]}}),
            json!({"type":"chat", "api":{"responses":null},"modalities":{"input":["text"],"output":["text"]}}),
        ];
        for (input, output) in [
            (json!(["image"]), json!(["text"])),
            (json!(["audio"]), json!(["text"])),
            (json!(["text"]), json!(["image"])),
            (json!(null), json!(["text"])),
            (json!(["text"]), json!([null])),
        ] {
            sources.push(json!({"type":"chat","api":{"responses":{}},"modalities":{"input":input,"output":output}}));
        }
        for source in sources {
            let catalog = test_catalog(source.clone());
            let mut candidate = model("listed", None, None, None);
            candidate.capabilities.tool_calling = Some(true);
            assert!(
                select(ModelPurpose::Chat, vec![candidate.clone()], Some(&catalog)).is_empty(),
                "{source}"
            );
            assert!(
                select(ModelPurpose::Transcription, vec![candidate], Some(&catalog)).is_empty(),
                "{source}"
            );
        }
    }

    #[test]
    fn native_modalities_fill_individual_gaps_and_override_catalog_modalities() {
        let catalog = test_catalog(chat_source());
        for (input, output, expected) in [
            (Some(vec!["audio".into()]), None, false),
            (None, Some(vec!["image".into()]), false),
            (Some(vec!["text".into(), "image".into()]), None, true),
        ] {
            let mut candidate = model("listed", None, None, None);
            candidate.capabilities.input_modalities = input;
            candidate.capabilities.output_modalities = output;
            assert_eq!(
                !select(ModelPurpose::Chat, vec![candidate], Some(&catalog)).is_empty(),
                expected
            );
        }
        let mut source = chat_source();
        source["modalities"]["input"] = Value::Null;
        let catalog = test_catalog(source);
        let mut candidate = model("listed", None, None, None);
        candidate.capabilities.input_modalities = Some(vec!["text".into()]);
        assert_eq!(
            select(ModelPurpose::Chat, vec![candidate], Some(&catalog)),
            ["listed"]
        );
    }

    #[test]
    fn independent_purposes_allow_multiple_uses_but_respect_adapter_ceiling() {
        let models = vec![
            model("chat", Some(true), None, None),
            model("vectors", Some(false), Some(true), None),
            model("speech", Some(false), None, Some(true)),
            model("multi", Some(true), Some(true), Some(true)),
            model("unknown", None, None, None),
        ];
        assert_eq!(
            select(ModelPurpose::Chat, models.clone(), None),
            ["chat", "multi"]
        );
        assert_eq!(
            select(ModelPurpose::Embeddings, models.clone(), None),
            ["vectors", "multi"]
        );
        assert_eq!(
            select(ModelPurpose::Transcription, models.clone(), None),
            ["speech", "multi"]
        );
        assert!(
            filter_models(
                "openai",
                "responses",
                &ProviderCapabilities::default(),
                ModelPurpose::Embeddings,
                models,
                None
            )
            .is_empty()
        );

        let catalog = test_catalog(json!({"modalities":{"output":["embeddings"]}}));
        assert_eq!(
            select(
                ModelPurpose::Embeddings,
                vec![model("listed", None, None, None)],
                Some(&catalog)
            ),
            ["listed"]
        );
        assert!(
            select(
                ModelPurpose::Embeddings,
                vec![model("listed", None, Some(false), None)],
                Some(&catalog)
            )
            .is_empty()
        );
        let mut candidate = model("no-catalog-row", None, None, None);
        candidate.capabilities.output_modalities = Some(vec!["embeddings".into()]);
        assert_eq!(
            select(ModelPurpose::Embeddings, vec![candidate], None),
            ["no-catalog-row"]
        );
        let mut candidate = model("listed", None, None, None);
        candidate.capabilities.output_modalities = Some(vec!["text".into()]);
        assert!(select(ModelPurpose::Embeddings, vec![candidate], Some(&catalog)).is_empty());
    }

    #[test]
    fn native_chat_evidence_stays_internal_to_gateway_selection() {
        let mut candidate = model("private", Some(true), None, None);
        candidate.capabilities.chat_operation = Some(true);
        let wire = serde_json::to_value(candidate).unwrap();
        assert!(wire["capabilities"].get("chat").is_none());
        assert!(wire["capabilities"].get("chat_operation").is_none());
    }
}
