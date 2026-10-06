//! Unexecuted source-to-consumer regression coverage, not final-JSON injection.
use super::{
    generator::{self, SOURCE_URLS, SourceSnapshot},
    *,
};
use crate::{ToolChoice, tools::policy};
use serde_json::json;

pub(crate) fn source_snapshot() -> SourceSnapshot {
    let mut source: SourceSnapshot =
        serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json")).unwrap();
    let models = &mut source.sources.get_mut(SOURCE_URLS[0]).unwrap().body["openai"]["models"];
    let template = models["gpt-5-nano"].clone();
    for (id, support) in [
        ("g03-positive", Some(true)),
        ("g03-negative", Some(false)),
        ("g03-missing", None),
    ] {
        let mut model = template.clone();
        if let Some(support) = support {
            model["tool_call"] = json!(support);
        } else {
            model.as_object_mut().unwrap().remove("tool_call");
        }
        models[id] = model;
    }
    source.sources.get_mut(SOURCE_URLS[0]).unwrap().body["openrouter"]["models"]["g03-negative"] =
        json!({"tool_call":true});
    let azure = &mut source.sources.get_mut(SOURCE_URLS[0]).unwrap().body;
    azure["azure"]["models"]["g03-negative"] = json!({"tool_call":false});
    azure["zai-coding-plan"]["models"]["g03-authority-specific"] = json!({"tool_call":false});
    for (url, field, marker) in [
        (SOURCE_URLS[1], "supported_parameters", "tools"),
        (SOURCE_URLS[2], "tags", "tool-use"),
    ] {
        let data = source.sources.get_mut(url).unwrap().body["data"]
            .as_array_mut()
            .unwrap();
        let template = data[0].clone();
        for (id, value) in [
            ("g03-positive", json!([marker])),
            ("g03-negative", json!([])),
            ("g03-missing", serde_json::Value::Null),
            ("g03-null", serde_json::Value::Null),
            ("g03-malformed", json!([42])),
        ] {
            let mut model = template.clone();
            model["id"] = json!(id);
            if id == "g03-missing" {
                model.as_object_mut().unwrap().remove(field);
            } else {
                model[field] = value;
            }
            data.push(model);
        }
    }
    source
}

pub(crate) fn generated_catalog() -> ModelCatalog {
    let generated = generator::generate(&source_snapshot(), false).unwrap();
    ModelCatalog::parse_with_capabilities(
        &serde_json::to_string(&generated.models).unwrap(),
        &serde_json::to_string(&generated.provenance).unwrap(),
        generated.tool_capabilities,
    )
    .unwrap()
}

pub(crate) fn discovered(id: &str, support: Option<bool>) -> ProviderModelInfo {
    ProviderModelInfo {
        id: id.into(),
        provider: "openai".into(),
        name: None,
        description: None,
        created: None,
        owned_by: None,
        limits: Default::default(),
        capabilities: pioneer_protocol::ProviderModelCapabilities {
            tool_calling: support,
            ..Default::default()
        },
        transcription: None,
        pricing: None,
        active: None,
        family: None,
        lifecycle_status: None,
    }
}

#[test]
fn source_negatives_survive_filters_alias_lookup_enrichment_and_policy() {
    let catalog = generated_catalog();
    for provider in ["openai", "openrouter", "vercel-ai-gateway"] {
        assert_eq!(catalog.tool_support(provider, "g03-positive"), Some(true));
        assert_eq!(catalog.tool_support(provider, "g03-negative"), Some(false));
        assert_eq!(catalog.tool_support(provider, "g03-missing"), None);
        assert_eq!(catalog.tool_support(provider, "unknown"), None);
        if provider != "openai" {
            assert_eq!(catalog.tool_support(provider, "g03-null"), None);
            assert_eq!(catalog.tool_support(provider, "g03-malformed"), None);
        }
        assert!(catalog.model(provider, "g03-negative").is_none());
    }
    assert_eq!(
        catalog.tool_support("azure_openai", "g03-negative"),
        Some(false)
    );
    assert_eq!(
        catalog.tool_support("azure-openai", "g03-negative"),
        Some(false)
    );
    assert_eq!(catalog.tool_support("azure", "g03-negative"), Some(false));
    assert_eq!(catalog.tool_support("glm", "g03-authority-specific"), None);
    assert_eq!(
        catalog.tool_support("zai-coding-plan", "g03-authority-specific"),
        Some(false)
    );
    let mut models = vec![
        discovered("g03-negative", None),
        discovered("g03-positive", Some(false)),
        discovered("unknown", None),
    ];
    catalog.enrich("openai", &mut models);
    assert_eq!(models[0].capabilities.tool_calling, Some(false));
    assert_eq!(models[1].capabilities.tool_calling, Some(false));
    assert_eq!(models[2].capabilities.tool_calling, None);
    for choice in [
        ToolChoice::Auto,
        ToolChoice::None,
        ToolChoice::Required,
        ToolChoice::Tool {
            name: "lookup".into(),
        },
    ] {
        let mut request = policy::test_request();
        request.model = "g03-negative".into();
        request.tool_choice = Some(choice);
        assert!(policy::prepare_request_with_catalog("openai", request, Some(&catalog)).is_err());
    }
    for id in ["g03-positive", "g03-missing", "unknown"] {
        let mut request = policy::test_request();
        request.model = id.into();
        assert!(policy::prepare_request_with_catalog("openai", request, Some(&catalog)).is_ok());
    }
    let mut disabled = policy::test_request();
    disabled.model = "g03-negative".into();
    disabled.tools = None;
    disabled.tool_choice = Some(ToolChoice::None);
    assert!(
        policy::prepare_request_with_catalog("openai", disabled.clone(), Some(&catalog)).is_ok()
    );
    let mut assistant = crate::ChatMessage::assistant("");
    assistant.tool_calls = Some(vec![crate::ProviderToolCall {
        id: "call".into(),
        name: "lookup".into(),
        arguments: "{}".into(),
    }]);
    disabled.messages = vec![
        assistant,
        crate::ChatMessage::tool_result("call", "lookup", "ok"),
    ];
    assert!(policy::prepare_request_with_catalog("openai", disabled, Some(&catalog)).is_err());
}

#[tokio::test]
async fn discovery_false_vetoes_positive_source_and_remains_authority_scoped() {
    let catalog = generated_catalog();
    let false_discovery = BTreeMap::from([("g03-positive".into(), false)]);
    policy::with_discovery_tools("openai", true, false_discovery, async {
        assert_eq!(
            policy::tool_support_with_catalog("openai", "g03-positive", Some(&catalog)),
            Some(false)
        );
        let mut request = policy::test_request();
        request.model = "g03-positive".into();
        assert!(policy::prepare_request_with_catalog("openai", request, Some(&catalog)).is_err());
        assert_eq!(
            policy::tool_support_with_catalog("openrouter", "g03-positive", Some(&catalog)),
            Some(true)
        );
    })
    .await;
    assert_eq!(
        policy::tool_support_with_catalog("openai", "g03-positive", Some(&catalog)),
        Some(true)
    );
    policy::with_discovery_tools("openai", false, BTreeMap::new(), async {
        assert_eq!(
            policy::tool_support_with_catalog("openai", "g03-negative", Some(&catalog)),
            None
        );
        let mut request = policy::test_request();
        request.model = "g03-negative".into();
        assert!(policy::prepare_request_with_catalog("openai", request, Some(&catalog)).is_ok());
    })
    .await;
}

#[test]
fn merged_glm_profiles_keep_standard_and_coding_capabilities_separate() {
    let mut source = source_snapshot();
    let body = &mut source.sources.get_mut(SOURCE_URLS[0]).unwrap().body;
    let template = body["openai"]["models"]["gpt-5-nano"].clone();
    for (upstream, support) in [
        ("zai", false),
        ("zhipuai", true),
        ("zai-coding-plan", true),
        ("zhipuai-coding-plan", false),
    ] {
        let mut model = template.clone();
        model["tool_call"] = json!(support);
        body[upstream]["models"]["g03-region-tools"] = model;
    }
    let mut generated = generator::generate(&source, false).unwrap();
    let catalog = ModelCatalog::parse_with_capabilities(
        &serde_json::to_string(&generated.models).unwrap(),
        &serde_json::to_string(&generated.provenance).unwrap(),
        generated.tool_capabilities.clone(),
    )
    .unwrap();
    for (provider, support) in [
        ("glm", true),
        ("zhipu", true),
        ("zai", false),
        ("glm-global", false),
        ("zai-coding", true),
        ("zai-coding-plan", true),
        ("glm-coding", false),
        ("zhipuai-coding-plan", false),
    ] {
        assert_eq!(
            catalog.tool_support(provider, "g03-region-tools"),
            Some(support),
            "{provider}"
        );
    }
    // This model is absent from the global standard list. Its coding metadata
    // lookup must not normalize the catalog key `zai` a second time to standard.
    assert_eq!(
        catalog
            .model("zai-coding", "g03-region-tools")
            .unwrap()
            .provider,
        "zai"
    );
    generated
        .models
        .get_mut("zai")
        .unwrap()
        .get_mut("g03-region-tools")
        .unwrap()["toolCalling"] = json!(false);
    let restricted = ModelCatalog::parse_with_capabilities(
        &serde_json::to_string(&generated.models).unwrap(),
        &serde_json::to_string(&generated.provenance).unwrap(),
        generated.tool_capabilities.clone(),
    )
    .unwrap();
    assert_eq!(
        restricted.tool_support("zai-coding", "g03-region-tools"),
        Some(false)
    );
    assert_eq!(
        restricted.tool_support("glm", "g03-region-tools"),
        Some(true)
    );
    for profile in ["glm", "zai", "glm-coding", "zai-coding"] {
        let mut request = policy::test_request();
        assert!(policy::prepare_request_with_catalog(profile, request.clone(), None).is_ok());
        request.parallel_tool_calls = Some(false);
        assert!(policy::prepare_request_with_catalog(profile, request.clone(), None).is_err());
        request.tool_choice = Some(ToolChoice::None);
        let disabled =
            policy::prepare_request_with_catalog(profile, request.clone(), None).unwrap();
        assert!(disabled.tools.is_none());
        for choice in [
            ToolChoice::Required,
            ToolChoice::Tool {
                name: "lookup".into(),
            },
        ] {
            request.parallel_tool_calls = None;
            request.tool_choice = Some(choice);
            assert!(policy::prepare_request_with_catalog(profile, request.clone(), None).is_err());
        }
    }
}
