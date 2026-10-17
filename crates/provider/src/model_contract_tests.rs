use super::*;
use crate::{ChatMessage, ReasoningConfig, ReasoningEffort, ToolDefinition};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};

struct Facts {
    support: Arc<AtomicBool>,
}
#[async_trait]
impl Provider for Facts {
    fn name(&self) -> &str {
        "openai"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            tool_calling: true,
            ..Default::default()
        }
    }
    async fn list_models(&self) -> Result<Vec<pioneer_protocol::ProviderModelInfo>> {
        let supported = self.support.load(Ordering::SeqCst);
        let mut model =
            crate::catalog::tool_tests::discovered("accepted-private-model", Some(supported));
        model.capabilities.reasoning = Some(pioneer_protocol::ProviderModelReasoningCapabilities {
            native: BTreeMap::from([("effort.high".into(), Some(supported))]),
            ..Default::default()
        });
        Ok(vec![model])
    }
    async fn prepare_input_budget(
        &self,
        request: ChatRequest,
    ) -> Result<crate::attachments::PreparedInputBudget> {
        crate::generation::validate_cap(self.name(), &request)?;
        Ok(crate::attachments::PreparedInputBudget {
            request,
            media: vec![],
        })
    }
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let request = crate::tools::policy::prepare_request(self.name(), request)?;
        let fields = crate::generation::chat_fields(self.name(), &request)?;
        Ok(ChatResponse {
            text: Value::Object(fields).to_string(),
            tool_calls: vec![],
            usage: None,
            reasoning_content: None,
            provider_replay_state: None,
            termination: crate::ProviderTermination::Complete,
        })
    }
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk_with(
                crate::ProviderTermination::Complete,
            )),
        ])
        .boxed())
    }
}
fn request(model: &str) -> ChatRequest {
    ChatRequest {
        model: model.into(),
        messages: vec![ChatMessage::user("same semantic request")],
        tools: Some(vec![ToolDefinition {
            name: "owned_tool".into(),
            description: "fixture".into(),
            parameters: json!({"type":"object","properties":{}}),
        }]),
        temperature: None,
        max_tokens: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    }
}
fn catalog(input: u64, tools: bool, parallel: bool) -> crate::catalog::ModelCatalog {
    crate::catalog::ModelCatalog::parse_with_capabilities(
        &json!({"openai":{"gpt-5.4":{"id":"gpt-5.4","name":"Known model","provider":"openai",
            "api":"openai-completions","baseUrl":"https://api.openai.com/v1","contextWindow":16000,
            "maxTokens":4000,"reasoning":true,"input":["text"],"cost":{},
            "thinkingLevelMap":{"high":"high"},"compat":{"supportsParallelToolCalls":parallel}}}}).to_string(),
        &json!({"openai":{"gpt-5.4":{"contextWindow":{"kind":"source","expression":"published"},
            "maxTokens":{"kind":"source","expression":"published"},
            "inputLimit":{"value":input,"origin":{"kind":"source","expression":"published-input"}}}}}).to_string(),
        Some(BTreeMap::from([("openai".into(), BTreeMap::from([("gpt-5.4".into(),tools)]))])),
    ).unwrap()
}
fn captured(catalog: &crate::catalog::ModelCatalog) -> AcceptedProviderModelContract {
    AcceptedProviderModelContract::capture_with_catalog(
        &Facts {
            support: Arc::new(AtomicBool::new(true)),
        },
        "gpt-5.4",
        true,
        None,
        None,
        Default::default(),
        ModelToolSupport::Unknown,
        Some(catalog),
        pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES,
    )
    .unwrap()
}

#[tokio::test]
async fn frozen_contract_controls_stream_chat_budget_and_blocking_catalog_after_refresh() {
    let first = catalog(12000, true, true);
    let original = captured(&first);
    let fresh = catalog(8000, false, true);
    assert_eq!(
        first.snapshot_id(),
        fresh.snapshot_id(),
        "models-only fingerprint misses both changed evidence sources"
    );
    let changed = captured(&fresh);
    assert_ne!(original.revision(), changed.revision());
    assert_eq!(changed.tool_support(), ModelToolSupport::Unsupported);
    let restored = AcceptedProviderModelContract::from_json(&original.to_json().unwrap()).unwrap();
    assert_eq!(restored, original);
    assert!(
        restored
            .to_json_with_limit(restored.to_json().unwrap().len() - 1)
            .is_err()
    );
    assert!(
        AcceptedProviderModelContract::capture_with_catalog(
            &Facts {
                support: Arc::new(AtomicBool::new(true))
            },
            "gpt-5.4",
            true,
            None,
            None,
            Default::default(),
            ModelToolSupport::Unknown,
            Some(&first),
            128,
        )
        .is_err()
    );
    let runtime = Arc::new(RuntimeContract::new(restored.clone()).unwrap());
    let state = Arc::new(crate::attachments::admission::AdmissionState::default().frozen(&runtime));
    let limits = tokio::task::spawn_blocking(move || {
        crate::attachments::admission::blocking_scope(Some(state), || {
            crate::catalog::model_catalog()
                .unwrap()
                .limits("openai", "gpt-5.4")
        })
    })
    .await
    .unwrap();
    assert_eq!(limits.max_input, Some(12000));
    assert_eq!(limits.max_output, Some(4000));
    let provider = restored
        .bind(Arc::new(Facts {
            support: Arc::new(AtomicBool::new(false)),
        }))
        .unwrap();
    assert_eq!(
        provider.model_tool_support("gpt-5.4"),
        ModelToolSupport::Supported
    );
    assert_eq!(
        provider.model_tool_support("another-model"),
        ModelToolSupport::Unknown
    );
    assert_eq!(
        provider
            .model_catalog_for_request("gpt-5.4")
            .unwrap()
            .limits("openai", "gpt-5.4")
            .max_input,
        Some(12000)
    );
    assert!(provider.model_catalog_for_request("another-model").is_err());
    let mut request = request("gpt-5.4");
    request.max_tokens = Some(5000);
    assert!(
        provider
            .prepare_input_budget(request.clone())
            .await
            .is_err()
    );
    request.max_tokens = Some(3000);
    request.reasoning = Some(ReasoningConfig::effort(ReasoningEffort::High));
    request.parallel_tool_calls = Some(false);
    let prepared = provider
        .prepare_input_budget(request.clone())
        .await
        .unwrap();
    assert_eq!(prepared.request.model, "gpt-5.4");
    let response: Value =
        serde_json::from_str(&provider.chat(request.clone()).await.unwrap().text).unwrap();
    assert_eq!(response["reasoning_effort"], "high");
    let mut stream = provider.stream_chat(request.clone()).await.unwrap();
    let streamed: Value =
        serde_json::from_str(&stream.next().await.unwrap().unwrap().delta).unwrap();
    assert_eq!(streamed, response);
    request.model = "another-model".into();
    assert!(provider.chat(request).await.is_err());
}

#[tokio::test]
async fn native_discovery_refresh_does_not_replace_accepted_tools_and_revocation_still_wins() {
    let support = Arc::new(AtomicBool::new(true));
    let provider: Arc<dyn Provider> = Arc::from(crate::registry::bind_direct_provider(
        Box::new(Facts {
            support: support.clone(),
        }),
        &"a".repeat(64),
    ));
    provider.list_models().await.unwrap();
    let original = provider
        .capture_model_contract("accepted-private-model")
        .unwrap();
    assert_eq!(
        original.native_reasoning.get("effort.high"),
        Some(&Some(true))
    );
    let restored = AcceptedProviderModelContract::from_json(&original.to_json().unwrap()).unwrap();
    assert_eq!(restored.native_reasoning, original.native_reasoning);
    assert_eq!(
        RuntimeContract::new(restored)
            .unwrap()
            .discovery
            .unwrap()
            .capabilities
            .reasoning
            .unwrap()
            .native
            .get("effort.high"),
        Some(&Some(true))
    );
    let frozen = original.clone().bind(provider.clone()).unwrap();
    support.store(false, Ordering::SeqCst);
    provider.list_models().await.unwrap();
    let changed = provider
        .capture_model_contract("accepted-private-model")
        .unwrap();
    assert_eq!(
        changed.native_reasoning.get("effort.high"),
        Some(&Some(false))
    );
    assert_ne!(original.revision(), changed.revision());
    assert_eq!(changed.tool_support(), ModelToolSupport::Unsupported);
    assert_eq!(
        frozen.model_tool_support("accepted-private-model"),
        ModelToolSupport::Supported
    );
    frozen
        .chat(request("accepted-private-model"))
        .await
        .unwrap();
    assert!(
        provider
            .chat(request("accepted-private-model"))
            .await
            .is_err()
    );
    let other: Arc<dyn Provider> = Arc::from(crate::registry::bind_direct_provider(
        Box::new(Facts { support }),
        &"b".repeat(64),
    ));
    assert!(
        original.bind(other).is_err(),
        "same brand/model cannot cross authority"
    );
    let registry = crate::ProviderRegistry::with_provider(
        "echo",
        Arc::new(crate::providers::EchoProvider::new()),
    );
    let raw = registry
        .get_or_create_for_workspace("owner", "echo")
        .unwrap();
    let frozen = raw
        .capture_model_contract("model")
        .unwrap()
        .bind(raw)
        .unwrap();
    registry.invalidate_workspace_provider("owner", "echo");
    assert!(
        frozen
            .prepare_input_budget(request("model"))
            .await
            .err()
            .expect("revoked provider must fail")
            .downcast_ref::<crate::ProviderAuthorityRevoked>()
            .is_some()
    );
}

#[test]
fn profile_version_integrity_real_bytes_and_safe_debug_fail_closed() {
    let accepted = captured(&catalog(12000, true, true));
    for (field, value) in [
        ("schema_version", json!(2)),
        ("adapter_api", json!("changed-api")),
        ("revision", json!("tampered")),
        ("effective_tools", json!("unsupported")),
        ("unknown", json!(true)),
    ] {
        let mut encoded: Value = serde_json::from_str(&accepted.to_json().unwrap()).unwrap();
        encoded[field] = value;
        assert!(AcceptedProviderModelContract::from_json(&encoded.to_string()).is_err());
    }
    let huge = json!({"private":"\"".repeat(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES)});
    let error = bounded_json(&huge).unwrap_err().to_string();
    assert!(!error.contains("private"));
    assert!(
        AcceptedProviderModelContract::from_json(
            &"é".repeat(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES)
        )
        .is_err()
    );
    assert!(validate_json_shape(&json!([vec![true; 5000]]).to_string()).is_err());
    let mut deep = "true".to_owned();
    for _ in 0..40 {
        deep = format!("[{deep}]");
    }
    assert!(validate_json_shape(&deep).is_err());
    let mut native = accepted.clone();
    native.native_reasoning = (0..65)
        .map(|i| (format!("effort.unknown{i}"), None))
        .collect();
    assert!(native.validate_identity("openai", "gpt-5.4", None).is_err());
    let debug = format!("{accepted:?}");
    assert!(!debug.contains("api.openai.com"));
    assert!(!debug.contains("catalog_json"));
    assert!(!debug.contains("published-input"));
}

#[test]
fn azure_alias_and_actual_chat_protocol_negative_are_part_of_selected_contract() {
    let provider = crate::providers::AzureOpenAiProvider::new("unused", "resource", "deployment");
    let accepted = provider.capture_model_contract("deployment").unwrap();
    accepted
        .require_selection("azure-openai", "deployment")
        .unwrap();
    accepted
        .require_selection("azure_openai", "deployment")
        .unwrap();
    assert!(accepted.require_selection("openai", "deployment").is_err());
    assert!(
        accepted
            .require_selection("azure-openai", "other-deployment")
            .is_err()
    );
    for model in ["gpt-6-astra", "gpt-6.1-sol-2026-10-01"] {
        assert_eq!(
            provider.model_tool_support(model),
            ModelToolSupport::Unsupported
        );
        assert_eq!(
            crate::providers::openai::OpenAiProvider::new("unused").model_tool_support(model),
            ModelToolSupport::Unsupported
        );
        assert_eq!(
            provider
                .capture_model_contract(model)
                .unwrap()
                .tool_support(),
            ModelToolSupport::Unsupported
        );
    }
    assert_eq!(
        provider.model_tool_support("opaque-new-deployment"),
        ModelToolSupport::Unknown
    );
}
