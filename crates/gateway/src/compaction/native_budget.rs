//! One Native/API budget resolver. CLI callers retain their technical budgets.
use anyhow::{Result, ensure};
use pioneer_compaction::ModelBudget;
use pioneer_protocol::NativeContextBudget;
use pioneer_provider::Provider;

/// Picker consent is scoped to the registry's endpoint/credential authority and model.
pub(crate) fn validate_manual_authority(
    provider: &dyn Provider,
    model: &str,
    manual: Option<u64>,
    authority: Option<&str>,
) -> Result<()> {
    if manual.is_some() {
        let expected = provider
            .authority_fingerprint()
            .map(|authority| pioneer_provider::catalog::context_choice_authority(authority, model));
        ensure!(
            expected.is_some() && authority == expected.as_deref(),
            "Context choice belongs to another provider authority/model; reopen the model picker"
        );
    }
    Ok(())
}

pub(crate) async fn capture(
    provider: &dyn Provider,
    instance: &str,
    model: &str,
    manual_window: Option<u64>,
) -> Result<NativeContextBudget> {
    provider.prepare_model(model).await?;
    let limits = provider.model_limits(model).await?;
    ensure!(
        limits.context_window > 0,
        "Native model has no valid context limit"
    );
    let context_window = match provider.model_context_tiers(model).await? {
        Some(metadata) => metadata
            .resolve(limits.context_window, manual_window)
            .map_err(anyhow::Error::msg)?,
        None => {
            ensure!(
                manual_window.is_none(),
                "Context pricing metadata is unavailable for this authority"
            );
            limits.context_window
        }
    };
    Ok(NativeContextBudget {
        instance: instance.into(),
        authority: provider.authority_fingerprint().map(str::to_owned),
        model: model.into(),
        manual_window,
        context_window,
        max_input: limits.max_input,
        max_output: limits.max_output,
    })
}

/// A saved launch can narrow under current technical limits, never widen.
pub(crate) async fn effective(
    provider: &dyn Provider,
    instance: &str,
    model: &str,
    saved: Option<&NativeContextBudget>,
    manual: Option<u64>,
) -> Result<ModelBudget> {
    let owned;
    let saved = if let Some(saved) = saved {
        saved
    } else {
        owned = capture(provider, instance, model, manual).await?;
        &owned
    };
    ensure!(
        saved.instance == instance
            && saved.model == model
            && saved.authority.as_deref() == provider.authority_fingerprint(),
        "Native context authorization belongs to another provider authority/model"
    );
    ensure!(
        saved.context_window > 0,
        "Invalid saved Native context budget"
    );
    let limits = provider.model_limits(model).await?;
    let current_window = match provider.model_context_tiers(model).await? {
        Some(metadata) => metadata
            .resolve(limits.context_window, saved.manual_window)
            .map_err(anyhow::Error::msg)?,
        None => {
            ensure!(
                saved.manual_window.is_none(),
                "Context metadata is unavailable for the saved manual selection"
            );
            limits.context_window
        }
    };
    let narrow = |a: Option<u64>, b: Option<u64>| match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    Ok(ModelBudget::new(
        Some(
            saved
                .context_window
                .min(limits.context_window)
                .min(current_window),
        ),
        narrow(saved.max_input, limits.max_input),
        narrow(saved.max_output, limits.max_output),
    ))
}

/// Freeze only Native/API summarization. CLI services keep their existing selection path.
pub(crate) async fn capture_summary(
    processor: &crate::message::MessageProcessor,
    workspace: &str,
    working: &NativeContextBudget,
) -> Result<Option<pioneer_protocol::NativeSummaryContext>> {
    let configured = processor.compaction_settings_for_workspace(workspace)?;
    let (budget, effort, inherits_working) = match configured.selection {
        None => (working.clone(), None, true),
        Some(selection) if selection.transport == pioneer_compaction::Transport::Api => {
            let provider = processor
                .provider_registry()
                .get_or_create_for_workspace(workspace, &selection.instance)?;
            validate_manual_authority(
                provider.as_ref(),
                &selection.model,
                selection.context_window,
                selection.context_authority.as_deref(),
            )?;
            (
                capture(
                    provider.as_ref(),
                    &selection.instance,
                    &selection.model,
                    selection.context_window,
                )
                .await?,
                selection.effort,
                false,
            )
        }
        Some(_) => return Ok(None),
    };
    Ok(Some(pioneer_protocol::NativeSummaryContext {
        budget,
        effort,
        inherits_working,
    }))
}

pub(crate) fn summary_selection(
    saved: &pioneer_protocol::NativeSummaryContext,
    current_effort: Option<String>,
) -> pioneer_compaction::ModelSelection {
    pioneer_compaction::ModelSelection {
        context_authority: saved.budget.authority.as_deref().map(|authority| {
            pioneer_provider::catalog::context_choice_authority(authority, &saved.budget.model)
        }),
        transport: pioneer_compaction::Transport::Api,
        instance: saved.budget.instance.clone(),
        model: saved.budget.model.clone(),
        effort: if saved.inherits_working {
            current_effort
        } else {
            saved.effort.clone()
        },
        context_window: Some(saved.budget.context_window),
    }
}

/// Server-only Task launch freezing, before Task's existing actor-contract commit.
pub(crate) async fn freeze_task_context(
    processor: &crate::message::MessageProcessor,
    workspace: &str,
    parent_turn: Option<&str>,
    context: &mut pioneer_tasks::TaskCreateContext,
) -> Result<()> {
    let Some(profile) = &context.resolved_launch_profile else {
        return Ok(());
    };
    if profile.backend != pioneer_protocol::AgentExecutionProfileBackend::ApiProvider {
        return Ok(());
    }
    let provider = processor
        .provider_registry()
        .get_or_create_for_workspace(workspace, &profile.provider_id)?;
    let parent = match parent_turn {
        Some(turn) => processor
            .crud_store
            .get_turn_execution_security_snapshot(turn)
            .await?
            .map(|record| record.snapshot),
        None => None,
    };
    context.native_context = match parent
        .as_ref()
        .and_then(|parent| parent.native_context.as_ref())
        .filter(|budget| {
            budget.instance == profile.provider_id
                && budget.model == profile.model_id
                && budget.authority.as_deref() == provider.authority_fingerprint()
        }) {
        Some(budget) => Some(budget.clone()),
        None => Some(
            capture(
                provider.as_ref(),
                &profile.provider_id,
                &profile.model_id,
                None,
            )
            .await?,
        ),
    };
    context.native_summary = match parent.and_then(|parent| parent.native_summary) {
        Some(mut summary) => {
            if summary.inherits_working {
                summary.budget = context
                    .native_context
                    .as_ref()
                    .expect("captured task budget")
                    .clone();
            }
            Some(summary)
        }
        None => {
            capture_summary(
                processor,
                workspace,
                context
                    .native_context
                    .as_ref()
                    .expect("captured task budget"),
            )
            .await?
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use pioneer_protocol::ProviderContextTiers;
    use pioneer_provider::{ChatRequest, ChatResponse, StreamChunk};
    struct BudgetProvider {
        authority: &'static str,
        windows: Vec<u64>,
    }
    #[async_trait]
    impl Provider for BudgetProvider {
        fn name(&self) -> &str {
            "budget-fixture"
        }
        fn authority_fingerprint(&self) -> Option<&str> {
            Some(self.authority)
        }
        async fn model_limits(&self, _: &str) -> Result<pioneer_provider::catalog::CatalogLimits> {
            Ok(pioneer_provider::catalog::CatalogLimits {
                context_window: 10000,
                context_origin: pioneer_provider::catalog::OriginKind::Source,
                max_input: Some(9000),
                max_output: Some(2048),
            })
        }
        async fn model_context_tiers(&self, _: &str) -> Result<Option<ProviderContextTiers>> {
            Ok(Some(ProviderContextTiers::Tiered {
                windows: self.windows.clone(),
                price_order_confirmed: true,
            }))
        }
        async fn chat(&self, _: ChatRequest) -> Result<ChatResponse> {
            unreachable!("budget preparation must not call inference")
        }
        async fn stream_chat(
            &self,
            _: ChatRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, Result<StreamChunk>>> {
            unreachable!("budget preparation must not call inference")
        }
        async fn list_models(&self) -> Result<Vec<pioneer_protocol::ProviderModelInfo>> {
            Ok(vec![])
        }
    }
    #[tokio::test]
    async fn durable_native_cap_never_widens_and_complete_request_uses_it() {
        let provider = BudgetProvider {
            authority: "account-a",
            windows: vec![1000, 10000],
        };
        let proof = pioneer_provider::catalog::context_choice_authority("account-a", "model");
        assert!(validate_manual_authority(&provider, "model", Some(10000), Some(&proof)).is_ok());
        assert!(validate_manual_authority(&provider, "other", Some(10000), Some(&proof)).is_err());
        assert!(validate_manual_authority(&provider, "model", Some(10000), None).is_err());
        let saved = capture(&provider, "instance", "model", None).await.unwrap();
        let budget = effective(&provider, "instance", "model", Some(&saved), None)
            .await
            .unwrap();
        assert_eq!(
            (budget.context, budget.input_limit, budget.max_output),
            (1000, Some(9000), Some(2048))
        );
        let request = ChatRequest {
            model: "model".into(),
            messages: vec![
                pioneer_provider::ChatMessage::system("fixed system"),
                pioneer_provider::ChatMessage::user("protected input ".repeat(1000)),
            ],
            temperature: None,
            max_tokens: Some(128),
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        let full = pioneer_agent::compaction::request::NativeRequestProjection::full(
            request.clone(),
            vec![],
            budget,
            false,
        )
        .unwrap();
        assert!(!full.fits);
        assert_eq!(full.request.messages, request.messages);
        let authorized = capture(&provider, "instance", "model", Some(10000))
            .await
            .unwrap();
        let wide = effective(&provider, "instance", "model", Some(&authorized), None)
            .await
            .unwrap();
        assert!(
            pioneer_agent::compaction::request::NativeRequestProjection::full(
                request,
                vec![],
                wide,
                false
            )
            .unwrap()
            .fits
        );
        let refreshed = BudgetProvider {
            authority: "account-a",
            windows: vec![2000, 10000],
        };
        assert_eq!(
            effective(&refreshed, "instance", "model", Some(&saved), None)
                .await
                .unwrap()
                .context,
            1000
        );
        let other = BudgetProvider {
            authority: "account-b",
            windows: vec![1000, 10000],
        };
        assert!(
            effective(&other, "instance", "model", Some(&authorized), None)
                .await
                .is_err()
        );
        assert!(
            effective(
                &provider,
                "instance",
                "other-model",
                Some(&authorized),
                None
            )
            .await
            .is_err()
        );
        assert!(
            capture(&provider, "instance", "model", Some(5000))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn refreshed_vercel_owner_overrides_old_chat_dto_and_narrows_saved_auto() {
        use pioneer_provider::{ProviderRegistry, ProviderTimeoutPolicy};
        use serde_json::json;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for threshold in [272000, 128000] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let mut bytes = [0; 4096];
                    let read = socket.read(&mut bytes).await.unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&bytes[..read]);
                    assert!(request.len() < 16384);
                }
                assert!(String::from_utf8_lossy(&request).starts_with("GET /v1/models"));
                let body = json!({"catalog_status":"complete","data":[{
                    "id":"private/chat", "type":"language", "tags":["tool-use"],
                    "context_window":1050000, "max_tokens":16000,
                    "modalities":{"input":["text"],"output":["text"]},
                    "pricing":{"input":"0.000002","output":"0.00001","input_tiers":[
                        {"min":0,"max":threshold + 1,"cost":"0.000002"},
                        {"min":threshold + 1,"cost":"0.000004"}
                    ]}
                },{
                    "id":"private/embedding", "type":"embedding", "context_window":8192,
                    "modalities":{"input":["text"],"output":["embedding"]}
                }]})
                .to_string();
                socket.write_all(format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ).as_bytes()).await.unwrap();
            }
        });
        let registry = ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
            |_, _| Ok("fixture-key".into()),
            |_, _| Ok(None),
            move |_, _| Ok(Some(root.clone())),
            ProviderTimeoutPolicy::default(),
        );
        let provider = registry
            .get_or_create_for_workspace("ws", "vercel-ai-gateway")
            .unwrap();
        let chat = provider.list_models().await.unwrap();
        let old_dto = chat
            .iter()
            .find(|model| model.id == "private/chat")
            .unwrap();
        assert_eq!(
            old_dto
                .context_tiers
                .as_ref()
                .unwrap()
                .resolve(1050000, None),
            Ok(272000)
        );
        let saved = capture(provider.as_ref(), "vercel-ai-gateway", "private/chat", None)
            .await
            .unwrap();
        assert_eq!(saved.context_window, 272000);
        // Embedding discovery refreshes the native owner but intentionally
        // does not replace the wrapper's chat input-admission DTO.
        provider.list_embedding_models().await.unwrap();
        server.await.unwrap();
        assert_eq!(
            provider
                .model_limits("private/chat")
                .await
                .unwrap()
                .context_window,
            1050000
        );
        assert_eq!(
            old_dto
                .context_tiers
                .as_ref()
                .unwrap()
                .resolve(1050000, None),
            Ok(272000)
        );
        assert_eq!(
            capture(provider.as_ref(), "vercel-ai-gateway", "private/chat", None)
                .await
                .unwrap()
                .context_window,
            128000
        );
        assert_eq!(
            effective(
                provider.as_ref(),
                "vercel-ai-gateway",
                "private/chat",
                Some(&saved),
                None
            )
            .await
            .unwrap()
            .context,
            128000
        );
    }
}
