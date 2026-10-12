//! First-party HTML execution. Context and canonical call identity are supplied
//! by the Gateway, while both provider paths use the same implementation.
mod browser;
use super::MessageProcessor;
use async_trait::async_trait;
use pioneer_agent::TurnToolContext;
use pioneer_artifacts::{ArtifactBindingTarget, IngestArtifactBytesRequest};
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
    ArtifactRole, ArtifactSummary,
};
use pioneer_tools::html::{
    self, Appearance, HTML_PREVIEW, HTML_RENDER, PreviewParams, RenderParams,
};
use pioneer_tools::{
    FunctionToolOutput, ToolError, ToolExtensionBundle, ToolHandler, ToolInvocation, ToolOutput,
    ToolPayload,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

impl MessageProcessor {
    pub(crate) fn html_tools(self: &Arc<Self>, context: TurnToolContext) -> ToolExtensionBundle {
        let handler = Arc::new(HtmlHandler {
            processor: self.clone(),
            context,
        });
        let specs = html::html_tool_specs();
        let handlers = specs
            .iter()
            .map(|spec| {
                (
                    spec.spec.name.clone(),
                    handler.clone() as Arc<dyn ToolHandler>,
                )
            })
            .collect();
        ToolExtensionBundle {
            specs,
            handlers,
            ..Default::default()
        }
    }

    pub(crate) async fn execute_html(
        &self,
        context: &TurnToolContext,
        call: &str,
        name: &str,
        arguments: Value,
        cancel: &CancellationToken,
    ) -> Result<Value, ToolError> {
        let local_cancel = cancel.child_token();
        let _cancel_on_drop = local_cancel.clone().drop_guard();
        let cancel = &local_cancel;
        let operation = async {
            self.html_authorize(context).await?;
            if cancel.is_cancelled() {
                return Err(ToolError::cancelled("HTML invocation cancelled"));
            }
            match name {
                HTML_RENDER => {
                    let params: RenderParams = serde_json::from_value(arguments)
                        .map_err(|e| ToolError::invalid_arguments(e.to_string()))?;
                    html::validate_html(&params.html)?;
                    html::validate_title(&params.title)?;
                    let title = if params.title.trim().is_empty() {
                        "HTML"
                    } else {
                        params.title.trim()
                    };
                    let summary = self
                        .html_ingest(
                            context,
                            call,
                            "document",
                            params.html.into_bytes(),
                            title,
                            ArtifactKind::File,
                            "text/html",
                            true,
                            Value::Null,
                            cancel,
                        )
                        .await?;
                    // Durable exact artifact+binding is committed before notification or
                    // success. A retry recovers that version, without another publication.
                    self.send_notification_to_thread_subscribers(
                        &context.thread_id,
                        pioneer_protocol::constants::events::ARTIFACT_CREATED,
                        &pioneer_protocol::ArtifactCreatedNotification {
                            workspace_id: context.workspace_id.clone(),
                            artifact: summary.clone(),
                        },
                    )
                    .await;
                    self.send_thread_artifacts_changed_to_thread_and_ancestors(
                        &context.workspace_id,
                        &context.thread_id,
                        vec![summary.artifact.artifact_id.clone()],
                        "html_render",
                        chrono::Utc::now().timestamp(),
                    )
                    .await;
                    self.notify_semantic_timeline_turn_state_changed(
                        &context.workspace_id,
                        &context.thread_id,
                        &context.turn_id,
                    )
                    .await;
                    Ok(
                        json!({"htmlPublication":{"workspaceId":context.workspace_id,"threadId":context.thread_id,"turnId":context.turn_id,"callId":call,"title":title,"artifact":summary.artifact}}),
                    )
                }
                HTML_PREVIEW => {
                    let params: PreviewParams = serde_json::from_value(arguments)
                        .map_err(|e| ToolError::invalid_arguments(e.to_string()))?;
                    html::validate_html(&params.html)?;
                    self.html_preview_vision(context).await?;
                    let input = self.html_ingest(context, call, "input", params.html.as_bytes().to_vec(), "HTML preview source", ArtifactKind::File, "text/html", false, json!({"appearance":if matches!(params.appearance,Appearance::Dark) {"dark"} else {"light"}}), cancel).await?;
                    let mut stored = std::collections::HashMap::new();
                    for width in html::PREVIEW_WIDTHS {
                        let id = html_identity(context, call, &width.to_string());
                        match self
                            .artifact_service
                            .get_artifact(&context.workspace_id, &id, None)
                            .await
                        {
                            Ok(summary)
                                if summary.artifact.status
                                    == pioneer_protocol::ArtifactStatus::Ready
                                    && summary.created_by_actor_id.as_deref()
                                        == Some(HTML_PREVIEW) =>
                            {
                                stored.insert(width, summary);
                            }
                            Ok(_) => {
                                return Err(ToolError::execution_failed(
                                    "preview screenshot was deleted or its exact identity changed",
                                ));
                            }
                            Err(pioneer_artifacts::ArtifactError::NotFound { .. }) => {}
                            Err(e) => return Err(ToolError::execution_failed(e.to_string())),
                        }
                    }
                    // Recover committed screenshots after a crash without changing
                    // their bytes or diagnostics. Missing presets use fresh contexts.
                    let captures = if stored.len() == 3 {
                        Vec::new()
                    } else {
                        browser::capture(
                            &params.html,
                            matches!(params.appearance, Appearance::Dark),
                            &self.artifact_runtime_home.join("html-preview"),
                            cancel,
                        )
                        .await
                        .map_err(|e| {
                            if cancel.is_cancelled() {
                                ToolError::cancelled("HTML preview cancelled")
                            } else {
                                ToolError::execution_failed(e.to_string())
                            }
                        })?
                    };
                    for capture in captures {
                        if cancel.is_cancelled() {
                            return Err(ToolError::cancelled("HTML preview cancelled"));
                        }
                        if stored.contains_key(&capture.width) {
                            continue;
                        }
                        let details = json!({"width":capture.width,"contentHeight":capture.content_height,"capturedHeight":capture.captured_height,"diagnostics":capture.diagnostics});
                        let summary = self
                            .html_ingest(
                                context,
                                call,
                                &capture.width.to_string(),
                                capture.png,
                                &format!("HTML preview {}px", capture.width),
                                ArtifactKind::Screenshot,
                                "image/png",
                                false,
                                details,
                                cancel,
                            )
                            .await?;
                        stored.insert(capture.width, summary);
                    }
                    let mut previews = Vec::with_capacity(3);
                    let mut images = Vec::with_capacity(3);
                    let mut media = Vec::with_capacity(6);
                    let mut bytes = 0u64;
                    for width in html::PREVIEW_WIDTHS {
                        let summary = stored.remove(&width).ok_or_else(|| {
                            ToolError::execution_failed("preview must contain all three presets")
                        })?;
                        bytes =
                            bytes.saturating_add(summary.artifact.size_bytes.unwrap_or(u64::MAX));
                        if bytes > browser::SCREENSHOT_BUDGET as u64 {
                            return Err(ToolError::execution_failed(
                                "three preview images exceed the shared transport budget",
                            ));
                        }
                        let resolved = self
                            .artifact_service
                            .resolve_provider_attachment(
                                &context.workspace_id,
                                &summary.artifact.artifact_id,
                                summary.artifact.version_id.as_deref(),
                            )
                            .await
                            .map_err(|_| {
                                ToolError::execution_failed("exact preview screenshot unavailable")
                            })?;
                        let details = summary.metadata["htmlTool"]["details"].clone();
                        if details["width"] != width {
                            return Err(ToolError::execution_failed(
                                "preview screenshot diagnostics are missing",
                            ));
                        }
                        let mut preview = details;
                        preview["artifact"] = json!(summary.artifact);
                        previews.push(preview);
                        media.push(pioneer_provider::MessageContentPart::text(format!(
                            "HTML preview — {width} CSS px"
                        )));
                        media.push(pioneer_provider::MessageContentPart::image(
                            resolved.attachment.clone(),
                        ));
                        images.push(json!({"width":width,"attachment":resolved.attachment}));
                    }
                    let value = json!({"source":input.artifact,"previews":previews,"llm_context":{"images":images}});
                    let pioneer_tools::LlmOutputPolicy::Structured { max_bytes } =
                        pioneer_tools::ToolOutputPolicySnapshot::for_tool_name(HTML_PREVIEW).llm
                    else {
                        unreachable!("HTML preview uses structured media")
                    };
                    if (pioneer_tools::ToolResultView::Json {
                        value: value.clone(),
                        truncated: false,
                    })
                    .serialized_size_bytes()
                        > max_bytes
                    {
                        return Err(ToolError::execution_failed(
                            "preview diagnostics and exact image references exceed the shared result budget",
                        ));
                    }
                    self.html_preview_media_contract(context, call, media)
                        .await?;
                    Ok(value)
                }
                _ => Err(ToolError::invalid_arguments("unknown HTML tool")),
            }
        };
        tokio::pin!(operation);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // Bound the wait even when a cancelled operation is queued for
                // database capacity. Owned preview cleanup continues on drop.
                let _ = tokio::time::timeout(Duration::from_secs(10), &mut operation).await;
                Err(ToolError::cancelled("HTML invocation cancelled"))
            }
            result = &mut operation => result,
            _ = tokio::time::sleep(Duration::from_secs(110)) => {
                local_cancel.cancel();
                // Preview's owned task reaps its child before this result leaves.
                let _ = tokio::time::timeout(Duration::from_secs(10), &mut operation).await;
                Err(ToolError::execution_failed("HTML operation deadline exceeded"))
            }
        }
    }

    pub(in crate::message) async fn notify_html_artifact_changed(
        &self,
        workspace: &str,
        artifact: &str,
    ) {
        let Ok(summary) = self
            .artifact_service
            .get_artifact(workspace, artifact, None)
            .await
        else {
            return;
        };
        if summary.created_by_actor_id.as_deref() != Some(HTML_RENDER) {
            return;
        }
        let mut owners = std::collections::HashSet::new();
        for binding in &summary.bindings {
            let (Some(thread), Some(turn)) =
                (binding.thread_id.as_deref(), binding.turn_id.as_deref())
            else {
                continue;
            };
            if pioneer_protocol::HtmlPublication::from_summary(&summary, thread, turn).is_some()
                && owners.insert((thread, turn))
            {
                self.send_thread_artifacts_changed_to_thread_and_ancestors(
                    workspace,
                    thread,
                    vec![artifact.to_owned()],
                    "html_artifact_changed",
                    chrono::Utc::now().timestamp(),
                )
                .await;
                self.notify_semantic_timeline_turn_state_changed(workspace, thread, turn)
                    .await;
            }
        }
    }

    async fn html_preview_media_contract(
        &self,
        context: &TurnToolContext,
        call: &str,
        parts: Vec<pioneer_provider::MessageContentPart>,
    ) -> Result<(), ToolError> {
        let admission = crate::authorization::ExecutionAuthorizationContext::load_for_turn(
            &self.crud_store,
            &context.turn_id,
        )
        .await
        .map_err(|_| ToolError::execution_failed("preview admitted model context unavailable"))?;
        if admission.continuation_cli_runtime().is_some() {
            return Ok(());
        }
        let (provider_id, model_id) = admission
            .continuation_provider()
            .ok_or_else(|| ToolError::execution_failed("preview admitted provider unavailable"))?;
        let provider = self
            .provider_registry
            .get_or_create_for_workspace(&context.workspace_id, provider_id)
            .map_err(|_| ToolError::execution_failed("preview provider unavailable"))?;
        let mut result = pioneer_provider::ChatMessage::tool_result(
            call,
            HTML_PREVIEW,
            "HTML preview diagnostics and exact screenshot references",
        );
        result.content_parts = parts;
        let request = pioneer_provider::ChatRequest {
            model: model_id.into(),
            messages: vec![
                pioneer_provider::ChatMessage::assistant_tool_calls(
                    None::<String>,
                    vec![pioneer_provider::ProviderToolCall {
                        id: call.into(),
                        name: HTML_PREVIEW.into(),
                        arguments: "{}".into(),
                    }],
                ),
                result,
            ],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        provider.prepare_input_budget(request).await.map_err(|_| ToolError::execution_failed(
            "html_preview images cannot be delivered by the admitted model/endpoint media contract; refresh discovery or select a supported vision model. html_render remains available"
        ))?;
        Ok(())
    }

    async fn html_preview_vision(&self, context: &TurnToolContext) -> Result<(), ToolError> {
        let admission = crate::authorization::ExecutionAuthorizationContext::load_for_turn(
            &self.crud_store,
            &context.turn_id,
        )
        .await
        .map_err(|_| ToolError::execution_failed("preview admitted model context unavailable"))?;
        let supported = if let Some((runtime, model_id)) = admission.continuation_cli_runtime() {
            self.cli_runtime_probe_snapshot(&context.workspace_id, runtime)
                .await
                .map_err(|_| {
                    ToolError::execution_failed("preview runtime capabilities unavailable")
                })?
                .and_then(|snapshot| snapshot.models)
                .and_then(|snapshot| {
                    snapshot
                        .result
                        .models
                        .into_iter()
                        .find(|model| model.id == model_id)
                })
                .and_then(|model| {
                    model.supports_vision.or_else(|| {
                        (!model.input_modalities.is_empty())
                            .then(|| model.input_modalities.iter().any(|m| m == "image"))
                    })
                })
        } else {
            let (provider_id, _) = admission.continuation_provider().ok_or_else(|| {
                ToolError::execution_failed("preview admitted provider unavailable")
            })?;
            let provider = self
                .provider_registry
                .get_or_create_for_workspace(&context.workspace_id, provider_id)
                .map_err(|_| ToolError::execution_failed("preview provider unavailable"))?;
            // Adapter ceiling before capture. The actual screenshots are checked
            // with the registry's admitted discovery/endpoint/size contracts
            // before success, including dynamic local models absent from catalog.
            Some(
                provider
                    .capabilities()
                    .input_types
                    .support_for(pioneer_provider::InputContentType::Image)
                    .is_supported(),
            )
        };
        match supported {
            Some(true) => Ok(()),
            Some(false) => Err(ToolError::execution_failed(
                "html_preview requires image input capability; html_render remains available",
            )),
            None => Err(ToolError::execution_failed(
                "html_preview image capability is unknown; refresh model discovery or select a vision model. html_render remains available",
            )),
        }
    }

    async fn html_authorize(&self, context: &TurnToolContext) -> Result<u64, ToolError> {
        let mut generation = None;
        for action in [
            crate::authorization::ResourceAction::ArtifactCreateThread,
            crate::authorization::ResourceAction::ArtifactBindThread,
        ] {
            let authorized = self
                .revalidate_tool_execution_authorization(
                    &context.workspace_id,
                    &context.thread_id,
                    &context.turn_id,
                    None,
                    action,
                )
                .await
                .map_err(|_| {
                    ToolError::execution_failed(
                        "HTML artifact unavailable for the current execution",
                    )
                })?;
            let current = authorized.validated_policy_generation();
            if generation.is_some_and(|previous| previous != current) {
                return Err(ToolError::execution_failed(
                    "HTML authorization changed during validation",
                ));
            }
            generation = Some(current);
        }
        Ok(generation.expect("two actions validated"))
    }

    async fn html_ingest(
        &self,
        context: &TurnToolContext,
        call: &str,
        index: &str,
        bytes: Vec<u8>,
        title: &str,
        kind: ArtifactKind,
        mime: &str,
        publication: bool,
        details: Value,
        cancel: &CancellationToken,
    ) -> Result<ArtifactSummary, ToolError> {
        let generation = self.html_authorize(context).await?;
        let identity = html_identity(context, call, index);
        let mut metadata = BTreeMap::new();
        metadata.insert("htmlTool".into(), json!({"version":1,"publication":publication,"turnId":context.turn_id,"callId":call,"index":index,"title":title,"details":details}));
        self.artifact_service
            .ingest_tool_bytes(
                IngestArtifactBytesRequest {
                    workspace_id: context.workspace_id.clone(),
                    primary_thread_id: Some(context.thread_id.clone()),
                    bytes,
                    display_name: title.to_owned(),
                    kind,
                    mime_type: Some(mime.into()),
                    created_by_kind: ArtifactCreatedByKind::Tool,
                    created_by_actor_id: Some(
                        if publication {
                            HTML_RENDER
                        } else {
                            HTML_PREVIEW
                        }
                        .into(),
                    ),
                    binding: Some(ArtifactBindingTarget {
                        thread_id: Some(context.thread_id.clone()),
                        turn_id: Some(context.turn_id.clone()),
                        message_id: None,
                        turn_item_id: None,
                        tool_call_id: Some(call.into()),
                        task_id: None,
                        task_run_id: None,
                        binding_kind: ArtifactBindingKind::ToolOutput,
                        direction: ArtifactBindingDirection::Output,
                        role: Some(ArtifactRole::Tool),
                        item_index: None,
                    }),
                    metadata,
                },
                identity,
                generation,
                cancel.clone(),
            )
            .await
            .map_err(|e| {
                if cancel.is_cancelled() {
                    ToolError::cancelled("HTML ingestion cancelled")
                } else {
                    ToolError::execution_failed(e.to_string())
                }
            })
    }
}

struct HtmlHandler {
    processor: Arc<MessageProcessor>,
    context: TurnToolContext,
}
#[async_trait]
impl ToolHandler for HtmlHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let ToolPayload::Function { arguments } = invocation.payload else {
            return Err(ToolError::invalid_arguments(
                "HTML tools require function arguments",
            ));
        };
        let value = self
            .processor
            .execute_html(
                &self.context,
                &invocation.call_id,
                &invocation.tool_name,
                arguments,
                &invocation.cancellation,
            )
            .await?;
        Ok(Box::new(FunctionToolOutput::with_payload(
            "HTML tool completed",
            true,
            value,
        )))
    }
}

/// Theme appears before authored scripts and content. No credentials, source URL,
/// filesystem access or Gateway/native RPC is injected into the document.
fn prepared_document(html: &str, dark: bool) -> String {
    let (background, foreground, muted, border, accent) = if dark {
        ("#18181b", "#fafafa", "#a1a1aa", "#3f3f46", "#60a5fa")
    } else {
        ("#ffffff", "#18181b", "#71717a", "#d4d4d8", "#2563eb")
    };
    format!(
        "<!doctype html><meta name=viewport content=\"width=device-width,initial-scale=1\"><style>:root{{color-scheme:{};--pioneer-background:{background};--pioneer-foreground:{foreground};--pioneer-muted:{muted};--pioneer-border:{border};--pioneer-accent:{accent}}}html,body{{margin:0;background:var(--pioneer-background);color:var(--pioneer-foreground)}}</style>{html}",
        if dark { "dark" } else { "light" }
    )
}

pub(crate) fn screenshot_budget() -> usize {
    browser::SCREENSHOT_BUDGET
}

fn html_identity(context: &TurnToolContext, call: &str, index: &str) -> String {
    let identity = serde_json::to_vec(&json!([
        "html-tools-v1",
        context.workspace_id,
        context.thread_id,
        context.turn_id,
        call,
        index
    ]))
    .expect("identity serializes");
    hex::encode(Sha256::digest(identity))
}
