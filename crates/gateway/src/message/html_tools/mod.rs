//! First-party HTML execution. Context and canonical call identity are supplied
//! by the Gateway, while both provider paths use the same implementation.
mod browser;
mod theme;
use super::MessageProcessor;
use async_trait::async_trait;
use pioneer_agent::TurnToolContext;
use pioneer_artifacts::{ArtifactBindingTarget, IngestArtifactBytesRequest};
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
    ArtifactRole, ArtifactSummary,
};
use pioneer_tools::html::{self, HTML_PREVIEW, HTML_RENDER, PreviewParams, RenderParams};
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
                            1,
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
                    // A recorded v1 call can recover its exact stored source and frames;
                    // metadata is never upgraded in place or used to synthesize images.
                    let source_id = html_identity(context, call, "input");
                    let legacy = match self
                        .artifact_service
                        .get_artifact(&context.workspace_id, &source_id, None)
                        .await
                    {
                        Ok(source) => source.metadata["htmlTool"]["version"] == 1,
                        Err(pioneer_artifacts::ArtifactError::NotFound { .. }) => false,
                        Err(error) => return Err(ToolError::execution_failed(error.to_string())),
                    };
                    let details = if legacy {
                        json!({"appearance":if matches!(params.appearance, html::Appearance::Dark) {"dark"} else {"light"}})
                    } else {
                        json!({"formatVersion":2})
                    };
                    let input = self
                        .html_ingest(
                            context,
                            call,
                            "input",
                            params.html.as_bytes().to_vec(),
                            "HTML preview source",
                            ArtifactKind::File,
                            "text/html",
                            false,
                            if legacy { 1 } else { 2 },
                            details,
                            cancel,
                        )
                        .await?;
                    let frames: Vec<(Option<&str>, u32, String)> = if legacy {
                        html::PREVIEW_WIDTHS
                            .iter()
                            .map(|&width| (None, width, width.to_string()))
                            .collect()
                    } else {
                        html::PREVIEW_FRAMES
                            .iter()
                            .map(|&(appearance, width)| {
                                (Some(appearance), width, format!("{appearance}-{width}"))
                            })
                            .collect()
                    };
                    let mut stored = std::collections::HashMap::new();
                    for (_, _, index) in &frames {
                        let id = html_identity(context, call, index);
                        match self
                            .artifact_service
                            .get_artifact(&context.workspace_id, &id, None)
                            .await
                        {
                            Ok(summary)
                                if summary.artifact.status
                                    == pioneer_protocol::ArtifactStatus::Ready
                                    && summary.created_by_actor_id.as_deref()
                                        == Some(HTML_PREVIEW)
                                    && summary.metadata["htmlTool"]["version"]
                                        == (if legacy { 1 } else { 2 })
                                    && summary.metadata["htmlTool"]["index"] == index.as_str()
                                    && summary.metadata["htmlTool"]["turnId"]
                                        == context.turn_id
                                    && summary.metadata["htmlTool"]["callId"] == call
                                    && summary.bindings.iter().any(|binding| {
                                        binding.thread_id.as_deref()
                                            == Some(context.thread_id.as_str())
                                            && binding.turn_id.as_deref()
                                                == Some(context.turn_id.as_str())
                                            && binding.tool_call_id.as_deref() == Some(call)
                                            && binding.version_id == summary.artifact.version_id
                                            && binding.binding_kind
                                                == ArtifactBindingKind::ToolOutput
                                            && binding.direction == ArtifactBindingDirection::Output
                                            && binding.role == Some(ArtifactRole::Tool)
                                    }) =>
                            {
                                stored.insert(index.clone(), summary);
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
                    let captures = if stored.len() == frames.len() {
                        Vec::new()
                    } else if legacy {
                        return Err(ToolError::execution_failed(
                            "legacy preview is incomplete; saved screenshots cannot be regenerated",
                        ));
                    } else {
                        browser::capture(
                            &params.html,
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
                        let index = format!("{}-{}", capture.appearance, capture.width);
                        if stored.contains_key(&index) {
                            continue;
                        }
                        let details = json!({"appearance":capture.appearance,"width":capture.width,"contentHeight":capture.content_height,"capturedHeight":capture.captured_height,"diagnostics":capture.diagnostics});
                        let summary = self
                            .html_ingest(
                                context,
                                call,
                                &index,
                                capture.png,
                                &format!("HTML preview {} {}px", capture.appearance, capture.width),
                                ArtifactKind::Screenshot,
                                "image/png",
                                false,
                                2,
                                details,
                                cancel,
                            )
                            .await?;
                        stored.insert(index, summary);
                    }
                    let mut previews = Vec::with_capacity(frames.len());
                    let mut images = Vec::with_capacity(frames.len());
                    let mut media = Vec::with_capacity(frames.len() * 2);
                    let mut bytes = 0u64;
                    for (appearance, width, index) in frames {
                        let summary = stored.remove(&index).ok_or_else(|| {
                            ToolError::execution_failed(
                                "preview must contain the complete ordered preset set",
                            )
                        })?;
                        bytes =
                            bytes.saturating_add(summary.artifact.size_bytes.unwrap_or(u64::MAX));
                        if bytes > browser::SCREENSHOT_BUDGET as u64 {
                            return Err(ToolError::execution_failed(
                                "preview images exceed the shared transport budget",
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
                        if details["width"] != width
                            || appearance
                                .is_some_and(|appearance| details["appearance"] != appearance)
                        {
                            return Err(ToolError::execution_failed(
                                "preview screenshot diagnostics are missing",
                            ));
                        }
                        let mut preview = details;
                        preview["artifact"] = json!(summary.artifact);
                        previews.push(preview);
                        let caption = match appearance {
                            Some(appearance) => {
                                format!("HTML preview — {appearance} {width} CSS px")
                            }
                            None => format!("HTML preview — {width} CSS px"),
                        };
                        media.push(pioneer_provider::MessageContentPart::text(caption));
                        media.push(pioneer_provider::MessageContentPart::image(
                            resolved.attachment.clone(),
                        ));
                        let mut image = json!({"width":width,"attachment":resolved.attachment});
                        if let Some(appearance) = appearance {
                            image["appearance"] = appearance.into();
                        }
                        images.push(image);
                    }
                    let mut value = json!({"source":input.artifact,"previews":previews,"llm_context":{"images":images}});
                    if !legacy {
                        value["formatVersion"] = 2.into();
                    }
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
                let turn_items = self
                    .crud_store
                    .list_turn_items_by_type_for_turns(&[turn.to_owned()], "agent_message")
                    .await;
                if let Ok(items) = turn_items {
                    if let Some(item) = items.get(turn).and_then(|items| {
                        items.iter().rev().find(|item| {
                            matches!(
                                item,
                                pioneer_protocol::TurnItem::AgentMessage {
                                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                                    ..
                                }
                            )
                        })
                    }) {
                        self.notify_semantic_timeline_item_changed(
                            workspace,
                            thread,
                            turn,
                            item,
                            Some("completed"),
                        )
                        .await;
                    }
                }
                if let Ok(Some(execution)) = self
                    .crud_store
                    .get_task_run_turn_by_turn(thread, turn)
                    .await
                {
                    if let Ok(deliveries) = self
                        .crud_store
                        .list_task_deliveries(pioneer_protocol::TaskDeliveriesParams {
                            workspace_id: workspace.into(),
                            run_id: Some(execution.run_id),
                            limit: Some(200),
                            ..Default::default()
                        })
                        .await
                    {
                        for delivery in deliveries.deliveries {
                            let (Some(target), Some(target_turn)) = (
                                delivery.target_thread_id.as_deref(),
                                delivery.delivered_turn_id.as_deref(),
                            ) else {
                                continue;
                            };
                            let item_id =
                                pioneer_protocol::task_delivery_result_item_id(&delivery.id);
                            if self
                                .html_publications_for_agent_response(
                                    workspace,
                                    target,
                                    target_turn,
                                    &item_id,
                                )
                                .await
                                .is_ok_and(|publications| {
                                    publications.iter().any(|publication| {
                                        publication.artifact.artifact_id == artifact
                                    })
                                })
                            {
                                if let Ok(Some(item)) =
                                    self.crud_store.get_turn_item(target_turn, &item_id).await
                                {
                                    self.notify_semantic_timeline_item_changed(
                                        workspace,
                                        target,
                                        target_turn,
                                        &item,
                                        Some("completed"),
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                }
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
        format_version: u32,
        details: Value,
        cancel: &CancellationToken,
    ) -> Result<ArtifactSummary, ToolError> {
        let generation = self.html_authorize(context).await?;
        let identity = html_identity(context, call, index);
        let mut metadata = BTreeMap::new();
        metadata.insert("htmlTool".into(), json!({"version":format_version,"publication":publication,"turnId":context.turn_id,"callId":call,"index":index,"title":title,"details":details}));
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
        let activity = tokio::select! { biased;
            _ = invocation.cancellation.cancelled() => return Err(ToolError::cancelled("HTML invocation cancelled")),
            guard = self.processor.artifact_service.html_preview_activity() => guard,
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
        Ok(Box::new(HtmlOutput {
            inner: FunctionToolOutput::with_payload("HTML tool completed", true, value),
            _activity: activity,
        }))
    }
}

// Keep temporary bytes through native ToolOutput consumption. Durable turn
// context/snapshots then protect the subsequent provider request and recovery.
struct HtmlOutput {
    inner: FunctionToolOutput,
    _activity: tokio::sync::OwnedRwLockReadGuard<()>,
}
impl ToolOutput for HtmlOutput {
    fn success(&self) -> bool {
        self.inner.success()
    }
    fn raw_text(&self) -> String {
        self.inner.raw_text()
    }
    fn raw_json(&self) -> Value {
        self.inner.raw_json()
    }
    fn to_model_input_item(
        &self,
        call_id: &str,
        tool_name: &str,
    ) -> pioneer_provider::ModelInputItem {
        self.inner.to_model_input_item(call_id, tool_name)
    }
}

/// Theme appears before authored scripts and content. No credentials, source URL,
/// filesystem access or Gateway/native RPC is injected into the document.
fn prepared_document(html: &str, dark: bool) -> String {
    let theme = theme::standard_theme_css(dark);
    format!(
        "<!doctype html><meta name=viewport content=\"width=device-width,initial-scale=1\"><style>{theme}html,body{{margin:0;background:var(--pioneer-background);color:var(--pioneer-foreground)}}</style>{html}"
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

impl super::MessageProcessor {
    pub(super) async fn html_publications_for_response_turn(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> anyhow::Result<Vec<pioneer_protocol::HtmlPublication>> {
        let page = self
            .artifact_service
            .list_artifacts(
                workspace,
                pioneer_artifacts::ArtifactListFilter {
                    thread_id: Some(thread.to_owned()),
                    turn_id: Some(turn.to_owned()),
                    include_deleted: true,
                    created_by_actor_id: Some(pioneer_tools::html::HTML_RENDER.into()),
                    limit: Some(200),
                    ..Default::default()
                },
            )
            .await?;
        if page.next_cursor.is_some() {
            return Err(anyhow::anyhow!(
                "turn artifact publication page exceeds timeline delivery budget"
            ));
        }
        let mut publications = Vec::new();
        for mut summary in page.items {
            let exact = summary
                .bindings
                .iter()
                .find(|binding| {
                    binding.thread_id.as_deref() == Some(thread)
                        && binding.turn_id.as_deref() == Some(turn)
                        && binding.binding_kind == pioneer_protocol::ArtifactBindingKind::ToolOutput
                        && binding.direction == pioneer_protocol::ArtifactBindingDirection::Output
                        && binding.role == Some(pioneer_protocol::ArtifactRole::Tool)
                })
                .and_then(|binding| binding.version_id.clone());
            if exact != summary.artifact.version_id {
                summary = self
                    .artifact_service
                    .get_artifact(workspace, &summary.artifact.artifact_id, exact.as_deref())
                    .await?;
            }
            let publication =
                pioneer_protocol::HtmlPublication::from_summary(&summary, thread, turn);
            let Some(publication) = publication else {
                continue;
            };
            let order = summary
                .bindings
                .iter()
                .filter(|b| {
                    b.thread_id.as_deref() == Some(thread)
                        && b.turn_id.as_deref() == Some(turn)
                        && b.tool_call_id.as_deref() == Some(publication.call_id.as_str())
                        && b.version_id == publication.artifact.version_id
                        && b.binding_kind == pioneer_protocol::ArtifactBindingKind::ToolOutput
                        && b.direction == pioneer_protocol::ArtifactBindingDirection::Output
                        && b.role == Some(pioneer_protocol::ArtifactRole::Tool)
                })
                .filter_map(|b| b.item_index)
                .min()
                .unwrap_or(i64::MAX);
            // The original binding is immutable; CLI echoes/projection rebuilds
            // must not move documents already attached to an agent response.
            publications.push((
                (
                    order,
                    summary.created_at,
                    publication.call_id.clone(),
                    publication.artifact.artifact_id.clone(),
                ),
                publication,
            ));
        }
        publications.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(publications
            .into_iter()
            .map(|(_, publication)| publication)
            .collect())
    }
}

impl MessageProcessor {
    /// Called only for an existing canonical final response block. Delivery
    /// supplies the destination; immutable source ownership is never rewritten.
    pub(super) async fn html_publications_for_agent_response(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> anyhow::Result<Vec<pioneer_protocol::HtmlPublication>> {
        let Some(delivery_id) = pioneer_protocol::task_delivery_id_from_result_item_id(item) else {
            return self
                .html_publications_for_response_turn(workspace, thread, turn)
                .await;
        };
        // The same committed response item that renders final text is required.
        // Delivery preparation alone must not expose pending or cancelled content.
        if !self
            .crud_store
            .get_turn_item(turn, item)
            .await?
            .is_some_and(|item| {
                matches!(
                    item,
                    pioneer_protocol::TurnItem::AgentMessage {
                        phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                        ..
                    }
                )
            })
        {
            return Ok(Vec::new());
        }
        let Some(delivery) = self.crud_store.get_task_delivery(delivery_id).await? else {
            return Ok(Vec::new());
        };
        if delivery.workspace_id != workspace
            || delivery.target_thread_id.as_deref() != Some(thread)
            || delivery.error_snapshot.is_some()
            || delivery
                .delivered_turn_id
                .as_deref()
                .is_some_and(|id| id != turn)
        {
            return Ok(Vec::new());
        }
        let Some(result) = &delivery.result_snapshot else {
            return Ok(Vec::new());
        };
        let mut sources = Vec::new();
        for artifact in &result.artifacts {
            if let Some(pioneer_protocol::TaskValue::Object(metadata)) = &artifact.metadata {
                if let (
                    Some(pioneer_protocol::TaskValue::String(source_thread)),
                    Some(pioneer_protocol::TaskValue::String(source_turn)),
                ) = (
                    metadata.get("htmlSourceThreadId"),
                    metadata.get("htmlSourceTurnId"),
                ) {
                    sources.push((source_thread.clone(), source_turn.clone()));
                }
            }
        }
        // Durable pre-integration deliveries already pin their source item in
        // the fallback result. Recover on bounded reads, without rewriting DB.
        let legacy = result.artifacts.is_empty();
        if legacy {
            if let Some(pioneer_protocol::TaskValue::Object(data)) = &result.data {
                if data.get("fallbackUsed") != Some(&pioneer_protocol::TaskValue::Bool(true))
                    || data.get("schemaValid") != Some(&pioneer_protocol::TaskValue::Bool(false))
                {
                    return Ok(Vec::new());
                }
                if let (
                    Some(pioneer_protocol::TaskValue::String(source_thread)),
                    Some(pioneer_protocol::TaskValue::String(source_turn)),
                    Some(pioneer_protocol::TaskValue::String(source_item)),
                ) = (
                    data.get("sourceThreadId"),
                    data.get("sourceTurnId"),
                    data.get("sourceItemId"),
                ) {
                    if self
                        .crud_store
                        .get_turn_item(source_turn, source_item)
                        .await?
                        .is_some_and(|item| {
                            matches!(
                                item,
                                pioneer_protocol::TurnItem::AgentMessage {
                                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                                    ..
                                }
                            )
                        })
                    {
                        sources.push((source_thread.clone(), source_turn.clone()));
                    }
                }
            }
        }
        sources.sort();
        sources.dedup();
        let mut publications = Vec::new();
        for (source_thread, source_turn) in sources {
            let Some(execution) = self
                .crud_store
                .get_task_run_turn_by_turn(&source_thread, &source_turn)
                .await?
            else {
                continue;
            };
            if execution.run_id != delivery.run_id
                || execution.task_id != delivery.task_id
                || execution.kind == pioneer_protocol::TaskRunTurnKind::Review
            {
                continue;
            }
            if legacy
                && !self
                    .crud_store
                    .get_task_result_candidate_by_turn(&execution.id)
                    .await?
                    .is_some_and(|candidate| {
                        candidate.task_id == delivery.task_id
                            && candidate.run_id == delivery.run_id
                            && candidate.thread_id == source_thread
                            && candidate.turn_id == source_turn
                            && candidate.result.as_ref() == Some(result)
                    })
            {
                continue;
            }
            // Approval/cancellation visibility belongs to the committed delivery
            // response, not mutable candidate status after that response exists.
            for publication in self
                .html_publications_for_response_turn(workspace, &source_thread, &source_turn)
                .await?
            {
                if legacy
                    || result.artifacts.iter().any(|artifact| {
                        artifact.artifact_id.as_ref() == Some(&publication.artifact.artifact_id)
                            && artifact.version_id == publication.artifact.version_id
                    })
                {
                    publications.push(publication);
                }
            }
        }
        Ok(publications)
    }
}
