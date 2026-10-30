use super::{ResponsesApiCompatibleProvider, replay};
use crate::attachments::{
    PreparedAttachment, PreparedAttachmentSource, PreparedProviderMessages, attachment_data_url,
    ensure_no_unrendered_attachments,
};
use crate::{ChatRequest, ProviderCapabilities, Role, ToolChoice};
use anyhow::{Result, bail, ensure};
use serde_json::{Value, json};

/// Preparation owns positional attachments and exact projected history. Upload
/// materialization replaces only these owned attachments; execution never prepares again.
pub(crate) struct PreparedRequest {
    pub(crate) request: ChatRequest,
    pub(crate) messages: PreparedProviderMessages,
    generation: crate::generation::Fields,
}
impl ResponsesApiCompatibleProvider {
    pub(crate) fn project(&self, mut request: ChatRequest) -> Result<ChatRequest> {
        request.messages = crate::history::project_messages_matching(
            &self.name,
            &request.model,
            None,
            &request.messages,
            crate::history::ProjectionProfile::Responses,
            |state| replay::compatible(state, &self.name, &request.model, &self.authority),
        )?;
        Ok(request)
    }
    pub(crate) async fn prepare(
        &self,
        request: ChatRequest,
        capabilities: &ProviderCapabilities,
    ) -> Result<PreparedRequest> {
        let request = crate::tools::policy::prepare_responses_request(&self.name, request)?;
        let request = self.project(request)?;
        let generation = crate::generation::responses_fields(&self.name, &request)?;
        // A compiled prompt is rendered exactly once as instructions. Remaining
        // system messages are retained only when no compiled prompt exists.
        let mut messages = crate::attachments::prepare_responses_messages_for_request_async(
            &self.name,
            capabilities,
            &request,
            &request.messages,
        )
        .await?;
        crate::tools::policy::prepare_responses_history(&self.name, &mut messages.messages)?;
        ensure_no_unrendered_attachments(&self.name, &messages)?;
        Ok(PreparedRequest {
            request,
            messages,
            generation,
        })
    }
    pub(crate) fn render(&self, prepared: &PreparedRequest, stream: bool) -> Result<Value> {
        let request = &prepared.request;
        let mut input = Vec::new();
        for (index, message) in prepared.messages.messages.iter().enumerate() {
            if request.compiled_prompt.is_some() && message.role == Role::System {
                continue;
            }
            let attachments = prepared
                .messages
                .attachments_for_message(index)
                .collect::<Vec<_>>();
            if message.role == Role::Assistant {
                if let Some(state) = &message.provider_replay_state {
                    ensure!(
                        replay::compatible(state, &self.name, &request.model, &self.authority),
                        "incompatible Responses continuation"
                    );
                    let output = replay::output(state)?;
                    let native_calls = output
                        .iter()
                        .filter(|item| item["type"] == "function_call")
                        .map(|item| crate::ProviderToolCall {
                            id: item["call_id"].as_str().unwrap_or_default().into(),
                            name: item["name"].as_str().unwrap_or_default().into(),
                            arguments: item["arguments"].as_str().unwrap_or_default().into(),
                        })
                        .collect::<Vec<_>>();
                    ensure!(
                        native_calls.as_slice()
                            == message.tool_calls.as_deref().unwrap_or_default(),
                        "Responses replay/common call bindings conflict"
                    );
                    input.extend(replay::input_items(output)?);
                    continue;
                }
                if let Some(reasoning) =
                    message.reasoning_content.as_ref().filter(|s| !s.is_empty())
                {
                    input.push(json!({"role":"assistant","content":format!("[Historical assistant reasoning]\n{reasoning}")}));
                }
                if !message.content.is_empty() {
                    input.push(json!({"role":"assistant","content":message.content}));
                }
                for call in message.tool_calls.iter().flatten() {
                    input.push(json!({"type":"function_call","call_id":call.id,"name":call.name,"arguments":call.arguments}));
                }
            } else if message.role == Role::Tool {
                let mut output = vec![json!({"type":"input_text","text":message.content})];
                for attachment in attachments {
                    output.push(media(attachment)?);
                }
                input.push(json!({"type":"function_call_output","call_id":message.tool_call_id,"output":output}));
            } else {
                let mut content = vec![json!({"type":"input_text","text":message.content})];
                for attachment in attachments {
                    content.push(media(attachment)?);
                }
                input.push(json!({"role":if message.role==Role::System {"system"} else {"user"},"content":content}));
            }
        }
        let mut body = json!({"model":request.model,"input":input,"store":false,"truncation":"disabled","include":["reasoning.encrypted_content"],"stream":stream});
        if let Some(prompt) = &request.compiled_prompt {
            if let Some(text) = prompt.full_system_text() {
                body["instructions"] = json!(text);
            }
        }
        if let Some(tools) = &request.tools {
            body["tools"]=json!(tools.iter().map(|tool|json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.parameters,"strict":false})).collect::<Vec<_>>());
        }
        if let Some(choice) = &request.tool_choice {
            body["tool_choice"] = match choice {
                ToolChoice::Auto => json!("auto"),
                ToolChoice::None => json!("none"),
                ToolChoice::Required => json!("required"),
                ToolChoice::Tool { name } => json!({"type":"function","name":name}),
            };
        }
        if let Some(parallel) = request.parallel_tool_calls {
            body["parallel_tool_calls"] = json!(parallel);
        }
        for (key, value) in &prepared.generation {
            body[key] = value.clone();
        }
        Ok(body)
    }
}
fn media(attachment: &PreparedAttachment) -> Result<Value> {
    match attachment.kind {
        crate::InputContentType::Image => Ok(
            json!({"type":"input_image","image_url":attachment_data_url(attachment)?,"detail":"auto"}),
        ),
        crate::InputContentType::File => {
            ensure!(
                attachment.mime_type == "application/pdf",
                "Responses file renderer accepts PDF only"
            );
            if let PreparedAttachmentSource::Reference { reference } = &attachment.source {
                Ok(json!({"type":"input_file","file_id":reference}))
            } else {
                Ok(
                    json!({"type":"input_file","filename":attachment.name,"file_data":attachment_data_url(attachment)?}),
                )
            }
        }
        _ => bail!("unsupported Responses media input"),
    }
}
