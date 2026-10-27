//! Media authority is established by the injected backend and artifact admission,
//! never by a tool name or a JSON field supplied by another tool.
use super::*;
use base64::Engine;
use pioneer_provider::{MessageAttachment, MessageContentPart, ModelInputItem};

#[async_trait]
pub trait ComputerUseImageSink: Send + Sync + std::fmt::Debug {
    async fn admit(
        &self,
        owner: &str,
        call_id: &str,
        index: usize,
        bytes: Vec<u8>,
        mime: &str,
        cancellation: CancellationToken,
    ) -> Result<MessageAttachment, ToolError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_provider::{AttachmentArtifactContext, AttachmentDataSource};

    #[derive(Debug, Default)]
    struct RecordingSink(std::sync::Mutex<Vec<Vec<u8>>>);
    #[async_trait]
    impl ComputerUseImageSink for RecordingSink {
        async fn admit(
            &self,
            _owner: &str,
            _call: &str,
            _index: usize,
            bytes: Vec<u8>,
            mime: &str,
            _cancellation: CancellationToken,
        ) -> Result<MessageAttachment, ToolError> {
            self.0.lock().unwrap().push(bytes);
            let mut image = MessageAttachment::from_path("/artifacts/original.png", mime);
            image.artifact = Some(AttachmentArtifactContext {
                workspace_id: "workspace".into(),
                artifact_id: "capture".into(),
                artifact_version_id: Some("v1".into()),
            });
            Ok(image)
        }
    }

    // Producer boundary: original byte transfer and metadata survive admission;
    // no claim is made here about ArtifactService's independently owned storage.
    #[tokio::test]
    async fn capture_admission_preserves_bytes_metadata_and_typed_media() {
        let sink = Arc::new(RecordingSink::default());
        let config = ComputerUseToolsConfig::default();
        config.image_admission.set(sink.clone());
        let raw = serde_json::json!({"content":[
            {"type":"text","text":"origin=(-8,3), scale=2"},
            {"type":"image","mimeType":"image/png","data":"AAECAw==","width":320,"height":240}
        ],"structuredContent":{"generation":7,"elementToken":"opaque","origin":{"x":-8,"y":3},"scale":2}});
        let output = trusted_output(
            raw.clone(),
            true,
            "capture".into(),
            &config.normalized(),
            "turn",
            "call",
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(*sink.0.lock().unwrap(), vec![vec![0, 1, 2, 3]]);
        let retained = output.raw_json();
        assert!(retained["content"][1].get("data").is_none());
        assert_eq!(retained["structuredContent"], raw["structuredContent"]);
        assert_eq!(retained["content"][1]["width"], 320);
        let message = output
            .to_model_input_item("call", "cua_get_window_state")
            .into_chat_message();
        assert_eq!(message.tool_call_id.as_deref(), Some("call"));
        let image = message
            .content_parts
            .iter()
            .find_map(|part| match part {
                MessageContentPart::Image { image } => Some(image),
                _ => None,
            })
            .expect("admitted typed image");
        assert_eq!(image.artifact.as_ref().unwrap().artifact_id, "capture");
        assert!(matches!(image.source, AttachmentDataSource::Path { .. }));
        assert!(message.content_parts.iter().any(|part| matches!(part, MessageContentPart::Text { text } if text.contains("elementToken"))));

        let missing = ComputerUseToolsConfig::default();
        assert!(
            trusted_output(
                raw,
                true,
                "capture".into(),
                &missing,
                "turn",
                "call",
                CancellationToken::new(),
                None,
            )
            .await
            .is_err()
        );
        assert!(
            FunctionToolOutput::with_payload("external", true, retained)
                .model_content_parts()
                .is_empty()
        );
    }
}

/// Shared by normalized configuration clones, including the agent manager.
#[derive(Clone, Default, Debug)]
pub struct ComputerUseImageAdmission(Arc<std::sync::RwLock<Option<Arc<dyn ComputerUseImageSink>>>>);
impl ComputerUseImageAdmission {
    pub fn set(&self, sink: Arc<dyn ComputerUseImageSink>) {
        *self.0.write().expect("CUA image admission lock") = Some(sink);
    }
}

struct ComputerUseOutput {
    inner: FunctionToolOutput,
    parts: Vec<MessageContentPart>,
    receipt: Option<Arc<dyn ToolDeliveryReceipt>>,
}
impl ToolOutput for ComputerUseOutput {
    fn delivery_receipt(&self) -> Option<Arc<dyn ToolDeliveryReceipt>> {
        self.receipt.clone()
    }
    fn is_computer_use_output(&self) -> bool {
        true
    }
    fn success(&self) -> bool {
        self.inner.success()
    }
    fn raw_text(&self) -> String {
        self.inner.raw_text()
    }
    fn raw_json(&self) -> Value {
        self.inner.raw_json()
    }
    fn model_content_parts(&self) -> Vec<MessageContentPart> {
        self.parts.clone()
    }
    fn to_model_input_item(&self, call_id: &str, tool_name: &str) -> ModelInputItem {
        let mut message = self
            .inner
            .to_model_input_item(call_id, tool_name)
            .into_chat_message();
        message.content_parts.extend(self.parts.clone());
        ModelInputItem::message(message)
    }
}

pub(super) async fn trusted_output(
    mut result: Value,
    success: bool,
    text: String,
    config: &ComputerUseToolsConfig,
    owner: &str,
    call_id: &str,
    cancellation: CancellationToken,
    receipt: Option<Arc<dyn ToolDeliveryReceipt>>,
) -> Result<Box<dyn ToolOutput>, ToolError> {
    let mut parts = Vec::new();
    let mut total = 0usize;
    let policy = pioneer_provider::default_attachment_pipeline_config();
    if let Some(content) = result.get_mut("content").and_then(Value::as_array_mut) {
        for (index, part) in content.iter_mut().enumerate() {
            if part.get("type").and_then(Value::as_str) != Some("image") {
                continue;
            }
            if parts.len() >= policy.max_attachments_per_request {
                return Err(ToolError::execution_failed(
                    "CUA image count exceeds media budget; do not replay operation",
                ));
            }
            let data = part.get("data").and_then(Value::as_str).ok_or_else(|| {
                ToolError::execution_failed("CUA image data missing; do not replay operation")
            })?;
            // Bound allocation before decoding. The normal provider byte ceiling
            // also bounds canonical history and CLI conversion of this output.
            let limit = policy
                .max_total_bytes_per_request
                .min(usize::try_from(config.max_total_bytes).unwrap_or(usize::MAX));
            let per_image = policy
                .max_bytes_per_attachment
                .min(limit.saturating_sub(total));
            if data.len()
                > per_image
                    .saturating_add(2)
                    .saturating_div(3)
                    .saturating_mul(4)
            {
                return Err(ToolError::execution_failed(
                    "CUA image exceeds media budget; do not replay operation",
                ));
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(data)
                .map_err(|_| {
                    ToolError::execution_failed(
                        "CUA image encoding invalid; do not replay operation",
                    )
                })?;
            total = total.saturating_add(bytes.len());
            if total > limit || bytes.len() > per_image {
                return Err(ToolError::execution_failed(
                    "CUA images exceed media budget; do not replay operation",
                ));
            }
            let mime = part
                .get("mimeType")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ToolError::execution_failed("CUA image MIME missing; do not replay operation")
                })?;
            let sink = config
                .image_admission
                .0
                .read()
                .expect("CUA image admission lock")
                .clone()
                .ok_or_else(|| {
                    ToolError::execution_failed(
                        "CUA artifact admission unavailable; do not replay operation",
                    )
                })?;
            let attachment = sink
                .admit(owner, call_id, index, bytes, mime, cancellation.clone())
                .await?;
            if attachment.artifact.is_none() {
                return Err(ToolError::execution_failed(
                    "CUA image admission returned no artifact identity",
                ));
            }
            // Preserve every geometry/token/metadata field; replace only binary
            // data with the exact admitted identity in the retained raw result.
            part.as_object_mut().expect("image object").remove("data");
            part["artifact"] = serde_json::to_value(&attachment.artifact).map_err(|_| {
                ToolError::execution_failed("CUA artifact identity serialization failed")
            })?;
            parts.push(MessageContentPart::image(attachment));
        }
    }
    if !parts.is_empty() {
        // Output summaries are deliberately content-free. Carry the genuine
        // capture metadata alongside the image through that projection; the
        // agent's normal result budget bounds text without guessing geometry.
        parts.insert(
            0,
            MessageContentPart::text(serde_json::to_string(&result).map_err(|_| {
                ToolError::execution_failed("CUA capture metadata serialization failed")
            })?),
        );
    }
    Ok(Box::new(ComputerUseOutput {
        inner: FunctionToolOutput::with_payload(text, success, result),
        parts,
        receipt,
    }))
}
