//! Byte-only ACP output adapter. URI hints never authorize filesystem or network IO.
use crate::{mcp_service::McpService, message::MessageProcessor};
use anyhow::{Context, Result, bail, ensure};
use base64::Engine;
use pioneer_artifacts::{ArtifactBindingTarget, IngestArtifactBytesRequest};
use pioneer_cli_agent_runtime::acp::schema::{ContentBlock, EmbeddedResourceResource};
use pioneer_protocol::*;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

impl McpService {
    pub(crate) async fn ingest_acp_output(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        identity: String,
        content: ContentBlock,
        cancel: CancellationToken,
    ) -> Result<ArtifactRef> {
        let processor = self
            .inner
            .native_tool_processor
            .read()
            .expect("processor bridge")
            .upgrade()
            .context("Artifact execution owner unavailable")?;
        processor
            .ingest_acp_output(workspace, thread, turn, item, identity, content, cancel)
            .await
    }
}
impl MessageProcessor {
    async fn ingest_acp_output(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        identity: String,
        content: ContentBlock,
        cancel: CancellationToken,
    ) -> Result<ArtifactRef> {
        let mut generation = None;
        for action in [
            crate::authorization::ResourceAction::ArtifactCreateThread,
            crate::authorization::ResourceAction::ArtifactBindThread,
        ] {
            let authorized = self
                .revalidate_tool_execution_authorization(workspace, thread, turn, None, action)
                .await?;
            let current = authorized.validated_policy_generation();
            ensure!(
                generation.is_none_or(|previous| previous == current),
                "Artifact authority changed"
            );
            generation = Some(current);
        }
        let (bytes, kind, mime) = tokio::task::spawn_blocking(move || decode(content)).await??;
        ensure!(!cancel.is_cancelled(), "ACP output cancelled");
        let summary = self
            .artifact_service
            .ingest_tool_bytes(
                IngestArtifactBytesRequest {
                    workspace_id: workspace.into(),
                    primary_thread_id: Some(thread.into()),
                    bytes,
                    display_name: "Agent output".into(),
                    kind,
                    mime_type: mime,
                    created_by_kind: ArtifactCreatedByKind::ExternalAgent,
                    created_by_actor_id: Some("acp_content".into()),
                    binding: Some(ArtifactBindingTarget {
                        thread_id: Some(thread.into()),
                        turn_id: Some(turn.into()),
                        turn_item_id: Some(item.into()),
                        message_id: None,
                        tool_call_id: None,
                        task_id: None,
                        task_run_id: None,
                        binding_kind: ArtifactBindingKind::AgentOutput,
                        direction: ArtifactBindingDirection::Output,
                        role: Some(ArtifactRole::Assistant),
                        item_index: None,
                    }),
                    metadata: BTreeMap::new(),
                },
                identity,
                generation.context("Artifact authority missing")?,
                cancel,
            )
            .await?;
        self.send_notification_to_thread_subscribers(
            thread,
            constants::events::ARTIFACT_CREATED,
            &ArtifactCreatedNotification {
                workspace_id: workspace.into(),
                artifact: summary.clone(),
            },
        )
        .await;
        self.send_thread_artifacts_changed_to_thread_and_ancestors(
            workspace,
            thread,
            vec![summary.artifact.artifact_id.clone()],
            "acp_output",
            chrono::Utc::now().timestamp(),
        )
        .await;
        Ok(summary.artifact)
    }
}
fn decode(content: ContentBlock) -> Result<(Vec<u8>, ArtifactKind, Option<String>)> {
    let (bytes, kind, mime) = match content {
        ContentBlock::Image(image) => (
            base64::engine::general_purpose::STANDARD.decode(image.data)?,
            ArtifactKind::Image,
            Some(image.mime_type),
        ),
        ContentBlock::Audio(audio) => (
            base64::engine::general_purpose::STANDARD.decode(audio.data)?,
            ArtifactKind::Audio,
            Some(audio.mime_type),
        ),
        ContentBlock::Resource(resource) => match resource.resource {
            EmbeddedResourceResource::TextResourceContents(text) => {
                (text.text.into_bytes(), ArtifactKind::Text, text.mime_type)
            }
            EmbeddedResourceResource::BlobResourceContents(blob) => (
                base64::engine::general_purpose::STANDARD.decode(blob.blob)?,
                ArtifactKind::File,
                blob.mime_type,
            ),
            _ => bail!("Unknown embedded content kind"),
        },
        _ => bail!("Not embedded media"),
    };
    ensure!(
        bytes.len() <= 8 * 1024 * 1024,
        "Embedded media exceeds byte limit"
    );
    Ok((bytes, kind, mime))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn embedded_text_does_not_dereference_its_uri_and_invalid_base64_is_rejected() {
        let content = serde_json::from_value(serde_json::json!({"type":"resource","resource":{"uri":"file:///unavailable/private","text":"exact bytes"}})).unwrap();
        let (bytes, kind, _) = decode(content).unwrap();
        assert_eq!(bytes, b"exact bytes");
        assert_eq!(kind, ArtifactKind::Text);
        assert!(
            decode(
                serde_json::from_value(
                    serde_json::json!({"type":"audio","mimeType":"audio/wav","data":"!!!"})
                )
                .unwrap()
            )
            .is_err()
        );
    }
}
