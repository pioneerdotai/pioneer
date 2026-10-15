use crate::CrudStore;
impl CrudStore {
    pub async fn record_tool_output(
        &self,
        id: &str,
        n: &pioneer_protocol::ItemDeltaNotification,
    ) -> anyhow::Result<()> {
        // Persist bounded immutable pieces, not a growing copy of the output.
        // A retry uses the same piece identities even after a partial write.
        const CHUNK_BYTES: usize = 128 * 1024;
        if n.delta.len() <= CHUNK_BYTES {
            return crate::repositories::tool_output::record_tool_output(self, id, n).await;
        }
        use sha2::{Digest, Sha256};
        let identity = serde_json::json!({
            "sourceSha256": hex::encode(Sha256::digest(n.delta.as_bytes())),
            "sourceBytes": n.delta.len(), "metadata": n.payload,
        });
        let mut offset = 0;
        while offset < n.delta.len() {
            let mut end = (offset + CHUNK_BYTES).min(n.delta.len());
            while !n.delta.is_char_boundary(end) {
                end -= 1;
            }
            let piece = pioneer_protocol::ItemDeltaNotification {
                workspace_id: n.workspace_id.clone(),
                thread_id: n.thread_id.clone(),
                turn_id: n.turn_id.clone(),
                item_id: n.item_id.clone(),
                delta: n.delta[offset..end].to_owned(),
                stream: n.stream.clone(),
                payload: if offset == 0 {
                    Some(identity.clone())
                } else {
                    None
                },
                markdown: None,
                markdown_version: None,
            };
            let piece_id = if offset == 0 {
                id.to_owned()
            } else {
                format!("{id}:{offset}")
            };
            crate::repositories::tool_output::record_tool_output(self, &piece_id, &piece).await?;
            offset = end;
        }
        Ok(())
    }
    pub async fn tool_output_page(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        after: i64,
    ) -> anyhow::Result<Vec<(i64, pioneer_entity::tool_output_chunk::Model)>> {
        crate::repositories::tool_output::tool_output_page(
            self, workspace, thread, turn, item, after,
        )
        .await
    }
}
