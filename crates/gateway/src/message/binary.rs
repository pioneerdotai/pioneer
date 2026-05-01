use super::*;
use crate::message::skills::SKILL_UPLOAD_CHUNK_FRAME_MAGIC;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewayBinaryFrameKind {
    SkillUploadChunk,
}

impl GatewayBinaryFrameKind {
    fn from_payload(payload: &[u8]) -> Option<Self> {
        if payload.starts_with(SKILL_UPLOAD_CHUNK_FRAME_MAGIC) {
            return Some(Self::SkillUploadChunk);
        }

        None
    }

    fn name(self) -> &'static str {
        match self {
            Self::SkillUploadChunk => "skills/upload/chunk",
        }
    }
}

impl MessageProcessor {
    pub(crate) async fn process_binary_frame(&self, connection_id: ConnectionId, payload: &[u8]) {
        let Some(kind) = GatewayBinaryFrameKind::from_payload(payload) else {
            warn!(
                connection_id,
                frame_len = payload.len(),
                "unknown gateway binary frame ignored"
            );
            return;
        };

        debug!(
            connection_id,
            frame_kind = kind.name(),
            frame_len = payload.len(),
            "processing gateway binary frame"
        );

        match kind {
            GatewayBinaryFrameKind::SkillUploadChunk => {
                self.process_skill_upload_chunk_frame(connection_id, payload)
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_frame_kind_detects_skill_upload_magic() {
        let mut payload = Vec::new();
        payload.extend_from_slice(SKILL_UPLOAD_CHUNK_FRAME_MAGIC);
        payload.extend_from_slice(&0u32.to_be_bytes());

        let kind = GatewayBinaryFrameKind::from_payload(payload.as_slice());

        assert_eq!(kind, Some(GatewayBinaryFrameKind::SkillUploadChunk));
        assert_eq!(kind.expect("kind").name(), "skills/upload/chunk");
    }

    #[test]
    fn binary_frame_kind_rejects_unknown_magic() {
        assert_eq!(GatewayBinaryFrameKind::from_payload(b"NOPE"), None);
    }
}
