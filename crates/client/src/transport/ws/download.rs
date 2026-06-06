//! WebSocket binary download helpers.

use super::frames::sha256_bytes;
use anyhow::{Context as _, Result, bail};
use pioneer_protocol::ArtifactDownloadChunkHeader;
use std::{collections::HashMap, sync::mpsc::Sender};

pub const ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC: &[u8; 4] = b"ARTD";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDownloadChunkPayload {
    pub header: ArtifactDownloadChunkHeader,
    pub bytes: Vec<u8>,
}

pub fn artifact_download_chunk_key(download_id: &str, offset: u64) -> String {
    format!("{download_id}:{offset}")
}

pub fn parse_artifact_download_chunk_frame(frame: &[u8]) -> Result<ArtifactDownloadChunkPayload> {
    if frame.len() < 8 {
        bail!("artifact download frame is too short");
    }
    if &frame[0..4] != ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC {
        bail!("artifact download frame has invalid magic");
    }
    let header_len = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
    let header_start = 8usize;
    let header_end = header_start.saturating_add(header_len);
    if header_end > frame.len() {
        bail!("artifact download frame header length exceeds frame length");
    }
    let header =
        serde_json::from_slice::<ArtifactDownloadChunkHeader>(&frame[header_start..header_end])
            .context("failed to parse artifact download frame header")?;
    let bytes = frame[header_end..].to_vec();
    if header.len != u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
        bail!("artifact download frame chunk length mismatch");
    }
    let actual_sha256 = sha256_bytes(bytes.as_slice());
    if header.chunk_sha256 != actual_sha256 {
        bail!("artifact download frame chunk sha256 mismatch");
    }
    Ok(ArtifactDownloadChunkPayload { header, bytes })
}

pub fn process_artifact_download_binary_frame(
    frame: &[u8],
    pending_download_chunks: &mut HashMap<
        String,
        Sender<std::result::Result<ArtifactDownloadChunkPayload, String>>,
    >,
) -> bool {
    if frame.len() < 4 || &frame[0..4] != ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC {
        return false;
    }

    let payload = match parse_artifact_download_chunk_frame(frame) {
        Ok(payload) => payload,
        Err(error) => {
            for (_, response_tx) in pending_download_chunks.drain() {
                let _ = response_tx.send(Err(format!("{error:#}")));
            }
            return true;
        }
    };
    let key =
        artifact_download_chunk_key(payload.header.download_id.as_str(), payload.header.offset);
    if let Some(response_tx) = pending_download_chunks.remove(&key) {
        let _ = response_tx.send(Ok(payload));
    }
    true
}

#[allow(clippy::too_many_arguments)]
pub fn validate_artifact_download_chunk_payload(
    payload: &ArtifactDownloadChunkPayload,
    workspace_id: &str,
    download_id: &str,
    artifact_id: &str,
    version_id: &str,
    offset: u64,
    len: u64,
    total_size_bytes: u64,
) -> Result<()> {
    let header = &payload.header;
    if header.workspace_id != workspace_id
        || header.download_id != download_id
        || header.artifact_id != artifact_id
        || header.version_id != version_id
    {
        bail!("artifact download frame identity mismatch");
    }
    if header.offset != offset || header.len != len {
        bail!("artifact download frame range mismatch");
    }
    if header.total_size_bytes != total_size_bytes {
        bail!("artifact download frame total size mismatch");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn download_parse_valid_artd_frame() {
        let frame = test_frame("download_1", 0, b"hello", true);

        let payload = parse_artifact_download_chunk_frame(frame.as_slice()).expect("parse frame");

        assert_eq!(payload.header.download_id, "download_1");
        assert_eq!(payload.header.offset, 0);
        assert_eq!(payload.bytes, b"hello");
    }

    #[test]
    fn download_rejects_wrong_magic() {
        let mut frame = test_frame("download_1", 0, b"hello", true);
        frame[0..4].copy_from_slice(b"NOPE");

        assert!(parse_artifact_download_chunk_frame(frame.as_slice()).is_err());
    }

    #[test]
    fn download_rejects_wrong_chunk_sha() {
        let header = json!({
            "workspace_id": "ws_1",
            "download_id": "download_1",
            "artifact_id": "art_1",
            "version_id": "ver_1",
            "offset": 0,
            "len": 5,
            "total_size_bytes": 5,
            "chunk_sha256": "0".repeat(64),
            "final_chunk": true
        });
        let header_bytes = serde_json::to_vec(&header).unwrap();
        let mut frame = Vec::new();
        frame.extend_from_slice(ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC);
        frame.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        frame.extend_from_slice(header_bytes.as_slice());
        frame.extend_from_slice(b"hello");

        assert!(parse_artifact_download_chunk_frame(frame.as_slice()).is_err());
    }

    #[test]
    fn download_rejects_wrong_offset_or_download_id() {
        let payload = parse_artifact_download_chunk_frame(
            test_frame("download_1", 4, b"hello", false).as_slice(),
        )
        .expect("parse frame");

        assert!(
            validate_artifact_download_chunk_payload(
                &payload,
                "ws_1",
                "download_2",
                "art_1",
                "ver_1",
                4,
                5,
                9,
            )
            .is_err()
        );
        assert!(
            validate_artifact_download_chunk_payload(
                &payload,
                "ws_1",
                "download_1",
                "art_1",
                "ver_1",
                0,
                5,
                9,
            )
            .is_err()
        );
    }

    #[test]
    fn download_chunk_key_matches_desktop_waiter_contract() {
        assert_eq!(
            artifact_download_chunk_key("download_1", 12),
            "download_1:12"
        );
    }

    #[test]
    fn download_process_routes_frame_to_matching_waiter() {
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let mut pending =
            HashMap::from([(artifact_download_chunk_key("download_1", 0), response_tx)]);
        let frame = test_frame("download_1", 0, b"hello", true);

        assert!(process_artifact_download_binary_frame(
            frame.as_slice(),
            &mut pending
        ));

        let payload = response_rx.recv().expect("routed payload").expect("ok");
        assert_eq!(payload.bytes, b"hello");
        assert!(pending.is_empty());
    }

    fn test_frame(download_id: &str, offset: u64, chunk: &[u8], final_chunk: bool) -> Vec<u8> {
        let header = ArtifactDownloadChunkHeader {
            workspace_id: "ws_1".to_owned(),
            download_id: download_id.to_owned(),
            artifact_id: "art_1".to_owned(),
            version_id: "ver_1".to_owned(),
            offset,
            len: chunk.len() as u64,
            total_size_bytes: offset + chunk.len() as u64,
            chunk_sha256: sha256_bytes(chunk),
            final_chunk,
        };
        let header_bytes = serde_json::to_vec(&header).unwrap();
        let mut frame = Vec::new();
        frame.extend_from_slice(ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC);
        frame.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        frame.extend_from_slice(header_bytes.as_slice());
        frame.extend_from_slice(chunk);
        frame
    }
}
