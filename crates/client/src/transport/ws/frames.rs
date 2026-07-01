//! WebSocket binary frame helpers.

use anyhow::{Context as _, Result, bail};
use pioneer_protocol::{
    ArtifactUploadChunkHeader, SkillsUploadChunkHeader, VoiceAudioFormat, VoiceChunkFrameHeader,
    encode_voice_chunk_frame,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::mpsc::Sender};

pub const SKILL_UPLOAD_FRAME_MAGIC: &[u8; 4] = b"PSU1";
pub const ARTIFACT_UPLOAD_FRAME_MAGIC: &[u8; 4] = b"ARTU";

pub fn upload_chunk_key(upload_id: &str, offset: u64) -> String {
    format!("{upload_id}:{offset}")
}

pub fn fail_pending_transfer_chunks<T>(
    pending_chunks: &mut HashMap<String, Sender<std::result::Result<T, String>>>,
    error: &str,
) {
    for (_, response_tx) in pending_chunks.drain() {
        let _ = response_tx.send(Err(error.to_owned()));
    }
}

pub fn encode_skill_upload_chunk_frame(
    workspace_id: String,
    upload_id: String,
    offset: u64,
    chunk: &[u8],
) -> Result<Vec<u8>> {
    validate_upload_chunk_frame_inputs("skill", workspace_id.as_str(), upload_id.as_str(), chunk)?;

    let header = SkillsUploadChunkHeader {
        workspace_id,
        upload_id,
        offset,
        len: u64::try_from(chunk.len()).context("skill upload chunk length overflow")?,
        chunk_sha256: Some(sha256_bytes(chunk)),
    };

    encode_chunk_frame(
        SKILL_UPLOAD_FRAME_MAGIC,
        &header,
        chunk,
        "skill upload chunk header",
    )
}

pub fn encode_artifact_upload_chunk_frame(
    workspace_id: String,
    upload_id: String,
    offset: u64,
    chunk: &[u8],
) -> Result<Vec<u8>> {
    validate_upload_chunk_frame_inputs(
        "artifact",
        workspace_id.as_str(),
        upload_id.as_str(),
        chunk,
    )?;

    let header = ArtifactUploadChunkHeader {
        workspace_id,
        upload_id,
        offset,
        len: u64::try_from(chunk.len()).context("artifact upload chunk length overflow")?,
        chunk_sha256: Some(sha256_bytes(chunk)),
    };

    encode_chunk_frame(
        ARTIFACT_UPLOAD_FRAME_MAGIC,
        &header,
        chunk,
        "artifact upload chunk header",
    )
}

pub fn encode_voice_audio_chunk_frame(
    session_id: String,
    sequence: u64,
    audio_format: VoiceAudioFormat,
    captured_at_unix_ms: Option<u64>,
    duration_ms: Option<u32>,
    pcm_chunk: &[u8],
) -> Result<Vec<u8>> {
    let header = VoiceChunkFrameHeader {
        session_id,
        sequence,
        sample_rate_hz: audio_format.sample_rate_hz,
        channels: audio_format.channels,
        encoding: audio_format.encoding,
        payload_len: 0,
        captured_at_unix_ms,
        duration_ms,
    };

    Ok(encode_voice_chunk_frame(header, pcm_chunk)?)
}

fn encode_chunk_frame<THeader: Serialize>(
    magic: &[u8; 4],
    header: &THeader,
    chunk: &[u8],
    header_context: &str,
) -> Result<Vec<u8>> {
    let header_bytes =
        serde_json::to_vec(header).with_context(|| format!("failed to encode {header_context}"))?;
    let header_len = u32::try_from(header_bytes.len())
        .with_context(|| format!("{header_context} is too large"))?;

    let mut payload = Vec::with_capacity(magic.len() + 4 + header_bytes.len() + chunk.len());
    payload.extend_from_slice(magic);
    payload.extend_from_slice(&header_len.to_be_bytes());
    payload.extend_from_slice(header_bytes.as_slice());
    payload.extend_from_slice(chunk);
    Ok(payload)
}

fn validate_upload_chunk_frame_inputs(
    kind: &str,
    workspace_id: &str,
    upload_id: &str,
    chunk: &[u8],
) -> Result<()> {
    if workspace_id.trim().is_empty() {
        bail!("workspace_id is required for {kind} upload chunk");
    }
    if upload_id.trim().is_empty() {
        bail!("upload_id is required for {kind} upload chunk");
    }
    if chunk.is_empty() {
        bail!("{kind} upload chunk cannot be empty");
    }
    Ok(())
}

pub(crate) fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_encode_skill_upload_chunk_frame_uses_psu1_magic_and_header() {
        let frame = encode_skill_upload_chunk_frame(
            "ws_1".to_owned(),
            "skill_upload_1".to_owned(),
            7,
            b"hello",
        )
        .expect("encode frame");

        assert_eq!(&frame[0..4], SKILL_UPLOAD_FRAME_MAGIC);

        let header_len = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
        let header_end = 8 + header_len;
        let header: SkillsUploadChunkHeader =
            serde_json::from_slice(&frame[8..header_end]).expect("decode header");

        assert_eq!(header.workspace_id, "ws_1");
        assert_eq!(header.upload_id, "skill_upload_1");
        assert_eq!(header.offset, 7);
        assert_eq!(header.len, 5);
        assert_eq!(
            header.chunk_sha256.as_deref(),
            Some(sha256_bytes(b"hello").as_str())
        );
        assert_eq!(&frame[header_end..], b"hello");
    }

    #[test]
    fn frames_reject_invalid_skill_upload_chunk_inputs() {
        assert_eq!(
            encode_skill_upload_chunk_frame(" ".to_owned(), "upload_1".to_owned(), 0, b"hello")
                .expect_err("workspace required")
                .to_string(),
            "workspace_id is required for skill upload chunk"
        );
        assert_eq!(
            encode_skill_upload_chunk_frame("ws_1".to_owned(), " ".to_owned(), 0, b"hello")
                .expect_err("upload required")
                .to_string(),
            "upload_id is required for skill upload chunk"
        );
        assert_eq!(
            encode_skill_upload_chunk_frame("ws_1".to_owned(), "upload_1".to_owned(), 0, b"")
                .expect_err("chunk required")
                .to_string(),
            "skill upload chunk cannot be empty"
        );
    }

    #[test]
    fn frames_upload_chunk_key_matches_desktop_waiter_contract() {
        assert_eq!(upload_chunk_key("upload_1", 42), "upload_1:42");
    }

    #[test]
    fn frames_fail_pending_transfer_chunks_drains_and_notifies_waiters() {
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let (second_tx, second_rx) = std::sync::mpsc::channel();
        let mut pending = HashMap::from([
            ("upload_1:0".to_owned(), first_tx),
            ("upload_1:5".to_owned(), second_tx),
        ]);

        fail_pending_transfer_chunks::<()>(&mut pending, "disconnected");

        assert!(pending.is_empty());
        assert_eq!(
            first_rx.recv().expect("first error"),
            Err("disconnected".to_owned())
        );
        assert_eq!(
            second_rx.recv().expect("second error"),
            Err("disconnected".to_owned())
        );
    }

    #[test]
    fn frames_encode_voice_audio_chunk_frame_uses_voc1_magic_and_audio_header() {
        let frame = encode_voice_audio_chunk_frame(
            "voice_session_1".to_owned(),
            9,
            VoiceAudioFormat {
                sample_rate_hz: 16_000,
                channels: 1,
                encoding: pioneer_protocol::VoiceAudioEncoding::PcmS16Le,
            },
            Some(1_725_000_000_020),
            Some(20),
            b"pcm",
        )
        .expect("encode voice frame");

        assert_eq!(&frame[0..4], pioneer_protocol::VOICE_CHUNK_FRAME_MAGIC);

        let decoded =
            pioneer_protocol::decode_voice_chunk_frame(frame.as_slice()).expect("decode frame");
        assert_eq!(decoded.header.session_id, "voice_session_1");
        assert_eq!(decoded.header.sequence, 9);
        assert_eq!(decoded.header.sample_rate_hz, 16_000);
        assert_eq!(decoded.header.channels, 1);
        assert_eq!(decoded.header.payload_len, 3);
        assert_eq!(decoded.header.duration_ms, Some(20));
        assert_eq!(decoded.audio_payload, b"pcm");
    }

    #[test]
    fn frames_reject_invalid_voice_audio_chunk_inputs() {
        let format = VoiceAudioFormat {
            sample_rate_hz: 16_000,
            channels: 1,
            encoding: pioneer_protocol::VoiceAudioEncoding::PcmS16Le,
        };

        assert!(
            encode_voice_audio_chunk_frame(" ".to_owned(), 0, format, None, None, b"pcm")
                .expect_err("session required")
                .to_string()
                .contains("voice session_id is required")
        );
        assert!(
            encode_voice_audio_chunk_frame(
                "voice_session_1".to_owned(),
                0,
                format,
                None,
                None,
                b""
            )
            .expect_err("payload required")
            .to_string()
            .contains("voice audio payload cannot be empty")
        );
        assert!(
            encode_voice_audio_chunk_frame(
                "voice_session_1".to_owned(),
                0,
                VoiceAudioFormat {
                    sample_rate_hz: 0,
                    channels: 1,
                    encoding: pioneer_protocol::VoiceAudioEncoding::PcmS16Le,
                },
                None,
                None,
                b"pcm",
            )
            .expect_err("sample rate required")
            .to_string()
            .contains("voice audio format is invalid")
        );
    }

    #[test]
    fn frames_encode_artifact_upload_chunk_frame_uses_artu_magic_and_header() {
        let frame = encode_artifact_upload_chunk_frame(
            "ws_1".to_owned(),
            "artifact_upload_1".to_owned(),
            11,
            b"hello",
        )
        .expect("encode frame");

        assert_eq!(&frame[0..4], ARTIFACT_UPLOAD_FRAME_MAGIC);

        let header_len = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
        let header_end = 8 + header_len;
        let header: ArtifactUploadChunkHeader =
            serde_json::from_slice(&frame[8..header_end]).expect("decode header");

        assert_eq!(header.workspace_id, "ws_1");
        assert_eq!(header.upload_id, "artifact_upload_1");
        assert_eq!(header.offset, 11);
        assert_eq!(header.len, 5);
        assert_eq!(
            header.chunk_sha256.as_deref(),
            Some(sha256_bytes(b"hello").as_str())
        );
        assert_eq!(&frame[header_end..], b"hello");
    }

    #[test]
    fn frames_reject_invalid_artifact_upload_chunk_inputs() {
        assert_eq!(
            encode_artifact_upload_chunk_frame(" ".to_owned(), "upload_1".to_owned(), 0, b"hello")
                .expect_err("workspace required")
                .to_string(),
            "workspace_id is required for artifact upload chunk"
        );
        assert_eq!(
            encode_artifact_upload_chunk_frame("ws_1".to_owned(), " ".to_owned(), 0, b"hello")
                .expect_err("upload required")
                .to_string(),
            "upload_id is required for artifact upload chunk"
        );
        assert_eq!(
            encode_artifact_upload_chunk_frame("ws_1".to_owned(), "upload_1".to_owned(), 0, b"")
                .expect_err("chunk required")
                .to_string(),
            "artifact upload chunk cannot be empty"
        );
    }
}
