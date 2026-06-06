//! Skill upload lifecycle.

use super::archive::SkillUploadArchive;
use anyhow::{Result, anyhow, bail};
use pioneer_protocol::{
    SkillArchiveFormat, SkillsUploadAbortParams, SkillsUploadChunkAckNotification,
    SkillsUploadFinishParams, SkillsUploadFinishResponse, SkillsUploadStartParams,
    SkillsUploadStartResponse,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillUploadProgress {
    pub label: String,
    pub sent_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillUploadChunk {
    pub offset: usize,
    pub offset_bytes: u64,
    pub next_offset: usize,
    pub next_offset_bytes: u64,
    pub bytes: Vec<u8>,
}

pub fn skill_upload_progress(
    label: impl Into<String>,
    sent_bytes: u64,
    total_bytes: u64,
) -> SkillUploadProgress {
    SkillUploadProgress {
        label: label.into(),
        sent_bytes,
        total_bytes,
    }
}

pub fn archive_compressed_size(archive: &SkillUploadArchive) -> Result<u64> {
    u64::try_from(archive.bytes.len()).map_err(|_| anyhow!("skill archive size overflow"))
}

pub fn skills_upload_start_params(
    workspace_id: impl Into<String>,
    archive: &SkillUploadArchive,
) -> Result<SkillsUploadStartParams> {
    Ok(SkillsUploadStartParams {
        workspace_id: workspace_id.into(),
        file_name: archive.file_name.clone(),
        archive_format: SkillArchiveFormat::TarGz,
        compressed_size_bytes: archive_compressed_size(archive)?,
        uncompressed_size_hint_bytes: Some(archive.uncompressed_size_bytes),
        sha256: archive.sha256.clone(),
    })
}

pub fn skill_upload_chunk_size(start: &SkillsUploadStartResponse) -> Result<usize> {
    usize::try_from(
        start
            .recommended_chunk_size_bytes
            .min(start.max_chunk_size_bytes)
            .max(1),
    )
    .map_err(|_| anyhow!("gateway upload chunk size overflow"))
}

pub fn next_skill_upload_chunk(
    bytes: &[u8],
    offset: usize,
    chunk_size: usize,
) -> Result<Option<SkillUploadChunk>> {
    if offset >= bytes.len() {
        return Ok(None);
    }
    if chunk_size == 0 {
        bail!("skill upload chunk size must be positive");
    }

    let end = offset.saturating_add(chunk_size).min(bytes.len());
    let offset_bytes =
        u64::try_from(offset).map_err(|_| anyhow!("skill upload offset overflow"))?;
    let next_offset_bytes =
        u64::try_from(end).map_err(|_| anyhow!("skill upload next offset overflow"))?;

    Ok(Some(SkillUploadChunk {
        offset,
        offset_bytes,
        next_offset: end,
        next_offset_bytes,
        bytes: bytes[offset..end].to_vec(),
    }))
}

pub fn validate_skill_upload_chunk_ack(
    ack: &SkillsUploadChunkAckNotification,
    expected_next_offset: u64,
) -> Result<()> {
    if ack.next_offset != expected_next_offset {
        bail!(
            "gateway acknowledged unexpected skill upload offset {}",
            ack.next_offset
        );
    }
    Ok(())
}

pub fn skills_upload_finish_params(
    workspace_id: impl Into<String>,
    upload_id: impl Into<String>,
) -> SkillsUploadFinishParams {
    SkillsUploadFinishParams {
        workspace_id: workspace_id.into(),
        upload_id: upload_id.into(),
    }
}

pub fn validate_skill_upload_finish_response(finish: &SkillsUploadFinishResponse) -> Result<()> {
    if finish.status != "finalized" {
        bail!(
            "gateway returned unexpected skill upload status {}",
            finish.status
        );
    }
    Ok(())
}

pub fn skills_upload_abort_params(
    workspace_id: impl Into<String>,
    upload_id: impl Into<String>,
) -> SkillsUploadAbortParams {
    SkillsUploadAbortParams {
        workspace_id: workspace_id.into(),
        upload_id: upload_id.into(),
    }
}

pub fn ensure_skill_upload_not_cancelled(cancelled: bool) -> Result<()> {
    if cancelled {
        return Err(anyhow!("skill upload cancelled"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive(bytes: Vec<u8>) -> SkillUploadArchive {
        SkillUploadArchive {
            file_name: "skill.tar.gz".to_owned(),
            bytes,
            sha256: "abc123".to_owned(),
            uncompressed_size_bytes: 100,
        }
    }

    fn start(recommended: u64, max: u64) -> SkillsUploadStartResponse {
        SkillsUploadStartResponse {
            upload_id: "upload".to_owned(),
            recommended_chunk_size_bytes: recommended,
            max_chunk_size_bytes: max,
            max_compressed_size_bytes: 10_000,
            max_uncompressed_size_bytes: 20_000,
            expires_at_unix: 1,
        }
    }

    #[test]
    fn upload_start_params_use_archive_metadata() {
        let archive = archive(vec![1, 2, 3]);
        let params = skills_upload_start_params("workspace", &archive).expect("params");

        assert_eq!(params.workspace_id, "workspace");
        assert_eq!(params.file_name, "skill.tar.gz");
        assert_eq!(params.archive_format, SkillArchiveFormat::TarGz);
        assert_eq!(params.compressed_size_bytes, 3);
        assert_eq!(params.uncompressed_size_hint_bytes, Some(100));
        assert_eq!(params.sha256, "abc123");
    }

    #[test]
    fn upload_chunk_size_uses_recommended_capped_by_max() {
        assert_eq!(skill_upload_chunk_size(&start(8, 5)).expect("size"), 5);
        assert_eq!(skill_upload_chunk_size(&start(0, 0)).expect("size"), 1);
    }

    #[test]
    fn upload_chunks_report_offsets_and_bytes() {
        let chunk = next_skill_upload_chunk(&[1, 2, 3, 4, 5], 2, 2)
            .expect("chunk")
            .expect("some");

        assert_eq!(chunk.offset, 2);
        assert_eq!(chunk.offset_bytes, 2);
        assert_eq!(chunk.next_offset, 4);
        assert_eq!(chunk.next_offset_bytes, 4);
        assert_eq!(chunk.bytes, vec![3, 4]);
        assert!(
            next_skill_upload_chunk(&[1, 2, 3], 3, 2)
                .expect("end")
                .is_none()
        );
    }

    #[test]
    fn upload_ack_finish_and_cancel_validation() {
        let ack = SkillsUploadChunkAckNotification {
            upload_id: "upload".to_owned(),
            offset: 0,
            len: 3,
            received_bytes: 3,
            next_offset: 3,
        };
        validate_skill_upload_chunk_ack(&ack, 3).expect("ack");
        assert!(validate_skill_upload_chunk_ack(&ack, 4).is_err());

        let finish = SkillsUploadFinishResponse {
            upload_id: "upload".to_owned(),
            status: "finalized".to_owned(),
            sha256: "abc123".to_owned(),
            compressed_size_bytes: 3,
        };
        validate_skill_upload_finish_response(&finish).expect("finish");

        assert!(ensure_skill_upload_not_cancelled(false).is_ok());
        assert!(ensure_skill_upload_not_cancelled(true).is_err());
    }

    #[test]
    fn finish_and_abort_params_preserve_workspace_and_upload() {
        let finish = skills_upload_finish_params("workspace", "upload");
        assert_eq!(finish.workspace_id, "workspace");
        assert_eq!(finish.upload_id, "upload");

        let abort = skills_upload_abort_params("workspace", "upload");
        assert_eq!(abort.workspace_id, "workspace");
        assert_eq!(abort.upload_id, "upload");
    }
}
