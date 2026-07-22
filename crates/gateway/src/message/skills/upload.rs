use super::*;
use anyhow::{Context, Result, anyhow, bail};
use flate2::read::GzDecoder;
use pioneer_crud::SkillUploadSessionRecord;
use pioneer_protocol::{
    SkillArchiveFormat, SkillsUploadAbortParams, SkillsUploadAbortResponse,
    SkillsUploadChunkAckNotification, SkillsUploadChunkHeader, SkillsUploadFinishParams,
    SkillsUploadFinishResponse, SkillsUploadStartParams, SkillsUploadStartResponse,
};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;
use tar::Archive;

pub(in crate::message) const SKILL_UPLOAD_CHUNK_FRAME_MAGIC: &[u8; 4] = b"PSU1";
const UPLOAD_STATUS_RECEIVING: &str = "receiving";
const UPLOAD_STATUS_FINALIZED: &str = "finalized";
const UPLOAD_STATUS_CONSUMED: &str = "consumed";
const UPLOAD_STATUS_ABORTED: &str = "aborted";
const UPLOAD_STATUS_EXPIRED: &str = "expired";

pub(super) struct MaterializedSkillSource {
    pub source_dir: PathBuf,
    pub cleanup_root: PathBuf,
    pub upload: SkillUploadSessionRecord,
}

#[derive(Debug, Clone)]
pub(super) struct SkillPackMemberSource {
    pub pack_member_key: String,
    pub source_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub(super) struct ValidatedSkillPackRoot {
    pub pack_name: String,
    pub members: Vec<SkillPackMemberSource>,
}

fn normalize_materialized_skill_frontmatter(source_dir: &Path) -> Result<()> {
    let skill_file = source_dir.join("SKILL.md");
    let raw = fs::read_to_string(skill_file.as_path())
        .with_context(|| format!("failed to read `{}`", skill_file.display()))?;
    let Some(normalized) =
        pioneer_skills::normalize_skill_markdown_plain_description(raw.as_str())?
    else {
        return Ok(());
    };
    fs::write(skill_file.as_path(), normalized)
        .with_context(|| format!("failed to normalize `{}`", skill_file.display()))?;
    Ok(())
}

impl MessageProcessor {
    pub(crate) async fn skills_upload_start(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillsUploadStartParams,
    ) {
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::SKILLS_UPLOAD_START,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let context = match self.skills_runtime_context(workspace_id.as_str()) {
            Ok(context) => context,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to resolve skills runtime context",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };
        self.cleanup_stale_skill_uploads(now_timestamp_secs()).await;

        if !matches!(params.archive_format, SkillArchiveFormat::TarGz) {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_UPLOAD_INVALID_REQUEST,
                    "unsupported skill archive format",
                    json!({"archive_format": params.archive_format.as_str()}),
                ),
            )
            .await;
            return;
        }

        if params.compressed_size_bytes == 0
            || params.compressed_size_bytes
                > u64::try_from(context.max_upload_compressed_bytes).unwrap_or(u64::MAX)
        {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_UPLOAD_SIZE_LIMIT,
                    "upload compressed size exceeds configured limit",
                    json!({
                        "compressed_size_bytes": params.compressed_size_bytes,
                        "max_compressed_size_bytes": context.max_upload_compressed_bytes,
                    }),
                ),
            )
            .await;
            return;
        }

        if let Some(uncompressed_hint) = params.uncompressed_size_hint_bytes
            && uncompressed_hint
                > u64::try_from(context.max_upload_uncompressed_bytes).unwrap_or(u64::MAX)
        {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_UPLOAD_SIZE_LIMIT,
                    "upload uncompressed size hint exceeds configured limit",
                    json!({
                        "uncompressed_size_hint_bytes": uncompressed_hint,
                        "max_uncompressed_size_bytes": context.max_upload_uncompressed_bytes,
                    }),
                ),
            )
            .await;
            return;
        }

        if !is_lower_hex_sha256(params.sha256.as_str()) {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_UPLOAD_INVALID_REQUEST,
                    "sha256 must be a lowercase hex SHA-256 digest",
                    json!({"sha256": params.sha256}),
                ),
            )
            .await;
            return;
        }

        let upload_id = pioneer_protocol::generate_id(21);
        let now = now_timestamp_secs();
        let expires_at = now.saturating_add(i64::try_from(context.upload_ttl_secs).unwrap_or(3600));
        let workspace_upload_dir = context
            .upload_root
            .join(sanitize_workspace_id_component(workspace_id.as_str()));
        let upload_dir = workspace_upload_dir.join(upload_id.as_str());
        let payload_path = upload_dir.join("payload.tar.gz");

        if let Err(error) = fs::create_dir_all(workspace_upload_dir.as_path())
            .and_then(|()| fs::create_dir(upload_dir.as_path()))
        {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to create upload directory",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }

        let record = SkillUploadSessionRecord {
            upload_id: upload_id.clone(),
            workspace_id: workspace_id.clone(),
            connection_id,
            status: UPLOAD_STATUS_RECEIVING.to_owned(),
            file_name: sanitize_file_name(params.file_name.as_str()),
            archive_format: params.archive_format.as_str().to_owned(),
            compressed_size_bytes: params.compressed_size_bytes,
            received_bytes: 0,
            sha256: params.sha256,
            payload_path: payload_path.display().to_string(),
            created_at_unix: now,
            expires_at_unix: expires_at,
            finalized_at_unix: None,
            consumed_at_unix: None,
            aborted_at_unix: None,
        };

        if let Err(error) = self.crud_store.insert_skill_upload_session(&record).await {
            let _ = fs::remove_dir_all(upload_dir.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to persist upload session",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }

        let payload = SkillsUploadStartResponse {
            upload_id,
            recommended_chunk_size_bytes: u64::try_from(
                context
                    .upload_recommended_chunk_size_bytes
                    .min(context.upload_max_chunk_size_bytes),
            )
            .unwrap_or(u64::MAX),
            max_chunk_size_bytes: u64::try_from(context.upload_max_chunk_size_bytes)
                .unwrap_or(u64::MAX),
            max_compressed_size_bytes: u64::try_from(context.max_upload_compressed_bytes)
                .unwrap_or(u64::MAX),
            max_uncompressed_size_bytes: u64::try_from(context.max_upload_uncompressed_bytes)
                .unwrap_or(u64::MAX),
            expires_at_unix: expires_at,
        };

        self.send_result(connection_id, request_id, &payload, "skills/upload/start")
            .await;
    }

    pub(crate) async fn skills_upload_finish(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillsUploadFinishParams,
    ) {
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::SKILLS_UPLOAD_FINISH,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let _guard = self
            .acquire_skill_upload_lock(params.upload_id.as_str())
            .await;
        let now = now_timestamp_secs();
        let Some(upload) = self
            .find_upload_for_connection(params.upload_id.as_str())
            .await
        else {
            self.send_upload_not_found(connection_id, request_id).await;
            return;
        };

        if let Err(error) =
            validate_upload_owner(&upload, workspace_id.as_str(), connection_id, now)
        {
            self.send_error(connection_id, error.with_request_id(request_id))
                .await;
            return;
        }

        if upload.status != UPLOAD_STATUS_RECEIVING {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_UPLOAD_INVALID_REQUEST,
                    "upload is not receiving chunks",
                    json!({"status": upload.status}),
                ),
            )
            .await;
            return;
        }

        if upload.received_bytes != upload.compressed_size_bytes {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_UPLOAD_SIZE_LIMIT,
                    "upload byte count is incomplete",
                    json!({
                        "received_bytes": upload.received_bytes,
                        "compressed_size_bytes": upload.compressed_size_bytes,
                    }),
                ),
            )
            .await;
            return;
        }

        match sha256_file(PathBuf::from(upload.payload_path.as_str()).as_path()) {
            Ok(actual) if actual == upload.sha256 => {}
            Ok(actual) => {
                if self
                    .crud_store
                    .transition_skill_upload_status(
                        upload.upload_id.as_str(),
                        &[UPLOAD_STATUS_RECEIVING],
                        UPLOAD_STATUS_ABORTED,
                        None,
                        None,
                        Some(now),
                        now,
                    )
                    .await
                    .ok()
                    .flatten()
                    .is_some()
                {
                    let _ = remove_upload_payload(&upload);
                }
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_UPLOAD_DIGEST_MISMATCH,
                        "upload digest mismatch",
                        json!({"expected": upload.sha256, "actual": actual}),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to hash upload payload",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        }

        let updated = match self
            .crud_store
            .transition_skill_upload_status(
                upload.upload_id.as_str(),
                &[UPLOAD_STATUS_RECEIVING],
                UPLOAD_STATUS_FINALIZED,
                Some(now),
                None,
                None,
                now,
            )
            .await
        {
            Ok(Some(row)) => row,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_UPLOAD_INVALID_REQUEST,
                        "upload changed state before it could be finalized",
                        json!({"upload_id": upload.upload_id}),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to finalize upload",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        let payload = SkillsUploadFinishResponse {
            upload_id: updated.upload_id,
            status: updated.status,
            sha256: updated.sha256,
            compressed_size_bytes: updated.compressed_size_bytes,
        };
        self.send_result(connection_id, request_id, &payload, "skills/upload/finish")
            .await;
    }

    pub(crate) async fn skills_upload_abort(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillsUploadAbortParams,
    ) {
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::SKILLS_UPLOAD_ABORT,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let _guard = self
            .acquire_skill_upload_lock(params.upload_id.as_str())
            .await;
        let now = now_timestamp_secs();
        let Some(upload) = self
            .find_upload_for_connection(params.upload_id.as_str())
            .await
        else {
            self.send_upload_not_found(connection_id, request_id).await;
            return;
        };

        if let Err(error) =
            validate_upload_owner(&upload, workspace_id.as_str(), connection_id, now)
        {
            self.send_error(connection_id, error.with_request_id(request_id))
                .await;
            return;
        }

        let updated = match self
            .crud_store
            .transition_skill_upload_status(
                upload.upload_id.as_str(),
                &[UPLOAD_STATUS_RECEIVING, UPLOAD_STATUS_FINALIZED],
                UPLOAD_STATUS_ABORTED,
                None,
                None,
                Some(now),
                now,
            )
            .await
        {
            Ok(Some(row)) => row,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_UPLOAD_INVALID_REQUEST,
                        "upload is already in a terminal state",
                        json!({"upload_id": upload.upload_id, "status": upload.status}),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to abort upload",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        let payload = SkillsUploadAbortResponse {
            upload_id: updated.upload_id,
            status: updated.status,
        };
        let _ = remove_upload_payload(&upload);
        self.send_result(connection_id, request_id, &payload, "skills/upload/abort")
            .await;
    }

    pub(crate) async fn process_skill_upload_chunk_frame(
        &self,
        connection_id: ConnectionId,
        frame: &[u8],
    ) {
        let (header, chunk) = match parse_upload_chunk_frame(frame) {
            Ok(value) => value,
            Err(error) => {
                warn!(connection_id, error = %format!("{error:#}"), "invalid skill upload chunk frame");
                return;
            }
        };

        let _guard = self
            .acquire_skill_upload_lock(header.upload_id.as_str())
            .await;
        let now = now_timestamp_secs();
        let Some(upload) = self
            .find_upload_for_connection(header.upload_id.as_str())
            .await
        else {
            warn!(
                connection_id,
                upload_id = header.upload_id.as_str(),
                "upload chunk referenced missing session"
            );
            return;
        };

        if let Err(error) =
            validate_upload_owner(&upload, header.workspace_id.as_str(), connection_id, now)
        {
            warn!(
                connection_id,
                upload_id = header.upload_id.as_str(),
                error = %error.message,
                "upload chunk rejected"
            );
            return;
        }

        if upload.status != UPLOAD_STATUS_RECEIVING {
            warn!(
                connection_id,
                upload_id = header.upload_id.as_str(),
                status = upload.status.as_str(),
                "upload chunk rejected for non-receiving upload"
            );
            return;
        }

        let context = match self.skills_runtime_context(upload.workspace_id.as_str()) {
            Ok(context) => context,
            Err(error) => {
                warn!(
                    connection_id,
                    upload_id = header.upload_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to resolve skills runtime context for upload chunk"
                );
                return;
            }
        };

        if chunk.len() > context.upload_max_chunk_size_bytes {
            warn!(
                connection_id,
                upload_id = header.upload_id.as_str(),
                chunk_len = chunk.len(),
                max_chunk_size_bytes = context.upload_max_chunk_size_bytes,
                "upload chunk exceeds configured max chunk size"
            );
            return;
        }

        if header.offset != upload.received_bytes {
            warn!(
                connection_id,
                upload_id = header.upload_id.as_str(),
                expected_offset = upload.received_bytes,
                actual_offset = header.offset,
                "upload chunk offset mismatch"
            );
            return;
        }

        if header.len != u64::try_from(chunk.len()).unwrap_or(u64::MAX) {
            warn!(
                connection_id,
                upload_id = header.upload_id.as_str(),
                "upload chunk declared length mismatch"
            );
            return;
        }

        if let Some(expected) = header.chunk_sha256.as_deref() {
            let actual = sha256_bytes(chunk);
            if expected != actual {
                self.invalidate_upload(upload.upload_id.as_str(), now, &upload)
                    .await;
                warn!(
                    connection_id,
                    upload_id = header.upload_id.as_str(),
                    "upload chunk digest mismatch"
                );
                return;
            }
        }

        let next_offset = header.offset.saturating_add(header.len);
        if next_offset > upload.compressed_size_bytes {
            warn!(
                connection_id,
                upload_id = header.upload_id.as_str(),
                "upload chunk exceeds declared compressed size"
            );
            return;
        }

        let payload_path = PathBuf::from(upload.payload_path.as_str());
        if let Some(parent) = payload_path.parent()
            && let Err(error) = fs::create_dir_all(parent)
        {
            error!(connection_id, error = %format!("{error:#}"), "failed to create upload payload parent");
            return;
        }

        if let Err(error) = append_chunk(payload_path.as_path(), chunk, header.offset) {
            error!(connection_id, error = %format!("{error:#}"), "failed to write skill upload chunk");
            return;
        }

        let updated = match self
            .crud_store
            .update_skill_upload_received_bytes(upload.upload_id.as_str(), next_offset, now)
            .await
        {
            Ok(Some(row)) => row,
            Ok(None) => {
                warn!(
                    connection_id,
                    upload_id = upload.upload_id.as_str(),
                    "upload vanished while writing chunk"
                );
                return;
            }
            Err(error) => {
                error!(connection_id, error = %format!("{error:#}"), "failed to update upload received bytes");
                return;
            }
        };

        let ack = SkillsUploadChunkAckNotification {
            upload_id: updated.upload_id,
            offset: header.offset,
            len: header.len,
            received_bytes: updated.received_bytes,
            next_offset: updated.received_bytes,
        };
        let notification = match JsonRpcNotification::from_params(
            events::SKILLS_UPLOAD_CHUNK_ACK,
            &ack,
        ) {
            Ok(notification) => notification,
            Err(error) => {
                error!(connection_id, error = %format!("{error:#}"), "failed to encode upload chunk ack");
                return;
            }
        };
        if let Err(error) = self.send_json(connection_id, &notification).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send upload chunk ack"
            );
        }
    }

    pub(super) async fn materialize_uploaded_skill_source(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        upload_id: &str,
        context: &SkillsRuntimeContext,
        request_id: &RequestId,
    ) -> Result<MaterializedSkillSource, JsonRpcErrorResponse> {
        let materialized = self
            .materialize_uploaded_archive_source(
                connection_id,
                workspace_id,
                upload_id,
                context,
                request_id,
            )
            .await?;
        match validate_single_skill_root(materialized.source_dir.clone()).and_then(|source_dir| {
            normalize_materialized_skill_frontmatter(source_dir.as_path())?;
            Ok(source_dir)
        }) {
            Ok(source_dir) => Ok(MaterializedSkillSource {
                source_dir,
                ..materialized
            }),
            Err(error) => {
                self.abort_invalid_materialized_upload(&materialized).await;
                Err(invalid_materialized_archive_error(request_id, error))
            }
        }
    }

    pub(super) async fn materialize_uploaded_skill_pack_source(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        upload_id: &str,
        context: &SkillsRuntimeContext,
        request_id: &RequestId,
    ) -> Result<(MaterializedSkillSource, ValidatedSkillPackRoot), JsonRpcErrorResponse> {
        let materialized = self
            .materialize_uploaded_archive_source(
                connection_id,
                workspace_id,
                upload_id,
                context,
                request_id,
            )
            .await?;
        match validate_skill_pack_root(materialized.source_dir.clone()).and_then(|validated| {
            for member in &validated.members {
                normalize_materialized_skill_frontmatter(member.source_dir.as_path())?;
            }
            Ok(validated)
        }) {
            Ok(validated) => Ok((materialized, validated)),
            Err(error) => {
                self.abort_invalid_materialized_upload(&materialized).await;
                Err(invalid_materialized_archive_error(request_id, error))
            }
        }
    }

    async fn materialize_uploaded_archive_source(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        upload_id: &str,
        context: &SkillsRuntimeContext,
        request_id: &RequestId,
    ) -> Result<MaterializedSkillSource, JsonRpcErrorResponse> {
        let now = now_timestamp_secs();
        let upload = self
            .find_upload_for_connection(upload_id)
            .await
            .ok_or_else(|| {
                skills_error(
                    Some(request_id.clone()),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_UPLOAD_NOT_FOUND,
                    "upload was not found",
                    json!({"upload_id": upload_id}),
                )
            })?;

        validate_upload_owner(&upload, workspace_id, connection_id, now)
            .map_err(|error| error.with_request_id(request_id.clone()))?;

        if upload.status != UPLOAD_STATUS_FINALIZED {
            return Err(skills_error(
                Some(request_id.clone()),
                INVALID_REQUEST_CODE,
                SKILLS_ERROR_UPLOAD_INVALID_REQUEST,
                "upload must be finalized before install/update",
                json!({"upload_id": upload_id, "status": upload.status}),
            ));
        }

        let cleanup_root = context.materialized_root.join(request_id.as_str());
        let _ = fs::remove_dir_all(cleanup_root.as_path());
        fs::create_dir_all(cleanup_root.as_path()).map_err(|error| {
            skills_error(
                Some(request_id.clone()),
                INVALID_REQUEST_CODE,
                SKILLS_ERROR_INTERNAL,
                "failed to create materialization directory",
                json!({"error": format!("{error:#}")}),
            )
        })?;

        let source_dir = match extract_archive_secure(
            PathBuf::from(upload.payload_path.as_str()).as_path(),
            cleanup_root.as_path(),
            context.max_upload_uncompressed_bytes,
            context.max_upload_archive_entries,
            context.security_policy.max_install_file_bytes,
        ) {
            Ok(source_dir) => source_dir,
            Err(error) => {
                let materialized = MaterializedSkillSource {
                    source_dir: cleanup_root.clone(),
                    cleanup_root,
                    upload,
                };
                self.abort_invalid_materialized_upload(&materialized).await;
                return Err(invalid_materialized_archive_error(request_id, error));
            }
        };

        Ok(MaterializedSkillSource {
            source_dir,
            cleanup_root,
            upload,
        })
    }

    async fn abort_invalid_materialized_upload(&self, materialized: &MaterializedSkillSource) {
        let _guard = self
            .acquire_skill_upload_lock(materialized.upload.upload_id.as_str())
            .await;
        let now = now_timestamp_secs();
        match self
            .crud_store
            .transition_skill_upload_status(
                materialized.upload.upload_id.as_str(),
                &[UPLOAD_STATUS_FINALIZED],
                UPLOAD_STATUS_ABORTED,
                None,
                None,
                Some(now),
                now,
            )
            .await
        {
            Ok(Some(_)) => {
                let _ = remove_upload_payload(&materialized.upload);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    upload_id = materialized.upload.upload_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to abort invalid materialized skill upload"
                );
            }
        }
        let _ = fs::remove_dir_all(materialized.cleanup_root.as_path());
    }

    pub(super) async fn revalidate_finalized_upload_locked(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        upload_id: &str,
        request_id: &RequestId,
    ) -> Result<SkillUploadSessionRecord, JsonRpcErrorResponse> {
        let now = now_timestamp_secs();
        let upload = self
            .find_upload_for_connection(upload_id)
            .await
            .ok_or_else(|| {
                skills_error(
                    Some(request_id.clone()),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_UPLOAD_NOT_FOUND,
                    "upload was not found",
                    json!({"upload_id": upload_id}),
                )
            })?;
        validate_upload_owner(&upload, workspace_id, connection_id, now)
            .map_err(|error| error.with_request_id(request_id.clone()))?;
        if upload.status != UPLOAD_STATUS_FINALIZED || upload.consumed_at_unix.is_some() {
            return Err(skills_error(
                Some(request_id.clone()),
                INVALID_REQUEST_CODE,
                SKILLS_ERROR_UPLOAD_INVALID_REQUEST,
                "upload is no longer finalized and unconsumed",
                json!({"upload_id": upload_id, "status": upload.status}),
            ));
        }
        Ok(upload)
    }

    pub(super) async fn mark_upload_consumed(&self, upload_id: &str, now: i64) -> Result<()> {
        let updated = self
            .crud_store
            .transition_skill_upload_status(
                upload_id,
                &[UPLOAD_STATUS_FINALIZED],
                UPLOAD_STATUS_CONSUMED,
                None,
                Some(now),
                None,
                now,
            )
            .await?;
        if updated.is_none() {
            bail!("upload `{upload_id}` is no longer finalized and unconsumed");
        }
        Ok(())
    }

    pub(super) fn cleanup_upload_artifacts(
        &self,
        upload: &SkillUploadSessionRecord,
        materialized_cleanup_root: &std::path::Path,
    ) {
        let _ = remove_upload_payload(upload);
        let _ = fs::remove_dir_all(materialized_cleanup_root);
    }

    async fn invalidate_upload(
        &self,
        upload_id: &str,
        now: i64,
        upload: &SkillUploadSessionRecord,
    ) {
        match self
            .crud_store
            .transition_skill_upload_status(
                upload_id,
                &[UPLOAD_STATUS_RECEIVING, UPLOAD_STATUS_FINALIZED],
                UPLOAD_STATUS_ABORTED,
                None,
                None,
                Some(now),
                now,
            )
            .await
        {
            Ok(Some(_)) => {
                let _ = remove_upload_payload(upload);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    upload_id,
                    error = %format!("{error:#}"),
                    "failed to mark invalid skill upload aborted"
                );
            }
        }
    }

    pub(crate) async fn cleanup_stale_skill_uploads(&self, now: i64) {
        let uploads = match self.crud_store.list_stale_skill_upload_sessions(now).await {
            Ok(uploads) => uploads,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to query stale skill upload sessions"
                );
                return;
            }
        };

        for upload in uploads {
            let _guard = self
                .acquire_skill_upload_lock(upload.upload_id.as_str())
                .await;
            let current = match self
                .crud_store
                .find_skill_upload_session(upload.upload_id.as_str())
                .await
            {
                Ok(Some(current)) => current,
                Ok(None) => continue,
                Err(error) => {
                    warn!(
                        upload_id = upload.upload_id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to re-read stale skill upload"
                    );
                    continue;
                }
            };
            if current.expires_at_unix <= now
                && matches!(
                    current.status.as_str(),
                    UPLOAD_STATUS_RECEIVING | UPLOAD_STATUS_FINALIZED
                )
            {
                match self
                    .crud_store
                    .transition_skill_upload_status(
                        current.upload_id.as_str(),
                        &[UPLOAD_STATUS_RECEIVING, UPLOAD_STATUS_FINALIZED],
                        UPLOAD_STATUS_EXPIRED,
                        None,
                        None,
                        None,
                        now,
                    )
                    .await
                {
                    Ok(Some(_)) => {
                        let _ = remove_upload_payload(&current);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(
                            upload_id = current.upload_id.as_str(),
                            error = %format!("{error:#}"),
                            "failed to mark skill upload expired"
                        );
                    }
                }
            } else if matches!(
                current.status.as_str(),
                UPLOAD_STATUS_ABORTED | UPLOAD_STATUS_CONSUMED | UPLOAD_STATUS_EXPIRED
            ) {
                let _ = remove_upload_payload(&current);
            }
        }

        self.cleanup_orphaned_upload_dirs(now).await;
        self.cleanup_orphaned_materialized_dirs(now).await;
    }

    async fn cleanup_orphaned_upload_dirs(&self, now: i64) {
        let contexts = self.active_skill_runtime_contexts().await;
        let stale_before = skill_upload_stale_before(&self.tool_loop_config, now);
        let mut roots = HashSet::new();
        for context in contexts {
            roots.insert(context.upload_root);
        }

        for upload_root in roots {
            let Ok(workspace_dirs) = fs::read_dir(upload_root.as_path()) else {
                continue;
            };
            for workspace_dir in workspace_dirs.flatten() {
                let workspace_path = workspace_dir.path();
                if !workspace_path.is_dir() {
                    continue;
                }
                let Ok(upload_dirs) = fs::read_dir(workspace_path.as_path()) else {
                    continue;
                };
                for upload_dir in upload_dirs.flatten() {
                    let upload_path = upload_dir.path();
                    if !upload_path.is_dir() {
                        continue;
                    }
                    let Some(upload_id) = upload_path.file_name().and_then(|value| value.to_str())
                    else {
                        continue;
                    };
                    let should_remove =
                        match self.crud_store.find_skill_upload_session(upload_id).await {
                            Ok(Some(upload)) => {
                                upload.expires_at_unix <= now
                                    || matches!(
                                        upload.status.as_str(),
                                        UPLOAD_STATUS_ABORTED
                                            | UPLOAD_STATUS_CONSUMED
                                            | UPLOAD_STATUS_EXPIRED
                                    )
                            }
                            Ok(None) => {
                                directory_modified_at_or_before(upload_path.as_path(), stale_before)
                            }
                            Err(error) => {
                                warn!(
                                    upload_id,
                                    error = %format!("{error:#}"),
                                    "failed to inspect skill upload session during orphan cleanup"
                                );
                                false
                            }
                        };
                    if should_remove {
                        let _ = fs::remove_dir_all(upload_path);
                    }
                }
            }
        }
    }

    async fn cleanup_orphaned_materialized_dirs(&self, now: i64) {
        let contexts = self.active_skill_runtime_contexts().await;
        let mut roots = HashSet::new();
        for context in contexts {
            roots.insert(context.materialized_root);
        }

        let stale_before = skill_upload_stale_before(&self.tool_loop_config, now);
        for root in roots {
            let Ok(entries) = fs::read_dir(root.as_path()) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                if directory_modified_at_or_before(path.as_path(), stale_before) {
                    let _ = fs::remove_dir_all(path);
                }
            }
        }
    }

    async fn active_skill_runtime_contexts(&self) -> Vec<SkillsRuntimeContext> {
        let workspaces = match self.workspace_manager.list_workspaces().await {
            Ok(workspaces) => workspaces,
            Err(error) => {
                warn!(
                    error = %error,
                    "failed to list workspaces for skill upload cleanup"
                );
                return Vec::new();
            }
        };

        workspaces
            .into_iter()
            .filter(|workspace| workspace.is_active)
            .filter_map(
                |workspace| match self.skills_runtime_context(workspace.id.as_str()) {
                    Ok(context) => Some(context),
                    Err(error) => {
                        warn!(
                            workspace_id = workspace.id.as_str(),
                            error = %format!("{error:#}"),
                            "failed to resolve skill upload cleanup context"
                        );
                        None
                    }
                },
            )
            .collect()
    }

    async fn find_upload_for_connection(
        &self,
        upload_id: &str,
    ) -> Option<SkillUploadSessionRecord> {
        match self.crud_store.find_skill_upload_session(upload_id).await {
            Ok(value) => value,
            Err(error) => {
                error!(upload_id, error = %format!("{error:#}"), "failed to load skill upload session");
                None
            }
        }
    }

    async fn send_upload_not_found(&self, connection_id: ConnectionId, request_id: RequestId) {
        self.send_error(
            connection_id,
            skills_error(
                Some(request_id),
                INVALID_PARAMS_CODE,
                SKILLS_ERROR_UPLOAD_NOT_FOUND,
                "upload was not found",
                json!({}),
            ),
        )
        .await;
    }

    async fn send_result<T: serde::Serialize>(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        payload: &T,
        label: &str,
    ) {
        let response = match JsonRpcResponse::from_result(request_id, payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        None,
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        format!("failed to encode {label} response"),
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send {label} response"
            );
        }
    }
}

fn invalid_materialized_archive_error(
    request_id: &RequestId,
    error: anyhow::Error,
) -> JsonRpcErrorResponse {
    skills_error(
        Some(request_id.clone()),
        INVALID_REQUEST_CODE,
        SKILLS_ERROR_UPLOAD_INVALID_ARCHIVE,
        "failed to materialize uploaded skill archive",
        json!({"error": format!("{error:#}")}),
    )
}

struct UploadValidationError {
    jsonrpc_code: i64,
    code: &'static str,
    message: String,
    details: serde_json::Value,
}

impl UploadValidationError {
    fn with_request_id(self, request_id: RequestId) -> JsonRpcErrorResponse {
        skills_error(
            Some(request_id),
            self.jsonrpc_code,
            self.code,
            self.message,
            self.details,
        )
    }
}

fn validate_upload_workspace(
    upload: &SkillUploadSessionRecord,
    workspace_id: &str,
) -> Result<(), UploadValidationError> {
    if upload.workspace_id != workspace_id {
        return Err(UploadValidationError {
            jsonrpc_code: INVALID_PARAMS_CODE,
            code: SKILLS_ERROR_UPLOAD_NOT_FOUND,
            message: "upload was not found for workspace".to_owned(),
            details: json!({"upload_id": upload.upload_id, "workspace_id": workspace_id}),
        });
    }
    Ok(())
}

fn validate_upload_owner(
    upload: &SkillUploadSessionRecord,
    workspace_id: &str,
    connection_id: ConnectionId,
    now: i64,
) -> Result<(), UploadValidationError> {
    validate_upload_workspace(upload, workspace_id)?;

    if upload.connection_id != connection_id {
        return Err(UploadValidationError {
            jsonrpc_code: INVALID_PARAMS_CODE,
            code: SKILLS_ERROR_UPLOAD_NOT_FOUND,
            message: "upload was not found for connection".to_owned(),
            details: json!({"upload_id": upload.upload_id}),
        });
    }

    if upload.expires_at_unix <= now {
        return Err(UploadValidationError {
            jsonrpc_code: INVALID_REQUEST_CODE,
            code: SKILLS_ERROR_UPLOAD_EXPIRED,
            message: "upload has expired".to_owned(),
            details: json!({"upload_id": upload.upload_id, "expires_at_unix": upload.expires_at_unix}),
        });
    }

    Ok(())
}

fn parse_upload_chunk_frame(frame: &[u8]) -> Result<(SkillsUploadChunkHeader, &[u8])> {
    if frame.len() < 8 {
        bail!("upload frame is too short");
    }
    if &frame[0..4] != SKILL_UPLOAD_CHUNK_FRAME_MAGIC {
        bail!("upload frame has invalid magic");
    }

    let header_len = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
    let header_start = 8usize;
    let header_end = header_start.saturating_add(header_len);
    if header_end > frame.len() {
        bail!("upload frame header length exceeds frame length");
    }

    let header =
        serde_json::from_slice::<SkillsUploadChunkHeader>(&frame[header_start..header_end])
            .context("failed to parse upload frame header")?;
    let chunk = &frame[header_end..];
    if header.len != u64::try_from(chunk.len()).unwrap_or(u64::MAX) {
        bail!("upload frame chunk length mismatch");
    }
    Ok((header, chunk))
}

fn append_chunk(path: &std::path::Path, chunk: &[u8], offset: u64) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let mut file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(path)
        .with_context(|| format!("failed to open upload payload `{}`", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to stat upload payload `{}`", path.display()))?;
    if metadata.len() < offset {
        bail!(
            "payload length {} does not match chunk offset {}",
            metadata.len(),
            offset
        );
    }
    if metadata.len() > offset {
        file.set_len(offset).with_context(|| {
            format!(
                "failed to truncate upload payload `{}` to authoritative offset {offset}",
                path.display()
            )
        })?;
    }
    file.seek(SeekFrom::Start(offset))
        .context("failed to seek upload payload")?;
    file.write_all(chunk)
        .context("failed to write upload payload chunk")?;
    file.sync_data()
        .context("failed to flush upload payload chunk")?;
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn sha256_file(path: &std::path::Path) -> Result<String> {
    use std::io::Read;

    let mut file =
        fs::File::open(path).with_context(|| format!("failed to open `{}`", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to hash `{}`", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sanitize_file_name(value: &str) -> String {
    let name = value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if name.is_empty() || name == "." || name == ".." {
        "skill.tar.gz".to_owned()
    } else {
        name
    }
}

fn remove_upload_payload(upload: &SkillUploadSessionRecord) -> Result<()> {
    let payload = PathBuf::from(upload.payload_path.as_str());
    if let Some(parent) = payload.parent()
        && parent.exists()
    {
        fs::remove_dir_all(parent)
            .with_context(|| format!("failed to remove upload directory `{}`", parent.display()))?;
    }
    Ok(())
}

fn skill_upload_stale_before(tool_loop_config: &ToolLoopConfig, now: i64) -> i64 {
    let ttl_secs =
        i64::try_from(tool_loop_config.skills.security.upload_ttl_secs.max(60)).unwrap_or(3600);
    now.saturating_sub(ttl_secs)
}

fn directory_modified_at_or_before(path: &std::path::Path, stale_before: i64) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    let Ok(age) = modified.duration_since(UNIX_EPOCH) else {
        return false;
    };
    i64::try_from(age.as_secs()).unwrap_or(i64::MAX) <= stale_before
}

pub(super) fn extract_archive_secure(
    archive_path: &std::path::Path,
    cleanup_root: &std::path::Path,
    max_uncompressed_bytes: usize,
    max_entries: usize,
    max_file_bytes: usize,
) -> Result<PathBuf> {
    let file = fs::File::open(archive_path)
        .with_context(|| format!("failed to open skill archive `{}`", archive_path.display()))?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);
    let mut root_name: Option<std::ffi::OsString> = None;
    let mut seen_paths = HashSet::new();
    let mut total_uncompressed = 0usize;
    let mut entry_count = 0usize;
    let cleanup_root = fs::canonicalize(cleanup_root).with_context(|| {
        format!(
            "failed to canonicalize materialization root `{}`",
            cleanup_root.display()
        )
    })?;

    for entry in archive.entries().context("failed to read skill archive")? {
        let mut entry = entry.context("failed to read archive entry")?;
        entry_count = entry_count.saturating_add(1);
        if entry_count > max_entries {
            bail!("archive has more than {max_entries} entries");
        }

        let raw_path = entry.path().context("failed to read archive entry path")?;
        let normalized = normalize_archive_path(raw_path.as_ref())?;
        if normalized.as_os_str().is_empty() {
            continue;
        }
        let Some(first_component) = normalized.components().next() else {
            continue;
        };
        let root_component = first_component.as_os_str().to_os_string();
        match root_name.as_ref() {
            Some(existing) if existing != &root_component => {
                bail!("archive contains multiple top-level roots");
            }
            None => root_name = Some(root_component),
            _ => {}
        }

        if !seen_paths.insert(normalized.clone()) {
            bail!("archive contains duplicate path `{}`", normalized.display());
        }

        let entry_type = entry.header().entry_type();
        let target_path = cleanup_root.join(normalized.as_path());
        if !target_path.starts_with(cleanup_root.as_path()) {
            bail!("archive entry escapes materialization root");
        }

        if entry_type.is_dir() {
            fs::create_dir_all(target_path.as_path()).with_context(|| {
                format!(
                    "failed to create archive directory `{}`",
                    target_path.display()
                )
            })?;
            ensure_materialized_path_contained(cleanup_root.as_path(), target_path.as_path())?;
            continue;
        }

        if !entry_type.is_file() {
            bail!(
                "archive contains unsupported entry `{}`",
                normalized.display()
            );
        }

        let file_size =
            usize::try_from(entry.header().size().unwrap_or(u64::MAX)).unwrap_or(usize::MAX);
        if file_size > max_file_bytes {
            bail!(
                "archive file `{}` exceeds per-file limit",
                normalized.display()
            );
        }
        total_uncompressed = total_uncompressed.saturating_add(file_size);
        if total_uncompressed > max_uncompressed_bytes {
            bail!("archive uncompressed size exceeds configured limit");
        }

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create `{}`", parent.display()))?;
        }
        let mut output = fs::File::create(target_path.as_path())
            .with_context(|| format!("failed to create `{}`", target_path.display()))?;
        io::copy(&mut entry, &mut output)
            .with_context(|| format!("failed to extract `{}`", target_path.display()))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = entry.header().mode().unwrap_or(0o644);
            let permissions = if mode & 0o111 != 0 { 0o755 } else { 0o644 };
            let _ = fs::set_permissions(
                target_path.as_path(),
                fs::Permissions::from_mode(permissions),
            );
        }
        ensure_materialized_path_contained(cleanup_root.as_path(), target_path.as_path())?;
    }

    let root = root_name.ok_or_else(|| anyhow!("archive is empty"))?;
    let source_dir = cleanup_root.join(root);
    ensure_materialized_path_contained(cleanup_root.as_path(), source_dir.as_path())?;
    Ok(source_dir)
}

pub(super) fn validate_single_skill_root(source_dir: PathBuf) -> Result<PathBuf> {
    if !source_dir.join("SKILL.md").is_file() {
        bail!("archive root is missing SKILL.md");
    }
    Ok(source_dir)
}

pub(super) fn validate_skill_pack_root(source_dir: PathBuf) -> Result<ValidatedSkillPackRoot> {
    if !source_dir.is_dir() {
        bail!("pack root must be a directory");
    }
    let pack_name = exact_utf8_component(
        source_dir
            .file_name()
            .ok_or_else(|| anyhow!("pack root must have a name"))?,
        "pack root",
    )?;

    let mut members = Vec::new();
    for entry in fs::read_dir(source_dir.as_path())
        .with_context(|| format!("failed to read pack root `{}`", source_dir.display()))?
    {
        let entry = entry.context("failed to read pack root entry")?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to inspect `{}`", entry.path().display()))?;
        if !file_type.is_dir() {
            bail!(
                "pack root may contain only immediate child directories; found `{}`",
                entry.path().display()
            );
        }
        let member_name = entry.file_name();
        let pack_member_key = exact_utf8_component(member_name.as_os_str(), "pack member")?;
        let member_dir = entry.path();
        if !member_dir.join("SKILL.md").is_file() {
            bail!("pack member `{pack_member_key}` is missing direct SKILL.md");
        }
        members.push(SkillPackMemberSource {
            pack_member_key,
            source_dir: member_dir,
        });
    }

    if members.is_empty() {
        bail!("pack root must contain at least one immediate child directory");
    }
    members.sort_by(|left, right| left.pack_member_key.cmp(&right.pack_member_key));

    Ok(ValidatedSkillPackRoot { pack_name, members })
}

fn exact_utf8_component(name: &std::ffi::OsStr, label: &str) -> Result<String> {
    name.to_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("{label} name must be valid UTF-8"))
}

fn ensure_materialized_path_contained(
    cleanup_root: &std::path::Path,
    path: &std::path::Path,
) -> Result<()> {
    let canonical = fs::canonicalize(path)
        .with_context(|| format!("failed to canonicalize `{}`", path.display()))?;
    if !canonical.starts_with(cleanup_root) {
        bail!("archive entry escapes materialization root after canonicalization");
    }
    Ok(())
}

fn normalize_archive_path(path: &std::path::Path) -> Result<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => normalized.push(value),
            Component::CurDir => {}
            Component::ParentDir => bail!("archive path contains parent traversal"),
            Component::RootDir | Component::Prefix(_) => bail!("archive path is absolute"),
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, GzBuilder};
    use std::io::{self, Cursor};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tar::{Builder, EntryType, Header};

    #[test]
    fn parse_upload_chunk_frame_accepts_valid_frame() {
        let header = SkillsUploadChunkHeader {
            workspace_id: "ws_000000000000000001".to_owned(),
            upload_id: "upl_000000000000001".to_owned(),
            offset: 7,
            len: 5,
            chunk_sha256: None,
        };
        let frame = upload_frame(&header, b"hello");

        let (parsed, chunk) = parse_upload_chunk_frame(frame.as_slice()).expect("valid frame");

        assert_eq!(parsed, header);
        assert_eq!(chunk, b"hello");
    }

    #[test]
    fn parse_upload_chunk_frame_rejects_bad_magic() {
        let header = SkillsUploadChunkHeader {
            workspace_id: "ws_000000000000000001".to_owned(),
            upload_id: "upl_000000000000001".to_owned(),
            offset: 0,
            len: 5,
            chunk_sha256: None,
        };
        let mut frame = upload_frame(&header, b"hello");
        frame[0..4].copy_from_slice(b"BAD!");

        let error = parse_upload_chunk_frame(frame.as_slice())
            .expect_err("bad magic should fail")
            .to_string();

        assert!(error.contains("magic"));
    }

    #[test]
    fn parse_upload_chunk_frame_rejects_malformed_header() {
        let mut frame = Vec::new();
        frame.extend_from_slice(SKILL_UPLOAD_CHUNK_FRAME_MAGIC);
        frame.extend_from_slice(&8u32.to_be_bytes());
        frame.extend_from_slice(b"not-json");
        frame.extend_from_slice(b"payload");

        let error = parse_upload_chunk_frame(frame.as_slice())
            .expect_err("malformed header should fail")
            .to_string();

        assert!(error.contains("header"));
    }

    #[test]
    fn append_chunk_enforces_authoritative_offset_and_truncates_stale_bytes() {
        let case = TempCase::new("append-offset");
        let payload = case.path.join("payload.tar.gz");

        append_chunk(payload.as_path(), b"hello", 0).expect("initial chunk");
        let gap = append_chunk(payload.as_path(), b"world", 6)
            .expect_err("gap should fail")
            .to_string();
        append_chunk(payload.as_path(), b"world", 4).expect("stale extra byte should be truncated");
        let contents = fs::read(payload.as_path()).expect("payload should be readable");

        assert!(gap.contains("does not match chunk offset"));
        assert_eq!(contents, b"hellworld");
    }

    #[test]
    fn extract_skill_archive_materializes_valid_root() {
        let case = TempCase::new("valid");
        let archive = case.path.join("skill.tar.gz");
        write_archive(
            archive.as_path(),
            &[
                TestEntry::dir("valid-skill"),
                TestEntry::file("valid-skill/SKILL.md", b"---\nname: valid\n---\nbody"),
                TestEntry::file("valid-skill/scripts/run.sh", b"#!/bin/sh\n"),
            ],
        );

        let source_dir = extract_archive_secure(
            archive.as_path(),
            case.materialized.as_path(),
            1024,
            8,
            1024,
        )
        .and_then(validate_single_skill_root)
        .expect("valid archive should extract");

        assert_eq!(
            source_dir,
            fs::canonicalize(case.materialized.join("valid-skill")).expect("canonical source dir")
        );
        assert!(source_dir.join("SKILL.md").is_file());
    }

    #[test]
    fn secure_extraction_accepts_pack_shape_but_single_skill_validation_rejects_it() {
        let case = TempCase::new("missing-skill-md");
        let archive = case.path.join("skill.tar.gz");
        write_archive(
            archive.as_path(),
            &[
                TestEntry::file("pack/first/SKILL.md", b"---\nname: first\n---\n"),
                TestEntry::file("pack/second/SKILL.md", b"---\nname: second\n---\n"),
            ],
        );

        let source_dir = extract_archive_secure(
            archive.as_path(),
            case.materialized.as_path(),
            1024,
            8,
            1024,
        )
        .expect("safe pack-shaped archive should extract");
        assert!(source_dir.join("first/SKILL.md").is_file());
        assert!(source_dir.join("second/SKILL.md").is_file());

        let error = validate_single_skill_root(source_dir)
            .expect_err("single-skill validation must reject a pack-shaped archive")
            .to_string();

        assert!(error.contains("SKILL.md"));
    }

    #[test]
    fn extract_skill_archive_rejects_traversal_entries() {
        let case = TempCase::new("traversal");
        let archive = case.path.join("skill.tar.gz");
        write_archive(
            archive.as_path(),
            &[
                TestEntry::file("skill/SKILL.md", b"---\nname: skill\n---\n"),
                TestEntry::file("../evil.txt", b"evil"),
            ],
        );

        let error = extract_archive_secure(
            archive.as_path(),
            case.materialized.as_path(),
            1024,
            8,
            1024,
        )
        .expect_err("traversal should fail")
        .to_string();

        assert!(error.contains("parent traversal") || error.contains("multiple top-level"));
    }

    #[test]
    fn extract_skill_archive_rejects_absolute_entries() {
        let case = TempCase::new("absolute");
        let archive = case.path.join("skill.tar.gz");
        write_archive(
            archive.as_path(),
            &[
                TestEntry::file("skill/SKILL.md", b"---\nname: skill\n---\n"),
                TestEntry::file("/tmp/evil.txt", b"evil"),
            ],
        );

        let error = extract_archive_secure(
            archive.as_path(),
            case.materialized.as_path(),
            1024,
            8,
            1024,
        )
        .expect_err("absolute path should fail")
        .to_string();

        assert!(error.contains("absolute") || error.contains("multiple top-level"));
    }

    #[test]
    fn extract_skill_archive_rejects_links_and_special_files() {
        for (name, entry) in [
            ("symlink", TestEntry::symlink("skill/link.md", "SKILL.md")),
            (
                "hardlink",
                TestEntry::hardlink("skill/hard.md", "skill/SKILL.md"),
            ),
            ("special", TestEntry::special("skill/device")),
        ] {
            let case = TempCase::new(name);
            let archive = case.path.join("skill.tar.gz");
            write_archive(
                archive.as_path(),
                &[
                    TestEntry::file("skill/SKILL.md", b"---\nname: skill\n---\n"),
                    entry,
                ],
            );

            let error = extract_archive_secure(
                archive.as_path(),
                case.materialized.as_path(),
                1024,
                8,
                1024,
            )
            .expect_err("unsafe entry should fail")
            .to_string();

            assert!(error.contains("unsupported entry"), "{name}: {error}");
        }
    }

    #[test]
    fn extract_skill_archive_rejects_duplicate_paths() {
        let case = TempCase::new("duplicate");
        let archive = case.path.join("skill.tar.gz");
        write_archive(
            archive.as_path(),
            &[
                TestEntry::file("skill/SKILL.md", b"---\nname: skill\n---\n"),
                TestEntry::file("skill/SKILL.md", b"duplicate"),
            ],
        );

        let error = extract_archive_secure(
            archive.as_path(),
            case.materialized.as_path(),
            1024,
            8,
            1024,
        )
        .expect_err("duplicate should fail")
        .to_string();

        assert!(error.contains("duplicate"));
    }

    #[test]
    fn extract_skill_archive_enforces_size_and_entry_limits() {
        let oversized = TempCase::new("oversized");
        let oversized_archive = oversized.path.join("skill.tar.gz");
        write_archive(
            oversized_archive.as_path(),
            &[TestEntry::file(
                "skill/SKILL.md",
                b"---\nname: skill\n---\nthis file is too large",
            )],
        );
        let oversized_error = extract_archive_secure(
            oversized_archive.as_path(),
            oversized.materialized.as_path(),
            1024,
            8,
            8,
        )
        .expect_err("oversized file should fail")
        .to_string();
        assert!(oversized_error.contains("per-file limit"));

        let too_many = TempCase::new("too-many");
        let too_many_archive = too_many.path.join("skill.tar.gz");
        write_archive(
            too_many_archive.as_path(),
            &[
                TestEntry::file("skill/SKILL.md", b"---\nname: skill\n---\n"),
                TestEntry::file("skill/a.txt", b"a"),
                TestEntry::file("skill/b.txt", b"b"),
            ],
        );
        let too_many_error = extract_archive_secure(
            too_many_archive.as_path(),
            too_many.materialized.as_path(),
            1024,
            2,
            1024,
        )
        .expect_err("entry limit should fail")
        .to_string();
        assert!(too_many_error.contains("entries"));
    }

    #[test]
    fn secure_extraction_rejects_empty_and_multiple_roots() {
        let empty = TempCase::new("empty-archive");
        let empty_archive = empty.path.join("skill.tar.gz");
        write_archive(empty_archive.as_path(), &[]);
        let empty_error = extract_archive_secure(
            empty_archive.as_path(),
            empty.materialized.as_path(),
            1024,
            8,
            1024,
        )
        .expect_err("empty archive should fail")
        .to_string();
        assert!(empty_error.contains("empty"));

        let multiple = TempCase::new("multiple-roots");
        let multiple_archive = multiple.path.join("skill.tar.gz");
        write_archive(
            multiple_archive.as_path(),
            &[
                TestEntry::file("first/SKILL.md", b"first"),
                TestEntry::file("second/SKILL.md", b"second"),
            ],
        );
        let multiple_error = extract_archive_secure(
            multiple_archive.as_path(),
            multiple.materialized.as_path(),
            1024,
            8,
            1024,
        )
        .expect_err("multiple roots should fail")
        .to_string();
        assert!(multiple_error.contains("multiple top-level roots"));
    }

    #[test]
    fn pack_shape_preserves_exact_names_orders_members_and_ignores_nested_directories() {
        let case = TempCase::new("pack-shape-valid");
        let root = case.materialized.join("Exact Pack Name");
        fs::create_dir_all(root.join("z-member/assets/nested")).expect("create z member");
        fs::create_dir_all(root.join("a-member")).expect("create a member");
        fs::write(root.join("z-member/SKILL.md"), b"z").expect("write z skill");
        fs::write(root.join("a-member/SKILL.md"), b"a").expect("write a skill");
        fs::write(root.join("z-member/assets/nested/SKILL.md"), b"nested")
            .expect("write nested asset");

        let validated = validate_skill_pack_root(root).expect("valid pack shape");

        assert_eq!(validated.pack_name, "Exact Pack Name");
        assert_eq!(
            validated
                .members
                .iter()
                .map(|member| member.pack_member_key.as_str())
                .collect::<Vec<_>>(),
            vec!["a-member", "z-member"]
        );
    }

    #[test]
    fn pack_shape_accepts_one_member() {
        let case = TempCase::new("pack-shape-one");
        let root = case.materialized.join("pack");
        fs::create_dir_all(root.join("only")).expect("create member");
        fs::write(root.join("only/SKILL.md"), b"skill").expect("write skill");

        let validated = validate_skill_pack_root(root).expect("valid one-member pack");

        assert_eq!(validated.members.len(), 1);
        assert_eq!(validated.members[0].pack_member_key, "only");
    }

    #[test]
    fn materialized_preflight_normalizes_plain_description_without_touching_other_content() {
        let case = TempCase::new("normalize-description");
        let skill_dir = case.materialized.join("aso-router");
        fs::create_dir_all(skill_dir.as_path()).expect("create skill directory");
        fs::write(
            skill_dir.join("SKILL.md"),
            b"---\nname: aso-router\ndescription: Routes requests. Triggers: /aso\nmetadata:\n  version: 1.0.0\n---\nInstructions\n",
        )
        .expect("write malformed skill");

        normalize_materialized_skill_frontmatter(skill_dir.as_path())
            .expect("materialized preflight should normalize description");

        let normalized = fs::read_to_string(skill_dir.join("SKILL.md"))
            .expect("read normalized materialized skill");
        assert!(normalized.contains("description: >-\n  Routes requests. Triggers: /aso\n"));
        assert!(normalized.ends_with("---\nInstructions\n"));
        normalize_materialized_skill_frontmatter(skill_dir.as_path())
            .expect("normalization should be idempotent");
        assert_eq!(
            fs::read_to_string(skill_dir.join("SKILL.md"))
                .expect("read normalized materialized skill again"),
            normalized
        );
    }

    #[test]
    fn pack_shape_rejects_non_directory_empty_root_files_and_missing_member_skill() {
        let non_directory = TempCase::new("pack-root-file");
        let root_file = non_directory.materialized.join("pack");
        fs::write(root_file.as_path(), b"not a directory").expect("write root file");
        assert!(
            validate_skill_pack_root(root_file)
                .expect_err("file root should fail")
                .to_string()
                .contains("must be a directory")
        );

        let empty = TempCase::new("pack-root-empty");
        let empty_root = empty.materialized.join("pack");
        fs::create_dir_all(empty_root.as_path()).expect("create empty root");
        assert!(
            validate_skill_pack_root(empty_root)
                .expect_err("empty root should fail")
                .to_string()
                .contains("at least one")
        );

        for file_name in ["SKILL.md", "README.md", "LICENSE", ".DS_Store"] {
            let case = TempCase::new(file_name);
            let root = case.materialized.join("pack");
            fs::create_dir_all(root.join("member")).expect("create member");
            fs::write(root.join("member/SKILL.md"), b"skill").expect("write member skill");
            fs::write(root.join(file_name), b"root file").expect("write root file");
            let error = validate_skill_pack_root(root)
                .expect_err("root regular file should fail")
                .to_string();
            assert!(error.contains("only immediate child directories"));
        }

        let missing = TempCase::new("pack-member-missing-skill");
        let missing_root = missing.materialized.join("pack");
        fs::create_dir_all(missing_root.join("member/nested")).expect("create nested member");
        fs::write(missing_root.join("member/nested/SKILL.md"), b"nested")
            .expect("write nested skill");
        let error = validate_skill_pack_root(missing_root)
            .expect_err("member without direct SKILL.md should fail")
            .to_string();
        assert!(error.contains("member") && error.contains("direct SKILL.md"));
    }

    #[cfg(unix)]
    #[test]
    fn pack_shape_rejects_non_utf8_root_and_member_names() {
        use std::os::unix::ffi::OsStringExt;

        let invalid = std::ffi::OsString::from_vec(vec![b'n', 0xff]);
        let root_error = exact_utf8_component(invalid.as_os_str(), "pack root")
            .expect_err("non-UTF-8 root should fail")
            .to_string();
        let member_error = exact_utf8_component(invalid.as_os_str(), "pack member")
            .expect_err("non-UTF-8 member should fail")
            .to_string();

        assert!(root_error.contains("pack root name must be valid UTF-8"));
        assert!(member_error.contains("pack member name must be valid UTF-8"));
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_archive_names_are_rejected_without_lossy_persistence() {
        let invalid_root = TempCase::new("non-utf8-root-archive");
        let invalid_root_archive = invalid_root.path.join("pack.tar.gz");
        write_raw_archive(
            invalid_root_archive.as_path(),
            &[(b"\xff/member/SKILL.md", b"skill")],
        );
        let root_result = extract_archive_secure(
            invalid_root_archive.as_path(),
            invalid_root.materialized.as_path(),
            1024,
            8,
            1024,
        );
        if let Ok(invalid_root_source) = root_result {
            let root_error = validate_skill_pack_root(invalid_root_source)
                .expect_err("pack endpoint should reject a non-UTF-8 root")
                .to_string();
            assert!(root_error.contains("pack root name must be valid UTF-8"));
        }
        assert!(!invalid_root.materialized.join("\u{fffd}").exists());

        let invalid_member = TempCase::new("non-utf8-member-archive");
        let invalid_member_archive = invalid_member.path.join("pack.tar.gz");
        write_raw_archive(
            invalid_member_archive.as_path(),
            &[(b"pack/\xff/SKILL.md", b"skill")],
        );
        let member_result = extract_archive_secure(
            invalid_member_archive.as_path(),
            invalid_member.materialized.as_path(),
            1024,
            8,
            1024,
        );
        if let Ok(invalid_member_source) = member_result {
            let member_error = validate_skill_pack_root(invalid_member_source)
                .expect_err("pack endpoint should reject a non-UTF-8 member")
                .to_string();
            assert!(member_error.contains("pack member name must be valid UTF-8"));
        }
        assert!(!invalid_member.materialized.join("pack/\u{fffd}").exists());
    }

    #[cfg(unix)]
    #[test]
    fn extraction_does_not_collapse_distinct_roots_through_lossy_text() {
        let case = TempCase::new("lossy-root-collision");
        let archive = case.path.join("pack.tar.gz");
        write_raw_archive(
            archive.as_path(),
            &[
                (b"\xef\xbf\xbd/SKILL.md", b"skill"),
                (b"\xff/ignored.txt", b"ignored"),
            ],
        );

        let error = extract_archive_secure(
            archive.as_path(),
            case.materialized.as_path(),
            1024,
            8,
            1024,
        )
        .expect_err("byte-distinct top-level roots must remain distinct")
        .to_string();

        assert!(error.contains("multiple top-level roots"));
    }

    fn upload_frame(header: &SkillsUploadChunkHeader, chunk: &[u8]) -> Vec<u8> {
        let header_bytes = serde_json::to_vec(header).expect("encode header");
        let mut frame = Vec::with_capacity(8 + header_bytes.len() + chunk.len());
        frame.extend_from_slice(SKILL_UPLOAD_CHUNK_FRAME_MAGIC);
        frame.extend_from_slice(
            &u32::try_from(header_bytes.len())
                .expect("header length")
                .to_be_bytes(),
        );
        frame.extend_from_slice(header_bytes.as_slice());
        frame.extend_from_slice(chunk);
        frame
    }

    struct TempCase {
        path: PathBuf,
        materialized: PathBuf,
    }

    impl TempCase {
        fn new(name: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!("pioneer-upload-extract-{name}-{nanos}"));
            let materialized = path.join("materialized");
            fs::create_dir_all(materialized.as_path()).expect("create temp dirs");
            Self { path, materialized }
        }
    }

    impl Drop for TempCase {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.path.as_path());
        }
    }

    #[derive(Clone)]
    struct TestEntry {
        path: &'static str,
        kind: TestEntryKind,
    }

    #[derive(Clone)]
    enum TestEntryKind {
        Dir,
        File(&'static [u8]),
        Symlink(&'static str),
        Hardlink(&'static str),
        Special,
    }

    impl TestEntry {
        fn dir(path: &'static str) -> Self {
            Self {
                path,
                kind: TestEntryKind::Dir,
            }
        }

        fn file(path: &'static str, contents: &'static [u8]) -> Self {
            Self {
                path,
                kind: TestEntryKind::File(contents),
            }
        }

        fn symlink(path: &'static str, target: &'static str) -> Self {
            Self {
                path,
                kind: TestEntryKind::Symlink(target),
            }
        }

        fn hardlink(path: &'static str, target: &'static str) -> Self {
            Self {
                path,
                kind: TestEntryKind::Hardlink(target),
            }
        }

        fn special(path: &'static str) -> Self {
            Self {
                path,
                kind: TestEntryKind::Special,
            }
        }
    }

    fn write_archive(path: &std::path::Path, entries: &[TestEntry]) {
        let file = fs::File::create(path).expect("create archive");
        let encoder = GzBuilder::new()
            .mtime(0)
            .write(file, Compression::default());
        let mut builder = Builder::new(encoder);
        for entry in entries {
            append_test_entry(&mut builder, entry);
        }
        builder
            .into_inner()
            .expect("finish tar")
            .finish()
            .expect("finish gzip");
    }

    #[cfg(unix)]
    fn write_raw_archive(path: &std::path::Path, entries: &[(&[u8], &[u8])]) {
        let file = fs::File::create(path).expect("create archive");
        let encoder = GzBuilder::new()
            .mtime(0)
            .write(file, Compression::default());
        let mut builder = Builder::new(encoder);
        for (entry_path, contents) in entries {
            let mut header = Header::new_gnu();
            set_test_header_path_bytes(&mut header, entry_path);
            header.set_entry_type(EntryType::Regular);
            header.set_size(u64::try_from(contents.len()).expect("content len"));
            header.set_uid(0);
            header.set_gid(0);
            header.set_mtime(0);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append(&header, Cursor::new(*contents))
                .expect("append raw-path file");
        }
        builder
            .into_inner()
            .expect("finish tar")
            .finish()
            .expect("finish gzip");
    }

    fn append_test_entry<W: io::Write>(builder: &mut Builder<W>, entry: &TestEntry) {
        let mut header = Header::new_gnu();
        set_test_header_path(&mut header, entry.path);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_mode(0o644);

        match &entry.kind {
            TestEntryKind::Dir => {
                header.set_entry_type(EntryType::Directory);
                header.set_size(0);
                header.set_mode(0o755);
                header.set_cksum();
                builder
                    .append(&header, Cursor::new(Vec::<u8>::new()))
                    .expect("append dir");
            }
            TestEntryKind::File(contents) => {
                header.set_entry_type(EntryType::Regular);
                header.set_size(u64::try_from(contents.len()).expect("content len"));
                header.set_cksum();
                builder
                    .append(&header, Cursor::new(*contents))
                    .expect("append file");
            }
            TestEntryKind::Symlink(target) => {
                header.set_entry_type(EntryType::Symlink);
                header.set_size(0);
                header.set_link_name(target).expect("set symlink target");
                header.set_cksum();
                builder
                    .append(&header, Cursor::new(Vec::<u8>::new()))
                    .expect("append symlink");
            }
            TestEntryKind::Hardlink(target) => {
                header.set_entry_type(EntryType::Link);
                header.set_size(0);
                header.set_link_name(target).expect("set hardlink target");
                header.set_cksum();
                builder
                    .append(&header, Cursor::new(Vec::<u8>::new()))
                    .expect("append hardlink");
            }
            TestEntryKind::Special => {
                header.set_entry_type(EntryType::Char);
                header.set_device_major(1).expect("set major");
                header.set_device_minor(3).expect("set minor");
                header.set_size(0);
                header.set_cksum();
                builder
                    .append(&header, Cursor::new(Vec::<u8>::new()))
                    .expect("append special");
            }
        }
    }

    fn set_test_header_path(header: &mut Header, path: &str) {
        if header.set_path(path).is_ok() {
            return;
        }

        let raw = path.as_bytes();
        assert!(raw.len() <= 100, "test path must fit tar name field");
        let bytes = header.as_mut_bytes();
        bytes[0..100].fill(0);
        bytes[0..raw.len()].copy_from_slice(raw);
    }

    #[cfg(unix)]
    fn set_test_header_path_bytes(header: &mut Header, path: &[u8]) {
        assert!(path.len() <= 100, "test path must fit tar name field");
        let bytes = header.as_mut_bytes();
        bytes[0..100].fill(0);
        bytes[0..path.len()].copy_from_slice(path);
    }
}
