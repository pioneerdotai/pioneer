use super::super::*;
use crate::message::artifacts::capabilities::{
    ARTIFACT_UPLOAD_MAX_CHUNK_SIZE_BYTES, ARTIFACT_UPLOAD_MAX_FILE_SIZE_BYTES,
    ARTIFACT_UPLOAD_RECOMMENDED_CHUNK_SIZE_BYTES,
};
use anyhow::{Context, Result, bail};
use pioneer_artifacts::{IngestArtifactTempFileRequest, mime::classify_kind};
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactRole,
    ArtifactUploadAbortParams, ArtifactUploadAbortResponse, ArtifactUploadChunkAckNotification,
    ArtifactUploadChunkHeader, ArtifactUploadFinishParams, ArtifactUploadFinishResponse,
    ArtifactUploadStartParams, ArtifactUploadStartResponse,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

pub(in crate::message) const ARTIFACT_UPLOAD_CHUNK_FRAME_MAGIC: &[u8; 4] = b"ARTU";
const ARTIFACT_UPLOAD_TTL_SECS: i64 = 3600;

#[derive(Debug, Clone)]
pub(in crate::message) struct ArtifactUploadSession {
    pub upload_id: String,
    pub connection_id: ConnectionId,
    pub workspace_id: String,
    pub thread_id: Option<String>,
    pub planned_turn_id: Option<String>,
    pub client_attachment_id: String,
    pub display_name: String,
    pub mime_type: Option<String>,
    pub expected_size_bytes: u64,
    pub expected_sha256: String,
    pub received_bytes: u64,
    pub temp_path: PathBuf,
    pub expires_at: i64,
}

#[derive(Debug)]
pub(in crate::message) struct ArtifactUploadSessionManager {
    temp_root: PathBuf,
    sessions: Mutex<HashMap<String, ArtifactUploadSession>>,
}

impl ArtifactUploadSessionManager {
    pub fn new(temp_root: PathBuf) -> Self {
        Self {
            temp_root,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn start(
        &self,
        connection_id: ConnectionId,
        params: ArtifactUploadStartParams,
        now: i64,
    ) -> Result<ArtifactUploadSession> {
        self.prune_expired(now).await;
        let upload_id = pioneer_protocol::generate_id(21);
        let upload_dir = self
            .temp_root
            .join(sanitize_path_segment(params.workspace_id.as_str()))
            .join(upload_id.as_str());
        let temp_path = upload_dir.join("payload.bin");
        tokio::fs::create_dir_all(upload_dir.as_path())
            .await
            .with_context(|| {
                format!(
                    "failed to create artifact upload dir {}",
                    upload_dir.display()
                )
            })?;
        let _ = tokio::fs::remove_file(temp_path.as_path()).await;

        let session = ArtifactUploadSession {
            upload_id: upload_id.clone(),
            connection_id,
            workspace_id: params.workspace_id,
            thread_id: params.thread_id,
            planned_turn_id: params.planned_turn_id,
            client_attachment_id: params.client_attachment_id,
            display_name: sanitize_file_name(params.file_name.as_str()),
            mime_type: params.mime_type,
            expected_size_bytes: params.size_bytes,
            expected_sha256: params.sha256,
            received_bytes: 0,
            temp_path,
            expires_at: now.saturating_add(ARTIFACT_UPLOAD_TTL_SECS),
        };
        self.sessions
            .lock()
            .await
            .insert(upload_id, session.clone());
        Ok(session)
    }

    pub async fn append_chunk(
        &self,
        connection_id: ConnectionId,
        header: &ArtifactUploadChunkHeader,
        chunk: &[u8],
        now: i64,
    ) -> Result<ArtifactUploadSession> {
        self.prune_expired(now).await;
        let mut guard = self.sessions.lock().await;
        let session = guard
            .get_mut(header.upload_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("artifact upload not found"))?;
        validate_session_owner(session, connection_id, header.workspace_id.as_str(), now)?;
        if chunk.is_empty() {
            bail!("artifact upload chunk is empty");
        }
        if chunk.len() > ARTIFACT_UPLOAD_MAX_CHUNK_SIZE_BYTES as usize {
            bail!("artifact upload chunk exceeds max chunk size");
        }
        if header.offset != session.received_bytes {
            bail!("artifact upload chunk offset mismatch");
        }
        let next_offset = header.offset.saturating_add(header.len);
        if next_offset > session.expected_size_bytes {
            bail!("artifact upload chunk exceeds declared size");
        }
        append_chunk(session.temp_path.as_path(), chunk, header.offset)?;
        session.received_bytes = next_offset;
        Ok(session.clone())
    }

    pub async fn finish(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        upload_id: &str,
        now: i64,
    ) -> Result<ArtifactUploadSession> {
        self.prune_expired(now).await;
        let session = {
            let guard = self.sessions.lock().await;
            guard
                .get(upload_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("artifact upload not found"))?
        };
        validate_session_owner(&session, connection_id, workspace_id, now)?;
        if session.received_bytes != session.expected_size_bytes {
            bail!("artifact upload is incomplete");
        }
        let actual_sha256 = sha256_file(session.temp_path.as_path())?;
        if actual_sha256 != session.expected_sha256 {
            self.abort_upload(upload_id).await;
            bail!("artifact upload final sha256 mismatch");
        }
        Ok(session)
    }

    pub async fn complete_success(&self, upload_id: &str) {
        self.abort_upload(upload_id).await;
    }

    pub async fn abort(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        upload_id: &str,
        now: i64,
    ) -> Result<()> {
        self.prune_expired(now).await;
        let session = {
            let guard = self.sessions.lock().await;
            guard
                .get(upload_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("artifact upload not found"))?
        };
        validate_session_owner(&session, connection_id, workspace_id, now)?;
        self.abort_upload(upload_id).await;
        Ok(())
    }

    pub async fn abort_connection(&self, connection_id: ConnectionId) {
        let upload_ids = {
            let guard = self.sessions.lock().await;
            guard
                .values()
                .filter(|session| session.connection_id == connection_id)
                .map(|session| session.upload_id.clone())
                .collect::<Vec<_>>()
        };
        for upload_id in upload_ids {
            self.abort_upload(upload_id.as_str()).await;
        }
    }

    pub async fn prune_expired(&self, now: i64) -> usize {
        let expired_sessions = {
            let mut guard = self.sessions.lock().await;
            let upload_ids = guard
                .values()
                .filter(|session| session.expires_at <= now)
                .map(|session| session.upload_id.clone())
                .collect::<Vec<_>>();
            upload_ids
                .into_iter()
                .filter_map(|upload_id| guard.remove(upload_id.as_str()))
                .collect::<Vec<_>>()
        };
        let removed = expired_sessions.len();
        for session in expired_sessions {
            remove_upload_temp(session.temp_path).await;
        }
        removed
    }

    async fn abort_upload(&self, upload_id: &str) {
        let session = self.sessions.lock().await.remove(upload_id);
        if let Some(session) = session {
            remove_upload_temp(session.temp_path).await;
        }
    }
}

async fn remove_upload_temp(temp_path: PathBuf) {
    if let Some(parent) = temp_path.parent() {
        let _ = tokio::fs::remove_dir_all(parent).await;
    } else {
        let _ = tokio::fs::remove_file(temp_path).await;
    }
}

impl MessageProcessor {
    pub(crate) async fn artifact_upload_start(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        mut params: ArtifactUploadStartParams,
    ) {
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_UPLOAD_START,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };
        params.workspace_id = workspace_id;

        if params.file_name.trim().is_empty()
            || params.client_attachment_id.trim().is_empty()
            || params.size_bytes > ARTIFACT_UPLOAD_MAX_FILE_SIZE_BYTES
            || !is_lower_hex_sha256(params.sha256.as_str())
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    "invalid artifact upload start params",
                ),
            )
            .await;
            return;
        }

        let now = now_timestamp_secs();
        let session = match self
            .artifact_uploads
            .start(connection_id, params, now)
            .await
        {
            Ok(session) => session,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to create artifact upload session: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let payload = ArtifactUploadStartResponse {
            upload_id: session.upload_id,
            recommended_chunk_size_bytes: ARTIFACT_UPLOAD_RECOMMENDED_CHUNK_SIZE_BYTES,
            max_chunk_size_bytes: ARTIFACT_UPLOAD_MAX_CHUNK_SIZE_BYTES,
            max_size_bytes: ARTIFACT_UPLOAD_MAX_FILE_SIZE_BYTES,
            expires_at_unix: session.expires_at,
        };
        self.send_artifact_result(
            connection_id,
            request_id,
            &payload,
            methods::ARTIFACT_UPLOAD_START,
        )
        .await;
    }

    pub(crate) async fn artifact_upload_finish(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ArtifactUploadFinishParams,
    ) {
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_UPLOAD_FINISH,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };
        let now = now_timestamp_secs();
        let session = match self
            .artifact_uploads
            .finish(
                connection_id,
                workspace_id.as_str(),
                params.upload_id.as_str(),
                now,
            )
            .await
        {
            Ok(session) => session,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("artifact upload finish failed: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let binding =
            session
                .thread_id
                .as_ref()
                .map(|thread_id| pioneer_artifacts::ArtifactBindingTarget {
                    thread_id: Some(thread_id.clone()),
                    turn_id: session.planned_turn_id.clone(),
                    message_id: None,
                    turn_item_id: None,
                    tool_call_id: None,
                    task_id: None,
                    task_run_id: None,
                    binding_kind: ArtifactBindingKind::DraftUpload,
                    direction: ArtifactBindingDirection::Input,
                    role: Some(ArtifactRole::User),
                    item_index: None,
                });
        let mime_type = session.mime_type.clone();
        let kind = classify_kind(mime_type.as_deref(), Some(session.temp_path.as_path()));
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert(
            "client_attachment_id".to_owned(),
            json!(session.client_attachment_id),
        );
        metadata.insert("source_kind".to_owned(), json!("remote_upload"));

        let summary = match self
            .artifact_service
            .ingest_temp_file(IngestArtifactTempFileRequest {
                workspace_id: session.workspace_id.clone(),
                primary_thread_id: session.thread_id.clone(),
                temp_path: session.temp_path.clone(),
                display_name: session.display_name.clone(),
                kind,
                mime_type,
                created_by_kind: ArtifactCreatedByKind::User,
                created_by_actor_id: None,
                binding,
                metadata,
            })
            .await
        {
            Ok(summary) => summary,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to persist artifact upload: {error}"),
                    ),
                )
                .await;
                return;
            }
        };
        self.artifact_uploads
            .complete_success(params.upload_id.as_str())
            .await;

        let artifact = summary.artifact.clone();
        if let Some(thread_id) = session.thread_id.as_deref() {
            self.send_notification_to_thread_subscribers(
                thread_id,
                events::ARTIFACT_CREATED,
                &ArtifactCreatedNotification {
                    workspace_id: session.workspace_id.clone(),
                    artifact: summary.clone(),
                },
            )
            .await;
            self.send_notification_to_thread_subscribers(
                thread_id,
                events::THREAD_ARTIFACTS_CHANGED,
                &ThreadArtifactsChangedNotification {
                    workspace_id: session.workspace_id.clone(),
                    thread_id: thread_id.to_owned(),
                    artifact_ids: vec![artifact.artifact_id.clone()],
                    reason: "user_upload".to_owned(),
                    generated_at: now_timestamp_secs(),
                },
            )
            .await;
        }

        let payload = ArtifactUploadFinishResponse {
            upload_id: params.upload_id,
            artifact,
        };
        self.send_artifact_result(
            connection_id,
            request_id,
            &payload,
            methods::ARTIFACT_UPLOAD_FINISH,
        )
        .await;
    }

    pub(crate) async fn artifact_upload_abort(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ArtifactUploadAbortParams,
    ) {
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_UPLOAD_ABORT,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };
        let now = now_timestamp_secs();
        if let Err(error) = self
            .artifact_uploads
            .abort(
                connection_id,
                workspace_id.as_str(),
                params.upload_id.as_str(),
                now,
            )
            .await
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!("artifact upload abort failed: {error:#}"),
                ),
            )
            .await;
            return;
        }
        let payload = ArtifactUploadAbortResponse {
            upload_id: params.upload_id,
            status: "aborted".to_owned(),
        };
        self.send_artifact_result(
            connection_id,
            request_id,
            &payload,
            methods::ARTIFACT_UPLOAD_ABORT,
        )
        .await;
    }

    pub(crate) async fn process_artifact_upload_chunk_frame(
        &self,
        connection_id: ConnectionId,
        frame: &[u8],
    ) {
        let (header, chunk) = match parse_artifact_upload_chunk_frame(frame) {
            Ok(value) => value,
            Err(error) => {
                warn!(connection_id, error = %format!("{error:#}"), "invalid artifact upload chunk frame");
                return;
            }
        };
        let now = now_timestamp_secs();
        let updated = match self
            .artifact_uploads
            .append_chunk(connection_id, &header, chunk, now)
            .await
        {
            Ok(session) => session,
            Err(error) => {
                warn!(
                    connection_id,
                    upload_id = header.upload_id.as_str(),
                    error = %format!("{error:#}"),
                    "artifact upload chunk rejected"
                );
                return;
            }
        };
        let ack = ArtifactUploadChunkAckNotification {
            workspace_id: updated.workspace_id,
            upload_id: updated.upload_id,
            offset: header.offset,
            len: header.len,
            received_bytes: updated.received_bytes,
            next_offset: updated.received_bytes,
        };
        let notification = match JsonRpcNotification::from_params(
            events::ARTIFACT_UPLOAD_CHUNK_ACK,
            &ack,
        ) {
            Ok(notification) => notification,
            Err(error) => {
                warn!(connection_id, error = %error, "failed to encode artifact upload chunk ack");
                return;
            }
        };
        if let Err(error) = self.send_json(connection_id, &notification).await {
            warn!(connection_id, error = %format!("{error:#}"), "failed to send artifact upload chunk ack");
        }
    }

    pub(in crate::message) async fn validate_artifact_workspace(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        workspace_id: String,
        method: &str,
    ) -> Result<String, JsonRpcErrorResponse> {
        if workspace_id.trim().is_empty() {
            return Err(JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_PARAMS_CODE,
                format!("`workspace_id` is required for `{method}`"),
            ));
        }
        let workspace_id = self
            .workspace_manager
            .validate_workspace_id(workspace_id.as_str())
            .await
            .map_err(|error| {
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!("workspace unavailable for `{method}`: {error}"),
                )
            })?;
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;
        Ok(workspace_id)
    }

    pub(in crate::message) async fn send_artifact_result<T: serde::Serialize>(
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
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode {label} response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };
        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(connection_id, error = %format!("{error:#}"), "failed to send {label} response");
        }
    }
}

pub(in crate::message) fn parse_artifact_upload_chunk_frame(
    frame: &[u8],
) -> Result<(ArtifactUploadChunkHeader, &[u8])> {
    if frame.len() < 8 {
        bail!("artifact upload frame is too short");
    }
    if &frame[0..4] != ARTIFACT_UPLOAD_CHUNK_FRAME_MAGIC {
        bail!("artifact upload frame has invalid magic");
    }
    let header_len = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
    let header_start = 8usize;
    let header_end = header_start.saturating_add(header_len);
    if header_end > frame.len() {
        bail!("artifact upload frame header length exceeds frame length");
    }
    let header =
        serde_json::from_slice::<ArtifactUploadChunkHeader>(&frame[header_start..header_end])
            .context("failed to parse artifact upload frame header")?;
    let chunk = &frame[header_end..];
    if header.len != u64::try_from(chunk.len()).unwrap_or(u64::MAX) {
        bail!("artifact upload frame chunk length mismatch");
    }
    if let Some(expected) = header.chunk_sha256.as_deref() {
        let actual = sha256_bytes(chunk);
        if expected != actual {
            bail!("artifact upload frame chunk sha256 mismatch");
        }
    }
    Ok((header, chunk))
}

fn validate_session_owner(
    session: &ArtifactUploadSession,
    connection_id: ConnectionId,
    workspace_id: &str,
    now: i64,
) -> Result<()> {
    if session.workspace_id != workspace_id || session.connection_id != connection_id {
        bail!("artifact upload not found");
    }
    if session.expires_at <= now {
        bail!("artifact upload expired");
    }
    Ok(())
}

fn append_chunk(path: &Path, chunk: &[u8], offset: u64) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create artifact upload dir {}", parent.display())
        })?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(path)
        .with_context(|| format!("failed to open artifact upload payload {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to stat artifact upload payload {}", path.display()))?;
    if metadata.len() < offset {
        bail!(
            "payload length {} does not match chunk offset {}",
            metadata.len(),
            offset
        );
    }
    if metadata.len() > offset {
        file.set_len(offset)
            .context("failed to truncate artifact upload payload")?;
    }
    file.seek(SeekFrom::Start(offset))
        .context("failed to seek artifact upload payload")?;
    file.write_all(chunk)
        .context("failed to write artifact upload payload")?;
    file.sync_data()
        .context("failed to flush artifact upload payload")?;
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn sha256_file(path: &Path) -> Result<String> {
    use std::io::Read;

    let mut file =
        fs::File::open(path).with_context(|| format!("failed to open `{}`", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read `{}`", path.display()))?;
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

fn sanitize_path_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn sanitize_file_name(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_control() || ch == '/' || ch == '\\' {
                '_'
            } else {
                ch
            }
        })
        .collect::<String>();
    let trimmed = sanitized.trim();
    if trimmed.is_empty() {
        "artifact".to_owned()
    } else {
        trimmed.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_upload_parse_valid_frame() {
        let chunk = b"hello";
        let header = ArtifactUploadChunkHeader {
            workspace_id: "ws_a".to_owned(),
            upload_id: "upl_a".to_owned(),
            offset: 0,
            len: chunk.len() as u64,
            chunk_sha256: Some(sha256_bytes(chunk)),
        };
        let header_json = serde_json::to_vec(&header).unwrap();
        let mut frame = Vec::new();
        frame.extend_from_slice(ARTIFACT_UPLOAD_CHUNK_FRAME_MAGIC);
        frame.extend_from_slice(&(header_json.len() as u32).to_be_bytes());
        frame.extend_from_slice(&header_json);
        frame.extend_from_slice(chunk);

        let (parsed, parsed_chunk) = parse_artifact_upload_chunk_frame(&frame).unwrap();

        assert_eq!(parsed, header);
        assert_eq!(parsed_chunk, chunk);
    }

    #[test]
    fn artifact_upload_parse_rejects_sha_mismatch() {
        let chunk = b"hello";
        let header = ArtifactUploadChunkHeader {
            workspace_id: "ws_a".to_owned(),
            upload_id: "upl_a".to_owned(),
            offset: 0,
            len: chunk.len() as u64,
            chunk_sha256: Some("0".repeat(64)),
        };
        let header_json = serde_json::to_vec(&header).unwrap();
        let mut frame = Vec::new();
        frame.extend_from_slice(ARTIFACT_UPLOAD_CHUNK_FRAME_MAGIC);
        frame.extend_from_slice(&(header_json.len() as u32).to_be_bytes());
        frame.extend_from_slice(&header_json);
        frame.extend_from_slice(chunk);

        assert!(parse_artifact_upload_chunk_frame(&frame).is_err());
    }

    fn start_params(
        workspace_id: &str,
        sha256: String,
        size_bytes: u64,
    ) -> ArtifactUploadStartParams {
        ArtifactUploadStartParams {
            workspace_id: workspace_id.to_owned(),
            thread_id: Some("thr_a".to_owned()),
            planned_turn_id: Some("turn_a".to_owned()),
            client_attachment_id: "client_a".to_owned(),
            file_name: "upload.txt".to_owned(),
            mime_type: Some("text/plain".to_owned()),
            size_bytes,
            sha256,
            source_kind: pioneer_protocol::ArtifactUploadSourceKind::UserComposer,
        }
    }

    #[tokio::test]
    async fn artifact_upload_session_start_and_valid_chunk_updates_offset() {
        let temp = tempfile::tempdir().expect("temp dir");
        let manager = ArtifactUploadSessionManager::new(temp.path().join("uploads"));
        let chunk = b"hello";
        let session = manager
            .start(
                7,
                start_params("ws_a", sha256_bytes(chunk), chunk.len() as u64),
                10,
            )
            .await
            .expect("start upload");
        let header = ArtifactUploadChunkHeader {
            workspace_id: "ws_a".to_owned(),
            upload_id: session.upload_id.clone(),
            offset: 0,
            len: chunk.len() as u64,
            chunk_sha256: Some(sha256_bytes(chunk)),
        };

        let updated = manager
            .append_chunk(7, &header, chunk, 11)
            .await
            .expect("append chunk");

        assert_eq!(updated.received_bytes, chunk.len() as u64);
        assert!(updated.temp_path.exists());
    }

    #[tokio::test]
    async fn artifact_upload_session_offset_mismatch_fails() {
        let temp = tempfile::tempdir().expect("temp dir");
        let manager = ArtifactUploadSessionManager::new(temp.path().join("uploads"));
        let chunk = b"hello";
        let session = manager
            .start(
                7,
                start_params("ws_a", sha256_bytes(chunk), chunk.len() as u64),
                10,
            )
            .await
            .expect("start upload");
        let header = ArtifactUploadChunkHeader {
            workspace_id: "ws_a".to_owned(),
            upload_id: session.upload_id,
            offset: 1,
            len: chunk.len() as u64,
            chunk_sha256: Some(sha256_bytes(chunk)),
        };

        assert!(manager.append_chunk(7, &header, chunk, 11).await.is_err());
    }

    #[tokio::test]
    async fn artifact_upload_session_is_connection_bound() {
        let temp = tempfile::tempdir().expect("temp dir");
        let manager = ArtifactUploadSessionManager::new(temp.path().join("uploads"));
        let chunk = b"hello";
        let session = manager
            .start(
                7,
                start_params("ws_a", sha256_bytes(chunk), chunk.len() as u64),
                10,
            )
            .await
            .expect("start upload");
        let header = ArtifactUploadChunkHeader {
            workspace_id: "ws_a".to_owned(),
            upload_id: session.upload_id,
            offset: 0,
            len: chunk.len() as u64,
            chunk_sha256: Some(sha256_bytes(chunk)),
        };

        assert!(manager.append_chunk(8, &header, chunk, 11).await.is_err());
    }

    #[tokio::test]
    async fn artifact_upload_final_sha_mismatch_removes_temp_state() {
        let temp = tempfile::tempdir().expect("temp dir");
        let manager = ArtifactUploadSessionManager::new(temp.path().join("uploads"));
        let chunk = b"hello";
        let session = manager
            .start(
                7,
                start_params("ws_a", "0".repeat(64), chunk.len() as u64),
                10,
            )
            .await
            .expect("start upload");
        let upload_id = session.upload_id.clone();
        let temp_path = session.temp_path.clone();
        let header = ArtifactUploadChunkHeader {
            workspace_id: "ws_a".to_owned(),
            upload_id: upload_id.clone(),
            offset: 0,
            len: chunk.len() as u64,
            chunk_sha256: Some(sha256_bytes(chunk)),
        };
        manager
            .append_chunk(7, &header, chunk, 11)
            .await
            .expect("append chunk");

        assert!(manager.finish(7, "ws_a", &upload_id, 12).await.is_err());
        assert!(!temp_path.exists());
        assert!(!manager.sessions.lock().await.contains_key(&upload_id));
    }

    #[tokio::test]
    async fn artifact_upload_connection_close_cleans_temp_files() {
        let temp = tempfile::tempdir().expect("temp dir");
        let manager = ArtifactUploadSessionManager::new(temp.path().join("uploads"));
        let chunk = b"hello";
        let session = manager
            .start(
                7,
                start_params("ws_a", sha256_bytes(chunk), chunk.len() as u64),
                10,
            )
            .await
            .expect("start upload");
        let upload_id = session.upload_id.clone();
        let temp_path = session.temp_path.clone();
        let header = ArtifactUploadChunkHeader {
            workspace_id: "ws_a".to_owned(),
            upload_id: upload_id.clone(),
            offset: 0,
            len: chunk.len() as u64,
            chunk_sha256: Some(sha256_bytes(chunk)),
        };
        manager
            .append_chunk(7, &header, chunk, 11)
            .await
            .expect("append chunk");

        manager.abort_connection(7).await;

        assert!(!temp_path.exists());
        assert!(!manager.sessions.lock().await.contains_key(&upload_id));
    }

    #[tokio::test]
    async fn artifact_upload_prunes_expired_sessions_and_temp_files() {
        let temp = tempfile::tempdir().expect("temp dir");
        let manager = ArtifactUploadSessionManager::new(temp.path().join("uploads"));
        let chunk = b"hello";
        let session = manager
            .start(
                7,
                start_params("ws_a", sha256_bytes(chunk), chunk.len() as u64),
                10,
            )
            .await
            .expect("start upload");
        let upload_id = session.upload_id.clone();
        let temp_path = session.temp_path.clone();
        let header = ArtifactUploadChunkHeader {
            workspace_id: "ws_a".to_owned(),
            upload_id: upload_id.clone(),
            offset: 0,
            len: chunk.len() as u64,
            chunk_sha256: Some(sha256_bytes(chunk)),
        };
        manager
            .append_chunk(7, &header, chunk, 11)
            .await
            .expect("append chunk");

        let removed = manager
            .prune_expired(10 + ARTIFACT_UPLOAD_TTL_SECS + 1)
            .await;

        assert_eq!(removed, 1);
        assert!(!temp_path.exists());
        assert!(!manager.sessions.lock().await.contains_key(&upload_id));
    }
}
