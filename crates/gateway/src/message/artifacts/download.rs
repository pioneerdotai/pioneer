use super::super::*;
use crate::message::artifacts::capabilities::{
    ARTIFACT_DOWNLOAD_MAX_CHUNK_SIZE_BYTES, ARTIFACT_DOWNLOAD_MAX_CONCURRENT_DOWNLOADS,
    ARTIFACT_DOWNLOAD_RECOMMENDED_CHUNK_SIZE_BYTES,
};
use anyhow::{Context, Result, bail};
use pioneer_artifacts::ArtifactDownloadSnapshot;
use pioneer_protocol::{
    ArtifactDownloadAbortParams, ArtifactDownloadAbortResponse, ArtifactDownloadChunkHeader,
    ArtifactDownloadChunkParams, ArtifactDownloadChunkResponse, ArtifactDownloadFinishParams,
    ArtifactDownloadFinishResponse, ArtifactDownloadStartParams, ArtifactDownloadStartResponse,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tokio::sync::Mutex;

pub(in crate::message) const ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC: &[u8; 4] = b"ARTD";
const ARTIFACT_DOWNLOAD_TTL_SECS: i64 = 3600;

#[derive(Debug, Clone)]
pub(in crate::message) struct ArtifactDownloadSession {
    pub download_id: String,
    pub owner: AuthenticatedTransferOwner,
    pub workspace_id: String,
    pub thread_id: Option<String>,
    pub artifact_id: String,
    pub artifact_version_id: String,
    pub blob_id: String,
    pub storage_key: String,
    pub display_name: String,
    pub mime_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
    pub created_at: i64,
    pub expires_at: i64,
}

#[derive(Debug, Default)]
pub(in crate::message) struct ArtifactDownloadSessionManager {
    sessions: Mutex<HashMap<String, ArtifactDownloadSession>>,
}

impl ArtifactDownloadSessionManager {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub async fn start<O>(
        &self,
        owner: O,
        snapshot: ArtifactDownloadSnapshot,
        now: i64,
    ) -> Result<ArtifactDownloadSession>
    where
        O: Into<AuthenticatedTransferOwner>,
    {
        self.start_scoped(owner, snapshot, None, now).await
    }

    pub async fn start_scoped<O>(
        &self,
        owner: O,
        snapshot: ArtifactDownloadSnapshot,
        thread_id: Option<String>,
        now: i64,
    ) -> Result<ArtifactDownloadSession>
    where
        O: Into<AuthenticatedTransferOwner>,
    {
        let owner = owner.into();
        let mut guard = self.sessions.lock().await;
        prune_expired_downloads(&mut guard, now);
        let active_for_scope = guard
            .values()
            .filter(|session| {
                session.owner == owner && session.workspace_id == snapshot.workspace_id
            })
            .count();
        if active_for_scope >= ARTIFACT_DOWNLOAD_MAX_CONCURRENT_DOWNLOADS as usize {
            bail!("artifact download concurrency limit exceeded");
        }

        let download_id = pioneer_protocol::generate_id(21);
        let session = ArtifactDownloadSession {
            download_id: download_id.clone(),
            owner,
            workspace_id: snapshot.workspace_id,
            thread_id,
            artifact_id: snapshot.artifact_id,
            artifact_version_id: snapshot.artifact_version_id,
            blob_id: snapshot.blob_id,
            storage_key: snapshot.storage_key,
            display_name: snapshot.display_name,
            mime_type: snapshot.mime_type,
            size_bytes: snapshot.size_bytes,
            sha256: snapshot.sha256,
            created_at: now,
            expires_at: now.saturating_add(ARTIFACT_DOWNLOAD_TTL_SECS),
        };
        guard.insert(download_id, session.clone());
        Ok(session)
    }

    pub async fn get<O>(
        &self,
        owner: O,
        workspace_id: &str,
        download_id: &str,
        now: i64,
    ) -> Result<ArtifactDownloadSession>
    where
        O: Into<AuthenticatedTransferOwner>,
    {
        let owner = owner.into();
        let mut guard = self.sessions.lock().await;
        prune_expired_downloads(&mut guard, now);
        let session = guard
            .get(download_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("artifact download not found"))?;
        validate_session_owner(&session, &owner, workspace_id, now)?;
        Ok(session)
    }

    pub async fn finish<O>(
        &self,
        owner: O,
        workspace_id: &str,
        download_id: &str,
        now: i64,
    ) -> Result<()>
    where
        O: Into<AuthenticatedTransferOwner>,
    {
        let owner = owner.into();
        let session = self.get(&owner, workspace_id, download_id, now).await?;
        self.sessions
            .lock()
            .await
            .remove(session.download_id.as_str());
        Ok(())
    }

    pub async fn abort<O>(
        &self,
        owner: O,
        workspace_id: &str,
        download_id: &str,
        now: i64,
    ) -> Result<()>
    where
        O: Into<AuthenticatedTransferOwner>,
    {
        self.finish(owner, workspace_id, download_id, now).await
    }

    pub async fn abort_connection(&self, connection_id: ConnectionId) {
        self.sessions
            .lock()
            .await
            .retain(|_, session| session.owner.connection_id != connection_id);
    }

    pub async fn abort_connection_scope(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        thread_id: Option<&str>,
    ) -> usize {
        let mut guard = self.sessions.lock().await;
        let before = guard.len();
        guard.retain(|_, session| {
            !(session.owner.connection_id == connection_id
                && session.workspace_id == workspace_id
                && thread_id
                    .is_none_or(|thread_id| session.thread_id.as_deref() == Some(thread_id)))
        });
        before.saturating_sub(guard.len())
    }

    #[cfg(test)]
    async fn session_count(&self) -> usize {
        self.sessions.lock().await.len()
    }
}

impl MessageProcessor {
    pub(crate) async fn artifact_download_start(
        &self,
        request_context: &RequestContext,
        authorization: &crate::authorization::AuthorizedArtifact,
        request_id: RequestId,
        params: ArtifactDownloadStartParams,
    ) {
        let connection_id = request_context.connection_id();
        let owner = AuthenticatedTransferOwner::from_request_context(request_context);
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_DOWNLOAD_START,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let snapshot = match self
            .artifact_service
            .download_snapshot(
                workspace_id.as_str(),
                params.artifact_id.as_str(),
                params.version_id.as_deref(),
            )
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to start artifact download: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        let artifact = snapshot.artifact.clone();
        if authorization.workspace_id() != snapshot.workspace_id
            || authorization.artifact_id() != snapshot.artifact_id
        {
            self.send_error(
                connection_id,
                crate::authorization::AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        let session = match self
            .artifact_downloads
            .start_scoped(
                owner,
                snapshot,
                authorization.thread_id().map(str::to_owned),
                now_timestamp_secs(),
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
                        format!("failed to create artifact download session: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        debug!(
            connection_id,
            download_id = session.download_id.as_str(),
            artifact_id = session.artifact_id.as_str(),
            version_id = session.artifact_version_id.as_str(),
            blob_id = session.blob_id.as_str(),
            mime_type = ?session.mime_type,
            created_at = session.created_at,
            "artifact download session created"
        );

        let preferred = params
            .preferred_chunk_size_bytes
            .unwrap_or(ARTIFACT_DOWNLOAD_RECOMMENDED_CHUNK_SIZE_BYTES);
        let payload = ArtifactDownloadStartResponse {
            download_id: session.download_id,
            artifact,
            file_name: session.display_name,
            size_bytes: session.size_bytes,
            sha256: session.sha256,
            recommended_chunk_size_bytes: preferred
                .clamp(1, ARTIFACT_DOWNLOAD_MAX_CHUNK_SIZE_BYTES),
            max_chunk_size_bytes: ARTIFACT_DOWNLOAD_MAX_CHUNK_SIZE_BYTES,
            expires_at_unix: session.expires_at,
        };
        self.send_artifact_result(
            connection_id,
            request_id,
            &payload,
            methods::ARTIFACT_DOWNLOAD_START,
        )
        .await;
    }

    pub(crate) async fn artifact_download_chunk(
        &self,
        request_context: &RequestContext,
        authorization: &crate::authorization::AuthorizedArtifact,
        request_id: RequestId,
        params: ArtifactDownloadChunkParams,
    ) {
        let connection_id = request_context.connection_id();
        let owner = AuthenticatedTransferOwner::from_request_context(request_context);
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_DOWNLOAD_CHUNK,
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
            .artifact_downloads
            .get(
                &owner,
                workspace_id.as_str(),
                params.download_id.as_str(),
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
                        INVALID_PARAMS_CODE,
                        format!("artifact download chunk failed: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        if authorization.workspace_id() != session.workspace_id
            || authorization.artifact_id() != session.artifact_id
        {
            self.send_error(
                connection_id,
                crate::authorization::AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if let Err(error) = validate_download_range(&session, params.offset, params.len) {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!("artifact download range rejected: {error:#}"),
                ),
            )
            .await;
            return;
        }

        let bytes = match self
            .artifact_service
            .read_blob_range(
                session.workspace_id.as_str(),
                session.storage_key.as_str(),
                params.offset,
                params.len,
            )
            .await
        {
            Ok(bytes) => bytes,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to read artifact download chunk: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        if bytes.len() != usize::try_from(params.len).unwrap_or(usize::MAX) {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    "artifact download chunk read returned a short range",
                ),
            )
            .await;
            return;
        }
        if let Err(error) = send_artifact_download_chunk_frame(
            self.session_manager.as_ref(),
            connection_id,
            &session,
            params.offset,
            bytes.as_slice(),
        )
        .await
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("failed to queue artifact download chunk: {error:#}"),
                ),
            )
            .await;
            return;
        }

        let payload = ArtifactDownloadChunkResponse {
            download_id: params.download_id,
            offset: params.offset,
            len: params.len,
            queued: true,
        };
        self.send_artifact_result(
            connection_id,
            request_id,
            &payload,
            methods::ARTIFACT_DOWNLOAD_CHUNK,
        )
        .await;
    }

    pub(crate) async fn artifact_download_finish(
        &self,
        request_context: &RequestContext,
        authorization: &crate::authorization::AuthorizedArtifact,
        request_id: RequestId,
        params: ArtifactDownloadFinishParams,
    ) {
        let connection_id = request_context.connection_id();
        let owner = AuthenticatedTransferOwner::from_request_context(request_context);
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_DOWNLOAD_FINISH,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };
        let session = match self
            .artifact_downloads
            .get(
                &owner,
                workspace_id.as_str(),
                params.download_id.as_str(),
                now_timestamp_secs(),
            )
            .await
        {
            Ok(session) => session,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!("artifact download finish failed: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        if authorization.workspace_id() != session.workspace_id
            || authorization.artifact_id() != session.artifact_id
        {
            self.send_error(
                connection_id,
                crate::authorization::AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if let Err(error) = self
            .artifact_downloads
            .finish(
                &owner,
                workspace_id.as_str(),
                params.download_id.as_str(),
                now_timestamp_secs(),
            )
            .await
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!("artifact download finish failed: {error:#}"),
                ),
            )
            .await;
            return;
        }
        let payload = ArtifactDownloadFinishResponse {
            download_id: params.download_id,
            status: "finished".to_owned(),
        };
        self.send_artifact_result(
            connection_id,
            request_id,
            &payload,
            methods::ARTIFACT_DOWNLOAD_FINISH,
        )
        .await;
    }

    pub(crate) async fn artifact_download_abort(
        &self,
        request_context: &RequestContext,
        authorization: &crate::authorization::AuthorizedArtifact,
        request_id: RequestId,
        params: ArtifactDownloadAbortParams,
    ) {
        let connection_id = request_context.connection_id();
        let owner = AuthenticatedTransferOwner::from_request_context(request_context);
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_DOWNLOAD_ABORT,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };
        let session = match self
            .artifact_downloads
            .get(
                &owner,
                workspace_id.as_str(),
                params.download_id.as_str(),
                now_timestamp_secs(),
            )
            .await
        {
            Ok(session) => session,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!("artifact download abort failed: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        if authorization.workspace_id() != session.workspace_id
            || authorization.artifact_id() != session.artifact_id
        {
            self.send_error(
                connection_id,
                crate::authorization::AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if let Err(error) = self
            .artifact_downloads
            .abort(
                &owner,
                workspace_id.as_str(),
                params.download_id.as_str(),
                now_timestamp_secs(),
            )
            .await
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!("artifact download abort failed: {error:#}"),
                ),
            )
            .await;
            return;
        }
        let payload = ArtifactDownloadAbortResponse {
            download_id: params.download_id,
            status: "aborted".to_owned(),
        };
        self.send_artifact_result(
            connection_id,
            request_id,
            &payload,
            methods::ARTIFACT_DOWNLOAD_ABORT,
        )
        .await;
    }
}

async fn send_artifact_download_chunk_frame(
    session_manager: &SessionManager,
    connection_id: ConnectionId,
    session: &ArtifactDownloadSession,
    offset: u64,
    chunk: &[u8],
) -> Result<()> {
    let len = u64::try_from(chunk.len()).context("artifact download chunk length overflow")?;
    let header = ArtifactDownloadChunkHeader {
        workspace_id: session.workspace_id.clone(),
        download_id: session.download_id.clone(),
        artifact_id: session.artifact_id.clone(),
        version_id: session.artifact_version_id.clone(),
        offset,
        len,
        total_size_bytes: session.size_bytes,
        chunk_sha256: sha256_bytes(chunk),
        final_chunk: offset.saturating_add(len) == session.size_bytes,
    };
    let payload = encode_artifact_download_chunk_frame(&header, chunk)?;
    session_manager.send_binary(connection_id, payload).await
}

pub(in crate::message) fn encode_artifact_download_chunk_frame(
    header: &ArtifactDownloadChunkHeader,
    chunk: &[u8],
) -> Result<Vec<u8>> {
    if header.len != u64::try_from(chunk.len()).unwrap_or(u64::MAX) {
        bail!("artifact download frame chunk length mismatch");
    }
    let header_bytes =
        serde_json::to_vec(header).context("failed to encode artifact download chunk header")?;
    let header_len =
        u32::try_from(header_bytes.len()).context("artifact download chunk header is too large")?;
    let mut payload = Vec::with_capacity(
        ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC.len() + 4 + header_bytes.len() + chunk.len(),
    );
    payload.extend_from_slice(ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC);
    payload.extend_from_slice(&header_len.to_be_bytes());
    payload.extend_from_slice(header_bytes.as_slice());
    payload.extend_from_slice(chunk);
    Ok(payload)
}

#[cfg(test)]
pub(in crate::message) fn parse_artifact_download_chunk_frame(
    frame: &[u8],
) -> Result<(ArtifactDownloadChunkHeader, &[u8])> {
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
    let chunk = &frame[header_end..];
    if header.len != u64::try_from(chunk.len()).unwrap_or(u64::MAX) {
        bail!("artifact download frame chunk length mismatch");
    }
    let actual_sha256 = sha256_bytes(chunk);
    if header.chunk_sha256 != actual_sha256 {
        bail!("artifact download frame chunk sha256 mismatch");
    }
    Ok((header, chunk))
}

fn validate_session_owner(
    session: &ArtifactDownloadSession,
    owner: &AuthenticatedTransferOwner,
    workspace_id: &str,
    now: i64,
) -> Result<()> {
    if session.workspace_id != workspace_id || session.owner != *owner {
        bail!("artifact download not found");
    }
    if session.expires_at <= now {
        bail!("artifact download expired");
    }
    Ok(())
}

fn validate_download_range(session: &ArtifactDownloadSession, offset: u64, len: u64) -> Result<()> {
    if len == 0 {
        bail!("artifact download chunk is empty");
    }
    if len > ARTIFACT_DOWNLOAD_MAX_CHUNK_SIZE_BYTES {
        bail!("artifact download chunk exceeds max chunk size");
    }
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("artifact download range overflows"))?;
    if offset >= session.size_bytes || end > session.size_bytes {
        bail!("artifact download range exceeds artifact size");
    }
    Ok(())
}

fn prune_expired_downloads(sessions: &mut HashMap<String, ArtifactDownloadSession>, now: i64) {
    sessions.retain(|_, session| session.expires_at > now);
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{ArtifactKind, ArtifactRef, ArtifactStatus};
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message;

    fn authenticated_owner(
        connection_id: ConnectionId,
        principal_byte: char,
        session_byte: char,
    ) -> AuthenticatedTransferOwner {
        AuthenticatedTransferOwner {
            principal_id: pioneer_protocol::PrincipalId::new(principal_byte.to_string().repeat(21))
                .expect("principal id"),
            auth_session_id: pioneer_protocol::AuthSessionId::new(
                session_byte.to_string().repeat(21),
            )
            .expect("auth session id"),
            connection_id,
        }
    }

    #[test]
    fn artifact_download_parse_valid_frame() {
        let chunk = b"hello";
        let header = ArtifactDownloadChunkHeader {
            workspace_id: "ws_a".to_owned(),
            download_id: "dl_a".to_owned(),
            artifact_id: "art_a".to_owned(),
            version_id: "av_a".to_owned(),
            offset: 0,
            len: chunk.len() as u64,
            total_size_bytes: chunk.len() as u64,
            chunk_sha256: sha256_bytes(chunk),
            final_chunk: true,
        };
        let frame = encode_artifact_download_chunk_frame(&header, chunk).unwrap();

        let (parsed, parsed_chunk) = parse_artifact_download_chunk_frame(&frame).unwrap();

        assert_eq!(parsed, header);
        assert_eq!(parsed_chunk, chunk);
    }

    #[tokio::test]
    async fn artifact_download_session_enforces_owner_and_limits() {
        let manager = ArtifactDownloadSessionManager::new();
        let first = manager
            .start(7, snapshot("ws_a", "art_a", 10), 100)
            .await
            .expect("first download");
        manager
            .start(7, snapshot("ws_a", "art_b", 10), 100)
            .await
            .expect("second download");
        assert!(
            manager
                .start(7, snapshot("ws_a", "art_c", 10), 100)
                .await
                .is_err()
        );

        assert!(
            manager
                .get(8, "ws_a", first.download_id.as_str(), 100)
                .await
                .is_err()
        );
        assert!(
            manager
                .get(7, "ws_b", first.download_id.as_str(), 100)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn artifact_download_finish_abort_and_connection_close_remove_sessions() {
        let manager = ArtifactDownloadSessionManager::new();
        let first = manager
            .start(7, snapshot("ws_a", "art_a", 10), 100)
            .await
            .expect("first download");
        manager
            .finish(7, "ws_a", first.download_id.as_str(), 100)
            .await
            .expect("finish");
        assert_eq!(manager.session_count().await, 0);

        let second = manager
            .start(7, snapshot("ws_a", "art_b", 10), 100)
            .await
            .expect("second download");
        manager
            .abort(7, "ws_a", second.download_id.as_str(), 100)
            .await
            .expect("abort");
        assert_eq!(manager.session_count().await, 0);

        manager
            .start(7, snapshot("ws_a", "art_c", 10), 100)
            .await
            .expect("third download");
        manager.abort_connection(7).await;
        assert_eq!(manager.session_count().await, 0);
    }

    #[tokio::test]
    async fn artifact_download_session_is_principal_and_auth_session_bound() {
        let manager = ArtifactDownloadSessionManager::new();
        let owner = authenticated_owner(7, 'P', 'S');
        let session = manager
            .start(owner.clone(), snapshot("ws_a", "art_a", 5), 100)
            .await
            .expect("download session");

        assert!(
            manager
                .get(
                    authenticated_owner(7, 'Q', 'S'),
                    "ws_a",
                    session.download_id.as_str(),
                    101,
                )
                .await
                .is_err()
        );
        assert!(
            manager
                .get(
                    authenticated_owner(7, 'P', 'T'),
                    "ws_a",
                    session.download_id.as_str(),
                    101,
                )
                .await
                .is_err()
        );
        assert!(
            manager
                .get(&owner, "ws_a", session.download_id.as_str(), 101)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn artifact_download_chunk_frame_reaches_registered_connection() {
        let session_manager = SessionManager::new();
        let (tx, mut rx) = mpsc::channel(2);
        let connection_id = crate::session::test_support::register_authenticated_test_connection(
            &session_manager,
            tx,
        )
        .await;
        let manager = ArtifactDownloadSessionManager::new();
        let session = manager
            .start(connection_id, snapshot("ws_a", "art_a", 5), 100)
            .await
            .expect("download session");

        send_artifact_download_chunk_frame(&session_manager, connection_id, &session, 0, b"hello")
            .await
            .expect("send frame");

        let Some(Message::Binary(payload)) = rx.recv().await else {
            panic!("expected binary frame");
        };
        let (header, chunk) = parse_artifact_download_chunk_frame(&payload).unwrap();
        assert_eq!(header.download_id, session.download_id);
        assert_eq!(header.final_chunk, true);
        assert_eq!(chunk, b"hello");
    }

    #[test]
    fn artifact_download_range_validation_rejects_invalid_ranges() {
        let session = ArtifactDownloadSession {
            download_id: "dl".to_owned(),
            owner: AuthenticatedTransferOwner::from(1),
            workspace_id: "ws".to_owned(),
            thread_id: None,
            artifact_id: "art".to_owned(),
            artifact_version_id: "av".to_owned(),
            blob_id: "blob".to_owned(),
            storage_key: "key".to_owned(),
            display_name: "file.txt".to_owned(),
            mime_type: None,
            size_bytes: 10,
            sha256: "0".repeat(64),
            created_at: 1,
            expires_at: 100,
        };

        assert!(validate_download_range(&session, 0, 0).is_err());
        assert!(
            validate_download_range(&session, 0, ARTIFACT_DOWNLOAD_MAX_CHUNK_SIZE_BYTES + 1)
                .is_err()
        );
        assert!(validate_download_range(&session, 10, 1).is_err());
        assert!(validate_download_range(&session, 9, 2).is_err());
        assert!(validate_download_range(&session, 9, 1).is_ok());
    }

    fn snapshot(
        workspace_id: &str,
        artifact_id: &str,
        size_bytes: u64,
    ) -> ArtifactDownloadSnapshot {
        ArtifactDownloadSnapshot {
            artifact: ArtifactRef {
                artifact_id: artifact_id.to_owned(),
                version_id: Some(format!("{artifact_id}_v1")),
                display_name: "file.txt".to_owned(),
                kind: ArtifactKind::File,
                mime_type: Some("text/plain".to_owned()),
                size_bytes: Some(size_bytes),
                sha256: Some("a".repeat(64)),
                status: ArtifactStatus::Ready,
                preview: None,
            },
            workspace_id: workspace_id.to_owned(),
            artifact_id: artifact_id.to_owned(),
            artifact_version_id: format!("{artifact_id}_v1"),
            blob_id: format!("{artifact_id}_blob"),
            storage_key: format!("{artifact_id}_storage"),
            display_name: "file.txt".to_owned(),
            mime_type: Some("text/plain".to_owned()),
            size_bytes,
            sha256: "a".repeat(64),
        }
    }
}
