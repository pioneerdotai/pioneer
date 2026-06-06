//! Artifact upload state and helpers.

use crate::{
    ClientError,
    platform::{ClientFileSystem, ClientPath},
};
use anyhow::{Context as _, Result, anyhow};
use pioneer_protocol::{
    ArtifactRef, ArtifactUploadAbortParams, ArtifactUploadAbortResponse,
    ArtifactUploadChunkAckNotification, ArtifactUploadFinishParams, ArtifactUploadFinishResponse,
    ArtifactUploadSourceKind, ArtifactUploadStartParams, ArtifactUploadStartResponse,
};

const DEFAULT_ARTIFACT_UPLOAD_CHUNK_SIZE: usize = 256 * 1024;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ArtifactUploadFileRequest {
    pub workspace_id: String,
    pub thread_id: Option<String>,
    pub planned_turn_id: Option<String>,
    pub client_attachment_id: String,
    pub path: ClientPath,
    pub mime_type: Option<String>,
}

pub trait ArtifactUploadTransport {
    fn artifact_upload_start(
        &self,
        params: ArtifactUploadStartParams,
    ) -> Result<ArtifactUploadStartResponse>;

    fn send_artifact_upload_chunk(
        &self,
        workspace_id: String,
        upload_id: String,
        offset: u64,
        chunk: Vec<u8>,
    ) -> Result<ArtifactUploadChunkAckNotification>;

    fn artifact_upload_finish(
        &self,
        params: ArtifactUploadFinishParams,
    ) -> Result<ArtifactUploadFinishResponse>;

    fn artifact_upload_abort(
        &self,
        params: ArtifactUploadAbortParams,
    ) -> Result<ArtifactUploadAbortResponse>;
}

pub fn upload_artifact_file<TTransport, TFileSystem>(
    transport: &TTransport,
    file_system: &TFileSystem,
    request: ArtifactUploadFileRequest,
) -> Result<ArtifactRef>
where
    TTransport: ArtifactUploadTransport,
    TFileSystem: ClientFileSystem,
{
    validate_artifact_upload_file_request(&request)?;

    let metadata = file_system
        .metadata(&request.path)
        .map_err(client_error_to_anyhow)
        .with_context(|| format!("failed to stat `{}`", request.path.as_path().display()))?;
    if !metadata.is_file {
        return Err(anyhow!("artifact upload path is not a regular file"));
    }

    let file_name = file_system
        .file_name(&request.path)
        .map_err(client_error_to_anyhow)?;
    let size_bytes = metadata.len;
    let sha256 = file_system
        .sha256_file(&request.path)
        .map_err(client_error_to_anyhow)
        .with_context(|| format!("failed to hash `{}`", request.path.as_path().display()))?;
    let mime_type = match request.mime_type.clone() {
        Some(mime_type) => Some(mime_type),
        None => file_system
            .mime_type(&request.path)
            .map_err(client_error_to_anyhow)?,
    };

    let start = transport.artifact_upload_start(ArtifactUploadStartParams {
        workspace_id: request.workspace_id.clone(),
        thread_id: request.thread_id.clone(),
        planned_turn_id: request.planned_turn_id.clone(),
        client_attachment_id: request.client_attachment_id.clone(),
        file_name,
        mime_type,
        size_bytes,
        sha256,
        source_kind: ArtifactUploadSourceKind::UserComposer,
    })?;

    let result = upload_artifact_file_chunks_and_finish(transport, file_system, &request, &start);
    if result.is_err() {
        let _ = transport.artifact_upload_abort(ArtifactUploadAbortParams {
            workspace_id: request.workspace_id,
            upload_id: start.upload_id,
        });
    }
    result
}

fn validate_artifact_upload_file_request(request: &ArtifactUploadFileRequest) -> Result<()> {
    if request.workspace_id.trim().is_empty() {
        return Err(anyhow!("workspace_id is required for artifact file upload"));
    }
    if request.client_attachment_id.trim().is_empty() {
        return Err(anyhow!(
            "client_attachment_id is required for artifact file upload"
        ));
    }
    Ok(())
}

fn upload_artifact_file_chunks_and_finish<TTransport, TFileSystem>(
    transport: &TTransport,
    file_system: &TFileSystem,
    request: &ArtifactUploadFileRequest,
    start: &ArtifactUploadStartResponse,
) -> Result<ArtifactRef>
where
    TTransport: ArtifactUploadTransport,
    TFileSystem: ClientFileSystem,
{
    let chunk_size = usize::try_from(
        start
            .recommended_chunk_size_bytes
            .min(start.max_chunk_size_bytes)
            .max(1),
    )
    .unwrap_or(DEFAULT_ARTIFACT_UPLOAD_CHUNK_SIZE);
    let mut reader = file_system
        .open_read(&request.path)
        .map_err(client_error_to_anyhow)
        .with_context(|| format!("failed to open `{}`", request.path.as_path().display()))?;
    let mut offset = 0_u64;
    let mut buffer = vec![0_u8; chunk_size];
    loop {
        let read = reader
            .read_chunk(buffer.as_mut_slice())
            .map_err(client_error_to_anyhow)
            .with_context(|| format!("failed to read `{}`", request.path.as_path().display()))?;
        if read == 0 {
            break;
        }
        let chunk = buffer[..read].to_vec();
        let ack = transport.send_artifact_upload_chunk(
            request.workspace_id.clone(),
            start.upload_id.clone(),
            offset,
            chunk,
        )?;
        offset = ack.next_offset;
    }

    let finish = transport.artifact_upload_finish(ArtifactUploadFinishParams {
        workspace_id: request.workspace_id.clone(),
        upload_id: start.upload_id.clone(),
    })?;
    Ok(finish.artifact)
}

fn client_error_to_anyhow(error: ClientError) -> anyhow::Error {
    match error {
        ClientError::InvalidState(message)
        | ClientError::Protocol(message)
        | ClientError::Platform(message) => anyhow!("{message}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::ClientFileMetadata;
    use pioneer_protocol::{ArtifactKind, ArtifactStatus};
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::SystemTime,
    };

    #[test]
    fn artifacts_upload_file_starts_chunks_and_finishes() {
        let transport = FakeUploadTransport::new();
        let file_system = FakeUploadFileSystem::new("report.txt", b"hello world".to_vec());

        let artifact = upload_artifact_file(
            &transport,
            &file_system,
            ArtifactUploadFileRequest {
                workspace_id: "ws_1".to_owned(),
                thread_id: Some("thread_1".to_owned()),
                planned_turn_id: Some("turn_1".to_owned()),
                client_attachment_id: "attachment_1".to_owned(),
                path: ClientPath::new("/tmp/report.txt"),
                mime_type: None,
            },
        )
        .expect("upload artifact");

        assert_eq!(artifact.artifact_id, "artifact_1");
        assert_eq!(transport.started().file_name, "report.txt");
        assert_eq!(transport.started().size_bytes, 11);
        assert_eq!(transport.started().mime_type.as_deref(), Some("text/plain"));
        assert_eq!(
            transport.chunks(),
            vec![
                (
                    "ws_1".to_owned(),
                    "upload_1".to_owned(),
                    0,
                    b"hello".to_vec()
                ),
                (
                    "ws_1".to_owned(),
                    "upload_1".to_owned(),
                    5,
                    b" worl".to_vec()
                ),
                ("ws_1".to_owned(), "upload_1".to_owned(), 10, b"d".to_vec()),
            ]
        );
        assert_eq!(transport.finished(), vec!["upload_1".to_owned()]);
        assert!(transport.aborted().is_empty());
    }

    #[test]
    fn artifacts_upload_file_aborts_when_chunk_send_fails() {
        let transport = FakeUploadTransport::new();
        transport.fail_next_chunk("temporary write failure");
        let file_system = FakeUploadFileSystem::new("report.txt", b"hello".to_vec());

        let error = upload_artifact_file(
            &transport,
            &file_system,
            ArtifactUploadFileRequest {
                workspace_id: "ws_1".to_owned(),
                thread_id: None,
                planned_turn_id: None,
                client_attachment_id: "attachment_1".to_owned(),
                path: ClientPath::new("/tmp/report.txt"),
                mime_type: None,
            },
        )
        .expect_err("chunk failure should abort");

        assert!(error.to_string().contains("temporary write failure"));
        assert_eq!(transport.aborted(), vec!["upload_1".to_owned()]);
        assert!(transport.finished().is_empty());
    }

    struct FakeUploadFileSystem {
        file_name: String,
        bytes: Vec<u8>,
    }

    impl FakeUploadFileSystem {
        fn new(file_name: &str, bytes: Vec<u8>) -> Self {
            Self {
                file_name: file_name.to_owned(),
                bytes,
            }
        }
    }

    impl ClientFileSystem for FakeUploadFileSystem {
        fn read_file(&self, _path: &ClientPath) -> crate::ClientResult<Vec<u8>> {
            Ok(self.bytes.clone())
        }

        fn metadata(&self, _path: &ClientPath) -> crate::ClientResult<ClientFileMetadata> {
            Ok(ClientFileMetadata {
                len: self.bytes.len() as u64,
                modified: Some(SystemTime::UNIX_EPOCH),
                is_file: true,
                is_dir: false,
            })
        }

        fn write_cache_file(&self, _key: &str, _bytes: &[u8]) -> crate::ClientResult<ClientPath> {
            Err(ClientError::platform(
                "cache writes are not used in upload tests",
            ))
        }

        fn file_name(&self, _path: &ClientPath) -> crate::ClientResult<String> {
            Ok(self.file_name.clone())
        }

        fn mime_type(&self, _path: &ClientPath) -> crate::ClientResult<Option<String>> {
            Ok(Some("text/plain".to_owned()))
        }
    }

    #[derive(Default)]
    struct FakeUploadTransport {
        state: Arc<Mutex<FakeUploadTransportState>>,
    }

    #[derive(Default)]
    struct FakeUploadTransportState {
        started: Vec<ArtifactUploadStartParams>,
        chunks: Vec<(String, String, u64, Vec<u8>)>,
        finished: Vec<String>,
        aborted: Vec<String>,
        chunk_failures: VecDeque<String>,
    }

    impl FakeUploadTransport {
        fn new() -> Self {
            Self::default()
        }

        fn fail_next_chunk(&self, error: &str) {
            self.state
                .lock()
                .expect("lock state")
                .chunk_failures
                .push_back(error.to_owned());
        }

        fn started(&self) -> ArtifactUploadStartParams {
            self.state
                .lock()
                .expect("lock state")
                .started
                .last()
                .expect("start request")
                .clone()
        }

        fn chunks(&self) -> Vec<(String, String, u64, Vec<u8>)> {
            self.state.lock().expect("lock state").chunks.clone()
        }

        fn finished(&self) -> Vec<String> {
            self.state.lock().expect("lock state").finished.clone()
        }

        fn aborted(&self) -> Vec<String> {
            self.state.lock().expect("lock state").aborted.clone()
        }
    }

    impl ArtifactUploadTransport for FakeUploadTransport {
        fn artifact_upload_start(
            &self,
            params: ArtifactUploadStartParams,
        ) -> Result<ArtifactUploadStartResponse> {
            self.state.lock().expect("lock state").started.push(params);
            Ok(ArtifactUploadStartResponse {
                upload_id: "upload_1".to_owned(),
                recommended_chunk_size_bytes: 5,
                max_chunk_size_bytes: 5,
                max_size_bytes: 100,
                expires_at_unix: 0,
            })
        }

        fn send_artifact_upload_chunk(
            &self,
            workspace_id: String,
            upload_id: String,
            offset: u64,
            chunk: Vec<u8>,
        ) -> Result<ArtifactUploadChunkAckNotification> {
            let mut state = self.state.lock().expect("lock state");
            if let Some(error) = state.chunk_failures.pop_front() {
                return Err(anyhow!("{error}"));
            }
            let len = chunk.len() as u64;
            state
                .chunks
                .push((workspace_id.clone(), upload_id.clone(), offset, chunk));
            Ok(ArtifactUploadChunkAckNotification {
                workspace_id,
                upload_id,
                offset,
                len,
                received_bytes: len,
                next_offset: offset + len,
            })
        }

        fn artifact_upload_finish(
            &self,
            params: ArtifactUploadFinishParams,
        ) -> Result<ArtifactUploadFinishResponse> {
            self.state
                .lock()
                .expect("lock state")
                .finished
                .push(params.upload_id.clone());
            Ok(ArtifactUploadFinishResponse {
                upload_id: params.upload_id,
                artifact: artifact_ref(),
            })
        }

        fn artifact_upload_abort(
            &self,
            params: ArtifactUploadAbortParams,
        ) -> Result<ArtifactUploadAbortResponse> {
            self.state
                .lock()
                .expect("lock state")
                .aborted
                .push(params.upload_id.clone());
            Ok(ArtifactUploadAbortResponse {
                upload_id: params.upload_id,
                status: "aborted".to_owned(),
            })
        }
    }

    fn artifact_ref() -> ArtifactRef {
        ArtifactRef {
            artifact_id: "artifact_1".to_owned(),
            version_id: Some("version_1".to_owned()),
            display_name: "report.txt".to_owned(),
            kind: ArtifactKind::File,
            mime_type: Some("text/plain".to_owned()),
            size_bytes: Some(11),
            sha256: Some("a".repeat(64)),
            status: ArtifactStatus::Ready,
            preview: None,
        }
    }
}
