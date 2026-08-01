use super::super::*;
use pioneer_protocol::{
    ArtifactCapabilitiesParams, ArtifactCapabilitiesResponse, ArtifactUploadCapabilities,
};

pub(in crate::message) const ARTIFACT_UPLOAD_RECOMMENDED_CHUNK_SIZE_BYTES: u64 = 256 * 1024;
pub(in crate::message) const ARTIFACT_UPLOAD_MAX_CHUNK_SIZE_BYTES: u64 = 1024 * 1024;
pub(in crate::message) const ARTIFACT_UPLOAD_MAX_FILE_SIZE_BYTES: u64 = 50 * 1024 * 1024;
pub(in crate::message) const ARTIFACT_UPLOAD_MAX_FILES_PER_TURN: u64 = 32;

impl MessageProcessor {
    pub(crate) async fn artifact_capabilities(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ArtifactCapabilitiesParams,
    ) {
        let connection_id = request_context.connection_id();
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_CAPABILITIES,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let payload = ArtifactCapabilitiesResponse {
            upload: ArtifactUploadCapabilities {
                required_for_local_paths: true,
                recommended_chunk_size_bytes: ARTIFACT_UPLOAD_RECOMMENDED_CHUNK_SIZE_BYTES,
                max_chunk_size_bytes: ARTIFACT_UPLOAD_MAX_CHUNK_SIZE_BYTES,
                max_file_size_bytes: ARTIFACT_UPLOAD_MAX_FILE_SIZE_BYTES,
                max_files_per_turn: ARTIFACT_UPLOAD_MAX_FILES_PER_TURN,
            },
        };
        debug!(
            connection_id,
            workspace_id = workspace_id.as_str(),
            "artifact capabilities resolved"
        );
        self.send_artifact_result(
            connection_id,
            request_id,
            &payload,
            methods::ARTIFACT_CAPABILITIES,
        )
        .await;
    }
}
