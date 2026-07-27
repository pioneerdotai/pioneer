use super::super::*;
use pioneer_protocol::{ArtifactReadParams, ArtifactReadResponse};

const ARTIFACT_JSON_READ_MAX_BYTES: u64 = 1024 * 1024;

impl MessageProcessor {
    pub(crate) async fn artifact_read(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        mut params: ArtifactReadParams,
    ) {
        let connection_id = request_context.connection_id();
        let workspace_id = match self
            .validate_artifact_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::ARTIFACT_READ,
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

        let response: ArtifactReadResponse = match self
            .artifact_service
            .read_artifact(params, ARTIFACT_JSON_READ_MAX_BYTES)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to read artifact: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        self.send_artifact_result(connection_id, request_id, &response, methods::ARTIFACT_READ)
            .await;
    }
}
