use super::*;

const THREAD_AGENTS_DOC_MAX_CHARS: usize = 64 * 1024;

impl MessageProcessor {
    pub(super) async fn thread_agents_doc_get(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ThreadAgentsDocGetParams,
    ) {
        let connection_id = request_context.connection_id();
        let (workspace_id, folder_id) = match self
            .validate_thread_agents_doc_scope(
                connection_id,
                request_id.clone(),
                methods::THREAD_AGENTS_DOC_GET,
                params.workspace_id,
                params.folder_id,
            )
            .await
        {
            Some(scope) => scope,
            None => return,
        };

        let context = match self
            .crud_store
            .get_thread_agents_doc_scope_context(workspace_id.as_str(), folder_id.as_deref())
            .await
        {
            Ok(context) => context,
            Err(error) => {
                self.send_thread_agents_doc_error(
                    connection_id,
                    request_id,
                    methods::THREAD_AGENTS_DOC_GET,
                    error,
                )
                .await;
                return;
            }
        };

        let response = ThreadAgentsDocGetResponse {
            explicit: context.explicit.map(thread_agents_doc_payload_from_record),
            effective: context
                .effective
                .map(thread_agents_doc_resolved_from_record),
        };
        self.send_thread_agents_doc_response(
            connection_id,
            request_id,
            &response,
            methods::THREAD_AGENTS_DOC_GET,
        )
        .await;
    }

    pub(super) async fn thread_agents_doc_save(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ThreadAgentsDocSaveParams,
    ) {
        let connection_id = request_context.connection_id();
        if params.content.chars().count() > THREAD_AGENTS_DOC_MAX_CHARS {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `content` must be at most {THREAD_AGENTS_DOC_MAX_CHARS} characters",
                        methods::THREAD_AGENTS_DOC_SAVE
                    ),
                ),
            )
            .await;
            return;
        }

        let (workspace_id, folder_id) = match self
            .validate_thread_agents_doc_scope(
                connection_id,
                request_id.clone(),
                methods::THREAD_AGENTS_DOC_SAVE,
                params.workspace_id,
                params.folder_id,
            )
            .await
        {
            Some(scope) => scope,
            None => return,
        };

        let save_reason = match params.save_reason {
            ThreadAgentsDocSaveReason::Autosave => {
                pioneer_crud::ThreadAgentsDocSaveReason::Autosave
            }
            ThreadAgentsDocSaveReason::Manual => pioneer_crud::ThreadAgentsDocSaveReason::Manual,
        };
        let doc = match self
            .crud_store
            .save_thread_agents_doc(
                workspace_id.as_str(),
                folder_id.as_deref(),
                params.content.as_str(),
                params.expected_version,
                None,
                save_reason,
            )
            .await
        {
            Ok(doc) => doc,
            Err(error) => {
                self.send_thread_agents_doc_error(
                    connection_id,
                    request_id,
                    methods::THREAD_AGENTS_DOC_SAVE,
                    error,
                )
                .await;
                return;
            }
        };

        let doc_payload = thread_agents_doc_payload_from_record(doc);
        let response = ThreadAgentsDocSaveResponse {
            doc: doc_payload.clone(),
        };
        self.send_thread_agents_doc_response(
            connection_id,
            request_id,
            &response,
            methods::THREAD_AGENTS_DOC_SAVE,
        )
        .await;

        self.notify_thread_agents_doc_changed(workspace_id, folder_id, Some(doc_payload), true)
            .await;
    }

    pub(super) async fn thread_agents_doc_archive(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ThreadAgentsDocArchiveParams,
    ) {
        let connection_id = request_context.connection_id();
        let (workspace_id, folder_id) = match self
            .validate_thread_agents_doc_scope(
                connection_id,
                request_id.clone(),
                methods::THREAD_AGENTS_DOC_ARCHIVE,
                params.workspace_id,
                params.folder_id,
            )
            .await
        {
            Some(scope) => scope,
            None => return,
        };

        let archived = match self
            .crud_store
            .archive_thread_agents_doc(
                workspace_id.as_str(),
                folder_id.as_deref(),
                params.expected_version,
                None,
            )
            .await
        {
            Ok(archived) => archived,
            Err(error) => {
                self.send_thread_agents_doc_error(
                    connection_id,
                    request_id,
                    methods::THREAD_AGENTS_DOC_ARCHIVE,
                    error,
                )
                .await;
                return;
            }
        };

        let effective = match self
            .crud_store
            .resolve_thread_agents_doc_for_folder(workspace_id.as_str(), folder_id.as_deref())
            .await
        {
            Ok(effective) => effective.map(thread_agents_doc_resolved_from_record),
            Err(error) => {
                self.send_thread_agents_doc_error(
                    connection_id,
                    request_id,
                    methods::THREAD_AGENTS_DOC_ARCHIVE,
                    error,
                )
                .await;
                return;
            }
        };

        let archived_payload = archived.map(thread_agents_doc_payload_from_record);
        let response = ThreadAgentsDocArchiveResponse {
            archived: archived_payload.is_some(),
            effective: effective.clone(),
        };
        self.send_thread_agents_doc_response(
            connection_id,
            request_id,
            &response,
            methods::THREAD_AGENTS_DOC_ARCHIVE,
        )
        .await;

        self.notify_thread_agents_doc_changed(workspace_id, folder_id, archived_payload, true)
            .await;
    }

    pub(super) async fn thread_agents_doc_resolve_for_thread(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ThreadAgentsDocResolveForThreadParams,
    ) {
        let connection_id = request_context.connection_id();
        if params.thread_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` is required",
                        methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD
                    ),
                ),
            )
            .await;
            return;
        }

        let workspace_id = match self
            .workspace_manager
            .validate_workspace_id(params.workspace_id.as_str())
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_workspace_validation_error(
                    connection_id,
                    request_id,
                    methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD,
                    error,
                )
                .await;
                return;
            }
        };
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        let effective = match self
            .crud_store
            .resolve_thread_agents_doc_for_thread(workspace_id.as_str(), params.thread_id.as_str())
            .await
        {
            Ok(effective) => effective.map(thread_agents_doc_resolved_from_record),
            Err(error) => {
                self.send_thread_agents_doc_error(
                    connection_id,
                    request_id,
                    methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD,
                    error,
                )
                .await;
                return;
            }
        };

        let response = ThreadAgentsDocResolveForThreadResponse { effective };
        self.send_thread_agents_doc_response(
            connection_id,
            request_id,
            &response,
            methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD,
        )
        .await;
    }

    async fn validate_thread_agents_doc_scope(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        method: &'static str,
        workspace_id: String,
        folder_id: Option<String>,
    ) -> Option<(String, Option<String>)> {
        let workspace_id = match self
            .workspace_manager
            .validate_workspace_id(workspace_id.as_str())
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_workspace_validation_error(connection_id, request_id, method, error)
                    .await;
                return None;
            }
        };

        let folder_id = match folder_id {
            Some(folder_id) if folder_id.trim().is_empty() => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!("invalid params for `{method}`: `folder_id` cannot be empty"),
                    ),
                )
                .await;
                return None;
            }
            value => value,
        };

        if let Some(folder_id) = folder_id.as_deref() {
            match self
                .crud_store
                .list_thread_folders(workspace_id.as_str())
                .await
            {
                Ok(folders) => {
                    if !folders.iter().any(|folder| folder.id == folder_id) {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request_id),
                                INVALID_PARAMS_CODE,
                                format!(
                                    "invalid params for `{method}`: folder `{folder_id}` does not exist in workspace `{workspace_id}`"
                                ),
                            ),
                        )
                        .await;
                        return None;
                    }
                }
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to validate folder for `{method}`: {error:#}"),
                        ),
                    )
                    .await;
                    return None;
                }
            }
        }

        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        Some((workspace_id, folder_id))
    }

    async fn send_workspace_validation_error(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        method: &str,
        error: WorkspaceError,
    ) {
        let (code, message) = match &error {
            WorkspaceError::Internal(message) => (
                INVALID_REQUEST_CODE,
                format!("failed to validate workspace for `{method}`: {message}"),
            ),
            _ => (
                INVALID_PARAMS_CODE,
                format!("invalid params for `{method}`: {error}"),
            ),
        };
        self.send_error(
            connection_id,
            JsonRpcErrorResponse::new(Some(request_id), code, message),
        )
        .await;
    }

    async fn send_thread_agents_doc_error(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        method: &str,
        error: pioneer_crud::ThreadAgentsDocError,
    ) {
        let (code, message) = match error {
            pioneer_crud::ThreadAgentsDocError::VersionConflict { expected, actual } => (
                INVALID_REQUEST_CODE,
                format!(
                    "failed to process `{method}`: version conflict, expected {expected}, actual {actual}"
                ),
            ),
            pioneer_crud::ThreadAgentsDocError::NotFound { message }
            | pioneer_crud::ThreadAgentsDocError::WorkspaceMismatch { message }
            | pioneer_crud::ThreadAgentsDocError::InvalidData { message } => (
                INVALID_PARAMS_CODE,
                format!("invalid params for `{method}`: {message}"),
            ),
            pioneer_crud::ThreadAgentsDocError::Database { message, source } => (
                INVALID_REQUEST_CODE,
                format!("failed to process `{method}`: {message}: {source}"),
            ),
        };

        self.send_error(
            connection_id,
            JsonRpcErrorResponse::new(Some(request_id), code, message),
        )
        .await;
    }

    async fn send_thread_agents_doc_response<T: Serialize>(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        payload: &T,
        method: &str,
    ) {
        let response = match JsonRpcResponse::from_result(request_id, payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response for `{method}`: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                method,
                error = %format!("{error:#}"),
                "failed to send thread agents doc response"
            );
        }
    }

    async fn notify_thread_agents_doc_changed(
        &self,
        workspace_id: String,
        folder_id: Option<String>,
        doc: Option<ThreadAgentsDocPayload>,
        effective_changed: bool,
    ) {
        let effective = match self
            .crud_store
            .resolve_thread_agents_doc_for_folder(workspace_id.as_str(), folder_id.as_deref())
            .await
        {
            Ok(effective) => effective.map(thread_agents_doc_resolved_from_record),
            Err(error) => {
                warn!(
                    workspace_id,
                    folder_id = folder_id.as_deref().unwrap_or("<root>"),
                    error = %error,
                    "failed to resolve effective AGENTS.md after change"
                );
                None
            }
        };

        let notification = ThreadAgentsDocChangedNotification {
            workspace_id: workspace_id.clone(),
            folder_id: folder_id.clone(),
            doc,
            effective,
            effective_changed,
        };
        self.send_notification_to_workspace_connections(
            workspace_id.as_str(),
            events::THREAD_AGENTS_DOC_CHANGED,
            &notification,
        )
        .await;
        self.notify_thread_tree_changed(workspace_id).await;
    }
}

fn thread_agents_doc_payload_from_record(
    record: pioneer_crud::ThreadAgentsDocRecord,
) -> ThreadAgentsDocPayload {
    ThreadAgentsDocPayload {
        id: record.id,
        workspace_id: record.workspace_id,
        folder_id: record.folder_id,
        status: thread_agents_doc_status_from_record(record.status),
        title: record.title,
        content: record.content,
        content_sha256: record.content_sha256,
        version: record.version,
        created_at: record.created_at_unix,
        updated_at: record.updated_at_unix,
    }
}

fn thread_agents_doc_resolved_from_record(
    record: pioneer_crud::ResolvedThreadAgentsDocRecord,
) -> ThreadAgentsDocResolvedPayload {
    ThreadAgentsDocResolvedPayload {
        doc: thread_agents_doc_payload_from_record(record.doc),
        source_folder_id: record.source_folder_id,
        source_path: record.source_path,
        inherited: record.inherited,
        resolved_for_folder_id: record.resolved_for_folder_id,
        resolved_at: record.resolved_at_unix,
    }
}

fn thread_agents_doc_status_from_record(
    status: pioneer_crud::ThreadAgentsDocStatus,
) -> ThreadAgentsDocStatus {
    match status {
        pioneer_crud::ThreadAgentsDocStatus::Draft => ThreadAgentsDocStatus::Draft,
        pioneer_crud::ThreadAgentsDocStatus::Active => ThreadAgentsDocStatus::Active,
        pioneer_crud::ThreadAgentsDocStatus::Archived => ThreadAgentsDocStatus::Archived,
    }
}
