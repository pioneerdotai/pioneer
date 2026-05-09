use super::*;

impl MessageProcessor {
    pub(super) async fn thread_start(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ThreadStartParams,
    ) {
        if params.thread_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` is required",
                        methods::THREAD_START
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
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to validate workspace for thread/start: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!("invalid params for `{}`: {error}", methods::THREAD_START),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        let persisted_thread = match self
            .crud_store
            .get_thread_model(params.thread_id.as_str())
            .await
        {
            Ok(model) => model,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread from storage: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Some(persisted_thread) = persisted_thread.as_ref()
            && persisted_thread.workspace_id != workspace_id
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: thread `{}` belongs to workspace `{}`",
                        methods::THREAD_START,
                        persisted_thread.id,
                        persisted_thread.workspace_id
                    ),
                ),
            )
            .await;
            return;
        }

        let persisted_sandbox_mode = match self
            .crud_store
            .get_thread_sandbox_mode(params.thread_id.as_str())
            .await
        {
            Ok(mode) => mode,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread sandbox policy: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let outcome = if persisted_thread.is_none() && persisted_sandbox_mode.is_none() {
            self.thread_manager
                .thread_start(connection_id, workspace_id, params)
                .await
        } else {
            self.thread_manager
                .thread_start_seeded(
                    connection_id,
                    workspace_id,
                    params,
                    persisted_thread,
                    persisted_sandbox_mode,
                )
                .await
        };

        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to create thread: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let response = match JsonRpcResponse::from_result(request_id, &outcome.response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
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
                "failed to send thread/start response"
            );
            return;
        }

        let notification = match JsonRpcNotification::from_params(
            events::THREAD_STARTED,
            &outcome.started_notification,
        ) {
            Ok(notification) => notification,
            Err(error) => {
                warn!(error = %error, "failed to encode thread/started notification");
                return;
            }
        };

        match serde_json::to_string(&notification) {
            Ok(payload) => {
                for notification_connection_id in outcome.started_notification_connection_ids {
                    if let Err(error) = self
                        .session_manager
                        .send_text(notification_connection_id, payload.clone())
                        .await
                    {
                        warn!(
                            connection_id = notification_connection_id,
                            error = %format!("{error:#}"),
                            "failed to send thread/started notification"
                        );
                    }
                }
            }
            Err(error) => {
                warn!(error = %error, "failed to serialize thread/started notification");
            }
        }
    }

    pub(super) async fn thread_tree(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ThreadTreeParams,
    ) {
        if params.workspace_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `workspace_id` is required",
                        methods::THREAD_TREE
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
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to validate workspace for thread/tree: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!("invalid params for `{}`: {error}", methods::THREAD_TREE),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        let threads = match self
            .list_threads_snapshot_for_connection(workspace_id.as_str(), 500, connection_id)
            .await
        {
            Ok(threads) => threads,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread tree threads: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let folders = match self
            .crud_store
            .list_thread_folders(workspace_id.as_str())
            .await
        {
            Ok(folders) => folders,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread folders: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let placements = match self
            .crud_store
            .list_thread_placements(workspace_id.as_str())
            .await
        {
            Ok(placements) => placements,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread placements: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let response_payload = ThreadTreeResponse {
            workspace_id,
            threads,
            folders,
            placements,
        };

        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
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
                "failed to send thread/tree response"
            );
        }
    }

    pub(super) async fn thread_get(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ThreadGetParams,
    ) {
        if params.thread_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` is required",
                        methods::THREAD_GET
                    ),
                ),
            )
            .await;
            return;
        }

        let thread = if let Some(thread) = self
            .thread_manager
            .thread_get(params.thread_id.as_str())
            .await
        {
            Some(thread)
        } else {
            match self
                .crud_store
                .get_thread_model(params.thread_id.as_str())
                .await
            {
                Ok(thread) => thread,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to load thread: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            }
        };

        let Some(thread) = thread else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("thread `{}` was not found", params.thread_id),
                ),
            )
            .await;
            return;
        };

        let response_payload = ThreadGetResponse { thread };

        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
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
                "failed to send thread/get response"
            );
        }
    }

    pub(super) async fn thread_history(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ThreadHistoryParams,
    ) {
        if params.thread_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` is required",
                        methods::THREAD_HISTORY
                    ),
                ),
            )
            .await;
            return;
        }

        if params.limit == Some(0) {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `limit` must be greater than zero",
                        methods::THREAD_HISTORY
                    ),
                ),
            )
            .await;
            return;
        }

        let limit = params.limit.map(u64::from);
        let snapshot = match self
            .crud_store
            .get_thread_history(params.thread_id.as_str(), limit)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread history: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let Some(snapshot) = snapshot else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("thread `{}` was not found", params.thread_id),
                ),
            )
            .await;
            return;
        };
        let mut snapshot = snapshot;

        Self::enrich_thread_history_markdown(snapshot.events.as_mut_slice());

        let response_payload = ThreadHistoryResponse {
            workspace_id: snapshot.workspace_id,
            thread_id: params.thread_id,
            events: snapshot.events,
        };
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
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
                "failed to send thread/history response"
            );
        }
    }

    pub(super) async fn list_threads_snapshot_for_connection(
        &self,
        workspace_id: &str,
        limit: u64,
        connection_id: ConnectionId,
    ) -> Result<Vec<pioneer_protocol::Thread>, anyhow::Error> {
        self.list_threads_snapshot_internal(workspace_id, limit, Some(connection_id))
            .await
    }

    async fn list_threads_snapshot_internal(
        &self,
        workspace_id: &str,
        limit: u64,
        connection_id: Option<ConnectionId>,
    ) -> Result<Vec<pioneer_protocol::Thread>, anyhow::Error> {
        let persisted_threads = self
            .crud_store
            .list_threads_for_workspace(workspace_id, limit)
            .await?;

        let mut threads_by_id: HashMap<String, pioneer_protocol::Thread> = persisted_threads
            .into_iter()
            .map(|thread| (thread.id.clone(), thread))
            .collect();

        for thread in self
            .thread_manager
            .list_threads_for_workspace_visible_to(workspace_id, connection_id)
            .await
        {
            match threads_by_id.get(thread.id.as_str()) {
                Some(existing) if existing.updated_at >= thread.updated_at => {}
                _ => {
                    threads_by_id.insert(thread.id.clone(), thread);
                }
            }
        }

        let mut threads: Vec<pioneer_protocol::Thread> = threads_by_id
            .into_values()
            .filter(|thread| {
                thread.sidebar_visibility == pioneer_protocol::ThreadSidebarVisibility::Visible
            })
            .collect();
        threads.sort_by(|lhs, rhs| {
            rhs.updated_at
                .cmp(&lhs.updated_at)
                .then_with(|| lhs.id.cmp(&rhs.id))
        });
        threads.truncate(limit as usize);
        Ok(threads)
    }

    pub(super) async fn thread_move(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ThreadMoveParams,
    ) {
        if params.workspace_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `workspace_id` is required",
                        methods::THREAD_MOVE
                    ),
                ),
            )
            .await;
            return;
        }

        if params.thread_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` is required",
                        methods::THREAD_MOVE
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
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to validate workspace for thread/move: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!("invalid params for `{}`: {error}", methods::THREAD_MOVE),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        let thread_workspace = if let Some(thread) = self
            .thread_manager
            .thread_get(params.thread_id.as_str())
            .await
        {
            Some(thread.workspace_id)
        } else {
            match self
                .crud_store
                .get_thread_model(params.thread_id.as_str())
                .await
            {
                Ok(thread) => thread.map(|thread| thread.workspace_id),
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to load thread for move: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            }
        };

        let Some(thread_workspace) = thread_workspace else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("thread `{}` was not found", params.thread_id),
                ),
            )
            .await;
            return;
        };

        if thread_workspace != workspace_id {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: thread `{}` belongs to workspace `{}`",
                        methods::THREAD_MOVE,
                        params.thread_id,
                        thread_workspace
                    ),
                ),
            )
            .await;
            return;
        }

        if let Err(error) = self
            .crud_store
            .move_thread_to_folder(
                workspace_id.as_str(),
                params.thread_id.as_str(),
                params.folder_id.as_deref(),
            )
            .await
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("failed to move thread: {error:#}"),
                ),
            )
            .await;
            return;
        }

        let response_payload = ThreadMoveResponse { moved: true };
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
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
                "failed to send thread/move response"
            );
            return;
        }

        self.notify_thread_tree_changed(workspace_id).await;
    }

    pub(super) async fn thread_folder_create(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ThreadFolderCreateParams,
    ) {
        if params.workspace_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `workspace_id` is required",
                        methods::THREAD_FOLDER_CREATE
                    ),
                ),
            )
            .await;
            return;
        }

        let name = params.name.trim();
        if name.is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `name` is required",
                        methods::THREAD_FOLDER_CREATE
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
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to validate workspace for thread/folder/create: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: {error}",
                            methods::THREAD_FOLDER_CREATE
                        ),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        let folder = match self
            .crud_store
            .create_thread_folder(
                workspace_id.as_str(),
                params.parent_folder_id.as_deref(),
                name,
            )
            .await
        {
            Ok(folder) => folder,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to create folder: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let response_payload = ThreadFolderCreateResponse { folder };
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
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
                "failed to send thread/folder/create response"
            );
            return;
        }

        self.notify_thread_tree_changed(workspace_id).await;
    }

    pub(super) async fn thread_folder_move(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ThreadFolderMoveParams,
    ) {
        if params.workspace_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `workspace_id` is required",
                        methods::THREAD_FOLDER_MOVE
                    ),
                ),
            )
            .await;
            return;
        }

        if params.folder_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `folder_id` is required",
                        methods::THREAD_FOLDER_MOVE
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
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to validate workspace for thread/folder/move: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: {error}",
                            methods::THREAD_FOLDER_MOVE
                        ),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        if let Err(error) = self
            .crud_store
            .move_folder(
                workspace_id.as_str(),
                params.folder_id.as_str(),
                params.parent_folder_id.as_deref(),
            )
            .await
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("failed to move folder: {error:#}"),
                ),
            )
            .await;
            return;
        }

        let response_payload = ThreadFolderMoveResponse { moved: true };
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
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
                "failed to send thread/folder/move response"
            );
            return;
        }

        self.notify_thread_tree_changed(workspace_id).await;
    }

    pub(super) async fn thread_folder_delete(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ThreadFolderDeleteParams,
    ) {
        if params.workspace_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `workspace_id` is required",
                        methods::THREAD_FOLDER_DELETE
                    ),
                ),
            )
            .await;
            return;
        }

        if params.folder_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `folder_id` is required",
                        methods::THREAD_FOLDER_DELETE
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
                let (code, message) = match &error {
                    WorkspaceError::Internal(message) => (
                        INVALID_REQUEST_CODE,
                        format!("failed to validate workspace for thread/folder/delete: {message}"),
                    ),
                    _ => (
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: {error}",
                            methods::THREAD_FOLDER_DELETE
                        ),
                    ),
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
                return;
            }
        };
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        let deleted = match self
            .crud_store
            .delete_thread_folder_promote(workspace_id.as_str(), params.folder_id.as_str())
            .await
        {
            Ok(deleted) => deleted,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to delete folder: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let response_payload = ThreadFolderDeleteResponse { deleted };
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
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
                "failed to send thread/folder/delete response"
            );
            return;
        }

        if deleted {
            self.notify_thread_tree_changed(workspace_id).await;
        }
    }

    pub(super) async fn notify_thread_tree_changed(&self, workspace_id: String) {
        let notification = ThreadTreeChangedNotification { workspace_id };
        self.send_notification_to_workspace_connections(
            notification.workspace_id.as_str(),
            events::THREAD_TREE_CHANGED,
            &notification,
        )
        .await;
    }

    pub(super) async fn thread_unsubscribe(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ThreadUnsubscribeParams,
    ) {
        let outcome = self
            .thread_manager
            .thread_unsubscribe(connection_id, &params.thread_id)
            .await;

        let response = match JsonRpcResponse::from_result(request_id, &outcome.response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
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
                "failed to send thread/unsubscribe response"
            );
            return;
        }

        let Some(closed_notification) = outcome.closed_notification else {
            return;
        };
        let closed_thread_id = closed_notification.thread_id.clone();

        let notification =
            match JsonRpcNotification::from_params(events::THREAD_CLOSED, &closed_notification) {
                Ok(notification) => notification,
                Err(error) => {
                    warn!(error = %error, "failed to encode thread/closed notification");
                    return;
                }
            };

        match serde_json::to_string(&notification) {
            Ok(payload) => {
                for notification_connection_id in outcome.closed_notification_connection_ids {
                    if let Err(error) = self
                        .session_manager
                        .send_text(notification_connection_id, payload.clone())
                        .await
                    {
                        warn!(
                            connection_id = notification_connection_id,
                            error = %format!("{error:#}"),
                            "failed to send thread/closed notification"
                        );
                    }
                }
            }
            Err(error) => {
                warn!(error = %error, "failed to serialize thread/closed notification");
            }
        }

        self.teardown_agent_thread(closed_thread_id.as_str()).await;
    }
}
