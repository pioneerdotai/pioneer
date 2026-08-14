use super::*;
use crate::authorization::{
    AuthorizationExternalError, AuthorizedThread, AuthorizedWorkspace, ResourceAction,
    ThreadAccessClass, record_authorization_unavailable,
};
use crate::thread::{RuntimeDraftAccess, ThreadSubscriptionIdentity};
use pioneer_crud::{PersistedThreadAccessClass, PrivateThreadParticipantMutation};
use pioneer_protocol::{
    PrincipalId, Thread, ThreadParticipantChangeKind, ThreadParticipantSummary,
    ThreadParticipantsChangedNotification, ThreadParticipantsResponse,
};

#[derive(Clone, Debug)]
pub(super) enum ThreadParticipantOperation {
    List,
    Add(PrincipalId),
    Remove(PrincipalId),
}

pub(super) enum ThreadAccessAuthorization<'a> {
    Persisted(&'a AuthorizedThread),
    RuntimeDraft(&'a RuntimeDraftAccess),
}

impl ThreadAccessAuthorization<'_> {
    pub(super) fn thread_id(&self) -> &str {
        match self {
            Self::Persisted(proof) => proof.thread_id(),
            Self::RuntimeDraft(access) => access.thread_id(),
        }
    }

    fn workspace_id(&self) -> &str {
        match self {
            Self::Persisted(proof) => proof.workspace_id(),
            Self::RuntimeDraft(access) => access.workspace_id(),
        }
    }
}

impl MessageProcessor {
    /// Revalidates a connection-owned draft against its in-memory owner
    /// capability and the same role/resource policy used for durable threads.
    pub(super) async fn authorize_runtime_draft_for_request(
        &self,
        request_context: &RequestContext,
        action: ResourceAction,
        thread_id: &str,
        expected_workspace_id: Option<&str>,
    ) -> anyhow::Result<
        Option<(
            RuntimeDraftAccess,
            crate::authorization::AuthorizationDecision,
        )>,
    > {
        let identity = ThreadSubscriptionIdentity::new(
            request_context.principal().principal_id.clone(),
            request_context.principal().session_id.clone(),
        );
        let Some(access) = self
            .thread_manager
            .authorize_runtime_draft(
                request_context.connection_id(),
                &identity,
                thread_id,
                expected_workspace_id,
            )
            .await
        else {
            return Ok(None);
        };

        let gate = crate::authorization::AuthorizationService::new().authorize_action(
            request_context.principal().kind,
            request_context.role_key(),
            action,
        );
        let resolver = crate::authorization::AuthorizationResolver::new((*self.crud_store).clone());
        match resolver
            .authorize_runtime_draft(request_context.principal(), &gate, action, &access)
            .await?
        {
            crate::authorization::ProofResolution::Authorized(proof) => {
                Ok(Some((access, proof.decision().clone())))
            }
            crate::authorization::ProofResolution::Denied(_) => Ok(None),
        }
    }
}

fn open_only_thread_start_params(thread: &Thread) -> ThreadStartParams {
    ThreadStartParams {
        thread_id: thread.id.clone(),
        workspace_id: thread.workspace_id.clone(),
        name: None,
        model: None,
        model_provider: None,
        sandbox: None,
        mode: None,
        origin_kind: None,
        sidebar_visibility: None,
        visibility: None,
        agent_nickname: None,
        agent_role: None,
    }
}

fn retain_accessible_thread_placements(
    placements: &mut Vec<pioneer_protocol::ThreadPlacement>,
    accessible_thread_ids: &HashSet<String>,
) {
    placements.retain(|placement| accessible_thread_ids.contains(&placement.thread_id));
}

fn retain_accessible_thread_agents_doc_summaries(
    summaries: &mut Vec<ThreadAgentsDocSummary>,
    folders: &[pioneer_protocol::ThreadFolder],
    placements: &[pioneer_protocol::ThreadPlacement],
    accessible_thread_ids: &HashSet<String>,
) {
    if accessible_thread_ids.is_empty() {
        summaries.clear();
        return;
    }

    let folder_parents = folders
        .iter()
        .map(|folder| (folder.id.as_str(), folder.parent_folder_id.as_deref()))
        .collect::<HashMap<_, _>>();
    let mut accessible_doc_scopes = HashSet::from([None]);

    for placement in placements
        .iter()
        .filter(|placement| accessible_thread_ids.contains(&placement.thread_id))
    {
        let mut folder_id = placement.folder_id.as_deref();
        let mut visited = HashSet::new();
        while let Some(candidate) = folder_id {
            let Some(parent_folder_id) = folder_parents.get(candidate) else {
                break;
            };
            if !visited.insert(candidate) {
                break;
            }
            accessible_doc_scopes.insert(Some(candidate.to_owned()));
            folder_id = *parent_folder_id;
        }
    }

    summaries.retain(|summary| accessible_doc_scopes.contains(&summary.folder_id));
}

impl MessageProcessor {
    pub(super) async fn thread_create_and_start(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedWorkspace,
        request_id: RequestId,
        params: ThreadStartParams,
    ) {
        let connection_id = request_context.connection_id();
        let expected_action = match params.visibility.unwrap_or(ThreadVisibility::Private) {
            ThreadVisibility::Private => ResourceAction::ThreadCreatePrivate,
            ThreadVisibility::Workspace => ResourceAction::ThreadCreateWorkspace,
        };
        if authorization.action() != expected_action
            || authorization.workspace_id() != params.workspace_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let access_class = match params.visibility.unwrap_or(ThreadVisibility::Private) {
            ThreadVisibility::Private => PersistedThreadAccessClass::Private,
            ThreadVisibility::Workspace => PersistedThreadAccessClass::Workspace,
        };

        let workspace_id = authorization.workspace_id().to_owned();
        let (mut thread, sandbox_mode) = match self
            .thread_manager
            .prepare_new_user_thread(workspace_id.clone(), &params)
        {
            Ok(prepared) => prepared,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to prepare thread: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        thread.visibility = Some(match access_class {
            PersistedThreadAccessClass::Private => ThreadVisibility::Private,
            PersistedThreadAccessClass::Workspace => ThreadVisibility::Workspace,
            PersistedThreadAccessClass::Internal => {
                unreachable!("ordinary thread creation cannot select internal access")
            }
        });

        let outcome = self
            .thread_manager
            .thread_start_draft_authenticated(
                connection_id,
                ThreadSubscriptionIdentity::new(
                    request_context.principal().principal_id.clone(),
                    request_context.principal().session_id.clone(),
                ),
                workspace_id.clone(),
                open_only_thread_start_params(&thread),
                Some(thread),
                Some(sandbox_mode),
            )
            .await
            .map_err(|error| format!("failed to publish runtime thread draft: {error:#}"));
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, error),
                )
                .await;
                return;
            }
        };
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id))
            .await;
        self.finish_thread_start(connection_id, request_id, outcome)
            .await;
    }

    pub(super) async fn thread_open(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedThread,
        request_id: RequestId,
        params: ThreadStartParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.action() != ResourceAction::ThreadRead
            || authorization.thread_id() != params.thread_id.trim()
            || authorization.workspace_id() != params.workspace_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if params.visibility.is_some() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    "`visibility` is valid only when creating a missing thread",
                ),
            )
            .await;
            return;
        }

        let persisted_thread = match self
            .crud_store
            .get_thread_model(authorization.thread_id())
            .await
        {
            Ok(Some(thread))
                if thread.workspace_id == authorization.workspace_id()
                    && thread.id == authorization.thread_id() =>
            {
                thread
            }
            Ok(_) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load authorized thread: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        let persisted_sandbox_mode = match self
            .crud_store
            .get_thread_sandbox_mode(authorization.thread_id())
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
        let workspace_id = authorization.workspace_id().to_owned();
        let outcome = match self
            .thread_manager
            .thread_start_seeded_authenticated(
                connection_id,
                ThreadSubscriptionIdentity::new(
                    request_context.principal().principal_id.clone(),
                    request_context.principal().session_id.clone(),
                ),
                workspace_id.clone(),
                open_only_thread_start_params(&persisted_thread),
                Some(persisted_thread),
                persisted_sandbox_mode,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to open authorized thread: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id))
            .await;
        self.finish_thread_start(connection_id, request_id, outcome)
            .await;
    }

    async fn finish_thread_start(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        outcome: crate::thread::ThreadStartOutcome,
    ) {
        let replay_workspace_id = outcome.response.thread.workspace_id.clone();
        let replay_thread_id = outcome.response.thread.id.clone();

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

        self.send_notification_to_authorized_thread_connections(
            replay_thread_id.as_str(),
            events::THREAD_STARTED,
            &outcome.started_notification,
            outcome.started_notification_connection_ids,
        )
        .await;

        self.replay_native_permission_requests_for_thread(
            connection_id,
            replay_workspace_id.as_str(),
            replay_thread_id.as_str(),
        )
        .await;
    }

    pub(super) async fn thread_tree(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedWorkspace,
        request_id: RequestId,
        params: ThreadTreeParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.action() != ResourceAction::WorkspaceRead
            || authorization.workspace_id() != params.workspace_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let workspace_id = authorization.workspace_id().to_owned();
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        let threads = match self
            .list_threads_snapshot_for_authorization(
                authorization,
                500,
                connection_id,
                &ThreadSubscriptionIdentity::new(
                    request_context.principal().principal_id.clone(),
                    request_context.principal().session_id.clone(),
                ),
            )
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

        let mut placements = match self
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
        let accessible_thread_ids = threads
            .iter()
            .map(|thread| thread.id.clone())
            .collect::<HashSet<_>>();
        let unread_counts = match self
            .crud_store
            .unread_counts_for_threads(
                &request_context.principal().principal_id,
                &accessible_thread_ids.iter().cloned().collect::<Vec<_>>(),
            )
            .await
        {
            Ok(counts) => counts,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread unread summaries: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        let unread = threads
            .iter()
            .map(|thread| ThreadUnreadSummary {
                thread_id: thread.id.clone(),
                unread_count: unread_counts.get(thread.id.as_str()).copied().unwrap_or(0),
            })
            .collect();
        retain_accessible_thread_placements(&mut placements, &accessible_thread_ids);

        let mut agents_docs = match self
            .crud_store
            .list_thread_agents_doc_summaries(workspace_id.as_str())
            .await
        {
            Ok(summaries) => summaries
                .into_iter()
                .map(Self::thread_tree_agents_doc_summary_from_record)
                .collect(),
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread AGENTS.md summaries: {error}"),
                    ),
                )
                .await;
                return;
            }
        };
        if !authorization.decision().is_absolute() {
            retain_accessible_thread_agents_doc_summaries(
                &mut agents_docs,
                &folders,
                &placements,
                &accessible_thread_ids,
            );
        }

        let response_payload = ThreadTreeResponse {
            workspace_id,
            threads,
            unread,
            folders,
            placements,
            agents_docs,
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
        request_context: &RequestContext,
        authorization: ThreadAccessAuthorization<'_>,
        request_id: RequestId,
        params: ThreadGetParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
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
        if thread.workspace_id != authorization.workspace_id() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let unread_count = match self
            .crud_store
            .unread_counts_for_threads(
                &request_context.principal().principal_id,
                std::slice::from_ref(&thread.id),
            )
            .await
        {
            Ok(counts) => counts.get(thread.id.as_str()).copied().unwrap_or(0),
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread unread count: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        let response_payload = ThreadGetResponse {
            thread,
            unread_count,
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
                "failed to send thread/get response"
            );
        }
    }

    #[cfg(test)]
    pub(super) async fn list_threads_snapshot_for_connection(
        &self,
        workspace_id: &str,
        limit: u64,
        connection_id: ConnectionId,
    ) -> Result<Vec<pioneer_protocol::Thread>, anyhow::Error> {
        self.list_threads_snapshot_internal(workspace_id, limit, Some(connection_id))
            .await
    }

    async fn list_threads_snapshot_for_authorization(
        &self,
        authorization: &AuthorizedWorkspace,
        limit: u64,
        connection_id: ConnectionId,
        identity: &ThreadSubscriptionIdentity,
    ) -> Result<Vec<pioneer_protocol::Thread>, anyhow::Error> {
        let persisted_threads = if authorization.decision().is_absolute() {
            self.crud_store
                .list_threads_for_workspace(authorization.workspace_id(), limit)
                .await?
        } else {
            self.crud_store
                .list_accessible_threads_for_principal(
                    authorization.principal_id(),
                    authorization.workspace_id(),
                    limit,
                )
                .await?
        };
        let allowed_ids = persisted_threads
            .iter()
            .map(|thread| thread.id.clone())
            .collect::<HashSet<_>>();
        let mut threads_by_id = HashMap::new();
        for thread in persisted_threads {
            threads_by_id.insert(thread.id.clone(), thread);
        }
        for thread in self
            .thread_manager
            .list_threads_for_workspace_visible_to(
                authorization.workspace_id(),
                Some(connection_id),
            )
            .await
        {
            if !allowed_ids.contains(thread.id.as_str())
                && self
                    .thread_manager
                    .authorize_runtime_draft(
                        connection_id,
                        identity,
                        thread.id.as_str(),
                        Some(authorization.workspace_id()),
                    )
                    .await
                    .is_none()
            {
                continue;
            }
            match threads_by_id.get(thread.id.as_str()) {
                Some(existing) if existing.updated_at >= thread.updated_at => {}
                _ => {
                    threads_by_id.insert(thread.id.clone(), thread);
                }
            }
        }
        let mut threads = threads_by_id.into_values().collect::<Vec<_>>();
        threads.sort_by(|lhs, rhs| {
            rhs.updated_at
                .cmp(&lhs.updated_at)
                .then_with(|| lhs.id.cmp(&rhs.id))
        });
        threads.truncate(limit as usize);
        Ok(threads)
    }

    #[cfg(test)]
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

    pub(super) async fn thread_update(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedThread,
        request_id: RequestId,
        params: ThreadUpdateParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.action() != ResourceAction::ThreadManage
            || authorization.workspace_id() != params.workspace_id.trim()
            || authorization.thread_id() != params.thread_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let name = match params.name.as_deref() {
            Some(name) if !name.trim().is_empty() => Some(name.trim()),
            Some(_) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: `name` must not be empty",
                            methods::THREAD_UPDATE
                        ),
                    ),
                )
                .await;
                return;
            }
            None => None,
        };
        if name.is_none() && params.visibility.is_none() && params.archived.is_none() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: at least one field is required",
                        methods::THREAD_UPDATE
                    ),
                ),
            )
            .await;
            return;
        }
        let visibility_changed = params.visibility.is_some();

        let workspace_id = authorization.workspace_id().to_owned();
        let access_class = params.visibility.map(|visibility| match visibility {
            ThreadVisibility::Private => PersistedThreadAccessClass::Private,
            ThreadVisibility::Workspace => PersistedThreadAccessClass::Workspace,
        });
        let scoped_principal_id =
            (!authorization.decision().is_absolute()).then(|| authorization.principal_id());
        let changed = match self
            .crud_store
            .update_user_thread_management(
                authorization.workspace_id(),
                authorization.thread_id(),
                scoped_principal_id,
                name,
                access_class,
                params.archived,
            )
            .await
        {
            Ok(Some(changed)) => changed,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to update authorized thread: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        if changed && params.visibility.is_some() {
            self.publish_committed_authorization_invalidation(
                AccessChangeKind::ThreadVisibility,
                None,
                workspace_id.clone(),
                Some(authorization.thread_id().to_owned()),
            )
            .await;
        }

        let thread = match self
            .crud_store
            .get_thread_model(authorization.thread_id())
            .await
        {
            Ok(Some(thread)) => thread,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load committed thread update: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        if thread.workspace_id != workspace_id {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;
        if changed {
            self.thread_manager
                .sync_thread_metadata_from_persisted(&thread)
                .await;
        }

        let response_payload = ThreadUpdateResponse {
            thread: thread.clone(),
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
                "failed to send thread/update response"
            );
            return;
        }

        if changed {
            let placement = if visibility_changed {
                self.crud_store
                    .get_thread_placement(thread.id.as_str())
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };
            let notification = ThreadUpdatedNotification {
                thread: thread.clone(),
                placement,
            };
            if visibility_changed {
                self.send_thread_scoped_notification_to_connections(
                    thread.id.as_str(),
                    events::THREAD_UPDATED,
                    &notification,
                    self.session_manager.connection_ids().await,
                )
                .await;
            } else {
                self.send_notification_to_thread_subscribers(
                    thread.id.as_str(),
                    events::THREAD_UPDATED,
                    &notification,
                )
                .await;
                self.notify_thread_tree_changed(workspace_id).await;
            }
            if let Some(name) = name {
                self.best_effort_sync_cli_runtime_thread_name(
                    thread.workspace_id.as_str(),
                    thread.id.as_str(),
                    name,
                )
                .await;
            }
        }
    }

    pub(super) async fn thread_participants(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedThread,
        request_id: RequestId,
        workspace_id: &str,
        thread_id: &str,
        operation: ThreadParticipantOperation,
    ) {
        let connection_id = request_context.connection_id();
        let listing = matches!(&operation, ThreadParticipantOperation::List);
        let expected_action = if listing {
            ResourceAction::ThreadRead
        } else {
            ResourceAction::ThreadParticipantsManage
        };
        if authorization.action() != expected_action
            || authorization.workspace_id() != workspace_id
            || authorization.thread_id() != thread_id
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        let absolute_authority = authorization.decision().is_absolute();
        let acting_scoped_principal = (!absolute_authority).then(|| authorization.principal_id());
        let actor = request_context.persisted_actor();
        let gateway_id = &request_context.principal().gateway_id;

        let (changed, participant_ids, access_change_kind, target_principal_id) = match operation {
            ThreadParticipantOperation::List => {
                let result =
                    if authorization.thread_access_class() == Some(ThreadAccessClass::Private) {
                        // Exact ThreadRead admission already established that the
                        // requester is a participant (or the Superuser). Do not
                        // re-interpret a read as creator-only management here.
                        self.crud_store
                            .list_private_thread_participant_ids(
                                gateway_id,
                                authorization.workspace_id(),
                                authorization.thread_id(),
                                None,
                            )
                            .await
                    } else {
                        Ok(Some(Vec::new()))
                    };
                match result {
                    Ok(Some(ids)) => (false, ids, None, None),
                    Ok(None) => {
                        self.send_error(
                            connection_id,
                            AuthorizationExternalError::NotFound.response(request_id),
                        )
                        .await;
                        return;
                    }
                    Err(error) => {
                        record_authorization_unavailable(
                            expected_action.safe_name(),
                            "thread",
                            "read",
                        );
                        warn!(
                            connection_id,
                            error = %format!("{error:#}"),
                            "private-thread participant list failed"
                        );
                        self.send_error(
                            connection_id,
                            AuthorizationExternalError::Unavailable.response(request_id),
                        )
                        .await;
                        return;
                    }
                }
            }
            ThreadParticipantOperation::Add(target) => {
                match self
                    .crud_store
                    .add_private_thread_participant(
                        gateway_id,
                        authorization.workspace_id(),
                        authorization.thread_id(),
                        acting_scoped_principal,
                        &target,
                        actor,
                    )
                    .await
                {
                    Ok(Some(PrivateThreadParticipantMutation::Applied {
                        changed,
                        participant_ids: ids,
                    })) => (
                        changed,
                        ids,
                        Some(AccessChangeKind::ThreadParticipantAdded),
                        Some(target),
                    ),
                    Ok(Some(PrivateThreadParticipantMutation::TargetUnavailable)) | Ok(None) => {
                        self.send_error(
                            connection_id,
                            AuthorizationExternalError::NotFound.response(request_id),
                        )
                        .await;
                        return;
                    }
                    Ok(Some(PrivateThreadParticipantMutation::MandatoryCreator)) => {
                        self.send_error(
                            connection_id,
                            AuthorizationExternalError::Forbidden.response(request_id),
                        )
                        .await;
                        return;
                    }
                    Err(error) => {
                        record_authorization_unavailable(
                            ResourceAction::ThreadParticipantsManage.safe_name(),
                            "thread",
                            "mutation",
                        );
                        warn!(
                            connection_id,
                            error = %format!("{error:#}"),
                            "private-thread participant add failed"
                        );
                        self.send_error(
                            connection_id,
                            AuthorizationExternalError::Unavailable.response(request_id),
                        )
                        .await;
                        return;
                    }
                }
            }
            ThreadParticipantOperation::Remove(target) => {
                match self
                    .crud_store
                    .remove_private_thread_participant(
                        gateway_id,
                        authorization.workspace_id(),
                        authorization.thread_id(),
                        acting_scoped_principal,
                        &target,
                    )
                    .await
                {
                    Ok(Some(PrivateThreadParticipantMutation::Applied {
                        changed,
                        participant_ids: ids,
                    })) => (
                        changed,
                        ids,
                        Some(AccessChangeKind::ThreadParticipantRemoved),
                        Some(target),
                    ),
                    Ok(Some(PrivateThreadParticipantMutation::TargetUnavailable)) | Ok(None) => {
                        self.send_error(
                            connection_id,
                            AuthorizationExternalError::NotFound.response(request_id),
                        )
                        .await;
                        return;
                    }
                    Ok(Some(PrivateThreadParticipantMutation::MandatoryCreator)) => {
                        self.send_error(
                            connection_id,
                            AuthorizationExternalError::Forbidden.response(request_id),
                        )
                        .await;
                        return;
                    }
                    Err(error) => {
                        record_authorization_unavailable(
                            ResourceAction::ThreadParticipantsManage.safe_name(),
                            "thread",
                            "mutation",
                        );
                        warn!(
                            connection_id,
                            error = %format!("{error:#}"),
                            "private-thread participant remove failed"
                        );
                        self.send_error(
                            connection_id,
                            AuthorizationExternalError::Unavailable.response(request_id),
                        )
                        .await;
                        return;
                    }
                }
            }
        };

        if changed {
            self.publish_committed_authorization_invalidation(
                access_change_kind
                    .expect("changed participant mutation carries an access-change kind"),
                target_principal_id.clone(),
                authorization.workspace_id().to_owned(),
                Some(authorization.thread_id().to_owned()),
            )
            .await;
        }

        let participants = participant_ids
            .iter()
            .cloned()
            .map(|principal_id| ThreadParticipantSummary { principal_id })
            .collect();
        let response_payload = ThreadParticipantsResponse {
            workspace_id: authorization.workspace_id().to_owned(),
            thread_id: authorization.thread_id().to_owned(),
            participant_ids,
            participants,
            changed,
        };
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode participant response: {error}"),
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
                "failed to send thread participant response"
            );
            return;
        }

        if changed {
            let change = match access_change_kind
                .expect("changed participant mutation carries an access-change kind")
            {
                AccessChangeKind::ThreadParticipantAdded => ThreadParticipantChangeKind::Added,
                AccessChangeKind::ThreadParticipantRemoved => ThreadParticipantChangeKind::Removed,
                _ => unreachable!("participant mutation emitted a non-participant change kind"),
            };
            let notification = ThreadParticipantsChangedNotification {
                workspace_id: authorization.workspace_id().to_owned(),
                thread_id: authorization.thread_id().to_owned(),
                change,
                principal_id: target_principal_id
                    .expect("changed participant mutation carries a target principal"),
            };
            self.send_notification_to_thread_subscribers(
                authorization.thread_id(),
                events::THREAD_PARTICIPANTS_CHANGED,
                &notification,
            )
            .await;

            // Participant mutations change discoverability for exactly this
            // thread. Deliver its committed snapshot to every connection that
            // remains authorized (including a newly added participant) rather
            // than invalidating or reloading the workspace thread tree.
            match self
                .crud_store
                .get_thread_model(authorization.thread_id())
                .await
            {
                Ok(Some(thread)) => {
                    let placement = self
                        .crud_store
                        .get_thread_placement(thread.id.as_str())
                        .await
                        .ok()
                        .flatten();
                    let notification = ThreadUpdatedNotification { thread, placement };
                    self.send_thread_scoped_notification_to_connections(
                        notification.thread.id.as_str(),
                        events::THREAD_UPDATED,
                        &notification,
                        self.session_manager.connection_ids().await,
                    )
                    .await;
                }
                Ok(None) => warn!(
                    thread_id = authorization.thread_id(),
                    "committed thread disappeared after participant mutation"
                ),
                Err(error) => warn!(
                    thread_id = authorization.thread_id(),
                    error = %format!("{error:#}"),
                    "failed to load committed thread after participant mutation"
                ),
            }
        }
    }

    pub(super) async fn thread_move(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ThreadMoveParams,
    ) {
        let connection_id = request_context.connection_id();
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
        request_context: &RequestContext,
        request_id: RequestId,
        params: ThreadFolderCreateParams,
    ) {
        let connection_id = request_context.connection_id();
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
        request_context: &RequestContext,
        request_id: RequestId,
        params: ThreadFolderMoveParams,
    ) {
        let connection_id = request_context.connection_id();
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
        request_context: &RequestContext,
        request_id: RequestId,
        params: ThreadFolderDeleteParams,
    ) {
        let connection_id = request_context.connection_id();
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

    fn thread_tree_agents_doc_summary_from_record(
        record: pioneer_crud::ThreadAgentsDocSummaryRecord,
    ) -> ThreadAgentsDocSummary {
        ThreadAgentsDocSummary {
            id: record.id,
            workspace_id: record.workspace_id,
            folder_id: record.folder_id,
            status: Self::thread_tree_agents_doc_status_from_record(record.status),
            content_sha256: record.content_sha256,
            version: record.version,
            char_count: record.char_count,
            updated_at: record.updated_at_unix,
        }
    }

    fn thread_tree_agents_doc_status_from_record(
        status: pioneer_crud::ThreadAgentsDocStatus,
    ) -> ThreadAgentsDocStatus {
        match status {
            pioneer_crud::ThreadAgentsDocStatus::Draft => ThreadAgentsDocStatus::Draft,
            pioneer_crud::ThreadAgentsDocStatus::Active => ThreadAgentsDocStatus::Active,
            pioneer_crud::ThreadAgentsDocStatus::Archived => ThreadAgentsDocStatus::Archived,
        }
    }

    pub(super) async fn thread_unsubscribe(
        &self,
        request_context: &RequestContext,
        authorization: ThreadAccessAuthorization<'_>,
        request_id: RequestId,
        params: ThreadUnsubscribeParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        let runtime_draft_access = match &authorization {
            ThreadAccessAuthorization::RuntimeDraft(access) => Some((*access).clone()),
            ThreadAccessAuthorization::Persisted(_) => None,
        };
        let outcome = self
            .thread_manager
            .thread_unsubscribe(connection_id, &params.thread_id)
            .await;
        if let Some(closed_notification) = outcome.closed_notification.as_ref() {
            self.teardown_agent_thread(closed_notification.thread_id.as_str())
                .await;
            if let Some(access) = runtime_draft_access.as_ref() {
                self.cleanup_abandoned_runtime_draft_artifacts(
                    access.workspace_id(),
                    access.thread_id(),
                )
                .await;
            }
        }

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

        if let Some(access) = runtime_draft_access.as_ref() {
            self.send_notification_to_removed_runtime_draft_owner(
                access,
                events::THREAD_CLOSED,
                &closed_notification,
                outcome.closed_notification_subscribers,
            )
            .await;
        } else {
            self.send_notification_to_removed_thread_subscribers(
                closed_thread_id.as_str(),
                events::THREAD_CLOSED,
                &closed_notification,
                outcome.closed_notification_subscribers,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod access_filter_tests {
    use super::*;

    #[test]
    fn folder_placements_do_not_reveal_inaccessible_thread_ids() {
        let mut placements = vec![
            pioneer_protocol::ThreadPlacement {
                thread_id: "accessible".to_owned(),
                workspace_id: "workspace".to_owned(),
                folder_id: Some("folder".to_owned()),
            },
            pioneer_protocol::ThreadPlacement {
                thread_id: "private-peer-or-internal".to_owned(),
                workspace_id: "workspace".to_owned(),
                folder_id: Some("folder".to_owned()),
            },
        ];
        retain_accessible_thread_placements(
            &mut placements,
            &HashSet::from(["accessible".to_owned()]),
        );
        assert_eq!(placements.len(), 1);
        assert_eq!(placements[0].thread_id, "accessible");
    }

    #[test]
    fn agents_doc_summaries_follow_only_accessible_thread_folder_lineage() {
        let folders = vec![
            pioneer_protocol::ThreadFolder {
                id: "parent".to_owned(),
                workspace_id: "workspace".to_owned(),
                parent_folder_id: None,
                name: "Parent".to_owned(),
                created_at: 1,
                updated_at: 1,
            },
            pioneer_protocol::ThreadFolder {
                id: "child".to_owned(),
                workspace_id: "workspace".to_owned(),
                parent_folder_id: Some("parent".to_owned()),
                name: "Child".to_owned(),
                created_at: 1,
                updated_at: 1,
            },
            pioneer_protocol::ThreadFolder {
                id: "private-peer".to_owned(),
                workspace_id: "workspace".to_owned(),
                parent_folder_id: None,
                name: "Private peer".to_owned(),
                created_at: 1,
                updated_at: 1,
            },
        ];
        let placements = vec![
            pioneer_protocol::ThreadPlacement {
                thread_id: "accessible".to_owned(),
                workspace_id: "workspace".to_owned(),
                folder_id: Some("child".to_owned()),
            },
            pioneer_protocol::ThreadPlacement {
                thread_id: "inaccessible".to_owned(),
                workspace_id: "workspace".to_owned(),
                folder_id: Some("private-peer".to_owned()),
            },
        ];
        let summary = |id: &str, folder_id: Option<&str>| ThreadAgentsDocSummary {
            id: id.to_owned(),
            workspace_id: "workspace".to_owned(),
            folder_id: folder_id.map(str::to_owned),
            status: ThreadAgentsDocStatus::Active,
            content_sha256: format!("sha-{id}"),
            version: 1,
            char_count: 1,
            updated_at: 1,
        };
        let mut summaries = vec![
            summary("root", None),
            summary("parent", Some("parent")),
            summary("child", Some("child")),
            summary("private-peer", Some("private-peer")),
        ];

        retain_accessible_thread_agents_doc_summaries(
            &mut summaries,
            &folders,
            &placements,
            &HashSet::from(["accessible".to_owned()]),
        );

        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.id.as_str())
                .collect::<Vec<_>>(),
            vec!["root", "parent", "child"]
        );
    }

    #[test]
    fn agents_doc_summaries_require_at_least_one_accessible_thread() {
        let mut summaries = vec![ThreadAgentsDocSummary {
            id: "root".to_owned(),
            workspace_id: "workspace".to_owned(),
            folder_id: None,
            status: ThreadAgentsDocStatus::Active,
            content_sha256: "sha-root".to_owned(),
            version: 1,
            char_count: 1,
            updated_at: 1,
        }];

        retain_accessible_thread_agents_doc_summaries(&mut summaries, &[], &[], &HashSet::new());

        assert!(summaries.is_empty());
    }
}
