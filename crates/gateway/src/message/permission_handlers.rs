use super::*;
use crate::authorization::{AuthorizationExternalError, AuthorizedTurn};

impl MessageProcessor {
    pub(super) async fn open_native_permission_request(
        &self,
        request: crate::permissions::GatewayPermissionApprovalRequest,
    ) {
        let Some(workspace_id) = request.workspace_id.clone() else {
            let _ = request
                .respond_to
                .send(pioneer_tools::PermissionApprovalResolution::Deny {
                    message: "permission request missing workspace context".to_owned(),
                });
            return;
        };
        let Some(thread_id) = request.thread_id.clone() else {
            let _ = request
                .respond_to
                .send(pioneer_tools::PermissionApprovalResolution::Deny {
                    message: "permission request missing thread context".to_owned(),
                });
            return;
        };
        let Some(turn_id) = request.turn_id.clone() else {
            let _ = request
                .respond_to
                .send(pioneer_tools::PermissionApprovalResolution::Deny {
                    message: "permission request missing turn context".to_owned(),
                });
            return;
        };
        let authorization_context = match self
            .revalidate_execution_authorization_for_turn(
                workspace_id.as_str(),
                thread_id.as_str(),
                turn_id.as_str(),
                crate::authorization::ResourceAction::ThreadWrite,
            )
            .await
        {
            Ok(context) => context,
            Err(error) => {
                warn!(
                    workspace_id,
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "denied permission request without current initiating authority"
                );
                let _ =
                    request
                        .respond_to
                        .send(pioneer_tools::PermissionApprovalResolution::Deny {
                            message: "turn authorization is no longer active".to_owned(),
                        });
                return;
            }
        };
        let session =
            match pioneer_crud::load_session(
                &self.crud_store.database_connection(),
                authorization_context.initiating_session_id(),
            )
            .await
            {
                Ok(Some(session)) if session.refresh_generation >= 0 => session,
                Ok(_) => {
                    let _ = request.respond_to.send(
                        pioneer_tools::PermissionApprovalResolution::Deny {
                            message: "turn authorization is no longer active".to_owned(),
                        },
                    );
                    return;
                }
                Err(error) => {
                    warn!(
                        workspace_id,
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "failed to bind permission request to initiating session generation"
                    );
                    let _ = request.respond_to.send(
                        pioneer_tools::PermissionApprovalResolution::Deny {
                            message: "turn authorization is unavailable".to_owned(),
                        },
                    );
                    return;
                }
            };
        let authorization_context_fingerprint =
            match authorization_context.authorization_fingerprint() {
                Ok(fingerprint) => fingerprint,
                Err(error) => {
                    warn!(
                        workspace_id,
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "failed to fingerprint permission authorization context"
                    );
                    let _ = request.respond_to.send(
                        pioneer_tools::PermissionApprovalResolution::Deny {
                            message: "turn authorization is unavailable".to_owned(),
                        },
                    );
                    return;
                }
            };

        let visible_thread_ids = self
            .native_permission_visible_thread_ids(thread_id.as_str())
            .await;
        let protocol_request = TurnPermissionApprovalRequest {
            request_id: request.request_id.clone(),
            workspace_id: workspace_id.clone(),
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            visible_thread_ids,
            tool_name: request.tool_name,
            action: request.key.action,
            scope_hash: request.key.normalized_scope_hash,
            reason: request.reason,
            summary: request.summary,
            details: request.details,
        };

        let initiating_principal_id = authorization_context.initiating_principal_id().clone();
        let initiating_session_id = authorization_context.initiating_session_id().clone();
        let pending = PendingNativePermissionApprovalRequest {
            workspace_id: workspace_id.clone(),
            thread_id,
            turn_id,
            initiating_principal_id: initiating_principal_id.clone(),
            initiating_session_id: initiating_session_id.clone(),
            initiating_session_generation: session.refresh_generation,
            authorization_context_fingerprint,
            request: protocol_request.clone(),
            respond_to: request.respond_to,
        };

        if let Some(previous) = self
            .native_permission_pending_requests
            .lock()
            .await
            .insert(protocol_request.request_id.clone(), pending)
        {
            let _ = previous
                .respond_to
                .send(pioneer_tools::PermissionApprovalResolution::Expired);
        }

        let notification_thread_id =
            native_permission_notification_thread_id(&protocol_request).to_owned();
        self.send_execution_initiator_notification(
            notification_thread_id.as_str(),
            initiating_principal_id.as_str(),
            initiating_session_id.as_str(),
            crate::authorization::ResourceAction::ThreadWrite,
            events::TURN_PERMISSION_REQUEST_OPENED,
            &TurnPermissionRequestOpenedNotification {
                request: protocol_request,
            },
        )
        .await;
    }

    pub(super) async fn replay_native_permission_requests_for_thread(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        thread_id: &str,
    ) {
        let mut requests = self
            .native_permission_pending_requests
            .lock()
            .await
            .values()
            .filter(|pending| {
                pending.workspace_id == workspace_id
                    && (pending.request.thread_id == thread_id
                        || pending
                            .request
                            .visible_thread_ids
                            .iter()
                            .any(|visible_thread_id| visible_thread_id == thread_id))
            })
            .map(|pending| {
                (
                    pending.request.clone(),
                    pending.initiating_principal_id.clone(),
                    pending.initiating_session_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        requests.sort_by(|left, right| left.0.request_id.cmp(&right.0.request_id));

        for (request, initiating_principal_id, initiating_session_id) in requests {
            self.send_execution_initiator_notification_to_connections(
                thread_id,
                initiating_principal_id.as_str(),
                initiating_session_id.as_str(),
                crate::authorization::ResourceAction::ThreadWrite,
                events::TURN_PERMISSION_REQUEST_OPENED,
                &TurnPermissionRequestOpenedNotification {
                    request: request.clone(),
                },
                vec![connection_id],
            )
            .await;

            let still_pending = self
                .native_permission_pending_requests
                .lock()
                .await
                .get(request.request_id.as_str())
                .is_some_and(|pending| pending.request == request);
            if !still_pending {
                self.send_execution_initiator_notification_to_connections(
                    thread_id,
                    initiating_principal_id.as_str(),
                    initiating_session_id.as_str(),
                    crate::authorization::ResourceAction::ThreadWrite,
                    events::TURN_PERMISSION_REQUEST_RESOLVED,
                    &TurnPermissionRequestResolvedNotification {
                        request_id: request.request_id,
                        workspace_id: request.workspace_id,
                        thread_id: request.thread_id,
                        turn_id: request.turn_id,
                        resolution: TurnPermissionApprovalResolution::Expired,
                    },
                    vec![connection_id],
                )
                .await;
            }
        }
    }

    pub(super) async fn turn_permission_request_respond(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnPermissionRequestRespondParams,
    ) {
        let connection_id = request_context.connection_id();
        let pending_identity = {
            let pending = self.native_permission_pending_requests.lock().await;
            pending.get(params.request_id.as_str()).map(|pending| {
                (
                    pending.workspace_id.clone(),
                    pending.thread_id.clone(),
                    pending.turn_id.clone(),
                    pending.initiating_principal_id.clone(),
                    pending.initiating_session_id.clone(),
                    pending.initiating_session_generation,
                    pending.authorization_context_fingerprint.clone(),
                )
            })
        };

        let Some((
            pending_workspace_id,
            pending_thread_id,
            pending_turn_id,
            initiating_principal_id,
            initiating_session_id,
            initiating_session_generation,
            authorization_context_fingerprint,
        )) = pending_identity
        else {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        };

        if authorization.workspace_id() != pending_workspace_id
            || authorization.thread_id() != pending_thread_id
            || authorization.turn_id() != pending_turn_id
            || request_context.principal().principal_id != initiating_principal_id
            || request_context.principal().session_id != initiating_session_id
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let current_context = match self
            .revalidate_execution_authorization_for_turn(
                pending_workspace_id.as_str(),
                pending_thread_id.as_str(),
                pending_turn_id.as_str(),
                crate::authorization::ResourceAction::ThreadWrite,
            )
            .await
        {
            Ok(context)
                if context.initiating_principal_id() == &initiating_principal_id
                    && context.initiating_session_id() == &initiating_session_id
                    && context
                        .authorization_fingerprint()
                        .is_ok_and(|current| current == authorization_context_fingerprint) =>
            {
                context
            }
            Ok(_) | Err(_) => {
                self.expire_native_permission_request_after_authority_loss(
                    params.request_id.as_str(),
                    &pending_workspace_id,
                    &pending_thread_id,
                    &pending_turn_id,
                    &initiating_principal_id,
                    &initiating_session_id,
                    initiating_session_generation,
                    authorization_context_fingerprint.as_str(),
                )
                .await;
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
        };
        let current_session = pioneer_crud::load_session(
            &self.crud_store.database_connection(),
            current_context.initiating_session_id(),
        )
        .await;
        if !matches!(
            current_session,
            Ok(Some(session)) if session.refresh_generation == initiating_session_generation
        ) {
            self.expire_native_permission_request_after_authority_loss(
                params.request_id.as_str(),
                &pending_workspace_id,
                &pending_thread_id,
                &pending_turn_id,
                &initiating_principal_id,
                &initiating_session_id,
                initiating_session_generation,
                authorization_context_fingerprint.as_str(),
            )
            .await;
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        if let Some(connection_workspace_id) = self
            .session_manager
            .connection_workspace_id(connection_id)
            .await
            && connection_workspace_id != pending_workspace_id
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let pending = {
            let mut requests = self.native_permission_pending_requests.lock().await;
            let unchanged = requests
                .get(params.request_id.as_str())
                .is_some_and(|pending| {
                    pending.workspace_id == pending_workspace_id
                        && pending.thread_id == pending_thread_id
                        && pending.turn_id == pending_turn_id
                        && pending.initiating_principal_id == initiating_principal_id
                        && pending.initiating_session_id == initiating_session_id
                        && pending.initiating_session_generation == initiating_session_generation
                        && pending.authorization_context_fingerprint
                            == authorization_context_fingerprint
                });
            unchanged
                .then(|| requests.remove(params.request_id.as_str()))
                .flatten()
        };
        let Some(pending) = pending else {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        };

        let notification_thread_id =
            native_permission_notification_thread_id(&pending.request).to_owned();
        let notification_principal_id = pending.initiating_principal_id.clone();
        let notification_session_id = pending.initiating_session_id.clone();
        let resolution = params.resolution;
        let _ = pending
            .respond_to
            .send(permission_approval_resolution_from_protocol(resolution));

        let response = TurnPermissionRequestRespondResponse {
            request_id: params.request_id.clone(),
            resolution,
        };
        self.send_turn_permission_response(connection_id, request_id, &response)
            .await;

        let workspace_id = pending.workspace_id.clone();
        let notification = TurnPermissionRequestResolvedNotification {
            request_id: params.request_id,
            workspace_id: workspace_id.clone(),
            thread_id: pending.thread_id,
            turn_id: pending.turn_id,
            resolution,
        };
        self.send_execution_initiator_notification(
            notification_thread_id.as_str(),
            notification_principal_id.as_str(),
            notification_session_id.as_str(),
            crate::authorization::ResourceAction::ThreadWrite,
            events::TURN_PERMISSION_REQUEST_RESOLVED,
            &notification,
        )
        .await;
    }

    pub(super) async fn cancel_native_permission_request(&self, request_id: &str) {
        let pending = self
            .native_permission_pending_requests
            .lock()
            .await
            .remove(request_id);

        let Some(pending) = pending else {
            return;
        };

        let notification_thread_id =
            native_permission_notification_thread_id(&pending.request).to_owned();
        let notification_principal_id = pending.initiating_principal_id.clone();
        let notification_session_id = pending.initiating_session_id.clone();
        let _ = pending
            .respond_to
            .send(pioneer_tools::PermissionApprovalResolution::Cancelled);

        let workspace_id = pending.workspace_id.clone();
        let notification = TurnPermissionRequestResolvedNotification {
            request_id: request_id.to_owned(),
            workspace_id: workspace_id.clone(),
            thread_id: pending.thread_id,
            turn_id: pending.turn_id,
            resolution: TurnPermissionApprovalResolution::Cancelled,
        };
        self.send_execution_initiator_notification(
            notification_thread_id.as_str(),
            notification_principal_id.as_str(),
            notification_session_id.as_str(),
            crate::authorization::ResourceAction::ThreadWrite,
            events::TURN_PERMISSION_REQUEST_RESOLVED,
            &notification,
        )
        .await;
    }

    pub(super) async fn expire_native_permission_requests_without_current_authority(
        &self,
        workspace_id: &str,
        affected_principal_id: Option<&pioneer_protocol::PrincipalId>,
    ) {
        let candidates = self
            .native_permission_pending_requests
            .lock()
            .await
            .values()
            .filter(|pending| {
                pending.workspace_id == workspace_id
                    && affected_principal_id
                        .is_none_or(|principal_id| &pending.initiating_principal_id == principal_id)
            })
            .map(|pending| {
                (
                    pending.request.request_id.clone(),
                    pending.workspace_id.clone(),
                    pending.thread_id.clone(),
                    pending.turn_id.clone(),
                    pending.initiating_principal_id.clone(),
                    pending.initiating_session_id.clone(),
                    pending.initiating_session_generation,
                    pending.authorization_context_fingerprint.clone(),
                )
            })
            .collect::<Vec<_>>();

        for (
            request_id,
            pending_workspace_id,
            pending_thread_id,
            pending_turn_id,
            initiating_principal_id,
            initiating_session_id,
            initiating_session_generation,
            authorization_context_fingerprint,
        ) in candidates
        {
            let authority_is_current = match self
                .revalidate_execution_authorization_for_turn(
                    pending_workspace_id.as_str(),
                    pending_thread_id.as_str(),
                    pending_turn_id.as_str(),
                    crate::authorization::ResourceAction::ThreadWrite,
                )
                .await
            {
                Ok(context)
                    if context.initiating_principal_id() == &initiating_principal_id
                        && context.initiating_session_id() == &initiating_session_id
                        && context
                            .authorization_fingerprint()
                            .is_ok_and(|current| current == authorization_context_fingerprint) =>
                {
                    matches!(
                        pioneer_crud::load_session(
                            &self.crud_store.database_connection(),
                            context.initiating_session_id(),
                        )
                        .await,
                        Ok(Some(session))
                            if session.refresh_generation == initiating_session_generation
                    )
                }
                Ok(_) | Err(_) => false,
            };
            if authority_is_current {
                continue;
            }
            self.expire_native_permission_request_after_authority_loss(
                request_id.as_str(),
                pending_workspace_id.as_str(),
                pending_thread_id.as_str(),
                pending_turn_id.as_str(),
                &initiating_principal_id,
                &initiating_session_id,
                initiating_session_generation,
                authorization_context_fingerprint.as_str(),
            )
            .await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn expire_native_permission_request_after_authority_loss(
        &self,
        request_id: &str,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        initiating_principal_id: &pioneer_protocol::PrincipalId,
        initiating_session_id: &pioneer_protocol::AuthSessionId,
        initiating_session_generation: i64,
        authorization_context_fingerprint: &str,
    ) {
        let pending = {
            let mut requests = self.native_permission_pending_requests.lock().await;
            let unchanged = requests.get(request_id).is_some_and(|pending| {
                pending.workspace_id == workspace_id
                    && pending.thread_id == thread_id
                    && pending.turn_id == turn_id
                    && &pending.initiating_principal_id == initiating_principal_id
                    && &pending.initiating_session_id == initiating_session_id
                    && pending.initiating_session_generation == initiating_session_generation
                    && pending.authorization_context_fingerprint
                        == authorization_context_fingerprint
            });
            unchanged.then(|| requests.remove(request_id)).flatten()
        };
        let Some(pending) = pending else {
            return;
        };

        let notification_thread_id =
            native_permission_notification_thread_id(&pending.request).to_owned();
        let notification_principal_id = pending.initiating_principal_id.clone();
        let notification_session_id = pending.initiating_session_id.clone();
        let _ = pending
            .respond_to
            .send(pioneer_tools::PermissionApprovalResolution::Expired);
        self.send_execution_initiator_notification(
            notification_thread_id.as_str(),
            notification_principal_id.as_str(),
            notification_session_id.as_str(),
            crate::authorization::ResourceAction::ThreadWrite,
            events::TURN_PERMISSION_REQUEST_RESOLVED,
            &TurnPermissionRequestResolvedNotification {
                request_id: request_id.to_owned(),
                workspace_id: pending.workspace_id,
                thread_id: pending.thread_id,
                turn_id: pending.turn_id,
                resolution: TurnPermissionApprovalResolution::Expired,
            },
        )
        .await;
    }

    async fn send_turn_permission_response<T: Serialize>(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        response_payload: &T,
    ) {
        let response = match JsonRpcResponse::from_result(request_id, response_payload) {
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
                "failed to send turn permission response"
            );
        }
    }

    async fn native_permission_visible_thread_ids(&self, thread_id: &str) -> Vec<String> {
        let mut visible_thread_ids = Vec::new();
        let mut seen = HashSet::<String>::new();
        let mut current = thread_id.to_owned();

        for _ in 0..64 {
            let lineage = match self
                .crud_store
                .get_task_thread_lineage(current.as_str())
                .await
            {
                Ok(Some(lineage)) => lineage,
                Ok(None) => break,
                Err(error) => {
                    warn!(
                        thread_id = current,
                        error = %format!("{error:#}"),
                        "failed to resolve visible parent threads for native permission request"
                    );
                    break;
                }
            };

            let parent_thread_id = lineage.parent_thread_id;
            if !seen.insert(parent_thread_id.clone()) {
                break;
            }
            visible_thread_ids.push(parent_thread_id.clone());
            current = parent_thread_id;
        }

        visible_thread_ids
    }
}

fn native_permission_notification_thread_id(request: &TurnPermissionApprovalRequest) -> &str {
    request
        .visible_thread_ids
        .last()
        .map(String::as_str)
        .unwrap_or(request.thread_id.as_str())
}

fn permission_approval_resolution_from_protocol(
    resolution: TurnPermissionApprovalResolution,
) -> pioneer_tools::PermissionApprovalResolution {
    match resolution {
        TurnPermissionApprovalResolution::AllowOnce => {
            pioneer_tools::PermissionApprovalResolution::AllowOnce
        }
        TurnPermissionApprovalResolution::AllowForTurn => {
            pioneer_tools::PermissionApprovalResolution::AllowForTurn
        }
        TurnPermissionApprovalResolution::Deny => {
            pioneer_tools::PermissionApprovalResolution::Deny {
                message: "permission request denied by user".to_owned(),
            }
        }
        TurnPermissionApprovalResolution::Cancelled => {
            pioneer_tools::PermissionApprovalResolution::Cancelled
        }
        TurnPermissionApprovalResolution::Expired => {
            pioneer_tools::PermissionApprovalResolution::Expired
        }
    }
}
