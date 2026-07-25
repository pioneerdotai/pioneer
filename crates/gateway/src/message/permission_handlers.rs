use super::*;

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

        let pending = PendingNativePermissionApprovalRequest {
            workspace_id: workspace_id.clone(),
            thread_id,
            turn_id,
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

        self.send_notification_to_workspace_connections(
            workspace_id.as_str(),
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
            .map(|pending| pending.request.clone())
            .collect::<Vec<_>>();
        requests.sort_by(|left, right| left.request_id.cmp(&right.request_id));

        for request in requests {
            self.send_notification_to_connections(
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
                self.send_notification_to_connections(
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
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnPermissionRequestRespondParams,
    ) {
        let pending = self
            .native_permission_pending_requests
            .lock()
            .await
            .remove(params.request_id.as_str());

        let Some(pending) = pending else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("stale permission request `{}`", params.request_id),
                ),
            )
            .await;
            return;
        };

        if let Some(connection_workspace_id) = self
            .session_manager
            .connection_workspace_id(connection_id)
            .await
            && connection_workspace_id != pending.workspace_id
        {
            let _ = pending
                .respond_to
                .send(pioneer_tools::PermissionApprovalResolution::Expired);
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "permission request `{}` belongs to workspace `{}`",
                        params.request_id, pending.workspace_id
                    ),
                ),
            )
            .await;
            return;
        }

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
        self.send_notification_to_workspace_connections(
            workspace_id.as_str(),
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
        self.send_notification_to_workspace_connections(
            workspace_id.as_str(),
            events::TURN_PERMISSION_REQUEST_RESOLVED,
            &notification,
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
