use super::*;
use crate::authorization::{AuthorizationExternalError, AuthorizedTurn};

fn public_permission_error(
    request_id: Option<RequestId>,
    jsonrpc_code: i64,
    stage: pioneer_protocol::PublicErrorStage,
    diagnostic: impl std::fmt::Display,
) -> JsonRpcErrorResponse {
    let public_code = if jsonrpc_code == INVALID_PARAMS_CODE {
        pioneer_protocol::PublicErrorCode::InvalidInput
    } else {
        pioneer_protocol::PublicErrorCode::Internal
    };
    crate::public_error::agent_rpc_error(request_id, jsonrpc_code, public_code, stage, diagnostic)
}

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
                crate::authorization::ResourceAction::AgentRequestObserve,
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
                    "denied permission request without current collaboration observation authority"
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
        let interaction_service = crate::human_interaction::HumanInteractionService::from_budget(
            authorization_context.human_interaction_budget(),
        );
        if let Err(error) = interaction_service.validate_native_request(&protocol_request) {
            warn!(
                workspace_id,
                thread_id,
                turn_id,
                request_id = protocol_request.request_id.as_str(),
                error = %format!("{error:#}"),
                "denied native permission request outside the human interaction budget"
            );
            let _ = request
                .respond_to
                .send(pioneer_tools::PermissionApprovalResolution::Deny {
                    message: "permission request exceeds the interaction budget".to_owned(),
                });
            return;
        }
        let initiating_principal_id = authorization_context.initiating_principal_id().clone();
        let initiating_session_id = authorization_context.initiating_session_id().clone();
        let durable_request = CLIRuntimePendingRequest {
            kind: CLIRuntimeRequestKind::Other,
            title: Some("Tool permission requested".to_owned()),
            message: protocol_request.summary.clone(),
            native_request_id: Some(protocol_request.request_id.clone()),
            payload: serde_json::to_value(&protocol_request).ok(),
        };
        let payload_json = match pioneer_crud::serialize_cli_runtime_json(&durable_request) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    workspace_id,
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to encode native permission request durably"
                );
                let _ =
                    request
                        .respond_to
                        .send(pioneer_tools::PermissionApprovalResolution::Deny {
                            message: "permission request could not be persisted".to_owned(),
                        });
                return;
            }
        };
        let now = chrono::Utc::now().fixed_offset();
        let opened = match self
            .crud_store
            .open_native_human_interaction_request_with_authorization(
                NewCliRuntimePendingRequest {
                    request_id: protocol_request.request_id.clone(),
                    runtime_id: NATIVE_HUMAN_INTERACTION_RUNTIME_ID.to_owned(),
                    runtime_kind: NATIVE_HUMAN_INTERACTION_RUNTIME_KIND.to_owned(),
                    workspace_id: workspace_id.clone(),
                    thread_id: thread_id.clone(),
                    turn_id: Some(turn_id.clone()),
                    native_thread_id: None,
                    native_turn_id: None,
                    native_item_id: None,
                    request_kind: NATIVE_HUMAN_INTERACTION_REQUEST_KIND.to_owned(),
                    payload_json,
                    created_at: now,
                    updated_at: now,
                },
                pioneer_crud::CliRuntimeRequestAuthorizationBinding {
                    initiating_principal_id: initiating_principal_id.to_string(),
                    initiating_session_id: initiating_session_id.to_string(),
                    initiating_session_generation: 0,
                    authorization_context_fingerprint: authorization_context_fingerprint.clone(),
                },
                interaction_service
                    .budget()
                    .max_pending_requests_per_execution,
            )
            .await
        {
            Ok(opened) => opened,
            Err(error) => {
                warn!(
                    workspace_id,
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to persist native permission request"
                );
                let _ =
                    request
                        .respond_to
                        .send(pioneer_tools::PermissionApprovalResolution::Deny {
                            message: "permission request could not be persisted".to_owned(),
                        });
                return;
            }
        };

        // A response can be accepted durably while the Gateway process is
        // down.  When the native execution is recovered it re-opens the same
        // deterministic request id; consume that response before publishing
        // another approval notification.  This is the restart rendezvous for
        // the native tool call, not a new permission decision.
        if opened.status != StoredCliRuntimePendingRequestStatus::Pending {
            if !durable_native_permission_request(&opened).is_some_and(|durable| {
                same_native_permission_request_contract(&durable, &protocol_request)
            }) {
                let _ = request
                    .respond_to
                    .send(pioneer_tools::PermissionApprovalResolution::Expired);
                return;
            }
            if let Some(resolution) = durable_native_permission_resolution(&opened) {
                if opened.status.is_terminal() {
                    let _ = request
                        .respond_to
                        .send(permission_approval_resolution_from_protocol(resolution));
                    return;
                }
                let delivering =
                    if opened.status == StoredCliRuntimePendingRequestStatus::Delivering {
                        opened.clone()
                    } else {
                        let Some(delivering) = self
                            .crud_store
                            .transition_native_human_interaction_delivery(
                                pioneer_crud::TransitionCliRuntimePendingRequestDelivery {
                                    request_id: opened.request_id.clone(),
                                    expected_status: opened.status,
                                    status: StoredCliRuntimePendingRequestStatus::Delivering,
                                    delivery_error: None,
                                    updated_at: now,
                                    resolved_at: None,
                                },
                            )
                            .await
                            .ok()
                            .flatten()
                        else {
                            let _ = request
                                .respond_to
                                .send(pioneer_tools::PermissionApprovalResolution::Expired);
                            return;
                        };
                        delivering
                    };
                let resolution_for_tool =
                    permission_approval_resolution_from_protocol(resolution.clone());
                if request.respond_to.send(resolution_for_tool).is_err() {
                    let now = chrono::Utc::now().fixed_offset();
                    let _ = self
                        .crud_store
                        .transition_native_human_interaction_delivery(
                            pioneer_crud::TransitionCliRuntimePendingRequestDelivery {
                                request_id: delivering.request_id,
                                expected_status: StoredCliRuntimePendingRequestStatus::Delivering,
                                status: StoredCliRuntimePendingRequestStatus::Expired,
                                delivery_error: Some(
                                    "native response lane did not acknowledge".to_owned(),
                                ),
                                updated_at: now,
                                resolved_at: Some(now),
                            },
                        )
                        .await;
                    return;
                }
                let now = chrono::Utc::now().fixed_offset();
                let _ = self
                    .crud_store
                    .transition_native_human_interaction_delivery(
                        pioneer_crud::TransitionCliRuntimePendingRequestDelivery {
                            request_id: delivering.request_id,
                            expected_status: StoredCliRuntimePendingRequestStatus::Delivering,
                            status: StoredCliRuntimePendingRequestStatus::Resolved,
                            delivery_error: None,
                            updated_at: now,
                            resolved_at: Some(now),
                        },
                    )
                    .await;
                return;
            }
            let _ = request
                .respond_to
                .send(pioneer_tools::PermissionApprovalResolution::Expired);
            return;
        }
        let pending = PendingNativePermissionApprovalRequest {
            workspace_id: workspace_id.clone(),
            thread_id,
            turn_id,
            initiating_principal_id: initiating_principal_id.clone(),
            initiating_session_id: initiating_session_id.clone(),
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

        let notification = TurnPermissionRequestOpenedNotification {
            request: protocol_request.clone(),
        };
        self.send_native_permission_collaborator_notification(
            &protocol_request,
            events::TURN_PERMISSION_REQUEST_OPENED,
            &notification,
        )
        .await;
    }

    pub(super) async fn replay_native_permission_requests_for_thread(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        thread_id: &str,
    ) {
        let live_request_ids = self
            .native_permission_pending_requests
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let now_unix_ms = chrono::Utc::now().timestamp_millis();
        let mut after = None;
        loop {
            let orphaned = match self
                .crud_store
                .list_cli_runtime_pending_requests(
                    pioneer_crud::CliRuntimePendingRequestListFilter {
                        workspace_id: Some(workspace_id.to_owned()),
                        runtime_id: Some(NATIVE_HUMAN_INTERACTION_RUNTIME_ID.to_owned()),
                        open_only: true,
                        after,
                        limit: Some(pioneer_crud::CLI_RUNTIME_PENDING_REQUEST_PAGE_MAX),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(orphaned) => orphaned,
                Err(error) => {
                    warn!(
                        workspace_id,
                        error = %format!("{error:#}"),
                        "failed to reconcile durable native human interactions during replay"
                    );
                    break;
                }
            };
            let next = orphaned
                .last()
                .map(|record| (record.created_at, record.request_id.clone()));
            for record in orphaned {
                if live_request_ids.contains(record.request_id.as_str()) {
                    continue;
                }
                // A restart drops only the in-process responder, not the
                // durable approval. Keep the row visible until its bounded
                // lifecycle TTL; a recovered native Turn can reopen the same
                // deterministic request id and attach a fresh responder.
                if now_unix_ms.saturating_sub(record.created_at.timestamp_millis())
                    < crate::human_interaction::HUMAN_INTERACTION_RESPONSE_TIMEOUT_MS
                {
                    if record.status == StoredCliRuntimePendingRequestStatus::Pending
                        && let Some(request) = durable_native_permission_request(&record)
                        && (request.thread_id == thread_id
                            || request
                                .visible_thread_ids
                                .iter()
                                .any(|visible_thread_id| visible_thread_id == thread_id))
                    {
                        self.send_execution_collaborator_notification_to_connections(
                            thread_id,
                            crate::authorization::ResourceAction::AgentRequestObserve,
                            events::TURN_PERMISSION_REQUEST_OPENED,
                            &TurnPermissionRequestOpenedNotification { request },
                            vec![connection_id],
                        )
                        .await;
                    }
                    continue;
                }
                let _ = self.expire_durable_native_permission_record(&record).await;
            }
            let Some(next) = next else {
                break;
            };
            after = Some(next);
        }
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
            self.send_execution_collaborator_notification_to_connections(
                thread_id,
                crate::authorization::ResourceAction::AgentRequestObserve,
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
                self.send_execution_collaborator_notification_to_connections(
                    thread_id,
                    crate::authorization::ResourceAction::AgentRequestObserve,
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
        let live_pending_identity = {
            let pending = self.native_permission_pending_requests.lock().await;
            pending.get(params.request_id.as_str()).map(|pending| {
                (
                    pending.workspace_id.clone(),
                    pending.thread_id.clone(),
                    pending.turn_id.clone(),
                    pending.initiating_principal_id.clone(),
                    pending.initiating_session_id.clone(),
                    pending.authorization_context_fingerprint.clone(),
                )
            })
        };
        let pending_identity = match live_pending_identity {
            Some(identity) => Some(identity),
            None => self
                .crud_store
                .get_cli_runtime_pending_request(params.request_id.as_str())
                .await
                .ok()
                .flatten()
                .filter(|record| record.status.is_open())
                .and_then(|record| durable_native_permission_identity(&record)),
        };

        let Some((
            pending_workspace_id,
            pending_thread_id,
            pending_turn_id,
            initiating_principal_id,
            initiating_session_id,
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
                crate::authorization::ResourceAction::AgentRequestRespond,
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
        let approval_cap = current_context.approval_scope_cap();
        let Some(responder_approval_cap) = crate::authorization::AuthorizationService::new()
            .approval_scope_cap(
                request_context.principal().kind,
                request_context.principal().role_key.as_ref(),
            )
        else {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        };
        let resolution_is_allowed = match &params.resolution {
            TurnPermissionApprovalResolution::AllowOnce => {
                approval_cap.allow_once && responder_approval_cap.allow_once
            }
            TurnPermissionApprovalResolution::AllowForTurn => {
                approval_cap.allow_for_turn && responder_approval_cap.allow_for_turn
            }
            TurnPermissionApprovalResolution::Deny
            | TurnPermissionApprovalResolution::Cancelled => true,
            TurnPermissionApprovalResolution::Expired => false,
        };
        if !resolution_is_allowed {
            self.send_error(
                connection_id,
                public_permission_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    pioneer_protocol::PublicErrorStage::Admission,
                    "permission response scope exceeds the execution approval cap",
                ),
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

        let response_json = match pioneer_crud::serialize_cli_runtime_json(&params.resolution) {
            Ok(response) => Some(response),
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_permission_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Persistence,
                        format!("failed to encode native permission response: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        let response_revision = match self.current_authorization_revision().await {
            Ok(revision) => match i64::try_from(revision) {
                Ok(revision) => revision,
                Err(_) => {
                    self.send_error(
                        connection_id,
                        public_permission_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            pioneer_protocol::PublicErrorStage::Persistence,
                            "authorization revision exceeds the durable interaction range",
                        ),
                    )
                    .await;
                    return;
                }
            },
            Err(error) => {
                self.send_error(
                    connection_id,
                    public_permission_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Persistence,
                        format!("failed to load interaction policy generation: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        let now = chrono::Utc::now().fixed_offset();
        let accepted = match self
            .crud_store
            .accept_native_human_interaction_response(
                pioneer_crud::AcceptCliRuntimePendingRequestResponse {
                    request_id: params.request_id.clone(),
                    response_json,
                    responding_principal_id: request_context.principal().principal_id.to_string(),
                    responding_session_id: request_context.principal().session_id.to_string(),
                    response_authorization_revision: response_revision,
                    response_contains_secret: false,
                    updated_at: now,
                },
            )
            .await
        {
            Ok(Some(accepted)) => accepted,
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
                    public_permission_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Persistence,
                        format!("failed to accept native permission response: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
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
                        && pending.authorization_context_fingerprint
                            == authorization_context_fingerprint
                });
            unchanged
                .then(|| requests.remove(params.request_id.as_str()))
                .flatten()
        };
        let Some(pending) = pending else {
            // The Gateway may have restarted after the durable response was
            // accepted.  Keep the response in `ResponseAccepted` so the
            // recovered native executor can rendezvous with it using the same
            // deterministic request id; replying to the collaborator is safe
            // because no process-local tool lane exists to deliver to here.
            let response = TurnPermissionRequestRespondResponse {
                request_id: params.request_id,
                resolution: params.resolution.clone(),
            };
            self.send_turn_permission_response(connection_id, request_id, &response)
                .await;
            if let Some(request) = durable_native_permission_request(&accepted) {
                let notification = TurnPermissionRequestResolvedNotification {
                    request_id: request.request_id.clone(),
                    workspace_id: request.workspace_id.clone(),
                    thread_id: request.thread_id.clone(),
                    turn_id: request.turn_id.clone(),
                    resolution: params.resolution,
                };
                self.send_native_permission_collaborator_notification(
                    &request,
                    events::TURN_PERMISSION_REQUEST_RESOLVED,
                    &notification,
                )
                .await;
            }
            return;
        };

        let delivering = match self
            .crud_store
            .transition_native_human_interaction_delivery(
                pioneer_crud::TransitionCliRuntimePendingRequestDelivery {
                    request_id: accepted.request_id.clone(),
                    expected_status: StoredCliRuntimePendingRequestStatus::ResponseAccepted,
                    status: StoredCliRuntimePendingRequestStatus::Delivering,
                    delivery_error: None,
                    updated_at: now,
                    resolved_at: None,
                },
            )
            .await
        {
            Ok(Some(delivering)) => delivering,
            Ok(None) | Err(_) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
        };

        let notification_request = pending.request.clone();
        let resolution = params.resolution;
        if pending
            .respond_to
            .send(permission_approval_resolution_from_protocol(
                resolution.clone(),
            ))
            .is_err()
        {
            let now = chrono::Utc::now().fixed_offset();
            let _ = self
                .crud_store
                .transition_native_human_interaction_delivery(
                    pioneer_crud::TransitionCliRuntimePendingRequestDelivery {
                        request_id: delivering.request_id,
                        expected_status: StoredCliRuntimePendingRequestStatus::Delivering,
                        status: StoredCliRuntimePendingRequestStatus::Expired,
                        delivery_error: Some("native response lane did not acknowledge".to_owned()),
                        updated_at: now,
                        resolved_at: Some(now),
                    },
                )
                .await;
            self.send_error(
                connection_id,
                public_permission_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    pioneer_protocol::PublicErrorStage::Delivery,
                    "native permission response lane is closed",
                ),
            )
            .await;
            return;
        }
        let now = chrono::Utc::now().fixed_offset();
        let delivered = self
            .crud_store
            .transition_native_human_interaction_delivery(
                pioneer_crud::TransitionCliRuntimePendingRequestDelivery {
                    request_id: delivering.request_id,
                    expected_status: StoredCliRuntimePendingRequestStatus::Delivering,
                    status: StoredCliRuntimePendingRequestStatus::Resolved,
                    delivery_error: None,
                    updated_at: now,
                    resolved_at: Some(now),
                },
            )
            .await;
        if !matches!(delivered, Ok(Some(_))) {
            self.send_error(
                connection_id,
                public_permission_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    pioneer_protocol::PublicErrorStage::Persistence,
                    "native permission response was delivered but durable acknowledgement failed",
                ),
            )
            .await;
            return;
        }

        let response = TurnPermissionRequestRespondResponse {
            request_id: params.request_id.clone(),
            resolution: resolution.clone(),
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
        self.send_native_permission_collaborator_notification(
            &notification_request,
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

        let notification_request = pending.request.clone();
        let _ = pending
            .respond_to
            .send(pioneer_tools::PermissionApprovalResolution::Cancelled);
        self.persist_native_interaction_terminal(
            request_id,
            StoredCliRuntimePendingRequestStatus::Cancelled,
            TurnPermissionApprovalResolution::Cancelled,
        )
        .await;

        let workspace_id = pending.workspace_id.clone();
        let notification = TurnPermissionRequestResolvedNotification {
            request_id: request_id.to_owned(),
            workspace_id: workspace_id.clone(),
            thread_id: pending.thread_id,
            turn_id: pending.turn_id,
            resolution: TurnPermissionApprovalResolution::Cancelled,
        };
        self.send_native_permission_collaborator_notification(
            &notification_request,
            events::TURN_PERMISSION_REQUEST_RESOLVED,
            &notification,
        )
        .await;
    }

    pub(super) async fn expire_native_permission_request(&self, request_id: &str) {
        let pending = self
            .native_permission_pending_requests
            .lock()
            .await
            .remove(request_id);

        let Some(pending) = pending else {
            return;
        };

        let notification_request = pending.request.clone();
        let _ = pending
            .respond_to
            .send(pioneer_tools::PermissionApprovalResolution::Expired);
        self.persist_native_interaction_terminal(
            request_id,
            StoredCliRuntimePendingRequestStatus::Expired,
            TurnPermissionApprovalResolution::Expired,
        )
        .await;

        let notification = TurnPermissionRequestResolvedNotification {
            request_id: request_id.to_owned(),
            workspace_id: pending.workspace_id,
            thread_id: pending.thread_id,
            turn_id: pending.turn_id,
            resolution: TurnPermissionApprovalResolution::Expired,
        };
        self.send_native_permission_collaborator_notification(
            &notification_request,
            events::TURN_PERMISSION_REQUEST_RESOLVED,
            &notification,
        )
        .await;
    }

    /// Returns whether a native Turn is currently waiting for a durable human
    /// approval.  The timeout supervisor uses this as a typed wait state: an
    /// approval wait is not ordinary tool liveness and must not be renewed by
    /// a heartbeat forever.  Orphaned requests are retained across restart
    /// until the same bounded TTL, then deterministically expire and block the
    /// Turn for resumable recovery when no in-process responder exists.
    pub(super) async fn reconcile_native_permission_wait_for_turn(
        &self,
        turn_id: &str,
        now_unix_ms: i64,
    ) -> anyhow::Result<bool> {
        let records = self
            .crud_store
            .list_cli_runtime_pending_requests(pioneer_crud::CliRuntimePendingRequestListFilter {
                runtime_id: Some(NATIVE_HUMAN_INTERACTION_RUNTIME_ID.to_owned()),
                turn_id: Some(turn_id.to_owned()),
                open_only: true,
                limit: None,
                ..Default::default()
            })
            .await?;
        let pending = records
            .into_iter()
            .filter(|record| record.status.is_open())
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(false);
        }

        for record in pending {
            let expired = now_unix_ms.saturating_sub(record.created_at.timestamp_millis())
                >= crate::human_interaction::HUMAN_INTERACTION_RESPONSE_TIMEOUT_MS;
            if !expired {
                continue;
            }

            let live = self
                .native_permission_pending_requests
                .lock()
                .await
                .contains_key(record.request_id.as_str());
            if live {
                self.expire_native_permission_request(record.request_id.as_str())
                    .await;
                continue;
            }

            self.expire_durable_native_permission_record(&record)
                .await?;
            let reason = format!(
                "native permission request `{}` expired while its responder was unavailable; Turn is resumable",
                record.request_id
            );
            let _ = self
                .mark_turn_blocked(record.thread_id.clone(), turn_id.to_owned(), reason)
                .await;
        }

        Ok(true)
    }

    async fn expire_durable_native_permission_record(
        &self,
        record: &CliRuntimePendingRequestRecord,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().fixed_offset();
        let response_json =
            pioneer_crud::serialize_cli_runtime_json(&TurnPermissionApprovalResolution::Expired)
                .ok();
        match record.status {
            StoredCliRuntimePendingRequestStatus::Pending => {
                self.crud_store
                    .resolve_native_human_interaction_request(
                        pioneer_crud::ResolveCliRuntimePendingRequest {
                            request_id: record.request_id.clone(),
                            status: StoredCliRuntimePendingRequestStatus::Expired,
                            response_json,
                            updated_at: now,
                            resolved_at: now,
                        },
                    )
                    .await?;
            }
            StoredCliRuntimePendingRequestStatus::ResponseAccepted => {
                let reset = self
                    .crud_store
                    .transition_native_human_interaction_delivery(
                        pioneer_crud::TransitionCliRuntimePendingRequestDelivery {
                            request_id: record.request_id.clone(),
                            expected_status: StoredCliRuntimePendingRequestStatus::ResponseAccepted,
                            status: StoredCliRuntimePendingRequestStatus::Pending,
                            delivery_error: Some(
                                "native response expired before recovery delivery".to_owned(),
                            ),
                            updated_at: now,
                            resolved_at: None,
                        },
                    )
                    .await?;
                if reset.is_some() {
                    self.crud_store
                        .resolve_native_human_interaction_request(
                            pioneer_crud::ResolveCliRuntimePendingRequest {
                                request_id: record.request_id.clone(),
                                status: StoredCliRuntimePendingRequestStatus::Expired,
                                response_json,
                                updated_at: now,
                                resolved_at: now,
                            },
                        )
                        .await?;
                }
            }
            StoredCliRuntimePendingRequestStatus::Delivering
            | StoredCliRuntimePendingRequestStatus::DeliveryFailed => {
                let mut status = record.status;
                if status == StoredCliRuntimePendingRequestStatus::DeliveryFailed {
                    let transitioned = self
                        .crud_store
                        .transition_native_human_interaction_delivery(
                            pioneer_crud::TransitionCliRuntimePendingRequestDelivery {
                                request_id: record.request_id.clone(),
                                expected_status:
                                    StoredCliRuntimePendingRequestStatus::DeliveryFailed,
                                status: StoredCliRuntimePendingRequestStatus::Delivering,
                                delivery_error: Some(
                                    "native response expired during recovery".to_owned(),
                                ),
                                updated_at: now,
                                resolved_at: None,
                            },
                        )
                        .await?;
                    if transitioned.is_none() {
                        return Ok(());
                    }
                    status = StoredCliRuntimePendingRequestStatus::Delivering;
                }
                if status == StoredCliRuntimePendingRequestStatus::Delivering {
                    let _ = self
                        .crud_store
                        .transition_native_human_interaction_delivery(
                            pioneer_crud::TransitionCliRuntimePendingRequestDelivery {
                                request_id: record.request_id.clone(),
                                expected_status: StoredCliRuntimePendingRequestStatus::Delivering,
                                status: StoredCliRuntimePendingRequestStatus::Expired,
                                delivery_error: Some(
                                    "native response expired during recovery".to_owned(),
                                ),
                                updated_at: now,
                                resolved_at: Some(now),
                            },
                        )
                        .await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) async fn expire_native_permission_requests_without_current_authority(
        &self,
        workspace_id: &str,
        _affected_principal_id: Option<&pioneer_protocol::PrincipalId>,
    ) {
        let candidates = self
            .native_permission_pending_requests
            .lock()
            .await
            .values()
            .filter(|pending| pending.workspace_id == workspace_id)
            .map(|pending| {
                (
                    pending.request.request_id.clone(),
                    pending.workspace_id.clone(),
                    pending.thread_id.clone(),
                    pending.turn_id.clone(),
                    pending.initiating_principal_id.clone(),
                    pending.initiating_session_id.clone(),
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
            authorization_context_fingerprint,
        ) in candidates
        {
            let authority_is_current = match self
                .revalidate_execution_authorization_for_turn(
                    pending_workspace_id.as_str(),
                    pending_thread_id.as_str(),
                    pending_turn_id.as_str(),
                    crate::authorization::ResourceAction::AgentRequestRespond,
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
                    true
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
                authorization_context_fingerprint.as_str(),
            )
            .await;
        }
    }

    async fn expire_native_permission_request_after_authority_loss(
        &self,
        request_id: &str,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        initiating_principal_id: &pioneer_protocol::PrincipalId,
        initiating_session_id: &pioneer_protocol::AuthSessionId,
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
                    && pending.authorization_context_fingerprint
                        == authorization_context_fingerprint
            });
            unchanged.then(|| requests.remove(request_id)).flatten()
        };
        let Some(pending) = pending else {
            return;
        };

        let notification_request = pending.request.clone();
        let _ = pending
            .respond_to
            .send(pioneer_tools::PermissionApprovalResolution::Expired);
        self.persist_native_interaction_terminal(
            request_id,
            StoredCliRuntimePendingRequestStatus::Expired,
            TurnPermissionApprovalResolution::Expired,
        )
        .await;
        self.send_native_permission_collaborator_notification(
            &notification_request,
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

    async fn persist_native_interaction_terminal(
        &self,
        request_id: &str,
        status: StoredCliRuntimePendingRequestStatus,
        resolution: TurnPermissionApprovalResolution,
    ) {
        let response_json = pioneer_crud::serialize_cli_runtime_json(&resolution).ok();
        let now = chrono::Utc::now().fixed_offset();
        if let Err(error) = self
            .crud_store
            .resolve_native_human_interaction_request(ResolveCliRuntimePendingRequest {
                request_id: request_id.to_owned(),
                status,
                response_json,
                updated_at: now,
                resolved_at: now,
            })
            .await
        {
            warn!(
                request_id,
                status = status.as_str(),
                error = %format!("{error:#}"),
                "failed to persist native human interaction terminal state"
            );
        }
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
                    public_permission_error(
                        None,
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorStage::Delivery,
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

    async fn send_native_permission_collaborator_notification<T: Serialize>(
        &self,
        request: &TurnPermissionApprovalRequest,
        method: &str,
        payload: &T,
    ) {
        let thread_ids = native_permission_notification_thread_ids(request);
        self.send_execution_collaborator_notification_for_threads(
            thread_ids.as_slice(),
            crate::authorization::ResourceAction::AgentRequestObserve,
            method,
            payload,
        )
        .await;
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

fn native_permission_notification_thread_ids(request: &TurnPermissionApprovalRequest) -> Vec<&str> {
    let mut thread_ids = vec![request.thread_id.as_str()];
    for visible_thread_id in &request.visible_thread_ids {
        if !thread_ids.contains(&visible_thread_id.as_str()) {
            thread_ids.push(visible_thread_id.as_str());
        }
    }
    thread_ids
}

fn durable_native_permission_request(
    record: &CliRuntimePendingRequestRecord,
) -> Option<TurnPermissionApprovalRequest> {
    if record.runtime_id != NATIVE_HUMAN_INTERACTION_RUNTIME_ID
        || record.request_kind != NATIVE_HUMAN_INTERACTION_REQUEST_KIND
        || record.turn_id.is_none()
    {
        return None;
    }
    let envelope = pioneer_crud::deserialize_cli_runtime_json::<CLIRuntimePendingRequest>(
        record.payload_json.as_str(),
    )
    .ok()?;
    if envelope.native_request_id.as_deref() != Some(record.request_id.as_str()) {
        return None;
    }
    let record_turn_id = record.turn_id.as_deref()?;
    let request =
        serde_json::from_value::<TurnPermissionApprovalRequest>(envelope.payload?).ok()?;
    (request.request_id == record.request_id
        && request.workspace_id == record.workspace_id
        && request.thread_id == record.thread_id
        && request.turn_id == record_turn_id)
        .then_some(request)
}

fn same_native_permission_request_contract(
    left: &TurnPermissionApprovalRequest,
    right: &TurnPermissionApprovalRequest,
) -> bool {
    left.request_id == right.request_id
        && left.workspace_id == right.workspace_id
        && left.thread_id == right.thread_id
        && left.turn_id == right.turn_id
        && left.tool_name == right.tool_name
        && left.action == right.action
        && left.scope_hash == right.scope_hash
        && left.reason == right.reason
        && left.summary == right.summary
        && left.details == right.details
}

fn durable_native_permission_resolution(
    record: &CliRuntimePendingRequestRecord,
) -> Option<TurnPermissionApprovalResolution> {
    let response_json = record.response_json.as_deref()?;
    pioneer_crud::deserialize_cli_runtime_json(response_json).ok()
}

fn durable_native_permission_identity(
    record: &CliRuntimePendingRequestRecord,
) -> Option<(
    String,
    String,
    String,
    pioneer_protocol::PrincipalId,
    pioneer_protocol::AuthSessionId,
    String,
)> {
    let binding = record.authorization_binding.as_ref()?;
    let request = durable_native_permission_request(record)?;
    Some((
        request.workspace_id,
        request.thread_id,
        request.turn_id,
        binding.initiating_principal_id.parse().ok()?,
        binding.initiating_session_id.parse().ok()?,
        binding.authorization_context_fingerprint.clone(),
    ))
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
