use pioneer_protocol::{
    AuthSessionId, AuthSessionTerminationReason, INVALID_PARAMS_CODE, INVALID_REQUEST_CODE,
    InvitationId, JsonRpcErrorResponse, JsonRpcResponse, MemberChangedNotification,
    MemberDeviceCreateParams, MemberDeviceCreateResponse, MemberListParams, MemberRemoveParams,
    MemberRestoreParams, MemberSuspendParams, PrincipalId, RequestId, WorkspaceId,
    WorkspaceMemberAddParams, WorkspaceMemberListParams, WorkspaceMemberRemoveParams,
    WorkspaceMembersChangedNotification, constants::events,
};
use std::collections::BTreeMap;
use tracing::warn;

use crate::auth::AuthErrorCode;
use crate::authorization::{
    AccessChangeKind, AuthorizationExternalError, AuthorizedMemberDirectory,
    AuthorizedMemberPrincipal, AuthorizedWorkspace, external_error_for_decision,
};
use crate::epic5_observability::{Epic5Operation, Epic5Outcome, record_latency, record_outcome};
use crate::member::{MemberService, MemberServiceError};
use crate::request_context::RequestContext;

use super::MessageProcessor;

impl MessageProcessor {
    pub(in crate::message) async fn member_suspend(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedMemberPrincipal,
        request_id: RequestId,
        params: MemberSuspendParams,
    ) {
        let started = std::time::Instant::now();
        let service = MemberService::with_rate_limits(
            (*self.crud_store).clone(),
            self.gateway_secrets.clone(),
            self.epic5_rate_limits.clone(),
        );
        let target_principal_id = params.principal_id.clone();
        match service
            .suspend(context.principal(), authorization, params)
            .await
        {
            Ok(committed) => {
                record_member_operation(
                    Epic5Operation::MemberSuspend,
                    if committed.response.changed {
                        Epic5Outcome::Success
                    } else {
                        Epic5Outcome::Noop
                    },
                    started,
                );
                tracing::debug!(
                    revoked_session_count = committed.revoked_session_ids.len(),
                    revoked_device_count = committed.revoked_device_ids.len(),
                    "committed Member suspension"
                );
                if committed.response.changed {
                    self.publish_member_lifecycle_change(
                        &target_principal_id,
                        &committed.affected_workspace_ids,
                        &[],
                    )
                    .await;
                    self.terminate_member_sessions_after_commit(
                        &target_principal_id,
                        committed.revoked_session_ids.clone(),
                        AuthSessionTerminationReason::PrincipalSuspended,
                        Epic5Operation::SessionTerminationSuspended,
                    )
                    .await;
                }
                self.send_member_result(context, request_id, &committed.response)
                    .await
            }
            Err(error) => {
                record_member_operation(
                    Epic5Operation::MemberSuspend,
                    member_error_outcome(&error),
                    started,
                );
                self.send_member_service_error(context, request_id, error)
                    .await
            }
        }
    }

    pub(in crate::message) async fn member_restore(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedMemberPrincipal,
        request_id: RequestId,
        params: MemberRestoreParams,
    ) {
        let started = std::time::Instant::now();
        let service = MemberService::with_rate_limits(
            (*self.crud_store).clone(),
            self.gateway_secrets.clone(),
            self.epic5_rate_limits.clone(),
        );
        let target_principal_id = params.principal_id.clone();
        match service
            .restore(context.principal(), authorization, params)
            .await
        {
            Ok(committed) => {
                record_member_operation(
                    Epic5Operation::MemberRestore,
                    if committed.response.changed {
                        Epic5Outcome::Success
                    } else {
                        Epic5Outcome::Noop
                    },
                    started,
                );
                if committed.response.changed {
                    self.publish_member_lifecycle_change(
                        &target_principal_id,
                        &committed.affected_workspace_ids,
                        &[],
                    )
                    .await;
                }
                self.send_member_result(context, request_id, &committed.response)
                    .await
            }
            Err(error) => {
                record_member_operation(
                    Epic5Operation::MemberRestore,
                    member_error_outcome(&error),
                    started,
                );
                self.send_member_service_error(context, request_id, error)
                    .await
            }
        }
    }

    pub(in crate::message) async fn member_remove(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedMemberPrincipal,
        request_id: RequestId,
        params: MemberRemoveParams,
    ) {
        let started = std::time::Instant::now();
        let service = MemberService::with_rate_limits(
            (*self.crud_store).clone(),
            self.gateway_secrets.clone(),
            self.epic5_rate_limits.clone(),
        );
        let target_principal_id = params.principal_id.clone();
        match service
            .remove(context.principal(), authorization, params)
            .await
        {
            Ok(committed) => {
                record_member_operation(
                    Epic5Operation::MemberRemove,
                    if committed.response.changed {
                        Epic5Outcome::Success
                    } else {
                        Epic5Outcome::Noop
                    },
                    started,
                );
                if committed.response.changed {
                    tracing::debug!(
                        revoked_session_count = committed.revoked_session_ids.len(),
                        revoked_device_count = committed.revoked_device_ids.len(),
                        removed_workspace_count = committed.removed_workspace_ids.len(),
                        removed_private_thread_count = committed.removed_private_thread_ids.len(),
                        changed_invitation_count = committed.changed_invitation_ids.len(),
                        "committed terminal Member removal"
                    );
                    self.publish_member_lifecycle_change(
                        &target_principal_id,
                        &committed.removed_workspace_ids,
                        &committed.changed_invitation_ids,
                    )
                    .await;
                    self.terminate_member_sessions_after_commit(
                        &target_principal_id,
                        committed.revoked_session_ids.clone(),
                        AuthSessionTerminationReason::PrincipalRemoved,
                        Epic5Operation::SessionTerminationRemoved,
                    )
                    .await;
                }
                self.send_member_result(context, request_id, &committed.response)
                    .await
            }
            Err(error) => {
                record_member_operation(
                    Epic5Operation::MemberRemove,
                    member_error_outcome(&error),
                    started,
                );
                self.send_member_service_error(context, request_id, error)
                    .await
            }
        }
    }

    pub(in crate::message) async fn member_device_create(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedMemberPrincipal,
        request_id: RequestId,
        params: MemberDeviceCreateParams,
    ) {
        if authorization.principal_id() != &context.principal().principal_id
            || authorization.action() != crate::authorization::ResourceAction::MemberDeviceCreate
            || authorization.target_principal_id() != &params.principal_id
        {
            self.send_error(
                context.connection_id(),
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        let Some(service) = self.auth_service.as_ref() else {
            self.send_error(
                context.connection_id(),
                AuthorizationExternalError::Unavailable.response(request_id),
            )
            .await;
            return;
        };
        match service
            .create_recovery_device(context.principal(), &params.principal_id)
            .await
        {
            Ok(activation) => {
                self.send_member_result(
                    context,
                    request_id,
                    &MemberDeviceCreateResponse {
                        principal_id: params.principal_id,
                        activation,
                    },
                )
                .await
            }
            Err(error) => {
                warn!(error = %error.code().as_str(), "member recovery device creation failed");
                self.send_error(
                    context.connection_id(),
                    recovery_device_error_response(request_id, error.code()),
                )
                .await;
            }
        }
    }

    pub(in crate::message) async fn member_list(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedMemberDirectory,
        request_id: RequestId,
        params: MemberListParams,
    ) {
        let service = MemberService::with_rate_limits(
            (*self.crud_store).clone(),
            self.gateway_secrets.clone(),
            self.epic5_rate_limits.clone(),
        );
        match service
            .list(context.principal(), authorization, params)
            .await
        {
            Ok(result) => self.send_member_result(context, request_id, &result).await,
            Err(error) => {
                self.send_member_service_error(context, request_id, error)
                    .await
            }
        }
    }

    pub(in crate::message) async fn workspace_member_list(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedWorkspace,
        request_id: RequestId,
        params: WorkspaceMemberListParams,
    ) {
        let service = MemberService::with_rate_limits(
            (*self.crud_store).clone(),
            self.gateway_secrets.clone(),
            self.epic5_rate_limits.clone(),
        );
        match service
            .workspace_list(context.principal(), authorization, params)
            .await
        {
            Ok(result) => self.send_member_result(context, request_id, &result).await,
            Err(error) => {
                self.send_member_service_error(context, request_id, error)
                    .await
            }
        }
    }

    pub(in crate::message) async fn workspace_member_add(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedWorkspace,
        request_id: RequestId,
        params: WorkspaceMemberAddParams,
    ) {
        let started = std::time::Instant::now();
        let service = MemberService::with_rate_limits(
            (*self.crud_store).clone(),
            self.gateway_secrets.clone(),
            self.epic5_rate_limits.clone(),
        );
        match service
            .workspace_add(context.principal(), authorization, params)
            .await
        {
            Ok(result) => {
                record_member_operation(
                    Epic5Operation::WorkspaceMemberAdd,
                    if result.changed {
                        Epic5Outcome::Success
                    } else {
                        Epic5Outcome::Noop
                    },
                    started,
                );
                if result.changed {
                    self.publish_workspace_membership_change(
                        result.workspace_id.clone(),
                        result.member.principal_id.clone(),
                    )
                    .await;
                }
                self.send_member_result(context, request_id, &result).await
            }
            Err(error) => {
                record_member_operation(
                    Epic5Operation::WorkspaceMemberAdd,
                    member_error_outcome(&error),
                    started,
                );
                self.send_member_service_error(context, request_id, error)
                    .await
            }
        }
    }

    pub(in crate::message) async fn workspace_member_remove(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedWorkspace,
        request_id: RequestId,
        params: WorkspaceMemberRemoveParams,
    ) {
        let started = std::time::Instant::now();
        let service = MemberService::with_rate_limits(
            (*self.crud_store).clone(),
            self.gateway_secrets.clone(),
            self.epic5_rate_limits.clone(),
        );
        match service
            .workspace_remove(context.principal(), authorization, params)
            .await
        {
            Ok(committed) => {
                record_member_operation(
                    Epic5Operation::WorkspaceMemberRemove,
                    if committed.response.changed {
                        Epic5Outcome::Success
                    } else {
                        Epic5Outcome::Noop
                    },
                    started,
                );
                if committed.response.changed {
                    tracing::debug!(
                        removed_private_thread_count = committed.removed_private_thread_ids.len(),
                        "publishing committed workspace membership removal"
                    );
                    self.publish_workspace_membership_change(
                        committed.response.workspace_id.clone(),
                        committed.response.member.principal_id.clone(),
                    )
                    .await;
                }
                self.send_member_result(context, request_id, &committed.response)
                    .await
            }
            Err(error) => {
                record_member_operation(
                    Epic5Operation::WorkspaceMemberRemove,
                    member_error_outcome(&error),
                    started,
                );
                self.send_member_service_error(context, request_id, error)
                    .await
            }
        }
    }

    async fn publish_workspace_membership_change(
        &self,
        workspace_id: WorkspaceId,
        target_principal_id: PrincipalId,
    ) {
        let signal = self
            .publish_committed_authorization_invalidation(
                AccessChangeKind::WorkspaceMembership,
                Some(target_principal_id.clone()),
                workspace_id.to_string(),
                None,
            )
            .await;
        self.send_notification_to_authorized_workspace_connections(
            workspace_id.as_str(),
            events::WORKSPACE_MEMBERS_CHANGED,
            &WorkspaceMembersChangedNotification {
                revision: signal.authorization_revision,
                workspace_id: workspace_id.clone(),
            },
        )
        .await;
        self.send_notification_to_authorized_member_connections(
            &target_principal_id,
            events::MEMBER_CHANGED,
            &MemberChangedNotification {
                revision: signal.authorization_revision,
                principal_id: target_principal_id.clone(),
            },
        )
        .await;
    }

    async fn publish_member_lifecycle_change(
        &self,
        target_principal_id: &PrincipalId,
        workspace_ids: &[WorkspaceId],
        invitation_ids: &[InvitationId],
    ) {
        let mut revision = if workspace_ids.is_empty() {
            self.authorization_invalidation_hub
                .advance_snapshot_revision()
        } else {
            self.authorization_invalidation_hub.current_revision()
        };
        for workspace_id in workspace_ids {
            revision = self
                .publish_committed_authorization_invalidation(
                    AccessChangeKind::WorkspaceMembership,
                    Some(target_principal_id.clone()),
                    workspace_id.to_string(),
                    None,
                )
                .await
                .authorization_revision;
            self.send_notification_to_authorized_workspace_connections(
                workspace_id.as_str(),
                events::WORKSPACE_MEMBERS_CHANGED,
                &WorkspaceMembersChangedNotification {
                    revision,
                    workspace_id: workspace_id.clone(),
                },
            )
            .await;
        }
        for invitation_id in invitation_ids {
            self.send_scoped_invitation_changed_notification(invitation_id, revision)
                .await;
        }
        self.send_notification_to_authorized_member_connections(
            target_principal_id,
            events::MEMBER_CHANGED,
            &MemberChangedNotification {
                revision,
                principal_id: target_principal_id.clone(),
            },
        )
        .await;
    }

    async fn terminate_member_sessions_after_commit(
        &self,
        target_principal_id: &PrincipalId,
        committed_session_ids: Vec<AuthSessionId>,
        reason: AuthSessionTerminationReason,
        metric_operation: Epic5Operation,
    ) {
        let mut session_ids = BTreeMap::new();
        for session_id in committed_session_ids {
            session_ids.insert(session_id.to_string(), session_id);
        }
        for session_id in self
            .session_manager
            .session_ids_for_principal(target_principal_id)
            .await
        {
            session_ids.insert(session_id.to_string(), session_id);
        }
        for session_id in session_ids.into_values() {
            self.session_manager
                .disconnect_session(&session_id, reason)
                .await;
            record_outcome(metric_operation, Epic5Outcome::Success);
        }
    }

    async fn send_member_result<T: serde::Serialize>(
        &self,
        context: &RequestContext,
        request_id: RequestId,
        result: &T,
    ) {
        match JsonRpcResponse::from_result(request_id.clone(), result) {
            Ok(response) => {
                if let Err(error) = self.send_json(context.connection_id(), &response).await {
                    warn!(error = %format!("{error:#}"), "failed to enqueue member response");
                }
            }
            Err(_) => {
                self.send_error(
                    context.connection_id(),
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
            }
        }
    }

    async fn send_member_service_error(
        &self,
        context: &RequestContext,
        request_id: RequestId,
        error: MemberServiceError,
    ) {
        let response = member_service_error_response(request_id, error);
        self.send_error(context.connection_id(), response).await;
    }
}

fn member_service_error_response(
    request_id: RequestId,
    error: MemberServiceError,
) -> JsonRpcErrorResponse {
    match error {
        MemberServiceError::InvalidParams => JsonRpcErrorResponse::new(
            Some(request_id),
            INVALID_PARAMS_CODE,
            "invalid member parameters",
        ),
        MemberServiceError::InvalidTarget => {
            let mut response = JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_PARAMS_CODE,
                "invalid member target",
            );
            response.error.data =
                serde_json::to_value(pioneer_protocol::MemberManagementErrorReason::InvalidTarget)
                    .ok();
            response
        }
        MemberServiceError::TargetUnavailable => {
            AuthorizationExternalError::Unavailable.response(request_id)
        }
        MemberServiceError::RateLimited => JsonRpcErrorResponse::new(
            Some(request_id),
            INVALID_REQUEST_CODE,
            "request rate limited",
        ),
        MemberServiceError::Conflict(reason) => {
            let mut response = JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_REQUEST_CODE,
                "member state conflict",
            );
            response.error.data = serde_json::to_value(reason).ok();
            response
        }
        MemberServiceError::Authorization(decision) => external_error_for_decision(&decision)
            .unwrap_or(AuthorizationExternalError::NotFound)
            .response(request_id),
        MemberServiceError::Unavailable(error) => {
            warn!(error = %format!("{error:#}"), "member service request failed");
            AuthorizationExternalError::Unavailable.response(request_id)
        }
    }
}

fn member_error_outcome(error: &MemberServiceError) -> Epic5Outcome {
    match error {
        MemberServiceError::InvalidParams => Epic5Outcome::Invalid,
        MemberServiceError::InvalidTarget => Epic5Outcome::Invalid,
        MemberServiceError::TargetUnavailable => Epic5Outcome::Unavailable,
        MemberServiceError::RateLimited => Epic5Outcome::RateLimited,
        MemberServiceError::Conflict(_) => Epic5Outcome::Conflict,
        MemberServiceError::Authorization(_) => Epic5Outcome::Denied,
        MemberServiceError::Unavailable(_) => Epic5Outcome::Unavailable,
    }
}

fn record_member_operation(
    operation: Epic5Operation,
    outcome: Epic5Outcome,
    started: std::time::Instant,
) {
    record_outcome(operation, outcome);
    record_latency(operation, started.elapsed());
}

fn recovery_device_error_response(
    request_id: RequestId,
    error: AuthErrorCode,
) -> JsonRpcErrorResponse {
    match error {
        AuthErrorCode::RecoveryRateLimited => JsonRpcErrorResponse::new(
            Some(request_id),
            INVALID_REQUEST_CODE,
            "request rate limited",
        ),
        AuthErrorCode::RecoveryInvalidTarget => {
            let mut response = JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_PARAMS_CODE,
                "invalid member target",
            );
            response.error.data =
                serde_json::to_value(pioneer_protocol::MemberManagementErrorReason::InvalidTarget)
                    .ok();
            response
        }
        _ => AuthorizationExternalError::Unavailable.response(request_id),
    }
}

#[cfg(test)]
mod tests {
    use pioneer_protocol::MemberManagementErrorReason;

    use super::*;

    fn request_id(value: u8) -> RequestId {
        RequestId::new(format!("R{value:020}")).expect("valid request id")
    }

    #[test]
    fn recovery_error_mapping_preserves_safe_management_classes() {
        let invalid_target =
            recovery_device_error_response(request_id(1), AuthErrorCode::RecoveryInvalidTarget);
        assert_eq!(invalid_target.error.code, INVALID_PARAMS_CODE);
        assert_eq!(
            invalid_target.error.data,
            serde_json::to_value(MemberManagementErrorReason::InvalidTarget).ok()
        );

        let rate_limited =
            recovery_device_error_response(request_id(2), AuthErrorCode::RecoveryRateLimited);
        assert_eq!(rate_limited.error.code, INVALID_REQUEST_CODE);

        let unavailable =
            recovery_device_error_response(request_id(3), AuthErrorCode::InvalidCredential);
        assert_ne!(unavailable.error.code, INVALID_PARAMS_CODE);
        assert_ne!(unavailable.error.code, INVALID_REQUEST_CODE);
    }

    #[test]
    fn direct_add_error_mapping_preserves_privacy_and_safe_invalid_target() {
        let unavailable =
            member_service_error_response(request_id(4), MemberServiceError::TargetUnavailable);
        assert_ne!(unavailable.error.code, INVALID_PARAMS_CODE);
        assert_ne!(unavailable.error.code, INVALID_REQUEST_CODE);
        assert_eq!(unavailable.error.message, "authorization_unavailable");
        assert_eq!(
            unavailable.error.data,
            Some(serde_json::json!({ "code": "authorization_unavailable" }))
        );

        let invalid_target =
            member_service_error_response(request_id(5), MemberServiceError::InvalidTarget);
        assert_eq!(invalid_target.error.code, INVALID_PARAMS_CODE);
        assert_eq!(
            invalid_target.error.data,
            serde_json::to_value(MemberManagementErrorReason::InvalidTarget).ok()
        );
    }
}
