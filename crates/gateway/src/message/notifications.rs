use super::*;

impl MessageProcessor {
    pub(super) async fn send_principal_owned_notification<T: Serialize>(
        &self,
        owner_principal_id: &str,
        method: &str,
        payload: &T,
    ) {
        self.send_personal_notification(
            Some(owner_principal_id),
            crate::authorization::ResourceAction::MemoryRead,
            method,
            payload,
        )
        .await;
    }

    pub(super) async fn send_task_user_notification<T: Serialize>(
        &self,
        workspace_id: &str,
        recipient_principal_id: &str,
        method: &str,
        payload: &T,
    ) -> usize {
        let candidate_connection_ids = self.session_manager.connection_ids().await;
        let initially_authorized_connection_ids = self
            .authorized_task_user_notification_recipients(
                workspace_id,
                recipient_principal_id,
                candidate_connection_ids,
            )
            .await;
        if initially_authorized_connection_ids.is_empty() {
            return 0;
        }
        let serialization_connection_ids = self
            .authorized_task_user_notification_recipients(
                workspace_id,
                recipient_principal_id,
                initially_authorized_connection_ids,
            )
            .await;
        if serialization_connection_ids.is_empty() {
            return 0;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return 0;
        };
        let connection_ids = self
            .authorized_task_user_notification_recipients(
                workspace_id,
                recipient_principal_id,
                serialization_connection_ids,
            )
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await
    }

    async fn authorized_task_user_notification_recipients(
        &self,
        workspace_id: &str,
        recipient_principal_id: &str,
        candidate_connection_ids: Vec<ConnectionId>,
    ) -> Vec<ConnectionId> {
        use crate::authorization::{
            ActionGateDecision, AuthorizationDecision, AuthorizationResolver, AuthorizationService,
            DenyReason, DisclosurePolicy, ProofResolution, ResourceAction,
            record_authorization_unavailable, record_workspace_notification_decision,
        };

        let workspace_id = workspace_id.trim();
        let recipient_principal_id = recipient_principal_id.trim();
        if workspace_id.is_empty()
            || recipient_principal_id.is_empty()
            || candidate_connection_ids.is_empty()
        {
            return Vec::new();
        }

        let action = ResourceAction::NotificationReadOwn;
        let service = AuthorizationService::new();
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        let mut recipients = Vec::with_capacity(candidate_connection_ids.len());
        for connection_id in candidate_connection_ids {
            let Ok(principal) = self
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };

            // This channel is principal-owned, including for absolute roles.
            // Superuser visibility into workspace resources must never turn an
            // exact-recipient task result into an administrative broadcast.
            if principal.principal_id.as_str() != recipient_principal_id {
                continue;
            }
            if let Some(auth_service) = self.auth_service.as_ref()
                && auth_service
                    .validate_session_lease(principal.as_ref())
                    .await
                    .is_err()
            {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::InactivePrincipal,
                    disclosure: DisclosurePolicy::AuthenticationTerminal,
                };
                record_workspace_notification_decision(action, &decision);
                continue;
            }

            let action_gate =
                service.authorize_action(principal.kind, principal.role_key.as_ref(), action);
            if let ActionGateDecision::Deny { reason, disclosure } = &action_gate {
                let decision = AuthorizationDecision::Deny {
                    reason: *reason,
                    disclosure: *disclosure,
                };
                record_workspace_notification_decision(action, &decision);
                continue;
            }

            match resolver
                .authorize_workspace(principal.as_ref(), &action_gate, action, workspace_id)
                .await
            {
                Ok(ProofResolution::Authorized(proof)) => {
                    record_workspace_notification_decision(action, proof.decision());
                    recipients.push(connection_id);
                }
                Ok(ProofResolution::Denied(decision)) => {
                    record_workspace_notification_decision(action, &decision);
                }
                Err(error) => {
                    record_authorization_unavailable(
                        action.safe_name(),
                        "workspace",
                        "task_user_notification",
                    );
                    warn!(
                        connection_id,
                        authorization_action = action.safe_name(),
                        authorization_resource_kind = "workspace",
                        error = %format!("{error:#}"),
                        "task user notification authorization unavailable"
                    );
                }
            }
        }
        recipients
    }

    pub(super) async fn send_superuser_personal_notification<T: Serialize>(
        &self,
        method: &str,
        payload: &T,
    ) {
        self.send_personal_notification(
            None,
            crate::authorization::ResourceAction::MemoryRead,
            method,
            payload,
        )
        .await;
    }

    async fn send_personal_notification<T: Serialize>(
        &self,
        owner_principal_id: Option<&str>,
        action: crate::authorization::ResourceAction,
        method: &str,
        payload: &T,
    ) -> usize {
        let candidate_connection_ids = self.session_manager.connection_ids().await;
        let initially_authorized_connection_ids = self
            .authorized_personal_notification_recipients(
                owner_principal_id,
                action,
                candidate_connection_ids,
            )
            .await;
        if initially_authorized_connection_ids.is_empty() {
            return 0;
        }
        let serialization_connection_ids = self
            .authorized_personal_notification_recipients(
                owner_principal_id,
                action,
                initially_authorized_connection_ids,
            )
            .await;
        if serialization_connection_ids.is_empty() {
            return 0;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return 0;
        };
        let connection_ids = self
            .authorized_personal_notification_recipients(
                owner_principal_id,
                action,
                serialization_connection_ids,
            )
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await
    }

    async fn authorized_personal_notification_recipients(
        &self,
        owner_principal_id: Option<&str>,
        action: crate::authorization::ResourceAction,
        candidate_connection_ids: Vec<ConnectionId>,
    ) -> Vec<ConnectionId> {
        use crate::authorization::{ActionGateDecision, AuthorizationService};

        let owner_principal_id = owner_principal_id
            .map(str::trim)
            .filter(|principal_id| !principal_id.is_empty());
        if candidate_connection_ids.is_empty() {
            return Vec::new();
        }

        let service = AuthorizationService::new();
        let mut recipients = Vec::with_capacity(candidate_connection_ids.len());
        for connection_id in candidate_connection_ids {
            let Ok(principal) = self
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };
            if let Some(auth_service) = self.auth_service.as_ref()
                && auth_service
                    .validate_session_lease(principal.as_ref())
                    .await
                    .is_err()
            {
                continue;
            }

            match service.authorize_action(principal.kind, principal.role_key.as_ref(), action) {
                ActionGateDecision::AllowAbsolute => recipients.push(connection_id),
                ActionGateDecision::RequireResource { .. }
                    if owner_principal_id
                        .is_some_and(|owner| principal.principal_id.as_str() == owner) =>
                {
                    recipients.push(connection_id);
                }
                ActionGateDecision::RequireResource { .. } | ActionGateDecision::Deny { .. } => {}
            }
        }
        recipients
    }

    pub(super) async fn send_execution_collaborator_notification<T: Serialize>(
        &self,
        thread_id: &str,
        action: crate::authorization::ResourceAction,
        method: &str,
        payload: &T,
    ) {
        let candidate_connection_ids = self.session_manager.connection_ids().await;
        self.send_execution_scoped_notification(
            thread_id,
            action,
            method,
            payload,
            candidate_connection_ids,
        )
        .await;
    }

    pub(super) async fn send_execution_collaborator_notification_to_connections<T: Serialize>(
        &self,
        thread_id: &str,
        action: crate::authorization::ResourceAction,
        method: &str,
        payload: &T,
        candidate_connection_ids: Vec<ConnectionId>,
    ) {
        self.send_execution_scoped_notification(
            thread_id,
            action,
            method,
            payload,
            candidate_connection_ids,
        )
        .await;
    }

    async fn send_execution_scoped_notification<T: Serialize>(
        &self,
        thread_id: &str,
        action: crate::authorization::ResourceAction,
        method: &str,
        payload: &T,
        candidate_connection_ids: Vec<ConnectionId>,
    ) {
        let initially_authorized_connection_ids = self
            .authorized_execution_collaborator_notification_recipients(
                thread_id,
                action,
                candidate_connection_ids,
            )
            .await;
        if initially_authorized_connection_ids.is_empty() {
            return;
        }
        let serialization_connection_ids = self
            .authorized_execution_collaborator_notification_recipients(
                thread_id,
                action,
                initially_authorized_connection_ids,
            )
            .await;
        if serialization_connection_ids.is_empty() {
            return;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return;
        };
        let connection_ids = self
            .authorized_execution_collaborator_notification_recipients(
                thread_id,
                action,
                serialization_connection_ids,
            )
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await;
    }

    async fn authorized_execution_collaborator_notification_recipients(
        &self,
        thread_id: &str,
        action: crate::authorization::ResourceAction,
        candidate_connection_ids: Vec<ConnectionId>,
    ) -> Vec<ConnectionId> {
        use crate::authorization::{
            ActionGateDecision, AuthorizationDecision, AuthorizationResolver, AuthorizationService,
            DenyReason, DisclosurePolicy, ProofResolution, record_authorization_unavailable,
            record_thread_notification_decision,
        };

        let thread_id = thread_id.trim();
        if thread_id.is_empty() || candidate_connection_ids.is_empty() {
            return Vec::new();
        }

        let service = AuthorizationService::new();
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        let mut recipients = Vec::with_capacity(candidate_connection_ids.len());
        for connection_id in candidate_connection_ids {
            let Ok(principal) = self
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };
            if let Some(auth_service) = self.auth_service.as_ref()
                && auth_service
                    .validate_session_lease(principal.as_ref())
                    .await
                    .is_err()
            {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::InactivePrincipal,
                    disclosure: DisclosurePolicy::AuthenticationTerminal,
                };
                record_thread_notification_decision(action, &decision);
                continue;
            }
            let action_gate =
                service.authorize_action(principal.kind, principal.role_key.as_ref(), action);
            if action_gate == ActionGateDecision::AllowAbsolute {
                recipients.push(connection_id);
                continue;
            }
            if let ActionGateDecision::Deny { reason, disclosure } = &action_gate {
                let decision = AuthorizationDecision::Deny {
                    reason: *reason,
                    disclosure: *disclosure,
                };
                record_thread_notification_decision(action, &decision);
                continue;
            }
            let mut resolution = match resolver
                .authorize_thread(principal.as_ref(), &action_gate, action, thread_id, None)
                .await
            {
                Ok(resolution) => resolution,
                Err(error) => {
                    record_authorization_unavailable(
                        action.safe_name(),
                        "thread",
                        "execution_notification",
                    );
                    warn!(
                        connection_id,
                        authorization_action = action.safe_name(),
                        authorization_resource_kind = "thread",
                        error = %format!("{error:#}"),
                        "execution notification authorization unavailable"
                    );
                    continue;
                }
            };
            if matches!(
                resolution.denial(),
                Some(AuthorizationDecision::Deny {
                    reason: DenyReason::MissingAuthoritativeResource,
                    ..
                })
            ) {
                resolution = match resolver
                    .authorize_internal_thread_via_root(
                        principal.as_ref(),
                        &action_gate,
                        action,
                        thread_id,
                        None,
                    )
                    .await
                {
                    Ok(resolution) => resolution,
                    Err(error) => {
                        record_authorization_unavailable(
                            action.safe_name(),
                            "thread_lineage",
                            "execution_notification",
                        );
                        warn!(
                            connection_id,
                            authorization_action = action.safe_name(),
                            authorization_resource_kind = "thread_lineage",
                            error = %format!("{error:#}"),
                            "execution notification lineage authorization unavailable"
                        );
                        continue;
                    }
                };
            }
            match resolution {
                ProofResolution::Authorized(proof) => {
                    record_thread_notification_decision(action, proof.decision());
                    recipients.push(connection_id);
                }
                ProofResolution::Denied(decision) => {
                    record_thread_notification_decision(action, &decision);
                }
            }
        }
        recipients
    }

    pub(super) async fn send_gateway_management_notification<T: Serialize>(
        &self,
        method: &str,
        payload: &T,
    ) {
        let candidate_connection_ids = self.session_manager.connection_ids().await;
        let initially_authorized_connection_ids = self
            .authorized_gateway_management_notification_recipients(candidate_connection_ids)
            .await;
        if initially_authorized_connection_ids.is_empty() {
            return;
        }
        let serialization_connection_ids = self
            .authorized_gateway_management_notification_recipients(
                initially_authorized_connection_ids,
            )
            .await;
        if serialization_connection_ids.is_empty() {
            return;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return;
        };
        let connection_ids = self
            .authorized_gateway_management_notification_recipients(serialization_connection_ids)
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await;
    }

    pub(super) async fn send_scoped_invitation_changed_notification(
        &self,
        invitation_id: &pioneer_protocol::InvitationId,
        revision: u64,
    ) {
        let Some(invitation) =
            pioneer_crud::load_invitation(&self.crud_store.database_connection(), invitation_id)
                .await
                .ok()
                .flatten()
        else {
            tracing::warn!(
                invitation_id = %invitation_id,
                "committed invitation notification could not reload authoritative owner"
            );
            return;
        };
        let Ok(inviter_principal_id) =
            pioneer_protocol::PrincipalId::new(invitation.created_by_principal_id)
        else {
            tracing::warn!(
                invitation_id = %invitation_id,
                "committed invitation notification has invalid persisted owner"
            );
            return;
        };
        let candidates = self.session_manager.connection_ids().await;
        let initially_authorized = self
            .authorized_invitation_notification_recipients(&inviter_principal_id, candidates)
            .await;
        if initially_authorized.is_empty() {
            return;
        }
        let serialization_authorized = self
            .authorized_invitation_notification_recipients(
                &inviter_principal_id,
                initially_authorized,
            )
            .await;
        if serialization_authorized.is_empty() {
            return;
        }
        let Some(serialized) = self.serialize_notification(
            events::INVITATION_CHANGED,
            &pioneer_protocol::InvitationChangedNotification {
                revision,
                invitation_id: invitation_id.clone(),
            },
        ) else {
            return;
        };
        let authorized = self
            .authorized_invitation_notification_recipients(
                &inviter_principal_id,
                serialization_authorized,
            )
            .await;
        self.send_serialized_notification_to_connections(
            events::INVITATION_CHANGED,
            &serialized,
            authorized,
        )
        .await;
    }

    pub(super) async fn send_scoped_invitation_authorization_changed_notification(
        &self,
        invitation_id: &pioneer_protocol::InvitationId,
        change: &pioneer_protocol::AuthorizationProjectionChangedNotification,
    ) {
        let Some(invitation) =
            pioneer_crud::load_invitation(&self.crud_store.database_connection(), invitation_id)
                .await
                .ok()
                .flatten()
        else {
            tracing::warn!(
                invitation_id = %invitation_id,
                "committed invitation authorization notification could not reload authoritative owner"
            );
            return;
        };
        let Ok(inviter_principal_id) =
            pioneer_protocol::PrincipalId::new(invitation.created_by_principal_id)
        else {
            return;
        };
        let candidates = self.session_manager.connection_ids().await;
        let initially_authorized = self
            .authorized_invitation_notification_recipients(&inviter_principal_id, candidates)
            .await;
        if initially_authorized.is_empty() {
            return;
        }
        let serialization_authorized = self
            .authorized_invitation_notification_recipients(
                &inviter_principal_id,
                initially_authorized,
            )
            .await;
        if serialization_authorized.is_empty() {
            return;
        }
        let Some(serialized) =
            self.serialize_notification(events::AUTHORIZATION_PROJECTION_CHANGED, change)
        else {
            return;
        };
        let authorized = self
            .authorized_invitation_notification_recipients(
                &inviter_principal_id,
                serialization_authorized,
            )
            .await;
        self.send_serialized_notification_to_connections(
            events::AUTHORIZATION_PROJECTION_CHANGED,
            &serialized,
            authorized,
        )
        .await;
    }

    pub(super) async fn authorized_invitation_notification_recipients(
        &self,
        inviter_principal_id: &pioneer_protocol::PrincipalId,
        candidate_connection_ids: Vec<ConnectionId>,
    ) -> Vec<ConnectionId> {
        use crate::authorization::{ActionGateDecision, AuthorizationService, ResourceAction};

        let service = AuthorizationService::new();
        let mut recipients = Vec::with_capacity(candidate_connection_ids.len());
        for connection_id in candidate_connection_ids {
            let Ok(principal) = self
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };
            if let Some(auth_service) = self.auth_service.as_ref()
                && auth_service
                    .validate_session_lease(principal.as_ref())
                    .await
                    .is_err()
            {
                continue;
            }
            match service.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::InvitationList,
            ) {
                ActionGateDecision::AllowAbsolute => recipients.push(connection_id),
                ActionGateDecision::RequireResource { .. }
                    if &principal.principal_id == inviter_principal_id =>
                {
                    recipients.push(connection_id);
                }
                ActionGateDecision::Deny { .. } | ActionGateDecision::RequireResource { .. } => {}
            }
        }
        recipients
    }

    async fn authorized_gateway_management_notification_recipients(
        &self,
        candidate_connection_ids: Vec<ConnectionId>,
    ) -> Vec<ConnectionId> {
        use crate::authorization::{ActionGateDecision, AuthorizationService, ResourceAction};

        let service = AuthorizationService::new();
        let mut recipients = Vec::with_capacity(candidate_connection_ids.len());
        for connection_id in candidate_connection_ids {
            let Ok(principal) = self
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };
            if let Some(auth_service) = self.auth_service.as_ref()
                && auth_service
                    .validate_session_lease(principal.as_ref())
                    .await
                    .is_err()
            {
                continue;
            }
            if service.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::GatewayManage,
            ) == ActionGateDecision::AllowAbsolute
            {
                recipients.push(connection_id);
            }
        }
        recipients
    }

    pub(super) async fn send_notification_to_workspace_connections<T: Serialize>(
        &self,
        workspace_id: &str,
        method: &str,
        payload: &T,
    ) {
        let candidate_connection_ids = self
            .session_manager
            .connection_ids_for_workspace(workspace_id)
            .await;
        let connection_ids = self
            .authorized_workspace_notification_recipients(workspace_id, candidate_connection_ids)
            .await;
        self.send_notification_to_reauthorized_workspace_connections(
            workspace_id,
            method,
            payload,
            connection_ids,
        )
        .await;
    }

    pub(super) async fn send_notification_to_authorized_workspace_connections<T: Serialize>(
        &self,
        workspace_id: &str,
        method: &str,
        payload: &T,
    ) {
        let candidate_connection_ids = self.session_manager.connection_ids().await;
        let connection_ids = self
            .authorized_workspace_notification_recipients(workspace_id, candidate_connection_ids)
            .await;
        self.send_notification_to_reauthorized_workspace_connections(
            workspace_id,
            method,
            payload,
            connection_ids,
        )
        .await;
    }

    pub(super) async fn send_notification_to_authorized_member_connections<T: Serialize>(
        &self,
        target_principal_id: &pioneer_protocol::PrincipalId,
        method: &str,
        payload: &T,
    ) {
        let candidates = self.session_manager.connection_ids().await;
        let initially_authorized = self
            .authorized_member_notification_recipients(target_principal_id, candidates)
            .await;
        if initially_authorized.is_empty() {
            return;
        }
        let serialization_authorized = self
            .authorized_member_notification_recipients(target_principal_id, initially_authorized)
            .await;
        if serialization_authorized.is_empty() {
            return;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return;
        };
        let authorized = self
            .authorized_member_notification_recipients(
                target_principal_id,
                serialization_authorized,
            )
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, authorized)
            .await;
    }

    async fn authorized_member_notification_recipients(
        &self,
        target_principal_id: &pioneer_protocol::PrincipalId,
        candidate_connection_ids: Vec<ConnectionId>,
    ) -> Vec<ConnectionId> {
        use crate::authorization::{
            ActionGateDecision, AuthorizationResolver, AuthorizationService, ProofResolution,
            ResourceAction, record_authorization_unavailable,
        };

        let service = AuthorizationService::new();
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        let database = self.crud_store.database_connection();
        let mut recipients = Vec::with_capacity(candidate_connection_ids.len());
        for connection_id in candidate_connection_ids {
            let Ok(principal) = self
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };
            if let Some(auth_service) = self.auth_service.as_ref()
                && auth_service
                    .validate_session_lease(principal.as_ref())
                    .await
                    .is_err()
            {
                continue;
            }
            let action = ResourceAction::MemberAvatarRead;
            let action_gate =
                service.authorize_action(principal.kind, principal.role_key.as_ref(), action);
            if matches!(action_gate, ActionGateDecision::Deny { .. }) {
                continue;
            }
            match resolver
                .authorize_member_avatar(
                    &database,
                    principal.as_ref(),
                    &action_gate,
                    target_principal_id,
                )
                .await
            {
                Ok(ProofResolution::Authorized(_)) => recipients.push(connection_id),
                Ok(ProofResolution::Denied(_)) => {}
                Err(error) => {
                    record_authorization_unavailable(
                        action.safe_name(),
                        "directory_principal",
                        "notification",
                    );
                    warn!(
                        connection_id,
                        error = %format!("{error:#}"),
                        "member notification authorization unavailable"
                    );
                }
            }
        }
        recipients
    }

    pub(super) async fn send_notification_to_reauthorized_workspace_connections<T: Serialize>(
        &self,
        workspace_id: &str,
        method: &str,
        payload: &T,
        initially_authorized_connection_ids: Vec<ConnectionId>,
    ) {
        if initially_authorized_connection_ids.is_empty() {
            return;
        }
        let serialization_connection_ids = self
            .authorized_workspace_notification_recipients(
                workspace_id,
                initially_authorized_connection_ids,
            )
            .await;
        if serialization_connection_ids.is_empty() {
            return;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return;
        };
        let connection_ids = self
            .authorized_workspace_notification_recipients(
                workspace_id,
                serialization_connection_ids,
            )
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await;
    }

    pub(super) async fn authorized_workspace_notification_recipients(
        &self,
        workspace_id: &str,
        candidate_connection_ids: Vec<ConnectionId>,
    ) -> Vec<ConnectionId> {
        use crate::authorization::{
            ActionGateDecision, AuthorizationDecision, AuthorizationResolver, AuthorizationService,
            DenyReason, DisclosurePolicy, ProofResolution, ResourceAction,
            record_authorization_unavailable, record_workspace_notification_decision,
        };

        let workspace_id = workspace_id.trim();
        if workspace_id.is_empty() || candidate_connection_ids.is_empty() {
            return Vec::new();
        }

        let service = AuthorizationService::new();
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        let mut recipients = Vec::with_capacity(candidate_connection_ids.len());

        for connection_id in candidate_connection_ids {
            let Ok(principal) = self
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };

            if let Some(auth_service) = self.auth_service.as_ref()
                && auth_service
                    .validate_session_lease(principal.as_ref())
                    .await
                    .is_err()
            {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::InactivePrincipal,
                    disclosure: DisclosurePolicy::AuthenticationTerminal,
                };
                record_workspace_notification_decision(ResourceAction::WorkspaceRead, &decision);
                continue;
            }

            let action_gate = service.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::WorkspaceRead,
            );
            if let ActionGateDecision::Deny { reason, disclosure } = &action_gate {
                let decision = AuthorizationDecision::Deny {
                    reason: *reason,
                    disclosure: *disclosure,
                };
                record_workspace_notification_decision(ResourceAction::WorkspaceRead, &decision);
                continue;
            }

            match resolver
                .authorize_workspace(
                    principal.as_ref(),
                    &action_gate,
                    ResourceAction::WorkspaceRead,
                    workspace_id,
                )
                .await
            {
                Ok(ProofResolution::Authorized(proof)) => {
                    record_workspace_notification_decision(
                        ResourceAction::WorkspaceRead,
                        proof.decision(),
                    );
                    recipients.push(connection_id);
                }
                Ok(ProofResolution::Denied(decision)) => {
                    record_workspace_notification_decision(
                        ResourceAction::WorkspaceRead,
                        &decision,
                    );
                }
                Err(error) => {
                    record_authorization_unavailable(
                        ResourceAction::WorkspaceRead.safe_name(),
                        "workspace",
                        "notification",
                    );
                    warn!(
                        connection_id,
                        authorization_action = ResourceAction::WorkspaceRead.safe_name(),
                        authorization_resource_kind = "workspace",
                        error = %format!("{error:#}"),
                        "workspace notification authorization unavailable"
                    );
                }
            }
        }

        recipients
    }

    pub(crate) async fn send_notification_to_thread_subscribers<T: Serialize>(
        &self,
        thread_id: &str,
        method: &str,
        payload: &T,
    ) {
        let subscribers = self.thread_manager.subscribed_connections(thread_id).await;
        self.send_notification_to_authorized_thread_subscribers(
            thread_id,
            method,
            payload,
            subscribers,
        )
        .await;
    }

    pub(crate) async fn send_notification_to_authorized_thread_connections<T: Serialize>(
        &self,
        thread_id: &str,
        method: &str,
        payload: &T,
        candidate_connection_ids: Vec<ConnectionId>,
    ) {
        let subscribers = self
            .thread_manager
            .subscribed_connections_for_candidates(thread_id, candidate_connection_ids)
            .await;
        self.send_notification_to_authorized_thread_subscribers(
            thread_id,
            method,
            payload,
            subscribers,
        )
        .await;
    }

    pub(crate) async fn send_notification_to_authorized_thread_subscribers<T: Serialize>(
        &self,
        thread_id: &str,
        method: &str,
        payload: &T,
        subscribers: Vec<crate::thread::ThreadSubscriber>,
    ) {
        let connection_ids = self
            .authorized_thread_notification_recipients(thread_id, subscribers)
            .await;
        self.send_notification_to_reauthorized_thread_connections(
            thread_id,
            method,
            payload,
            connection_ids,
        )
        .await;
    }

    pub(crate) async fn send_notification_to_removed_thread_subscribers<T: Serialize>(
        &self,
        thread_id: &str,
        method: &str,
        payload: &T,
        subscribers: Vec<crate::thread::ThreadSubscriber>,
    ) {
        let initially_authorized_connection_ids = self
            .authorized_thread_notification_recipients(thread_id, subscribers.clone())
            .await;
        let serialization_subscribers =
            retain_thread_subscribers(subscribers, &initially_authorized_connection_ids);
        let serialization_connection_ids = self
            .authorized_thread_notification_recipients(thread_id, serialization_subscribers.clone())
            .await;
        if serialization_connection_ids.is_empty() {
            return;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return;
        };
        let final_subscribers =
            retain_thread_subscribers(serialization_subscribers, &serialization_connection_ids);
        let connection_ids = self
            .authorized_thread_notification_recipients(thread_id, final_subscribers)
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await;
    }

    pub(crate) async fn send_notification_to_removed_runtime_draft_owner<T: Serialize>(
        &self,
        access: &crate::thread::RuntimeDraftAccess,
        method: &str,
        payload: &T,
        subscribers: Vec<crate::thread::ThreadSubscriber>,
    ) {
        let initially_authorized = self
            .authorized_removed_runtime_draft_recipients(access, subscribers.clone())
            .await;
        let serialization_subscribers =
            retain_thread_subscribers(subscribers, &initially_authorized);
        let serialization_authorized = self
            .authorized_removed_runtime_draft_recipients(access, serialization_subscribers.clone())
            .await;
        if serialization_authorized.is_empty() {
            return;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return;
        };
        let final_subscribers =
            retain_thread_subscribers(serialization_subscribers, &serialization_authorized);
        let connection_ids = self
            .authorized_removed_runtime_draft_recipients(access, final_subscribers)
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await;
    }

    async fn authorized_removed_runtime_draft_recipients(
        &self,
        access: &crate::thread::RuntimeDraftAccess,
        subscribers: Vec<crate::thread::ThreadSubscriber>,
    ) -> Vec<ConnectionId> {
        use crate::authorization::{
            AuthorizationResolver, AuthorizationService, ProofResolution, ResourceAction,
            record_authorization_unavailable, record_thread_notification_decision,
        };

        let owner = access.owner();
        let service = AuthorizationService::new();
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        let mut recipients = Vec::with_capacity(1);
        for subscriber in subscribers {
            if &subscriber != owner {
                continue;
            }
            let Ok(principal) = self
                .session_manager
                .connection_principal(subscriber.connection_id)
                .await
            else {
                continue;
            };
            if principal.principal_id != owner.identity.principal_id
                || principal.session_id != owner.identity.session_id
            {
                continue;
            }
            if let Some(auth_service) = self.auth_service.as_ref()
                && auth_service
                    .validate_session_lease(principal.as_ref())
                    .await
                    .is_err()
            {
                continue;
            }

            let action = ResourceAction::ThreadRead;
            let gate =
                service.authorize_action(principal.kind, principal.role_key.as_ref(), action);
            match resolver
                .authorize_runtime_draft(principal.as_ref(), &gate, action, access)
                .await
            {
                Ok(ProofResolution::Authorized(proof)) => {
                    record_thread_notification_decision(action, proof.decision());
                    recipients.push(subscriber.connection_id);
                }
                Ok(ProofResolution::Denied(decision)) => {
                    record_thread_notification_decision(action, &decision);
                }
                Err(error) => {
                    record_authorization_unavailable(
                        action.safe_name(),
                        "runtime_draft",
                        "notification",
                    );
                    warn!(
                        connection_id = subscriber.connection_id,
                        error = %format!("{error:#}"),
                        "removed runtime draft notification authorization unavailable"
                    );
                }
            }
        }
        recipients
    }

    pub(super) async fn send_notification_to_reauthorized_thread_connections<T: Serialize>(
        &self,
        thread_id: &str,
        method: &str,
        payload: &T,
        initially_authorized_connection_ids: Vec<ConnectionId>,
    ) {
        if initially_authorized_connection_ids.is_empty() {
            return;
        }
        let serialization_subscribers = self
            .thread_manager
            .subscribed_connections_for_candidates(thread_id, initially_authorized_connection_ids)
            .await;
        let serialization_connection_ids = self
            .authorized_thread_notification_recipients(thread_id, serialization_subscribers)
            .await;
        if serialization_connection_ids.is_empty() {
            return;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return;
        };
        let subscribers = self
            .thread_manager
            .subscribed_connections_for_candidates(thread_id, serialization_connection_ids)
            .await;
        let connection_ids = self
            .authorized_thread_notification_recipients(thread_id, subscribers)
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await;
    }

    pub(super) async fn send_thread_scoped_notification_to_connections<T: Serialize>(
        &self,
        thread_id: &str,
        method: &str,
        payload: &T,
        candidate_connection_ids: Vec<ConnectionId>,
    ) {
        let initially_authorized_connection_ids = self
            .authorized_thread_connection_recipients(thread_id, candidate_connection_ids)
            .await;
        if initially_authorized_connection_ids.is_empty() {
            return;
        }
        let serialization_connection_ids = self
            .authorized_thread_connection_recipients(thread_id, initially_authorized_connection_ids)
            .await;
        if serialization_connection_ids.is_empty() {
            return;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return;
        };
        let connection_ids = self
            .authorized_thread_connection_recipients(thread_id, serialization_connection_ids)
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await;
    }

    pub(super) async fn authorized_thread_notification_recipients(
        &self,
        thread_id: &str,
        subscribers: Vec<crate::thread::ThreadSubscriber>,
    ) -> Vec<ConnectionId> {
        let candidates = subscribers
            .into_iter()
            .map(|subscriber| (subscriber.connection_id, Some(subscriber.identity)))
            .collect();
        self.authorized_thread_notification_candidates(thread_id, candidates)
            .await
    }

    async fn authorized_thread_connection_recipients(
        &self,
        thread_id: &str,
        connection_ids: Vec<ConnectionId>,
    ) -> Vec<ConnectionId> {
        let candidates = connection_ids
            .into_iter()
            .map(|connection_id| (connection_id, None))
            .collect();
        self.authorized_thread_notification_candidates(thread_id, candidates)
            .await
    }

    async fn authorized_thread_notification_candidates(
        &self,
        thread_id: &str,
        candidates: Vec<(
            ConnectionId,
            Option<crate::thread::ThreadSubscriptionIdentity>,
        )>,
    ) -> Vec<ConnectionId> {
        use crate::authorization::{
            ActionGateDecision, AuthorizationDecision, AuthorizationResolver, AuthorizationService,
            DenyReason, DisclosurePolicy, ProofResolution, ResourceAction,
            record_authorization_unavailable, record_thread_notification_decision,
        };

        let thread_id = thread_id.trim();
        if thread_id.is_empty() || candidates.is_empty() {
            return Vec::new();
        }

        let service = AuthorizationService::new();
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        let mut recipients = Vec::with_capacity(candidates.len());

        for (connection_id, expected_identity) in candidates {
            let Ok(principal) = self
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };
            if expected_identity.as_ref().is_some_and(|identity| {
                principal.principal_id != identity.principal_id
                    || principal.session_id != identity.session_id
            }) {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::InactivePrincipal,
                    disclosure: DisclosurePolicy::AuthenticationTerminal,
                };
                record_thread_notification_decision(ResourceAction::ThreadRead, &decision);
                continue;
            }

            if let Some(auth_service) = self.auth_service.as_ref()
                && auth_service
                    .validate_session_lease(principal.as_ref())
                    .await
                    .is_err()
            {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::InactivePrincipal,
                    disclosure: DisclosurePolicy::AuthenticationTerminal,
                };
                record_thread_notification_decision(ResourceAction::ThreadRead, &decision);
                continue;
            }

            let action_gate = service.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::ThreadRead,
            );
            if let ActionGateDecision::Deny { reason, disclosure } = &action_gate {
                let decision = AuthorizationDecision::Deny {
                    reason: *reason,
                    disclosure: *disclosure,
                };
                record_thread_notification_decision(ResourceAction::ThreadRead, &decision);
                continue;
            }

            let identity = crate::thread::ThreadSubscriptionIdentity::new(
                principal.principal_id.clone(),
                principal.session_id.clone(),
            );
            if let Some(draft) = self
                .thread_manager
                .authorize_runtime_draft(connection_id, &identity, thread_id, None)
                .await
            {
                let action = ResourceAction::ThreadRead;
                let gate =
                    service.authorize_action(principal.kind, principal.role_key.as_ref(), action);
                match resolver
                    .authorize_runtime_draft(principal.as_ref(), &gate, action, &draft)
                    .await
                {
                    Ok(ProofResolution::Authorized(proof)) => {
                        record_thread_notification_decision(action, proof.decision());
                        recipients.push(connection_id);
                    }
                    Ok(ProofResolution::Denied(decision)) => {
                        record_thread_notification_decision(action, &decision);
                    }
                    Err(error) => {
                        record_authorization_unavailable(
                            action.safe_name(),
                            "runtime_draft",
                            "notification",
                        );
                        warn!(
                            connection_id,
                            error = %format!("{error:#}"),
                            "runtime draft notification authorization unavailable"
                        );
                    }
                }
                continue;
            }

            let mut resolution = match resolver
                .authorize_thread(
                    principal.as_ref(),
                    &action_gate,
                    ResourceAction::ThreadRead,
                    thread_id,
                    None,
                )
                .await
            {
                Ok(resolution) => resolution,
                Err(error) => {
                    record_authorization_unavailable(
                        ResourceAction::ThreadRead.safe_name(),
                        "thread",
                        "notification",
                    );
                    warn!(
                        connection_id,
                        authorization_action = ResourceAction::ThreadRead.safe_name(),
                        authorization_resource_kind = "thread",
                        error = %format!("{error:#}"),
                        "thread notification authorization unavailable"
                    );
                    continue;
                }
            };
            if matches!(
                resolution.denial(),
                Some(AuthorizationDecision::Deny {
                    reason: DenyReason::MissingAuthoritativeResource,
                    ..
                })
            ) {
                resolution = match resolver
                    .authorize_internal_thread_via_root(
                        principal.as_ref(),
                        &action_gate,
                        ResourceAction::ThreadRead,
                        thread_id,
                        None,
                    )
                    .await
                {
                    Ok(resolution) => resolution,
                    Err(error) => {
                        record_authorization_unavailable(
                            ResourceAction::ThreadRead.safe_name(),
                            "thread_lineage",
                            "notification",
                        );
                        warn!(
                            connection_id,
                            authorization_action = ResourceAction::ThreadRead.safe_name(),
                            authorization_resource_kind = "thread_lineage",
                            error = %format!("{error:#}"),
                            "thread notification lineage authorization unavailable"
                        );
                        continue;
                    }
                };
            }
            match resolution {
                ProofResolution::Authorized(proof) => {
                    record_thread_notification_decision(
                        ResourceAction::ThreadRead,
                        proof.decision(),
                    );
                    recipients.push(connection_id);
                }
                ProofResolution::Denied(decision) => {
                    record_thread_notification_decision(ResourceAction::ThreadRead, &decision);
                }
            }
        }

        recipients
    }

    pub(crate) async fn send_notification_to_task_workspace_connections<T: Serialize>(
        &self,
        task_id: &str,
        workspace_id: &str,
        method: &str,
        payload: &T,
    ) {
        let candidate_connection_ids = self
            .session_manager
            .connection_ids_for_workspace(workspace_id)
            .await;
        let connection_ids = self
            .authorized_task_notification_recipients(
                task_id,
                workspace_id,
                candidate_connection_ids,
            )
            .await;
        self.send_notification_to_reauthorized_task_connections(
            task_id,
            workspace_id,
            method,
            payload,
            connection_ids,
        )
        .await;
    }

    pub(super) async fn send_notification_to_reauthorized_task_connections<T: Serialize>(
        &self,
        task_id: &str,
        workspace_id: &str,
        method: &str,
        payload: &T,
        initially_authorized_connection_ids: Vec<ConnectionId>,
    ) {
        if initially_authorized_connection_ids.is_empty() {
            return;
        }
        let serialization_connection_ids = self
            .authorized_task_notification_recipients(
                task_id,
                workspace_id,
                initially_authorized_connection_ids,
            )
            .await;
        if serialization_connection_ids.is_empty() {
            return;
        }
        let Some(serialized) = self.serialize_notification(method, payload) else {
            return;
        };
        let connection_ids = self
            .authorized_task_notification_recipients(
                task_id,
                workspace_id,
                serialization_connection_ids,
            )
            .await;
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await;
    }

    pub(super) async fn authorized_task_notification_recipients(
        &self,
        task_id: &str,
        workspace_id: &str,
        candidate_connection_ids: Vec<ConnectionId>,
    ) -> Vec<ConnectionId> {
        use crate::authorization::{
            ActionGateDecision, AuthorizationDecision, AuthorizationResolver, AuthorizationService,
            DenyReason, DisclosurePolicy, ProofResolution, ResourceAction,
            record_authorization_unavailable, record_task_notification_decision,
        };

        let task_id = task_id.trim();
        let workspace_id = workspace_id.trim();
        if task_id.is_empty() || workspace_id.is_empty() || candidate_connection_ids.is_empty() {
            return Vec::new();
        }

        let service = AuthorizationService::new();
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        let mut recipients = Vec::with_capacity(candidate_connection_ids.len());
        for connection_id in candidate_connection_ids {
            let Ok(principal) = self
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };
            if let Some(auth_service) = self.auth_service.as_ref()
                && auth_service
                    .validate_session_lease(principal.as_ref())
                    .await
                    .is_err()
            {
                let decision = AuthorizationDecision::Deny {
                    reason: DenyReason::InactivePrincipal,
                    disclosure: DisclosurePolicy::AuthenticationTerminal,
                };
                record_task_notification_decision(ResourceAction::TaskRead, &decision);
                continue;
            }
            let action_gate = service.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::TaskRead,
            );
            if let ActionGateDecision::Deny { reason, disclosure } = &action_gate {
                let decision = AuthorizationDecision::Deny {
                    reason: *reason,
                    disclosure: *disclosure,
                };
                record_task_notification_decision(ResourceAction::TaskRead, &decision);
                continue;
            }
            match resolver
                .authorize_task(
                    principal.as_ref(),
                    &action_gate,
                    ResourceAction::TaskRead,
                    task_id,
                    Some(workspace_id),
                    None,
                )
                .await
            {
                Ok(ProofResolution::Authorized(proof)) => {
                    record_task_notification_decision(ResourceAction::TaskRead, proof.decision());
                    recipients.push(connection_id);
                }
                Ok(ProofResolution::Denied(decision)) => {
                    record_task_notification_decision(ResourceAction::TaskRead, &decision);
                }
                Err(error) => {
                    record_authorization_unavailable(
                        ResourceAction::TaskRead.safe_name(),
                        "task",
                        "notification",
                    );
                    warn!(
                        connection_id,
                        authorization_action = ResourceAction::TaskRead.safe_name(),
                        authorization_resource_kind = "task",
                        error = %format!("{error:#}"),
                        "task notification authorization unavailable"
                    );
                }
            }
        }
        recipients
    }

    pub(super) async fn send_notification_to_connections<T: Serialize>(
        &self,
        method: &str,
        payload: &T,
        connection_ids: Vec<ConnectionId>,
    ) {
        if connection_ids.is_empty() {
            return;
        }

        let Some(serialized) = self.serialize_notification(method, payload) else {
            return;
        };
        self.send_serialized_notification_to_connections(method, &serialized, connection_ids)
            .await;
    }

    fn serialize_notification<T: Serialize>(&self, method: &str, payload: &T) -> Option<String> {
        let notification = match JsonRpcNotification::from_params(method, payload) {
            Ok(notification) => notification,
            Err(error) => {
                crate::epic5_observability::record_outcome(
                    crate::epic5_observability::Epic5Operation::Notification,
                    crate::epic5_observability::Epic5Outcome::Unavailable,
                );
                error!(method, error = %error, "failed to encode notification");
                return None;
            }
        };

        match serde_json::to_string(&notification) {
            Ok(payload) => Some(payload),
            Err(error) => {
                crate::epic5_observability::record_outcome(
                    crate::epic5_observability::Epic5Operation::Notification,
                    crate::epic5_observability::Epic5Outcome::Unavailable,
                );
                error!(method, error = %error, "failed to serialize notification");
                None
            }
        }
    }

    async fn send_serialized_notification_to_connections(
        &self,
        method: &str,
        serialized: &str,
        connection_ids: Vec<ConnectionId>,
    ) -> usize {
        let mut accepted = 0usize;
        for target_connection_id in connection_ids {
            if let Err(error) = self
                .session_manager
                .try_send_notification_text(target_connection_id, serialized.to_owned())
                .await
            {
                crate::epic5_observability::record_outcome(
                    crate::epic5_observability::Epic5Operation::Notification,
                    crate::epic5_observability::Epic5Outcome::Unavailable,
                );
                warn!(
                    connection_id = target_connection_id,
                    method,
                    error = %format!("{error:#}"),
                    "failed to send notification"
                );
            } else {
                accepted = accepted.saturating_add(1);
            }
        }
        accepted
    }

    pub(super) async fn send_error(
        &self,
        connection_id: ConnectionId,
        response: JsonRpcErrorResponse,
    ) {
        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send JSON-RPC error response"
            );
        }
    }

    pub(super) async fn send_json<T: Serialize>(
        &self,
        connection_id: ConnectionId,
        value: &T,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(value)?;
        self.session_manager.send_text(connection_id, payload).await
    }
}

fn retain_thread_subscribers(
    subscribers: Vec<crate::thread::ThreadSubscriber>,
    candidate_connection_ids: &[ConnectionId],
) -> Vec<crate::thread::ThreadSubscriber> {
    let candidate_connection_ids = candidate_connection_ids
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    subscribers
        .into_iter()
        .filter(|subscriber| candidate_connection_ids.contains(&subscriber.connection_id))
        .collect()
}
