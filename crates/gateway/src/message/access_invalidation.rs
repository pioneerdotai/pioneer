use super::MessageProcessor;
use crate::auth::AuthenticatedSessionPrincipal;
use crate::authorization::{
    AccessChangeKind, AccessChangeSignal, ActionGateDecision, AuthorizationResolver,
    AuthorizationService, ProofResolution, ResourceAction, record_subscription_evictions,
};
use crate::session::ConnectionId;
use anyhow::anyhow;
use pioneer_protocol::{
    AccessChangedNotification, AuthSessionTerminationReason, constants::events,
};
use std::collections::HashSet;
use tracing::warn;

impl MessageProcessor {
    /// Applies a protected-content-free invalidation only after its durable ACL
    /// mutation has committed. Every affected live-state holder is removed
    /// before the safe client signal is queued.
    pub(crate) async fn apply_committed_authorization_invalidation(
        &self,
        signal: &AccessChangeSignal,
    ) {
        self.expire_native_permission_requests_without_current_authority(
            signal.workspace_id.as_str(),
            signal.affected_principal_id.as_ref(),
        )
        .await;
        self.expire_cli_runtime_pending_requests_without_current_authority(
            signal.workspace_id.as_str(),
            signal.affected_principal_id.as_ref(),
        )
        .await;

        let thread_subscribers = match signal.thread_id.as_deref() {
            Some(thread_id) => self
                .thread_manager
                .subscribed_connection_ids(thread_id)
                .await
                .into_iter()
                .collect::<HashSet<_>>(),
            None => HashSet::new(),
        };
        let mut notification_recipients = Vec::new();

        for connection_id in self.session_manager.connection_ids().await {
            let Ok(principal) = self
                .session_manager
                .connection_principal(connection_id)
                .await
            else {
                continue;
            };
            if signal
                .affected_principal_id
                .as_ref()
                .is_some_and(|affected| affected != &principal.principal_id)
            {
                continue;
            }

            let selected_affected_workspace = self
                .session_manager
                .connection_workspace_id(connection_id)
                .await
                .as_deref()
                == Some(signal.workspace_id.as_str());
            let had_thread_subscription = thread_subscribers.contains(&connection_id);
            let workspace_access = match self
                .current_workspace_access(principal.as_ref(), signal.workspace_id.as_str())
                .await
            {
                Ok(allowed) => allowed,
                Err(error) => {
                    if signal.affected_principal_id.is_some()
                        || selected_affected_workspace
                        || had_thread_subscription
                    {
                        self.close_session_after_eviction_failure(
                            connection_id,
                            principal.as_ref(),
                            signal,
                            error,
                        )
                        .await;
                    }
                    continue;
                }
            };
            let thread_access = match signal.thread_id.as_deref() {
                Some(thread_id) => match self
                    .current_thread_access(principal.as_ref(), thread_id)
                    .await
                {
                    Ok(allowed) => Some(allowed),
                    Err(error) => {
                        if signal.affected_principal_id.is_some()
                            || workspace_access
                            || had_thread_subscription
                        {
                            self.close_session_after_eviction_failure(
                                connection_id,
                                principal.as_ref(),
                                signal,
                                error,
                            )
                            .await;
                        }
                        continue;
                    }
                },
                None => None,
            };

            let workspace_access_lost =
                signal.kind == AccessChangeKind::WorkspaceMembership && !workspace_access;
            let thread_access_lost = matches!(
                signal.kind,
                AccessChangeKind::ThreadVisibility | AccessChangeKind::ThreadParticipantRemoved
            ) && thread_access == Some(false);

            if workspace_access_lost || thread_access_lost {
                let eviction_thread_id = thread_access_lost
                    .then_some(signal.thread_id.as_deref())
                    .flatten();
                let evicted_subscriptions = self
                    .thread_manager
                    .evict_connection_scope(
                        connection_id,
                        signal.workspace_id.as_str(),
                        eviction_thread_id,
                    )
                    .await;
                record_subscription_evictions(evicted_subscriptions.len());
                self.artifact_uploads
                    .abort_connection_scope(
                        connection_id,
                        signal.workspace_id.as_str(),
                        eviction_thread_id,
                    )
                    .await;
                self.artifact_downloads
                    .abort_connection_scope(
                        connection_id,
                        signal.workspace_id.as_str(),
                        eviction_thread_id,
                    )
                    .await;

                let removed_voice_sessions = match self.voice_sessions.cleanup_connection_scope(
                    connection_id,
                    signal.workspace_id.as_str(),
                    eviction_thread_id,
                ) {
                    Ok(sessions) => sessions,
                    Err(error) => {
                        self.close_session_after_eviction_failure(
                            connection_id,
                            principal.as_ref(),
                            signal,
                            anyhow!("failed to evict voice sessions: {error}"),
                        )
                        .await;
                        continue;
                    }
                };
                for session in removed_voice_sessions {
                    // A buffer may already have been consumed during
                    // finalization. Once the authoritative voice session is
                    // removed, an absent buffer cannot preserve authority.
                    let _ = self
                        .voice_session_buffers
                        .remove_session(session.session_id.as_str());
                }

                if workspace_access_lost && selected_affected_workspace {
                    self.session_manager
                        .set_connection_workspace(connection_id, None)
                        .await;
                }
            }

            if signal.affected_principal_id.is_some()
                || selected_affected_workspace
                || had_thread_subscription
                || workspace_access
            {
                notification_recipients.push(connection_id);
            }
        }

        self.send_notification_to_connections(
            events::ACCESS_CHANGED,
            &AccessChangedNotification {
                authorization_revision: signal.authorization_revision,
                workspace_id: signal.workspace_id.clone(),
                thread_id: signal.thread_id.clone(),
                change: signal.kind,
            },
            notification_recipients,
        )
        .await;
    }

    async fn current_workspace_access(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        workspace_id: &str,
    ) -> anyhow::Result<bool> {
        if let Some(auth_service) = self.auth_service.as_ref() {
            auth_service.validate_session_lease(principal).await?;
        }
        let action = ResourceAction::WorkspaceRead;
        let action_gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            action,
        );
        if matches!(action_gate, ActionGateDecision::Deny { .. }) {
            return Ok(false);
        }
        Ok(matches!(
            AuthorizationResolver::new((*self.crud_store).clone())
                .authorize_workspace(principal, &action_gate, action, workspace_id)
                .await?,
            ProofResolution::Authorized(_)
        ))
    }

    async fn current_thread_access(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        thread_id: &str,
    ) -> anyhow::Result<bool> {
        if let Some(auth_service) = self.auth_service.as_ref() {
            auth_service.validate_session_lease(principal).await?;
        }
        let action = ResourceAction::ThreadRead;
        let action_gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            action,
        );
        if matches!(action_gate, ActionGateDecision::Deny { .. }) {
            return Ok(false);
        }
        Ok(matches!(
            AuthorizationResolver::new((*self.crud_store).clone())
                .authorize_thread(principal, &action_gate, action, thread_id, None)
                .await?,
            ProofResolution::Authorized(_)
        ))
    }

    async fn close_session_after_eviction_failure(
        &self,
        connection_id: ConnectionId,
        principal: &AuthenticatedSessionPrincipal,
        signal: &AccessChangeSignal,
        error: anyhow::Error,
    ) {
        crate::epic5_observability::record_outcome(
            crate::epic5_observability::Epic5Operation::Invalidation,
            crate::epic5_observability::Epic5Outcome::Unavailable,
        );
        warn!(
            connection_id,
            principal_id = %principal.principal_id,
            auth_session_id = %principal.session_id,
            authorization_revision = signal.authorization_revision,
            access_change = signal.kind.as_str(),
            error = %format!("{error:#}"),
            "authorization runtime eviction failed; closing session fail-safe"
        );
        self.session_manager
            .disconnect_session(
                &principal.session_id,
                AuthSessionTerminationReason::SessionRevoked,
            )
            .await;
    }
}
