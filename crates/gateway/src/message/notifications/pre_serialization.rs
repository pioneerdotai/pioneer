use super::*;
use crate::authorization::{
    ActionGateDecision, AuthorizationDecision, AuthorizationResolver, AuthorizationService,
    DenyReason, DisclosurePolicy, ProofResolution, ResourceAction,
    record_authorization_unavailable, record_thread_notification_decision,
};
use pioneer_crud::AUTHORIZATION_FACTS_BATCH_LIMIT;

impl MessageProcessor {
    /// One ordinary subscriber stage. Facts never escape this future or survive
    /// serialization, and every connection retains its own identity/lease gate.
    pub(super) async fn pre_serialization_thread_notification_recipients(
        &self,
        thread_id: &str,
        subscribers: Vec<crate::thread::ThreadSubscriber>,
    ) -> Vec<ConnectionId> {
        let thread_id = thread_id.trim();
        if thread_id.is_empty() || subscribers.is_empty() {
            return Vec::new();
        }
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        let scope = match resolver.load_thread_notification_scope(thread_id).await {
            Ok(scope) => scope,
            Err(_) => {
                record_authorization_unavailable(
                    ResourceAction::ThreadRead.safe_name(),
                    "thread",
                    "notification",
                );
                return Vec::new();
            }
        };
        let service = AuthorizationService::new();
        let mut recipients = Vec::new();
        for chunk in subscribers.chunks(AUTHORIZATION_FACTS_BATCH_LIMIT) {
            let mut candidates = Vec::with_capacity(chunk.len());
            let mut special = Vec::new();
            for subscriber in chunk {
                let Ok(principal) = self
                    .session_manager
                    .connection_principal(subscriber.connection_id)
                    .await
                else {
                    continue;
                };
                if principal.principal_id != subscriber.identity.principal_id
                    || principal.session_id != subscriber.identity.session_id
                {
                    record_inactive_identity();
                    continue;
                }
                // Internal lineage retains its current policy. Exact drafts
                // take precedence even if a persisted row appeared meanwhile.
                if scope.is_internal()
                    || self
                        .thread_manager
                        .authorize_runtime_draft(
                            subscriber.connection_id,
                            &subscriber.identity,
                            thread_id,
                            None,
                        )
                        .await
                        .is_some()
                {
                    special.push((subscriber.connection_id, Some(subscriber.identity.clone())));
                    continue;
                }
                if scope.is_missing() {
                    record_thread_notification_decision(
                        ResourceAction::ThreadRead,
                        &AuthorizationDecision::Deny {
                            reason: DenyReason::MissingAuthoritativeResource,
                            disclosure: DisclosurePolicy::NotFound,
                        },
                    );
                    continue;
                }
                candidates.push((subscriber.connection_id, principal));
            }
            recipients.extend(
                self.authorized_thread_notification_candidates(thread_id, special)
                    .await,
            );
            if candidates.is_empty() {
                continue;
            }
            let session_ids = candidates
                .iter()
                .map(|(_, principal)| principal.session_id.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let leases = if let Some(auth) = &self.auth_service {
                match auth
                    .load_notification_session_leases(
                        &session_ids,
                        &self.crud_store.database_connection(),
                    )
                    .await
                {
                    Ok(rows) => Some(rows),
                    Err(_) => {
                        record_authorization_unavailable(
                            ResourceAction::ThreadRead.safe_name(),
                            "session",
                            "notification",
                        );
                        continue;
                    }
                }
            } else {
                None
            };
            let mut gated = Vec::with_capacity(candidates.len());
            let mut principal_ids = std::collections::HashSet::new();
            for (connection_id, principal) in candidates {
                if let Some(auth) = &self.auth_service
                    && auth
                        .validate_notification_session_lease(
                            principal.as_ref(),
                            leases
                                .as_ref()
                                .and_then(|rows| rows.get(principal.session_id.as_str())),
                        )
                        .is_err()
                {
                    record_inactive_identity();
                    continue;
                }
                let gate = service.authorize_action(
                    principal.kind,
                    principal.role_key.as_ref(),
                    ResourceAction::ThreadRead,
                );
                if let ActionGateDecision::Deny { reason, disclosure } = &gate {
                    record_thread_notification_decision(
                        ResourceAction::ThreadRead,
                        &AuthorizationDecision::Deny {
                            reason: *reason,
                            disclosure: *disclosure,
                        },
                    );
                    continue;
                }
                if !gate.is_final_allow() {
                    principal_ids.insert(principal.principal_id.clone());
                }
                gated.push((connection_id, principal, gate));
            }
            if gated.is_empty() {
                continue;
            }
            let memberships = match resolver
                .load_thread_notification_memberships(
                    &scope,
                    &principal_ids.into_iter().collect::<Vec<_>>(),
                )
                .await
            {
                Ok(facts) => facts,
                Err(_) => {
                    record_authorization_unavailable(
                        ResourceAction::ThreadRead.safe_name(),
                        "thread",
                        "notification",
                    );
                    continue;
                }
            };
            for (connection_id, principal, gate) in gated {
                match resolver.authorize_thread_notification(
                    principal.as_ref(),
                    &gate,
                    &memberships,
                ) {
                    ProofResolution::Authorized(proof) => {
                        record_thread_notification_decision(
                            ResourceAction::ThreadRead,
                            proof.decision(),
                        );
                        recipients.push(connection_id);
                    }
                    ProofResolution::Denied(decision) => {
                        record_thread_notification_decision(ResourceAction::ThreadRead, &decision)
                    }
                }
            }
            // Leases/memberships and every bounded input set are dropped here.
        }
        recipients
    }
}

fn record_inactive_identity() {
    record_thread_notification_decision(
        ResourceAction::ThreadRead,
        &AuthorizationDecision::Deny {
            reason: DenyReason::InactivePrincipal,
            disclosure: DisclosurePolicy::AuthenticationTerminal,
        },
    );
}
