use async_trait::async_trait;
use pioneer_protocol::{
    MemberChangedNotification, WorkspaceMembersChangedNotification, constants::events,
};

use crate::auth::{InvitationAcceptCommitted, InvitationAcceptPostCommitHook};
use crate::authorization::AccessChangeKind;

use super::MessageProcessor;

#[async_trait]
impl InvitationAcceptPostCommitHook for MessageProcessor {
    async fn invitation_accepted(&self, committed: InvitationAcceptCommitted) {
        tracing::debug!(
            invitation_id = %committed.invitation_id,
            inviter_principal_id = %committed.inviter_principal_id,
            accepted_principal_id = %committed.accepted_principal_id,
            "publishing committed invitation acceptance lifecycle"
        );
        let role_change = self
            .authorization_invalidation_hub
            .publish_change(
                pioneer_protocol::AuthorizationChangeKind::RoleAssignment,
                pioneer_protocol::AuthorizationChangeScope::Principal {
                    principal_id: committed.accepted_principal_id.clone(),
                },
            )
            .await
            .expect("invitation acceptance must publish durable role assignment");
        self.send_notification_to_authorized_member_connections(
            &committed.accepted_principal_id,
            events::AUTHORIZATION_PROJECTION_CHANGED,
            &role_change,
        )
        .await;
        for workspace_id in &committed.workspace_ids {
            let revision = self
                .publish_committed_authorization_invalidation(
                    AccessChangeKind::WorkspaceMembership,
                    Some(committed.accepted_principal_id.clone()),
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
        let revision = self
            .publish_invitation_selector_change(&committed.invitation_id)
            .await
            .policy_generation
            .get();
        self.send_scoped_invitation_changed_notification(&committed.invitation_id, revision)
            .await;
        self.send_notification_to_authorized_member_connections(
            &committed.accepted_principal_id,
            events::MEMBER_CHANGED,
            &MemberChangedNotification {
                revision,
                principal_id: committed.accepted_principal_id.clone(),
            },
        )
        .await;
    }

    async fn invitation_changed(&self, invitation_id: pioneer_protocol::InvitationId) {
        let revision = self
            .publish_invitation_selector_change(&invitation_id)
            .await
            .policy_generation
            .get();
        self.send_scoped_invitation_changed_notification(&invitation_id, revision)
            .await;
    }
}
