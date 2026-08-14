use anyhow::{Context, Result};
use pioneer_entity::audit_event;
use pioneer_protocol::{
    AUDIT_METADATA_VERSION_V1, AuditAction, AuditEventDomain, AuditEventId, AuditTargetKind,
    AuthSessionId, BoundedServerGeneratedMetadata, GatewayId, PolicyGeneration, PrincipalId,
    RoleKey, WorkspaceId,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ActiveModelTrait, DatabaseTransaction, Set};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAdministrativeAuditEvent {
    pub id: AuditEventId,
    pub gateway_id: GatewayId,
    pub actor_principal_id: Option<PrincipalId>,
    pub actor_session_id: Option<AuthSessionId>,
    pub action: AuditAction,
    pub target_kind: AuditTargetKind,
    pub target_id: String,
    pub workspace_id: Option<WorkspaceId>,
    pub metadata: BoundedServerGeneratedMetadata,
    pub policy_generation: PolicyGeneration,
    pub policy_role_key: Option<RoleKey>,
    pub policy_fingerprint: String,
    pub created_at: DateTimeWithTimeZone,
}

pub async fn insert_administrative_audit_event(
    transaction: &DatabaseTransaction,
    event: NewAdministrativeAuditEvent,
) -> Result<audit_event::Model> {
    let metadata_json = event
        .metadata
        .to_json_string()
        .context("failed to serialize bounded administrative audit metadata")?;
    audit_event::ActiveModel {
        id: Set(event.id.to_string()),
        gateway_id: Set(event.gateway_id.to_string()),
        actor_principal_id: Set(event.actor_principal_id.map(|id| id.to_string())),
        actor_session_id: Set(event.actor_session_id.map(|id| id.to_string())),
        action: Set(audit_action_to_db(event.action).to_owned()),
        domain: Set(audit_domain_to_db(AuditEventDomain::Administration).to_owned()),
        target_kind: Set(audit_target_kind_to_db(event.target_kind).to_owned()),
        target_id: Set(event.target_id),
        workspace_id: Set(event.workspace_id.map(|id| id.to_string())),
        metadata_version: Set(i64::from(AUDIT_METADATA_VERSION_V1)),
        metadata_json: Set(metadata_json),
        policy_generation: Set(i64::try_from(event.policy_generation.get())
            .context("administrative audit policy generation exceeds SQLite INTEGER")?),
        policy_role_key: Set(event.policy_role_key.map(|role| role.to_string())),
        policy_fingerprint: Set(event.policy_fingerprint),
        created_at: Set(event.created_at),
    }
    .insert(transaction)
    .await
    .context("failed to append administrative audit event")
}

pub const fn audit_domain_to_db(domain: AuditEventDomain) -> &'static str {
    match domain {
        AuditEventDomain::Administration => "administration",
    }
}

pub const fn audit_action_to_db(action: AuditAction) -> &'static str {
    match action {
        AuditAction::InvitationCreated => "invitation_created",
        AuditAction::InvitationRevoked => "invitation_revoked",
        AuditAction::InvitationExpired => "invitation_expired",
        AuditAction::InvitationAccepted => "invitation_accepted",
        AuditAction::WorkspaceMemberAdded => "workspace_member_added",
        AuditAction::WorkspaceMemberRemoved => "workspace_member_removed",
        AuditAction::MemberSuspended => "member_suspended",
        AuditAction::MemberRestored => "member_restored",
        AuditAction::MemberRemoved => "member_removed",
        AuditAction::MemberRecoveryDeviceCreated => "member_recovery_device_created",
    }
}

pub const fn audit_target_kind_to_db(kind: AuditTargetKind) -> &'static str {
    match kind {
        AuditTargetKind::Invitation => "invitation",
        AuditTargetKind::Principal => "principal",
        AuditTargetKind::WorkspaceMembership => "workspace_membership",
        AuditTargetKind::DeviceSession => "device_session",
    }
}
