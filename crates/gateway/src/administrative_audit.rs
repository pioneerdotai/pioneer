use anyhow::{Context, Result};
use pioneer_protocol::{
    ADMINISTRATION_DOMAIN_ID_LEN, AuditAction, AuditEventId, AuditTargetKind, AuthSessionId,
    BoundedServerGeneratedMetadata, DeviceId, GatewayId, InvitationId, PrincipalId, RoleKey,
    WorkspaceId, generate_id,
};
use sea_orm::{DatabaseTransaction, entity::prelude::DateTimeWithTimeZone};

/// The only Gateway writer for durable Epic 5 administrative audit events.
///
/// Callers can provide only typed server-owned identifiers. Action, target
/// kind, metadata shape/version and the 16 KiB bound are selected here rather
/// than accepted as client-controlled JSON.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct AdministrativeAuditWriter;

struct AdministrativeAuditRecord {
    action: AuditAction,
    target_kind: AuditTargetKind,
    target_id: String,
    workspace_id: Option<WorkspaceId>,
    metadata: BoundedServerGeneratedMetadata,
}

impl AdministrativeAuditWriter {
    pub(crate) async fn invitation_created(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor_principal_id: &PrincipalId,
        actor_session_id: &AuthSessionId,
        invitation_id: &InvitationId,
        workspace_ids: Vec<WorkspaceId>,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        let metadata = BoundedServerGeneratedMetadata::invitation_workspace_grants(workspace_ids)
            .context("failed to construct invitation-created audit metadata")?;
        self.append(
            transaction,
            gateway_id,
            Some((actor_principal_id, actor_session_id)),
            AdministrativeAuditRecord {
                action: AuditAction::InvitationCreated,
                target_kind: AuditTargetKind::Invitation,
                target_id: invitation_id.to_string(),
                workspace_id: None,
                metadata,
            },
            now,
        )
        .await
    }

    pub(crate) async fn invitation_revoked(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor_principal_id: &PrincipalId,
        actor_session_id: &AuthSessionId,
        invitation_id: &InvitationId,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        self.invitation_terminal(
            transaction,
            gateway_id,
            Some((actor_principal_id, actor_session_id)),
            invitation_id,
            AuditAction::InvitationRevoked,
            now,
        )
        .await
    }

    pub(crate) async fn invitation_revoked_by_system(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        invitation_id: &InvitationId,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        self.invitation_terminal(
            transaction,
            gateway_id,
            None,
            invitation_id,
            AuditAction::InvitationRevoked,
            now,
        )
        .await
    }

    pub(crate) async fn invitation_expired(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        invitation_id: &InvitationId,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        self.invitation_terminal(
            transaction,
            gateway_id,
            None,
            invitation_id,
            AuditAction::InvitationExpired,
            now,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invitation_accepted(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        invitation_id: &InvitationId,
        principal_id: &PrincipalId,
        device_id: &DeviceId,
        session_id: &AuthSessionId,
        workspace_ids: Vec<WorkspaceId>,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        let metadata = BoundedServerGeneratedMetadata::invitation_acceptance(
            principal_id.clone(),
            device_id.clone(),
            session_id.clone(),
            workspace_ids,
        )
        .context("failed to construct invitation-accepted audit metadata")?;
        self.append(
            transaction,
            gateway_id,
            Some((principal_id, session_id)),
            AdministrativeAuditRecord {
                action: AuditAction::InvitationAccepted,
                target_kind: AuditTargetKind::Invitation,
                target_id: invitation_id.to_string(),
                workspace_id: None,
                metadata,
            },
            now,
        )
        .await
    }

    pub(crate) async fn workspace_member_added(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor_principal_id: &PrincipalId,
        actor_session_id: &AuthSessionId,
        target_principal_id: &PrincipalId,
        workspace_id: &WorkspaceId,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        self.workspace_membership(
            transaction,
            gateway_id,
            actor_principal_id,
            actor_session_id,
            target_principal_id,
            workspace_id,
            AuditAction::WorkspaceMemberAdded,
            now,
        )
        .await
    }

    pub(crate) async fn workspace_member_removed(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor_principal_id: &PrincipalId,
        actor_session_id: &AuthSessionId,
        target_principal_id: &PrincipalId,
        workspace_id: &WorkspaceId,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        self.workspace_membership(
            transaction,
            gateway_id,
            actor_principal_id,
            actor_session_id,
            target_principal_id,
            workspace_id,
            AuditAction::WorkspaceMemberRemoved,
            now,
        )
        .await
    }

    pub(crate) async fn member_suspended(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor_principal_id: &PrincipalId,
        actor_session_id: &AuthSessionId,
        target_principal_id: &PrincipalId,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        self.member_lifecycle(
            transaction,
            gateway_id,
            actor_principal_id,
            actor_session_id,
            target_principal_id,
            AuditAction::MemberSuspended,
            now,
        )
        .await
    }

    pub(crate) async fn member_restored(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor_principal_id: &PrincipalId,
        actor_session_id: &AuthSessionId,
        target_principal_id: &PrincipalId,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        self.member_lifecycle(
            transaction,
            gateway_id,
            actor_principal_id,
            actor_session_id,
            target_principal_id,
            AuditAction::MemberRestored,
            now,
        )
        .await
    }

    pub(crate) async fn member_removed(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor_principal_id: &PrincipalId,
        actor_session_id: &AuthSessionId,
        target_principal_id: &PrincipalId,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        self.member_lifecycle(
            transaction,
            gateway_id,
            actor_principal_id,
            actor_session_id,
            target_principal_id,
            AuditAction::MemberRemoved,
            now,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn member_recovery_device_created(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor_principal_id: &PrincipalId,
        actor_session_id: &AuthSessionId,
        target_principal_id: &PrincipalId,
        device_id: &DeviceId,
        session_id: &AuthSessionId,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        self.append(
            transaction,
            gateway_id,
            Some((actor_principal_id, actor_session_id)),
            AdministrativeAuditRecord {
                action: AuditAction::MemberRecoveryDeviceCreated,
                target_kind: AuditTargetKind::DeviceSession,
                target_id: device_id.to_string(),
                workspace_id: None,
                metadata: BoundedServerGeneratedMetadata::recovery_device(
                    target_principal_id.clone(),
                    device_id.clone(),
                    session_id.clone(),
                ),
            },
            now,
        )
        .await
    }

    async fn invitation_terminal(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor: Option<(&PrincipalId, &AuthSessionId)>,
        invitation_id: &InvitationId,
        action: AuditAction,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        debug_assert!(matches!(
            action,
            AuditAction::InvitationRevoked | AuditAction::InvitationExpired
        ));
        self.append(
            transaction,
            gateway_id,
            actor,
            AdministrativeAuditRecord {
                action,
                target_kind: AuditTargetKind::Invitation,
                target_id: invitation_id.to_string(),
                workspace_id: None,
                metadata: BoundedServerGeneratedMetadata::empty(),
            },
            now,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn workspace_membership(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor_principal_id: &PrincipalId,
        actor_session_id: &AuthSessionId,
        target_principal_id: &PrincipalId,
        workspace_id: &WorkspaceId,
        action: AuditAction,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        debug_assert!(matches!(
            action,
            AuditAction::WorkspaceMemberAdded | AuditAction::WorkspaceMemberRemoved
        ));
        self.append(
            transaction,
            gateway_id,
            Some((actor_principal_id, actor_session_id)),
            AdministrativeAuditRecord {
                action,
                target_kind: AuditTargetKind::WorkspaceMembership,
                target_id: target_principal_id.to_string(),
                workspace_id: Some(workspace_id.clone()),
                metadata: BoundedServerGeneratedMetadata::workspace_membership(
                    target_principal_id.clone(),
                ),
            },
            now,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn member_lifecycle(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor_principal_id: &PrincipalId,
        actor_session_id: &AuthSessionId,
        target_principal_id: &PrincipalId,
        action: AuditAction,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        debug_assert!(matches!(
            action,
            AuditAction::MemberSuspended | AuditAction::MemberRestored | AuditAction::MemberRemoved
        ));
        self.append(
            transaction,
            gateway_id,
            Some((actor_principal_id, actor_session_id)),
            AdministrativeAuditRecord {
                action,
                target_kind: AuditTargetKind::Principal,
                target_id: target_principal_id.to_string(),
                workspace_id: None,
                metadata: BoundedServerGeneratedMetadata::empty(),
            },
            now,
        )
        .await
    }

    async fn append(
        self,
        transaction: &DatabaseTransaction,
        gateway_id: &GatewayId,
        actor: Option<(&PrincipalId, &AuthSessionId)>,
        record: AdministrativeAuditRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<()> {
        let started = std::time::Instant::now();
        let id = AuditEventId::new(generate_id(ADMINISTRATION_DOMAIN_ID_LEN))
            .context("generated invalid administrative audit event id")?;
        let policy_generation = pioneer_crud::current_policy_generation_on(transaction).await?;
        let registry = crate::authorization::RoleDefinitionRegistry::new();
        let policy_role_key = if let Some((actor_principal_id, _)) = actor {
            let principal = pioneer_crud::load_principal_by_id(transaction, actor_principal_id)
                .await?
                .context("administrative audit actor principal is missing")?;
            let persisted_role = principal
                .role_key
                .map(RoleKey::new)
                .transpose()
                .context("administrative audit actor has invalid persisted role")?;
            let definition = registry
                .resolve(principal.kind, persisted_role.as_ref())
                .context("administrative audit actor role is unsupported")?;
            Some(RoleKey::new(definition.key).expect("registered role key must be valid"))
        } else {
            None
        };
        let result = pioneer_crud::insert_administrative_audit_event(
            transaction,
            pioneer_crud::NewAdministrativeAuditEvent {
                id,
                gateway_id: gateway_id.clone(),
                actor_principal_id: actor.map(|(principal_id, _)| principal_id.clone()),
                actor_session_id: actor.map(|(_, session_id)| session_id.clone()),
                action: record.action,
                target_kind: record.target_kind,
                target_id: record.target_id,
                workspace_id: record.workspace_id,
                metadata: record.metadata,
                policy_generation,
                policy_role_key,
                policy_fingerprint: registry.policy_fingerprint(),
                created_at: now,
            },
        )
        .await;
        crate::epic5_observability::record_outcome(
            crate::epic5_observability::Epic5Operation::AuditWrite,
            if result.is_ok() {
                crate::epic5_observability::Epic5Outcome::Success
            } else {
                crate::epic5_observability::Epic5Outcome::Unavailable
            },
        );
        crate::epic5_observability::record_latency(
            crate::epic5_observability::Epic5Operation::AuditWrite,
            started.elapsed(),
        );
        result.map(|_| ())
    }
}
