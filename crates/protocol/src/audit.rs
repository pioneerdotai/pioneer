use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    AuditEventId, AuthSessionId, DeviceId, GatewayId, PrincipalId, WorkspaceId,
    invitation::{INVITATION_MAX_WORKSPACE_GRANTS, INVITATION_MIN_WORKSPACE_GRANTS},
};

pub const AUDIT_METADATA_VERSION_V1: u32 = 1;
pub const AUDIT_METADATA_MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventDomain {
    Administration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    InvitationCreated,
    InvitationRevoked,
    InvitationExpired,
    InvitationAccepted,
    WorkspaceMemberAdded,
    WorkspaceMemberRemoved,
    MemberSuspended,
    MemberRestored,
    MemberRemoved,
    MemberRecoveryDeviceCreated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditTargetKind {
    Invitation,
    Principal,
    WorkspaceMembership,
    DeviceSession,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct BoundedServerGeneratedMetadata(ServerGeneratedAuditMetadata);

impl BoundedServerGeneratedMetadata {
    pub fn empty() -> Self {
        Self(ServerGeneratedAuditMetadata::Empty)
    }

    pub fn invitation_workspace_grants(
        mut workspace_ids: Vec<WorkspaceId>,
    ) -> Result<Self, AuditMetadataError> {
        workspace_ids.sort();
        workspace_ids.dedup();
        validate_grant_count(workspace_ids.len())?;
        Ok(Self(
            ServerGeneratedAuditMetadata::InvitationWorkspaceGrants { workspace_ids },
        ))
    }

    pub fn workspace_membership(principal_id: PrincipalId) -> Self {
        Self(ServerGeneratedAuditMetadata::WorkspaceMembership { principal_id })
    }

    pub fn invitation_acceptance(
        principal_id: PrincipalId,
        device_id: DeviceId,
        session_id: AuthSessionId,
        mut workspace_ids: Vec<WorkspaceId>,
    ) -> Result<Self, AuditMetadataError> {
        workspace_ids.sort();
        workspace_ids.dedup();
        validate_grant_count(workspace_ids.len())?;
        Ok(Self(ServerGeneratedAuditMetadata::InvitationAcceptance {
            principal_id,
            device_id,
            session_id,
            workspace_ids,
        }))
    }

    pub fn recovery_device(
        principal_id: PrincipalId,
        device_id: DeviceId,
        session_id: AuthSessionId,
    ) -> Self {
        Self(ServerGeneratedAuditMetadata::RecoveryDevice {
            principal_id,
            device_id,
            session_id,
        })
    }

    pub fn recovery_device_binding(&self) -> Option<(&PrincipalId, &DeviceId, &AuthSessionId)> {
        match &self.0 {
            ServerGeneratedAuditMetadata::RecoveryDevice {
                principal_id,
                device_id,
                session_id,
            } => Some((principal_id, device_id, session_id)),
            _ => None,
        }
    }

    pub fn to_json_string(&self) -> Result<String, AuditMetadataError> {
        let json = serde_json::to_string(self)
            .map_err(|error| AuditMetadataError::Serialization(error.to_string()))?;
        if json.len() > AUDIT_METADATA_MAX_BYTES {
            return Err(AuditMetadataError::TooLarge {
                maximum: AUDIT_METADATA_MAX_BYTES,
                actual: json.len(),
            });
        }
        Ok(json)
    }
}

impl<'de> Deserialize<'de> for BoundedServerGeneratedMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let metadata = ServerGeneratedAuditMetadata::deserialize(deserializer)?;
        let workspace_ids = match &metadata {
            ServerGeneratedAuditMetadata::InvitationWorkspaceGrants { workspace_ids }
            | ServerGeneratedAuditMetadata::InvitationAcceptance { workspace_ids, .. } => {
                Some(workspace_ids)
            }
            _ => None,
        };
        if let Some(workspace_ids) = workspace_ids {
            validate_grant_count(workspace_ids.len()).map_err(serde::de::Error::custom)?;
            if workspace_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(serde::de::Error::custom(
                    "audit invitation workspace IDs must be sorted and unique",
                ));
            }
        }
        let bounded = Self(metadata);
        bounded.to_json_string().map_err(serde::de::Error::custom)?;
        Ok(bounded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ServerGeneratedAuditMetadata {
    Empty,
    InvitationWorkspaceGrants {
        workspace_ids: Vec<WorkspaceId>,
    },
    InvitationAcceptance {
        principal_id: PrincipalId,
        device_id: DeviceId,
        session_id: AuthSessionId,
        workspace_ids: Vec<WorkspaceId>,
    },
    WorkspaceMembership {
        principal_id: PrincipalId,
    },
    RecoveryDevice {
        principal_id: PrincipalId,
        device_id: DeviceId,
        session_id: AuthSessionId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuditEvent {
    pub id: AuditEventId,
    pub gateway_id: GatewayId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<PrincipalId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_session_id: Option<AuthSessionId>,
    pub domain: AuditEventDomain,
    pub action: AuditAction,
    pub target_kind: AuditTargetKind,
    pub target_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<WorkspaceId>,
    pub metadata_version: u32,
    pub metadata: BoundedServerGeneratedMetadata,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditMetadataError {
    WorkspaceGrantCount {
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    TooLarge {
        maximum: usize,
        actual: usize,
    },
    Serialization(String),
}

impl fmt::Display for AuditMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkspaceGrantCount {
                minimum,
                maximum,
                actual,
            } => write!(
                formatter,
                "audit invitation grants must contain {minimum} to {maximum} workspaces, got {actual}"
            ),
            Self::TooLarge { maximum, actual } => write!(
                formatter,
                "audit metadata must contain at most {maximum} bytes, got {actual}"
            ),
            Self::Serialization(error) => {
                write!(formatter, "audit metadata serialization failed: {error}")
            }
        }
    }
}

impl std::error::Error for AuditMetadataError {}

fn validate_grant_count(actual: usize) -> Result<(), AuditMetadataError> {
    if !(INVITATION_MIN_WORKSPACE_GRANTS..=INVITATION_MAX_WORKSPACE_GRANTS).contains(&actual) {
        return Err(AuditMetadataError::WorkspaceGrantCount {
            minimum: INVITATION_MIN_WORKSPACE_GRANTS,
            maximum: INVITATION_MAX_WORKSPACE_GRANTS,
            actual,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AUDIT_METADATA_MAX_BYTES, AUDIT_METADATA_VERSION_V1, AuditAction, AuditEventDomain,
        AuditTargetKind, BoundedServerGeneratedMetadata,
    };
    use crate::{AuthSessionId, DeviceId, PrincipalId, WorkspaceId};
    use serde_json::json;

    #[test]
    fn administrative_audit_vocabulary_is_exact_snake_case() {
        assert_eq!(AUDIT_METADATA_VERSION_V1, 1);
        assert_eq!(AUDIT_METADATA_MAX_BYTES, 16_384);
        assert_eq!(
            serde_json::to_value(AuditEventDomain::Administration).unwrap(),
            json!("administration")
        );
        assert_eq!(
            serde_json::to_value(AuditAction::MemberRecoveryDeviceCreated).unwrap(),
            json!("member_recovery_device_created")
        );
        assert_eq!(
            serde_json::to_value(AuditTargetKind::WorkspaceMembership).unwrap(),
            json!("workspace_membership")
        );
        assert!(serde_json::from_value::<AuditAction>(json!("login_failed")).is_err());
    }

    #[test]
    fn invitation_audit_metadata_is_canonical_typed_and_bounded() {
        let metadata = BoundedServerGeneratedMetadata::invitation_workspace_grants(vec![
            WorkspaceId::new("W00000000000000000002").unwrap(),
            WorkspaceId::new("W00000000000000000001").unwrap(),
            WorkspaceId::new("W00000000000000000001").unwrap(),
        ])
        .unwrap();
        let value = serde_json::to_value(&metadata).unwrap();
        assert_eq!(
            value,
            json!({
                "kind": "invitation_workspace_grants",
                "workspace_ids": [
                    "W00000000000000000001",
                    "W00000000000000000002"
                ]
            })
        );
        assert!(metadata.to_json_string().unwrap().len() < AUDIT_METADATA_MAX_BYTES);
        assert!(
            serde_json::from_value::<BoundedServerGeneratedMetadata>(json!({
                "kind": "invitation_workspace_grants",
                "workspace_ids": []
            }))
            .is_err()
        );
    }

    #[test]
    fn recovery_audit_metadata_binds_target_principal_device_and_session() {
        let principal_id = PrincipalId::new("P00000000000000000002").unwrap();
        let device_id = DeviceId::new("D00000000000000000002").unwrap();
        let session_id = AuthSessionId::new("S00000000000000000002").unwrap();
        let metadata = BoundedServerGeneratedMetadata::recovery_device(
            principal_id.clone(),
            device_id.clone(),
            session_id.clone(),
        );
        assert_eq!(
            metadata.recovery_device_binding(),
            Some((&principal_id, &device_id, &session_id))
        );
        assert_eq!(
            serde_json::to_value(metadata).unwrap(),
            json!({
                "kind": "recovery_device",
                "principal_id": "P00000000000000000002",
                "device_id": "D00000000000000000002",
                "session_id": "S00000000000000000002"
            })
        );
    }
}
