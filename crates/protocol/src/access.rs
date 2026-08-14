use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{InvitationId, PrincipalId, RoleKey};

/// Durable, monotonically increasing authorization policy/ACL generation.
/// Zero is reserved for an uninitialized process and is never persisted.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct PolicyGeneration(u64);

impl PolicyGeneration {
    pub const INITIAL: Self = Self(1);

    pub const fn new(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Payload-safe type of committed authorization input change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationChangeKind {
    CodePolicy,
    RolePolicy,
    RoleAssignment,
    WorkspaceAcl,
    ThreadAcl,
    ResourceSelector,
}

/// Exact invalidation scope without policy contents or protected metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum AuthorizationChangeScope {
    Global,
    Role {
        role_key: RoleKey,
    },
    Principal {
        principal_id: PrincipalId,
    },
    PrincipalWorkspace {
        principal_id: PrincipalId,
        workspace_id: String,
    },
    PrincipalThread {
        principal_id: PrincipalId,
        workspace_id: String,
        thread_id: String,
    },
    Invitation {
        invitation_id: InvitationId,
    },
    Workspace {
        workspace_id: String,
    },
    Thread {
        workspace_id: String,
        thread_id: String,
    },
    ResourceSelector {
        workspace_id: String,
        selector: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthorizationProjectionChangedNotification {
    pub policy_generation: PolicyGeneration,
    pub change: AuthorizationChangeKind,
    pub affected: AuthorizationChangeScope,
}

/// Payload-safe reason for invalidating client authorization-derived state.
///
/// This vocabulary deliberately contains no protected resource metadata or
/// policy-engine details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AccessChangeKind {
    WorkspaceMembership,
    ThreadCreated,
    ThreadVisibility,
    ThreadParticipantAdded,
    ThreadParticipantRemoved,
}

impl AccessChangeKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceMembership => "workspace_membership",
            Self::ThreadCreated => "thread_created",
            Self::ThreadVisibility => "thread_visibility",
            Self::ThreadParticipantAdded => "thread_participant_added",
            Self::ThreadParticipantRemoved => "thread_participant_removed",
        }
    }
}

/// Authoritative access result for the recipient of an access-change event.
/// This is required: omission is a protocol incompatibility, never a legacy
/// security mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AccessChangeOutcome {
    Retained,
    Revoked,
}

/// Minimal notification telling an authenticated client to re-resolve access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AccessChangedNotification {
    pub authorization_revision: u64,
    pub workspace_id: String,
    /// Exact affected thread when the committed ACL mutation is thread-scoped.
    ///
    /// This is an opaque server-owned identifier, not protected thread
    /// content. It is omitted for workspace-wide changes and lets clients
    /// evict only the affected thread instead of discarding unrelated
    /// workspace state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    /// Server-resolved result for this exact recipient and affected scope.
    pub outcome: AccessChangeOutcome,
    pub change: AccessChangeKind,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn access_changed_round_trip_is_minimal_and_payload_safe() {
        let notification = AccessChangedNotification {
            authorization_revision: 42,
            workspace_id: "workspace-red".to_owned(),
            thread_id: Some("thread-private".to_owned()),
            outcome: AccessChangeOutcome::Revoked,
            change: AccessChangeKind::ThreadParticipantRemoved,
        };
        let value = serde_json::to_value(&notification).expect("notification should encode");
        assert_eq!(
            value,
            json!({
                "authorization_revision": 42,
                "workspace_id": "workspace-red",
                "thread_id": "thread-private",
                "outcome": "revoked",
                "change": "thread_participant_removed"
            })
        );
        assert_eq!(
            serde_json::from_value::<AccessChangedNotification>(value)
                .expect("notification should decode"),
            notification
        );

        let workspace_wide = AccessChangedNotification {
            authorization_revision: 43,
            workspace_id: "workspace-red".to_owned(),
            thread_id: None,
            outcome: AccessChangeOutcome::Retained,
            change: AccessChangeKind::WorkspaceMembership,
        };
        assert_eq!(
            serde_json::to_value(workspace_wide).expect("workspace notification should encode"),
            json!({
                "authorization_revision": 43,
                "workspace_id": "workspace-red",
                "outcome": "retained",
                "change": "workspace_membership"
            })
        );
    }

    #[test]
    fn policy_change_scope_round_trip_is_typed_and_exact() {
        let notification = AuthorizationProjectionChangedNotification {
            policy_generation: PolicyGeneration::new(44).unwrap(),
            change: AuthorizationChangeKind::RoleAssignment,
            affected: AuthorizationChangeScope::Principal {
                principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            },
        };
        let encoded = serde_json::to_value(&notification).unwrap();
        assert_eq!(encoded["policy_generation"], 44);
        assert_eq!(encoded["change"], "role_assignment");
        assert_eq!(encoded["affected"]["scope"], "principal");
        assert_eq!(
            serde_json::from_value::<AuthorizationProjectionChangedNotification>(encoded).unwrap(),
            notification
        );
        assert!(PolicyGeneration::new(0).is_none());

        let exact_acl = AuthorizationProjectionChangedNotification {
            policy_generation: PolicyGeneration::new(45).unwrap(),
            change: AuthorizationChangeKind::ThreadAcl,
            affected: AuthorizationChangeScope::PrincipalThread {
                principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
                workspace_id: "workspace-red".to_owned(),
                thread_id: "thread-private".to_owned(),
            },
        };
        let encoded = serde_json::to_value(&exact_acl).unwrap();
        assert_eq!(encoded["affected"]["scope"], "principal_thread");
        assert_eq!(encoded["affected"]["workspace_id"], "workspace-red");
        assert_eq!(encoded["affected"]["thread_id"], "thread-private");
        assert_eq!(
            serde_json::from_value::<AuthorizationProjectionChangedNotification>(encoded).unwrap(),
            exact_acl
        );
    }
}
