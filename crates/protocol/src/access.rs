use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    pub change: AccessChangeKind,
}

#[cfg(test)]
mod tests {
    use super::{AccessChangeKind, AccessChangedNotification};
    use serde_json::json;

    #[test]
    fn access_changed_round_trip_is_minimal_and_payload_safe() {
        let notification = AccessChangedNotification {
            authorization_revision: 42,
            workspace_id: "workspace-red".to_owned(),
            thread_id: Some("thread-private".to_owned()),
            change: AccessChangeKind::ThreadParticipantRemoved,
        };
        let value = serde_json::to_value(&notification).expect("notification should encode");
        assert_eq!(
            value,
            json!({
                "authorization_revision": 42,
                "workspace_id": "workspace-red",
                "thread_id": "thread-private",
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
            change: AccessChangeKind::WorkspaceMembership,
        };
        assert_eq!(
            serde_json::to_value(workspace_wide).expect("workspace notification should encode"),
            json!({
                "authorization_revision": 43,
                "workspace_id": "workspace-red",
                "change": "workspace_membership"
            })
        );
    }
}
