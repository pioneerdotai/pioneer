use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::PrincipalId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Superuser,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalStatus {
    Active,
    Suspended,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum PersistedActorRef {
    Principal(PrincipalId),
    System,
}

#[cfg(test)]
mod tests {
    use super::{PersistedActorRef, PrincipalKind, PrincipalStatus};
    use crate::PrincipalId;
    use serde_json::json;

    #[test]
    fn principal_vocabulary_uses_stable_snake_case_values() {
        assert_eq!(
            serde_json::to_value(PrincipalKind::Superuser).unwrap(),
            json!("superuser")
        );
        assert_eq!(
            serde_json::to_value(PrincipalKind::User).unwrap(),
            json!("user")
        );
        assert_eq!(
            serde_json::to_value(PrincipalStatus::Active).unwrap(),
            json!("active")
        );
        assert_eq!(
            serde_json::to_value(PrincipalStatus::Suspended).unwrap(),
            json!("suspended")
        );
        assert_eq!(
            serde_json::to_value(PrincipalStatus::Removed).unwrap(),
            json!("removed")
        );
    }

    #[test]
    fn persisted_actor_round_trips_principal_and_system() {
        let principal = PersistedActorRef::Principal(
            PrincipalId::new("P00000000000000000001").expect("valid principal id"),
        );

        let principal_value = json!({
            "kind": "principal",
            "id": "P00000000000000000001"
        });
        assert_eq!(serde_json::to_value(&principal).unwrap(), principal_value);
        assert_eq!(
            serde_json::from_value::<PersistedActorRef>(principal_value).unwrap(),
            principal
        );

        let system_value = json!({"kind": "system"});
        assert_eq!(
            serde_json::to_value(PersistedActorRef::System).unwrap(),
            system_value
        );
        assert_eq!(
            serde_json::from_value::<PersistedActorRef>(system_value).unwrap(),
            PersistedActorRef::System
        );
    }

    #[test]
    fn persisted_actor_rejects_invalid_principal_ids_and_unknown_kinds() {
        assert!(
            serde_json::from_value::<PersistedActorRef>(json!({
                "kind": "principal",
                "id": "superuser"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<PersistedActorRef>(json!({
                "kind": "unknown"
            }))
            .is_err()
        );
    }
}
