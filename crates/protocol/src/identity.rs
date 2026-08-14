use schemars::JsonSchema;
use serde::de::Error as DeError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::PrincipalId;

pub const ROLE_KEY_MAX_LEN: usize = 32;
/// Stable capability-snapshot identifier for the built-in Superuser kind.
/// Superusers still persist with a null `RoleKey`; this string is presentation
/// metadata, not a user-role value.
pub const SUPERUSER_CAPABILITY_ROLE_KEY: &str = "superuser";
pub const MEMBER_ROLE_KEY: &str = "member";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct RoleKey(
    #[schemars(length(min = 1, max = 32), regex(pattern = r"^[a-z][a-z0-9_-]{0,31}$"))] String,
);

impl RoleKey {
    pub fn new(value: impl Into<String>) -> Result<Self, RoleKeyError> {
        let value = value.into();
        validate_role_key(value.as_str())?;
        Ok(Self(value))
    }

    pub fn member() -> Self {
        Self(MEMBER_ROLE_KEY.to_owned())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for RoleKey {
    type Err = RoleKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for RoleKey {
    type Error = RoleKeyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for RoleKey {
    type Error = RoleKeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<str> for RoleKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RoleKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RoleKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleKeyError {
    Empty,
    TooLong { maximum: usize, actual: usize },
    InvalidFirstCharacter { character: char },
    InvalidCharacter { index: usize, character: char },
}

impl fmt::Display for RoleKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("role key must not be empty"),
            Self::TooLong { maximum, actual } => {
                write!(
                    formatter,
                    "role key must contain at most {maximum} bytes, got {actual}"
                )
            }
            Self::InvalidFirstCharacter { character } => write!(
                formatter,
                "role key must start with a lowercase ASCII letter, got {character:?}"
            ),
            Self::InvalidCharacter { index, character } => write!(
                formatter,
                "role key may contain only lowercase ASCII letters, digits, `_` and `-`; found {character:?} at byte {index}"
            ),
        }
    }
}

impl std::error::Error for RoleKeyError {}

fn validate_role_key(value: &str) -> Result<(), RoleKeyError> {
    if value.is_empty() {
        return Err(RoleKeyError::Empty);
    }
    if value.len() > ROLE_KEY_MAX_LEN {
        return Err(RoleKeyError::TooLong {
            maximum: ROLE_KEY_MAX_LEN,
            actual: value.len(),
        });
    }
    let first = value.chars().next().expect("non-empty role key");
    if !first.is_ascii_lowercase() {
        return Err(RoleKeyError::InvalidFirstCharacter { character: first });
    }
    if let Some((index, character)) = value.char_indices().find(|(_, character)| {
        !character.is_ascii_lowercase()
            && !character.is_ascii_digit()
            && !matches!(character, '_' | '-')
    }) {
        return Err(RoleKeyError::InvalidCharacter { index, character });
    }
    Ok(())
}

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
    use super::{
        MEMBER_ROLE_KEY, PersistedActorRef, PrincipalKind, PrincipalStatus, ROLE_KEY_MAX_LEN,
        RoleKey,
    };
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

    #[test]
    fn role_key_is_canonical_bounded_and_round_trips() {
        let member = RoleKey::member();
        assert_eq!(member.as_str(), MEMBER_ROLE_KEY);
        assert_eq!(
            serde_json::from_value::<RoleKey>(serde_json::json!("member")).unwrap(),
            member
        );
        assert_eq!(
            serde_json::to_value(member).unwrap(),
            serde_json::json!("member")
        );

        let future = RoleKey::new("future_role-2").expect("valid future role key");
        assert_eq!(future.to_string(), "future_role-2");
        assert!(RoleKey::new("a".repeat(ROLE_KEY_MAX_LEN)).is_ok());
    }

    #[test]
    fn role_key_rejects_noncanonical_or_unbounded_values() {
        for value in [
            "",
            "Member",
            " member",
            "member ",
            "member.admin",
            "member/admin",
            "mémber",
            "1member",
        ] {
            assert!(RoleKey::new(value).is_err(), "{value:?} must be rejected");
            assert!(
                serde_json::from_value::<RoleKey>(serde_json::json!(value)).is_err(),
                "{value:?} must fail deserialization"
            );
        }
        assert!(RoleKey::new("a".repeat(ROLE_KEY_MAX_LEN + 1)).is_err());
    }
}
