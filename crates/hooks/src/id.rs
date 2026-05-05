use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookIdError {
    type_name: &'static str,
    reason: &'static str,
}

impl HookIdError {
    fn new(type_name: &'static str, reason: &'static str) -> Self {
        Self { type_name, reason }
    }
}

impl fmt::Display for HookIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.type_name, self.reason)
    }
}

impl std::error::Error for HookIdError {}

macro_rules! hook_id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, HookIdError> {
                let value = value.into();
                validate_id_value(stringify!($name), value.as_str())?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = HookIdError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl std::str::FromStr for $name {
            type Err = HookIdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_str(HookIdVisitor::<$name> {
                    type_name: stringify!($name),
                    marker: std::marker::PhantomData,
                })
            }
        }
    };
}

hook_id_type!(HookId);
hook_id_type!(HookSubscriptionId);
hook_id_type!(HookRunId);
hook_id_type!(HookCompactionId);
hook_id_type!(HookWorkspaceId);
hook_id_type!(HookThreadId);
hook_id_type!(HookTurnId);
hook_id_type!(HookTaskId);
hook_id_type!(HookAgentId);
hook_id_type!(HookActorId);
hook_id_type!(HookFeatureFlag);
hook_id_type!(HookMetadataKey);
hook_id_type!(HookDomain);
hook_id_type!(HookPolicyKey);
hook_id_type!(HookContributionId);
hook_id_type!(HookContributionHash);
hook_id_type!(HookSectionId);
hook_id_type!(HookSourceId);
hook_id_type!(HookDiagnosticCode);
hook_id_type!(HookAuditEventKind);
hook_id_type!(HookCapability);
hook_id_type!(HookKind);
hook_id_type!(HookFilterKey);

fn validate_id_value(type_name: &'static str, value: &str) -> Result<(), HookIdError> {
    if value.is_empty() {
        return Err(HookIdError::new(type_name, "cannot be empty"));
    }
    if value.trim() != value {
        return Err(HookIdError::new(
            type_name,
            "cannot contain leading or trailing whitespace",
        ));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(HookIdError::new(type_name, "cannot contain whitespace"));
    }
    Ok(())
}

struct HookIdVisitor<T> {
    type_name: &'static str,
    marker: std::marker::PhantomData<T>,
}

impl<'de, T> Visitor<'de> for HookIdVisitor<T>
where
    T: TryFrom<String, Error = HookIdError>,
{
    type Value = T;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "a non-empty {} string", self.type_name)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        T::try_from(value.to_owned()).map_err(E::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_id_rejects_empty_value() {
        assert!(HookId::new("").is_err());
    }

    #[test]
    fn hook_id_rejects_whitespace_value() {
        assert!(HookId::new("   ").is_err());
        assert!(HookId::new(" hook.id").is_err());
        assert!(HookId::new("hook id").is_err());
    }

    #[test]
    fn hook_id_serializes_as_string() {
        let id = HookId::new("policy.turn_classifier").expect("valid id");
        let value = serde_json::to_value(&id).expect("id should serialize");
        assert_eq!(value, serde_json::json!("policy.turn_classifier"));
        let decoded: HookId = serde_json::from_value(value).expect("id should deserialize");
        assert_eq!(decoded.as_str(), "policy.turn_classifier");
    }
}
