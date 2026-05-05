use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookTextError {
    type_name: &'static str,
    reason: &'static str,
}

impl HookTextError {
    fn new(type_name: &'static str, reason: &'static str) -> Self {
        Self { type_name, reason }
    }
}

impl fmt::Display for HookTextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.type_name, self.reason)
    }
}

impl std::error::Error for HookTextError {}

macro_rules! hook_text_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, HookTextError> {
                let value = value.into();
                validate_text_value(stringify!($name), value.as_str())?;
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
            type Error = HookTextError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl std::str::FromStr for $name {
            type Err = HookTextError;

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
                deserializer.deserialize_str(HookTextVisitor::<$name> {
                    type_name: stringify!($name),
                    marker: std::marker::PhantomData,
                })
            }
        }
    };
}

hook_text_type!(HookDiagnosticMessage);
hook_text_type!(HookPromptContent);
hook_text_type!(HookPromptSectionTitle);
hook_text_type!(HookSourceLabel);

fn validate_text_value(type_name: &'static str, value: &str) -> Result<(), HookTextError> {
    if value.trim().is_empty() {
        return Err(HookTextError::new(type_name, "cannot be empty"));
    }
    Ok(())
}

struct HookTextVisitor<T> {
    type_name: &'static str,
    marker: std::marker::PhantomData<T>,
}

impl<'de, T> Visitor<'de> for HookTextVisitor<T>
where
    T: TryFrom<String, Error = HookTextError>,
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
    fn hook_text_rejects_empty_value() {
        assert!(HookPromptContent::new("").is_err());
        assert!(HookPromptContent::new("   ").is_err());
    }

    #[test]
    fn hook_text_serializes_as_string() {
        let text = HookPromptContent::new("context").expect("valid text");
        let value = serde_json::to_value(&text).expect("text should serialize");
        assert_eq!(value, serde_json::json!("context"));
        let decoded: HookPromptContent =
            serde_json::from_value(value).expect("text should deserialize");
        assert_eq!(decoded.as_str(), "context");
    }
}
