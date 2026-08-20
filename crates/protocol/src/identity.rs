use schemars::JsonSchema;
use serde::de::Error as DeError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::{CLIAgentRuntimeKind, PrincipalId, WorkspaceId};

pub const AGENT_OPAQUE_ID_LEN: usize = 21;
pub const AGENT_NICKNAME_MIN_LEN: usize = 2;
pub const AGENT_NICKNAME_MAX_LEN: usize = 32;
pub const AGENT_DISPLAY_NAME_MAX_SCALARS: usize = 128;
pub const AGENT_DISPLAY_NAME_MAX_UTF8_BYTES: usize = 512;
pub const AGENT_ROLE_LABEL_MAX_SCALARS: usize = 64;

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
    AgentExecution(AgentExecutionId),
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentIdentityValidationError {
    Empty {
        field: &'static str,
    },
    InvalidLength {
        field: &'static str,
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    InvalidExactLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    InvalidFirstCharacter {
        field: &'static str,
        character: char,
    },
    InvalidCharacter {
        field: &'static str,
        index: usize,
        character: char,
    },
    ControlCharacter {
        field: &'static str,
        index: usize,
        character: char,
    },
}

impl fmt::Display for AgentIdentityValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { field } => write!(formatter, "{field} must not be empty"),
            Self::InvalidLength {
                field,
                minimum,
                maximum,
                actual,
            } => write!(
                formatter,
                "{field} must contain {minimum} to {maximum} characters, got {actual}"
            ),
            Self::InvalidExactLength {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "{field} must contain exactly {expected} characters, got {actual}"
            ),
            Self::InvalidFirstCharacter { field, character } => write!(
                formatter,
                "{field} must start with an ASCII alphanumeric character, got {character:?}"
            ),
            Self::InvalidCharacter {
                field,
                index,
                character,
            } => write!(
                formatter,
                "{field} contains invalid character {character:?} at byte {index}"
            ),
            Self::ControlCharacter {
                field,
                index,
                character,
            } => write!(
                formatter,
                "{field} contains control character {character:?} at byte {index}"
            ),
        }
    }
}

impl std::error::Error for AgentIdentityValidationError {}

fn validate_agent_id(
    value: String,
    field: &'static str,
) -> Result<String, AgentIdentityValidationError> {
    let actual = value.chars().count();
    if actual != AGENT_OPAQUE_ID_LEN {
        return Err(AgentIdentityValidationError::InvalidExactLength {
            field,
            expected: AGENT_OPAQUE_ID_LEN,
            actual,
        });
    }
    if let Some((index, character)) = value
        .char_indices()
        .find(|(_, character)| !character.is_ascii_alphanumeric())
    {
        return Err(AgentIdentityValidationError::InvalidCharacter {
            field,
            index,
            character,
        });
    }
    Ok(value)
}

macro_rules! define_agent_id {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
        #[serde(transparent)]
        pub struct $name(
            #[schemars(length(equal = 21), regex(pattern = r"^[A-Za-z0-9]{21}$"))] String,
        );

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, AgentIdentityValidationError> {
                validate_agent_id(value.into(), $field).map(Self)
            }

            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl FromStr for $name {
            type Err = AgentIdentityValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = AgentIdentityValidationError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = AgentIdentityValidationError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(D::Error::custom)
            }
        }
    };
}

define_agent_id!(AgentIdentityId, "agent identity id");
define_agent_id!(AgentExecutionId, "agent execution id");
define_agent_id!(AgentActionId, "agent action id");
define_agent_id!(AgentDelegationRouteId, "agent delegation route id");
define_agent_id!(AgentRouteGrantId, "agent route grant id");
define_agent_id!(AgentExecutionProfileId, "agent execution profile id");

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct AgentNicknameKey(
    #[schemars(
        length(min = 2, max = 32),
        regex(pattern = r"^[a-z0-9][a-z0-9_.-]{1,31}$")
    )]
    String,
);

impl AgentNicknameKey {
    pub fn new(value: impl Into<String>) -> Result<Self, AgentIdentityValidationError> {
        let value = value.into().to_ascii_lowercase();
        let length = value.len();
        if !(AGENT_NICKNAME_MIN_LEN..=AGENT_NICKNAME_MAX_LEN).contains(&length) {
            return Err(AgentIdentityValidationError::InvalidLength {
                field: "agent nickname",
                minimum: AGENT_NICKNAME_MIN_LEN,
                maximum: AGENT_NICKNAME_MAX_LEN,
                actual: length,
            });
        }
        let first = value.chars().next().expect("bounded nickname is non-empty");
        if !first.is_ascii_alphanumeric() {
            return Err(AgentIdentityValidationError::InvalidFirstCharacter {
                field: "agent nickname",
                character: first,
            });
        }
        if let Some((index, character)) = value.char_indices().find(|(_, character)| {
            !character.is_ascii_alphanumeric() && !matches!(character, '_' | '.' | '-')
        }) {
            return Err(AgentIdentityValidationError::InvalidCharacter {
                field: "agent nickname",
                index,
                character,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for AgentNicknameKey {
    type Err = AgentIdentityValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for AgentNicknameKey {
    type Error = AgentIdentityValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for AgentNicknameKey {
    type Error = AgentIdentityValidationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<str> for AgentNicknameKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for AgentNicknameKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AgentNicknameKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct AgentDisplayName(#[schemars(length(min = 1, max = 128))] String);

impl AgentDisplayName {
    pub fn new(value: impl Into<String>) -> Result<Self, AgentIdentityValidationError> {
        let value = value.into().trim().to_owned();
        if value.is_empty() {
            return Err(AgentIdentityValidationError::Empty {
                field: "agent display name",
            });
        }
        if value.chars().count() > AGENT_DISPLAY_NAME_MAX_SCALARS
            || value.len() > AGENT_DISPLAY_NAME_MAX_UTF8_BYTES
        {
            return Err(AgentIdentityValidationError::InvalidLength {
                field: "agent display name",
                minimum: 1,
                maximum: AGENT_DISPLAY_NAME_MAX_SCALARS,
                actual: value.chars().count(),
            });
        }
        if let Some((index, character)) = value
            .char_indices()
            .find(|(_, character)| character.is_control())
        {
            return Err(AgentIdentityValidationError::ControlCharacter {
                field: "agent display name",
                index,
                character,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for AgentDisplayName {
    type Err = AgentIdentityValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for AgentDisplayName {
    type Error = AgentIdentityValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for AgentDisplayName {
    type Error = AgentIdentityValidationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<str> for AgentDisplayName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for AgentDisplayName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AgentDisplayName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct AgentRoleLabel(#[schemars(length(min = 1, max = 64))] String);

impl AgentRoleLabel {
    pub fn new(value: impl Into<String>) -> Result<Self, AgentIdentityValidationError> {
        let value = value.into().trim().to_owned();
        let length = value.chars().count();
        if length == 0 {
            return Err(AgentIdentityValidationError::Empty {
                field: "agent role label",
            });
        }
        if length > AGENT_ROLE_LABEL_MAX_SCALARS {
            return Err(AgentIdentityValidationError::InvalidLength {
                field: "agent role label",
                minimum: 1,
                maximum: AGENT_ROLE_LABEL_MAX_SCALARS,
                actual: length,
            });
        }
        if let Some((index, character)) = value
            .char_indices()
            .find(|(_, character)| character.is_control())
        {
            return Err(AgentIdentityValidationError::ControlCharacter {
                field: "agent role label",
                index,
                character,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for AgentRoleLabel {
    type Err = AgentIdentityValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for AgentRoleLabel {
    type Error = AgentIdentityValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for AgentRoleLabel {
    type Error = AgentIdentityValidationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<str> for AgentRoleLabel {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for AgentRoleLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AgentRoleLabel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum AgentIdentitySource {
    NativeAgent {
        native_agent_id: String,
    },
    CliRuntimeInstance {
        runtime_instance_id: String,
        kind: CLIAgentRuntimeKind,
    },
    Ephemeral {
        allocation_id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentIdentityStatus {
    Active,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentIdentitySourceKind {
    NativeAgent,
    CliRuntimeInstance,
    Ephemeral,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentIdentity {
    pub id: AgentIdentityId,
    pub workspace_id: WorkspaceId,
    pub source: AgentIdentitySource,
    pub source_revision: u64,
    pub source_fingerprint: String,
    pub status: AgentIdentityStatus,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_at: Option<i64>,
}

impl AgentIdentitySource {
    pub const fn kind(&self) -> AgentIdentitySourceKind {
        match self {
            Self::NativeAgent { .. } => AgentIdentitySourceKind::NativeAgent,
            Self::CliRuntimeInstance { .. } => AgentIdentitySourceKind::CliRuntimeInstance,
            Self::Ephemeral { .. } => AgentIdentitySourceKind::Ephemeral,
        }
    }
}

pub const PIONEER_NATIVE_AGENT_KEY: &str = "pioneer";
pub const PIONEER_AGENT_DISPLAY_NAME: &str = "Pioneer";
pub const PIONEER_AGENT_NICKNAME: &str = "pioneer";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentIdentityProjection {
    pub id: AgentIdentityId,
    pub source_kind: AgentIdentitySourceKind,
    pub display_name: String,
    pub nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_label: Option<String>,
    pub source_revision: u64,
    pub source_fingerprint: String,
}

impl AgentIdentityProjection {
    pub fn new(
        id: AgentIdentityId,
        source_kind: AgentIdentitySourceKind,
        display_name: impl Into<String>,
        nickname: impl Into<String>,
        avatar_revision: Option<String>,
        role_label: Option<String>,
        source_revision: u64,
        source_fingerprint: impl Into<String>,
    ) -> Result<Self, AgentIdentityValidationError> {
        let display_name = AgentDisplayName::new(display_name)?.to_string();
        let nickname = AgentNicknameKey::new(nickname)?.to_string();
        let role_label = role_label
            .map(AgentRoleLabel::new)
            .transpose()?
            .map(|label| label.to_string());
        Ok(Self {
            id,
            source_kind,
            display_name,
            nickname,
            avatar_revision,
            role_label,
            source_revision,
            source_fingerprint: source_fingerprint.into(),
        })
    }

    pub fn pioneer(
        id: AgentIdentityId,
        source_revision: u64,
        source_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            id,
            source_kind: AgentIdentitySourceKind::NativeAgent,
            display_name: PIONEER_AGENT_DISPLAY_NAME.to_owned(),
            nickname: PIONEER_AGENT_NICKNAME.to_owned(),
            avatar_revision: None,
            role_label: None,
            source_revision,
            source_fingerprint: source_fingerprint.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentPresentationSnapshot {
    pub agent_identity_id: AgentIdentityId,
    pub agent_execution_id: AgentExecutionId,
    pub identity_source_kind: AgentIdentitySourceKind,
    pub identity_source_revision: u64,
    pub display_name: String,
    pub nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_label: Option<String>,
}

impl AgentPresentationSnapshot {
    /// Build the immutable conversation author snapshot for an execution.
    /// Current identity/settings rows are intentionally not consulted after
    /// this conversion, so historical rows retain the presentation that was
    /// committed with the action.
    pub fn to_turn_author_snapshot(&self) -> crate::TurnAuthorSnapshot {
        crate::TurnAuthorSnapshot {
            actor: PersistedActorRef::AgentExecution(self.agent_execution_id.clone()),
            display_name: self.display_name.clone(),
            nickname: self.nickname.clone(),
            avatar_revision: self.avatar_revision.clone(),
            agent: Some(self.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct ServiceId(String);

impl ServiceId {
    pub fn new(value: impl Into<String>) -> Result<Self, AgentIdentityValidationError> {
        let value = value.into();
        validate_non_empty_ascii_token(value, "service id").map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct SystemIssuer(String);

impl SystemIssuer {
    pub fn new(value: impl Into<String>) -> Result<Self, AgentIdentityValidationError> {
        let value = value.into();
        validate_non_empty_ascii_token(value, "system issuer").map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

fn validate_non_empty_ascii_token(
    value: String,
    field: &'static str,
) -> Result<String, AgentIdentityValidationError> {
    if value.is_empty() {
        return Err(AgentIdentityValidationError::Empty { field });
    }
    if value.len() > 128 {
        return Err(AgentIdentityValidationError::InvalidLength {
            field,
            minimum: 1,
            maximum: 128,
            actual: value.len(),
        });
    }
    if let Some((index, character)) = value.char_indices().find(|(_, character)| {
        !character.is_ascii_alphanumeric() && !matches!(character, '_' | '-')
    }) {
        return Err(AgentIdentityValidationError::InvalidCharacter {
            field,
            index,
            character,
        });
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum ConversationActorRef {
    Principal(PrincipalId),
    AgentExecution(AgentExecutionId),
    System,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum AuthorizationSubjectRef {
    Principal(PrincipalId),
    AgentExecution(AgentExecutionId),
    Service(ServiceId),
    System(SystemIssuer),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum AdministrativeActorRef {
    Principal(PrincipalId),
    Service(ServiceId),
    System(SystemIssuer),
}

#[cfg(test)]
mod tests {
    use super::{
        AgentDisplayName, AgentExecutionId, AgentIdentityProjection, AgentIdentitySource,
        AgentIdentitySourceKind, AgentIdentityStatus, AgentNicknameKey, AgentPresentationSnapshot,
        AgentRoleLabel, AgentRouteGrantId, ConversationActorRef, MEMBER_ROLE_KEY,
        PIONEER_AGENT_DISPLAY_NAME, PIONEER_AGENT_NICKNAME, PersistedActorRef, PrincipalKind,
        PrincipalStatus, ROLE_KEY_MAX_LEN, RoleKey,
    };
    use crate::{CLIAgentRuntimeKind, PrincipalId};
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

    #[test]
    fn purpose_specific_agent_ids_round_trip_without_becoming_principals() {
        let execution =
            AgentExecutionId::new("E00000000000000000001").expect("valid agent execution id");
        let actor = ConversationActorRef::AgentExecution(execution.clone());
        let encoded = json!({
            "kind": "agent_execution",
            "id": "E00000000000000000001"
        });
        assert_eq!(serde_json::to_value(&actor).unwrap(), encoded);
        assert_eq!(
            serde_json::from_value::<ConversationActorRef>(encoded).unwrap(),
            actor
        );
        assert_eq!(
            serde_json::from_value::<PersistedActorRef>(json!({
                "kind": "agent_execution",
                "id": "E00000000000000000001"
            }))
            .unwrap(),
            PersistedActorRef::AgentExecution(execution)
        );
        assert!(AgentExecutionId::new("not-an-agent-id").is_err());
        assert!(AgentExecutionId::new("E0000000000000000000!").is_err());
        assert_eq!(
            serde_json::from_value::<AgentRouteGrantId>(json!("G00000000000000000001"))
                .expect("valid route grant id")
                .as_str(),
            "G00000000000000000001"
        );
    }

    #[test]
    fn conversation_actor_keeps_principal_and_system_wire_values() {
        let principal = ConversationActorRef::Principal(
            PrincipalId::new("P00000000000000000001").expect("valid principal id"),
        );
        assert_eq!(
            serde_json::from_value::<ConversationActorRef>(json!({
                "kind": "principal",
                "id": "P00000000000000000001"
            }))
            .unwrap(),
            principal
        );
        assert_eq!(
            serde_json::from_value::<ConversationActorRef>(json!({ "kind": "system" })).unwrap(),
            ConversationActorRef::System
        );
    }

    #[test]
    fn agent_source_and_status_are_explicit_and_provider_free() {
        let source = AgentIdentitySource::CliRuntimeInstance {
            runtime_instance_id: "codex-secondary".to_owned(),
            kind: CLIAgentRuntimeKind::Codex,
        };
        let encoded = serde_json::to_value(&source).unwrap();
        assert_eq!(
            encoded,
            json!({
                "kind": "cli_runtime_instance",
                "value": {
                    "runtime_instance_id": "codex-secondary",
                    "kind": "codex"
                }
            })
        );
        assert_eq!(
            serde_json::from_value::<AgentIdentitySource>(encoded).unwrap(),
            source
        );
        assert_eq!(
            serde_json::to_value(AgentIdentityStatus::Retired).unwrap(),
            json!("retired")
        );
    }

    #[test]
    fn identity_projection_is_safe_and_pioneer_presentation_is_deterministic() {
        let identity_id =
            super::AgentIdentityId::new("N00000000000000000001").expect("valid identity id");
        let projection = AgentIdentityProjection::pioneer(identity_id.clone(), 7, "fingerprint-7");
        assert_eq!(projection.display_name, PIONEER_AGENT_DISPLAY_NAME);
        assert_eq!(projection.nickname, PIONEER_AGENT_NICKNAME);
        assert_eq!(projection.source_kind, AgentIdentitySourceKind::NativeAgent);

        let encoded = serde_json::to_string(&projection).expect("projection should serialize");
        for forbidden in [
            "prompt",
            "provider",
            "model",
            "credentials",
            "skills",
            "mcp",
            "execution_defaults",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "projection leaked forbidden field {forbidden}"
            );
        }

        let snapshot = AgentPresentationSnapshot {
            agent_identity_id: identity_id,
            agent_execution_id: super::AgentExecutionId::new("X00000000000000000001")
                .expect("valid execution id"),
            identity_source_kind: projection.source_kind,
            identity_source_revision: projection.source_revision,
            display_name: projection.display_name.clone(),
            nickname: projection.nickname.clone(),
            avatar_revision: projection.avatar_revision.clone(),
            role_label: projection.role_label.clone(),
        };
        assert_eq!(snapshot, snapshot.clone());
        assert_eq!(snapshot.display_name, PIONEER_AGENT_DISPLAY_NAME);
    }

    #[test]
    fn identity_projection_validates_presentation_and_distinguishes_cli_instances() {
        let first = AgentIdentityProjection::new(
            super::AgentIdentityId::new("C00000000000000000001").unwrap(),
            AgentIdentitySourceKind::CliRuntimeInstance,
            "Codex One",
            "codex-one",
            None,
            Some("worker".to_owned()),
            1,
            "codex-one-fingerprint",
        )
        .expect("first CLI projection should be valid");
        let second = AgentIdentityProjection::new(
            super::AgentIdentityId::new("C00000000000000000002").unwrap(),
            AgentIdentitySourceKind::CliRuntimeInstance,
            "Codex Two",
            "codex-two",
            None,
            Some("worker".to_owned()),
            1,
            "codex-two-fingerprint",
        )
        .expect("second CLI projection should be valid");
        assert_ne!(first.id, second.id);
        assert_ne!(first.nickname, second.nickname);
        assert!(
            AgentIdentityProjection::new(
                super::AgentIdentityId::new("C00000000000000000003").unwrap(),
                AgentIdentitySourceKind::CliRuntimeInstance,
                "Codex",
                "bad/nickname",
                None,
                None,
                1,
                "fingerprint",
            )
            .is_err()
        );
    }

    #[test]
    fn names_nicknames_and_roles_are_bounded_and_normalized() {
        assert_eq!(
            AgentNicknameKey::new("Worker.One").unwrap().as_str(),
            "worker.one"
        );
        assert!(AgentNicknameKey::new("x").is_err());
        assert!(AgentNicknameKey::new("bad/name").is_err());
        assert!(AgentDisplayName::new("  Native Reviewer  ").is_ok());
        assert!(AgentDisplayName::new("\n").is_err());
        assert!(AgentRoleLabel::new("reviewer").is_ok());
        assert!(AgentRoleLabel::new("\u{0000}").is_err());
    }
}
