use nanoid::nanoid;
use schemars::JsonSchema;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::str::FromStr;

pub const SKILL_ID_LEN: usize = 21;
pub const SKILL_PACK_ID_LEN: usize = SKILL_ID_LEN;
pub const GATEWAY_ID_LEN: usize = 21;
pub const PRINCIPAL_ID_LEN: usize = 21;
pub const AUTH_DOMAIN_ID_LEN: usize = 21;

const ALPHANUMERIC: [char; 62] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '1', '2', '3', '4', '5',
    '6', '7', '8', '9', '0',
];

pub fn generate_id(len: usize) -> String {
    nanoid!(len, &ALPHANUMERIC)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CheckedIdValidationError {
    InvalidLength { expected: usize, actual: usize },
    InvalidCharacter { index: usize, character: char },
}

fn validate_checked_id(
    value: String,
    expected_len: usize,
) -> Result<String, CheckedIdValidationError> {
    let actual = value.chars().count();
    if actual != expected_len {
        return Err(CheckedIdValidationError::InvalidLength {
            expected: expected_len,
            actual,
        });
    }

    if let Some((index, character)) = value
        .char_indices()
        .find(|(_, character)| !character.is_ascii_alphanumeric())
    {
        return Err(CheckedIdValidationError::InvalidCharacter { index, character });
    }

    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct SkillId(#[schemars(length(equal = 21), regex(pattern = r"^[A-Za-z0-9]{21}$"))] String);

impl SkillId {
    pub fn new(value: impl Into<String>) -> Result<Self, SkillIdError> {
        validate_checked_id(value.into(), SKILL_ID_LEN)
            .map(Self)
            .map_err(SkillIdError::from)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for SkillId {
    type Err = SkillIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for SkillId {
    type Error = SkillIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for SkillId {
    type Error = SkillIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<str> for SkillId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for SkillId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SkillId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillIdError {
    InvalidLength { expected: usize, actual: usize },
    InvalidCharacter { index: usize, character: char },
}

impl fmt::Display for SkillIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { expected, actual } => {
                write!(
                    f,
                    "skill id must be exactly {expected} characters, got {actual}"
                )
            }
            Self::InvalidCharacter { index, character } => write!(
                f,
                "skill id must contain only ASCII alphanumeric characters; found {character:?} at byte {index}"
            ),
        }
    }
}

impl std::error::Error for SkillIdError {}

impl From<CheckedIdValidationError> for SkillIdError {
    fn from(error: CheckedIdValidationError) -> Self {
        match error {
            CheckedIdValidationError::InvalidLength { expected, actual } => {
                Self::InvalidLength { expected, actual }
            }
            CheckedIdValidationError::InvalidCharacter { index, character } => {
                Self::InvalidCharacter { index, character }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct SkillPackId(
    #[schemars(length(equal = 21), regex(pattern = r"^[A-Za-z0-9]{21}$"))] String,
);

impl SkillPackId {
    pub fn new(value: impl Into<String>) -> Result<Self, SkillPackIdError> {
        validate_checked_id(value.into(), SKILL_PACK_ID_LEN)
            .map(Self)
            .map_err(SkillPackIdError::from)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for SkillPackId {
    type Err = SkillPackIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for SkillPackId {
    type Error = SkillPackIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for SkillPackId {
    type Error = SkillPackIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<str> for SkillPackId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for SkillPackId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SkillPackId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillPackIdError {
    InvalidLength { expected: usize, actual: usize },
    InvalidCharacter { index: usize, character: char },
}

impl fmt::Display for SkillPackIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { expected, actual } => write!(
                f,
                "skill pack id must be exactly {expected} characters, got {actual}"
            ),
            Self::InvalidCharacter { index, character } => write!(
                f,
                "skill pack id must contain only ASCII alphanumeric characters; found {character:?} at byte {index}"
            ),
        }
    }
}

impl std::error::Error for SkillPackIdError {}

impl From<CheckedIdValidationError> for SkillPackIdError {
    fn from(error: CheckedIdValidationError) -> Self {
        match error {
            CheckedIdValidationError::InvalidLength { expected, actual } => {
                Self::InvalidLength { expected, actual }
            }
            CheckedIdValidationError::InvalidCharacter { index, character } => {
                Self::InvalidCharacter { index, character }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct GatewayId(#[schemars(length(equal = 21), regex(pattern = r"^[A-Za-z0-9]{21}$"))] String);

impl GatewayId {
    pub fn new(value: impl Into<String>) -> Result<Self, GatewayIdError> {
        validate_checked_id(value.into(), GATEWAY_ID_LEN)
            .map(Self)
            .map_err(GatewayIdError::from)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for GatewayId {
    type Err = GatewayIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for GatewayId {
    type Error = GatewayIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for GatewayId {
    type Error = GatewayIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<str> for GatewayId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for GatewayId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GatewayId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayIdError {
    InvalidLength { expected: usize, actual: usize },
    InvalidCharacter { index: usize, character: char },
}

impl fmt::Display for GatewayIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { expected, actual } => write!(
                f,
                "gateway id must be exactly {expected} characters, got {actual}"
            ),
            Self::InvalidCharacter { index, character } => write!(
                f,
                "gateway id must contain only ASCII alphanumeric characters; found {character:?} at byte {index}"
            ),
        }
    }
}

impl std::error::Error for GatewayIdError {}

impl From<CheckedIdValidationError> for GatewayIdError {
    fn from(error: CheckedIdValidationError) -> Self {
        match error {
            CheckedIdValidationError::InvalidLength { expected, actual } => {
                Self::InvalidLength { expected, actual }
            }
            CheckedIdValidationError::InvalidCharacter { index, character } => {
                Self::InvalidCharacter { index, character }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct PrincipalId(
    #[schemars(length(equal = 21), regex(pattern = r"^[A-Za-z0-9]{21}$"))] String,
);

impl PrincipalId {
    pub fn new(value: impl Into<String>) -> Result<Self, PrincipalIdError> {
        validate_checked_id(value.into(), PRINCIPAL_ID_LEN)
            .map(Self)
            .map_err(PrincipalIdError::from)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for PrincipalId {
    type Err = PrincipalIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for PrincipalId {
    type Error = PrincipalIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for PrincipalId {
    type Error = PrincipalIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<str> for PrincipalId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PrincipalId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrincipalIdError {
    InvalidLength { expected: usize, actual: usize },
    InvalidCharacter { index: usize, character: char },
}

impl fmt::Display for PrincipalIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { expected, actual } => write!(
                f,
                "principal id must be exactly {expected} characters, got {actual}"
            ),
            Self::InvalidCharacter { index, character } => write!(
                f,
                "principal id must contain only ASCII alphanumeric characters; found {character:?} at byte {index}"
            ),
        }
    }
}

impl std::error::Error for PrincipalIdError {}

impl From<CheckedIdValidationError> for PrincipalIdError {
    fn from(error: CheckedIdValidationError) -> Self {
        match error {
            CheckedIdValidationError::InvalidLength { expected, actual } => {
                Self::InvalidLength { expected, actual }
            }
            CheckedIdValidationError::InvalidCharacter { index, character } => {
                Self::InvalidCharacter { index, character }
            }
        }
    }
}

macro_rules! auth_domain_id {
    ($name:ident, $label:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
        #[serde(transparent)]
        pub struct $name(
            #[schemars(length(equal = 21), regex(pattern = r"^[A-Za-z0-9]{21}$"))] String,
        );

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, AuthDomainIdError> {
                validate_checked_id(value.into(), AUTH_DOMAIN_ID_LEN)
                    .map(Self)
                    .map_err(|error| AuthDomainIdError::from_validation($label, error))
            }

            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl FromStr for $name {
            type Err = AuthDomainIdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = AuthDomainIdError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = AuthDomainIdError;

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
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(D::Error::custom)
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthDomainIdError {
    InvalidLength {
        domain: &'static str,
        expected: usize,
        actual: usize,
    },
    InvalidCharacter {
        domain: &'static str,
        index: usize,
        character: char,
    },
}

impl AuthDomainIdError {
    fn from_validation(domain: &'static str, error: CheckedIdValidationError) -> Self {
        match error {
            CheckedIdValidationError::InvalidLength { expected, actual } => Self::InvalidLength {
                domain,
                expected,
                actual,
            },
            CheckedIdValidationError::InvalidCharacter { index, character } => {
                Self::InvalidCharacter {
                    domain,
                    index,
                    character,
                }
            }
        }
    }
}

impl fmt::Display for AuthDomainIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength {
                domain,
                expected,
                actual,
            } => write!(
                formatter,
                "{domain} must be exactly {expected} characters, got {actual}"
            ),
            Self::InvalidCharacter {
                domain,
                index,
                character,
            } => write!(
                formatter,
                "{domain} must contain only ASCII alphanumeric characters; found {character:?} at byte {index}"
            ),
        }
    }
}

impl std::error::Error for AuthDomainIdError {}

auth_domain_id!(DeviceId, "device id");
auth_domain_id!(AuthSessionId, "auth session id");
auth_domain_id!(RefreshCredentialId, "refresh credential id");
auth_domain_id!(TokenFamilyId, "token family id");

#[cfg(test)]
mod tests {
    use super::{
        AUTH_DOMAIN_ID_LEN, AuthSessionId, DeviceId, GATEWAY_ID_LEN, GatewayId, GatewayIdError,
        PRINCIPAL_ID_LEN, PrincipalId, PrincipalIdError, RefreshCredentialId, SKILL_ID_LEN,
        SKILL_PACK_ID_LEN, SkillId, SkillIdError, SkillPackId, SkillPackIdError, TokenFamilyId,
        generate_id,
    };
    use serde_json::json;

    #[test]
    fn generate_id_produces_alphanumeric_with_requested_length() {
        let id = generate_id(SKILL_ID_LEN);
        assert_eq!(id.len(), SKILL_ID_LEN);
        assert!(id.chars().all(|value| value.is_ascii_alphanumeric()));
        assert!(SkillId::new(id).is_ok());
    }

    #[test]
    fn gateway_and_principal_ids_round_trip_as_validated_strings() {
        let gateway = GatewayId::new("G00000000000000000001").expect("valid gateway id");
        let principal = PrincipalId::new("P00000000000000000001").expect("valid principal id");

        assert_eq!(gateway.as_str(), "G00000000000000000001");
        assert_eq!(principal.as_str(), "P00000000000000000001");
        assert_eq!(
            "G00000000000000000001".parse::<GatewayId>().unwrap(),
            gateway
        );
        assert_eq!(
            PrincipalId::try_from("P00000000000000000001").unwrap(),
            principal
        );
        assert_eq!(
            serde_json::from_value::<GatewayId>(json!("G00000000000000000001")).unwrap(),
            gateway
        );
        assert_eq!(
            serde_json::from_value::<PrincipalId>(json!("P00000000000000000001")).unwrap(),
            principal
        );
    }

    #[test]
    fn gateway_and_principal_ids_reject_invalid_lengths_and_characters() {
        assert_eq!(
            GatewayId::new("short"),
            Err(GatewayIdError::InvalidLength {
                expected: GATEWAY_ID_LEN,
                actual: 5,
            })
        );
        assert_eq!(
            PrincipalId::new("short"),
            Err(PrincipalIdError::InvalidLength {
                expected: PRINCIPAL_ID_LEN,
                actual: 5,
            })
        );

        for value in [
            "G0000000000000000000-",
            "G0000000000000000000 ",
            "G0000000000000000000é",
        ] {
            assert!(GatewayId::new(value).is_err());
            assert!(serde_json::from_value::<GatewayId>(json!(value)).is_err());
        }
        for value in [
            "P0000000000000000000-",
            "P0000000000000000000 ",
            "P0000000000000000000é",
        ] {
            assert!(PrincipalId::new(value).is_err());
            assert!(serde_json::from_value::<PrincipalId>(json!(value)).is_err());
        }
    }

    #[test]
    fn gateway_and_principal_id_schemas_are_constrained_strings() {
        for schema in [
            serde_json::to_value(schemars::schema_for!(GatewayId)).unwrap(),
            serde_json::to_value(schemars::schema_for!(PrincipalId)).unwrap(),
        ] {
            assert_eq!(schema["type"], "string");
            assert_eq!(schema["minLength"], 21);
            assert_eq!(schema["maxLength"], 21);
            assert_eq!(schema["pattern"], "^[A-Za-z0-9]{21}$");
        }
    }

    #[test]
    fn auth_domain_ids_are_distinct_constrained_string_types() {
        let raw = "D00000000000000000001";
        let device = DeviceId::new(raw).expect("device id");
        let session = AuthSessionId::new(raw).expect("session id");
        let refresh = RefreshCredentialId::new(raw).expect("refresh id");
        let family = TokenFamilyId::new(raw).expect("family id");

        assert_eq!(device.as_str(), raw);
        assert_eq!(session.as_str(), raw);
        assert_eq!(refresh.as_str(), raw);
        assert_eq!(family.as_str(), raw);
        assert!(DeviceId::new("short").is_err());
        assert!(AuthSessionId::new("D0000000000000000000-").is_err());
        assert_eq!(AUTH_DOMAIN_ID_LEN, 21);

        for schema in [
            serde_json::to_value(schemars::schema_for!(DeviceId)).unwrap(),
            serde_json::to_value(schemars::schema_for!(AuthSessionId)).unwrap(),
            serde_json::to_value(schemars::schema_for!(RefreshCredentialId)).unwrap(),
            serde_json::to_value(schemars::schema_for!(TokenFamilyId)).unwrap(),
        ] {
            assert_eq!(schema["minLength"], 21);
            assert_eq!(schema["maxLength"], 21);
            assert_eq!(schema["pattern"], "^[A-Za-z0-9]{21}$");
        }
    }

    #[test]
    fn skill_id_round_trips_as_a_string() {
        let value = "AbCdEfGhIjKlMnOpQr123";
        let id = SkillId::new(value).expect("valid skill id");

        assert_eq!(id.as_str(), value);
        assert_eq!(id.to_string(), value);
        assert_eq!(serde_json::to_value(&id).unwrap(), json!(value));
        assert_eq!(serde_json::from_value::<SkillId>(json!(value)).unwrap(), id);
    }

    #[test]
    fn skill_id_rejects_invalid_lengths() {
        assert_eq!(
            SkillId::new("short"),
            Err(SkillIdError::InvalidLength {
                expected: SKILL_ID_LEN,
                actual: 5,
            })
        );
        assert!(matches!(
            SkillId::new("AbCdEfGhIjKlMnOpQr1234"),
            Err(SkillIdError::InvalidLength { .. })
        ));
        assert!(matches!(
            SkillId::new(""),
            Err(SkillIdError::InvalidLength { .. })
        ));
    }

    #[test]
    fn skill_id_rejects_punctuation_whitespace_and_unicode() {
        for value in [
            "AbCdEfGhIjKlMnOpQr12-",
            "AbCdEfGhIjKlMnOpQr12 ",
            "AbCdEfGhIjKlMnOpQr12é",
        ] {
            assert!(
                SkillId::new(value).is_err(),
                "expected {value:?} to be rejected"
            );
            assert!(
                serde_json::from_value::<SkillId>(json!(value)).is_err(),
                "expected serialized {value:?} to be rejected"
            );
        }
    }

    #[test]
    fn skill_id_schema_is_a_constrained_string() {
        let schema = serde_json::to_value(schemars::schema_for!(SkillId)).unwrap();

        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], SKILL_ID_LEN);
        assert_eq!(schema["maxLength"], SKILL_ID_LEN);
        assert_eq!(schema["pattern"], "^[A-Za-z0-9]{21}$");
    }

    #[test]
    fn skill_pack_id_round_trips_as_a_string() {
        let value = "ZyXwVuTsRqPoNmLkJi987";
        let id = SkillPackId::new(value).expect("valid skill pack id");

        assert_eq!(id.as_str(), value);
        assert_eq!(id.to_string(), value);
        assert_eq!(serde_json::to_value(&id).unwrap(), json!(value));
        assert_eq!(
            serde_json::from_value::<SkillPackId>(json!(value)).unwrap(),
            id
        );
    }

    #[test]
    fn skill_pack_id_rejects_invalid_lengths() {
        assert_eq!(
            SkillPackId::new("short"),
            Err(SkillPackIdError::InvalidLength {
                expected: SKILL_PACK_ID_LEN,
                actual: 5,
            })
        );
        assert!(matches!(
            SkillPackId::new("ZyXwVuTsRqPoNmLkJi9876"),
            Err(SkillPackIdError::InvalidLength { .. })
        ));
        assert!(matches!(
            SkillPackId::new(""),
            Err(SkillPackIdError::InvalidLength { .. })
        ));
    }

    #[test]
    fn skill_pack_id_rejects_punctuation_whitespace_and_unicode() {
        for value in [
            "ZyXwVuTsRqPoNmLkJi98-",
            "ZyXwVuTsRqPoNmLkJi98 ",
            "ZyXwVuTsRqPoNmLkJi98é",
        ] {
            assert!(
                SkillPackId::new(value).is_err(),
                "expected {value:?} to be rejected"
            );
            assert!(
                serde_json::from_value::<SkillPackId>(json!(value)).is_err(),
                "expected serialized {value:?} to be rejected"
            );
        }
    }

    #[test]
    fn skill_pack_id_schema_is_a_constrained_string() {
        let schema = serde_json::to_value(schemars::schema_for!(SkillPackId)).unwrap();

        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], SKILL_PACK_ID_LEN);
        assert_eq!(schema["maxLength"], SKILL_PACK_ID_LEN);
        assert_eq!(schema["pattern"], "^[A-Za-z0-9]{21}$");
    }
}
