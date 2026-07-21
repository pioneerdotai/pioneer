use nanoid::nanoid;
use schemars::JsonSchema;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::str::FromStr;

pub const SKILL_ID_LEN: usize = 21;

const ALPHANUMERIC: [char; 62] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '1', '2', '3', '4', '5',
    '6', '7', '8', '9', '0',
];

pub fn generate_id(len: usize) -> String {
    nanoid!(len, &ALPHANUMERIC)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct SkillId(#[schemars(length(equal = 21), regex(pattern = r"^[A-Za-z0-9]{21}$"))] String);

impl SkillId {
    pub fn new(value: impl Into<String>) -> Result<Self, SkillIdError> {
        let value = value.into();
        let actual = value.chars().count();
        if actual != SKILL_ID_LEN {
            return Err(SkillIdError::InvalidLength {
                expected: SKILL_ID_LEN,
                actual,
            });
        }

        if let Some((index, character)) = value
            .char_indices()
            .find(|(_, character)| !character.is_ascii_alphanumeric())
        {
            return Err(SkillIdError::InvalidCharacter { index, character });
        }

        Ok(Self(value))
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

#[cfg(test)]
mod tests {
    use super::{SKILL_ID_LEN, SkillId, SkillIdError, generate_id};
    use serde_json::json;

    #[test]
    fn generate_id_produces_alphanumeric_with_requested_length() {
        let id = generate_id(SKILL_ID_LEN);
        assert_eq!(id.len(), SKILL_ID_LEN);
        assert!(id.chars().all(|value| value.is_ascii_alphanumeric()));
        assert!(SkillId::new(id).is_ok());
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
}
