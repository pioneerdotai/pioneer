use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    AuthDeviceCreateResponse, PrincipalId, PrincipalKind, PrincipalStatus, RoleKey, WorkspaceId,
};

pub const MEMBER_DISPLAY_NAME_MAX_SCALARS: usize = 128;
pub const MEMBER_DISPLAY_NAME_MAX_UTF8_BYTES: usize = 512;
pub const MEMBER_NICKNAME_MIN_LEN: usize = 2;
pub const MEMBER_NICKNAME_MAX_LEN: usize = 32;
pub const PROFILE_AVATAR_MAX_DECODED_BYTES: usize = 256 * 1024;
pub const PROFILE_AVATAR_MAX_DIMENSION: u32 = 1024;
pub const PROFILE_AVATAR_MAX_BASE64_LEN: usize = PROFILE_AVATAR_MAX_DECODED_BYTES.div_ceil(3) * 4;
pub const MEMBER_DIRECTORY_DEFAULT_LIMIT: u32 = 50;
pub const MEMBER_DIRECTORY_MAX_LIMIT: u32 = 100;
pub const MEMBER_DIRECTORY_CURSOR_MAX_BYTES: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum ProfileAvatarMediaType {
    #[serde(rename = "image/png")]
    Png,
    #[serde(rename = "image/jpeg")]
    Jpeg,
    #[serde(rename = "image/webp")]
    Webp,
}

impl ProfileAvatarMediaType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "ProfileAvatarInputWire")]
pub struct ProfileAvatarInput {
    pub media_type: ProfileAvatarMediaType,
    #[schemars(length(min = 1, max = 349528))]
    pub content_base64: String,
}

impl fmt::Debug for ProfileAvatarInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProfileAvatarInput")
            .field("media_type", &self.media_type)
            .field("content_base64", &"[redacted]")
            .finish()
    }
}

impl ProfileAvatarInput {
    pub fn new(
        media_type: ProfileAvatarMediaType,
        content_base64: impl Into<String>,
    ) -> Result<Self, MemberProfileValidationError> {
        let content_base64 = content_base64.into();
        if content_base64.is_empty() {
            return Err(MemberProfileValidationError::AvatarEmpty);
        }
        if content_base64.len() > PROFILE_AVATAR_MAX_BASE64_LEN {
            return Err(MemberProfileValidationError::AvatarEncodedTooLarge {
                maximum: PROFILE_AVATAR_MAX_BASE64_LEN,
                actual: content_base64.len(),
            });
        }
        Ok(Self {
            media_type,
            content_base64,
        })
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ProfileAvatarInputWire {
    media_type: ProfileAvatarMediaType,
    #[schemars(length(min = 1, max = 349528))]
    content_base64: String,
}

impl TryFrom<ProfileAvatarInputWire> for ProfileAvatarInput {
    type Error = MemberProfileValidationError;

    fn try_from(value: ProfileAvatarInputWire) -> Result<Self, Self::Error> {
        Self::new(value.media_type, value.content_base64)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "NewMemberProfileWire")]
pub struct NewMemberProfile {
    #[schemars(length(min = 1, max = 128))]
    pub display_name: String,
    #[schemars(
        length(min = 2, max = 32),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,31}$")
    )]
    pub nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar: Option<ProfileAvatarInput>,
}

impl NewMemberProfile {
    pub fn new(
        display_name: impl Into<String>,
        nickname: impl Into<String>,
        avatar: Option<ProfileAvatarInput>,
    ) -> Result<Self, MemberProfileValidationError> {
        let display_name = display_name.into().trim().to_owned();
        validate_display_name(&display_name)?;
        let nickname = nickname.into();
        validate_nickname(&nickname)?;
        Ok(Self {
            display_name,
            nickname,
            avatar,
        })
    }

    pub fn nickname_key(&self) -> String {
        self.nickname.to_ascii_lowercase()
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NewMemberProfileWire {
    #[schemars(length(min = 1, max = 128))]
    display_name: String,
    #[schemars(
        length(min = 2, max = 32),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,31}$")
    )]
    nickname: String,
    #[serde(default)]
    avatar: Option<ProfileAvatarInput>,
}

impl TryFrom<NewMemberProfileWire> for NewMemberProfile {
    type Error = MemberProfileValidationError;

    fn try_from(value: NewMemberProfileWire) -> Result<Self, Self::Error> {
        Self::new(value.display_name, value.nickname, value.avatar)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemberSummary {
    pub principal_id: PrincipalId,
    pub kind: PrincipalKind,
    pub display_name: String,
    pub nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_key: Option<RoleKey>,
    pub status: PrincipalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct MemberListParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl MemberListParams {
    pub fn validate(&self) -> Result<u32, MemberDirectoryParamsError> {
        validate_directory_page(self.cursor.as_deref(), self.limit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemberListResponse {
    pub members: Vec<MemberSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

macro_rules! member_lifecycle_params {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            pub principal_id: PrincipalId,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub expected_status: Option<PrincipalStatus>,
        }
    };
}

member_lifecycle_params!(MemberSuspendParams);
member_lifecycle_params!(MemberRestoreParams);
member_lifecycle_params!(MemberRemoveParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemberMutationResponse {
    pub member: MemberSummary,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemberDeviceCreateParams {
    pub principal_id: PrincipalId,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemberDeviceCreateResponse {
    pub principal_id: PrincipalId,
    pub activation: AuthDeviceCreateResponse,
}

impl fmt::Debug for MemberDeviceCreateResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemberDeviceCreateResponse")
            .field("principal_id", &self.principal_id)
            .field("activation", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceMemberListParams {
    pub workspace_id: WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl WorkspaceMemberListParams {
    pub fn validate(&self) -> Result<u32, MemberDirectoryParamsError> {
        validate_directory_page(self.cursor.as_deref(), self.limit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WorkspaceMemberListResponse {
    pub workspace_id: WorkspaceId,
    pub members: Vec<MemberSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceMemberAddParams {
    pub workspace_id: WorkspaceId,
    pub principal_id: PrincipalId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceMemberRemoveParams {
    pub workspace_id: WorkspaceId,
    pub principal_id: PrincipalId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WorkspaceMemberMutationResponse {
    pub workspace_id: WorkspaceId,
    pub member: MemberSummary,
    pub changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MemberManagementErrorReason {
    InvalidTarget,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemberChangedNotification {
    pub revision: u64,
    pub principal_id: PrincipalId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WorkspaceMembersChangedNotification {
    pub revision: u64,
    pub workspace_id: WorkspaceId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDirectoryParamsError {
    InvalidCursor,
    InvalidLimit,
}

impl fmt::Display for MemberDirectoryParamsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCursor => formatter.write_str("invalid member directory cursor"),
            Self::InvalidLimit => formatter.write_str("invalid member directory page limit"),
        }
    }
}

impl std::error::Error for MemberDirectoryParamsError {}

fn validate_directory_page(
    cursor: Option<&str>,
    limit: Option<u32>,
) -> Result<u32, MemberDirectoryParamsError> {
    if cursor
        .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MEMBER_DIRECTORY_CURSOR_MAX_BYTES)
    {
        return Err(MemberDirectoryParamsError::InvalidCursor);
    }
    let limit = limit.unwrap_or(MEMBER_DIRECTORY_DEFAULT_LIMIT);
    if !(1..=MEMBER_DIRECTORY_MAX_LIMIT).contains(&limit) {
        return Err(MemberDirectoryParamsError::InvalidLimit);
    }
    Ok(limit)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberProfileValidationError {
    DisplayNameEmpty,
    DisplayNameTooLong {
        maximum_scalars: usize,
        actual_scalars: usize,
    },
    DisplayNameTooManyBytes {
        maximum: usize,
        actual: usize,
    },
    DisplayNameControlCharacter {
        index: usize,
        character: char,
    },
    NicknameLength {
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    NicknameInvalidFirstCharacter {
        character: char,
    },
    NicknameInvalidCharacter {
        index: usize,
        character: char,
    },
    AvatarEmpty,
    AvatarEncodedTooLarge {
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Display for MemberProfileValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DisplayNameEmpty => formatter.write_str("display name must not be empty"),
            Self::DisplayNameTooLong {
                maximum_scalars,
                actual_scalars,
            } => write!(
                formatter,
                "display name must contain at most {maximum_scalars} Unicode scalar values, got {actual_scalars}"
            ),
            Self::DisplayNameTooManyBytes { maximum, actual } => write!(
                formatter,
                "display name must contain at most {maximum} UTF-8 bytes, got {actual}"
            ),
            Self::DisplayNameControlCharacter { index, character } => write!(
                formatter,
                "display name must not contain control characters; found {character:?} at byte {index}"
            ),
            Self::NicknameLength {
                minimum,
                maximum,
                actual,
            } => write!(
                formatter,
                "nickname must contain {minimum} to {maximum} ASCII characters, got {actual}"
            ),
            Self::NicknameInvalidFirstCharacter { character } => write!(
                formatter,
                "nickname must start with an ASCII letter or digit, got {character:?}"
            ),
            Self::NicknameInvalidCharacter { index, character } => write!(
                formatter,
                "nickname may contain only ASCII letters, digits, `_`, `.` and `-`; found {character:?} at byte {index}"
            ),
            Self::AvatarEmpty => formatter.write_str("avatar content must not be empty"),
            Self::AvatarEncodedTooLarge { maximum, actual } => write!(
                formatter,
                "avatar base64 must contain at most {maximum} bytes, got {actual}"
            ),
        }
    }
}

impl std::error::Error for MemberProfileValidationError {}

fn validate_display_name(value: &str) -> Result<(), MemberProfileValidationError> {
    if value.is_empty() {
        return Err(MemberProfileValidationError::DisplayNameEmpty);
    }
    let scalar_count = value.chars().count();
    if scalar_count > MEMBER_DISPLAY_NAME_MAX_SCALARS {
        return Err(MemberProfileValidationError::DisplayNameTooLong {
            maximum_scalars: MEMBER_DISPLAY_NAME_MAX_SCALARS,
            actual_scalars: scalar_count,
        });
    }
    if value.len() > MEMBER_DISPLAY_NAME_MAX_UTF8_BYTES {
        return Err(MemberProfileValidationError::DisplayNameTooManyBytes {
            maximum: MEMBER_DISPLAY_NAME_MAX_UTF8_BYTES,
            actual: value.len(),
        });
    }
    if let Some((index, character)) = value
        .char_indices()
        .find(|(_, character)| character.is_control())
    {
        return Err(MemberProfileValidationError::DisplayNameControlCharacter { index, character });
    }
    Ok(())
}

fn validate_nickname(value: &str) -> Result<(), MemberProfileValidationError> {
    let length = value.len();
    if !(MEMBER_NICKNAME_MIN_LEN..=MEMBER_NICKNAME_MAX_LEN).contains(&length) || !value.is_ascii() {
        return Err(MemberProfileValidationError::NicknameLength {
            minimum: MEMBER_NICKNAME_MIN_LEN,
            maximum: MEMBER_NICKNAME_MAX_LEN,
            actual: length,
        });
    }
    let first = value.chars().next().expect("bounded non-empty nickname");
    if !first.is_ascii_alphanumeric() {
        return Err(
            MemberProfileValidationError::NicknameInvalidFirstCharacter { character: first },
        );
    }
    if let Some((index, character)) = value.char_indices().find(|(_, character)| {
        !character.is_ascii_alphanumeric() && !matches!(character, '_' | '.' | '-')
    }) {
        return Err(MemberProfileValidationError::NicknameInvalidCharacter { index, character });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MEMBER_DIRECTORY_CURSOR_MAX_BYTES, MEMBER_DIRECTORY_DEFAULT_LIMIT,
        MEMBER_DIRECTORY_MAX_LIMIT, MEMBER_DISPLAY_NAME_MAX_SCALARS,
        MEMBER_DISPLAY_NAME_MAX_UTF8_BYTES, MEMBER_NICKNAME_MAX_LEN, MemberDeviceCreateParams,
        MemberListParams, MemberRemoveParams, MemberRestoreParams, MemberSuspendParams,
        NewMemberProfile, PROFILE_AVATAR_MAX_BASE64_LEN, PROFILE_AVATAR_MAX_DECODED_BYTES,
        PROFILE_AVATAR_MAX_DIMENSION, ProfileAvatarInput, ProfileAvatarMediaType,
        WorkspaceMemberAddParams, WorkspaceMemberListParams, WorkspaceMemberRemoveParams,
    };
    use serde_json::json;

    #[test]
    fn member_profile_normalizes_display_name_and_canonicalizes_nickname_key() {
        let profile = NewMemberProfile::new("  Александр  ", "Alex.Smith", None).unwrap();
        assert_eq!(profile.display_name, "Александр");
        assert_eq!(profile.nickname, "Alex.Smith");
        assert_eq!(profile.nickname_key(), "alex.smith");
        assert_eq!(MEMBER_DISPLAY_NAME_MAX_SCALARS, 128);
        assert_eq!(MEMBER_DISPLAY_NAME_MAX_UTF8_BYTES, 512);
        assert_eq!(MEMBER_NICKNAME_MAX_LEN, 32);
    }

    #[test]
    fn member_profile_rejects_controls_invalid_nicknames_and_unknown_fields() {
        assert!(NewMemberProfile::new("A\nB", "member", None).is_err());
        assert!(NewMemberProfile::new("Member", "_member", None).is_err());
        assert!(NewMemberProfile::new("Member", "mémber", None).is_err());
        assert!(
            serde_json::from_value::<NewMemberProfile>(json!({
                "display_name": "Member",
                "nickname": "member",
                "email": "not-supported@example.invalid"
            }))
            .is_err()
        );
    }

    #[test]
    fn avatar_input_is_media_typed_and_allocation_bounded() {
        assert_eq!(PROFILE_AVATAR_MAX_DECODED_BYTES, 262_144);
        assert_eq!(PROFILE_AVATAR_MAX_DIMENSION, 1024);
        let avatar = ProfileAvatarInput::new(ProfileAvatarMediaType::Png, "iVBORw0KGgo=")
            .expect("bounded avatar input");
        assert_eq!(avatar.media_type.as_str(), "image/png");
        assert!(ProfileAvatarInput::new(ProfileAvatarMediaType::Png, "").is_err());
        assert!(
            ProfileAvatarInput::new(
                ProfileAvatarMediaType::Webp,
                "A".repeat(PROFILE_AVATAR_MAX_BASE64_LEN + 1)
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<ProfileAvatarInput>(json!({
                "media_type": "image/png",
                "content_base64": "aQ==",
                "url": "https://example.invalid/avatar.png"
            }))
            .is_err()
        );
    }

    #[test]
    fn member_request_dtos_enforce_bounds_and_reject_unknown_fields() {
        assert_eq!(
            MemberListParams::default().validate().unwrap(),
            MEMBER_DIRECTORY_DEFAULT_LIMIT
        );
        assert!(
            MemberListParams {
                limit: Some(0),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            MemberListParams {
                limit: Some(MEMBER_DIRECTORY_MAX_LIMIT + 1),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            MemberListParams {
                cursor: Some("x".repeat(MEMBER_DIRECTORY_CURSOR_MAX_BYTES + 1)),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        let workspace_page: WorkspaceMemberListParams = serde_json::from_value(json!({
            "workspace_id": "W00000000000000000001",
            "limit": MEMBER_DIRECTORY_MAX_LIMIT
        }))
        .unwrap();
        assert_eq!(
            workspace_page.validate().unwrap(),
            MEMBER_DIRECTORY_MAX_LIMIT
        );

        let principal = "P00000000000000000001";
        let workspace = "W00000000000000000001";
        let values = [
            serde_json::from_value::<MemberListParams>(json!({ "extra": true })).is_err(),
            serde_json::from_value::<MemberSuspendParams>(json!({
                "principal_id": principal,
                "force": true
            }))
            .is_err(),
            serde_json::from_value::<MemberRestoreParams>(json!({
                "principal_id": principal,
                "force": true
            }))
            .is_err(),
            serde_json::from_value::<MemberRemoveParams>(json!({
                "principal_id": principal,
                "force": true
            }))
            .is_err(),
            serde_json::from_value::<MemberDeviceCreateParams>(json!({
                "principal_id": principal,
                "admin": true
            }))
            .is_err(),
            serde_json::from_value::<WorkspaceMemberAddParams>(json!({
                "workspace_id": workspace,
                "principal_id": principal,
                "role": "owner"
            }))
            .is_err(),
            serde_json::from_value::<WorkspaceMemberRemoveParams>(json!({
                "workspace_id": workspace,
                "principal_id": principal,
                "force": true
            }))
            .is_err(),
        ];
        assert!(values.into_iter().all(|rejected| rejected));
    }
}
