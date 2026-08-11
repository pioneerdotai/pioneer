use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use crate::{
    AuthSessionId, DeviceId, GatewayBaseUrl, GatewayId, NewMemberProfile, PioneerAppUrlScheme,
    PrincipalId, PrincipalKind, ProfileAvatarInput, RoleKey, TokenFamilyId,
};

pub const REFRESH_CREDENTIAL_PREFIX: &str = "prf2_";
pub const REFRESH_CREDENTIAL_BODY_LEN: usize = 164;
pub const DEVICE_SESSION_AUTH_PROTOCOL_VERSION: u32 = 3;
pub const DEVICE_ACTIVATION_CODE_SYMBOLS: usize = 8;
pub const DEVICE_ACTIVATION_LOCATOR_SYMBOLS: usize = 1;
pub const DEVICE_ACTIVATION_MAX_FAILED_ATTEMPTS: u32 = 5;
pub const DEVICE_ACTIVATION_ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
pub const MAX_PROTECTED_GATEWAY_URI_BYTES: usize = 8_192;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ClientKind {
    Desktop,
    Mobile,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeviceStatus {
    Pending,
    Active,
    Revoked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthSessionStatus {
    Pending,
    Active,
    Revoked,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthSessionRevokeReason {
    Logout,
    SelfRevoke,
    DeviceRevoke,
    ActivationAttemptsExceeded,
    RefreshReuse,
    PrincipalSuspended,
    PrincipalRemoved,
    Superseded,
    SecurityReset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthSessionTerminationReason {
    SessionRevoked,
    SessionExpired,
    SessionCompromised,
    PrincipalSuspended,
    PrincipalRemoved,
}

impl AuthSessionTerminationReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionRevoked => "session_revoked",
            Self::SessionExpired => "session_expired",
            Self::SessionCompromised => "session_compromised",
            Self::PrincipalSuspended => "principal_suspended",
            Self::PrincipalRemoved => "principal_removed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthCredentialPurpose {
    Access,
    Refresh,
    DeviceActivation,
    Invitation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthGatewaySnapshot {
    pub id: GatewayId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthPrincipalSnapshot {
    pub id: PrincipalId,
    pub kind: PrincipalKind,
    pub display_name: String,
    pub nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthDeviceSnapshot {
    pub id: DeviceId,
    pub installation_id: String,
    pub display_name: String,
    pub client_kind: ClientKind,
    pub status: DeviceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthSessionSnapshot {
    pub id: AuthSessionId,
    pub device_id: DeviceId,
    pub token_family_id: TokenFamilyId,
    pub status: AuthSessionStatus,
    pub refresh_generation: u64,
    pub refresh_expires_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthMeResponse {
    pub gateway: AuthGatewaySnapshot,
    pub principal: AuthPrincipalSnapshot,
    pub device: AuthDeviceSnapshot,
    pub session: AuthSessionSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_key: Option<RoleKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum AuthProfileAvatarUpdate {
    #[default]
    Unchanged,
    Remove,
    Set {
        avatar: ProfileAvatarInput,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "AuthProfileUpdateParamsWire")]
pub struct AuthProfileUpdateParams {
    pub display_name: String,
    pub nickname: String,
    pub avatar: AuthProfileAvatarUpdate,
}

impl AuthProfileUpdateParams {
    pub fn new(
        display_name: impl Into<String>,
        nickname: impl Into<String>,
        avatar: AuthProfileAvatarUpdate,
    ) -> Result<Self, crate::MemberProfileValidationError> {
        let normalized = NewMemberProfile::new(display_name, nickname, None)?;
        Ok(Self {
            display_name: normalized.display_name,
            nickname: normalized.nickname,
            avatar,
        })
    }

    pub fn nickname_key(&self) -> String {
        self.nickname.to_ascii_lowercase()
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AuthProfileUpdateParamsWire {
    #[schemars(length(min = 1, max = 128))]
    display_name: String,
    #[schemars(
        length(min = 2, max = 32),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,31}$")
    )]
    nickname: String,
    #[serde(default)]
    avatar: AuthProfileAvatarUpdate,
}

impl TryFrom<AuthProfileUpdateParamsWire> for AuthProfileUpdateParams {
    type Error = crate::MemberProfileValidationError;

    fn try_from(value: AuthProfileUpdateParamsWire) -> Result<Self, Self::Error> {
        Self::new(value.display_name, value.nickname, value.avatar)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthProfileUpdateResponse {
    pub principal: AuthPrincipalSnapshot,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthSessionListItem {
    pub device: AuthDeviceSnapshot,
    pub session: AuthSessionSnapshot,
    pub current: bool,
    pub last_seen_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthSessionListResponse {
    pub sessions: Vec<AuthSessionListItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthSessionRevokeParams {
    pub session_id: AuthSessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_status: Option<AuthSessionStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthSessionRevokeResponse {
    pub session_id: AuthSessionId,
    pub revoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthLogoutResponse {
    pub session_id: AuthSessionId,
    pub revoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthSessionRevokedNotification {
    pub session_id: AuthSessionId,
    pub reason: AuthSessionTerminationReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthAccessExpiringNotification {
    pub session_id: AuthSessionId,
    pub access_expires_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClientInstallationDescriptor {
    pub installation_id: String,
    pub display_name: String,
    pub client_kind: ClientKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthRefreshParams {
    pub refresh_request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct AuthSecretString(String);

impl AuthSecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for AuthSecretString {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[redacted]")
    }
}

impl Drop for AuthSecretString {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStorageOrder {
    PersistRefreshBeforeActivatingAccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthSessionGrant {
    pub gateway: AuthGatewaySnapshot,
    pub principal: AuthPrincipalSnapshot,
    pub device: AuthDeviceSnapshot,
    pub session: AuthSessionSnapshot,
    pub access_token: AuthSecretString,
    pub access_expires_at_unix: u64,
    pub refresh_token: AuthSecretString,
    pub refresh_expires_at_unix: u64,
    pub refresh_generation: u64,
    pub auth_protocol_version: u32,
    pub credential_storage_order: CredentialStorageOrder,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthRefreshGrant {
    pub gateway: AuthGatewaySnapshot,
    pub principal: AuthPrincipalSnapshot,
    pub access_token: AuthSecretString,
    pub access_expires_at_unix: u64,
    pub refresh_token: AuthSecretString,
    pub refresh_expires_at_unix: u64,
    pub refresh_generation: u64,
    pub session: AuthSessionSnapshot,
    pub device: AuthDeviceSnapshot,
    pub auth_protocol_version: u32,
    pub credential_storage_order: CredentialStorageOrder,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthDeviceCreateResponse {
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub activation_code: AuthSecretString,
    pub expires_at_unix: u64,
    pub gateway_id: GatewayId,
}

impl std::fmt::Debug for AuthDeviceCreateResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthDeviceCreateResponse")
            .field("device_id", &self.device_id)
            .field("session_id", &self.session_id)
            .field("activation_code", &"[redacted]")
            .field("expires_at_unix", &self.expires_at_unix)
            .field("gateway_id", &self.gateway_id)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthDeviceActivateParams {
    pub installation: ClientInstallationDescriptor,
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthDeviceActivationPresentation {
    pub gateway_base_url: GatewayBaseUrl,
    pub gateway_id: GatewayId,
    activation_code: AuthSecretString,
    app_url_scheme: PioneerAppUrlScheme,
}

impl AuthDeviceActivationPresentation {
    pub fn new(
        gateway_base_url: GatewayBaseUrl,
        gateway_id: GatewayId,
        activation_code: impl Into<String>,
    ) -> Result<Self, String> {
        Self::new_with_scheme(
            gateway_base_url,
            gateway_id,
            activation_code,
            PioneerAppUrlScheme::Production,
        )
    }

    pub fn new_with_scheme(
        gateway_base_url: GatewayBaseUrl,
        gateway_id: GatewayId,
        activation_code: impl Into<String>,
        app_url_scheme: PioneerAppUrlScheme,
    ) -> Result<Self, String> {
        let activation_code = AuthSecretString::new(format_device_activation_code(
            activation_code.into().as_str(),
        )?);
        Ok(Self {
            gateway_base_url,
            gateway_id,
            activation_code,
            app_url_scheme,
        })
    }

    pub fn activation_code(&self) -> &str {
        self.activation_code.expose_secret()
    }

    pub fn app_url_scheme(&self) -> PioneerAppUrlScheme {
        self.app_url_scheme
    }

    pub fn to_uri(&self) -> String {
        let mut uri =
            url::Url::parse(format!("{}://activate", self.app_url_scheme.as_str()).as_str())
                .expect("Pioneer activation URI is valid");
        uri.query_pairs_mut()
            .append_pair("gateway_base_url", self.gateway_base_url.as_str())
            .append_pair("gateway_id", self.gateway_id.as_str());
        let fragment = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("code", self.activation_code.expose_secret())
            .finish();
        uri.set_fragment(Some(fragment.as_str()));
        uri.to_string()
    }

    pub fn parse(uri: &str) -> Result<Self, String> {
        if uri.is_empty() || uri.len() > MAX_PROTECTED_GATEWAY_URI_BYTES {
            return Err("invalid device activation URI length".to_owned());
        }
        let parsed =
            url::Url::parse(uri).map_err(|_| "invalid device activation URI".to_owned())?;
        let app_url_scheme = PioneerAppUrlScheme::parse(parsed.scheme())
            .ok_or_else(|| "invalid device activation URI scheme".to_owned())?;
        if parsed.host_str() != Some("activate")
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || !matches!(parsed.path(), "" | "/")
        {
            return Err("invalid device activation URI target".to_owned());
        }
        let mut gateway_base_url = None;
        let mut gateway_id = None;
        for (key, value) in parsed.query_pairs() {
            match key.as_ref() {
                "gateway_base_url" if gateway_base_url.is_none() => {
                    gateway_base_url = Some(
                        GatewayBaseUrl::parse_presentation(value.as_ref())
                            .map_err(|_| "device activation URI has an invalid Gateway base URL")?,
                    );
                }
                "gateway_base_url" => {
                    return Err("device activation URI contains duplicate fields".to_owned());
                }
                "gateway_id" if gateway_id.is_none() => {
                    gateway_id = Some(
                        GatewayId::new(value.into_owned())
                            .map_err(|_| "device activation URI has an invalid Gateway id")?,
                    );
                }
                "gateway_id" => {
                    return Err("device activation URI contains duplicate fields".to_owned());
                }
                "code" => {
                    return Err(
                        "device activation code must be carried only in the URI fragment"
                            .to_owned(),
                    );
                }
                _ => return Err("device activation URI contains an unsupported field".to_owned()),
            }
        }
        let fragment = parsed
            .fragment()
            .ok_or_else(|| "device activation URI has no code fragment".to_owned())?;
        let mut code = None;
        for (key, value) in url::form_urlencoded::parse(fragment.as_bytes()) {
            match key.as_ref() {
                "code" if code.is_none() => code = Some(value.into_owned()),
                "code" => {
                    return Err("device activation URI contains duplicate code fields".to_owned());
                }
                _ => {
                    return Err(
                        "device activation URI fragment contains an unsupported field".to_owned(),
                    );
                }
            }
        }
        Self::new_with_scheme(
            gateway_base_url
                .ok_or_else(|| "device activation URI has no Gateway base URL".to_owned())?,
            gateway_id.ok_or_else(|| "device activation URI has no Gateway id".to_owned())?,
            code.ok_or_else(|| "device activation URI has no code fragment".to_owned())?,
            app_url_scheme,
        )
    }
}

/// Normalizes a user-entered Crockford Base32 activation-code prefix.
///
/// Lowercase input, the conventional Crockford aliases (`O` → `0`,
/// `I`/`L` → `1`), and one visual separator are accepted. The returned value
/// is always uppercase, ungrouped, and suitable for cryptographic
/// fingerprinting. Empty and partial values are valid for interactive inputs.
pub fn normalize_device_activation_code_input(value: &str) -> Result<String, String> {
    let mut normalized = String::with_capacity(DEVICE_ACTIVATION_CODE_SYMBOLS);
    let mut saw_separator = false;
    for character in value.chars() {
        if character == '-' {
            if saw_separator || normalized.len() != DEVICE_ACTIVATION_CODE_SYMBOLS / 2 {
                return Err("device activation code has an invalid separator".to_owned());
            }
            saw_separator = true;
            continue;
        }
        if !character.is_ascii() || normalized.len() >= DEVICE_ACTIVATION_CODE_SYMBOLS {
            return Err("device activation code has an invalid length".to_owned());
        }
        let canonical = match character.to_ascii_uppercase() {
            'O' => '0',
            'I' | 'L' => '1',
            value
                if DEVICE_ACTIVATION_ALPHABET
                    .iter()
                    .any(|candidate| char::from(*candidate) == value) =>
            {
                value
            }
            _ => return Err("device activation code contains an invalid symbol".to_owned()),
        };
        normalized.push(canonical);
    }
    Ok(normalized)
}

pub fn normalize_device_activation_code(value: &str) -> Result<String, String> {
    let normalized = normalize_device_activation_code_input(value)?;
    if normalized.len() != DEVICE_ACTIVATION_CODE_SYMBOLS {
        return Err("device activation code must contain exactly 8 symbols".to_owned());
    }
    Ok(normalized)
}

pub fn format_device_activation_code(value: &str) -> Result<String, String> {
    let canonical = normalize_device_activation_code(value)?;
    Ok(format!(
        "{}-{}",
        &canonical[..DEVICE_ACTIVATION_CODE_SYMBOLS / 2],
        &canonical[DEVICE_ACTIVATION_CODE_SYMBOLS / 2..]
    ))
}

pub fn device_activation_locator(value: &str) -> Result<String, String> {
    let canonical = normalize_device_activation_code(value)?;
    Ok(canonical[..DEVICE_ACTIVATION_LOCATOR_SYMBOLS].to_owned())
}

pub fn encode_device_activation_entropy(entropy: [u8; 5]) -> String {
    let value = u64::from_be_bytes([
        0, 0, 0, entropy[0], entropy[1], entropy[2], entropy[3], entropy[4],
    ]);
    let mut encoded = String::with_capacity(DEVICE_ACTIVATION_CODE_SYMBOLS);
    for shift in (0..40).step_by(5).rev() {
        let index = usize::try_from((value >> shift) & 0x1f)
            .expect("five-bit Crockford index always fits usize");
        encoded.push(char::from(DEVICE_ACTIVATION_ALPHABET[index]));
    }
    encoded
}

impl std::fmt::Debug for AuthDeviceActivationPresentation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthDeviceActivationPresentation")
            .field("gateway_base_url", &self.gateway_base_url)
            .field("gateway_id", &self.gateway_id)
            .field("activation_code", &"[redacted]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_enums_use_bounded_snake_case_wire_names() {
        assert_eq!(
            serde_json::to_string(&ClientKind::Desktop).unwrap(),
            "\"desktop\""
        );
        assert_eq!(
            serde_json::to_string(&AuthSessionRevokeReason::RefreshReuse).unwrap(),
            "\"refresh_reuse\""
        );
        assert_eq!(
            serde_json::to_string(&AuthSessionRevokeReason::ActivationAttemptsExceeded).unwrap(),
            "\"activation_attempts_exceeded\""
        );
        assert_eq!(
            serde_json::to_string(&AuthCredentialPurpose::Invitation).unwrap(),
            "\"invitation\""
        );
        assert!(serde_json::from_str::<DeviceStatus>("\"unknown\"").is_err());
    }

    #[test]
    fn auth_me_role_key_is_additive_and_validated() {
        let legacy = serde_json::json!({
            "gateway": {"id": "G00000000000000000001"},
            "principal": {
                "id": "P00000000000000000001",
                "kind": "superuser",
                "display_name": "Superuser",
                "nickname": "superuser"
            },
            "device": {
                "id": "D00000000000000000001",
                "installation_id": "desktop-installation",
                "display_name": "Desktop",
                "client_kind": "desktop",
                "status": "active"
            },
            "session": {
                "id": "S00000000000000000001",
                "device_id": "D00000000000000000001",
                "token_family_id": "F00000000000000000001",
                "status": "active",
                "refresh_generation": 1,
                "refresh_expires_at_unix": 2
            }
        });
        let decoded: AuthMeResponse =
            serde_json::from_value(legacy.clone()).expect("legacy auth/me should decode");
        assert_eq!(decoded.role_key, None);

        let mut member = legacy;
        member["principal"]["kind"] = serde_json::json!("user");
        member["role_key"] = serde_json::json!("member");
        let decoded: AuthMeResponse =
            serde_json::from_value(member).expect("Member auth/me should decode");
        assert_eq!(decoded.role_key, Some(RoleKey::member()));

        let mut invalid = serde_json::to_value(decoded).expect("Member auth/me should re-encode");
        invalid["role_key"] = serde_json::json!("Member");
        assert!(serde_json::from_value::<AuthMeResponse>(invalid).is_err());
    }

    #[test]
    fn profile_update_normalizes_identity_and_defaults_avatar_to_unchanged() {
        let decoded = serde_json::from_value::<AuthProfileUpdateParams>(serde_json::json!({
            "display_name": "  Alice Smith  ",
            "nickname": "Alice.Smith"
        }))
        .expect("valid profile update");
        assert_eq!(decoded.display_name, "Alice Smith");
        assert_eq!(decoded.nickname, "Alice.Smith");
        assert_eq!(decoded.nickname_key(), "alice.smith");
        assert_eq!(decoded.avatar, AuthProfileAvatarUpdate::Unchanged);
    }

    #[test]
    fn profile_update_avatar_actions_are_bounded_and_tagged() {
        let removed =
            AuthProfileUpdateParams::new("Alice", "alice", AuthProfileAvatarUpdate::Remove)
                .unwrap();
        assert_eq!(
            serde_json::to_value(removed).unwrap()["avatar"],
            serde_json::json!({"action": "remove"})
        );
        assert!(
            serde_json::from_value::<AuthProfileUpdateParams>(serde_json::json!({
                "display_name": "Alice",
                "nickname": "not allowed",
                "avatar": {"action": "unchanged"}
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<AuthProfileUpdateParams>(serde_json::json!({
                "display_name": "Alice",
                "nickname": "alice",
                "avatar": {"action": "unchanged"},
                "principal_id": "P00000000000000000001"
            }))
            .is_err()
        );
    }

    #[test]
    fn security_sensitive_auth_params_reject_unknown_fields() {
        assert!(
            serde_json::from_value::<AuthDeviceActivateParams>(serde_json::json!({
                "installation": {
                    "installation_id": "desktop-installation",
                    "display_name": "Pioneer Desktop",
                    "client_kind": "desktop"
                },
                "exchange_id": "obsolete"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<AuthSessionRevokeParams>(serde_json::json!({
                "session_id": "S00000000000000000001",
                "activation_id": "obsolete"
            }))
            .is_err()
        );
    }

    #[test]
    fn activation_presentation_round_trips_with_fragment_only_secret() {
        let gateway_id = GatewayId::new("G00000000000000000001").unwrap();
        let code = "K7M4-P9Q2";
        let presentation = AuthDeviceActivationPresentation::new(
            GatewayBaseUrl::parse_presentation("https://gateway.example.test/pioneer").unwrap(),
            gateway_id.clone(),
            code,
        )
        .unwrap();
        let uri = presentation.to_uri();
        let parsed_url = url::Url::parse(&uri).unwrap();
        assert!(!parsed_url.query().unwrap_or_default().contains(code));
        assert!(
            parsed_url
                .query()
                .unwrap_or_default()
                .contains(gateway_id.as_str())
        );
        assert!(parsed_url.fragment().unwrap_or_default().contains("code="));
        let round_trip = AuthDeviceActivationPresentation::parse(&uri).unwrap();
        assert_eq!(round_trip, presentation);
        assert!(!format!("{presentation:?}").contains(code));
    }

    #[test]
    fn activation_presentation_preserves_production_and_development_schemes() {
        for scheme in [
            PioneerAppUrlScheme::Production,
            PioneerAppUrlScheme::Development,
        ] {
            let presentation = AuthDeviceActivationPresentation::new_with_scheme(
                GatewayBaseUrl::parse_presentation("https://gateway.example.test/").unwrap(),
                GatewayId::new("G00000000000000000001").unwrap(),
                "K7M4-P9Q2",
                scheme,
            )
            .unwrap();
            let uri = presentation.to_uri();
            assert!(uri.starts_with(format!("{}://activate", scheme.as_str()).as_str()));
            let parsed = AuthDeviceActivationPresentation::parse(uri.as_str()).unwrap();
            assert_eq!(parsed.app_url_scheme(), scheme);
            assert_eq!(parsed, presentation);
        }
        assert!(
            AuthDeviceActivationPresentation::parse(
                "pioneer-preview://activate?gateway_base_url=https%3A%2F%2Fgateway.example.test%2F&gateway_id=G00000000000000000001#code=K7M4-P9Q2"
            )
            .is_err()
        );
    }

    #[test]
    fn activation_presentation_rejects_ambiguous_or_leaky_fields() {
        let gateway_id = GatewayId::new("G00000000000000000001").unwrap();
        let code = "K7M4-P9Q2";
        for uri in [
            format!(
                "pioneer://activate?gateway_base_url=https%3A%2F%2Fgateway.example%2F&gateway_id={gateway_id}&code={code}#code={code}"
            ),
            format!(
                "pioneer://activate?gateway_base_url=https%3A%2F%2Fgateway.example%2F&gateway_base_url=https%3A%2F%2Fother.example%2F&gateway_id={gateway_id}#code={code}"
            ),
            format!(
                "pioneer://activate?gateway_base_url=https%3A%2F%2Fgateway.example%2F&gateway_id={gateway_id}#code={code}&code={code}"
            ),
            format!(
                "pioneer://activate?gateway_base_url=https%3A%2F%2Fgateway.example%2F&gateway_id={gateway_id}#code={code}&extra=1"
            ),
            format!(
                "pioneer://activate/unexpected?gateway_base_url=https%3A%2F%2Fgateway.example%2F&gateway_id={gateway_id}#code={code}"
            ),
            format!(
                "pioneer://user@activate?gateway_base_url=https%3A%2F%2Fgateway.example%2F&gateway_id={gateway_id}#code={code}"
            ),
            format!(
                "pioneer://activate:1234?gateway_base_url=https%3A%2F%2Fgateway.example%2F&gateway_id={gateway_id}#code={code}"
            ),
            format!(
                "pioneer://activate?gateway=wss%3A%2F%2Fgateway.example%2F&gateway_id={gateway_id}#code={code}"
            ),
        ] {
            assert!(
                AuthDeviceActivationPresentation::parse(&uri).is_err(),
                "{uri}"
            );
        }
        assert!(
            AuthDeviceActivationPresentation::new(
                GatewayBaseUrl::parse_presentation("https://gateway.example").unwrap(),
                gateway_id.clone(),
                "K7M4-P9Q!",
            )
            .is_err()
        );
        assert!(
            AuthDeviceActivationPresentation::new(
                GatewayBaseUrl::parse_presentation("https://gateway.example").unwrap(),
                gateway_id,
                "K7M4-P9Q22",
            )
            .is_err()
        );
        assert!(
            AuthDeviceActivationPresentation::parse(
                "x".repeat(MAX_PROTECTED_GATEWAY_URI_BYTES + 1).as_str()
            )
            .is_err()
        );
    }

    #[test]
    fn activation_presentation_accepts_canonical_local_remote_and_tls_bases() {
        let gateway_id = GatewayId::new("G00000000000000000001").unwrap();
        for endpoint in [
            "http://localhost:17878",
            "http://127.0.0.1:17878",
            "http://[::1]:17878",
            "http://91.224.86.172:17878",
            "https://gateway.example.test/pioneer",
        ] {
            assert!(
                AuthDeviceActivationPresentation::new(
                    GatewayBaseUrl::parse_presentation(endpoint).unwrap(),
                    gateway_id.clone(),
                    "K7M4-P9Q2",
                )
                .is_ok(),
                "{endpoint}"
            );
        }
    }

    #[test]
    fn activation_code_normalizes_crockford_aliases_and_formats_groups() {
        assert_eq!(
            normalize_device_activation_code("k7m4-p9q2").unwrap(),
            "K7M4P9Q2"
        );
        assert_eq!(
            normalize_device_activation_code("oilm-p9q2").unwrap(),
            "011MP9Q2"
        );
        assert_eq!(
            format_device_activation_code("k7m4p9q2").unwrap(),
            "K7M4-P9Q2"
        );
        assert_eq!(device_activation_locator("k7m4-p9q2").unwrap(), "K");
        assert_eq!(
            normalize_device_activation_code_input("k7m4-").unwrap(),
            "K7M4"
        );
        assert!(normalize_device_activation_code("K7M4-P9Q").is_err());
        assert!(normalize_device_activation_code("K7M4--P9Q2").is_err());
        assert!(normalize_device_activation_code("K7M4-P9U2").is_err());
    }

    #[test]
    fn activation_entropy_is_exactly_eight_crockford_symbols() {
        assert_eq!(encode_device_activation_entropy([0; 5]), "00000000");
        assert_eq!(encode_device_activation_entropy([0xff; 5]), "ZZZZZZZZ");
    }
}
