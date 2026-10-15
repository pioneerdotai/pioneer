use pioneer_client::gateway::endpoint::GatewayBaseUrl;
use pioneer_protocol::{
    AuthDeviceActivationPresentation, AuthDeviceCreateResponse, AuthSecretString, AuthSessionId,
    DeviceId, GatewayId, PioneerAppUrlScheme,
};
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum ClientDeviceActivationPresentationRequest {
    Current {
        generation: u64,
    },
    Created {
        gateway_base_url: GatewayBaseUrl,
        created_device: AuthDeviceCreateResponse,
        app_url_scheme: PioneerAppUrlScheme,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct ClientDeviceActivationPresentationResult {
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub gateway_id: GatewayId,
    pub gateway_base_url: GatewayBaseUrl,
    pub expires_at_unix: u64,
    pub manual_code: AuthSecretString,
    pub deep_link: AuthSecretString,
    pub qr_width: u64,
    pub qr_modules: Vec<bool>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientDeviceActivationParseRequest {
    pub uri: AuthSecretString,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct ClientDeviceActivationParseResult {
    pub gateway_base_url: GatewayBaseUrl,
    pub gateway_id: GatewayId,
    pub activation_code: AuthSecretString,
}

impl ClientDeviceActivationPresentationResult {
    pub fn from_request(
        request: ClientDeviceActivationPresentationRequest,
    ) -> Result<Self, String> {
        let ClientDeviceActivationPresentationRequest::Created {
            gateway_base_url,
            created_device,
            app_url_scheme,
        } = request
        else {
            return Err("activation presentation requires its Client owner".into());
        };
        let presentation =
            pioneer_client::gateway::device_activation::DeviceActivationQrPresentation::from_created_device_with_scheme(
                &gateway_base_url,
                created_device,
                app_url_scheme,
            )
            .map_err(|error| error.to_string())?;
        Self::from_presentation(presentation)
    }
    pub fn from_presentation(
        presentation: pioneer_client::gateway::device_activation::DeviceActivationQrPresentation,
    ) -> Result<Self, String> {
        let gateway_base_url = presentation.gateway_base_url.clone();
        Ok(Self {
            device_id: presentation.device_id.clone(),
            session_id: presentation.session_id.clone(),
            gateway_id: presentation.gateway_id.clone(),
            gateway_base_url,
            expires_at_unix: presentation.expires_at_unix,
            manual_code: AuthSecretString::new(presentation.manual_code()),
            deep_link: AuthSecretString::new(presentation.deep_link()),
            qr_width: u64::try_from(presentation.qr_width())
                .map_err(|_| "activation QR width is too large".to_owned())?,
            qr_modules: presentation.qr_modules().to_vec(),
        })
    }
}

impl ClientDeviceActivationParseResult {
    pub fn from_request(request: ClientDeviceActivationParseRequest) -> Result<Self, String> {
        let presentation = AuthDeviceActivationPresentation::parse(request.uri.expose_secret())?;
        let activation_code = AuthSecretString::new(presentation.activation_code());
        Ok(Self {
            gateway_base_url: presentation.gateway_base_url,
            gateway_id: presentation.gateway_id,
            activation_code,
        })
    }
}

macro_rules! redacted_activation_debug {
    ($type:ty) => {
        impl std::fmt::Debug for $type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($type))
                    .field("secret", &"[redacted]")
                    .finish()
            }
        }
    };
}

redacted_activation_debug!(ClientDeviceActivationPresentationRequest);
redacted_activation_debug!(ClientDeviceActivationPresentationResult);
redacted_activation_debug!(ClientDeviceActivationParseRequest);
redacted_activation_debug!(ClientDeviceActivationParseResult);

pub const INVALID_AUTH_REQUEST_CODE: &str = "invalid_auth_request";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_direct_call_contract_round_trips_and_redacts_debug() {
        let token = "K7M4-P9Q2".to_owned();
        let gateway_id = GatewayId::new("G00000000000000000001").unwrap();
        let request = ClientDeviceActivationPresentationRequest::Created {
            gateway_base_url: GatewayBaseUrl::parse_presentation(
                "https://gateway.example/pioneer/",
            )
            .unwrap(),
            created_device: AuthDeviceCreateResponse {
                device_id: DeviceId::new("D00000000000000000001").unwrap(),
                session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
                activation_code: AuthSecretString::new(token.clone()),
                expires_at_unix: 1_800_000_000,
                gateway_id: gateway_id.clone(),
            },
            app_url_scheme: PioneerAppUrlScheme::Development,
        };
        assert!(!format!("{request:?}").contains(&token));

        let presentation = ClientDeviceActivationPresentationResult::from_request(request).unwrap();
        assert_eq!(presentation.manual_code.expose_secret(), token);
        assert!(
            presentation
                .deep_link
                .expose_secret()
                .starts_with("pioneer-dev://activate")
        );
        assert_eq!(
            presentation.qr_modules.len(),
            usize::try_from(presentation.qr_width * presentation.qr_width).unwrap()
        );
        assert!(!format!("{presentation:?}").contains(&token));

        let parsed =
            ClientDeviceActivationParseResult::from_request(ClientDeviceActivationParseRequest {
                uri: presentation.deep_link.clone(),
            })
            .unwrap();
        assert_eq!(parsed.gateway_id, gateway_id);
        assert_eq!(
            parsed.gateway_base_url.as_str(),
            "https://gateway.example/pioneer/"
        );
        assert_eq!(parsed.activation_code.expose_secret(), token);
        assert!(!format!("{parsed:?}").contains(&token));
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientGatewaySessionValidationRequest {
    Envelope {
        envelope: serde_json::Value,
    },
    Refresh {
        envelope: pioneer_client::gateway::session_envelope::GatewaySessionEnvelope,
        installation_id: String,
        grant: pioneer_protocol::AuthRefreshGrant,
    },
    Identity {
        envelope: pioneer_client::gateway::session_envelope::GatewaySessionEnvelope,
        installation_id: String,
        identity: pioneer_protocol::AuthMeResponse,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct ClientGatewaySessionValidationResult {
    pub valid: bool,
    pub terminal_reason: Option<pioneer_client::gateway::session_lifecycle::SessionTerminalReason>,
}

pub fn validate_gateway_session(
    request: ClientGatewaySessionValidationRequest,
) -> ClientGatewaySessionValidationResult {
    use ClientGatewaySessionValidationRequest::*;
    const MAX_SAFE_INTEGER: u64 = (1_u64 << 53) - 1;
    let (valid, terminal_reason) = match request {
        Envelope { envelope } => (
            serde_json::from_value::<
                pioneer_client::gateway::session_envelope::GatewaySessionEnvelope,
            >(envelope)
            .is_ok_and(|envelope| {
                envelope.validate().is_ok()
                    && envelope.refresh_generation <= MAX_SAFE_INTEGER
                    && envelope.refresh_expires_at_unix <= MAX_SAFE_INTEGER
            }),
            None,
        ),
        Refresh {
            envelope,
            installation_id,
            grant,
        } => (
            envelope.accepts_refresh(
                &installation_id,
                pioneer_protocol::ClientKind::Mobile,
                &grant,
            ) && envelope.refresh_generation < MAX_SAFE_INTEGER
                && grant.refresh_generation <= MAX_SAFE_INTEGER
                && grant.refresh_expires_at_unix > 0
                && grant.refresh_expires_at_unix <= MAX_SAFE_INTEGER
                && grant.access_expires_at_unix <= MAX_SAFE_INTEGER,
            None,
        ),
        Identity {
            envelope,
            installation_id,
            identity,
        } => {
            let reason = envelope.identity_failure(
                Some(&envelope.gateway_id),
                &installation_id,
                pioneer_protocol::ClientKind::Mobile,
                &identity,
            );
            (reason.is_none(), reason)
        }
    };
    ClientGatewaySessionValidationResult {
        valid,
        terminal_reason,
    }
}

#[cfg(test)]
mod session_validation_tests {
    use serde_json::{Value, json};

    fn envelope() -> Value {
        json!({"schema_version":2,"gateway_id":"G00000000000000000001","principal_id":"P00000000000000000001","device_id":"D00000000000000000001","session_id":"S00000000000000000001","token_family_id":"F00000000000000000001","installation_id":"installation-mobile-1","refresh_generation":0,"refresh_expires_at_unix":1900000000,"refresh_token":format!("prf2_{}", "r".repeat(164))})
    }

    #[test]
    fn envelope_boundary_rejects_missing_binding_unknown_secrets_and_bad_request_ids() {
        let runtime = crate::ClientFfiRuntime::default();
        let check = |envelope| {
            runtime
                .gateway_session_validate(
                    &json!({"kind":"envelope","envelope":envelope}).to_string(),
                )
                .unwrap()
                .valid
        };
        assert!(check(envelope()));
        for field in ["installation_id", "token_family_id"] {
            let mut value = envelope();
            value.as_object_mut().unwrap().remove(field);
            assert!(!check(value));
        }
        for (field, value) in [
            ("access_token", json!("not-durable")),
            ("pending_refresh_request_id", json!("not-a-request-id")),
            ("refresh_generation", json!(9007199254740992_u64)),
        ] {
            let mut invalid = envelope();
            invalid[field] = value;
            assert!(!check(invalid));
        }
    }

    #[test]
    fn shared_refresh_validation_checks_the_mobile_installation_family_and_identity() {
        let runtime = crate::ClientFfiRuntime::default();
        let grant = json!({
            "auth_protocol_version":pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION,
            "credential_storage_order":"persist_refresh_before_activating_access",
            "gateway":{"id":"G00000000000000000001"},
            "principal":{"id":"P00000000000000000001","kind":"user","display_name":"Synthetic","nickname":"synthetic"},
            "device":{"id":"D00000000000000000001","installation_id":"installation-mobile-1","display_name":"Synthetic","client_kind":"mobile","status":"active"},
            "session":{"id":"S00000000000000000001","device_id":"D00000000000000000001","token_family_id":"F00000000000000000001","status":"active","refresh_generation":1,"refresh_expires_at_unix":1900000000},
            "access_token":"synthetic-access","access_expires_at_unix":1800000900,
            "refresh_token":format!("prf2_{}", "s".repeat(164)),"refresh_generation":1,"refresh_expires_at_unix":1900000000
        });
        let check = |grant| {
            runtime.gateway_session_validate(&json!({"kind":"refresh","envelope":envelope(),"installation_id":"installation-mobile-1","grant":grant}).to_string())
        };
        assert!(check(grant.clone()).unwrap().valid);
        let mut wrong_installation = grant.clone();
        wrong_installation["device"]["installation_id"] = json!("different-installation");
        assert!(!check(wrong_installation).unwrap().valid);
        let mut wrong_family = grant.clone();
        wrong_family["session"]["token_family_id"] = json!("F00000000000000000002");
        assert!(!check(wrong_family).unwrap().valid);
        let mut malformed_family = grant.clone();
        malformed_family["session"]["token_family_id"] = json!("invalid-family");
        assert!(check(malformed_family).is_err());
        let mut stored = envelope();
        stored["refresh_generation"] = json!(1);
        let mut identity = json!({"gateway":grant["gateway"],"principal":grant["principal"],"device":grant["device"],"session":grant["session"],"role_key":null});
        identity["device"]["installation_id"] = json!("different-installation");
        let result = runtime.gateway_session_validate(&json!({"kind":"identity","envelope":stored,"installation_id":"installation-mobile-1","identity":identity}).to_string()).unwrap();
        assert!(!result.valid);
        assert_eq!(result.terminal_reason, Some(pioneer_client::gateway::session_lifecycle::SessionTerminalReason::SessionCompromised));
    }
}
