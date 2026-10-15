use pioneer_client::gateway::invitation::InvitationQrPresentation;
use pioneer_protocol::{AuthSecretString, GatewayBaseUrl, GatewayId, InvitationTransportSecurity};
use serde::{Deserialize, Serialize};

pub const INVALID_INVITATION_REQUEST_CODE: &str = "invalid_invitation_request";

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientInvitationPresentationRequest {
    pub uri: AuthSecretString,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct ClientInvitationPresentationResult {
    pub gateway_base_url: GatewayBaseUrl,
    pub gateway_id: GatewayId,
    pub transport_security: InvitationTransportSecurity,
    pub canonical_uri: AuthSecretString,
    pub qr_payload: AuthSecretString,
    pub qr_width: usize,
    pub qr_modules: Vec<bool>,
}

impl ClientInvitationPresentationResult {
    pub fn from_request(request: ClientInvitationPresentationRequest) -> Result<Self, String> {
        let presentation = InvitationQrPresentation::parse(request.uri.expose_secret())
            .map_err(|_| "invalid invitation URI".to_owned())?;
        let canonical_uri = AuthSecretString::new(presentation.deep_link());
        let qr_payload = std::str::from_utf8(presentation.qr_payload())
            .map(|payload| AuthSecretString::new(payload.to_owned()))
            .map_err(|_| "invalid invitation QR payload".to_owned())?;
        let (qr_width, qr_modules) = presentation
            .qr_modules()
            .map_err(|_| "invalid invitation QR payload".to_owned())?;
        Ok(Self {
            gateway_base_url: presentation.gateway_base_url().clone(),
            gateway_id: presentation.gateway_id().clone(),
            transport_security: presentation.transport_security(),
            canonical_uri,
            qr_payload,
            qr_width,
            qr_modules,
        })
    }
}

macro_rules! redacted_debug {
    ($type:ty, $name:literal) => {
        impl std::fmt::Debug for $type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct($name)
                    .field("secret", &"[redacted]")
                    .finish()
            }
        }
    };
}

redacted_debug!(
    ClientInvitationPresentationRequest,
    "ClientInvitationPresentationRequest"
);
redacted_debug!(
    ClientInvitationPresentationResult,
    "ClientInvitationPresentationResult"
);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn presentation_and_secret_results_have_redacted_debug() {
        let secret = format!("pinv1_{}", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let uri = format!(
            "pioneer://invite?gateway_base_url=https%3A%2F%2Fgateway.example%2Fpioneer%2F&gateway_id=G00000000000000000001#token={secret}"
        );
        let request = ClientInvitationPresentationRequest {
            uri: AuthSecretString::new(uri.clone()),
        };
        assert!(!format!("{request:?}").contains(&secret));

        let result = ClientInvitationPresentationResult::from_request(request)
            .expect("valid shared invitation presentation");
        assert_eq!(result.canonical_uri.expose_secret(), uri);
        assert!(!format!("{result:?}").contains(&secret));
    }
}
