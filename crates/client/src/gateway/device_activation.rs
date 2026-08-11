//! Secret-bearing, direct-call-only pending-device activation presentation.
//!
//! This type must never be serialized into `ClientEvent`, diagnostics, logs, or
//! persistent client state. The URI and QR modules both encode the activation
//! credential and are intentionally redacted from `Debug`.

use anyhow::{Context, Result};
use pioneer_protocol::{
    AuthDeviceActivationPresentation, AuthDeviceCreateResponse, AuthSecretString, AuthSessionId,
    DeviceId, GatewayId, PioneerAppUrlScheme,
};
use qrcode::{Color, QrCode};

use super::endpoint::GatewayBaseUrl;

#[derive(Clone, PartialEq, Eq)]
pub struct DeviceActivationQrPresentation {
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub gateway_id: GatewayId,
    pub gateway_base_url: GatewayBaseUrl,
    pub expires_at_unix: u64,
    manual_code: AuthSecretString,
    deep_link: AuthSecretString,
    width: usize,
    modules: Vec<bool>,
}

impl DeviceActivationQrPresentation {
    pub fn from_created_device(
        gateway_base_url: &GatewayBaseUrl,
        created: AuthDeviceCreateResponse,
    ) -> Result<Self> {
        Self::from_created_device_with_scheme(
            gateway_base_url,
            created,
            PioneerAppUrlScheme::for_current_build(),
        )
    }

    pub fn from_created_device_with_scheme(
        gateway_base_url: &GatewayBaseUrl,
        created: AuthDeviceCreateResponse,
        app_url_scheme: PioneerAppUrlScheme,
    ) -> Result<Self> {
        let presentation = AuthDeviceActivationPresentation::new_with_scheme(
            gateway_base_url.clone(),
            created.gateway_id.clone(),
            created.activation_code.expose_secret(),
            app_url_scheme,
        )
        .map_err(anyhow::Error::msg)?;
        let deep_link = presentation.to_uri();
        let qr =
            QrCode::new(deep_link.as_bytes()).context("failed to encode device activation QR")?;
        let width = qr.width();
        let modules = qr
            .to_colors()
            .into_iter()
            .map(|color| color == Color::Dark)
            .collect();

        Ok(Self {
            device_id: created.device_id,
            session_id: created.session_id,
            gateway_id: created.gateway_id,
            gateway_base_url: gateway_base_url.clone(),
            expires_at_unix: created.expires_at_unix,
            manual_code: created.activation_code,
            deep_link: AuthSecretString::new(deep_link),
            width,
            modules,
        })
    }

    pub fn manual_code(&self) -> &str {
        self.manual_code.expose_secret()
    }

    pub fn deep_link(&self) -> &str {
        self.deep_link.expose_secret()
    }

    pub fn qr_width(&self) -> usize {
        self.width
    }

    pub fn qr_modules(&self) -> &[bool] {
        &self.modules
    }
}

impl std::fmt::Debug for DeviceActivationQrPresentation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeviceActivationQrPresentation")
            .field("device_id", &self.device_id)
            .field("session_id", &self.session_id)
            .field("gateway_id", &self.gateway_id)
            .field("gateway_base_url", &self.gateway_base_url)
            .field("expires_at_unix", &self.expires_at_unix)
            .field("manual_code", &"[redacted]")
            .field("deep_link", &"[redacted]")
            .field("qr_modules", &"[redacted]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn created_device() -> AuthDeviceCreateResponse {
        AuthDeviceCreateResponse {
            device_id: DeviceId::new("D00000000000000000001").unwrap(),
            session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
            activation_code: AuthSecretString::new("K7M4-P9Q2"),
            expires_at_unix: 1_800_000_000,
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
        }
    }

    #[test]
    fn activation_qr_and_manual_code_round_trip_without_debug_leakage() {
        let base = GatewayBaseUrl::parse_presentation("91.224.86.172:17878").unwrap();
        let presentation =
            DeviceActivationQrPresentation::from_created_device(&base, created_device()).unwrap();
        assert_eq!(
            presentation.gateway_base_url.as_str(),
            "http://91.224.86.172:17878/"
        );
        let parsed = AuthDeviceActivationPresentation::parse(presentation.deep_link()).unwrap();
        assert_eq!(parsed.gateway_id, presentation.gateway_id);
        assert_eq!(parsed.activation_code(), presentation.manual_code());
        assert_eq!(
            presentation.qr_modules().len(),
            presentation.qr_width() * presentation.qr_width()
        );
        assert!(presentation.qr_modules().iter().any(|module| *module));

        let rendered = format!("{presentation:?}");
        assert!(!rendered.contains(presentation.manual_code()));
        assert!(!rendered.contains("pioneer://activate"));
        assert!(rendered.contains("[redacted]"));
    }
}
