//! Canonical Gateway base URL validation and structured derivation.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::{Host, Url};

use super::connectivity::DEFAULT_GATEWAY_PORT;

pub const PIONEER_PROTOCOL_VERSION_HEADER: &str = "Pioneer-Protocol-Version";
pub const PIONEER_PROTOCOL_VERSION: &str = "1";

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Hash)]
#[cfg_attr(any(feature = "schema", test), schemars(transparent))]
pub struct GatewayBaseUrl(String);

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayTransportSecurity {
    LoopbackPlaintext,
    RemotePlaintext,
    Tls,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayBaseUrlError {
    Empty,
    UnsupportedScheme,
    MissingHost,
    CredentialsForbidden,
    QueryForbidden,
    FragmentForbidden,
    UnspecifiedDestination,
    InvalidBasePath,
    InvalidUrl,
    InvalidStoragePath,
}

impl GatewayBaseUrl {
    pub fn parse_presentation(input: &str) -> Result<Self, GatewayBaseUrlError> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(GatewayBaseUrlError::Empty);
        }
        if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
            return Err(GatewayBaseUrlError::UnsupportedScheme);
        }

        let candidate = if trimmed.contains("://") {
            trimmed.to_owned()
        } else if let Ok(ip) = trimmed.parse::<IpAddr>() {
            format!("http://{}:{DEFAULT_GATEWAY_PORT}", format_host(ip))
        } else {
            format!("http://{trimmed}")
        };
        Self::parse_canonical(candidate.as_str())
    }

    pub fn from_local_listen_addr(input: &str) -> Result<Self, GatewayBaseUrlError> {
        let socket = SocketAddr::from_str(input.trim()).map_err(|_| GatewayBaseUrlError::InvalidUrl)?;
        let destination = match socket.ip() {
            IpAddr::V4(gateway_base_url) if gateway_base_url.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(gateway_base_url) if gateway_base_url.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            gateway_base_url => gateway_base_url,
        };
        Self::parse_canonical(
            format!("http://{}:{}", format_host(destination), socket.port()).as_str(),
        )
    }

    /// Converts the protected WS endpoint carried by a device-activation
    /// presentation into the canonical HTTP base URL. UI layers must not
    /// duplicate this parsing or scheme conversion.
    pub fn from_websocket_presentation(input: &str) -> Result<Self, GatewayBaseUrlError> {
        let mut url = Url::parse(input.trim()).map_err(|_| GatewayBaseUrlError::InvalidUrl)?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(GatewayBaseUrlError::CredentialsForbidden);
        }
        if url.query().is_some() {
            return Err(GatewayBaseUrlError::QueryForbidden);
        }
        if url.fragment().is_some() {
            return Err(GatewayBaseUrlError::FragmentForbidden);
        }
        let scheme = match url.scheme() {
            "ws" => "http",
            "wss" => "https",
            _ => return Err(GatewayBaseUrlError::UnsupportedScheme),
        };
        url.set_scheme(scheme)
            .map_err(|_| GatewayBaseUrlError::InvalidUrl)?;
        Self::parse_canonical(url.as_str())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn websocket_url(&self) -> Url {
        let mut url = Url::parse(self.as_str()).expect("GatewayBaseUrl invariant");
        let scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            _ => unreachable!("GatewayBaseUrl has an HTTP scheme"),
        };
        url.set_scheme(scheme)
            .expect("ws/wss is compatible with an HTTP URL");
        url
    }

    pub fn storage_url(&self, storage_path: &str) -> Result<Url, GatewayBaseUrlError> {
        let relative = storage_path.trim_start_matches('/');
        if !relative.starts_with("storage/")
            || relative.split('/').any(|segment| segment == "." || segment == "..")
        {
            return Err(GatewayBaseUrlError::InvalidStoragePath);
        }
        Url::parse(self.as_str())
            .expect("GatewayBaseUrl invariant")
            .join(relative)
            .map_err(|_| GatewayBaseUrlError::InvalidStoragePath)
    }

    pub fn socket_address_input(&self) -> String {
        let url = Url::parse(self.as_str()).expect("GatewayBaseUrl invariant");
        let host = match url.host().expect("GatewayBaseUrl has a host") {
            Host::Ipv4(address) => address.to_string(),
            Host::Ipv6(address) => format!("[{address}]"),
            Host::Domain(domain) => domain.to_owned(),
        };
        let port = url
            .port_or_known_default()
            .expect("http and https have known default ports");
        format!("{host}:{port}")
    }

    pub fn transport_security(&self) -> GatewayTransportSecurity {
        let url = Url::parse(self.as_str()).expect("GatewayBaseUrl invariant");
        if url.scheme() == "https" {
            return GatewayTransportSecurity::Tls;
        }
        if is_loopback_host(url.host()) {
            GatewayTransportSecurity::LoopbackPlaintext
        } else {
            GatewayTransportSecurity::RemotePlaintext
        }
    }

    fn parse_canonical(input: &str) -> Result<Self, GatewayBaseUrlError> {
        let mut url = Url::parse(input).map_err(|_| GatewayBaseUrlError::InvalidUrl)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(GatewayBaseUrlError::UnsupportedScheme);
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(GatewayBaseUrlError::CredentialsForbidden);
        }
        if url.query().is_some() {
            return Err(GatewayBaseUrlError::QueryForbidden);
        }
        if url.fragment().is_some() {
            return Err(GatewayBaseUrlError::FragmentForbidden);
        }
        let host = url.host().ok_or(GatewayBaseUrlError::MissingHost)?;
        if is_unspecified_host(host) {
            return Err(GatewayBaseUrlError::UnspecifiedDestination);
        }
        if url.cannot_be_a_base()
            || url.path().contains('\\')
            || url.path_segments().is_none()
            || url
                .path_segments()
                .into_iter()
                .flatten()
                .any(|segment| segment == "." || segment == "..")
        {
            return Err(GatewayBaseUrlError::InvalidBasePath);
        }

        let normalized_path = if url.path() == "/" {
            "/".to_owned()
        } else {
            format!("{}/", url.path().trim_end_matches('/'))
        };
        url.set_path(normalized_path.as_str());
        Ok(Self(url.to_string()))
    }
}

fn format_host(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    }
}

fn is_unspecified_host(host: Host<&str>) -> bool {
    match host {
        Host::Ipv4(gateway_base_url) => gateway_base_url.is_unspecified(),
        Host::Ipv6(gateway_base_url) => gateway_base_url.is_unspecified(),
        Host::Domain(_) => false,
    }
}

fn is_loopback_host(host: Option<Host<&str>>) -> bool {
    match host {
        Some(Host::Ipv4(gateway_base_url)) => gateway_base_url.is_loopback(),
        Some(Host::Ipv6(gateway_base_url)) => gateway_base_url.is_loopback(),
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

impl fmt::Debug for GatewayBaseUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("GatewayBaseUrl").field(&self.0).finish()
    }
}

impl fmt::Display for GatewayBaseUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Display for GatewayBaseUrlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "Gateway base URL must not be empty",
            Self::UnsupportedScheme => "Gateway base URL must use http or https",
            Self::MissingHost => "Gateway base URL must contain a host",
            Self::CredentialsForbidden => "Gateway base URL must not contain credentials",
            Self::QueryForbidden => "Gateway base URL must not contain a query",
            Self::FragmentForbidden => "Gateway base URL must not contain a fragment",
            Self::UnspecifiedDestination => {
                "Gateway base URL cannot target an unspecified destination"
            }
            Self::InvalidBasePath => "Gateway base URL contains an invalid base path",
            Self::InvalidUrl => "Gateway base URL is invalid",
            Self::InvalidStoragePath => "Gateway storage path is invalid",
        })
    }
}

impl std::error::Error for GatewayBaseUrlError {}

impl FromStr for GatewayBaseUrl {
    type Err = GatewayBaseUrlError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse_presentation(value)
    }
}

impl Serialize for GatewayBaseUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GatewayBaseUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse_canonical(value.as_str()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_matrix_normalizes_and_derives_without_losing_prefix() {
        let cases = [
            ("127.0.0.1", "http://127.0.0.1:17878/", "ws://127.0.0.1:17878/"),
            ("127.0.0.1:17878", "http://127.0.0.1:17878/", "ws://127.0.0.1:17878/"),
            ("192.168.1.20:19000", "http://192.168.1.20:19000/", "ws://192.168.1.20:19000/"),
            ("http://[::1]:17878", "http://[::1]:17878/", "ws://[::1]:17878/"),
            ("https://relay.example/pioneer", "https://relay.example/pioneer/", "wss://relay.example/pioneer/"),
            ("https://relay.example:8443/custom/", "https://relay.example:8443/custom/", "wss://relay.example:8443/custom/"),
        ];
        for (input, normalized, ws) in cases {
            let base = GatewayBaseUrl::parse_presentation(input).unwrap();
            assert_eq!(base.as_str(), normalized);
            assert_eq!(base.websocket_url().as_str(), ws);
            assert_eq!(
                base.storage_url("storage/views/grant").unwrap().as_str(),
                format!("{normalized}storage/views/grant")
            );
        }
    }

    #[test]
    fn endpoint_rejects_secrets_legacy_schemes_and_unsafe_authorities() {
        for input in [
            "",
            "ws://localhost:17878",
            "wss://relay.example",
            "https://user:secret@relay.example",
            "https://relay.example?token=secret",
            "https://relay.example/#secret",
            "http://0.0.0.0:17878",
            "http://[::]:17878",
        ] {
            let error = GatewayBaseUrl::parse_presentation(input).unwrap_err();
            assert!(!format!("{error:?} {error}").contains("secret"));
        }
    }

    #[test]
    fn activation_ws_endpoint_is_converted_by_the_shared_authority() {
        assert_eq!(
            GatewayBaseUrl::from_websocket_presentation("wss://relay.example/pioneer/")
                .unwrap()
                .as_str(),
            "https://relay.example/pioneer/"
        );
        assert_eq!(
            GatewayBaseUrl::from_websocket_presentation("ws://192.0.2.10:17878")
                .unwrap()
                .as_str(),
            "http://192.0.2.10:17878/"
        );
        assert!(
            GatewayBaseUrl::from_websocket_presentation("wss://user:secret@example.com/")
                .is_err()
        );
        assert!(GatewayBaseUrl::from_websocket_presentation("https://example.com/").is_err());
    }

    #[test]
    fn local_bind_addresses_become_loopback_destinations() {
        assert_eq!(
            GatewayBaseUrl::from_local_listen_addr("0.0.0.0:17878")
                .unwrap()
                .as_str(),
            "http://127.0.0.1:17878/"
        );
        assert_eq!(
            GatewayBaseUrl::from_local_listen_addr("[::]:17878")
                .unwrap()
                .as_str(),
            "http://[::1]:17878/"
        );
    }

    #[test]
    fn transport_security_distinguishes_loopback_plaintext_remote_plaintext_and_tls() {
        let cases = [
            ("http://localhost:17878", GatewayTransportSecurity::LoopbackPlaintext),
            ("http://127.0.0.1:17878", GatewayTransportSecurity::LoopbackPlaintext),
            ("http://[::1]:17878", GatewayTransportSecurity::LoopbackPlaintext),
            ("http://192.168.1.20:17878", GatewayTransportSecurity::RemotePlaintext),
            ("https://relay.example", GatewayTransportSecurity::Tls),
        ];
        for (input, expected) in cases {
            assert_eq!(
                GatewayBaseUrl::parse_presentation(input)
                    .unwrap()
                    .transport_security(),
                expected
            );
        }
    }
}
