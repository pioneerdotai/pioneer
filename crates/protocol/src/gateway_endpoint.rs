//! Canonical Gateway endpoint authority shared by every Pioneer layer.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::{Host, Url};

pub const DEFAULT_GATEWAY_PORT: u16 = 17878;
pub const PIONEER_PROTOCOL_VERSION_HEADER: &str = "Pioneer-Protocol-Version";
pub const PIONEER_PROTOCOL_VERSION_NUMBER: u16 = 1;
pub const PIONEER_PROTOCOL_VERSION: &str = "1";

#[derive(Clone, PartialEq, Eq, Hash, JsonSchema)]
#[schemars(transparent)]
pub struct GatewayBaseUrl(String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
        let socket =
            SocketAddr::from_str(input.trim()).map_err(|_| GatewayBaseUrlError::InvalidUrl)?;
        let destination = match socket.ip() {
            IpAddr::V4(address) if address.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(address) if address.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            address => address,
        };
        Self::parse_canonical(
            format!("http://{}:{}", format_host(destination), socket.port()).as_str(),
        )
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
        let relative = canonical_storage_path(storage_path)?;
        let base = Url::parse(self.as_str()).expect("GatewayBaseUrl invariant");
        let storage_prefix = format!("{}storage/", base.path());
        let target = base
            .join(relative.as_str())
            .map_err(|_| GatewayBaseUrlError::InvalidStoragePath)?;
        if target.query().is_some()
            || target.fragment().is_some()
            || !target.path().starts_with(storage_prefix.as_str())
        {
            return Err(GatewayBaseUrlError::InvalidStoragePath);
        }
        Ok(target)
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
        // `url` deliberately normalizes dot segments. Inspect the original
        // representation before relying on that normalized value, otherwise a
        // configured base such as `/pioneer/../other` could silently change
        // authority. This is also applied to scheme-less presentation input
        // after it has been expanded to an HTTP URL.
        if raw_base_path_is_ambiguous(input) {
            return Err(GatewayBaseUrlError::InvalidBasePath);
        }
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

#[doc(hidden)]
pub fn canonical_storage_path(storage_path: &str) -> Result<String, GatewayBaseUrlError> {
    let relative = match storage_path.strip_prefix('/') {
        Some(relative) if !relative.starts_with('/') => relative,
        Some(_) => return Err(GatewayBaseUrlError::InvalidStoragePath),
        None => storage_path,
    };
    if !relative.starts_with("storage/")
        || relative.ends_with('/')
        || relative.contains(['%', '?', '#', '\\'])
        || relative.split('/').any(|segment| {
            segment.is_empty()
                || segment == "."
                || segment == ".."
                || segment.bytes().any(|byte| byte.is_ascii_control())
        })
    {
        return Err(GatewayBaseUrlError::InvalidStoragePath);
    }
    Ok(relative.to_owned())
}

fn raw_base_path_is_ambiguous(input: &str) -> bool {
    let Some((_, authority_and_path)) = input.split_once("://") else {
        return false;
    };
    let Some(path_start) = authority_and_path.find('/') else {
        return false;
    };
    let raw_path = authority_and_path[path_start..]
        .split(['?', '#'])
        .next()
        .unwrap_or(&authority_and_path[path_start..]);
    if raw_path.contains('\\') {
        return true;
    }
    let segments = raw_path.split('/').collect::<Vec<_>>();
    segments.iter().enumerate().any(|(index, segment)| {
        let is_edge = index == 0 || index + 1 == segments.len();
        if segment.is_empty() {
            return !is_edge;
        }
        let lower = segment.to_ascii_lowercase();
        lower.contains("%2f")
            || lower.contains("%5c")
            || lower.replace("%2e", ".") == "."
            || lower.replace("%2e", ".") == ".."
    })
}

fn format_host(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    }
}

fn is_unspecified_host(host: Host<&str>) -> bool {
    match host {
        Host::Ipv4(address) => address.is_unspecified(),
        Host::Ipv6(address) => address.is_unspecified(),
        Host::Domain(_) => false,
    }
}

fn is_loopback_host(host: Option<Host<&str>>) -> bool {
    match host {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

impl fmt::Debug for GatewayBaseUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("GatewayBaseUrl")
            .field(&self.0)
            .finish()
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
        assert_eq!(
            PIONEER_PROTOCOL_VERSION_NUMBER.to_string(),
            PIONEER_PROTOCOL_VERSION
        );
        let cases = [
            (
                "127.0.0.1",
                "http://127.0.0.1:17878/",
                "ws://127.0.0.1:17878/",
            ),
            (
                "192.168.1.20:19000",
                "http://192.168.1.20:19000/",
                "ws://192.168.1.20:19000/",
            ),
            (
                "https://relay.example/pioneer",
                "https://relay.example/pioneer/",
                "wss://relay.example/pioneer/",
            ),
        ];
        for (input, normalized, websocket) in cases {
            let base = GatewayBaseUrl::parse_presentation(input).unwrap();
            assert_eq!(base.as_str(), normalized);
            assert_eq!(base.websocket_url().as_str(), websocket);
            assert_eq!(
                base.storage_url("storage/views/grant").unwrap().as_str(),
                format!("{normalized}storage/views/grant")
            );
        }
    }

    #[test]
    fn endpoint_rejects_legacy_schemes_secrets_and_ambiguous_paths() {
        for input in [
            "",
            "ws://localhost:17878",
            "wss://relay.example",
            "https://user:secret@relay.example",
            "https://relay.example?token=secret",
            "https://relay.example/#secret",
            "http://0.0.0.0:17878",
            "http://[::]:17878",
            "https://relay.example/pioneer/../other",
            "https://relay.example/pioneer/%2e%2e/other",
            "relay.example/pioneer/../other",
            "relay.example/pioneer/%2e%2e/other",
            "https://relay.example/pioneer//other",
            "https://relay.example/pioneer/%2fother",
        ] {
            let error = GatewayBaseUrl::parse_presentation(input).unwrap_err();
            assert!(!format!("{error:?} {error}").contains("secret"));
        }
    }

    #[test]
    fn storage_derivation_cannot_escape_the_canonical_namespace() {
        let base = GatewayBaseUrl::parse_presentation("https://relay.example/pioneer/").unwrap();
        for path in [
            "storage/../health",
            "storage/%2e%2e/health",
            "storage//workspaces/W1",
            "//storage/workspaces/W1",
            "storage/workspaces/W1?token=secret",
            "storage/workspaces/W1/",
        ] {
            assert_eq!(
                base.storage_url(path),
                Err(GatewayBaseUrlError::InvalidStoragePath),
                "path {path}"
            );
        }
    }

    #[test]
    fn local_bind_and_transport_security_are_canonical() {
        assert_eq!(
            GatewayBaseUrl::from_local_listen_addr("0.0.0.0:17878")
                .unwrap()
                .as_str(),
            "http://127.0.0.1:17878/"
        );
        let cases = [
            (
                "http://localhost:17878",
                GatewayTransportSecurity::LoopbackPlaintext,
            ),
            (
                "http://192.168.1.20:17878",
                GatewayTransportSecurity::RemotePlaintext,
            ),
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
