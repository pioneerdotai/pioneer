use crate::error::ToolError;
use pioneer_protocol::{TurnExecutionSecuritySnapshot, TurnNetworkMode};
use std::net::IpAddr;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkPolicyDecision {
    Allowed(NetworkPolicyGrant),
    Denied(NetworkPolicyDeny),
}

impl NetworkPolicyDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed(_))
    }

    pub fn deny(&self) -> Option<&NetworkPolicyDeny> {
        match self {
            Self::Allowed(_) => None,
            Self::Denied(deny) => Some(deny),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkPolicyGrant {
    pub operation: String,
    pub url: String,
    pub host: String,
    pub mode: TurnNetworkMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkPolicyDeny {
    pub operation: String,
    pub url: String,
    pub host: Option<String>,
    pub reason: NetworkPolicyDenyReason,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkPolicyDenyReason {
    InvalidUrl,
    UnsupportedScheme,
    MissingHost,
    NetworkDisabled,
    HostDenied,
    HostNotAllowed,
}

pub struct NetworkPolicyChecker;

impl NetworkPolicyChecker {
    pub fn check_url(
        snapshot: &TurnExecutionSecuritySnapshot,
        url: &str,
        operation: impl Into<String>,
    ) -> NetworkPolicyDecision {
        let operation = operation.into();
        let parsed = match Url::parse(url) {
            Ok(parsed) => parsed,
            Err(error) => {
                return deny(
                    operation,
                    url,
                    None,
                    NetworkPolicyDenyReason::InvalidUrl,
                    format!("invalid url `{url}`: {error}"),
                );
            }
        };

        match parsed.scheme() {
            "http" | "https" => {}
            other => {
                return deny(
                    operation,
                    url,
                    parsed.host_str().map(ToOwned::to_owned),
                    NetworkPolicyDenyReason::UnsupportedScheme,
                    format!("unsupported url scheme `{other}` (expected http or https)"),
                );
            }
        }

        let Some(host) = parsed
            .host_str()
            .map(normalize_host)
            .filter(|host| !host.is_empty())
        else {
            return deny(
                operation,
                url,
                None,
                NetworkPolicyDenyReason::MissingHost,
                "network policy requires a host for HTTP access".to_owned(),
            );
        };

        let policy = &snapshot.network;
        if network_rule_list_matches(policy.denied_domains.as_slice(), &parsed, host.as_str()) {
            return deny(
                operation,
                url,
                Some(host),
                NetworkPolicyDenyReason::HostDenied,
                "network access denied by turn security policy".to_owned(),
            );
        }

        match policy.mode {
            TurnNetworkMode::Enabled => NetworkPolicyDecision::Allowed(NetworkPolicyGrant {
                operation,
                url: url.to_owned(),
                host,
                mode: policy.mode,
            }),
            TurnNetworkMode::Disabled => deny(
                operation,
                url,
                Some(host),
                NetworkPolicyDenyReason::NetworkDisabled,
                "network access is disabled for this turn".to_owned(),
            ),
            TurnNetworkMode::Restricted => {
                // `allow_localhost` is the address-space capability needed to
                // connect to loopback. When explicit URL rules are present,
                // they still constrain scheme/host/port; the boolean must not
                // turn one approved localhost origin into every local service.
                if policy.allow_localhost
                    && is_localhost(host.as_str())
                    && policy.allowed_domains.is_empty()
                {
                    return NetworkPolicyDecision::Allowed(NetworkPolicyGrant {
                        operation,
                        url: url.to_owned(),
                        host,
                        mode: policy.mode,
                    });
                }

                if network_rule_list_matches(
                    policy.allowed_domains.as_slice(),
                    &parsed,
                    host.as_str(),
                ) {
                    return NetworkPolicyDecision::Allowed(NetworkPolicyGrant {
                        operation,
                        url: url.to_owned(),
                        host,
                        mode: policy.mode,
                    });
                }

                deny(
                    operation,
                    url,
                    Some(host),
                    NetworkPolicyDenyReason::HostNotAllowed,
                    "network host is not in the turn security allowlist".to_owned(),
                )
            }
        }
    }
}

pub fn enforce_network_url(
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    url: &str,
    operation: &str,
) -> Result<(), ToolError> {
    let Some(snapshot) = snapshot else {
        return Err(ToolError::Rejected(format!(
            "missing turn execution security snapshot; refusing {operation} network access without resolved sandbox policy"
        )));
    };

    match NetworkPolicyChecker::check_url(snapshot, url, operation) {
        NetworkPolicyDecision::Allowed(_) => Ok(()),
        NetworkPolicyDecision::Denied(deny) => match deny.reason {
            NetworkPolicyDenyReason::InvalidUrl
            | NetworkPolicyDenyReason::UnsupportedScheme
            | NetworkPolicyDenyReason::MissingHost => {
                Err(ToolError::invalid_arguments(deny.message))
            }
            NetworkPolicyDenyReason::NetworkDisabled
            | NetworkPolicyDenyReason::HostDenied
            | NetworkPolicyDenyReason::HostNotAllowed => Err(ToolError::Rejected(deny.message)),
        },
    }
}

fn deny(
    operation: String,
    url: &str,
    host: Option<String>,
    reason: NetworkPolicyDenyReason,
    message: String,
) -> NetworkPolicyDecision {
    NetworkPolicyDecision::Denied(NetworkPolicyDeny {
        operation,
        url: url.to_owned(),
        host,
        reason,
        message,
    })
}

fn network_rule_list_matches(rules: &[String], url: &Url, host: &str) -> bool {
    rules.iter().any(|rule| {
        if let Some(exact) = rule.trim().strip_prefix('=') {
            if let Ok(exact_url) = Url::parse(exact)
                && matches!(exact_url.scheme(), "http" | "https")
                && exact_url.host_str().is_some()
            {
                return exact_url.scheme().eq_ignore_ascii_case(url.scheme())
                    && exact_url
                        .host_str()
                        .is_some_and(|exact_host| normalize_host(exact_host) == host)
                    && exact_url.port_or_known_default() == url.port_or_known_default();
            }
            return normalize_host(exact) == host;
        }
        let domain = normalize_domain_rule(rule);
        !domain.is_empty() && domain_matches(host, domain.as_str())
    })
}

fn domain_matches(host: &str, domain: &str) -> bool {
    host == domain || host.ends_with(format!(".{domain}").as_str())
}

fn normalize_host(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn normalize_domain_rule(domain: &str) -> String {
    domain
        .trim()
        .trim_start_matches("*.")
        .trim_start_matches('.')
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

pub(crate) fn is_localhost(host: &str) -> bool {
    host == "localhost"
        || host.ends_with(".localhost")
        || host
            .parse::<IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{TurnNetworkPolicySnapshot, TurnPermissionProfileSource};
    use pioneer_protocol::{TurnPermissionMode, TurnPermissionProfileSnapshot};

    fn snapshot_with_network(network: TurnNetworkPolicySnapshot) -> TurnExecutionSecuritySnapshot {
        let mut snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            "/tmp/workspace",
            1_700_000_000_000,
        );
        snapshot.permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::FullAccess,
            TurnPermissionProfileSource::Composer,
        );
        snapshot.network = network.clone();
        snapshot.sandbox.network = network;
        snapshot
    }

    #[test]
    fn network_policy_disabled_denies_before_http_access() {
        let snapshot = snapshot_with_network(TurnNetworkPolicySnapshot::disabled());

        let decision =
            NetworkPolicyChecker::check_url(&snapshot, "https://example.com", "web_fetch");

        let deny = decision.deny().expect("network should be denied");
        assert_eq!(deny.reason, NetworkPolicyDenyReason::NetworkDisabled);
        assert_eq!(deny.host.as_deref(), Some("example.com"));
    }

    #[test]
    fn network_policy_enabled_allows_public_host() {
        let snapshot = snapshot_with_network(TurnNetworkPolicySnapshot::enabled());

        assert!(
            NetworkPolicyChecker::check_url(&snapshot, "https://example.com", "web_fetch")
                .is_allowed()
        );
    }

    #[test]
    fn network_policy_rejects_missing_security_snapshot() {
        let error = enforce_network_url(None, "https://example.com", "web_fetch")
            .expect_err("missing security snapshot should fail closed");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("missing turn execution security snapshot"))
        );
    }

    #[test]
    fn network_policy_host_and_origin_decision_matrix() {
        for (case, allowed, denied, allow_localhost, urls) in [
            (
                "network_policy_restricted_denies_host_outside_allowlist",
                &["allowed.example"][..],
                &[][..],
                false,
                &[(
                    "https://blocked.example",
                    Some(NetworkPolicyDenyReason::HostNotAllowed),
                )][..],
            ),
            (
                "network_policy_restricted_allows_subdomain_allowlist_match",
                &["example.com"][..],
                &[][..],
                false,
                &[("https://api.example.com", None)][..],
            ),
            (
                "network_policy_exact_host_rule_does_not_authorize_subdomains",
                &["=example.com"][..],
                &[][..],
                false,
                &[
                    ("https://example.com", None),
                    (
                        "https://redirect.example.com",
                        Some(NetworkPolicyDenyReason::HostNotAllowed),
                    ),
                ][..],
            ),
            (
                "network_policy_exact_origin_rule_binds_scheme_and_effective_port",
                &["=https://example.com:8443"][..],
                &[][..],
                false,
                &[
                    ("https://example.com:8443/allowed", None),
                    (
                        "https://example.com:9443/blocked",
                        Some(NetworkPolicyDenyReason::HostNotAllowed),
                    ),
                    (
                        "http://example.com:8443/blocked",
                        Some(NetworkPolicyDenyReason::HostNotAllowed),
                    ),
                    (
                        "https://api.example.com:8443/blocked",
                        Some(NetworkPolicyDenyReason::HostNotAllowed),
                    ),
                ][..],
            ),
            (
                "localhost_address_capability_does_not_widen_an_exact_origin_rule",
                &["=http://localhost:4321"][..],
                &[][..],
                true,
                &[
                    ("http://localhost:4321/allowed", None),
                    (
                        "http://localhost:4322/blocked",
                        Some(NetworkPolicyDenyReason::HostNotAllowed),
                    ),
                    (
                        "https://localhost:4321/blocked",
                        Some(NetworkPolicyDenyReason::HostNotAllowed),
                    ),
                    (
                        "http://child.localhost:4321/blocked",
                        Some(NetworkPolicyDenyReason::HostNotAllowed),
                    ),
                ][..],
            ),
            (
                "network_policy_exact_origin_normalizes_default_port",
                &["=https://example.com"][..],
                &[][..],
                false,
                &[
                    ("https://example.com:443/allowed", None),
                    (
                        "https://example.com:444/blocked",
                        Some(NetworkPolicyDenyReason::HostNotAllowed),
                    ),
                ][..],
            ),
            (
                "network_policy_denied_domain_overrides_allowlist",
                &["example.com"][..],
                &["api.example.com"][..],
                false,
                &[(
                    "https://api.example.com",
                    Some(NetworkPolicyDenyReason::HostDenied),
                )][..],
            ),
            (
                "network_policy_allowlist_does_not_match_similar_suffix",
                &["example.com"][..],
                &[][..],
                false,
                &[(
                    "https://badexample.com",
                    Some(NetworkPolicyDenyReason::HostNotAllowed),
                )][..],
            ),
        ] {
            let mut policy = TurnNetworkPolicySnapshot::disabled();
            policy.mode = TurnNetworkMode::Restricted;
            policy.allowed_domains = allowed.iter().map(|domain| (*domain).to_owned()).collect();
            policy.denied_domains = denied.iter().map(|domain| (*domain).to_owned()).collect();
            policy.allow_localhost = allow_localhost;
            for &(url, expected_denial) in urls {
                let snapshot = snapshot_with_network(policy.clone());
                let decision = NetworkPolicyChecker::check_url(&snapshot, url, "web_fetch");
                assert_eq!(
                    decision.is_allowed(),
                    expected_denial.is_none(),
                    "{case}: {url}"
                );
                assert_eq!(
                    decision.deny().map(|denial| denial.reason),
                    expected_denial,
                    "{case}: {url}"
                );
            }
        }
    }
}
