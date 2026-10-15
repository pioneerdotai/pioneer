use crate::attachments::errors::AttachmentPipelineError;
use crate::attachments::observability;
use crate::attachments::types::AttachmentSecurityPolicy;
use anyhow::Result;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use url::Url;

pub fn canonicalize_path(
    provider_name: &str,
    path: &str,
    policy: &AttachmentSecurityPolicy,
) -> Result<PathBuf> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(AttachmentPipelineError::unsupported_attachment_source("empty_path").into());
    }

    let canonical = Path::new(trimmed).canonicalize().map_err(|error| {
        AttachmentPipelineError::unsupported_attachment_source(
            format!("path_not_accessible:{trimmed}:{error}").as_str(),
        )
    })?;

    if policy.enforce_path_allowlist {
        if policy.allowed_path_roots.is_empty() {
            return enforce_or_dry_run(
                provider_name,
                "path:[redacted]",
                "path allowlist is enabled but has no roots",
                policy,
                AttachmentPipelineError::unsupported_attachment_source(
                    "path_allowlist_has_no_roots",
                ),
            )
            .map(|_| canonical);
        }

        let mut allowed = false;
        for root in &policy.allowed_path_roots {
            let root_canonical = match root.canonicalize() {
                Ok(value) => value,
                Err(_) => continue,
            };
            if canonical.starts_with(root_canonical.as_path()) {
                allowed = true;
                break;
            }
        }

        if !allowed {
            return enforce_or_dry_run(
                provider_name,
                "path:[redacted]",
                "path is outside allowed_path_roots",
                policy,
                AttachmentPipelineError::unsupported_attachment_source(
                    "path_outside_allowed_roots",
                ),
            )
            .map(|_| canonical);
        }
    }

    Ok(canonical)
}

pub fn parse_and_validate_url(
    provider_name: &str,
    raw_url: &str,
    policy: &AttachmentSecurityPolicy,
) -> Result<Url> {
    if !policy.allow_url_sources {
        enforce_or_dry_run(
            provider_name,
            "url:[redacted]",
            "URL attachment sources are disabled by policy",
            policy,
            AttachmentPipelineError::url_source_blocked("url sources disabled"),
        )?;
    }
    if policy.url_allowed_domains.is_empty() {
        return enforce_or_dry_run(
            provider_name,
            "url:[redacted]",
            "URL attachment sources require a non-empty domain allowlist",
            policy,
            AttachmentPipelineError::url_source_blocked("URL domain allowlist is empty"),
        )
        .map(|_| Url::parse("https://invalid.invalid").expect("static URL"));
    }

    let parsed = Url::parse(raw_url)
        .map_err(|_| AttachmentPipelineError::url_source_blocked("invalid URL syntax"))?;
    validate_url(provider_name, &parsed, policy)?;
    Ok(parsed)
}

pub fn validate_url(
    provider_name: &str,
    url: &Url,
    policy: &AttachmentSecurityPolicy,
) -> Result<()> {
    if !url.username().is_empty() || url.password().is_some() {
        return enforce_or_dry_run(
            provider_name,
            "url:[redacted]",
            "URL userinfo is forbidden",
            policy,
            AttachmentPipelineError::url_source_blocked("URL userinfo is forbidden"),
        )
        .map(|_| ());
    }

    let scheme = url.scheme().to_ascii_lowercase();
    if scheme != "https" && !(policy.allow_http && scheme == "http") {
        return enforce_or_dry_run(
            provider_name,
            safe_url_label(url).as_str(),
            format!("URL scheme `{scheme}` is not allowed").as_str(),
            policy,
            AttachmentPipelineError::url_source_blocked("scheme not allowed"),
        )
        .map(|_| ());
    }

    let Some(host) = url.host_str() else {
        return Err(AttachmentPipelineError::url_source_blocked("URL host is missing").into());
    };

    validate_host_name(provider_name, host, policy)?;
    resolve_and_validate_host(provider_name, host, url.port_or_known_default(), policy)?;

    Ok(())
}

pub fn validate_redirect(
    provider_name: &str,
    previous: &Url,
    next: &Url,
    policy: &AttachmentSecurityPolicy,
) -> Result<()> {
    if previous.scheme().eq_ignore_ascii_case("https") && next.scheme().eq_ignore_ascii_case("http")
    {
        return enforce_or_dry_run(
            provider_name,
            "url:[redacted]",
            "HTTPS to HTTP redirect downgrade is forbidden",
            policy,
            AttachmentPipelineError::url_redirect_blocked(
                "HTTPS to HTTP redirect downgrade is forbidden",
            ),
        )
        .map(|_| ());
    }
    validate_url(provider_name, next, policy)
}

pub fn safe_url_label(url: &Url) -> String {
    format!("url:{}", safe_url_origin(url))
}

pub fn safe_url_origin(url: &Url) -> String {
    let host = url.host_str().unwrap_or("invalid-host");
    match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    }
}

pub fn validate_host_name(
    provider_name: &str,
    host: &str,
    policy: &AttachmentSecurityPolicy,
) -> Result<()> {
    let host_lower = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if host_lower.is_empty() {
        return Err(AttachmentPipelineError::url_source_blocked("empty URL host").into());
    }

    let blocked = policy.url_blocked_domains.iter().any(|candidate| {
        let candidate = candidate.trim().trim_end_matches('.').to_ascii_lowercase();
        !candidate.is_empty() && domain_matches(host_lower.as_str(), candidate.as_str())
    });
    if blocked {
        return enforce_or_dry_run(
            provider_name,
            format!("host:{host}").as_str(),
            "host is blocked by url_blocked_domains policy",
            policy,
            AttachmentPipelineError::url_source_blocked("blocked host"),
        )
        .map(|_| ());
    }

    if !policy.url_allowed_domains.is_empty() {
        let allowed = policy.url_allowed_domains.iter().any(|candidate| {
            let candidate = candidate.trim().trim_end_matches('.').to_ascii_lowercase();
            !candidate.is_empty() && domain_matches(host_lower.as_str(), candidate.as_str())
        });
        if !allowed {
            return enforce_or_dry_run(
                provider_name,
                format!("host:{host}").as_str(),
                "host is not present in url_allowed_domains policy",
                policy,
                AttachmentPipelineError::url_source_blocked("host not allowlisted"),
            )
            .map(|_| ());
        }
    }

    Ok(())
}

pub fn resolve_and_validate_host(
    provider_name: &str,
    host: &str,
    port: Option<u16>,
    policy: &AttachmentSecurityPolicy,
) -> Result<()> {
    resolve_and_validate_host_addresses(provider_name, host, port, policy).map(|_| ())
}

pub fn resolve_and_validate_host_addresses(
    provider_name: &str,
    host: &str,
    port: Option<u16>,
    policy: &AttachmentSecurityPolicy,
) -> Result<Vec<SocketAddr>> {
    resolve_and_validate_host_addresses_with(provider_name, host, port, policy, |host, port| {
        (host, port)
            .to_socket_addrs()
            .map(|addresses| addresses.collect())
    })
}

fn resolve_and_validate_host_addresses_with<F>(
    provider_name: &str,
    host: &str,
    port: Option<u16>,
    policy: &AttachmentSecurityPolicy,
    resolve: F,
) -> Result<Vec<SocketAddr>>
where
    F: FnOnce(&str, u16) -> std::io::Result<Vec<SocketAddr>>,
{
    let port = port.unwrap_or(443);

    if let Ok(ip) = host.parse::<IpAddr>() {
        validate_ip(provider_name, ip, policy)?;
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    let resolved = resolve(host, port).map_err(|error| {
        AttachmentPipelineError::url_source_blocked(format!("dns resolve failed: {error}"))
    })?;

    let mut addresses = Vec::new();
    for addr in resolved {
        validate_ip(provider_name, addr.ip(), policy)?;
        if !addresses.contains(&addr) {
            addresses.push(addr);
        }
    }

    if addresses.is_empty() {
        return Err(AttachmentPipelineError::url_source_blocked(
            "dns resolve returned no addresses",
        )
        .into());
    }

    Ok(addresses)
}

pub fn validate_connected_peer(
    provider_name: &str,
    peer: SocketAddr,
    pinned_addresses: &[SocketAddr],
    policy: &AttachmentSecurityPolicy,
) -> Result<()> {
    validate_ip(provider_name, peer.ip(), policy)?;
    if !pinned_addresses.iter().any(|address| *address == peer) {
        return Err(AttachmentPipelineError::url_source_blocked(
            "connected peer does not match the pinned DNS decision",
        )
        .into());
    }
    Ok(())
}

fn validate_ip(provider_name: &str, ip: IpAddr, policy: &AttachmentSecurityPolicy) -> Result<()> {
    if policy.allow_private_network {
        return Ok(());
    }

    if is_private_or_local_ip(ip) {
        return enforce_or_dry_run(
            provider_name,
            format!("ip:{ip}").as_str(),
            "private/local IP targets are blocked",
            policy,
            AttachmentPipelineError::url_source_blocked("private IP blocked"),
        )
        .map(|_| ());
    }

    Ok(())
}

fn enforce_or_dry_run(
    provider_name: &str,
    source: &str,
    reason: &str,
    policy: &AttachmentSecurityPolicy,
    error: AttachmentPipelineError,
) -> Result<bool> {
    observability::emit_security_blocked(provider_name, source, reason, policy.dry_run);
    // `dry_run` controls audit presentation only. A security decision must
    // never turn into permission to perform the side effect it rejected.
    Err(error.into())
}

fn is_private_or_local_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(value) => {
            value.is_private()
                || value.is_loopback()
                || value.is_link_local()
                || value.is_broadcast()
                || value.is_unspecified()
                || value.octets()[0] == 0
                || value.octets() == Ipv4Addr::new(169, 254, 169, 254).octets()
        }
        IpAddr::V6(value) => {
            value.is_loopback()
                || value.is_unspecified()
                || value.is_multicast()
                || value.is_unique_local()
                || is_ipv6_link_local(value)
        }
    }
}

fn is_ipv6_link_local(ip: Ipv6Addr) -> bool {
    // fe80::/10
    let seg0 = ip.segments()[0];
    (seg0 & 0xffc0) == 0xfe80
}

fn domain_matches(host: &str, candidate: &str) -> bool {
    host == candidate || host.ends_with(format!(".{candidate}").as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_private_ip_literals_by_default() {
        let policy = AttachmentSecurityPolicy::default();
        let err = resolve_and_validate_host("test", "127.0.0.1", Some(80), &policy)
            .expect_err("private IP must be blocked");
        assert!(err.to_string().contains("URL_SOURCE_BLOCKED"));
    }

    #[test]
    fn dry_run_never_authorizes_a_blocked_side_effect() {
        let mut policy = AttachmentSecurityPolicy::default();
        policy.dry_run = true;
        let error = resolve_and_validate_host("test", "127.0.0.1", Some(80), &policy)
            .expect_err("audit mode must remain fail closed");
        assert!(error.to_string().contains("URL_SOURCE_BLOCKED"));
    }

    #[test]
    fn url_sources_require_explicit_enablement_and_allowlist() {
        let policy = AttachmentSecurityPolicy::default();
        assert!(parse_and_validate_url("test", "https://example.com/a", &policy).is_err());

        let mut enabled = policy;
        enabled.allow_url_sources = true;
        assert!(parse_and_validate_url("test", "https://example.com/a", &enabled).is_err());
    }

    #[test]
    fn rejects_url_userinfo_without_exposing_it() {
        let mut policy = AttachmentSecurityPolicy::default();
        policy.allow_url_sources = true;
        policy.url_allowed_domains = vec!["example.com".to_owned()];
        let secret = "do-not-log-this";
        let error = parse_and_validate_url(
            "test",
            format!("https://user:{secret}@example.com/a?token={secret}").as_str(),
            &policy,
        )
        .expect_err("userinfo must be rejected");
        assert!(!format!("{error:#}").contains(secret));
    }

    #[test]
    fn rejects_https_to_http_redirect_even_when_http_is_enabled() {
        let mut policy = AttachmentSecurityPolicy::default();
        policy.allow_url_sources = true;
        policy.allow_http = true;
        policy.allow_private_network = true;
        policy.url_allowed_domains = vec!["example.com".to_owned()];
        let previous = Url::parse("https://example.com/a").unwrap();
        let next = Url::parse("http://example.com/b").unwrap();
        assert!(validate_redirect("test", &previous, &next, &policy).is_err());
    }

    #[test]
    fn allows_private_ip_when_policy_permits() {
        let mut policy = AttachmentSecurityPolicy::default();
        policy.allow_private_network = true;
        resolve_and_validate_host("test", "127.0.0.1", Some(80), &policy)
            .expect("private IP should be allowed");
    }

    #[test]
    fn rejects_connected_peer_from_a_dns_rebinding_decision() {
        let policy = AttachmentSecurityPolicy::default();
        let pinned = [SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
            80,
        )];
        let rebound_peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 80);

        let error = validate_connected_peer("test", rebound_peer, &pinned, &policy)
            .expect_err("a peer outside the pinned DNS result must fail closed");

        assert!(error.to_string().contains("URL_SOURCE_BLOCKED"));
        assert!(error.to_string().contains("private IP blocked"));
    }

    #[test]
    fn rejects_public_peer_that_does_not_match_the_pinned_dns_address() {
        let mut policy = AttachmentSecurityPolicy::default();
        policy.allow_private_network = true;
        let pinned = [SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
            443,
        )];
        let different_peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 9)), 443);

        let error = validate_connected_peer("test", different_peer, &pinned, &policy)
            .expect_err("a connected peer must match the pinned DNS result");

        assert!(
            error
                .to_string()
                .contains("connected peer does not match the pinned DNS decision")
        );
    }

    #[test]
    fn controlled_dns_mixed_public_private_answer_fails_closed() {
        let policy = AttachmentSecurityPolicy::default();
        let result = resolve_and_validate_host_addresses_with(
            "test",
            "controlled.invalid",
            Some(443),
            &policy,
            |_, port| {
                Ok(vec![
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), port),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
                ])
            },
        );
        assert!(
            result.is_err(),
            "one forbidden DNS candidate must reject the decision"
        );
    }

    #[test]
    fn controlled_dns_rebinding_cannot_change_the_pinned_peer_decision() {
        let mut policy = AttachmentSecurityPolicy::default();
        policy.allow_private_network = true;
        let pinned = resolve_and_validate_host_addresses_with(
            "test",
            "controlled.invalid",
            Some(443),
            &policy,
            |_, port| {
                Ok(vec![SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
                    port,
                )])
            },
        )
        .unwrap();
        let rebound = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443);
        assert!(validate_connected_peer("test", rebound, &pinned, &policy).is_err());
    }

    #[test]
    fn domain_policy_enforces_allowlist() {
        let mut policy = AttachmentSecurityPolicy::default();
        policy.url_allowed_domains = vec!["example.com".to_owned()];
        validate_host_name("test", "api.example.com", &policy)
            .expect("subdomain should be allowlisted");
        let err = validate_host_name("test", "openai.com", &policy)
            .expect_err("host outside allowlist must be blocked");
        assert!(err.to_string().contains("URL_SOURCE_BLOCKED"));
    }

    #[test]
    fn empty_url_allowlist_allows_hosts() {
        let policy = AttachmentSecurityPolicy::default();
        validate_host_name("test", "openai.com", &policy)
            .expect("empty url_allowed_domains should not restrict hosts");
    }

    #[test]
    fn empty_path_allowlist_fails_closed_when_enforced() {
        let tmp = std::env::temp_dir().join(format!(
            "pioneer-attachments-empty-allowlist-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(tmp.as_path()).expect("create temp dir");
        let file = tmp.join("sample.bin");
        std::fs::write(file.as_path(), [1u8, 2, 3]).expect("write temp file");

        let mut policy = AttachmentSecurityPolicy::default();
        policy.enforce_path_allowlist = true;
        policy.allowed_path_roots = Vec::new();

        let error = canonicalize_path("test", file.to_string_lossy().as_ref(), &policy)
            .expect_err("enabled empty path allowlist must fail closed");
        assert!(error.to_string().contains("UNSUPPORTED_ATTACHMENT_SOURCE"));
    }

    #[test]
    fn disabled_path_allowlist_does_not_restrict_even_when_roots_are_present() {
        let temp_dir = std::env::temp_dir().join("pioneer-path-allowlist-list-driven");
        std::fs::create_dir_all(temp_dir.as_path()).expect("create temp dir");
        let target = temp_dir.join("sample.bin");
        std::fs::write(target.as_path(), [1u8, 2, 3]).expect("write temp file");

        let mut policy = AttachmentSecurityPolicy::default();
        policy.enforce_path_allowlist = false;
        policy.allowed_path_roots = vec![std::env::temp_dir().join("pioneer-some-other-root")];

        canonicalize_path("test", target.to_string_lossy().as_ref(), &policy)
            .expect("disabled allowlist must not silently enforce configured roots");
    }
}
