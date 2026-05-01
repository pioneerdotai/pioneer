use crate::attachments::errors::AttachmentPipelineError;
use crate::attachments::observability;
use crate::attachments::types::AttachmentSecurityPolicy;
use anyhow::Result;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};
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

    // Allowlist semantics are list-driven.
    // Empty list means "allow all"; non-empty list means strict allowlist.
    if !policy.allowed_path_roots.is_empty() {
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
                format!("path:{trimmed}").as_str(),
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
        let _ = enforce_or_dry_run(
            provider_name,
            format!("url:{raw_url}").as_str(),
            "URL attachment sources are disabled by policy",
            policy,
            AttachmentPipelineError::url_source_blocked("url sources disabled"),
        )?;
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
    let scheme = url.scheme().to_ascii_lowercase();
    if scheme != "https" && !(policy.allow_http && scheme == "http") {
        return enforce_or_dry_run(
            provider_name,
            format!("url:{url}").as_str(),
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
    let port = port.unwrap_or(443);

    if let Ok(ip) = host.parse::<IpAddr>() {
        return validate_ip(provider_name, ip, policy);
    }

    let resolved = (host, port).to_socket_addrs().map_err(|error| {
        AttachmentPipelineError::url_source_blocked(format!("dns resolve failed: {error}"))
    })?;

    let mut saw_any = false;
    for addr in resolved {
        saw_any = true;
        validate_ip(provider_name, addr.ip(), policy)?;
    }

    if !saw_any {
        return Err(AttachmentPipelineError::url_source_blocked(
            "dns resolve returned no addresses",
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
    if policy.dry_run {
        return Ok(true);
    }
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
    fn allows_private_ip_when_policy_permits() {
        let mut policy = AttachmentSecurityPolicy::default();
        policy.allow_private_network = true;
        resolve_and_validate_host("test", "127.0.0.1", Some(80), &policy)
            .expect("private IP should be allowed");
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
    fn empty_path_allowlist_does_not_restrict_when_enforced() {
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

        canonicalize_path("test", file.to_string_lossy().as_ref(), &policy)
            .expect("empty allowed_path_roots should not restrict paths");
    }

    #[test]
    fn non_empty_path_allowlist_restricts_even_when_enforce_flag_is_false() {
        let temp_dir = std::env::temp_dir().join("pioneer-path-allowlist-list-driven");
        std::fs::create_dir_all(temp_dir.as_path()).expect("create temp dir");
        let target = temp_dir.join("sample.bin");
        std::fs::write(target.as_path(), [1u8, 2, 3]).expect("write temp file");

        let mut policy = AttachmentSecurityPolicy::default();
        policy.enforce_path_allowlist = false;
        policy.allowed_path_roots = vec![std::env::temp_dir().join("pioneer-some-other-root")];

        let err = canonicalize_path("test", target.to_string_lossy().as_ref(), &policy)
            .expect_err("non-empty allowed_path_roots must restrict paths");
        assert!(err.to_string().contains("UNSUPPORTED_ATTACHMENT_SOURCE"));
    }
}
