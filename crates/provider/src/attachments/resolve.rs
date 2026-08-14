use crate::attachments::errors::AttachmentPipelineError;
use crate::attachments::normalize::{estimate_decoded_base64_size, hash_string, normalize_mime};
use crate::attachments::runtime::{self, AttachmentOperationError};
use crate::attachments::security;
use crate::attachments::types::{AttachmentPipelineConfig, PreparedAttachmentSource};
use crate::types::{AttachmentDataSource, InputContentType, MessageAttachment};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::blocking::{Client as BlockingClient, Response as BlockingResponse};
use reqwest::redirect::Policy as RedirectPolicy;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone)]
pub struct ResolvedAttachmentSource {
    pub bytes: Option<Vec<u8>>,
    pub source: PreparedAttachmentSource,
    pub source_label: String,
    pub source_name: Option<String>,
}

pub fn resolve_attachment_source(
    provider_name: &str,
    attachment: &MessageAttachment,
    _kind: InputContentType,
    config: &AttachmentPipelineConfig,
    source_limit: usize,
) -> Result<ResolvedAttachmentSource> {
    let resolved = match &attachment.source {
        AttachmentDataSource::Bytes { base64_data } => {
            let normalized_base64 = compact_base64(base64_data.as_str());
            if normalized_base64.len() > config.normalization.max_base64_chars {
                return Err(AttachmentPipelineError::attachment_too_large(
                    normalized_base64.len(),
                    config.normalization.max_base64_chars,
                    "base64_payload",
                )
                .into());
            }

            let estimated_size = estimate_decoded_base64_size(normalized_base64.as_str());
            if estimated_size > source_limit {
                return Err(AttachmentPipelineError::attachment_too_large(
                    estimated_size,
                    source_limit,
                    "base64_payload",
                )
                .into());
            }

            let decoded = BASE64
                .decode(normalized_base64.as_bytes())
                .context("failed to decode base64 attachment bytes")?;
            ResolvedAttachmentSource {
                bytes: Some(decoded),
                source: PreparedAttachmentSource::Bytes,
                source_label: "bytes".to_owned(),
                source_name: None,
            }
        }
        AttachmentDataSource::Path { path } => {
            let canonical = security::canonicalize_path(provider_name, path, &config.security)?;
            let loaded = read_file_limited(canonical.as_path(), source_limit)?;
            let source_name = canonical
                .file_name()
                .and_then(|value| value.to_str())
                .map(str::to_owned);
            ResolvedAttachmentSource {
                bytes: Some(loaded),
                source: PreparedAttachmentSource::Path {
                    path: canonical.display().to_string(),
                },
                source_label: format!("path:{}", canonical.display()),
                source_name,
            }
        }
        AttachmentDataSource::Url { url } => {
            let parsed = security::parse_and_validate_url(provider_name, url, &config.security)?;
            let bytes = fetch_url_attachment(
                provider_name,
                parsed.as_str(),
                attachment.mime_type.as_str(),
                config,
                source_limit,
            )?;
            let source_name = parsed
                .path_segments()
                .and_then(|segments| segments.last())
                .filter(|segment| !segment.trim().is_empty())
                .map(str::to_owned);
            ResolvedAttachmentSource {
                bytes: Some(bytes),
                source: PreparedAttachmentSource::Url {
                    url: parsed.to_string(),
                },
                source_label: format!("url:{}", parsed),
                source_name,
            }
        }
        AttachmentDataSource::Reference { reference } => {
            if reference.trim().is_empty() {
                return Err(AttachmentPipelineError::unsupported_attachment_source(
                    "empty_reference",
                )
                .into());
            }
            ResolvedAttachmentSource {
                bytes: None,
                source: PreparedAttachmentSource::Reference {
                    reference: reference.clone(),
                },
                source_label: format!("ref:{reference}"),
                source_name: None,
            }
        }
    };

    Ok(resolved)
}

fn read_file_limited(path: &std::path::Path, max_bytes: usize) -> Result<Vec<u8>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open attachment path `{}`", path.display()))?;
    if let Ok(metadata) = file.metadata()
        && metadata.is_file()
        && metadata.len() > max_bytes as u64
    {
        return Err(AttachmentPipelineError::attachment_too_large(
            usize::try_from(metadata.len()).unwrap_or(usize::MAX),
            max_bytes,
            path.display().to_string().as_str(),
        )
        .into());
    }
    let mut bytes = Vec::new();
    file.take(max_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read attachment path `{}`", path.display()))?;
    if bytes.len() > max_bytes {
        return Err(AttachmentPipelineError::attachment_too_large(
            bytes.len(),
            max_bytes,
            path.display().to_string().as_str(),
        )
        .into());
    }
    Ok(bytes)
}

fn compact_base64(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect()
}

fn fetch_url_attachment(
    provider_name: &str,
    raw_url: &str,
    expected_mime: &str,
    config: &AttachmentPipelineConfig,
    source_limit: usize,
) -> Result<Vec<u8>> {
    let mut current_url =
        security::parse_and_validate_url(provider_name, raw_url, &config.security)?;
    let expected_mime = normalize_mime(expected_mime)?;

    for redirect_index in 0..=config.security.max_url_redirects {
        security::validate_url(provider_name, &current_url, &config.security)?;
        let host = current_url
            .host_str()
            .ok_or_else(|| AttachmentPipelineError::url_source_blocked("URL host is missing"))?;
        let pinned_addresses = security::resolve_and_validate_host_addresses(
            provider_name,
            host,
            current_url.port_or_known_default(),
            &config.security,
        )?;
        let client = build_blocking_client(
            config.security.url_fetch_timeout_ms,
            host,
            pinned_addresses.as_slice(),
        )?;

        let endpoint_identity = format!(
            "{}://{}:{}",
            current_url.scheme(),
            current_url.host_str().unwrap_or("unknown"),
            current_url.port_or_known_default().unwrap_or_default(),
        );
        let operation_authority = runtime::AttachmentOperationAuthority::new(
            runtime::current_authority_fingerprint()?,
            "url_fetch",
            endpoint_identity,
        );
        let response = match runtime::execute_with_retry_blocking(
            provider_name,
            "url_fetch",
            &operation_authority,
            &config.runtime,
            |_| send_url_request(&client, &current_url),
        ) {
            Ok(response) => response,
            Err(error) => {
                if let Some(reqwest) = error.downcast_ref::<reqwest::Error>()
                    && reqwest.is_timeout()
                {
                    return Err(AttachmentPipelineError::url_fetch_timeout(
                        current_url.as_str(),
                        config.security.url_fetch_timeout_ms,
                    )
                    .into());
                }
                return Err(error);
            }
        };
        let peer = response.remote_addr().ok_or_else(|| {
            AttachmentPipelineError::url_source_blocked(
                "attachment fetch did not expose its connected peer",
            )
        })?;
        security::validate_connected_peer(
            provider_name,
            peer,
            pinned_addresses.as_slice(),
            &config.security,
        )?;

        if response.status().is_redirection() {
            if redirect_index >= config.security.max_url_redirects {
                return Err(AttachmentPipelineError::url_redirect_blocked(format!(
                    "too many redirects while fetching `{raw_url}`"
                ))
                .into());
            }
            let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
                return Err(AttachmentPipelineError::url_redirect_blocked(
                    "redirect response missing Location header",
                )
                .into());
            };
            let location = location.to_str().map_err(|_| {
                AttachmentPipelineError::url_redirect_blocked("invalid Location header")
            })?;
            let next_url = current_url.join(location).map_err(|_| {
                AttachmentPipelineError::url_redirect_blocked("failed to resolve redirect URL")
            })?;
            security::validate_url(provider_name, &next_url, &config.security)?;
            current_url = next_url;
            continue;
        }

        let response_limit = config.security.url_fetch_max_bytes.min(source_limit);
        if let Some(content_length) = response.content_length()
            && content_length > response_limit as u64
        {
            return Err(AttachmentPipelineError::url_fetch_budget_exceeded(response_limit).into());
        }

        if !response.status().is_success() {
            let status = response.status();
            let diagnostic_limit = response_limit.min(16 * 1024);
            let body = read_response_limited(response, diagnostic_limit)?;
            let body = String::from_utf8_lossy(&body);
            let body_preview = body.chars().take(4_096).collect::<String>();
            return Err(anyhow!(
                "URL fetch failed for `{}` with status {}: {}",
                current_url,
                status,
                body_preview
            ));
        }

        validate_observed_content_type(
            expected_mime.as_str(),
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            config,
        )?;

        return read_response_limited(response, response_limit);
    }

    Err(AttachmentPipelineError::url_redirect_blocked(format!(
        "redirect resolution failed for `{raw_url}`"
    ))
    .into())
}

fn validate_observed_content_type(
    expected_mime: &str,
    observed_content_type: Option<&str>,
    config: &AttachmentPipelineConfig,
) -> Result<()> {
    let Some(observed_raw) = observed_content_type else {
        return Ok(());
    };

    let observed_base = observed_raw
        .split(';')
        .next()
        .map(str::trim)
        .unwrap_or_default();
    if observed_base.is_empty() {
        return Ok(());
    }

    let observed_mime = normalize_mime(observed_base)?;
    if observed_mime == expected_mime {
        return Ok(());
    }

    if observed_mime == "application/octet-stream" {
        return Ok(());
    }

    if config.normalization.strict_mime_match {
        return Err(
            AttachmentPipelineError::mime_mismatch(expected_mime, observed_mime.as_str()).into(),
        );
    }

    let expected_top = expected_mime.split('/').next().unwrap_or_default();
    let observed_top = observed_mime.split('/').next().unwrap_or_default();
    if expected_top != observed_top {
        return Err(
            AttachmentPipelineError::mime_mismatch(expected_mime, observed_mime.as_str()).into(),
        );
    }

    Ok(())
}

fn send_url_request(
    client: &BlockingClient,
    current_url: &Url,
) -> std::result::Result<BlockingResponse, AttachmentOperationError> {
    let response = client
        .get(current_url.clone())
        .send()
        .map_err(classify_reqwest_error)?;
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return Err(AttachmentOperationError::retryable(anyhow!(
            "transient URL fetch response for `{}`: {}",
            current_url,
            status
        )));
    }
    Ok(response)
}

fn classify_reqwest_error(error: reqwest::Error) -> AttachmentOperationError {
    if error.is_timeout() {
        return AttachmentOperationError::retryable(error);
    }
    if error.is_connect() || error.is_request() || error.is_body() || error.is_decode() {
        return AttachmentOperationError::retryable(error);
    }
    AttachmentOperationError::non_retryable(error)
}

fn build_blocking_client(
    timeout_ms: u64,
    host: &str,
    pinned_addresses: &[SocketAddr],
) -> Result<BlockingClient> {
    BlockingClient::builder()
        .timeout(Duration::from_millis(timeout_ms.max(1)))
        .redirect(RedirectPolicy::none())
        .no_proxy()
        .resolve_to_addrs(host, pinned_addresses)
        .build()
        .context("failed to build blocking HTTP client for attachment URL fetch")
}

fn read_response_limited(response: BlockingResponse, max_bytes: usize) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut limited = response.take((max_bytes.saturating_add(1)) as u64);
    limited.read_to_end(&mut buffer)?;
    if buffer.len() > max_bytes {
        return Err(AttachmentPipelineError::url_fetch_budget_exceeded(max_bytes).into());
    }
    Ok(buffer)
}

pub fn resolve_sha256(
    provided_sha256: Option<&str>,
    bytes: Option<&[u8]>,
    source_label: &str,
) -> Result<String> {
    if let Some(sha) = provided_sha256
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let normalized = sha.to_ascii_lowercase();
        if let Some(data) = bytes {
            let computed = crate::attachments::normalize::hash_bytes(data);
            if normalized != computed {
                return Err(AttachmentPipelineError::contract_violation(format!(
                    "provided sha256 does not match attachment content (provided={}, computed={computed})",
                    normalized
                ))
                .into());
            }
        }
        return Ok(normalized);
    }

    if let Some(data) = bytes {
        return Ok(crate::attachments::normalize::hash_bytes(data));
    }

    Ok(hash_string(source_label))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_type_guard_allows_missing_header() {
        let config = AttachmentPipelineConfig::default();
        validate_observed_content_type("image/png", None, &config)
            .expect("missing header should pass");
    }

    #[test]
    fn content_type_guard_rejects_cross_media_mismatch() {
        let config = AttachmentPipelineConfig::default();
        let err = validate_observed_content_type("image/png", Some("text/plain"), &config)
            .expect_err("cross-media mismatch must fail");
        assert!(err.to_string().contains("MIME_MISMATCH"));
    }

    #[test]
    fn content_type_guard_allows_octet_stream_fallback() {
        let config = AttachmentPipelineConfig::default();
        validate_observed_content_type(
            "application/pdf",
            Some("application/octet-stream"),
            &config,
        )
        .expect("octet-stream should be accepted as generic fallback");
    }
}
