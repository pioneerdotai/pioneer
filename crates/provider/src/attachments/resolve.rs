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

/// Async source I/O runs before bounded CPU normalization; redirects/retries
/// spend the owning attempt allowance and retain production pinned-peer guards.
pub(crate) async fn resolve_attachment_source_async(
    provider: &str,
    attachment: &MessageAttachment,
    kind: InputContentType,
    config: &AttachmentPipelineConfig,
    limit: usize,
) -> Result<ResolvedAttachmentSource> {
    runtime::bounded_work(async {
        runtime::check_work()?;
        match &attachment.source {
            AttachmentDataSource::Path { path } => {
                use tokio::io::AsyncReadExt;
                let canonical = tokio::fs::canonicalize(path).await.map_err(|_| AttachmentPipelineError::unsupported_attachment_source("file_open"))?;
                if config.security.enforce_path_allowlist {
                    let mut allowed = false;
                    for root in &config.security.allowed_path_roots {
                        runtime::check_work()?;
                        if let Ok(root) = tokio::fs::canonicalize(root).await {
                            allowed |= canonical.starts_with(root);
                        }
                    }
                    if !allowed { return Err(AttachmentPipelineError::unsupported_attachment_source("path_outside_allowed_roots").into()); }
                }
                let mut file = tokio::fs::File::open(&canonical).await.map_err(|_| AttachmentPipelineError::unsupported_attachment_source("file_open"))?;
                let metadata = file.metadata().await.map_err(|_| AttachmentPipelineError::unsupported_attachment_source("file_metadata"))?;
                if !metadata.is_file() { return Err(AttachmentPipelineError::unsupported_attachment_source("regular_file_required").into()); }
                if metadata.len() > limit as u64 { return Err(AttachmentPipelineError::attachment_too_large(metadata.len() as usize, limit, "path").into()); }
                let mut bytes = Vec::new();
                let mut chunk = [0; 64 * 1024];
                loop {
                    runtime::check_work()?;
                    let count = file.read(&mut chunk).await.map_err(|_| AttachmentPipelineError::unsupported_attachment_source("file_read"))?;
                    if count == 0 { break; }
                    if count > limit.saturating_sub(bytes.len()) { return Err(AttachmentPipelineError::attachment_too_large(bytes.len().saturating_add(count), limit, "path").into()); }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                Ok(ResolvedAttachmentSource { bytes: Some(bytes), source: PreparedAttachmentSource::Bytes,
                    source_label: "path:[redacted]".to_owned(), source_name: canonical.file_name().and_then(|s| s.to_str()).map(str::to_owned) })
            }
            AttachmentDataSource::Url { url } => {
                let parsed = security::parse_url_without_dns(provider, url, &config.security)?;
                #[cfg(test)]
                let fixture = super::admission::current().and_then(|state| state.fixture_url(parsed.as_str()));
                #[cfg(not(test))]
                let fixture: Option<Vec<u8>> = None;
                let bytes = match fixture {
                    Some(bytes) if bytes.len() <= limit => bytes,
                    Some(_) => return Err(AttachmentPipelineError::url_fetch_budget_exceeded(limit).into()),
                    None => fetch_url_attachment_async(provider, parsed.clone(), &attachment.mime_type, config, limit).await?,
                };
                Ok(ResolvedAttachmentSource { bytes: Some(bytes), source: PreparedAttachmentSource::Bytes,
                    source_label: "url:[redacted]".to_owned(), source_name: parsed.path_segments().and_then(|s| s.last()).filter(|s| !s.is_empty()).map(str::to_owned) })
            }
            _ => resolve_attachment_source(provider, attachment, kind, config, limit),
        }
    }).await
}

async fn fetch_url_attachment_async(
    provider: &str,
    mut url: Url,
    expected_mime: &str,
    config: &AttachmentPipelineConfig,
    limit: usize,
) -> Result<Vec<u8>> {
    use futures_util::StreamExt;
    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(config.security.url_fetch_timeout_ms.max(1));
    tokio::time::timeout_at(deadline, async {
        for redirect in 0..=config.security.max_url_redirects {
            runtime::check_work()?;
            security::validate_url_without_dns(provider, &url, &config.security)?;
            let host = url
                .host_str()
                .ok_or_else(|| AttachmentPipelineError::url_source_blocked("missing host"))?;
            let addresses =
                tokio::net::lookup_host((host, url.port_or_known_default().unwrap_or(443)))
                    .await
                    .map_err(|_| AttachmentPipelineError::url_source_blocked("dns failure"))?;
            let mut pinned = Vec::new();
            for address in addresses {
                if pinned.len() >= 64 {
                    return Err(AttachmentPipelineError::url_source_blocked(
                        "DNS cardinality limit",
                    )
                    .into());
                }
                security::validate_connected_peer(provider, address, &[address], &config.security)?;
                if !pinned.contains(&address) {
                    pinned.push(address);
                }
            }
            if pinned.is_empty() {
                return Err(AttachmentPipelineError::url_source_blocked("DNS empty").into());
            }
            let client = reqwest::Client::builder()
                .redirect(RedirectPolicy::none())
                .no_proxy()
                .resolve_to_addrs(host, &pinned)
                .build()
                .map_err(|_| AttachmentPipelineError::url_source_blocked("client build"))?;
            let authority = runtime::AttachmentOperationAuthority::new(
                runtime::current_authority_fingerprint()?,
                "url_fetch",
                format!(
                    "{}://{}:{}",
                    url.scheme(),
                    host,
                    url.port_or_known_default().unwrap_or(443)
                ),
            );
            let response = runtime::execute_with_retry_async(
                provider,
                "url_fetch",
                &authority,
                &config.runtime,
                |_| async {
                    let response = client
                        .get(url.clone())
                        .send()
                        .await
                        .map_err(classify_reqwest_error)?;
                    let status = response.status();
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
                    {
                        return Err(AttachmentOperationError::retryable(
                            crate::failure::ProviderTransportFailure {
                                classification: crate::failure::classify_transport_status(
                                    Some(status.as_u16()),
                                    None,
                                ),
                            },
                        ));
                    }
                    Ok(response)
                },
            )
            .await?;
            let peer = response
                .remote_addr()
                .ok_or_else(|| AttachmentPipelineError::url_source_blocked("missing peer"))?;
            security::validate_connected_peer(provider, peer, &pinned, &config.security)?;
            if response.status().is_redirection() {
                if redirect == config.security.max_url_redirects {
                    return Err(
                        AttachmentPipelineError::url_redirect_blocked("redirect limit").into(),
                    );
                }
                let next = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| url.join(v).ok())
                    .ok_or_else(|| {
                        AttachmentPipelineError::url_redirect_blocked("invalid location")
                    })?;
                if url.scheme() == "https" && next.scheme() == "http" {
                    return Err(AttachmentPipelineError::url_redirect_blocked("downgrade").into());
                }
                url = next;
                continue;
            }
            if !response.status().is_success() {
                return Err(crate::failure::ProviderTransportFailure {
                    classification: crate::failure::classify_transport_status(
                        Some(response.status().as_u16()),
                        None,
                    ),
                }
                .into());
            }
            validate_observed_content_type(
                expected_mime,
                response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok()),
                config,
            )?;
            let limit = limit.min(config.security.url_fetch_max_bytes);
            if response
                .content_length()
                .is_some_and(|length| length > limit as u64)
            {
                return Err(AttachmentPipelineError::url_fetch_budget_exceeded(limit).into());
            }
            let mut bytes = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                runtime::check_work()?;
                let chunk = chunk.map_err(crate::failure::safe_transport_error)?;
                if chunk.len() > limit.saturating_sub(bytes.len()) {
                    return Err(AttachmentPipelineError::url_fetch_budget_exceeded(limit).into());
                }
                bytes.extend_from_slice(&chunk);
            }
            return Ok(bytes);
        }
        Err(AttachmentPipelineError::url_redirect_blocked("redirect failure").into())
    })
    .await
    .map_err(|_| crate::failure::transport_timeout_error())?
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
            if source_limit == 0 {
                return Err(
                    AttachmentPipelineError::attachment_too_large(1, 0, "base64_payload").into(),
                );
            }
            // Keep both the wire scan and normalized allocation proportional
            // to the remaining aggregate budget, not the global attachment cap.
            let budget_chars = source_limit
                .saturating_add(2)
                .checked_div(3)
                .unwrap_or(usize::MAX)
                .saturating_mul(4)
                .saturating_add(4);
            let normalized_base64 = compact_base64(
                base64_data.as_str(),
                config.normalization.max_base64_chars.min(budget_chars),
            )?;

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
            if decoded.len() > source_limit {
                return Err(AttachmentPipelineError::attachment_too_large(
                    decoded.len(),
                    source_limit,
                    "base64_payload",
                )
                .into());
            }
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
                source_label: "path:[redacted]".to_owned(),
                source_name,
            }
        }
        AttachmentDataSource::Url { url } => {
            let parsed = security::parse_and_validate_url(provider_name, url, &config.security)?;
            // Fixture transport replaces only HTTP delivery, after production
            // opt-in/domain/URL guards; no mock server or live network required.
            #[cfg(test)]
            let fixture = super::admission::current().and_then(|s| s.fixture_url(parsed.as_str()));
            #[cfg(not(test))]
            let fixture: Option<Vec<u8>> = None;
            let bytes = if let Some(bytes) = fixture {
                anyhow::ensure!(
                    bytes.len() <= source_limit,
                    "fixture URL exceeds source limit"
                );
                bytes
            } else {
                fetch_url_attachment(
                    provider_name,
                    parsed.as_str(),
                    attachment.mime_type.as_str(),
                    config,
                    source_limit,
                )?
            };
            let source_name = parsed
                .path_segments()
                .and_then(|segments| segments.last())
                .filter(|segment| !segment.trim().is_empty())
                .map(str::to_owned);
            ResolvedAttachmentSource {
                bytes: Some(bytes),
                source: PreparedAttachmentSource::Url {
                    // The fetched bytes are authoritative from this point.
                    // Persist only a credential/query-free origin label.
                    url: security::safe_url_origin(&parsed),
                },
                source_label: security::safe_url_label(&parsed),
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
                source_label: "reference:[redacted]".to_owned(),
                source_name: None,
            }
        }
    };

    Ok(resolved)
}

fn read_file_limited(path: &std::path::Path, max_bytes: usize) -> Result<Vec<u8>> {
    runtime::check_work()?;
    let mut file = File::open(path)
        .map_err(|_| AttachmentPipelineError::unsupported_attachment_source("file_open"))?;
    if let Ok(metadata) = file.metadata()
        && metadata.is_file()
        && metadata.len() > max_bytes as u64
    {
        return Err(AttachmentPipelineError::attachment_too_large(
            usize::try_from(metadata.len()).unwrap_or(usize::MAX),
            max_bytes,
            "path",
        )
        .into());
    }
    if !file
        .metadata()
        .map_err(|_| AttachmentPipelineError::unsupported_attachment_source("file_metadata"))?
        .is_file()
    {
        return Err(AttachmentPipelineError::unsupported_attachment_source(
            "regular_file_required",
        )
        .into());
    }
    let mut bytes = Vec::new();
    let mut chunk = [0; 64 * 1024];
    loop {
        runtime::check_work()?;
        let count = file
            .read(&mut chunk)
            .map_err(|_| AttachmentPipelineError::unsupported_attachment_source("file_read"))?;
        if count == 0 {
            break;
        }
        if count > max_bytes.saturating_sub(bytes.len()) {
            return Err(AttachmentPipelineError::attachment_too_large(
                bytes.len().saturating_add(count),
                max_bytes,
                "path",
            )
            .into());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    if bytes.len() > max_bytes {
        return Err(
            AttachmentPipelineError::attachment_too_large(bytes.len(), max_bytes, "path").into(),
        );
    }
    Ok(bytes)
}

fn compact_base64(value: &str, max_chars: usize) -> Result<String> {
    // Whitespace is accepted in the wire representation, but it cannot turn
    // into an unbounded pre-validation allocation.  A bounded slack still
    // permits normal wrapped base64 while rejecting pathological input before
    // constructing the normalized String.
    let raw_limit = max_chars.saturating_mul(4).max(max_chars);
    if value.len() > raw_limit {
        return Err(AttachmentPipelineError::attachment_too_large(
            value.len(),
            raw_limit,
            "base64_payload",
        )
        .into());
    }

    let mut normalized = String::with_capacity(value.len().min(max_chars));
    for (index, character) in value.chars().enumerate() {
        if index % 65_536 == 0 {
            runtime::check_work()?;
        }
        if character.is_ascii_whitespace() {
            continue;
        }
        if normalized.len() >= max_chars {
            return Err(AttachmentPipelineError::attachment_too_large(
                normalized.len().saturating_add(character.len_utf8()),
                max_chars,
                "base64_payload",
            )
            .into());
        }
        normalized.push(character);
    }
    Ok(normalized)
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
        runtime::check_work()?;
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
                    "too many redirects while fetching {}",
                    security::safe_url_label(&current_url)
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
            security::validate_redirect(provider_name, &current_url, &next_url, &config.security)?;
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
            return Err(AttachmentPipelineError::http_status(response.status().as_u16()).into());
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

    Err(AttachmentPipelineError::url_redirect_blocked("redirect resolution failed").into())
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
            security::safe_url_label(current_url),
            status
        )));
    }
    Ok(response)
}

fn classify_reqwest_error(error: reqwest::Error) -> AttachmentOperationError {
    let retryable = error.is_timeout()
        || error.is_connect()
        || error.is_request()
        || error.is_body()
        || error.is_decode();
    let safe = crate::failure::safe_transport_error(error);
    if retryable {
        return AttachmentOperationError::retryable(safe);
    }
    AttachmentOperationError::non_retryable(safe)
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
    limited
        .read_to_end(&mut buffer)
        .map_err(|_| AttachmentPipelineError::unsupported_attachment_source("response_read"))?;
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
    use std::io::Write as _;
    use std::net::TcpListener;

    fn chunked_response_server(
        response_body: &'static str,
    ) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("attachment test listener");
        let address = listener.local_addr().expect("attachment test address");
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("attachment request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).expect("read attachment request");
            let response = format!(
                "HTTP/1.1 404 Not Found\r\nTransfer-Encoding: chunked\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                response_body.len(),
                response_body,
            );
            stream
                .write_all(response.as_bytes())
                .expect("write attachment response");
        });
        (format!("http://{address}/attachment"), server)
    }

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

    #[test]
    fn base64_normalization_rejects_oversized_wire_input_before_decoding() {
        let mut config = AttachmentPipelineConfig::default();
        config.normalization.max_base64_chars = 8;
        let attachment = MessageAttachment {
            mime_type: "application/octet-stream".to_owned(),
            name: Some("payload.bin".to_owned()),
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data: "A".repeat(40),
            },
            artifact: None,
        };

        let error =
            resolve_attachment_source("test", &attachment, InputContentType::File, &config, 1024)
                .expect_err("oversized base64 must fail before decode");
        assert!(error.to_string().contains("ATTACHMENT_TOO_LARGE"));
    }

    #[test]
    fn base64_normalization_is_bounded_by_remaining_aggregate_budget() {
        let config = AttachmentPipelineConfig::default();
        let attachment = MessageAttachment {
            mime_type: "application/octet-stream".to_owned(),
            name: None,
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data: BASE64.encode(vec![7_u8; 1024]),
            },
            artifact: None,
        };
        let error =
            resolve_attachment_source("test", &attachment, InputContentType::File, &config, 1)
                .expect_err("remaining budget must bound normalization before decode");
        assert!(error.to_string().contains("ATTACHMENT_TOO_LARGE"));
    }

    #[test]
    fn chunked_http_error_body_is_never_materialized_for_diagnostics() {
        let (url, server) = chunked_response_server(
            "this diagnostic body is deliberately larger than the attachment budget",
        );
        let mut config = AttachmentPipelineConfig::default();
        config.security.allow_url_sources = true;
        config.security.allow_http = true;
        config.security.allow_private_network = true;
        config.security.url_allowed_domains = vec!["127.0.0.1".to_owned()];
        config.security.url_fetch_max_bytes = 32;
        config.runtime.retry.max_attempts = 1;

        let attachment = MessageAttachment {
            mime_type: "image/png".to_owned(),
            name: Some("error.png".to_owned()),
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Url { url: url.clone() },
            artifact: None,
        };
        let error =
            runtime::with_blocking_authority_scope("attachment-resolve-test".to_owned(), || {
                resolve_attachment_source("test", &attachment, InputContentType::Image, &config, 32)
            })
            .expect_err("oversized chunked error diagnostics must be rejected");

        assert!(error.to_string().contains("URL_FETCH_HTTP_ERROR"));
        assert!(!error.to_string().contains("deliberately larger"));
        server.join().expect("attachment test server");
    }
}

#[cfg(test)]
mod r04_source_tests {
    use super::*;

    #[tokio::test]
    async fn original_url_ingress_is_read_once_and_owned_bytes_ignore_changed_response() {
        let state = std::sync::Arc::new(super::super::admission::AdmissionState::default());
        let url = "https://example.com/original?token=SECRET";
        let original = b"original immutable bytes";
        let mut config = AttachmentPipelineConfig::default();
        config.security.allow_url_sources = true;
        config.security.url_allowed_domains = vec!["example.com".into()];
        for kind in [
            InputContentType::Image,
            InputContentType::File,
            InputContentType::Audio,
            InputContentType::Video,
        ] {
            state
                .url_fixtures
                .write()
                .unwrap()
                .insert(url.into(), original.to_vec());
            let source = MessageAttachment::from_url(url, "application/octet-stream");
            let bytes = super::super::admission::scope(
                state.clone(),
                super::super::read_accepted_source_bytes(
                    "fixture",
                    "r04-url-ingress".into(),
                    &source,
                    kind,
                    &config,
                    1024,
                ),
            )
            .await
            .unwrap();
            assert_eq!(bytes, original);
            state
                .url_fixtures
                .write()
                .unwrap()
                .insert(url.into(), b"replacement".to_vec());
            let owned = MessageAttachment {
                source: super::super::owned_bytes_source(&bytes),
                sha256: Some(resolve_sha256(None, Some(&bytes), "source").unwrap()),
                ..source
            };
            let replay = super::super::admission::scope(
                state.clone(),
                super::super::read_accepted_source_bytes(
                    "fixture",
                    "r04-url-ingress".into(),
                    &owned,
                    kind,
                    &config,
                    1024,
                ),
            )
            .await
            .unwrap();
            assert_eq!(replay, original);
        }
        assert_eq!(
            state.url_reads.load(std::sync::atomic::Ordering::Relaxed),
            4
        );
        let mut mismatched = MessageAttachment::from_url(url, "application/octet-stream");
        mismatched.sha256 = Some("wrong-SECRET-hash".into());
        let error = super::super::admission::scope(
            state,
            super::super::read_accepted_source_bytes(
                "fixture",
                "r04-url-ingress".into(),
                &mismatched,
                InputContentType::File,
                &config,
                1024,
            ),
        )
        .await
        .unwrap_err();
        assert!(!format!("{error:#?}").contains("SECRET"));
    }

    #[tokio::test]
    async fn stopped_source_does_not_open_path_or_resolve_url_and_diagnostics_are_safe() {
        let context =
            crate::ProviderAttemptContext::new(crate::ProviderTimeoutPolicy::default(), false)
                .unwrap();
        context.cancellation.cancel();
        let config = AttachmentPipelineConfig::default();
        for attachment in [
            MessageAttachment::from_path("/private/SECRET/missing.png", "image/png"),
            MessageAttachment::from_url(
                "https://user:SECRET@example.invalid/object?token=SECRET",
                "image/png",
            ),
        ] {
            let child = crate::ProviderAttemptContext {
                cancellation: context.cancellation.child_token(),
                ..context.clone()
            };
            let error = crate::with_provider_attempt_context(
                child,
                resolve_attachment_source_async(
                    "fixture",
                    &attachment,
                    InputContentType::Image,
                    &config,
                    1024,
                ),
            )
            .await
            .unwrap_err();
            assert!(crate::failure::classify_transport_error(&error).is_some());
            assert!(!format!("{error:#?}").contains("SECRET"));
        }
    }

    #[tokio::test]
    async fn async_file_read_is_bounded_and_rejects_declared_hash_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("SECRET-file.bin");
        tokio::fs::write(&path, b"original bytes").await.unwrap();
        let attachment =
            MessageAttachment::from_path(path.to_string_lossy(), "application/octet-stream");
        let config = AttachmentPipelineConfig::default();
        let error = resolve_attachment_source_async(
            "fixture",
            &attachment,
            InputContentType::File,
            &config,
            3,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("ATTACHMENT_TOO_LARGE"));
        assert!(!format!("{error:#?}").contains("SECRET"));
        let source = resolve_attachment_source_async(
            "fixture",
            &attachment,
            InputContentType::File,
            &config,
            1024,
        )
        .await
        .unwrap();
        assert_eq!(source.bytes.as_deref(), Some(b"original bytes".as_slice()));
        assert!(
            resolve_sha256(Some("wrong-SECRET-hash"), source.bytes.as_deref(), "source").is_err()
        );
        let hash_error =
            resolve_sha256(Some("wrong-SECRET-hash"), source.bytes.as_deref(), "source")
                .unwrap_err();
        assert!(!format!("{hash_error:#?}").contains("SECRET"));
    }

    #[tokio::test]
    async fn stop_cancels_a_stalled_body_without_another_fetch() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let headers_written = std::sync::Arc::new(tokio::sync::Notify::new());
        let notify = headers_written.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            socket.read(&mut buffer).await.unwrap();
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
            notify.notify_one();
            std::future::pending::<()>().await;
        });
        let context =
            crate::ProviderAttemptContext::new(crate::ProviderTimeoutPolicy::default(), false)
                .unwrap();
        let mut config = AttachmentPipelineConfig::default();
        config.security.allow_url_sources = true;
        config.security.allow_http = true;
        config.security.allow_private_network = true; // fixture-only explicit authority
        config.security.url_allowed_domains = vec!["127.0.0.1".into()];
        let attachment = MessageAttachment::from_url(
            format!("http://{address}/source?token=SECRET"),
            "application/octet-stream",
        );
        let cancel = context.cancellation.clone();
        let (result, ()) = tokio::join!(
            crate::with_provider_attempt_context(
                context,
                runtime::with_async_authority_scope(
                    "r04-stalled-test".into(),
                    resolve_attachment_source_async(
                        "fixture",
                        &attachment,
                        InputContentType::File,
                        &config,
                        1024
                    )
                )
            ),
            async {
                headers_written.notified().await;
                cancel.cancel();
            }
        );
        let error = result.unwrap_err();
        assert!(crate::failure::classify_transport_error(&error).is_some());
        assert!(!format!("{error:#?}").contains("SECRET"));
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    #[test]
    fn locator_and_body_fields_never_enter_typed_attachment_diagnostics() {
        for error in [
            AttachmentPipelineError::unsupported_attachment_source("/private/SECRET"),
            AttachmentPipelineError::url_fetch_timeout(
                "https://user:SECRET@host/path?token=SECRET",
                5,
            ),
            AttachmentPipelineError::retry_exhausted("upload", 3, "upstream SECRET body"),
            AttachmentPipelineError::invalid_file_name("/private/SECRET"),
            AttachmentPipelineError::invalid_mime("application/SECRET"),
            AttachmentPipelineError::contract_violation("nested SECRET chain"),
        ] {
            assert!(!format!("{error:?} {error}").contains("SECRET"));
        }
    }
}
