use crate::types::ProviderTimeoutPolicy;
use anyhow::{Context, Result, anyhow};
use reqwest::{Client, Proxy, RequestBuilder, Response};
use std::cell::RefCell;

pub(crate) fn build_client(policy: ProviderTimeoutPolicy) -> Client {
    let proxy_url = active_provider_proxy_url();
    build_client_with_proxy(policy, proxy_url.as_deref()).expect("failed to build HTTP client")
}

pub fn validate_proxy_url(proxy_url: &str) -> Result<String> {
    let proxy_url = proxy_url.trim();
    if proxy_url.is_empty() {
        return Err(anyhow!("provider proxy URL must not be empty"));
    }
    // Proxy URLs may contain credentials.  Do not attach the raw value (or
    // reqwest's parser error, which can echo it) to the returned diagnostic.
    Proxy::all(proxy_url).map_err(|_| anyhow!("invalid provider proxy URL"))?;
    Ok(proxy_url.to_owned())
}

pub(crate) fn with_provider_proxy<T>(proxy_url: Option<&str>, f: impl FnOnce() -> T) -> T {
    ACTIVE_PROVIDER_PROXY_URL.with(|active| {
        let previous = active.replace(proxy_url.map(str::to_owned));
        let result = f();
        active.replace(previous);
        result
    })
}

fn build_client_with_proxy(
    policy: ProviderTimeoutPolicy,
    proxy_url: Option<&str>,
) -> Result<Client> {
    let mut builder = Client::builder().connect_timeout(policy.connect_timeout);
    if let Some(proxy_url) = proxy_url {
        builder = builder
            .proxy(Proxy::all(proxy_url).map_err(|_| anyhow!("invalid provider proxy URL"))?);
    }
    builder.build().context("failed to build HTTP client")
}

fn active_provider_proxy_url() -> Option<String> {
    ACTIVE_PROVIDER_PROXY_URL.with(|active| active.borrow().clone())
}

thread_local! {
    static ACTIVE_PROVIDER_PROXY_URL: RefCell<Option<String>> = const { RefCell::new(None) };
}

pub(crate) fn non_stream_request(
    builder: RequestBuilder,
    policy: ProviderTimeoutPolicy,
) -> RequestBuilder {
    builder.timeout(policy.non_stream_request_timeout)
}

pub(crate) fn stream_request(
    builder: RequestBuilder,
    policy: ProviderTimeoutPolicy,
) -> RequestBuilder {
    if let Some(timeout) = policy.max_stream_duration {
        builder.timeout(timeout)
    } else {
        builder
    }
}

pub(crate) async fn send_stream_request(
    builder: RequestBuilder,
    policy: ProviderTimeoutPolicy,
) -> Result<Response> {
    let send = stream_request(builder, policy).send();
    tokio::time::timeout(policy.first_chunk_timeout, send)
        .await
        .map_err(|_| {
            anyhow!(
                "provider stream timed out before response headers after {:?}",
                policy.first_chunk_timeout
            )
        })?
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_proxy_url_accepts_http_and_socks5_urls() {
        assert_eq!(
            validate_proxy_url(" http://user:pass@127.0.0.1:8080 ").expect("http proxy"),
            "http://user:pass@127.0.0.1:8080"
        );
        assert_eq!(
            validate_proxy_url("socks5://127.0.0.1:1080").expect("socks proxy"),
            "socks5://127.0.0.1:1080"
        );
    }

    #[test]
    fn validate_proxy_url_rejects_empty_urls() {
        let error = validate_proxy_url("   ").expect_err("empty proxy should fail");
        assert!(
            format!("{error:#}").contains("proxy URL must not be empty"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn invalid_proxy_url_diagnostics_redact_embedded_credentials() {
        let secret = "proxy-password-should-not-leak";
        let raw = format!("http://proxy-user:{secret}@[");
        let error = validate_proxy_url(raw.as_str()).expect_err("malformed proxy should fail");
        let rendered = format!("{error:#}");
        assert_eq!(rendered, "invalid provider proxy URL");
        assert!(!rendered.contains(secret));
        assert!(!rendered.contains("proxy-user"));
    }

    #[test]
    fn client_builder_diagnostics_redact_embedded_credentials() {
        let secret = "builder-password-should-not-leak";
        let raw = format!("http://builder-user:{secret}@[");
        let error = build_client_with_proxy(ProviderTimeoutPolicy::default(), Some(raw.as_str()))
            .expect_err("malformed proxy should fail client construction");
        let rendered = format!("{error:#}");
        assert_eq!(rendered, "invalid provider proxy URL");
        assert!(!rendered.contains(secret));
        assert!(!rendered.contains("builder-user"));
    }
}
