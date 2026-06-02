use crate::types::ProviderTimeoutPolicy;
use anyhow::{Result, anyhow};
use reqwest::{Client, RequestBuilder, Response};

pub(crate) fn build_client(policy: ProviderTimeoutPolicy) -> Client {
    Client::builder()
        .connect_timeout(policy.connect_timeout)
        .build()
        .expect("failed to build HTTP client")
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
