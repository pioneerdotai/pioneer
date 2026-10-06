use pioneer_protocol::{
    JSONRPC_VERSION, JsonRpcError, JsonRpcErrorResponse, PUBLIC_ERROR_VERSION, PublicError,
    PublicErrorCode, PublicErrorStage, RequestId,
};

/// Builds a safe representation, including for durable error projections.
/// Constructing or serializing this value does not register an incident.
pub(crate) fn build_public_error(code: PublicErrorCode, stage: PublicErrorStage) -> PublicError {
    let correlation_id = uuid::Uuid::new_v4().to_string();
    PublicError {
        version: PUBLIC_ERROR_VERSION,
        code,
        stage,
        message: public_message(code).to_owned(),
        retryable: matches!(
            code,
            PublicErrorCode::Unavailable | PublicErrorCode::Timeout | PublicErrorCode::Conflict
        ),
        retry_after_ms: None,
        correlation_id,
    }
}

/// Records a new, unexpected operation failure using the client's correlation id.
pub(crate) fn report_agent_failure(error: &PublicError, raw_diagnostic: impl std::fmt::Display) {
    tracing::error!(
        correlation_id = %error.correlation_id,
        stage = ?error.stage,
        code = ?error.code,
        raw_diagnostic = %raw_diagnostic,
        "agent-domain operation failed"
    );
}

/// Operation boundaries retain ERROR diagnostics unless the typed cause is
/// explicitly recognized by that operation as an expected refusal.
pub(crate) fn map_agent_failure(
    code: PublicErrorCode,
    stage: PublicErrorStage,
    raw_diagnostic: impl std::fmt::Display,
) -> PublicError {
    let error = build_public_error(code, stage);
    report_agent_failure(&error, raw_diagnostic);
    error
}

/// Callers supply fixed operation/cause labels, never identifiers or raw text.
/// WARN preserves refusal and authorization audit as a breadcrumb, not an event.
pub(crate) fn report_expected_failure(
    error: &PublicError,
    operation: &'static str,
    failure_class: &'static str,
) {
    tracing::warn!(
        correlation_id = %error.correlation_id,
        stage = ?error.stage,
        code = ?error.code,
        operation,
        failure_class,
        "agent-domain operation refused"
    );
}

/// Builds the only JSON-RPC error shape that agent-domain operations may expose.
///
/// `raw_diagnostic` is deliberately consumed only by [`map_agent_failure`], which
/// records it against a correlation id. It is never copied into the transport
/// message or `data` projection.
pub(crate) fn agent_rpc_error(
    request_id: Option<RequestId>,
    jsonrpc_code: i64,
    public_code: PublicErrorCode,
    stage: PublicErrorStage,
    raw_diagnostic: impl std::fmt::Display,
) -> JsonRpcErrorResponse {
    let public_error = map_agent_failure(public_code, stage, raw_diagnostic);
    rpc_error_from_public_error(request_id, jsonrpc_code, public_error)
}

/// A protocol refusal recognized at its operation boundary, with fixed audit labels.
pub(crate) fn expected_agent_rpc_error(
    request_id: Option<RequestId>,
    jsonrpc_code: i64,
    public_code: PublicErrorCode,
    stage: PublicErrorStage,
    operation: &'static str,
    failure_class: &'static str,
) -> JsonRpcErrorResponse {
    let public_error = build_public_error(public_code, stage);
    report_expected_failure(&public_error, operation, failure_class);
    rpc_error_from_public_error(request_id, jsonrpc_code, public_error)
}

pub(crate) fn rpc_error_from_public_error(
    request_id: Option<RequestId>,
    jsonrpc_code: i64,
    public_error: PublicError,
) -> JsonRpcErrorResponse {
    JsonRpcErrorResponse {
        jsonrpc: JSONRPC_VERSION.to_owned(),
        id: request_id,
        error: JsonRpcError {
            code: jsonrpc_code,
            message: public_error.message.clone(),
            data: Some(serde_json::json!({ "public_error": public_error })),
        },
    }
}

fn public_message(code: PublicErrorCode) -> &'static str {
    match code {
        PublicErrorCode::InvalidInput => "The request is invalid.",
        PublicErrorCode::PolicyDenied => "This operation is not permitted.",
        PublicErrorCode::NotFound => "The requested resource is unavailable.",
        PublicErrorCode::Conflict => "The operation conflicts with current state.",
        PublicErrorCode::ResourceExhausted => "The operation exceeds its resource budget.",
        PublicErrorCode::Unavailable => "The agent service is temporarily unavailable.",
        PublicErrorCode::Timeout => "The agent operation timed out.",
        PublicErrorCode::Internal => "The agent operation could not be completed.",
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use tracing_subscriber::prelude::*;

    fn event_subscriber() -> impl tracing::Subscriber + Send + Sync {
        tracing_subscriber::registry().with(sentry::integrations::tracing::layer().event_filter(
            |metadata| {
                use sentry::integrations::tracing::EventFilter;
                match *metadata.level() {
                    tracing::Level::ERROR => EventFilter::Event,
                    tracing::Level::TRACE => EventFilter::Ignore,
                    _ => EventFilter::Breadcrumb,
                }
            },
        ))
    }

    /// Thread-local tracing and isolated Sentry hub backed exclusively by
    /// sentry's in-memory TestTransport. No global subscriber or network client.
    pub(crate) fn capture_events<R>(
        f: impl FnOnce() -> R,
    ) -> (R, Vec<sentry::protocol::Event<'static>>) {
        let subscriber = event_subscriber();
        let mut result = None;
        let events = sentry::test::with_captured_events(|| {
            tracing::subscriber::with_default(subscriber, || result = Some(f()));
        });
        (result.expect("capture closure completed"), events)
    }

    /// Scope the real reporting layer to one asynchronous operation. The
    /// in-memory transport keeps the original diagnostic available to fixture
    /// assertions without changing public errors or the global subscriber.
    pub(crate) async fn capture_events_async<R>(
        future: impl std::future::Future<Output = R>,
    ) -> (R, Vec<sentry::protocol::Event<'static>>) {
        use sentry::SentryFutureExt;
        use tracing::instrument::WithSubscriber;
        let transport = sentry::test::TestTransport::new();
        let mut options = sentry::ClientOptions::default();
        options.dsn = Some("https://public@sentry.invalid/1".parse().unwrap());
        options.transport = Some(std::sync::Arc::new(transport.clone()));
        options.default_integrations = false;
        let hub = std::sync::Arc::new(sentry::Hub::new(
            Some(std::sync::Arc::new(sentry::Client::from(options))),
            Default::default(),
        ));
        let result = future
            .with_subscriber(event_subscriber())
            .bind_hub(hub)
            .await;
        (result, transport.fetch_and_clear_events())
    }

    pub(crate) fn assert_correlated(
        event: &sentry::protocol::Event<'_>,
        error: &pioneer_protocol::PublicError,
    ) {
        assert_eq!(event.level, sentry::Level::Error);
        assert_eq!(
            event.message.as_deref(),
            Some("agent-domain operation failed")
        );
        let sentry::protocol::Context::Other(fields) = &event.contexts["Rust Tracing Fields"]
        else {
            panic!("tracing fields must be present");
        };
        assert_eq!(
            fields["correlation_id"],
            serde_json::json!(error.correlation_id)
        );
    }
}

#[cfg(test)]
mod tests {
    use pioneer_protocol::{PublicErrorCode, PublicErrorStage, RequestId};

    use super::agent_rpc_error;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scoped_async_capture_keeps_original_admission_diagnostic_across_fresh_tasks() {
        const DIAGNOSTIC: &str = "fixture-only original admission cause";
        let (public, events) = super::test_support::capture_events_async(async {
            crate::message::message_fresh_task(async {
                tokio::task::yield_now().await;
                super::map_agent_failure(
                    PublicErrorCode::Internal,
                    PublicErrorStage::Admission,
                    DIAGNOSTIC,
                )
            })
            .await
            .unwrap()
        })
        .await;
        assert_eq!(events.len(), 1);
        super::test_support::assert_correlated(&events[0], &public);
        let sentry::protocol::Context::Other(fields) = &events[0].contexts["Rust Tracing Fields"]
        else {
            panic!("tracing fields must be present");
        };
        assert_eq!(fields["raw_diagnostic"], DIAGNOSTIC);
        assert!(!serde_json::to_string(&public).unwrap().contains(DIAGNOSTIC));
    }

    #[test]
    fn explicitly_expected_rpc_refusal_preserves_the_transport_contract() {
        use super::{expected_agent_rpc_error, test_support::capture_events};
        let request_id = RequestId::new("R".repeat(21)).unwrap();
        let (response, events) = capture_events(|| {
            expected_agent_rpc_error(
                Some(request_id.clone()),
                -32602,
                PublicErrorCode::InvalidInput,
                PublicErrorStage::Admission,
                "task_create",
                "invalid_delivery_policy",
            )
        });
        assert!(events.is_empty());
        assert_eq!(response.jsonrpc, pioneer_protocol::JSONRPC_VERSION);
        assert_eq!(response.id, Some(request_id));
        assert_eq!(response.error.code, -32602);
        let public: pioneer_protocol::PublicError =
            serde_json::from_value(response.error.data.unwrap()["public_error"].clone()).unwrap();
        assert_eq!(public.version, pioneer_protocol::PUBLIC_ERROR_VERSION);
        assert_eq!(public.code, PublicErrorCode::InvalidInput);
        assert_eq!(public.stage, PublicErrorStage::Admission);
        assert_eq!(response.error.message, public.message);
        assert!(!public.retryable);
        assert_eq!(public.retry_after_ms, None);
        assert!(!public.correlation_id.is_empty());
    }

    #[test]
    fn construction_is_silent_and_reporting_uses_the_same_correlation() {
        use super::{build_public_error, report_agent_failure, test_support::*};
        let (error, events) = capture_events(|| {
            build_public_error(PublicErrorCode::Internal, PublicErrorStage::Execution)
        });
        assert!(events.is_empty());
        assert_eq!(error.version, pioneer_protocol::PUBLIC_ERROR_VERSION);
        assert!(!error.retryable);
        assert_eq!(error.retry_after_ms, None);
        assert!(uuid::Uuid::parse_str(&error.correlation_id).is_ok());
        let (_, events) =
            capture_events(|| report_agent_failure(&error, "new infrastructure failure"));
        assert_eq!(events.len(), 1);
        assert_correlated(&events[0], &error);
    }

    #[test]
    fn unclassified_failures_are_not_suppressed_by_public_code_or_diagnostic_text() {
        use super::{map_agent_failure, test_support::*};
        for code in [
            PublicErrorCode::InvalidInput,
            PublicErrorCode::Conflict,
            PublicErrorCode::NotFound,
            PublicErrorCode::Unavailable,
            PublicErrorCode::Internal,
        ] {
            let (error, events) = capture_events(|| {
                map_agent_failure(
                    code,
                    PublicErrorStage::Execution,
                    "cancelled unknown session expected refusal",
                )
            });
            assert_eq!(events.len(), 1);
            assert_correlated(&events[0], &error);
        }
    }

    #[test]
    fn rpc_boundary_never_serializes_raw_diagnostics() {
        let canary = "postgres://secret@host/db /Users/operator/.ssh/id_ed25519 bearer-token";
        let (response, events) = super::test_support::capture_events(|| {
            agent_rpc_error(
                Some(RequestId::new("aaaaaaaaaaaaaaaaaaaaa").expect("valid request id")),
                -32600,
                PublicErrorCode::Internal,
                PublicErrorStage::Execution,
                format_args!("runtime failed: {canary}"),
            )
        });
        let encoded = serde_json::to_string(&response).expect("public error must serialize");

        assert!(!encoded.contains(canary));
        assert!(!encoded.contains("id_ed25519"));
        assert!(!encoded.contains("bearer-token"));
        let public_error: pioneer_protocol::PublicError = response
            .error
            .data
            .as_ref()
            .and_then(|value| value.get("public_error"))
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .expect("typed public error must be present");
        assert_eq!(events.len(), 1);
        super::test_support::assert_correlated(&events[0], &public_error);
        assert_eq!(response.jsonrpc, pioneer_protocol::JSONRPC_VERSION);
        assert_eq!(response.error.code, -32600);
        assert_eq!(public_error.code, PublicErrorCode::Internal);
        assert_eq!(public_error.stage, PublicErrorStage::Execution);
        assert_eq!(response.error.message, public_error.message);
        assert!(!public_error.correlation_id.is_empty());
    }
}
