//! Stateless synchronous function Responses profile. All native data stays here.
mod output;
pub(crate) mod replay;
mod request;
#[cfg(test)]
mod tests;
mod wire;

use super::AuthStyle;
use crate::{
    ChatRequest, ChatResponse, Provider, ProviderCapabilities, ProviderFailureClassification,
    ProviderInputCapabilities, ProviderResponseLimits, ProviderStream, ProviderTimeoutPolicy,
    StreamChunk,
};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::{StreamExt, stream::BoxStream};
use output::Reducer;
use serde_json::Value;

pub struct ResponsesApiCompatibleProvider {
    name: String,
    root: String,
    credential: String,
    auth: AuthStyle,
    authority: String,
    policy: ProviderTimeoutPolicy,
    client: reqwest::Client,
    input_types: ProviderInputCapabilities,
    provisional_messages: bool,
}

/// Public formatting omits the original response retained for local diagnostics.
#[derive(Debug)]
struct SafeError(ProviderFailureClassification);
impl std::fmt::Display for SafeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Responses request failed")
    }
}
impl std::error::Error for SafeError {}
impl SafeError {
    fn native(value: &Value, status: Option<u16>) -> Self {
        use pioneer_protocol::ProviderFailureClass as Class;
        let allowed = |s: &&str| {
            matches!(
                *s,
                "rate_limit_exceeded"
                    | "insufficient_quota"
                    | "invalid_api_key"
                    | "authentication_error"
                    | "permission_denied"
                    | "model_not_found"
                    | "invalid_request_error"
                    | "context_length_exceeded"
                    | "server_error"
                    | "internal_server_error"
                    | "overloaded"
                    | "content_policy_violation"
            )
        };
        let code = value["code"]
            .as_str()
            .filter(allowed)
            .or_else(|| value["type"].as_str().filter(allowed));
        let message = value["message"]
            .as_str()
            .or_else(|| value.as_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let class = match code {
            Some("rate_limit_exceeded") => Class::RateLimit,
            Some("insufficient_quota" | "content_policy_violation") => Class::ProviderRejected,
            Some("permission_denied") => Class::AuthOrPermission,
            Some("model_not_found") => Class::ModelNotFound,
            Some("invalid_api_key" | "authentication_error") => Class::AuthExpired,
            Some("context_length_exceeded") => Class::ContextTooLarge,
            Some("invalid_request_error") => Class::MalformedProviderRequest,
            Some("server_error" | "internal_server_error" | "overloaded") => Class::Provider5xx,
            _ if matches!(status, Some(408 | 425)) => Class::NetworkTransient,
            _ => crate::failure::classify_provider_failure_class(
                &message,
                pioneer_protocol::ProviderFailureStage::Connect,
                status,
                code,
            ),
        };
        let mut facts = ProviderFailureClassification::new(class);
        facts.http_status = status;
        facts.provider_code = code.map(str::to_owned);
        facts.error_reason = Self::reason(value, &message);
        Self(facts)
    }

    /// Public descriptions use controlled reasons; raw responses stay separate.
    /// These diagnostics never participate in classification or retry policy.
    fn reason(value: &Value, message: &str) -> Option<pioneer_protocol::ProviderErrorReason> {
        use pioneer_protocol::ProviderErrorReason as Reason;
        let code = value["code"].as_str().or_else(|| value["type"].as_str());
        let specific = match code {
            Some("missing_required_parameter") => Some(Reason::MissingRequiredParameter),
            Some("unsupported_parameter" | "unknown_parameter") => {
                Some(Reason::UnsupportedParameter)
            }
            Some("invalid_value" | "unsupported_value") => Some(Reason::InvalidParameterValue),
            Some("invalid_type") => Some(Reason::InvalidParameterType),
            _ => None,
        };
        if specific.is_some() {
            return specific;
        }
        if message.contains("item") && message.contains("without its required") {
            return Some(Reason::ResponsesMissingRequiredItem);
        }
        if message.starts_with("no tool output found")
            || message.starts_with("no tool call found for function call output")
        {
            return Some(Reason::ResponsesUnpairedToolCall);
        }
        if message.starts_with("missing required parameter") {
            return Some(Reason::MissingRequiredParameter);
        }
        if message.starts_with("unsupported parameter")
            || message.starts_with("unknown parameter")
            || message.starts_with("unrecognized request argument")
        {
            return Some(Reason::UnsupportedParameter);
        }
        if message.starts_with("invalid value") || message.starts_with("unsupported value") {
            return Some(Reason::InvalidParameterValue);
        }
        if message.starts_with("invalid type") {
            return Some(Reason::InvalidParameterType);
        }
        match code {
            Some("invalid_api_key" | "authentication_error") => Some(Reason::Authentication),
            Some("insufficient_quota") => Some(Reason::PaymentRequired),
            Some("invalid_request_error") => Some(Reason::InvalidRequest),
            Some("model_not_found") => Some(Reason::NotFound),
            Some("server_error" | "internal_server_error" | "overloaded") => Some(Reason::Server),
            Some(code) => Reason::from_openrouter_code(code).or(Some(Reason::Unmapped)),
            None if !message.is_empty() => Some(Reason::Unmapped),
            None => None,
        }
    }
    fn protocol(error: anyhow::Error) -> anyhow::Error {
        if error.downcast_ref::<SafeError>().is_some() {
            return error;
        }
        use pioneer_protocol::ProviderFailureClass as Class;
        let preserve_source = error.is::<crate::ProviderResponseTooLarge>()
            || crate::failure::provider_stream_incomplete(&error).is_some();
        let class = if error.is::<crate::ProviderResponseTooLarge>() {
            Class::ProviderRejected
        } else if crate::failure::provider_stream_incomplete(&error).is_some() {
            Class::StreamTruncated
        } else {
            Class::MalformedProviderRequest
        };
        let mut facts = ProviderFailureClassification::new(class);
        if !preserve_source {
            facts.provider_code = Some("responses_protocol_error".into());
        }
        let mut response = pioneer_protocol::ProviderErrorResponse::from_bytes(&[], false);
        response.processing_error = Some(format!("{error:#}"));
        facts.raw_response = Some(response);
        if preserve_source {
            // Preserve typed bounds/completion causes and their existing retry
            // classification while attaching the same local diagnostic fields.
            error.context(SafeError(facts))
        } else {
            SafeError(facts).into()
        }
    }
    fn with_response(mut error: anyhow::Error, body: &[u8]) -> anyhow::Error {
        if let Some(failure) = error.downcast_mut::<Self>() {
            let captured = pioneer_protocol::ProviderErrorResponse::from_bytes(body, false);
            let response = failure.0.raw_response.get_or_insert_with(|| {
                pioneer_protocol::ProviderErrorResponse::from_bytes(&[], false)
            });
            response.body = captured.body;
            response.truncated |= captured.truncated;
        }
        error
    }
    fn with_stream_context(
        mut error: anyhow::Error,
        bytes: &[u8],
        truncated: bool,
    ) -> anyhow::Error {
        let incomplete = crate::failure::provider_stream_incomplete(&error).is_some()
            || error
                .downcast_ref::<Self>()
                .is_some_and(|failure| failure.0.is_network_error);
        if let Some(failure) = error.downcast_mut::<Self>() {
            let response = failure.0.raw_response.get_or_insert_with(|| {
                pioneer_protocol::ProviderErrorResponse::from_bytes(&[], false)
            });
            response.stream_context = Some(String::from_utf8_lossy(bytes).into_owned());
            response.truncated |= truncated || incomplete;
        }
        error
    }
    fn transport(error: anyhow::Error) -> anyhow::Error {
        if error
            .downcast_ref::<crate::ProviderResponseTooLarge>()
            .is_some()
            || crate::failure::provider_stream_incomplete(&error).is_some()
        {
            return error;
        }
        let mut c = ProviderFailureClassification::new(
            pioneer_protocol::ProviderFailureClass::NetworkTransient,
        );
        c.is_network_error = true;
        if let Some(e) = error.downcast_ref::<reqwest::Error>() {
            c.http_status = e.status().map(|s| s.as_u16());
        }
        SafeError(c).into()
    }
}
pub(crate) fn classify_failure(error: &anyhow::Error) -> Option<ProviderFailureClassification> {
    let classification = error
        .downcast_ref::<SafeError>()
        .map(|e| e.0.clone())
        .or_else(|| crate::failure::classify_stream_error(error));
    classification.map(|mut facts| {
        if facts.request_id.is_none() {
            facts.request_id = crate::usage::error_usage(error)
                .and_then(|usage| usage.request_id.clone())
                .and_then(|id| pioneer_protocol::ProviderRequestId::try_from(id).ok());
        }
        facts
    })
}

impl ResponsesApiCompatibleProvider {
    pub fn new(
        name: impl Into<String>,
        root: impl Into<String>,
        credential: impl Into<String>,
        auth: AuthStyle,
    ) -> Self {
        let name = name.into();
        let root = root.into().trim_end_matches('/').to_owned();
        let credential = credential.into();
        use sha2::Digest;
        let auth_identity = match &auth {
            AuthStyle::Bearer => "bearer".to_owned(),
            AuthStyle::XApiKey => "x-api-key".to_owned(),
            AuthStyle::Custom(header) => format!("custom:{header}"),
        };
        let authority = hex::encode(sha2::Sha256::digest(format!(
            "responses-authority-v1\0{name}\0{root}\0{auth_identity}\0{credential}"
        )));
        let policy = ProviderTimeoutPolicy::default();
        Self {
            name,
            root,
            credential,
            auth,
            authority,
            policy,
            client: crate::http::build_client(policy),
            input_types: ProviderInputCapabilities::disabled_for_all_file_types(),
            provisional_messages: false,
        }
    }
    /// Publish unresolved message text through StartProvisional/Resolve events.
    /// Wrappers opt in when phase metadata can arrive after text; the default
    /// buffers unresolved text until its phase is known.
    pub fn with_provisional_messages(mut self, enabled: bool) -> Self {
        self.provisional_messages = enabled;
        self
    }
    pub fn with_input_capabilities(mut self, mut input_types: ProviderInputCapabilities) -> Self {
        input_types.audio = crate::InputTypeSupport::disabled();
        input_types.video = crate::InputTypeSupport::disabled();
        input_types.file.file_upload = false;
        self.input_types = input_types;
        self
    }
    pub fn with_timeout_policy(mut self, policy: ProviderTimeoutPolicy) -> Self {
        self.policy = policy;
        self.client = crate::http::build_client(policy);
        self
    }
    pub(crate) fn from_context(
        name: String,
        root: String,
        credential: String,
        auth: AuthStyle,
        authority: String,
        policy: ProviderTimeoutPolicy,
        client: reqwest::Client,
        input_types: ProviderInputCapabilities,
    ) -> Self {
        Self {
            name,
            root,
            credential,
            auth,
            authority,
            policy,
            client,
            input_types,
            provisional_messages: false,
        }
    }
    fn endpoint(&self) -> String {
        if self.root.ends_with("/responses") {
            self.root.clone()
        } else {
            format!("{}/responses", self.root)
        }
    }
    fn builder(&self, body: &Value) -> reqwest::RequestBuilder {
        let builder = self.client.post(self.endpoint()).json(body);
        match &self.auth {
            AuthStyle::Bearer => builder.bearer_auth(&self.credential),
            AuthStyle::XApiKey => builder.header("x-api-key", &self.credential),
            AuthStyle::Custom(header) => builder.header(header.as_str(), &self.credential),
        }
    }
    async fn send(&self, body: &Value, stream: bool) -> Result<reqwest::Response> {
        let result = if stream {
            crate::http::send_stream_request(self.builder(body), self.policy).await
        } else {
            crate::http::non_stream_request(self.builder(body), self.policy)
                .send()
                .await
                .map_err(Into::into)
        };
        let response = result.map_err(|e| SafeError::transport(e))?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let id = crate::usage::native_id(
            response
                .headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok()),
        );
        let retry = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .map(|s| s.saturating_mul(1000));
        // Keep the bounded original body even if it is not JSON or the read
        // fails partway through. This payload is not used in public error prose.
        let mut bytes = Vec::new();
        let mut incomplete = false;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else {
                incomplete = true;
                break;
            };
            let remaining = pioneer_protocol::ProviderErrorResponse::MAX_BYTES - bytes.len();
            bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            if chunk.len() > remaining {
                incomplete = true;
                break;
            }
        }
        let body_retry = (!incomplete)
            .then(|| std::str::from_utf8(&bytes).ok())
            .flatten()
            .and_then(crate::failure::extract_retry_after_ms);
        let value = (!incomplete)
            .then(|| {
                wire::parse(&bytes, ProviderResponseLimits::default().max_retained_bytes)
                    .ok()
                    .map(|(value, _)| value)
            })
            .flatten()
            .unwrap_or(Value::Null);
        let mut failure = SafeError::native(value.get("error").unwrap_or(&value), Some(status));
        failure.0.raw_response = Some(pioneer_protocol::ProviderErrorResponse::from_bytes(
            &bytes, incomplete,
        ));
        failure.0.retry_after_ms = retry.or(body_retry);
        let mut usage = crate::TokenUsage::default().with_request_id(id.as_deref());
        usage.api = Some("responses".into());
        usage.route = crate::usage::route(&self.root, "/responses");
        Err(crate::usage::with_error_usage(failure.into(), &usage))
    }
    pub(crate) async fn execute_chat(
        &self,
        prepared: request::PreparedRequest,
    ) -> Result<ChatResponse> {
        let body = self.render(&prepared, false)?;
        let response = self.send(&body, false).await?;
        let id = crate::usage::native_id(
            response
                .headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok()),
        );
        let mut reducer = Reducer::new(
            &self.name,
            &prepared.request.model,
            &self.authority,
            prepared.request.parallel_tool_calls,
            id.as_deref(),
        );
        reducer.bind_request(&prepared.request);
        reducer.bind_cache_comparison(prepared.comparison.clone());
        reducer.usage.api = Some("responses".into());
        reducer.usage.route = crate::usage::route(&self.root, "/responses");
        let bytes = crate::http::read_response_bytes_bounded(
            response,
            ProviderResponseLimits::default().max_transport_bytes,
            "provider_response",
        )
        .await
        .map_err(|e| crate::usage::with_error_usage(SafeError::transport(e), &reducer.usage))?;
        let budget = reducer.parse_budget(bytes.len()).map_err(|error| {
            crate::usage::with_error_usage(
                SafeError::with_response(SafeError::protocol(error), &bytes),
                &reducer.usage,
            )
        })?;
        let (value, retained) = wire::parse(&bytes, budget).map_err(|error| {
            crate::usage::with_error_usage(
                SafeError::with_response(SafeError::protocol(error), &bytes),
                &reducer.usage,
            )
        })?;
        reducer.stage(retained).map_err(|error| {
            crate::usage::with_error_usage(
                SafeError::with_response(SafeError::protocol(error), &bytes),
                &reducer.usage,
            )
        })?;
        reducer.snapshot(&value).map_err(|e| {
            crate::usage::with_error_usage(
                SafeError::with_response(SafeError::protocol(e), &bytes),
                &reducer.usage,
            )
        })?;
        drop(bytes);
        drop(value);
        reducer
            .stage(0)
            .map_err(|error| crate::usage::with_error_usage(error, &reducer.usage))?;
        let usage = reducer.usage.clone();
        reducer
            .response()
            .map_err(|e| crate::usage::with_error_usage(SafeError::protocol(e), &usage))
    }
    pub(crate) async fn execute_stream(
        &self,
        prepared: request::PreparedRequest,
    ) -> Result<ProviderStream> {
        let body = self.render(&prepared, true)?;
        let response = self.send(&body, true).await?;
        let id = crate::usage::native_id(
            response
                .headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok()),
        );
        let diagnostics = crate::ProviderStreamDiagnostics::default();
        if let Some(id) = id
            .clone()
            .and_then(|id| pioneer_protocol::ProviderRequestId::try_from(id).ok())
        {
            diagnostics.set_request_id(id);
        }
        let byte_stream = crate::http::bounded_response_stream(
            response,
            ProviderResponseLimits::default().max_transport_bytes,
            "provider_stream",
        );
        let mut reducer = Reducer::new(
            &self.name,
            &prepared.request.model,
            &self.authority,
            prepared.request.parallel_tool_calls,
            id.as_deref(),
        )
        .with_provisional_messages(self.provisional_messages);
        reducer.bind_request(&prepared.request);
        reducer.bind_cache_comparison(prepared.comparison.clone());
        reducer.usage.api = Some("responses".into());
        reducer.usage.route = crate::usage::route(&self.root, "/responses");
        Ok(ProviderStream {
            stream: Self::decode(byte_stream, reducer),
            diagnostics,
            message_delivery: if self.provisional_messages {
                crate::ProviderMessageDelivery::Provisional
            } else {
                crate::ProviderMessageDelivery::default()
            },
        })
    }
    fn decode(
        mut bytes: BoxStream<'static, Result<bytes::Bytes>>,
        mut reducer: Reducer,
    ) -> BoxStream<'static, Result<StreamChunk>> {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let task = tokio::spawn(async move {
            let mut decoder = crate::tools::stream::IncrementalSseDecoder::default();
            // A request-owned copy of received wire bytes is discarded on
            // success/cancellation. Persist it only on failure, so a comparison
            // error retains both the previous item.done and terminal snapshot.
            // This separate diagnostic buffer shares the transport byte ceiling;
            // it does not change the reducer's existing admission decisions.
            let mut stream_context = Vec::new();
            let mut context_truncated = false;
            loop {
                let next =
                    tokio::select! { biased; _=tx.closed()=>return, next=bytes.next()=>next };
                let result: Result<bool> = async {
                    let Some(next) = next else {
                        std::mem::take(&mut decoder).finish()?;
                        return Err(
                            crate::failure::ProviderStreamIncomplete::EofWithoutTerminalMarker
                                .into(),
                        );
                    };
                    let next = next.map_err(|e| SafeError::transport(e))?;
                    let remaining = pioneer_protocol::ProviderErrorResponse::MAX_BYTES
                        .saturating_sub(stream_context.len());
                    stream_context.extend_from_slice(&next[..next.len().min(remaining)]);
                    context_truncated |= next.len() > remaining;
                    // Framing can copy the incoming bytes and the pending frame.
                    // Admit that peak before asking the shared decoder to allocate.
                    reducer.stage(
                        next.len()
                            .saturating_mul(2)
                            .saturating_add(decoder.retained_bytes().saturating_mul(2)),
                    )?;
                    let frames = decoder.push(&next)?;
                    let buffers = next
                        .len()
                        .saturating_add(decoder.retained_bytes())
                        .saturating_add(frames.iter().map(|event| event.data.len()).sum::<usize>());
                    reducer.stage(buffers)?;
                    for event in frames {
                        if tx.is_closed() {
                            return Ok(true);
                        }
                        tokio::task::consume_budget().await;
                        if event.data.trim() == "[DONE]" {
                            return Err(
                                crate::failure::ProviderStreamIncomplete::EofWithoutTerminalMarker
                                    .into(),
                            );
                        }
                        let (value, retained) =
                            wire::parse(event.data.as_bytes(), reducer.parse_budget(buffers)?)
                                .map_err(|error| {
                                    SafeError::with_response(
                                        SafeError::protocol(error),
                                        event.data.as_bytes(),
                                    )
                                })?;
                        reducer.stage(buffers.saturating_add(retained))?;
                        let chunks =
                            reducer
                                .event(event.event.as_deref(), &value)
                                .map_err(|error| {
                                    SafeError::with_response(
                                        SafeError::protocol(error),
                                        event.data.as_bytes(),
                                    )
                                })?;
                        for chunk in chunks {
                            if tx.send(Ok(chunk)).await.is_err() {
                                return Ok(true);
                            }
                        }
                        if reducer.is_terminal() {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                .await;
                match result {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(error) => {
                        let _ = tx
                            .send(Err(crate::usage::with_error_usage(
                                SafeError::with_stream_context(
                                    SafeError::protocol(error),
                                    &stream_context,
                                    context_truncated,
                                ),
                                &reducer.usage,
                            )))
                            .await;
                        return;
                    }
                }
            }
        });
        // Owning stream cancellation aborts I/O even while waiting for network.
        struct Abort(tokio::task::JoinHandle<()>);
        impl Drop for Abort {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let guard = Abort(task);
        Box::pin(futures_util::stream::unfold(
            (rx, guard),
            |(mut rx, guard)| async move { rx.recv().await.map(|item| (item, (rx, guard))) },
        ))
    }
}
#[async_trait]
impl Provider for ResponsesApiCompatibleProvider {
    fn name(&self) -> &str {
        &self.name
    }
    fn usage_api(&self) -> &'static str {
        "responses"
    }
    fn usage_route(&self) -> Option<String> {
        crate::usage::route(&self.root, "/responses")
    }
    fn authority_fingerprint(&self) -> Option<&str> {
        Some(&self.authority)
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: self.input_types.image.is_supported(),
            tool_calling: true,
            input_types: self.input_types.clone(),
            ..Default::default()
        }
    }
    fn classify_failure(&self, error: &anyhow::Error) -> Option<ProviderFailureClassification> {
        classify_failure(error).or_else(|| crate::failure::classify_stream_error(error))
    }
    async fn prepare_input_budget(
        &self,
        request: ChatRequest,
    ) -> Result<crate::attachments::PreparedInputBudget> {
        let request = crate::tools::policy::prepare_responses_request(&self.name, request)?;
        crate::generation::responses_fields(&self.name, &request)?;
        let request = self.project(request)?;
        crate::attachments::input_estimate::prepare_responses(
            &self.name,
            &self.capabilities(),
            request,
        )
        .await
    }
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let prepared = self.prepare(request, &self.capabilities()).await?;
        self.execute_chat(prepared).await
    }
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        Ok(self.stream_chat_with_diagnostics(request).await?.stream)
    }
    async fn stream_chat_with_diagnostics(&self, request: ChatRequest) -> Result<ProviderStream> {
        let prepared = self.prepare(request, &self.capabilities()).await?;
        self.execute_stream(prepared).await
    }
}
