//! Capability-authenticated task ingress. Neither request paths nor payloads
//! are traced: the path contains a credential and the body is private input.

use std::time::Duration;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use futures_util::StreamExt as _;
use pioneer_protocol::{TaskRunInputEncoding, TaskRunTriggerInput};
use subtle::ConstantTimeEq as _;

use super::GatewayHttpState;
use super::auth::new_request_id;
use super::errors::{HttpError, HttpErrorKind};

pub(super) const MAX_WEBHOOK_BODY_BYTES: usize = 1024 * 1024;
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) async fn task_webhook_route(
    State(state): State<GatewayHttpState>,
    Path((trigger_id, secret)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let request_id = new_request_id();
    // Opaque, bounded capabilities. Missing endpoints and invalid credentials
    // deliberately have the same outward response.
    if trigger_id.len() != 21
        || !trigger_id.bytes().all(|byte| byte.is_ascii_alphanumeric())
        || secret.len() != 64
        || !secret.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return HttpError::not_found(request_id).into_response();
    }
    if !state.is_ready() {
        return HttpError::service_unavailable(request_id).into_response();
    }
    let service = state.message_processor.task_runtime.service();
    let trigger = match service.webhook_trigger(&trigger_id).await {
        Ok(Some(trigger)) => trigger,
        Ok(None) => return HttpError::not_found(request_id).into_response(),
        Err(_) => return HttpError::service_unavailable(request_id).into_response(),
    };
    let Some(secret_ref) = trigger.webhook_secret_ref.as_deref() else {
        return HttpError::not_found(request_id).into_response();
    };
    let expected = match state
        .message_processor
        .gateway_secrets()
        .get_task_webhook_secret(secret_ref)
    {
        Ok(Some(secret)) => zeroize::Zeroizing::new(secret),
        Ok(None) => return HttpError::not_found(request_id).into_response(),
        Err(_) => return HttpError::service_unavailable(request_id).into_response(),
    };
    if !bool::from(expected.as_bytes().ct_eq(secret.as_bytes())) {
        return HttpError::not_found(request_id).into_response();
    }
    let content_type = match content_type(&headers) {
        Ok(value) => value,
        Err(()) => return HttpError::bad_request(request_id).into_response(),
    };
    // Bound both allocation and total read time, including chunked requests.
    // No detached work: the response follows the durable admission commit.
    let bytes = match tokio::time::timeout(BODY_READ_TIMEOUT, read_body(body)).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(kind)) => return HttpError::new(kind, request_id).into_response(),
        Err(_) => return HttpError::new(HttpErrorKind::RequestTimeout, request_id).into_response(),
    };
    let input = trigger_input(content_type, &bytes);
    match service.accept_webhook(&trigger_id, secret_ref, input).await {
        Ok(run) => {
            let mut response = (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({ "runId": run.id })),
            )
                .into_response();
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                axum::http::HeaderValue::from_static("no-store"),
            );
            response
        }
        Err(error) => {
            let kind = match error.downcast_ref::<pioneer_tasks::TaskOperationError>() {
                Some(pioneer_tasks::TaskOperationError::StateConflict) => HttpErrorKind::Conflict,
                Some(pioneer_tasks::TaskOperationError::AccessDenied) => HttpErrorKind::NotFound,
                None => HttpErrorKind::ServiceUnavailable {
                    retry_after_seconds: None,
                },
            };
            HttpError::new(kind, request_id).into_response()
        }
    }
}

async fn read_body(body: Body) -> Result<Vec<u8>, HttpErrorKind> {
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| HttpErrorKind::BadRequest)?;
        if chunk.len() > MAX_WEBHOOK_BODY_BYTES.saturating_sub(bytes.len()) {
            return Err(HttpErrorKind::PayloadTooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn content_type(headers: &HeaderMap) -> Result<Option<String>, ()> {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let value = values
        .next()
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| ())?;
    if values.next().is_some() {
        return Err(());
    }
    Ok(value)
}

fn trigger_input(content_type: Option<String>, body: &[u8]) -> TaskRunTriggerInput {
    match std::str::from_utf8(body) {
        Ok(text) => TaskRunTriggerInput {
            content_type,
            encoding: TaskRunInputEncoding::Utf8,
            body: text.to_owned(),
        },
        Err(_) => TaskRunTriggerInput {
            content_type,
            encoding: TaskRunInputEncoding::Base64,
            body: base64::engine::general_purpose::STANDARD.encode(body),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bodies_accept_plain_text_json_empty_and_binary_without_envelope() {
        for (content_type, payload) in [
            (Some("text/plain; charset=utf-8"), "привет\n".as_bytes()),
            (
                Some("application/json"),
                br#"{"anything": [1, 2]}"#.as_slice(),
            ),
            (None, b"".as_slice()),
            (
                Some("application/octet-stream"),
                b"\x00\xff\xfe\r\n".as_slice(),
            ),
        ] {
            let bytes = read_body(Body::from(payload.to_vec())).await.unwrap();
            let input = trigger_input(content_type.map(str::to_owned), &bytes);
            let decoded = match input.encoding {
                TaskRunInputEncoding::Utf8 => input.body.into_bytes(),
                TaskRunInputEncoding::Base64 => base64::engine::general_purpose::STANDARD
                    .decode(input.body)
                    .unwrap(),
            };
            assert_eq!(decoded, payload);
            assert_eq!(input.content_type.as_deref(), content_type);
        }
    }

    #[tokio::test]
    async fn chunked_body_limit_bounds_ingress() {
        let chunks = futures_util::stream::iter([
            Ok::<_, std::io::Error>(vec![b'a'; MAX_WEBHOOK_BODY_BYTES]),
            Ok(vec![b'b']),
        ]);
        assert_eq!(
            read_body(Body::from_stream(chunks)).await.unwrap_err(),
            HttpErrorKind::PayloadTooLarge
        );
        assert_eq!(
            read_body(Body::from(vec![b'a'; MAX_WEBHOOK_BODY_BYTES]))
                .await
                .unwrap()
                .len(),
            MAX_WEBHOOK_BODY_BYTES
        );
    }
}
