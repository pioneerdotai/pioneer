use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::header::{CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE};
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;

use crate::auth::{AuthenticatedSessionPrincipal, CapturedAdmission, RestrictedAdmission};
use crate::transport::http::GatewayHttpState;

const MAX_AUTH_IN_FLIGHT_PER_ADDRESS: usize = 8;
const MAX_AUTH_IN_FLIGHT_GLOBAL: usize = 512;
const MAX_AUTH_TRACKED_ADDRESSES: usize = 1_024;
const MAX_AUTH_FAILURE_PENALTY_STEPS: u32 = 10;
const AUTH_FAILURE_PENALTY_STEP: Duration = Duration::from_millis(25);
const AUTH_FAILURE_STATE_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Debug)]
pub(crate) enum AdmittedConnection {
    Normal(Arc<AuthenticatedSessionPrincipal>),
    Restricted {
        admission: RestrictedAdmission,
        permit: AuthAttemptPermit,
    },
}

#[derive(Default)]
struct AuthAbuseState {
    addresses: HashMap<IpAddr, AuthAddressState>,
    total_in_flight: usize,
    gateway_failed_attempts: u32,
    gateway_last_failure: Option<Instant>,
}

struct AuthAddressState {
    in_flight: usize,
    failed_attempts: u32,
    last_touched: Instant,
}

#[derive(Default)]
pub(crate) struct AuthAbuseLimiter {
    state: Mutex<AuthAbuseState>,
}

#[derive(Debug)]
pub(crate) struct AuthAttemptPermit {
    limiter: Arc<AuthAbuseLimiter>,
    address: IpAddr,
    penalty: Duration,
}

impl std::fmt::Debug for AuthAbuseLimiter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthAbuseLimiter")
            .finish_non_exhaustive()
    }
}

impl AuthAbuseLimiter {
    fn try_acquire(self: &Arc<Self>, address: IpAddr) -> Option<AuthAttemptPermit> {
        let now = Instant::now();
        let mut state = self.state.lock().ok()?;
        state.addresses.retain(|_, entry| {
            entry.in_flight > 0
                || now.saturating_duration_since(entry.last_touched) < AUTH_FAILURE_STATE_TTL
        });
        if state.gateway_last_failure.is_some_and(|last_failure| {
            now.saturating_duration_since(last_failure) >= AUTH_FAILURE_STATE_TTL
        }) {
            state.gateway_failed_attempts = 0;
            state.gateway_last_failure = None;
        }
        if state.total_in_flight >= MAX_AUTH_IN_FLIGHT_GLOBAL {
            return None;
        }
        if !state.addresses.contains_key(&address)
            && state.addresses.len() >= MAX_AUTH_TRACKED_ADDRESSES
        {
            let oldest_idle = state
                .addresses
                .iter()
                .filter(|(_, entry)| entry.in_flight == 0)
                .min_by_key(|(_, entry)| entry.last_touched)
                .map(|(address, _)| *address);
            if let Some(oldest_idle) = oldest_idle {
                state.addresses.remove(&oldest_idle);
            } else {
                return None;
            }
        }
        let address_penalty_steps = {
            let entry = state.addresses.entry(address).or_insert(AuthAddressState {
                in_flight: 0,
                failed_attempts: 0,
                last_touched: now,
            });
            if entry.in_flight >= MAX_AUTH_IN_FLIGHT_PER_ADDRESS {
                return None;
            }
            let penalty_steps = entry.failed_attempts.min(MAX_AUTH_FAILURE_PENALTY_STEPS);
            entry.in_flight += 1;
            entry.last_touched = now;
            penalty_steps
        };
        let penalty_steps = address_penalty_steps
            .max(state.gateway_failed_attempts)
            .min(MAX_AUTH_FAILURE_PENALTY_STEPS);
        state.total_in_flight += 1;
        Some(AuthAttemptPermit {
            limiter: self.clone(),
            address,
            penalty: AUTH_FAILURE_PENALTY_STEP.saturating_mul(penalty_steps),
        })
    }

    #[cfg(test)]
    fn test_observation(&self, address: IpAddr) -> (usize, usize, u32) {
        let state = self.state.lock().expect("auth limiter test observation");
        let address_in_flight = state
            .addresses
            .get(&address)
            .map_or(0, |entry| entry.in_flight);
        (
            state.total_in_flight,
            address_in_flight,
            state.gateway_failed_attempts,
        )
    }
}

impl AuthAttemptPermit {
    pub(crate) fn record_success(&self) {
        if let Ok(mut state) = self.limiter.state.lock()
            && let Some(entry) = state.addresses.get_mut(&self.address)
        {
            entry.failed_attempts = 0;
            entry.last_touched = Instant::now();
        }
    }

    pub(crate) fn record_failure(&self) {
        if let Ok(mut state) = self.limiter.state.lock() {
            let now = Instant::now();
            if let Some(entry) = state.addresses.get_mut(&self.address) {
                entry.failed_attempts = entry
                    .failed_attempts
                    .saturating_add(1)
                    .min(MAX_AUTH_FAILURE_PENALTY_STEPS);
                entry.last_touched = now;
            }
            state.gateway_failed_attempts = state
                .gateway_failed_attempts
                .saturating_add(1)
                .min(MAX_AUTH_FAILURE_PENALTY_STEPS);
            state.gateway_last_failure = Some(now);
        }
    }
}

impl Drop for AuthAttemptPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.limiter.state.lock() {
            if let Some(entry) = state.addresses.get_mut(&self.address) {
                entry.in_flight = entry.in_flight.saturating_sub(1);
                entry.last_touched = Instant::now();
            }
            state.total_in_flight = state.total_in_flight.saturating_sub(1);
        }
    }
}

pub(crate) async fn admit(
    state: &GatewayHttpState,
    client_ip: IpAddr,
    headers: &HeaderMap,
) -> Result<AdmittedConnection, Response> {
    crate::transport::protocol::validate_protocol_version(headers)
        .map_err(|_| bounded_error(StatusCode::BAD_REQUEST, "unsupported protocol version"))?;
    validate_standard_websocket_headers(headers)?;

    let permit = state
        .auth_abuse_limiter
        .try_acquire(client_ip)
        .ok_or_else(|| bounded_error(StatusCode::TOO_MANY_REQUESTS, "connection rejected"))?;
    if !permit.penalty.is_zero() {
        tokio::time::sleep(permit.penalty).await;
    }

    let admission = state.auth.capture_headers(headers).map_err(|error| {
        permit.record_failure();
        tracing::debug!(
            event = "websocket_auth",
            outcome = "rejected",
            reason_code = error.code().as_str(),
        );
        bounded_error(StatusCode::UNAUTHORIZED, "missing or invalid bearer token")
    })?;

    match admission {
        CapturedAdmission::Access(credential) => {
            let deadline =
                Duration::from_secs(state.config.gateway.auth.auth_exchange_timeout_seconds);
            match tokio::time::timeout(deadline, state.auth_service.authenticate_access(credential))
                .await
            {
                Ok(Ok(principal)) => {
                    permit.record_success();
                    Ok(AdmittedConnection::Normal(principal))
                }
                Ok(Err(error)) => {
                    permit.record_failure();
                    tracing::debug!(
                        event = "websocket_auth",
                        outcome = "rejected",
                        reason_code = error.code().as_str(),
                    );
                    Err(bounded_error(
                        StatusCode::UNAUTHORIZED,
                        "missing or invalid bearer token",
                    ))
                }
                Err(_) => {
                    permit.record_failure();
                    Err(bounded_error(
                        StatusCode::REQUEST_TIMEOUT,
                        "request timed out",
                    ))
                }
            }
        }
        CapturedAdmission::Restricted(admission) => {
            Ok(AdmittedConnection::Restricted { admission, permit })
        }
    }
}

fn validate_standard_websocket_headers(headers: &HeaderMap) -> Result<(), Response> {
    require_single_header(headers, CONNECTION, |value| {
        value
            .split(|byte| *byte == b',')
            .any(|part| part.trim_ascii().eq_ignore_ascii_case(b"upgrade"))
    })?;
    require_single_header(headers, UPGRADE, |value| {
        value.eq_ignore_ascii_case(b"websocket")
    })?;
    require_single_header(headers, SEC_WEBSOCKET_VERSION, |value| value == b"13")?;
    require_single_header(headers, SEC_WEBSOCKET_KEY, |value| {
        base64::engine::general_purpose::STANDARD
            .decode(value)
            .is_ok_and(|decoded| decoded.len() == 16)
    })?;
    Ok(())
}

fn require_single_header(
    headers: &HeaderMap,
    name: HeaderName,
    validate: impl FnOnce(&[u8]) -> bool,
) -> Result<(), Response> {
    let values = headers.get_all(&name).iter().collect::<Vec<_>>();
    if values.len() != 1 || !validate(values[0].as_bytes()) {
        return Err(bounded_error(
            StatusCode::BAD_REQUEST,
            "invalid websocket request",
        ));
    }
    Ok(())
}

fn bounded_error(status: StatusCode, message: &'static str) -> Response {
    (
        status,
        [("content-type", "text/plain; charset=utf-8")],
        message,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn valid_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::transport::protocol::PIONEER_PROTOCOL_VERSION_HEADER,
            HeaderValue::from_static(crate::transport::protocol::PIONEER_PROTOCOL_VERSION),
        );
        headers.insert(CONNECTION, HeaderValue::from_static("Upgrade"));
        headers.insert(UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(SEC_WEBSOCKET_VERSION, HeaderValue::from_static("13"));
        headers.insert(
            SEC_WEBSOCKET_KEY,
            HeaderValue::from_static("dGhlIHNhbXBsZSBub25jZQ=="),
        );
        headers
    }

    #[test]
    fn standard_handshake_headers_are_strict_and_single() {
        let mut headers = valid_headers();
        assert!(validate_standard_websocket_headers(&headers).is_ok());
        headers.append(CONNECTION, HeaderValue::from_static("Upgrade"));
        assert!(validate_standard_websocket_headers(&headers).is_err());

        let mut headers = valid_headers();
        headers.insert(SEC_WEBSOCKET_KEY, HeaderValue::from_static("not-base64"));
        assert!(validate_standard_websocket_headers(&headers).is_err());
    }

    #[test]
    fn abuse_limiter_is_per_address_and_globally_bounded() {
        let limiter = Arc::new(AuthAbuseLimiter::default());
        let address = "127.0.0.1".parse().unwrap();
        let permits = (0..MAX_AUTH_IN_FLIGHT_PER_ADDRESS)
            .map(|_| limiter.try_acquire(address).unwrap())
            .collect::<Vec<_>>();
        assert!(limiter.try_acquire(address).is_none());
        drop(permits);
        assert!(limiter.try_acquire(address).is_some());
    }

    #[test]
    fn auth_permits_release_on_success_failure_drop_and_global_exhaustion() {
        let limiter = Arc::new(AuthAbuseLimiter::default());
        let first_address: IpAddr = "127.0.0.1".parse().unwrap();
        let first = limiter.try_acquire(first_address).unwrap();
        assert_eq!(limiter.test_observation(first_address), (1, 1, 0));
        first.record_failure();
        assert_eq!(limiter.test_observation(first_address), (1, 1, 1));
        drop(first);
        assert_eq!(limiter.test_observation(first_address), (0, 0, 1));

        let recovered = limiter.try_acquire(first_address).unwrap();
        assert_eq!(recovered.penalty, AUTH_FAILURE_PENALTY_STEP);
        recovered.record_success();
        drop(recovered);
        assert_eq!(limiter.test_observation(first_address), (0, 0, 1));

        let permits = (0..MAX_AUTH_IN_FLIGHT_GLOBAL)
            .map(|index| {
                let octet_2 = ((index / (254 * 254)) % 254 + 1) as u8;
                let octet_3 = ((index / 254) % 254 + 1) as u8;
                let octet_4 = (index % 254 + 1) as u8;
                let address = IpAddr::from([10, octet_2, octet_3, octet_4]);
                limiter.try_acquire(address).expect("global permit")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            limiter.test_observation(first_address).0,
            MAX_AUTH_IN_FLIGHT_GLOBAL
        );
        assert!(limiter.try_acquire("192.0.2.42".parse().unwrap()).is_none());
        drop(permits);
        assert_eq!(limiter.test_observation(first_address).0, 0);
    }
}
