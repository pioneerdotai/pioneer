//! Gateway timing and retry configuration.

use std::{error::Error, fmt, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewayTimings {
    pub connect_timeout: Duration,
    pub startup_timeout: Duration,
    pub poll_interval: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewayWsTimings {
    pub connect_timeout: Duration,
    pub ping_interval: Duration,
    pub pong_timeout: Duration,
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
    pub reconnect_jitter_percent: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayTimingError {
    FieldMustBePositive(&'static str),
    ReconnectInitialGreaterThanMax,
    ReconnectJitterPercentTooHigh { value: u8 },
}

impl GatewayTimings {
    pub fn from_millis(
        connect_timeout_ms: u64,
        startup_timeout_ms: u64,
        poll_interval_ms: u64,
    ) -> Result<Self, GatewayTimingError> {
        ensure_positive("connect_timeout_ms", connect_timeout_ms)?;
        ensure_positive("startup_timeout_ms", startup_timeout_ms)?;
        ensure_positive("poll_interval_ms", poll_interval_ms)?;

        Ok(Self {
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            startup_timeout: Duration::from_millis(startup_timeout_ms),
            poll_interval: Duration::from_millis(poll_interval_ms),
        })
    }
}

impl GatewayWsTimings {
    pub fn from_millis(
        connect_timeout_ms: u64,
        ping_interval_ms: u64,
        pong_timeout_ms: u64,
        reconnect_initial_ms: u64,
        reconnect_max_ms: u64,
        reconnect_jitter_percent: u8,
    ) -> Result<Self, GatewayTimingError> {
        ensure_positive("connect_timeout_ms", connect_timeout_ms)?;
        ensure_positive("ws_ping_interval_ms", ping_interval_ms)?;
        ensure_positive("ws_pong_timeout_ms", pong_timeout_ms)?;
        ensure_positive("ws_reconnect_initial_ms", reconnect_initial_ms)?;
        ensure_positive("ws_reconnect_max_ms", reconnect_max_ms)?;

        if reconnect_initial_ms > reconnect_max_ms {
            return Err(GatewayTimingError::ReconnectInitialGreaterThanMax);
        }
        if reconnect_jitter_percent > 100 {
            return Err(GatewayTimingError::ReconnectJitterPercentTooHigh {
                value: reconnect_jitter_percent,
            });
        }

        Ok(Self {
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            ping_interval: Duration::from_millis(ping_interval_ms),
            pong_timeout: Duration::from_millis(pong_timeout_ms),
            reconnect_initial: Duration::from_millis(reconnect_initial_ms),
            reconnect_max: Duration::from_millis(reconnect_max_ms),
            reconnect_jitter_percent,
        })
    }
}

impl fmt::Display for GatewayTimingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FieldMustBePositive(field) => write!(f, "{field} must be positive"),
            Self::ReconnectInitialGreaterThanMax => {
                write!(f, "ws_reconnect_initial_ms must be <= ws_reconnect_max_ms")
            }
            Self::ReconnectJitterPercentTooHigh { value } => {
                write!(f, "ws_reconnect_jitter_percent must be <= 100, got {value}")
            }
        }
    }
}

impl Error for GatewayTimingError {}

fn ensure_positive(field: &'static str, value: u64) -> Result<(), GatewayTimingError> {
    if value == 0 {
        return Err(GatewayTimingError::FieldMustBePositive(field));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_timings_convert_millis_to_durations() {
        let timings = GatewayTimings::from_millis(100, 200, 300).expect("valid timings");

        assert_eq!(timings.connect_timeout, Duration::from_millis(100));
        assert_eq!(timings.startup_timeout, Duration::from_millis(200));
        assert_eq!(timings.poll_interval, Duration::from_millis(300));
    }

    #[test]
    fn gateway_ws_timings_validate_reconnect_bounds() {
        let error = GatewayWsTimings::from_millis(100, 200, 300, 500, 400, 0)
            .expect_err("initial reconnect must not exceed max");

        assert_eq!(error, GatewayTimingError::ReconnectInitialGreaterThanMax);
    }

    #[test]
    fn gateway_ws_timings_validate_jitter_percent() {
        let error = GatewayWsTimings::from_millis(100, 200, 300, 400, 500, 101)
            .expect_err("jitter percent must be bounded");

        assert_eq!(
            error,
            GatewayTimingError::ReconnectJitterPercentTooHigh { value: 101 }
        );
    }
}
