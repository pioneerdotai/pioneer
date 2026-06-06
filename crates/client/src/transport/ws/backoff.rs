//! WebSocket reconnect backoff helpers.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn next_backoff(current: Duration, max: Duration) -> Duration {
    if current >= max {
        return max;
    }

    let doubled_ms = current.as_millis().saturating_mul(2);
    let max_ms = max.as_millis();
    Duration::from_millis(u128::min(doubled_ms, max_ms) as u64)
}

pub fn jitter_delay(base: Duration, jitter_percent: u8) -> Duration {
    if jitter_percent == 0 {
        return base;
    }

    let base_ms = duration_to_millis_u64(base);
    if base_ms <= 1 {
        return base;
    }

    let jitter_span = base_ms
        .saturating_mul(jitter_percent as u64)
        .checked_div(100)
        .unwrap_or(0);
    if jitter_span == 0 {
        return base;
    }

    let width = jitter_span.saturating_mul(2).saturating_add(1);
    if width == 0 {
        return base;
    }

    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let offset = seed % width;
    let adjusted = if offset >= jitter_span {
        base_ms.saturating_add(offset - jitter_span)
    } else {
        base_ms.saturating_sub(jitter_span - offset)
    };

    Duration::from_millis(adjusted.max(1))
}

pub fn duration_to_millis_u64(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_ws_next_backoff_doubles_until_max() {
        let max = Duration::from_millis(1_000);
        let mut value = Duration::from_millis(100);

        value = next_backoff(value, max);
        assert_eq!(value, Duration::from_millis(200));
        value = next_backoff(value, max);
        assert_eq!(value, Duration::from_millis(400));
        value = next_backoff(value, max);
        assert_eq!(value, Duration::from_millis(800));
        value = next_backoff(value, max);
        assert_eq!(value, max);
    }

    #[test]
    fn transport_ws_duration_to_millis_u64_handles_regular_values() {
        assert_eq!(duration_to_millis_u64(Duration::from_millis(123)), 123);
    }
}
