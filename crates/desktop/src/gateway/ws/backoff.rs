use super::*;

pub(super) fn next_backoff(current: Duration, max: Duration) -> Duration {
    if current >= max {
        return max;
    }

    let doubled_ms = current.as_millis().saturating_mul(2);
    let max_ms = max.as_millis();
    Duration::from_millis(u128::min(doubled_ms, max_ms) as u64)
}

pub(super) fn jitter_delay(base: Duration, jitter_percent: u8) -> Duration {
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

pub(super) fn duration_to_millis_u64(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}
