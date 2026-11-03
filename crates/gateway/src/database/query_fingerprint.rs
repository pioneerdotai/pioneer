use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::sync::{Arc, Mutex};

const MAX_DATABASE_RAW_QUERY_CACHE_ENTRIES: usize = 4_096;
const MAX_DATABASE_QUERY_FINGERPRINTS: usize = 2_048;
const MAX_DATABASE_METRIC_SERIES: usize = 1_024;
const DEFAULT_OTEL_METRIC_CARDINALITY_LIMIT: usize = 2_000;
const DATABASE_METRIC_OVERFLOW_SERIES: usize = pioneer_observability::DatabaseWorkload::CARDINALITY
    * pioneer_observability::DatabaseRole::CARDINALITY
    * pioneer_observability::DatabaseOperation::CARDINALITY
    * 2;
const _: () = assert!(
    MAX_DATABASE_METRIC_SERIES + DATABASE_METRIC_OVERFLOW_SERIES
        < DEFAULT_OTEL_METRIC_CARDINALITY_LIMIT
);

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct QueryMetricSeries {
    fingerprint: u64,
    role: pioneer_observability::DatabaseRole,
    operation: pioneer_observability::DatabaseOperation,
    workload: pioneer_observability::DatabaseWorkload,
    failed: bool,
}

/// Both identities come from the same bounded lookup. Zero remains the
/// existing metric overflow bucket, never a canonical SQL identity.
#[derive(Clone, Debug)]
pub(super) struct QueryFingerprints {
    pub(super) canonical: Option<u64>,
    pub(super) metric: u64,
    pub(super) series: Arc<pioneer_observability::DatabaseOperationSeries>,
}

#[derive(Debug, Default)]
pub(super) struct QueryFingerprintState {
    pub(super) raw_query_cache: HashMap<u64, u64>,
    pub(super) normalized_fingerprints: HashMap<Box<str>, u64>,
    metric_series: HashMap<QueryMetricSeries, Arc<pioneer_observability::DatabaseOperationSeries>>,
    overflow_series:
        HashMap<QueryMetricSeries, Arc<pioneer_observability::DatabaseOperationSeries>>,
}

#[derive(Debug)]
pub(super) struct QueryFingerprintLimiter {
    pub(super) max_raw_query_cache_entries: usize,
    pub(super) max_fingerprints: usize,
    pub(super) max_metric_series: usize,
    pub(super) state: Mutex<QueryFingerprintState>,
}

impl Default for QueryFingerprintLimiter {
    fn default() -> Self {
        Self {
            max_raw_query_cache_entries: MAX_DATABASE_RAW_QUERY_CACHE_ENTRIES,
            max_fingerprints: MAX_DATABASE_QUERY_FINGERPRINTS,
            max_metric_series: MAX_DATABASE_METRIC_SERIES,
            state: Mutex::new(QueryFingerprintState::default()),
        }
    }
}

impl QueryFingerprintLimiter {
    pub(super) fn metric_fingerprint(
        &self,
        sql: &str,
        role: pioneer_observability::DatabaseRole,
        operation: pioneer_observability::DatabaseOperation,
        workload: pioneer_observability::DatabaseWorkload,
        failed: bool,
    ) -> QueryFingerprints {
        let raw_query_key = transient_sql_cache_key(sql);
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(fingerprint) = state.raw_query_cache.get(&raw_query_key).copied() {
                let (metric, series) = self.admit_metric_series(
                    &mut state,
                    fingerprint,
                    role,
                    operation,
                    workload,
                    failed,
                );
                return QueryFingerprints {
                    canonical: (fingerprint != 0).then_some(fingerprint),
                    metric,
                    series,
                };
            }
        }

        // Normalize only cache misses. The cache key itself is transient and
        // opaque, so raw SQL (which can contain a literal in handwritten
        // statements) is never retained by the observability layer.
        let normalized = normalize_sql_shape(sql);

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let fingerprint = state
            .raw_query_cache
            .get(&raw_query_key)
            .copied()
            .unwrap_or_else(|| {
                let fingerprint = if let Some(fingerprint) =
                    state.normalized_fingerprints.get(normalized.as_str())
                {
                    *fingerprint
                } else if state.normalized_fingerprints.len() >= self.max_fingerprints {
                    0
                } else {
                    let fingerprint = stable_normalized_sql_fingerprint(normalized.as_str());
                    state
                        .normalized_fingerprints
                        .insert(normalized.into_boxed_str(), fingerprint);
                    fingerprint
                };
                if state.raw_query_cache.len() < self.max_raw_query_cache_entries {
                    state.raw_query_cache.insert(raw_query_key, fingerprint);
                }
                fingerprint
            });
        let (metric, series) =
            self.admit_metric_series(&mut state, fingerprint, role, operation, workload, failed);
        QueryFingerprints {
            canonical: (fingerprint != 0).then_some(fingerprint),
            metric,
            series,
        }
    }

    fn admit_metric_series(
        &self,
        state: &mut QueryFingerprintState,
        fingerprint: u64,
        role: pioneer_observability::DatabaseRole,
        operation: pioneer_observability::DatabaseOperation,
        workload: pioneer_observability::DatabaseWorkload,
        failed: bool,
    ) -> (u64, Arc<pioneer_observability::DatabaseOperationSeries>) {
        let series = QueryMetricSeries {
            fingerprint,
            role,
            operation,
            workload,
            failed,
        };
        if fingerprint != 0 {
            if let Some(handles) = state.metric_series.get(&series) {
                return (fingerprint, handles.clone());
            }
            if state.metric_series.len() < self.max_metric_series {
                let handles = Arc::new(pioneer_observability::DatabaseOperationSeries::default());
                state.metric_series.insert(series, handles.clone());
                return (fingerprint, handles);
            }
        }
        // One overflow identity per finite role/operation/workload/outcome tuple,
        // separate from canonical identity. Never consume the admission budget.
        let overflow = QueryMetricSeries {
            fingerprint: 0,
            ..series
        };
        let handles = state.overflow_series.entry(overflow).or_default().clone();
        (0, handles)
    }
}

fn stable_normalized_sql_fingerprint(normalized: &str) -> u64 {
    let digest = Sha256::digest(normalized.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let fingerprint = u64::from_be_bytes(bytes) & i64::MAX as u64;
    fingerprint.max(1)
}

fn transient_sql_cache_key(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

fn normalize_sql_shape(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len().min(4_096));
    let mut chars = sql.chars().peekable();
    let mut pending_space = false;

    while let Some(character) = chars.next() {
        if character.is_whitespace() {
            pending_space = !normalized.is_empty();
            continue;
        }
        if character == '-' && chars.peek() == Some(&'-') {
            chars.next();
            for comment_character in chars.by_ref() {
                if comment_character == '\n' || comment_character == '\r' {
                    break;
                }
            }
            pending_space = !normalized.is_empty();
            continue;
        }
        if character == '/' && chars.peek() == Some(&'*') {
            chars.next();
            let mut previous = '\0';
            for comment_character in chars.by_ref() {
                if previous == '*' && comment_character == '/' {
                    break;
                }
                previous = comment_character;
            }
            pending_space = !normalized.is_empty();
            continue;
        }
        if matches!(
            character,
            '=' | '<' | '>' | '+' | '-' | '*' | '/' | '%' | '|' | '&'
        ) {
            normalized.push(character);
            pending_space = false;
            continue;
        }
        if pending_space {
            if !matches!(character, ',' | ')' | ';')
                && !normalized.ends_with('(')
                && !normalized.ends_with(',')
                && !normalized.ends_with(' ')
                && !normalized.ends_with('=')
                && !normalized.ends_with('<')
                && !normalized.ends_with('>')
                && !normalized.ends_with('+')
                && !normalized.ends_with('-')
                && !normalized.ends_with('*')
                && !normalized.ends_with('/')
                && !normalized.ends_with('%')
                && !normalized.ends_with('|')
                && !normalized.ends_with('&')
            {
                normalized.push(' ');
            }
            pending_space = false;
        }
        if character == '\'' {
            normalized.push('?');
            while let Some(literal_character) = chars.next() {
                if literal_character != '\'' {
                    continue;
                }
                if chars.peek() == Some(&'\'') {
                    chars.next();
                } else {
                    break;
                }
            }
            continue;
        }
        if character == '?' {
            normalized.push('?');
            while chars.peek().is_some_and(|next| next.is_ascii_digit()) {
                chars.next();
            }
            continue;
        }
        let begins_numeric_literal = character.is_ascii_digit()
            && normalized
                .chars()
                .next_back()
                .is_none_or(|previous| !previous.is_ascii_alphanumeric() && previous != '_');
        if begins_numeric_literal {
            normalized.push('?');
            while chars.peek().is_some_and(|next| {
                next.is_ascii_alphanumeric() || matches!(next, '.' | '_' | '+' | '-')
            }) {
                chars.next();
            }
            continue;
        }
        normalized.extend(character.to_lowercase());
    }

    let trimmed_len = normalized.trim_end().len();
    normalized.truncate(trimmed_len);
    normalized
}
