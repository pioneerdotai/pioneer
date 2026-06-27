use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use pioneer_protocol::{
    INVALID_PARAMS_CODE, JsonRpcErrorResponse, RequestId, TimelineCursor, TimelinePageAnchor,
};
use serde::{Deserialize, Serialize};
use std::fmt;

pub(super) const THREAD_TIMELINE_DEFAULT_LIMIT: u32 = 40;
pub(super) const THREAD_TIMELINE_MAX_LIMIT: u32 = 100;
pub(super) const TURN_WORK_DEFAULT_LIMIT: u32 = 100;
pub(super) const TURN_WORK_MAX_LIMIT: u32 = 200;

const MAX_CURSOR_ENCODED_BYTES: usize = 2048;
const MAX_CURSOR_DECODED_BYTES: usize = 1536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TimelineLimitPolicy {
    pub default_limit: u32,
    pub max_limit: u32,
}

impl TimelineLimitPolicy {
    pub const fn thread_timeline() -> Self {
        Self {
            default_limit: THREAD_TIMELINE_DEFAULT_LIMIT,
            max_limit: THREAD_TIMELINE_MAX_LIMIT,
        }
    }

    pub const fn turn_work() -> Self {
        Self {
            default_limit: TURN_WORK_DEFAULT_LIMIT,
            max_limit: TURN_WORK_MAX_LIMIT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ThreadTimelineCursor {
    pub projection_version: i64,
    pub thread_id: String,
    pub block_id: String,
    pub sort_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TurnWorkCursor {
    pub projection_version: i64,
    pub thread_id: String,
    pub turn_id: String,
    pub work_item_id: String,
    pub order_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ResolvedTimelineAnchor {
    Newest,
    Oldest,
    Before(String),
    After(String),
    Around(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TimelineCursorError {
    InvalidCursor,
    CursorScopeMismatch { expected: String, actual: String },
    WrongCursorKind { expected: &'static str },
    StaleCursor { expected: i64, actual: i64 },
    InvalidLimitZero,
    LimitTooLarge { max: u32, actual: u32 },
}

impl TimelineCursorError {
    pub(super) fn into_error_response(
        self,
        request_id: RequestId,
        method: &'static str,
    ) -> JsonRpcErrorResponse {
        JsonRpcErrorResponse::new(
            Some(request_id),
            INVALID_PARAMS_CODE,
            format!("invalid params for `{method}`: {self}"),
        )
    }
}

impl fmt::Display for TimelineCursorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCursor => f.write_str("cursor is invalid or stale"),
            Self::CursorScopeMismatch { expected, actual } => {
                write!(
                    f,
                    "cursor belongs to `{actual}` but request targets `{expected}`"
                )
            }
            Self::WrongCursorKind { expected } => {
                write!(f, "cursor kind does not match `{expected}`")
            }
            Self::StaleCursor { expected, actual } => {
                write!(
                    f,
                    "cursor projection version `{actual}` is stale; current version is `{expected}`"
                )
            }
            Self::InvalidLimitZero => f.write_str("`limit` must be greater than zero"),
            Self::LimitTooLarge { max, actual } => {
                write!(f, "`limit` must be <= {max}, got {actual}")
            }
        }
    }
}

impl std::error::Error for TimelineCursorError {}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TimelineCursorPayload {
    ThreadTimelineBlock {
        projection_version: i64,
        thread_id: String,
        block_id: String,
        sort_key: String,
    },
    TurnWorkItem {
        projection_version: i64,
        thread_id: String,
        turn_id: String,
        work_item_id: String,
        order_key: String,
    },
}

pub(super) fn validate_timeline_limit(
    requested: Option<u32>,
    policy: TimelineLimitPolicy,
) -> Result<u64, TimelineCursorError> {
    let limit = requested.unwrap_or(policy.default_limit);
    if limit == 0 {
        return Err(TimelineCursorError::InvalidLimitZero);
    }
    if limit > policy.max_limit {
        return Err(TimelineCursorError::LimitTooLarge {
            max: policy.max_limit,
            actual: limit,
        });
    }
    Ok(u64::from(limit))
}

pub(super) fn encode_thread_timeline_cursor(
    projection_version: i64,
    thread_id: &str,
    block_id: &str,
    sort_key: &str,
) -> Result<TimelineCursor, TimelineCursorError> {
    encode_payload(&TimelineCursorPayload::ThreadTimelineBlock {
        projection_version,
        thread_id: thread_id.to_owned(),
        block_id: block_id.to_owned(),
        sort_key: sort_key.to_owned(),
    })
}

pub(super) fn encode_turn_work_cursor(
    projection_version: i64,
    thread_id: &str,
    turn_id: &str,
    work_item_id: &str,
    order_key: &str,
) -> Result<TimelineCursor, TimelineCursorError> {
    encode_payload(&TimelineCursorPayload::TurnWorkItem {
        projection_version,
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        work_item_id: work_item_id.to_owned(),
        order_key: order_key.to_owned(),
    })
}

pub(super) fn decode_thread_timeline_cursor(
    cursor: &TimelineCursor,
    expected_thread_id: &str,
    expected_projection_version: i64,
) -> Result<ThreadTimelineCursor, TimelineCursorError> {
    match decode_payload(cursor)? {
        TimelineCursorPayload::ThreadTimelineBlock {
            projection_version,
            thread_id,
            block_id,
            sort_key,
        } => {
            if projection_version != expected_projection_version {
                return Err(TimelineCursorError::StaleCursor {
                    expected: expected_projection_version,
                    actual: projection_version,
                });
            }
            if thread_id != expected_thread_id {
                return Err(TimelineCursorError::CursorScopeMismatch {
                    expected: expected_thread_id.to_owned(),
                    actual: thread_id,
                });
            }
            Ok(ThreadTimelineCursor {
                projection_version,
                thread_id,
                block_id,
                sort_key,
            })
        }
        TimelineCursorPayload::TurnWorkItem { .. } => Err(TimelineCursorError::WrongCursorKind {
            expected: "thread_timeline_block",
        }),
    }
}

pub(super) fn decode_turn_work_cursor(
    cursor: &TimelineCursor,
    expected_thread_id: &str,
    expected_turn_id: &str,
    expected_projection_version: i64,
) -> Result<TurnWorkCursor, TimelineCursorError> {
    match decode_payload(cursor)? {
        TimelineCursorPayload::TurnWorkItem {
            projection_version,
            thread_id,
            turn_id,
            work_item_id,
            order_key,
        } => {
            if projection_version != expected_projection_version {
                return Err(TimelineCursorError::StaleCursor {
                    expected: expected_projection_version,
                    actual: projection_version,
                });
            }
            let expected = format!("{expected_thread_id}/{expected_turn_id}");
            let actual = format!("{thread_id}/{turn_id}");
            if thread_id != expected_thread_id || turn_id != expected_turn_id {
                return Err(TimelineCursorError::CursorScopeMismatch { expected, actual });
            }
            Ok(TurnWorkCursor {
                projection_version,
                thread_id,
                turn_id,
                work_item_id,
                order_key,
            })
        }
        TimelineCursorPayload::ThreadTimelineBlock { .. } => {
            Err(TimelineCursorError::WrongCursorKind {
                expected: "turn_work_item",
            })
        }
    }
}

pub(super) fn resolve_thread_timeline_anchor(
    anchor: &TimelinePageAnchor,
    thread_id: &str,
    projection_version: i64,
) -> Result<ResolvedTimelineAnchor, TimelineCursorError> {
    match anchor {
        TimelinePageAnchor::Newest => Ok(ResolvedTimelineAnchor::Newest),
        TimelinePageAnchor::Oldest => Ok(ResolvedTimelineAnchor::Oldest),
        TimelinePageAnchor::Before { cursor } => {
            let cursor = decode_thread_timeline_cursor(cursor, thread_id, projection_version)?;
            Ok(ResolvedTimelineAnchor::Before(cursor.sort_key))
        }
        TimelinePageAnchor::After { cursor } => {
            let cursor = decode_thread_timeline_cursor(cursor, thread_id, projection_version)?;
            Ok(ResolvedTimelineAnchor::After(cursor.sort_key))
        }
        TimelinePageAnchor::Around { cursor } => {
            let cursor = decode_thread_timeline_cursor(cursor, thread_id, projection_version)?;
            Ok(ResolvedTimelineAnchor::Around(cursor.sort_key))
        }
    }
}

pub(super) fn resolve_turn_work_anchor(
    anchor: &TimelinePageAnchor,
    thread_id: &str,
    turn_id: &str,
    projection_version: i64,
) -> Result<ResolvedTimelineAnchor, TimelineCursorError> {
    match anchor {
        TimelinePageAnchor::Newest => Ok(ResolvedTimelineAnchor::Newest),
        TimelinePageAnchor::Oldest => Ok(ResolvedTimelineAnchor::Oldest),
        TimelinePageAnchor::Before { cursor } => {
            let cursor = decode_turn_work_cursor(cursor, thread_id, turn_id, projection_version)?;
            Ok(ResolvedTimelineAnchor::Before(cursor.order_key))
        }
        TimelinePageAnchor::After { cursor } => {
            let cursor = decode_turn_work_cursor(cursor, thread_id, turn_id, projection_version)?;
            Ok(ResolvedTimelineAnchor::After(cursor.order_key))
        }
        TimelinePageAnchor::Around { cursor } => {
            let cursor = decode_turn_work_cursor(cursor, thread_id, turn_id, projection_version)?;
            Ok(ResolvedTimelineAnchor::Around(cursor.order_key))
        }
    }
}

fn encode_payload(payload: &TimelineCursorPayload) -> Result<TimelineCursor, TimelineCursorError> {
    let bytes = serde_json::to_vec(payload).map_err(|_| TimelineCursorError::InvalidCursor)?;
    if bytes.len() > MAX_CURSOR_DECODED_BYTES {
        return Err(TimelineCursorError::InvalidCursor);
    }
    Ok(TimelineCursor {
        value: URL_SAFE_NO_PAD.encode(bytes),
    })
}

fn decode_payload(cursor: &TimelineCursor) -> Result<TimelineCursorPayload, TimelineCursorError> {
    if cursor.value.is_empty() || cursor.value.len() > MAX_CURSOR_ENCODED_BYTES {
        return Err(TimelineCursorError::InvalidCursor);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor.value.as_bytes())
        .map_err(|_| TimelineCursorError::InvalidCursor)?;
    if bytes.len() > MAX_CURSOR_DECODED_BYTES {
        return Err(TimelineCursorError::InvalidCursor);
    }
    serde_json::from_slice(&bytes).map_err(|_| TimelineCursorError::InvalidCursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_cursor_roundtrips_and_validates_thread_scope() {
        let cursor = encode_thread_timeline_cursor(1, "thread-a", "block-a", "0001")
            .expect("cursor should encode");

        let decoded = decode_thread_timeline_cursor(&cursor, "thread-a", 1)
            .expect("cursor should decode for the same thread");
        assert_eq!(decoded.projection_version, 1);
        assert_eq!(decoded.block_id, "block-a");
        assert_eq!(decoded.sort_key, "0001");

        let error = decode_thread_timeline_cursor(&cursor, "thread-b", 1)
            .expect_err("cursor must not cross thread scope");
        assert!(matches!(
            error,
            TimelineCursorError::CursorScopeMismatch { .. }
        ));

        let error = decode_thread_timeline_cursor(&cursor, "thread-a", 2)
            .expect_err("cursor must not cross projection versions");
        assert!(matches!(error, TimelineCursorError::StaleCursor { .. }));
    }

    #[test]
    fn turn_work_cursor_roundtrips_and_validates_turn_scope() {
        let cursor = encode_turn_work_cursor(1, "thread-a", "turn-a", "work-a", "0002")
            .expect("cursor should encode");

        let decoded = decode_turn_work_cursor(&cursor, "thread-a", "turn-a", 1)
            .expect("cursor should decode for the same turn");
        assert_eq!(decoded.projection_version, 1);
        assert_eq!(decoded.work_item_id, "work-a");
        assert_eq!(decoded.order_key, "0002");

        let error = decode_turn_work_cursor(&cursor, "thread-a", "turn-b", 1)
            .expect_err("cursor must not cross turn scope");
        assert!(matches!(
            error,
            TimelineCursorError::CursorScopeMismatch { .. }
        ));

        let error = decode_turn_work_cursor(&cursor, "thread-a", "turn-a", 2)
            .expect_err("cursor must not cross projection versions");
        assert!(matches!(error, TimelineCursorError::StaleCursor { .. }));
    }

    #[test]
    fn rejects_invalid_cursor_without_panicking() {
        let cursor = TimelineCursor {
            value: "not-base64-json".to_owned(),
        };
        let error = decode_thread_timeline_cursor(&cursor, "thread-a", 1)
            .expect_err("invalid cursor should be rejected");
        assert_eq!(error, TimelineCursorError::InvalidCursor);
    }

    #[test]
    fn rejects_excessive_limits_before_query() {
        assert_eq!(
            validate_timeline_limit(None, TimelineLimitPolicy::thread_timeline()).unwrap(),
            u64::from(THREAD_TIMELINE_DEFAULT_LIMIT)
        );
        assert_eq!(
            validate_timeline_limit(
                Some(THREAD_TIMELINE_MAX_LIMIT),
                TimelineLimitPolicy::thread_timeline()
            )
            .unwrap(),
            u64::from(THREAD_TIMELINE_MAX_LIMIT)
        );
        assert!(matches!(
            validate_timeline_limit(Some(0), TimelineLimitPolicy::thread_timeline()),
            Err(TimelineCursorError::InvalidLimitZero)
        ));
        assert!(matches!(
            validate_timeline_limit(
                Some(THREAD_TIMELINE_MAX_LIMIT + 1),
                TimelineLimitPolicy::thread_timeline()
            ),
            Err(TimelineCursorError::LimitTooLarge { .. })
        ));
    }
}
