use crate::ClientFfiError;
use pioneer_client::{
    rpc::WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE,
    transport::ws::{command_sender as ws_commands, worker},
};
use pioneer_protocol::{
    ThreadTimelinePageParams, ThreadTimelinePageResponse, TurnWorkItemsGetParams,
    TurnWorkItemsGetResponse, TurnWorkPageParams, TurnWorkPageResponse,
};

pub const TIMELINE_ERROR_CANCELLED: &str = "pioneer_timeline_cancelled";
pub const TIMELINE_ERROR_RECONNECT_REQUIRED: &str = "pioneer_timeline_reconnect_required";
pub const TIMELINE_ERROR_STALE_CURSOR: &str = "pioneer_timeline_stale_cursor";
pub const TIMELINE_ERROR_VALIDATION: &str = "pioneer_timeline_validation_error";

pub fn thread_timeline_page(
    transport: &impl pioneer_client::rpc::JsonRpcRequestTransport,
    params: ThreadTimelinePageParams,
) -> Result<ThreadTimelinePageResponse, ClientFfiError> {
    ws_commands::thread_timeline_page(transport, params).map_err(map_timeline_page_error)
}

pub fn turn_work_page(
    transport: &impl pioneer_client::rpc::JsonRpcRequestTransport,
    params: TurnWorkPageParams,
) -> Result<TurnWorkPageResponse, ClientFfiError> {
    ws_commands::turn_work_page(transport, params).map_err(map_timeline_page_error)
}

pub fn turn_work_items_get(
    transport: &impl pioneer_client::rpc::JsonRpcRequestTransport,
    params: TurnWorkItemsGetParams,
) -> Result<TurnWorkItemsGetResponse, ClientFfiError> {
    ws_commands::turn_work_items_get(transport, params).map_err(map_timeline_page_error)
}

fn map_timeline_page_error(error: anyhow::Error) -> ClientFfiError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();

    if lower.contains("cursor is invalid or stale")
        || lower.contains("cursor belongs to")
        || lower.contains("cursor kind does not match")
    {
        return ClientFfiError::new(message, TIMELINE_ERROR_STALE_CURSOR);
    }

    if lower.contains("cancelled")
        || lower.contains("canceled")
        || lower.contains("operation was aborted")
        || lower.contains("request aborted")
    {
        return ClientFfiError::new(message, TIMELINE_ERROR_CANCELLED);
    }

    if message == WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE
        || lower.contains("timed out waiting for `threadtimeline/page` response")
        || lower.contains("timed out waiting for `thread/timeline/page` response")
        || lower.contains("timed out waiting for `turnwork/page` response")
        || lower.contains("timed out waiting for `turn/work/page` response")
        || lower.contains(worker::WEBSOCKET_PONG_TIMEOUT_MESSAGE)
        || lower.contains(worker::WEBSOCKET_COMMAND_CHANNEL_CLOSED_MESSAGE)
        || lower.contains(worker::WEBSOCKET_CLOSED_BY_PEER_MESSAGE)
        || lower.contains(worker::WEBSOCKET_STREAM_ENDED_MESSAGE)
        || lower.contains("websocket read failed")
        || lower.contains("websocket write failed")
        || lower.contains("connection reset")
        || lower.contains("connection refused")
    {
        return ClientFfiError::new(message, TIMELINE_ERROR_RECONNECT_REQUIRED);
    }

    if lower.contains("invalid params for `threadtimeline/page`")
        || lower.contains("invalid params for `thread/timeline/page`")
        || lower.contains("invalid params for `turnwork/page`")
        || lower.contains("invalid params for `turn/work/page`")
        || lower.contains("thread_id is required for threadtimeline/page")
        || lower.contains("thread_id is required for thread/timeline/page")
        || lower.contains("thread_id is required for turnwork/page")
        || lower.contains("thread_id is required for turn/work/page")
        || lower.contains("turn_id is required for turnwork/page")
        || lower.contains("turn_id is required for turn/work/page")
        || lower.contains("failed to encode json-rpc params")
        || lower.contains("failed to decode `threadtimeline/page` response payload")
        || lower.contains("failed to decode `turnwork/page` response payload")
    {
        return ClientFfiError::new(message, TIMELINE_ERROR_VALIDATION);
    }

    ClientFfiError::new(message, ClientFfiError::GENERIC_CODE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn timeline_error_mapping_distinguishes_cursor_validation_and_reconnect() {
        assert_eq!(
            map_timeline_page_error(anyhow!(
                "invalid params for `thread/timeline/page`: cursor is invalid or stale"
            ))
            .code,
            TIMELINE_ERROR_STALE_CURSOR
        );
        assert_eq!(
            map_timeline_page_error(anyhow!("thread_id is required for thread/timeline/page")).code,
            TIMELINE_ERROR_VALIDATION
        );
        assert_eq!(
            map_timeline_page_error(anyhow!("websocket read failed: closed")).code,
            TIMELINE_ERROR_RECONNECT_REQUIRED
        );
    }
}
