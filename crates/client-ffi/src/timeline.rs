use crate::ClientFfiError;
use pioneer_client::{
    rpc::WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE,
    transport::ws::{command_sender as ws_commands, worker},
};
use pioneer_protocol::{
    ThreadReadParams, ThreadReadResponse, ThreadTimelinePageParams, ThreadTimelinePageResponse,
    TurnMessageDeleteParams, TurnMessageDeleteResponse, TurnMessageEditParams,
    TurnMessageEditResponse, TurnMessageErrorReason, TurnMessageRevisionsPageParams,
    TurnMessageRevisionsPageResponse, TurnWorkItemsGetParams, TurnWorkItemsGetResponse,
    TurnWorkPageParams, TurnWorkPageResponse,
};

pub const TIMELINE_ERROR_CANCELLED: &str = "pioneer_timeline_cancelled";
pub const TIMELINE_ERROR_RECONNECT_REQUIRED: &str = "pioneer_timeline_reconnect_required";
pub const TIMELINE_ERROR_STALE_CURSOR: &str = "pioneer_timeline_stale_cursor";
pub const TIMELINE_ERROR_VALIDATION: &str = "pioneer_timeline_validation_error";
pub const TURN_MESSAGE_ERROR_INVALID_INPUT: &str = "pioneer_turn_message_invalid_input";
pub const TURN_MESSAGE_ERROR_INVALID_TARGET: &str = "pioneer_turn_message_invalid_target";
pub const TURN_MESSAGE_ERROR_IMMUTABLE: &str = "pioneer_turn_message_immutable";
pub const TURN_MESSAGE_ERROR_DELETED: &str = "pioneer_turn_message_deleted";
pub const TURN_MESSAGE_ERROR_REVISION_CONFLICT: &str = "pioneer_turn_message_revision_conflict";
pub const THREAD_READ_ERROR: &str = "pioneer_thread_read_error";

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

pub fn turn_message_edit(
    transport: &impl pioneer_client::rpc::JsonRpcRequestTransport,
    params: TurnMessageEditParams,
) -> Result<TurnMessageEditResponse, ClientFfiError> {
    ws_commands::turn_message_edit(transport, params).map_err(map_turn_message_error)
}

pub fn turn_message_delete(
    transport: &impl pioneer_client::rpc::JsonRpcRequestTransport,
    params: TurnMessageDeleteParams,
) -> Result<TurnMessageDeleteResponse, ClientFfiError> {
    ws_commands::turn_message_delete(transport, params).map_err(map_turn_message_error)
}

pub fn turn_message_revisions_page(
    transport: &impl pioneer_client::rpc::JsonRpcRequestTransport,
    params: TurnMessageRevisionsPageParams,
) -> Result<TurnMessageRevisionsPageResponse, ClientFfiError> {
    ws_commands::turn_message_revisions_page(transport, params).map_err(map_turn_message_error)
}

pub fn thread_read(
    transport: &impl pioneer_client::rpc::JsonRpcRequestTransport,
    params: ThreadReadParams,
) -> Result<ThreadReadResponse, ClientFfiError> {
    ws_commands::thread_read(transport, params)
        .map_err(|error| ClientFfiError::new(format!("{error:#}"), THREAD_READ_ERROR))
}

fn map_turn_message_error(error: anyhow::Error) -> ClientFfiError {
    let code = match ws_commands::turn_message_error_reason(&error) {
        Some(TurnMessageErrorReason::InvalidInput) => TURN_MESSAGE_ERROR_INVALID_INPUT,
        Some(TurnMessageErrorReason::InvalidTarget) => TURN_MESSAGE_ERROR_INVALID_TARGET,
        Some(TurnMessageErrorReason::ImmutableMessage) => TURN_MESSAGE_ERROR_IMMUTABLE,
        Some(TurnMessageErrorReason::DeletedMessage) => TURN_MESSAGE_ERROR_DELETED,
        Some(TurnMessageErrorReason::RevisionConflict) => TURN_MESSAGE_ERROR_REVISION_CONFLICT,
        None => ClientFfiError::GENERIC_CODE,
    };
    ClientFfiError::new(format!("{error:#}"), code)
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

    #[test]
    fn message_revision_conflict_maps_to_stable_refetch_code() {
        let error = anyhow::Error::new(pioneer_client::rpc::JsonRpcResponseError::server(
            Some(pioneer_protocol::INVALID_REQUEST_CODE),
            "message revision conflict",
            Some("revision_conflict".to_owned()),
        ));

        assert_eq!(
            map_turn_message_error(error).code,
            TURN_MESSAGE_ERROR_REVISION_CONFLICT
        );
    }
}
