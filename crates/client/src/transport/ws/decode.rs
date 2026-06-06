//! WebSocket message decoding helpers.

use super::{
    GatewayWsEvent,
    download::ArtifactDownloadChunkPayload,
    frames::{fail_pending_transfer_chunks, upload_chunk_key},
};
use crate::rpc::{
    PendingJsonRpcRequests, decode_json_rpc_response_value, fail_pending_json_rpc_requests,
};
use pioneer_protocol::{
    ArtifactUploadChunkAckNotification, GatewayNotification, JsonRpcNotification,
    SkillsUploadChunkAckNotification, constants::events,
};
use serde_json::Value as JsonValue;
use std::{collections::HashMap, sync::mpsc::Sender};

pub fn process_text_payload(
    payload: &str,
    connection_id: u64,
    pending_requests: &mut PendingJsonRpcRequests,
    pending_upload_chunks: &mut HashMap<
        String,
        Sender<std::result::Result<SkillsUploadChunkAckNotification, String>>,
    >,
    pending_artifact_upload_chunks: &mut HashMap<
        String,
        Sender<std::result::Result<ArtifactUploadChunkAckNotification, String>>,
    >,
    event_tx: &Sender<GatewayWsEvent>,
) {
    let value = match serde_json::from_str::<JsonValue>(payload) {
        Ok(value) => value,
        Err(_) => return,
    };

    if let Some((response_id, response)) = decode_json_rpc_response_value(&value) {
        let Some(response_tx) = pending_requests.remove(response_id.as_str()) else {
            return;
        };

        let _ = response_tx.send(response);
        return;
    }

    let notification = match serde_json::from_value::<JsonRpcNotification>(value) {
        Ok(notification) => notification,
        Err(_) => return,
    };

    if notification.method == events::SKILLS_UPLOAD_CHUNK_ACK
        && let Some(params) = notification.params.clone()
        && let Ok(ack) = serde_json::from_value::<SkillsUploadChunkAckNotification>(params)
    {
        let key = upload_ack_key(ack.upload_id.as_str(), ack.offset);
        if let Some(response_tx) = pending_upload_chunks.remove(&key) {
            let _ = response_tx.send(Ok(ack.clone()));
        }
    }

    if notification.method == events::ARTIFACT_UPLOAD_CHUNK_ACK
        && let Some(params) = notification.params.clone()
        && let Ok(ack) = serde_json::from_value::<ArtifactUploadChunkAckNotification>(params)
    {
        let key = upload_ack_key(ack.upload_id.as_str(), ack.offset);
        if let Some(response_tx) = pending_artifact_upload_chunks.remove(&key) {
            let _ = response_tx.send(Ok(ack.clone()));
        }
    }

    if let Some(notification) = GatewayNotification::from_jsonrpc(notification) {
        let _ = event_tx.send(GatewayWsEvent::Notification {
            connection_id,
            notification,
        });
    }
}

pub fn upload_ack_key(upload_id: &str, offset: u64) -> String {
    upload_chunk_key(upload_id, offset)
}

pub fn fail_pending_requests(pending_requests: &mut PendingJsonRpcRequests, error: &str) {
    fail_pending_json_rpc_requests(pending_requests, error);
}

pub fn fail_pending_upload_chunks(
    pending_upload_chunks: &mut HashMap<
        String,
        Sender<std::result::Result<SkillsUploadChunkAckNotification, String>>,
    >,
    error: &str,
) {
    fail_pending_transfer_chunks(pending_upload_chunks, error);
}

pub fn fail_pending_artifact_upload_chunks(
    pending_upload_chunks: &mut HashMap<
        String,
        Sender<std::result::Result<ArtifactUploadChunkAckNotification, String>>,
    >,
    error: &str,
) {
    fail_pending_transfer_chunks(pending_upload_chunks, error);
}

pub fn fail_pending_artifact_download_chunks(
    pending_download_chunks: &mut HashMap<
        String,
        Sender<std::result::Result<ArtifactDownloadChunkPayload, String>>,
    >,
    error: &str,
) {
    fail_pending_transfer_chunks(pending_download_chunks, error);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::{timings::GatewayWsTimings, types::GatewayEndpointKind};
    use crate::transport::ws::GatewayWsConnectSpec;
    use pioneer_protocol::{GatewayNotification, TurnItem, constants::events};
    use serde_json::json;
    use std::time::Duration;

    fn spec() -> GatewayWsConnectSpec {
        GatewayWsConnectSpec {
            endpoint_id: "remote".to_owned(),
            endpoint_name: "Remote".to_owned(),
            endpoint_kind: GatewayEndpointKind::Remote,
            address: "127.0.0.1:22000".to_owned(),
            auth_token: None,
            timings: GatewayWsTimings::from_millis(100, 200, 300, 400, 1_000, 0).expect("timings"),
        }
    }

    #[test]
    fn decode_routes_unknown_gateway_notification() {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let mut pending_requests = PendingJsonRpcRequests::default();
        let mut pending_upload_chunks = HashMap::new();
        let mut pending_artifact_upload_chunks = HashMap::new();
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "item/tool.started",
            "params": {
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "name": "tool-call"
            }
        })
        .to_string();

        process_text_payload(
            payload.as_str(),
            17,
            &mut pending_requests,
            &mut pending_upload_chunks,
            &mut pending_artifact_upload_chunks,
            &event_tx,
        );

        let event = event_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("expected websocket event");
        assert!(matches!(
            event,
            GatewayWsEvent::Notification {
                connection_id: 17,
                notification: GatewayNotification::Unknown(_),
            }
        ));
    }

    #[test]
    fn decode_routes_typed_item_completed_notification() {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let mut pending_requests = PendingJsonRpcRequests::default();
        let mut pending_upload_chunks = HashMap::new();
        let mut pending_artifact_upload_chunks = HashMap::new();
        let payload = json!({
            "jsonrpc": "2.0",
            "method": events::ITEM_COMPLETED,
            "params": {
                "workspace_id": "ws_1",
                "thread_id": "thread_parent000001",
                "turn_id": "turn_parent0000001",
                "item": {
                    "type": "task",
                    "id": "task_item_review001",
                    "taskId": "task_review00000001",
                    "runId": "run_review000000001",
                    "parentTaskId": null,
                    "rootTaskId": null,
                    "title": "Review child work",
                    "status": "waiting_review",
                    "triggerKind": "immediate",
                    "executorKind": "agent",
                    "childThreadId": "thread_child0000001",
                    "childTurnId": "turn_child000000001",
                    "agentRole": "worker",
                    "depth": 0,
                    "maxDepth": 3,
                    "nextFireAt": null,
                    "resultPreview": null,
                    "errorPreview": null,
                    "createdAt": 10,
                    "updatedAt": 20
                }
            }
        })
        .to_string();

        process_text_payload(
            payload.as_str(),
            17,
            &mut pending_requests,
            &mut pending_upload_chunks,
            &mut pending_artifact_upload_chunks,
            &event_tx,
        );

        let event = event_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("expected websocket event");
        let GatewayWsEvent::Notification {
            notification: GatewayNotification::ItemCompleted(notification),
            ..
        } = event
        else {
            panic!("unexpected event");
        };
        assert!(matches!(notification.item, TurnItem::Task { .. }));
    }

    #[test]
    fn decode_routes_json_rpc_response_to_pending_request() {
        let mut pending_requests = PendingJsonRpcRequests::default();
        let mut pending_upload_chunks = HashMap::new();
        let mut pending_artifact_upload_chunks = HashMap::new();
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        pending_requests.insert("req_1".to_owned(), response_tx);
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let payload = json!({
            "jsonrpc": "2.0",
            "id": "req_1",
            "result": { "ok": true }
        })
        .to_string();

        process_text_payload(
            payload.as_str(),
            17,
            &mut pending_requests,
            &mut pending_upload_chunks,
            &mut pending_artifact_upload_chunks,
            &event_tx,
        );

        assert!(event_rx.try_recv().is_err());
        assert_eq!(
            response_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("response"),
            Ok(json!({ "ok": true }))
        );
        assert!(pending_requests.is_empty());
    }

    #[test]
    fn decode_helper_keeps_upload_ack_key_contract() {
        let _ = spec();
        assert_eq!(upload_ack_key("upload_1", 42), "upload_1:42");
    }
}
