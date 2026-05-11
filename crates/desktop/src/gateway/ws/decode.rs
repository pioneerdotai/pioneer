use super::*;

pub(super) fn process_text_payload(
    payload: &str,
    connection_id: u64,
    pending_requests: &mut HashMap<String, Sender<std::result::Result<JsonValue, String>>>,
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

    if let Some(response_id) = value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
    {
        let Some(response_tx) = pending_requests.remove(&response_id) else {
            return;
        };

        if let Some(result) = value.get("result") {
            let _ = response_tx.send(Ok(result.clone()));
            return;
        }

        if let Some(error) = value.get("error") {
            let message = error
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("JSON-RPC request failed");
            let _ = response_tx.send(Err(message.to_owned()));
            return;
        }

        let _ = response_tx.send(Err("invalid JSON-RPC response payload".to_owned()));
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

pub(super) fn upload_ack_key(upload_id: &str, offset: u64) -> String {
    format!("{upload_id}:{offset}")
}

pub(super) fn fail_pending_requests(
    pending_requests: &mut HashMap<String, Sender<std::result::Result<JsonValue, String>>>,
    error: &str,
) {
    for (_, response_tx) in pending_requests.drain() {
        let _ = response_tx.send(Err(error.to_owned()));
    }
}

pub(super) fn fail_pending_upload_chunks(
    pending_upload_chunks: &mut HashMap<
        String,
        Sender<std::result::Result<SkillsUploadChunkAckNotification, String>>,
    >,
    error: &str,
) {
    for (_, response_tx) in pending_upload_chunks.drain() {
        let _ = response_tx.send(Err(error.to_owned()));
    }
}

pub(super) fn fail_pending_artifact_upload_chunks(
    pending_upload_chunks: &mut HashMap<
        String,
        Sender<std::result::Result<ArtifactUploadChunkAckNotification, String>>,
    >,
    error: &str,
) {
    for (_, response_tx) in pending_upload_chunks.drain() {
        let _ = response_tx.send(Err(error.to_owned()));
    }
}

pub(super) fn fail_pending_artifact_download_chunks(
    pending_download_chunks: &mut HashMap<
        String,
        Sender<std::result::Result<ArtifactDownloadChunkPayload, String>>,
    >,
    error: &str,
) {
    for (_, response_tx) in pending_download_chunks.drain() {
        let _ = response_tx.send(Err(error.to_owned()));
    }
}
