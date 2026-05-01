use super::*;

pub(crate) fn build_ws_connect_spec(
    runtime: &GatewayRuntime,
    endpoint: &GatewayEndpoint,
) -> GatewayWsConnectSpec {
    GatewayWsConnectSpec {
        endpoint_id: endpoint.id.clone(),
        endpoint_name: endpoint.name.clone(),
        endpoint_kind: endpoint.kind,
        address: endpoint.address.clone(),
        auth_token: endpoint.auth_token.clone(),
        timings: runtime.ws_timings(),
    }
}

pub(crate) fn event_connection_id(event: &GatewayWsEvent) -> u64 {
    match event {
        GatewayWsEvent::Connecting { connection_id, .. }
        | GatewayWsEvent::Connected { connection_id, .. }
        | GatewayWsEvent::Reconnecting { connection_id, .. }
        | GatewayWsEvent::Disconnected { connection_id, .. }
        | GatewayWsEvent::ConnectFailed { connection_id, .. }
        | GatewayWsEvent::Notification { connection_id, .. } => *connection_id,
    }
}

pub(crate) fn should_apply_ws_event(
    active_connection_id: Option<u64>,
    event: &GatewayWsEvent,
) -> bool {
    active_connection_id == Some(event_connection_id(event))
}

pub(crate) fn is_transient_thread_start_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    if message.contains("invalid json-rpc response payload")
        || message.contains("failed to decode `thread/start` response payload")
        || message.contains("invalid request")
    {
        return false;
    }

    message.contains("timeout")
        || message.contains("timed out")
        || message.contains("temporar")
        || message.contains("websocket")
        || message.contains("connection")
        || message.contains("internal error")
}

pub(crate) fn thread_start_retry_delay(attempt: u32) -> Duration {
    let multiplier = 1u64 << attempt.min(8);
    let delay_ms = THREAD_START_RETRY_INITIAL_DELAY_MS.saturating_mul(multiplier);
    Duration::from_millis(delay_ms.min(THREAD_START_RETRY_MAX_DELAY_MS))
}

pub(crate) fn turn_resume_retry_delay(attempt: u32) -> Duration {
    let multiplier = 1u64 << attempt.min(8);
    let delay_ms = TURN_RESUME_RETRY_INITIAL_DELAY_MS.saturating_mul(multiplier);
    Duration::from_millis(delay_ms.min(TURN_RESUME_RETRY_MAX_DELAY_MS))
}

pub(crate) fn should_apply_gateway_operation_result(
    current_epoch: u64,
    operation_epoch: u64,
) -> bool {
    current_epoch == operation_epoch
}

pub(crate) fn warning_notification_messages(warnings: &[GatewayInstallWarning]) -> Vec<String> {
    warnings
        .iter()
        .filter_map(|warning| {
            let code = warning.code.trim();
            if code == "path_update_skipped" {
                return Some(
                    t!(
                        "gateway.notification.path_update_skipped",
                        bin_dir = default_user_command_bin_dir_label()
                    )
                    .to_string(),
                );
            }

            let message = warning.message.trim();
            if message.is_empty() {
                None
            } else {
                Some(message.to_owned())
            }
        })
        .collect()
}
