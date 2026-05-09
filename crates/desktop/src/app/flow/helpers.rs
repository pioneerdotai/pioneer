use super::*;

pub(crate) fn build_ws_connect_spec(
    runtime: &GatewayRuntime,
    endpoint: &GatewayEndpoint,
) -> anyhow::Result<GatewayWsConnectSpec> {
    Ok(GatewayWsConnectSpec {
        endpoint_id: endpoint.id.clone(),
        endpoint_name: endpoint.name.clone(),
        endpoint_kind: endpoint.kind,
        address: endpoint.address.clone(),
        auth_token: runtime.gateway_auth_token_for_endpoint(endpoint)?,
        timings: ws_timings_for_endpoint(runtime, endpoint.kind),
    })
}

pub(crate) fn build_remote_candidate_ws_connect_spec(
    runtime: &GatewayRuntime,
    name: &str,
    address: &str,
    token: &str,
) -> GatewayWsConnectSpec {
    let endpoint_name = if name.trim().is_empty() {
        t!("gateway.endpoint.remote_name", index = 1).to_string()
    } else {
        name.trim().to_owned()
    };

    GatewayWsConnectSpec {
        endpoint_id: format!("candidate-{}", generate_id(ID_LEN)),
        endpoint_name,
        endpoint_kind: GatewayEndpointKind::Remote,
        address: address.trim().to_owned(),
        auth_token: {
            let token = token.trim();
            if token.is_empty() {
                None
            } else {
                Some(token.to_owned())
            }
        },
        timings: ws_timings_for_endpoint(runtime, GatewayEndpointKind::Remote),
    }
}

pub(crate) fn validate_remote_candidate_gateway_connection(
    runtime: &GatewayRuntime,
    name: &str,
    address: &str,
    token: &str,
) -> anyhow::Result<()> {
    let address = crate::gateway::normalize_address(address)?;

    let validation_client = GatewayWsClient::new();
    let validation_sender = validation_client.command_sender();
    let spec = build_remote_candidate_ws_connect_spec(runtime, name, address.as_str(), token);
    let result = validation_sender.connect_and_wait(spec);
    let _ = validation_sender.shutdown();
    result.map(|_| ())
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

pub(crate) fn gateway_activation_requires_local_start(
    endpoint_kind: Option<GatewayEndpointKind>,
) -> bool {
    endpoint_kind == Some(GatewayEndpointKind::Local)
}

fn ws_timings_for_endpoint(
    runtime: &GatewayRuntime,
    endpoint_kind: GatewayEndpointKind,
) -> crate::gateway::GatewayWsTimings {
    let mut timings = runtime.ws_timings();
    if endpoint_kind == GatewayEndpointKind::Remote {
        let minimum = Duration::from_millis(REMOTE_WS_CONNECT_TIMEOUT_MIN_MS);
        if timings.connect_timeout < minimum {
            timings.connect_timeout = minimum;
        }
    }
    timings
}

pub(crate) fn gateway_has_ready_ws_connection(
    connection_state: GatewayConnectionState,
    ws_connection_id: Option<u64>,
) -> bool {
    connection_state == GatewayConnectionState::Connected && ws_connection_id.is_some()
}

pub(crate) fn gateway_activation_is_noop(
    active_gateway_id: Option<&str>,
    gateway_id: &str,
    connection_state: GatewayConnectionState,
    ws_connection_id: Option<u64>,
) -> bool {
    active_gateway_id == Some(gateway_id)
        && gateway_has_ready_ws_connection(connection_state, ws_connection_id)
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
