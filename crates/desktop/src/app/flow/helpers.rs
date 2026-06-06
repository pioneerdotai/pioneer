use super::*;
use pioneer_client::gateway::runtime as client_gateway_runtime;
#[cfg(test)]
use pioneer_client::threads::resume as thread_resume;
use pioneer_client::threads::start as thread_start;
use pioneer_client::transport::ws as client_ws;

pub(crate) fn build_ws_connect_spec(
    runtime: &GatewayRuntime,
    endpoint: &GatewayEndpoint,
) -> anyhow::Result<GatewayWsConnectSpec> {
    let plan = client_gateway_runtime::plan_gateway_connect_spec(
        endpoint,
        runtime.gateway_auth_token_for_endpoint(endpoint)?,
        ws_timings_for_endpoint(runtime, endpoint.kind),
    );
    Ok(ws_connect_spec_from_plan(plan))
}

pub(crate) fn build_remote_candidate_ws_connect_spec(
    runtime: &GatewayRuntime,
    name: &str,
    address: &str,
    token: &str,
) -> GatewayWsConnectSpec {
    let plan = client_gateway_runtime::plan_remote_candidate_connect_spec(
        format!("candidate-{}", generate_id(ID_LEN)),
        name,
        t!("gateway.endpoint.remote_name", index = 1).to_string(),
        address,
        token,
        ws_timings_for_endpoint(runtime, GatewayEndpointKind::Remote),
    );
    ws_connect_spec_from_plan(plan)
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

pub(crate) fn should_apply_ws_event(
    active_connection_id: Option<u64>,
    event: &GatewayWsEvent,
) -> bool {
    client_ws::should_apply_ws_event(active_connection_id, event)
}

pub(crate) fn is_transient_thread_start_error(error: &anyhow::Error) -> bool {
    thread_start::is_transient_thread_start_error_message(format!("{error:#}").as_str())
}

#[cfg(test)]
pub(crate) fn thread_start_retry_delay(attempt: u32) -> Duration {
    thread_start::thread_start_retry_delay(attempt)
}

#[cfg(test)]
pub(crate) fn turn_resume_retry_delay(attempt: u32) -> Duration {
    thread_resume::turn_resume_retry_delay(attempt)
}

pub(crate) fn should_apply_gateway_operation_result(
    current_epoch: u64,
    operation_epoch: u64,
) -> bool {
    client_gateway_runtime::should_apply_gateway_operation_result(current_epoch, operation_epoch)
}

pub(crate) fn gateway_activation_requires_local_start(
    endpoint_kind: Option<GatewayEndpointKind>,
) -> bool {
    client_gateway_runtime::gateway_activation_requires_local_start(endpoint_kind)
}

fn ws_timings_for_endpoint(
    runtime: &GatewayRuntime,
    endpoint_kind: GatewayEndpointKind,
) -> crate::gateway::GatewayWsTimings {
    client_gateway_runtime::ws_timings_for_endpoint(
        runtime.ws_timings(),
        endpoint_kind,
        Duration::from_millis(REMOTE_WS_CONNECT_TIMEOUT_MIN_MS),
    )
}

fn ws_connect_spec_from_plan(
    plan: client_gateway_runtime::GatewayConnectSpecPlan,
) -> GatewayWsConnectSpec {
    GatewayWsConnectSpec {
        endpoint_id: plan.endpoint_id,
        endpoint_name: plan.endpoint_name,
        endpoint_kind: plan.endpoint_kind,
        address: plan.address,
        auth_token: plan.auth_token,
        timings: plan.timings,
    }
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
    client_gateway_runtime::gateway_activation_is_noop(
        active_gateway_id,
        gateway_id,
        gateway_has_ready_ws_connection(connection_state, ws_connection_id),
    )
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
