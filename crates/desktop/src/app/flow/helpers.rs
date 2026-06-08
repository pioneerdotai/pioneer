use super::*;
use pioneer_client::gateway::{runtime as client_gateway_runtime, setup as client_gateway_setup};

pub(crate) fn build_ws_connect_spec(
    runtime: &GatewayRuntime,
    endpoint: &GatewayEndpoint,
) -> anyhow::Result<GatewayWsConnectSpec> {
    let plan = client_gateway_runtime::plan_gateway_connect_spec(
        endpoint,
        runtime.gateway_auth_token_for_endpoint(endpoint)?,
        ws_timings_for_endpoint(runtime, endpoint.kind),
    );
    Ok(plan.into())
}

#[cfg(test)]
pub(crate) fn build_remote_candidate_ws_connect_spec(
    runtime: &GatewayRuntime,
    name: &str,
    address: &str,
    token: &str,
) -> GatewayWsConnectSpec {
    let plan = client_gateway_runtime::plan_remote_candidate_connect_spec(
        format!("candidate-{}", pioneer_protocol::generate_id(ID_LEN)),
        name,
        t!("gateway.endpoint.remote_name", index = 1).to_string(),
        address,
        token,
        ws_timings_for_endpoint(runtime, GatewayEndpointKind::Remote),
    );
    plan.into()
}

pub(crate) fn validate_remote_candidate_gateway_connection(
    runtime: &GatewayRuntime,
    address: &str,
    token: &str,
) -> anyhow::Result<()> {
    let address = crate::gateway::normalize_address(address)?;
    let timings = ws_timings_for_endpoint(runtime, GatewayEndpointKind::Remote);
    client_gateway_setup::validate_remote_gateway_connection_with_timings(
        address.as_str(),
        Some(token),
        timings,
    )
    .map(|_| ())
    .map_err(|error| anyhow::anyhow!(error))
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

pub(crate) fn gateway_has_ready_ws_connection(
    connection_state: GatewayConnectionState,
    ws_connection_id: Option<u64>,
) -> bool {
    pioneer_client::state::client_state::gateway_has_ready_ws_connection(
        connection_state,
        ws_connection_id,
    )
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
