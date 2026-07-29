use super::*;
use pioneer_client::gateway::runtime as client_gateway_runtime;
use pioneer_client::gateway::session_lifecycle::SessionTerminalReason;

pub(crate) fn desktop_session_terminal_message(reason: SessionTerminalReason) -> String {
    match reason {
        SessionTerminalReason::AuthenticationRequired => {
            t!("gateway.session_terminal.authentication_required").to_string()
        }
        SessionTerminalReason::SessionRevoked => t!("gateway.session_terminal.revoked").to_string(),
        SessionTerminalReason::SessionExpired | SessionTerminalReason::RefreshCredentialInvalid => {
            t!("gateway.session_terminal.expired").to_string()
        }
        SessionTerminalReason::SessionCompromised
        | SessionTerminalReason::RefreshOutcomeUnknown => {
            t!("gateway.session_terminal.compromised").to_string()
        }
        SessionTerminalReason::GatewayIdentityMismatch => {
            t!("gateway.session_terminal.gateway_mismatch").to_string()
        }
        SessionTerminalReason::SecureStorageFailed => {
            t!("gateway.session_terminal.storage_failed").to_string()
        }
    }
}

pub(crate) fn build_ws_connect_spec(
    runtime: &mut GatewayRuntime,
    endpoint: &GatewayEndpoint,
) -> anyhow::Result<GatewayWsConnectSpec> {
    match runtime.prepare_gateway_session(endpoint.id.as_str())? {
        crate::gateway::DesktopSessionPreparation::Ready(ready) => {
            Ok(ready.spec.into_connect_spec())
        }
        crate::gateway::DesktopSessionPreparation::Terminal(terminal) => {
            anyhow::bail!(desktop_session_terminal_message(terminal.reason))
        }
    }
}

pub(crate) fn build_local_ws_connect_spec_with_recovery(
    runtime: &mut GatewayRuntime,
    endpoint: &GatewayEndpoint,
) -> anyhow::Result<GatewayWsConnectSpec> {
    match runtime.prepare_gateway_session(endpoint.id.as_str())? {
        crate::gateway::DesktopSessionPreparation::Ready(ready) => {
            Ok(ready.spec.into_connect_spec())
        }
        crate::gateway::DesktopSessionPreparation::Terminal(_) => {
            runtime.stage_local_gateway_session_recovery(endpoint.id.as_str())?;
            match runtime.prepare_gateway_session(endpoint.id.as_str())? {
                crate::gateway::DesktopSessionPreparation::Ready(ready) => {
                    Ok(ready.spec.into_connect_spec())
                }
                crate::gateway::DesktopSessionPreparation::Terminal(terminal) => {
                    anyhow::bail!(desktop_session_terminal_message(terminal.reason))
                }
            }
        }
    }
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
