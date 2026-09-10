use super::*;
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
        SessionTerminalReason::PrincipalSuspended => {
            t!("gateway.session_terminal.principal_suspended").to_string()
        }
        SessionTerminalReason::PrincipalRemoved => {
            t!("gateway.session_terminal.principal_removed").to_string()
        }
        SessionTerminalReason::GatewayIdentityMismatch => {
            t!("gateway.session_terminal.gateway_mismatch").to_string()
        }
        SessionTerminalReason::SecureStorageFailed => {
            t!("gateway.session_terminal.storage_failed").to_string()
        }
    }
}
