//! Secret-preserving workspace-file view actions for first-party mobile shells.

use pioneer_client::transport::{
    http::{BrowserViewUrl, GatewayHttpAuthorityError, GatewayHttpError},
    ws::GatewayWsCommandSender,
};
use pioneer_protocol::ThreadFileViewGrantCreateParams;
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use crate::ClientFfiError;

pub(crate) const INVALID_THREAD_FILE_ACTION_CODE: &str = "invalid_thread_file_action";

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadFileViewOpenRequest {
    #[cfg_attr(feature = "schema", schemars(length(min = 1, max = 128)))]
    pub thread_id: String,
    #[cfg_attr(feature = "schema", schemars(length(min = 1, max = 128)))]
    pub turn_id: String,
    #[cfg_attr(feature = "schema", schemars(length(min = 1, max = 128)))]
    pub item_id: String,
    #[cfg_attr(feature = "schema", schemars(length(min = 1, max = 4096)))]
    pub href: String,
}

impl std::fmt::Debug for ClientThreadFileViewOpenRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientThreadFileViewOpenRequest")
            .field("thread_id", &self.thread_id)
            .field("turn_id", &self.turn_id)
            .field("item_id", &self.item_id)
            .field("href", &"[redacted]")
            .finish()
    }
}

impl Drop for ClientThreadFileViewOpenRequest {
    fn drop(&mut self) {
        self.href.zeroize();
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize, PartialEq, Eq)]
pub struct ClientThreadFileViewOpenResult {
    #[cfg_attr(feature = "schema", schemars(length(min = 1)))]
    pub view_url: String,
    pub expires_at: u64,
    #[cfg_attr(feature = "schema", schemars(length(min = 1, max = 255)))]
    pub file_name: String,
    #[cfg_attr(feature = "schema", schemars(length(min = 1, max = 255)))]
    pub content_type: String,
    #[cfg_attr(feature = "schema", schemars(range(max = 10485760)))]
    pub size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
}

impl std::fmt::Debug for ClientThreadFileViewOpenResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientThreadFileViewOpenResult")
            .field("view_url", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .field("file_name", &self.file_name)
            .field("content_type", &self.content_type)
            .field("size_bytes", &self.size_bytes)
            .field("line", &self.line)
            .field("column", &self.column)
            .finish()
    }
}

impl Drop for ClientThreadFileViewOpenResult {
    fn drop(&mut self) {
        self.view_url.zeroize();
    }
}

pub(crate) fn open_thread_file_view(
    sender: &GatewayWsCommandSender,
    request: ClientThreadFileViewOpenRequest,
) -> Result<ClientThreadFileViewOpenResult, ClientFfiError> {
    let grant = sender
        .thread_file_view_grant_create(ThreadFileViewGrantCreateParams {
            thread_id: request.thread_id.clone(),
            turn_id: request.turn_id.clone(),
            item_id: request.item_id.clone(),
            href: request.href.clone(),
        })
        .map_err(map_rpc_error)?;
    let access = sender
        .current_gateway_http_access()
        .map_err(map_authority_error)?;
    let view = BrowserViewUrl::resolve(&access.gateway_base_url, grant.relative_url.as_str())
        .map_err(map_http_error)?;
    Ok(ClientThreadFileViewOpenResult {
        view_url: view.expose_url().to_owned(),
        expires_at: grant.expires_at,
        file_name: grant.file_name.clone(),
        content_type: grant.content_type.clone(),
        size_bytes: grant.size_bytes,
        line: grant.line,
        column: grant.column,
    })
}

fn map_authority_error(error: GatewayHttpAuthorityError) -> ClientFfiError {
    let code = match error {
        GatewayHttpAuthorityError::Terminal(_) => "thread_file_authentication_required",
        GatewayHttpAuthorityError::TemporarilyUnavailable => "thread_file_action_failed",
    };
    ClientFfiError::new("workspace file view is unavailable", code)
}

fn map_rpc_error(error: anyhow::Error) -> ClientFfiError {
    let lower = format!("{error:#}").to_ascii_lowercase();
    let code = if lower.contains("10 mib") {
        "thread_file_too_large"
    } else if lower.contains("unauthorized") || lower.contains("authentication") {
        "thread_file_authentication_required"
    } else if lower.contains("forbidden") || lower.contains("not found") {
        "thread_file_unavailable"
    } else {
        "thread_file_action_failed"
    };
    ClientFfiError::new("workspace file view is unavailable", code)
}

fn map_http_error(error: GatewayHttpError) -> ClientFfiError {
    let code = match error {
        GatewayHttpError::AuthenticationTerminal(_)
        | GatewayHttpError::AuthenticationUnavailable
        | GatewayHttpError::Unauthorized => "thread_file_authentication_required",
        GatewayHttpError::Forbidden | GatewayHttpError::NotFound => "thread_file_unavailable",
        GatewayHttpError::InvalidEndpoint
        | GatewayHttpError::GatewayPinMismatch
        | GatewayHttpError::SessionMismatch => "thread_file_reconfiguration_required",
        _ => "thread_file_action_failed",
    };
    ClientFfiError::new("workspace file HTTP view is unavailable", code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_redacts_local_paths_and_view_urls() {
        let request = ClientThreadFileViewOpenRequest {
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            item_id: "item-1".to_owned(),
            href: "/Users/private/project/main.rs".to_owned(),
        };
        let request_debug = format!("{request:?}");
        assert!(request_debug.contains("[redacted]"));
        assert!(!request_debug.contains("/Users/private"));

        let result = ClientThreadFileViewOpenResult {
            view_url: "https://gateway.example/storage/views/secret".to_owned(),
            expires_at: 1,
            file_name: "main.rs".to_owned(),
            content_type: "text/plain; charset=utf-8".to_owned(),
            size_bytes: 10,
            line: Some(3),
            column: None,
        };
        let result_debug = format!("{result:?}");
        assert!(result_debug.contains("[redacted]"));
        assert!(!result_debug.contains("/storage/views/secret"));
    }
}
