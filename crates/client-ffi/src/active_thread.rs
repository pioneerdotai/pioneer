//! DTO and native file adaptation for typed Client thread operations.
use pioneer_client::{ClientError, ClientResult, core::ClientCore, runtime::ClientRuntime};
use pioneer_protocol::Thread;
use serde::{Deserialize, Serialize};
use std::{fs, sync::Arc};
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadOpenRequest {
    pub thread: Thread,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadOpenByIdRequest {
    pub thread_id: String,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientEnsureWorkspaceDraftRequest {
    pub workspace_id: String,
    #[serde(default)]
    pub visibility: Option<pioneer_protocol::ThreadVisibility>,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadSendTextRequest {
    pub operation: pioneer_client::composer::store::ComposerOperationIdentity,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct ClientActiveThreadSendTextResult {
    pub thread_id: String,
    pub turn_id: String,
    pub pending_request_id: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientPrepareVoiceComposerSnapshotRequest {
    pub operation: pioneer_client::composer::store::ComposerOperationIdentity,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientActiveThreadClearResult {
    pub unsubscribed_thread_ids: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClientActiveThreadSnapshot {
    pub thread_id: String,
    pub workspace_id: String,
    pub draft_thread_id: Option<String>,
    pub thread: Option<Thread>,
    pub history_loaded: bool,
    pub history_loading: bool,
    pub projection: pioneer_client::conversation::ConversationViewState,
    pub active_turn_security_summary: Option<pioneer_client::security::ClientTurnSecuritySummary>,
    pub active_turn_security_diagnostics:
        Vec<pioneer_client::security::ClientSecurityDiagnosticRow>,
    pub pending_requests: Vec<pioneer_client::cli_runtime::approvals::PendingRequest>,
    pub domain_revision: u64,
    pub timeline_revision: u64,
}

pub fn open_thread(
    core: &ClientCore,
    runtime: &ClientRuntime,
    request: ClientActiveThreadOpenRequest,
) -> anyhow::Result<()> {
    core.open_thread(&runtime.ws_command_sender(), request.thread)
}
pub fn open_thread_by_id(
    core: &ClientCore,
    runtime: &ClientRuntime,
    request: ClientActiveThreadOpenByIdRequest,
) -> anyhow::Result<()> {
    core.open_thread_by_id(&runtime.ws_command_sender(), &request.thread_id)
}
pub fn open_or_create_new_thread(
    core: &ClientCore,
    runtime: &ClientRuntime,
    request: ClientEnsureWorkspaceDraftRequest,
) -> anyhow::Result<String> {
    core.open_workspace_draft(
        &runtime.ws_command_sender(),
        &request.workspace_id,
        request
            .visibility
            .unwrap_or(pioneer_protocol::ThreadVisibility::Private),
    )
}
pub fn send_text_turn(
    core: &Arc<ClientCore>,
    _runtime: &ClientRuntime,
    request: ClientActiveThreadSendTextRequest,
) -> anyhow::Result<ClientActiveThreadSendTextResult> {
    let result = core.submit_composer_send(
        request.operation,
        &ClientFfiFileSystem,
        pioneer_client::composer::workflow::ComposerSendContext {
            workspace_id: request.workspace_id,
            endpoint_kind: None,
            failure_message: "Failed to send message".into(),
        },
    )?;
    Ok(ClientActiveThreadSendTextResult {
        thread_id: result.thread_id,
        turn_id: result.turn_id,
        pending_request_id: result.pending_request_id,
    })
}
pub fn prepare_voice_composer_snapshot(
    core: &ClientCore,
    _runtime: &ClientRuntime,
    request: ClientPrepareVoiceComposerSnapshotRequest,
) -> anyhow::Result<pioneer_client::composer::turn_prepare::PreparedVoiceComposerSnapshot> {
    core.prepare_composer_voice(&request.operation, &ClientFfiFileSystem, None)
}
pub fn clear(
    core: &ClientCore,
    runtime: &ClientRuntime,
) -> anyhow::Result<ClientActiveThreadClearResult> {
    Ok(ClientActiveThreadClearResult {
        unsubscribed_thread_ids: core.close_thread_sessions(&runtime.ws_command_sender()),
    })
}
#[derive(Clone, Copy, Debug)]
struct ClientFfiFileSystem;

impl pioneer_client::platform::ClientFileSystem for ClientFfiFileSystem {
    fn read_file(&self, path: &pioneer_client::platform::ClientPath) -> ClientResult<Vec<u8>> {
        fs::read(path.as_path()).map_err(|error| {
            ClientError::platform(format!(
                "failed to read `{}`: {error}",
                path.as_path().display()
            ))
        })
    }

    fn metadata(
        &self,
        path: &pioneer_client::platform::ClientPath,
    ) -> ClientResult<pioneer_client::platform::ClientFileMetadata> {
        let metadata = fs::metadata(path.as_path()).map_err(|error| {
            ClientError::platform(format!(
                "failed to stat `{}`: {error}",
                path.as_path().display()
            ))
        })?;
        Ok(pioneer_client::platform::ClientFileMetadata {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            is_file: metadata.is_file(),
            is_dir: metadata.is_dir(),
        })
    }

    fn write_cache_file(
        &self,
        _key: &str,
        _bytes: &[u8],
    ) -> ClientResult<pioneer_client::platform::ClientPath> {
        Err(ClientError::platform(
            "cache writes are not supported by composer upload filesystem adapter",
        ))
    }

    fn open_read(
        &self,
        path: &pioneer_client::platform::ClientPath,
    ) -> ClientResult<Box<dyn pioneer_client::platform::ClientFileReader>> {
        let file = fs::File::open(path.as_path()).map_err(|error| {
            ClientError::platform(format!(
                "failed to open `{}`: {error}",
                path.as_path().display()
            ))
        })?;
        Ok(Box::new(file))
    }
}
