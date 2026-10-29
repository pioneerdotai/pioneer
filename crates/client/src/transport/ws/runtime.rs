mod client;
mod command_sender;
pub(crate) use command_sender::ConnectionRequestTransport;

// Minimal distinct domain-control reserve. Keep it through the response, not
// only until the frame leaves the queue; this is not a measured throughput cap.
const SOCKET_CANCEL_CAPACITY: usize = 1;
const MAX_SOCKET_CANCEL_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConnectionScope {
    connection: u64,
    incarnation: u64,
}
#[derive(Clone)]
pub(crate) struct SocketReceipt {
    scope: ConnectionScope,
    live: Arc<Mutex<bool>>,
    cancels: tokio::sync::mpsc::Sender<SocketCancel>,
    cancel_slots: Arc<tokio::sync::Semaphore>,
}
impl SocketReceipt {
    pub(crate) fn new(connection: u64) -> (Self, SocketCancelReceiver) {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let (cancels, receiver) = tokio::sync::mpsc::channel(SOCKET_CANCEL_CAPACITY);
        let cancel_slots = Arc::new(tokio::sync::Semaphore::new(SOCKET_CANCEL_CAPACITY));
        let live = Arc::new(Mutex::new(true));
        let owner = SocketCancelReceiver {
            receiver,
            live: live.clone(),
            slots: cancel_slots.clone(),
        };
        (
            Self {
                scope: ConnectionScope {
                    connection,
                    incarnation: NEXT
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                            next.checked_add(1)
                        })
                        .expect("socket incarnation exhausted"),
                },
                live,
                cancels,
                cancel_slots,
            },
            owner,
        )
    }

    pub(crate) fn admit_cancel(
        &self,
        request_id: String,
        payload: String,
        response: crate::rpc::JsonRpcResponseSender,
    ) -> std::result::Result<(), String> {
        // Bound retained payload as well as count. The gateway deferred budget
        // is also 4 MiB; a control request is normally only a few identifiers.
        if payload.len() > MAX_SOCKET_CANCEL_BYTES {
            let message = "websocket Cancel payload too large";
            let _ = response.send(Err(crate::rpc::JsonRpcResponseError::transport(message)));
            return Err(message.into());
        }
        let slot = match self.cancel_slots.clone().try_acquire_owned() {
            Ok(slot) => slot,
            Err(_) => {
                let message = "websocket Cancel capacity full or retired";
                let _ = response.send(Err(crate::rpc::JsonRpcResponseError::transport(message)));
                return Err(message.into());
            }
        };
        self.bind_request(&response);
        if !response.is_active() || !response.hold_control_slot(slot) {
            response.cancel();
            return Err("websocket Cancel request retired".into());
        }
        let fallback = response.clone();
        self.cancels
            .try_send(SocketCancel {
                request_id,
                payload,
                response,
            })
            .map_err(|_| {
                let message = "websocket Cancel mailbox full or closed";
                let _ = fallback.send(Err(crate::rpc::JsonRpcResponseError::transport(message)));
                message.to_owned()
            })
    }

    pub(crate) fn bind_request(&self, response: &crate::rpc::JsonRpcResponseSender) {
        response.bind_transport(self.live.clone());
    }
    pub(crate) fn retire(&self) {
        *self.live.lock().unwrap() = false;
        self.cancel_slots.close();
    }
    fn is_current(&self) -> bool {
        *self.live.lock().unwrap()
    }
}
/// Only this exact canonical method bypasses queued ordinary commands. This
/// selects transport admission, not request validity or gateway authorization.
pub(super) fn is_turn_cancel(payload: &str) -> bool {
    #[derive(serde::Deserialize)]
    struct Method<'a> {
        #[serde(borrow)]
        method: std::borrow::Cow<'a, str>,
    }
    serde_json::from_str::<Method<'_>>(payload)
        .is_ok_and(|request| request.method == pioneer_protocol::constants::methods::TURN_CANCEL)
}

pub(crate) struct SocketCancel {
    pub(super) request_id: String,
    pub(super) payload: String,
    pub(super) response: crate::rpc::JsonRpcResponseSender,
}

pub(crate) struct SocketCancelReceiver {
    pub(super) receiver: tokio::sync::mpsc::Receiver<SocketCancel>,
    live: Arc<Mutex<bool>>,
    slots: Arc<tokio::sync::Semaphore>,
}
impl Drop for SocketCancelReceiver {
    fn drop(&mut self) {
        *self.live.lock().unwrap() = false;
        self.slots.close();
        self.receiver.close();
        while let Ok(command) = self.receiver.try_recv() {
            let _ = command
                .response
                .send(Err(crate::rpc::JsonRpcResponseError::transport(
                    "websocket Cancel owner dropped",
                )));
        }
    }
}

pub(crate) struct SocketLifetime(pub(crate) SocketReceipt);
impl Drop for SocketLifetime {
    fn drop(&mut self) {
        self.0.retire();
    }
}

use crate::transport::http::{GatewayHttpAccess, GatewayHttpAuthorityError};
use anyhow::{Context, Result, anyhow};
use pioneer_protocol::{
    ArtifactBindParams, ArtifactBindResponse, ArtifactCapabilitiesParams,
    ArtifactCapabilitiesResponse, ArtifactDeleteParams, ArtifactDeleteResponse, ArtifactGetParams,
    ArtifactGetResponse, ArtifactListForMessageParams, ArtifactListForThreadParams,
    ArtifactListForTurnParams, ArtifactListParams, ArtifactListResponse, ArtifactRestoreParams,
    ArtifactRestoreResponse, ArtifactUploadAbortParams, ArtifactUploadAbortResponse,
    ArtifactUploadChunkAckNotification, ArtifactUploadFinishParams, ArtifactUploadFinishResponse,
    ArtifactUploadStartParams, ArtifactUploadStartResponse, ArtifactViewGrantCreateParams,
    ArtifactViewGrantCreateResponse, AuthDeviceCreateResponse, AuthLogoutResponse, AuthMeResponse,
    AuthProfileUpdateParams, AuthProfileUpdateResponse, AuthSessionListResponse,
    AuthSessionRevokeParams, AuthSessionRevokeResponse, AuthorizationCapabilitiesParams,
    AuthorizationCapabilitySnapshot, CLIRuntimeListModelsParams, CLIRuntimeListModelsResponse,
    CLIRuntimeListParams, CLIRuntimeListResponse, CLIRuntimeLoginCancelParams,
    CLIRuntimeLoginCancelResponse, CLIRuntimeLoginStartParams, CLIRuntimeLoginStartResponse,
    CLIRuntimeProxyDeleteParams, CLIRuntimeProxyDeleteResponse, CLIRuntimeProxySetParams,
    CLIRuntimeProxySetResponse, CLIRuntimeRefreshParams, CLIRuntimeRefreshResponse,
    CLIRuntimeRequestRespondParams, CLIRuntimeRequestRespondResponse, CLIRuntimeReviewStartParams,
    CLIRuntimeReviewStartResponse, CLIRuntimeStatusParams, CLIRuntimeStatusResponse,
    CLIRuntimeThreadBindingGetParams, CLIRuntimeThreadBindingGetResponse,
    CLIRuntimeThreadCompactParams, CLIRuntimeThreadCompactResponse, CLIRuntimeThreadForkParams,
    CLIRuntimeThreadForkResponse, CLIRuntimeTurnSteerParams, CLIRuntimeTurnSteerResponse,
    GatewaySettingsGetResponse, GatewaySettingsUpdate, GatewaySettingsUpdateResponse,
    InvitationCreateParams, InvitationCreateResponse, InvitationListParams, InvitationListResponse,
    InvitationRevokeParams, InvitationRevokeResponse, McpInstallParams, McpInstallResponse,
    McpListParams, McpListResponse, McpPolicySetParams, McpPolicySetResponse,
    McpServerDetailsParams, McpServerDetailsResponse, McpServerRestartParams,
    McpServerRestartResponse, McpUninstallParams, McpUninstallResponse, MemberDeviceCreateParams,
    MemberDeviceCreateResponse, MemberListParams, MemberListResponse, MemberMutationResponse,
    MemberRemoveParams, MemberRestoreParams, MemberSuspendParams, ProviderConfigureParams,
    ProviderConfigureResponse, ProviderDeleteApiKeyParams, ProviderDeleteApiKeyResponse,
    ProviderListModelsParams, ProviderListModelsResponse, ProviderListParams, ProviderListResponse,
    ProviderSetApiKeyParams, ProviderSetApiKeyResponse, SkillListParams, SkillListResponse,
    SkillsHealthParams, SkillsHealthResponse, SkillsInstallParams, SkillsInstallResponse,
    SkillsPackInstallParams, SkillsPackInstallResponse, SkillsPackUninstallParams,
    SkillsPackUninstallResponse, SkillsPackUpdateParams, SkillsPackUpdateResponse,
    SkillsPolicyListParams, SkillsPolicyListResponse, SkillsPolicySetParams,
    SkillsPolicySetResponse, SkillsUninstallParams, SkillsUninstallResponse, SkillsUpdateParams,
    SkillsUpdateResponse, SkillsUploadAbortParams, SkillsUploadAbortResponse,
    SkillsUploadChunkAckNotification, SkillsUploadFinishParams, SkillsUploadFinishResponse,
    SkillsUploadStartParams, SkillsUploadStartResponse, TaskAcceptParams, TaskAcceptResponse,
    TaskCancelParams, TaskCancelResponse, TaskFinishParams, TaskFinishResponse, TaskReviseParams,
    TaskReviseResponse, TaskUserNotificationAcknowledgeParams,
    TaskUserNotificationAcknowledgeResponse, TaskUserNotificationListParams,
    TaskUserNotificationListResponse, ThreadAgentsDocArchiveParams, ThreadAgentsDocArchiveResponse,
    ThreadAgentsDocGetParams, ThreadAgentsDocGetResponse, ThreadAgentsDocResolveForThreadParams,
    ThreadAgentsDocResolveForThreadResponse, ThreadAgentsDocSaveParams,
    ThreadAgentsDocSaveResponse, ThreadFileViewGrantCreateParams,
    ThreadFileViewGrantCreateResponse, ThreadFolderCreateParams, ThreadFolderCreateResponse,
    ThreadFolderDeleteParams, ThreadFolderDeleteResponse, ThreadFolderMoveParams,
    ThreadFolderMoveResponse, ThreadGetParams, ThreadGetResponse, ThreadMoveParams,
    ThreadMoveResponse, ThreadParticipantMutationParams, ThreadParticipantsListParams,
    ThreadParticipantsResponse, ThreadStartParams, ThreadStartResponse, ThreadTimelinePageParams,
    ThreadTimelinePageResponse, ThreadTreeParams, ThreadTreeResponse, ThreadUnsubscribeResponse,
    ThreadUpdateParams, ThreadUpdateResponse, TurnCancelParams, TurnCancelResponse, TurnGetParams,
    TurnGetResponse, TurnItemsParams, TurnItemsResponse, TurnPermissionRequestRespondParams,
    TurnPermissionRequestRespondResponse, TurnStartParams, TurnStartResponse,
    TurnWorkItemsGetParams, TurnWorkItemsGetResponse, TurnWorkPageParams, TurnWorkPageResponse,
    VoiceAudioFormat, VoiceSessionCancelParams, VoiceSessionCancelResponse,
    VoiceSessionFinalizeParams, VoiceSessionFinalizeResponse, VoiceSessionStartParams,
    VoiceSessionStartResponse, VoiceStatusParams, VoiceStatusResponse, WorkspaceCreateParams,
    WorkspaceCreateResponse, WorkspaceDefaultResponse, WorkspaceListResponse,
    WorkspaceMemberAddParams, WorkspaceMemberListParams, WorkspaceMemberListResponse,
    WorkspaceMemberMutationResponse, WorkspaceMemberRemoveParams, WorkspaceSelectParams,
    WorkspaceSelectResponse, WorkspaceUpdateParams, WorkspaceUpdateResponse,
};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::client::{GatewayWsCommand, WsSender, spawn_worker, work_channel};
use super::{GatewayWsConnectSpec, GatewayWsEvent};
use pioneer_observability::work::Boundary;

const UPLOAD_CHUNK_ACK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct GatewayWsCommandSender {
    command_tx: WsSender<GatewayWsCommand>,
    next_connection_id: Arc<AtomicU64>,
    socket: Arc<Mutex<Option<SocketReceipt>>>,
    session_access: Arc<Mutex<Option<GatewayHttpAccess>>>,
    connection_generations: Arc<Mutex<GatewayConnectionGenerations>>,
    #[cfg(test)]
    test_cancels: Arc<Mutex<Option<SocketCancelReceiver>>>,
    #[cfg(test)]
    pub(crate) test_requests:
        Arc<Mutex<Option<Arc<dyn crate::rpc::JsonRpcRequestTransport + Send + Sync>>>>,
}

#[derive(Clone)]
pub struct GatewayWsClient {
    command_sender: GatewayWsCommandSender,
    event_rx: Arc<Mutex<Receiver<pioneer_observability::work::WorkItem<GatewayWsEvent>>>>,
    #[cfg(test)]
    test_event_tx: Arc<Mutex<Option<super::client::GatewayWsEventSender>>>,
}

#[derive(Default)]
struct GatewayConnectionGenerations {
    active: Option<u64>,
    pending: Option<u64>,
    retiring: Option<u64>,
}

struct GatewayConnectionAttempt<'a> {
    sender: &'a GatewayWsCommandSender,
    id: u64,
}

impl GatewayConnectionAttempt<'_> {
    fn complete(self, access: Option<GatewayHttpAccess>) -> Result<u64> {
        let mut generations = self
            .sender
            .connection_generations
            .lock()
            .map_err(|_| anyhow!("Gateway connection owner poisoned"))?;
        if generations.pending != Some(self.id) {
            return Err(anyhow!("Gateway connection operation superseded"));
        }
        self.sender.replace_http_session_access(access)?;
        generations.active = Some(self.id);
        generations.pending = None;
        Ok(self.id)
    }
}

impl Drop for GatewayConnectionAttempt<'_> {
    fn drop(&mut self) {
        if let Ok(mut generations) = self.sender.connection_generations.lock() {
            if generations.pending == Some(self.id) {
                generations.pending = None;
            }
        }
    }
}
