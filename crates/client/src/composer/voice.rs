//! Voice session commands consume the operation's captured scope and prepared context.

use super::store::{
    ComposerIntent, ComposerOperationCompletion, ComposerOperationIdentity, ComposerOperationKind,
    ComposerOperationStatus,
};
use crate::core::{ClientCore, ClientTransitionOutcome};
use crate::rpc::JsonRpcRequestTransport;
use crate::transport::ws::command_sender;
use pioneer_protocol::{
    VoiceAudioFormat, VoiceSessionCancelParams, VoiceSessionCancelResponse,
    VoiceSessionFinalizeParams, VoiceSessionFinalizeResponse, VoiceSessionStartParams,
    VoiceSessionStartResponse,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerVoiceStartRequest {
    pub operation: ComposerOperationIdentity,
    pub audio_format: VoiceAudioFormat,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerVoiceFinalizeRequest {
    pub operation: ComposerOperationIdentity,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerVoiceCancelRequest {
    pub operation: ComposerOperationIdentity,
    pub session_id: String,
    pub reason: Option<String>,
}

// Exact-session cleanup remains available after its draft retires. This contains
// identity only; access tokens stay in the accepted transport owner.
#[derive(Clone)]
pub(super) struct VoiceSessionCleanup {
    session_id: Option<String>,
    identity: ComposerOperationIdentity,
    authority: Option<(String, String, String)>,
    cleanup_requested: bool,
}
const VOICE_SESSION_CLEANUP_LIMIT: usize = 64;

#[derive(Default)]
pub(crate) struct ComposerVoiceCleanupController {
    sender: Option<std::sync::mpsc::SyncSender<()>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ComposerVoiceCleanupController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
    }
}
impl Drop for ComposerVoiceCleanupController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComposerVoiceCapturePreflightState {
    Checking,
    Ready,
}

impl ClientCore {
    /// Produces the native capture plan before a shell asks for microphone access.
    pub fn prepare_composer_voice_capture(
        &self,
        identity: ComposerOperationIdentity,
    ) -> anyhow::Result<super::store::ComposerOperationPlan> {
        self.prepare_composer_voice_capture_with(identity.clone(), |workspace_id| {
            self.wait_composer_session_refresh(&identity)?;
            let sender = self.compatibility_runtime().ws_command_sender();
            let access = sender
                .current_gateway_http_access()
                .map_err(|_| anyhow::anyhow!("Voice session authority unavailable"))?;
            command_sender::voice_status(
                &sender.requests_for_connection(access.generation),
                pioneer_protocol::VoiceStatusParams {
                    workspace_id: Some(workspace_id.into()),
                },
            )
        })
    }

    #[cfg(test)]
    fn prepare_composer_voice_capture_using(
        &self,
        identity: ComposerOperationIdentity,
        sender: &impl JsonRpcRequestTransport,
    ) -> anyhow::Result<super::store::ComposerOperationPlan> {
        self.prepare_composer_voice_capture_with(identity, |workspace_id| {
            command_sender::voice_status(
                sender,
                pioneer_protocol::VoiceStatusParams {
                    workspace_id: Some(workspace_id.into()),
                },
            )
        })
    }
    fn prepare_composer_voice_capture_with(
        &self,
        identity: ComposerOperationIdentity,
        request: impl FnOnce(&str) -> anyhow::Result<pioneer_protocol::VoiceStatusResponse>,
    ) -> anyhow::Result<super::store::ComposerOperationPlan> {
        let authority = self.voice_session_authority();
        let plan = self
            .composer_operation_plan(&identity)
            .filter(|plan| plan.kind == ComposerOperationKind::Voice)
            .ok_or_else(|| anyhow::anyhow!("Voice operation cancelled"))?;
        let context = plan
            .voice_start
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Voice requires an opened thread"))?;
        {
            let mut store = self.composer_store.lock().expect("composer store poisoned");
            let current = store
                .drafts
                .get(&identity.thread_id)
                .ok_or_else(|| anyhow::anyhow!("Voice operation cancelled"))?;
            let mut next = (**current).clone();
            let operation = next
                .operation
                .as_mut()
                .filter(|operation| operation.identity == identity && operation.pending())
                .ok_or_else(|| anyhow::anyhow!("Voice operation cancelled"))?;
            match operation.voice_capture_preflight {
                Some(ComposerVoiceCapturePreflightState::Ready) => return Ok(plan),
                Some(ComposerVoiceCapturePreflightState::Checking) => {
                    anyhow::bail!("Voice readiness check already pending")
                }
                None => {
                    operation.voice_capture_preflight =
                        Some(ComposerVoiceCapturePreflightState::Checking)
                }
            }
            self.publish_composer_model_display(&mut store, next);
        }
        let response = request(&context.workspace_id);
        let result = response.and_then(|response| {
            anyhow::ensure!(
                self.voice_session_authority() == authority
                    && self.composer_operation_plan(&identity).is_some(),
                "Voice operation cancelled"
            );
            use pioneer_protocol::VoiceStatus;
            if response.status != VoiceStatus::Ready {
                let fallback = match response.status {
                    VoiceStatus::ModelDownloading => "Voice model is still downloading.",
                    VoiceStatus::ModelLoading => "Voice model is still loading.",
                    VoiceStatus::Busy | VoiceStatus::Recording | VoiceStatus::Transcribing => {
                        "Voice input is busy."
                    }
                    VoiceStatus::Error | VoiceStatus::Unavailable => "Voice input is unavailable.",
                    VoiceStatus::Disabled => "Voice input is not ready.",
                    VoiceStatus::Ready => unreachable!(),
                };
                anyhow::bail!(
                    "{}",
                    response
                        .error
                        .map(|error| error.message)
                        .filter(|message| !message.is_empty())
                        .unwrap_or_else(|| fallback.into())
                );
            }
            let mut store = self.composer_store.lock().expect("composer store poisoned");
            let current = store
                .drafts
                .get(&identity.thread_id)
                .ok_or_else(|| anyhow::anyhow!("Voice operation cancelled"))?;
            let mut next = (**current).clone();
            let operation = next
                .operation
                .as_mut()
                .filter(|operation| {
                    operation.identity == identity
                        && operation.pending()
                        && operation.voice_capture_preflight
                            == Some(ComposerVoiceCapturePreflightState::Checking)
                })
                .ok_or_else(|| anyhow::anyhow!("Voice operation cancelled"))?;
            operation.voice_capture_preflight = Some(ComposerVoiceCapturePreflightState::Ready);
            self.publish_composer_model_display(&mut store, next);
            Ok(plan)
        });
        if let Err(error) = &result {
            self.complete_composer_operation(
                identity,
                ComposerOperationCompletion::Failed {
                    message: format!("{error:#}"),
                },
            );
        }
        result
    }

    pub fn start_composer_voice_session(
        &self,
        request: ComposerVoiceStartRequest,
    ) -> anyhow::Result<VoiceSessionStartResponse> {
        self.prepare_composer_voice_capture(request.operation.clone())?;
        let sender = self.compatibility_runtime().ws_command_sender();
        let access = sender
            .current_gateway_http_access()
            .map_err(|_| anyhow::anyhow!("Voice session authority unavailable"))?;
        // The claimed start owns its failure. A duplicate start must not fail
        // a capture that is already using this operation.
        self.start_composer_voice_session_using(
            request,
            &sender.requests_for_connection(access.generation),
        )
    }

    fn start_composer_voice_session_using(
        &self,
        request: ComposerVoiceStartRequest,
        sender: &impl JsonRpcRequestTransport,
    ) -> anyhow::Result<VoiceSessionStartResponse> {
        let identity = request.operation;
        let plan = self
            .composer_operation_plan(&identity)
            .filter(|p| p.kind == ComposerOperationKind::Voice)
            .ok_or_else(|| anyhow::anyhow!("Voice operation cancelled"))?;
        let context = plan
            .voice_start
            .ok_or_else(|| anyhow::anyhow!("Voice requires an opened thread"))?;
        anyhow::ensure!(
            self.composer_intent(ComposerIntent::StartVoiceCapture {
                identity: identity.clone()
            })
            .outcome()
                == ClientTransitionOutcome::Changed,
            "Voice session already starting"
        );
        let authority = self.voice_session_authority();
        let result = (|| {
            {
                let mut store = self.composer_store.lock().expect("composer store poisoned");
                anyhow::ensure!(
                    store.voice_sessions.len() < VOICE_SESSION_CLEANUP_LIMIT,
                    "Voice session cleanup capacity exhausted"
                );
                store.voice_sessions.insert(
                    identity.generation,
                    VoiceSessionCleanup {
                        session_id: None,
                        identity: identity.clone(),
                        authority: authority.clone(),
                        cleanup_requested: false,
                    },
                );
            }
            self.refresh_thread_subscription(sender, &context.thread_id, &context.workspace_id)?;
            anyhow::ensure!(
                self.composer_operation_plan(&identity).is_some(),
                "Voice operation cancelled"
            );
            let response = command_sender::voice_session_start(
                sender,
                VoiceSessionStartParams {
                    context,
                    audio_format: request.audio_format,
                },
            )?;
            if self.voice_session_authority() != authority || self.is_stopped() {
                return Err(anyhow::anyhow!("Voice session authority retired"));
            }
            {
                let mut store = self.composer_store.lock().expect("composer store poisoned");
                let session = store
                    .voice_sessions
                    .get_mut(&identity.generation)
                    .ok_or_else(|| anyhow::anyhow!("Voice session owner retired"))?;
                session.session_id = Some(response.session_id.clone());
            }
            if self
                .composer_intent(ComposerIntent::VoiceSessionStarted {
                    identity: identity.clone(),
                    session_id: response.session_id.clone(),
                })
                .outcome()
                != ClientTransitionOutcome::Changed
            {
                let _ = self.cancel_composer_voice_session_using(
                    ComposerVoiceCancelRequest {
                        operation: identity.clone(),
                        session_id: response.session_id,
                        reason: Some("voice_operation_cancelled".into()),
                    },
                    sender,
                );
                return Err(anyhow::anyhow!("Voice operation cancelled"));
            }
            Ok(response)
        })();
        if let Err(error) = &result {
            {
                let mut store = self.composer_store.lock().expect("composer store poisoned");
                if store
                    .voice_sessions
                    .get(&identity.generation)
                    .is_some_and(|session| session.session_id.is_none())
                {
                    store.voice_sessions.remove(&identity.generation);
                }
            }

            self.complete_composer_operation(
                identity,
                ComposerOperationCompletion::Failed {
                    message: format!("{error:#}"),
                },
            );
        }
        result
    }

    pub fn composer_voice_session_matches(
        &self,
        identity: &ComposerOperationIdentity,
        session_id: &str,
    ) -> bool {
        let authority = self.voice_session_authority();
        let session_matches = self
            .composer_store
            .lock()
            .expect("composer store poisoned")
            .voice_sessions
            .get(&identity.generation)
            .is_some_and(|session| {
                session.identity == *identity
                    && session.session_id.as_deref() == Some(session_id)
                    && session.authority == authority
            });
        !self.is_stopped()
            && session_matches
            && self
                .composer_snapshot(&identity.thread_id)
                .is_some_and(|p| {
                    p.operation().is_some_and(|operation| {
                        operation.identity == *identity
                            && operation.kind == ComposerOperationKind::Voice
                            && operation.pending()
                            && operation.voice_session_id.as_deref() == Some(session_id)
                    })
                })
    }

    pub fn send_composer_voice_audio_chunk(
        &self,
        identity: &ComposerOperationIdentity,
        session_id: String,
        sequence: u64,
        audio_format: VoiceAudioFormat,
        captured_at_unix_ms: Option<u64>,
        duration_ms: Option<u32>,
        pcm_chunk: Vec<u8>,
    ) -> anyhow::Result<()> {
        let sender = self.compatibility_runtime().ws_command_sender();
        let access = sender
            .current_gateway_http_access()
            .map_err(|_| anyhow::anyhow!("Voice session authority unavailable"))?;
        anyhow::ensure!(
            self.composer_voice_session_matches(identity, &session_id),
            "Voice operation cancelled"
        );
        let result = sender.send_voice_audio_chunk_for_connection(
            access.generation,
            session_id,
            sequence,
            audio_format,
            captured_at_unix_ms,
            duration_ms,
            pcm_chunk,
        );
        if let Err(error) = &result {
            self.complete_composer_operation(
                identity.clone(),
                ComposerOperationCompletion::Failed {
                    message: format!("{error:#}"),
                },
            );
        }
        result
    }

    pub fn finalize_composer_voice_session(
        &self,
        request: ComposerVoiceFinalizeRequest,
    ) -> anyhow::Result<VoiceSessionFinalizeResponse> {
        let sender = self.compatibility_runtime().ws_command_sender();
        let access = sender
            .current_gateway_http_access()
            .map_err(|_| anyhow::anyhow!("Voice session authority unavailable"))?;
        self.finalize_composer_voice_session_using(
            request,
            &sender.requests_for_connection(access.generation),
        )
    }

    fn finalize_composer_voice_session_using(
        &self,
        request: ComposerVoiceFinalizeRequest,
        sender: &impl JsonRpcRequestTransport,
    ) -> anyhow::Result<VoiceSessionFinalizeResponse> {
        let identity = request.operation;
        let publication = self
            .composer_snapshot(&identity.thread_id)
            .ok_or_else(|| anyhow::anyhow!("Voice operation cancelled"))?;
        let operation = publication
            .operation()
            .filter(|operation| {
                operation.identity == identity
                    && operation.kind == ComposerOperationKind::Voice
                    && operation.status == ComposerOperationStatus::Prepared
            })
            .ok_or_else(|| anyhow::anyhow!("Voice operation is not prepared"))?;
        let session_id = operation
            .voice_session_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Voice session is not started"))?;
        anyhow::ensure!(
            self.composer_voice_session_matches(&identity, &session_id),
            "Voice session authority retired"
        );
        let context = operation
            .voice_context
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Voice context is not prepared"))?;
        anyhow::ensure!(
            self.composer_intent(ComposerIntent::FinalizeVoiceCapture {
                identity: identity.clone()
            })
            .outcome()
                == ClientTransitionOutcome::Changed,
            "Voice finalization cancelled or already requested"
        );
        let result = command_sender::voice_session_finalize(
            sender,
            VoiceSessionFinalizeParams {
                session_id: session_id.clone(),
                context,
            },
        );
        if let Ok(response) = &result {
            self.composer_intent(ComposerIntent::VoiceFinalized {
                identity: identity.clone(),
                response: response.clone(),
            });
        }
        if let Err(error) = &result {
            self.complete_composer_operation(
                identity.clone(),
                ComposerOperationCompletion::Failed {
                    message: format!("{error:#}"),
                },
            );
            let _ = self.cancel_composer_voice_session_using(
                ComposerVoiceCancelRequest {
                    operation: identity,
                    session_id,
                    reason: Some("voice_finalize_failed".into()),
                },
                sender,
            );
        }
        result
    }

    fn voice_session_authority(&self) -> Option<(String, String, String)> {
        self.compatibility_runtime()
            .ws_command_sender()
            .current_gateway_http_access()
            .ok()
            .map(|access| {
                (
                    access.gateway_base_url.as_str().to_owned(),
                    access.gateway_id.to_string(),
                    access.session_id.to_string(),
                )
            })
    }
    pub fn cancel_composer_voice_session(
        &self,
        request: ComposerVoiceCancelRequest,
    ) -> anyhow::Result<VoiceSessionCancelResponse> {
        let sender = self.compatibility_runtime().ws_command_sender();
        let Ok(access) = sender.current_gateway_http_access() else {
            return Ok(VoiceSessionCancelResponse { cancelled: false });
        };
        self.cancel_composer_voice_session_using(
            request,
            &sender.requests_for_connection(access.generation),
        )
    }
    fn cancel_composer_voice_session_using(
        &self,
        request: ComposerVoiceCancelRequest,
        sender: &impl JsonRpcRequestTransport,
    ) -> anyhow::Result<VoiceSessionCancelResponse> {
        let claimed = {
            let mut store = self.composer_store.lock().expect("composer store poisoned");
            if self.is_stopped()
                || store
                    .voice_sessions
                    .get(&request.operation.generation)
                    .is_none_or(|session| {
                        session.identity != request.operation
                            || session.session_id.as_deref() != Some(request.session_id.as_str())
                    })
            {
                return Ok(VoiceSessionCancelResponse { cancelled: false });
            }
            store
                .voice_sessions
                .remove(&request.operation.generation)
                .unwrap()
        };
        if claimed.authority != self.voice_session_authority() {
            return Ok(VoiceSessionCancelResponse { cancelled: false });
        }
        self.complete_composer_operation(request.operation, ComposerOperationCompletion::Cancelled);
        command_sender::voice_session_cancel(
            sender,
            VoiceSessionCancelParams {
                session_id: request.session_id,
                reason: request.reason,
            },
        )
    }
    pub(super) fn retire_voice_operation(&self, identity: &ComposerOperationIdentity, sent: bool) {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let Some(session) = store
            .voice_sessions
            .get_mut(&identity.generation)
            .filter(|session| session.identity == *identity)
        else {
            return;
        };
        if sent {
            store.voice_sessions.remove(&identity.generation);
            return;
        }
        session.cleanup_requested = true;
        drop(store);
        if let Some(sender) = self
            .composer_voice_cleanup
            .lock()
            .expect("voice cleanup poisoned")
            .sender
            .as_ref()
        {
            // The single wake is coalesced; the bounded session registry owns every request.
            let _ = sender.try_send(());
        }
    }
    fn pending_voice_cleanup(&self) -> Option<ComposerVoiceCancelRequest> {
        self.composer_store
            .lock()
            .expect("composer store poisoned")
            .voice_sessions
            .values()
            .find_map(|session| {
                session
                    .cleanup_requested
                    .then(|| {
                        Some(ComposerVoiceCancelRequest {
                            operation: session.identity.clone(),
                            session_id: session.session_id.clone()?,
                            reason: Some("voice_operation_cancelled".into()),
                        })
                    })
                    .flatten()
            })
    }
    pub(crate) fn start_composer_voice_cleanup_controller(self: &std::sync::Arc<Self>) {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let weak = std::sync::Arc::downgrade(self);
        let task = std::thread::spawn(move || {
            while receiver.recv().is_ok() {
                loop {
                    let Some(core) = weak.upgrade().filter(|core| !core.is_stopped()) else {
                        return;
                    };
                    let Some(request) = core.pending_voice_cleanup() else {
                        break;
                    };
                    // A disconnected authority cannot receive cleanup. Remove this exact lease;
                    // a later connection must never receive an old session's command.
                    if core.voice_session_authority().is_none() {
                        core.forget_completed_voice_session(&request.session_id);
                        continue;
                    }
                    let _ = core.cancel_composer_voice_session(request);
                }
            }
        });
        *self
            .composer_voice_cleanup
            .lock()
            .expect("voice cleanup poisoned") = ComposerVoiceCleanupController {
            sender: Some(sender),
            task: Some(task),
        };
    }
    pub(super) fn forget_completed_voice_session(&self, session: &str) {
        self.composer_store
            .lock()
            .expect("composer store poisoned")
            .voice_sessions
            .retain(|_, value| value.session_id.as_deref() != Some(session));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::turn_prepare::PreparedVoiceComposerSnapshot;
    use pioneer_protocol::*;
    use std::cell::RefCell;
    use std::sync::Arc;

    fn fixture() -> (Arc<ClientCore>, ComposerOperationIdentity, Thread) {
        let core = Arc::new(ClientCore::new());
        let thread = Thread {
            workspace_id: "workspace".into(),
            id: "thread".into(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Message,
            model: "model".into(),
            model_provider: "provider".into(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 1,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: vec![],
        };
        core.upsert_thread(thread.clone());
        core.composer_intent(ComposerIntent::Open {
            thread_id: thread.id.clone(),
            defaults: Default::default(),
        });
        let draft_id = core.composer_snapshot(&thread.id).unwrap().draft_id();
        core.composer_intent(ComposerIntent::EditText {
            thread_id: thread.id.clone(),
            draft_id,
            text: "preserved draft".into(),
        });
        core.composer_intent(ComposerIntent::BeginOperation {
            thread_id: thread.id.clone(),
            draft_id,
            operation: ComposerOperationKind::Voice,
        });
        let identity = core
            .composer_snapshot(&thread.id)
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        (core, identity, thread)
    }

    struct Transport<'a> {
        thread: Thread,
        calls: RefCell<Vec<serde_json::Value>>,
        on_start: Box<dyn Fn() + 'a>,
        fail_finalize: bool,
    }
    impl JsonRpcRequestTransport for Transport<'_> {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            self.calls.borrow_mut().push(request.clone());
            let result = match request["method"].as_str().unwrap() {
                "thread/start" => serde_json::to_value(ThreadStartResponse {
                    thread: self.thread.clone(),
                    sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
                })
                .unwrap(),
                "voice/session/start" => {
                    (self.on_start)();
                    serde_json::json!({ "session_id": "session", "status": "recording" })
                }
                "voice/session/finalize" if self.fail_finalize => {
                    return Err("synthetic finalize failure".into());
                }
                "voice/session/finalize" => serde_json::json!({ "status": "transcribing" }),
                "voice/session/cancel" => serde_json::json!({ "cancelled": true }),
                method => panic!("unexpected method: {method}"),
            };
            response.send(Ok(result)).map_err(|e| e.to_string())
        }
    }
    struct ReadinessTransport<'a> {
        status: VoiceStatus,
        calls: std::cell::Cell<usize>,
        during: Box<dyn Fn() + 'a>,
    }
    impl JsonRpcRequestTransport for ReadinessTransport<'_> {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(request["method"], "voice/status");
            assert_eq!(request["params"]["workspace_id"], "workspace");
            self.calls.set(self.calls.get() + 1);
            (self.during)();
            response
                .send(Ok(serde_json::json!({"status": self.status})))
                .map_err(|error| error.to_string())
        }
    }
    #[test]
    fn capture_preflight_is_claimed_once_and_reuses_only_the_matching_ready_plan() {
        let (core, identity, _) = fixture();
        let transport = ReadinessTransport {
            status: VoiceStatus::Ready,
            calls: Default::default(),
            during: Box::new(|| {}),
        };
        let first = core
            .prepare_composer_voice_capture_using(identity.clone(), &transport)
            .unwrap();
        let publication = core.composer_snapshot(&identity.thread_id).unwrap();
        assert_eq!(
            core.prepare_composer_voice_capture_using(identity.clone(), &transport)
                .unwrap(),
            first
        );
        assert!(Arc::ptr_eq(
            &publication,
            &core.composer_snapshot(&identity.thread_id).unwrap()
        ));
        assert_eq!(transport.calls.get(), 1);
        core.complete_composer_operation(identity.clone(), ComposerOperationCompletion::Cancelled);
        assert!(
            core.prepare_composer_voice_capture_using(identity, &transport)
                .is_err()
        );
        assert_eq!(transport.calls.get(), 1);
    }
    #[test]
    fn capture_preflight_failure_preserves_draft_and_rejects_late_ready_results() {
        for status in [
            VoiceStatus::Busy,
            VoiceStatus::Disabled,
            VoiceStatus::ModelDownloading,
        ] {
            let (core, identity, _) = fixture();
            let before = core.composer_snapshot(&identity.thread_id).unwrap();
            let transport = ReadinessTransport {
                status,
                calls: Default::default(),
                during: Box::new(|| {}),
            };
            assert!(
                core.prepare_composer_voice_capture_using(identity.clone(), &transport)
                    .is_err()
            );
            let after = core.composer_snapshot(&identity.thread_id).unwrap();
            assert_eq!(after.draft(), before.draft());
            assert!(matches!(
                after.operation().unwrap().status,
                ComposerOperationStatus::Failed { .. }
            ));
        }
        let (core, identity, _) = fixture();
        let replacement = RefCell::new(None);
        let transport = ReadinessTransport {
            status: VoiceStatus::Ready,
            calls: Default::default(),
            during: Box::new(|| {
                core.composer_intent(ComposerIntent::Clear {
                    thread_id: identity.thread_id.clone(),
                    draft_id: identity.draft_id,
                });
                *replacement.borrow_mut() = core.composer_snapshot(&identity.thread_id);
            }),
        };
        assert!(
            core.prepare_composer_voice_capture_using(identity.clone(), &transport)
                .is_err()
        );
        assert!(Arc::ptr_eq(
            replacement.borrow().as_ref().unwrap(),
            &core.composer_snapshot(&identity.thread_id).unwrap()
        ));
    }
    #[test]
    fn capture_transport_failure_is_owned_and_duplicate_pending_request_is_noop() {
        let (core, identity, _) = fixture();
        let before = core.composer_snapshot(&identity.thread_id).unwrap();
        assert!(
            core.prepare_composer_voice_capture_with(identity.clone(), |_| {
                let pending = core.composer_snapshot(&identity.thread_id).unwrap();
                assert!(
                    core.prepare_composer_voice_capture_with(identity.clone(), |_| {
                        panic!("duplicate preflight performed IO")
                    })
                    .is_err()
                );
                assert!(Arc::ptr_eq(
                    &pending,
                    &core.composer_snapshot(&identity.thread_id).unwrap()
                ));
                anyhow::bail!("synthetic missing transport")
            })
            .is_err()
        );
        let after = core.composer_snapshot(&identity.thread_id).unwrap();
        assert_eq!(before.draft(), after.draft());
        assert!(matches!(
            after.operation().unwrap().status,
            ComposerOperationStatus::Failed { .. }
        ));
    }
    fn start_request(operation: ComposerOperationIdentity) -> ComposerVoiceStartRequest {
        ComposerVoiceStartRequest {
            operation,
            audio_format: VoiceAudioFormat {
                sample_rate_hz: 16_000,
                channels: 1,
                encoding: VoiceAudioEncoding::PcmS16Le,
            },
        }
    }
    fn prepare(core: &ClientCore, identity: &ComposerOperationIdentity) -> VoiceTurnContext {
        let scope = core
            .composer_operation_plan(identity)
            .unwrap()
            .voice_start
            .unwrap();
        let context: VoiceTurnContext = serde_json::from_value(serde_json::json!({
            "workspace_id": scope.workspace_id, "thread_id": scope.thread_id, "turn_id": scope.turn_id, "prepared_input": []
        })).unwrap();
        core.composer_intent(ComposerIntent::PrepareOperation {
            identity: identity.clone(),
        });
        core.composer_intent(ComposerIntent::UploadOperation {
            identity: identity.clone(),
        });
        let snapshot = PreparedVoiceComposerSnapshot {
            context: context.clone(),
            attachments: vec![],
            uploaded_attachment_artifacts: vec![],
            locked_attachment_count: 0,
            locked_capability_count: 0,
        };
        assert!(core.complete_composer_operation(
            identity.clone(),
            ComposerOperationCompletion::VoicePrepared { snapshot }
        ));
        context
    }

    #[test]
    fn exact_session_cleanup_is_claimed_once_and_wrong_identity_is_noop() {
        let (core, identity, thread) = fixture();
        let transport = Transport {
            thread,
            calls: Default::default(),
            on_start: Box::new(|| {}),
            fail_finalize: false,
        };
        core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
            .unwrap();
        let before = core.composer_snapshot("thread").unwrap();
        let mut wrong = identity.clone();
        wrong.generation += 1;
        let request = ComposerVoiceCancelRequest {
            operation: identity,
            session_id: "session".into(),
            reason: None,
        };
        assert!(
            !core
                .cancel_composer_voice_session_using(
                    ComposerVoiceCancelRequest {
                        operation: wrong,
                        ..request.clone()
                    },
                    &transport
                )
                .unwrap()
                .cancelled
        );
        assert!(
            !core
                .cancel_composer_voice_session_using(
                    ComposerVoiceCancelRequest {
                        session_id: "other".into(),
                        ..request.clone()
                    },
                    &transport
                )
                .unwrap()
                .cancelled
        );
        assert!(Arc::ptr_eq(
            &before,
            &core.composer_snapshot("thread").unwrap()
        ));
        assert_eq!(transport.calls.borrow().len(), 2);
        assert!(
            core.cancel_composer_voice_session_using(request.clone(), &transport)
                .unwrap()
                .cancelled
        );
        let cancelled = core.composer_snapshot("thread").unwrap();
        assert!(
            !core
                .cancel_composer_voice_session_using(request, &transport)
                .unwrap()
                .cancelled
        );
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.composer_snapshot("thread").unwrap()
        ));
        assert_eq!(transport.calls.borrow().len(), 3);
    }

    #[test]
    fn retired_draft_cleanup_does_not_mutate_its_replacement_and_rejects_changed_authority() {
        for changed_authority in [false, true] {
            let (core, identity, thread) = fixture();
            let transport = Transport {
                thread,
                calls: Default::default(),
                on_start: Box::new(|| {}),
                fail_finalize: false,
            };
            core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
                .unwrap();
            core.composer_intent(ComposerIntent::Clear {
                thread_id: "thread".into(),
                draft_id: identity.draft_id,
            });
            if changed_authority {
                core.composer_store
                    .lock()
                    .unwrap()
                    .voice_sessions
                    .get_mut(&identity.generation)
                    .unwrap()
                    .authority = Some((
                    "https://retired.invalid".into(),
                    "old-gateway".into(),
                    "old-session".into(),
                ));
            }
            let before = core.composer_snapshot("thread").unwrap();
            assert_eq!(
                core.cancel_composer_voice_session_using(
                    ComposerVoiceCancelRequest {
                        operation: identity,
                        session_id: "session".into(),
                        reason: None
                    },
                    &transport
                )
                .unwrap()
                .cancelled,
                !changed_authority
            );
            assert!(Arc::ptr_eq(
                &before,
                &core.composer_snapshot("thread").unwrap()
            ));
            assert_eq!(
                transport.calls.borrow().len(),
                if changed_authority { 2 } else { 3 }
            );
        }
    }

    #[test]
    fn cleanup_capacity_is_reserved_before_rpc_and_overload_preserves_draft_failure() {
        let (core, identity, thread) = fixture();
        let transport = Transport {
            thread,
            calls: Default::default(),
            on_start: Box::new(|| {}),
            fail_finalize: false,
        };
        for generation in 100..100 + VOICE_SESSION_CLEANUP_LIMIT as u64 {
            core.composer_store.lock().unwrap().voice_sessions.insert(
                generation,
                VoiceSessionCleanup {
                    session_id: None,
                    identity: ComposerOperationIdentity {
                        generation,
                        ..identity.clone()
                    },
                    authority: None,
                    cleanup_requested: false,
                },
            );
        }
        assert!(
            core.start_composer_voice_session_using(start_request(identity), &transport)
                .is_err()
        );
        assert!(transport.calls.borrow().is_empty());
        let input = core.composer_snapshot("thread").unwrap();
        assert_eq!(input.draft().text, "preserved draft");
        assert!(matches!(
            input.operation().unwrap().status,
            ComposerOperationStatus::Failed { .. }
        ));
        assert_eq!(
            core.composer_store.lock().unwrap().voice_sessions.len(),
            VOICE_SESSION_CLEANUP_LIMIT
        );
    }

    #[test]
    fn duplicate_start_and_finalize_do_no_io_and_use_client_captured_context() {
        let (core, identity, thread) = fixture();
        let transport = Transport {
            thread,
            calls: Default::default(),
            on_start: Box::new(|| {}),
            fail_finalize: false,
        };
        core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
            .unwrap();
        assert!(
            core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
                .is_err()
        );
        assert_eq!(transport.calls.borrow().len(), 2);
        let context = prepare(&core, &identity);
        let request = ComposerVoiceFinalizeRequest {
            operation: identity.clone(),
        };
        core.finalize_composer_voice_session_using(request.clone(), &transport)
            .unwrap();
        assert!(
            core.finalize_composer_voice_session_using(request, &transport)
                .is_err()
        );
        assert_eq!(transport.calls.borrow().len(), 3);
        assert_eq!(
            transport.calls.borrow()[2]["params"],
            serde_json::json!({ "session_id": "session", "context": context })
        );
        assert_eq!(
            core.composer_snapshot("thread").unwrap().draft().text,
            "preserved draft"
        );
        assert!(core.complete_composer_operation(identity, ComposerOperationCompletion::Sent));
        assert!(
            core.composer_snapshot("thread")
                .unwrap()
                .draft()
                .text
                .is_empty()
        );
    }

    #[test]
    fn committed_voice_publishes_matching_result_without_shell_reduction() {
        use crate::voice::VoiceFinalizeUiAction;
        use pioneer_protocol::{
            GatewayNotification, VoiceSessionOutcome, VoiceSessionResultNotification,
        };
        use std::sync::Arc;
        let (core, identity, thread) = fixture();
        let transport = Transport {
            thread,
            calls: Default::default(),
            on_start: Box::new(|| {}),
            fail_finalize: false,
        };
        core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
            .unwrap();
        let commit = ComposerIntent::CommitVoiceCapture {
            identity: identity.clone(),
        };
        assert_eq!(
            core.composer_intent(commit.clone()).outcome(),
            ClientTransitionOutcome::Changed
        );
        let committed = core.composer_snapshot("thread").unwrap();
        assert!(committed.operation().unwrap().voice_committing);
        assert_eq!(
            core.composer_intent(commit).outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(
            &committed,
            &core.composer_snapshot("thread").unwrap()
        ));
        let context = prepare(&core, &identity);
        core.finalize_composer_voice_session_using(
            ComposerVoiceFinalizeRequest {
                operation: identity.clone(),
            },
            &transport,
        )
        .unwrap();
        let waiting = core.composer_snapshot("thread").unwrap();
        assert_eq!(
            waiting
                .operation()
                .unwrap()
                .voice_finalize
                .as_ref()
                .unwrap()
                .action,
            VoiceFinalizeUiAction::KeepFinalizing
        );
        let notification = |session: &str, turn: &str, outcome| {
            GatewayNotification::VoiceSessionResult(VoiceSessionResultNotification {
                session_id: session.into(),
                turn_id: Some(turn.into()),
                outcome,
                error: None,
            })
        };
        core.observe_composer_voice_notification(&notification(
            "other-session",
            &context.turn_id,
            VoiceSessionOutcome::TurnStarted,
        ));
        core.observe_composer_voice_notification(&notification(
            "session",
            "other-turn",
            VoiceSessionOutcome::TurnStarted,
        ));
        assert!(Arc::ptr_eq(
            &waiting,
            &core.composer_snapshot("thread").unwrap()
        ));
        let success = notification(
            "session",
            &context.turn_id,
            VoiceSessionOutcome::TurnStarted,
        );
        core.observe_composer_voice_notification(&success);
        let completed = core.composer_snapshot("thread").unwrap();
        let operation = completed.operation().unwrap();
        assert!(completed.draft().text.is_empty());
        assert_ne!(completed.draft_id(), identity.draft_id);
        assert_eq!(
            operation.voice_turn_id.as_deref(),
            Some(context.turn_id.as_str())
        );
        assert!(operation.voice_committing);
        assert!(operation.plan.is_none());
        assert_eq!(
            operation.voice_result.as_ref().unwrap().action,
            VoiceFinalizeUiAction::ClearFinalizing
        );
        core.observe_composer_voice_notification(&success);
        core.observe_composer_voice_notification(&notification(
            "session",
            &context.turn_id,
            VoiceSessionOutcome::NoSpeech,
        ));
        assert_eq!(
            core.composer_intent(ComposerIntent::VoiceFinalized {
                identity,
                response: VoiceSessionFinalizeResponse {
                    status: pioneer_protocol::VoiceStatus::Ready
                }
            })
            .outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(
            &completed,
            &core.composer_snapshot("thread").unwrap()
        ));
    }

    #[test]
    fn voice_failure_and_cancellation_publish_terminal_state_and_preserve_draft() {
        use crate::voice::VoiceFinalizeUiAction;
        use pioneer_protocol::{
            GatewayNotification, VoiceSessionOutcome, VoiceSessionResultNotification,
        };
        for outcome in [
            VoiceSessionOutcome::NoSpeech,
            VoiceSessionOutcome::Failed,
            VoiceSessionOutcome::Cancelled,
        ] {
            let (core, identity, thread) = fixture();
            let transport = Transport {
                thread,
                calls: Default::default(),
                on_start: Box::new(|| {}),
                fail_finalize: false,
            };
            core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
                .unwrap();
            core.composer_intent(ComposerIntent::CommitVoiceCapture {
                identity: identity.clone(),
            });
            core.observe_composer_voice_notification(&GatewayNotification::VoiceSessionResult(
                VoiceSessionResultNotification {
                    session_id: "session".into(),
                    turn_id: None,
                    outcome,
                    error: None,
                },
            ));
            let publication = core.composer_snapshot("thread").unwrap();
            let operation = publication.operation().unwrap();
            assert_eq!(publication.draft().text, "preserved draft");
            assert_eq!(publication.draft_id(), identity.draft_id);
            assert!(!operation.voice_committing);
            assert!(!operation.pending());
            assert_eq!(
                operation.voice_result.as_ref().unwrap().action,
                match outcome {
                    VoiceSessionOutcome::NoSpeech => VoiceFinalizeUiAction::ShowNoSpeechError,
                    VoiceSessionOutcome::Failed => VoiceFinalizeUiAction::ShowFinalizeError,
                    _ => VoiceFinalizeUiAction::ClearFinalizing,
                }
            );
            assert_eq!(
                core.composer_intent(ComposerIntent::CommitVoiceCapture { identity })
                    .outcome(),
                ClientTransitionOutcome::Noop
            );
        }
    }

    #[test]
    fn late_start_response_cancels_exact_session_and_preserves_replacement_draft() {
        let (core, identity, thread) = fixture();
        let transport = Transport {
            thread,
            calls: Default::default(),
            on_start: Box::new(|| {
                core.composer_intent(ComposerIntent::Clear {
                    thread_id: "thread".into(),
                    draft_id: identity.draft_id,
                });
            }),
            fail_finalize: false,
        };
        assert!(
            core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
                .is_err()
        );
        let current = core.composer_snapshot("thread").unwrap();
        assert_ne!(current.draft_id(), identity.draft_id);
        assert!(current.operation().is_none());
        assert_eq!(transport.calls.borrow().len(), 3);
        assert_eq!(
            transport.calls.borrow()[2]["params"]["session_id"],
            "session"
        );
        assert!(!core.composer_voice_session_matches(&identity, "session"));
        assert!(
            core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
                .is_err()
        );
        assert!(Arc::ptr_eq(
            &current,
            &core.composer_snapshot("thread").unwrap()
        ));
        assert_eq!(transport.calls.borrow().len(), 3);
    }

    #[test]
    fn finalize_failure_cleans_session_and_keeps_draft_without_retry() {
        let (core, identity, thread) = fixture();
        let transport = Transport {
            thread,
            calls: Default::default(),
            on_start: Box::new(|| {}),
            fail_finalize: true,
        };
        core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
            .unwrap();
        prepare(&core, &identity);
        let request = ComposerVoiceFinalizeRequest {
            operation: identity.clone(),
        };
        assert!(
            core.finalize_composer_voice_session_using(request.clone(), &transport)
                .is_err()
        );
        let current = core.composer_snapshot("thread").unwrap();
        assert_eq!(current.draft().text, "preserved draft");
        assert!(matches!(
            current.operation().unwrap().status,
            ComposerOperationStatus::Failed { .. }
        ));
        assert!(
            core.finalize_composer_voice_session_using(request, &transport)
                .is_err()
        );
        assert_eq!(transport.calls.borrow().len(), 4);
        assert_eq!(
            transport.calls.borrow()[3]["params"]["session_id"],
            "session"
        );
        assert!(!core.complete_composer_operation(identity, ComposerOperationCompletion::Sent));
        assert!(Arc::ptr_eq(
            &current,
            &core.composer_snapshot("thread").unwrap()
        ));
    }
    #[test]
    fn terminal_operation_and_scope_retirement_schedule_exact_session_without_a_native_handle() {
        for terminal in [
            ComposerOperationCompletion::Cancelled,
            ComposerOperationCompletion::Failed {
                message: "native input failed".into(),
            },
        ] {
            let (core, identity, thread) = fixture();
            let transport = Transport {
                thread,
                calls: Default::default(),
                on_start: Box::new(|| {}),
                fail_finalize: false,
            };
            core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
                .unwrap();
            core.complete_composer_operation(identity.clone(), terminal.clone());
            let cleanup = core.pending_voice_cleanup().unwrap();
            assert_eq!(cleanup.operation, identity);
            core.cancel_composer_voice_session_using(cleanup, &transport)
                .unwrap();
            assert!(core.pending_voice_cleanup().is_none());
            let input = core.composer_snapshot("thread").unwrap();
            assert_eq!(input.draft().text, "preserved draft");
            match terminal {
                ComposerOperationCompletion::Failed { message } => assert_eq!(
                    input.operation().unwrap().status,
                    ComposerOperationStatus::Failed { message }
                ),
                _ => assert_eq!(
                    input.operation().unwrap().status,
                    ComposerOperationStatus::Cancelled
                ),
            }
        }
        let (core, identity, thread) = fixture();
        let transport = Transport {
            thread,
            calls: Default::default(),
            on_start: Box::new(|| {}),
            fail_finalize: false,
        };
        core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
            .unwrap();
        core.cancel_composer_requests_for_thread("thread");
        assert_eq!(core.pending_voice_cleanup().unwrap().operation, identity);
    }
    #[test]
    fn successful_voice_and_gateway_result_release_cleanup_capacity_without_cancel() {
        let (core, identity, thread) = fixture();
        let transport = Transport {
            thread,
            calls: Default::default(),
            on_start: Box::new(|| {}),
            fail_finalize: false,
        };
        core.start_composer_voice_session_using(start_request(identity.clone()), &transport)
            .unwrap();
        core.retire_voice_operation(&identity, true);
        assert!(
            core.composer_store
                .lock()
                .unwrap()
                .voice_sessions
                .is_empty()
        );
        assert!(core.pending_voice_cleanup().is_none());
    }
}
