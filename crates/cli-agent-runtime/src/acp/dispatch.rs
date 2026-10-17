use super::{
    ProtocolState, schema,
    transport::{CallbackControl, Incoming, RequestId, Sender, Transport},
    value,
};
use agent_client_protocol_schema::ProtocolVersion;
use async_trait::async_trait;
use schema::*;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub type CallbackRequest = AgentRequest;

/// The Gateway session owner implements this boundary. Each instance must bind
/// to one immutable principal/workspace/process generation. Capability flags
/// describe the handlers available under that authority, not agent identity.
#[async_trait]
pub trait Host: Send + Sync + 'static {
    fn capabilities(&self) -> ClientCapabilities;
    async fn request(
        &self,
        id: RequestId,
        request: CallbackRequest,
        cancelled: CancellationToken,
    ) -> Result<Value>;
    /// Commit/project ordered typed updates into the existing hub. Waiting
    /// for human interaction belongs in request(), never in this callback.
    async fn update(&self, update: SessionNotification) -> Result<()>;
    async fn elicitation_complete(
        &self,
        notification: CompleteElicitationNotification,
    ) -> Result<()>;
    /// This barrier runs in the same ingress as session/update, before the
    /// request waiter is released. Only this response ends a prompt.
    async fn prompt_response(
        &self,
        session_id: SessionId,
        request_id: RequestId,
        response: Result<PromptResponse>,
    ) -> Result<()>;
    /// Release the exact session's scoped FS/terminal/UI resources after a
    /// successful standard close/delete. This explicit stop is distinct from
    /// load/resume, which can never establish an interrupted prompt outcome.
    async fn session_closed(&self, session_id: SessionId) -> Result<()>;
    async fn disconnected(&self);
    async fn request_cancelled(&self, _id: &RequestId) -> Result<()> {
        Ok(())
    }
}

pub(crate) fn cancel_permissions(sender: &Sender, session_id: &SessionId) -> Result<()> {
    sender.cancel_permissions(&session_id.to_string());
    Ok(())
}

pub(crate) async fn run(
    mut transport: Transport,
    host: Arc<dyn Host>,
    state: Arc<Mutex<ProtocolState>>,
    capabilities: ClientCapabilities,
    max_callbacks: usize,
) {
    let sender = transport.sender.clone();
    let shutdown = sender.cancellation();
    let mut callbacks = JoinSet::new();
    let url_elicitations = Arc::new(Mutex::new(HashSet::new()));
    loop {
        let incoming = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            done = callbacks.join_next(), if !callbacks.is_empty() => {
                if done.is_some_and(|result| result.is_err()) { break; }
                continue;
            },
            incoming = transport.incoming.recv() => match incoming { Some(incoming) => incoming, None => break },
        };
        let handled = match incoming {
            Incoming::InvalidResponse => break,
            Incoming::Invalid { id, error } => sender.invalid(id, error),
            Incoming::Notification { method, params } => {
                match method.as_str() {
                    "$/cancel_request" => {
                        if let Some(id) = params
                            .get("requestId")
                            .and_then(|id| serde_json::from_value(id.clone()).ok())
                        {
                            sender.cancel_callback(&id);
                        }
                        Ok(())
                    }
                    "session/update" => match session_notification(params) {
                        Ok(Some(notification)) => {
                            let applied = apply_update(&state, &notification, &capabilities);
                            match applied {
                                Ok(()) => host.update(notification).await,
                                Err(e) => Err(e),
                            }
                        }
                        Ok(None) => Ok(()),
                        Err(error) => Err(error),
                    },
                    "elicitation/complete" => {
                        match parse::<CompleteElicitationNotification>(params) {
                            Ok(notification)
                                if url_elicitations
                                    .lock()
                                    .expect("ACP URL state poisoned")
                                    .remove(&notification.elicitation_id.to_string()) =>
                            {
                                host.elicitation_complete(notification).await
                            }
                            Ok(_) => Ok(()),
                            Err(error) => Err(error),
                        }
                    }
                    // Extensions never dispatch business logic or generate a response.
                    _ => Ok(()),
                }
            }
            Incoming::Request { id, method, params } => {
                let parsed = parse_callback(&method, params, &capabilities);
                match parsed {
                    Err(error) => sender.respond(&id, Err(error)),
                    Ok(_) if callbacks.len() >= max_callbacks => sender.respond(
                        &id,
                        Err(Error::new(-32603, "ACP callback capacity exceeded")),
                    ),
                    Ok(request) => {
                        let (parent_request, url_id) = match &request {
                            AgentRequest::CreateElicitationRequest(request) => {
                                let parent = match request.scope() {
                                    ElicitationScope::Request(scope) => Some(
                                        serde_json::from_value(
                                            value(&scope.request_id).unwrap_or(Value::Null),
                                        )
                                        .ok(),
                                    ),
                                    _ => None,
                                };
                                if parent.as_ref().is_some_and(|id| {
                                    id.as_ref()
                                        .is_none_or(|id| sender.pending_method(id).is_none())
                                }) {
                                    if sender.respond(&id, Err(Error::invalid_params())).is_err() {
                                        break;
                                    }
                                    continue;
                                }
                                let url = match &request.mode {
                                    ElicitationMode::Url(mode) => {
                                        Some(mode.elicitation_id.to_string())
                                    }
                                    _ => None,
                                };
                                if url.as_ref().is_some_and(|id| {
                                    let urls =
                                        url_elicitations.lock().expect("ACP URL state poisoned");
                                    urls.contains(id) || urls.len() >= max_callbacks
                                }) {
                                    if sender.respond(&id, Err(Error::invalid_params())).is_err() {
                                        break;
                                    }
                                    continue;
                                }
                                (parent.flatten(), url)
                            }
                            _ => (None, None),
                        };
                        let control = CallbackControl {
                            session: callback_session(&request),
                            permission: matches!(
                                &request,
                                AgentRequest::RequestPermissionRequest(_)
                            ),
                            token: shutdown.child_token(),
                            parent_request,
                        };
                        let cancelled = control.token.clone();
                        if let Err(error) = sender.register_callback(id.clone(), control) {
                            // Reused active IDs make response correlation ambiguous.
                            let _ = error;
                            break;
                        }
                        let host = host.clone();
                        if let Some(id) = &url_id {
                            url_elicitations
                                .lock()
                                .expect("ACP URL state poisoned")
                                .insert(id.clone());
                        }
                        let sender = sender.clone();
                        let urls = url_elicitations.clone();
                        callbacks.spawn(async move {
                            let result = tokio::select! {
                                biased;
                                _ = cancelled.cancelled() => {
                                    host.request_cancelled(&id).await.and_then(|()| cancelled_result(&request))
                                },
                                result = host.request(id.clone(), request.clone(), cancelled.clone()) => result.and_then(|result| validate_response(&request, result)),
                            };
                            let accepted = sender.respond_callback(&id, result, cancelled_result(&request));
                            if let Some(url_id) = url_id
                                && !accepted.as_ref().is_ok_and(|accepted| *accepted) { urls.lock().expect("ACP URL state poisoned").remove(&url_id); }
                            if accepted.is_err() { sender.close(); }
                        });
                        Ok(())
                    }
                }
            }
            Incoming::Response { id, result } => {
                if let Some((method, params)) = sender.pending_context(&id) {
                    let response_was_success = result.is_ok();
                    let result = result.and_then(|value| {
                        apply_response(&state, &method, &params, &value, &capabilities)?;
                        Ok(value)
                    });
                    if matches!(
                        method.as_str(),
                        "session/load"
                            | "session/resume"
                            | "session/delete"
                            | "session/close"
                            | "session/set_config_option"
                            | "session/set_mode"
                    ) && let Some(id) = params.get("sessionId").and_then(Value::as_str)
                    {
                        state
                            .lock()
                            .expect("ACP state poisoned")
                            .pending_configuration
                            .remove(id);
                    }
                    let projected = if method == "session/prompt" {
                        match parse::<PromptRequest>(params) {
                            Ok(request) => {
                                host.prompt_response(
                                    request.session_id,
                                    id.clone(),
                                    result.clone().and_then(parse),
                                )
                                .await
                            }
                            Err(error) => Err(error),
                        }
                    } else if matches!(method.as_str(), "session/close" | "session/delete")
                        && result.is_ok()
                    {
                        match params.get("sessionId").and_then(Value::as_str) {
                            Some(id) => {
                                sender.cancel_session(id);
                                host.session_closed(SessionId::new(id)).await
                            }
                            None => Err(Error::invalid_params()),
                        }
                    } else if (method == "initialize" || response_was_success) && result.is_err() {
                        Err(result.clone().unwrap_err())
                    } else {
                        Ok(())
                    };
                    match projected {
                        Ok(()) => {
                            if method == "session/prompt"
                                && let Some(session) =
                                    sender.pending_context(&id).and_then(|(_, params)| {
                                        params
                                            .get("sessionId")
                                            .and_then(Value::as_str)
                                            .map(str::to_owned)
                                    })
                            {
                                finish_prompt(&state, &session, &id);
                            }
                            sender.complete(&id, result);
                            Ok(())
                        }
                        Err(error) => {
                            sender.complete(&id, Err(error.clone()));
                            Err(error)
                        }
                    }
                } else {
                    // A cancelled/dropped request may have a late response. It
                    // cannot resolve a different generation or request.
                    Ok(())
                }
            }
        };
        if handled.is_err() {
            break;
        }
    }
    sender.close();
    // Cancelling drops each owned host future and its scoped request resources.
    callbacks.abort_all();
    while callbacks.join_next().await.is_some() {}
}

fn parse<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|_| Error::invalid_params())
}

fn session_notification(params: Value) -> Result<Option<SessionNotification>> {
    if let Some(kind) = params
        .pointer("/update/sessionUpdate")
        .and_then(Value::as_str)
        && !matches!(
            kind,
            "user_message_chunk"
                | "agent_message_chunk"
                | "agent_thought_chunk"
                | "tool_call"
                | "tool_call_update"
                | "plan"
                | "available_commands_update"
                | "current_mode_update"
                | "config_option_update"
                | "session_info_update"
                | "usage_update"
                | "notice"
                | "compaction_update"
                | "compaction_summary_chunk"
        )
    {
        // Unknown additive updates have no authority and no business effects.
        return Ok(None);
    }
    parse(params).map(Some)
}

fn parse_callback(
    method: &str,
    params: Value,
    capabilities: &ClientCapabilities,
) -> Result<AgentRequest> {
    match method {
        "session/request_permission" => parse(params).map(AgentRequest::RequestPermissionRequest),
        "fs/read_text_file" if capabilities.fs.read_text_file => {
            parse::<ReadTextFileRequest>(params).and_then(|request| {
                if !request.path.is_absolute() || request.line == Some(0) {
                    return Err(Error::invalid_params());
                }
                Ok(AgentRequest::ReadTextFileRequest(request))
            })
        }
        "fs/write_text_file" if capabilities.fs.write_text_file => {
            parse::<WriteTextFileRequest>(params).and_then(|request| {
                if !request.path.is_absolute() {
                    return Err(Error::invalid_params());
                }
                Ok(AgentRequest::WriteTextFileRequest(request))
            })
        }
        "terminal/create" if capabilities.terminal => parse::<CreateTerminalRequest>(params)
            .and_then(|request| {
                if request.cwd.as_ref().is_some_and(|path| !path.is_absolute()) {
                    return Err(Error::invalid_params());
                }
                Ok(AgentRequest::CreateTerminalRequest(request))
            }),
        "terminal/output" if capabilities.terminal => {
            parse(params).map(AgentRequest::TerminalOutputRequest)
        }
        "terminal/wait_for_exit" if capabilities.terminal => {
            parse(params).map(AgentRequest::WaitForTerminalExitRequest)
        }
        "terminal/kill" if capabilities.terminal => {
            parse(params).map(AgentRequest::KillTerminalRequest)
        }
        "terminal/release" if capabilities.terminal => {
            parse(params).map(AgentRequest::ReleaseTerminalRequest)
        }
        "elicitation/create" => {
            if params.get("sessionId").is_some() == params.get("requestId").is_some() {
                return Err(Error::invalid_params());
            }
            let request: CreateElicitationRequest = parse(params)?;
            let supported = match &request.mode {
                ElicitationMode::Form(_) => capabilities
                    .elicitation
                    .as_ref()
                    .is_some_and(|e| e.supports_form()),
                ElicitationMode::Url(_) => capabilities
                    .elicitation
                    .as_ref()
                    .is_some_and(|e| e.supports_url()),
                _ => false,
            };
            if !supported {
                return Err(Error::invalid_params());
            }
            super::elicitation::validate_request(&request)?;
            Ok(AgentRequest::CreateElicitationRequest(request))
        }
        _ => Err(Error::method_not_found()),
    }
}

fn callback_session(request: &AgentRequest) -> Option<String> {
    let session = match request {
        AgentRequest::RequestPermissionRequest(r) => &r.session_id,
        AgentRequest::ReadTextFileRequest(r) => &r.session_id,
        AgentRequest::WriteTextFileRequest(r) => &r.session_id,
        AgentRequest::CreateTerminalRequest(r) => &r.session_id,
        AgentRequest::TerminalOutputRequest(r) => &r.session_id,
        AgentRequest::WaitForTerminalExitRequest(r) => &r.session_id,
        AgentRequest::KillTerminalRequest(r) => &r.session_id,
        AgentRequest::ReleaseTerminalRequest(r) => &r.session_id,
        AgentRequest::CreateElicitationRequest(r) => match r.scope() {
            ElicitationScope::Session(scope) => &scope.session_id,
            _ => return None,
        },
        _ => return None,
    };
    Some(session.to_string())
}

fn cancelled_result(request: &AgentRequest) -> Result<Value> {
    match request {
        AgentRequest::RequestPermissionRequest(_) => value(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        )),
        AgentRequest::CreateElicitationRequest(_) => {
            value(CreateElicitationResponse::new(ElicitationAction::Cancel))
        }
        _ => Err(Error::request_cancelled()),
    }
}

pub fn validate_response(request: &AgentRequest, response: Value) -> Result<Value> {
    match request {
        AgentRequest::RequestPermissionRequest(request) => {
            let parsed: RequestPermissionResponse = parse(response.clone())?;
            match parsed.outcome {
                RequestPermissionOutcome::Selected(selected)
                    if request
                        .options
                        .iter()
                        .any(|o| o.option_id == selected.option_id) => {}
                RequestPermissionOutcome::Cancelled => {}
                _ => return Err(Error::invalid_params()),
            }
        }
        AgentRequest::ReadTextFileRequest(_) => {
            let _: ReadTextFileResponse = parse(response.clone())?;
        }
        AgentRequest::WriteTextFileRequest(_) => {
            let _: WriteTextFileResponse = parse(response.clone())?;
        }
        AgentRequest::CreateTerminalRequest(_) => {
            let _: CreateTerminalResponse = parse(response.clone())?;
        }
        AgentRequest::TerminalOutputRequest(_) => {
            let _: TerminalOutputResponse = parse(response.clone())?;
        }
        AgentRequest::WaitForTerminalExitRequest(_) => {
            let _: WaitForTerminalExitResponse = parse(response.clone())?;
        }
        AgentRequest::KillTerminalRequest(_) => {
            let _: KillTerminalResponse = parse(response.clone())?;
        }
        AgentRequest::ReleaseTerminalRequest(_) => {
            let _: ReleaseTerminalResponse = parse(response.clone())?;
        }
        AgentRequest::CreateElicitationRequest(request) => {
            let parsed: CreateElicitationResponse = parse(response.clone())?;
            super::elicitation::validate_response(request, &parsed)?;
        }
        _ => return Err(Error::method_not_found()),
    }
    Ok(response)
}

fn apply_update(
    state: &Mutex<ProtocolState>,
    notification: &SessionNotification,
    capabilities: &ClientCapabilities,
) -> Result<()> {
    let mut state = state.lock().expect("ACP state poisoned");
    match &notification.update {
        SessionUpdate::ConfigOptionUpdate(update) => {
            check_boolean_options(&update.config_options, capabilities)?;
            let revision = state.next_revision()?;
            if let Some(snapshot) = state
                .configurations
                .get_mut(&notification.session_id.to_string())
            {
                snapshot.update(update.config_options.clone(), revision)?;
            }
        }
        SessionUpdate::CurrentModeUpdate(update) => {
            let revision = state.next_revision()?;
            if let Some(snapshot) = state
                .configurations
                .get_mut(&notification.session_id.to_string())
            {
                snapshot.revision = revision;
                if let Some(modes) = &mut snapshot.modes {
                    modes.current_mode_id = update.current_mode_id.clone();
                }
            }
        }
        SessionUpdate::Notice(_)
            if capabilities
                .session
                .as_ref()
                .and_then(|s| s.notices.as_ref())
                .is_none() =>
        {
            return Err(Error::invalid_request());
        }
        SessionUpdate::CompactionUpdate(_) | SessionUpdate::CompactionSummaryChunk(_)
            if capabilities
                .session
                .as_ref()
                .and_then(|s| s.compaction.as_ref())
                .is_none() =>
        {
            return Err(Error::invalid_request());
        }
        _ => {}
    }
    Ok(())
}

fn check_boolean_options(
    options: &[SessionConfigOption],
    capabilities: &ClientCapabilities,
) -> Result<()> {
    if options
        .iter()
        .any(|o| matches!(o.kind, SessionConfigKind::Boolean(_)))
        && capabilities
            .session
            .as_ref()
            .and_then(|s| s.config_options.as_ref())
            .and_then(|s| s.boolean.as_ref())
            .is_none()
    {
        return Err(Error::invalid_request());
    }
    Ok(())
}

fn apply_response(
    state: &Mutex<ProtocolState>,
    method: &str,
    params: &Value,
    response: &Value,
    capabilities: &ClientCapabilities,
) -> Result<()> {
    let mut state = state.lock().expect("ACP state poisoned");
    match method {
        "initialize" => {
            let response: InitializeResponse = parse(response.clone())?;
            if response.protocol_version != ProtocolVersion::V1 {
                return Err(Error::new(-32603, "Unsupported ACP protocol version"));
            }
            state.initialized = Some(response);
        }
        "session/new" | "session/load" | "session/resume" => {
            let (id, options, modes) = if method == "session/new" {
                let response: NewSessionResponse = parse(response.clone())?;
                if state
                    .configurations
                    .contains_key(&response.session_id.to_string())
                {
                    return Err(Error::invalid_request());
                }
                (
                    response.session_id.to_string(),
                    response.config_options,
                    response.modes,
                )
            } else {
                let id = params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .ok_or_else(Error::invalid_params)?
                    .to_owned();
                if method == "session/load" {
                    let response: LoadSessionResponse = parse(response.clone())?;
                    (id, response.config_options, response.modes)
                } else {
                    let response: ResumeSessionResponse = parse(response.clone())?;
                    (id, response.config_options, response.modes)
                }
            };
            if !state.configurations.contains_key(&id) && state.configurations.len() >= 32 {
                return Err(Error::new(-32603, "ACP session capacity exceeded"));
            }
            check_boolean_options(options.as_deref().unwrap_or_default(), capabilities)?;
            let mut snapshot = state.configurations.get(&id).cloned().unwrap_or_default();
            snapshot.connection_id = state.connection_id;
            snapshot.replace(options, modes, state.next_revision()?)?;
            state.configurations.insert(id, snapshot);
        }
        "session/set_config_option" => {
            let response: SetSessionConfigOptionResponse = parse(response.clone())?;
            let revision = state.next_revision()?;
            check_boolean_options(&response.config_options, capabilities)?;
            let id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .ok_or_else(Error::invalid_params)?;
            state
                .configurations
                .get_mut(id)
                .ok_or_else(Error::invalid_params)?
                .update(response.config_options, revision)?;
        }
        "session/set_mode" => {
            let _: SetSessionModeResponse = parse(response.clone())?;
            let revision = state.next_revision()?;
            let id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .ok_or_else(Error::invalid_params)?;
            let mode = params
                .get("modeId")
                .and_then(Value::as_str)
                .ok_or_else(Error::invalid_params)?;
            if let Some(snapshot) = state.configurations.get_mut(id) {
                snapshot.revision = revision;
                if let Some(modes) = &mut snapshot.modes {
                    modes.current_mode_id = SessionModeId::new(mode);
                }
            }
        }
        "session/delete" | "session/close" => {
            let _: CloseSessionResponse = parse(response.clone())?;
            if let Some(id) = params.get("sessionId").and_then(Value::as_str) {
                state.configurations.remove(id);
                state.active_prompts.remove(id);
            }
        }
        "session/prompt" => {
            let _: PromptResponse = parse(response.clone())?;
        }
        "authenticate" => {
            let _: AuthenticateResponse = parse(response.clone())?;
        }
        "logout" => {
            let _: LogoutResponse = parse(response.clone())?;
        }
        "session/list" => {
            let _: ListSessionsResponse = parse(response.clone())?;
        }
        _ => {}
    }
    Ok(())
}

// A response from an explicitly closed prompt may arrive after the same opaque
// session ID was loaded again. It cannot clear the new prompt's busy fence.
fn finish_prompt(state: &Mutex<ProtocolState>, session: &str, request: &RequestId) {
    let mut state = state.lock().expect("ACP state poisoned");
    if state.active_prompts.get(session) == Some(request) {
        state.active_prompts.remove(session);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::AsyncWriteExt;

    struct RecordingHost {
        order: Mutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl Host for RecordingHost {
        fn capabilities(&self) -> ClientCapabilities {
            ClientCapabilities::new()
        }
        async fn request(
            &self,
            _: RequestId,
            _: CallbackRequest,
            _: CancellationToken,
        ) -> Result<Value> {
            std::future::pending().await
        }
        async fn update(&self, _: SessionNotification) -> Result<()> {
            self.order.lock().unwrap().push("update");
            Ok(())
        }
        async fn elicitation_complete(&self, _: CompleteElicitationNotification) -> Result<()> {
            Ok(())
        }
        async fn prompt_response(
            &self,
            _: SessionId,
            _: RequestId,
            response: Result<PromptResponse>,
        ) -> Result<()> {
            response?;
            self.order.lock().unwrap().push("terminal");
            Ok(())
        }
        async fn disconnected(&self) {
            self.order.lock().unwrap().push("disconnect");
        }
        async fn session_closed(&self, _: SessionId) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn permission_wait_does_not_block_updates_or_prompt_completion() {
        let (client_read, mut agent_write) = tokio::io::duplex(4096);
        let (_agent_read, client_write) = tokio::io::duplex(4096);
        let transport = Transport::connect(
            client_read,
            client_write,
            super::super::transport::TransportLimits::default(),
        )
        .unwrap();
        let sender = transport.sender.clone();
        let pending = sender
            .request("session/prompt", &PromptRequest::new("s", Vec::new()))
            .unwrap();
        let host = Arc::new(RecordingHost {
            order: Mutex::new(Vec::new()),
        });
        let driver_host = host.clone();
        let driver = tokio::spawn(async move {
            run(
                transport,
                driver_host.clone(),
                Arc::new(Mutex::new(ProtocolState::default())),
                driver_host.capabilities(),
                8,
            )
            .await;
            driver_host.disconnected().await;
        });
        for frame in [
            json!({"jsonrpc":"2.0","id":"permission","method":"session/request_permission","params":{"sessionId":"s","toolCall":{"toolCallId":"t"},"options":[{"optionId":"opaque","name":"Allow","kind":"allow_once"}]}}),
            json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"answer"}}}}),
            json!({"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}),
        ] {
            let bytes = format!("{frame}\n");
            agent_write.write_all(bytes.as_bytes()).await.unwrap();
        }
        drop(agent_write);
        pending.receive().await.unwrap();
        driver.await.unwrap();
        assert_eq!(
            *host.order.lock().unwrap(),
            vec!["update", "terminal", "disconnect"]
        );
    }

    #[test]
    fn permission_response_uses_declared_opaque_option_id() {
        let request = AgentRequest::RequestPermissionRequest(serde_json::from_value(json!({"sessionId":"s","toolCall":{"toolCallId":"t"},"options":[{"optionId":"strange/id","name":"Allow","kind":"allow_once"}]})).unwrap());
        assert!(
            validate_response(
                &request,
                json!({"outcome":{"outcome":"selected","optionId":"allow"}})
            )
            .is_err()
        );
        assert!(
            validate_response(
                &request,
                json!({"outcome":{"outcome":"selected","optionId":"strange/id"}})
            )
            .is_ok()
        );
        assert_eq!(
            cancelled_result(&request).unwrap(),
            json!({"outcome":{"outcome":"cancelled"}})
        );
    }

    #[test]
    fn optional_callbacks_and_url_modes_require_exact_capability() {
        let capabilities = ClientCapabilities::new();
        assert!(
            parse_callback(
                "fs/read_text_file",
                json!({"sessionId":"s","path":"/tmp/a"}),
                &capabilities
            )
            .is_err()
        );
        let error = parse_callback("elicitation/create", json!({"mode":"url","requestId":1,"message":"Login","elicitationId":"opaque","url":"https://example.test"}), &capabilities).unwrap_err();
        assert_eq!(i32::from(error.code), -32602);
        let error = parse_callback("vendor/private", json!({}), &capabilities).unwrap_err();
        assert_eq!(i32::from(error.code), -32601);
        assert!(
            session_notification(
                json!({"sessionId":"s","update":{"sessionUpdate":"future_update"}})
            )
            .unwrap()
            .is_none()
        );
        assert!(
            session_notification(
                json!({"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk"}})
            )
            .is_err()
        );
    }

    #[test]
    fn config_version_cannot_alias_after_close_load_or_connection_replacement() {
        let state = Mutex::new(ProtocolState::default());
        let caps = ClientCapabilities::new();
        apply_response(
            &state,
            "session/new",
            &json!({}),
            &json!({"sessionId":"s","configOptions":[]}),
            &caps,
        )
        .unwrap();
        let first = state.lock().unwrap().configurations["s"].version();
        apply_response(
            &state,
            "session/close",
            &json!({"sessionId":"s"}),
            &json!({}),
            &caps,
        )
        .unwrap();
        apply_response(
            &state,
            "session/load",
            &json!({"sessionId":"s"}),
            &json!({"configOptions":[]}),
            &caps,
        )
        .unwrap();
        let loaded = state.lock().unwrap().configurations["s"].version();
        assert_eq!(first.connection_id, loaded.connection_id);
        assert_ne!(first.revision, loaded.revision);
        let replacement = Mutex::new(ProtocolState::default());
        apply_response(
            &replacement,
            "session/new",
            &json!({}),
            &json!({"sessionId":"s","configOptions":[]}),
            &caps,
        )
        .unwrap();
        assert_ne!(
            first,
            replacement.lock().unwrap().configurations["s"].version()
        );
    }
    #[test]
    fn closed_prompt_response_cannot_clear_reloaded_sessions_new_prompt() {
        let state = Mutex::new(ProtocolState::default());
        let old = RequestId::Number(1);
        let new = RequestId::Number(2);
        state
            .lock()
            .unwrap()
            .active_prompts
            .insert("same".into(), old.clone());
        apply_response(
            &state,
            "session/close",
            &json!({"sessionId":"same"}),
            &json!({}),
            &ClientCapabilities::new(),
        )
        .unwrap();
        assert!(!state.lock().unwrap().active_prompts.contains_key("same"));
        state
            .lock()
            .unwrap()
            .active_prompts
            .insert("same".into(), new.clone());
        finish_prompt(&state, "same", &old);
        assert_eq!(state.lock().unwrap().active_prompts.get("same"), Some(&new));
        finish_prompt(&state, "same", &new);
        assert!(state.lock().unwrap().active_prompts.is_empty());
    }
}
