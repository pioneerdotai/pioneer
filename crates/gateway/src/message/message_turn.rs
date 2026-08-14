use super::dispatch::RequestAdmission;
use super::*;
use crate::authorization::{RuntimeDraftCreator, RuntimeDraftMaterialization};
use pioneer_protocol::{
    UserInput, VoiceError, VoiceErrorKind, VoiceSessionOutcome, VoiceSessionResultNotification,
};

/// Selects how the Message leaf reports its result. Normal `turn/start`
/// requests receive JSON-RPC responses; voice transcription uses the existing
/// voice-session notification channel while committing the exact same Message
/// turn projection.
pub(super) enum MessageTurnResponse {
    JsonRpc {
        request_id: RequestId,
    },
    Voice {
        session_id: String,
        thread_id: String,
        turn_id: String,
    },
}

impl MessageTurnResponse {
    async fn send_error(
        &self,
        processor: &MessageProcessor,
        connection_id: ConnectionId,
        code: i64,
        public_code: pioneer_protocol::PublicErrorCode,
        message: impl Into<String>,
    ) {
        let public_error = crate::public_error::map_agent_failure(
            public_code,
            pioneer_protocol::PublicErrorStage::Admission,
            message.into(),
        );
        match self {
            Self::JsonRpc { request_id } => {
                processor
                    .send_error(
                        connection_id,
                        JsonRpcErrorResponse {
                            jsonrpc: pioneer_protocol::JSONRPC_VERSION.to_owned(),
                            id: Some(request_id.clone()),
                            error: pioneer_protocol::JsonRpcError {
                                code,
                                message: public_error.message.clone(),
                                data: Some(serde_json::json!({ "public_error": public_error })),
                            },
                        },
                    )
                    .await;
            }
            Self::Voice {
                session_id,
                thread_id,
                turn_id,
            } => {
                processor
                    .send_voice_session_result_notification(
                        connection_id,
                        thread_id,
                        VoiceSessionResultNotification {
                            session_id: session_id.clone(),
                            outcome: VoiceSessionOutcome::Failed,
                            turn_id: Some(turn_id.clone()),
                            error: Some(VoiceError {
                                kind: VoiceErrorKind::Unknown,
                                message: public_error.message.clone(),
                                public_error: Some(public_error),
                            }),
                        },
                    )
                    .await;
            }
        }
    }

    async fn send_success(
        &self,
        processor: &MessageProcessor,
        connection_id: ConnectionId,
        response: &pioneer_protocol::TurnStartResponse,
    ) {
        match self {
            Self::JsonRpc { request_id } => {
                let _ = send_message_turn_response(
                    processor,
                    connection_id,
                    request_id.clone(),
                    response,
                )
                .await;
            }
            Self::Voice {
                session_id,
                thread_id,
                ..
            } => {
                processor
                    .send_voice_session_result_notification(
                        connection_id,
                        thread_id,
                        VoiceSessionResultNotification {
                            session_id: session_id.clone(),
                            outcome: VoiceSessionOutcome::TurnStarted,
                            turn_id: Some(response.turn.id.clone()),
                            error: None,
                        },
                    )
                    .await;
            }
        }
    }

    async fn send_conflict(&self, processor: &MessageProcessor, connection_id: ConnectionId) {
        self.send_error(
            processor,
            connection_id,
            INVALID_REQUEST_CODE,
            pioneer_protocol::PublicErrorCode::Conflict,
            "turn_id is already used by a different request",
        )
        .await;
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct MessageInputMetrics {
    pub input_bytes: usize,
    pub attachment_count: usize,
}

pub(super) fn message_input_metrics(input: &[UserInput]) -> MessageInputMetrics {
    MessageInputMetrics {
        input_bytes: serde_json::to_vec(input).map_or(0, |bytes| bytes.len()),
        attachment_count: input
            .iter()
            .filter(|value| matches!(value, UserInput::Artifact { .. }))
            .count(),
    }
}

fn log_message_turn_outcome(
    params: &TurnStartParams,
    metrics: MessageInputMetrics,
    outcome: &'static str,
    revision: u64,
    commit_latency_ms: u128,
    operation_latency_ms: u128,
) {
    debug!(
        thread_id = params.thread_id.as_str(),
        turn_id = params.turn_id.as_str(),
        mode = "message",
        input_bytes = metrics.input_bytes,
        attachment_count = metrics.attachment_count,
        revision,
        outcome,
        commit_latency_ms,
        operation_latency_ms,
        "Message turn/start outcome"
    );
}

pub(super) const fn effective_turn_mode(
    requested: Option<pioneer_protocol::ThreadMode>,
    thread_default: pioneer_protocol::ThreadMode,
) -> pioneer_protocol::ThreadMode {
    match requested {
        Some(mode) => mode,
        None => thread_default,
    }
}

pub(super) fn contains_client_author_snapshot(params: &JsonValue) -> bool {
    params.as_object().is_some_and(|object| {
        object.keys().any(|key| {
            let normalized = key
                .chars()
                .filter(|character| character.is_ascii_alphanumeric())
                .flat_map(char::to_lowercase)
                .collect::<String>();
            (normalized.starts_with("author") && !normalized.starts_with("authorization"))
                || normalized.starts_with("initiatedbyactor")
        })
    })
}

pub(super) fn validate_message_content(
    input: &[UserInput],
    mention_count: usize,
) -> anyhow::Result<()> {
    pioneer_protocol::validate_turn_message_content(input, mention_count)
        .map_err(anyhow::Error::msg)
}

fn validate_message_turn_params(
    params: &TurnStartParams,
    client_author_override: bool,
) -> anyhow::Result<()> {
    if params.mode != Some(pioneer_protocol::ThreadMode::Message) {
        anyhow::bail!("Message turn requires explicit effective Message mode");
    }
    if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
        anyhow::bail!("`thread_id` and `turn_id` are required");
    }
    if client_author_override {
        anyhow::bail!("Message author is server-owned");
    }
    if !params.capabilities.is_empty()
        || params.model.is_some()
        || params.model_provider.is_some()
        || params.sandbox_policy.is_some()
        || params.execution_backend.is_some()
        || params.reasoning.is_some()
        || params.permission_profile.is_some()
        || params.cli_runtime_options.is_some()
    {
        anyhow::bail!("Message does not accept execution-only fields");
    }
    validate_message_content(&params.input, params.mentioned_principal_ids.len())?;
    if params
        .reply_to_turn_id
        .as_deref()
        .is_some_and(|turn_id| turn_id.trim().is_empty())
    {
        anyhow::bail!("Message reply target cannot be empty");
    }
    Ok(())
}

pub(super) fn normalize_turn_collaboration_params(
    params: &mut TurnStartParams,
) -> anyhow::Result<()> {
    if params.mentioned_principal_ids.len() > pioneer_protocol::TURN_MESSAGE_MENTION_MAX_COUNT {
        anyhow::bail!("Turn mentions exceed the item limit");
    }
    if params
        .reply_to_turn_id
        .as_deref()
        .is_some_and(|turn_id| turn_id.trim().is_empty())
    {
        anyhow::bail!("Turn reply target cannot be empty");
    }
    params.thread_id = params.thread_id.trim().to_owned();
    params.turn_id = params.turn_id.trim().to_owned();
    params.reply_to_turn_id = params
        .reply_to_turn_id
        .take()
        .map(|turn_id| turn_id.trim().to_owned());
    params.mentioned_principal_ids.sort();
    params.mentioned_principal_ids.dedup();
    Ok(())
}

pub(super) async fn resolve_turn_author_snapshot(
    crud_store: &pioneer_crud::CrudStore,
    actor: &pioneer_protocol::PersistedActorRef,
) -> anyhow::Result<Option<pioneer_protocol::TurnAuthorSnapshot>> {
    let pioneer_protocol::PersistedActorRef::Principal(principal_id) = actor else {
        return Ok(None);
    };
    let database = crud_store.database_connection();
    let principal = pioneer_crud::load_principal_by_id(&database, principal_id)
        .await
        .context("failed to resolve Turn author principal")?
        .context("authenticated Turn author no longer exists")?;
    if principal.status != pioneer_protocol::PrincipalStatus::Active {
        anyhow::bail!("authenticated Turn author is no longer active");
    }
    let avatar_revision = pioneer_crud::load_principal_avatar(&database, principal_id)
        .await
        .context("failed to resolve Turn author avatar")?
        .map(|avatar| hex::encode(avatar.content_hash));
    Ok(Some(pioneer_protocol::TurnAuthorSnapshot {
        actor: actor.clone(),
        display_name: principal.display_name,
        nickname: principal.nickname,
        avatar_revision,
    }))
}

/// Resolves reply/mention metadata inside the already-authorized Turn scope.
/// The returned values are immutable snapshots stored on the existing Turn;
/// they are not grants and do not create a parallel message model.
pub(super) async fn resolve_turn_collaboration_metadata(
    crud_store: &pioneer_crud::CrudStore,
    actor: &pioneer_protocol::PersistedActorRef,
    params: &TurnStartParams,
) -> anyhow::Result<Vec<pioneer_protocol::TurnMention>> {
    if let Some(reply_to_turn_id) = params.reply_to_turn_id.as_deref() {
        let target = crud_store
            .get_turn(params.thread_id.as_str(), reply_to_turn_id)
            .await
            .context("failed to resolve reply target")?
            .map(|(_, turn)| turn)
            .filter(|turn| {
                turn.turn_kind == pioneer_protocol::TurnKind::Conversation
                    && turn.origin == pioneer_protocol::TurnOrigin::User
            });
        if target.is_none() {
            anyhow::bail!("reply target is unavailable");
        }
    }

    resolve_turn_mentions(crud_store, actor, params.mentioned_principal_ids.as_slice()).await
}

pub(super) async fn resolve_turn_mentions(
    crud_store: &pioneer_crud::CrudStore,
    actor: &pioneer_protocol::PersistedActorRef,
    mentioned_principal_ids: &[pioneer_protocol::PrincipalId],
) -> anyhow::Result<Vec<pioneer_protocol::TurnMention>> {
    if mentioned_principal_ids.is_empty() {
        return Ok(Vec::new());
    }

    let pioneer_protocol::PersistedActorRef::Principal(viewer_principal_id) = actor else {
        anyhow::bail!("system Turn cannot contain mentions");
    };
    let database = crud_store.database_connection();
    let viewer = pioneer_crud::load_principal_by_id(&database, viewer_principal_id)
        .await
        .context("failed to resolve mention viewer")?
        .context("mention viewer is unavailable")?;
    if viewer.status != pioneer_protocol::PrincipalStatus::Active {
        anyhow::bail!("mention viewer is unavailable");
    }
    let viewer_role_key = viewer
        .role_key
        .as_deref()
        .map(pioneer_protocol::RoleKey::new)
        .transpose()
        .context("mention viewer has an invalid role")?;
    let viewer_disclosure = crate::authorization::AuthorizationService::new()
        .role_disclosure_policy(viewer.kind, viewer_role_key.as_ref())
        .context("mention viewer has an unsupported role")?;

    let mut mentions = Vec::with_capacity(mentioned_principal_ids.len());
    for target_principal_id in mentioned_principal_ids {
        let target = pioneer_crud::load_principal_by_id(&database, target_principal_id)
            .await
            .context("failed to resolve mentioned principal")?;
        let target_role_key = target
            .as_ref()
            .and_then(|target| target.role_key.as_deref())
            .map(pioneer_protocol::RoleKey::new)
            .transpose()
            .context("mentioned principal has an invalid role")?;
        let visible = match target.as_ref() {
            Some(target)
                if target.gateway_id == viewer.gateway_id
                    && target.status == pioneer_protocol::PrincipalStatus::Active
                    && crate::authorization::AuthorizationService::new()
                        .resolved_role_key(target.kind, target_role_key.as_ref())
                        .is_some() =>
            {
                match viewer_disclosure {
                    crate::authorization::RoleDisclosurePolicy::Administrative => true,
                    crate::authorization::RoleDisclosurePolicy::Collaborator => {
                        target.id == viewer.id
                            || target.kind == pioneer_protocol::PrincipalKind::Superuser
                            || pioneer_crud::find_shared_workspace_principal_for_principal(
                                &database,
                                &viewer.gateway_id,
                                &viewer.id,
                                target_principal_id,
                            )
                            .await
                            .context("failed to resolve scoped mentioned principal")?
                            .is_some()
                    }
                }
            }
            _ => false,
        };
        let Some(target) = target.filter(|_| visible) else {
            anyhow::bail!("mentioned principal is unavailable");
        };
        mentions.push(pioneer_protocol::TurnMention {
            principal_id: target.id,
            nickname: target.nickname,
        });
    }
    Ok(mentions)
}

async fn persist_completed_message_turn(
    crud_store: &pioneer_crud::CrudStore,
    outcome: &crate::thread::CompletedMessageTurnStartOutcome,
    actor: pioneer_protocol::PersistedActorRef,
    audit_event: pioneer_protocol::TurnPermissionAuditEvent,
    runtime_draft: Option<&RuntimeDraftMaterialization>,
) -> anyhow::Result<()> {
    let write = || pioneer_crud::CompletedMessageTurnWrite {
        thread: &outcome.materialization.thread,
        sandbox_mode: outcome.materialization.sandbox_mode,
        started_turn: &outcome.materialization.turn,
        input: &outcome.materialization.input,
        actor: actor.clone(),
        completed: outcome.completed_notification.clone(),
        audit_event: audit_event.clone(),
    };
    match runtime_draft.map(RuntimeDraftMaterialization::creator) {
        Some(RuntimeDraftCreator::ScopedPrincipal {
            gateway_id,
            principal_id,
            access_class,
        }) => {
            crud_store
                .materialize_new_member_completed_message_turn_with_permission_audit(
                    write(),
                    gateway_id,
                    principal_id,
                    *access_class,
                )
                .await
        }
        Some(RuntimeDraftCreator::Absolute { access_class }) => {
            crud_store
                .materialize_new_superuser_completed_message_turn_with_permission_audit(
                    write(),
                    *access_class,
                )
                .await
        }
        None => {
            crud_store
                .materialize_completed_message_turn_with_permission_audit(write())
                .await
        }
    }
}

enum ExistingMessageTurn {
    Missing,
    Idempotent(pioneer_protocol::TurnStartResponse),
    Conflict,
}

async fn existing_message_turn(
    crud_store: &pioneer_crud::CrudStore,
    actor: &pioneer_protocol::PersistedActorRef,
    params: &TurnStartParams,
) -> anyhow::Result<ExistingMessageTurn> {
    let Some((_workspace_id, turn)) = crud_store
        .get_turn(params.thread_id.as_str(), params.turn_id.as_str())
        .await?
    else {
        if crud_store
            .get_turn_location(params.turn_id.as_str())
            .await?
            .is_some()
        {
            return Ok(ExistingMessageTurn::Conflict);
        }
        return Ok(ExistingMessageTurn::Missing);
    };
    let database = crud_store.database_connection();
    let persisted_actor =
        pioneer_crud::find_turn_initiator(&database, params.turn_id.as_str()).await?;
    let (input, persisted_mentions) = if turn.message_revision == 0 {
        (
            crud_store.get_turn_inputs(params.turn_id.as_str()).await?,
            turn.mentions
                .iter()
                .map(|mention| mention.principal_id.clone())
                .collect::<Vec<_>>(),
        )
    } else {
        let original = crud_store
            .get_turn_message_revisions(params.turn_id.as_str(), Some(1), 1)
            .await?
            .into_iter()
            .next()
            .filter(|revision| revision.revision == 0);
        let Some(original) = original else {
            return Ok(ExistingMessageTurn::Conflict);
        };
        let Some(original_input) = original.input else {
            return Ok(ExistingMessageTurn::Conflict);
        };
        (
            original_input,
            original
                .mentions
                .into_iter()
                .map(|mention| mention.principal_id)
                .collect::<Vec<_>>(),
        )
    };
    let identical = persisted_actor.as_ref() == Some(actor)
        && turn.status == pioneer_protocol::TurnStatus::Completed
        && turn.mode == pioneer_protocol::ThreadMode::Message
        && turn.reply_to_turn_id == params.reply_to_turn_id
        && persisted_mentions == params.mentioned_principal_ids
        && input == params.input;
    if identical {
        Ok(ExistingMessageTurn::Idempotent(
            pioneer_protocol::TurnStartResponse { turn },
        ))
    } else {
        Ok(ExistingMessageTurn::Conflict)
    }
}

async fn send_message_turn_response(
    processor: &MessageProcessor,
    connection_id: ConnectionId,
    request_id: RequestId,
    response: &pioneer_protocol::TurnStartResponse,
) -> bool {
    let response = match JsonRpcResponse::from_result(request_id, response) {
        Ok(response) => response,
        Err(error) => {
            processor
                .send_error(
                    connection_id,
                    crate::public_error::agent_rpc_error(
                        None,
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorCode::Internal,
                        pioneer_protocol::PublicErrorStage::Delivery,
                        format!("failed to encode Message response: {error}"),
                    ),
                )
                .await;
            return false;
        }
    };
    if let Err(error) = processor.send_json(connection_id, &response).await {
        warn!(
            connection_id,
            error = %format!("{error:#}"),
            "failed to send Message turn/start response"
        );
        return false;
    }
    true
}

/// Narrow authorization carrier for the instant-completed Message leaf.
/// It deliberately contains no provider, executor, tool or runtime state.
pub(super) struct MessageTurnAdmission {
    runtime_draft: Option<RuntimeDraftMaterialization>,
}

#[cfg(test)]
mod tests {
    use super::{
        contains_client_author_snapshot, effective_turn_mode, normalize_turn_collaboration_params,
        validate_message_turn_params,
    };
    use pioneer_protocol::{PrincipalId, ThreadMode, TurnStartParams, UserInput};
    use serde_json::json;

    fn message_params() -> TurnStartParams {
        TurnStartParams {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            input: vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: Some(ThreadMode::Message),
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        }
    }

    #[test]
    fn explicit_turn_mode_wins_without_changing_thread_default() {
        assert_eq!(
            effective_turn_mode(Some(ThreadMode::Message), ThreadMode::Agent),
            ThreadMode::Message
        );
        assert_eq!(
            effective_turn_mode(Some(ThreadMode::Agent), ThreadMode::Message),
            ThreadMode::Agent
        );
    }

    #[test]
    fn omitted_turn_mode_uses_persisted_thread_default() {
        assert_eq!(
            effective_turn_mode(None, ThreadMode::Message),
            ThreadMode::Message
        );
        assert_eq!(
            effective_turn_mode(None, ThreadMode::Chat),
            ThreadMode::Chat
        );
    }

    #[test]
    fn message_validation_rejects_empty_and_execution_fields() {
        let mut params = message_params();
        params.input = vec![UserInput::Text {
            text: "  ".to_owned(),
            text_elements: Vec::new(),
        }];
        assert!(validate_message_turn_params(&params, false).is_err());

        let mut params = message_params();
        params.model = Some("model".to_owned());
        assert!(validate_message_turn_params(&params, false).is_err());

        let mut params = message_params();
        params.input = vec![UserInput::Artifact {
            artifact_id: "artifact_1".to_owned(),
            version_id: None,
        }];
        assert!(validate_message_turn_params(&params, false).is_err());
    }

    #[test]
    fn client_author_snapshot_is_detected_and_rejected() {
        for params in [
            json!({"authorSnapshot": {"displayName": "forged"}}),
            json!({"AUTHOR_DISPLAY_NAME_SNAPSHOT": "forged"}),
            json!({"author-nickname-snapshot": "forged"}),
            json!({"authorAvatarRevisionSnapshot": "forged"}),
            json!({"initiated_by_actor_id": "forged"}),
            json!({"initiatedByActorKind": "forged"}),
        ] {
            assert!(contains_client_author_snapshot(&params));
        }
        assert!(!contains_client_author_snapshot(&json!({
            "authorization_context": "unrelated"
        })));
        assert!(validate_message_turn_params(&message_params(), true).is_err());
    }

    #[test]
    fn collaboration_ids_are_bounded_deduplicated_and_sorted() {
        let mut params = message_params();
        params.reply_to_turn_id = Some("  target_turn  ".to_owned());
        let first = PrincipalId::new("P00000000000000000002").expect("principal id");
        let second = PrincipalId::new("P00000000000000000001").expect("principal id");
        params.mentioned_principal_ids = vec![first.clone(), second.clone(), first];

        normalize_turn_collaboration_params(&mut params).expect("metadata should normalize");

        assert_eq!(params.reply_to_turn_id.as_deref(), Some("target_turn"));
        assert_eq!(
            params.mentioned_principal_ids,
            vec![
                second,
                PrincipalId::new("P00000000000000000002").expect("principal id")
            ]
        );
    }
}

impl MessageTurnAdmission {
    pub(super) fn from_voice_execution_admission(
        admission: &crate::authorization::ExecutionAuthorizationAdmission,
    ) -> Self {
        Self {
            runtime_draft: admission.runtime_draft().cloned(),
        }
    }

    pub(super) fn from_dispatch(
        request_context: &RequestContext,
        admission: &RequestAdmission,
        thread_id: &str,
    ) -> anyhow::Result<Self> {
        if let Some(proof) = admission.thread() {
            if proof.thread_id() != thread_id {
                anyhow::bail!("message authorization thread does not match request");
            }
            if proof.action() != crate::authorization::ResourceAction::MessageCreate {
                anyhow::bail!("message authorization action does not match request");
            }
            return Ok(Self {
                runtime_draft: None,
            });
        }

        if let Some(access) = admission.runtime_draft() {
            if access.thread_id() != thread_id {
                anyhow::bail!("message runtime draft does not match request");
            }
            return Ok(Self {
                runtime_draft: Some(RuntimeDraftMaterialization::from_authorized_runtime_draft(
                    request_context,
                    access.clone(),
                )?),
            });
        }

        anyhow::bail!("turn/start admission has no authorized thread")
    }

    pub(super) fn runtime_draft(&self) -> Option<&RuntimeDraftMaterialization> {
        self.runtime_draft.as_ref()
    }
}

impl MessageProcessor {
    /// Message stays in this small leaf and completes atomically without
    /// importing executor types or entering an execution pipeline.
    pub(super) fn turn_start_message<'a>(
        &'a self,
        request_context: &'a RequestContext,
        admission: MessageTurnAdmission,
        response: MessageTurnResponse,
        mut params: TurnStartParams,
        client_author_override: bool,
    ) -> MessageFuture<'a, ()> {
        let connection_id = request_context.connection_id();
        message_future(async move {
            let operation_started = std::time::Instant::now();
            let input_metrics = message_input_metrics(params.input.as_slice());
            if let Err(error) = validate_message_turn_params(&params, client_author_override) {
                log_message_turn_outcome(
                    &params,
                    input_metrics,
                    "rejected",
                    0,
                    0,
                    operation_started.elapsed().as_millis(),
                );
                response
                    .send_error(
                        self,
                        connection_id,
                        INVALID_PARAMS_CODE,
                        pioneer_protocol::PublicErrorCode::InvalidInput,
                        format!("invalid params for `{}`: {error}", methods::TURN_START),
                    )
                    .await;
                return;
            }
            if let Err(error) = normalize_turn_collaboration_params(&mut params) {
                log_message_turn_outcome(
                    &params,
                    input_metrics,
                    "rejected",
                    0,
                    0,
                    operation_started.elapsed().as_millis(),
                );
                response
                    .send_error(
                        self,
                        connection_id,
                        INVALID_PARAMS_CODE,
                        pioneer_protocol::PublicErrorCode::InvalidInput,
                        format!("invalid params for `{}`: {error}", methods::TURN_START),
                    )
                    .await;
                return;
            }
            let Some(thread) = self
                .thread_manager
                .thread_get(params.thread_id.as_str())
                .await
            else {
                log_message_turn_outcome(
                    &params,
                    input_metrics,
                    "rejected",
                    0,
                    0,
                    operation_started.elapsed().as_millis(),
                );
                response
                    .send_error(
                        self,
                        connection_id,
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorCode::Unavailable,
                        "authorized Message thread is unavailable",
                    )
                    .await;
                return;
            };
            let request_actor = request_context.persisted_actor();
            match existing_message_turn(self.crud_store.as_ref(), &request_actor, &params).await {
                Ok(ExistingMessageTurn::Idempotent(turn_response)) => {
                    log_message_turn_outcome(
                        &params,
                        input_metrics,
                        "idempotent",
                        turn_response.turn.message_revision,
                        0,
                        operation_started.elapsed().as_millis(),
                    );
                    response
                        .send_success(self, connection_id, &turn_response)
                        .await;
                    return;
                }
                Ok(ExistingMessageTurn::Conflict) => {
                    log_message_turn_outcome(
                        &params,
                        input_metrics,
                        "conflict",
                        0,
                        0,
                        operation_started.elapsed().as_millis(),
                    );
                    response.send_conflict(self, connection_id).await;
                    return;
                }
                Ok(ExistingMessageTurn::Missing) => {}
                Err(error) => {
                    log_message_turn_outcome(
                        &params,
                        input_metrics,
                        "rejected",
                        0,
                        0,
                        operation_started.elapsed().as_millis(),
                    );
                    response
                        .send_error(
                            self,
                            connection_id,
                            INVALID_REQUEST_CODE,
                            pioneer_protocol::PublicErrorCode::Internal,
                            format!("failed to resolve Message idempotency: {error:#}"),
                        )
                        .await;
                    return;
                }
            }
            if let Err(error) = self
                .validate_turn_artifact_user_inputs(
                    thread.workspace_id.as_str(),
                    thread.id.as_str(),
                    params.input.as_slice(),
                )
                .await
            {
                log_message_turn_outcome(
                    &params,
                    input_metrics,
                    "rejected",
                    0,
                    0,
                    operation_started.elapsed().as_millis(),
                );
                response
                    .send_error(
                        self,
                        connection_id,
                        INVALID_REQUEST_CODE,
                        pioneer_protocol::PublicErrorCode::Unavailable,
                        format!("invalid Message attachment: {error}"),
                    )
                    .await;
                return;
            }
            let author = match resolve_turn_author_snapshot(
                self.crud_store.as_ref(),
                &request_actor,
            )
            .await
            {
                Ok(Some(author)) => author,
                Ok(None) => {
                    log_message_turn_outcome(
                        &params,
                        input_metrics,
                        "rejected",
                        0,
                        0,
                        operation_started.elapsed().as_millis(),
                    );
                    response
                        .send_error(
                            self,
                            connection_id,
                            INVALID_REQUEST_CODE,
                            pioneer_protocol::PublicErrorCode::Unavailable,
                            "authenticated Message author is unavailable",
                        )
                        .await;
                    return;
                }
                Err(error) => {
                    log_message_turn_outcome(
                        &params,
                        input_metrics,
                        "rejected",
                        0,
                        0,
                        operation_started.elapsed().as_millis(),
                    );
                    response
                        .send_error(
                            self,
                            connection_id,
                            INVALID_REQUEST_CODE,
                            pioneer_protocol::PublicErrorCode::Internal,
                            format!("failed to resolve Message author: {error:#}"),
                        )
                        .await;
                    return;
                }
            };
            let mentions = match resolve_turn_collaboration_metadata(
                self.crud_store.as_ref(),
                &request_actor,
                &params,
            )
            .await
            {
                Ok(mentions) => mentions,
                Err(error) => {
                    log_message_turn_outcome(
                        &params,
                        input_metrics,
                        "rejected",
                        0,
                        0,
                        operation_started.elapsed().as_millis(),
                    );
                    response
                        .send_error(
                            self,
                            connection_id,
                            INVALID_REQUEST_CODE,
                            pioneer_protocol::PublicErrorCode::InvalidInput,
                            format!("invalid Turn collaboration metadata: {error}"),
                        )
                        .await;
                    return;
                }
            };
            let outcome = match self
                .thread_manager
                .prepare_completed_message_turn(connection_id, &params, author, mentions)
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    log_message_turn_outcome(
                        &params,
                        input_metrics,
                        "rejected",
                        0,
                        0,
                        operation_started.elapsed().as_millis(),
                    );
                    response
                        .send_error(
                            self,
                            connection_id,
                            INVALID_REQUEST_CODE,
                            pioneer_protocol::PublicErrorCode::Internal,
                            format!("failed to admit Message turn: {error:#}"),
                        )
                        .await;
                    return;
                }
            };
            let audit_event = self.turn_profile_selected_audit_event_for_turn(
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
                outcome.materialization.turn.permission_profile.clone(),
            );
            let commit_started = std::time::Instant::now();
            if let Err(error) = persist_completed_message_turn(
                self.crud_store.as_ref(),
                &outcome,
                request_actor.clone(),
                audit_event,
                admission.runtime_draft(),
            )
            .await
            {
                match existing_message_turn(self.crud_store.as_ref(), &request_actor, &params).await
                {
                    Ok(ExistingMessageTurn::Idempotent(turn_response)) => {
                        log_message_turn_outcome(
                            &params,
                            input_metrics,
                            "idempotent",
                            turn_response.turn.message_revision,
                            commit_started.elapsed().as_millis(),
                            operation_started.elapsed().as_millis(),
                        );
                        response
                            .send_success(self, connection_id, &turn_response)
                            .await;
                    }
                    Ok(ExistingMessageTurn::Conflict) => {
                        log_message_turn_outcome(
                            &params,
                            input_metrics,
                            "conflict",
                            0,
                            commit_started.elapsed().as_millis(),
                            operation_started.elapsed().as_millis(),
                        );
                        response.send_conflict(self, connection_id).await;
                    }
                    Ok(ExistingMessageTurn::Missing) | Err(_) => {
                        log_message_turn_outcome(
                            &params,
                            input_metrics,
                            "rejected",
                            0,
                            commit_started.elapsed().as_millis(),
                            operation_started.elapsed().as_millis(),
                        );
                        response
                            .send_error(
                                self,
                                connection_id,
                                INVALID_REQUEST_CODE,
                                pioneer_protocol::PublicErrorCode::Internal,
                                format!("failed to persist Message turn: {error:#}"),
                            )
                            .await;
                    }
                }
                return;
            }
            log_message_turn_outcome(
                &params,
                input_metrics,
                "accepted",
                outcome.response.turn.message_revision,
                commit_started.elapsed().as_millis(),
                operation_started.elapsed().as_millis(),
            );

            if let Some(runtime_draft) = admission.runtime_draft() {
                self.complete_runtime_draft_materialization_record(runtime_draft)
                    .await;
            }
            if let Err(error) = self
                .thread_manager
                .commit_completed_message_turn(&outcome)
                .await
            {
                warn!(
                    thread_id = outcome.completed_notification.thread_id.as_str(),
                    turn_id = outcome.completed_notification.turn.id.as_str(),
                    error = %format!("{error:#}"),
                    "committed Message turn could not be applied to loaded state"
                );
            }

            self.session_manager
                .set_connection_workspace(
                    connection_id,
                    Some(outcome.completed_notification.workspace_id.clone()),
                )
                .await;
            response
                .send_success(self, connection_id, &outcome.response)
                .await;

            self.send_notification_to_authorized_thread_connections(
                outcome.started_notification.thread_id.as_str(),
                events::TURN_STARTED,
                &outcome.started_notification,
                outcome.notification_connection_ids.clone(),
            )
            .await;
            self.send_notification_to_authorized_thread_connections(
                outcome.completed_notification.thread_id.as_str(),
                events::TURN_COMPLETED,
                &outcome.completed_notification,
                outcome.notification_connection_ids.clone(),
            )
            .await;
            self.notify_semantic_user_message_changed(
                outcome.completed_notification.workspace_id.as_str(),
                outcome.completed_notification.thread_id.as_str(),
                outcome.completed_notification.turn.id.as_str(),
            )
            .await;
            self.notify_thread_tree_changed(outcome.completed_notification.workspace_id.clone())
                .await;
        })
    }
}
