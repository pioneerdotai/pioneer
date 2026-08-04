use super::*;
use crate::authorization::{AuthorizationExternalError, AuthorizedTurn};
use base64::Engine as _;
use pioneer_protocol::{PersistedActorRef, PrincipalKind, TurnMessageErrorReason};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MessageMutationActor {
    Author,
    SuperuserModerator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MessageMutationOperation {
    Edit { expected_revision: u64 },
    Delete { expected_revision: u64 },
    RevisionsRead,
}

impl MessageMutationOperation {
    fn name(self) -> &'static str {
        match self {
            Self::Edit { .. } => "edit",
            Self::Delete { .. } => "delete",
            Self::RevisionsRead => "revisions_read",
        }
    }

    fn expected_revision(self) -> u64 {
        match self {
            Self::Edit { expected_revision } | Self::Delete { expected_revision } => {
                expected_revision
            }
            Self::RevisionsRead => 0,
        }
    }
}

fn log_message_mutation_outcome(
    authorization: &AuthorizedTurn,
    operation: MessageMutationOperation,
    metrics: super::message_turn::MessageInputMetrics,
    outcome: &'static str,
    revision: u64,
    started: std::time::Instant,
) {
    debug!(
        workspace_id = authorization.workspace_id(),
        thread_id = authorization.thread_id(),
        turn_id = authorization.turn_id(),
        mode = "message",
        operation = operation.name(),
        input_bytes = metrics.input_bytes,
        attachment_count = metrics.attachment_count,
        expected_revision = operation.expected_revision(),
        revision,
        outcome,
        latency_ms = started.elapsed().as_millis(),
        "Message collaboration operation outcome"
    );
}

fn mutation_error_outcome(error: &MessageMutationError) -> &'static str {
    if matches!(error, MessageMutationError::RevisionConflict) {
        "conflict"
    } else {
        "rejected"
    }
}

#[derive(Debug)]
pub(super) struct ResolvedMessageMutation {
    pub collaboration: pioneer_crud::PersistedTurnCollaboration,
    pub actor: Option<MessageMutationActor>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TurnMessageRevisionCursor {
    version: u8,
    turn_id: String,
    before_revision: u64,
}

fn encode_revision_cursor(turn_id: &str, before_revision: u64) -> Option<String> {
    let payload = TurnMessageRevisionCursor {
        version: 1,
        turn_id: turn_id.to_owned(),
        before_revision,
    };
    serde_json::to_vec(&payload)
        .ok()
        .map(|bytes| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_revision_cursor(cursor: &str, turn_id: &str) -> Option<u64> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor.as_bytes())
        .ok()?;
    let payload: TurnMessageRevisionCursor = serde_json::from_slice(&bytes).ok()?;
    (payload.version == 1 && payload.turn_id == turn_id).then_some(payload.before_revision)
}

fn apply_revision_disclosure(
    revisions: &mut [pioneer_protocol::TurnMessageRevision],
    message_deleted: bool,
    actor: Option<MessageMutationActor>,
) {
    if !message_deleted || actor.is_some() {
        return;
    }
    for revision in revisions {
        revision.input = None;
        revision.mentions.clear();
    }
}

#[derive(Debug)]
pub(super) enum MessageMutationError {
    NotFound,
    Forbidden,
    InvalidTarget,
    ImmutableMessage,
    DeletedMessage,
    RevisionConflict,
    InvalidParams,
    Unavailable(anyhow::Error),
}

impl MessageMutationError {
    fn reason(&self) -> Option<TurnMessageErrorReason> {
        match self {
            Self::NotFound | Self::Forbidden | Self::Unavailable(_) => None,
            Self::InvalidParams => Some(TurnMessageErrorReason::InvalidInput),
            Self::InvalidTarget => Some(TurnMessageErrorReason::InvalidTarget),
            Self::ImmutableMessage => Some(TurnMessageErrorReason::ImmutableMessage),
            Self::DeletedMessage => Some(TurnMessageErrorReason::DeletedMessage),
            Self::RevisionConflict => Some(TurnMessageErrorReason::RevisionConflict),
        }
    }
}

fn crud_mutation_error(error: anyhow::Error) -> MessageMutationError {
    match error
        .downcast_ref::<pioneer_crud::TurnMessageMutationFailure>()
        .copied()
    {
        Some(pioneer_crud::TurnMessageMutationFailure::NotFound) => MessageMutationError::NotFound,
        Some(pioneer_crud::TurnMessageMutationFailure::Forbidden) => {
            MessageMutationError::Forbidden
        }
        Some(pioneer_crud::TurnMessageMutationFailure::InvalidInput) => {
            MessageMutationError::InvalidParams
        }
        Some(pioneer_crud::TurnMessageMutationFailure::InvalidTarget) => {
            MessageMutationError::InvalidTarget
        }
        Some(pioneer_crud::TurnMessageMutationFailure::ImmutableMessage) => {
            MessageMutationError::ImmutableMessage
        }
        Some(pioneer_crud::TurnMessageMutationFailure::DeletedMessage) => {
            MessageMutationError::DeletedMessage
        }
        Some(pioneer_crud::TurnMessageMutationFailure::RevisionConflict) => {
            MessageMutationError::RevisionConflict
        }
        None => MessageMutationError::Unavailable(error),
    }
}

fn mutation_policy(
    operation: MessageMutationOperation,
    message_mutation_eligible: bool,
    message_deleted: bool,
    current_revision: u64,
    original_author: Option<&pioneer_protocol::PersistedActorRef>,
    caller: &pioneer_protocol::PrincipalId,
    caller_kind: PrincipalKind,
) -> Result<Option<MessageMutationActor>, MessageMutationError> {
    if !message_mutation_eligible {
        return Err(MessageMutationError::ImmutableMessage);
    }
    if !matches!(original_author, Some(PersistedActorRef::Principal(_))) {
        return Err(MessageMutationError::ImmutableMessage);
    }

    let actor = if original_author
        == Some(&pioneer_protocol::PersistedActorRef::Principal(
            caller.clone(),
        )) {
        MessageMutationActor::Author
    } else if caller_kind == PrincipalKind::Superuser {
        MessageMutationActor::SuperuserModerator
    } else if operation != MessageMutationOperation::RevisionsRead {
        return Err(MessageMutationError::Forbidden);
    } else {
        return Ok(None);
    };

    if operation == MessageMutationOperation::RevisionsRead {
        return Ok(Some(actor));
    }

    match operation {
        MessageMutationOperation::Edit { expected_revision } => {
            if message_deleted {
                return Err(MessageMutationError::DeletedMessage);
            }
            if expected_revision != current_revision {
                return Err(MessageMutationError::RevisionConflict);
            }
            Ok(Some(actor))
        }
        MessageMutationOperation::Delete { expected_revision } => {
            if message_deleted {
                let duplicate_delete = expected_revision == current_revision
                    || expected_revision.checked_add(1) == Some(current_revision);
                if duplicate_delete {
                    return Ok(Some(actor));
                }
                return Err(MessageMutationError::RevisionConflict);
            }
            if expected_revision != current_revision {
                return Err(MessageMutationError::RevisionConflict);
            }
            Ok(Some(actor))
        }
        MessageMutationOperation::RevisionsRead => unreachable!("handled above"),
    }
}

impl MessageProcessor {
    async fn sync_committed_message_mutation(&self, thread_id: &str, turn_id: &str) {
        let persisted_thread = match self.crud_store.get_thread_model(thread_id).await {
            Ok(Some(thread)) => thread,
            Ok(None) => {
                warn!(
                    thread_id,
                    turn_id, "committed Message thread is unavailable"
                );
                return;
            }
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to load committed Message thread"
                );
                return;
            }
        };
        if let Err(error) = self
            .thread_manager
            .sync_message_mutation_from_persisted(&persisted_thread, turn_id)
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to synchronize committed Message mutation into loaded state"
            );
        }
    }

    pub(super) async fn resolve_message_mutation(
        &self,
        context: &crate::request_context::RequestContext,
        authorization: &AuthorizedTurn,
        operation: MessageMutationOperation,
    ) -> Result<ResolvedMessageMutation, MessageMutationError> {
        if authorization.thread_id().is_empty() || authorization.turn_id().is_empty() {
            return Err(MessageMutationError::NotFound);
        }
        let (_, collaboration) = pioneer_crud::find_turn_collaboration(
            &self.crud_store.database_connection(),
            authorization.thread_id(),
            authorization.turn_id(),
        )
        .await
        .map_err(MessageMutationError::Unavailable)?
        .ok_or(MessageMutationError::NotFound)?;
        let original_author = collaboration.author.as_ref().map(|author| &author.actor);
        let actor = mutation_policy(
            operation,
            collaboration.mode.message_mutation_eligible,
            collaboration.message_deleted,
            collaboration.message_revision,
            original_author,
            &context.principal().principal_id,
            context.principal().kind,
        )?;
        Ok(ResolvedMessageMutation {
            collaboration,
            actor,
        })
    }

    pub(super) async fn send_message_mutation_error(
        &self,
        context: &crate::request_context::RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        error: MessageMutationError,
    ) {
        let response = match error {
            MessageMutationError::NotFound => {
                AuthorizationExternalError::NotFound.response(request_id)
            }
            MessageMutationError::Forbidden => {
                AuthorizationExternalError::Forbidden.response(request_id)
            }
            MessageMutationError::Unavailable(error) => {
                warn!(error = %format!("{error:#}"), "Turn message mutation failed");
                AuthorizationExternalError::Unavailable.response(request_id)
            }
            error => {
                let code = if matches!(error, MessageMutationError::InvalidParams) {
                    INVALID_PARAMS_CODE
                } else {
                    INVALID_REQUEST_CODE
                };
                let message = match error {
                    MessageMutationError::InvalidParams => "invalid message input",
                    MessageMutationError::InvalidTarget => "invalid message target",
                    MessageMutationError::ImmutableMessage => "immutable message",
                    MessageMutationError::DeletedMessage => "deleted message",
                    MessageMutationError::RevisionConflict => "message revision conflict",
                    _ => unreachable!("handled above"),
                };
                let mut response =
                    JsonRpcErrorResponse::new(Some(request_id.clone()), code, message);
                response.error.data = if matches!(error, MessageMutationError::RevisionConflict) {
                    match pioneer_crud::find_turn_collaboration(
                        &self.crud_store.database_connection(),
                        authorization.thread_id(),
                        authorization.turn_id(),
                    )
                    .await
                    {
                        Ok(Some((_, collaboration))) => Some(serde_json::json!({
                            "code": TurnMessageErrorReason::RevisionConflict,
                            "current_revision": collaboration.message_revision,
                        })),
                        Ok(None) => {
                            return self
                                .send_error(
                                    context.connection_id(),
                                    AuthorizationExternalError::NotFound.response(request_id),
                                )
                                .await;
                        }
                        Err(error) => {
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to resolve current Message revision for conflict"
                            );
                            return self
                                .send_error(
                                    context.connection_id(),
                                    AuthorizationExternalError::Unavailable.response(request_id),
                                )
                                .await;
                        }
                    }
                } else {
                    error
                        .reason()
                        .and_then(|reason| serde_json::to_value(reason).ok())
                };
                response
            }
        };
        self.send_error(context.connection_id(), response).await;
    }

    // Mutations stay on the existing Turn ingress and persistence path. Policy
    // is resolved before replacement content is validated so inaccessible Turns
    // retain the authorization boundary's normal non-disclosure behavior.
    pub(super) async fn turn_message_edit(
        &self,
        context: &crate::request_context::RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnMessageEditParams,
    ) {
        let started = std::time::Instant::now();
        let input_metrics = super::message_turn::message_input_metrics(params.input.as_slice());
        let operation = MessageMutationOperation::Edit {
            expected_revision: params.expected_revision,
        };
        if let Err(error) = self
            .resolve_message_mutation(context, authorization, operation)
            .await
        {
            log_message_mutation_outcome(
                authorization,
                operation,
                input_metrics,
                mutation_error_outcome(&error),
                operation.expected_revision(),
                started,
            );
            self.send_message_mutation_error(context, authorization, request_id, error)
                .await;
            return;
        }

        if let Err(error) = super::message_turn::validate_message_content(
            params.input.as_slice(),
            params.mentioned_principal_ids.len(),
        ) {
            debug!(
                thread_id = authorization.thread_id(),
                turn_id = authorization.turn_id(),
                error = %format!("{error:#}"),
                "rejected invalid Message edit content"
            );
            log_message_mutation_outcome(
                authorization,
                operation,
                input_metrics,
                "rejected",
                operation.expected_revision(),
                started,
            );
            self.send_message_mutation_error(
                context,
                authorization,
                request_id,
                MessageMutationError::InvalidParams,
            )
            .await;
            return;
        }
        let mut mentioned_principal_ids = params.mentioned_principal_ids;
        mentioned_principal_ids.sort();
        mentioned_principal_ids.dedup();
        if let Err(error) = self
            .validate_turn_artifact_user_inputs(
                authorization.workspace_id(),
                authorization.thread_id(),
                params.input.as_slice(),
            )
            .await
        {
            debug!(
                thread_id = authorization.thread_id(),
                turn_id = authorization.turn_id(),
                error = %format!("{error:#}"),
                "rejected unavailable Message edit artifact"
            );
            log_message_mutation_outcome(
                authorization,
                operation,
                input_metrics,
                "rejected",
                operation.expected_revision(),
                started,
            );
            self.send_message_mutation_error(
                context,
                authorization,
                request_id,
                MessageMutationError::InvalidTarget,
            )
            .await;
            return;
        }
        let actor = context.persisted_actor();
        let mentions = match super::message_turn::resolve_turn_mentions(
            self.crud_store.as_ref(),
            &actor,
            mentioned_principal_ids.as_slice(),
        )
        .await
        {
            Ok(mentions) => mentions,
            Err(error) => {
                debug!(
                    thread_id = authorization.thread_id(),
                    turn_id = authorization.turn_id(),
                    error = %format!("{error:#}"),
                    "rejected unavailable Message edit mention"
                );
                log_message_mutation_outcome(
                    authorization,
                    operation,
                    input_metrics,
                    "rejected",
                    operation.expected_revision(),
                    started,
                );
                self.send_message_mutation_error(
                    context,
                    authorization,
                    request_id,
                    MessageMutationError::InvalidTarget,
                )
                .await;
                return;
            }
        };
        let changed_at_unix = chrono::Utc::now().timestamp();
        let event = match self
            .crud_store
            .edit_turn_message(pioneer_crud::EditTurnMessageRequest {
                workspace_id: authorization.workspace_id().to_owned(),
                thread_id: authorization.thread_id().to_owned(),
                turn_id: authorization.turn_id().to_owned(),
                expected_revision: params.expected_revision,
                input: params.input,
                mentions,
                changed_by: actor,
                changed_at_unix,
            })
            .await
        {
            Ok(event) => event,
            Err(error) => {
                let error = crud_mutation_error(error);
                log_message_mutation_outcome(
                    authorization,
                    operation,
                    input_metrics,
                    mutation_error_outcome(&error),
                    operation.expected_revision(),
                    started,
                );
                self.send_message_mutation_error(context, authorization, request_id, error)
                    .await;
                return;
            }
        };

        log_message_mutation_outcome(
            authorization,
            operation,
            input_metrics,
            "accepted",
            event.turn.message_revision,
            started,
        );

        let response = pioneer_protocol::TurnMessageEditResponse {
            turn: event.turn.clone(),
        };
        self.sync_committed_message_mutation(authorization.thread_id(), authorization.turn_id())
            .await;
        match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => {
                if let Err(error) = self.send_json(context.connection_id(), &response).await {
                    warn!(error = %format!("{error:#}"), "failed to send Message edit response");
                }
            }
            Err(error) => {
                warn!(error = %error, "failed to encode Message edit response");
            }
        }
        self.notify_semantic_user_message_changed(
            authorization.workspace_id(),
            authorization.thread_id(),
            authorization.turn_id(),
        )
        .await;
        self.notify_thread_tree_changed(authorization.workspace_id().to_owned())
            .await;
    }

    pub(super) async fn turn_message_delete(
        &self,
        context: &crate::request_context::RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnMessageDeleteParams,
    ) {
        let started = std::time::Instant::now();
        let input_metrics = super::message_turn::MessageInputMetrics::default();
        let operation = MessageMutationOperation::Delete {
            expected_revision: params.expected_revision,
        };
        if let Err(error) = self
            .resolve_message_mutation(context, authorization, operation)
            .await
        {
            log_message_mutation_outcome(
                authorization,
                operation,
                input_metrics,
                mutation_error_outcome(&error),
                operation.expected_revision(),
                started,
            );
            self.send_message_mutation_error(context, authorization, request_id, error)
                .await;
            return;
        }
        let changed_at_unix = chrono::Utc::now().timestamp();
        let result = match self
            .crud_store
            .delete_turn_message(pioneer_crud::DeleteTurnMessageRequest {
                workspace_id: authorization.workspace_id().to_owned(),
                thread_id: authorization.thread_id().to_owned(),
                turn_id: authorization.turn_id().to_owned(),
                expected_revision: params.expected_revision,
                changed_by: context.persisted_actor(),
                changed_at_unix,
            })
            .await
        {
            Ok(result) => result,
            Err(error) => {
                let error = crud_mutation_error(error);
                log_message_mutation_outcome(
                    authorization,
                    operation,
                    input_metrics,
                    mutation_error_outcome(&error),
                    operation.expected_revision(),
                    started,
                );
                self.send_message_mutation_error(context, authorization, request_id, error)
                    .await;
                return;
            }
        };
        let mutation_outcome = if result.event.is_some() {
            "accepted"
        } else {
            "idempotent"
        };
        log_message_mutation_outcome(
            authorization,
            operation,
            input_metrics,
            mutation_outcome,
            result.turn.message_revision,
            started,
        );
        self.sync_committed_message_mutation(authorization.thread_id(), authorization.turn_id())
            .await;
        let response = pioneer_protocol::TurnMessageDeleteResponse { turn: result.turn };
        match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => {
                if let Err(error) = self.send_json(context.connection_id(), &response).await {
                    warn!(error = %format!("{error:#}"), "failed to send Message delete response");
                }
            }
            Err(error) => warn!(error = %error, "failed to encode Message delete response"),
        }
        if result.event.is_some() {
            self.notify_semantic_user_message_changed(
                authorization.workspace_id(),
                authorization.thread_id(),
                authorization.turn_id(),
            )
            .await;
            self.notify_thread_tree_changed(authorization.workspace_id().to_owned())
                .await;
        }
    }

    pub(super) async fn turn_message_revisions_page(
        &self,
        context: &crate::request_context::RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnMessageRevisionsPageParams,
    ) {
        let started = std::time::Instant::now();
        let operation = MessageMutationOperation::RevisionsRead;
        let input_metrics = super::message_turn::MessageInputMetrics::default();
        let limit = match params.validated_limit() {
            Ok(limit) => limit,
            Err(_) => {
                log_message_mutation_outcome(
                    authorization,
                    operation,
                    input_metrics,
                    "rejected",
                    0,
                    started,
                );
                self.send_message_mutation_error(
                    context,
                    authorization,
                    request_id,
                    MessageMutationError::InvalidParams,
                )
                .await;
                return;
            }
        };
        let before_revision = match params.cursor.as_deref() {
            None => None,
            Some(cursor) => match decode_revision_cursor(cursor, authorization.turn_id()) {
                Some(revision) => Some(revision),
                None => {
                    log_message_mutation_outcome(
                        authorization,
                        operation,
                        input_metrics,
                        "rejected",
                        0,
                        started,
                    );
                    self.send_message_mutation_error(
                        context,
                        authorization,
                        request_id,
                        MessageMutationError::InvalidParams,
                    )
                    .await;
                    return;
                }
            },
        };
        let initial_resolution = match self
            .resolve_message_mutation(
                context,
                authorization,
                MessageMutationOperation::RevisionsRead,
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => {
                log_message_mutation_outcome(
                    authorization,
                    operation,
                    input_metrics,
                    mutation_error_outcome(&error),
                    0,
                    started,
                );
                self.send_message_mutation_error(context, authorization, request_id, error)
                    .await;
                return;
            }
        };
        let requested = u64::from(limit).saturating_add(1);
        let mut revisions = match self
            .crud_store
            .get_turn_message_revisions(authorization.turn_id(), before_revision, requested)
            .await
        {
            Ok(revisions) => revisions,
            Err(error) => {
                log_message_mutation_outcome(
                    authorization,
                    operation,
                    input_metrics,
                    "rejected",
                    initial_resolution.collaboration.message_revision,
                    started,
                );
                self.send_message_mutation_error(
                    context,
                    authorization,
                    request_id,
                    MessageMutationError::Unavailable(error),
                )
                .await;
                return;
            }
        };
        let has_more = revisions.len() > limit as usize;
        revisions.truncate(limit as usize);

        // Re-read the authoritative tombstone after loading the immutable
        // revision rows. A delete may commit between the initial policy check
        // and the page query; applying disclosure from that stale snapshot
        // could otherwise expose the newly deleted body to an ordinary
        // participant. This second read is the response's disclosure
        // linearization point.
        let resolved = match self
            .resolve_message_mutation(
                context,
                authorization,
                MessageMutationOperation::RevisionsRead,
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => {
                log_message_mutation_outcome(
                    authorization,
                    operation,
                    input_metrics,
                    mutation_error_outcome(&error),
                    initial_resolution.collaboration.message_revision,
                    started,
                );
                self.send_message_mutation_error(context, authorization, request_id, error)
                    .await;
                return;
            }
        };
        apply_revision_disclosure(
            revisions.as_mut_slice(),
            resolved.collaboration.message_deleted,
            resolved.actor,
        );
        let next_cursor = if has_more {
            revisions.last().and_then(|revision| {
                encode_revision_cursor(authorization.turn_id(), revision.revision)
            })
        } else {
            None
        };
        let response = pioneer_protocol::TurnMessageRevisionsPageResponse {
            workspace_id: authorization.workspace_id().to_owned(),
            thread_id: authorization.thread_id().to_owned(),
            turn_id: authorization.turn_id().to_owned(),
            revisions,
            next_cursor,
        };
        log_message_mutation_outcome(
            authorization,
            operation,
            input_metrics,
            "accepted",
            resolved.collaboration.message_revision,
            started,
        );
        match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => {
                if let Err(error) = self.send_json(context.connection_id(), &response).await {
                    warn!(error = %format!("{error:#}"), "failed to send Message revisions response");
                }
            }
            Err(error) => warn!(error = %error, "failed to encode Message revisions response"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{PersistedActorRef, PrincipalId, ThreadMode};

    fn principal(value: &str) -> PrincipalId {
        PrincipalId::new(value).expect("valid principal id")
    }

    #[test]
    fn mutation_policy_allows_only_message_author_or_superuser() {
        let author = principal("P00000000000000000001");
        let other = principal("P00000000000000000002");
        let author_ref = PersistedActorRef::Principal(author.clone());

        assert_eq!(
            mutation_policy(
                MessageMutationOperation::Edit {
                    expected_revision: 0,
                },
                true,
                false,
                0,
                Some(&author_ref),
                &author,
                PrincipalKind::User,
            )
            .expect("author may edit"),
            Some(MessageMutationActor::Author)
        );
        assert!(matches!(
            mutation_policy(
                MessageMutationOperation::Delete {
                    expected_revision: 0,
                },
                true,
                false,
                0,
                Some(&author_ref),
                &other,
                PrincipalKind::User,
            ),
            Err(MessageMutationError::Forbidden)
        ));
        assert_eq!(
            mutation_policy(
                MessageMutationOperation::Delete {
                    expected_revision: 0,
                },
                true,
                false,
                0,
                Some(&author_ref),
                &other,
                PrincipalKind::Superuser,
            )
            .expect("Superuser may moderate"),
            Some(MessageMutationActor::SuperuserModerator)
        );
    }

    #[test]
    fn mutation_policy_keeps_execution_and_legacy_turns_immutable() {
        let caller = principal("P00000000000000000001");
        let author = PersistedActorRef::Principal(caller.clone());
        for mode in [None, Some(ThreadMode::Chat), Some(ThreadMode::Agent)] {
            assert!(matches!(
                mutation_policy(
                    MessageMutationOperation::Edit {
                        expected_revision: 0,
                    },
                    mode == Some(ThreadMode::Message),
                    false,
                    0,
                    Some(&author),
                    &caller,
                    PrincipalKind::Superuser,
                ),
                Err(MessageMutationError::ImmutableMessage)
            ));
        }
    }

    #[test]
    fn mutation_policy_keeps_system_and_unattributed_message_rows_immutable() {
        let caller = principal("P00000000000000000001");
        let system = PersistedActorRef::System;
        for original_author in [Some(&system), None] {
            assert!(matches!(
                mutation_policy(
                    MessageMutationOperation::Delete {
                        expected_revision: 0,
                    },
                    true,
                    false,
                    0,
                    original_author,
                    &caller,
                    PrincipalKind::Superuser,
                ),
                Err(MessageMutationError::ImmutableMessage)
            ));
        }
    }

    #[test]
    fn mutation_policy_enforces_revision_and_duplicate_delete_contract() {
        let caller = principal("P00000000000000000001");
        let author = PersistedActorRef::Principal(caller.clone());
        assert!(matches!(
            mutation_policy(
                MessageMutationOperation::Edit {
                    expected_revision: 1,
                },
                true,
                false,
                2,
                Some(&author),
                &caller,
                PrincipalKind::User,
            ),
            Err(MessageMutationError::RevisionConflict)
        ));
        assert_eq!(
            mutation_policy(
                MessageMutationOperation::Delete {
                    expected_revision: 1,
                },
                true,
                true,
                2,
                Some(&author),
                &caller,
                PrincipalKind::User,
            )
            .expect("same delete retry is idempotent"),
            Some(MessageMutationActor::Author)
        );
        assert!(matches!(
            mutation_policy(
                MessageMutationOperation::Delete {
                    expected_revision: 0,
                },
                true,
                true,
                2,
                Some(&author),
                &caller,
                PrincipalKind::User,
            ),
            Err(MessageMutationError::RevisionConflict)
        ));
    }

    #[test]
    fn revision_cursor_is_scoped_and_deleted_body_is_redacted_for_participants() {
        let cursor = encode_revision_cursor("turn-a", 7).expect("cursor should encode");
        assert_eq!(decode_revision_cursor(cursor.as_str(), "turn-a"), Some(7));
        assert_eq!(decode_revision_cursor(cursor.as_str(), "turn-b"), None);

        let mut revisions = vec![pioneer_protocol::TurnMessageRevision {
            turn_id: "turn-a".to_owned(),
            revision: 7,
            change_kind: pioneer_protocol::TurnMessageRevisionChangeKind::Delete,
            changed_by: PersistedActorRef::System,
            created_at: 1,
            input: Some(vec![pioneer_protocol::UserInput::Text {
                text: "private body".to_owned(),
                text_elements: Vec::new(),
            }]),
            mentions: vec![pioneer_protocol::TurnMention {
                principal_id: principal("P00000000000000000003"),
                nickname: "private-nickname".to_owned(),
            }],
        }];
        apply_revision_disclosure(revisions.as_mut_slice(), true, None);
        assert_eq!(revisions[0].input, None);
        assert!(revisions[0].mentions.is_empty());

        let mut author_revisions = revisions.clone();
        author_revisions[0].input = Some(Vec::new());
        apply_revision_disclosure(
            author_revisions.as_mut_slice(),
            true,
            Some(MessageMutationActor::Author),
        );
        assert_eq!(author_revisions[0].input, Some(Vec::new()));
    }
}
