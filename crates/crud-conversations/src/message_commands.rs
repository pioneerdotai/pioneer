//! Prepare and revalidate edit/delete commands before canonical event publication.
use crate::protocol_projection::thread_snapshot_turn_from_db_model;
use crate::{thread_metadata as thread, turn_writes};
use anyhow::{Context, Result};
use pioneer_crud_access::{
    authorization_scope::{
        principal_current_thread_access_kind, resolve_artifact_authorization_scope,
    },
    identity::load_principal_by_id,
    membership::find_shared_workspace_principal_for_principal,
    turn_execution::find_turn_initiator,
};
use pioneer_protocol::{
    PersistedActorRef, Turn, TurnMention, TurnMessageDeletedEvent, TurnMessageEditedEvent,
    UserInput,
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use std::{collections::HashSet, error::Error as StdError, fmt};

struct MessageMutationFence {
    thread: pioneer_entity::thread::Model,
    turn: pioneer_entity::turn::Model,
}

#[derive(Debug, Clone)]
pub struct EditTurnMessageRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub expected_revision: u64,
    pub input: Vec<UserInput>,
    pub mentions: Vec<TurnMention>,
    pub changed_by: PersistedActorRef,
    pub changed_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct DeleteTurnMessageRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub expected_revision: u64,
    pub changed_by: PersistedActorRef,
    pub changed_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteTurnMessageResult {
    pub turn: Turn,
    /// `None` means the requested tombstone already existed. No revision,
    /// event, audit row or notification is produced for that idempotent retry.
    pub event: Option<TurnMessageDeletedEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnMessageMutationFailure {
    NotFound,
    Forbidden,
    InvalidInput,
    InvalidTarget,
    ImmutableMessage,
    DeletedMessage,
    RevisionConflict,
}

impl fmt::Display for TurnMessageMutationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::NotFound => "Turn message was not found",
            Self::Forbidden => "Turn message mutation is forbidden",
            Self::InvalidInput => "Turn message input is invalid",
            Self::InvalidTarget => "Turn message target is unavailable",
            Self::ImmutableMessage => "Turn is not a mutable Message",
            Self::DeletedMessage => "Message is deleted",
            Self::RevisionConflict => "Turn message revision conflict",
        };
        formatter.write_str(message)
    }
}

impl StdError for TurnMessageMutationFailure {}

pub async fn message_mutation_actor_current_thread_write_kind<C: ConnectionTrait>(
    transaction: &C,
    workspace_id: &str,
    thread_id: &str,
    changed_by: &PersistedActorRef,
) -> Result<Option<pioneer_protocol::PrincipalKind>> {
    let PersistedActorRef::Principal(principal_id) = changed_by else {
        return Ok(None);
    };
    principal_current_thread_access_kind(transaction, workspace_id, thread_id, principal_id).await
}

async fn revalidate_turn_message_edit_targets<C: ConnectionTrait>(
    transaction: &C,
    workspace_id: &str,
    thread_id: &str,
    changed_by: &PersistedActorRef,
    input: &[UserInput],
    mentions: &[TurnMention],
) -> Result<()> {
    let PersistedActorRef::Principal(viewer_principal_id) = changed_by else {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::InvalidTarget,
        ));
    };
    let viewer = load_principal_by_id(transaction, viewer_principal_id)
        .await?
        .filter(|principal| principal.status == pioneer_protocol::PrincipalStatus::Active)
        .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::InvalidTarget))?;

    for mention in mentions {
        let target = load_principal_by_id(transaction, &mention.principal_id)
            .await?
            .filter(|principal| {
                principal.gateway_id == viewer.gateway_id
                    && principal.status == pioneer_protocol::PrincipalStatus::Active
                    && (principal.kind == pioneer_protocol::PrincipalKind::Superuser
                        || (principal.kind == pioneer_protocol::PrincipalKind::User
                            && principal
                                .role_key
                                .as_deref()
                                .is_some_and(|role| pioneer_protocol::RoleKey::new(role).is_ok())))
            })
            .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::InvalidTarget))?;
        let visible = match viewer.kind {
            pioneer_protocol::PrincipalKind::Superuser => true,
            pioneer_protocol::PrincipalKind::User => {
                target.id == viewer.id
                    || target.kind == pioneer_protocol::PrincipalKind::Superuser
                    || find_shared_workspace_principal_for_principal(
                        transaction,
                        &viewer.gateway_id,
                        &viewer.id,
                        &target.id,
                    )
                    .await?
                    .is_some()
            }
        };
        if !visible || target.nickname != mention.nickname {
            return Err(anyhow::Error::new(
                TurnMessageMutationFailure::InvalidTarget,
            ));
        }
    }

    for value in input {
        let UserInput::Artifact {
            artifact_id,
            version_id: Some(version_id),
        } = value
        else {
            continue;
        };
        let artifact = pioneer_entity::artifact::Entity::find_by_id(artifact_id.clone())
            .one(transaction)
            .await
            .context("failed to revalidate Message artifact")?
            .filter(|artifact| {
                artifact.workspace_id == workspace_id
                    && artifact.status == "ready"
                    && artifact.deleted_at.is_none()
            })
            .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::InvalidTarget))?;
        let exact_version =
            pioneer_entity::artifact_version::Entity::find_by_id(version_id.clone())
                .filter(
                    pioneer_entity::artifact_version::Column::WorkspaceId
                        .eq(workspace_id.to_owned()),
                )
                .filter(
                    pioneer_entity::artifact_version::Column::ArtifactId.eq(artifact.id.clone()),
                )
                .one(transaction)
                .await
                .context("failed to revalidate exact Message artifact version")?
                .is_some();
        let scope = resolve_artifact_authorization_scope(
            transaction,
            artifact_id,
            Some(workspace_id),
            None,
        )
        .await?
        .filter(|scope| {
            scope.workspace_is_active
                && scope
                    .thread_id
                    .as_deref()
                    .map_or(true, |artifact_thread_id| artifact_thread_id == thread_id)
        });
        if !exact_version || scope.is_none() {
            return Err(anyhow::Error::new(
                TurnMessageMutationFailure::InvalidTarget,
            ));
        }
    }

    Ok(())
}

fn validate_turn_message_edit_shape(input: &[UserInput], mentions: &[TurnMention]) -> Result<()> {
    if pioneer_protocol::validate_turn_message_content(input, mentions.len()).is_err() {
        return Err(anyhow::Error::new(TurnMessageMutationFailure::InvalidInput));
    }
    let mut seen_mentions = HashSet::with_capacity(mentions.len());
    if mentions
        .iter()
        .any(|mention| !seen_mentions.insert(mention.principal_id.clone()))
    {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::InvalidTarget,
        ));
    }
    Ok(())
}

pub struct PreparedMessageEdit {
    pub event: TurnMessageEditedEvent,
    fence: MessageMutationFence,
}

pub async fn prepare_message_edit<C: ConnectionTrait>(
    db: &C,
    request: &EditTurnMessageRequest,
) -> Result<PreparedMessageEdit> {
    validate_turn_message_edit_shape(request.input.as_slice(), request.mentions.as_slice())?;
    let prepared_thread = thread::find_thread_by_id(db, request.thread_id.as_str())
        .await?
        .filter(|thread| thread.workspace_id == request.workspace_id)
        .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    let (prepared_turn, prepared_collaboration) = turn_writes::find_turn_collaboration(
        db,
        request.thread_id.as_str(),
        request.turn_id.as_str(),
    )
    .await?
    .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    if !prepared_collaboration.mode.message_mutation_eligible
        || prepared_turn.turn_kind != "conversation"
        || prepared_turn.origin != "user"
        || prepared_turn.status != "completed"
    {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::ImmutableMessage,
        ));
    }
    let original_author = find_turn_initiator(db, request.turn_id.as_str()).await?;
    if !matches!(&original_author, Some(PersistedActorRef::Principal(_))) {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::ImmutableMessage,
        ));
    }
    let current_actor_kind = message_mutation_actor_current_thread_write_kind(
        db,
        request.workspace_id.as_str(),
        request.thread_id.as_str(),
        &request.changed_by,
    )
    .await?
    .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    let author_edit = original_author.as_ref() == Some(&request.changed_by);
    let moderated =
        !author_edit && current_actor_kind == pioneer_protocol::PrincipalKind::Superuser;
    if !author_edit && !moderated {
        return Err(anyhow::Error::new(TurnMessageMutationFailure::Forbidden));
    }
    revalidate_turn_message_edit_targets(
        db,
        request.workspace_id.as_str(),
        request.thread_id.as_str(),
        &request.changed_by,
        request.input.as_slice(),
        request.mentions.as_slice(),
    )
    .await?;
    if prepared_collaboration.message_deleted {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::DeletedMessage,
        ));
    }
    if prepared_collaboration.message_revision != request.expected_revision {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::RevisionConflict,
        ));
    }
    let mut updated_turn = thread_snapshot_turn_from_db_model(prepared_turn.clone())?
        .context("edited Message no longer maps to a Turn")?;
    updated_turn.message_revision = request.expected_revision.saturating_add(1);
    updated_turn.mentions = request.mentions.clone();
    let event = TurnMessageEditedEvent {
        workspace_id: prepared_thread.workspace_id.clone(),
        thread_id: request.thread_id.clone(),
        turn: updated_turn,
        input: request.input.clone(),
        changed_by: request.changed_by.clone(),
        changed_at: request.changed_at_unix,
    };

    Ok(PreparedMessageEdit {
        event,
        fence: MessageMutationFence {
            thread: prepared_thread,
            turn: prepared_turn,
        },
    })
}
pub async fn revalidate_message_edit<C: ConnectionTrait>(
    transaction: &C,
    request: &EditTurnMessageRequest,
    prepared: &PreparedMessageEdit,
) -> Result<()> {
    let thread_model = thread::find_thread_by_id(transaction, request.thread_id.as_str())
        .await?
        .filter(|thread| thread.workspace_id == request.workspace_id)
        .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    let (turn_model, collaboration) = turn_writes::find_turn_collaboration(
        transaction,
        request.thread_id.as_str(),
        request.turn_id.as_str(),
    )
    .await?
    .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    if thread_model.id != prepared.fence.thread.id
        || thread_model.workspace_id != prepared.fence.thread.workspace_id
        || turn_model.updated_at != prepared.fence.turn.updated_at
    {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::RevisionConflict,
        ));
    }

    if !collaboration.mode.message_mutation_eligible
        || turn_model.turn_kind != "conversation"
        || turn_model.origin != "user"
        || turn_model.status != "completed"
    {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::ImmutableMessage,
        ));
    }
    let original_author = find_turn_initiator(transaction, request.turn_id.as_str()).await?;
    if !matches!(&original_author, Some(PersistedActorRef::Principal(_))) {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::ImmutableMessage,
        ));
    }
    let current_actor_kind = message_mutation_actor_current_thread_write_kind(
        transaction,
        request.workspace_id.as_str(),
        request.thread_id.as_str(),
        &request.changed_by,
    )
    .await?
    .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    let author_edit = original_author.as_ref() == Some(&request.changed_by);
    let moderated =
        !author_edit && current_actor_kind == pioneer_protocol::PrincipalKind::Superuser;
    if !author_edit && !moderated {
        return Err(anyhow::Error::new(TurnMessageMutationFailure::Forbidden));
    }
    revalidate_turn_message_edit_targets(
        transaction,
        request.workspace_id.as_str(),
        request.thread_id.as_str(),
        &request.changed_by,
        request.input.as_slice(),
        request.mentions.as_slice(),
    )
    .await?;
    if collaboration.message_deleted {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::DeletedMessage,
        ));
    }
    if collaboration.message_revision != request.expected_revision {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::RevisionConflict,
        ));
    }

    Ok(())
}

pub struct PreparedMessageDelete {
    pub event: TurnMessageDeletedEvent,
    fence: MessageMutationFence,
}

pub enum MessageDeletePreparation {
    Ready(PreparedMessageDelete),
    AlreadyDeleted(DeleteTurnMessageResult),
}

pub async fn prepare_message_delete<C: ConnectionTrait>(
    db: &C,
    request: &DeleteTurnMessageRequest,
) -> Result<MessageDeletePreparation> {
    let prepared_thread = thread::find_thread_by_id(db, request.thread_id.as_str())
        .await?
        .filter(|thread| thread.workspace_id == request.workspace_id)
        .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    let (prepared_turn, prepared_collaboration) = turn_writes::find_turn_collaboration(
        db,
        request.thread_id.as_str(),
        request.turn_id.as_str(),
    )
    .await?
    .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    if !prepared_collaboration.mode.message_mutation_eligible
        || prepared_turn.turn_kind != "conversation"
        || prepared_turn.origin != "user"
        || prepared_turn.status != "completed"
    {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::ImmutableMessage,
        ));
    }
    let original_author = find_turn_initiator(db, request.turn_id.as_str()).await?;
    if !matches!(&original_author, Some(PersistedActorRef::Principal(_))) {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::ImmutableMessage,
        ));
    }
    let current_actor_kind = message_mutation_actor_current_thread_write_kind(
        db,
        request.workspace_id.as_str(),
        request.thread_id.as_str(),
        &request.changed_by,
    )
    .await?
    .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    let author_delete = original_author.as_ref() == Some(&request.changed_by);
    let moderated =
        !author_delete && current_actor_kind == pioneer_protocol::PrincipalKind::Superuser;
    if !author_delete && !moderated {
        return Err(anyhow::Error::new(TurnMessageMutationFailure::Forbidden));
    }
    if prepared_collaboration.message_deleted {
        let duplicate = request.expected_revision == prepared_collaboration.message_revision
            || request.expected_revision.checked_add(1)
                == Some(prepared_collaboration.message_revision);
        if !duplicate {
            return Err(anyhow::Error::new(
                TurnMessageMutationFailure::RevisionConflict,
            ));
        }
        let turn = thread_snapshot_turn_from_db_model(prepared_turn)?
            .context("deleted Message no longer maps to a Turn")?;
        return Ok(MessageDeletePreparation::AlreadyDeleted(
            DeleteTurnMessageResult { turn, event: None },
        ));
    }
    if prepared_collaboration.message_revision != request.expected_revision {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::RevisionConflict,
        ));
    }
    let mut updated_turn = thread_snapshot_turn_from_db_model(prepared_turn.clone())?
        .context("deleted Message no longer maps to a Turn")?;
    updated_turn.message_revision = request.expected_revision.saturating_add(1);
    updated_turn.mentions.clear();
    updated_turn.message_deleted = true;
    let event = TurnMessageDeletedEvent {
        workspace_id: prepared_thread.workspace_id.clone(),
        thread_id: request.thread_id.clone(),
        turn: updated_turn.clone(),
        deleted_by: request.changed_by.clone(),
        deleted_at: request.changed_at_unix,
    };

    Ok(MessageDeletePreparation::Ready(PreparedMessageDelete {
        event,
        fence: MessageMutationFence {
            thread: prepared_thread,
            turn: prepared_turn,
        },
    }))
}
pub async fn revalidate_message_delete<C: ConnectionTrait>(
    transaction: &C,
    request: &DeleteTurnMessageRequest,
    prepared: &PreparedMessageDelete,
) -> Result<Option<DeleteTurnMessageResult>> {
    let thread_model = thread::find_thread_by_id(transaction, request.thread_id.as_str())
        .await?
        .filter(|thread| thread.workspace_id == request.workspace_id)
        .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    let (turn_model, collaboration) = turn_writes::find_turn_collaboration(
        transaction,
        request.thread_id.as_str(),
        request.turn_id.as_str(),
    )
    .await?
    .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    if thread_model.id != prepared.fence.thread.id
        || thread_model.workspace_id != prepared.fence.thread.workspace_id
        || turn_model.updated_at != prepared.fence.turn.updated_at
    {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::RevisionConflict,
        ));
    }

    if !collaboration.mode.message_mutation_eligible
        || turn_model.turn_kind != "conversation"
        || turn_model.origin != "user"
        || turn_model.status != "completed"
    {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::ImmutableMessage,
        ));
    }
    let original_author = find_turn_initiator(transaction, request.turn_id.as_str()).await?;
    if !matches!(&original_author, Some(PersistedActorRef::Principal(_))) {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::ImmutableMessage,
        ));
    }
    let current_actor_kind = message_mutation_actor_current_thread_write_kind(
        transaction,
        request.workspace_id.as_str(),
        request.thread_id.as_str(),
        &request.changed_by,
    )
    .await?
    .ok_or_else(|| anyhow::Error::new(TurnMessageMutationFailure::NotFound))?;
    let author_delete = original_author.as_ref() == Some(&request.changed_by);
    let moderated =
        !author_delete && current_actor_kind == pioneer_protocol::PrincipalKind::Superuser;
    if !author_delete && !moderated {
        return Err(anyhow::Error::new(TurnMessageMutationFailure::Forbidden));
    }

    if collaboration.message_deleted {
        let duplicate = request.expected_revision == collaboration.message_revision
            || request.expected_revision.checked_add(1) == Some(collaboration.message_revision);
        if !duplicate {
            return Err(anyhow::Error::new(
                TurnMessageMutationFailure::RevisionConflict,
            ));
        }
        let turn = thread_snapshot_turn_from_db_model(turn_model)?
            .context("deleted Message no longer maps to a Turn")?;
        return Ok(Some(DeleteTurnMessageResult { turn, event: None }));
    }
    if collaboration.message_revision != request.expected_revision {
        return Err(anyhow::Error::new(
            TurnMessageMutationFailure::RevisionConflict,
        ));
    }

    Ok(None)
}
