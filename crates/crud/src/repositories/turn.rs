use anyhow::{Context, Result};
use pioneer_entity::{turn, turn_input, turn_item, turn_message_revision, turn_status_history};
use pioneer_protocol::{
    PersistedActorRef, Turn, TurnAuthorSnapshot, TurnExecutionSecuritySnapshot, TurnItem, TurnKind,
    TurnMention, TurnMessageRevision, TurnMessageRevisionChangeKind, TurnStatus, UserInput,
    generate_id,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, Statement,
};

use crate::convention::{
    PersistedTurnSendMode, canonical_turn_mentions_json, input_type_and_text,
    turn_item_id_and_type_to_db, turn_kind_to_db, turn_mentions_from_db, turn_origin_to_db,
    turn_permission_mode_to_db, turn_permission_profile_source_to_db, turn_send_mode_from_db,
    turn_send_mode_to_db, turn_status_to_db, validate_turn_author_snapshot,
};
use crate::repositories::identity::{actor_ref_from_db, actor_ref_to_db};

const DB_ID_LEN: usize = 21;
const TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES: usize = 1_048_576;
const TURN_AGENT_AUTHOR_SNAPSHOT_JSON_MAX_BYTES: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedTurnCollaboration {
    pub mode: PersistedTurnSendMode,
    pub author: Option<TurnAuthorSnapshot>,
    pub reply_to_turn_id: Option<String>,
    pub mentions: Vec<TurnMention>,
    pub message_revision: u64,
    pub message_deleted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewTurnMessageRevision<'a> {
    pub turn_id: &'a str,
    pub revision: u64,
    pub input: &'a [UserInput],
    pub mentions: &'a [TurnMention],
    pub changed_by: &'a PersistedActorRef,
    pub change_kind: TurnMessageRevisionChangeKind,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnPromptManifestColumns {
    pub prompt_manifest_json: String,
    pub prompt_compiler_version: String,
    pub prompt_profile: String,
    pub prompt_fingerprint_stable: String,
    pub prompt_fingerprint_dynamic: String,
    pub prompt_fingerprint_full: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TurnPermissionProfileColumns {
    mode: String,
    source: String,
    snapshot_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionSecuritySnapshotRecord {
    pub turn_id: String,
    pub version: i64,
    pub snapshot: TurnExecutionSecuritySnapshot,
}

pub async fn find_turn_by_id<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<turn::Model>> {
    let existing = turn::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query turn by id")?;
    Ok(existing)
}

pub async fn find_turn_by_thread_and_id<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .one(db)
        .await
        .context("failed to query turn by thread and id")
}

pub async fn find_turns_by_thread_and_ids<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_ids: &[String],
) -> Result<Vec<turn::Model>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }

    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Id.is_in(turn_ids.iter().cloned()))
        .all(db)
        .await
        .context("failed to query turns by thread and ids")
}

pub async fn has_in_progress_conversation_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<bool> {
    let count = turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Status.eq(turn_status_to_db(TurnStatus::InProgress)))
        .filter(turn::Column::TurnKind.eq(turn_kind_to_db(TurnKind::Conversation)))
        .count(db)
        .await
        .context("failed to count in-progress conversation turns")?;
    Ok(count > 0)
}

pub async fn upsert_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    thread_id: &str,
    turn_model: &Turn,
    prompt_manifest: Option<&TurnPromptManifestColumns>,
    reasoning_effort: Option<&str>,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    upsert_turn_with_actor_columns(
        db,
        turn_id,
        thread_id,
        turn_model,
        prompt_manifest,
        reasoning_effort,
        None,
        None,
        false,
        created_at,
        updated_at,
    )
    .await
}

pub async fn upsert_turn_with_initiator<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    thread_id: &str,
    turn_model: &Turn,
    prompt_manifest: Option<&TurnPromptManifestColumns>,
    reasoning_effort: Option<&str>,
    initiator: &PersistedActorRef,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    if let Some(existing) = find_turn_by_id(db, turn_id).await? {
        let existing_actor = actor_ref_from_db(
            existing.initiated_by_actor_kind.as_deref(),
            existing.initiated_by_actor_id.as_deref(),
        )
        .with_context(|| format!("turn `{turn_id}` has an invalid persisted initiator pair"))?;
        let Some(existing_actor) = existing_actor else {
            anyhow::bail!("turn `{turn_id}` is missing its persisted initiator");
        };
        if &existing_actor != initiator {
            anyhow::bail!("turn `{turn_id}` already has a different persisted initiator");
        }
    }
    let (actor_kind, actor_id) = actor_ref_to_db(initiator);
    upsert_turn_with_actor_columns(
        db,
        turn_id,
        thread_id,
        turn_model,
        prompt_manifest,
        reasoning_effort,
        actor_kind,
        actor_id,
        true,
        created_at,
        updated_at,
    )
    .await
}

pub async fn find_turn_initiator<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<PersistedActorRef>> {
    let Some(turn) = find_turn_by_id(db, turn_id).await? else {
        return Ok(None);
    };
    actor_ref_from_db(
        turn.initiated_by_actor_kind.as_deref(),
        turn.initiated_by_actor_id.as_deref(),
    )
    .with_context(|| format!("turn `{turn_id}` has an invalid persisted initiator pair"))
}

#[allow(clippy::too_many_arguments)]
async fn upsert_turn_with_actor_columns<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    thread_id: &str,
    turn_model: &Turn,
    prompt_manifest: Option<&TurnPromptManifestColumns>,
    reasoning_effort: Option<&str>,
    initiated_by_actor_kind: Option<String>,
    initiated_by_actor_id: Option<String>,
    update_initiator: bool,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    let existing_collaboration = find_turn_by_id(db, turn_id).await?;
    let preserve_legacy_null_mode = existing_collaboration
        .as_ref()
        .is_some_and(|existing| existing.send_mode.is_none());
    let mut update_columns = vec![
        turn::Column::ThreadId,
        turn::Column::Status,
        turn::Column::TurnKind,
        turn::Column::Origin,
        turn::Column::Error,
        turn::Column::UpdatedAt,
        turn::Column::SendMode,
        turn::Column::ReplyToTurnId,
        turn::Column::MentionsJson,
        turn::Column::MessageRevision,
    ];
    update_columns.extend([
        turn::Column::PermissionProfileMode,
        turn::Column::PermissionProfileSource,
        turn::Column::PermissionProfileSnapshotJson,
    ]);
    if update_initiator {
        update_columns.extend([
            turn::Column::InitiatedByActorKind,
            turn::Column::InitiatedByActorId,
        ]);
    }
    let permission_profile_columns = build_turn_permission_profile_columns(turn_model)?;
    let supplied_initiator = actor_ref_from_db(
        initiated_by_actor_kind.as_deref(),
        initiated_by_actor_id.as_deref(),
    )?;
    let effective_initiator = if supplied_initiator.is_some() {
        supplied_initiator
    } else if let Some(existing) = existing_collaboration.as_ref() {
        actor_ref_from_db(
            existing.initiated_by_actor_kind.as_deref(),
            existing.initiated_by_actor_id.as_deref(),
        )
        .with_context(|| format!("turn `{turn_id}` has an invalid persisted initiator pair"))?
    } else {
        None
    };
    if let Some(author) = turn_model.author.as_ref() {
        if effective_initiator.as_ref() != Some(&author.actor) {
            anyhow::bail!("Turn author snapshot does not match its persisted initiator");
        }
    }
    if let Some(existing) = existing_collaboration.as_ref()
        && existing.send_mode.is_some()
    {
        let persisted = collaboration_from_model(existing).with_context(|| {
            format!("turn `{turn_id}` has invalid immutable collaboration facts")
        })?;
        if turn_model.author.is_some() && persisted.author != turn_model.author {
            anyhow::bail!("Turn author snapshot cannot change after initial persistence");
        }
    }
    let author = turn_model
        .author
        .as_ref()
        .map(|snapshot| {
            validate_turn_author_snapshot(snapshot)?;
            let agent_json = snapshot
                .agent
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .context("failed to encode Turn agent author snapshot")?;
            if agent_json
                .as_ref()
                .is_some_and(|json| json.len() > TURN_AGENT_AUTHOR_SNAPSHOT_JSON_MAX_BYTES)
            {
                anyhow::bail!(
                    "Turn agent author snapshot exceeds {TURN_AGENT_AUTHOR_SNAPSHOT_JSON_MAX_BYTES} bytes"
                );
            }
            Ok::<_, anyhow::Error>((
                Some(snapshot.display_name.clone()),
                Some(snapshot.nickname.clone()),
                snapshot.avatar_revision.clone(),
                agent_json,
            ))
        })
        .transpose()?
        .unwrap_or((None, None, None, None));
    let mentions_json = canonical_turn_mentions_json(&turn_model.mentions)?;
    let message_revision = i64::try_from(turn_model.message_revision)
        .context("Turn message revision exceeds database integer range")?;
    let (
        send_mode,
        author_display_name_snapshot,
        author_nickname_snapshot,
        author_avatar_revision_snapshot,
        author_agent_snapshot_json,
        reply_to_turn_id,
        mentions_json,
        message_revision,
    ) = if preserve_legacy_null_mode {
        let existing = existing_collaboration
            .as_ref()
            .expect("legacy preservation requires an existing Turn");
        (
            None,
            existing.author_display_name_snapshot.clone(),
            existing.author_nickname_snapshot.clone(),
            existing.author_avatar_revision_snapshot.clone(),
            existing.author_agent_snapshot_json.clone(),
            existing.reply_to_turn_id.clone(),
            existing.mentions_json.clone(),
            existing.message_revision,
        )
    } else {
        (
            Some(turn_send_mode_to_db(turn_model.mode).to_owned()),
            author.0,
            author.1,
            author.2,
            author.3,
            turn_model.reply_to_turn_id.clone(),
            mentions_json,
            message_revision,
        )
    };

    turn::Entity::insert(turn::ActiveModel {
        id: Set(turn_id.to_owned()),
        thread_id: Set(thread_id.to_owned()),
        initiated_by_actor_id: Set(initiated_by_actor_id),
        initiated_by_actor_kind: Set(initiated_by_actor_kind),
        status: Set(turn_status_to_db(turn_model.status).to_owned()),
        turn_kind: Set(turn_kind_to_db(turn_model.turn_kind).to_owned()),
        origin: Set(turn_origin_to_db(turn_model.origin).to_owned()),
        error: Set(turn_model.error.clone()),
        prompt_manifest_json: Set(prompt_manifest
            .map(|manifest| manifest.prompt_manifest_json.clone())
            .unwrap_or_else(|| "{}".to_owned())),
        prompt_compiler_version: Set(
            prompt_manifest.map(|manifest| manifest.prompt_compiler_version.clone())
        ),
        prompt_profile: Set(prompt_manifest.map(|manifest| manifest.prompt_profile.clone())),
        prompt_fingerprint_stable: Set(
            prompt_manifest.map(|manifest| manifest.prompt_fingerprint_stable.clone())
        ),
        prompt_fingerprint_dynamic: Set(
            prompt_manifest.map(|manifest| manifest.prompt_fingerprint_dynamic.clone())
        ),
        prompt_fingerprint_full: Set(
            prompt_manifest.map(|manifest| manifest.prompt_fingerprint_full.clone())
        ),
        reasoning_effort: Set(reasoning_effort.map(str::to_owned)),
        permission_profile_mode: Set(Some(permission_profile_columns.mode)),
        permission_profile_source: Set(Some(permission_profile_columns.source)),
        permission_profile_snapshot_json: Set(Some(permission_profile_columns.snapshot_json)),
        execution_security_snapshot_version: Set(None),
        execution_security_snapshot_json: Set(None),
        execution_authorization_context_json: Set(None),
        send_mode: Set(send_mode),
        author_display_name_snapshot: Set(author_display_name_snapshot),
        author_nickname_snapshot: Set(author_nickname_snapshot),
        author_avatar_revision_snapshot: Set(author_avatar_revision_snapshot),
        author_agent_snapshot_json: Set(author_agent_snapshot_json),
        reply_to_turn_id: Set(reply_to_turn_id),
        mentions_json: Set(mentions_json),
        message_revision: Set(message_revision),
        message_deleted_at: Set(None),
        message_deleted_by_actor_id: Set(None),
        message_deleted_by_actor_kind: Set(None),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::column(turn::Column::Id)
            .update_columns(update_columns)
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert turn")?;

    Ok(())
}

pub fn collaboration_from_model(model: &turn::Model) -> Result<PersistedTurnCollaboration> {
    let mode = turn_send_mode_from_db(model.send_mode.as_deref())?;
    let actor = actor_ref_from_db(
        model.initiated_by_actor_kind.as_deref(),
        model.initiated_by_actor_id.as_deref(),
    )
    .context("Turn has an invalid persisted initiator pair")?;
    if model.author_agent_snapshot_json.is_some()
        && (!matches!(actor.as_ref(), Some(PersistedActorRef::AgentExecution(_)))
            || model.author_display_name_snapshot.is_none()
            || model.author_nickname_snapshot.is_none())
    {
        anyhow::bail!("Turn has an agent author snapshot outside a complete AgentExecution author");
    }
    let author = match (
        actor,
        model.author_display_name_snapshot.as_ref(),
        model.author_nickname_snapshot.as_ref(),
    ) {
        (Some(actor), Some(display_name), Some(nickname)) => {
            let agent = model
                .author_agent_snapshot_json
                .as_deref()
                .map(|json| {
                    if json.len() > TURN_AGENT_AUTHOR_SNAPSHOT_JSON_MAX_BYTES {
                        anyhow::bail!(
                            "persisted Turn agent author snapshot exceeds {TURN_AGENT_AUTHOR_SNAPSHOT_JSON_MAX_BYTES} bytes"
                        );
                    }
                    serde_json::from_str(json)
                        .context("failed to decode persisted Turn agent author snapshot")
                })
                .transpose()?;
            let snapshot = TurnAuthorSnapshot {
                actor,
                display_name: display_name.clone(),
                nickname: nickname.clone(),
                avatar_revision: model.author_avatar_revision_snapshot.clone(),
                agent,
            };
            validate_turn_author_snapshot(&snapshot)?;
            Some(snapshot)
        }
        (None, None, None) => None,
        (Some(actor), None, None) => {
            let (display_name, nickname) = match &actor {
                PersistedActorRef::Principal(principal_id) => {
                    (principal_id.to_string(), principal_id.to_string())
                }
                PersistedActorRef::AgentExecution(execution_id) => {
                    (execution_id.to_string(), format!("agent-{}", execution_id))
                }
                PersistedActorRef::System => ("System".to_owned(), "system".to_owned()),
            };
            Some(TurnAuthorSnapshot {
                actor,
                display_name,
                nickname,
                avatar_revision: None,
                agent: None,
            })
        }
        _ => anyhow::bail!("Turn has an incomplete persisted author snapshot"),
    };
    let message_revision = u64::try_from(model.message_revision)
        .context("Turn has a negative persisted message revision")?;
    Ok(PersistedTurnCollaboration {
        mode,
        author,
        reply_to_turn_id: model.reply_to_turn_id.clone(),
        mentions: turn_mentions_from_db(model.mentions_json.as_str())?,
        message_revision,
        message_deleted: model.message_deleted_at.is_some(),
    })
}

pub async fn find_turn_collaboration<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<(turn::Model, PersistedTurnCollaboration)>> {
    let Some(model) = find_turn_by_thread_and_id(db, thread_id, turn_id).await? else {
        return Ok(None);
    };
    let collaboration = collaboration_from_model(&model)?;
    Ok(Some((model, collaboration)))
}

pub async fn insert_turn_message_revision<C: ConnectionTrait>(
    db: &C,
    revision: NewTurnMessageRevision<'_>,
) -> Result<()> {
    let revision_number = i64::try_from(revision.revision)
        .context("Turn message revision exceeds database integer range")?;
    let input_json = serde_json::to_string(revision.input)
        .context("failed to encode Turn message revision input")?;
    if input_json.len() > TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES {
        anyhow::bail!(
            "Turn message revision input exceeds {TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES} bytes"
        );
    }
    let mentions_json = canonical_turn_mentions_json(revision.mentions)?;
    let (changed_by_actor_kind, changed_by_actor_id) = actor_ref_to_db(revision.changed_by);
    let changed_by_actor_kind =
        changed_by_actor_kind.context("Turn message revision actor kind must be persisted")?;
    let change_kind = match revision.change_kind {
        TurnMessageRevisionChangeKind::Edit => "edit",
        TurnMessageRevisionChangeKind::Delete => "delete",
    };
    turn_message_revision::Entity::insert(turn_message_revision::ActiveModel {
        turn_id: Set(revision.turn_id.to_owned()),
        revision: Set(revision_number),
        input_json: Set(input_json),
        mentions_json: Set(mentions_json),
        changed_by_actor_kind: Set(changed_by_actor_kind),
        changed_by_actor_id: Set(changed_by_actor_id),
        change_kind: Set(change_kind.to_owned()),
        created_at: Set(revision.created_at),
    })
    .exec(db)
    .await
    .context("failed to insert Turn message revision")?;
    Ok(())
}

pub async fn insert_turn_message_revision_if_absent<C: ConnectionTrait>(
    db: &C,
    revision: NewTurnMessageRevision<'_>,
) -> Result<()> {
    let revision_number = i64::try_from(revision.revision)
        .context("Turn message revision exceeds database integer range")?;
    let input_json = serde_json::to_string(revision.input)
        .context("failed to encode Turn message revision input")?;
    if input_json.len() > TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES {
        anyhow::bail!(
            "Turn message revision input exceeds {TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES} bytes"
        );
    }
    let mentions_json = canonical_turn_mentions_json(revision.mentions)?;
    let (changed_by_actor_kind, changed_by_actor_id) = actor_ref_to_db(revision.changed_by);
    let changed_by_actor_kind =
        changed_by_actor_kind.context("Turn message revision actor kind must be persisted")?;
    let change_kind = match revision.change_kind {
        TurnMessageRevisionChangeKind::Edit => "edit",
        TurnMessageRevisionChangeKind::Delete => "delete",
    };
    turn_message_revision::Entity::insert(turn_message_revision::ActiveModel {
        turn_id: Set(revision.turn_id.to_owned()),
        revision: Set(revision_number),
        input_json: Set(input_json),
        mentions_json: Set(mentions_json),
        changed_by_actor_kind: Set(changed_by_actor_kind),
        changed_by_actor_id: Set(changed_by_actor_id),
        change_kind: Set(change_kind.to_owned()),
        created_at: Set(revision.created_at),
    })
    .on_conflict(
        OnConflict::columns([
            turn_message_revision::Column::TurnId,
            turn_message_revision::Column::Revision,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to idempotently insert Turn message revision")?;
    Ok(())
}

pub async fn list_turn_message_revisions<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    before_revision: Option<u64>,
    limit: u64,
) -> Result<Vec<turn_message_revision::Model>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut query = turn_message_revision::Entity::find()
        .filter(turn_message_revision::Column::TurnId.eq(turn_id.to_owned()));
    if let Some(before_revision) = before_revision {
        let before_revision = i64::try_from(before_revision)
            .context("Turn message revision cursor exceeds database integer range")?;
        query = query.filter(turn_message_revision::Column::Revision.lt(before_revision));
    }
    query
        .order_by_desc(turn_message_revision::Column::Revision)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list Turn message revisions")
}

pub fn turn_message_revision_from_model(
    model: turn_message_revision::Model,
) -> Result<TurnMessageRevision> {
    let revision = u64::try_from(model.revision)
        .context("Turn message revision has a negative persisted revision")?;
    let changed_by = actor_ref_from_db(
        Some(model.changed_by_actor_kind.as_str()),
        model.changed_by_actor_id.as_deref(),
    )?
    .context("Turn message revision is missing its persisted actor")?;
    let change_kind = match model.change_kind.as_str() {
        "edit" => TurnMessageRevisionChangeKind::Edit,
        "delete" => TurnMessageRevisionChangeKind::Delete,
        unknown => anyhow::bail!("unknown Turn message revision change kind `{unknown}`"),
    };
    if model.input_json.len() > TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES {
        anyhow::bail!(
            "persisted Turn message revision input exceeds {TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES} bytes"
        );
    }
    let input = serde_json::from_str::<Vec<UserInput>>(model.input_json.as_str())
        .context("failed to decode Turn message revision input")?;
    Ok(TurnMessageRevision {
        turn_id: model.turn_id,
        revision,
        change_kind,
        changed_by,
        created_at: model.created_at.timestamp(),
        input: Some(input),
        mentions: turn_mentions_from_db(model.mentions_json.as_str())?,
    })
}

pub async fn set_turn_execution_security_snapshot<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    snapshot: &TurnExecutionSecuritySnapshot,
) -> Result<bool> {
    let version = i32::try_from(snapshot.version)
        .context("turn execution security snapshot version exceeds database integer range")?;
    let snapshot_json = serde_json::to_string(snapshot)
        .context("failed to serialize turn execution security snapshot to json")?;

    let update_result = turn::Entity::update_many()
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .col_expr(
            turn::Column::ExecutionSecuritySnapshotVersion,
            Expr::value(version),
        )
        .col_expr(
            turn::Column::ExecutionSecuritySnapshotJson,
            Expr::value(snapshot_json),
        )
        .exec(db)
        .await
        .context("failed to update turn execution security snapshot")?;

    Ok(update_result.rows_affected > 0)
}

pub async fn set_turn_execution_envelope<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    snapshot: &TurnExecutionSecuritySnapshot,
    authorization_context_json: &str,
) -> Result<bool> {
    let version = i32::try_from(snapshot.version)
        .context("turn execution security snapshot version exceeds database integer range")?;
    let snapshot_json = serde_json::to_string(snapshot)
        .context("failed to serialize turn execution security snapshot to json")?;
    let authorization_context_json = authorization_context_json.trim();
    if authorization_context_json.is_empty() {
        anyhow::bail!("execution authorization context must not be empty");
    }
    let update_result = turn::Entity::update_many()
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .col_expr(
            turn::Column::ExecutionSecuritySnapshotVersion,
            Expr::value(version),
        )
        .col_expr(
            turn::Column::ExecutionSecuritySnapshotJson,
            Expr::value(snapshot_json),
        )
        .col_expr(
            turn::Column::ExecutionAuthorizationContextJson,
            Expr::value(authorization_context_json.to_owned()),
        )
        .exec(db)
        .await
        .context("failed to update turn execution envelope")?;
    Ok(update_result.rows_affected > 0)
}

pub async fn find_turn_execution_security_snapshot<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<TurnExecutionSecuritySnapshotRecord>> {
    let Some(model) = find_turn_by_id(db, turn_id).await? else {
        return Ok(None);
    };
    let Some(snapshot_json) = model
        .execution_security_snapshot_json
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "{}" && *value != "null")
    else {
        return Ok(None);
    };

    let snapshot = serde_json::from_str::<TurnExecutionSecuritySnapshot>(snapshot_json)
        .with_context(|| {
            format!("failed to decode execution security snapshot for turn `{turn_id}`")
        })?;
    let version = model
        .execution_security_snapshot_version
        .unwrap_or_else(|| i64::from(snapshot.version));

    Ok(Some(TurnExecutionSecuritySnapshotRecord {
        turn_id: model.id,
        version,
        snapshot,
    }))
}

pub async fn set_turn_execution_authorization_context<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    context_json: &str,
) -> Result<bool> {
    let context_json = context_json.trim();
    if context_json.is_empty() {
        anyhow::bail!("execution authorization context must not be empty");
    }
    let update_result = turn::Entity::update_many()
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .col_expr(
            turn::Column::ExecutionAuthorizationContextJson,
            Expr::value(context_json.to_owned()),
        )
        .exec(db)
        .await
        .context("failed to update turn execution authorization context")?;
    Ok(update_result.rows_affected > 0)
}

pub async fn find_turn_execution_authorization_context<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<String>> {
    Ok(find_turn_by_id(db, turn_id)
        .await?
        .and_then(|model| model.execution_authorization_context_json)
        .filter(|json| !json.trim().is_empty()))
}

fn build_turn_permission_profile_columns(
    turn_model: &Turn,
) -> Result<TurnPermissionProfileColumns> {
    let snapshot = &turn_model.permission_profile;
    let snapshot_json = serde_json::to_string(&snapshot)
        .context("failed to serialize turn permission profile snapshot to json")?;

    Ok(TurnPermissionProfileColumns {
        mode: turn_permission_mode_to_db(snapshot.mode).to_owned(),
        source: turn_permission_profile_source_to_db(snapshot.source).to_owned(),
        snapshot_json,
    })
}

pub async fn update_turn_prompt_manifest<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    prompt_manifest: &TurnPromptManifestColumns,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let update_result = turn::Entity::update_many()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .col_expr(
            turn::Column::PromptManifestJson,
            Expr::value(prompt_manifest.prompt_manifest_json.clone()),
        )
        .col_expr(
            turn::Column::PromptCompilerVersion,
            Expr::value(prompt_manifest.prompt_compiler_version.clone()),
        )
        .col_expr(
            turn::Column::PromptProfile,
            Expr::value(prompt_manifest.prompt_profile.clone()),
        )
        .col_expr(
            turn::Column::PromptFingerprintStable,
            Expr::value(prompt_manifest.prompt_fingerprint_stable.clone()),
        )
        .col_expr(
            turn::Column::PromptFingerprintDynamic,
            Expr::value(prompt_manifest.prompt_fingerprint_dynamic.clone()),
        )
        .col_expr(
            turn::Column::PromptFingerprintFull,
            Expr::value(prompt_manifest.prompt_fingerprint_full.clone()),
        )
        .col_expr(turn::Column::UpdatedAt, Expr::value(updated_at))
        .exec(db)
        .await
        .context("failed to update turn prompt manifest")?;

    Ok(update_result.rows_affected > 0)
}

pub async fn update_turn_status<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    status: TurnStatus,
    error: Option<&str>,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let update_result = turn::Entity::update_many()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .col_expr(
            turn::Column::Status,
            Expr::value(turn_status_to_db(status).to_owned()),
        )
        .col_expr(turn::Column::Error, Expr::value(error.map(str::to_owned)))
        .col_expr(turn::Column::UpdatedAt, Expr::value(updated_at))
        .exec(db)
        .await
        .context("failed to update turn status")?;

    Ok(update_result.rows_affected > 0)
}

pub async fn replace_turn_input<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    input: &[UserInput],
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    turn_input::Entity::delete_many()
        .filter(turn_input::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to clear turn input rows before projection")?;

    for (index, item) in input.iter().enumerate() {
        let (input_type, text) = input_type_and_text(item);
        let payload_json =
            serde_json::to_string(item).context("failed to serialize turn input payload")?;

        turn_input::Entity::insert(turn_input::ActiveModel {
            id: Set(generate_id(DB_ID_LEN)),
            turn_id: Set(turn_id.to_owned()),
            input_index: Set(i64::try_from(index).unwrap_or(i64::MAX)),
            input_type: Set(input_type.to_owned()),
            text: Set(text),
            payload: Set(payload_json),
            created_at: Set(created_at),
        })
        .exec(db)
        .await
        .context("failed to insert projected turn input row")?;
    }

    Ok(())
}

pub async fn compare_and_set_message_turn_mutation<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    expected_revision: u64,
    mentions: &[TurnMention],
    deleted_by: Option<&PersistedActorRef>,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let expected_revision = i64::try_from(expected_revision)
        .context("expected Turn message revision exceeds database integer range")?;
    let next_revision = expected_revision
        .checked_add(1)
        .context("Turn message revision exceeds database integer range")?;
    let mentions_json = canonical_turn_mentions_json(mentions)?;
    let (deleted_by_actor_kind, deleted_by_actor_id) = match deleted_by {
        Some(actor) => actor_ref_to_db(actor),
        None => (None, None),
    };

    let result = turn::Entity::update_many()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .filter(turn::Column::SendMode.eq("message"))
        .filter(turn::Column::MessageRevision.eq(expected_revision))
        .filter(turn::Column::MessageDeletedAt.is_null())
        .col_expr(turn::Column::MentionsJson, Expr::value(mentions_json))
        .col_expr(turn::Column::MessageRevision, Expr::value(next_revision))
        .col_expr(
            turn::Column::MessageDeletedAt,
            Expr::value(deleted_by.map(|_| updated_at)),
        )
        .col_expr(
            turn::Column::MessageDeletedByActorKind,
            Expr::value(deleted_by_actor_kind),
        )
        .col_expr(
            turn::Column::MessageDeletedByActorId,
            Expr::value(deleted_by_actor_id),
        )
        .col_expr(turn::Column::UpdatedAt, Expr::value(updated_at))
        .exec(db)
        .await
        .context("failed to compare-and-set Turn message mutation")?;

    Ok(result.rows_affected == 1)
}

pub async fn project_message_turn_mutation_state<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    revision: u64,
    mentions: &[TurnMention],
    deleted_by: Option<&PersistedActorRef>,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let revision = i64::try_from(revision)
        .context("projected Turn message revision exceeds database integer range")?;
    let mentions_json = canonical_turn_mentions_json(mentions)?;
    let (deleted_by_actor_kind, deleted_by_actor_id) = match deleted_by {
        Some(actor) => actor_ref_to_db(actor),
        None => (None, None),
    };
    let result = turn::Entity::update_many()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .filter(turn::Column::SendMode.eq("message"))
        .filter(turn::Column::MessageRevision.lte(revision))
        .col_expr(turn::Column::MentionsJson, Expr::value(mentions_json))
        .col_expr(turn::Column::MessageRevision, Expr::value(revision))
        .col_expr(
            turn::Column::MessageDeletedAt,
            Expr::value(deleted_by.map(|_| updated_at)),
        )
        .col_expr(
            turn::Column::MessageDeletedByActorKind,
            Expr::value(deleted_by_actor_kind),
        )
        .col_expr(
            turn::Column::MessageDeletedByActorId,
            Expr::value(deleted_by_actor_id),
        )
        .col_expr(turn::Column::UpdatedAt, Expr::value(updated_at))
        .exec(db)
        .await
        .context("failed to project Turn message mutation state")?;
    Ok(result.rows_affected == 1)
}

pub async fn append_turn_status_history<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    status: TurnStatus,
    error: Option<String>,
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    turn_status_history::Entity::insert(turn_status_history::ActiveModel {
        id: Set(generate_id(DB_ID_LEN)),
        turn_id: Set(turn_id.to_owned()),
        status: Set(turn_status_to_db(status).to_owned()),
        error: Set(error),
        created_at: Set(created_at),
    })
    .exec(db)
    .await
    .context("failed to append turn status history")?;

    Ok(())
}

pub async fn upsert_turn_item<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item: &TurnItem,
    status: Option<&str>,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    let (item_id, item_type) = turn_item_id_and_type_to_db(item);
    let payload_json =
        serde_json::to_string(item).context("failed to serialize turn item payload")?;

    if db.get_database_backend() == DatabaseBackend::Sqlite {
        return upsert_turn_item_sqlite_compatible(
            db,
            turn_id,
            item_id,
            item_type,
            status,
            payload_json.as_str(),
            created_at,
            updated_at,
        )
        .await;
    }

    turn_item::Entity::insert(turn_item::ActiveModel {
        id: Set(generate_id(DB_ID_LEN)),
        turn_id: Set(turn_id.to_owned()),
        item_id: Set(item_id.to_owned()),
        item_type: Set(item_type.to_owned()),
        status: Set(status.map(str::to_owned)),
        payload: Set(payload_json),
        active_attempt_number: Set(0),
        active_attempt_status: Set(None),
        active_attempt_id: Set(None),
        last_heartbeat_at: Set(None),
        lease_expires_at: Set(None),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::columns([turn_item::Column::TurnId, turn_item::Column::ItemId])
            .update_columns([
                turn_item::Column::ItemType,
                turn_item::Column::Status,
                turn_item::Column::Payload,
                turn_item::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert turn item")?;

    Ok(())
}

async fn upsert_turn_item_sqlite_compatible<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
    item_type: &str,
    status: Option<&str>,
    payload_json: &str,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    if sqlite_turn_item_exists(db, turn_id, item_id).await? {
        db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            r#"
                UPDATE turn_item
                SET
                    item_type = ?,
                    status = ?,
                    payload = ?,
                    updated_at = ?
                WHERE turn_id = ? AND item_id = ?
            "#,
            vec![
                item_type.to_owned().into(),
                status.map(str::to_owned).into(),
                payload_json.to_owned().into(),
                updated_at.into(),
                turn_id.to_owned().into(),
                item_id.to_owned().into(),
            ],
        ))
        .await
        .context("failed to update turn item")?;
        return Ok(());
    }

    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        r#"
            INSERT INTO turn_item (
                id,
                turn_id,
                item_id,
                item_type,
                status,
                payload,
                active_attempt_number,
                active_attempt_status,
                active_attempt_id,
                last_heartbeat_at,
                lease_expires_at,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, ?, ?)
        "#,
        vec![
            generate_id(DB_ID_LEN).into(),
            turn_id.to_owned().into(),
            item_id.to_owned().into(),
            item_type.to_owned().into(),
            status.map(str::to_owned).into(),
            payload_json.to_owned().into(),
            created_at.into(),
            updated_at.into(),
        ],
    ))
    .await
    .context("failed to insert turn item")?;

    Ok(())
}

async fn sqlite_turn_item_exists<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<bool> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            r#"
                SELECT COUNT(*) AS value
                FROM turn_item
                WHERE turn_id = ? AND item_id = ?
            "#,
            [turn_id.to_owned().into(), item_id.to_owned().into()],
        ))
        .await
        .context("failed to query turn item before upsert")?
        .context("turn item existence query unexpectedly returned no rows")?;
    let count = row
        .try_get::<i64>("", "value")
        .context("failed to decode turn item existence count")?;
    Ok(count > 0)
}

pub async fn find_turn_item_type<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<String>> {
    let row = turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemId.eq(item_id.to_owned()))
        .one(db)
        .await
        .context("failed to query turn item type")?;
    Ok(row.map(|row| row.item_type))
}

pub async fn find_turn_item<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<turn_item::Model>> {
    turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemId.eq(item_id.to_owned()))
        .one(db)
        .await
        .context("failed to query turn item row")
}

pub async fn list_turn_items_by_ids<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_ids: &[String],
) -> Result<Vec<turn_item::Model>> {
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }

    turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemId.is_in(item_ids.iter().cloned()))
        .all(db)
        .await
        .context("failed to query turn item rows by id")
}

pub async fn list_turn_items_by_type<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_type: &str,
) -> Result<Vec<turn_item::Model>> {
    turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemType.eq(item_type.to_owned()))
        .order_by_asc(turn_item::Column::CreatedAt)
        .order_by_asc(turn_item::Column::ItemId)
        .all(db)
        .await
        .context("failed to query turn item rows by type")
}

pub async fn list_turn_items_by_type_for_turns<C: ConnectionTrait>(
    db: &C,
    turn_ids: &[String],
    item_type: &str,
) -> Result<Vec<turn_item::Model>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }

    turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.is_in(turn_ids.iter().cloned()))
        .filter(turn_item::Column::ItemType.eq(item_type.to_owned()))
        .order_by_asc(turn_item::Column::TurnId)
        .order_by_asc(turn_item::Column::CreatedAt)
        .order_by_asc(turn_item::Column::ItemId)
        .all(db)
        .await
        .context("failed to query turn item rows by type for turns")
}

pub async fn find_terminal_turns_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<turn::Model>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut rows = turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Status.is_in(["completed", "failed", "interrupted"]))
        .order_by_desc(turn::Column::CreatedAt)
        .order_by_desc(turn::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .context("failed to query terminal turns for thread")?;

    // Dynamic Composer admits the source Conversation turn and its detached TaskRun occurrence
    // in the same second. Keep the entire timestamp bucket at the history-window boundary so a
    // hard LIMIT cannot retain the delivered assistant entry while dropping its source message
    // (or vice versa).
    if rows.len() == usize::try_from(limit).unwrap_or(usize::MAX) {
        if let Some(boundary_created_at) = rows.last().map(|row| row.created_at) {
            let existing_ids = rows
                .iter()
                .map(|row| row.id.clone())
                .collect::<std::collections::BTreeSet<_>>();
            let boundary_rows = turn::Entity::find()
                .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
                .filter(turn::Column::Status.is_in(["completed", "failed", "interrupted"]))
                .filter(turn::Column::CreatedAt.eq(boundary_created_at))
                .order_by_desc(turn::Column::Id)
                .all(db)
                .await
                .context("failed to query terminal turn history boundary")?;
            rows.extend(
                boundary_rows
                    .into_iter()
                    .filter(|row| !existing_ids.contains(&row.id)),
            );
        }
    }

    rows.sort_by(|left, right| {
        (left.created_at, left.id.as_str()).cmp(&(right.created_at, right.id.as_str()))
    });
    Ok(rows)
}

pub async fn find_latest_turn_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_desc(turn::Column::CreatedAt)
        .order_by_desc(turn::Column::Id)
        .one(db)
        .await
        .context("failed to query latest turn for thread")
}

pub async fn find_latest_conversation_turn_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::TurnKind.eq(turn_kind_to_db(TurnKind::Conversation)))
        .order_by_desc(turn::Column::CreatedAt)
        .order_by_desc(turn::Column::Id)
        .one(db)
        .await
        .context("failed to query latest conversation turn for thread")
}

pub async fn find_oldest_turn_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_asc(turn::Column::CreatedAt)
        .order_by_asc(turn::Column::Id)
        .one(db)
        .await
        .context("failed to query oldest turn for thread")
}

pub async fn find_turn_inputs<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<turn_input::Model>> {
    turn_input::Entity::find()
        .filter(turn_input::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(turn_input::Column::InputIndex)
        .all(db)
        .await
        .context("failed to query turn inputs")
}

pub async fn find_turn_inputs_for_turns<C: ConnectionTrait>(
    db: &C,
    turn_ids: &[String],
) -> Result<Vec<turn_input::Model>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }

    turn_input::Entity::find()
        .filter(turn_input::Column::TurnId.is_in(turn_ids.iter().cloned()))
        .order_by_asc(turn_input::Column::TurnId)
        .order_by_asc(turn_input::Column::InputIndex)
        .all(db)
        .await
        .context("failed to query turn inputs for turns")
}

pub async fn find_completed_turn_items<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<turn_item::Model>> {
    turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemType.eq("agent_message"))
        .filter(turn_item::Column::Status.eq("completed"))
        .order_by_asc(turn_item::Column::CreatedAt)
        .order_by_asc(turn_item::Column::ItemId)
        .all(db)
        .await
        .context("failed to query completed turn items")
}

pub async fn count_completed_turns_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<u64> {
    let count = turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Status.eq("completed"))
        .count(db)
        .await
        .context("failed to count completed turns for thread")?;
    Ok(count)
}

pub async fn find_completed_turns_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Vec<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Status.eq("completed"))
        .order_by_asc(turn::Column::CreatedAt)
        .order_by_asc(turn::Column::Id)
        .all(db)
        .await
        .context("failed to query completed turns for thread")
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_protocol::AgentMessagePhase;
    use pioneer_protocol::ThreadMode;
    use sea_orm::{Database, DatabaseConnection, DbBackend};
    use serde_json::json;

    #[test]
    fn legacy_null_send_mode_is_chat_compatible_but_not_message_mutable() {
        let model = collaboration_test_turn(None, None, None, "[]");
        let collaboration = collaboration_from_model(&model).expect("legacy Turn should decode");
        assert_eq!(collaboration.mode.effective_mode, ThreadMode::Chat);
        assert!(!collaboration.mode.message_mutation_eligible);
        assert_eq!(
            collaboration
                .author
                .expect("stable legacy fallback")
                .display_name,
            "System"
        );
    }

    #[test]
    fn persisted_message_collaboration_roundtrips_without_a_second_identity() {
        let model = collaboration_test_turn(
            Some("message"),
            Some("Member A"),
            Some("member_a"),
            r#"[{"principal_id":"P00000000000000000002","nickname":"member_b"}]"#,
        );
        let collaboration = collaboration_from_model(&model).expect("Message Turn should decode");
        assert_eq!(collaboration.mode.effective_mode, ThreadMode::Message);
        assert!(collaboration.mode.message_mutation_eligible);
        assert_eq!(collaboration.mentions.len(), 1);
        assert_eq!(collaboration.message_revision, 2);
        assert!(collaboration.message_deleted);
        assert_eq!(
            collaboration.author.expect("author snapshot").actor,
            PersistedActorRef::Principal(
                pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap()
            )
        );
    }

    #[test]
    fn persisted_agent_collaboration_roundtrips_the_exact_immutable_snapshot() {
        let mut model =
            collaboration_test_turn(Some("message"), Some("Builder"), Some("builder"), "[]");
        model.initiated_by_actor_kind = Some("agent_execution".to_owned());
        model.initiated_by_actor_id = Some("E00000000000000000001".to_owned());
        model.author_agent_snapshot_json = Some(
            serde_json::to_string(&pioneer_protocol::AgentPresentationSnapshot {
                agent_identity_id: pioneer_protocol::AgentIdentityId::new("I00000000000000000001")
                    .unwrap(),
                agent_execution_id: pioneer_protocol::AgentExecutionId::new(
                    "E00000000000000000001",
                )
                .unwrap(),
                identity_source_kind: pioneer_protocol::AgentIdentitySourceKind::NativeAgent,
                identity_source_revision: 7,
                display_name: "Builder".to_owned(),
                nickname: "builder".to_owned(),
                avatar_revision: Some("avatar-7".to_owned()),
                role_label: Some("Reviewer".to_owned()),
            })
            .unwrap(),
        );
        model.author_avatar_revision_snapshot = Some("avatar-7".to_owned());

        let author = collaboration_from_model(&model)
            .expect("exact Agent author should decode")
            .author
            .expect("Agent author");
        assert_eq!(author.display_name, "Builder");
        assert_eq!(author.nickname, "builder");
        assert_eq!(
            author
                .agent
                .expect("rich Agent snapshot")
                .role_label
                .as_deref(),
            Some("Reviewer")
        );
    }

    #[test]
    fn persisted_agent_snapshot_cannot_hide_under_an_incomplete_or_human_author() {
        let mut model =
            collaboration_test_turn(Some("message"), Some("Member A"), Some("member_a"), "[]");
        model.author_agent_snapshot_json = Some("{}".to_owned());
        assert!(collaboration_from_model(&model).is_err());

        model.initiated_by_actor_kind = Some("agent_execution".to_owned());
        model.initiated_by_actor_id = Some("E00000000000000000001".to_owned());
        model.author_display_name_snapshot = None;
        assert!(collaboration_from_model(&model).is_err());
    }

    #[tokio::test]
    async fn upsert_turn_item_supports_sqlite_zstd_view() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        enable_turn_item_payload_zstd(&connection).await;

        let created_at = fixed_test_datetime();
        let first = agent_message_item("item_zstd", "first payload");
        upsert_turn_item(
            &connection,
            "turn_item_zstd",
            &first,
            Some("running"),
            created_at,
            created_at,
        )
        .await
        .expect("turn_item insert should work through sqlite-zstd view");

        let inserted = find_turn_item(&connection, "turn_item_zstd", "item_zstd")
            .await
            .expect("turn_item query should work")
            .expect("turn_item row should exist");
        assert_eq!(inserted.status.as_deref(), Some("running"));
        assert!(inserted.payload.contains("first payload"));

        let updated_at = fixed_later_test_datetime();
        let updated = agent_message_item("item_zstd", "updated payload");
        upsert_turn_item(
            &connection,
            "turn_item_zstd",
            &updated,
            Some("completed"),
            created_at,
            updated_at,
        )
        .await
        .expect("turn_item update should work through sqlite-zstd view");

        let stored = find_turn_item(&connection, "turn_item_zstd", "item_zstd")
            .await
            .expect("updated turn_item query should work")
            .expect("updated turn_item row should exist");
        assert_eq!(stored.status.as_deref(), Some("completed"));
        assert!(stored.payload.contains("updated payload"));
        assert_eq!(stored.created_at, created_at);
        assert_eq!(stored.updated_at, updated_at);

        let backing_rows =
            query_i64(&connection, "SELECT COUNT(*) AS value FROM _turn_item_zstd").await;
        assert_eq!(backing_rows, 1);
    }

    #[tokio::test]
    async fn epic6_turn_columns_and_revision_use_existing_turn_repository() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        connection
            .execute_unprepared(
                "INSERT INTO workspace (id, name, is_active, is_current) \
                 VALUES ('workspace_epic6_repo', 'Epic 6', 1, 1); \
                 INSERT INTO thread \
                    (id, workspace_id, preview, mode, model, model_provider, status) \
                 VALUES \
                    ('H00000000000000000001', 'workspace_epic6_repo', '', 'Chat', \
                     'model', 'provider', 'active');",
            )
            .await
            .expect("Turn owner fixture should insert");

        let now = fixed_test_datetime();
        let actor = PersistedActorRef::System;
        let message = Turn {
            id: "T00000000000000000001".to_owned(),
            status: TurnStatus::Completed,
            turn_kind: TurnKind::Conversation,
            origin: pioneer_protocol::TurnOrigin::User,
            mode: ThreadMode::Message,
            author: Some(TurnAuthorSnapshot {
                actor: actor.clone(),
                display_name: "System".to_owned(),
                nickname: "system".to_owned(),
                avatar_revision: None,
                agent: None,
            }),
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        upsert_turn_with_initiator(
            &connection,
            message.id.as_str(),
            "H00000000000000000001",
            &message,
            None,
            None,
            &actor,
            now,
            now,
        )
        .await
        .expect("Message Turn should persist through the existing repository");

        let (_, collaboration) =
            find_turn_collaboration(&connection, "H00000000000000000001", message.id.as_str())
                .await
                .expect("Message Turn should load")
                .expect("Message Turn should exist");
        assert_eq!(collaboration.mode.effective_mode, ThreadMode::Message);
        assert!(collaboration.mode.message_mutation_eligible);

        insert_turn_message_revision(
            &connection,
            NewTurnMessageRevision {
                turn_id: message.id.as_str(),
                revision: 0,
                input: &[UserInput::Text {
                    text: "original".to_owned(),
                    text_elements: Vec::new(),
                }],
                mentions: &[],
                changed_by: &actor,
                change_kind: TurnMessageRevisionChangeKind::Edit,
                created_at: now,
            },
        )
        .await
        .expect("previous Turn version should persist");
        let revisions = list_turn_message_revisions(&connection, message.id.as_str(), None, 10)
            .await
            .expect("revisions should list");
        assert_eq!(revisions.len(), 1);
        let revision = turn_message_revision_from_model(revisions[0].clone())
            .expect("revision should map to protocol");
        assert_eq!(revision.turn_id, message.id);
        assert_eq!(revision.revision, 0);
        assert_eq!(revision.changed_by, actor);
    }

    async fn enable_turn_item_payload_zstd(connection: &DatabaseConnection) {
        let sqlite_zstd_config = json!({
            "table": "turn_item",
            "column": "payload",
            "compression_level": 19,
            "dict_chooser": "'turn_item.payload'"
        });
        connection
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT zstd_enable_transparent(?) AS value",
                [sqlite_zstd_config.to_string().into()],
            ))
            .await
            .expect("sqlite-zstd should enable transparent compression");
    }

    fn agent_message_item(item_id: &str, marker: &str) -> TurnItem {
        TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: format!("{marker} {}", "abc123xyz ".repeat(64)),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        }
    }

    fn fixed_test_datetime() -> DateTimeWithTimeZone {
        chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z").expect("valid fixed test date")
    }

    fn fixed_later_test_datetime() -> DateTimeWithTimeZone {
        chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:01Z").expect("valid fixed test date")
    }

    fn collaboration_test_turn(
        send_mode: Option<&str>,
        display_name: Option<&str>,
        nickname: Option<&str>,
        mentions_json: &str,
    ) -> turn::Model {
        let now = fixed_test_datetime();
        turn::Model {
            id: "T00000000000000000001".to_owned(),
            thread_id: "H00000000000000000001".to_owned(),
            status: "completed".to_owned(),
            error: None,
            prompt_manifest_json: "{}".to_owned(),
            prompt_compiler_version: None,
            prompt_profile: None,
            prompt_fingerprint_stable: None,
            prompt_fingerprint_dynamic: None,
            prompt_fingerprint_full: None,
            created_at: now,
            updated_at: now,
            turn_kind: "conversation".to_owned(),
            origin: "user".to_owned(),
            reasoning_effort: None,
            permission_profile_mode: None,
            permission_profile_source: None,
            permission_profile_snapshot_json: None,
            execution_security_snapshot_version: None,
            execution_security_snapshot_json: None,
            initiated_by_actor_id: send_mode.map(|_| "P00000000000000000001".to_owned()),
            initiated_by_actor_kind: Some(if send_mode.is_some() {
                "principal".to_owned()
            } else {
                "system".to_owned()
            }),
            execution_authorization_context_json: None,
            send_mode: send_mode.map(str::to_owned),
            author_display_name_snapshot: display_name.map(str::to_owned),
            author_nickname_snapshot: nickname.map(str::to_owned),
            author_avatar_revision_snapshot: None,
            author_agent_snapshot_json: None,
            reply_to_turn_id: send_mode.map(|_| "T00000000000000000000".to_owned()),
            mentions_json: mentions_json.to_owned(),
            message_revision: if send_mode.is_some() { 2 } else { 0 },
            message_deleted_at: send_mode.map(|_| now),
            message_deleted_by_actor_id: None,
            message_deleted_by_actor_kind: send_mode.map(|_| "system".to_owned()),
        }
    }

    async fn query_i64(connection: &DatabaseConnection, sql: &str) -> i64 {
        let row = connection
            .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql.to_owned()))
            .await
            .expect("query should execute")
            .expect("query should return row");
        row.try_get::<i64>("", "value")
            .expect("value should decode")
    }
}
