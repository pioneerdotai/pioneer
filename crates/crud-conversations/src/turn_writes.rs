//! Turn state transitions and historical status receipts.

use anyhow::{Context, Result};
use pioneer_crud_access::identity::{actor_ref_from_db, actor_ref_to_db};
use pioneer_entity::{turn, turn_status_history};
use pioneer_protocol::{
    PersistedActorRef, Turn, TurnMention, TurnPermissionMode, TurnPermissionProfileSource,
    TurnStatus, TurnWorkOwner, generate_id,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Set};

use crate::collaboration::{
    PersistedTurnCollaboration, TURN_AGENT_AUTHOR_SNAPSHOT_JSON_MAX_BYTES,
    canonical_turn_mentions_json, collaboration_from_model, turn_send_mode_to_db,
    validate_turn_author_snapshot,
};
use crate::turn_reads::{find_turn_by_id, find_turn_by_thread_and_id};
use crate::values::{turn_kind_to_db, turn_origin_to_db, turn_status_to_db};

const DB_ID_LEN: usize = 21;

pub struct TurnChange {
    pub previous: Option<turn::Model>,
    pub current: turn::Model,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedMessageTurnMutationState {
    thread_id: String,
    turn_id: String,
    revision: i64,
    mentions_json: String,
    deleted_at: Option<DateTimeWithTimeZone>,
    deleted_by_actor_kind: Option<String>,
    deleted_by_actor_id: Option<String>,
    updated_at: DateTimeWithTimeZone,
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

#[derive(Debug, Clone)]
pub struct PreparedTurnUpsert {
    expected_existing: Option<turn::Model>,
    row: turn::ActiveModel,
    update_columns: Vec<turn::Column>,
}

impl PreparedTurnUpsert {
    pub fn changes_status_history(&self, status: TurnStatus, error: Option<&str>) -> bool {
        self.expected_existing.as_ref().is_none_or(|existing| {
            existing.status != turn_status_to_db(status) || existing.error.as_deref() != error
        })
    }
}

#[derive(Debug, Clone)]
pub struct PreparedTurnStatusHistory {
    row: turn_status_history::ActiveModel,
}

pub fn turn_permission_mode_to_db(mode: TurnPermissionMode) -> &'static str {
    match mode {
        TurnPermissionMode::FullAccess => "full_access",
        TurnPermissionMode::AutoAcceptEdits => "auto_accept_edits",
        TurnPermissionMode::Supervised => "supervised",
    }
}

pub fn turn_permission_mode_from_db(value: &str) -> Option<TurnPermissionMode> {
    match value {
        "full_access" => Some(TurnPermissionMode::FullAccess),
        "auto_accept_edits" => Some(TurnPermissionMode::AutoAcceptEdits),
        "supervised" => Some(TurnPermissionMode::Supervised),
        _ => None,
    }
}

pub fn turn_permission_profile_source_to_db(source: TurnPermissionProfileSource) -> &'static str {
    match source {
        TurnPermissionProfileSource::Composer => "composer",
        TurnPermissionProfileSource::Defaulted => "defaulted",
        TurnPermissionProfileSource::InheritedFromParentTurn => "inherited_from_parent_turn",
        TurnPermissionProfileSource::TaskPermissionCap => "task_permission_cap",
        TurnPermissionProfileSource::System => "system",
    }
}

pub fn turn_permission_profile_source_from_db(value: &str) -> Option<TurnPermissionProfileSource> {
    match value {
        "composer" => Some(TurnPermissionProfileSource::Composer),
        "defaulted" => Some(TurnPermissionProfileSource::Defaulted),
        "inherited_from_parent_turn" => Some(TurnPermissionProfileSource::InheritedFromParentTurn),
        "task_permission_cap" => Some(TurnPermissionProfileSource::TaskPermissionCap),
        "system" => Some(TurnPermissionProfileSource::System),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn prepare_projected_turn_upsert<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    thread_id: &str,
    turn_model: &Turn,
    prompt_manifest: Option<&TurnPromptManifestColumns>,
    reasoning_effort: Option<&str>,
    work_owner: Option<TurnWorkOwner>,
    initiator: Option<&PersistedActorRef>,
    update_initiator: bool,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<PreparedTurnUpsert> {
    let (actor_kind, actor_id) = match initiator {
        Some(initiator) => actor_ref_to_db(initiator),
        None => (None, None),
    };
    prepare_turn_upsert_with_actor_columns(
        db,
        turn_id,
        thread_id,
        turn_model,
        prompt_manifest,
        reasoning_effort,
        work_owner,
        actor_kind,
        actor_id,
        update_initiator,
        created_at,
        updated_at,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn prepare_turn_upsert_with_actor_columns<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    thread_id: &str,
    turn_model: &Turn,
    prompt_manifest: Option<&TurnPromptManifestColumns>,
    reasoning_effort: Option<&str>,
    work_owner: Option<TurnWorkOwner>,
    initiated_by_actor_kind: Option<String>,
    initiated_by_actor_id: Option<String>,
    update_initiator: bool,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<PreparedTurnUpsert> {
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
    if work_owner.is_some() {
        update_columns.push(turn::Column::WorkOwner);
    }
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
    if update_initiator
        && let (Some(existing), Some(supplied)) =
            (existing_collaboration.as_ref(), supplied_initiator.as_ref())
    {
        let existing_initiator = actor_ref_from_db(
            existing.initiated_by_actor_kind.as_deref(),
            existing.initiated_by_actor_id.as_deref(),
        )
        .with_context(|| format!("turn `{turn_id}` has an invalid persisted initiator pair"))?
        .with_context(|| format!("turn `{turn_id}` is missing its persisted initiator"))?;
        if &existing_initiator != supplied {
            anyhow::bail!("turn `{turn_id}` already has a different persisted initiator");
        }
    }
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
    let supplied_work_owner = work_owner.map(TurnWorkOwner::as_db_str);
    if let (Some(existing), Some(supplied_work_owner)) =
        (existing_collaboration.as_ref(), supplied_work_owner)
        && existing.work_owner != supplied_work_owner
    {
        anyhow::bail!("Turn work owner cannot change after initial persistence");
    }
    let effective_work_owner = supplied_work_owner
        .map(str::to_owned)
        .or_else(|| {
            existing_collaboration
                .as_ref()
                .map(|existing| existing.work_owner.clone())
        })
        .unwrap_or_else(|| TurnWorkOwner::Turn.as_db_str().to_owned());
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

    let row = turn::ActiveModel {
        insertion_ordinal: Default::default(),
        publication_xid: Default::default(),
        package_selection_json: Set(None),
        id: Set(turn_id.to_owned()),
        thread_id: Set(thread_id.to_owned()),
        initiated_by_actor_id: Set(initiated_by_actor_id),
        initiated_by_actor_kind: Set(initiated_by_actor_kind),
        status: Set(turn_status_to_db(turn_model.status).to_owned()),
        turn_kind: Set(turn_kind_to_db(turn_model.turn_kind).to_owned()),
        origin: Set(turn_origin_to_db(turn_model.origin).to_owned()),
        work_owner: Set(effective_work_owner),
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
    };
    Ok(PreparedTurnUpsert {
        expected_existing: existing_collaboration,
        row,
        update_columns,
    })
}

pub async fn apply_prepared_turn_upsert<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnUpsert,
) -> Result<TurnChange> {
    let turn_id = prepared.row.id.as_ref().to_owned();
    let current = turn::Entity::find_by_id(turn_id.as_str())
        .lock_exclusive()
        .one(db)
        .await?;
    if current != prepared.expected_existing {
        anyhow::bail!("turn `{turn_id}` changed during projection preparation");
    }
    let mut conflict = OnConflict::column(turn::Column::Id);
    if current.is_some() {
        conflict.update_columns(prepared.update_columns);
    } else {
        conflict.do_nothing();
    }
    let affected = turn::Entity::insert(prepared.row)
        .on_conflict(conflict.to_owned())
        .exec_without_returning(db)
        .await
        .context("failed to upsert turn")?;
    if affected != 1 {
        anyhow::bail!("turn `{turn_id}` changed during projection preparation");
    }

    let persisted = turn::Entity::find_by_id(&turn_id)
        .one(db)
        .await?
        .context("upserted turn is missing")?;
    Ok(TurnChange {
        previous: current,
        current: persisted,
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
) -> Result<Option<TurnChange>> {
    let old = turn::Entity::find_by_id(turn_id)
        .lock_exclusive()
        .one(db)
        .await?;
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

    if update_result.rows_affected == 0 {
        return Ok(None);
    }
    let persisted = turn::Entity::find_by_id(turn_id)
        .one(db)
        .await?
        .context("updated turn is missing")?;
    Ok(Some(TurnChange {
        previous: old,
        current: persisted,
    }))
}

pub fn prepare_message_turn_mutation_state(
    thread_id: &str,
    turn_id: &str,
    revision: u64,
    mentions: &[TurnMention],
    deleted_by: Option<&PersistedActorRef>,
    updated_at: DateTimeWithTimeZone,
) -> Result<PreparedMessageTurnMutationState> {
    let revision = i64::try_from(revision)
        .context("projected Turn message revision exceeds database integer range")?;
    let mentions_json = canonical_turn_mentions_json(mentions)?;
    let (deleted_by_actor_kind, deleted_by_actor_id) = match deleted_by {
        Some(actor) => actor_ref_to_db(actor),
        None => (None, None),
    };
    Ok(PreparedMessageTurnMutationState {
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        revision,
        mentions_json,
        deleted_at: deleted_by.map(|_| updated_at),
        deleted_by_actor_kind,
        deleted_by_actor_id,
        updated_at,
    })
}

pub async fn project_prepared_message_turn_mutation_state<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedMessageTurnMutationState,
) -> Result<bool> {
    let result = turn::Entity::update_many()
        .filter(turn::Column::ThreadId.eq(prepared.thread_id))
        .filter(turn::Column::Id.eq(prepared.turn_id))
        .filter(turn::Column::SendMode.eq("message"))
        .filter(turn::Column::MessageRevision.lte(prepared.revision))
        .col_expr(
            turn::Column::MentionsJson,
            Expr::value(prepared.mentions_json),
        )
        .col_expr(
            turn::Column::MessageRevision,
            Expr::value(prepared.revision),
        )
        .col_expr(
            turn::Column::MessageDeletedAt,
            Expr::value(prepared.deleted_at),
        )
        .col_expr(
            turn::Column::MessageDeletedByActorKind,
            Expr::value(prepared.deleted_by_actor_kind),
        )
        .col_expr(
            turn::Column::MessageDeletedByActorId,
            Expr::value(prepared.deleted_by_actor_id),
        )
        .col_expr(turn::Column::UpdatedAt, Expr::value(prepared.updated_at))
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
    let prepared = prepare_turn_status_history(turn_id, status, error, created_at);
    append_prepared_turn_status_history(db, prepared).await
}

pub fn prepare_turn_status_history(
    turn_id: &str,
    status: TurnStatus,
    error: Option<String>,
    created_at: DateTimeWithTimeZone,
) -> PreparedTurnStatusHistory {
    PreparedTurnStatusHistory {
        row: turn_status_history::ActiveModel {
            id: Set(generate_id(DB_ID_LEN)),
            turn_id: Set(turn_id.to_owned()),
            status: Set(turn_status_to_db(status).to_owned()),
            error: Set(error),
            created_at: Set(created_at),
        },
    }
}

pub async fn append_prepared_turn_status_history<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnStatusHistory,
) -> Result<()> {
    turn_status_history::Entity::insert(prepared.row)
        .exec(db)
        .await
        .context("failed to append turn status history")?;
    Ok(())
}

/// Encode the protocol manifest before write admission.
pub async fn write_turn_prompt_manifest(
    db: &pioneer_sqlite::SqliteDatabase,
    thread_id: &str,
    turn_id: &str,
    manifest: &pioneer_protocol::PromptManifest,
    event_timestamp_secs: i64,
) -> Result<bool> {
    let columns = crate::protocol_projection::build_turn_prompt_manifest_columns(manifest)?;
    pioneer_sqlite::retry_with_backoff(
        || {
            update_turn_prompt_manifest(
                db,
                thread_id,
                turn_id,
                &columns,
                crate::values::unix_to_datetime(event_timestamp_secs),
            )
        },
        pioneer_sqlite::is_retryable_database_conflict,
        pioneer_sqlite::DEFAULT_LOCK_RETRY_ATTEMPTS,
        std::time::Duration::from_millis(pioneer_sqlite::DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}
