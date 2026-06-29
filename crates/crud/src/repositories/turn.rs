use anyhow::{Context, Result};
use pioneer_entity::{turn, turn_input, turn_item, turn_status_history};
use pioneer_protocol::{Turn, TurnItem, TurnStatus, UserInput, generate_id};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
};

use crate::convention::{
    input_type_and_text, turn_item_id_and_type_to_db, turn_kind_to_db, turn_origin_to_db,
    turn_permission_mode_to_db, turn_permission_profile_source_to_db, turn_status_to_db,
};

const DB_ID_LEN: usize = 21;

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
    let mut update_columns = vec![
        turn::Column::ThreadId,
        turn::Column::Status,
        turn::Column::TurnKind,
        turn::Column::Origin,
        turn::Column::Error,
        turn::Column::UpdatedAt,
    ];
    update_columns.extend([
        turn::Column::PermissionProfileMode,
        turn::Column::PermissionProfileSource,
        turn::Column::PermissionProfileSnapshotJson,
    ]);
    let permission_profile_columns = build_turn_permission_profile_columns(turn_model)?;

    turn::Entity::insert(turn::ActiveModel {
        id: Set(turn_id.to_owned()),
        thread_id: Set(thread_id.to_owned()),
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

pub async fn find_terminal_turns_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<turn::Model>> {
    let mut rows = turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Status.is_in(["completed", "failed", "interrupted"]))
        .order_by_desc(turn::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to query terminal turns for thread")?;

    rows.reverse();
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

pub async fn find_completed_turn_items<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<turn_item::Model>> {
    turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemType.eq("agent_message"))
        .filter(turn_item::Column::Status.eq("completed"))
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

pub async fn find_completed_turns_in_range<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    skip: u64,
    take: u64,
) -> Result<Vec<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Status.eq("completed"))
        .order_by_asc(turn::Column::CreatedAt)
        .offset(skip)
        .limit(take)
        .all(db)
        .await
        .context("failed to query completed turns in range")
}
