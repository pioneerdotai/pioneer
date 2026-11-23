use anyhow::{Context, Result};
use pioneer_crud_payloads::storage::{self, PreparedPayload};
use pioneer_entity::{portable_payload_value as value, turn_item as item};
use pioneer_protocol::{TurnItem, TurnItemType, generate_id};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, OnConflict, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set,
    entity::prelude::DateTimeWithTimeZone,
};

pub struct ItemWrite {
    pub id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: String,
    pub status: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

pub async fn upsert(
    tx: &DatabaseTransaction,
    item: ItemWrite,
    body: &PreparedPayload,
) -> Result<()> {
    use item::Column as C;

    let metadata = body.metadata();
    let current_pointer = Expr::col((item::Entity, C::PayloadPayloadId));
    let equivalent_payload = Query::select()
        .expr(Expr::val(1))
        .from(value::Entity)
        .and_where(Expr::col((value::Entity, value::Column::Id)).eq(current_pointer.clone()))
        .and_where(value::Column::OriginalBytes.eq(i64::try_from(metadata.original_bytes)?))
        .and_where(value::Column::OriginalChars.eq(i64::try_from(metadata.original_chars)?))
        .and_where(value::Column::Sha256.eq(metadata.sha256.to_vec()))
        .to_owned();
    let payload_pointer = Expr::case(Expr::exists(equivalent_payload), current_pointer)
        .finally(Expr::col((Alias::new("excluded"), C::PayloadPayloadId)));

    body.insert(tx).await?;
    let stored = item::Entity::insert(item::ActiveModel {
        id: Set(item.id),
        turn_id: Set(item.turn_id),
        item_id: Set(item.item_id),
        item_type: Set(item.item_type),
        status: Set(item.status),
        payload: Set(None),
        payload_payload_id: Set(Some(body.id().to_owned())),
        active_attempt_number: Set(0),
        created_at: Set(item.created_at),
        updated_at: Set(item.updated_at),
        ..Default::default()
    })
    .on_conflict(
        OnConflict::columns([C::TurnId, C::ItemId])
            .update_columns([C::ItemType, C::Status, C::Payload, C::UpdatedAt])
            .value(C::PayloadPayloadId, payload_pointer)
            .to_owned(),
    )
    .exec_with_returning(tx)
    .await
    .context("upsert canonical turn item")?;
    if stored.payload_payload_id.as_deref() != Some(body.id()) {
        // Replaying the same bytes keeps the original locator and source
        // revision. Discard the unattached preparation in the same transaction.
        storage::delete(tx, body.id()).await?;
    }
    Ok(())
}

pub fn turn_item_type_to_db(item_type: TurnItemType) -> &'static str {
    match item_type {
        TurnItemType::UserMessage => "user_message",
        TurnItemType::AgentMessage => "agent_message",
        TurnItemType::Reasoning => "reasoning",
        TurnItemType::SystemEvent => "system_event",
        TurnItemType::Task => "task",
        TurnItemType::CommandExecution => "command_execution",
        TurnItemType::FileChange => "file_change",
        TurnItemType::WebSearch => "web_search",
        TurnItemType::WebFetch => "web_fetch",
        TurnItemType::Download => "download",
        TurnItemType::DynamicToolCall => "dynamic_tool_call",
    }
}

pub fn turn_item_type_from_db(value: &str) -> Option<TurnItemType> {
    match value {
        "user_message" => Some(TurnItemType::UserMessage),
        "agent_message" => Some(TurnItemType::AgentMessage),
        "reasoning" => Some(TurnItemType::Reasoning),
        "system_event" => Some(TurnItemType::SystemEvent),
        "task" => Some(TurnItemType::Task),
        "command_execution" => Some(TurnItemType::CommandExecution),
        "file_change" => Some(TurnItemType::FileChange),
        "web_search" => Some(TurnItemType::WebSearch),
        "web_fetch" => Some(TurnItemType::WebFetch),
        "download" => Some(TurnItemType::Download),
        "dynamic_tool_call" => Some(TurnItemType::DynamicToolCall),
        _ => None,
    }
}

pub async fn find_turn_item_metadata<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<item::Model>> {
    item::Entity::find()
        .filter(item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(item::Column::ItemId.eq(item_id.to_owned()))
        .one(db)
        .await
        .context("failed to query turn item row")
}

const DB_ID_LEN: usize = 21;

#[derive(Clone, Debug)]
pub struct PreparedTurnItemProjection {
    row_id: String,
    turn_id: String,
    item_id: String,
    item_type: String,
    status: Option<String>,
    payload_json: String,
    body: pioneer_crud_payloads::storage::PreparedPayload,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
}

impl PreparedTurnItemProjection {
    /// An update carries content, not a completion transition. Check the
    /// transaction's current item so event B observes event A in atomic batches.
    pub async fn preserve_uncommitted_source_status<C: ConnectionTrait>(
        &mut self,
        db: &C,
    ) -> Result<()> {
        if matches!(
            self.item_type.as_str(),
            "user_message" | "agent_message" | "task"
        ) {
            let current = find_turn_item_metadata(db, &self.turn_id, &self.item_id).await?;
            if current
                .as_ref()
                .is_none_or(|row| !source_status_is_committed(row.status.as_deref()))
            {
                self.status = Some("in_progress".to_owned());
            }
        }
        Ok(())
    }

    pub fn payload_json(&self) -> &str {
        self.payload_json.as_str()
    }
}

pub fn prepare_turn_item_projection(
    turn_id: &str,
    item: &TurnItem,
    status: Option<&str>,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<PreparedTurnItemProjection> {
    let item_id = item.item_id();
    let item_type = turn_item_type_to_db(item.item_type());
    let payload_json =
        serde_json::to_string(item).context("failed to serialize turn item payload")?;
    let body = pioneer_crud_payloads::storage::PreparedPayload::new_turn_item(&payload_json)?;
    Ok(PreparedTurnItemProjection {
        row_id: generate_id(DB_ID_LEN),
        turn_id: turn_id.to_owned(),
        item_id: item_id.to_owned(),
        item_type: item_type.to_owned(),
        status: status.map(str::to_owned),
        payload_json,
        body,
        created_at,
        updated_at,
    })
}

pub async fn upsert_prepared_turn_item(
    db: &sea_orm::DatabaseTransaction,
    prepared: PreparedTurnItemProjection,
) -> Result<()> {
    upsert(
        db,
        ItemWrite {
            id: prepared.row_id,
            turn_id: prepared.turn_id,
            item_id: prepared.item_id,
            item_type: prepared.item_type,
            status: prepared.status,
            created_at: prepared.created_at,
            updated_at: prepared.updated_at,
        },
        &prepared.body,
    )
    .await
}

pub fn source_status_is_committed(status: Option<&str>) -> bool {
    matches!(
        status,
        Some("completed" | "failed" | "timed_out" | "cancelled")
    )
}
