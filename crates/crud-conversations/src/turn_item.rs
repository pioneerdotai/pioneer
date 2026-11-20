use anyhow::{Context, Result};
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_entity::turn_item as item;
use pioneer_protocol::TurnItemType;
use sea_orm::{
    DatabaseTransaction, EntityTrait, Set, entity::prelude::DateTimeWithTimeZone,
    sea_query::OnConflict,
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

    body.insert(tx).await?;
    item::Entity::insert(item::ActiveModel {
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
            .update_columns([
                C::ItemType,
                C::Status,
                C::Payload,
                C::PayloadPayloadId,
                C::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec_without_returning(tx)
    .await
    .context("upsert canonical turn item")?;
    Ok(())
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
