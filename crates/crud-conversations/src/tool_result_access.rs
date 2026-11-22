use anyhow::Result;
use pioneer_entity::{thread, turn, turn_item};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::{ColumnTrait, EntityTrait, JoinType, QueryFilter, QuerySelect};

/// Resolve ownership using canonical columns. Never deserialize full provider
/// output/storage merely to authorize a result. Only observer parameters can
/// introduce additional protected source references; other arguments are ignored.
pub async fn tool_result_access_source(
    db: &PostgresDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
    item: &str,
) -> Result<Option<serde_json::Value>> {
    let query = scoped_tool_item(workspace, thread, turn, item)
        .filter(turn::Column::MessageDeletedAt.is_null())
        .filter(turn_item::Column::ItemType.is_in([
            "command_execution",
            "file_change",
            "web_search",
            "web_fetch",
            "download",
            "dynamic_tool_call",
        ]));
    let Some((row, payload)) = crate::turn_payload::exact_items(db, query.limit(1))
        .await?
        .pop()
    else {
        return Ok(None);
    };
    let value = pioneer_crud_payloads::json_fields::first_object_fields(&payload, &["toolName"])?;
    let tool = value.get("toolName").and_then(|v| v.as_str());
    let arguments = if matches!(
        tool,
        Some(
            "threads_list"
                | "threads_history_read"
                | "threads_children_list"
                | "threads_tools_result_read"
        )
    ) {
        let value =
            pioneer_crud_payloads::json_fields::first_object_fields(&payload, &["arguments"])?;
        match value.get("arguments") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(text)) => {
                Some(serde_json::from_str::<serde_json::Value>(text)?)
            }
            Some(value) => Some(value.clone()),
        }
    } else {
        None
    };
    Ok(Some(
        serde_json::json!({"itemType":row.item_type,"toolName":tool,"arguments":arguments}),
    ))
}

// Typed canonical owner lookup shared by the two JSON-dependent tool reads.
fn scoped_tool_item(
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
    item: &str,
) -> sea_orm::Select<turn_item::Entity> {
    turn_item::Entity::find()
        .join(
            JoinType::InnerJoin,
            turn_item::Entity::belongs_to(turn::Entity)
                .from(turn_item::Column::TurnId)
                .to(turn::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .filter(thread::Column::WorkspaceId.eq(workspace))
        .filter(turn::Column::ThreadId.eq(thread_id))
        .filter(turn::Column::Id.eq(turn_id))
        .filter(turn_item::Column::ItemId.eq(item))
}
