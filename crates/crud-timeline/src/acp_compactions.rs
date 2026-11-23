use crate::timeline_projection::WorkItemClassification;
use anyhow::{Context, Result, ensure};
use pioneer_crud_payloads::storage::{self, Selection};
use pioneer_entity::{
    portable_payload_value as value, turn_cli_runtime_binding as binding, turn_item as item,
    turn_work_item_projection as work,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{ItemUpdatedNotification, TurnItem};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func};
use sea_orm::{
    ColumnTrait, EntityTrait, FromQueryResult, JoinType, QueryFilter, QueryOrder, QuerySelect,
    RelationTrait,
};

const MAX_ITEMS: u64 = 8192;
const MAX_BYTES: i64 = 16 * 1024 * 1024;

#[derive(FromQueryResult)]
struct CompactionSource {
    id: String,
    turn_id: String,
    thread_id: String,
    payload_payload_id: Option<String>,
    bytes: i64,
}

/// Read canonical compaction bodies for a session and retain their original turn IDs.
pub async fn list_acp_compaction_items(
    db: &PostgresDatabase,
    workspace: &str,
    runtime: &str,
    continuation: &str,
    session: &str,
) -> Result<Vec<ItemUpdatedNotification>> {
    let reader = db.begin_read().await?;
    // Use the semantic projection to locate compactions in portable storage.
    // The byte budget uses payload metadata before any encoded bodies are read.
    let inline_payload = Expr::col((item::Entity, item::Column::Payload)).cast_as("text");
    let sources = item::Entity::find()
        .select_only()
        .columns([
            item::Column::Id,
            item::Column::TurnId,
            item::Column::PayloadPayloadId,
        ])
        .column_as(binding::Column::ThreadId, "thread_id")
        .expr_as(
            Func::coalesce([
                Expr::col((value::Entity, value::Column::OriginalBytes)),
                Func::cust(Alias::new("octet_length"))
                    .arg(inline_payload.clone())
                    .into(),
            ]),
            "bytes",
        )
        .join(
            JoinType::InnerJoin,
            item::Entity::belongs_to(binding::Entity)
                .from(item::Column::TurnId)
                .to(binding::Column::TurnId)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            item::Entity::belongs_to(work::Entity)
                .from((item::Column::TurnId, item::Column::ItemId))
                .to((work::Column::TurnId, work::Column::ItemId))
                .into(),
        )
        .join(
            JoinType::LeftJoin,
            item::Relation::PortablePayloadValue.def(),
        )
        .filter(binding::Column::WorkspaceId.eq(workspace))
        .filter(binding::Column::RuntimeId.eq(runtime))
        .filter(binding::Column::ContinuationThreadId.eq(continuation))
        .filter(binding::Column::RuntimeKind.eq("acp"))
        .filter(binding::Column::NativeThreadId.eq(session))
        .filter(item::Column::ItemType.eq("system_event"))
        .filter(item::Column::ItemId.like(r"acp\_%"))
        .filter(
            work::Column::Classification
                .eq(WorkItemClassification::AgentContextCompaction.as_str()),
        )
        .order_by_asc(item::Column::CreatedAt)
        .order_by_asc(item::Column::Id)
        .limit(MAX_ITEMS + 1)
        .into_model::<CompactionSource>()
        .all(&reader)
        .await?;
    ensure!(
        sources.len() as u64 <= MAX_ITEMS,
        "ACP canonical compaction capacity exceeded"
    );
    let bytes = sources
        .iter()
        .fold(0_i64, |total, source| total.saturating_add(source.bytes));
    ensure!(
        bytes <= MAX_BYTES,
        "ACP canonical compaction byte budget exceeded"
    );

    let mut captured = Vec::with_capacity(sources.len());
    for source in sources {
        let (portable, inline) = if let Some(id) = &source.payload_payload_id {
            let body = storage::capture(&reader, id, Selection::Full)
                .await?
                .context("missing portable ACP compaction body")?;
            (Some(body), None)
        } else {
            let body = item::Entity::find_by_id(&source.id)
                .select_only()
                .expr(inline_payload.clone())
                .into_tuple::<String>()
                .one(&reader)
                .await?;
            (None, body)
        };
        captured.push((source, portable, inline));
    }
    reader.commit().await?;

    let mut items = Vec::with_capacity(captured.len());
    for (source, portable, inline) in captured {
        let payload = match portable {
            Some(body) => body.into_text()?,
            None => inline.context("missing inline ACP compaction body")?,
        };
        let item: TurnItem =
            serde_json::from_str(&payload).context("invalid canonical ACP compaction")?;
        if matches!(&item, TurnItem::SystemEvent { code: Some(code), details: Some(details), .. }
            if code == "agent_context_compaction"
            && details.get("compactionId").is_some_and(serde_json::Value::is_string)
            && details.get("acpStatus").is_some_and(serde_json::Value::is_string))
        {
            items.push(ItemUpdatedNotification {
                workspace_id: workspace.to_owned(),
                thread_id: source.thread_id,
                turn_id: source.turn_id,
                item,
            });
        }
    }
    Ok(items)
}
