use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::{thread, tool_output_chunk, turn};
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ActiveValue::Set, JoinType, QueryOrder, QuerySelect, TransactionTrait, entity::prelude::*,
};

// Prepare bytes before writer admission. Scope and event identity are database
// state, so revalidate both inside the transaction that inserts the chunk.
pub(crate) async fn record_tool_output(
    store: &CrudStore,
    id: &str,
    n: &pioneer_protocol::ItemDeltaNotification,
) -> Result<()> {
    ensure!(
        n.delta.len() <= 128 * 1024,
        "tool output chunk exceeds frame limit"
    );
    let stream = serde_json::to_string(&n.stream)?;
    let metadata = n.payload.as_ref().map(serde_json::to_string).transpose()?;
    let row = tool_output_chunk::ActiveModel {
        id: Set(id.into()),
        turn_id: Set(n.turn_id.clone()),
        item_id: Set(n.item_id.clone()),
        stream: Set(stream),
        text: Set(n.delta.clone()),
        metadata: Set(metadata),
        ..Default::default()
    };
    store
        .run_serialized_write(|| {
            let row = row.clone();
            async move {
                let tx = store.connection.begin().await?;
                ensure!(
                    thread::Entity::find_by_id(&n.thread_id)
                        .filter(thread::Column::WorkspaceId.eq(&n.workspace_id))
                        .select_only()
                        .column(thread::Column::Id)
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                        .is_some(),
                    "tool output workspace mismatch"
                );
                ensure!(
                    turn::Entity::find_by_id(&n.turn_id)
                        .filter(turn::Column::ThreadId.eq(&n.thread_id))
                        .select_only()
                        .column(turn::Column::Id)
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                        .is_some(),
                    "tool output turn mismatch"
                );
                if let Some(old) = tool_output_chunk::Entity::find()
                    .filter(tool_output_chunk::Column::Id.eq(id))
                    .one(&tx)
                    .await?
                {
                    ensure!(
                        old.turn_id == n.turn_id
                            && old.item_id == n.item_id
                            && old.text == n.delta
                            && old.stream == *row.stream.as_ref()
                            && old.metadata == *row.metadata.as_ref(),
                        "tool output identity conflict"
                    );
                } else {
                    tool_output_chunk::Entity::insert(row).exec(&tx).await?;
                }
                tx.commit().await?;
                Ok(())
            }
        })
        .await
}
/// Bounded ordered chunks; caller releases reader capacity before rendering.
pub(crate) async fn tool_output_page(
    store: &CrudStore,
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    after: i64,
) -> Result<Vec<(i64, tool_output_chunk::Model)>> {
    let rows = tool_output_chunk::Entity::find()
        .join(
            JoinType::InnerJoin,
            tool_output_chunk::Entity::belongs_to(turn::Entity)
                .from(tool_output_chunk::Column::TurnId)
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
        .filter(Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(workspace))
        .filter(Expr::col((turn::Entity, turn::Column::ThreadId)).eq(thread_id))
        .filter(tool_output_chunk::Column::TurnId.eq(turn_id))
        .filter(tool_output_chunk::Column::ItemId.eq(item_id))
        .filter(tool_output_chunk::Column::Ordinal.gt(after))
        .order_by_asc(tool_output_chunk::Column::Ordinal)
        .limit(128)
        .all(&store.connection)
        .await?;
    Ok(rows.into_iter().map(|row| (row.ordinal, row)).collect())
}
