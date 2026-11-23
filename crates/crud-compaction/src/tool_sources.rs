//! Scoped shell-source identity queries over canonical turn items.
use anyhow::Result;
use pioneer_compaction::SourceRef;
use pioneer_entity::{compaction_item_revision, thread, turn, turn_item};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};

pub fn turn_item_has_shell_storage() -> Expr {
    use pioneer_crud_payloads::json_projection::{json_path_text, queryable_json};
    use pioneer_entity::portable_payload_value as payload;

    let pointer = Expr::col((turn_item::Entity, turn_item::Column::PayloadPayloadId));
    let shell_payload = Query::select()
        .expr(Expr::val(1))
        .from(payload::Entity)
        .and_where(Expr::col((payload::Entity, payload::Column::Id)).eq(pointer.clone()))
        .and_where(payload::Column::TurnItemShell.eq(true))
        .to_owned();
    Expr::case(pointer.is_not_null(), Expr::exists(shell_payload))
        .finally(
            json_path_text(
                queryable_json(Expr::col((turn_item::Entity, turn_item::Column::Payload))),
                &["storage", "kind"],
            )
            .eq("shell"),
        )
        .into()
}

pub async fn compaction_tool_item_reference<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    item: &str,
) -> Result<Option<SourceRef>> {
    let row = turn_item::Entity::find()
        .select_only()
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
        .join(
            JoinType::InnerJoin,
            turn_item::Entity::belongs_to(compaction_item_revision::Entity)
                .from(turn_item::Column::Id)
                .to(compaction_item_revision::Column::SourceId)
                .into(),
        )
        .expr(Expr::col((turn_item::Entity, turn_item::Column::Id)))
        .expr(Expr::col((
            compaction_item_revision::Entity,
            compaction_item_revision::Column::Revision,
        )))
        .filter(compaction_item_revision::Column::Present.eq(1_i64))
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(Expr::col((thread::Entity, thread::Column::Id)).eq(Expr::Value(thread.into())))
                .and(
                    Expr::col((turn_item::Entity, turn_item::Column::TurnId))
                        .eq(Expr::Value(turn.into())),
                )
                .and(
                    Expr::col((turn_item::Entity, turn_item::Column::ItemId))
                        .eq(Expr::Value(item.into())),
                )
                .and(turn_item_has_shell_storage()),
        )
        .limit(1)
        .into_tuple::<(String, i64)>()
        .one(db)
        .await?;
    Ok(row.map(|(id, revision)| SourceRef {
        scope: format!("item:{turn}"),
        id,
        version: format!("item-revision:{revision}"),
    }))
}
