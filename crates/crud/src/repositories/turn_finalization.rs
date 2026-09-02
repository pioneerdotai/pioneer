use anyhow::{Context, Result};
use pioneer_entity::turn_finalization;
use pioneer_protocol::{ItemCompletedNotification, TurnItem};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Set,
};
use sha2::{Digest, Sha256};

pub const STATUS_PREPARED: &str = "prepared";
pub const STATUS_COMMITTED: &str = "committed";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareTurnFinalizationOutcome {
    Prepared,
    AlreadyPrepared,
    AlreadyCommitted,
}

pub fn item_digest(notification: &ItemCompletedNotification) -> Result<(String, String)> {
    let mut provider_item = notification.item.clone();
    let TurnItem::AgentMessage {
        markdown,
        markdown_version,
        ..
    } = &mut provider_item
    else {
        anyhow::bail!("turn finalization requires an AgentMessage item");
    };
    // Markdown is a deterministic Gateway projection, not provider output.
    // Renderer upgrades between an ACK retry and restart must not turn the
    // same accepted answer into a conflicting finalization generation.
    *markdown = None;
    *markdown_version = None;
    let item_json =
        serde_json::to_string(&notification.item).context("failed to serialize final item")?;
    let provider_json =
        serde_json::to_string(&provider_item).context("failed to serialize provider final item")?;
    let digest = hex::encode(Sha256::digest(provider_json.as_bytes()));
    Ok((item_json, digest))
}

pub async fn prepare<C: ConnectionTrait>(
    db: &C,
    notification: &ItemCompletedNotification,
    generation: i64,
    prepared_at: DateTimeWithTimeZone,
) -> Result<PrepareTurnFinalizationOutcome> {
    if generation <= 0 {
        anyhow::bail!("turn finalization generation must be positive");
    }
    if notification.item.item_id().trim().is_empty() {
        anyhow::bail!("turn finalization item identity is empty");
    }
    let (item_json, digest) = item_digest(notification)?;
    if let Some(existing) = turn_finalization::Entity::find_by_id(notification.turn_id.clone())
        .one(db)
        .await
        .context("failed to query turn finalization")?
    {
        if existing.thread_id != notification.thread_id
            || existing.workspace_id != notification.workspace_id
            || existing.generation != generation
            || existing.item_id != notification.item.item_id()
            || existing.item_digest != digest
        {
            anyhow::bail!(
                "turn `{}` already has a conflicting finalization intent",
                notification.turn_id
            );
        }
        return match existing.status.as_str() {
            STATUS_COMMITTED => Ok(PrepareTurnFinalizationOutcome::AlreadyCommitted),
            STATUS_PREPARED => Ok(PrepareTurnFinalizationOutcome::AlreadyPrepared),
            status => anyhow::bail!(
                "turn `{}` has unknown finalization state `{status}`",
                notification.turn_id
            ),
        };
    }

    turn_finalization::ActiveModel {
        turn_id: Set(notification.turn_id.clone()),
        thread_id: Set(notification.thread_id.clone()),
        workspace_id: Set(notification.workspace_id.clone()),
        generation: Set(generation),
        item_id: Set(notification.item.item_id().to_owned()),
        item_json: Set(item_json),
        item_digest: Set(digest),
        status: Set(STATUS_PREPARED.to_owned()),
        prepared_at: Set(prepared_at),
        committed_at: Set(None),
        created_at: Set(prepared_at),
        updated_at: Set(prepared_at),
    }
    .insert(db)
    .await
    .context("failed to insert turn finalization")?;
    Ok(PrepareTurnFinalizationOutcome::Prepared)
}

pub async fn find_by_turn_id<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<turn_finalization::Model>> {
    turn_finalization::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query turn finalization")
}

pub async fn list_prepared<C: ConnectionTrait>(
    db: &C,
    limit: u64,
) -> Result<Vec<turn_finalization::Model>> {
    turn_finalization::Entity::find()
        .filter(turn_finalization::Column::Status.eq(STATUS_PREPARED))
        .limit(limit)
        .all(db)
        .await
        .context("failed to list prepared turn finalizations")
}

pub async fn delete_prepared_by_turn_id<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<bool> {
    let affected = turn_finalization::Entity::delete_many()
        .filter(turn_finalization::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_finalization::Column::Status.eq(STATUS_PREPARED))
        .exec(db)
        .await
        .context("failed to delete superseded prepared turn finalization")?
        .rows_affected;
    Ok(affected == 1)
}

pub async fn mark_committed<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    generation: i64,
    committed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = turn_finalization::Entity::update_many()
        .col_expr(
            turn_finalization::Column::Status,
            sea_orm::sea_query::Expr::value(STATUS_COMMITTED.to_owned()),
        )
        .col_expr(
            turn_finalization::Column::CommittedAt,
            sea_orm::sea_query::Expr::value(Some(committed_at)),
        )
        .col_expr(
            turn_finalization::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(committed_at),
        )
        .filter(turn_finalization::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_finalization::Column::Generation.eq(generation))
        .filter(turn_finalization::Column::Status.eq(STATUS_PREPARED))
        .exec(db)
        .await
        .context("failed to mark turn finalization committed")?
        .rows_affected;
    Ok(affected == 1)
}

pub fn notification_from_model(
    model: &turn_finalization::Model,
) -> Result<ItemCompletedNotification> {
    let item = serde_json::from_str(&model.item_json)
        .context("failed to decode durable finalization item")?;
    Ok(ItemCompletedNotification {
        workspace_id: model.workspace_id.clone(),
        thread_id: model.thread_id.clone(),
        turn_id: model.turn_id.clone(),
        item,
    })
}
