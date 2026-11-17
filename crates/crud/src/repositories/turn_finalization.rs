use anyhow::{Context, Result};
use pioneer_entity::{
    turn, turn_event, turn_event_projection_stream_state as stream, turn_finalization,
};
use pioneer_protocol::{ItemCompletedNotification, TurnItem};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait, Query};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, FromQueryResult,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set,
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

/// Presence discovery does not materialize the accepted provider answer.
pub async fn exists<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<bool> {
    Ok(turn_finalization::Entity::find_by_id(turn_id.to_owned())
        .count(db)
        .await
        .context("failed to check turn finalization presence")?
        != 0)
}

pub async fn list_prepared_ids<C: ConnectionTrait>(db: &C, limit: u64) -> Result<Vec<String>> {
    turn_finalization::Entity::find()
        .select_only()
        .column(turn_finalization::Column::TurnId)
        .filter(turn_finalization::Column::Status.eq(STATUS_PREPARED))
        .order_by_asc(turn_finalization::Column::PreparedAt)
        .order_by_asc(turn_finalization::Column::TurnId)
        .limit(limit)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to list prepared turn finalizations")
}

/// Owner-local progress only. Durable intent rows remain the source of truth;
/// restart or an end-of-pass resets this cursor and retries every failed row.
#[derive(Debug, Clone, FromQueryResult)]
pub(crate) struct PreparedCursor {
    pub turn_id: String,
    pub prepared_at: DateTimeWithTimeZone,
}

#[derive(Debug, Default)]
pub(crate) struct PreparedPass {
    pub after: Option<PreparedCursor>,
    pub upper: Option<PreparedCursor>,
    pub failed: bool,
}

pub(crate) async fn prepared_upper<C: ConnectionTrait>(db: &C) -> Result<Option<PreparedCursor>> {
    turn_finalization::Entity::find()
        .select_only()
        .column(turn_finalization::Column::TurnId)
        .column(turn_finalization::Column::PreparedAt)
        .filter(turn_finalization::Column::Status.eq(STATUS_PREPARED))
        .order_by_desc(turn_finalization::Column::PreparedAt)
        .order_by_desc(turn_finalization::Column::TurnId)
        .into_model::<PreparedCursor>()
        .one(db)
        .await
        .context("failed to discover prepared finalization pass upper bound")
}

/// Prefix eligibility is applied before LIMIT. Missing dependencies are kept as
/// candidates so corrupt data reports an error rather than masquerading as idle.
pub(crate) async fn list_eligible_prepared<C: ConnectionTrait>(
    db: &C,
    after: Option<&PreparedCursor>,
    upper: &PreparedCursor,
    limit: u64,
) -> Result<Vec<PreparedCursor>> {
    eligible_prepared(db, after, Some(upper), limit, None).await
}

pub(crate) async fn list_eligible_prepared_for_turns<C: ConnectionTrait>(
    db: &C,
    turn_ids: &[String],
) -> Result<Vec<PreparedCursor>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }
    eligible_prepared(db, None, None, turn_ids.len() as u64, Some(turn_ids)).await
}

async fn eligible_prepared<C: ConnectionTrait>(
    db: &C,
    after: Option<&PreparedCursor>,
    upper: Option<&PreparedCursor>,
    limit: u64,
    turn_ids: Option<&[String]>,
) -> Result<Vec<PreparedCursor>> {
    let outer_id = || Expr::col((turn_finalization::Entity, turn_finalization::Column::TurnId));
    let active = Query::select()
        .expr(Expr::val(1))
        .from(turn::Entity)
        .and_where(Expr::col((turn::Entity, turn::Column::Id)).eq(outer_id()))
        .and_where(Expr::col((turn::Entity, turn::Column::Status)).eq("in_progress"))
        .to_owned();
    let events = Query::select()
        .expr(Expr::val(1))
        .from(turn_event::Entity)
        .and_where(Expr::col((turn_event::Entity, turn_event::Column::TurnId)).eq(outer_id()))
        .to_owned();
    let newer = Query::select()
        .expr(Expr::val(1))
        .from(turn_event::Entity)
        .and_where(Expr::col((turn_event::Entity, turn_event::Column::TurnId)).eq(outer_id()))
        .and_where(
            Expr::col((turn_event::Entity, turn_event::Column::Sequence)).gt(Expr::col((
                stream::Entity,
                stream::Column::ProjectedThroughSequence,
            ))),
        )
        .to_owned();
    let stream_exists = Query::select()
        .expr(Expr::val(1))
        .from(stream::Entity)
        .and_where(Expr::col((stream::Entity, stream::Column::TurnId)).eq(outer_id()))
        .to_owned();
    let ready_stream = Query::select()
        .expr(Expr::val(1))
        .from(stream::Entity)
        .and_where(Expr::col((stream::Entity, stream::Column::TurnId)).eq(outer_id()))
        .and_where(Expr::exists(newer).not())
        .to_owned();
    let mut query = turn_finalization::Entity::find()
        .select_only()
        .column(turn_finalization::Column::TurnId)
        .column(turn_finalization::Column::PreparedAt)
        .filter(turn_finalization::Column::Status.eq(STATUS_PREPARED))
        .filter(
            Condition::any()
                .add(Expr::exists(active).not())
                .add(Expr::exists(events).not())
                .add(Expr::exists(stream_exists).not())
                .add(Expr::exists(ready_stream)),
        );
    if let Some(turn_ids) = turn_ids {
        query = query.filter(turn_finalization::Column::TurnId.is_in(turn_ids.to_vec()));
    }
    if let Some(upper) = upper {
        query = query.filter(
            Condition::any()
                .add(turn_finalization::Column::PreparedAt.lt(upper.prepared_at))
                .add(
                    Condition::all()
                        .add(turn_finalization::Column::PreparedAt.eq(upper.prepared_at))
                        .add(turn_finalization::Column::TurnId.lte(upper.turn_id.clone())),
                ),
        );
    }
    if let Some(after) = after {
        query = query.filter(
            Condition::any()
                .add(turn_finalization::Column::PreparedAt.gt(after.prepared_at))
                .add(
                    Condition::all()
                        .add(turn_finalization::Column::PreparedAt.eq(after.prepared_at))
                        .add(turn_finalization::Column::TurnId.gt(after.turn_id.clone())),
                ),
        );
    }
    query
        .order_by_asc(turn_finalization::Column::PreparedAt)
        .order_by_asc(turn_finalization::Column::TurnId)
        .limit(limit)
        .into_model::<PreparedCursor>()
        .all(db)
        .await
        .context("failed to discover eligible prepared finalizations")
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
