//! Server unread eligibility is independent of confirmed read receipts.
//! Access uses existing second-resolution membership timestamps. First readable
//! positions survive projection replacement; edits and replay never republish.
use anyhow::{Context, Result};
use pioneer_entity::unread_message_position;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Set};

pub(crate) async fn publish_message_position<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    block_id: &str,
    turn_id: &str,
    candidate: String,
    published_at: i64,
) -> Result<String> {
    let existing =
        unread_message_position::Entity::find_by_id((thread_id.to_owned(), block_id.to_owned()))
            .select_only()
            .column(unread_message_position::Column::SortKey)
            .into_tuple::<String>()
            .one(db)
            .await?;
    if let Some(existing) = existing {
        // Empty marks historical source absent from the rollout projection.
        // Restore its position but retain published_at=0: never new unread.
        if existing.is_empty() {
            unread_message_position::Entity::update_many()
                .filter(unread_message_position::Column::ThreadId.eq(thread_id))
                .filter(unread_message_position::Column::BlockId.eq(block_id))
                .col_expr(
                    unread_message_position::Column::SortKey,
                    Expr::val(candidate.clone()),
                )
                .exec(db)
                .await?;
            return Ok(candidate);
        }
        return Ok(existing);
    }
    // Late answers must reach the grant's timestamp range, even when the Turn
    // and item began before access. Retain the accepted per-thread logical tick
    // rule for real read cursors; no global publication clock is needed.
    let (tick, suffix) = candidate
        .split_once(':')
        .context("invalid message position")?;
    let tick = std::cmp::max(
        tick.parse::<u64>()?,
        u64::try_from(std::cmp::max(published_at, 0))?.saturating_mul(1000),
    );
    let candidate = format!("{tick:020}:{suffix}");
    use pioneer_entity::{thread_read_cursor as cursor, thread_timeline_block as block};
    // Exact confirmed reads can survive projection loss. Both maxima use
    // existing per-thread indexes, in the same bounded SELECT as before.
    let blocks = Query::select()
        .expr(Expr::col(block::Column::SortKey).max())
        .from(block::Entity)
        .and_where(block::Column::ThreadId.eq(thread_id))
        .to_owned();
    let reads = Query::select()
        .expr(Expr::col(cursor::Column::LastReadSortKey).max())
        .from(cursor::Entity)
        .and_where(cursor::Column::ThreadId.eq(thread_id))
        .to_owned();
    let latest_query = Query::select()
        .expr_as(
            Func::cust(Alias::new("MAX"))
                .arg(Expr::from(blocks).if_null(""))
                .arg(Expr::from(reads).if_null("")),
            "position",
        )
        .to_owned();
    let latest = db
        .query_one_raw(db.get_database_backend().build(&latest_query))
        .await?
        .context("missing latest readable position")?
        .try_get::<String>("", "position")?;
    let position = if latest >= candidate {
        let (tick, _) = latest
            .split_once(':')
            .context("invalid latest message position")?;
        let (_, suffix) = candidate
            .split_once(':')
            .context("invalid new message position")?;
        let tick = tick
            .parse::<u64>()?
            .checked_add(1)
            .context("message position overflow")?;
        format!("{tick:020}:{suffix}")
    } else {
        candidate
    };
    unread_message_position::Entity::insert(unread_message_position::ActiveModel {
        thread_id: Set(thread_id.into()),
        block_id: Set(block_id.into()),
        turn_id: Set(turn_id.into()),
        sort_key: Set(position.clone()),
        published_at: Set(published_at),
    })
    .exec_without_returning(db)
    .await?;
    Ok(position)
}
