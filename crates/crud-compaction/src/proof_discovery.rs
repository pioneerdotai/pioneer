//! Bounded, advisory discovery of unsealed checkpoint headers.

use anyhow::Result;
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_operation as operation,
    compaction_operation_projection as projection,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, JoinType, Order, Query};
use sea_orm::{Condition, ConnectionTrait, EntityTrait, QueryOrder, QuerySelect};

pub struct ProofCandidatePage {
    pub last_id: String,
    pub eligible: Vec<String>,
}

pub async fn proof_candidate_upper_bound(db: &impl ConnectionTrait) -> Result<Option<String>> {
    Ok(checkpoint::Entity::find()
        .select_only()
        .column(checkpoint::Column::Id)
        .order_by_desc(checkpoint::Column::Id)
        .into_tuple()
        .one(db)
        .await?)
}

pub async fn proof_candidate_page(
    db: &impl ConnectionTrait,
    targets: Option<&[String]>,
    after: &str,
    upper: &str,
    limit: u64,
) -> Result<Option<ProofCandidatePage>> {
    let headers = Query::select()
        .from(checkpoint::Entity)
        .cond_where(match targets {
            Some(ids) => Condition::all().add(
                Expr::col((checkpoint::Entity, checkpoint::Column::Id)).is_in(ids.iter().cloned()),
            ),
            None => Condition::all(),
        })
        .columns([
            checkpoint::Column::Id,
            checkpoint::Column::Owner,
            checkpoint::Column::OperationId,
            checkpoint::Column::ProofVersion,
            checkpoint::Column::Status,
        ])
        .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::ProofVersion)).eq(0))
        .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).gt(after))
        .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).lte(upper))
        .order_by((checkpoint::Entity, checkpoint::Column::Id), Order::Asc)
        .limit(limit)
        .to_owned();
    // Page the headers before joining metadata. Each header has at
    // most one matching operation and projection.
    let query = Query::select()
        .from_subquery(headers, Alias::new("compaction_checkpoint"))
        .columns(
            [
                checkpoint::Column::Id,
                checkpoint::Column::ProofVersion,
                checkpoint::Column::Status,
            ]
            .map(|column| (checkpoint::Entity, column)),
        )
        .expr_as(
            Expr::col((operation::Entity, operation::Column::Status)),
            Alias::new("operation_status"),
        )
        .expr_as(
            Expr::col((projection::Entity, projection::Column::OperationId)),
            Alias::new("bound"),
        )
        .join(
            JoinType::LeftJoin,
            operation::Entity,
            Expr::col((operation::Entity, operation::Column::Id))
                .eq(Expr::col((
                    checkpoint::Entity,
                    checkpoint::Column::OperationId,
                )))
                .and(
                    Expr::col((operation::Entity, operation::Column::Owner))
                        .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                ),
        )
        .join(
            JoinType::LeftJoin,
            projection::Entity,
            Expr::col((projection::Entity, projection::Column::OperationId)).eq(Expr::col((
                checkpoint::Entity,
                checkpoint::Column::OperationId,
            ))),
        )
        .order_by((checkpoint::Entity, checkpoint::Column::Id), Order::Asc)
        .to_owned();
    let rows = db.query_all(&query).await?;
    let Some(last) = rows.last() else {
        return Ok(None);
    };
    let mut page = ProofCandidatePage {
        last_id: last.try_get("", "id")?,
        eligible: Vec::new(),
    };
    for row in rows {
        let status: String = row.try_get("", "status")?;
        let op: Option<String> = row.try_get("", "operation_status")?;
        let no_projection = row.try_get::<Option<String>>("", "bound")?.is_none();
        if status == "applied"
            || (status == "retained" && op.as_deref() == Some("completed"))
            || no_projection
        {
            page.eligible.push(row.try_get("", "id")?);
        }
    }
    Ok(Some(page))
}
