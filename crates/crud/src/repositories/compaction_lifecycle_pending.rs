//! Current obligations only. Times are Unix milliseconds throughout.
use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_context as context, compaction_lifecycle_pending as pending,
    compaction_lifecycle_scope as scope, compaction_lifecycle_sequence as sequence,
    compaction_operation as operation,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::{BinOper, Expr, ExprTrait, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
};

pub type CompactionLifecycleCandidate = pending::Model;
/// Reservation failed and its exact-snapshot deferral could not be stored.
/// The worker must delay its next quantum; an immediate rediscovery is unsafe.
#[derive(Debug)]
pub struct CompactionLifecycleStorageError(anyhow::Error);
impl std::fmt::Display for CompactionLifecycleStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("compaction lifecycle bookkeeping unavailable")
    }
}
impl std::error::Error for CompactionLifecycleStorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
#[derive(Clone, Debug)]
pub struct CompactionLifecycleClaim {
    pub(crate) row: pending::Model,
}
impl CompactionLifecycleClaim {
    pub fn operation_id(&self) -> &str {
        &self.row.operation_id
    }
}
fn delay(attempts: i64) -> i64 {
    std::cmp::min(
        5_000_i64 << attempts.saturating_sub(1).clamp(0, 15),
        300_000,
    )
}
fn pending_filter(row: &pending::Model) -> sea_orm::Condition {
    sea_orm::Condition::all()
        .add(pending::Column::OperationId.eq(row.operation_id.clone()))
        .add(pending::Column::Generation.eq(row.generation))
        .add(pending::Column::EligibleAt.eq(row.eligible_at))
        .add(pending::Column::RetryNotBefore.eq(row.retry_not_before))
        .add(pending::Column::DueAt.eq(row.due_at))
        .add(pending::Column::Attempts.eq(row.attempts))
        .add(
            Expr::col(pending::Column::ClaimToken)
                .binary(BinOper::Is, Expr::val(row.claim_token.clone())),
        )
}
pub(crate) async fn owns<C: ConnectionTrait>(
    db: &C,
    claim: &CompactionLifecycleClaim,
) -> Result<bool> {
    Ok(pending::Entity::find()
        .filter(pending_filter(&claim.row))
        .one(db)
        .await?
        .is_some())
}
pub(crate) async fn due<C: ConnectionTrait>(db: &C, now: i64) -> Result<Vec<pending::Model>> {
    Ok(pending::Entity::find()
        .filter(pending::Column::DueAt.lte(now))
        .order_by_asc(pending::Column::DueAt)
        .order_by_asc(pending::Column::Generation)
        .order_by_asc(pending::Column::OperationId)
        .limit(8)
        .all(db)
        .await?)
}
pub(crate) async fn claim(
    db: &SqliteDatabase,
    row: &pending::Model,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<Option<CompactionLifecycleClaim>> {
    let token = uuid::Uuid::new_v4().to_string();
    let attempts = std::cmp::min(row.attempts.saturating_add(1), 16);
    let retry_delay = delay(attempts);
    let reserve = async {
        let tx = db.begin().await?;
        let now = clock();
        let retry = now.saturating_add(retry_delay);
        let count = pending::Entity::update_many()
            .col_expr(pending::Column::ClaimToken, Expr::val(token.clone()))
            .col_expr(pending::Column::Attempts, Expr::val(attempts))
            .col_expr(pending::Column::RetryNotBefore, Expr::val(retry))
            .col_expr(
                pending::Column::DueAt,
                Expr::val(std::cmp::max(row.eligible_at, retry)),
            )
            .filter(pending_filter(row))
            .filter(pending::Column::DueAt.lte(now))
            .exec(&tx)
            .await?
            .rows_affected;
        // Unknown commit is never retried; the caller skips this locator.
        tx.commit().await?;
        let mut claimed = row.clone();
        claimed.claim_token = Some(token.clone());
        claimed.attempts = attempts;
        claimed.retry_not_before = retry;
        claimed.due_at = std::cmp::max(claimed.eligible_at, retry);
        Ok::<_, sea_orm::DbErr>((count == 1).then_some(CompactionLifecycleClaim { row: claimed }))
    }
    .await;
    match reserve {
        Ok(claim) => Ok(claim),
        Err(error) => {
            // Exact full pre-claim snapshot, including NULL token. If the
            // commit succeeded, this CAS cannot defer the successful holder.
            let defer = async {
                let tx = db.begin().await?;
                let now = clock();
                let retry = now.saturating_add(retry_delay);
                pending::Entity::update_many()
                    .col_expr(pending::Column::Attempts, Expr::val(attempts))
                    .col_expr(pending::Column::RetryNotBefore, Expr::val(retry))
                    .col_expr(
                        pending::Column::DueAt,
                        Expr::val(std::cmp::max(row.eligible_at, retry)),
                    )
                    .filter(pending_filter(row))
                    .filter(pending::Column::DueAt.lte(now))
                    .exec(&tx)
                    .await?;
                tx.commit().await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(error) = defer {
                return Err(CompactionLifecycleStorageError(error).into());
            }
            Err(error.into())
        }
    }
}
/// Only called after checking the exact facts/claim under this same writer.
/// A successful own source write may have bumped generation; this is the
/// post-repair row, protected from other writers by the domain transaction.
pub(crate) async fn acknowledge<C: ConnectionTrait>(
    db: &C,
    id: &str,
    next_eligibility: Option<i64>,
) -> Result<()> {
    if let Some(deadline) = next_eligibility {
        pending::Entity::update_many()
            .col_expr(pending::Column::EligibleAt, Expr::val(deadline))
            .col_expr(pending::Column::RetryNotBefore, Expr::val(0_i64))
            .col_expr(pending::Column::DueAt, Expr::val(deadline))
            .col_expr(pending::Column::Attempts, Expr::val(0_i64))
            .col_expr(pending::Column::ClaimToken, Expr::val(None::<String>))
            .filter(pending::Column::OperationId.eq(id))
            .exec(db)
            .await?;
    } else {
        pending::Entity::delete_by_id(id).exec(db).await?;
    }
    Ok(())
}
fn scope_filter(row: &scope::Model) -> sea_orm::Condition {
    sea_orm::Condition::all()
        .add(scope::Column::Kind.eq(row.kind.clone()))
        .add(scope::Column::Owner.eq(row.owner.clone()))
        .add(scope::Column::TurnId.eq(row.turn_id.clone()))
        .add(scope::Column::Generation.eq(row.generation))
        .add(scope::Column::DueAt.eq(row.due_at))
        .add(scope::Column::Attempts.eq(row.attempts))
        .add(
            Expr::col(scope::Column::ClaimToken)
                .binary(BinOper::Is, Expr::val(row.claim_token.clone())),
        )
        .add(
            Expr::col(scope::Column::CursorId)
                .binary(BinOper::Is, Expr::val(row.cursor_id.clone())),
        )
        .add(Expr::col(scope::Column::UpperId).binary(BinOper::Is, Expr::val(row.upper_id.clone())))
}
fn operations(row: &scope::Model) -> sea_orm::Select<operation::Entity> {
    let query = operation::Entity::find();
    match row.kind.as_str() {
        "seed" => query.filter(operation::Column::Status.eq("running")),
        "turn" => query.filter(operation::Column::ExecutionTurn.eq(row.turn_id.clone())),
        "owner_turn" => query
            .filter(operation::Column::Owner.eq(row.owner.clone()))
            .filter(operation::Column::ExecutionTurn.eq(row.turn_id.clone())),
        "owner" => query.filter(operation::Column::Owner.eq(row.owner.clone())),
        _ => query.filter(Expr::val(0).eq(1)),
    }
}
async fn next_generation<C: ConnectionTrait>(db: &C) -> Result<i64> {
    let row = sequence::Entity::find_by_id(1)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("lifecycle sequence missing"))?;
    let generation = row
        .generation
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("lifecycle generation exhausted"))?;
    sequence::Entity::update_many()
        .col_expr(sequence::Column::Generation, Expr::val(generation))
        .filter(sequence::Column::Singleton.eq(1))
        .exec(db)
        .await?;
    Ok(generation)
}
async fn enqueue<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    // Expansion wakes the exact locator immediately so it can evaluate Stop
    // and bindings. A successful running check restores its semantic deadline.
    let generation = next_generation(db).await?;
    pending::Entity::insert(pending::ActiveModel {
        operation_id: sea_orm::Set(id.to_owned()),
        generation: sea_orm::Set(generation),
        eligible_at: sea_orm::Set(0),
        retry_not_before: sea_orm::Set(0),
        due_at: sea_orm::Set(0),
        attempts: sea_orm::Set(0),
        claim_token: sea_orm::Set(None),
    })
    .on_conflict(
        OnConflict::column(pending::Column::OperationId)
            .update_columns([
                pending::Column::Generation,
                pending::Column::EligibleAt,
                pending::Column::ClaimToken,
            ])
            .value(
                pending::Column::DueAt,
                Expr::col((pending::Entity, pending::Column::RetryNotBefore)),
            )
            .to_owned(),
    )
    .exec_without_returning(db)
    .await?;
    Ok(())
}
/// Each class gets four inputs, including the scope locator itself (up to
/// three expansion outputs). Pending attempts get eight: total <=16.
pub(crate) async fn expand(
    db: &SqliteDatabase,
    seed: bool,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<u64> {
    let query = scope::Entity::find().filter(scope::Column::DueAt.lte(clock()));
    let row = if seed {
        // The singleton seed locator uses the complete scope PK. Once it is
        // gone, idle seed discovery must not walk ordinary due scopes.
        query
            .filter(scope::Column::Kind.eq("seed"))
            .filter(scope::Column::Owner.eq(""))
            .filter(scope::Column::TurnId.eq(""))
    } else {
        query.filter(scope::Column::Kind.ne("seed"))
    };
    let Some(row) = row
        .order_by_asc(scope::Column::DueAt)
        .order_by_asc(scope::Column::Generation)
        .one(db)
        .await
        .map_err(|error| CompactionLifecycleStorageError(error.into()))?
    else {
        return Ok(0);
    };
    let token = uuid::Uuid::new_v4().to_string();
    let attempts = std::cmp::min(row.attempts.saturating_add(1), 16);
    let reserve = async {
        let tx = db.begin().await?;
        let reserved_at = clock();
        let count = scope::Entity::update_many()
            .col_expr(scope::Column::ClaimToken, Expr::val(token.clone()))
            .col_expr(scope::Column::Attempts, Expr::val(attempts))
            .col_expr(
                scope::Column::DueAt,
                Expr::val(reserved_at.saturating_add(delay(attempts))),
            )
            .filter(scope_filter(&row))
            .filter(scope::Column::DueAt.lte(reserved_at))
            .exec(&tx)
            .await?
            .rows_affected;
        let claimed =
            scope::Entity::find_by_id((row.kind.clone(), row.owner.clone(), row.turn_id.clone()))
                .one(&tx)
                .await?;
        tx.commit().await?;
        Ok::<_, sea_orm::DbErr>((count == 1).then_some(claimed).flatten())
    }
    .await;
    let claimed = match reserve {
        Ok(Some(row)) => row,
        Ok(None) => return Ok(1),
        Err(error) => {
            let defer = async {
                let tx = db.begin().await?;
                let now = clock();
                scope::Entity::update_many()
                    .col_expr(scope::Column::Attempts, Expr::val(attempts))
                    .col_expr(
                        scope::Column::DueAt,
                        Expr::val(now.saturating_add(delay(attempts))),
                    )
                    .filter(scope_filter(&row))
                    .filter(scope::Column::DueAt.lte(now))
                    .exec(&tx)
                    .await?;
                tx.commit().await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(error) = defer {
                return Err(CompactionLifecycleStorageError(error).into());
            }
            return Err(error.into());
        }
    };
    ensure!(
        ["seed", "turn", "owner_turn", "owner", "thread"].contains(&claimed.kind.as_str()),
        "invalid lifecycle scope kind"
    );
    // Preparation reads bounded metadata, releases the reader, then commits
    // cursor and enqueue atomically. Significant refresh resets both bounds.
    let upper = if let Some(upper) = claimed.upper_id.clone() {
        Some(upper)
    } else if claimed.kind == "thread" {
        context::Entity::find()
            .select_only()
            .column(context::Column::Owner)
            .filter(context::Column::ThreadId.eq(claimed.turn_id.clone()))
            .order_by_desc(context::Column::Owner)
            .into_tuple::<String>()
            .one(db)
            .await?
    } else {
        operations(&claimed)
            .select_only()
            .column(operation::Column::Id)
            .order_by_desc(operation::Column::Id)
            .into_tuple::<String>()
            .one(db)
            .await?
    };
    let ids = if let Some(upper) = upper.as_ref() {
        if claimed.kind == "thread" {
            context::Entity::find()
                .select_only()
                .column(context::Column::Owner)
                .filter(context::Column::ThreadId.eq(claimed.turn_id.clone()))
                .filter(
                    sea_orm::Condition::all().add_option(
                        claimed
                            .cursor_id
                            .as_ref()
                            .map(|cursor| context::Column::Owner.gt(cursor.clone())),
                    ),
                )
                .filter(context::Column::Owner.lte(upper.clone()))
                .order_by_asc(context::Column::Owner)
                .limit(3)
                .into_tuple::<String>()
                .all(db)
                .await?
        } else {
            operations(&claimed)
                .select_only()
                .column(operation::Column::Id)
                .filter(
                    sea_orm::Condition::all().add_option(
                        claimed
                            .cursor_id
                            .as_ref()
                            .map(|cursor| operation::Column::Id.gt(cursor.clone())),
                    ),
                )
                .filter(operation::Column::Id.lte(upper.clone()))
                .order_by_asc(operation::Column::Id)
                .limit(3)
                .into_tuple::<String>()
                .all(db)
                .await?
        }
    } else {
        Vec::new()
    };
    let tx = db.begin().await?;
    if scope::Entity::find()
        .filter(scope_filter(&claimed))
        .one(&tx)
        .await?
        .is_none()
    {
        tx.rollback().await?;
        return Ok(1 + ids.len() as u64);
    }
    for id in &ids {
        if claimed.kind == "thread" {
            let generation = next_generation(&tx).await?;
            scope::Entity::insert(scope::ActiveModel {
                kind: sea_orm::Set("owner".into()),
                owner: sea_orm::Set(id.clone()),
                turn_id: sea_orm::Set(String::new()),
                generation: sea_orm::Set(generation),
                due_at: sea_orm::Set(0),
                attempts: sea_orm::Set(0),
                claim_token: sea_orm::Set(None),
                cursor_id: sea_orm::Set(None),
                upper_id: sea_orm::Set(None),
            })
            .on_conflict(
                OnConflict::columns([
                    scope::Column::Kind,
                    scope::Column::Owner,
                    scope::Column::TurnId,
                ])
                .update_columns([
                    scope::Column::Generation,
                    scope::Column::ClaimToken,
                    scope::Column::CursorId,
                    scope::Column::UpperId,
                ])
                .to_owned(),
            )
            .exec_without_returning(&tx)
            .await?;
        } else {
            // IDs may have disappeared or rebound since discovery. Enqueue is
            // still safe; the point repair checks all fresh bindings.
            enqueue(&tx, id).await?;
        }
    }
    if ids.len() < 3 || ids.last() == upper.as_ref() {
        scope::Entity::delete_many()
            .filter(scope_filter(&claimed))
            .exec(&tx)
            .await?;
        if seed {
            sequence::Entity::update_many()
                .col_expr(sequence::Column::SeedComplete, Expr::val(1_i64))
                .filter(sequence::Column::Singleton.eq(1))
                .exec(&tx)
                .await?;
        }
    } else {
        scope::Entity::update_many()
            .col_expr(scope::Column::CursorId, Expr::val(ids.last().cloned()))
            .col_expr(scope::Column::UpperId, Expr::val(upper))
            .col_expr(scope::Column::ClaimToken, Expr::val(None::<String>))
            .col_expr(scope::Column::Attempts, Expr::val(0_i64))
            .col_expr(scope::Column::DueAt, Expr::val(clock()))
            .filter(scope_filter(&claimed))
            .exec(&tx)
            .await?;
    }
    tx.commit().await?;
    Ok(1 + ids.len() as u64)
}
