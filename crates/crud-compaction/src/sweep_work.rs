//! Durable eligibility for the physical frozen-storage sweep.

use std::{future::Future, time::Duration};

use anyhow::Result;
use pioneer_entity::{
    compaction_frozen_span as span, compaction_frozen_sweep_consumer as consumer,
    compaction_frozen_sweep_fanout as fanout, compaction_frozen_sweep_work as work,
};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use sea_orm::sea_query::{Alias, BinOper, Expr, ExprTrait, Func, OnConflict, Order, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Statement, TransactionTrait,
};

pub(crate) async fn retry_conflict<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    retry_with_backoff(
        operation,
        is_retryable_database_conflict,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

#[derive(Clone)]
pub struct Work {
    pub manifest: String,
    pub kind: i64,
    pub revision: i64,
}

fn addressed_scope(targets: Option<&[String]>) -> Condition {
    match targets {
        Some(ids) => {
            Condition::all().add(Expr::col(Alias::new("manifest_id")).is_in(ids.iter().cloned()))
        }
        None => Condition::all(),
    }
}

pub async fn fanout_step(db: &PostgresDatabase, targets: Option<&[String]>) -> Result<bool> {
    // An explicit insertion ordinal bounds each service round.
    let cursor = db
        .query_one(
            &Query::select()
                .columns([
                    fanout::Column::Revision,
                    fanout::Column::AfterRow,
                    fanout::Column::UntilRow,
                ])
                .from(fanout::Entity)
                .and_where(Expr::col(fanout::Column::Id).eq(1))
                .to_owned(),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("fanout cursor missing"))?;
    let turn: i64 = cursor.try_get("", "revision")?;
    let after_row: i64 = cursor.try_get("", "after_row")?;
    let mut until_row: i64 = cursor.try_get("", "until_row")?;
    let mut query = Query::select();
    query
        .columns([
            consumer::Column::ConsumerRow,
            consumer::Column::ManifestId,
            consumer::Column::Kind,
            consumer::Column::Revision,
            consumer::Column::AfterSource,
        ])
        .from(consumer::Entity)
        .cond_where(addressed_scope(targets))
        .and_where(Expr::col(consumer::Column::Pending).eq(1))
        .and_where(Expr::col(consumer::Column::ConsumerRow).gt(after_row))
        .and_where(Expr::col(consumer::Column::ConsumerRow).lte(until_row))
        .order_by(consumer::Column::ConsumerRow, Order::Asc)
        .limit(1);
    let mut row = db.query_one(&query).await?;
    if row.is_none() {
        until_row = db
            .query_one(
                &Query::select()
                    .expr_as(
                        Func::coalesce([
                            Expr::col(consumer::Column::ConsumerRow).max(),
                            Expr::val(0),
                        ]),
                        Alias::new("until_row"),
                    )
                    .from(consumer::Entity)
                    .cond_where(addressed_scope(targets))
                    .to_owned(),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("consumer frontier missing"))?
            .try_get("", "until_row")?;
        row = db
            .query_one(
                &Query::select()
                    .columns([
                        consumer::Column::ConsumerRow,
                        consumer::Column::ManifestId,
                        consumer::Column::Kind,
                        consumer::Column::Revision,
                        consumer::Column::AfterSource,
                    ])
                    .from(consumer::Entity)
                    .cond_where(addressed_scope(targets))
                    .and_where(Expr::col(consumer::Column::Pending).eq(1))
                    .and_where(Expr::col(consumer::Column::ConsumerRow).lte(until_row))
                    .order_by(consumer::Column::ConsumerRow, Order::Asc)
                    .limit(1)
                    .to_owned(),
            )
            .await?;
    }
    let Some(row) = row else { return Ok(false) };
    let consumer_row: i64 = row.try_get("", "consumer_row")?;
    let id: String = row.try_get("", "manifest_id")?;
    let kind: i64 = row.try_get("", "kind")?;
    let revision: i64 = row.try_get("", "revision")?;
    let after: Option<String> = row.try_get("", "after_source")?;
    let mut query = Query::select();
    query
        .column(span::Column::SourceManifest)
        .from(span::Entity)
        .and_where(Expr::col(span::Column::ManifestId).eq(&id))
        .and_where(Expr::col(span::Column::Kind).eq(kind));
    if let Some(after) = &after {
        query.and_where(Expr::col(span::Column::SourceManifest).gt(after));
    }
    query
        .order_by(span::Column::SourceManifest, Order::Asc)
        .limit(1);
    let next = db.query_one(&query).await?;
    let next: Option<String> = next.map(|r| r.try_get("", "source_manifest")).transpose()?;
    retry_conflict(|| async {
        let tx = db.begin().await?;
        // Rotation advances even on consumer CAS conflict, so reset cannot pin
        // later consumers. Rotation, source cursor and delivery commit together.
        let rotated = tx
            .execute(
                &Query::update()
                    .table(fanout::Entity)
                    .value(
                        fanout::Column::Revision,
                        Expr::col(fanout::Column::Revision).add(1),
                    )
                    .value(fanout::Column::AfterRow, consumer_row)
                    .value(fanout::Column::UntilRow, until_row)
                    .and_where(Expr::col(fanout::Column::Id).eq(1))
                    .and_where(Expr::col(fanout::Column::Revision).eq(turn))
                    .to_owned(),
            )
            .await?
            .rows_affected();
        if rotated == 0 {
            tx.commit().await?;
            return Ok(());
        }
        if next.is_some() {
            // Frozen-history triggers acquire the queue allocator lock before
            // updating consumers. A fanout that enqueues work must take it in
            // the same order, before its consumer compare-and-set.
            tx.execute_unprepared("SELECT pg_catalog.pg_advisory_xact_lock(1885955182, 2)")
                .await?;
        }
        let changed = tx
            .execute(
                &Query::update()
                    .table(consumer::Entity)
                    .value(consumer::Column::AfterSource, next.clone())
                    .value(consumer::Column::Pending, i64::from(next.is_some()))
                    .and_where(Expr::col(consumer::Column::ManifestId).eq(&id))
                    .and_where(Expr::col(consumer::Column::Kind).eq(kind))
                    .and_where(Expr::col(consumer::Column::Revision).eq(revision))
                    .and_where(Expr::col(consumer::Column::Pending).eq(1))
                    .and_where(Expr::col(consumer::Column::AfterSource).binary(
                        BinOper::Custom("IS NOT DISTINCT FROM"),
                        Expr::val(after.clone()),
                    ))
                    .to_owned(),
            )
            .await?
            .rows_affected();
        if changed == 1 {
            if let Some(source) = &next {
                tx.execute(
                    &Query::insert()
                        .into_table(work::Entity)
                        .columns([work::Column::ManifestId, work::Column::Kind])
                        .values_panic([source.clone().into(), kind.into()])
                        .on_conflict(
                            OnConflict::columns([work::Column::ManifestId, work::Column::Kind])
                                .value(
                                    work::Column::Revision,
                                    Expr::col((work::Entity, work::Column::Revision)).add(1),
                                )
                                .value(work::Column::Pending, 1)
                                .value(work::Column::RetryAt, 0)
                                .to_owned(),
                        )
                        .to_owned(),
                )
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    })
    .await?;
    Ok(true)
}

/// Advisory service order only: losing it cannot acknowledge durable work.
#[derive(Default)]
pub struct ServiceRound {
    pub after_row: i64,
    pub until_row: i64,
    complete: bool,
    propagated: bool,
}

impl ServiceRound {
    pub fn invalidate(&mut self) {
        self.propagated = true;
    }
}

pub enum Completion {
    Complete(Statement),
    Blocked,
    Retry,
}

pub async fn next_retry_deadline(db: &PostgresDatabase) -> Result<Option<i64>> {
    Ok(work::Entity::find()
        .select_only()
        .column(work::Column::RetryAt)
        .filter(work::Column::Pending.eq(1))
        .filter(work::Column::RetryAt.gt(0))
        .order_by_asc(work::Column::RetryAt)
        .into_tuple::<i64>()
        .one(db)
        .await?)
}

pub enum Admission {
    Selected(Work),
    Continue,
    Idle,
}

pub async fn discover(
    db: &PostgresDatabase,
    active: &[(&str, i64)],
    kind: i64,
    round: &mut ServiceRound,
    targets: Option<&[String]>,
    retries_only: bool,
) -> Result<Admission> {
    if round.complete || (round.until_row > 0 && round.after_row >= round.until_row) {
        if !round.propagated {
            round.complete = true;
            return Ok(Admission::Idle);
        }
        // Finish the finite frontier before revisiting keys invalidated by
        // fanout behind it. An ACK can release a pre-fanout deletion fence
        // without changing that source's work revision again.
        *round = ServiceRound::default();
    }
    let mut due = Query::select();
    due.expr(Expr::val(1))
        .from(work::Entity)
        .cond_where(addressed_scope(targets))
        .cond_where(if retries_only {
            Condition::all().add(work::Column::RetryAt.gt(0))
        } else {
            Condition::all()
        })
        .and_where(Expr::col(work::Column::Pending).eq(1))
        .and_where(Expr::col(work::Column::Kind).eq(kind))
        .and_where(Expr::col(work::Column::RetryAt).lte(Expr::cust(
            "floor(extract(epoch FROM statement_timestamp()))::bigint",
        )))
        .limit(1);
    for (id, kind) in active {
        due.and_where(
            Expr::col(work::Column::ManifestId)
                .eq(*id)
                .and(Expr::col(work::Column::Kind).eq(*kind))
                .not(),
        );
    }
    // No due non-active work means idle without walking future-retry pages.
    if db.query_one(&due).await?.is_none() {
        round.complete = true;
        round.propagated = false;
        return Ok(Admission::Idle);
    }
    if round.after_row >= round.until_row {
        round.until_row = db
            .query_one(
                &Query::select()
                    .expr_as(
                        Func::coalesce([Expr::col(work::Column::WorkRow).max(), Expr::val(0)]),
                        Alias::new("until_row"),
                    )
                    .from(work::Entity)
                    .cond_where(addressed_scope(targets))
                    .cond_where(if retries_only {
                        Condition::all().add(work::Column::RetryAt.gt(0))
                    } else {
                        Condition::all()
                    })
                    .to_owned(),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("work frontier missing"))?
            .try_get("", "until_row")?;
        round.after_row = 0;
        // This frontier already includes propagation completed before its capture.
        round.propagated = false;
    }
    // Stable insertion order, not mutable retry_at or TEXT order. New rows
    // cannot enter this round; re-dirty/retry cannot move a visited row ahead.
    let page = Query::select()
        .columns([
            work::Column::WorkRow,
            work::Column::ManifestId,
            work::Column::Kind,
            work::Column::Revision,
        ])
        .expr_as(
            Expr::col(work::Column::RetryAt).lte(Expr::cust(
                "floor(extract(epoch FROM statement_timestamp()))::bigint",
            )),
            Alias::new("due"),
        )
        .from(work::Entity)
        .cond_where(addressed_scope(targets))
        .cond_where(if retries_only {
            Condition::all().add(work::Column::RetryAt.gt(0))
        } else {
            Condition::all()
        })
        .and_where(Expr::col(work::Column::Pending).eq(1))
        .and_where(Expr::col(work::Column::Kind).eq(kind))
        .and_where(Expr::col(work::Column::WorkRow).gt(round.after_row))
        .and_where(Expr::col(work::Column::WorkRow).lte(round.until_row))
        .order_by(work::Column::WorkRow, Order::Asc)
        .limit(16)
        .to_owned();
    let rows = db.query_all(&page).await?;
    if rows.is_empty() {
        round.after_row = round.until_row;
    }
    for row in rows {
        round.after_row = row.try_get("", "work_row")?;
        let id: String = row.try_get("", "manifest_id")?;
        if !row.try_get::<bool>("", "due")? || active.contains(&(id.as_str(), kind)) {
            continue;
        }
        return Ok(Admission::Selected(Work {
            manifest: id,
            kind,
            revision: row.try_get("", "revision")?,
        }));
    }
    // Due work can be behind the cursor or beyond this round's frontier.
    Ok(Admission::Continue)
}

impl Work {
    /// Relevant own/current reverse pending consumers fence pre-fanout changes.
    /// Disconnected OLD sources were dirtied directly by the span trigger.
    pub fn fence(&self) -> Statement {
        let own = Query::select()
            .expr(Expr::val(1))
            .from(consumer::Entity)
            .and_where(Expr::col(consumer::Column::ManifestId).eq(&self.manifest))
            .and_where(Expr::col(consumer::Column::Kind).eq(self.kind))
            .and_where(Expr::col(consumer::Column::Pending).eq(1))
            .to_owned();
        let pending = Query::select()
            .expr(Expr::val(1))
            .from_as(consumer::Entity, Alias::new("c"))
            .and_where(
                Expr::col((Alias::new("c"), consumer::Column::ManifestId))
                    .eq(Expr::col((Alias::new("s"), span::Column::ManifestId))),
            )
            .and_where(
                Expr::col((Alias::new("c"), consumer::Column::Kind))
                    .eq(Expr::col((Alias::new("s"), span::Column::Kind))),
            )
            .and_where(Expr::col((Alias::new("c"), consumer::Column::Pending)).eq(1))
            .to_owned();
        let reverse = Query::select()
            .expr(Expr::val(1))
            .from_as(span::Entity, Alias::new("s"))
            .and_where(
                Expr::col((Alias::new("s"), span::Column::SourceManifest)).eq(&self.manifest),
            )
            .and_where(Expr::col((Alias::new("s"), span::Column::Kind)).eq(self.kind))
            .and_where(Expr::exists(pending))
            .to_owned();
        DbBackend::Postgres.build(
            &Query::select()
                .expr(Expr::val(1))
                .from(work::Entity)
                .and_where(Expr::col(work::Column::ManifestId).eq(&self.manifest))
                .and_where(Expr::col(work::Column::Kind).eq(self.kind))
                .and_where(Expr::col(work::Column::Revision).eq(self.revision))
                .and_where(Expr::col(work::Column::Pending).eq(1))
                .and_where(Expr::exists(own).not())
                .and_where(Expr::exists(reverse).not())
                .to_owned(),
        )
    }

    /// Completion validates geometry in this same short writer boundary.
    /// Expected protection stays pending until invalidation; errors retain a
    /// bounded retry. CAS never clears newer domain work in either case.
    pub async fn finish(&self, db: &PostgresDatabase, completion: Completion) -> Result<()> {
        retry_conflict(|| async {
            let tx = db.begin().await?;
            let valid = if let Completion::Complete(guard) = &completion {
                tx.query_one_raw(self.fence()).await?.is_some()
                    && tx.query_one_raw(guard.clone()).await?.is_some()
            } else {
                false
            };
            let retry = if valid {
                Expr::val(0)
            } else if !matches!(completion, Completion::Retry) {
                // A protected generation is reconsidered on dependency
                // invalidation. It is not an error-owned clock retry.
                Expr::val(0)
            } else {
                Expr::cust("floor(extract(epoch FROM statement_timestamp()))::bigint").add(60)
            };
            tx.execute(
                &Query::update()
                    .table(work::Entity)
                    .value(work::Column::Pending, i64::from(!valid))
                    .value(work::Column::RetryAt, retry)
                    .and_where(Expr::col(work::Column::ManifestId).eq(&self.manifest))
                    .and_where(Expr::col(work::Column::Kind).eq(self.kind))
                    .and_where(Expr::col(work::Column::Revision).eq(self.revision))
                    .and_where(Expr::col(work::Column::Pending).eq(1))
                    .to_owned(),
            )
            .await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }
}
