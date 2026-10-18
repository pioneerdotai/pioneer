//! Durable eligibility only. These tokens never authorize physical deletion.
//! All helpers inherit the maintenance handle and release capacity per quantum.
use super::frozen_storage_lifetime::{frozen_scope_query, physical_table};
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_frozen_history as history, compaction_frozen_layout as layout,
    compaction_frozen_span as span, frozen_sweep_bootstrap as bootstrap,
    frozen_sweep_consumer as consumer, frozen_sweep_fanout as fanout, frozen_sweep_work as work,
};
use sea_orm::sea_query::{
    Alias, BinOper, Expr, ExprTrait, Func, IntoIden, IntoTableRef, JoinType, OnConflict, Order,
    Query, SelectStatement,
};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

#[derive(Clone)]
pub(super) struct Work {
    pub manifest: String,
    pub kind: i64,
    pub revision: i64,
}

/// Single group per quantum, with a persisted upper bound for each family.
/// Concurrent groups on either side of the cursor are covered by INSERT triggers.
pub(super) async fn bootstrap_step(store: &CrudStore) -> Result<bool> {
    let row = store
        .connection
        .query_one(
            &Query::select()
                .columns([
                    bootstrap::Column::Kind,
                    bootstrap::Column::Started,
                    bootstrap::Column::UpperManifest,
                    bootstrap::Column::AfterManifest,
                ])
                .from(bootstrap::Entity)
                .and_where(Expr::col(bootstrap::Column::Complete).eq(0))
                .order_by(bootstrap::Column::Kind, Order::Asc)
                .limit(1)
                .to_owned(),
        )
        .await?;
    let Some(row) = row else { return Ok(false) };
    let kind: i64 = row.try_get("", "kind")?;
    let started: i64 = row.try_get("", "started")?;
    let upper: Option<String> = row.try_get("", "upper_manifest")?;
    let after: Option<String> = row.try_get("", "after_manifest")?;
    let (table, _) = physical_table(kind);
    // Bounded indexed seeks outside the writer. Cursor CAS below permits
    // duplicate discovery after cancellation, never a skipped enqueue.
    let mut query = Query::select();
    query
        .column(Alias::new("manifest_id"))
        .from(Alias::new(table))
        .limit(1);
    if started == 0 {
        query.order_by(Alias::new("manifest_id"), Order::Desc);
    } else {
        query.and_where(Expr::col(Alias::new("manifest_id")).lte(upper.clone()));
        if let Some(after) = &after {
            query.and_where(Expr::col(Alias::new("manifest_id")).gt(after));
        }
        query.order_by(Alias::new("manifest_id"), Order::Asc);
    }
    let next = store.connection.query_one(&query).await?;
    let next: Option<String> = next.map(|r| r.try_get("", "manifest_id")).transpose()?;
    // Find a witness with the original oracle on the reader. Reverse scope
    // discovery can inspect many endpoints; the writer must only recheck the
    // selected header/link row key, not repeat that discovery under capacity.
    let scope = if let Some(id) = next.as_deref().filter(|_| started != 0) {
        let witness = store
            .connection
            .query_one(&frozen_scope_query(id))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("legacy frozen dependency scope unknown; cleanup waits for repair")
            })?;
        Some(scope_witness_guard(id, &witness)?)
    } else {
        None
    };
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            // Scope authority is the current witness, not enqueue or an old reader
            // result. Link/header validation and cursor/work commit are atomic.
            if let Some(scope) = &scope {
                ensure!(
                    tx.query_one(scope).await?.is_some(),
                    "legacy frozen dependency scope unknown; cleanup waits for repair"
                );
            }
            let changed = tx
                .execute(
                    &Query::update()
                        .table(bootstrap::Entity)
                        .value(bootstrap::Column::Started, 1)
                        .value(
                            bootstrap::Column::UpperManifest,
                            Expr::case(Expr::col(bootstrap::Column::Started).eq(0), next.clone())
                                .finally(Expr::col(bootstrap::Column::UpperManifest)),
                        )
                        .value(
                            bootstrap::Column::AfterManifest,
                            Expr::case(
                                Expr::col(bootstrap::Column::Started).eq(0),
                                Expr::val(None::<String>),
                            )
                            .finally(next.clone()),
                        )
                        .value(bootstrap::Column::Complete, i64::from(next.is_none()))
                        .and_where(Expr::col(bootstrap::Column::Kind).eq(kind))
                        .and_where(Expr::col(bootstrap::Column::Complete).eq(0))
                        .and_where(Expr::col(bootstrap::Column::Started).eq(started))
                        .and_where(
                            Expr::col(bootstrap::Column::AfterManifest)
                                .binary(BinOper::Is, Expr::val(after.clone())),
                        )
                        .to_owned(),
                )
                .await?
                .rows_affected();
            if changed == 1 && started != 0 {
                if let Some(id) = &next {
                    // Omit the integer PK/default fields; no RETURNING or readback.
                    tx.execute(
                        &Query::insert()
                            .into_table(work::Entity)
                            .columns([work::Column::ManifestId, work::Column::Kind])
                            .values_panic([id.clone().into(), kind.into()])
                            .on_conflict(
                                OnConflict::columns([work::Column::ManifestId, work::Column::Kind])
                                    .do_nothing()
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

fn scope_witness_guard(id: &str, row: &sea_orm::QueryResult) -> Result<SelectStatement> {
    let via: i64 = row.try_get("", "scope_via")?;
    if via == 0 {
        return Ok(Query::select()
            .expr(Expr::val(1))
            .from(history::Entity)
            .and_where(Expr::col(history::Column::Id).eq(id))
            .and_where(Expr::col(history::Column::WorkspaceId).ne(""))
            .to_owned());
    }
    let witness: i64 = row.try_get("", "scope_row")?;
    let (table, header, predicate) = match via {
        1 => (
            layout::Entity.into_table_ref(),
            layout::Column::ManifestId.into_iden(),
            layout::Column::Candidate.into_iden(),
        ),
        2 => (
            layout::Entity.into_table_ref(),
            layout::Column::Candidate.into_iden(),
            layout::Column::ManifestId.into_iden(),
        ),
        3 => (
            span::Entity.into_table_ref(),
            span::Column::ManifestId.into_iden(),
            span::Column::SourceManifest.into_iden(),
        ),
        4 => (
            span::Entity.into_table_ref(),
            span::Column::SourceManifest.into_iden(),
            span::Column::ManifestId.into_iden(),
        ),
        _ => anyhow::bail!("invalid frozen scope witness"),
    };
    // rowid only locates a witness; current link/header predicates authorize
    // scope. Reuse cannot forge known scope, and malformed legacy kind/start
    // values do not introduce a stricter prerequisite than the original oracle.
    Ok(Query::select()
        .expr(Expr::val(1))
        .from_as(table, Alias::new("s"))
        .join_as(
            JoinType::InnerJoin,
            history::Entity,
            Alias::new("h"),
            Expr::col((Alias::new("h"), history::Column::Id))
                .eq(Expr::col((Alias::new("s"), header))),
        )
        .and_where(Expr::col((Alias::new("s"), Alias::new("rowid"))).eq(witness))
        .and_where(Expr::col((Alias::new("s"), predicate)).eq(id))
        .and_where(Expr::col((Alias::new("h"), history::Column::WorkspaceId)).ne(""))
        .to_owned())
}

/// A consumer may reference arbitrarily many backing groups. The covering
/// index seeks past the previous source, coalescing repeated spans per source.
pub(super) async fn fanout_step(store: &CrudStore) -> Result<bool> {
    // Explicit retained INTEGER PRIMARY KEY columns alias rowid, including
    // across VACUUM. Freeze an insertion frontier for each fair service round.
    let cursor = store
        .connection
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
        .and_where(Expr::col(consumer::Column::Pending).eq(1))
        .and_where(Expr::col(consumer::Column::ConsumerRow).gt(after_row))
        .and_where(Expr::col(consumer::Column::ConsumerRow).lte(until_row))
        .order_by(consumer::Column::ConsumerRow, Order::Asc)
        .limit(1);
    let mut row = store.connection.query_one(&query).await?;
    if row.is_none() {
        until_row = store
            .connection
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
                    .to_owned(),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("consumer frontier missing"))?
            .try_get("", "until_row")?;
        row = store
            .connection
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
    let next = store.connection.query_one(&query).await?;
    let next: Option<String> = next.map(|r| r.try_get("", "source_manifest")).transpose()?;
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
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
                        .and_where(
                            Expr::col(consumer::Column::AfterSource)
                                .binary(BinOper::Is, Expr::val(after.clone())),
                        )
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
                                        Expr::col(work::Column::Revision).add(1),
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
pub(super) struct ServiceRound {
    pub(super) after_row: i64,
    pub(super) until_row: i64,
}

pub(super) enum Admission {
    Selected(Work),
    Continue,
    Idle,
}

pub(super) async fn discover(
    store: &CrudStore,
    active: &[(&str, i64)],
    kind: i64,
    round: &mut ServiceRound,
) -> Result<Admission> {
    let mut due = Query::select();
    due.expr(Expr::val(1))
        .from(work::Entity)
        .and_where(Expr::col(work::Column::Pending).eq(1))
        .and_where(Expr::col(work::Column::Kind).eq(kind))
        .and_where(
            Expr::col(work::Column::RetryAt).lte(Expr::expr(Func::cust(Alias::new("unixepoch")))),
        )
        .limit(1);
    for (id, kind) in active {
        due.and_where(
            Expr::col(work::Column::ManifestId)
                .eq(*id)
                .and(Expr::col(work::Column::Kind).eq(*kind))
                .not(),
        );
    }
    // SeaQuery's SQLite SELECT builder has no INDEXED BY API (its index hint
    // extension is MySQL-only). Inject only the hint into builder-owned SQL;
    // all projections/predicates/limits/values remain typed and bound.
    let mut due = DbBackend::Sqlite.build(&due);
    ensure!(
        due.sql.matches("FROM \"frozen_sweep_work\"").count() == 1,
        "unexpected sweep due table"
    );
    due.sql = due.sql.replacen(
        "FROM \"frozen_sweep_work\"",
        "FROM \"frozen_sweep_work\" INDEXED BY \"frozen_sweep_pending\"",
        1,
    );
    // No due non-active work means idle without walking future-retry pages.
    if store.connection.query_one_raw(due).await?.is_none() {
        return Ok(Admission::Idle);
    }
    if round.after_row >= round.until_row {
        round.until_row = store
            .connection
            .query_one(
                &Query::select()
                    .expr_as(
                        Func::coalesce([Expr::col(work::Column::WorkRow).max(), Expr::val(0)]),
                        Alias::new("until_row"),
                    )
                    .from(work::Entity)
                    .to_owned(),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("work frontier missing"))?
            .try_get("", "until_row")?;
        round.after_row = 0;
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
            Expr::col(work::Column::RetryAt).lte(Expr::expr(Func::cust(Alias::new("unixepoch")))),
            Alias::new("due"),
        )
        .from(work::Entity)
        .and_where(Expr::col(work::Column::Pending).eq(1))
        .and_where(Expr::col(work::Column::Kind).eq(kind))
        .and_where(Expr::col(work::Column::WorkRow).gt(round.after_row))
        .and_where(Expr::col(work::Column::WorkRow).lte(round.until_row))
        .order_by(work::Column::WorkRow, Order::Asc)
        .limit(16)
        .to_owned();
    let mut page = DbBackend::Sqlite.build(&page);
    ensure!(
        page.sql.matches("FROM \"frozen_sweep_work\"").count() == 1,
        "unexpected sweep service table"
    );
    page.sql = page.sql.replacen(
        "FROM \"frozen_sweep_work\"",
        "FROM \"frozen_sweep_work\" INDEXED BY \"frozen_sweep_service\"",
        1,
    );
    let rows = store.connection.query_all_raw(page).await?;
    if rows.is_empty() {
        round.after_row = round.until_row;
    }
    for row in rows {
        round.after_row = row.try_get("", "work_row")?;
        let id: String = row.try_get("", "manifest_id")?;
        if row.try_get::<i64>("", "due")? == 0 || active.contains(&(id.as_str(), kind)) {
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
    pub(super) fn fence(&self) -> Statement {
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
        DbBackend::Sqlite.build(
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
    /// Refusal persists a delayed retry; CAS never clears newer domain work.
    pub(super) async fn finish(
        &self,
        store: &CrudStore,
        validation: Option<Statement>,
    ) -> Result<()> {
        store
            .run_serialized_write(|| async {
                let tx = store.connection.begin().await?;
                let valid = if let Some(guard) = &validation {
                    tx.query_one_raw(self.fence()).await?.is_some()
                        && tx.query_one_raw(guard.clone()).await?.is_some()
                } else {
                    false
                };
                let retry = if valid {
                    Expr::val(0)
                } else {
                    Expr::expr(Func::cust(Alias::new("unixepoch"))).add(60)
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
