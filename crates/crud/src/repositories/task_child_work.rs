//! Durable child work. All deadlines are Unix seconds. Call enqueue helpers
//! inside the domain transaction; notifications belong after commit.
use crate::CrudStore;
use anyhow::{Context, Result, ensure};
use pioneer_entity::{task_child_reconcile_pending as point, task_child_reconcile_scope as scope};
use pioneer_protocol::generate_id;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    TransactionTrait,
};

const PAGE: u64 = 32;
const LEASE: i64 = 60;
fn token() -> String {
    generate_id(32)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildScope {
    Task(String),
    Run(String),
    Group {
        task_id: String,
        run_group_id: String,
    },
    ChildThread(String),
    CreatorTurn {
        thread_id: String,
        turn_id: String,
    },
    CreatorThread(String),
    AcceptedSource(String),
    ParentThread(String),
    #[cfg(test)]
    HistoricalSeed,
}
impl ChildScope {
    fn locator(&self) -> (&'static str, String) {
        match self {
            Self::Task(k) => ("task", k.clone()),
            Self::Run(k) => ("run", k.clone()),
            Self::Group {
                task_id,
                run_group_id,
            } => (
                "task+run-group",
                serde_json::to_string(&(task_id, run_group_id)).expect("string pair"),
            ),
            Self::ChildThread(k) => ("child-thread", k.clone()),
            Self::CreatorTurn { thread_id, turn_id } => (
                "creator-turn",
                serde_json::to_string(&(thread_id, turn_id)).expect("string pair"),
            ),
            Self::CreatorThread(k) => ("creator-thread", k.clone()),
            Self::AcceptedSource(k) => ("accepted-source", k.clone()),
            Self::ParentThread(k) => ("parent-thread", k.clone()),
            #[cfg(test)]
            Self::HistoricalSeed => ("historical-seed", "v1".into()),
        }
    }
}

pub async fn enqueue_point<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    now: i64,
) -> Result<()> {
    let revision = token();
    if let Some(old) = point::Entity::find_by_id(turn_id).one(db).await? {
        let changed = point::Entity::update_many()
            .col_expr(point::Column::ThreadId, Expr::value(thread_id))
            .col_expr(point::Column::Revision, Expr::value(revision))
            .col_expr(point::Column::NextAttemptAt, Expr::value(now))
            .col_expr(point::Column::AttemptCount, Expr::value(0i64))
            .col_expr(
                point::Column::ClaimToken,
                Expr::value(Option::<String>::None),
            )
            .filter(point::Column::TurnId.eq(turn_id))
            .filter(point::Column::Revision.eq(old.revision))
            .exec(db)
            .await?;
        if changed.rows_affected != 1 {
            return Err(super::portable_payload::PreflightRace.into());
        }
    } else {
        point::Entity::insert(point::ActiveModel {
            turn_id: Set(turn_id.into()),
            thread_id: Set(thread_id.into()),
            revision: Set(revision),
            next_attempt_at: Set(Some(now)),
            attempt_count: Set(0),
            claim_token: Set(None),
        })
        .exec_without_returning(db)
        .await
        .map_err(insert_race)?;
    }
    Ok(())
}

// Discovery is not a new domain revision. Repeated compatibility/seed pages
// must not steal a live claim or wake an unchanged parked dependency. A legacy
// selector has already proved eligibility, so it may revive a parked ticket
// after an unrecognized old writer supplied its missing dependency.
async fn ensure_discovered_point<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    now: i64,
    revive_parked: bool,
) -> Result<()> {
    let current = point::Entity::find_by_id(turn_id).one(db).await?;
    if current.as_ref().is_none_or(|row| {
        row.thread_id != thread_id || (revive_parked && row.next_attempt_at.is_none())
    }) {
        enqueue_point(db, thread_id, turn_id, now).await?;
    }
    Ok(())
}

pub async fn enqueue_scope<C: ConnectionTrait>(db: &C, input: ChildScope, now: i64) -> Result<()> {
    let (kind, key) = input.locator();
    let revision = token();
    if let Some(old) = scope::Entity::find_by_id((kind.to_owned(), key.clone()))
        .one(db)
        .await?
    {
        // New invalidations preserve an active pass, cursor and lease. EOF sees
        // the new request and starts another pass, including behind-cursor rows.
        let mut update = scope::Entity::update_many()
            .col_expr(scope::Column::RequestedRevision, Expr::value(revision))
            .filter(scope_fence(&old))
            .filter(scope::Column::RequestedRevision.eq(&old.requested_revision));
        if old.claim_token.is_none() {
            // A newer request may wake deferred work, never postpone a due
            // pass while a continuous producer keeps moving wall time ahead.
            let due = old.next_attempt_at.map_or(now, |due| due.min(now));
            update = update.col_expr(scope::Column::NextAttemptAt, Expr::value(due));
        }
        if update.exec(db).await?.rows_affected != 1 {
            return Err(super::portable_payload::PreflightRace.into());
        }
    } else {
        scope::Entity::insert(scope::ActiveModel {
            scope_kind: Set(kind.into()),
            scope_key: Set(key),
            incarnation_token: Set(token()),
            requested_revision: Set(revision),
            pass_revision: Set(None),
            cursor: Set(None),
            upper_watermark: Set(None),
            next_attempt_at: Set(Some(now)),
            attempt_count: Set(0),
            claim_token: Set(None),
        })
        .exec_without_returning(db)
        .await
        .map_err(insert_race)?;
    }
    Ok(())
}

fn point_fence(row: &point::Model) -> sea_orm::Condition {
    let mut c = sea_orm::Condition::all()
        .add(point::Column::TurnId.eq(&row.turn_id))
        .add(point::Column::Revision.eq(&row.revision));
    c = if let Some(t) = &row.claim_token {
        c.add(point::Column::ClaimToken.eq(t))
    } else {
        c.add(point::Column::ClaimToken.is_null())
    };
    if let Some(due) = row.next_attempt_at {
        c.add(point::Column::NextAttemptAt.eq(due))
    } else {
        c.add(point::Column::NextAttemptAt.is_null())
    }
}
fn scope_fence(row: &scope::Model) -> sea_orm::Condition {
    let mut c = sea_orm::Condition::all()
        .add(scope::Column::ScopeKind.eq(&row.scope_kind))
        .add(scope::Column::ScopeKey.eq(&row.scope_key))
        .add(scope::Column::IncarnationToken.eq(&row.incarnation_token));
    c = if let Some(t) = &row.claim_token {
        c.add(scope::Column::ClaimToken.eq(t))
    } else {
        c.add(scope::Column::ClaimToken.is_null())
    };
    if let Some(due) = row.next_attempt_at {
        c.add(scope::Column::NextAttemptAt.eq(due))
    } else {
        c.add(scope::Column::NextAttemptAt.is_null())
    }
}

fn scope_pass_fence(row: &scope::Model) -> sea_orm::Condition {
    let mut fence = scope_fence(row);
    for (column, value) in [
        (scope::Column::PassRevision, &row.pass_revision),
        (scope::Column::Cursor, &row.cursor),
        (scope::Column::UpperWatermark, &row.upper_watermark),
    ] {
        fence = if let Some(value) = value {
            fence.add(column.eq(value))
        } else {
            fence.add(column.is_null())
        };
    }
    fence
}

impl CrudStore {
    pub async fn discover_child_points(&self, now: i64, limit: u64) -> Result<Vec<point::Model>> {
        self.discover_child_points_after(now, limit, None).await
    }
    /// Rotate through the indexed due set even when a poison head cannot be
    /// deferred. The caller retains this advisory cursor only, never ownership.
    pub async fn discover_child_points_after(
        &self,
        now: i64,
        limit: u64,
        after: Option<(i64, &str)>,
    ) -> Result<Vec<point::Model>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let query = point::Entity::find()
            .filter(point::Column::NextAttemptAt.lte(now))
            .order_by_asc(point::Column::NextAttemptAt)
            .order_by_asc(point::Column::TurnId)
            .limit(limit.min(PAGE));
        if let Some((due, id)) = after {
            let rows = query
                .clone()
                .filter(
                    sea_orm::Condition::any()
                        .add(point::Column::NextAttemptAt.gt(due))
                        .add(
                            sea_orm::Condition::all()
                                .add(point::Column::NextAttemptAt.eq(due))
                                .add(point::Column::TurnId.gt(id)),
                        ),
                )
                .all(&self.connection)
                .await?;
            if !rows.is_empty() {
                return Ok(rows);
            }
        }
        Ok(query.all(&self.connection).await?)
    }
    /// Finite advisory pass boundary. No ownership or writer admission.
    pub async fn child_point_due_upper(&self, now: i64) -> Result<Option<(i64, String)>> {
        Ok(point::Entity::find()
            .select_only()
            .columns([point::Column::NextAttemptAt, point::Column::TurnId])
            .filter(point::Column::NextAttemptAt.lte(now))
            .order_by_desc(point::Column::NextAttemptAt)
            .order_by_desc(point::Column::TurnId)
            .into_tuple::<(i64, String)>()
            .one(&self.connection)
            .await?)
    }
    /// An empty suffix ends this pass; it never silently wraps into new growth.
    pub async fn discover_child_points_in_pass(
        &self,
        now: i64,
        limit: u64,
        after: Option<&(i64, String)>,
        upper: &(i64, String),
    ) -> Result<Vec<point::Model>> {
        let mut query = point::Entity::find()
            .filter(point::Column::NextAttemptAt.lte(now))
            .filter(
                sea_orm::Condition::any()
                    .add(point::Column::NextAttemptAt.lt(upper.0))
                    .add(
                        sea_orm::Condition::all()
                            .add(point::Column::NextAttemptAt.eq(upper.0))
                            .add(point::Column::TurnId.lte(&upper.1)),
                    ),
            );
        if let Some((due, id)) = after {
            query = query.filter(
                sea_orm::Condition::any()
                    .add(point::Column::NextAttemptAt.gt(*due))
                    .add(
                        sea_orm::Condition::all()
                            .add(point::Column::NextAttemptAt.eq(*due))
                            .add(point::Column::TurnId.gt(id)),
                    ),
            );
        }
        Ok(query
            .order_by_asc(point::Column::NextAttemptAt)
            .order_by_asc(point::Column::TurnId)
            .limit(limit.min(PAGE))
            .all(&self.connection)
            .await?)
    }
    /// Exact-snapshot independent deferral after claim/ACK failure. An unknown
    /// committed claim has a different token/deadline and cannot be cleared by
    /// its old preclaim snapshot. No row is inserted/recreated by this helper.
    pub async fn defer_child_point_snapshot<F: Fn() -> i64 + Sync>(
        &self,
        snapshot: &point::Model,
        clock: &F,
    ) -> Result<bool> {
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let due = clock().saturating_add(2);
            let changed = point::Entity::update_many()
                .filter(point_fence(snapshot))
                .col_expr(point::Column::NextAttemptAt, Expr::value(due))
                .col_expr(
                    point::Column::ClaimToken,
                    Expr::value(Option::<String>::None),
                )
                .exec(&tx)
                .await?
                .rows_affected;
            tx.commit().await?;
            Ok(changed == 1)
        })
        .await
    }
    pub async fn claim_child_point(
        &self,
        candidate: &point::Model,
        now: i64,
    ) -> Result<Option<point::Model>> {
        self.claim_child_point_with_clock(candidate, &|| now).await
    }
    pub async fn claim_child_point_with_clock<F: Fn() -> i64 + Sync>(
        &self,
        candidate: &point::Model,
        clock: &F,
    ) -> Result<Option<point::Model>> {
        if candidate.next_attempt_at.is_none_or(|due| due > clock()) {
            return Ok(None);
        }
        self.run_serialized_write(|| async {
            let claim = token();
            let tx = self.connection.begin().await?;
            let reserved_at = clock();
            let until = reserved_at.saturating_add(LEASE);
            let affected = point::Entity::update_many()
                .filter(point_fence(candidate))
                .filter(point::Column::NextAttemptAt.lte(reserved_at))
                .col_expr(point::Column::ClaimToken, Expr::value(&claim))
                .col_expr(point::Column::NextAttemptAt, Expr::value(until))
                .col_expr(
                    point::Column::AttemptCount,
                    Expr::value(candidate.attempt_count.saturating_add(1)),
                )
                .exec(&tx)
                .await?
                .rows_affected;
            tx.commit().await?;
            Ok((affected == 1).then(|| {
                let mut row = candidate.clone();
                row.claim_token = Some(claim);
                row.next_attempt_at = Some(until);
                row.attempt_count = row.attempt_count.saturating_add(1);
                row
            }))
        })
        .await
    }
    /// None parks a proven durable dependency. Runtime-local waits use a timed
    /// retry. An ACK from an older producer revision cannot remove new work.
    pub async fn finish_child_point(
        &self,
        claim: &point::Model,
        retry_at: Option<i64>,
        done: bool,
    ) -> Result<bool> {
        ensure!(claim.claim_token.is_some(), "unclaimed child ACK");
        self.run_serialized_write(|| async {
            let rows = if done {
                point::Entity::delete_many()
                    .filter(point_fence(claim))
                    .exec(&self.connection)
                    .await?
                    .rows_affected
            } else {
                point::Entity::update_many()
                    .filter(point_fence(claim))
                    .col_expr(
                        point::Column::ClaimToken,
                        Expr::value(Option::<String>::None),
                    )
                    .col_expr(point::Column::NextAttemptAt, Expr::value(retry_at))
                    .exec(&self.connection)
                    .await?
                    .rows_affected
            };
            Ok(rows == 1)
        })
        .await
    }
}

#[derive(Debug)]
enum Output {
    Scope(ChildScope),
    Point(String, String),
}
struct Item {
    cursor: String,
    output: Output,
}

// Each scope has its own indexed keyset. No global history JOIN is used by
// steady-state discovery. Upper is captured in the same short transaction as
// pass_revision. A fresh pass catches insertions behind an existing cursor.
async fn page<C: ConnectionTrait>(
    db: &C,
    row: &scope::Model,
    upper_only: bool,
) -> Result<Vec<Item>> {
    use pioneer_entity::{
        task, task_run, task_run_conversation_snapshot as accepted, task_run_turn as link, turn,
    };
    use sea_orm::{Condition, Order};
    let order = if upper_only { Order::Desc } else { Order::Asc };
    let limit = if upper_only { 1 } else { PAGE };
    let lower = if upper_only {
        None
    } else {
        row.cursor.as_deref()
    };
    let upper = if upper_only {
        None
    } else {
        row.upper_watermark.as_deref()
    };
    macro_rules! keyset {
        ($q:expr, $col:expr) => {{
            let mut q = $q;
            if let Some(k) = lower {
                q = q.filter($col.gt(k));
            }
            if let Some(k) = upper {
                q = q.filter($col.lte(k));
            }
            q.order_by($col, order.clone()).limit(limit)
        }};
    }
    let mut out = Vec::new();
    match row.scope_kind.as_str() {
        "task" => {
            let q = task_run::Entity::find().filter(task_run::Column::TaskId.eq(&row.scope_key));
            for id in keyset!(q, task_run::Column::Id)
                .select_only()
                .column(task_run::Column::Id)
                .into_tuple::<String>()
                .all(db)
                .await?
            {
                out.push(Item {
                    cursor: id.clone(),
                    output: Output::Scope(ChildScope::Run(id)),
                });
            }
        }
        "task+run-group" => {
            let (task_id, group): (String, String) = serde_json::from_str(&row.scope_key)?;
            let mut q = task_run::Entity::find()
                .filter(task_run::Column::TaskId.eq(task_id))
                .filter(task_run::Column::RunGroupId.eq(group));
            if let Some(k) = lower {
                q = q.filter(task_run::Column::AttemptNumber.gt(k.parse::<i64>()?));
            }
            if let Some(k) = upper {
                q = q.filter(task_run::Column::AttemptNumber.lte(k.parse::<i64>()?));
            }
            for (id, n) in q
                .select_only()
                .columns([task_run::Column::Id, task_run::Column::AttemptNumber])
                .order_by(task_run::Column::AttemptNumber, order)
                .limit(limit)
                .into_tuple::<(String, i64)>()
                .all(db)
                .await?
            {
                out.push(Item {
                    cursor: n.to_string(),
                    output: Output::Scope(ChildScope::Run(id)),
                });
            }
        }
        "run" => {
            let mut q = link::Entity::find().filter(link::Column::RunId.eq(&row.scope_key));
            if let Some(k) = lower {
                q = q.filter(link::Column::Sequence.gt(k.parse::<i64>()?));
            }
            if let Some(k) = upper {
                q = q.filter(link::Column::Sequence.lte(k.parse::<i64>()?));
            }
            for (thread, id, n) in q
                .select_only()
                .columns([
                    link::Column::ThreadId,
                    link::Column::TurnId,
                    link::Column::Sequence,
                ])
                .order_by(link::Column::Sequence, order)
                .limit(limit)
                .into_tuple::<(String, String, i64)>()
                .all(db)
                .await?
            {
                out.push(Item {
                    cursor: n.to_string(),
                    output: Output::Point(thread, id),
                });
            }
        }
        "child-thread" => {
            let q = link::Entity::find().filter(link::Column::ThreadId.eq(&row.scope_key));
            for (id, thread, turn) in keyset!(q, link::Column::Id)
                .select_only()
                .columns([
                    link::Column::Id,
                    link::Column::ThreadId,
                    link::Column::TurnId,
                ])
                .into_tuple::<(String, String, String)>()
                .all(db)
                .await?
            {
                out.push(Item {
                    cursor: id,
                    output: Output::Point(thread, turn),
                });
            }
        }
        "creator-turn" => {
            let (thread, id): (String, String) = serde_json::from_str(&row.scope_key)?;
            let q = task::Entity::find()
                .filter(task::Column::CreatedByThreadId.eq(thread))
                .filter(task::Column::CreatedByTurnId.eq(id));
            for id in keyset!(q, task::Column::Id)
                .select_only()
                .column(task::Column::Id)
                .into_tuple::<String>()
                .all(db)
                .await?
            {
                out.push(Item {
                    cursor: id.clone(),
                    output: Output::Scope(ChildScope::Task(id)),
                });
            }
        }
        "creator-thread" => {
            // NULL creators cannot provide positive launch authority.
            let mut q = task::Entity::find()
                .filter(task::Column::CreatedByThreadId.eq(&row.scope_key))
                .filter(task::Column::CreatedByTurnId.is_not_null());
            if let Some(k) = lower {
                let (creator, id): (String, String) = serde_json::from_str(k)?;
                q = q.filter(
                    Condition::any()
                        .add(task::Column::CreatedByTurnId.gt(&creator))
                        .add(
                            Condition::all()
                                .add(task::Column::CreatedByTurnId.eq(creator))
                                .add(task::Column::Id.gt(id)),
                        ),
                );
            }
            if let Some(k) = upper {
                let (creator, id): (String, String) = serde_json::from_str(k)?;
                q = q.filter(
                    Condition::any()
                        .add(task::Column::CreatedByTurnId.lt(&creator))
                        .add(
                            Condition::all()
                                .add(task::Column::CreatedByTurnId.eq(creator))
                                .add(task::Column::Id.lte(id)),
                        ),
                );
            }
            for (creator, id) in q
                .select_only()
                .columns([task::Column::CreatedByTurnId, task::Column::Id])
                .order_by(task::Column::CreatedByTurnId, order.clone())
                .order_by(task::Column::Id, order)
                .limit(limit)
                .into_tuple::<(String, String)>()
                .all(db)
                .await?
            {
                out.push(Item {
                    cursor: serde_json::to_string(&(creator, &id))?,
                    output: Output::Scope(ChildScope::Task(id)),
                });
            }
        }
        "accepted-source" => {
            let q =
                accepted::Entity::find().filter(accepted::Column::SourceTurnId.eq(&row.scope_key));
            for id in keyset!(q, accepted::Column::RunId)
                .select_only()
                .column(accepted::Column::RunId)
                .into_tuple::<String>()
                .all(db)
                .await?
            {
                out.push(Item {
                    cursor: id.clone(),
                    output: Output::Scope(ChildScope::Run(id)),
                });
            }
        }
        "parent-thread" => {
            let q = turn::Entity::find()
                .filter(turn::Column::ThreadId.eq(&row.scope_key))
                .filter(turn::Column::TurnKind.eq("task_run"));
            for id in keyset!(q, turn::Column::Id)
                .select_only()
                .column(turn::Column::Id)
                .into_tuple::<String>()
                .all(db)
                .await?
            {
                out.push(Item {
                    cursor: id.clone(),
                    output: Output::Scope(ChildScope::Run(id)),
                });
            }
        }
        "historical-seed" => {
            // Walk the existing link PK, not an IN-subquery that may materialize
            // the entire child history once per page. Point processing performs
            // current terminal/ownership selection through exact indexed reads.
            let q = link::Entity::find();
            for (id, thread, turn) in keyset!(q, link::Column::Id)
                .select_only()
                .columns([
                    link::Column::Id,
                    link::Column::ThreadId,
                    link::Column::TurnId,
                ])
                .into_tuple::<(String, String, String)>()
                .all(db)
                .await?
            {
                out.push(Item {
                    cursor: id,
                    output: Output::Point(thread, turn),
                });
            }
        }
        _ => anyhow::bail!("unknown child scope kind"),
    }
    Ok(out)
}

impl CrudStore {
    /// Advisory read first: an idle poll never reserves the writer. Seed is
    /// owned by Gateway startup migration lifecycle, not the live worker.
    pub async fn expand_child_scope(&self, now: i64, seed: bool) -> Result<bool> {
        self.expand_child_scope_with_clock(&|| now, seed).await
    }
    pub async fn expand_child_scope_with_clock<F: Fn() -> i64 + Sync>(
        &self,
        clock: &F,
        seed: bool,
    ) -> Result<bool> {
        let Some(candidate) = self.discover_child_scope_after(clock(), seed, None).await? else {
            return Ok(false);
        };
        self.expand_child_scope_candidate_with_clock(&candidate, clock, seed)
            .await
    }
    /// Advisory indexed discovery only. Seed has a separate durable owner and
    /// never participates in live rotation.
    pub async fn discover_child_scope_after(
        &self,
        now: i64,
        seed: bool,
        after: Option<&(i64, String, String)>,
    ) -> Result<Option<scope::Model>> {
        let base = scope::Entity::find()
            .filter(scope::Column::NextAttemptAt.lte(now))
            .filter(if seed {
                scope::Column::ScopeKind.eq("historical-seed")
            } else {
                scope::Column::ScopeKind.ne("historical-seed")
            })
            .order_by_asc(scope::Column::NextAttemptAt)
            .order_by_asc(scope::Column::ScopeKind)
            .order_by_asc(scope::Column::ScopeKey);
        let mut query = base.clone();
        if !seed {
            if let Some((due, kind, key)) = after {
                query = query.filter(
                    sea_orm::Condition::any()
                        .add(scope::Column::NextAttemptAt.gt(*due))
                        .add(
                            sea_orm::Condition::all()
                                .add(scope::Column::NextAttemptAt.eq(*due))
                                .add(scope::Column::ScopeKind.gt(kind.clone())),
                        )
                        .add(
                            sea_orm::Condition::all()
                                .add(scope::Column::NextAttemptAt.eq(*due))
                                .add(scope::Column::ScopeKind.eq(kind.clone()))
                                .add(scope::Column::ScopeKey.gt(key.clone())),
                        ),
                );
            }
        }
        let mut candidate = query.one(&self.connection).await?;
        if candidate.is_none() && !seed && after.is_some() {
            candidate = base.one(&self.connection).await?;
        }
        Ok(candidate)
    }
    pub async fn child_scope_due_upper(&self, now: i64) -> Result<Option<(i64, String, String)>> {
        Ok(scope::Entity::find()
            .select_only()
            .columns([
                scope::Column::NextAttemptAt,
                scope::Column::ScopeKind,
                scope::Column::ScopeKey,
            ])
            .filter(scope::Column::NextAttemptAt.lte(now))
            .filter(scope::Column::ScopeKind.ne("historical-seed"))
            .order_by_desc(scope::Column::NextAttemptAt)
            .order_by_desc(scope::Column::ScopeKind)
            .order_by_desc(scope::Column::ScopeKey)
            .into_tuple::<(i64, String, String)>()
            .one(&self.connection)
            .await?)
    }
    pub async fn discover_child_scope_in_pass(
        &self,
        now: i64,
        after: Option<&(i64, String, String)>,
        upper: &(i64, String, String),
    ) -> Result<Option<scope::Model>> {
        let mut query = scope::Entity::find()
            .filter(scope::Column::NextAttemptAt.lte(now))
            .filter(scope::Column::ScopeKind.ne("historical-seed"))
            .filter(
                sea_orm::Condition::any()
                    .add(scope::Column::NextAttemptAt.lt(upper.0))
                    .add(
                        sea_orm::Condition::all()
                            .add(scope::Column::NextAttemptAt.eq(upper.0))
                            .add(scope::Column::ScopeKind.lt(&upper.1)),
                    )
                    .add(
                        sea_orm::Condition::all()
                            .add(scope::Column::NextAttemptAt.eq(upper.0))
                            .add(scope::Column::ScopeKind.eq(&upper.1))
                            .add(scope::Column::ScopeKey.lte(&upper.2)),
                    ),
            );
        if let Some((due, kind, key)) = after {
            query = query.filter(
                sea_orm::Condition::any()
                    .add(scope::Column::NextAttemptAt.gt(*due))
                    .add(
                        sea_orm::Condition::all()
                            .add(scope::Column::NextAttemptAt.eq(*due))
                            .add(scope::Column::ScopeKind.gt(kind)),
                    )
                    .add(
                        sea_orm::Condition::all()
                            .add(scope::Column::NextAttemptAt.eq(*due))
                            .add(scope::Column::ScopeKind.eq(kind))
                            .add(scope::Column::ScopeKey.gt(key)),
                    ),
            );
        }
        Ok(query
            .order_by_asc(scope::Column::NextAttemptAt)
            .order_by_asc(scope::Column::ScopeKind)
            .order_by_asc(scope::Column::ScopeKey)
            .one(&self.connection)
            .await?)
    }
    pub async fn expand_child_scope_candidate_with_clock<F: Fn() -> i64 + Sync>(
        &self,
        candidate: &scope::Model,
        clock: &F,
        seed: bool,
    ) -> Result<bool> {
        let outcome = self
            .run_serialized_write(|| async {
                let claim = token();
                let tx = self.connection.begin().await?;
                let now = clock();
                if scope::Entity::update_many()
                    .filter(scope_fence(&candidate))
                    .filter(scope::Column::NextAttemptAt.lte(now))
                    .col_expr(
                        scope::Column::AttemptCount,
                        Expr::value(candidate.attempt_count.saturating_add(1)),
                    )
                    .col_expr(scope::Column::ClaimToken, Expr::value(&claim))
                    .col_expr(
                        scope::Column::NextAttemptAt,
                        Expr::value(now.saturating_add(LEASE)),
                    )
                    .exec(&tx)
                    .await?
                    .rows_affected
                    != 1
                {
                    tx.rollback().await?;
                    return Ok(true);
                }
                let mut current = scope::Entity::find_by_id((
                    candidate.scope_kind.clone(),
                    candidate.scope_key.clone(),
                ))
                .one(&tx)
                .await?
                .expect("claimed scope exists");
                let claimed = current.clone();
                if current.pass_revision.is_none() {
                    current.pass_revision = Some(current.requested_revision.clone());
                    current.cursor = None;
                    current.upper_watermark = page(&tx, &current, true)
                        .await?
                        .first()
                        .map(|item| item.cursor.clone());
                }
                let items = if current.upper_watermark.is_some() {
                    page(&tx, &current, false).await?
                } else {
                    Vec::new()
                };
                let last = items.last().map(|item| item.cursor.clone());
                let eof = items.len() < PAGE as usize;
                for item in items {
                    match item.output {
                        Output::Scope(input) => enqueue_scope(&tx, input, now).await?,
                        Output::Point(thread, id) => {
                            if seed {
                                ensure_discovered_point(&tx, &thread, &id, now, false).await?;
                            } else {
                                enqueue_point(&tx, &thread, &id, now).await?;
                            }
                        }
                    }
                }
                // Lock-free CAS still fences ABA and prior holders. Producer writes
                // serialize on this row; a changed request is never acknowledged.
                let mut update = scope::Entity::update_many().filter(scope_pass_fence(&claimed));
                if eof && current.pass_revision.as_ref() == Some(&current.requested_revision) {
                    if seed {
                        update = update.col_expr(
                            scope::Column::NextAttemptAt,
                            Expr::value(Option::<i64>::None),
                        );
                    } else {
                        if !(scope::Entity::delete_many()
                            .filter(scope_pass_fence(&claimed))
                            .filter(
                                scope::Column::RequestedRevision.eq(&current.requested_revision),
                            )
                            .exec(&tx)
                            .await?
                            .rows_affected
                            == 1)
                        {
                            return Err(super::portable_payload::PreflightRace.into());
                        }
                        tx.commit().await?;
                        return Ok(true);
                    }
                } else {
                    update = update.col_expr(scope::Column::NextAttemptAt, Expr::value(now));
                }
                if eof {
                    update = update
                        .col_expr(
                            scope::Column::PassRevision,
                            Expr::value(Option::<String>::None),
                        )
                        .col_expr(scope::Column::Cursor, Expr::value(Option::<String>::None))
                        .col_expr(
                            scope::Column::UpperWatermark,
                            Expr::value(Option::<String>::None),
                        );
                } else {
                    update = update
                        .col_expr(
                            scope::Column::PassRevision,
                            Expr::value(current.pass_revision.clone()),
                        )
                        .col_expr(scope::Column::Cursor, Expr::value(last))
                        .col_expr(
                            scope::Column::UpperWatermark,
                            Expr::value(current.upper_watermark.clone()),
                        );
                }
                if !(update
                    .filter(scope::Column::RequestedRevision.eq(&current.requested_revision))
                    .col_expr(
                        scope::Column::ClaimToken,
                        Expr::value(Option::<String>::None),
                    )
                    .exec(&tx)
                    .await?
                    .rows_affected
                    == 1)
                {
                    return Err(super::portable_payload::PreflightRace.into());
                }
                tx.commit().await?;
                Ok(true)
            })
            .await;
        if outcome.is_err() {
            // A poisoned locator must not monopolize the sorted due head.
            // The prior transaction has dropped; unknown commits are fenced
            // against the advisory snapshot before this independent deferral.
            let deferred = self
                .run_serialized_write(|| async {
                    scope::Entity::update_many()
                        .filter(scope_fence(&candidate))
                        .filter(scope::Column::RequestedRevision.eq(&candidate.requested_revision))
                        .col_expr(
                            scope::Column::NextAttemptAt,
                            Expr::value(clock().saturating_add(2)),
                        )
                        .col_expr(
                            scope::Column::ClaimToken,
                            Expr::value(Option::<String>::None),
                        )
                        .exec(&self.connection)
                        .await?;
                    Ok(())
                })
                .await;
            if let Err(deferral) = deferred {
                return Err(outcome.unwrap_err().context(format!(
                    "child scope independent deferral also failed: {deferral:#}"
                )));
            }
        }
        outcome
    }
}

impl CrudStore {
    pub async fn child_point_record(
        &self,
        claim: &point::Model,
    ) -> Result<Option<crate::UnreconciledTerminalTaskChildTurnRecord>> {
        use pioneer_entity::{task, task_run, task_run_turn, turn};
        let canonical = turn::Entity::find_by_id(&claim.turn_id)
            .select_only()
            .column(turn::Column::ThreadId)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?;
        if canonical.as_ref().is_some_and(|id| id != &claim.thread_id) {
            // This is consumer bookkeeping, not a new domain invalidation.
            // Only the exact original claim may relinquish its lease and
            // schedule a canonical retry. A newer revision/owner is untouched.
            self.run_serialized_write(|| async {
                let tx = self.connection.begin().await?;
                // Domain before queue follows cleanup/producer lock order.
                let current =
                    super::task_child_producers::lock_capsule_turn(&tx, &claim.turn_id).await?;
                let owned = point::Entity::find()
                    .filter(point_fence(claim))
                    .filter(point::Column::ThreadId.eq(&claim.thread_id))
                    .lock_exclusive()
                    .one(&tx)
                    .await?;
                if owned.is_none() {
                    tx.rollback().await?;
                    return Ok(());
                }
                if let Some(thread_id) = current {
                    let changed = point::Entity::update_many()
                        .col_expr(point::Column::ThreadId, Expr::value(thread_id))
                        .col_expr(point::Column::Revision, Expr::value(token()))
                        .col_expr(
                            point::Column::NextAttemptAt,
                            Expr::value(chrono::Utc::now().timestamp()),
                        )
                        .col_expr(point::Column::AttemptCount, Expr::value(0i64))
                        .col_expr(
                            point::Column::ClaimToken,
                            Expr::value(Option::<String>::None),
                        )
                        .filter(point_fence(claim))
                        .filter(point::Column::ThreadId.eq(&claim.thread_id))
                        .exec(&tx)
                        .await?
                        .rows_affected;
                    if changed != 1 {
                        return Err(super::portable_payload::PreflightRace.into());
                    }
                }
                tx.commit().await?;
                Ok(())
            })
            .await?;
            return Ok(None);
        }
        let Some((turn_status, turn_error)) = turn::Entity::find_by_id(&claim.turn_id)
            .filter(turn::Column::ThreadId.eq(&claim.thread_id))
            .filter(turn::Column::TurnKind.eq("conversation"))
            .filter(turn::Column::Status.is_in(["completed", "failed", "interrupted", "blocked"]))
            .select_only()
            .columns([turn::Column::Status, turn::Column::Error])
            .into_tuple::<(String, Option<String>)>()
            .one(&self.connection)
            .await?
        else {
            return Ok(None);
        };
        let Some((task_id, run_id, link_status)) = task_run_turn::Entity::find()
            .filter(task_run_turn::Column::TurnId.eq(&claim.turn_id))
            .filter(task_run_turn::Column::ThreadId.eq(&claim.thread_id))
            .select_only()
            .columns([
                task_run_turn::Column::TaskId,
                task_run_turn::Column::RunId,
                task_run_turn::Column::Status,
            ])
            .into_tuple::<(String, String, String)>()
            .one(&self.connection)
            .await?
        else {
            return Ok(None);
        };
        let Some(run_status) = task_run::Entity::find_by_id(run_id)
            .filter(task_run::Column::TaskId.eq(&task_id))
            .select_only()
            .column(task_run::Column::Status)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?
        else {
            return Ok(None);
        };
        let Some(owner_status) = task::Entity::find_by_id(task_id)
            .select_only()
            .column(task::Column::Status)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?
        else {
            return Ok(None);
        };
        let terminal = matches!(
            run_status.as_str(),
            "succeeded" | "failed" | "cancelled" | "timed_out"
        );
        if terminal {
            if turn_status == "blocked" {
                return Ok(None);
            }
            use pioneer_entity::{turn_llm_context as context, turn_runtime_snapshot as snapshot};
            let snapshot_present = snapshot::Entity::find_by_id(&claim.turn_id)
                .select_only()
                .column(snapshot::Column::TurnId)
                .into_tuple::<String>()
                .one(&self.connection)
                .await?
                .is_some();
            let context_present = if snapshot_present {
                false
            } else {
                context::Entity::find()
                    .select_only()
                    .column(context::Column::Id)
                    .filter(context::Column::TurnId.eq(&claim.turn_id))
                    .filter(
                        context::Column::Source
                            .is_not_in(super::compaction::MODEL_HISTORY_CONTEXT_SOURCES),
                    )
                    .into_tuple::<String>()
                    .one(&self.connection)
                    .await?
                    .is_some()
            };
            if !snapshot_present && !context_present {
                return Ok(None);
            }
        }
        let active = matches!(
            run_status.as_str(),
            "queued" | "starting" | "running" | "waiting" | "waiting_review"
        ) && matches!(
            owner_status.as_str(),
            "draft" | "scheduled" | "queued" | "running" | "waiting" | "waiting_review"
        ) && (link_status == "in_progress"
            || (link_status == "candidate_created" && run_status != "waiting_review")
            || (link_status == "cancelled" && turn_status == "interrupted"));
        if !terminal && !active {
            return Ok(None);
        }
        Ok(Some(crate::UnreconciledTerminalTaskChildTurnRecord {
            thread_id: claim.thread_id.clone(),
            turn_id: claim.turn_id.clone(),
            status: crate::convention::turn_status_from_db(&turn_status)
                .context("unknown child turn status")?,
            error: turn_error,
        }))
    }
}

impl CrudStore {
    /// Queue fence plus current domain proof, capsule deletion and ACK in one
    /// writer transaction. A queue token alone never authorizes deletion.
    pub async fn cleanup_claimed_child_capsule(&self, claim: &point::Model) -> Result<bool> {
        use pioneer_entity::{task, task_run, task_run_turn as link, thread, turn};
        self.run_serialized_write(|| async {
            let tx = super::task_child_cleanup::begin_authoritative(&self.connection).await?;
            super::task_child_cleanup::lock_authority(&tx, &claim.thread_id, &claim.turn_id)
                .await?;
            if point::Entity::find()
                .filter(point_fence(claim))
                .one(&tx)
                .await?
                .is_none()
            {
                tx.rollback().await?;
                return Ok(false);
            }
            let cleanup_ready =
                super::task_child_cleanup::ready(&tx, &claim.thread_id, &claim.turn_id).await?;
            match cleanup_ready {
                Some(false) => {
                    tx.rollback().await?;
                    return Ok(false);
                }
                Some(true) => {}
                None => {
                    let Some(child) = link::Entity::find()
                        .filter(link::Column::TurnId.eq(&claim.turn_id))
                        .filter(link::Column::ThreadId.eq(&claim.thread_id))
                        .one(&tx)
                        .await?
                    else {
                        tx.rollback().await?;
                        return Ok(false);
                    };
                    if matches!(child.status.as_str(), "in_progress" | "blocked") {
                        tx.rollback().await?;
                        return Ok(false);
                    }
                    let Some(_run) = task_run::Entity::find_by_id(&child.run_id)
                        .filter(task_run::Column::TaskId.eq(&child.task_id))
                        .filter(task_run::Column::Status.is_in([
                            "queued",
                            "starting",
                            "running",
                            "waiting",
                            "waiting_review",
                        ]))
                        .select_only()
                        .column(task_run::Column::Id)
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                    else {
                        tx.rollback().await?;
                        return Ok(false);
                    };
                    let Some(workspace) = task::Entity::find_by_id(&child.task_id)
                        .filter(task::Column::Status.is_in([
                            "draft",
                            "scheduled",
                            "queued",
                            "running",
                            "waiting",
                            "waiting_review",
                        ]))
                        .select_only()
                        .column(task::Column::WorkspaceId)
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                    else {
                        tx.rollback().await?;
                        return Ok(false);
                    };
                    if thread::Entity::find_by_id(&claim.thread_id)
                        .filter(thread::Column::WorkspaceId.eq(workspace))
                        .select_only()
                        .column(thread::Column::Id)
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                        .is_none()
                        || turn::Entity::find_by_id(&claim.turn_id)
                            .filter(turn::Column::ThreadId.eq(&claim.thread_id))
                            .filter(turn::Column::TurnKind.eq("conversation"))
                            .filter(turn::Column::Status.is_in([
                                "completed",
                                "failed",
                                "interrupted",
                            ]))
                            .select_only()
                            .column(turn::Column::Id)
                            .into_tuple::<String>()
                            .one(&tx)
                            .await?
                            .is_none()
                    {
                        tx.rollback().await?;
                        return Ok(false);
                    }
                }
            }
            if cleanup_ready == Some(true) {
                super::turn_llm_context::delete_turn_llm_context_for_settled_child(
                    &tx,
                    &claim.turn_id,
                )
                .await?;
            } else {
                super::turn_llm_context::delete_turn_llm_context_for_turn(&tx, &claim.turn_id)
                    .await?;
            }
            super::turn_runtime_snapshot::delete_turn_runtime_snapshot(&tx, &claim.turn_id).await?;
            if !(point::Entity::delete_many()
                .filter(point_fence(claim))
                .exec(&tx)
                .await?
                .rows_affected
                == 1)
            {
                return Err(super::portable_payload::PreflightRace.into());
            }
            tx.commit().await?;
            Ok(true)
        })
        .await
    }
}

fn insert_race(error: sea_orm::DbErr) -> anyhow::Error {
    if matches!(
        error.sql_err(),
        Some(sea_orm::SqlErr::UniqueConstraintViolation(_))
    ) {
        super::portable_payload::PreflightRace.into()
    } else {
        error.into()
    }
}
/// SQLSTATE/engine conditions that require restarting the entire transaction.
/// Other errors, including ambiguous transport/commit failures, are not races.
pub(crate) fn is_transaction_retry(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let Some(error) = cause.downcast_ref::<sea_orm::DbErr>() else {
            return false;
        };
        let runtime = match error {
            sea_orm::DbErr::Query(r) | sea_orm::DbErr::Exec(r) => r,
            _ => return false,
        };
        if let sea_orm::RuntimeErr::SqlxError(error) = runtime {
            if let sea_orm::sqlx::Error::Database(error) = error.as_ref() {
                if matches!(error.code().as_deref(), Some("40001" | "40P01")) {
                    return true;
                }
                #[cfg(feature = "mysql")]
                if let Some(mysql) =
                    error.try_downcast_ref::<sea_orm::sqlx::mysql::MySqlDatabaseError>()
                {
                    return matches!(mysql.number(), 1205 | 1213);
                }
                return false;
            }
        }
        false
    })
}

impl CrudStore {
    /// Cutover requires BOTH committed bootstrap completion and the recognized
    /// typed producer generation. Unknown legacy trigger generations keep the
    /// historical compatibility reader; a storage error is never empty/ready.
    pub async fn child_work_queues_ready(&self) -> Result<bool> {
        if self
            .child_seed_outcome(chrono::Utc::now().timestamp())
            .await?
            != ChildSeedOutcome::CommittedDone
        {
            return Ok(false);
        }
        super::task_occurrence_producers::typed(&self.connection).await
    }
    pub async fn discover_legacy_child_work(&self, limit: u64) -> Result<()> {
        let rows = self
            .list_unreconciled_terminal_task_child_turns(limit.min(PAGE))
            .await?;
        if rows.is_empty() {
            return Ok(());
        }
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let now = chrono::Utc::now().timestamp();
            for row in &rows {
                use pioneer_entity::turn;
                // Historical discovery was outside this transaction. Fence the
                // current source locator before it can replace a moved ticket.
                let mut query = turn::Entity::find_by_id(&row.turn_id)
                    .filter(turn::Column::ThreadId.eq(&row.thread_id))
                    .select_only()
                    .column(turn::Column::Id);
                if tx.get_database_backend() != sea_orm::DbBackend::Sqlite {
                    query = query.lock_exclusive();
                }
                if query.into_tuple::<String>().one(&tx).await?.is_some() {
                    ensure_discovered_point(&tx, &row.thread_id, &row.turn_id, now, true).await?;
                }
            }
            tx.commit().await?;
            Ok(())
        })
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildSeedOutcome {
    CommittedDone,
    Progress,
    Deferred { retry_at: i64 },
    Claimed { expires_at: i64 },
    Missing,
}
fn seed_outcome(row: Option<&scope::Model>, now: i64) -> Result<ChildSeedOutcome> {
    let Some(row) = row else {
        return Ok(ChildSeedOutcome::Missing);
    };
    let Some(deadline) = row.next_attempt_at else {
        ensure!(
            row.claim_token.is_none()
                && row.pass_revision.is_none()
                && row.cursor.is_none()
                && row.upper_watermark.is_none(),
            "unfinished child seed has no retry deadline"
        );
        return Ok(ChildSeedOutcome::CommittedDone);
    };
    if deadline > now {
        if row.claim_token.is_some() {
            Ok(ChildSeedOutcome::Claimed {
                expires_at: deadline,
            })
        } else {
            Ok(ChildSeedOutcome::Deferred { retry_at: deadline })
        }
    } else {
        Ok(ChildSeedOutcome::Progress)
    }
}
impl CrudStore {
    pub async fn child_seed_outcome(&self, now: i64) -> Result<ChildSeedOutcome> {
        let row = scope::Entity::find_by_id(("historical-seed".to_owned(), "v1".to_owned()))
            .one(&self.connection)
            .await?;
        seed_outcome(row.as_ref(), now)
    }
    /// One owned seed quantum. Only the committed EOF marker means Done;
    /// an empty due probe or a future lease does not complete bootstrap.
    pub async fn seed_child_work_with_clock<F: Fn() -> i64 + Sync>(
        &self,
        clock: &F,
    ) -> Result<ChildSeedOutcome> {
        let row = scope::Entity::find_by_id(("historical-seed".to_owned(), "v1".to_owned()))
            .one(&self.connection)
            .await?;
        let outcome = seed_outcome(row.as_ref(), clock())?;
        if outcome != ChildSeedOutcome::Progress {
            return Ok(outcome);
        }
        let row = row.expect("progress has a locator");
        match self
            .expand_child_scope_candidate_with_clock(&row, clock, true)
            .await
        {
            Ok(_) => self.child_seed_outcome(clock()).await,
            Err(error) => {
                // A committed deferral or an unknown successful claim remains
                // owned by this runner until its deadline, never reported done.
                let observed =
                    scope::Entity::find_by_id(("historical-seed".to_owned(), "v1".to_owned()))
                        .one(&self.connection)
                        .await;
                match observed {
                    Ok(fresh) => match seed_outcome(fresh.as_ref(), clock()) {
                        Ok(ChildSeedOutcome::Progress) if fresh.as_ref() != Some(&row) => {
                            Ok(ChildSeedOutcome::Progress)
                        }
                        Ok(
                            state @ (ChildSeedOutcome::Deferred { .. }
                            | ChildSeedOutcome::Claimed { .. }
                            | ChildSeedOutcome::CommittedDone
                            | ChildSeedOutcome::Missing),
                        ) => Ok(state),
                        _ => Err(error),
                    },
                    Err(_) => Err(error),
                }
            }
        }
    }
}

impl CrudStore {
    pub async fn next_task_child_work_deadline(&self, now: i64) -> Result<Option<i64>> {
        let seed = match self.child_seed_outcome(now).await? {
            ChildSeedOutcome::CommittedDone => None,
            ChildSeedOutcome::Deferred { retry_at } => Some(retry_at),
            ChildSeedOutcome::Claimed { expires_at } => Some(expires_at),
            ChildSeedOutcome::Progress | ChildSeedOutcome::Missing => Some(now),
        };
        let point = point::Entity::find()
            .select_only()
            .column(point::Column::NextAttemptAt)
            .filter(point::Column::NextAttemptAt.is_not_null())
            .order_by_asc(point::Column::NextAttemptAt)
            .limit(1)
            .into_tuple::<i64>()
            .one(&self.connection)
            .await?;
        let scope = scope::Entity::find()
            .select_only()
            .column(scope::Column::NextAttemptAt)
            .filter(scope::Column::NextAttemptAt.is_not_null())
            .order_by_asc(scope::Column::NextAttemptAt)
            .limit(1)
            .into_tuple::<i64>()
            .one(&self.connection)
            .await?;
        Ok(seed.into_iter().chain(point).chain(scope).min())
    }
}
