//! Bounded discovery and expansion of durable task-child scopes.
use anyhow::Result;
use pioneer_entity::{task_child_reconcile_pending as point, task_child_reconcile_scope as scope};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect,
};

use crate::child_work::{self, enqueue_point};

const PAGE: u64 = 32;
const LEASE: i64 = 60;

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
        }
    }
}

pub async fn enqueue_scope<C: ConnectionTrait>(db: &C, input: ChildScope, now: i64) -> Result<()> {
    let (kind, key) = input.locator();
    child_work::enqueue_scope(db, kind, &key, now).await
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
        _ => anyhow::bail!("unknown child scope kind"),
    }
    Ok(out)
}

/// Advisory indexed discovery only.
pub async fn discover_child_scope_after(
    db: &impl ConnectionTrait,
    now: i64,
    after: Option<&(i64, String, String)>,
) -> Result<Option<scope::Model>> {
    let base = scope::Entity::find()
        .filter(scope::Column::NextAttemptAt.lte(now))
        .order_by_asc(scope::Column::NextAttemptAt)
        .order_by_asc(scope::Column::ScopeKind)
        .order_by_asc(scope::Column::ScopeKey);
    let mut query = base.clone();
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
    let mut candidate = query.one(db).await?;
    if candidate.is_none() && after.is_some() {
        candidate = base.one(db).await?;
    }
    Ok(candidate)
}
pub async fn child_scope_due_upper(
    db: &impl ConnectionTrait,
    now: i64,
) -> Result<Option<(i64, String, String)>> {
    Ok(scope::Entity::find()
        .select_only()
        .columns([
            scope::Column::NextAttemptAt,
            scope::Column::ScopeKind,
            scope::Column::ScopeKey,
        ])
        .filter(scope::Column::NextAttemptAt.lte(now))
        .order_by_desc(scope::Column::NextAttemptAt)
        .order_by_desc(scope::Column::ScopeKind)
        .order_by_desc(scope::Column::ScopeKey)
        .into_tuple::<(i64, String, String)>()
        .one(db)
        .await?)
}
pub async fn discover_child_scope_in_pass(
    db: &impl ConnectionTrait,
    now: i64,
    after: Option<&(i64, String, String)>,
    upper: &(i64, String, String),
) -> Result<Option<scope::Model>> {
    let mut query = scope::Entity::find()
        .filter(scope::Column::NextAttemptAt.lte(now))
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
        .one(db)
        .await?)
}

/// The claimed scope changed before the page could be acknowledged.
#[derive(Debug)]
pub struct ChildScopeRace;

impl std::fmt::Display for ChildScopeRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("task child scope changed before acknowledgement")
    }
}

impl std::error::Error for ChildScopeRace {}

/// Expand at most one page, retaining the claim only for this transaction.
pub async fn expand_child_scope_page(
    tx: &DatabaseTransaction,
    candidate: &scope::Model,
    claim: &str,
    now: i64,
) -> Result<bool> {
    if scope::Entity::update_many()
        .filter(scope_fence(candidate))
        .filter(scope::Column::NextAttemptAt.lte(now))
        .col_expr(
            scope::Column::AttemptCount,
            Expr::value(candidate.attempt_count.saturating_add(1)),
        )
        .col_expr(scope::Column::ClaimToken, Expr::value(claim))
        .col_expr(
            scope::Column::NextAttemptAt,
            Expr::value(now.saturating_add(LEASE)),
        )
        .exec(tx)
        .await?
        .rows_affected
        != 1
    {
        return Ok(false);
    }
    let mut current = scope::Entity::find()
        .filter(scope::Column::ScopeKind.eq(candidate.scope_kind.clone()))
        .filter(scope::Column::ScopeKey.eq(candidate.scope_key.clone()))
        .one(tx)
        .await?
        .expect("claimed scope exists");
    let claimed = current.clone();
    if current.pass_revision.is_none() {
        current.pass_revision = Some(current.requested_revision.clone());
        current.cursor = None;
        current.upper_watermark = page(tx, &current, true)
            .await?
            .first()
            .map(|item| item.cursor.clone());
    }
    let items = if current.upper_watermark.is_some() {
        page(tx, &current, false).await?
    } else {
        Vec::new()
    };
    let last = items.last().map(|item| item.cursor.clone());
    let eof = items.len() < PAGE as usize;
    for item in items {
        match item.output {
            Output::Scope(input) => enqueue_scope(tx, input, now).await?,
            Output::Point(thread, id) => {
                enqueue_point(tx, &thread, &id, now).await?;
            }
        }
    }
    // Lock-free CAS still fences ABA and prior holders. Producer writes
    // serialize on this row; a changed request is never acknowledged.
    let mut update = scope::Entity::update_many().filter(scope_pass_fence(&claimed));
    if eof && current.pass_revision.as_ref() == Some(&current.requested_revision) {
        if !(scope::Entity::delete_many()
            .filter(scope_pass_fence(&claimed))
            .filter(scope::Column::RequestedRevision.eq(&current.requested_revision))
            .exec(tx)
            .await?
            .rows_affected
            == 1)
        {
            return Err(ChildScopeRace.into());
        }
        return Ok(true);
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
        .exec(tx)
        .await?
        .rows_affected
        == 1)
    {
        return Err(ChildScopeRace.into());
    }
    Ok(true)
}

pub async fn defer_child_scope_snapshot(
    db: &impl ConnectionTrait,
    candidate: &scope::Model,
    now: i64,
) -> Result<()> {
    scope::Entity::update_many()
        .filter(scope_fence(candidate))
        .filter(scope::Column::RequestedRevision.eq(&candidate.requested_revision))
        .col_expr(
            scope::Column::NextAttemptAt,
            Expr::value(now.saturating_add(2)),
        )
        .col_expr(
            scope::Column::ClaimToken,
            Expr::value(Option::<String>::None),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn next_task_child_work_deadline(db: &impl ConnectionTrait) -> Result<Option<i64>> {
    let point = point::Entity::find()
        .select_only()
        .column(point::Column::NextAttemptAt)
        .filter(point::Column::NextAttemptAt.is_not_null())
        .order_by_asc(point::Column::NextAttemptAt)
        .limit(1)
        .into_tuple::<i64>()
        .one(db)
        .await?;
    let scope = scope::Entity::find()
        .select_only()
        .column(scope::Column::NextAttemptAt)
        .filter(scope::Column::NextAttemptAt.is_not_null())
        .order_by_asc(scope::Column::NextAttemptAt)
        .limit(1)
        .into_tuple::<i64>()
        .one(db)
        .await?;
    Ok(point.into_iter().chain(scope).min())
}
