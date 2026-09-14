//! Bounded, demand-driven registration of legacy metadata. Discovery happens
//! outside the writer; sources and cursor are revalidated inside the transaction.
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_event_revision, compaction_history_preparation as preparation,
    compaction_input_revision, compaction_projection_epoch as epoch, compaction_source_revision,
    thread, turn, turn_event, turn_input, turn_llm_context,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, OnConflict};
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, IntoActiveModel, QueryOrder, QuerySelect, TransactionTrait,
    entity::prelude::*,
};
const PAGE: u64 = 128;

async fn scoped<C: ConnectionTrait>(db: &C, workspace: &str, thread_id: &str) -> Result<bool> {
    Ok(thread::Entity::find_by_id(thread_id)
        .filter(thread::Column::WorkspaceId.eq(workspace))
        .select_only()
        .column(thread::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}
async fn progress<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread_id: &str,
) -> Result<Option<preparation::Model>> {
    if !scoped(db, workspace, thread_id).await? {
        return Ok(None);
    }
    Ok(preparation::Entity::find_by_id(thread_id).one(db).await?)
}
pub(crate) async fn ready(store: &CrudStore, workspace: &str, thread: &str) -> Result<bool> {
    Ok(progress(&store.connection, workspace, thread)
        .await?
        .is_some_and(|p| p.ready == 1))
}

// Entity projections deliberately select metadata only, including through the
// compressed event view. No Model containing a source payload is loaded.
macro_rules! page {
    ($entity:ident, $ordinal:ident, $db:expr, $turn:expr, $after:expr) => {
        $entity::Entity::find()
            .select_only()
            .column($entity::Column::Id)
            .column($entity::Column::$ordinal)
            .filter($entity::Column::TurnId.eq($turn))
            .filter($entity::Column::$ordinal.gt($after))
            .order_by_asc($entity::Column::$ordinal)
            .limit(PAGE)
            .into_tuple::<(String, i64)>()
            .all($db)
            .await?
    };
}
macro_rules! high_water {
    ($entity:ident, $db:expr) => {
        $entity::Entity::find()
            .select_only()
            .column_as($entity::Column::CaptureOrder.max(), "maximum")
            .into_tuple::<Option<i64>>()
            .one($db)
            .await?
            .flatten()
            .unwrap_or(0)
    };
}
// Candidates are rediscovered by exact IDs inside the transaction. One bounded
// insert_many replaces the SQL CTE; existing revisions/tombstones always win.
macro_rules! register {
    ($source:ident, $revision:ident, $ordinal:ident, $db:expr, $turn:expr, $ids:expr) => {{
        let sources = $source::Entity::find()
            .select_only()
            .column($source::Column::Id)
            .filter($source::Column::TurnId.eq($turn))
            .filter($source::Column::Id.is_in($ids.clone()))
            .order_by_asc($source::Column::$ordinal)
            .order_by_asc($source::Column::Id)
            .into_tuple::<String>()
            .all($db)
            .await?;
        let existing: std::collections::HashSet<String> = $revision::Entity::find()
            .select_only()
            .column($revision::Column::SourceId)
            .filter($revision::Column::SourceId.is_in($ids.clone()))
            .into_tuple::<String>()
            .all($db)
            .await?
            .into_iter()
            .collect();
        let base = high_water!($revision, $db);
        let rows: Vec<_> = sources
            .into_iter()
            .filter(|id| !existing.contains(id))
            .enumerate()
            .map(|(index, id)| $revision::ActiveModel {
                source_id: Set(id),
                turn_id: Set($turn.to_owned()),
                revision: Set(1),
                present: Set(1),
                capture_order: Set(base + index as i64 + 1),
                ..Default::default()
            })
            .collect();
        let count = rows.len() as i64;
        if !rows.is_empty() {
            $revision::Entity::insert_many(rows).exec($db).await?;
        }
        count
    }};
}

pub(crate) async fn quantum(store: &CrudStore, workspace: &str, thread_id: &str) -> Result<bool> {
    let Some(p) = progress(&store.connection, workspace, thread_id).await? else {
        store
            .run_serialized_write(|| async {
                let txn = store.connection.begin().await?;
                ensure!(
                    scoped(&txn, workspace, thread_id).await?,
                    "history preparation thread is unavailable"
                );
                let upper = turn::Entity::find()
                    .select_only()
                    .expr_as(Expr::col(Alias::new("rowid")).max(), "maximum")
                    .filter(turn::Column::ThreadId.eq(thread_id))
                    .into_tuple::<Option<i64>>()
                    .one(&txn)
                    .await?
                    .flatten()
                    .unwrap_or(0);
                preparation::Entity::insert(preparation::ActiveModel {
                    thread_id: Set(thread_id.into()),
                    upper_turn_rowid: Set(upper),
                    ..Default::default()
                })
                .on_conflict(
                    OnConflict::column(preparation::Column::ThreadId)
                        .do_nothing()
                        .to_owned(),
                )
                .try_insert()
                .exec(&txn)
                .await?;
                txn.commit().await?;
                Ok(())
            })
            .await?;
        return Ok(false);
    };
    if p.ready == 1 {
        return Ok(true);
    }
    let mut next = p.clone();
    let mut ids = Vec::new();
    let mut finished = false;
    if let Some(turn_id) = &p.turn_id {
        if p.source_kind == 3 {
            next.turn_id = None;
            next.source_kind = 0;
            next.after_sequence = -1;
        } else {
            let page = match p.source_kind {
                0 => page!(
                    turn_input,
                    InputIndex,
                    &store.connection,
                    turn_id,
                    p.after_sequence
                ),
                1 => page!(
                    turn_event,
                    Sequence,
                    &store.connection,
                    turn_id,
                    p.after_sequence
                ),
                2 => page!(
                    turn_llm_context,
                    Sequence,
                    &store.connection,
                    turn_id,
                    p.after_sequence
                ),
                _ => anyhow::bail!("invalid history preparation source kind"),
            };
            if let Some((_, ordinal)) = page.last() {
                next.after_sequence = *ordinal;
                ids = page.into_iter().map(|(id, _)| id).collect();
            } else {
                next.source_kind += 1;
                next.after_sequence = -1;
            }
        }
    } else {
        let row = turn::Entity::find()
            .select_only()
            .column(turn::Column::Id)
            .expr_as(Expr::col(Alias::new("rowid")), "ordinal")
            .filter(turn::Column::ThreadId.eq(thread_id))
            .filter(Expr::col(Alias::new("rowid")).gt(p.turn_rowid))
            .filter(Expr::col(Alias::new("rowid")).lte(p.upper_turn_rowid))
            .order_by_asc(Expr::col(Alias::new("rowid")))
            .limit(1)
            .into_tuple::<(String, i64)>()
            .one(&store.connection)
            .await?;
        if let Some((id, ordinal)) = row {
            next.turn_id = Some(id);
            next.turn_rowid = ordinal;
        } else {
            finished = true;
        }
    }
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            let current = progress(&txn, workspace, thread_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("history preparation thread disappeared"))?;
            if current.step != p.step || current.ready == 1 {
                txn.rollback().await?;
                return Ok(current.ready == 1);
            }
            let mut next = next.clone();
            if !ids.is_empty() {
                let turn_id = p.turn_id.as_deref().expect("source page has a turn");
                let available = turn::Entity::find_by_id(turn_id)
                    .filter(turn::Column::ThreadId.eq(thread_id))
                    .select_only()
                    .column(turn::Column::Id)
                    .into_tuple::<String>()
                    .one(&txn)
                    .await?
                    .is_some();
                if available {
                    next.inserted += match p.source_kind {
                        0 => register!(
                            turn_input,
                            compaction_input_revision,
                            InputIndex,
                            &txn,
                            turn_id,
                            ids
                        ),
                        1 => register!(
                            turn_event,
                            compaction_event_revision,
                            Sequence,
                            &txn,
                            turn_id,
                            ids
                        ),
                        2 => register!(
                            turn_llm_context,
                            compaction_source_revision,
                            Sequence,
                            &txn,
                            turn_id,
                            ids
                        ),
                        _ => unreachable!(),
                    };
                }
            }
            if finished {
                if p.inserted > 0 {
                    epoch::Entity::insert(epoch::ActiveModel {
                        thread_id: Set(thread_id.into()),
                        version: Set(1),
                        ..Default::default()
                    })
                    .on_conflict(
                        OnConflict::column(epoch::Column::ThreadId)
                            .value(
                                epoch::Column::Version,
                                Expr::col(epoch::Column::Version).add(1),
                            )
                            .to_owned(),
                    )
                    .exec(&txn)
                    .await?;
                }
                next.input_order = high_water!(compaction_input_revision, &txn);
                next.event_order = high_water!(compaction_event_revision, &txn);
                next.context_order = high_water!(compaction_source_revision, &txn);
            }
            next.step += 1;
            next.ready = i64::from(finished);
            // Cursor was revalidated under the same serialized write transaction.
            next.into_active_model().reset_all().update(&txn).await?;
            txn.commit().await?;
            Ok(finished)
        })
        .await
}
