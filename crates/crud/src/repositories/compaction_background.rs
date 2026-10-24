//! Durable locators for optional root conversation preparation. No history copy.
use anyhow::{Result, ensure};
use pioneer_entity::{compaction_history_check, thread, turn};
use sea_orm::sea_query::{Expr, ExprTrait, Func, JoinType, OnConflict, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QuerySelect,
};
#[derive(Debug, Clone, FromQueryResult)]
pub struct CompletedHistoryCheck {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub descriptor: Option<String>,
    pub revision: i64,
    pub failures: i64,
    pub attempt_deadline_ms: Option<i64>,
    pub diagnostic: Option<String>,
    pub config_hash: Option<String>,
    pub outcome: Option<String>,
    pub managed: i64,
}
pub(crate) async fn compaction_enqueue_native_history_check<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    descriptor: &str,
) -> Result<()> {
    ensure!(
        descriptor.len() <= 16384,
        "history descriptor exceeds bound"
    );
    let inserted = db
        .execute(
            &Query::insert()
                .into_table(compaction_history_check::Entity)
                .columns([
                    compaction_history_check::Column::TurnId,
                    compaction_history_check::Column::Descriptor,
                ])
                .select_from(
                    turn::Entity::find()
                        .select_only()
                        .join(
                            JoinType::InnerJoin,
                            turn::Entity::belongs_to(thread::Entity)
                                .from(turn::Column::ThreadId)
                                .to(thread::Column::Id)
                                .into(),
                        )
                        .expr(Expr::col((turn::Entity, turn::Column::Id)))
                        .expr(Expr::Value(descriptor.into()))
                        .filter(
                            Expr::col((turn::Entity, turn::Column::Id))
                                .eq(Expr::Value(turn.into()))
                                .and(
                                    Expr::col((turn::Entity, turn::Column::ThreadId))
                                        .eq(Expr::Value(thread.into())),
                                )
                                .and(
                                    Expr::col((thread::Entity, thread::Column::WorkspaceId))
                                        .eq(Expr::Value(workspace.into())),
                                )
                                .and(
                                    Expr::col((turn::Entity, turn::Column::Status))
                                        .eq(Expr::Value("in_progress".into())),
                                ),
                        )
                        .into_query(),
                )?
                .on_conflict(
                    OnConflict::column(compaction_history_check::Column::TurnId)
                        .update_column(compaction_history_check::Column::Descriptor)
                        .action_and_where(
                            Expr::col((
                                compaction_history_check::Entity,
                                compaction_history_check::Column::State,
                            ))
                            .eq("pending")
                            .and(
                                Expr::col((
                                    compaction_history_check::Entity,
                                    compaction_history_check::Column::Managed,
                                ))
                                .eq(0_i64),
                            ),
                        )
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    if inserted.rows_affected() == 0 {
        let valid_existing = compaction_history_check::Entity::find_by_id(turn)
            .join(
                JoinType::InnerJoin,
                compaction_history_check::Entity::belongs_to(turn::Entity)
                    .from(compaction_history_check::Column::TurnId)
                    .to(turn::Column::Id)
                    .into(),
            )
            .join(
                JoinType::InnerJoin,
                turn::Entity::belongs_to(thread::Entity)
                    .from(turn::Column::ThreadId)
                    .to(thread::Column::Id)
                    .into(),
            )
            .filter(turn::Column::ThreadId.eq(thread))
            .filter(thread::Column::WorkspaceId.eq(workspace))
            .one(db)
            .await?
            .is_some();
        ensure!(valid_existing, "native history intent scope mismatch");
    }
    Ok(())
}
/// A newer execution invalidates an optional older check. Inspect one
/// locator at a time rather than bulk-updating a thread's retained history.
pub(crate) async fn compaction_history_check_is_current<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    revision: Option<i64>,
) -> Result<bool> {
    // Only publication-triggered level-zero work is eligible. Native execution
    // intents remain durable lifecycle metadata, including those of children.
    Ok(db.query_one_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DbBackend::Sqlite,
        r#"SELECT 1 FROM compaction_history_check h
           JOIN turn t ON t.id=h.turn_id
           JOIN compaction_root_activity a ON a.thread_id=t.thread_id AND a.user_turn_id=t.id
           WHERE h.turn_id=? AND (? IS NULL OR h.revision=?) AND h.state='pending' AND h.managed=1 AND h.purpose='root_conversation' AND a.recovery_pending=0 AND t.status IN ('completed','failed','blocked')
             AND NOT EXISTS (SELECT 1 FROM thread_lineage l WHERE l.child_thread_id=t.thread_id)
             AND NOT EXISTS (SELECT 1 FROM compaction_execution_stop s
                 JOIN compaction_context c ON c.owner=s.owner
                 WHERE s.turn_id=t.id AND c.thread_id=t.thread_id)"#,
        [turn.into(), revision.into(), revision.into()],
    )).await?.is_some())
}

/// Indexed control metadata only; callers release the reader before capture,
/// provider preparation or waiting for foreground ownership.
pub(crate) async fn root_history_activity<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<Option<i64>> {
    let row = db.query_one_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DbBackend::Sqlite,
        "SELECT a.last_user_activity_ms FROM compaction_root_activity a JOIN thread h ON h.id=a.thread_id WHERE h.workspace_id=? AND a.thread_id=? AND a.user_turn_id=? AND a.recovery_pending=0",
        [workspace.into(), thread.into(), turn.into()],
    )).await?;
    row.map(|r| r.try_get("", "last_user_activity_ms"))
        .transpose()
        .map_err(Into::into)
}

pub(crate) async fn root_history_busy<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    root: &str,
) -> Result<bool> {
    Ok(db.query_one_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DbBackend::Sqlite,
        r#"SELECT 1 FROM thread h WHERE h.id=? AND h.workspace_id=? AND (
            EXISTS (SELECT 1 FROM turn t WHERE t.thread_id=h.id AND t.status='in_progress'
                    AND t.origin='user' AND t.work_owner='turn')
            OR EXISTS (SELECT 1 FROM task k WHERE k.created_by_thread_id=h.id AND k.workspace_id=h.workspace_id
                AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.version')=1
                AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.launch.thread_id')=h.id
                AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.launch.turn_id')=k.created_by_turn_id
                AND k.metadata_json IS NOT NULL AND k.status NOT IN ('completed','failed','blocked','cancelled')
                AND EXISTS (SELECT 1 FROM turn human WHERE human.id=k.created_by_turn_id
                    AND human.thread_id=h.id AND human.origin='user' AND human.turn_kind='conversation'
                    AND human.initiated_by_actor_kind='principal' AND human.work_owner='detached_task'))
        )"#,
        [root.into(), workspace.into()],
    )).await?.is_some())
}

/// A technical Composer execution shares foreground priority with the visible
/// conversation. Ordinary nested tasks do not acquire that conversation key.
pub(crate) async fn root_for_history_execution<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<Option<String>> {
    let row = db.query_one_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DbBackend::Sqlite,
        r#"SELECT k.created_by_thread_id AS root FROM task_run_turn x
            JOIN task k ON k.id=x.task_id
            JOIN thread h ON h.id=k.created_by_thread_id AND h.workspace_id=k.workspace_id
            JOIN turn u ON u.id=k.created_by_turn_id AND u.thread_id=h.id AND u.origin='user'
                AND u.turn_kind='conversation' AND u.initiated_by_actor_kind='principal' AND u.work_owner='detached_task'
            WHERE x.turn_id=? AND x.thread_id=? AND k.workspace_id=?
              AND h.origin_kind IN ('user','collaborative','direct_message')
              AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.version')=1
              AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.launch.thread_id')=h.id
              AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.launch.turn_id')=u.id
              AND NOT EXISTS (SELECT 1 FROM thread_lineage l WHERE l.child_thread_id=h.id)
            LIMIT 1"#,
        [turn.into(), thread.into(), workspace.into()],
    )).await?;
    row.map(|r| r.try_get("", "root"))
        .transpose()
        .map_err(Into::into)
}

/// Recover only roots first touched after upgrade. Publication already saved
/// the latest input, its own original ready time, and durable queue debt. Read
/// at most 16 roots / two 64-row index seeks per root; never hold a reader while
/// processing rows or committing progress. New publication/input uses MAX and
/// does not reset this cursor. No clock, payload, LLM or service is involved.
pub(crate) async fn recover_root_activity_quantum(store: &crate::CrudStore) -> Result<()> {
    use sea_orm::{DbBackend, Statement};
    const EVENT_MS: &str = "(CAST(strftime('%s',created_at) AS INTEGER)*1000 + CAST(substr(strftime('%f',created_at),4,3) AS INTEGER))";
    let roots = store.connection.query_all_raw(Statement::from_string(DbBackend::Sqlite,
        "SELECT thread_id,last_user_activity_ms,recovery_before_ms,recovery_before_id FROM compaction_root_activity WHERE recovery_pending=1 ORDER BY thread_id LIMIT 16".to_owned()
    )).await?;
    for root in roots {
        let thread: String = root.try_get("", "thread_id")?;
        let activity: i64 = root.try_get("", "last_user_activity_ms")?;
        let before_ms: i64 = root.try_get("", "recovery_before_ms")?;
        let before_id: String = root.try_get("", "recovery_before_id")?;
        // Separate equality/earlier seeks give each cursor component an
        // explicit index range, including arbitrarily many equal timestamps.
        // MATERIALIZED bounds authority work to the newest 64 of <=128 keys.
        // No OFFSET, residual tuple filter or unbounded historical sort.
        let rows = if activity >= before_ms {
            // A publication/input already covers every remaining timestamp.
            Vec::new()
        } else {
            store.connection.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            format!(r#"WITH same_time AS MATERIALIZED (
              SELECT id,turn_id,event_type,{EVENT_MS} AS event_ms FROM turn_event
              WHERE thread_id=? AND event_type IN ('turn/completed','item/completed')
                AND {EVENT_MS}=? AND id<?
              ORDER BY id DESC LIMIT 64
            ), earlier_time AS MATERIALIZED (
              SELECT id,turn_id,event_type,{EVENT_MS} AS event_ms FROM turn_event
              WHERE thread_id=? AND event_type IN ('turn/completed','item/completed')
                AND {EVENT_MS}<? AND {EVENT_MS}>?
              ORDER BY {EVENT_MS} DESC,id DESC LIMIT 64
            ), quantum AS MATERIALIZED (
              SELECT * FROM same_time UNION ALL SELECT * FROM earlier_time
              ORDER BY event_ms DESC,id DESC LIMIT 64
            )
            SELECT q.id,q.event_ms,CASE WHEN
              (q.event_type='turn/completed' AND EXISTS (
                SELECT 1 FROM turn t WHERE t.id=q.turn_id AND t.thread_id=?
                  AND t.origin='user' AND t.turn_kind='conversation'
                  AND t.initiated_by_actor_kind='principal' AND t.work_owner='turn' AND t.status='completed'
                  AND EXISTS(SELECT 1 FROM turn_item i WHERE i.turn_id=t.id AND i.item_type='agent_message' AND i.status='completed')))
              OR (q.event_type='item/completed' AND EXISTS (
                SELECT 1 FROM compaction_event_revision r
                JOIN task_delivery d ON d.id=substr(r.item_id,length('task_delivery_result_')+1)
                  AND r.item_id='task_delivery_result_'||d.id
                JOIN thread h ON h.id=d.target_thread_id AND h.workspace_id=d.workspace_id
                JOIN compaction_delivery_output b ON b.delivery_id=d.id
                JOIN compaction_task_output s ON s.task_run_turn_id=b.task_run_turn_id
                  AND s.task_id=d.task_id AND s.run_id=d.run_id AND s.workspace_id=d.workspace_id
                JOIN task_result_candidate c ON c.id=b.candidate_id AND c.status='accepted'
                  AND c.task_run_turn_id=s.task_run_turn_id AND c.task_id=s.task_id AND c.run_id=s.run_id
                  AND c.thread_id=s.source_thread AND c.turn_id=s.source_turn
                JOIN task k ON k.id=d.task_id AND k.workspace_id=d.workspace_id
                JOIN turn human ON human.id=k.created_by_turn_id AND human.thread_id=h.id
                  AND human.origin='user' AND human.turn_kind='conversation'
                  AND human.initiated_by_actor_kind='principal' AND human.work_owner='detached_task'
                WHERE r.source_id=q.id AND r.turn_id=q.turn_id AND r.present=1 AND r.projection_revision=r.revision
                  AND d.delivered_turn_id=q.turn_id AND h.id=?
                  AND d.mode='thread' AND d.status='delivered' AND d.delivered_at IS NOT NULL AND d.error_snapshot_json IS NULL
                  AND k.created_by_thread_id=h.id
                  AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{{}}' END,'$.composerWork.version')=1
                  AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{{}}' END,'$.composerWork.launch.thread_id')=h.id
                  AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{{}}' END,'$.composerWork.launch.turn_id')=human.id
                  AND EXISTS(SELECT 1 FROM turn_item i WHERE i.turn_id=q.turn_id AND i.item_id=r.item_id AND i.status='completed')))
              THEN q.event_ms ELSE 0 END AS confirmed_ms
            FROM quantum q ORDER BY q.event_ms DESC,q.id DESC"#),
            [thread.clone().into(),before_ms.into(),before_id.clone().into(),thread.clone().into(),before_ms.into(),activity.into(),thread.clone().into(),thread.clone().into()]
        )).await?
        };
        let mut confirmed = 0_i64;
        let mut next_ms = before_ms;
        let mut next_id = before_id.clone();
        for row in &rows {
            confirmed = std::cmp::max(confirmed, row.try_get("", "confirmed_ms")?);
            next_ms = row.try_get("", "event_ms")?;
            next_id = row.try_get("", "id")?;
        }
        // Once the newest confirmed event is found, older pages cannot raise
        // the watermark. Current publications have independently raised MAX.
        let pending = i64::from(rows.len() == 64 && confirmed == 0);
        store.run_serialized_write(|| async {
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE compaction_root_activity SET last_user_activity_ms=MAX(last_user_activity_ms,?),recovery_pending=?,recovery_before_ms=?,recovery_before_id=? WHERE thread_id=? AND recovery_pending=1 AND recovery_before_ms=? AND recovery_before_id=?",
                [confirmed.into(),pending.into(),next_ms.into(),next_id.clone().into(),thread.clone().into(),before_ms.into(),before_id.clone().into()]
            )).await?;
            Ok(())
        }).await?;
    }
    Ok(())
}

/// One bounded metadata page; caller releases reader capacity before decoding.
pub(crate) async fn compaction_pending_history_checks<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<CompletedHistoryCheck>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .min(i64::MAX as u128) as i64;
    compaction_due_history_checks(db, now).await
}
pub(crate) async fn compaction_due_history_checks<C: ConnectionTrait>(
    db: &C,
    now: i64,
) -> Result<Vec<CompletedHistoryCheck>> {
    // The root watermark is also the policy discriminator for pre-upgrade
    // pending checks. Never discover the old child-completion queue policy.
    CompletedHistoryCheck::find_by_statement(sea_orm::Statement::from_sql_and_values(
        sea_orm::DbBackend::Sqlite,
        r#"SELECT h.turn_id,t.thread_id,th.workspace_id,
            COALESCE(b.runtime_id,th.model_provider) AS runtime_id,
            COALESCE(b.runtime_kind,'') AS runtime_kind,
            COALESCE(b.model,th.model) AS model,t.reasoning_effort,
            h.descriptor,h.revision,h.failures,h.attempt_deadline_ms,h.diagnostic,h.config_hash,h.outcome,h.managed
           FROM compaction_history_check h
           JOIN turn t ON t.id=h.turn_id
           JOIN compaction_root_activity a ON a.thread_id=t.thread_id AND a.user_turn_id=t.id
           JOIN thread th ON th.id=t.thread_id
           LEFT JOIN turn_cli_runtime_binding b ON b.turn_id=t.id AND b.thread_id=t.thread_id AND b.workspace_id=th.workspace_id
           WHERE h.next_attempt_ms<=? AND h.state='pending' AND h.managed=1 AND h.purpose='root_conversation' AND a.recovery_pending=0 AND t.status IN ('completed','failed','blocked')
             AND NOT EXISTS (SELECT 1 FROM thread_lineage l WHERE l.child_thread_id=t.thread_id)
           ORDER BY h.next_attempt_ms,h.turn_id LIMIT 16"#,
        [now.into()],
    )).all(db).await.map_err(Into::into)
}

/// Persist the captured settings/deadline before service admission; restart
/// reuses this exact descriptor. It contains model metadata, never messages.
pub(crate) async fn compaction_capture_history_check<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    descriptor: &str,
) -> Result<Option<String>> {
    ensure!(
        descriptor.len() <= 16384,
        "CLI history descriptor exceeds bound"
    );
    let rows = compaction_history_check::Entity::update_many()
        .col_expr(
            compaction_history_check::Column::Descriptor,
            Func::coalesce([
                Expr::col(compaction_history_check::Column::Descriptor),
                Expr::val(descriptor),
            ])
            .into(),
        )
        .filter(compaction_history_check::Column::TurnId.eq(turn))
        .filter(compaction_history_check::Column::State.eq("pending"))
        .exec_with_returning(db)
        .await?;
    Ok(rows.into_iter().next().and_then(|row| row.descriptor))
}
pub(crate) async fn compaction_finish_history_check<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    outcome: &str,
) -> Result<()> {
    use pioneer_entity::compaction_history_check as check;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
    ensure!(
        matches!(outcome, "completed" | "cancelled" | "failed"),
        "invalid CLI history outcome"
    );
    check::Entity::update_many()
        .set(check::ActiveModel {
            state: Set("finished".to_owned()),
            outcome: Set(Some(outcome.to_owned())),
            ..Default::default()
        })
        .filter(check::Column::TurnId.eq(turn))
        .filter(check::Column::State.eq("pending"))
        .exec(db)
        .await?;
    Ok(())
}

use sea_orm::QueryTrait;

/// CAS admission. Revalidate currency within the writer transaction: neither a
/// stale discovery page nor Stop/new input can resurrect a completed check.
pub(crate) async fn claim_history_check(
    store: &crate::CrudStore,
    id: &str,
    revision: i64,
    now: i64,
) -> Result<Option<compaction_history_check::Model>> {
    use compaction_history_check::{ActiveModel as A, Column as C, Entity as E};
    use sea_orm::{Set, TransactionTrait};
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            let row = E::find_by_id(id).one(&tx).await?;
            let Some(row) = row.filter(|r| {
                r.revision == revision
                    && r.next_attempt_ms <= now
                    && r.state == "pending"
                    && r.purpose == "root_conversation"
            }) else {
                tx.rollback().await?;
                return Ok(None);
            };
            E::update_many()
                .set(A {
                    state: Set("pending".into()),
                    managed: Set(1),
                    revision: Set(revision + 1),
                    ..Default::default()
                })
                .filter(C::TurnId.eq(id))
                .exec(&tx)
                .await?;
            if !compaction_history_check_is_current(&tx, id, None).await? {
                compaction_finish_history_check(&tx, id, "cancelled").await?;
                tx.commit().await?;
                return Ok(None);
            }
            let claimed = E::find_by_id(id).one(&tx).await?;
            tx.commit().await?;
            Ok(claimed)
        })
        .await
}
/// Captured input was prepared without holding database capacity. CAS prevents
/// a cancelled or replaced job from installing it. Deadlines survive restart.
pub(crate) async fn begin_history_attempt<C: ConnectionTrait>(
    db: &C,
    id: &str,
    revision: i64,
    descriptor: &str,
    hash: &str,
    now: i64,
) -> Result<Option<i64>> {
    use compaction_history_check::{ActiveModel as A, Column as C, Entity as E};
    use sea_orm::Set;
    ensure!(
        descriptor.len() <= 16384 && hash.len() <= 128,
        "history check metadata exceeds bound"
    );
    let rows = E::update_many()
        .set(A {
            descriptor: Set(Some(descriptor.into())),
            config_hash: Set(Some(hash.into())),
            ..Default::default()
        })
        .col_expr(
            C::AttemptDeadlineMs,
            Func::coalesce([
                Expr::col(C::AttemptDeadlineMs),
                Expr::val(now.saturating_add(pioneer_compaction::OPERATION_MILLIS as i64)),
            ])
            .into(),
        )
        .filter(C::TurnId.eq(id))
        .filter(C::Revision.eq(revision))
        .filter(C::State.eq("pending"))
        .exec_with_returning(db)
        .await?;
    Ok(rows.first().and_then(|r| r.attempt_deadline_ms))
}
pub(crate) async fn record_history_result(
    store: &crate::CrudStore,
    id: &str,
    revision: i64,
    failures: i64,
    outcome: super::compaction_check_result::HistoryCheckOutcome,
    diagnostic: &str,
    now: i64,
) -> Result<bool> {
    use compaction_history_check::{ActiveModel as A, Column as C, Entity as E};
    use sea_orm::Set;
    ensure!(diagnostic.len() <= 4096, "history diagnostic exceeds bound");
    use sea_orm::TransactionTrait;
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            let outcome = if compaction_history_check_is_current(&tx, id, None).await? {
                outcome
            } else {
                super::compaction_check_result::HistoryCheckOutcome::Cancelled
            };
            let (state, failures, due) = outcome.schedule(failures, now);
            let label = if state == "finished"
                && outcome == super::compaction_check_result::HistoryCheckOutcome::Retryable
            {
                "failed"
            } else {
                outcome.as_str()
            };
            let result = E::update_many()
                .set(A {
                    state: Set(state.into()),
                    outcome: Set(Some(label.into())),
                    failures: Set(failures),
                    next_attempt_ms: Set(due),
                    diagnostic: Set(Some(diagnostic.into())),
                    attempt_deadline_ms: Set(None),
                    revision: Set(revision + 1),
                    ..Default::default()
                })
                .filter(C::TurnId.eq(id))
                .filter(C::Revision.eq(revision))
                .filter(C::State.eq("pending"))
                .exec(&tx)
                .await?;
            tx.commit().await?;
            Ok(result.rows_affected == 1)
        })
        .await
}
