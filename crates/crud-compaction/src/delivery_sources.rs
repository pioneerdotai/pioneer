//! Bounded source metadata for committed Task delivery history.

use sea_orm::{ConnectionTrait, DbBackend, DbErr, FromQueryResult, Statement};

#[derive(sea_orm::FromQueryResult)]
pub struct DeliveryTurnRow {
    pub turn_id: String,
    pub event_high_water: Option<i64>,
}

#[derive(sea_orm::FromQueryResult)]
pub struct DeliveredOutputRow {
    pub event_id: String,
    pub event_turn: String,
    pub sequence: i64,
    pub event_type: String,
    pub revision: i64,
    pub capture_order: i64,
    pub projection_revision: Option<i64>,
    pub delivery_id: Option<String>,
    pub candidate_id: Option<String>,
    pub task_run_turn_id: Option<String>,
    pub source_thread: Option<String>,
    pub source_turn: Option<String>,
    pub bound_candidate: Option<String>,
}

/// The scoped delivery index is the starting point. Both existing Turn fence
/// components exclude newly created Turns, including IDs inserted below the
/// lexical high water.
/// The scalar seek uses the foreign-key index collation. The Turn ID itself
/// uses C collation to keep client cursor ordering stable.
const DELIVERY_TURN_SQL: &str = r#"SELECT d.delivered_turn_id AS turn_id,
    (SELECT e.sequence FROM turn_event e WHERE e.turn_id=t.id COLLATE "default"
     ORDER BY e.sequence DESC LIMIT 1) AS event_high_water
FROM task_delivery d
JOIN turn t ON t.id=d.delivered_turn_id AND t.thread_id=d.target_thread_id
JOIN thread h ON h.id=t.thread_id AND h.workspace_id=d.workspace_id
LEFT JOIN LATERAL (
    SELECT c.sequence FROM compaction_turn_creation c
    WHERE c.turn_id=t.id COLLATE "default" LIMIT 1
) ct ON TRUE
WHERE d.workspace_id=$1 AND d.target_thread_id=$2 AND d.delivered_turn_id>$3
  AND d.delivered_turn_id<=$4 AND (ct.sequence IS NULL OR ct.sequence<=$5)
  AND ($6::bigint IS NULL OR t.created_at < to_timestamp($6::bigint))
  AND d.status='delivered'
ORDER BY d.delivered_turn_id LIMIT 1"#;

/// Capture eligibility is checked BEFORE LIMIT: late events cannot fill a
/// page or alter its continuation. The fixed per-Turn upper seek bound also
/// prevents a continuing append from extending the SQL's inspected tail.
/// JOIN keeps revisions as point lookups from scoped canonical events.
const DELIVERY_EVENT_QUANTUM_SQL: &str = r#"
WITH quantum AS MATERIALIZED (
    SELECT e.id,e.turn_id,e.thread_id,e.sequence,e.event_type,
           r.revision AS selected_revision,r.capture_order AS selected_order
    FROM turn t JOIN thread h ON h.id=t.thread_id
    JOIN turn_event e ON e.turn_id=$1 AND e.thread_id=$2
    JOIN compaction_event_revision r ON r.source_id=e.id
    WHERE t.id=$1 AND t.thread_id=$2 AND h.workspace_id=$3
      AND ($4::bigint IS NULL OR e.sequence > $4)
      AND e.sequence<=$5 AND r.turn_id=e.turn_id AND r.present=1
      AND r.capture_order<=$6
      AND ($7::bigint IS NULL OR e.created_at < to_timestamp($7::bigint))
    ORDER BY e.sequence LIMIT 127
)
"#;

/// A refreshed page is resolved by exact source keys and expected revisions.
/// No Turn/event pagination predicate is reused here. Any edit, deletion,
/// sequence/scope change, or replacement removes a row and fails validation.
/// Each lateral limit fixes the lookup to the unique source key before the
/// outer predicates, so a refresh cannot turn into a scan of scoped history.
const DELIVERY_RECHECK_QUANTUM_SQL: &str = r#"
WITH quantum AS MATERIALIZED (
    SELECT e.id,e.turn_id,e.thread_id,e.sequence,e.event_type,
           r.revision AS selected_revision,r.capture_order AS selected_order
    FROM json_array_elements($1::text::json) AS w(value)
    JOIN LATERAL (
        SELECT event.id,event.turn_id,event.thread_id,event.sequence,event.event_type
        FROM turn_event event WHERE event.id=(w.value->'source'->>'id') LIMIT 1
    ) e ON TRUE
    JOIN turn t ON t.id=e.turn_id AND t.thread_id=e.thread_id
    JOIN thread h ON h.id=t.thread_id
    JOIN LATERAL (
        SELECT revision.turn_id,revision.revision,revision.capture_order,revision.present
        FROM compaction_event_revision revision WHERE revision.source_id=e.id LIMIT 1
    ) r ON TRUE
    WHERE h.workspace_id=$2 AND t.thread_id=$3 AND r.turn_id=e.turn_id
      AND 'event:'||e.turn_id=(w.value->'source'->>'scope')
      AND 'event-revision:'||r.revision=(w.value->'source'->>'version')
      AND e.sequence=(w.value->>'sequence')::bigint
      AND r.capture_order=(w.value->>'capture_order')::bigint
      AND r.present=1 AND r.capture_order<=$4
)
"#;

// SeaQuery does not expose json_array_elements with WITH column names in its
// table expressions. The bounded delivery scan and exact refresh share this
// projection, binding the same three typed values after their own CTE inputs.
fn delivery_bindings_sql(input_count: usize) -> String {
    let prefix = input_count + 1;
    let event_type = input_count + 2;
    let workspace = input_count + 3;
    format!(
        r#"
SELECT q.id AS event_id,q.turn_id AS event_turn,q.sequence,q.event_type,
       r.revision,r.capture_order,r.projection_revision,
       d.id AS delivery_id,b.candidate_id,b.task_run_turn_id,
       s.source_thread,s.source_turn,c.id AS bound_candidate
FROM quantum q
JOIN compaction_event_revision r ON r.source_id=q.id
    AND r.turn_id=q.turn_id AND r.revision=q.selected_revision
    AND r.capture_order=q.selected_order AND r.present=1
LEFT JOIN LATERAL (
    SELECT delivery.id,delivery.task_id,delivery.run_id,delivery.workspace_id,
           delivery.target_thread_id,delivery.delivered_turn_id,delivery.status
    FROM task_delivery delivery
    WHERE delivery.id=substr(r.item_id,length(${prefix}::text)+1) LIMIT 1
) d ON r.item_id=(${prefix} || d.id)
   AND q.event_type=${event_type} AND r.projection_revision=r.revision
   AND d.workspace_id=${workspace} AND d.target_thread_id=q.thread_id
   AND d.delivered_turn_id=q.turn_id AND d.status='delivered'
LEFT JOIN compaction_delivery_output b ON b.delivery_id=d.id
LEFT JOIN compaction_task_output s
    ON s.task_run_turn_id=b.task_run_turn_id AND s.task_id=d.task_id
   AND s.run_id=d.run_id AND s.workspace_id=d.workspace_id
LEFT JOIN task_result_candidate c
    ON c.id=b.candidate_id AND c.task_run_turn_id=s.task_run_turn_id
   AND c.task_id=s.task_id AND c.run_id=s.run_id
   AND c.thread_id=s.source_thread AND c.turn_id=s.source_turn
ORDER BY q.sequence
"#
    )
}

fn delivery_binding_values(workspace: &str, prefix: &str, event_type: &str) -> [sea_orm::Value; 3] {
    [prefix.into(), event_type.into(), workspace.into()]
}

pub async fn next_delivery_turn(
    db: &impl ConnectionTrait,
    workspace: &str,
    thread: &str,
    after_turn: &str,
    through_turn: &str,
    turn_order: i64,
    before: Option<i64>,
) -> Result<Option<DeliveryTurnRow>, DbErr> {
    DeliveryTurnRow::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        DELIVERY_TURN_SQL,
        [
            workspace.into(),
            thread.into(),
            after_turn.into(),
            through_turn.into(),
            turn_order.into(),
            before.into(),
        ],
    ))
    .one(db)
    .await
}

pub async fn delivery_events(
    db: &impl ConnectionTrait,
    turn: &str,
    thread: &str,
    workspace: &str,
    after_sequence: Option<i64>,
    through_sequence: i64,
    event_order: i64,
    before: Option<i64>,
    prefix: &str,
    event_type: &str,
) -> Result<Vec<DeliveredOutputRow>, DbErr> {
    let mut values = vec![
        turn.into(),
        thread.into(),
        workspace.into(),
        after_sequence.into(),
        through_sequence.into(),
        event_order.into(),
        before.into(),
    ];
    let sql = DELIVERY_EVENT_QUANTUM_SQL.to_owned() + &delivery_bindings_sql(values.len());
    values.extend(delivery_binding_values(workspace, prefix, event_type));
    DeliveredOutputRow::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        sql,
        values,
    ))
    .all(db)
    .await
}

pub async fn recheck_delivery_events(
    db: &impl ConnectionTrait,
    selected: &str,
    workspace: &str,
    thread: &str,
    event_order: i64,
    prefix: &str,
    event_type: &str,
) -> Result<Vec<DeliveredOutputRow>, DbErr> {
    let mut values = vec![
        selected.into(),
        workspace.into(),
        thread.into(),
        event_order.into(),
    ];
    let sql = DELIVERY_RECHECK_QUANTUM_SQL.to_owned() + &delivery_bindings_sql(values.len());
    values.extend(delivery_binding_values(workspace, prefix, event_type));
    DeliveredOutputRow::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        sql,
        values,
    ))
    .all(db)
    .await
}
