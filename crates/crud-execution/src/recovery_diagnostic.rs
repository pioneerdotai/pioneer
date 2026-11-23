use pioneer_entity::{recovery_job as job, turn};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, DbErr, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait, Statement,
};

type DiagnosticRow = (Option<String>, Option<String>);

pub async fn failed(
    db: &impl ConnectionTrait,
    turn_id: &str,
) -> Result<Option<DiagnosticRow>, DbErr> {
    let mut query = job::Entity::find()
        .select_only()
        .column(job::Column::Diagnostic)
        .column_as(Expr::col((turn::Entity, turn::Column::Error)), "error")
        .filter(job::Column::TurnId.eq(turn_id))
        .filter(turn::Column::Status.eq("failed"))
        .filter(job::Column::Status.is_in(["failed", "exhausted"]))
        .order_by_desc(job::Column::UpdatedAt)
        .order_by_desc(job::Column::Id)
        .limit(1);
    QueryTrait::query(&mut query).join(
        JoinType::InnerJoin,
        turn::Entity,
        Expr::col((turn::Entity, turn::Column::Id)).equals((job::Entity, job::Column::TurnId)),
    );
    query.into_tuple::<DiagnosticRow>().one(db).await
}

pub async fn blocked(
    db: &impl ConnectionTrait,
    turn_id: &str,
    job_id: Option<&str>,
    source: Option<(&str, &str, i64)>,
) -> Result<Option<DiagnosticRow>, DbErr> {
    // This projection has to join the latest transition on the correlated
    // stream sequence. The generated entities expose no such relation.
    let row = db.query_one_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT j.diagnostic, t.error FROM turn t
         JOIN turn_event e ON e.turn_id = t.id AND e.sequence =
           (SELECT MAX(sequence) FROM turn_event WHERE turn_id = t.id
            AND event_type IN ('turn/started', 'turn/blocked', 'turn/failed', 'turn/completed'))
         JOIN recovery_job j ON j.id = $1
         JOIN recovery_terminalization_outbox o ON o.recovery_job_id = j.id
         WHERE t.id = $2 AND e.id = $3 AND e.portable_identity = $4 AND e.portable_revision = $5
         AND t.status = 'blocked' AND e.event_type = 'turn/blocked'
         AND j.turn_id = t.id AND j.status = 'blocked' AND o.status = 'delivered'
         AND o.turn_id = j.turn_id AND o.item_id = j.item_id AND o.item_type = j.item_type
         AND o.recovery_status = j.status AND o.attempt_number = GREATEST(j.run_count, 1)
         AND o.error_message = COALESCE(j.last_error, j.reason, 'recovery reached a terminal outcome')",
        [
            job_id.map(str::to_owned).into(),
            turn_id.into(),
            source.map(|(event, _, _)| event.to_owned()).into(),
            source.map(|(_, identity, _)| identity.to_owned()).into(),
            source.map(|(_, _, revision)| revision).into(),
        ],
    )).await?;
    row.map(|row| Ok((row.try_get("", "diagnostic")?, row.try_get("", "error")?)))
        .transpose()
}
