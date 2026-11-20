use anyhow::{Context, Result};
use pioneer_entity::{
    recovery_job, recovery_terminalization_outbox as outbox, turn, turn_cli_runtime_binding,
    turn_execution, turn_runtime_snapshot,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Asterisk, Expr, ExprTrait, Func, LockType, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, EntityTrait, FromQueryResult,
    QueryFilter, QueryOrder, QuerySelect,
};

#[derive(FromQueryResult)]
struct NativeTurnCounts {
    active_turns: i64,
    stale_running_turns: i64,
}

pub async fn active_native_turn_counts(
    db: &impl ConnectionTrait,
    stale_cutoff: DateTimeWithTimeZone,
) -> Result<(u64, u64)> {
    let turn_id = Expr::col((turn::Entity, turn::Column::Id));
    let executions = Query::select()
        .expr(Expr::val(1))
        .from(turn_execution::Entity)
        .and_where(Expr::col(turn_execution::Column::TurnId).eq(turn_id.clone()))
        .to_owned();
    let native_executions = executions
        .clone()
        .and_where(turn_execution::Column::ExecutorKind.is_in(["native_agent", "api_provider"]))
        .to_owned();
    let runtimes = Query::select()
        .expr(Expr::val(1))
        .from(turn_runtime_snapshot::Entity)
        .and_where(Expr::col(turn_runtime_snapshot::Column::TurnId).eq(turn_id.clone()))
        .to_owned();
    let cli_bindings = Query::select()
        .expr(Expr::val(1))
        .from(turn_cli_runtime_binding::Entity)
        .and_where(Expr::col(turn_cli_runtime_binding::Column::TurnId).eq(turn_id))
        .to_owned();
    let counts = turn::Entity::find()
        .select_only()
        .column_as(Expr::expr(Func::count(Expr::col(Asterisk))), "active_turns")
        .column_as(
            Expr::expr(
                Func::count(Expr::col(Asterisk)).filter(turn::Column::UpdatedAt.lte(stale_cutoff)),
            ),
            "stale_running_turns",
        )
        .filter(turn::Column::Status.eq("in_progress"))
        .filter(
            Condition::any().add(Expr::exists(native_executions)).add(
                Condition::all()
                    .add(Expr::exists(executions).not())
                    .add(turn::Column::TurnKind.eq("conversation"))
                    .add(Expr::exists(runtimes))
                    .add(Expr::exists(cli_bindings).not()),
            ),
        )
        .into_model::<NativeTurnCounts>()
        .one(db)
        .await
        .context("failed to count active native Turns")?
        .context("native Turn health query returned no row")?;
    Ok((
        u64::try_from(counts.active_turns).context("active native Turn count is invalid")?,
        u64::try_from(counts.stale_running_turns).context("stale native Turn count is invalid")?,
    ))
}

/// The outer joins preserve claims whose referenced work is missing so the
/// caller can classify them and report durable quarantine.
#[derive(FromQueryResult)]
pub struct ClaimedRecoveryFacts {
    pub recovery_job_id: String,
    pub job_id: Option<String>,
    pub job_turn_id: Option<String>,
    pub job_item_id: Option<String>,
    pub job_item_type: Option<String>,
    pub job_status: Option<String>,
    pub job_attempt_number: i64,
    pub job_error_message: String,
    pub turn_row_id: Option<String>,
    pub turn_status: Option<String>,
}

pub async fn load_claimed_recovery_facts(
    tx: &DatabaseTransaction,
    claim_token: &str,
    job_ids: impl IntoIterator<Item = String>,
) -> Result<Vec<ClaimedRecoveryFacts>> {
    let job = |column| Expr::col((recovery_job::Entity, column));
    let mut claims = outbox::Entity::find()
        .select_only()
        .column(outbox::Column::RecoveryJobId)
        .column_as(job(recovery_job::Column::Id), "job_id")
        .column_as(job(recovery_job::Column::TurnId), "job_turn_id")
        .column_as(job(recovery_job::Column::ItemId), "job_item_id")
        .column_as(job(recovery_job::Column::ItemType), "job_item_type")
        .column_as(job(recovery_job::Column::Status), "job_status")
        .column_as(
            Expr::expr(Func::greatest([
                job(recovery_job::Column::RunCount),
                Expr::val(1_i64),
            ])),
            "job_attempt_number",
        )
        .column_as(
            Expr::expr(Func::coalesce([
                job(recovery_job::Column::LastError),
                job(recovery_job::Column::Reason),
                Expr::val("recovery reached a terminal outcome"),
            ])),
            "job_error_message",
        )
        .column_as(Expr::col((turn::Entity, turn::Column::Id)), "turn_row_id")
        .column_as(
            Expr::col((turn::Entity, turn::Column::Status)),
            "turn_status",
        )
        .left_join(recovery_job::Entity)
        .join(
            sea_orm::JoinType::LeftJoin,
            outbox::Entity::belongs_to(turn::Entity)
                .from(outbox::Column::TurnId)
                .to(turn::Column::Id)
                .into(),
        )
        .filter(outbox::Column::Status.eq("delivering"))
        .filter(outbox::Column::ClaimToken.eq(claim_token))
        .filter(outbox::Column::RecoveryJobId.is_in(job_ids))
        .order_by_asc(outbox::Column::RecoveryJobId);
    // Nullable sides of these joins cannot be row-locked. Only lock claims
    // between reading their fence and classifying them.
    QuerySelect::query(&mut claims).lock_with_tables(LockType::Update, [outbox::Entity]);
    claims
        .into_model::<ClaimedRecoveryFacts>()
        .all(tx)
        .await
        .context("failed to classify claimed recovery terminalization batch")
}
