use anyhow::Result;
use pioneer_entity::{task_event, task_occurrence_contract, task_run};
use pioneer_protocol::constants::events;
use sea_orm::sea_query::extension::postgres::PgExpr;
use sea_orm::sea_query::{Expr, ExprTrait, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QuerySelect,
};

/// Prove that the queue watermark identifies an admitted webhook or its exact
/// retry. Compare the original JSON bytes without fetching either body.
pub async fn is_webhook_queue_run<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    trigger_id: &str,
    completing_run_id: &str,
    latest_number: i64,
) -> Result<bool> {
    use task_occurrence_contract::{Column as Occurrence, Entity as Contract};
    use task_run::{Column as Run, Entity as Runs};
    let run = |field| Expr::col((Runs, field));
    let source = |field| Expr::col(("source", field));
    let origin = |field| Expr::col(("origin", field));
    let contract = |field| Expr::col((Contract, field));
    let occurrence = |task: Expr, trigger: Expr| {
        let mut query = Query::select();
        query
            .expr(1)
            .from(Contract)
            .and_where(contract(Occurrence::TaskId).eq(task))
            .and_where(contract(Occurrence::TriggerId).eq(trigger));
        query
    };
    let original = occurrence(run(Run::TaskId), run(Run::TriggerId))
        .and_where(contract(Occurrence::RunId).eq(run(Run::Id)))
        .and_where(
            contract(Occurrence::OccurrenceKey).eq(Expr::val("webhook:").concat(run(Run::Id))),
        )
        .to_owned();
    let retry_number = Expr::case(
        contract(Occurrence::RunId).eq(run(Run::Id)),
        run(Run::AttemptNumber),
    )
    .case(source(Run::AttemptNumber).eq(1_i64), 0_i64)
    .finally(source(Run::AttemptNumber));
    let retry = occurrence(source(Run::TaskId), source(Run::TriggerId))
        .join_as(
            JoinType::InnerJoin,
            Runs,
            "origin",
            origin(Run::Id).eq(contract(Occurrence::OccurrenceId)),
        )
        .and_where(contract(Occurrence::RunId).is_in([source(Run::Id), run(Run::Id)]))
        .and_where(origin(Run::TaskId).eq(source(Run::TaskId)))
        .and_where(origin(Run::TriggerId).eq(source(Run::TriggerId)))
        .and_where(origin(Run::RunGroupId).eq(source(Run::RunGroupId)))
        .and_where(origin(Run::ExecutorKind).eq("agent"))
        .and_where(origin(Run::AttemptNumber).eq(1_i64))
        .and_where(origin(Run::RetryOfRunId).is_null())
        .and_where(origin(Run::ParentRunId).is_null())
        .and_where(origin(Run::RunNumber).lte(source(Run::RunNumber)))
        .and_where(
            contract(Occurrence::OccurrenceKey).eq(Expr::val("webhook:").concat(origin(Run::Id))),
        )
        .and_where(
            contract(Occurrence::ActionIdempotencyKey).eq(Expr::val("task:")
                .concat(source(Run::TaskId))
                .concat(":")
                .concat(origin(Run::Id))),
        )
        .and_where(contract(Occurrence::RetryAttempt).eq(retry_number))
        .to_owned();
    let retry_origin = Condition::all()
        .add(source(Run::Id).ne(completing_run_id))
        .add(source(Run::TaskId).eq(run(Run::TaskId)))
        .add(source(Run::TriggerId).eq(run(Run::TriggerId)))
        .add(source(Run::ExecutorKind).eq("agent"))
        .add(source(Run::Status).eq("failed"))
        .add(source(Run::CompletedAt).is_not_null())
        .add(source(Run::RunNumber).lt(run(Run::RunNumber)))
        .add(run(Run::ParentRunId).eq(source(Run::Id)))
        .add(run(Run::RunGroupId).eq(source(Run::RunGroupId)))
        .add(run(Run::AttemptNumber).eq(source(Run::AttemptNumber).add(1_i64)))
        .add(run(Run::TriggerInputJson).eq(source(Run::TriggerInputJson)))
        .add(Expr::exists(retry));
    let mut query = Runs::find()
        .select_only()
        .column(Run::Id)
        .filter(Run::TaskId.eq(task_id))
        .filter(Run::TriggerId.eq(trigger_id))
        .filter(Run::RunNumber.eq(latest_number))
        .filter(Run::ExecutorKind.eq("agent"))
        .filter(Run::TriggerInputJson.is_not_null())
        .filter(
            Condition::any()
                .add(
                    Condition::all()
                        .add(Run::RetryOfRunId.is_null())
                        .add(Run::AttemptNumber.eq(1_i64))
                        .add(Expr::exists(original)),
                )
                .add(retry_origin),
        );
    QuerySelect::query(&mut query).join_as(
        JoinType::LeftJoin,
        Runs,
        "source",
        source(Run::Id).eq(run(Run::RetryOfRunId)),
    );
    Ok(query.into_tuple::<String>().one(db).await?.is_some())
}

/// Find the last producer of the shared activity label before filtering it
/// by run or trigger. A terminalized sibling can still own that last event.
pub async fn has_webhook_sibling_activity<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    trigger_id: &str,
    completing_run_id: &str,
    status: &str,
) -> Result<bool> {
    use task_event::Column as Event;
    use task_occurrence_contract::{Column as Occurrence, Entity as Contract};
    use task_run::{Column as Run, Entity as Runs};
    let activity = match status {
        "running" => &[events::TASK_RUN_STARTED, events::TASK_REVISION_REQUESTED][..],
        "waiting_review" => &[events::TASK_RUN_ENTERED_REVIEW][..],
        _ => return Ok(false),
    };
    let event = |field| Expr::col(("latest", field));
    let run = |field| Expr::col((Runs, field));
    let latest = Query::select()
        .columns([Event::TaskId, Event::RunId, Event::EventType])
        .from(task_event::Entity)
        .and_where(Event::TaskId.eq(task_id))
        .and_where(Event::EventType.is_in([
            events::TASK_QUEUED,
            events::TASK_RUN_STARTED,
            events::TASK_RUN_ENTERED_REVIEW,
            events::TASK_REVISION_REQUESTED,
        ]))
        .order_by(Event::Sequence, sea_orm::sea_query::Order::Desc)
        .limit(1)
        .to_owned();
    let occurrence = Query::select()
        .expr(1)
        .from(Contract)
        .and_where(Expr::col((Contract, Occurrence::RunId)).eq(run(Run::Id)))
        .and_where(Expr::col((Contract, Occurrence::TaskId)).eq(run(Run::TaskId)))
        .and_where(Expr::col((Contract, Occurrence::TriggerId)).eq(run(Run::TriggerId)))
        .to_owned();
    let mut query = Runs::find()
        .select_only()
        .column(Run::Id)
        .filter(Run::TriggerId.eq(trigger_id))
        .filter(Run::Id.ne(completing_run_id))
        .filter(event(Event::EventType).is_in(activity.iter().copied()))
        .filter(Run::ExecutorKind.eq("agent"))
        .filter(Run::TriggerInputJson.is_not_null())
        .filter(Run::StartedAt.is_not_null())
        .filter(Expr::exists(occurrence));
    QuerySelect::query(&mut query).join_subquery(
        JoinType::InnerJoin,
        latest,
        "latest",
        event(Event::RunId)
            .eq(run(Run::Id))
            .and(event(Event::TaskId).eq(run(Run::TaskId))),
    );
    Ok(query.into_tuple::<String>().one(db).await?.is_some())
}
