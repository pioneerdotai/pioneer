use anyhow::{Context, Result};
use pioneer_entity::{
    agent_execution, task, task_occurrence_contract, task_run, task_run_execution,
};
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, FromQueryResult, JoinType, QueryFilter,
    QueryOrder, QuerySelect,
};

#[derive(Debug, Clone, PartialEq, Eq, FromQueryResult)]
pub struct TerminalTaskOccurrenceMismatch {
    pub task_id: String,
    pub run_id: String,
    pub execution_id: String,
    pub run_status: String,
    pub execution_status: String,
    pub occurrence_status: String,
    pub expected_occurrence_status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalTaskOccurrenceMismatchPage {
    pub mismatches: Vec<TerminalTaskOccurrenceMismatch>,
    pub scanned_occurrences: usize,
    pub next_cursor: Option<String>,
}

const TERMINAL_OCCURRENCE_SCAN_HARD_LIMIT: u64 = 512;

/// Scans a bounded primary-key page, then checks only those occurrence ids for
/// an exact terminal authority chain. The cursor advances across poison or
/// ambiguous rows, so either can be skipped without starving later records.
pub async fn scan_terminal_task_occurrence_mismatches<C: ConnectionTrait>(
    db: &C,
    after_occurrence_id: Option<&str>,
    limit: u64,
) -> Result<TerminalTaskOccurrenceMismatchPage> {
    if limit == 0 {
        return Ok(TerminalTaskOccurrenceMismatchPage {
            mismatches: Vec::new(),
            scanned_occurrences: 0,
            next_cursor: None,
        });
    }
    let scan_limit = std::cmp::min(limit, TERMINAL_OCCURRENCE_SCAN_HARD_LIMIT);
    let mut occurrence_query = task_occurrence_contract::Entity::find()
        .select_only()
        .column(task_occurrence_contract::Column::OccurrenceId)
        .order_by_asc(task_occurrence_contract::Column::OccurrenceId)
        .limit(scan_limit.saturating_add(1));
    if let Some(after_occurrence_id) = after_occurrence_id {
        occurrence_query = occurrence_query.filter(
            task_occurrence_contract::Column::OccurrenceId.gt(after_occurrence_id.to_owned()),
        );
    }
    let mut occurrence_ids = occurrence_query
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to scan bounded Task occurrence page")?;
    let page_size = usize::try_from(scan_limit).expect("hard scan limit fits usize");
    let next_cursor = if occurrence_ids.len() > page_size {
        occurrence_ids.truncate(page_size);
        occurrence_ids.last().cloned()
    } else {
        None
    };
    let scanned_occurrences = occurrence_ids.len();
    let mismatches = list_terminal_task_occurrence_mismatches_for_ids(db, &occurrence_ids).await?;
    Ok(TerminalTaskOccurrenceMismatchPage {
        mismatches,
        scanned_occurrences,
        next_cursor,
    })
}

/// Completes an explicit diagnostic scan through bounded primary-key pages.
/// Used by the explicit runtime invariant scanner. Periodic maintenance uses
/// persistent dirty locators and must never call this diagnostic scan.
pub async fn list_terminal_task_occurrence_mismatches<C: ConnectionTrait>(
    db: &C,
    limit: u64,
) -> Result<Vec<TerminalTaskOccurrenceMismatch>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut after_occurrence_id = None;
    let mut mismatches = Vec::new();
    loop {
        let page = scan_terminal_task_occurrence_mismatches(
            db,
            after_occurrence_id.as_deref(),
            TERMINAL_OCCURRENCE_SCAN_HARD_LIMIT,
        )
        .await?;
        mismatches.extend(page.mismatches);
        if u64::try_from(mismatches.len()).unwrap_or(u64::MAX) >= limit {
            mismatches.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
            break;
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        after_occurrence_id = Some(next_cursor);
    }
    Ok(mismatches)
}

async fn list_terminal_task_occurrence_mismatches_for_ids<C: ConnectionTrait>(
    db: &C,
    occurrence_ids: &[String],
) -> Result<Vec<TerminalTaskOccurrenceMismatch>> {
    if occurrence_ids.is_empty() {
        return Ok(Vec::new());
    }
    use agent_execution::{Column as Agent, Entity as Agents};
    use task::{Column as Task, Entity as Tasks};
    use task_occurrence_contract::{Column as Occurrence, Entity as Occurrences};
    use task_run::{Column as Run, Entity as Runs};
    use task_run_execution::{Column as Execution, Entity as Executions};
    let occurrence = |field| Expr::col((Occurrences, field));
    let run = |field| Expr::col((Runs, field));
    let task = |field| Expr::col((Tasks, field));
    let execution = |field| Expr::col((Executions, field));
    let agent = |field| Expr::col((Agents, field));
    let expected = Expr::case(run(Run::Status).eq("succeeded"), "delivered")
        .case(run(Run::Status).eq("cancelled"), "cancelled")
        .finally("failed");
    let agent_authority = Condition::all()
        .add(execution(Execution::ExecutorKind).eq("agent"))
        .add(occurrence(Occurrence::AgentExecutionId).eq(execution(Execution::Id)))
        .add(agent(Agent::Id).is_not_null())
        .add(agent(Agent::WorkspaceId).eq(task(Task::WorkspaceId)))
        .add(agent(Agent::ParentTaskId).eq(run(Run::TaskId)))
        .add(agent(Agent::ExecutionGeneration).eq(occurrence(Occurrence::ExecutionGeneration)))
        .add(agent(Agent::Status).eq(execution(Execution::Status)))
        .add(agent(Agent::FinishedAt).is_not_null())
        .add(
            occurrence(Occurrence::WorkGraphRootExecutionId)
                .eq(agent(Agent::WorkGraphRootExecutionId)),
        )
        .add(
            occurrence(Occurrence::RootResourceScopeId).eq(agent(Agent::WorkGraphRootExecutionId)),
        );
    let system_authority = Condition::all()
        .add(execution(Execution::ExecutorKind).eq("system"))
        .add(occurrence(Occurrence::AgentExecutionId).is_null())
        .add(occurrence(Occurrence::WorkGraphRootExecutionId).is_null())
        .add(occurrence(Occurrence::RootResourceScopeId).is_null());
    let mut select = Occurrences::find()
        .select_only()
        .column(Occurrence::TaskId)
        .column(Occurrence::RunId)
        .expr_as(execution(Execution::Id), "execution_id")
        .expr_as(run(Run::Status), "run_status")
        .expr_as(execution(Execution::Status), "execution_status")
        .expr_as(occurrence(Occurrence::Status), "occurrence_status")
        .expr_as(expected.clone(), "expected_occurrence_status")
        .filter(Occurrence::OccurrenceId.is_in(occurrence_ids.iter().cloned()))
        .filter(occurrence(Occurrence::TaskId).eq(run(Run::TaskId)))
        .filter(task(Task::ExecutorKind).eq(run(Run::ExecutorKind)))
        .filter(execution(Execution::TaskId).eq(run(Run::TaskId)))
        .filter(execution(Execution::ExecutorKind).eq(run(Run::ExecutorKind)))
        .filter(run(Run::CompletedAt).is_not_null())
        .filter(execution(Execution::CompletedAt).is_not_null())
        .filter(run(Run::Status).is_in([
            "succeeded",
            "failed",
            "blocked",
            "timed_out",
            "cancelled",
        ]))
        .filter(execution(Execution::Status).eq(run(Run::Status)))
        .filter(Condition::any().add(agent_authority).add(system_authority))
        .filter(occurrence(Occurrence::Status).ne(expected))
        .order_by_asc(Occurrence::OccurrenceId);
    QuerySelect::query(&mut select)
        .join(
            JoinType::InnerJoin,
            Runs,
            run(Run::Id).eq(occurrence(Occurrence::RunId)),
        )
        .join(
            JoinType::InnerJoin,
            Tasks,
            task(Task::Id).eq(run(Run::TaskId)),
        )
        .join(
            JoinType::InnerJoin,
            Executions,
            execution(Execution::TaskRunId).eq(run(Run::Id)),
        )
        .join(
            JoinType::LeftJoin,
            Agents,
            agent(Agent::Id).eq(execution(Execution::Id)),
        );
    select
        .into_model::<TerminalTaskOccurrenceMismatch>()
        .all(db)
        .await
        .context("failed to list terminal Task occurrence mismatches")
}
