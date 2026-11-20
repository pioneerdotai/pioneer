use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use pioneer_entity::{
    agent_execution, agent_identity, task_run_execution, task_run_turn, turn_cli_runtime_binding,
};
use pioneer_protocol::CLIAgentRuntimeKind;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QueryOrder, QuerySelect,
};

/// CLI attribution follows the exact execution, run turn, and binding.
pub async fn cli_author_kinds<C: ConnectionTrait>(
    db: &C,
    execution_ids: &[String],
) -> Result<BTreeMap<String, CLIAgentRuntimeKind>> {
    if execution_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    use agent_execution::{Column as Execution, Entity as Executions};
    use agent_identity::{Column as Identity, Entity as Identities};
    use task_run_execution::{Column as Run, Entity as Runs};
    use task_run_turn::{Column as Turn, Entity as Turns};
    use turn_cli_runtime_binding::{Column as Binding, Entity as Bindings};
    let execution = |field| Expr::col((Executions, field));
    let identity = |field| Expr::col((Identities, field));
    let run = |field| Expr::col((Runs, field));
    let turn = |field| Expr::col((Turns, field));
    let binding = |field| Expr::col((Bindings, field));
    let mut query = Executions::find()
        .select_only()
        .column(Execution::Id)
        .expr_as(binding(Binding::RuntimeKind), "runtime_kind")
        .distinct()
        .filter(Execution::Id.is_in(execution_ids.iter().cloned()))
        .filter(identity(Identity::SourceKind).eq("cli_runtime_instance"))
        .filter(binding(Binding::WorkspaceId).eq(execution(Execution::WorkspaceId)))
        .filter(binding(Binding::RuntimeId).eq(identity(Identity::SourceId)))
        .order_by_asc(Execution::Id)
        .order_by_asc(binding(Binding::RuntimeKind))
        .limit(
            u64::try_from(execution_ids.len())?
                .saturating_mul(3)
                .saturating_add(1),
        );
    QuerySelect::query(&mut query)
        .join(
            JoinType::InnerJoin,
            Identities,
            identity(Identity::Id).eq(execution(Execution::AgentIdentityId)),
        )
        .join(
            JoinType::InnerJoin,
            Runs,
            run(Run::Id)
                .eq(execution(Execution::Id))
                .and(run(Run::TaskId).eq(execution(Execution::ParentTaskId))),
        )
        .join(
            JoinType::InnerJoin,
            Turns,
            turn(Turn::RunId)
                .eq(run(Run::TaskRunId))
                .and(turn(Turn::ExecutionId).eq(run(Run::Id))),
        )
        .join(
            JoinType::InnerJoin,
            Bindings,
            binding(Binding::TurnId).eq(turn(Turn::TurnId)),
        );
    let mut runtime_kinds = BTreeMap::new();
    for (execution_id, kind) in query.into_tuple::<(String, String)>().all(db).await? {
        let kind = serde_json::from_value::<CLIAgentRuntimeKind>(serde_json::Value::String(kind))
            .context("invalid CLI provider kind in Agent execution binding")?;
        ensure!(
            runtime_kinds
                .insert(execution_id, kind)
                .is_none_or(|old| old == kind),
            "Agent execution has conflicting CLI provider identities"
        );
    }
    Ok(runtime_kinds)
}
