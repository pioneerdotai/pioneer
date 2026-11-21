use anyhow::{Context, Result};
use pioneer_entity::{agent_execution_resource_state, agent_turn_response_execution};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

pub async fn load_agent_execution_resource_state<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
) -> Result<Option<agent_execution_resource_state::Model>> {
    agent_execution_resource_state::Entity::find()
        .filter(agent_execution_resource_state::Column::ExecutionId.eq(execution_id.to_owned()))
        .order_by_desc(agent_execution_resource_state::Column::AttemptGeneration)
        .one(db)
        .await
        .context("failed to load agent execution resource state")
}

pub async fn load_agent_turn_response<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<agent_turn_response_execution::Model>> {
    agent_turn_response_execution::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to load responding AgentExecution")
}

/// Serialize continuation replay before reading the current binding and attempt.
pub async fn lock_cli_turn_binding<C: ConnectionTrait>(
    transaction: &C,
    turn_id: &str,
) -> Result<()> {
    use pioneer_entity::turn_cli_runtime_binding as binding;

    binding::Entity::find_by_id(turn_id.to_owned())
        .select_only()
        .column(binding::Column::TurnId)
        .lock(sea_orm::sea_query::LockType::NoKeyUpdate)
        .into_tuple::<String>()
        .one(transaction)
        .await?
        .context("task continuation binding disappeared")?;
    Ok(())
}
