#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_entity::task_agent_spec;
use pioneer_protocol::TaskAgentSpec;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::util::{optional_typed_json_to_db, typed_json_to_db, unix_to_datetime};

pub async fn upsert_agent_spec<C: ConnectionTrait>(db: &C, spec: &TaskAgentSpec) -> Result<()> {
    task_agent_spec::Entity::insert(active_model_from_spec(spec)?)
        .on_conflict(
            OnConflict::column(task_agent_spec::Column::Id)
                .update_columns([
                    task_agent_spec::Column::TaskId,
                    task_agent_spec::Column::RunId,
                    task_agent_spec::Column::AgentRole,
                    task_agent_spec::Column::AgentNickname,
                    task_agent_spec::Column::Model,
                    task_agent_spec::Column::ModelProvider,
                    task_agent_spec::Column::PromptJson,
                    task_agent_spec::Column::ContextPolicyJson,
                    task_agent_spec::Column::ToolPolicyJson,
                    task_agent_spec::Column::PermissionCapJson,
                    task_agent_spec::Column::SecurityCapJson,
                    task_agent_spec::Column::ResultContractJson,
                    task_agent_spec::Column::ReviewPolicyJson,
                    task_agent_spec::Column::Depth,
                    task_agent_spec::Column::MaxDepth,
                    task_agent_spec::Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task agent spec")?;
    Ok(())
}

pub async fn find_latest_agent_spec_by_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Option<task_agent_spec::Model>> {
    task_agent_spec::Entity::find()
        .filter(task_agent_spec::Column::TaskId.eq(task_id.to_owned()))
        .order_by_desc(task_agent_spec::Column::UpdatedAt)
        .one(db)
        .await
        .context("failed to query latest task agent spec")
}

pub async fn find_agent_spec_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Option<task_agent_spec::Model>> {
    task_agent_spec::Entity::find()
        .filter(task_agent_spec::Column::RunId.eq(run_id.to_owned()))
        .order_by_desc(task_agent_spec::Column::UpdatedAt)
        .one(db)
        .await
        .context("failed to query task agent spec by run")
}

pub async fn list_agent_specs_by_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Vec<task_agent_spec::Model>> {
    task_agent_spec::Entity::find()
        .filter(task_agent_spec::Column::TaskId.eq(task_id.to_owned()))
        .order_by_asc(task_agent_spec::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list task agent specs")
}

pub async fn list_agent_specs_by_depth<C: ConnectionTrait>(
    db: &C,
    depth: i64,
) -> Result<Vec<task_agent_spec::Model>> {
    task_agent_spec::Entity::find()
        .filter(task_agent_spec::Column::Depth.eq(depth))
        .order_by_desc(task_agent_spec::Column::UpdatedAt)
        .all(db)
        .await
        .context("failed to list task agent specs by depth")
}

fn active_model_from_spec(spec: &TaskAgentSpec) -> Result<task_agent_spec::ActiveModel> {
    Ok(task_agent_spec::ActiveModel {
        id: Set(spec.id.clone()),
        task_id: Set(spec.task_id.clone()),
        run_id: Set(spec.run_id.clone()),
        agent_role: Set(spec.agent_role.clone()),
        agent_nickname: Set(spec.agent_nickname.clone()),
        model: Set(spec.model.clone()),
        model_provider: Set(spec.model_provider.clone()),
        prompt_json: Set(typed_json_to_db(&spec.prompt)?),
        context_policy_json: Set(optional_typed_json_to_db(&spec.context_policy)?),
        tool_policy_json: Set(optional_typed_json_to_db(&spec.tool_policy)?),
        permission_cap_json: Set(optional_typed_json_to_db(&spec.permission_cap)?),
        security_cap_json: Set(optional_typed_json_to_db(&spec.security_cap)?),
        result_contract_json: Set(optional_typed_json_to_db(&spec.result_contract)?),
        review_policy_json: Set(optional_typed_json_to_db(&spec.review_policy)?),
        depth: Set(spec.depth),
        max_depth: Set(spec.max_depth),
        created_at: Set(unix_to_datetime(spec.created_at)),
        updated_at: Set(unix_to_datetime(spec.updated_at)),
    })
}
