//! Persisted exact launch grants and their authenticated historical ceilings.
use anyhow::{Context, Result, bail};
use pioneer_entity::agent_execution_grant;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionGrantInput {
    pub id: String,
    pub execution_id: String,
    pub parent_execution_id: Option<String>,
    pub child_identity_id: String,
    pub grant_fingerprint: String,
    pub grant_json: String,
    pub now: DateTimeWithTimeZone,
}

pub async fn insert_agent_execution_grant<C: ConnectionTrait>(
    db: &C,
    input: &AgentExecutionGrantInput,
) -> Result<agent_execution_grant::Model> {
    let (identity, profile, child_launch_grant) =
        parse_agent_execution_launch_grant(input.grant_json.as_str())?;
    child_launch_grant.validate_for_execution().map_err(|_| {
        anyhow::anyhow!("historical child launch ceiling cannot authorize a new execution")
    })?;
    if input.grant_fingerprint != agent_execution_grant_fingerprint(input.grant_json.as_str())?
        || !agent_execution_launch_grant_matches_child_identity(
            &identity,
            &profile,
            &child_launch_grant,
            input.child_identity_id.as_str(),
        )
    {
        bail!("agent execution grant identity/profile is outside its child launch ceiling");
    }
    agent_execution_grant::Entity::insert(agent_execution_grant::ActiveModel {
        id: Set(input.id.clone()),
        execution_id: Set(input.execution_id.clone()),
        parent_execution_id: Set(input.parent_execution_id.clone()),
        child_identity_id: Set(input.child_identity_id.clone()),
        grant_fingerprint: Set(input.grant_fingerprint.clone()),
        grant_json: Set(input.grant_json.clone()),
        created_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::column(agent_execution_grant::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist agent execution grant")?;
    let grant = agent_execution_grant::Entity::find_by_id(input.id.clone())
        .one(db)
        .await
        .context("failed to reload agent execution grant")?
        .context("agent execution grant disappeared after idempotent insert")?;
    let grant = upgrade_agent_execution_grant_model_to_current(grant)
        .context("failed to validate reloaded agent execution grant")?;
    if grant.execution_id != input.execution_id
        || grant.parent_execution_id != input.parent_execution_id
        || grant.child_identity_id != input.child_identity_id
        || grant.grant_fingerprint != input.grant_fingerprint
        || grant.grant_json != input.grant_json
    {
        bail!("agent execution grant id was reused with different immutable facts");
    }
    Ok(grant)
}

pub fn agent_execution_grant_fingerprint(grant_json: &str) -> Result<String> {
    let value: serde_json::Value =
        serde_json::from_str(grant_json).context("agent execution grant JSON is invalid")?;
    let canonical =
        serde_json::to_vec(&value).context("agent execution grant could not be canonicalized")?;
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:execution-grant:v1\0");
    digest.update(canonical);
    Ok(hex::encode(digest.finalize()))
}

/// Verifies the original outer and nested snapshots. Neither the bytes nor
/// either fingerprint may be rewritten by a reader.
pub fn upgrade_agent_execution_grant_model_to_current(
    grant: agent_execution_grant::Model,
) -> Result<agent_execution_grant::Model> {
    let stored_fingerprint = agent_execution_grant_fingerprint(grant.grant_json.as_str())?;
    if grant.grant_fingerprint != stored_fingerprint {
        bail!("agent execution grant fingerprint does not match its stored JSON");
    }
    let (identity, profile, child_launch_grant) =
        parse_agent_execution_launch_grant(grant.grant_json.as_str())?;
    if !agent_execution_launch_grant_matches_child_identity(
        &identity,
        &profile,
        &child_launch_grant,
        grant.child_identity_id.as_str(),
    ) {
        bail!("agent execution grant identity/profile is outside its child launch ceiling");
    }
    Ok(grant)
}

fn agent_execution_launch_grant_matches_child_identity(
    identity: &pioneer_protocol::AgentIdentityProjection,
    profile: &pioneer_protocol::AgentExecutionProfileProjection,
    child_launch_grant: &pioneer_protocol::ChildAgentLaunchGrantSet,
    child_identity_id: &str,
) -> bool {
    identity.id.as_str() == child_identity_id
        && child_launch_grant
            .validate_selected_profile(identity, profile)
            .is_ok()
}

pub fn parse_agent_execution_launch_grant(
    grant_json: &str,
) -> Result<(
    pioneer_protocol::AgentIdentityProjection,
    pioneer_protocol::AgentExecutionProfileProjection,
    pioneer_protocol::ChildAgentLaunchGrantSet,
)> {
    let grant_json: serde_json::Value =
        serde_json::from_str(grant_json).context("agent execution grant JSON is invalid")?;
    let identity: pioneer_protocol::AgentIdentityProjection = serde_json::from_value(
        grant_json
            .get("identity")
            .cloned()
            .context("agent execution grant has no exact identity")?,
    )
    .context("agent execution grant identity is invalid")?;
    let profile: pioneer_protocol::AgentExecutionProfileProjection = serde_json::from_value(
        grant_json
            .get("profile")
            .cloned()
            .context("agent execution grant has no exact profile")?,
    )
    .context("agent execution grant profile is invalid")?;
    let child_launch_grant: pioneer_protocol::ChildAgentLaunchGrantSet = serde_json::from_value(
        grant_json
            .get("child_launch_grant")
            .cloned()
            .context("agent execution grant has no immutable child launch ceiling")?,
    )
    .context("agent execution child launch ceiling is invalid")?;
    child_launch_grant.validate().map_err(|error| {
        anyhow::anyhow!("agent execution child launch ceiling is invalid: {error:?}")
    })?;
    Ok((identity, profile, child_launch_grant))
}

pub async fn load_agent_execution_grant<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
) -> Result<Option<agent_execution_grant::Model>> {
    agent_execution_grant::Entity::find()
        .filter(agent_execution_grant::Column::ExecutionId.eq(execution_id.to_owned()))
        .one(db)
        .await
        .context("failed to load agent execution grant")?
        .map(upgrade_agent_execution_grant_model_to_current)
        .transpose()
        .context("failed to validate loaded agent execution grant")
}
