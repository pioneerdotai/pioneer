//! Persist and validate exact delegation routes and their lifecycle events.
use crate::agent_identity::AGENT_PROJECTION_BATCH_LIMIT;
use crate::canonical_id::canonical_agent_id;
use anyhow::{Context, Result, anyhow, bail};
use pioneer_crud_access::policy_generation::current_policy_generation_on;
use pioneer_entity::{
    agent_delegation_route, agent_delegation_route_event, agent_execution, agent_identity, thread,
    thread_lineage,
};
use pioneer_protocol::{
    AgentDelegationRouteId, AgentDelegationRouteProjection, AgentExecutionId,
    AgentExecutionProfileId, AgentIdentityId, AgentRouteAction, AgentRouteDisclosurePolicy,
    AgentRouteKind, AgentRouteStatus, PersistedActorRef,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, DbBackend, EntityTrait,
    ExprTrait, QueryFilter, QueryOrder, QuerySelect, Set, Statement,
};
use std::collections::BTreeMap;

pub const AGENT_ROUTE_GRAPH_MAX_EDGES: usize = 2_048;
const AGENT_ROUTE_EXPIRY_BATCH_SIZE: u64 = 512;
const ROUTE_GRAPH_LOCK_SEED: i64 = 0x7069_6f6e_726f_7574;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDelegationRouteInput {
    pub id: String,
    pub source_execution_id: String,
    pub destination_thread_id: String,
    pub source_capsule_id: Option<String>,
    pub destination_capsule_id: Option<String>,
    pub source_workspace_id: Option<String>,
    pub destination_workspace_id: Option<String>,
    pub source_gateway_id: Option<String>,
    pub destination_gateway_id: Option<String>,
    pub source_identity_id: Option<String>,
    pub destination_agent_identity_id: Option<String>,
    pub destination_profile_id: Option<String>,
    pub home_capsule_id: Option<String>,
    pub route_kind: String,
    pub authority_actor_json: String,
    pub authority_fingerprint: String,
    pub allowed_actions_json: String,
    pub disclosure_json: String,
    pub route_generation: i64,
    pub source_policy_generation: i64,
    pub destination_policy_generation: i64,
    pub hop_count: i64,
    pub max_hops: i64,
    pub return_route_id: Option<String>,
    pub grant_fingerprint: String,
    pub status: String,
    pub updated_at: DateTimeWithTimeZone,
    pub expires_at: Option<DateTimeWithTimeZone>,
    pub now: DateTimeWithTimeZone,
}

pub async fn insert_agent_delegation_route(
    db: &DatabaseTransaction,
    input: &AgentDelegationRouteInput,
) -> Result<agent_delegation_route::Model> {
    if !matches!(input.status.as_str(), "prepared" | "active") {
        bail!("agent delegation route can only be created prepared or active");
    }
    validate_agent_delegation_route_input(db, input).await?;
    agent_delegation_route::Entity::insert(agent_delegation_route::ActiveModel {
        id: Set(input.id.clone()),
        source_execution_id: Set(input.source_execution_id.clone()),
        destination_thread_id: Set(input.destination_thread_id.clone()),
        source_capsule_id: Set(input.source_capsule_id.clone()),
        destination_capsule_id: Set(input.destination_capsule_id.clone()),
        source_workspace_id: Set(input.source_workspace_id.clone()),
        destination_workspace_id: Set(input.destination_workspace_id.clone()),
        source_gateway_id: Set(input.source_gateway_id.clone()),
        destination_gateway_id: Set(input.destination_gateway_id.clone()),
        source_identity_id: Set(input.source_identity_id.clone()),
        destination_agent_identity_id: Set(input.destination_agent_identity_id.clone()),
        destination_profile_id: Set(input.destination_profile_id.clone()),
        home_capsule_id: Set(input.home_capsule_id.clone()),
        route_kind: Set(input.route_kind.clone()),
        authority_actor_json: Set(Some(input.authority_actor_json.clone())),
        authority_fingerprint: Set(Some(input.authority_fingerprint.clone())),
        allowed_actions_json: Set(input.allowed_actions_json.clone()),
        disclosure_json: Set(input.disclosure_json.clone()),
        route_generation: Set(input.route_generation),
        source_policy_generation: Set(input.source_policy_generation),
        destination_policy_generation: Set(input.destination_policy_generation),
        hop_count: Set(input.hop_count),
        max_hops: Set(input.max_hops),
        return_route_id: Set(input.return_route_id.clone()),
        grant_fingerprint: Set(input.grant_fingerprint.clone()),
        status: Set(input.status.clone()),
        created_at: Set(input.now),
        updated_at: Set(input.updated_at),
        expires_at: Set(input.expires_at),
    })
    .on_conflict(
        OnConflict::column(agent_delegation_route::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist agent delegation route")?;
    let route = agent_delegation_route::Entity::find_by_id(input.id.clone())
        .one(db)
        .await
        .context("failed to reload agent delegation route")?
        .context("agent delegation route disappeared after idempotent insert")?;
    if route.source_execution_id != input.source_execution_id
        || route.destination_thread_id != input.destination_thread_id
        || route.source_capsule_id != input.source_capsule_id
        || route.destination_capsule_id != input.destination_capsule_id
        || route.source_workspace_id != input.source_workspace_id
        || route.destination_workspace_id != input.destination_workspace_id
        || route.source_gateway_id != input.source_gateway_id
        || route.destination_gateway_id != input.destination_gateway_id
        || route.source_identity_id != input.source_identity_id
        || route.destination_agent_identity_id != input.destination_agent_identity_id
        || route.destination_profile_id != input.destination_profile_id
        || route.home_capsule_id != input.home_capsule_id
        || route.route_kind != input.route_kind
        || route.authority_actor_json.as_deref() != Some(input.authority_actor_json.as_str())
        || route.authority_fingerprint.as_deref() != Some(input.authority_fingerprint.as_str())
        || route.allowed_actions_json != input.allowed_actions_json
        || route.disclosure_json != input.disclosure_json
        || route.route_generation != input.route_generation
        || route.source_policy_generation != input.source_policy_generation
        || route.destination_policy_generation != input.destination_policy_generation
        || route.hop_count != input.hop_count
        || route.max_hops != input.max_hops
        || route.return_route_id != input.return_route_id
        || route.grant_fingerprint != input.grant_fingerprint
    {
        bail!("agent delegation route id was reused with different route facts");
    }
    append_agent_route_event(
        db,
        route.id.as_str(),
        "created",
        input.route_generation,
        input.authority_actor_json.as_str(),
        input.authority_fingerprint.as_str(),
        input.now.clone(),
    )
    .await?;
    Ok(route)
}

async fn append_agent_route_event(
    db: &DatabaseTransaction,
    route_id: &str,
    event_kind: &str,
    route_generation: i64,
    authority_actor_json: &str,
    authority_fingerprint: &str,
    occurred_at: DateTimeWithTimeZone,
) -> Result<agent_delegation_route_event::Model> {
    if !matches!(event_kind, "created" | "revoked" | "expired") {
        bail!("agent route lifecycle event kind is invalid");
    }
    let _: PersistedActorRef = serde_json::from_str(authority_actor_json)
        .context("agent route lifecycle actor is invalid")?;
    if route_generation < 1 || authority_fingerprint.trim().is_empty() {
        bail!("agent route lifecycle authority is invalid");
    }
    let id = canonical_agent_id('V', &format!("route-event\0{route_id}\0{route_generation}"));
    agent_delegation_route_event::Entity::insert(agent_delegation_route_event::ActiveModel {
        id: Set(id.clone()),
        route_id: Set(route_id.to_owned()),
        event_kind: Set(event_kind.to_owned()),
        route_generation: Set(route_generation),
        authority_actor_json: Set(authority_actor_json.to_owned()),
        authority_fingerprint: Set(authority_fingerprint.to_owned()),
        occurred_at: Set(occurred_at),
    })
    .on_conflict(
        OnConflict::column(agent_delegation_route_event::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to append Agent route lifecycle event")?;
    let event = agent_delegation_route_event::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload Agent route lifecycle event")?
        .context("Agent route lifecycle event disappeared after append")?;
    if event.route_id != route_id
        || event.event_kind != event_kind
        || event.route_generation != route_generation
        || event.authority_actor_json != authority_actor_json
        || event.authority_fingerprint != authority_fingerprint
    {
        bail!("Agent route lifecycle event id was reused with different facts");
    }
    Ok(event)
}

async fn validate_agent_delegation_route_input(
    db: &DatabaseTransaction,
    input: &AgentDelegationRouteInput,
) -> Result<()> {
    let kind = match input.route_kind.as_str() {
        "execution_bound" => AgentRouteKind::ExecutionBound,
        "identity_bound" => AgentRouteKind::IdentityBound,
        "capsule_bound" => AgentRouteKind::CapsuleBound,
        _ => bail!("agent delegation route has an invalid binding kind"),
    };
    let authority_actor: PersistedActorRef = serde_json::from_str(&input.authority_actor_json)
        .context("agent delegation route authority actor is invalid")?;
    if input.authority_fingerprint.trim().is_empty() {
        bail!("agent delegation route authority fingerprint is required");
    }
    let status = match input.status.as_str() {
        "prepared" => AgentRouteStatus::Prepared,
        "active" => AgentRouteStatus::Active,
        _ => bail!("agent delegation route has an invalid creation status"),
    };
    if input.route_generation != 1 {
        bail!("agent delegation route must begin at generation one");
    }
    let source_capsule_id = input
        .source_capsule_id
        .clone()
        .or_else(|| input.home_capsule_id.clone())
        .context("agent delegation route has no source capsule")?;
    let destination_capsule_id = input
        .destination_capsule_id
        .clone()
        .context("agent delegation route has no destination capsule")?;
    let source_workspace_id = input
        .source_workspace_id
        .clone()
        .context("agent delegation route has no source workspace")?;
    let destination_workspace_id = input
        .destination_workspace_id
        .clone()
        .context("agent delegation route has no destination workspace")?;
    let source_gateway_id = input
        .source_gateway_id
        .clone()
        .context("agent delegation route has no source Gateway")?;
    let destination_gateway_id = input
        .destination_gateway_id
        .clone()
        .context("agent delegation route has no destination Gateway")?;
    let source_identity_id = input
        .source_identity_id
        .as_deref()
        .context("agent delegation route has no source identity")?;
    if let Some(profile_id) = input.destination_profile_id.as_deref() {
        AgentExecutionProfileId::new(profile_id.to_owned())
            .context("agent delegation route destination profile id is invalid")?;
    }
    let projection = AgentDelegationRouteProjection {
        id: AgentDelegationRouteId::new(input.id.clone())
            .context("agent delegation route id is invalid")?,
        source_execution_id: AgentExecutionId::new(input.source_execution_id.clone())
            .context("agent delegation route source execution id is invalid")?,
        source_capsule_id: source_capsule_id.clone(),
        destination_thread_id: input.destination_thread_id.clone(),
        destination_capsule_id: destination_capsule_id.clone(),
        kind,
        status,
        allowed_actions: serde_json::from_str(input.allowed_actions_json.as_str())
            .context("agent delegation route action subset is invalid")?,
        disclosure: serde_json::from_str(input.disclosure_json.as_str())
            .context("agent delegation route disclosure policy is invalid")?,
        source_agent_identity_id: AgentIdentityId::new(source_identity_id.to_owned())
            .context("agent delegation route source identity id is invalid")?,
        destination_agent_identity_id: input
            .destination_agent_identity_id
            .as_deref()
            .map(|id| AgentIdentityId::new(id.to_owned()))
            .transpose()
            .context("agent delegation route destination identity id is invalid")?,
        destination_profile_id: input.destination_profile_id.clone(),
        source_workspace_id: source_workspace_id.clone(),
        destination_workspace_id: destination_workspace_id.clone(),
        source_gateway_id: source_gateway_id.clone(),
        destination_gateway_id: destination_gateway_id.clone(),
        generation: u64::try_from(input.route_generation)
            .context("agent delegation route generation is invalid")?,
        source_policy_generation: u64::try_from(input.source_policy_generation)
            .context("agent delegation route source policy generation is invalid")?,
        destination_policy_generation: u64::try_from(input.destination_policy_generation)
            .context("agent delegation route destination policy generation is invalid")?,
        hop_count: u16::try_from(input.hop_count)
            .context("agent delegation route hop count is invalid")?,
        max_hops: u16::try_from(input.max_hops)
            .context("agent delegation route max hops is invalid")?,
        grant_fingerprint: input.grant_fingerprint.clone(),
        expires_at: input
            .expires_at
            .as_ref()
            .map(|value| value.timestamp_millis()),
        return_route_id: input
            .return_route_id
            .as_deref()
            .map(|id| AgentDelegationRouteId::new(id.to_owned()))
            .transpose()
            .context("agent delegation route return route id is invalid")?,
    };
    projection
        .validate(Some(input.now.timestamp_millis()))
        .map_err(|error| anyhow!("agent delegation route is invalid: {error:?}"))?;
    if projection.max_hops > 8 {
        bail!("agent delegation route exceeds the server hop boundary");
    }
    let current_policy_generation = current_policy_generation_on(db).await?;
    let current_policy_generation = i64::try_from(current_policy_generation.get())
        .context("current policy generation exceeds persistence bounds")?;
    if input.source_policy_generation != current_policy_generation
        || input.destination_policy_generation != current_policy_generation
    {
        bail!("agent delegation route policy generation is stale");
    }
    if source_capsule_id == destination_capsule_id {
        bail!("same-capsule Agent actions must not manufacture route authority");
    }
    if source_capsule_id != destination_capsule_id
        && input.expires_at.is_none()
        && !matches!(
            (&kind, &authority_actor),
            (
                AgentRouteKind::ExecutionBound | AgentRouteKind::CapsuleBound,
                PersistedActorRef::AgentExecution(actor_execution_id)
            ) if actor_execution_id.as_str() == input.source_execution_id
        )
    {
        bail!(
            "cross-capsule route without a timestamp boundary must be server-owned and execution/capsule-bound"
        );
    }
    let source_execution = agent_execution::Entity::find_by_id(input.source_execution_id.clone())
        .one(db)
        .await
        .context("failed to validate route source execution")?
        .context("agent delegation route source execution is missing")?;
    if source_execution.workspace_id != source_workspace_id
        || source_execution.agent_identity_id != source_identity_id
        || source_execution.home_root_thread_id != source_capsule_id
        || matches!(
            source_execution.status.as_str(),
            "completed" | "succeeded" | "failed" | "blocked" | "cancelled" | "timed_out"
        )
        || source_execution.finished_at.is_some()
    {
        bail!("agent delegation route source binding is inconsistent");
    }
    let source_identity = agent_identity::Entity::find_by_id(source_identity_id.to_owned())
        .one(db)
        .await
        .context("failed to validate route source identity")?
        .context("agent delegation route source identity is missing")?;
    if source_identity.workspace_id != source_workspace_id || source_identity.status != "active" {
        bail!("agent delegation route source identity is unavailable");
    }
    if let Some(destination_identity_id) = input.destination_agent_identity_id.as_deref() {
        let destination_identity =
            agent_identity::Entity::find_by_id(destination_identity_id.to_owned())
                .one(db)
                .await
                .context("failed to validate route destination identity")?
                .context("agent delegation route destination identity is missing")?;
        if destination_identity.workspace_id != destination_workspace_id
            || destination_identity.status != "active"
        {
            bail!("agent delegation route destination identity is unavailable");
        }
    }
    match authority_actor {
        PersistedActorRef::AgentExecution(actor_execution_id)
            if actor_execution_id.as_str() != input.source_execution_id =>
        {
            bail!("automatic agent route authority differs from its source execution");
        }
        PersistedActorRef::System => {
            bail!("System cannot create an agent delegation route");
        }
        PersistedActorRef::Principal(_) | PersistedActorRef::AgentExecution(_) => {}
    }
    let destination = thread::Entity::find_by_id(input.destination_thread_id.clone())
        .one(db)
        .await
        .context("failed to validate route destination thread")?
        .context("agent delegation route destination thread is missing")?;
    if destination.workspace_id != destination_workspace_id {
        bail!("agent delegation route destination workspace is inconsistent");
    }
    if source_capsule_id != destination_capsule_id {
        let persisted_destination_capsule = if destination.access_class == "internal" {
            thread_lineage::Entity::find_by_id(destination.id.clone())
                .one(db)
                .await
                .context("failed to validate route destination lineage")?
                .context("cross-capsule route destination has no internal lineage")?
                .root_thread_id
        } else {
            destination.id.clone()
        };
        if persisted_destination_capsule != destination_capsule_id {
            bail!("agent delegation route destination capsule is inconsistent");
        }
    }
    if let Some(return_route_id) = input.return_route_id.as_deref() {
        let return_route = agent_delegation_route::Entity::find_by_id(return_route_id.to_owned())
            .one(db)
            .await
            .context("failed to validate result return route")?
            .context("agent delegation route return edge is missing")?;
        let return_projection = agent_delegation_route_projection(&return_route)
            .context("agent delegation route return edge is invalid")?;
        if return_route.source_policy_generation != current_policy_generation
            || return_route.destination_policy_generation != current_policy_generation
            || !projection
                .can_return_result_via(&return_projection, Some(input.now.timestamp_millis()))
        {
            bail!("agent delegation route return edge does not authorize exact result delivery");
        }
    }
    // Routes sharing this graph must validate against the last committed
    // edge set. Distinct local workspaces and Gateways remain independent.
    // SeaORM has no PostgreSQL transaction advisory lock operation.
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pg_advisory_xact_lock(hashtextextended(json_build_array($1::text, $2::text)::text, $3))",
        [
            source_workspace_id.as_str().into(),
            source_gateway_id.as_str().into(),
            ROUTE_GRAPH_LOCK_SEED.into(),
        ],
    ))
    .await
    .context("lock the Agent delegation route graph")?;
    let existing_routes = agent_delegation_route::Entity::find()
        .filter(agent_delegation_route::Column::Id.ne(input.id.clone()))
        .filter(agent_delegation_route::Column::Status.is_in(["prepared", "active"]))
        .filter(agent_delegation_route::Column::SourceWorkspaceId.eq(source_workspace_id.clone()))
        .filter(
            agent_delegation_route::Column::DestinationWorkspaceId
                .eq(destination_workspace_id.clone()),
        )
        .filter(agent_delegation_route::Column::SourceGatewayId.eq(source_gateway_id.clone()))
        .filter(
            agent_delegation_route::Column::DestinationGatewayId.eq(destination_gateway_id.clone()),
        )
        .filter(
            agent_delegation_route::Column::ExpiresAt
                .is_null()
                .or(agent_delegation_route::Column::ExpiresAt.gt(input.now.clone())),
        )
        .limit((AGENT_ROUTE_GRAPH_MAX_EDGES as u64).saturating_add(1))
        .all(db)
        .await
        .context("failed to load Agent route graph")?;
    if existing_routes.len() > AGENT_ROUTE_GRAPH_MAX_EDGES {
        bail!("Agent route graph exceeds the bounded edge limit");
    }
    let mut edges = Vec::with_capacity(existing_routes.len().saturating_add(1));
    for route in existing_routes {
        let actions =
            serde_json::from_str::<Vec<AgentRouteAction>>(route.allowed_actions_json.as_str())
                .with_context(|| {
                    format!("Agent route `{}` has an invalid action subset", route.id)
                })?;
        if actions == [AgentRouteAction::DeliverResult] {
            continue;
        }
        let source = route
            .source_capsule_id
            .context("Agent route graph edge has no source capsule")?;
        let destination = route
            .destination_capsule_id
            .context("Agent route graph edge has no destination capsule")?;
        if source != destination {
            edges.push((source, destination));
        }
    }
    if source_capsule_id != destination_capsule_id
        && projection.allowed_actions != [AgentRouteAction::DeliverResult]
    {
        edges.push((source_capsule_id, destination_capsule_id));
    }
    pioneer_protocol::validate_agent_route_graph(&edges, AGENT_ROUTE_GRAPH_MAX_EDGES, 8)
        .map_err(|error| anyhow!("agent delegation route graph is invalid: {error:?}"))?;
    Ok(())
}

pub async fn load_agent_delegation_route<C: ConnectionTrait>(
    db: &C,
    route_id: &str,
) -> Result<Option<agent_delegation_route::Model>> {
    agent_delegation_route::Entity::find_by_id(route_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent delegation route")
}

pub fn agent_delegation_route_projection(
    route: &agent_delegation_route::Model,
) -> Result<AgentDelegationRouteProjection> {
    let kind = match route.route_kind.as_str() {
        "execution_bound" => AgentRouteKind::ExecutionBound,
        "identity_bound" => AgentRouteKind::IdentityBound,
        "capsule_bound" => AgentRouteKind::CapsuleBound,
        other => bail!("unknown agent delegation route kind `{other}`"),
    };
    let status = match route.status.as_str() {
        "prepared" => AgentRouteStatus::Prepared,
        "active" => AgentRouteStatus::Active,
        "expired" => AgentRouteStatus::Expired,
        "revoked" => AgentRouteStatus::Revoked,
        other => bail!("unknown agent delegation route status `{other}`"),
    };
    route
        .source_capsule_id
        .as_deref()
        .or(route.home_capsule_id.as_deref())
        .context("agent delegation route has no source capsule")?;
    let destination_capsule_id = route
        .destination_capsule_id
        .as_deref()
        .context("agent delegation route has no destination capsule")?
        .to_owned();
    let source_capsule_id = route
        .source_capsule_id
        .as_deref()
        .or(route.home_capsule_id.as_deref())
        .context("agent delegation route has no source capsule")?
        .to_owned();
    let source_workspace_id = route
        .source_workspace_id
        .as_deref()
        .context("agent delegation route has no source workspace")?
        .to_owned();
    let destination_workspace_id = route
        .destination_workspace_id
        .as_deref()
        .context("agent delegation route has no destination workspace")?
        .to_owned();
    let source_gateway_id = route
        .source_gateway_id
        .as_deref()
        .context("agent delegation route has no source gateway")?
        .to_owned();
    let destination_gateway_id = route
        .destination_gateway_id
        .as_deref()
        .context("agent delegation route has no destination gateway")?
        .to_owned();
    let allowed_actions: Vec<AgentRouteAction> = serde_json::from_str(&route.allowed_actions_json)
        .context("agent delegation route has invalid action subset")?;
    let disclosure: AgentRouteDisclosurePolicy = serde_json::from_str(&route.disclosure_json)
        .context("agent delegation route has invalid disclosure policy")?;
    let projection = AgentDelegationRouteProjection {
        id: pioneer_protocol::AgentDelegationRouteId::new(route.id.clone())
            .context("agent delegation route has invalid id")?,
        source_execution_id: AgentExecutionId::new(route.source_execution_id.clone())
            .context("agent delegation route has invalid source execution")?,
        source_capsule_id,
        destination_thread_id: route.destination_thread_id.clone(),
        destination_capsule_id,
        kind,
        status,
        allowed_actions,
        disclosure,
        source_agent_identity_id: route
            .source_identity_id
            .as_deref()
            .map(|id| AgentIdentityId::new(id.to_owned()))
            .transpose()
            .context("agent delegation route has invalid source identity")?
            .context("agent delegation route has no source identity")?,
        destination_agent_identity_id: route
            .destination_agent_identity_id
            .as_deref()
            .map(|id| AgentIdentityId::new(id.to_owned()))
            .transpose()
            .context("agent delegation route has invalid destination identity")?,
        destination_profile_id: route.destination_profile_id.clone(),
        source_workspace_id,
        destination_workspace_id,
        source_gateway_id,
        destination_gateway_id,
        generation: u64::try_from(route.route_generation)
            .context("agent delegation route has invalid generation")?,
        source_policy_generation: u64::try_from(route.source_policy_generation)
            .context("agent delegation route has invalid source policy generation")?,
        destination_policy_generation: u64::try_from(route.destination_policy_generation)
            .context("agent delegation route has invalid destination policy generation")?,
        hop_count: u16::try_from(route.hop_count)
            .context("agent delegation route has invalid hop count")?,
        max_hops: u16::try_from(route.max_hops)
            .context("agent delegation route has invalid max hops")?,
        grant_fingerprint: route.grant_fingerprint.clone(),
        expires_at: route
            .expires_at
            .as_ref()
            .map(|value| value.timestamp_millis()),
        return_route_id: route
            .return_route_id
            .as_deref()
            .map(|id| pioneer_protocol::AgentDelegationRouteId::new(id.to_owned()))
            .transpose()
            .context("agent delegation route has invalid return route")?,
    };
    projection.validate(None).map_err(|error| {
        anyhow!("agent delegation route projection failed validation: {error:?}")
    })?;
    Ok(projection)
}

pub async fn list_agent_delegation_routes<C: ConnectionTrait>(
    db: &C,
    source_execution_id: &str,
    after: Option<(DateTimeWithTimeZone, String)>,
    limit: u64,
) -> Result<Vec<agent_delegation_route::Model>> {
    if limit == 0 || limit > AGENT_PROJECTION_BATCH_LIMIT as u64 + 1 {
        bail!("Agent route list batch exceeds its bounded limit");
    }
    let mut query = agent_delegation_route::Entity::find().filter(
        agent_delegation_route::Column::SourceExecutionId.eq(source_execution_id.to_owned()),
    );
    if let Some((created_at, route_id)) = after {
        query = query.filter(
            Condition::any()
                .add(agent_delegation_route::Column::CreatedAt.gt(created_at.clone()))
                .add(
                    Condition::all()
                        .add(agent_delegation_route::Column::CreatedAt.eq(created_at))
                        .add(agent_delegation_route::Column::Id.gt(route_id)),
                ),
        );
    }
    query
        .order_by_asc(agent_delegation_route::Column::CreatedAt)
        .order_by_asc(agent_delegation_route::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list agent delegation routes")
}

/// List every route that can be bound to this exact execution: direct
/// execution-bound edges, identity-bound policy for its immutable source identity,
/// and capsule-bound policy for its persisted home root. Callers still revalidate status, expiry, generations and
/// destination policy before projecting an opaque target option.
pub async fn list_agent_delegation_routes_for_source<C: ConnectionTrait>(
    db: &C,
    source_execution_id: &str,
    source_identity_id: &str,
) -> Result<Vec<agent_delegation_route::Model>> {
    // The caller supplies only the execution key; capsule authority comes from storage.
    let execution = agent_execution::Entity::find_by_id(source_execution_id.to_owned())
        .one(db)
        .await?
        .context("route source execution is missing")?;
    if execution.agent_identity_id != source_identity_id {
        bail!("route source identity differs from persisted execution");
    }
    agent_delegation_route::Entity::find()
        .filter(
            Condition::any()
                .add(
                    Condition::all()
                        .add(agent_delegation_route::Column::RouteKind.eq("capsule_bound"))
                        .add(
                            agent_delegation_route::Column::SourceCapsuleId
                                .eq(execution.home_root_thread_id),
                        )
                        .add(
                            agent_delegation_route::Column::SourceWorkspaceId
                                .eq(execution.workspace_id),
                        ),
                )
                .add(
                    agent_delegation_route::Column::SourceExecutionId
                        .eq(source_execution_id.to_owned()),
                )
                .add(
                    Condition::all()
                        .add(agent_delegation_route::Column::RouteKind.eq("identity_bound"))
                        .add(
                            agent_delegation_route::Column::SourceIdentityId
                                .eq(source_identity_id.to_owned()),
                        ),
                ),
        )
        .filter(agent_delegation_route::Column::Status.eq("active"))
        .filter(
            agent_delegation_route::Column::ExpiresAt
                .is_null()
                .or(agent_delegation_route::Column::ExpiresAt.gt(chrono::Utc::now().fixed_offset())),
        )
        .order_by_asc(agent_delegation_route::Column::CreatedAt)
        .limit((AGENT_ROUTE_GRAPH_MAX_EDGES as u64).saturating_add(1))
        .all(db)
        .await
        .context("failed to list Agent routes for source binding")
        .and_then(|routes| {
            if routes.len() > AGENT_ROUTE_GRAPH_MAX_EDGES {
                bail!("Agent route binding exceeds the bounded edge limit");
            }
            Ok(routes)
        })
}

/// Revoke only the currently active/prepared generation. The affected-row
/// count is the CAS result, so a concurrent revoke or expiry cannot be
/// mistaken for a successful route mutation.
pub async fn revoke_agent_delegation_route(
    db: &DatabaseTransaction,
    route_id: &str,
    expected_generation: i64,
    expected_policy_generation: i64,
    authority_actor_json: &str,
    authority_fingerprint: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let actor: PersistedActorRef =
        serde_json::from_str(authority_actor_json).context("route revocation actor is invalid")?;
    if !matches!(actor, PersistedActorRef::Principal(_)) {
        bail!("only an authenticated principal may revoke an Agent route");
    }
    if authority_fingerprint.trim().is_empty() {
        bail!("route revocation authority fingerprint is required");
    }
    let current_policy_generation = current_policy_generation_on(db).await?;
    if i64::try_from(current_policy_generation.get())
        .context("current policy generation exceeds persistence bounds")?
        != expected_policy_generation
    {
        bail!("route revocation authority is stale");
    }
    let result = agent_delegation_route::Entity::update_many()
        .col_expr(
            agent_delegation_route::Column::Status,
            sea_orm::sea_query::Expr::value("revoked"),
        )
        .col_expr(
            agent_delegation_route::Column::RouteGeneration,
            sea_orm::sea_query::Expr::cust("route_generation + 1"),
        )
        .col_expr(
            agent_delegation_route::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            agent_delegation_route::Column::AuthorityActorJson,
            sea_orm::sea_query::Expr::value(Some(authority_actor_json.to_owned())),
        )
        .col_expr(
            agent_delegation_route::Column::AuthorityFingerprint,
            sea_orm::sea_query::Expr::value(Some(authority_fingerprint.to_owned())),
        )
        .filter(agent_delegation_route::Column::Id.eq(route_id.to_owned()))
        .filter(agent_delegation_route::Column::RouteGeneration.eq(expected_generation))
        .filter(agent_delegation_route::Column::RouteGeneration.lt(i64::MAX))
        .filter(agent_delegation_route::Column::Status.is_in(["prepared", "active"]))
        .filter(
            agent_delegation_route::Column::ExpiresAt
                .is_null()
                .or(agent_delegation_route::Column::ExpiresAt.gt(now)),
        )
        .exec(db)
        .await
        .context("failed to revoke agent delegation route")?;
    if result.rows_affected != 1 {
        return Ok(false);
    }
    append_agent_route_event(
        db,
        route_id,
        "revoked",
        expected_generation
            .checked_add(1)
            .context("route generation overflow")?,
        authority_actor_json,
        authority_fingerprint,
        now,
    )
    .await?;
    Ok(true)
}

#[derive(Debug, Clone)]
pub struct PreparedAgentDelegationRouteExpiryBatch {
    routes: Vec<PreparedAgentDelegationRouteExpiry>,
}

#[derive(Debug, Clone)]
struct PreparedAgentDelegationRouteExpiry {
    expected: agent_delegation_route::Model,
    event_id: String,
    next_generation: i64,
    authority_actor_json: String,
    authority_fingerprint: String,
    occurred_at: DateTimeWithTimeZone,
}

pub async fn prepare_agent_delegation_route_expiry_batch<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
) -> Result<PreparedAgentDelegationRouteExpiryBatch> {
    let candidates = agent_delegation_route::Entity::find()
        .filter(agent_delegation_route::Column::Status.is_in(["prepared", "active"]))
        .filter(agent_delegation_route::Column::ExpiresAt.lte(now))
        .filter(agent_delegation_route::Column::RouteGeneration.lt(i64::MAX))
        .order_by_asc(agent_delegation_route::Column::ExpiresAt)
        .order_by_asc(agent_delegation_route::Column::Id)
        .limit(AGENT_ROUTE_EXPIRY_BATCH_SIZE)
        .all(db)
        .await
        .context("failed to load expiring agent delegation routes")?;
    let authority_actor_json = serde_json::to_string(&PersistedActorRef::System)
        .context("failed to serialize System actor for route expiry")?;
    let authority_fingerprint = "agent-route-expiry".to_owned();
    let routes = candidates
        .into_iter()
        .map(|expected| {
            let next_generation = expected
                .route_generation
                .checked_add(1)
                .context("route generation overflow")?;
            Ok(PreparedAgentDelegationRouteExpiry {
                event_id: canonical_agent_id(
                    'V',
                    &format!("route-event\0{}\0{next_generation}", expected.id),
                ),
                expected,
                next_generation,
                authority_actor_json: authority_actor_json.clone(),
                authority_fingerprint: authority_fingerprint.clone(),
                occurred_at: now,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedAgentDelegationRouteExpiryBatch { routes })
}

pub async fn apply_prepared_agent_delegation_route_expiry_batch(
    db: &DatabaseTransaction,
    prepared: &PreparedAgentDelegationRouteExpiryBatch,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    if prepared.routes.is_empty() {
        return Ok(0);
    }

    let route_ids = prepared
        .routes
        .iter()
        .map(|route| route.expected.id.clone())
        .collect::<Vec<_>>();
    let current = agent_delegation_route::Entity::find()
        .filter(agent_delegation_route::Column::Id.is_in(route_ids))
        .order_by_asc(agent_delegation_route::Column::Id)
        .lock_exclusive()
        .all(db)
        .await
        .context("failed to revalidate expiring agent delegation routes")?
        .into_iter()
        .map(|route| (route.id.clone(), route))
        .collect::<BTreeMap<_, _>>();
    let eligible = prepared
        .routes
        .iter()
        .filter(|route| current.get(route.expected.id.as_str()) == Some(&route.expected))
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return Ok(0);
    }

    let eligible_ids = eligible
        .iter()
        .map(|route| route.expected.id.clone())
        .collect::<Vec<_>>();
    let updated = agent_delegation_route::Entity::update_many()
        .col_expr(
            agent_delegation_route::Column::Status,
            sea_orm::sea_query::Expr::value("expired"),
        )
        .col_expr(
            agent_delegation_route::Column::RouteGeneration,
            sea_orm::sea_query::Expr::cust("route_generation + 1"),
        )
        .col_expr(
            agent_delegation_route::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            agent_delegation_route::Column::AuthorityActorJson,
            sea_orm::sea_query::Expr::value(Some(eligible[0].authority_actor_json.clone())),
        )
        .col_expr(
            agent_delegation_route::Column::AuthorityFingerprint,
            sea_orm::sea_query::Expr::value(Some(eligible[0].authority_fingerprint.clone())),
        )
        .filter(agent_delegation_route::Column::Id.is_in(eligible_ids))
        .filter(agent_delegation_route::Column::Status.is_in(["prepared", "active"]))
        .filter(agent_delegation_route::Column::ExpiresAt.lte(now))
        .filter(agent_delegation_route::Column::RouteGeneration.lt(i64::MAX))
        .exec(db)
        .await
        .context("failed to bulk-expire agent delegation routes")?;
    let expected_updated = u64::try_from(eligible.len()).unwrap_or(u64::MAX);
    if updated.rows_affected != expected_updated {
        bail!("agent delegation routes changed while the expiry batch was committing");
    }

    let event_models = eligible
        .iter()
        .map(|route| agent_delegation_route_event::ActiveModel {
            id: Set(route.event_id.clone()),
            route_id: Set(route.expected.id.clone()),
            event_kind: Set("expired".to_owned()),
            route_generation: Set(route.next_generation),
            authority_actor_json: Set(route.authority_actor_json.clone()),
            authority_fingerprint: Set(route.authority_fingerprint.clone()),
            occurred_at: Set(route.occurred_at),
        })
        .collect::<Vec<_>>();
    agent_delegation_route_event::Entity::insert_many(event_models)
        .on_conflict(
            OnConflict::column(agent_delegation_route_event::Column::Id)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context("failed to append bulk Agent route expiry events")?;

    let event_ids = eligible
        .iter()
        .map(|route| route.event_id.clone())
        .collect::<Vec<_>>();
    let persisted_events = agent_delegation_route_event::Entity::find()
        .filter(agent_delegation_route_event::Column::Id.is_in(event_ids))
        .all(db)
        .await
        .context("failed to verify bulk Agent route expiry events")?
        .into_iter()
        .map(|event| (event.id.clone(), event))
        .collect::<BTreeMap<_, _>>();
    for route in &eligible {
        let event = persisted_events
            .get(route.event_id.as_str())
            .context("Agent route expiry event disappeared after append")?;
        if event.route_id != route.expected.id
            || event.event_kind != "expired"
            || event.route_generation != route.next_generation
            || event.authority_actor_json != route.authority_actor_json
            || event.authority_fingerprint != route.authority_fingerprint
            || event.occurred_at != route.occurred_at
        {
            bail!("Agent route expiry event id was reused with different facts");
        }
    }

    Ok(expected_updated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_postgres_test::with_database;
    use sea_orm::{PaginatorTrait, TransactionTrait};
    use std::time::Duration;

    fn route(
        source: &str,
        destination: &str,
        now: DateTimeWithTimeZone,
        generation: i64,
    ) -> Result<AgentDelegationRouteInput> {
        let source_execution_id = canonical_agent_id('E', source);
        let actor =
            PersistedActorRef::AgentExecution(AgentExecutionId::new(source_execution_id.clone())?);
        Ok(AgentDelegationRouteInput {
            id: canonical_agent_id('R', source),
            source_execution_id,
            destination_thread_id: destination.to_owned(),
            source_capsule_id: Some(source.to_owned()),
            destination_capsule_id: Some(destination.to_owned()),
            source_workspace_id: Some("routes".into()),
            destination_workspace_id: Some("routes".into()),
            source_gateway_id: Some("local".into()),
            destination_gateway_id: Some("local".into()),
            source_identity_id: Some("I00000000000000000001".into()),
            destination_agent_identity_id: None,
            destination_profile_id: None,
            home_capsule_id: Some(source.to_owned()),
            route_kind: "execution_bound".into(),
            authority_actor_json: serde_json::to_string(&actor)?,
            authority_fingerprint: "authorization".into(),
            allowed_actions_json: serde_json::to_string(&[AgentRouteAction::SendMessage])?,
            disclosure_json: serde_json::to_string(&AgentRouteDisclosurePolicy {
                text: true,
                ..Default::default()
            })?,
            route_generation: 1,
            source_policy_generation: generation,
            destination_policy_generation: generation,
            hop_count: 0,
            max_hops: 8,
            return_route_id: None,
            grant_fingerprint: "grant".into(),
            status: "active".into(),
            updated_at: now,
            expires_at: Some(now + chrono::Duration::hours(1)),
            now,
        })
    }

    #[tokio::test]
    async fn reverse_route_waits_for_committed_graph_and_can_proceed_after_rollback() -> Result<()>
    {
        for commit_first in [true, false] {
            with_database(async |fixture| {
                let db = fixture.connection();
                db.execute_unprepared(
                    "INSERT INTO workspace(id,name) VALUES ('routes','Routes');
                     INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
                     VALUES ('a','routes','','agent','model','provider','active'),
                            ('b','routes','','agent','model','provider','active');
                     INSERT INTO agent_identity(id,workspace_id,source_kind,source_id,source_fingerprint)
                     VALUES ('I00000000000000000001','routes','ephemeral','route-test','identity');",
                ).await?;
                for capsule in ["a", "b"] {
                    db.execute_raw(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "INSERT INTO agent_execution
                         (id,workspace_id,agent_identity_id,identity_source_revision,identity_source_fingerprint,
                          home_root_thread_id,work_graph_root_execution_id,requested_identity_selection_json,
                          requested_profile_selection_json,authorization_context_fingerprint,status)
                         VALUES ($1,'routes','I00000000000000000001',1,'identity',$2,$1,'{}','{}','auth','running')",
                        [canonical_agent_id('E', capsule).into(), capsule.into()],
                    )).await?;
                }

                let generation = i64::try_from(current_policy_generation_on(db).await?.get())?;
                let now = chrono::Utc::now().fixed_offset();
                let first = db.begin().await?;
                let first_id = insert_agent_delegation_route(&first, &route("a", "b", now, generation)?)
                    .await?.id;
                let other = fixture.connect().await?;
                let reverse = other.begin().await?;
                let reverse_pid = reverse.query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid() AS pid",
                )).await?.context("reverse route backend pid")?.try_get::<i32>("", "pid")?;
                let reverse_input = route("b", "a", now, generation)?;
                let reverse_id = reverse_input.id.clone();
                let writer = tokio::spawn(async move {
                    let outcome = insert_agent_delegation_route(&reverse, &reverse_input).await;
                    if outcome.is_ok() {
                        reverse.commit().await?;
                    } else {
                        reverse.rollback().await?;
                    }
                    outcome
                });
                let waiting = tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let waiting = db.query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0 AS waiting",
                            [reverse_pid.into()],
                        )).await?.context("reverse route lock state")?.try_get::<bool>("", "waiting")?;
                        if waiting {
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::task::yield_now().await;
                    }
                }).await;
                if commit_first {
                    first.commit().await?;
                } else {
                    first.rollback().await?;
                }
                let outcome = writer.await?;
                waiting.context("reverse route should wait for the graph's writer")??;
                let surviving = if commit_first {
                    let error = outcome.expect_err("the reverse route would introduce a cycle");
                    assert!(error.to_string().contains("route graph is invalid"), "{error:#}");
                    first_id
                } else {
                    assert_eq!(outcome?.id, reverse_id);
                    reverse_id
                };
                assert_eq!(agent_delegation_route::Entity::find().count(db).await?, 1);
                assert_eq!(agent_delegation_route_event::Entity::find().count(db).await?, 1);

                let later = now + chrono::Duration::hours(2);
                let expired = prepare_agent_delegation_route_expiry_batch(db, later).await?;
                let tx = db.begin().await?;
                assert_eq!(apply_prepared_agent_delegation_route_expiry_batch(&tx, &expired, later).await?, 1);
                tx.rollback().await?;
                assert_eq!(load_agent_delegation_route(db, &surviving).await?.unwrap().status, "active");
                assert_eq!(agent_delegation_route_event::Entity::find().count(db).await?, 1);
                let tx = db.begin().await?;
                assert_eq!(apply_prepared_agent_delegation_route_expiry_batch(&tx, &expired, later).await?, 1);
                tx.commit().await?;
                assert_eq!(load_agent_delegation_route(db, &surviving).await?.unwrap().status, "expired");
                assert_eq!(agent_delegation_route_event::Entity::find().count(db).await?, 2);
                other.close().await?;
                Ok(())
            }).await?;
        }
        Ok(())
    }
}
