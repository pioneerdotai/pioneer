//! Authenticated human/service lifecycle for persistent Agent routes.
//!
//! Model-facing adapters never call this service. The request is structured,
//! both collaboration roots are resolved through the canonical authorization
//! service, and CRUD rechecks the policy generation in the write transaction.

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use pioneer_crud::{AgentDelegationRouteInput, CrudStore};
use pioneer_protocol::{
    AgentDelegationRouteCreateParams, AgentDelegationRouteListParams,
    AgentDelegationRouteListResponse, AgentDelegationRouteProjection,
    AgentDelegationRouteRevokeParams, AgentRootDelegationRequest, AgentRouteAction,
    PersistedActorRef,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::request_context::RequestContext;

use super::{AuthorizationResolver, AuthorizationService, ProofResolution, ResourceAction};

const ROUTE_IDEMPOTENCY_KEY_MAX_BYTES: usize = 255;
const ROOT_ROUTE_MAX_COUNT: usize = 8;
const ROOT_ROUTE_MAX_LIFETIME_MILLIS: i64 = 15 * 60 * 1_000;
const ROUTE_LIST_DEFAULT_LIMIT: usize = 50;
const ROUTE_LIST_MAX_LIMIT: usize = 100;
const ROUTE_LIST_CURSOR_MAX_BYTES: usize = 128;

#[derive(Clone)]
pub(crate) struct AgentRouteManagementService {
    store: CrudStore,
    authorization: AuthorizationService,
    resolver: AuthorizationResolver,
}

impl AgentRouteManagementService {
    pub(crate) fn new(store: Arc<CrudStore>) -> Self {
        let store = store.as_ref().clone();
        Self {
            resolver: AuthorizationResolver::new(store.clone()),
            authorization: AuthorizationService::new(),
            store,
        }
    }

    pub(crate) async fn prepare_root_routes(
        &self,
        context: &RequestContext,
        workspace_id: &str,
        source_capsule_id: &str,
        turn_id: &str,
        mut requests: Vec<AgentRootDelegationRequest>,
    ) -> Result<Vec<super::RootAgentRouteGrant>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        if requests.len() > ROOT_ROUTE_MAX_COUNT
            || workspace_id.trim().is_empty()
            || source_capsule_id.trim().is_empty()
            || turn_id.trim().is_empty()
            || turn_id.trim() != turn_id
        {
            bail!("root Agent route request is invalid");
        }
        let policy_generation = self.current_policy_generation().await?;
        self.require_thread_action(
            context,
            ResourceAction::AgentSourceExport,
            source_capsule_id,
            Some(workspace_id),
        )
        .await?;
        let now = pioneer_crud::utc_now();
        let latest_expiry = now
            .timestamp_millis()
            .checked_add(ROOT_ROUTE_MAX_LIFETIME_MILLIS)
            .context("root Agent route expiry exceeds timestamp bounds")?;
        let authority_actor =
            PersistedActorRef::Principal(context.principal().principal_id.clone());
        let authority_actor_json = serde_json::to_string(&authority_actor)
            .context("failed to encode root Agent route authority")?;
        let mut route_ids = std::collections::BTreeSet::new();
        let mut grants = Vec::with_capacity(requests.len());
        for request in &mut requests {
            validate_idempotency_key(request.idempotency_key.as_str())?;
            if request.destination_thread_id.trim() != request.destination_thread_id
                || request.destination_thread_id.is_empty()
                || request.expires_at <= now.timestamp_millis()
                || request.expires_at > latest_expiry
            {
                bail!("root Agent route request is invalid");
            }
            request.allowed_actions.sort_by_key(route_action_order);
            if request.allowed_actions.is_empty()
                || request
                    .allowed_actions
                    .windows(2)
                    .any(|pair| pair[0] == pair[1])
                || !route_disclosure_matches_actions(
                    request.allowed_actions.as_slice(),
                    request.disclosure,
                )
            {
                bail!("root Agent route request is invalid");
            }
            let mut destination = None;
            for action in request.allowed_actions.iter().copied() {
                for required in destination_actions(action) {
                    let authorized = self
                        .require_thread_action(
                            context,
                            *required,
                            request.destination_thread_id.as_str(),
                            Some(workspace_id),
                        )
                        .await?;
                    destination.get_or_insert(authorized);
                }
            }
            let destination = destination.context("root Agent route has no destination action")?;
            if destination.workspace_id != workspace_id
                || destination.capsule_id == source_capsule_id
            {
                bail!("root Agent route is not authorized");
            }
            self.require_destination_identity(
                destination.workspace_id.as_str(),
                request.destination_agent_identity_id.as_ref(),
                request.destination_profile_id.as_ref(),
            )
            .await?;
            let route_id = pioneer_crud::canonical_agent_id(
                'R',
                &format!(
                    "root-admission-route\0{turn_id}\0{}",
                    request.idempotency_key
                ),
            );
            if !route_ids.insert(route_id.clone()) {
                bail!("root Agent route idempotency key is duplicated");
            }
            let authority_fingerprint =
                root_authority_fingerprint(context, turn_id, request, policy_generation)?;
            let grant_fingerprint = route_grant_fingerprint(
                route_id.as_str(),
                crate::message::agent_action_tools::root_agent_execution_id_for_turn(turn_id)
                    .as_str(),
                request.destination_thread_id.as_str(),
                authority_fingerprint.as_str(),
            );
            grants.push(super::RootAgentRouteGrant {
                route_id: pioneer_protocol::AgentDelegationRouteId::new(route_id)
                    .context("generated root Agent route id is invalid")?,
                destination_thread_id: request.destination_thread_id.clone(),
                destination_capsule_id: destination.capsule_id,
                gateway_id: context.principal().gateway_id.to_string(),
                allowed_actions: request.allowed_actions.clone(),
                disclosure: request.disclosure,
                destination_agent_identity_id: request.destination_agent_identity_id.clone(),
                destination_profile_id: request.destination_profile_id.clone(),
                expires_at: request.expires_at,
                return_route_id: request.return_route_id.clone(),
                authority_actor_json: authority_actor_json.clone(),
                authority_fingerprint,
                grant_fingerprint,
                policy_generation: u64::try_from(policy_generation)
                    .context("root Agent route policy generation is invalid")?,
            });
        }
        if self.current_policy_generation().await? != policy_generation {
            bail!("root Agent route policy changed during admission");
        }
        Ok(grants)
    }

    pub(crate) async fn create(
        &self,
        context: &RequestContext,
        mut params: AgentDelegationRouteCreateParams,
    ) -> Result<AgentDelegationRouteProjection> {
        validate_idempotency_key(params.idempotency_key.as_str())?;
        if params.destination_thread_id.trim() != params.destination_thread_id
            || params.destination_thread_id.is_empty()
        {
            bail!("Agent route request is invalid");
        }
        params.allowed_actions.sort_by_key(route_action_order);
        if params.allowed_actions.is_empty()
            || params
                .allowed_actions
                .windows(2)
                .any(|pair| pair[0] == pair[1])
            || !route_disclosure_matches_actions(
                params.allowed_actions.as_slice(),
                params.disclosure,
            )
        {
            bail!("Agent route request is invalid");
        }
        let now = pioneer_crud::utc_now();
        let expires_at = DateTime::<Utc>::from_timestamp_millis(params.expires_at)
            .map(|value| value.fixed_offset())
            .filter(|expires_at| expires_at > &now)
            .context("Agent route request is invalid")?;
        let admitted_policy_generation = self.current_policy_generation().await?;
        let source_execution = pioneer_crud::load_agent_execution(
            &self.store.database_connection(),
            params.source_execution_id.as_str(),
        )
        .await
        .context("Agent route authority is unavailable")?
        .context("Agent route is not authorized")?;

        let source = self
            .require_thread_action(
                context,
                ResourceAction::AgentRouteCreate,
                source_execution.home_root_thread_id.as_str(),
                Some(source_execution.workspace_id.as_str()),
            )
            .await?;
        self.require_thread_action(
            context,
            ResourceAction::AgentSourceExport,
            source_execution.home_root_thread_id.as_str(),
            Some(source_execution.workspace_id.as_str()),
        )
        .await?;
        let destination = self
            .require_thread_action(
                context,
                ResourceAction::AgentRouteCreate,
                params.destination_thread_id.as_str(),
                Some(source_execution.workspace_id.as_str()),
            )
            .await?;
        for action in params.allowed_actions.iter().copied() {
            for required in destination_actions(action) {
                self.require_thread_action(
                    context,
                    *required,
                    params.destination_thread_id.as_str(),
                    Some(source_execution.workspace_id.as_str()),
                )
                .await?;
            }
        }
        if source.workspace_id != destination.workspace_id
            || source.workspace_id != source_execution.workspace_id
            || source.capsule_id == destination.capsule_id
        {
            bail!("Agent route is not authorized");
        }
        self.require_destination_identity(
            destination.workspace_id.as_str(),
            params.destination_agent_identity_id.as_ref(),
            params.destination_profile_id.as_ref(),
        )
        .await?;

        let policy_generation = self.current_policy_generation().await?;
        if policy_generation != admitted_policy_generation {
            bail!("Agent route policy changed during admission");
        }
        let authority_actor =
            PersistedActorRef::Principal(context.principal().principal_id.clone());
        let authority_actor_json = serde_json::to_string(&authority_actor)
            .context("failed to encode Agent route authority")?;
        let authority_fingerprint =
            create_authority_fingerprint(context, &params, policy_generation)?;
        let route_id = pioneer_crud::canonical_agent_id(
            'R',
            &format!(
                "managed-route\0{}\0{}\0{}",
                context.principal().gateway_id,
                context.principal().principal_id,
                params.idempotency_key
            ),
        );
        let grant_fingerprint = route_grant_fingerprint(
            route_id.as_str(),
            params.source_execution_id.as_str(),
            params.destination_thread_id.as_str(),
            authority_fingerprint.as_str(),
        );
        let input = AgentDelegationRouteInput {
            id: route_id,
            source_execution_id: params.source_execution_id.to_string(),
            destination_thread_id: params.destination_thread_id,
            source_capsule_id: Some(source.capsule_id.clone()),
            destination_capsule_id: Some(destination.capsule_id),
            source_workspace_id: Some(source.workspace_id.clone()),
            destination_workspace_id: Some(destination.workspace_id),
            source_gateway_id: Some(context.principal().gateway_id.to_string()),
            destination_gateway_id: Some(context.principal().gateway_id.to_string()),
            source_identity_id: Some(source_execution.agent_identity_id),
            destination_agent_identity_id: params
                .destination_agent_identity_id
                .map(|id| id.to_string()),
            destination_profile_id: params.destination_profile_id.map(|id| id.to_string()),
            home_capsule_id: Some(source.capsule_id),
            route_kind: match params.kind {
                pioneer_protocol::AgentRouteKind::ExecutionBound => "execution_bound",
                pioneer_protocol::AgentRouteKind::IdentityBound => "identity_bound",
            }
            .to_owned(),
            authority_actor_json,
            authority_fingerprint,
            allowed_actions_json: serde_json::to_string(&params.allowed_actions)
                .context("failed to encode Agent route action subset")?,
            disclosure_json: serde_json::to_string(&params.disclosure)
                .context("failed to encode Agent route disclosure")?,
            route_generation: 1,
            source_policy_generation: policy_generation,
            destination_policy_generation: policy_generation,
            hop_count: 1,
            max_hops: 8,
            return_route_id: params.return_route_id.map(|id| id.to_string()),
            grant_fingerprint,
            status: "active".to_owned(),
            updated_at: now.clone(),
            expires_at: Some(expires_at),
            now,
        };
        let route = self
            .store
            .create_agent_delegation_route(input)
            .await
            .context("failed to create Agent route")?;
        pioneer_crud::agent_delegation_route_projection(&route)
            .context("failed to project created Agent route")
    }

    pub(crate) async fn list(
        &self,
        context: &RequestContext,
        params: AgentDelegationRouteListParams,
    ) -> Result<AgentDelegationRouteListResponse> {
        let limit = usize::from(params.limit.unwrap_or(ROUTE_LIST_DEFAULT_LIMIT as u16));
        if !(1..=ROUTE_LIST_MAX_LIMIT).contains(&limit) {
            bail!("Agent route list limit is invalid");
        }
        let source_execution = pioneer_crud::load_agent_execution(
            &self.store.database_connection(),
            params.source_execution_id.as_str(),
        )
        .await
        .context("Agent route authority is unavailable")?
        .context("Agent route is not authorized")?;
        self.require_thread_action(
            context,
            ResourceAction::AgentRouteCreate,
            source_execution.home_root_thread_id.as_str(),
            Some(source_execution.workspace_id.as_str()),
        )
        .await?;
        self.store
            .expire_agent_delegation_routes(pioneer_crud::utc_now())
            .await
            .context("failed to expire Agent routes")?;
        let after = if let Some(cursor) = params.cursor.as_deref() {
            let route_id = decode_route_list_cursor(cursor)?;
            let route = pioneer_crud::load_agent_delegation_route(
                &self.store.database_connection(),
                route_id.as_str(),
            )
            .await
            .context("Agent route list cursor is invalid")?
            .filter(|route| route.source_execution_id == params.source_execution_id.as_str())
            .context("Agent route list cursor is invalid")?;
            Some((route.created_at, route.id))
        } else {
            None
        };
        let mut rows = pioneer_crud::list_agent_delegation_routes(
            &self.store.database_connection(),
            params.source_execution_id.as_str(),
            after,
            u64::try_from(limit.saturating_add(1)).unwrap_or(u64::MAX),
        )
        .await
        .context("failed to list Agent routes")?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_cursor = has_more
            .then(|| {
                rows.last()
                    .map(|route| encode_route_list_cursor(route.id.as_str()))
            })
            .flatten();
        let mut routes = Vec::with_capacity(rows.len());
        for row in rows {
            if self
                .resolve_thread_action(
                    context,
                    ResourceAction::ThreadRead,
                    row.destination_thread_id.as_str(),
                    Some(source_execution.workspace_id.as_str()),
                )
                .await?
                .is_none()
            {
                continue;
            }
            routes.push(
                pioneer_crud::agent_delegation_route_projection(&row)
                    .context("failed to project Agent route")?,
            );
        }
        Ok(AgentDelegationRouteListResponse {
            routes,
            next_cursor,
        })
    }

    pub(crate) async fn revoke(
        &self,
        context: &RequestContext,
        params: AgentDelegationRouteRevokeParams,
    ) -> Result<AgentDelegationRouteProjection> {
        validate_idempotency_key(params.idempotency_key.as_str())?;
        let admitted_policy_generation = self.current_policy_generation().await?;
        let route = pioneer_crud::load_agent_delegation_route(
            &self.store.database_connection(),
            params.route_id.as_str(),
        )
        .await
        .context("Agent route authority is unavailable")?
        .context("Agent route is not authorized")?;
        let source_execution = pioneer_crud::load_agent_execution(
            &self.store.database_connection(),
            route.source_execution_id.as_str(),
        )
        .await
        .context("Agent route authority is unavailable")?
        .context("Agent route is not authorized")?;
        self.require_thread_action(
            context,
            ResourceAction::AgentRouteRevoke,
            source_execution.home_root_thread_id.as_str(),
            route.source_workspace_id.as_deref(),
        )
        .await?;
        self.require_thread_action(
            context,
            ResourceAction::AgentRouteRevoke,
            route.destination_thread_id.as_str(),
            route.destination_workspace_id.as_deref(),
        )
        .await?;
        let expected_generation =
            i64::try_from(params.expected_generation).context("Agent route request is invalid")?;
        let policy_generation = self.current_policy_generation().await?;
        if policy_generation != admitted_policy_generation {
            bail!("Agent route policy changed during admission");
        }
        let authority_actor =
            PersistedActorRef::Principal(context.principal().principal_id.clone());
        let authority_actor_json = serde_json::to_string(&authority_actor)
            .context("failed to encode Agent route authority")?;
        let authority_fingerprint = revoke_authority_fingerprint(
            context,
            params.route_id.as_str(),
            expected_generation,
            params.idempotency_key.as_str(),
            policy_generation,
        );
        let revoked_generation = expected_generation
            .checked_add(1)
            .context("Agent route generation is exhausted")?;
        if route.status == "revoked"
            && route.route_generation == revoked_generation
            && route.authority_actor_json.as_deref() == Some(authority_actor_json.as_str())
            && route.authority_fingerprint.as_deref() == Some(authority_fingerprint.as_str())
        {
            return pioneer_crud::agent_delegation_route_projection(&route)
                .context("failed to project revoked Agent route");
        }
        let revoked = self
            .store
            .revoke_agent_delegation_route(
                params.route_id.as_str(),
                expected_generation,
                policy_generation,
                authority_actor_json.as_str(),
                authority_fingerprint.as_str(),
                pioneer_crud::utc_now(),
            )
            .await
            .context("failed to revoke Agent route")?;
        if !revoked {
            bail!("Agent route changed before revocation");
        }
        let route = pioneer_crud::load_agent_delegation_route(
            &self.store.database_connection(),
            params.route_id.as_str(),
        )
        .await
        .context("failed to reload revoked Agent route")?
        .context("revoked Agent route disappeared")?;
        pioneer_crud::agent_delegation_route_projection(&route)
            .context("failed to project revoked Agent route")
    }

    async fn require_thread_action(
        &self,
        context: &RequestContext,
        action: ResourceAction,
        thread_id: &str,
        workspace_id: Option<&str>,
    ) -> Result<AuthorizedRouteThread> {
        self.resolve_thread_action(context, action, thread_id, workspace_id)
            .await?
            .context("Agent route is not authorized")
    }

    async fn resolve_thread_action(
        &self,
        context: &RequestContext,
        action: ResourceAction,
        thread_id: &str,
        workspace_id: Option<&str>,
    ) -> Result<Option<AuthorizedRouteThread>> {
        let gate = self.authorization.authorize_action(
            context.principal().kind,
            context.role_key(),
            action,
        );
        if !gate.permits_resource_resolution() {
            return Ok(None);
        }
        let direct = self
            .resolver
            .authorize_thread(context.principal(), &gate, action, thread_id, workspace_id)
            .await
            .context("Agent route authority is unavailable")?;
        let resolution = match direct {
            ProofResolution::Authorized(proof) => ProofResolution::Authorized(proof),
            ProofResolution::Denied(_) => self
                .resolver
                .authorize_internal_thread_via_root(
                    context.principal(),
                    &gate,
                    action,
                    thread_id,
                    workspace_id,
                )
                .await
                .context("Agent route authority is unavailable")?,
        };
        match resolution {
            ProofResolution::Authorized(proof) => Ok(Some(AuthorizedRouteThread {
                workspace_id: proof.workspace_id().to_owned(),
                capsule_id: proof.collaboration_root_thread_id().to_owned(),
            })),
            ProofResolution::Denied(_) => Ok(None),
        }
    }

    async fn current_policy_generation(&self) -> Result<i64> {
        let generation = pioneer_crud::current_policy_generation(&self.store.database_connection())
            .await
            .context("Agent route authority is unavailable")?;
        i64::try_from(generation.get()).context("Agent route authority is unavailable")
    }

    async fn require_destination_identity(
        &self,
        workspace_id: &str,
        identity_id: Option<&pioneer_protocol::AgentIdentityId>,
        profile_id: Option<&pioneer_protocol::AgentExecutionProfileId>,
    ) -> Result<()> {
        if profile_id.is_some() && identity_id.is_none() {
            bail!("Agent route is not authorized");
        }
        let Some(identity_id) = identity_id else {
            return Ok(());
        };
        let identity = pioneer_crud::load_active_agent_identity(
            &self.store.database_connection(),
            workspace_id,
            identity_id.as_str(),
        )
        .await
        .context("Agent route destination identity is unavailable")?;
        if identity.is_none() {
            bail!("Agent route is not authorized");
        }
        Ok(())
    }
}

fn encode_route_list_cursor(route_id: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(route_id.as_bytes())
}

fn decode_route_list_cursor(cursor: &str) -> Result<String> {
    if cursor.is_empty() || cursor.len() > ROUTE_LIST_CURSOR_MAX_BYTES {
        bail!("Agent route list cursor is invalid");
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .context("Agent route list cursor is invalid")?;
    if decoded.len() != 21 {
        bail!("Agent route list cursor is invalid");
    }
    let route_id = String::from_utf8(decoded).context("Agent route list cursor is invalid")?;
    pioneer_protocol::AgentDelegationRouteId::new(route_id.clone())
        .context("Agent route list cursor is invalid")?;
    Ok(route_id)
}

struct AuthorizedRouteThread {
    workspace_id: String,
    capsule_id: String,
}

fn validate_idempotency_key(value: &str) -> Result<()> {
    if value.trim() != value
        || value.is_empty()
        || value.len() > ROUTE_IDEMPOTENCY_KEY_MAX_BYTES
        || value.chars().any(char::is_control)
    {
        bail!("Agent route request is invalid");
    }
    Ok(())
}

const fn route_action_order(action: &AgentRouteAction) -> u8 {
    match action {
        AgentRouteAction::SendMessage => 0,
        AgentRouteAction::StartAgent => 1,
        AgentRouteAction::CreateTask => 2,
        AgentRouteAction::ScheduleTask => 3,
        AgentRouteAction::ReviewTaskResult => 4,
        AgentRouteAction::DeliverResult => 5,
    }
}

fn destination_actions(action: AgentRouteAction) -> &'static [ResourceAction] {
    match action {
        AgentRouteAction::SendMessage | AgentRouteAction::DeliverResult => {
            &[ResourceAction::MessageCreate]
        }
        AgentRouteAction::StartAgent => &[ResourceAction::AgentTurnStart],
        AgentRouteAction::CreateTask => &[ResourceAction::TaskCreate],
        AgentRouteAction::ScheduleTask => &[
            ResourceAction::TaskCreate,
            ResourceAction::TaskScheduleManage,
        ],
        AgentRouteAction::ReviewTaskResult => &[ResourceAction::TaskReview],
    }
}

fn route_disclosure_matches_actions(
    actions: &[AgentRouteAction],
    disclosure: pioneer_protocol::AgentRouteDisclosurePolicy,
) -> bool {
    if !disclosure.allows_anything() {
        return false;
    }
    let delivers_result = actions.contains(&AgentRouteAction::DeliverResult);
    delivers_result
        == !matches!(
            disclosure.result_return,
            pioneer_protocol::AgentResultReturnPolicy::None
        )
}

fn create_authority_fingerprint(
    context: &RequestContext,
    params: &AgentDelegationRouteCreateParams,
    policy_generation: i64,
) -> Result<String> {
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:managed-route-create:v1\0");
    digest.update(context.principal().gateway_id.as_str().as_bytes());
    digest.update([0]);
    digest.update(context.principal().principal_id.as_str().as_bytes());
    digest.update([0]);
    digest.update(policy_generation.to_string().as_bytes());
    digest.update([0]);
    digest.update(serde_json::to_vec(params).context("failed to fingerprint Agent route request")?);
    Ok(hex::encode(digest.finalize()))
}

fn revoke_authority_fingerprint(
    context: &RequestContext,
    route_id: &str,
    expected_generation: i64,
    idempotency_key: &str,
    policy_generation: i64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:managed-route-revoke:v1\0");
    for value in [
        context.principal().gateway_id.as_str(),
        context.principal().principal_id.as_str(),
        route_id,
        idempotency_key,
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    digest.update(expected_generation.to_string().as_bytes());
    digest.update([0]);
    digest.update(policy_generation.to_string().as_bytes());
    hex::encode(digest.finalize())
}

fn route_grant_fingerprint(
    route_id: &str,
    source_execution_id: &str,
    destination_thread_id: &str,
    authority_fingerprint: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:managed-route-grant:v1\0");
    for value in [
        route_id,
        source_execution_id,
        destination_thread_id,
        authority_fingerprint,
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    hex::encode(digest.finalize())
}

fn root_authority_fingerprint(
    context: &RequestContext,
    turn_id: &str,
    request: &AgentRootDelegationRequest,
    policy_generation: i64,
) -> Result<String> {
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:root-admission-route:v1\0");
    for value in [
        context.principal().gateway_id.as_str(),
        context.principal().principal_id.as_str(),
        turn_id,
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    digest.update(policy_generation.to_string().as_bytes());
    digest.update([0]);
    digest.update(
        serde_json::to_vec(request).context("failed to fingerprint root Agent route request")?,
    );
    Ok(hex::encode(digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_management_request_keys_are_bounded() {
        assert!(validate_idempotency_key("route-1").is_ok());
        assert!(validate_idempotency_key("").is_err());
        assert!(validate_idempotency_key(" route-1").is_err());
        assert!(validate_idempotency_key(&"x".repeat(256)).is_err());
    }

    #[test]
    fn scheduled_route_requires_create_and_schedule_rights() {
        assert_eq!(
            destination_actions(AgentRouteAction::ScheduleTask),
            &[
                ResourceAction::TaskCreate,
                ResourceAction::TaskScheduleManage
            ]
        );
    }
}
