//! Profile presentation exposed to a viewer through an admitted App view.
use std::{collections::HashSet, time::Duration};

use axum::{
    Json,
    body::{Body, to_bytes},
    http::{HeaderMap, Method},
    response::{IntoResponse, Response},
};
use pioneer_protocol::{
    AgentIdentityId, CLIAgentRuntimeKind, ImageMediaType, PrincipalId, PrincipalKind,
    PrincipalStatus, RequestId,
};
use serde::{Deserialize, Serialize};

use crate::auth::AuthenticatedSessionPrincipal;

use super::{
    avatars::{avatar_response, map_avatar_error, system_agent_avatar},
    errors::{HttpError, HttpErrorKind},
    images::{image_representation_response, valid_revision},
    state::GatewayHttpState,
};

const LIMIT: usize = 64;
const BODY_LIMIT: usize = 8192;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
enum IdentityType {
    Human,
    Agent,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
struct Identity {
    #[serde(rename = "type")]
    kind: IdentityType,
    id: String,
}

impl Identity {
    fn valid(&self) -> bool {
        match self.kind {
            IdentityType::Human => PrincipalId::new(self.id.clone()).is_ok(),
            IdentityType::Agent => AgentIdentityId::new(self.id.clone()).is_ok(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveRequest {
    #[serde(default)]
    include_self: bool,
    #[serde(default)]
    identities: Vec<Identity>,
}

#[derive(Serialize)]
struct Profile {
    #[serde(flatten)]
    identity: Identity,
    display_name: String,
    nickname: String,
    avatar_revision: Option<String>,
}

#[derive(Serialize)]
struct Resolved {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "self")]
    viewer: Option<Profile>,
    profiles: Vec<Profile>,
}

async fn profile(
    state: &GatewayHttpState,
    viewer: &AuthenticatedSessionPrincipal,
    workspace_id: &str,
    identity: Identity,
) -> anyhow::Result<Option<Profile>> {
    let db = state.message_processor.crud_store.database_connection();
    let (display_name, nickname, avatar_revision) = match identity.kind {
        IdentityType::Human => {
            let id = PrincipalId::new(identity.id.clone())?;
            let Some(target) = pioneer_crud::load_principal_by_id(&db, &id).await? else {
                return Ok(None);
            };
            if target.gateway_id != viewer.gateway_id || target.status == PrincipalStatus::Removed {
                return Ok(None);
            }
            let is_viewer = id == viewer.principal_id;
            let is_directory_superuser =
                target.kind == PrincipalKind::Superuser && target.status == PrincipalStatus::Active;
            if !is_viewer
                && !is_directory_superuser
                && pioneer_crud::find_workspace_membership(&db, &id, workspace_id)
                    .await?
                    .is_none()
            {
                return Ok(None);
            }
            let Some(profile) = pioneer_crud::load_principal_profile(&db, &id).await? else {
                return Ok(None);
            };
            (
                profile.display_name,
                profile.nickname,
                profile.avatar_hash.map(hex::encode),
            )
        }
        IdentityType::Agent => {
            let Some(agent) =
                pioneer_crud::load_active_agent_identity(&db, workspace_id, &identity.id).await?
            else {
                return Ok(None);
            };
            // Execution-local identities are not workspace directory entries.
            if agent.source_kind == pioneer_crud::SOURCE_EPHEMERAL {
                return Ok(None);
            }
            let Some(profile) = pioneer_crud::load_current_agent_presentation_snapshot(
                &db,
                &agent.id,
                agent.source_revision,
                &agent.source_fingerprint,
            )
            .await?
            else {
                return Ok(None);
            };
            let avatar = profile
                .avatar_revision
                .filter(|revision| system_agent_avatar(revision).is_some());
            let avatar = avatar.or_else(|| match agent.source_kind.as_str() {
                pioneer_crud::SOURCE_NATIVE_AGENT
                    if profile.nickname == pioneer_protocol::PIONEER_AGENT_NICKNAME =>
                {
                    Some(pioneer_protocol::PIONEER_AGENT_AVATAR_REVISION.to_owned())
                }
                pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE => {
                    let kind = match profile.role_label.as_deref()? {
                        "codex" => CLIAgentRuntimeKind::Codex,
                        "claude" => CLIAgentRuntimeKind::Claude,
                        "opencode" => CLIAgentRuntimeKind::OpenCode,
                        _ => return None,
                    };
                    Some(pioneer_protocol::cli_runtime_avatar_revision(kind, false).to_owned())
                }
                _ => None,
            });
            (profile.display_name, profile.nickname, avatar)
        }
    };
    Ok(Some(Profile {
        identity,
        display_name,
        nickname,
        avatar_revision,
    }))
}

pub(super) async fn request(
    state: &GatewayHttpState,
    viewer: &AuthenticatedSessionPrincipal,
    workspace_id: &str,
    path: &str,
    method: &Method,
    headers: &HeaderMap,
    body: Body,
    id: &RequestId,
) -> Result<Response, HttpError> {
    if path == "_pioneer/profiles/resolve" && method == Method::POST {
        let bytes = tokio::time::timeout(Duration::from_secs(10), to_bytes(body, BODY_LIMIT))
            .await
            .map_err(|_| HttpError::new(HttpErrorKind::RequestTimeout, id.clone()))?
            .map_err(|_| HttpError::new(HttpErrorKind::PayloadTooLarge, id.clone()))?;
        let input: ResolveRequest =
            serde_json::from_slice(&bytes).map_err(|_| HttpError::bad_request(id.clone()))?;
        if input.identities.len() > LIMIT
            || input.identities.iter().any(|identity| !identity.valid())
        {
            return Err(HttpError::bad_request(id.clone()));
        }
        let viewer_profile = if input.include_self {
            profile(
                state,
                viewer,
                workspace_id,
                Identity {
                    kind: IdentityType::Human,
                    id: viewer.principal_id.to_string(),
                },
            )
            .await
            .map_err(|_| HttpError::service_unavailable(id.clone()))?
        } else {
            None
        };
        let mut seen = HashSet::new();
        let mut profiles = Vec::new();
        for identity in input.identities {
            if viewer_profile
                .as_ref()
                .is_some_and(|profile| profile.identity == identity)
            {
                continue;
            }
            if !seen.insert((identity.kind, identity.id.clone())) {
                continue;
            }
            if let Some(found) = profile(state, viewer, workspace_id, identity)
                .await
                .map_err(|_| HttpError::service_unavailable(id.clone()))?
            {
                profiles.push(found);
            }
        }
        return Ok(Json(Resolved {
            viewer: viewer_profile,
            profiles,
        })
        .into_response());
    }

    if method != Method::GET && method != Method::HEAD {
        return Err(HttpError::not_found(id.clone()));
    }
    let mut parts = path.split('/');
    let (
        Some("_pioneer"),
        Some("profiles"),
        Some(kind),
        Some(actor_id),
        Some("avatar"),
        Some(revision),
        None,
    ) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    )
    else {
        return Err(HttpError::not_found(id.clone()));
    };
    let kind = match kind {
        "human" => IdentityType::Human,
        "agent" => IdentityType::Agent,
        _ => return Err(HttpError::not_found(id.clone())),
    };
    let identity = Identity {
        kind,
        id: actor_id.to_owned(),
    };
    if !identity.valid() || !valid_revision(revision) {
        return Err(HttpError::not_found(id.clone()));
    }
    let visible = profile(state, viewer, workspace_id, identity)
        .await
        .map_err(|_| HttpError::service_unavailable(id.clone()))?
        .is_some_and(|profile| profile.avatar_revision.as_deref() == Some(revision));
    if !visible {
        return Err(HttpError::not_found(id.clone()));
    }
    match kind {
        IdentityType::Human => {
            let principal_id = PrincipalId::new(actor_id.to_owned())
                .map_err(|_| HttpError::not_found(id.clone()))?;
            let snapshot = state
                .message_processor
                .member_service()
                .avatar_snapshot(viewer, &principal_id, Some(revision))
                .await
                .map_err(|error| map_avatar_error(error, id.clone()))?;
            avatar_response(snapshot, method.clone(), headers, id)
        }
        IdentityType::Agent => {
            let image =
                system_agent_avatar(revision).ok_or_else(|| HttpError::not_found(id.clone()))?;
            image_representation_response(
                image,
                ImageMediaType::Png,
                revision,
                method.clone(),
                headers,
                id,
            )
        }
    }
}
