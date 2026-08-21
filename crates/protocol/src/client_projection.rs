//! Server-owned semantic projections shared by every Pioneer client.
//!
//! The client shells must render these values as received.  In particular, an
//! agent author is never inferred from a task, turn origin, provider, model, or
//! the currently signed-in principal.

use crate::{
    AgentDelegationRouteProjection, AgentExecutionId, AgentExecutionProfileProjection,
    AgentRouteAction, AgentRouteDisclosurePolicy,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CrossThreadSourceVisibility {
    Accessible,
    Inaccessible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SafeRouteProvenance {
    pub action: AgentRouteAction,
    pub visibility: CrossThreadSourceVisibility,
    /// Present only when the viewer has source read authority.  Source title,
    /// participants, prompts, and raw identifiers are never sent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_thread_label: Option<String>,
    pub disclosure: AgentRouteDisclosurePolicy,
}

impl SafeRouteProvenance {
    pub const fn delegated_action(action: AgentRouteAction) -> Self {
        Self {
            action,
            visibility: CrossThreadSourceVisibility::Inaccessible,
            source_thread_label: None,
            disclosure: AgentRouteDisclosurePolicy {
                text: false,
                artifacts: false,
                context: false,
                user_input: false,
                result_return: crate::AgentResultReturnPolicy::None,
            },
        }
    }

    pub fn from_route(
        route: &AgentDelegationRouteProjection,
        action: AgentRouteAction,
        source_read_authorized: bool,
        source_thread_label: Option<String>,
    ) -> Self {
        let accessible = source_read_authorized && route.permits(action, None);
        Self {
            action,
            visibility: if accessible {
                CrossThreadSourceVisibility::Accessible
            } else {
                CrossThreadSourceVisibility::Inaccessible
            },
            source_thread_label: accessible.then_some(source_thread_label).flatten(),
            disclosure: if accessible {
                route.disclosure
            } else {
                AgentRouteDisclosurePolicy::default()
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentWorkNodeState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentWorkNodeProjection {
    pub execution_id: AgentExecutionId,
    pub state: AgentWorkNodeState,
    pub progress_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentWorkGraphProjection {
    pub root_execution_id: AgentExecutionId,
    /// Monotonic persisted resource-state timestamp used only to reject stale
    /// graph projections racing with live queue/promotion/finalization events.
    pub updated_at_unix_micros: i64,
    pub queued_count: u64,
    pub running_count: u64,
    pub terminal_count: u64,
    /// True while one or more authorized nodes are durably queued for
    /// server-owned capacity. This is resource state, not an authorization
    /// failure and never implies that the whole graph is blocked.
    pub saturated: bool,
    /// Stable, bounded nodes in the exact root graph. No prompt, provider,
    /// model, thread title, runtime path, or other private payload is exposed.
    pub nodes: Vec<AgentWorkNodeProjection>,
}

/// The profile data a product surface may explain without exposing provider,
/// model, credentials, runtime IDs, or local paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SafeExecutionProfileMetadata {
    pub profile_id: String,
    pub backend: String,
    pub provider_label: Option<String>,
    pub model_label: Option<String>,
}

impl SafeExecutionProfileMetadata {
    pub fn from_projection(profile: &AgentExecutionProfileProjection) -> Self {
        let (backend, provider_label, model_label) = match &profile.backend {
            crate::AgentExecutionProfileBackend::ApiProvider => (
                "api".to_owned(),
                Some(profile.provider_display_name.clone()),
                Some(profile.model_display_name.clone()),
            ),
            crate::AgentExecutionProfileBackend::CliRuntime { .. } => {
                ("cli".to_owned(), None, None)
            }
            crate::AgentExecutionProfileBackend::AcpAgentRuntime { .. } => {
                ("acp".to_owned(), None, None)
            }
        };
        Self {
            profile_id: profile.id.as_str().to_owned(),
            backend,
            provider_label,
            model_label,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inaccessible_route_has_no_source_label_or_disclosure() {
        let route = SafeRouteProvenance::delegated_action(AgentRouteAction::SendMessage);
        assert!(route.source_thread_label.is_none());
        assert!(!route.disclosure.allows_anything());
    }
}
