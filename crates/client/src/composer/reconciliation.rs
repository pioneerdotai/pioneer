//! Pure reconciliation of long-lived composer intent with a server-owned
//! execution draft policy.

use crate::providers::list::runtime_id_from_cli_runtime_provider_key;
use pioneer_protocol::{
    AuthorizationExecutionDraftPolicyProjection, AuthorizationResourceSelector, TurnPermissionMode,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionDraftSelection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<TurnPermissionMode>,
    #[serde(default)]
    pub skill_ids: Vec<String>,
    #[serde(default)]
    pub mcp_server_ids: Vec<String>,
    #[serde(default)]
    pub has_attachments: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionDraftReconciliationKind {
    PolicyGeneration,
    Provider,
    Model,
    PermissionMode,
    Skill,
    McpServer,
    Attachment,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionDraftReconciliationReason {
    pub kind: ExecutionDraftReconciliationKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    pub reason: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionDraftReconciliation {
    pub draft: ExecutionDraftSelection,
    pub changed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<ExecutionDraftReconciliationReason>,
}

pub fn reconcile_execution_draft(
    draft: &ExecutionDraftSelection,
    policy: &AuthorizationExecutionDraftPolicyProjection,
) -> ExecutionDraftReconciliation {
    let mut next = draft.clone();
    let mut reasons = Vec::new();
    if draft.policy_fingerprint.as_deref() != Some(policy.fingerprint.as_str()) {
        reasons.push(reason(
            ExecutionDraftReconciliationKind::PolicyGeneration,
            None,
        ));
    }
    next.policy_fingerprint = Some(policy.fingerprint.clone());

    match next.provider.as_deref() {
        Some(provider) => {
            if let Some(runtime_id) = runtime_id_from_cli_runtime_provider_key(provider) {
                if !selector_allows(&policy.resources.cli_runtimes, runtime_id) {
                    reasons.push(reason(
                        ExecutionDraftReconciliationKind::Provider,
                        Some(provider.to_owned()),
                    ));
                    next.provider = None;
                    next.model = None;
                } else if let Some(model) = next.model.as_deref()
                    && !policy.resources.cli_models_all
                    && !policy
                        .resources
                        .cli_models
                        .iter()
                        .any(|grant| grant.runtime_id == runtime_id && grant.model == model)
                {
                    reasons.push(reason(
                        ExecutionDraftReconciliationKind::Model,
                        Some(model.to_owned()),
                    ));
                    next.model = None;
                }
            } else if !selector_allows(&policy.resources.providers, provider) {
                reasons.push(reason(
                    ExecutionDraftReconciliationKind::Provider,
                    Some(provider.to_owned()),
                ));
                next.provider = None;
                next.model = None;
            } else if let Some(model) = next.model.as_deref()
                && !policy.resources.provider_models_all
                && !policy
                    .resources
                    .provider_models
                    .iter()
                    .any(|grant| grant.provider == provider && grant.model == model)
            {
                reasons.push(reason(
                    ExecutionDraftReconciliationKind::Model,
                    Some(model.to_owned()),
                ));
                next.model = None;
            }
        }
        None if next.model.is_some() => {
            reasons.push(reason(
                ExecutionDraftReconciliationKind::Model,
                next.model.clone(),
            ));
            next.model = None;
        }
        None => {}
    }

    if next.permission_mode.is_none_or(|mode| {
        !policy
            .permission_options
            .iter()
            .any(|option| option.mode == mode)
    }) {
        if let Some(mode) = next.permission_mode {
            reasons.push(reason(
                ExecutionDraftReconciliationKind::PermissionMode,
                Some(mode.as_str().to_owned()),
            ));
        }
        next.permission_mode = policy.permission_options.last().map(|option| option.mode);
    }

    next.skill_ids.retain(|id| {
        let allowed = selector_allows(&policy.resources.skills, id);
        if !allowed {
            reasons.push(reason(
                ExecutionDraftReconciliationKind::Skill,
                Some(id.clone()),
            ));
        }
        allowed
    });
    next.mcp_server_ids.retain(|id| {
        let allowed = selector_allows(&policy.resources.mcp_servers, id);
        if !allowed {
            reasons.push(reason(
                ExecutionDraftReconciliationKind::McpServer,
                Some(id.clone()),
            ));
        }
        allowed
    });
    if next.has_attachments && !policy.can_attach_artifacts {
        reasons.push(reason(ExecutionDraftReconciliationKind::Attachment, None));
        next.has_attachments = false;
    }

    ExecutionDraftReconciliation {
        changed: &next != draft,
        draft: next,
        reasons,
    }
}

fn selector_allows(selector: &AuthorizationResourceSelector, id: &str) -> bool {
    selector.all || selector.ids.iter().any(|allowed| allowed == id)
}

fn reason(
    kind: ExecutionDraftReconciliationKind,
    resource_id: Option<String>,
) -> ExecutionDraftReconciliationReason {
    ExecutionDraftReconciliationReason {
        kind,
        resource_id,
        reason: "not_allowed_by_current_policy".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        AuthorizationCliModelGrant, AuthorizationOperationalResourceProjection,
        AuthorizationProviderModelGrant,
    };

    fn policy(
        resources: AuthorizationOperationalResourceProjection,
    ) -> AuthorizationExecutionDraftPolicyProjection {
        AuthorizationExecutionDraftPolicyProjection {
            fingerprint: "policy-v1".to_owned(),
            resources,
            permission_options: Vec::new(),
            can_attach_artifacts: false,
            mcp_invocation_limits: Default::default(),
        }
    }

    fn draft(provider: &str, model: &str) -> ExecutionDraftSelection {
        ExecutionDraftSelection {
            provider: Some(provider.to_owned()),
            model: Some(model.to_owned()),
            ..Default::default()
        }
    }

    #[test]
    fn empty_provider_model_grants_mean_none_unless_all_is_explicit() {
        let base = AuthorizationOperationalResourceProjection {
            fingerprint: "resources-v1".to_owned(),
            providers: AuthorizationResourceSelector {
                all: false,
                ids: vec!["provider-a".to_owned()],
            },
            ..Default::default()
        };

        let denied =
            reconcile_execution_draft(&draft("provider-a", "model-a"), &policy(base.clone()));
        assert_eq!(denied.draft.provider.as_deref(), Some("provider-a"));
        assert_eq!(denied.draft.model, None);

        let allowed = reconcile_execution_draft(
            &draft("provider-a", "model-a"),
            &policy(AuthorizationOperationalResourceProjection {
                provider_models_all: true,
                ..base
            }),
        );
        assert_eq!(allowed.draft.model.as_deref(), Some("model-a"));
    }

    #[test]
    fn exact_provider_and_cli_model_grants_preserve_only_the_matching_pair() {
        let provider_policy = policy(AuthorizationOperationalResourceProjection {
            fingerprint: "resources-provider".to_owned(),
            providers: AuthorizationResourceSelector {
                all: false,
                ids: vec!["provider-a".to_owned()],
            },
            provider_models: vec![AuthorizationProviderModelGrant {
                provider: "provider-a".to_owned(),
                model: "model-a".to_owned(),
            }],
            ..Default::default()
        });
        assert_eq!(
            reconcile_execution_draft(&draft("provider-a", "model-a"), &provider_policy)
                .draft
                .model
                .as_deref(),
            Some("model-a")
        );
        assert_eq!(
            reconcile_execution_draft(&draft("provider-a", "model-b"), &provider_policy)
                .draft
                .model,
            None
        );

        let cli_policy = policy(AuthorizationOperationalResourceProjection {
            fingerprint: "resources-cli".to_owned(),
            cli_runtimes: AuthorizationResourceSelector {
                all: false,
                ids: vec!["codex".to_owned()],
            },
            cli_models: vec![AuthorizationCliModelGrant {
                runtime_id: "codex".to_owned(),
                model: "model-a".to_owned(),
            }],
            ..Default::default()
        });
        assert_eq!(
            reconcile_execution_draft(&draft("cli_runtime:codex", "model-a"), &cli_policy)
                .draft
                .model
                .as_deref(),
            Some("model-a")
        );
        assert_eq!(
            reconcile_execution_draft(&draft("cli_runtime:codex", "model-b"), &cli_policy)
                .draft
                .model,
            None
        );
    }
}
