use pioneer_config::{GatewaySelfImprovementConfig, GatewaySelfImprovementModelSelectionConfig};
use pioneer_provider::{ProviderCapabilities, ProviderRegistry};

const EXTERNAL_CLI_PROVIDER_PREFIX: &str = "cli_runtime:";

/// The authoritative Gateway view of desired self-improvement settings.
///
/// `default_model` and `reviewer_model_override` contain only selections that
/// resolve to a native API chat provider. `reviewer_model` is the effective
/// reviewer selection after exact default-model inheritance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthoritativeSelfImprovementSettings {
    pub desired_enabled: bool,
    pub effective_enabled: bool,
    pub default_model: Option<GatewaySelfImprovementModelSelectionConfig>,
    pub reviewer_model_override: Option<GatewaySelfImprovementModelSelectionConfig>,
    pub reviewer_model: Option<GatewaySelfImprovementModelSelectionConfig>,
}

impl AuthoritativeSelfImprovementSettings {
    pub(crate) fn desired_config(&self) -> GatewaySelfImprovementConfig {
        GatewaySelfImprovementConfig {
            enabled: self.desired_enabled,
            default_model: self.default_model.clone(),
            reviewer_model: self.reviewer_model_override.clone(),
        }
    }
}

/// Returns whether an already-resolved provider is eligible for Proposal 58.
///
/// The predicate is deliberately concrete: self-improvement accepts native
/// API chat providers only. Local endpoints and external CLI runtime IDs are
/// rejected even if they happen to expose a text-shaped provider adapter.
pub(crate) fn model_provider_is_eligible(
    provider_name: &str,
    capabilities: &ProviderCapabilities,
) -> bool {
    let provider_name = provider_name.trim();
    !provider_name.is_empty()
        && capabilities.input_types.text
        && !crate::secrets::is_local_provider(provider_name)
        && !is_external_cli_provider(provider_name)
}

pub(crate) fn resolve_authoritative_settings(
    desired: &GatewaySelfImprovementConfig,
    mut capabilities_for: impl FnMut(&str) -> Option<ProviderCapabilities>,
) -> AuthoritativeSelfImprovementSettings {
    let default_model = desired.default_model.as_ref().and_then(|selection| {
        eligible_selection(selection, &mut capabilities_for).then(|| selection.clone())
    });
    let reviewer_model_override = desired.reviewer_model.as_ref().and_then(|selection| {
        eligible_selection(selection, &mut capabilities_for).then(|| selection.clone())
    });
    let reviewer_model = reviewer_model_override
        .clone()
        .or_else(|| default_model.clone());

    AuthoritativeSelfImprovementSettings {
        desired_enabled: desired.enabled,
        effective_enabled: desired.enabled && default_model.is_some(),
        default_model,
        reviewer_model_override,
        reviewer_model,
    }
}

pub(crate) fn resolve_authoritative_settings_for_workspace(
    desired: &GatewaySelfImprovementConfig,
    provider_registry: &ProviderRegistry,
    workspace_id: Option<&str>,
) -> AuthoritativeSelfImprovementSettings {
    resolve_authoritative_settings(desired, |provider_name| {
        let provider = match workspace_id {
            Some(workspace_id) => {
                provider_registry.get_or_create_for_workspace(workspace_id, provider_name)
            }
            None => provider_registry.get_or_create(provider_name),
        }
        .ok()?;
        Some(provider.capabilities())
    })
}

pub(crate) fn validate_authoritative_selections_for_workspace(
    desired: &GatewaySelfImprovementConfig,
    provider_registry: &ProviderRegistry,
    workspace_id: Option<&str>,
) -> anyhow::Result<()> {
    for (field, selection) in [
        ("default_model", desired.default_model.as_ref()),
        ("reviewer_model", desired.reviewer_model.as_ref()),
    ] {
        let Some(selection) = selection else {
            continue;
        };
        let provider = match workspace_id {
            Some(workspace_id) => provider_registry
                .get_or_create_for_workspace(workspace_id, selection.provider.as_str()),
            None => provider_registry.get_or_create(selection.provider.as_str()),
        }
        .map_err(|_| {
            anyhow::anyhow!(
                "self-improvement {field} provider `{}` is not a native API chat provider",
                selection.provider
            )
        })?;
        if !model_provider_is_eligible(selection.provider.as_str(), &provider.capabilities()) {
            anyhow::bail!(
                "self-improvement {field} provider `{}` is not a native API chat provider",
                selection.provider
            );
        }
    }
    Ok(())
}

fn eligible_selection(
    selection: &GatewaySelfImprovementModelSelectionConfig,
    capabilities_for: &mut impl FnMut(&str) -> Option<ProviderCapabilities>,
) -> bool {
    !selection.provider.trim().is_empty()
        && !selection.model.trim().is_empty()
        && capabilities_for(selection.provider.as_str()).is_some_and(|capabilities| {
            model_provider_is_eligible(selection.provider.as_str(), &capabilities)
        })
}

fn is_external_cli_provider(provider_name: &str) -> bool {
    provider_name.starts_with(EXTERNAL_CLI_PROVIDER_PREFIX)
        || matches!(provider_name, "codex" | "claude")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use pioneer_provider::ProviderInputCapabilities;
    use pioneer_provider::providers::EchoProvider;

    fn model(provider: &str, model: &str) -> GatewaySelfImprovementModelSelectionConfig {
        GatewaySelfImprovementModelSelectionConfig {
            provider: provider.to_owned(),
            model: model.to_owned(),
        }
    }

    fn api_chat_capabilities() -> ProviderCapabilities {
        ProviderCapabilities {
            input_types: ProviderInputCapabilities::default(),
            ..ProviderCapabilities::default()
        }
    }

    #[test]
    fn default_settings_are_authoritatively_disabled_without_models() {
        let resolved =
            resolve_authoritative_settings(&GatewaySelfImprovementConfig::default(), |_| {
                Some(api_chat_capabilities())
            });

        assert!(!resolved.desired_enabled);
        assert!(!resolved.effective_enabled);
        assert!(resolved.default_model.is_none());
        assert!(resolved.reviewer_model_override.is_none());
        assert!(resolved.reviewer_model.is_none());
    }

    #[test]
    fn enabled_requires_eligible_default_and_reviewer_inherits_it_exactly() {
        let desired = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(model("openai", "gpt-5.4")),
            reviewer_model: None,
        };
        let resolved = resolve_authoritative_settings(&desired, |provider| {
            (provider == "openai").then(api_chat_capabilities)
        });

        assert!(resolved.effective_enabled);
        assert_eq!(resolved.default_model, desired.default_model);
        assert!(resolved.reviewer_model_override.is_none());
        assert_eq!(resolved.reviewer_model, desired.default_model);
    }

    #[test]
    fn local_cli_unknown_and_non_chat_providers_are_ineligible() {
        let non_chat = ProviderCapabilities {
            input_types: ProviderInputCapabilities {
                text: false,
                ..ProviderInputCapabilities::default()
            },
            ..ProviderCapabilities::default()
        };
        for provider in [
            "local",
            "ollama",
            "lmstudio",
            "cli_runtime:codex",
            "codex",
            "claude",
        ] {
            assert!(!model_provider_is_eligible(
                provider,
                &api_chat_capabilities()
            ));
        }
        assert!(!model_provider_is_eligible("openai", &non_chat));

        let desired = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(model("missing", "model")),
            reviewer_model: Some(model("openai", "reviewer")),
        };
        let resolved = resolve_authoritative_settings(&desired, |provider| {
            (provider == "openai").then(api_chat_capabilities)
        });
        assert!(!resolved.effective_enabled);
        assert!(resolved.default_model.is_none());
        assert_eq!(resolved.reviewer_model_override, desired.reviewer_model);
        assert_eq!(resolved.reviewer_model, desired.reviewer_model);
    }

    #[test]
    fn invalid_reviewer_override_is_removed_and_falls_back_to_default() {
        let desired = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(model("openai", "learner")),
            reviewer_model: Some(model("local", "reviewer")),
        };
        let resolved = resolve_authoritative_settings(&desired, |_| Some(api_chat_capabilities()));

        assert!(resolved.effective_enabled);
        assert!(resolved.reviewer_model_override.is_none());
        assert_eq!(resolved.reviewer_model, desired.default_model);
        assert_eq!(
            resolved.desired_config(),
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: desired.default_model,
                reviewer_model: None,
            }
        );
    }

    #[test]
    fn handcrafted_local_cli_and_unknown_selections_are_rejected_by_registry_policy() {
        let registry = ProviderRegistry::with_provider("openai", Arc::new(EchoProvider::new()));
        let valid = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(model("openai", "gpt-5.4")),
            reviewer_model: None,
        };
        validate_authoritative_selections_for_workspace(&valid, &registry, Some("workspace"))
            .expect("native API chat provider must be accepted");

        for provider in ["local", "ollama", "cli_runtime:codex", "not-a-provider"] {
            let invalid = GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(model(provider, "model")),
                reviewer_model: None,
            };
            let error = validate_authoritative_selections_for_workspace(
                &invalid,
                &registry,
                Some("workspace"),
            )
            .expect_err("non-API selection must be rejected");
            assert!(
                format!("{error:#}").contains("not a native API chat provider"),
                "unexpected eligibility error: {error:#}"
            );
        }
    }
}
