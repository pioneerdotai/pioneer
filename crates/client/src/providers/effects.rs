//! Provider platform effects contain only Client-validated presentation data.
use crate::core::*;
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderPresentationIntent {
    CopyDiagnostics {
        workspace_id: String,
        runtime_id: String,
    },
    OpenPath {
        workspace_id: String,
        path: String,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind")]
pub enum ProviderPresentationEffect {
    #[serde(rename = "copy_provider_diagnostics")]
    CopyDiagnostics { value: String },
    #[serde(rename = "open_provider_path")]
    OpenPath { path: String },
}
impl ClientCore {
    pub fn provider_presentation_intent(
        &self,
        intent: ProviderPresentationIntent,
    ) -> ClientTransition {
        let workspace = match &intent {
            ProviderPresentationIntent::CopyDiagnostics { workspace_id, .. }
            | ProviderPresentationIntent::OpenPath { workspace_id, .. } => workspace_id,
        };
        let capabilities = self
            .authorization_snapshot(Some(workspace), None)
            .or_else(|| self.authorization_snapshot(None, None))
            .as_ref()
            .map(crate::authorization::principal_presentation_capabilities)
            .unwrap_or_default();
        if self.is_stopped() || !capabilities.can_manage_capabilities {
            return self.reject_intent();
        }
        let runtimes = self.provider_runtime_snapshot(workspace);
        let effect = match &intent {
            ProviderPresentationIntent::CopyDiagnostics { runtime_id, .. } => {
                let Some(runtime) = runtimes
                    .as_ref()
                    .and_then(|p| p.runtimes().iter().find(|r| &r.runtime_id == runtime_id))
                else {
                    return self.reject_intent();
                };
                ProviderPresentationEffect::CopyDiagnostics {
                    value: super::diagnostics::cli_runtime_provider_diagnostics_json(
                        runtime.runtime(),
                    ),
                }
            }
            ProviderPresentationIntent::OpenPath { path, .. } => {
                let valid = runtimes.as_ref().is_some_and(|p| {
                    p.runtimes().iter().any(|r| {
                        r.home_path.as_deref() == Some(path)
                            || r.shadow_home_path.as_deref() == Some(path)
                    })
                }) || self.gateway_settings().settings.as_ref().is_some_and(|s| {
                    s.cli_runtimes.instances.iter().any(|r| {
                        r.home_path == *path || r.shadow_home_path.as_deref() == Some(path)
                    })
                });
                if path.trim().is_empty() || !valid {
                    return self.reject_intent();
                }
                ProviderPresentationEffect::OpenPath { path: path.clone() }
            }
        };
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        owner.effect_generation = owner
            .effect_generation
            .checked_add(1)
            .expect("provider effect generation exhausted");
        let plan = ClientEffectPlan::new(
            ClientOperationId::new("providers/presentation").unwrap(),
            ClientGeneration::new(owner.effect_generation),
            ClientPlannedEffect::ProviderPresentation(effect),
        );
        self.transition(
            &ClientMutationAuthority { _private: () },
            vec![],
            vec![plan],
        )
    }
}
