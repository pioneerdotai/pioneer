//! Shared conversion between the ordinary model selector and workspace settings.
use crate::{
    composer::model_selection::ModelSelectorSelection,
    providers::list::{cli_runtime_provider_key, runtime_id_from_cli_runtime_provider_key},
};
use pioneer_protocol::{
    CLIAgentRuntimeKind, GatewayCliRuntimeInstanceSettings, GatewayModelSelection,
    ModelSelectionTransport,
};

pub fn from_selector(
    selection: ModelSelectorSelection,
    runtimes: &[GatewayCliRuntimeInstanceSettings],
) -> Option<GatewayModelSelection> {
    let provider = selection.provider?;
    let model = selection.model?;
    let (transport, instance) =
        if let Some(id) = runtime_id_from_cli_runtime_provider_key(&provider) {
            let runtime = runtimes
                .iter()
                .find(|runtime| runtime.id == id && runtime.enabled)?;
            (
                match runtime.kind {
                    CLIAgentRuntimeKind::Codex => ModelSelectionTransport::Codex,
                    CLIAgentRuntimeKind::Claude => ModelSelectionTransport::Claude,
                    CLIAgentRuntimeKind::OpenCode => ModelSelectionTransport::OpenCode,
                },
                id.to_owned(),
            )
        } else {
            (ModelSelectionTransport::Api, provider)
        };
    GatewayModelSelection::Explicit {
        service_mode: selection.selected_service_mode,
        context_authority: selection.selected_context_authority,
        context_window: selection.selected_context_window,
        transport,
        instance,
        model,
        reasoning_effort: selection.selected_reasoning_effort,
    }
    .normalized()
    .ok()
}
pub fn to_selector(selection: &GatewayModelSelection) -> ModelSelectorSelection {
    match selection {
        GatewayModelSelection::Inherit => ModelSelectorSelection::default(),
        GatewayModelSelection::Explicit {
            service_mode,
            context_authority,
            context_window,
            transport,
            instance,
            model,
            reasoning_effort,
        } => ModelSelectorSelection {
            provider: Some(if *transport == ModelSelectionTransport::Api {
                instance.clone()
            } else {
                cli_runtime_provider_key(instance)
            }),
            model: Some(model.clone()),
            selected_reasoning_effort: reasoning_effort.clone(),
            selected_context_window: *context_window,
            selected_service_mode: service_mode.clone(),
            selected_context_authority: context_authority.clone(),
        },
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selector_roundtrip_retains_cli_kind_and_effort_and_rejects_missing_instance() {
        let runtime = GatewayCliRuntimeInstanceSettings::default_claude();
        let selection = GatewayModelSelection::Explicit {
            service_mode: None,
            context_authority: None,
            context_window: None,
            transport: ModelSelectionTransport::Claude,
            instance: runtime.id.clone(),
            model: "selected-model".into(),
            reasoning_effort: Some("high".into()),
        };
        assert_eq!(
            from_selector(to_selector(&selection), &[runtime]),
            Some(selection.clone())
        );
        assert_eq!(from_selector(to_selector(&selection), &[]), None);
        assert_eq!(
            to_selector(&GatewayModelSelection::Inherit),
            ModelSelectorSelection::default()
        );
    }
}
