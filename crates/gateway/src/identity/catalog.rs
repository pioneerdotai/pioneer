use anyhow::{Context, Result};
use pioneer_crud::CliRuntimeIdentitySeed;

pub(crate) fn from_effective_settings(
    configured: Vec<pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig>,
    identity_settings: &pioneer_protocol::GatewayCliRuntimeSettings,
) -> Result<Vec<CliRuntimeIdentitySeed>> {
    configured
        .into_iter()
        .map(|instance| {
            let source_revision_material = serde_json::to_string(&instance)
                .context("failed to fingerprint canonical CLI runtime settings")?;
            Ok(CliRuntimeIdentitySeed {
                nickname: identity_settings
                    .instances
                    .iter()
                    .find(|configured| configured.id == instance.id)
                    .map(|configured| configured.nickname.clone())
                    .unwrap_or_else(|| instance.id.clone()),
                id: instance.id,
                kind: match instance.kind {
                    pioneer_config::GatewayCliAgentRuntimeKindConfig::Codex => "codex".to_owned(),
                    pioneer_config::GatewayCliAgentRuntimeKindConfig::Claude => "claude".to_owned(),
                },
                display_name: instance.display_name,
                enabled: instance.enabled,
                source_revision_material,
            })
        })
        .collect()
}
