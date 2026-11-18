//! Shared ACP descriptor and human-input presentation. Shells send intents and
//! retain only editable values; Gateway owns session, transport and authority.
use crate::core::ClientCore;
use anyhow::{Context, Result};
pub use pioneer_protocol::{
    AcpAuthMethodKind, AcpSessionOperation, AcpSessionParams, AcpSessionResponse,
    AcpSessionSnapshot, AcpTurnConfiguration, AgentExecutionBackend, TurnCLIRuntimeOptions, acp,
};

pub fn capture_configuration(snapshot: &AcpSessionSnapshot) -> Result<AcpTurnConfiguration> {
    let mut values = std::collections::BTreeMap::new();
    for option in &snapshot.config_options {
        let value = match &option.kind {
            acp::SessionConfigKind::Select(select) => {
                acp::SessionConfigOptionValue::value_id(select.current_value.clone())
            }
            acp::SessionConfigKind::Boolean(boolean) => {
                acp::SessionConfigOptionValue::boolean(boolean.current_value)
            }
            _ => anyhow::bail!("unsupported configuration control"),
        };
        values.insert(option.id.to_string(), value);
    }
    Ok(AcpTurnConfiguration {
        initial_descriptor_fingerprint: snapshot.initial_descriptor_fingerprint.clone(),
        configuration_replay: snapshot.configuration_replay.clone(),
        continuation_requested: snapshot.continuation_requested,
        session_id: snapshot
            .session_id
            .clone()
            .context("Prepare the agent session before sending")?,
        expected: snapshot
            .version
            .clone()
            .context("Agent configuration is unavailable")?,
        descriptor_fingerprint: pioneer_protocol::acp_descriptor_fingerprint(
            &snapshot.config_options,
            snapshot.modes.as_ref(),
        )?,
        values,
        mode: (!snapshot.has_config_options)
            .then(|| {
                snapshot
                    .modes
                    .as_ref()
                    .map(|modes| modes.current_mode_id.to_string())
            })
            .flatten(),
        additional_directories: snapshot.additional_directories.clone(),
    })
}

impl ClientCore {
    pub(crate) fn prepared_acp_provider(
        &self,
        workspace: &str,
        thread: &str,
        provider: &str,
    ) -> bool {
        crate::providers::list::runtime_id_from_cli_runtime_provider_key(provider)
            .and_then(|runtime| self.acp_session_snapshot(workspace, thread, runtime))
            .is_some_and(|snapshot| snapshot.connected && capture_configuration(&snapshot).is_ok())
    }

    pub(crate) fn acp_options_for_backend(
        &self,
        backend: Option<&AgentExecutionBackend>,
        workspace: &str,
        thread: &str,
    ) -> Result<Option<TurnCLIRuntimeOptions>> {
        let Some(AgentExecutionBackend::ACPAgentRuntime { runtime_id }) = backend else {
            return Ok(None);
        };
        let snapshot = self
            .acp_session_snapshot(workspace, thread, runtime_id)
            .context("Prepare the agent session and choose its parameters before sending")?;
        Ok(Some(TurnCLIRuntimeOptions {
            acp_configuration: Some(capture_configuration(&snapshot)?),
            sandbox: None,
            effort: None,
            personality: None,
            summary: None,
            steer_if_active: None,
        }))
    }
}

/// Allocate an invocation identity before a terminal side effect can begin.
/// Both shells retain it through their existing view lifecycle.
pub fn new_auth_terminal_id() -> String {
    pioneer_protocol::generate_id(32)
}
