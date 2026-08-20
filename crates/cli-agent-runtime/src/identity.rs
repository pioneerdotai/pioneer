//! Stable identity/profile binding for configured CLI runtime instances.
//!
//! A runtime process or session is not an actor.  The configured instance ID
//! is the stable source of the exact agent identity; process/session IDs are
//! only reconnect fences and are never projected to the model.

use pioneer_protocol::{
    AgentDisplayName, AgentExecutionProfileBackend, AgentExecutionProfileId,
    AgentExecutionProfileProjection, AgentIdentityId, AgentIdentityProjection,
    AgentIdentitySourceKind, AgentIdentityValidationError, AgentNicknameKey, CLIAgentRuntimeKind,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliRuntimeIdentityBindingError {
    EmptyRuntimeInstanceId,
    InvalidDisplayName(AgentIdentityValidationError),
    InvalidNickname(AgentIdentityValidationError),
    InvalidProfile(AgentExecutionProfileIdError),
    IdentityMismatch,
    ProfileMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentExecutionProfileIdError {
    Invalid(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CliRuntimeReconnectDecision {
    Resume,
    FenceStaleSession,
    DenyDisabled,
    DenyIdentityMismatch,
    DenyProfileMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliRuntimeExecutionBinding {
    pub runtime_instance_id: String,
    pub identity_id: AgentIdentityId,
    pub profile_id: AgentExecutionProfileId,
    pub session_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeAgentBinding {
    runtime_instance_id: String,
    kind: CLIAgentRuntimeKind,
    display_name: String,
    nickname: String,
    enabled: bool,
    source_revision: u64,
    identity_id: AgentIdentityId,
    source_fingerprint: String,
}

impl CliRuntimeAgentBinding {
    pub fn new(
        runtime_instance_id: impl Into<String>,
        kind: CLIAgentRuntimeKind,
        display_name: impl Into<String>,
        nickname: impl Into<String>,
        enabled: bool,
        source_revision: u64,
    ) -> Result<Self, CliRuntimeIdentityBindingError> {
        let runtime_instance_id = runtime_instance_id.into().trim().to_owned();
        if runtime_instance_id.is_empty() {
            return Err(CliRuntimeIdentityBindingError::EmptyRuntimeInstanceId);
        }
        let display_name = AgentDisplayName::new(display_name)
            .map_err(CliRuntimeIdentityBindingError::InvalidDisplayName)?
            .to_string();
        let nickname = AgentNicknameKey::new(nickname)
            .map_err(CliRuntimeIdentityBindingError::InvalidNickname)?
            .to_string();
        let identity_id = stable_identity_id(runtime_instance_id.as_str(), kind);
        let source_fingerprint = source_fingerprint(
            runtime_instance_id.as_str(),
            kind,
            display_name.as_str(),
            nickname.as_str(),
            source_revision,
        );
        Ok(Self {
            runtime_instance_id,
            kind,
            display_name,
            nickname,
            enabled,
            source_revision,
            identity_id,
            source_fingerprint,
        })
    }

    pub fn runtime_instance_id(&self) -> &str {
        self.runtime_instance_id.as_str()
    }

    pub fn identity_id(&self) -> &AgentIdentityId {
        &self.identity_id
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn allows_new_execution(&self) -> bool {
        self.enabled
    }

    /// Existing authored rows remain readable after disabling the runtime.
    pub const fn preserves_history_when_disabled() -> bool {
        true
    }

    pub fn identity_projection(&self) -> AgentIdentityProjection {
        AgentIdentityProjection {
            id: self.identity_id.clone(),
            source_kind: AgentIdentitySourceKind::CliRuntimeInstance,
            display_name: self.display_name.clone(),
            nickname: self.nickname.clone(),
            avatar_revision: None,
            role_label: Some(match self.kind {
                CLIAgentRuntimeKind::Codex => "Codex CLI".to_owned(),
                CLIAgentRuntimeKind::Claude => "Claude CLI".to_owned(),
            }),
            source_revision: self.source_revision,
            source_fingerprint: self.source_fingerprint.clone(),
        }
    }

    pub fn profile_is_bound(&self, profile: &AgentExecutionProfileProjection) -> bool {
        profile
            .compatible_agent_identity_ids
            .contains(&self.identity_id)
            && matches!(
                &profile.backend,
                AgentExecutionProfileBackend::CliRuntime {
                    runtime_instance_id
                } if runtime_instance_id == &self.runtime_instance_id
            )
    }

    pub fn bind_execution(
        &self,
        profile: &AgentExecutionProfileProjection,
        session_generation: u64,
    ) -> Result<CliRuntimeExecutionBinding, CliRuntimeIdentityBindingError> {
        if !self.enabled {
            return Err(CliRuntimeIdentityBindingError::ProfileMismatch);
        }
        if !self.profile_is_bound(profile) {
            return Err(CliRuntimeIdentityBindingError::ProfileMismatch);
        }
        Ok(CliRuntimeExecutionBinding {
            runtime_instance_id: self.runtime_instance_id.clone(),
            identity_id: self.identity_id.clone(),
            profile_id: profile.id.clone(),
            session_generation,
        })
    }

    pub fn reconnect(
        &self,
        persisted: &CliRuntimeExecutionBinding,
        observed_session_generation: u64,
        profile: &AgentExecutionProfileProjection,
    ) -> CliRuntimeReconnectDecision {
        if persisted.runtime_instance_id != self.runtime_instance_id
            || persisted.identity_id != self.identity_id
        {
            return CliRuntimeReconnectDecision::DenyIdentityMismatch;
        }
        if !self.profile_is_bound(profile) || persisted.profile_id != profile.id {
            return CliRuntimeReconnectDecision::DenyProfileMismatch;
        }
        if !self.enabled {
            return CliRuntimeReconnectDecision::DenyDisabled;
        }
        if observed_session_generation != persisted.session_generation {
            return CliRuntimeReconnectDecision::FenceStaleSession;
        }
        CliRuntimeReconnectDecision::Resume
    }
}

fn stable_identity_id(runtime_instance_id: &str, kind: CLIAgentRuntimeKind) -> AgentIdentityId {
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:cli-identity:v1\0");
    digest.update(runtime_kind_name(kind).as_bytes());
    digest.update([0]);
    digest.update(runtime_instance_id.as_bytes());
    let encoded = hex::encode(digest.finalize());
    AgentIdentityId::new(format!("C{}", &encoded[..20]))
        .expect("hashed CLI identity id is exactly 21 alphanumeric characters")
}

fn source_fingerprint(
    runtime_instance_id: &str,
    kind: CLIAgentRuntimeKind,
    display_name: &str,
    nickname: &str,
    source_revision: u64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:cli-source:v1\0");
    for value in [
        runtime_instance_id,
        runtime_kind_name(kind),
        display_name,
        nickname,
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    digest.update(source_revision.to_be_bytes());
    hex::encode(digest.finalize())
}

const fn runtime_kind_name(kind: CLIAgentRuntimeKind) -> &'static str {
    match kind {
        CLIAgentRuntimeKind::Codex => "codex",
        CLIAgentRuntimeKind::Claude => "claude",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(id: &str, kind: CLIAgentRuntimeKind, enabled: bool) -> CliRuntimeAgentBinding {
        CliRuntimeAgentBinding::new(id, kind, "Configured CLI", id, enabled, 1).unwrap()
    }

    fn profile(binding: &CliRuntimeAgentBinding) -> AgentExecutionProfileProjection {
        AgentExecutionProfileProjection {
            id: AgentExecutionProfileId::new("P12345678901234567890").unwrap(),
            compatible_agent_identity_ids: vec![binding.identity_id().clone()],
            backend: AgentExecutionProfileBackend::CliRuntime {
                runtime_instance_id: binding.runtime_instance_id().to_owned(),
            },
            provider_id: "server".to_owned(),
            model_id: "server".to_owned(),
            provider_display_name: "CLI".to_owned(),
            model_display_name: "CLI".to_owned(),
            allowed_reasoning: Vec::new(),
            allowed_permission_profiles: Vec::new(),
            catalog_generation: 1,
            policy_generation: 1,
            fingerprint: "profile".to_owned(),
        }
    }

    #[test]
    fn multiple_instances_have_distinct_stable_identities() {
        let first = binding("codex-personal", CLIAgentRuntimeKind::Codex, true);
        let second = binding("codex-work", CLIAgentRuntimeKind::Codex, true);
        let restart = binding("codex-personal", CLIAgentRuntimeKind::Codex, true);
        assert_ne!(first.identity_id(), second.identity_id());
        assert_eq!(first.identity_id(), restart.identity_id());
        assert_eq!(first.identity_projection().nickname, "codex-personal");
    }

    #[test]
    fn runtime_id_cannot_impersonate_another_identity_or_profile() {
        let first = binding("codex-one", CLIAgentRuntimeKind::Codex, true);
        let second = binding("codex-two", CLIAgentRuntimeKind::Codex, true);
        let profile = profile(&first);
        assert!(first.profile_is_bound(&profile));
        assert!(!second.profile_is_bound(&profile));
        let persisted = first.bind_execution(&profile, 7).unwrap();
        assert_eq!(
            second.reconnect(&persisted, 7, &profile),
            CliRuntimeReconnectDecision::DenyIdentityMismatch
        );
    }

    #[test]
    fn disabled_runtime_denies_new_execution_but_preserves_history_and_fences_reconnect() {
        let enabled = binding("claude-review", CLIAgentRuntimeKind::Claude, true);
        let profile = profile(&enabled);
        let persisted = enabled.bind_execution(&profile, 2).unwrap();
        let disabled = binding("claude-review", CLIAgentRuntimeKind::Claude, false);
        assert!(!disabled.allows_new_execution());
        assert!(CliRuntimeAgentBinding::preserves_history_when_disabled());
        assert_eq!(
            disabled.reconnect(&persisted, 2, &profile),
            CliRuntimeReconnectDecision::DenyDisabled
        );
    }

    #[test]
    fn reconnect_fences_stale_session_and_resumes_exact_persisted_binding() {
        let runtime = binding("codex-review", CLIAgentRuntimeKind::Codex, true);
        let profile = profile(&runtime);
        let persisted = runtime.bind_execution(&profile, 4).unwrap();
        assert_eq!(
            runtime.reconnect(&persisted, 3, &profile),
            CliRuntimeReconnectDecision::FenceStaleSession
        );
        assert_eq!(
            runtime.reconnect(&persisted, 4, &profile),
            CliRuntimeReconnectDecision::Resume
        );
    }
}
