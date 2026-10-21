//! Shared, typed ACP session controls. Prompt execution stays in turn/start.
//! SDK descriptors are data; opaque ids never select a local adapter or policy.
use crate::acp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AcpConfigurationVersion {
    pub connection_id: String,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct AcpSessionParams {
    pub workspace_id: String,
    pub thread_id: String,
    pub runtime_id: String,
    pub operation: AcpSessionOperation,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AcpSessionOperation {
    Prepare {
        #[serde(default)]
        additional_directories: Vec<String>,
    },
    SetOption {
        session_id: String,
        expected: AcpConfigurationVersion,
        config_id: String,
        value: acp::SessionConfigOptionValue,
    },
    SetMode {
        session_id: String,
        expected: AcpConfigurationVersion,
        mode_id: String,
    },
    Authenticate {
        method_id: String,
    },
    TerminalAuthStart {
        expected_generation: u64,
        terminal_id: String,
        method_id: String,
        columns: u16,
        rows: u16,
    },
    TerminalAuthInput {
        terminal_id: String,
        data: String,
    },
    TerminalAuthOutput {
        terminal_id: String,
    },
    TerminalAuthClose {
        expected_generation: u64,
        terminal_id: String,
    },
    Logout,
    List {
        cursor: Option<String>,
        cwd: Option<String>,
    },
    Load {
        session_id: String,
    },
    Resume {
        session_id: String,
    },
    Delete {
        session_id: String,
    },
    Close {
        session_id: String,
    },
    Snapshot,
}

/// An actually applied choice, in receive order. The descriptor identity is
/// checked before replay; opaque ids never determine dependency ordering.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct AcpConfigurationChoice {
    pub descriptor_fingerprint: String,
    pub config_id: String,
    pub value: acp::SessionConfigOptionValue,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct AcpSessionSnapshot {
    #[serde(default)]
    pub initial_descriptor_fingerprint: String,
    #[serde(default)]
    pub configuration_replay: Vec<AcpConfigurationChoice>,
    #[serde(default)]
    pub continuation_requested: bool,
    pub workspace_id: String,
    pub thread_id: String,
    pub runtime_id: String,
    pub generation: u64,
    pub connected: bool,
    pub presentation_revision: u64,
    pub capabilities: acp::AgentCapabilities,
    pub auth_methods: Vec<AcpAuthMethod>,
    pub session_id: Option<String>,
    pub version: Option<AcpConfigurationVersion>,
    pub has_config_options: bool,
    // SDK 1.11's schema for the flattened kind drops currentValue/options.
    // Repair only the shared schema boundary; runtime serde remains the SDK.
    #[schemars(with = "Vec<AcpConfigOptionSchema>")]
    pub config_options: Vec<acp::SessionConfigOption>,
    pub modes: Option<acp::SessionModeState>,
    pub commands: Vec<acp::AvailableCommand>,
    pub session_info: Option<acp::SessionInfoUpdate>,
    pub usage: Option<acp::UsageUpdate>,
    pub notice: Option<acp::Notice>,
    pub completed_elicitations: Vec<String>,
    pub additional_directories: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct AcpSessionResponse {
    pub snapshot: AcpSessionSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

/// Selection captured from a version-checked preparation, then frozen with the
/// existing launch/input. The actual admitted session revalidates descriptor
/// identity and every opaque value before dispatch. No elevated instructions.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct AcpTurnConfiguration {
    #[serde(default)]
    pub initial_descriptor_fingerprint: String,
    #[serde(default)]
    pub configuration_replay: Vec<AcpConfigurationChoice>,
    #[serde(default)]
    pub continuation_requested: bool,
    pub session_id: String,
    pub expected: AcpConfigurationVersion,
    pub descriptor_fingerprint: String,
    pub values: BTreeMap<String, acp::SessionConfigOptionValue>,
    pub mode: Option<String>,
    #[serde(default)]
    pub additional_directories: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcpAuthMethodKind {
    Agent,
    Terminal,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AcpAuthMethod {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub kind: AcpAuthMethodKind,
}

/// Descriptor identity excludes current selections; a fresh session may start
/// at different defaults, but every offered id/value must still match exactly.
pub fn acp_descriptor_fingerprint(
    options: &[acp::SessionConfigOption],
    modes: Option<&acp::SessionModeState>,
) -> Result<String, serde_json::Error> {
    use sha2::{Digest, Sha256};
    let mut options = serde_json::to_value(options)?;
    if let Some(options) = options.as_array_mut() {
        for option in options {
            if let Some(option) = option.as_object_mut() {
                option.remove("currentValue");
            }
        }
    }
    let mut modes = serde_json::to_value(modes)?;
    if let Some(modes) = modes.as_object_mut() {
        modes.remove("currentModeId");
    }
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
        options, modes,
    ))?)))
}

// Auth input can contain credentials. Ordinary request diagnostics must not
// interpolate an operation payload (including PTY keystrokes).
impl std::fmt::Debug for AcpSessionOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AcpSessionOperation(<redacted>)")
    }
}

#[derive(JsonSchema)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct AcpConfigOptionSchema {
    id: String,
    name: String,
    description: Option<String>,
    category: Option<acp::SessionConfigOptionCategory>,
    #[serde(flatten)]
    kind: AcpConfigKindSchema,
    #[serde(rename = "_meta")]
    meta: Option<acp::Meta>,
}
#[derive(JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(dead_code)]
enum AcpConfigKindSchema {
    Select {
        #[serde(rename = "currentValue")]
        current_value: String,
        options: acp::SessionConfigSelectOptions,
    },
    Boolean {
        #[serde(rename = "currentValue")]
        current_value: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn descriptor_identity_excludes_values_but_preserves_opaque_offers() {
        let mut options: Vec<acp::SessionConfigOption> = serde_json::from_value(serde_json::json!([{"id":"opaque","name":"Choice","type":"select","currentValue":"a","options":[{"value":"a","name":"A"},{"value":"b","name":"B"}]}])).unwrap();
        let original = acp_descriptor_fingerprint(&options, None).unwrap();
        if let acp::SessionConfigKind::Select(select) = &mut options[0].kind {
            select.current_value = acp::SessionConfigValueId::new("b");
        }
        assert_eq!(
            original,
            acp_descriptor_fingerprint(&options, None).unwrap()
        );
        options[0].name = "Changed descriptor".into();
        assert_ne!(
            original,
            acp_descriptor_fingerprint(&options, None).unwrap()
        );
    }
    #[test]
    fn generated_config_schema_contains_values_and_options() {
        let schema = serde_json::to_value(schemars::schema_for!(AcpSessionSnapshot))
            .unwrap()
            .to_string();
        assert!(schema.contains("currentValue"));
        assert!(schema.contains("options"));
    }
}
