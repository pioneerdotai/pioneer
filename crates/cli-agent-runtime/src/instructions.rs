use anyhow::{Result, bail};
use pioneer_protocol::CLIAgentRuntimeKind;
use sha2::{Digest, Sha256};
use std::fmt;

pub const MAX_CLI_ELEVATED_INSTRUCTION_BYTES: usize = 64 * 1024;

#[derive(Clone, PartialEq, Eq)]
pub struct CLIRuntimeElevatedInstructions {
    text: String,
    fingerprint: String,
}

impl CLIRuntimeElevatedInstructions {
    pub fn try_new(text: impl Into<String>, fingerprint: impl Into<String>) -> Result<Self> {
        let text = text.into();
        if text.trim().is_empty() {
            bail!("CLI elevated instructions cannot be empty");
        }
        if text.len() > MAX_CLI_ELEVATED_INSTRUCTION_BYTES {
            bail!(
                "CLI elevated instructions exceed the {} byte limit",
                MAX_CLI_ELEVATED_INSTRUCTION_BYTES
            );
        }
        let fingerprint = fingerprint.into();
        if fingerprint.len() != 64
            || !fingerprint
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("CLI elevated instruction fingerprint must be a SHA-256 hex digest");
        }
        let expected_fingerprint = Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        if !fingerprint.eq_ignore_ascii_case(expected_fingerprint.as_str()) {
            bail!("CLI elevated instruction fingerprint does not match its text");
        }
        Ok(Self { text, fingerprint })
    }

    pub fn text(&self) -> &str {
        self.text.as_str()
    }

    pub fn fingerprint(&self) -> &str {
        self.fingerprint.as_str()
    }
}

impl fmt::Debug for CLIRuntimeElevatedInstructions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CLIRuntimeElevatedInstructions")
            .field("text", &"[REDACTED]")
            .field("fingerprint", &self.fingerprint)
            .finish()
    }
}

/// ACP v1 has no elevated instruction channel. Carrying accepted context as
/// ordinary user content is a different delivery contract, even when its
/// text and immutable digest originate in the same instruction compiler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CLIRuntimeInstructionDelivery {
    Elevated(CLIRuntimeElevatedInstructions),
    UserContext(CLIUserContext),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CLIUserContext(CLIRuntimeElevatedInstructions);

impl CLIRuntimeInstructionDelivery {
    pub fn for_runtime(
        kind: CLIAgentRuntimeKind,
        text: impl Into<String>,
        fingerprint: impl Into<String>,
    ) -> Result<Self> {
        let validated = CLIRuntimeElevatedInstructions::try_new(text, fingerprint)?;
        Ok(match kind {
            CLIAgentRuntimeKind::Acp => Self::UserContext(CLIUserContext(validated)),
            CLIAgentRuntimeKind::Codex | CLIAgentRuntimeKind::Claude => Self::Elevated(validated),
        })
    }

    pub fn elevated(&self) -> Result<&CLIRuntimeElevatedInstructions> {
        match self {
            Self::Elevated(instructions) => Ok(instructions),
            Self::UserContext(_) => {
                bail!("ordinary user context cannot be delivered as elevated instructions")
            }
        }
    }

    pub fn text(&self) -> &str {
        match self {
            Self::Elevated(value) => value.text(),
            Self::UserContext(value) => value.0.text(),
        }
    }

    pub fn fingerprint(&self) -> &str {
        match self {
            Self::Elevated(value) => value.fingerprint(),
            Self::UserContext(value) => value.0.fingerprint(),
        }
    }

    pub fn transport(&self, kind: CLIAgentRuntimeKind) -> Result<&'static str> {
        match (kind, self) {
            (CLIAgentRuntimeKind::Acp, Self::UserContext(_)) => Ok("acp_user_context"),
            (_, Self::Elevated(_)) => CLIRuntimeElevatedInstructionTransport::for_runtime(kind)
                .map(CLIRuntimeElevatedInstructionTransport::as_str)
                .ok_or_else(|| anyhow::anyhow!("ACP has no elevated instruction transport")),
            _ => bail!("instruction delivery does not match the runtime transport"),
        }
    }
}

impl From<CLIRuntimeElevatedInstructions> for CLIRuntimeInstructionDelivery {
    fn from(value: CLIRuntimeElevatedInstructions) -> Self {
        Self::Elevated(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CLIRuntimeElevatedInstructionTransport {
    CodexTurnCollaborationMode,
    ClaudeAppendSystemPromptFile,
}

impl CLIRuntimeElevatedInstructionTransport {
    pub fn for_runtime(kind: CLIAgentRuntimeKind) -> Option<Self> {
        match kind {
            CLIAgentRuntimeKind::Codex => Some(Self::CodexTurnCollaborationMode),
            CLIAgentRuntimeKind::Claude => Some(Self::ClaudeAppendSystemPromptFile),
            CLIAgentRuntimeKind::Acp => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CodexTurnCollaborationMode => "codex_turn_collaboration_mode",
            Self::ClaudeAppendSystemPromptFile => "claude_append_system_prompt_file",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CLIRuntimeElevatedInstructionTransport, CLIRuntimeElevatedInstructions,
        MAX_CLI_ELEVATED_INSTRUCTION_BYTES,
    };
    use pioneer_protocol::CLIAgentRuntimeKind;
    use sha2::{Digest, Sha256};

    #[test]
    fn validates_and_redacts_elevated_instructions() {
        let text = "secret governing text";
        let fingerprint = Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let instructions = CLIRuntimeElevatedInstructions::try_new(text, fingerprint).unwrap();
        assert_eq!(instructions.text(), "secret governing text");
        let debug = format!("{instructions:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret governing text"));
    }

    #[test]
    fn rejects_empty_invalid_or_oversized_payloads() {
        assert!(CLIRuntimeElevatedInstructions::try_new("", "a".repeat(64)).is_err());
        assert!(CLIRuntimeElevatedInstructions::try_new("text", "not-a-digest").is_err());
        assert!(CLIRuntimeElevatedInstructions::try_new("text", "a".repeat(64)).is_err());
        assert!(
            CLIRuntimeElevatedInstructions::try_new(
                "x".repeat(MAX_CLI_ELEVATED_INSTRUCTION_BYTES + 1),
                "a".repeat(64)
            )
            .is_err()
        );
    }

    #[test]
    fn acp_context_cannot_claim_an_elevated_transport() {
        use super::CLIRuntimeInstructionDelivery;
        let text = "accepted context";
        let fingerprint = hex::encode(Sha256::digest(text.as_bytes()));
        let context =
            CLIRuntimeInstructionDelivery::for_runtime(CLIAgentRuntimeKind::Acp, text, fingerprint)
                .unwrap();
        assert!(context.elevated().is_err());
        assert_eq!(
            context.transport(CLIAgentRuntimeKind::Acp).unwrap(),
            "acp_user_context"
        );
        assert!(context.transport(CLIAgentRuntimeKind::Codex).is_err());
        assert_eq!(
            CLIRuntimeElevatedInstructionTransport::for_runtime(CLIAgentRuntimeKind::Acp),
            None
        );
    }

    #[test]
    fn runtime_transport_mapping_is_explicit() {
        assert_eq!(
            CLIRuntimeElevatedInstructionTransport::for_runtime(CLIAgentRuntimeKind::Codex),
            Some(CLIRuntimeElevatedInstructionTransport::CodexTurnCollaborationMode)
        );
        assert_eq!(
            CLIRuntimeElevatedInstructionTransport::for_runtime(CLIAgentRuntimeKind::Claude),
            Some(CLIRuntimeElevatedInstructionTransport::ClaudeAppendSystemPromptFile)
        );
    }
}
