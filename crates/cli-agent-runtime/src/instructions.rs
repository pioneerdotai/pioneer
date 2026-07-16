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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CLIRuntimeElevatedInstructionTransport {
    CodexDeveloperInstructions,
    ClaudeAppendSystemPromptFile,
}

impl CLIRuntimeElevatedInstructionTransport {
    pub fn for_runtime(kind: CLIAgentRuntimeKind) -> Self {
        match kind {
            CLIAgentRuntimeKind::Codex => Self::CodexDeveloperInstructions,
            CLIAgentRuntimeKind::Claude => Self::ClaudeAppendSystemPromptFile,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CodexDeveloperInstructions => "codex_developer_instructions",
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
    fn runtime_transport_mapping_is_explicit() {
        assert_eq!(
            CLIRuntimeElevatedInstructionTransport::for_runtime(CLIAgentRuntimeKind::Codex),
            CLIRuntimeElevatedInstructionTransport::CodexDeveloperInstructions
        );
        assert_eq!(
            CLIRuntimeElevatedInstructionTransport::for_runtime(CLIAgentRuntimeKind::Claude),
            CLIRuntimeElevatedInstructionTransport::ClaudeAppendSystemPromptFile
        );
    }
}
