//! Validation for provider arguments owned by Pioneer's managed CLI boundary.

use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CLIRuntimeProvider {
    Codex,
    Claude,
}

impl CLIRuntimeProvider {
    fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedLaunchArgumentError {
    provider: CLIRuntimeProvider,
    argument_index: usize,
    reserved_option: &'static str,
    reason: &'static str,
}

impl ReservedLaunchArgumentError {
    fn new(
        provider: CLIRuntimeProvider,
        argument_index: usize,
        reserved_option: &'static str,
        reason: &'static str,
    ) -> Self {
        Self {
            provider,
            argument_index,
            reserved_option,
            reason,
        }
    }

    pub fn provider(&self) -> CLIRuntimeProvider {
        self.provider
    }

    pub fn argument_index(&self) -> usize {
        self.argument_index
    }

    pub fn reserved_option(&self) -> &'static str {
        self.reserved_option
    }
}

impl fmt::Display for ReservedLaunchArgumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} custom launch argument {} uses reserved option `{}`: {}",
            self.provider.as_str(),
            self.argument_index,
            self.reserved_option,
            self.reason
        )
    }
}

impl Error for ReservedLaunchArgumentError {}

pub fn validate_codex_custom_args(args: &[String]) -> Result<(), ReservedLaunchArgumentError> {
    let mut index = 0;
    while index < args.len() {
        let argument = args[index].as_str();
        let normalized = flag_name(argument);
        if let Some(reserved) = codex_reserved_standalone_flag(normalized) {
            return Err(ReservedLaunchArgumentError::new(
                CLIRuntimeProvider::Codex,
                index,
                reserved,
                "Pioneer owns the app-server transport and isolation boundary",
            ));
        }

        if matches!(normalized, "-c" | "--config") || argument.starts_with("-c=") {
            let override_value =
                inline_value(argument).or_else(|| args.get(index + 1).map(String::as_str));
            if override_value.is_none() {
                return Err(ReservedLaunchArgumentError::new(
                    CLIRuntimeProvider::Codex,
                    index,
                    "--config",
                    "a config override must include a key/value pair",
                ));
            }
            if codex_config_override_is_reserved(override_value.unwrap_or_default()) {
                return Err(ReservedLaunchArgumentError::new(
                    CLIRuntimeProvider::Codex,
                    index,
                    "--config",
                    "the override changes managed MCP, approval, sandbox, or config isolation",
                ));
            }
            if inline_value(argument).is_none() {
                index += 1;
            }
        } else if let Some(override_value) = argument.strip_prefix("-c") {
            if !override_value.is_empty() && codex_config_override_is_reserved(override_value) {
                return Err(ReservedLaunchArgumentError::new(
                    CLIRuntimeProvider::Codex,
                    index,
                    "--config",
                    "the override changes managed MCP, approval, sandbox, or config isolation",
                ));
            }
        }
        index += 1;
    }
    Ok(())
}

pub fn validate_claude_custom_args(args: &[String]) -> Result<(), ReservedLaunchArgumentError> {
    let mut index = 0;
    while index < args.len() {
        let argument = args[index].as_str();
        let normalized = flag_name(argument).to_ascii_lowercase();

        if (normalized.starts_with("-r") || normalized.starts_with("-c"))
            && !normalized.starts_with("--")
            && normalized.len() > 2
        {
            let reserved = if normalized.starts_with("-r") {
                "--resume"
            } else {
                "--continue"
            };
            return Err(ReservedLaunchArgumentError::new(
                CLIRuntimeProvider::Claude,
                index,
                reserved,
                "Pioneer owns the managed provider session boundary",
            ));
        }

        if let Some(reserved) = claude_reserved_flag(normalized.as_str()) {
            return Err(ReservedLaunchArgumentError::new(
                CLIRuntimeProvider::Claude,
                index,
                reserved,
                "Pioneer owns the managed MCP, permission, protocol, or session boundary",
            ));
        }

        if matches!(
            normalized.as_str(),
            "--disallowedtools" | "--disallowed-tools"
        ) {
            let conflicts = if let Some(inline) = inline_value(argument) {
                inline.to_ascii_lowercase().contains("mcp__pioneer")
            } else {
                let end = args[index + 1..]
                    .iter()
                    .position(|value| value.starts_with('-'))
                    .map(|offset| index + 1 + offset)
                    .unwrap_or(args.len());
                args[index + 1..end]
                    .iter()
                    .any(|value| value.to_ascii_lowercase().contains("mcp__pioneer"))
            };
            if conflicts {
                return Err(ReservedLaunchArgumentError::new(
                    CLIRuntimeProvider::Claude,
                    index,
                    "--disallowedTools",
                    "the value conflicts with the exact synthetic MCP projection",
                ));
            }
        }

        index += 1;
    }
    Ok(())
}

fn flag_name(argument: &str) -> &str {
    argument.split_once('=').map_or(argument, |(name, _)| name)
}

fn inline_value(argument: &str) -> Option<&str> {
    argument.split_once('=').map(|(_, value)| value)
}

fn codex_reserved_standalone_flag(flag: &str) -> Option<&'static str> {
    match flag {
        "--enable" => Some("--enable"),
        "--disable" => Some("--disable"),
        "--listen" => Some("--listen"),
        "--stdio" => Some("--stdio"),
        "--ws-auth" => Some("--ws-auth"),
        "--ws-token-file" => Some("--ws-token-file"),
        "--ws-token-sha256" => Some("--ws-token-sha256"),
        "--ws-shared-secret-file" => Some("--ws-shared-secret-file"),
        "--ws-issuer" => Some("--ws-issuer"),
        "--ws-audience" => Some("--ws-audience"),
        "--ws-max-clock-skew-seconds" => Some("--ws-max-clock-skew-seconds"),
        "daemon" => Some("daemon"),
        "proxy" => Some("proxy"),
        _ => None,
    }
}

fn codex_config_override_is_reserved(value: &str) -> bool {
    let key = value
        .split_once('=')
        .map_or(value, |(key, _)| key)
        .trim()
        .trim_matches(|character| matches!(character, '\'' | '"'))
        .to_ascii_lowercase()
        .replace('-', "_");
    [
        "mcp",
        "mcp_servers",
        "approval_policy",
        "projects",
        "sandbox",
        "sandbox_mode",
        "sandbox_permissions",
        "shell_environment_policy",
        "features.mcp",
        "features.apps",
        "features.enable_mcp_apps",
        "features.plugins",
        "features.remote_plugin",
        "features.skill_mcp_dependency_install",
        "features.skills",
    ]
    .iter()
    .any(|reserved| key == *reserved || key.starts_with(&format!("{reserved}.")))
}

fn claude_reserved_flag(flag: &str) -> Option<&'static str> {
    match flag {
        "--mcp-config" => Some("--mcp-config"),
        "--strict-mcp-config" => Some("--strict-mcp-config"),
        "--safe-mode" => Some("--safe-mode"),
        "--tools" => Some("--tools"),
        "--allowedtools" | "--allowed-tools" => Some("--allowedTools"),
        "--permission-prompt-tool" => Some("--permission-prompt-tool"),
        "--permission-mode" => Some("--permission-mode"),
        "--setting-sources" => Some("--setting-sources"),
        "--settings" => Some("--settings"),
        "--session-id" => Some("--session-id"),
        "--resume" | "-r" => Some("--resume"),
        "--continue" | "-c" => Some("--continue"),
        "--fork-session" => Some("--fork-session"),
        "--no-session-persistence" => Some("--no-session-persistence"),
        "--dangerously-skip-permissions" => Some("--dangerously-skip-permissions"),
        "--allow-dangerously-skip-permissions" => Some("--allow-dangerously-skip-permissions"),
        "--plugin-dir" => Some("--plugin-dir"),
        "--plugin-url" => Some("--plugin-url"),
        "--output-format" => Some("--output-format"),
        "--input-format" => Some("--input-format"),
        "--system-prompt" => Some("--system-prompt"),
        "--append-system-prompt" => Some("--append-system-prompt"),
        "--append-system-prompt-file" => Some("--append-system-prompt-file"),
        "--include-partial-messages" => Some("--include-partial-messages"),
        "--verbose" => Some("--verbose"),
        "--print" | "-p" => Some("--print"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn reserved_codex_arguments_cover_split_inline_alias_and_transport_forms() {
        for values in [
            args(&["-c", "mcp_servers.pioneer.enabled=true"]),
            args(&["--config=mcp_servers={}"]),
            args(&["-cmcp_servers.pioneer.enabled=true"]),
            args(&["--enable", "apps"]),
            args(&["--disable=skills"]),
            args(&["--listen", "ws://127.0.0.1:9000"]),
            args(&["--stdio"]),
            args(&["--ws-token-file=/tmp/token"]),
            args(&["daemon"]),
        ] {
            assert!(validate_codex_custom_args(&values).is_err(), "{values:?}");
        }
        for values in [
            args(&["-c", "model=\"gpt-5\""]),
            args(&["--config=model_reasoning_effort=\"high\""]),
            args(&["--strict-config"]),
        ] {
            validate_codex_custom_args(&values)
                .unwrap_or_else(|error| panic!("{values:?}: {error}"));
        }
    }

    #[test]
    fn reserved_claude_arguments_cover_aliases_and_inline_forms() {
        for values in [
            args(&["--mcp-config", "/tmp/config.json"]),
            args(&["--strict-mcp-config"]),
            args(&["--allowedTools=mcp__pioneer__tool"]),
            args(&["--allowed-tools", "Bash"]),
            args(&["--disallowedTools", "mcp__pioneer__tool"]),
            args(&["--permission-mode=bypassPermissions"]),
            args(&["--setting-sources", "project"]),
            args(&["--settings={}"]),
            args(&["--session-id", "00000000-0000-0000-0000-000000000000"]),
            args(&["--resume=thread"]),
            args(&["-r", "thread"]),
            args(&["-rthread"]),
            args(&["--continue"]),
            args(&["-c"]),
            args(&["-cconversation"]),
            args(&["--fork-session"]),
            args(&["--no-session-persistence"]),
            args(&["--dangerously-skip-permissions"]),
            args(&["--plugin-dir", "/tmp/plugin"]),
            args(&["--input-format", "text"]),
            args(&["--output-format=json"]),
            args(&["--system-prompt", "replacement"]),
            args(&["--append-system-prompt", "unmanaged"]),
            args(&["--append-system-prompt-file=/tmp/unmanaged.md"]),
        ] {
            assert!(validate_claude_custom_args(&values).is_err(), "{values:?}");
        }

        for values in [
            args(&["--model", "sonnet"]),
            args(&["--effort=high"]),
            args(&["--disallowed-tools", "Bash", "Edit"]),
            args(&["--max-budget-usd", "5"]),
        ] {
            validate_claude_custom_args(&values)
                .unwrap_or_else(|error| panic!("{values:?}: {error}"));
        }
    }

    #[test]
    fn reserved_argument_errors_never_echo_argument_values() {
        let canary = "proposal53-reserved-secret-canary";
        let error = validate_claude_custom_args(&args(&["--settings", canary]))
            .expect_err("settings must be reserved");
        assert!(!format!("{error:?}").contains(canary));
        assert!(!error.to_string().contains(canary));
    }
}
