use crate::compile::{SkillDefinition, SkillDependencySet};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum DependencyKind {
    Env,
    Bin,
    Command,
    Mcp,
    ApiKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum DependencyStatus {
    Satisfied,
    Missing,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyDiagnostic {
    pub kind: DependencyKind,
    pub name: String,
    pub status: DependencyStatus,
    pub hint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DependencyCheckResult {
    #[serde(default)]
    pub diagnostics: Vec<DependencyDiagnostic>,
}

impl DependencyCheckResult {
    pub fn has_failures(&self) -> bool {
        self.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.status,
                DependencyStatus::Missing | DependencyStatus::Blocked
            )
        })
    }

    pub fn failing_diagnostics(&self) -> Vec<DependencyDiagnostic> {
        self.diagnostics
            .iter()
            .filter(|diagnostic| {
                matches!(
                    diagnostic.status,
                    DependencyStatus::Missing | DependencyStatus::Blocked
                )
            })
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DependencyCheckInput {
    #[serde(default)]
    pub available_mcp: Vec<String>,
    #[serde(default)]
    pub blocked_env: Vec<String>,
    #[serde(default)]
    pub blocked_bins: Vec<String>,
    #[serde(default)]
    pub blocked_commands: Vec<String>,
    #[serde(default)]
    pub blocked_api_keys: Vec<String>,
    #[serde(default)]
    pub blocked_mcp: Vec<String>,
    #[serde(default)]
    pub path_override: Option<String>,
}

impl DependencyCheckInput {
    pub fn baseline() -> Self {
        Self::default()
    }

    pub fn with_available_mcp(available_mcp: Vec<String>) -> Self {
        let mut input = Self::baseline();
        input.available_mcp = available_mcp;
        input
    }
}

pub fn evaluate_skill_dependencies(
    skill: &SkillDefinition,
    input: &DependencyCheckInput,
) -> DependencyCheckResult {
    evaluate_dependency_set(&skill.dependencies, input)
}

pub fn evaluate_dependency_set(
    dependencies: &SkillDependencySet,
    input: &DependencyCheckInput,
) -> DependencyCheckResult {
    let available_mcp = normalize_set(input.available_mcp.as_slice());
    let blocked_env = normalize_set(input.blocked_env.as_slice());
    let blocked_bins = normalize_set(input.blocked_bins.as_slice());
    let blocked_commands = normalize_set(input.blocked_commands.as_slice());
    let blocked_api_keys = normalize_set(input.blocked_api_keys.as_slice());
    let blocked_mcp = normalize_set(input.blocked_mcp.as_slice());

    let mut diagnostics = Vec::new();

    for name in normalize_set(dependencies.env.as_slice()) {
        diagnostics.push(DependencyDiagnostic {
            kind: DependencyKind::Env,
            name: name.clone(),
            status: dependency_status(
                blocked_env.contains(name.as_str()),
                env_var_satisfied(name.as_str()),
            ),
            hint: format!("Set environment variable `{name}` with a non-empty value."),
        });
    }

    for name in normalize_set(dependencies.api_keys.as_slice()) {
        diagnostics.push(DependencyDiagnostic {
            kind: DependencyKind::ApiKey,
            name: name.clone(),
            status: dependency_status(
                blocked_api_keys.contains(name.as_str()),
                env_var_satisfied(name.as_str()),
            ),
            hint: format!("Provide API key in environment variable `{name}`."),
        });
    }

    for name in normalize_set(dependencies.bins.as_slice()) {
        diagnostics.push(DependencyDiagnostic {
            kind: DependencyKind::Bin,
            name: name.clone(),
            status: dependency_status(
                blocked_bins.contains(name.as_str()),
                binary_in_path(name.as_str(), input.path_override.as_deref()),
            ),
            hint: format!("Install `{name}` and ensure it is available in PATH."),
        });
    }

    for name in normalize_set(dependencies.commands.as_slice()) {
        diagnostics.push(DependencyDiagnostic {
            kind: DependencyKind::Command,
            name: name.clone(),
            status: dependency_status(
                blocked_commands.contains(name.as_str()),
                binary_in_path(name.as_str(), input.path_override.as_deref()),
            ),
            hint: format!("Install `{name}` command and ensure it is available in PATH."),
        });
    }

    for name in normalize_set(dependencies.mcp.as_slice()) {
        diagnostics.push(DependencyDiagnostic {
            kind: DependencyKind::Mcp,
            name: name.clone(),
            status: dependency_status(
                blocked_mcp.contains(name.as_str()),
                available_mcp.contains(name.as_str()),
            ),
            hint: format!("Register MCP server `{name}` in gateway configuration."),
        });
    }

    diagnostics.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.status.cmp(&right.status))
    });

    DependencyCheckResult { diagnostics }
}

fn dependency_status(blocked: bool, satisfied: bool) -> DependencyStatus {
    if blocked {
        return DependencyStatus::Blocked;
    }
    if satisfied {
        DependencyStatus::Satisfied
    } else {
        DependencyStatus::Missing
    }
}

fn env_var_satisfied(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn binary_in_path(binary: &str, path_override: Option<&str>) -> bool {
    let raw_path = match path_override {
        Some(path) => path.to_owned(),
        None => match std::env::var("PATH") {
            Ok(path) => path,
            Err(_) => return false,
        },
    };

    std::env::split_paths(raw_path.as_str()).any(|base| binary_in_directory(base, binary))
}

fn binary_in_directory(base: PathBuf, binary: &str) -> bool {
    let candidate = base.join(binary);
    if candidate.is_file() {
        return true;
    }

    #[cfg(windows)]
    {
        for ext in [".exe", ".cmd", ".bat"] {
            if base.join(format!("{binary}{ext}")).is_file() {
                return true;
            }
        }
    }

    false
}

fn normalize_set(values: &[String]) -> BTreeSet<String> {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>()
}

#[cfg(test)]
mod tests {
    use super::{DependencyCheckInput, DependencyKind, DependencyStatus, evaluate_dependency_set};
    use crate::compile::SkillDependencySet;

    fn empty_path() -> String {
        std::env::temp_dir()
            .join("pioneer-empty-path")
            .display()
            .to_string()
    }

    #[test]
    fn dependency_evaluator_reports_satisfied_and_missing() {
        let dependencies = SkillDependencySet {
            env: vec!["HOME".to_owned()],
            bins: vec!["definitely-missing-bin-phase3".to_owned()],
            commands: vec!["cargo".to_owned()],
            mcp: vec!["missing-mcp".to_owned()],
            api_keys: vec!["MISSING_PHASE3_API_KEY".to_owned()],
            config: Vec::new(),
        };

        let result = evaluate_dependency_set(
            &dependencies,
            &DependencyCheckInput {
                available_mcp: Vec::new(),
                blocked_env: Vec::new(),
                blocked_bins: Vec::new(),
                blocked_commands: Vec::new(),
                blocked_api_keys: Vec::new(),
                blocked_mcp: Vec::new(),
                path_override: Some(std::env::var("PATH").unwrap_or_default()),
            },
        );

        assert!(result.has_failures());
        assert!(
            result
                .diagnostics
                .iter()
                .any(|item| item.kind == DependencyKind::Env
                    && item.status == DependencyStatus::Satisfied)
        );
        assert!(result.diagnostics.iter().any(
            |item| item.kind == DependencyKind::Bin && item.status == DependencyStatus::Missing
        ));
        assert!(result.diagnostics.iter().any(
            |item| item.kind == DependencyKind::Mcp && item.status == DependencyStatus::Missing
        ));
        assert!(
            result
                .diagnostics
                .iter()
                .any(|item| item.kind == DependencyKind::ApiKey
                    && item.status == DependencyStatus::Missing)
        );
    }

    #[test]
    fn dependency_evaluator_reports_blocked_per_policy() {
        let dependencies = SkillDependencySet {
            env: vec!["HOME".to_owned()],
            bins: vec!["cargo".to_owned()],
            commands: vec!["cargo".to_owned()],
            mcp: vec!["server-a".to_owned()],
            api_keys: vec!["OPENAI_API_KEY".to_owned()],
            config: Vec::new(),
        };

        let result = evaluate_dependency_set(
            &dependencies,
            &DependencyCheckInput {
                available_mcp: vec!["server-a".to_owned()],
                blocked_env: vec!["HOME".to_owned()],
                blocked_bins: vec!["cargo".to_owned()],
                blocked_commands: vec!["cargo".to_owned()],
                blocked_api_keys: vec!["OPENAI_API_KEY".to_owned()],
                blocked_mcp: vec!["server-a".to_owned()],
                path_override: Some(empty_path()),
            },
        );

        assert!(result.has_failures());
        assert!(
            result
                .diagnostics
                .iter()
                .all(|item| item.status == DependencyStatus::Blocked)
        );
    }

    #[test]
    fn dependency_evaluator_is_deterministic() {
        let dependencies = SkillDependencySet {
            env: vec!["B".to_owned(), "A".to_owned(), "A".to_owned()],
            bins: vec!["z".to_owned(), "a".to_owned()],
            commands: vec!["cmd-b".to_owned(), "cmd-a".to_owned()],
            mcp: vec!["mcp-b".to_owned(), "mcp-a".to_owned()],
            api_keys: vec!["K2".to_owned(), "K1".to_owned()],
            config: Vec::new(),
        };

        let input = DependencyCheckInput {
            available_mcp: vec!["mcp-a".to_owned(), "mcp-b".to_owned()],
            blocked_env: Vec::new(),
            blocked_bins: Vec::new(),
            blocked_commands: Vec::new(),
            blocked_api_keys: Vec::new(),
            blocked_mcp: Vec::new(),
            path_override: Some(empty_path()),
        };

        let first = evaluate_dependency_set(&dependencies, &input);
        let second = evaluate_dependency_set(&dependencies, &input);
        assert_eq!(first, second);
    }
}
