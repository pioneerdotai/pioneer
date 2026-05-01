use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueLevel {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillValidationIssue {
    pub code: String,
    pub level: IssueLevel,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
}

impl SkillValidationIssue {
    pub fn error(
        code: impl Into<String>,
        message: impl Into<String>,
        field_path: Option<String>,
    ) -> Self {
        Self {
            code: code.into(),
            level: IssueLevel::Error,
            message: message.into(),
            field_path,
        }
    }

    pub fn warning(
        code: impl Into<String>,
        message: impl Into<String>,
        field_path: Option<String>,
    ) -> Self {
        Self {
            code: code.into(),
            level: IssueLevel::Warning,
            message: message.into(),
            field_path,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConformanceResult {
    pub compliant: bool,
    #[serde(default)]
    pub issues: Vec<SkillValidationIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SkillConformanceReport {
    pub agentskills_strict: ConformanceResult,
    pub openclaw_compat: ConformanceResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AllowedToolsInputKind {
    #[default]
    Missing,
    String,
    Sequence,
    Invalid,
}

#[derive(Debug, Clone)]
pub struct ValidationInput<'a> {
    pub name: Option<&'a str>,
    pub description: Option<&'a str>,
    pub license: Option<&'a str>,
    pub compatibility: Option<&'a str>,
    pub parent_directory_name: Option<&'a str>,
    pub allowed_tools_input_kind: AllowedToolsInputKind,
    pub metadata: &'a JsonValue,
    pub carried_issues: &'a [SkillValidationIssue],
}

pub fn build_conformance_report(input: ValidationInput<'_>) -> SkillConformanceReport {
    let mut strict = validate_agentskills_strict(&input);
    let mut openclaw = validate_openclaw_compat(&input);

    strict.issues.extend(input.carried_issues.iter().cloned());
    openclaw.issues.extend(input.carried_issues.iter().cloned());

    strict.issues = normalized_issue_list(strict.issues);
    strict.compliant = !strict
        .issues
        .iter()
        .any(|issue| matches!(issue.level, IssueLevel::Error));

    openclaw.issues = normalized_issue_list(openclaw.issues);
    openclaw.compliant = !openclaw
        .issues
        .iter()
        .any(|issue| matches!(issue.level, IssueLevel::Error));

    SkillConformanceReport {
        agentskills_strict: strict,
        openclaw_compat: openclaw,
    }
}

fn validate_agentskills_strict(input: &ValidationInput<'_>) -> ConformanceResult {
    let mut issues = Vec::new();

    let Some(name) = input.name.map(str::trim).filter(|value| !value.is_empty()) else {
        issues.push(SkillValidationIssue::error(
            "strict.name.required",
            "`name` is required by AgentSkills strict profile",
            Some("name".to_owned()),
        ));
        return ConformanceResult {
            compliant: false,
            issues,
        };
    };

    if name.chars().count() > 64 {
        issues.push(SkillValidationIssue::error(
            "strict.name.length",
            "`name` must be <= 64 characters",
            Some("name".to_owned()),
        ));
    }

    if name.starts_with('-') || name.ends_with('-') {
        issues.push(SkillValidationIssue::error(
            "strict.name.hyphen_bounds",
            "`name` must not start or end with `-`",
            Some("name".to_owned()),
        ));
    }

    if name.contains("--") {
        issues.push(SkillValidationIssue::error(
            "strict.name.consecutive_hyphen",
            "`name` must not contain consecutive hyphens",
            Some("name".to_owned()),
        ));
    }

    let Some(description) = input
        .description
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        issues.push(SkillValidationIssue::error(
            "strict.description.required",
            "`description` is required by AgentSkills strict profile",
            Some("description".to_owned()),
        ));
        return ConformanceResult {
            compliant: false,
            issues,
        };
    };

    if description.chars().count() > 1024 {
        issues.push(SkillValidationIssue::error(
            "strict.description.length",
            "`description` must be <= 1024 characters",
            Some("description".to_owned()),
        ));
    }

    if let Some(compatibility) = input.compatibility.map(str::trim) {
        if compatibility.is_empty() || compatibility.chars().count() > 500 {
            issues.push(SkillValidationIssue::error(
                "strict.compatibility.length",
                "`compatibility` must be 1..=500 characters when provided",
                Some("compatibility".to_owned()),
            ));
        }
    }

    if let Some(license) = input.license.map(str::trim)
        && license.is_empty()
    {
        issues.push(SkillValidationIssue::error(
            "strict.license.empty",
            "`license` must not be empty when provided",
            Some("license".to_owned()),
        ));
    }

    match input.allowed_tools_input_kind {
        AllowedToolsInputKind::Missing | AllowedToolsInputKind::String => {}
        AllowedToolsInputKind::Sequence | AllowedToolsInputKind::Invalid => {
            issues.push(SkillValidationIssue::error(
                "strict.allowed_tools.format",
                "`allowed-tools` must be a space-separated string in strict profile",
                Some("allowed-tools".to_owned()),
            ));
        }
    }

    match input.metadata {
        JsonValue::Object(map) => {
            for (key, value) in map {
                let allow_namespace_object =
                    (key == "openclaw" || key == "clawdbot") && value.is_object();

                if !value.is_string() && !allow_namespace_object {
                    issues.push(SkillValidationIssue::error(
                        "strict.metadata.value_type",
                        format!(
                            "`metadata.{key}` must be a string in strict profile (object is allowed only for `openclaw` and `clawdbot` namespaces)"
                        ),
                        Some(format!("metadata.{key}")),
                    ));
                }
            }
        }
        _ => {
            issues.push(SkillValidationIssue::error(
                "strict.metadata.type",
                "`metadata` must be a map in strict profile",
                Some("metadata".to_owned()),
            ));
        }
    }

    ConformanceResult {
        compliant: issues
            .iter()
            .all(|issue| !matches!(issue.level, IssueLevel::Error)),
        issues,
    }
}

fn validate_openclaw_compat(input: &ValidationInput<'_>) -> ConformanceResult {
    let mut issues = Vec::new();

    let Some(name) = input.name.map(str::trim).filter(|value| !value.is_empty()) else {
        issues.push(SkillValidationIssue::error(
            "openclaw.name.required",
            "`name` is required by OpenClaw compatibility profile",
            Some("name".to_owned()),
        ));
        return ConformanceResult {
            compliant: false,
            issues,
        };
    };

    if !name
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        issues.push(SkillValidationIssue::warning(
            "openclaw.name.snake_case",
            "`name` should be snake_case for OpenClaw compatibility",
            Some("name".to_owned()),
        ));
    }

    if input
        .description
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        issues.push(SkillValidationIssue::error(
            "openclaw.description.required",
            "`description` is required by OpenClaw compatibility profile",
            Some("description".to_owned()),
        ));
    }

    let metadata = match input.metadata {
        JsonValue::Object(map) => map,
        _ => {
            issues.push(SkillValidationIssue::error(
                "openclaw.metadata.type",
                "`metadata` must be an object for OpenClaw compatibility checks",
                Some("metadata".to_owned()),
            ));
            return ConformanceResult {
                compliant: false,
                issues,
            };
        }
    };

    validate_openclaw_namespace(metadata.get("openclaw"), &mut issues);
    validate_clawdbot_namespace(metadata.get("clawdbot"), &mut issues);

    ConformanceResult {
        compliant: issues
            .iter()
            .all(|issue| !matches!(issue.level, IssueLevel::Error)),
        issues,
    }
}

fn validate_openclaw_namespace(value: Option<&JsonValue>, issues: &mut Vec<SkillValidationIssue>) {
    let Some(value) = value else {
        return;
    };

    let Some(map) = value.as_object() else {
        issues.push(SkillValidationIssue::error(
            "openclaw.metadata.openclaw.type",
            "`metadata.openclaw` must be an object",
            Some("metadata.openclaw".to_owned()),
        ));
        return;
    };

    validate_string_array(map.get("os"), "metadata.openclaw.os", issues);

    if let Some(requires) = map.get("requires") {
        let Some(requires_map) = requires.as_object() else {
            issues.push(SkillValidationIssue::error(
                "openclaw.metadata.openclaw.requires.type",
                "`metadata.openclaw.requires` must be an object",
                Some("metadata.openclaw.requires".to_owned()),
            ));
            return;
        };

        validate_string_array(
            requires_map.get("bins"),
            "metadata.openclaw.requires.bins",
            issues,
        );
        validate_string_array(
            requires_map.get("config"),
            "metadata.openclaw.requires.config",
            issues,
        );
    }

    validate_optional_string(map.get("skillKey"), "metadata.openclaw.skillKey", issues);
    validate_optional_string(map.get("homepage"), "metadata.openclaw.homepage", issues);
}

fn validate_clawdbot_namespace(value: Option<&JsonValue>, issues: &mut Vec<SkillValidationIssue>) {
    let Some(value) = value else {
        return;
    };

    let Some(map) = value.as_object() else {
        issues.push(SkillValidationIssue::error(
            "openclaw.metadata.clawdbot.type",
            "`metadata.clawdbot` must be an object",
            Some("metadata.clawdbot".to_owned()),
        ));
        return;
    };

    validate_optional_string(map.get("emoji"), "metadata.clawdbot.emoji", issues);
    validate_optional_string(map.get("homepage"), "metadata.clawdbot.homepage", issues);

    if let Some(requires) = map.get("requires") {
        let Some(requires_map) = requires.as_object() else {
            issues.push(SkillValidationIssue::error(
                "openclaw.metadata.clawdbot.requires.type",
                "`metadata.clawdbot.requires` must be an object",
                Some("metadata.clawdbot.requires".to_owned()),
            ));
            return;
        };

        validate_string_array(
            requires_map.get("commands"),
            "metadata.clawdbot.requires.commands",
            issues,
        );
        validate_string_array(
            requires_map.get("bins"),
            "metadata.clawdbot.requires.bins",
            issues,
        );
    }
}

fn validate_optional_string(
    value: Option<&JsonValue>,
    field_path: &str,
    issues: &mut Vec<SkillValidationIssue>,
) {
    let Some(value) = value else {
        return;
    };

    if !value.is_string() {
        issues.push(SkillValidationIssue::error(
            format!("openclaw.{field_path}.type"),
            format!("`{field_path}` must be a string"),
            Some(field_path.to_owned()),
        ));
    }
}

fn validate_string_array(
    value: Option<&JsonValue>,
    field_path: &str,
    issues: &mut Vec<SkillValidationIssue>,
) {
    let Some(value) = value else {
        return;
    };

    let Some(items) = value.as_array() else {
        issues.push(SkillValidationIssue::error(
            format!("openclaw.{field_path}.type"),
            format!("`{field_path}` must be an array of strings"),
            Some(field_path.to_owned()),
        ));
        return;
    };

    for (index, item) in items.iter().enumerate() {
        if !item.is_string() {
            issues.push(SkillValidationIssue::error(
                format!("openclaw.{field_path}.item_type"),
                format!("`{field_path}[{index}]` must be a string"),
                Some(format!("{field_path}[{index}]")),
            ));
        }
    }
}

fn normalized_issue_list(mut issues: Vec<SkillValidationIssue>) -> Vec<SkillValidationIssue> {
    issues.sort_by(|left, right| {
        left.code
            .cmp(&right.code)
            .then_with(|| left.field_path.cmp(&right.field_path))
            .then_with(|| left.message.cmp(&right.message))
    });
    issues.dedup();
    issues
}

#[cfg(test)]
mod tests {
    use super::{
        AllowedToolsInputKind, IssueLevel, SkillValidationIssue, ValidationInput,
        build_conformance_report,
    };

    #[test]
    fn strict_profile_flags_list_allowed_tools() {
        let report = build_conformance_report(ValidationInput {
            name: Some("good-name"),
            description: Some("desc"),
            license: None,
            compatibility: None,
            parent_directory_name: Some("good-name"),
            allowed_tools_input_kind: AllowedToolsInputKind::Sequence,
            metadata: &serde_json::json!({}),
            carried_issues: &[],
        });

        assert!(!report.agentskills_strict.compliant);
        assert!(
            report
                .agentskills_strict
                .issues
                .iter()
                .any(|issue| issue.code == "strict.allowed_tools.format")
        );
    }

    #[test]
    fn openclaw_profile_accepts_metadata_shapes() {
        let report = build_conformance_report(ValidationInput {
            name: Some("hello_world"),
            description: Some("desc"),
            license: None,
            compatibility: None,
            parent_directory_name: Some("hello_world"),
            allowed_tools_input_kind: AllowedToolsInputKind::String,
            metadata: &serde_json::json!({
                "openclaw": {
                    "os": ["darwin"],
                    "requires": {
                        "bins": ["node"],
                        "config": ["my.key"]
                    },
                    "skillKey": "hello-world",
                    "homepage": "https://example.com"
                },
                "clawdbot": {
                    "emoji": "🌐",
                    "requires": {
                        "commands": ["agent-browser"]
                    }
                }
            }),
            carried_issues: &[],
        });

        assert!(report.openclaw_compat.compliant);
    }

    #[test]
    fn strict_profile_accepts_openclaw_and_clawdbot_metadata_namespaces() {
        let report = build_conformance_report(ValidationInput {
            name: Some("good-name"),
            description: Some("desc"),
            license: None,
            compatibility: None,
            parent_directory_name: Some("different-dir"),
            allowed_tools_input_kind: AllowedToolsInputKind::String,
            metadata: &serde_json::json!({
                "openclaw": {
                    "requires": { "bins": ["node"] }
                },
                "clawdbot": {
                    "requires": { "commands": ["agent-browser"] }
                }
            }),
            carried_issues: &[],
        });

        assert!(report.agentskills_strict.compliant);
        assert!(
            report
                .agentskills_strict
                .issues
                .iter()
                .all(|issue| issue.code != "strict.metadata.value_type")
        );
    }

    #[test]
    fn strict_profile_rejects_unknown_object_metadata_namespace() {
        let report = build_conformance_report(ValidationInput {
            name: Some("good-name"),
            description: Some("desc"),
            license: None,
            compatibility: None,
            parent_directory_name: Some("good-name"),
            allowed_tools_input_kind: AllowedToolsInputKind::String,
            metadata: &serde_json::json!({
                "custom": {
                    "nested": true
                }
            }),
            carried_issues: &[],
        });

        assert!(!report.agentskills_strict.compliant);
        assert!(
            report
                .agentskills_strict
                .issues
                .iter()
                .any(|issue| issue.code == "strict.metadata.value_type")
        );
    }

    #[test]
    fn carried_issues_make_profiles_non_compliant() {
        let carried = vec![SkillValidationIssue {
            code: "metadata.parse.invalid_json".to_owned(),
            level: IssueLevel::Error,
            message: "invalid json".to_owned(),
            field_path: Some("metadata".to_owned()),
        }];

        let report = build_conformance_report(ValidationInput {
            name: Some("good-name"),
            description: Some("desc"),
            license: None,
            compatibility: None,
            parent_directory_name: Some("good-name"),
            allowed_tools_input_kind: AllowedToolsInputKind::String,
            metadata: &serde_json::json!({}),
            carried_issues: carried.as_slice(),
        });

        assert!(!report.agentskills_strict.compliant);
        assert!(!report.openclaw_compat.compliant);
    }
}
