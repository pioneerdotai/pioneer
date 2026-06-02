use crate::contract::{SkillDependencies, SkillSourceKind, SkillTrustLevel};
use crate::runtime::SkillRuntimeToolDefinition;
use crate::validation::{IssueLevel, SkillConformanceReport};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillIdentity {
    pub owner: String,
    pub slug: String,
    pub name: String,
    pub display_name: String,
    pub source_kind: SkillSourceKind,
    pub source_root: String,
    pub skill_dir: String,
    pub skill_file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_hint: Option<String>,
    pub fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillInstructions {
    pub description: String,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillRuntime {
    pub user_invocable: bool,
    pub disable_model_invocation: bool,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub runtime_tools: Vec<SkillRuntimeToolDefinition>,
    pub trust_level: SkillTrustLevel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SkillDependencySet {
    #[serde(default)]
    pub env: Vec<String>,
    #[serde(default)]
    pub bins: Vec<String>,
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub config: Vec<String>,
    #[serde(default)]
    pub mcp: Vec<String>,
    #[serde(default)]
    pub api_keys: Vec<String>,
}

impl SkillDependencySet {
    pub fn normalize(mut self) -> Self {
        self.env = normalize_string_list(self.env);
        self.bins = normalize_string_list(self.bins);
        self.commands = normalize_string_list(self.commands);
        self.config = normalize_string_list(self.config);
        self.mcp = normalize_string_list(self.mcp);
        self.api_keys = normalize_string_list(self.api_keys);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OpenClawMetadata {
    #[serde(default)]
    pub os: Vec<String>,
    #[serde(default)]
    pub requires_bins: Vec<String>,
    #[serde(default)]
    pub requires_config: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClawdbotMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    #[serde(default)]
    pub requires_commands: Vec<String>,
    #[serde(default)]
    pub requires_bins: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SkillKnownMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub openclaw: Option<OpenClawMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clawdbot: Option<ClawdbotMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SkillImplicitInvocationPolicy {
    #[default]
    UserControlled,
    Required,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SkillPolicyHints {
    #[serde(default)]
    pub implicit_invocation: SkillImplicitInvocationPolicy,
    pub activation_blocked: bool,
    #[serde(default)]
    pub block_issue_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillDefinition {
    pub identity: SkillIdentity,
    pub instructions: SkillInstructions,
    pub runtime: SkillRuntime,
    pub dependencies: SkillDependencySet,
    pub policy_hints: SkillPolicyHints,
    pub metadata_known: SkillKnownMetadata,
    pub metadata_raw: JsonValue,
    pub conformance: SkillConformanceReport,
}

#[derive(Debug, Clone)]
pub struct CompileSkillInput {
    pub owner: String,
    pub slug: String,
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub body: String,
    pub source_kind: SkillSourceKind,
    pub source_root: String,
    pub skill_dir: String,
    pub skill_file: String,
    pub version_hint: Option<String>,
    pub fingerprint: String,
    pub user_invocable: bool,
    pub disable_model_invocation: bool,
    pub paths: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub runtime_tools: Vec<SkillRuntimeToolDefinition>,
    pub trust_level: SkillTrustLevel,
    pub dependencies: SkillDependencies,
    pub license: Option<String>,
    pub compatibility: Option<String>,
    pub metadata_raw: JsonValue,
    pub conformance: SkillConformanceReport,
}

pub fn compile_skill_definition(input: CompileSkillInput) -> SkillDefinition {
    let metadata_known = extract_known_metadata(&input.metadata_raw);
    let dependencies = merge_dependencies(&input.dependencies, &metadata_known);
    let policy_hints = compile_policy_hints(&input.conformance);

    SkillDefinition {
        identity: SkillIdentity {
            owner: input.owner,
            slug: input.slug,
            name: input.name,
            display_name: input.display_name,
            source_kind: input.source_kind,
            source_root: input.source_root,
            skill_dir: input.skill_dir,
            skill_file: input.skill_file,
            version_hint: input.version_hint,
            fingerprint: input.fingerprint,
        },
        instructions: SkillInstructions {
            description: input.description,
            body: input.body,
            license: input.license,
            compatibility: input.compatibility,
        },
        runtime: SkillRuntime {
            user_invocable: input.user_invocable,
            disable_model_invocation: input.disable_model_invocation,
            paths: normalize_string_list(input.paths),
            allowed_tools: normalize_string_list(input.allowed_tools),
            runtime_tools: normalize_runtime_tools(input.runtime_tools),
            trust_level: input.trust_level,
        },
        dependencies,
        policy_hints,
        metadata_known,
        metadata_raw: input.metadata_raw,
        conformance: input.conformance,
    }
}

fn normalize_runtime_tools(
    mut tools: Vec<SkillRuntimeToolDefinition>,
) -> Vec<SkillRuntimeToolDefinition> {
    tools.sort_by(|left, right| {
        left.tool_slug
            .as_str()
            .cmp(right.tool_slug.as_str())
            .then_with(|| left.description.as_str().cmp(right.description.as_str()))
    });
    tools
}

fn normalize_string_list(values: Vec<String>) -> Vec<String> {
    let mut normalized = values
        .into_iter()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn extract_known_metadata(metadata: &JsonValue) -> SkillKnownMetadata {
    let Some(map) = metadata.as_object() else {
        return SkillKnownMetadata::default();
    };

    SkillKnownMetadata {
        openclaw: extract_openclaw_metadata(map.get("openclaw")),
        clawdbot: extract_clawdbot_metadata(map.get("clawdbot")),
    }
}

fn extract_openclaw_metadata(value: Option<&JsonValue>) -> Option<OpenClawMetadata> {
    let map = value?.as_object()?;

    let mut out = OpenClawMetadata {
        os: extract_string_array(map.get("os")),
        requires_bins: Vec::new(),
        requires_config: Vec::new(),
        skill_key: map
            .get("skillKey")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        homepage: map
            .get("homepage")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
    };

    if let Some(requires) = map.get("requires").and_then(JsonValue::as_object) {
        out.requires_bins = extract_string_array(requires.get("bins"));
        out.requires_config = extract_string_array(requires.get("config"));
    }

    out.os = normalize_string_list(out.os);
    out.requires_bins = normalize_string_list(out.requires_bins);
    out.requires_config = normalize_string_list(out.requires_config);

    if out.os.is_empty()
        && out.requires_bins.is_empty()
        && out.requires_config.is_empty()
        && out.skill_key.is_none()
        && out.homepage.is_none()
    {
        return None;
    }

    Some(out)
}

fn extract_clawdbot_metadata(value: Option<&JsonValue>) -> Option<ClawdbotMetadata> {
    let map = value?.as_object()?;

    let mut out = ClawdbotMetadata {
        emoji: map
            .get("emoji")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        homepage: map
            .get("homepage")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        requires_commands: Vec::new(),
        requires_bins: Vec::new(),
    };

    if let Some(requires) = map.get("requires").and_then(JsonValue::as_object) {
        out.requires_commands = extract_string_array(requires.get("commands"));
        out.requires_bins = extract_string_array(requires.get("bins"));
    }

    out.requires_commands = normalize_string_list(out.requires_commands);
    out.requires_bins = normalize_string_list(out.requires_bins);

    if out.emoji.is_none()
        && out.homepage.is_none()
        && out.requires_commands.is_empty()
        && out.requires_bins.is_empty()
    {
        return None;
    }

    Some(out)
}

fn extract_string_array(value: Option<&JsonValue>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };

    match value {
        JsonValue::String(single) => vec![single.trim().to_owned()],
        JsonValue::Array(values) => values
            .iter()
            .filter_map(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

fn merge_dependencies(
    base: &SkillDependencies,
    metadata: &SkillKnownMetadata,
) -> SkillDependencySet {
    let mut merged = SkillDependencySet {
        env: base.env.clone(),
        bins: base.bins.clone(),
        commands: base.commands.clone(),
        config: base.config.clone(),
        mcp: base.mcp.clone(),
        api_keys: base.api_keys.clone(),
    };

    if let Some(openclaw) = &metadata.openclaw {
        merged.bins.extend(openclaw.requires_bins.clone());
        merged.config.extend(openclaw.requires_config.clone());
    }

    if let Some(clawdbot) = &metadata.clawdbot {
        merged.commands.extend(clawdbot.requires_commands.clone());
        merged.bins.extend(clawdbot.requires_bins.clone());
    }

    merged.normalize()
}

fn compile_policy_hints(conformance: &SkillConformanceReport) -> SkillPolicyHints {
    let mut issue_codes = conformance
        .openclaw_compat
        .issues
        .iter()
        .filter(|issue| {
            matches!(issue.level, IssueLevel::Error)
                && (issue.code.starts_with("contract.metadata.")
                    || issue.code.starts_with("openclaw.metadata."))
        })
        .map(|issue| issue.code.clone())
        .collect::<Vec<_>>();

    issue_codes.sort();
    issue_codes.dedup();

    SkillPolicyHints {
        implicit_invocation: SkillImplicitInvocationPolicy::UserControlled,
        activation_blocked: !issue_codes.is_empty(),
        block_issue_codes: issue_codes,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CompileSkillInput, SkillDependencySet, SkillImplicitInvocationPolicy, SkillSourceKind,
        compile_skill_definition, normalize_string_list,
    };
    use crate::contract::{SkillDependencies, SkillTrustLevel, default_skill_conformance};

    #[test]
    fn normalize_string_list_deduplicates_and_sorts() {
        let normalized = normalize_string_list(vec![
            " node ".to_owned(),
            "cargo".to_owned(),
            "node".to_owned(),
            String::new(),
        ]);
        assert_eq!(normalized, vec!["cargo".to_owned(), "node".to_owned()]);
    }

    #[test]
    fn metadata_dependencies_are_merged_and_deduped() {
        let manifest = compile_skill_definition(CompileSkillInput {
            owner: "local".to_owned(),
            slug: "agent-browser".to_owned(),
            name: "agent-browser".to_owned(),
            display_name: "Agent Browser".to_owned(),
            description: "desc".to_owned(),
            body: "body".to_owned(),
            source_kind: SkillSourceKind::User,
            source_root: "/tmp".to_owned(),
            skill_dir: "/tmp/agent-browser".to_owned(),
            skill_file: "/tmp/agent-browser/SKILL.md".to_owned(),
            version_hint: None,
            fingerprint: "fp".to_owned(),
            user_invocable: true,
            disable_model_invocation: false,
            paths: vec!["src/**".to_owned()],
            allowed_tools: vec!["Read".to_owned()],
            runtime_tools: Vec::new(),
            trust_level: SkillTrustLevel::Community,
            dependencies: SkillDependencies {
                env: vec!["OPENAI_API_KEY".to_owned()],
                bins: vec!["node".to_owned()],
                commands: Vec::new(),
                config: Vec::new(),
                mcp: Vec::new(),
                api_keys: Vec::new(),
            },
            license: None,
            compatibility: None,
            metadata_raw: serde_json::json!({
                "openclaw": {
                    "requires": {
                        "bins": ["npm"],
                        "config": ["skill.browser"]
                    }
                },
                "clawdbot": {
                    "requires": {
                        "commands": ["agent-browser"],
                        "bins": ["node"]
                    }
                }
            }),
            conformance: default_skill_conformance(),
        });

        assert_eq!(
            manifest.dependencies,
            SkillDependencySet {
                env: vec!["OPENAI_API_KEY".to_owned()],
                bins: vec!["node".to_owned(), "npm".to_owned()],
                commands: vec!["agent-browser".to_owned()],
                config: vec!["skill.browser".to_owned()],
                mcp: Vec::new(),
                api_keys: Vec::new(),
            }
        );
    }

    #[test]
    fn metadata_parse_errors_block_activation() {
        let mut conformance = default_skill_conformance();
        conformance
            .openclaw_compat
            .issues
            .push(crate::validation::SkillValidationIssue::error(
                "contract.metadata.invalid_json",
                "metadata parse failed",
                Some("metadata".to_owned()),
            ));
        conformance.openclaw_compat.compliant = false;

        let manifest = compile_skill_definition(CompileSkillInput {
            owner: "local".to_owned(),
            slug: "bad".to_owned(),
            name: "bad".to_owned(),
            display_name: "bad".to_owned(),
            description: "desc".to_owned(),
            body: "body".to_owned(),
            source_kind: SkillSourceKind::User,
            source_root: "/tmp".to_owned(),
            skill_dir: "/tmp/bad".to_owned(),
            skill_file: "/tmp/bad/SKILL.md".to_owned(),
            version_hint: None,
            fingerprint: "fp".to_owned(),
            user_invocable: true,
            disable_model_invocation: false,
            paths: Vec::new(),
            allowed_tools: Vec::new(),
            runtime_tools: Vec::new(),
            trust_level: SkillTrustLevel::Community,
            dependencies: SkillDependencies::default(),
            license: None,
            compatibility: None,
            metadata_raw: serde_json::json!(null),
            conformance,
        });

        assert!(manifest.policy_hints.activation_blocked);
        assert_eq!(
            manifest.policy_hints.implicit_invocation,
            SkillImplicitInvocationPolicy::UserControlled
        );
        assert_eq!(
            manifest.policy_hints.block_issue_codes,
            vec!["contract.metadata.invalid_json".to_owned()]
        );
    }
}
