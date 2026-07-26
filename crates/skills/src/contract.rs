use crate::compile::{
    CompileSkillInput, SkillDefinition, SkillImplicitInvocationPolicy, compile_skill_definition,
};
use crate::runtime::{
    DynamicToolOutputPolicyDeclaration, RuntimeExecutionClassHint, SkillRuntimeToolDefinition,
    SkillRuntimeToolKind,
};
use crate::validation::{
    AllowedToolsInputKind, ConformanceResult, SkillConformanceReport, SkillValidationIssue,
    ValidationInput, build_conformance_report,
};
use anyhow::{Context, Result, bail};
use pioneer_protocol::SkillId;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillSourceKind {
    System,
    User,
    Registry,
}

impl SkillSourceKind {
    pub fn as_db_value(&self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Registry => "registry",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillTrustLevel {
    Internal,
    Verified,
    Community,
    Untrusted,
}

impl Default for SkillTrustLevel {
    fn default() -> Self {
        Self::Community
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SkillDependencies {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillCatalogSnapshot {
    pub version: u64,
    pub generated_at_unix: i64,
    #[serde(default)]
    pub skills: Vec<SkillDefinition>,
}

#[derive(Debug, Default)]
struct ParsedFrontmatter {
    name: Option<String>,
    owner: Option<String>,
    description: Option<String>,
    slug: Option<String>,
    license: Option<String>,
    compatibility: Option<String>,
    version_hint: Option<String>,
    user_invocable: bool,
    disable_model_invocation: bool,
    paths: Vec<String>,
    allowed_tools: Vec<String>,
    allowed_tools_input_kind: AllowedToolsInputKind,
    runtime_tools: Vec<SkillRuntimeToolDefinition>,
    dependencies: SkillDependencies,
    implicit_invocation: SkillImplicitInvocationPolicy,
    catalog_hidden: bool,
    metadata: JsonValue,
    issues: Vec<SkillValidationIssue>,
}

#[derive(Debug, Clone, Default)]
struct ParsedSidecarMeta {
    owner: Option<String>,
    slug: Option<String>,
    version_hint: Option<String>,
    display_name: Option<String>,
}

/// Source-neutral identity and location data used while parsing one `SKILL.md`.
///
/// Filesystem callers populate the override fields from `_meta.json`. Inline
/// callers leave them empty and provide stable diagnostic labels instead of
/// synthetic paths.
#[derive(Debug, Clone)]
pub struct SkillMarkdownParseContext {
    pub skill_id: SkillId,
    pub source_kind: SkillSourceKind,
    pub source_root: String,
    pub skill_dir: String,
    pub skill_file: String,
    pub parent_directory_name: String,
    pub identity_owner_override: Option<String>,
    pub identity_slug_override: Option<String>,
    pub version_hint_override: Option<String>,
    pub display_name_override: Option<String>,
}

fn split_frontmatter(content: &str) -> (Option<&str>, &str) {
    let normalized = content.strip_prefix('\u{feff}').unwrap_or(content);
    if !normalized.starts_with("---\n") {
        return (None, normalized);
    }

    let rest = &normalized[4..];
    if let Some(end) = rest.find("\n---\n") {
        let frontmatter = &rest[..end];
        let body = &rest[end + 5..];
        return (Some(frontmatter), body);
    }

    if let Some(end) = rest.find("\n---") {
        let after = &rest[end + 4..];
        if after.is_empty() || after.starts_with('\n') {
            let frontmatter = &rest[..end];
            let body = after.strip_prefix('\n').unwrap_or(after);
            return (Some(frontmatter), body);
        }
    }

    (None, normalized)
}

fn yaml_lookup<'a>(map: &'a serde_yaml::Mapping, key: &str) -> Option<&'a serde_yaml::Value> {
    map.get(serde_yaml::Value::String(key.to_owned()))
}

fn parse_string_field(
    map: &serde_yaml::Mapping,
    key: &str,
    issues: &mut Vec<SkillValidationIssue>,
) -> Option<String> {
    let Some(value) = yaml_lookup(map, key) else {
        return None;
    };

    match value {
        serde_yaml::Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }
        serde_yaml::Value::Null => None,
        _ => {
            issues.push(SkillValidationIssue::error(
                format!("contract.{key}.type"),
                format!("`{key}` must be a string when provided"),
                Some(key.to_owned()),
            ));
            None
        }
    }
}

fn parse_bool_value(
    map: &serde_yaml::Mapping,
    key: &str,
    default: bool,
    issues: &mut Vec<SkillValidationIssue>,
) -> bool {
    let Some(value) = yaml_lookup(map, key) else {
        return default;
    };

    match value {
        serde_yaml::Value::Bool(flag) => *flag,
        _ => {
            issues.push(SkillValidationIssue::error(
                format!("contract.{key}.type"),
                format!("`{key}` must be a boolean when provided"),
                Some(key.to_owned()),
            ));
            default
        }
    }
}

fn parse_string_list_value(
    value: Option<&serde_yaml::Value>,
    field_path: &str,
    allow_single_string: bool,
    issues: &mut Vec<SkillValidationIssue>,
) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };

    match value {
        serde_yaml::Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return Vec::new();
            }
            if allow_single_string {
                vec![trimmed.to_owned()]
            } else {
                issues.push(SkillValidationIssue::error(
                    format!("contract.{field_path}.type"),
                    format!("`{field_path}` must be a list of strings"),
                    Some(field_path.to_owned()),
                ));
                Vec::new()
            }
        }
        serde_yaml::Value::Sequence(items) => items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| match item {
                serde_yaml::Value::String(text) => {
                    let trimmed = text.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_owned())
                    }
                }
                _ => {
                    issues.push(SkillValidationIssue::error(
                        format!("contract.{field_path}.item_type"),
                        format!("`{field_path}[{index}]` must be a string"),
                        Some(format!("{field_path}[{index}]")),
                    ));
                    None
                }
            })
            .collect(),
        serde_yaml::Value::Null => Vec::new(),
        _ => {
            issues.push(SkillValidationIssue::error(
                format!("contract.{field_path}.type"),
                format!("`{field_path}` must be a list of strings"),
                Some(field_path.to_owned()),
            ));
            Vec::new()
        }
    }
}

fn parse_allowed_tools(
    map: &serde_yaml::Mapping,
    issues: &mut Vec<SkillValidationIssue>,
) -> (Vec<String>, AllowedToolsInputKind) {
    let Some(value) = yaml_lookup(map, "allowed-tools") else {
        return (Vec::new(), AllowedToolsInputKind::Missing);
    };

    match value {
        serde_yaml::Value::String(text) => {
            let normalized = text
                .split_whitespace()
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            (normalized, AllowedToolsInputKind::String)
        }
        serde_yaml::Value::Sequence(_) => (
            parse_string_list_value(Some(value), "allowed-tools", false, issues),
            AllowedToolsInputKind::Sequence,
        ),
        serde_yaml::Value::Null => (Vec::new(), AllowedToolsInputKind::Missing),
        _ => {
            issues.push(SkillValidationIssue::error(
                "contract.allowed-tools.type",
                "`allowed-tools` must be a string or list of strings",
                Some("allowed-tools".to_owned()),
            ));
            (Vec::new(), AllowedToolsInputKind::Invalid)
        }
    }
}

fn parse_dependencies(
    map: &serde_yaml::Mapping,
    issues: &mut Vec<SkillValidationIssue>,
) -> SkillDependencies {
    let Some(value) = yaml_lookup(map, "dependencies") else {
        return SkillDependencies::default();
    };

    let Some(dep_map) = value.as_mapping() else {
        issues.push(SkillValidationIssue::error(
            "contract.dependencies.type",
            "`dependencies` must be a mapping",
            Some("dependencies".to_owned()),
        ));
        return SkillDependencies::default();
    };

    SkillDependencies {
        env: parse_string_list_value(
            yaml_lookup(dep_map, "env"),
            "dependencies.env",
            true,
            issues,
        ),
        bins: parse_string_list_value(
            yaml_lookup(dep_map, "bins"),
            "dependencies.bins",
            true,
            issues,
        ),
        commands: parse_string_list_value(
            yaml_lookup(dep_map, "commands"),
            "dependencies.commands",
            true,
            issues,
        ),
        config: parse_string_list_value(
            yaml_lookup(dep_map, "config"),
            "dependencies.config",
            true,
            issues,
        ),
        mcp: parse_string_list_value(
            yaml_lookup(dep_map, "mcp"),
            "dependencies.mcp",
            true,
            issues,
        ),
        api_keys: parse_string_list_value(
            yaml_lookup(dep_map, "api_keys"),
            "dependencies.api_keys",
            true,
            issues,
        ),
    }
}

fn parse_metadata(map: &serde_yaml::Mapping, issues: &mut Vec<SkillValidationIssue>) -> JsonValue {
    let Some(value) = yaml_lookup(map, "metadata") else {
        return serde_json::json!({});
    };

    match value {
        serde_yaml::Value::Null => serde_json::json!({}),
        serde_yaml::Value::Mapping(_) => {
            let json = serde_json::to_value(value).unwrap_or_else(|error| {
                issues.push(SkillValidationIssue::error(
                    "contract.metadata.serialize",
                    format!("failed to convert metadata mapping to JSON: {error}"),
                    Some("metadata".to_owned()),
                ));
                serde_json::json!({})
            });
            if !json.is_object() {
                issues.push(SkillValidationIssue::error(
                    "contract.metadata.type",
                    "`metadata` must decode into an object",
                    Some("metadata".to_owned()),
                ));
            }
            json
        }
        serde_yaml::Value::String(raw) => {
            let trimmed = raw.trim();
            let looks_like_json_object = trimmed.starts_with('{') && trimmed.ends_with('}');
            if !looks_like_json_object {
                issues.push(SkillValidationIssue::error(
                    "contract.metadata.type",
                    "`metadata` must be an object or JSON-object string",
                    Some("metadata".to_owned()),
                ));
                return JsonValue::String(raw.clone());
            }

            match serde_json::from_str::<JsonValue>(trimmed) {
                Ok(value) => {
                    if !value.is_object() {
                        issues.push(SkillValidationIssue::error(
                            "contract.metadata.type",
                            "`metadata` JSON string must decode into an object",
                            Some("metadata".to_owned()),
                        ));
                    }
                    value
                }
                Err(error) => {
                    issues.push(SkillValidationIssue::error(
                        "contract.metadata.invalid_json",
                        format!("failed to parse metadata JSON string: {error}"),
                        Some("metadata".to_owned()),
                    ));
                    JsonValue::String(raw.clone())
                }
            }
        }
        _ => {
            issues.push(SkillValidationIssue::error(
                "contract.metadata.type",
                "`metadata` must be an object or JSON-object string",
                Some("metadata".to_owned()),
            ));
            serde_json::to_value(value).unwrap_or_else(|_| JsonValue::Null)
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct FrontmatterRuntimeSection {
    #[serde(default)]
    tools: Vec<FrontmatterRuntimeTool>,
}

#[derive(Debug, Clone, Deserialize)]
struct FrontmatterRuntimeTool {
    tool_slug: String,
    #[serde(default)]
    description: Option<String>,
    kind: SkillRuntimeToolKind,
    #[serde(default = "default_runtime_parameters_schema")]
    parameters: serde_json::Value,
    #[serde(default)]
    execution_class: RuntimeExecutionClassHint,
    #[serde(default)]
    config: serde_json::Value,
    #[serde(default)]
    output_policy: Option<serde_yaml::Value>,
}

fn default_runtime_parameters_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": true
    })
}

fn parse_runtime_tools(
    map: &serde_yaml::Mapping,
    issues: &mut Vec<SkillValidationIssue>,
) -> Vec<SkillRuntimeToolDefinition> {
    let Some(runtime_value) = yaml_lookup(map, "runtime") else {
        return Vec::new();
    };

    let runtime = match serde_yaml::from_value::<FrontmatterRuntimeSection>(runtime_value.clone()) {
        Ok(runtime) => runtime,
        Err(error) => {
            issues.push(SkillValidationIssue::error(
                "contract.runtime.type",
                format!("`runtime` must decode as an object with optional `tools`: {error}"),
                Some("runtime".to_owned()),
            ));
            return Vec::new();
        }
    };

    runtime
        .tools
        .into_iter()
        .filter_map(|tool| {
            let tool_slug = normalize_skill_slug(tool.tool_slug.as_str());
            if tool_slug.is_empty() {
                issues.push(SkillValidationIssue::error(
                    "contract.runtime.tools.tool_slug",
                    "runtime tool slug must not be empty",
                    Some("runtime.tools[].tool_slug".to_owned()),
                ));
                return None;
            }

            let description = tool
                .description
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| format!("Runtime tool `{tool_slug}`"));

            let output_policy = match tool.output_policy {
                Some(raw_policy) => {
                    match serde_yaml::from_value::<DynamicToolOutputPolicyDeclaration>(raw_policy) {
                        Ok(policy) => Some(policy),
                        Err(error) => {
                            issues.push(SkillValidationIssue::error(
                                "contract.runtime.tools.output_policy",
                                format!(
                                    "runtime tool `{tool_slug}` has invalid output_policy: {error}"
                                ),
                                Some(format!("runtime.tools[{tool_slug}].output_policy")),
                            ));
                            return None;
                        }
                    }
                }
                None => None,
            };

            Some(SkillRuntimeToolDefinition {
                tool_slug,
                description,
                kind: tool.kind,
                parameters: tool.parameters,
                execution_class: tool.execution_class,
                config: tool.config,
                output_policy,
            })
        })
        .collect()
}

fn parse_implicit_invocation_policy(
    map: &serde_yaml::Mapping,
    issues: &mut Vec<SkillValidationIssue>,
) -> SkillImplicitInvocationPolicy {
    let Some(value) = yaml_lookup(map, "implicit-invocation") else {
        return SkillImplicitInvocationPolicy::UserControlled;
    };

    match value {
        serde_yaml::Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "required" => SkillImplicitInvocationPolicy::Required,
            "user-controlled" | "user_controlled" | "optional" => {
                SkillImplicitInvocationPolicy::UserControlled
            }
            _ => {
                issues.push(SkillValidationIssue::error(
                    "contract.implicit-invocation.value",
                    "`implicit-invocation` must be `required` or `user-controlled`",
                    Some("implicit-invocation".to_owned()),
                ));
                SkillImplicitInvocationPolicy::UserControlled
            }
        },
        serde_yaml::Value::Null => SkillImplicitInvocationPolicy::UserControlled,
        _ => {
            issues.push(SkillValidationIssue::error(
                "contract.implicit-invocation.type",
                "`implicit-invocation` must be a string when provided",
                Some("implicit-invocation".to_owned()),
            ));
            SkillImplicitInvocationPolicy::UserControlled
        }
    }
}

fn parse_catalog_hidden(map: &serde_yaml::Mapping, issues: &mut Vec<SkillValidationIssue>) -> bool {
    parse_bool_value(map, "catalog-hide", false, issues)
}

fn normalize_plain_description_scalar(frontmatter: &str) -> Option<String> {
    let mut candidate = None;

    for (index, line) in frontmatter.lines().enumerate() {
        let Some(value) = line.strip_prefix("description:") else {
            continue;
        };
        let value = value.trim();
        let is_explicit_yaml_scalar = value.starts_with(['\'', '"', '|', '>']);
        let contains_mapping_separator = value.contains(": ") || value.contains(":\t");
        if value.is_empty() || is_explicit_yaml_scalar || !contains_mapping_separator {
            return None;
        }
        if candidate.replace((index, value)).is_some() {
            return None;
        }
    }

    let (candidate_index, candidate_value) = candidate?;
    let mut normalized = String::with_capacity(frontmatter.len() + 8);
    for (index, line) in frontmatter.lines().enumerate() {
        if index == candidate_index {
            normalized.push_str("description: >-\n  ");
            normalized.push_str(candidate_value);
        } else {
            normalized.push_str(line);
        }
        normalized.push('\n');
    }
    Some(normalized)
}

/// Canonicalizes one compatibility case without mutating the caller's source:
/// an otherwise valid frontmatter whose top-level, unquoted `description` contains `: `.
/// Strict-valid input is left unchanged and unrelated YAML errors remain errors.
pub fn normalize_skill_markdown_plain_description(content: &str) -> Result<Option<String>> {
    let normalized_content = content.replace("\r\n", "\n");
    let (frontmatter, body) = split_frontmatter(normalized_content.as_str());
    let Some(frontmatter) = frontmatter else {
        return Ok(None);
    };

    let strict_error = match serde_yaml::from_str::<serde_yaml::Value>(frontmatter) {
        Ok(_) => return Ok(None),
        Err(error) => error,
    };
    let Some(normalized_frontmatter) = normalize_plain_description_scalar(frontmatter) else {
        return Err(strict_error).context("failed to parse YAML frontmatter");
    };
    if serde_yaml::from_str::<serde_yaml::Value>(normalized_frontmatter.as_str()).is_err() {
        return Err(strict_error).context("failed to parse YAML frontmatter");
    }

    Ok(Some(format!("---\n{normalized_frontmatter}---\n{body}")))
}

fn parse_frontmatter(frontmatter: Option<&str>) -> Result<ParsedFrontmatter> {
    let mut parsed = ParsedFrontmatter {
        user_invocable: true,
        disable_model_invocation: false,
        implicit_invocation: SkillImplicitInvocationPolicy::UserControlled,
        catalog_hidden: false,
        metadata: serde_json::json!({}),
        ..ParsedFrontmatter::default()
    };

    let Some(frontmatter) = frontmatter else {
        return Ok(parsed);
    };

    let root = serde_yaml::from_str::<serde_yaml::Value>(frontmatter)
        .context("failed to parse YAML frontmatter")?;
    let map = root
        .as_mapping()
        .context("YAML frontmatter root must be a mapping")?;

    parsed.name = parse_string_field(map, "name", &mut parsed.issues);
    parsed.owner = parse_string_field(map, "owner", &mut parsed.issues);
    parsed.description = parse_string_field(map, "description", &mut parsed.issues);
    parsed.slug = parse_string_field(map, "slug", &mut parsed.issues);
    parsed.license = parse_string_field(map, "license", &mut parsed.issues);
    parsed.compatibility = parse_string_field(map, "compatibility", &mut parsed.issues);
    parsed.version_hint = parse_string_field(map, "version", &mut parsed.issues);

    parsed.user_invocable = parse_bool_value(map, "user-invocable", true, &mut parsed.issues);
    parsed.disable_model_invocation =
        parse_bool_value(map, "disable-model-invocation", false, &mut parsed.issues);

    parsed.paths =
        parse_string_list_value(yaml_lookup(map, "paths"), "paths", true, &mut parsed.issues);

    let (allowed_tools, allowed_tools_input_kind) = parse_allowed_tools(map, &mut parsed.issues);
    parsed.allowed_tools = allowed_tools;
    parsed.allowed_tools_input_kind = allowed_tools_input_kind;

    parsed.dependencies = parse_dependencies(map, &mut parsed.issues);
    parsed.runtime_tools = parse_runtime_tools(map, &mut parsed.issues);
    parsed.implicit_invocation = parse_implicit_invocation_policy(map, &mut parsed.issues);
    parsed.catalog_hidden = parse_catalog_hidden(map, &mut parsed.issues);

    parsed.metadata = parse_metadata(map, &mut parsed.issues);

    Ok(parsed)
}

fn normalize_implicit_invocation_policy_for_source(
    source_kind: &SkillSourceKind,
    frontmatter: &mut ParsedFrontmatter,
) {
    if !matches!(
        frontmatter.implicit_invocation,
        SkillImplicitInvocationPolicy::Required
    ) {
        return;
    }

    if !matches!(source_kind, SkillSourceKind::System) {
        frontmatter.issues.push(SkillValidationIssue::warning(
            "contract.implicit-invocation.source_kind",
            "`implicit-invocation: required` is supported only for system skills",
            Some("implicit-invocation".to_owned()),
        ));
        frontmatter.implicit_invocation = SkillImplicitInvocationPolicy::UserControlled;
        return;
    }

    if !frontmatter.user_invocable {
        frontmatter.issues.push(SkillValidationIssue::error(
            "contract.implicit-invocation.user_invocable_conflict",
            "`implicit-invocation: required` requires `user-invocable` to be true",
            Some("implicit-invocation".to_owned()),
        ));
    }

    if frontmatter.disable_model_invocation {
        frontmatter.issues.push(SkillValidationIssue::error(
            "contract.implicit-invocation.disable_model_invocation_conflict",
            "`implicit-invocation: required` cannot be combined with `disable-model-invocation: true`",
            Some("implicit-invocation".to_owned()),
        ));
    }
}

pub fn normalize_skill_slug(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut previous_dash = false;

    for ch in input.chars() {
        let keep = ch.is_ascii_alphanumeric();
        if keep {
            out.push(ch.to_ascii_lowercase());
            previous_dash = false;
            continue;
        }

        if !previous_dash {
            out.push('-');
            previous_dash = true;
        }
    }

    out.trim_matches('-').to_owned()
}

fn normalize_owner(input: &str) -> String {
    normalize_skill_slug(input)
}

fn parse_sidecar_meta(skill_dir: &Path) -> Result<ParsedSidecarMeta> {
    let meta_file = skill_dir.join("_meta.json");
    if !meta_file.is_file() {
        return Ok(ParsedSidecarMeta::default());
    }

    let raw = fs::read_to_string(meta_file.as_path())
        .with_context(|| format!("failed to read `_meta.json` at `{}`", meta_file.display()))?;
    let json = serde_json::from_str::<JsonValue>(raw.as_str())
        .with_context(|| format!("failed to parse `_meta.json` at `{}`", meta_file.display()))?;
    let Some(map) = json.as_object() else {
        bail!(
            "`_meta.json` at `{}` must contain a JSON object",
            meta_file.display()
        );
    };

    let owner = map
        .get("owner")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            map.get("ownerId")
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        });

    let slug = map
        .get("slug")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);

    let version_hint = map
        .get("version")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            map.get("latest")
                .and_then(JsonValue::as_object)
                .and_then(|latest| latest.get("version"))
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        });

    let display_name = map
        .get("displayName")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);

    Ok(ParsedSidecarMeta {
        owner,
        slug,
        version_hint,
        display_name,
    })
}

fn fallback_description(body: &str) -> String {
    body.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("No description")
        .to_owned()
}

fn fingerprint_for_content(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn default_skill_conformance() -> SkillConformanceReport {
    SkillConformanceReport {
        agentskills_strict: ConformanceResult {
            compliant: true,
            issues: Vec::new(),
        },
        openclaw_compat: ConformanceResult {
            compliant: true,
            issues: Vec::new(),
        },
    }
}

/// Parses in-memory `SKILL.md` content through the same contract used for
/// filesystem packages.
pub fn parse_skill_markdown(
    content: &str,
    context: SkillMarkdownParseContext,
) -> Result<SkillDefinition> {
    let normalized = content.replace("\r\n", "\n");
    let (frontmatter_block, body_block) = split_frontmatter(normalized.as_str());
    let mut frontmatter = parse_frontmatter(frontmatter_block)?;
    normalize_implicit_invocation_policy_for_source(&context.source_kind, &mut frontmatter);
    let body = body_block.trim().to_owned();

    if body.is_empty() {
        bail!("skill `{}` has empty body", context.skill_file);
    }

    let raw_slug = context
        .identity_slug_override
        .as_deref()
        .or(frontmatter.slug.as_deref())
        .unwrap_or(context.parent_directory_name.as_str());
    let slug = normalize_skill_slug(raw_slug);

    if slug.is_empty() {
        bail!(
            "skill `{}` could not derive a slug from frontmatter `slug`, identity override, or the parent identity",
            context.skill_file
        );
    }

    let owner = context
        .identity_owner_override
        .as_deref()
        .or(frontmatter.owner.as_deref())
        .map(normalize_owner)
        .filter(|value| !value.is_empty());

    let name = frontmatter
        .name
        .clone()
        .or(context.display_name_override)
        .unwrap_or_else(|| slug.clone());
    let display_name = name.clone();
    let description = frontmatter
        .description
        .clone()
        .unwrap_or_else(|| fallback_description(body.as_str()));
    let runtime_tools = frontmatter.runtime_tools.clone();
    let fingerprint = fingerprint_for_content(normalized.as_str());

    let conformance = build_conformance_report(ValidationInput {
        name: Some(name.as_str()),
        description: Some(description.as_str()),
        license: frontmatter.license.as_deref(),
        compatibility: frontmatter.compatibility.as_deref(),
        parent_directory_name: Some(context.parent_directory_name.as_str()),
        allowed_tools_input_kind: frontmatter.allowed_tools_input_kind,
        metadata: &frontmatter.metadata,
        carried_issues: frontmatter.issues.as_slice(),
    });

    let mut definition = compile_skill_definition(CompileSkillInput {
        skill_id: context.skill_id,
        owner,
        slug: slug.clone(),
        name: name.clone(),
        display_name: display_name.clone(),
        description: description.clone(),
        body: body.clone(),
        source_kind: context.source_kind.clone(),
        source_root: context.source_root,
        skill_dir: context.skill_dir,
        skill_file: context.skill_file,
        version_hint: context
            .version_hint_override
            .or(frontmatter.version_hint.clone()),
        fingerprint: fingerprint.clone(),
        user_invocable: frontmatter.user_invocable,
        disable_model_invocation: frontmatter.disable_model_invocation,
        paths: frontmatter.paths,
        allowed_tools: frontmatter.allowed_tools,
        runtime_tools,
        trust_level: SkillTrustLevel::default(),
        dependencies: frontmatter.dependencies,
        license: frontmatter.license.clone(),
        compatibility: frontmatter.compatibility.clone(),
        metadata_raw: frontmatter.metadata.clone(),
        conformance: conformance.clone(),
    });
    if matches!(
        frontmatter.implicit_invocation,
        SkillImplicitInvocationPolicy::Required
    ) && matches!(context.source_kind, SkillSourceKind::System)
    {
        definition.policy_hints.implicit_invocation = SkillImplicitInvocationPolicy::Required;
    }
    definition.policy_hints.catalog_hidden = frontmatter.catalog_hidden;

    Ok(definition)
}

pub fn parse_skill_from_file(
    skill_id: SkillId,
    skill_file: &Path,
    source_kind: SkillSourceKind,
    source_root: &Path,
    max_file_bytes: usize,
) -> Result<SkillDefinition> {
    let metadata = fs::metadata(skill_file)
        .with_context(|| format!("failed to stat skill file `{}`", skill_file.display()))?;
    let file_size = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    if file_size > max_file_bytes {
        bail!(
            "skill file `{}` exceeds size limit: {} > {}",
            skill_file.display(),
            file_size,
            max_file_bytes
        );
    }

    let raw = fs::read_to_string(skill_file)
        .with_context(|| format!("failed to read skill file `{}`", skill_file.display()))?;

    let skill_dir = skill_file
        .parent()
        .context("skill file does not have a parent directory")?;
    let sidecar_meta = parse_sidecar_meta(skill_dir)?;
    let parent_directory_name = skill_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("skill")
        .to_owned();

    parse_skill_markdown(
        raw.as_str(),
        SkillMarkdownParseContext {
            skill_id,
            source_kind,
            source_root: source_root.display().to_string(),
            skill_dir: skill_dir.display().to_string(),
            skill_file: skill_file.display().to_string(),
            parent_directory_name,
            identity_owner_override: sidecar_meta.owner,
            identity_slug_override: sidecar_meta.slug,
            version_hint_override: sidecar_meta.version_hint,
            display_name_override: sidecar_meta.display_name,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{
        SkillDefinition, SkillMarkdownParseContext, SkillSourceKind,
        normalize_skill_markdown_plain_description, parse_skill_from_file as parse_skill_with_id,
        parse_skill_markdown,
    };
    use crate::compile::SkillImplicitInvocationPolicy;
    use crate::runtime::{
        DynamicDeltaOutputRequest, DynamicStorageOutputRequest, DynamicTimelineOutputRequest,
        SkillRuntimeToolKind,
    };
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;

    fn parse_skill_from_file(
        skill_file: &Path,
        source_kind: SkillSourceKind,
        source_root: &Path,
        max_file_bytes: usize,
    ) -> anyhow::Result<SkillDefinition> {
        parse_skill_with_id(
            pioneer_protocol::SkillId::new("EEEEEEEEEEEEEEEEEEEEE").expect("valid test skill id"),
            skill_file,
            source_kind,
            source_root,
            max_file_bytes,
        )
    }

    fn temp_case(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("unix timestamp")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pioneer-skill-contract-{name}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn write_skill(base: &PathBuf, dir_name: &str, content: &str) -> PathBuf {
        let skill_dir = base.join(dir_name);
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(skill_dir.join("SKILL.md"), content).expect("write skill file");
        skill_dir.join("SKILL.md")
    }

    #[test]
    fn filesystem_parser_delegates_to_source_neutral_markdown_parser() {
        let dir = temp_case("source-neutral-delegation");
        let content = concat!(
            "---\n",
            "name: delegated-skill\n",
            "slug: delegated-skill\n",
            "description: Shared parser behavior\n",
            "---\n",
            "Follow the same contract.\n"
        );
        let skill_file = write_skill(&dir, "delegated-skill", content);
        let skill_id =
            pioneer_protocol::SkillId::new("EEEEEEEEEEEEEEEEEEEEE").expect("valid test skill id");
        let from_file = parse_skill_with_id(
            skill_id.clone(),
            skill_file.as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("filesystem parser");
        let from_memory = parse_skill_markdown(
            content,
            SkillMarkdownParseContext {
                skill_id,
                source_kind: SkillSourceKind::User,
                source_root: dir.display().to_string(),
                skill_dir: skill_file
                    .parent()
                    .expect("skill parent")
                    .display()
                    .to_string(),
                skill_file: skill_file.display().to_string(),
                parent_directory_name: "delegated-skill".to_owned(),
                identity_owner_override: None,
                identity_slug_override: None,
                version_hint_override: None,
                display_name_override: None,
            },
        )
        .expect("source-neutral parser");

        assert_eq!(
            serde_json::to_value(from_file).expect("serialize filesystem result"),
            serde_json::to_value(from_memory).expect("serialize in-memory result")
        );
    }

    #[test]
    fn parses_yaml_frontmatter_with_nested_objects() {
        let dir = temp_case("yaml-nested");
        let skill_file = write_skill(
            &dir,
            "agent-browser",
            r#"---
name: agent-browser
slug: agent-browser
description: Browser helper
metadata:
  openclaw:
    os:
      - darwin
      - linux
    requires:
      bins: [node, npm]
      config:
        - skills.browser.enabled
  clawdbot:
    emoji: "🌐"
    requires:
      commands:
        - agent-browser
allowed-tools: Bash(agent-browser:*) Read
dependencies:
  env: [OPENAI_API_KEY]
---
Instructions
"#,
        );

        let skill = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("skill parses");

        assert_eq!(skill.identity.slug, "agent-browser");
        assert_eq!(
            skill.runtime.allowed_tools,
            vec!["Bash(agent-browser:*)", "Read"]
        );
        assert_eq!(skill.dependencies.bins, vec!["node", "npm"]);
        assert_eq!(skill.dependencies.commands, vec!["agent-browser"]);
        assert!(skill.conformance.openclaw_compat.compliant);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn accepts_unquoted_plain_description_with_mapping_separator() {
        let dir = temp_case("plain-description-mapping-separator");
        let raw = r#"---
name: aso-router
description: Routes ambiguous ASO requests. Triggers: "/aso", "aso help".
metadata:
  version: 1.0.0
---
Route the request to the correct specialist.
"#;
        let skill_file = write_skill(&dir, "aso-router", raw);
        let source_before = fs::read(&skill_file).expect("read source before parsing");

        parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            1024 * 1024,
        )
        .expect_err("strict parser must reject the original malformed YAML");

        let normalized = normalize_skill_markdown_plain_description(raw)
            .expect("compatibility normalization should succeed")
            .expect("description should require normalization");
        assert!(normalized.contains("description: >-\n  Routes ambiguous ASO requests."));
        assert_eq!(
            fs::read(&skill_file).expect("read source after pure normalization"),
            source_before,
            "normalization must not rewrite the caller's source file"
        );
        fs::write(&skill_file, normalized).expect("write staged normalized skill");

        let skill = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("compatibility description should parse");

        assert_eq!(
            skill.instructions.description,
            "Routes ambiguous ASO requests. Triggers: \"/aso\", \"aso help\"."
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn description_compatibility_does_not_mask_other_invalid_yaml() {
        let error = normalize_skill_markdown_plain_description(
            "---\n\
             name: aso-router\n\
             description: Routes requests. Triggers: /aso\n\
             metadata: [unterminated\n\
             ---\n\
             Instructions\n",
        )
        .expect_err("unrelated invalid YAML must remain rejected");

        assert!(
            format!("{error:#}").contains("failed to parse YAML frontmatter"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn parses_single_line_json_metadata_object() {
        let dir = temp_case("json-metadata");
        let skill_file = write_skill(
            &dir,
            "agent-browser",
            r#"---
name: Agent Browser
slug: agent-browser
description: Browser helper
metadata: {"clawdbot":{"emoji":"🌐","requires":{"commands":["agent-browser"],"bins":["node"]}}}
---
Instructions
"#,
        );

        let skill = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("skill parses");

        assert_eq!(skill.dependencies.commands, vec!["agent-browser"]);
        assert_eq!(skill.dependencies.bins, vec!["node"]);
        assert!(skill.metadata_raw.get("clawdbot").is_some());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn system_skill_can_require_implicit_invocation() {
        let dir = temp_case("required-implicit-system");
        let skill_file = write_skill(
            &dir,
            "locked-implicit",
            r#"---
name: locked-implicit
slug: locked-implicit
description: Locked implicit system skill
implicit-invocation: required
---
Instructions
"#,
        );

        let skill = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::System,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("system skill parses");

        assert_eq!(
            skill.policy_hints.implicit_invocation,
            SkillImplicitInvocationPolicy::Required
        );
        assert!(skill.conformance.agentskills_strict.compliant);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn non_system_skill_required_implicit_invocation_is_ignored_with_warning() {
        let dir = temp_case("required-implicit-user");
        let skill_file = write_skill(
            &dir,
            "locked-implicit",
            r#"---
name: locked-implicit
slug: locked-implicit
description: User skill cannot require implicit invocation
implicit-invocation: required
---
Instructions
"#,
        );

        let skill = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("user skill parses");

        assert_eq!(
            skill.policy_hints.implicit_invocation,
            SkillImplicitInvocationPolicy::UserControlled
        );
        assert!(skill.conformance.agentskills_strict.compliant);
        assert!(
            skill
                .conformance
                .agentskills_strict
                .issues
                .iter()
                .any(|issue| issue.code == "contract.implicit-invocation.source_kind")
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn parses_catalog_hide_frontmatter() {
        let dir = temp_case("catalog-hide");
        let skill_file = write_skill(
            &dir,
            "hidden-skill",
            r#"---
name: hidden-skill
slug: hidden-skill
description: Hidden from prompt catalog
catalog-hide: true
---
Instructions
"#,
        );

        let skill = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::System,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("skill parses");

        assert!(skill.policy_hints.catalog_hidden);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn derives_and_normalizes_slug_from_skill_directory_when_metadata_omits_slug() {
        let dir = temp_case("directory-slug");
        let skill_file = write_skill(
            &dir,
            "Folder_Name",
            r#"---
name: docx
description: Word document helper
---
Use scripts under this skill.
"#,
        );

        let skill = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("skill parses");

        assert_eq!(skill.identity.slug, "folder-name");
        assert_eq!(skill.identity.name, "docx");
        assert_eq!(skill.identity.owner, None);
        assert_eq!(skill.identity.skill_id.as_str(), "EEEEEEEEEEEEEEEEEEEEE");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn strict_profile_reports_issue_codes() {
        let dir = temp_case("strict-fail");
        let skill_file = write_skill(
            &dir,
            "agent-browser",
            r#"---
name: Agent Browser
slug: agent-browser
description: Browser helper
allowed-tools:
  - Bash(agent-browser:*)
---
Instructions
"#,
        );

        let skill = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("skill parses");

        assert!(!skill.conformance.agentskills_strict.compliant);
        assert!(
            skill
                .conformance
                .agentskills_strict
                .issues
                .iter()
                .any(|issue| issue.code == "strict.allowed_tools.format")
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn trust_level_defaults_to_community_and_ignores_frontmatter() {
        let dir = temp_case("source-trust-policy");
        let skill_file = write_skill(
            &dir,
            "my-skill",
            r#"---
name: my-skill
slug: my-skill
description: desc
trust: untrusted
security:
  trust: untrusted
---
body
"#,
        );

        let system = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::System,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("parse system skill");
        assert_eq!(
            system.runtime.trust_level,
            super::SkillTrustLevel::Community
        );

        let registry = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::Registry,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("parse registry skill");
        assert_eq!(
            registry.runtime.trust_level,
            super::SkillTrustLevel::Community
        );

        let workspace = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            1024 * 1024,
        )
        .expect("parse workspace skill");
        assert_eq!(
            workspace.runtime.trust_level,
            super::SkillTrustLevel::Community
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn parses_runtime_tools_from_skill_frontmatter() {
        let dir = temp_case("runtime-tool");
        let skill_dir = dir.join("test-skill");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: test-skill
slug: test-skill
description: Skill with runtime tool
runtime:
  tools:
    - tool_slug: fetch_data
      description: Fetch data
      kind: http
      parameters:
        type: object
      execution_class: shared
      config:
        method: GET
        url: https://example.com
---
Body"#,
        )
        .expect("write skill");

        let skill = parse_skill_from_file(
            skill_dir.join("SKILL.md").as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            4 * 1024,
        )
        .expect("skill parses");

        assert_eq!(skill.runtime.runtime_tools.len(), 1);
        assert_eq!(skill.runtime.runtime_tools[0].tool_slug, "fetch-data");
        assert_eq!(
            skill.runtime.runtime_tools[0].kind,
            SkillRuntimeToolKind::Http
        );
        assert!(skill.runtime.runtime_tools[0].output_policy.is_none());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn parses_runtime_tool_output_policy_from_skill_frontmatter() {
        let dir = temp_case("runtime-tool-output-policy");
        let skill_dir = dir.join("test-skill");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: test-skill
slug: test-skill
description: Skill with runtime tool policy
runtime:
  tools:
    - tool_slug: fetch_data
      description: Fetch data
      kind: http
      parameters:
        type: object
      output_policy:
        timeline:
          mode: summary
          max_chars: 1200
        storage:
          mode: metadata_only
        recovery:
          mode: evidence
          diagnostic_excerpt:
            mode: disabled
        deltas:
          mode: progress_only
---
Body"#,
        )
        .expect("write skill");

        let skill = parse_skill_from_file(
            skill_dir.join("SKILL.md").as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            4 * 1024,
        )
        .expect("skill parses");

        let output_policy = skill.runtime.runtime_tools[0]
            .output_policy
            .as_ref()
            .expect("runtime output policy should parse");
        assert!(output_policy.timeline.is_some());
        assert!(output_policy.storage.is_some());
        assert!(output_policy.recovery.is_some());
        assert!(output_policy.deltas.is_some());
        assert!(matches!(
            output_policy.timeline,
            Some(DynamicTimelineOutputRequest::Summary {
                max_chars: Some(1200)
            })
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn parses_shell_runtime_tool_output_policy_limits() {
        let dir = temp_case("runtime-tool-shell-output-policy");
        let skill_dir = dir.join("test-skill");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: test-skill
slug: test-skill
description: Skill with shell runtime tool policy
runtime:
  tools:
    - tool_slug: run_report
      description: Run report
      kind: shell
      parameters:
        type: object
      config:
        command: ["/bin/sh", "-c", "printf ok"]
      output_policy:
        timeline:
          mode: full
          max_bytes: 9999999
        storage:
          mode: full
          max_bytes: 8888888
        deltas:
          mode: persist_and_display
          max_chunk_bytes: 777777
          max_total_bytes: 6666666
---
Body"#,
        )
        .expect("write skill");

        let skill = parse_skill_from_file(
            skill_dir.join("SKILL.md").as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            4 * 1024,
        )
        .expect("skill parses");

        let output_policy = skill.runtime.runtime_tools[0]
            .output_policy
            .as_ref()
            .expect("runtime output policy should parse");
        assert!(matches!(
            output_policy.timeline,
            Some(DynamicTimelineOutputRequest::Full {
                max_bytes: Some(9_999_999)
            })
        ));
        assert!(matches!(
            output_policy.storage,
            Some(DynamicStorageOutputRequest::Full {
                max_bytes: Some(8_888_888)
            })
        ));
        assert!(matches!(
            output_policy.deltas,
            Some(DynamicDeltaOutputRequest::PersistAndDisplay {
                max_chunk_bytes: Some(777_777),
                max_total_bytes: Some(6_666_666)
            })
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn invalid_runtime_tool_output_policy_excludes_only_that_tool() {
        let dir = temp_case("runtime-tool-output-policy-invalid");
        let skill_dir = dir.join("test-skill");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: test-skill
slug: test-skill
description: Skill with runtime tool policy
runtime:
  tools:
    - tool_slug: bad_tool
      description: Bad tool
      kind: http
      parameters:
        type: object
      output_policy:
        storage:
          mode: full
          unexpected_field: true
    - tool_slug: good_tool
      description: Good tool
      kind: http
      parameters:
        type: object
---
Body"#,
        )
        .expect("write skill");

        let skill = parse_skill_from_file(
            skill_dir.join("SKILL.md").as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            4 * 1024,
        )
        .expect("skill parses");

        assert_eq!(skill.runtime.runtime_tools.len(), 1);
        assert_eq!(skill.runtime.runtime_tools[0].tool_slug, "good-tool");
        assert!(
            skill
                .conformance
                .agentskills_strict
                .issues
                .iter()
                .any(|issue| issue.code == "contract.runtime.tools.output_policy")
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn unknown_runtime_tool_output_policy_mode_excludes_tool() {
        let dir = temp_case("runtime-tool-output-policy-unknown-mode");
        let skill_dir = dir.join("test-skill");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: test-skill
slug: test-skill
description: Skill with invalid runtime tool policy
runtime:
  tools:
    - tool_slug: bad_tool
      description: Bad tool
      kind: http
      parameters:
        type: object
      output_policy:
        storage:
          mode: raw_forever
---
Body"#,
        )
        .expect("write skill");

        let skill = parse_skill_from_file(
            skill_dir.join("SKILL.md").as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            4 * 1024,
        )
        .expect("skill parses");

        assert!(skill.runtime.runtime_tools.is_empty());
        assert!(
            skill
                .conformance
                .agentskills_strict
                .issues
                .iter()
                .any(|issue| issue.code == "contract.runtime.tools.output_policy")
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn sidecar_meta_overrides_slug_owner_and_version() {
        let dir = temp_case("sidecar-meta");
        let skill_dir = dir.join("source");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: Browser Helper
owner: FrontmatterOwner
slug: frontmatter-slug
description: desc
---
Body"#,
        )
        .expect("write skill");
        fs::write(
            skill_dir.join("_meta.json"),
            r#"{
  "owner": "ClawD",
  "slug": "Agent_Browser",
  "latest": {
    "version": "0.2.0"
  },
  "displayName": "Agent Browser"
}"#,
        )
        .expect("write sidecar");

        let skill_bytes_before = fs::read(skill_dir.join("SKILL.md")).expect("read skill before");
        let meta_bytes_before = fs::read(skill_dir.join("_meta.json")).expect("read meta before");

        let skill = parse_skill_from_file(
            skill_dir.join("SKILL.md").as_path(),
            SkillSourceKind::Registry,
            dir.as_path(),
            4 * 1024,
        )
        .expect("skill parses");

        assert_eq!(skill.identity.owner.as_deref(), Some("clawd"));
        assert_eq!(skill.identity.slug, "agent-browser");
        assert_eq!(skill.identity.version_hint.as_deref(), Some("0.2.0"));
        assert_eq!(skill.identity.display_name, "Browser Helper");
        assert_eq!(
            fs::read(skill_dir.join("SKILL.md")).expect("read skill after"),
            skill_bytes_before
        );
        assert_eq!(
            fs::read(skill_dir.join("_meta.json")).expect("read meta after"),
            meta_bytes_before
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn frontmatter_slug_and_owner_override_folder_fallbacks() {
        let dir = temp_case("frontmatter-identity-metadata");
        let skill_file = write_skill(
            &dir,
            "Folder_Name",
            r#"---
name: Presentation Name
owner: Explicit_Owner
slug: Frontmatter_Slug
description: desc
---
Body"#,
        );

        let skill = parse_skill_from_file(
            skill_file.as_path(),
            SkillSourceKind::User,
            dir.as_path(),
            4 * 1024,
        )
        .expect("skill parses");

        assert_eq!(skill.identity.owner.as_deref(), Some("explicit-owner"));
        assert_eq!(skill.identity.slug, "frontmatter-slug");
        assert_eq!(skill.identity.name, "Presentation Name");

        let _ = fs::remove_dir_all(dir);
    }
}
