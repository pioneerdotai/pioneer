use crate::compile::{CompileSkillInput, SkillDefinition, compile_skill_definition};
use crate::runtime::{
    DynamicToolOutputPolicyDeclaration, RuntimeExecutionClassHint, SkillRuntimeToolDefinition,
    SkillRuntimeToolKind,
};
use crate::validation::{
    AllowedToolsInputKind, ConformanceResult, SkillConformanceReport, SkillValidationIssue,
    ValidationInput, build_conformance_report,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillSourceKind {
    System,
    User,
    Workspace,
    Registry,
}

impl SkillSourceKind {
    pub fn as_db_value(&self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Workspace => "workspace",
            Self::Registry => "registry",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
            let tool_slug = normalize_slug(tool.tool_slug.as_str());
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

fn parse_frontmatter(frontmatter: Option<&str>) -> Result<ParsedFrontmatter> {
    let mut parsed = ParsedFrontmatter {
        user_invocable: true,
        disable_model_invocation: false,
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

    parsed.metadata = parse_metadata(map, &mut parsed.issues);

    Ok(parsed)
}

fn normalize_slug(input: &str) -> String {
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
    normalize_slug(input)
}

fn default_owner_for_source_kind(source_kind: &SkillSourceKind) -> &'static str {
    match source_kind {
        SkillSourceKind::System => "pioneer",
        SkillSourceKind::User => "pioneer",
        SkillSourceKind::Workspace => "pioneer",
        SkillSourceKind::Registry => "pioneer",
    }
}

fn owner_from_relative_path(skill_dir: &Path, source_root: &Path) -> Option<String> {
    let relative = skill_dir.strip_prefix(source_root).ok()?;
    let mut components = relative.components();
    let owner_component = components.next()?;
    if components.next().is_none() {
        return None;
    }
    let owner_raw = owner_component
        .as_os_str()
        .to_string_lossy()
        .trim()
        .to_owned();
    if owner_raw.is_empty() {
        return None;
    }

    let normalized = normalize_owner(owner_raw.as_str());
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
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

pub fn qualified_skill_slug(owner: &str, slug: &str) -> String {
    format!("{owner}/{slug}")
}

pub fn split_qualified_skill_slug(value: &str) -> Option<(String, String)> {
    let trimmed = value.trim();
    let (owner, slug) = trimmed.split_once('/')?;
    let owner = owner.trim();
    let slug = slug.trim();
    if owner.is_empty() || slug.is_empty() {
        return None;
    }
    Some((owner.to_owned(), slug.to_owned()))
}

pub fn is_qualified_skill_slug(value: &str) -> bool {
    split_qualified_skill_slug(value).is_some()
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

pub fn parse_skill_from_file(
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
    let normalized = raw.replace("\r\n", "\n");

    let (frontmatter_block, body_block) = split_frontmatter(normalized.as_str());
    let frontmatter = parse_frontmatter(frontmatter_block)?;
    let body = body_block.trim().to_owned();

    if body.is_empty() {
        bail!("skill `{}` has empty body", skill_file.display());
    }

    let skill_dir = skill_file
        .parent()
        .context("skill file does not have a parent directory")?;

    let sidecar_meta = parse_sidecar_meta(skill_dir)?;

    let parent_directory_name = skill_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("skill")
        .to_owned();

    let slug = sidecar_meta
        .slug
        .as_deref()
        .or(frontmatter.slug.as_deref())
        .map(normalize_slug)
        .filter(|value| !value.is_empty());

    let Some(slug) = slug else {
        bail!(
            "skill `{}` is missing required `slug`; define frontmatter `slug` or `_meta.json.slug`",
            skill_file.display()
        );
    };

    let owner = sidecar_meta
        .owner
        .as_deref()
        .or(frontmatter.owner.as_deref())
        .map(normalize_owner)
        .filter(|value| !value.is_empty())
        .or_else(|| owner_from_relative_path(skill_dir, source_root))
        .unwrap_or_else(|| default_owner_for_source_kind(&source_kind).to_owned());

    let name = frontmatter
        .name
        .clone()
        .or(sidecar_meta.display_name.clone())
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
        parent_directory_name: Some(parent_directory_name.as_str()),
        allowed_tools_input_kind: frontmatter.allowed_tools_input_kind,
        metadata: &frontmatter.metadata,
        carried_issues: frontmatter.issues.as_slice(),
    });

    let definition = compile_skill_definition(CompileSkillInput {
        owner,
        slug: slug.clone(),
        name: name.clone(),
        display_name: display_name.clone(),
        description: description.clone(),
        body: body.clone(),
        source_kind: source_kind.clone(),
        source_root: source_root.display().to_string(),
        skill_dir: skill_dir.display().to_string(),
        skill_file: skill_file.display().to_string(),
        version_hint: sidecar_meta
            .version_hint
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

    Ok(definition)
}

#[cfg(test)]
mod tests {
    use super::{SkillSourceKind, parse_skill_from_file};
    use crate::runtime::{
        DynamicDeltaOutputRequest, DynamicStorageOutputRequest, DynamicTimelineOutputRequest,
        SkillRuntimeToolKind,
    };
    use std::fs;
    use std::path::PathBuf;

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
            SkillSourceKind::Workspace,
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
            SkillSourceKind::Workspace,
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
            SkillSourceKind::Workspace,
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
            SkillSourceKind::Workspace,
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
            SkillSourceKind::Workspace,
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
            SkillSourceKind::Workspace,
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
        command: ["sh", "-lc", "printf ok"]
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
            SkillSourceKind::Workspace,
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
            SkillSourceKind::Workspace,
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
            SkillSourceKind::Workspace,
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

        let skill = parse_skill_from_file(
            skill_dir.join("SKILL.md").as_path(),
            SkillSourceKind::Registry,
            dir.as_path(),
            4 * 1024,
        )
        .expect("skill parses");

        assert_eq!(skill.identity.owner, "clawd");
        assert_eq!(skill.identity.slug, "agent-browser");
        assert_eq!(skill.identity.version_hint.as_deref(), Some("0.2.0"));
        assert_eq!(skill.identity.display_name, "Browser Helper");

        let _ = fs::remove_dir_all(dir);
    }
}
