use crate::contract::{SkillSourceKind, SkillTrustLevel};
use crate::dependencies::{DependencyCheckInput, DependencyDiagnostic, evaluate_dependency_set};
use crate::resolver::ResolvedSkill;
use crate::security::{SkillSecurityPolicy, minimum_trust_for_tool_kind, trust_satisfies_minimum};
use pioneer_protocol::{
    DeltaOutputPolicy, DiagnosticExcerptPolicy, LlmOutputPolicy, LlmRetentionPolicy,
    RecoveryOutputPolicy, StorageOutputPolicy, TimelineOutputPolicy,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

const CANONICAL_PREFIX: &str = "skill";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRuntimeToolKind {
    Shell,
    Http,
    FunctionProxy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeExecutionClassHint {
    #[default]
    Shared,
    Exclusive,
    SessionScoped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct DynamicToolOutputPolicyDeclaration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm: Option<DynamicLlmOutputRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm_retention: Option<DynamicLlmRetentionRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeline: Option<DynamicTimelineOutputRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<DynamicStorageOutputRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<DynamicRecoveryOutputRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deltas: Option<DynamicDeltaOutputRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", deny_unknown_fields)]
pub enum DynamicLlmOutputRequest {
    Full {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_bytes: Option<usize>,
    },
    Structured {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_bytes: Option<usize>,
    },
    SummaryOnly,
}

impl DynamicLlmOutputRequest {
    pub fn to_policy(&self, default_max_bytes: usize) -> LlmOutputPolicy {
        match self {
            Self::Full { max_bytes } => LlmOutputPolicy::Full {
                max_bytes: max_bytes.unwrap_or(default_max_bytes),
            },
            Self::Structured { max_bytes } => LlmOutputPolicy::Structured {
                max_bytes: max_bytes.unwrap_or(default_max_bytes),
            },
            Self::SummaryOnly => LlmOutputPolicy::SummaryOnly,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", deny_unknown_fields)]
pub enum DynamicLlmRetentionRequest {
    UntilTurnTerminal {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_bytes: Option<usize>,
    },
    DoNotRetain,
}

impl DynamicLlmRetentionRequest {
    pub fn to_policy(&self, default_max_bytes: usize) -> LlmRetentionPolicy {
        match self {
            Self::UntilTurnTerminal { max_bytes } => LlmRetentionPolicy::UntilTurnTerminal {
                max_bytes: max_bytes.unwrap_or(default_max_bytes),
            },
            Self::DoNotRetain => LlmRetentionPolicy::DoNotRetain,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", deny_unknown_fields)]
pub enum DynamicTimelineOutputRequest {
    Full {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_bytes: Option<usize>,
    },
    Summary {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_chars: Option<usize>,
    },
    MetadataOnly,
    Hidden,
}

impl DynamicTimelineOutputRequest {
    pub fn to_policy(
        &self,
        default_max_chars: usize,
        default_max_bytes: usize,
    ) -> TimelineOutputPolicy {
        match self {
            Self::Full { max_bytes } => TimelineOutputPolicy::Full {
                max_bytes: max_bytes.unwrap_or(default_max_bytes),
            },
            Self::Summary { max_chars } => TimelineOutputPolicy::Summary {
                max_chars: max_chars.unwrap_or(default_max_chars),
            },
            Self::MetadataOnly => TimelineOutputPolicy::MetadataOnly,
            Self::Hidden => TimelineOutputPolicy::Hidden,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", deny_unknown_fields)]
pub enum DynamicStorageOutputRequest {
    Full {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_bytes: Option<usize>,
    },
    Summary {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_chars: Option<usize>,
    },
    MetadataOnly,
    None,
}

impl DynamicStorageOutputRequest {
    pub fn to_policy(
        &self,
        default_max_chars: usize,
        default_max_bytes: usize,
    ) -> StorageOutputPolicy {
        match self {
            Self::Full { max_bytes } => StorageOutputPolicy::Full {
                max_bytes: max_bytes.unwrap_or(default_max_bytes),
            },
            Self::Summary { max_chars } => StorageOutputPolicy::Summary {
                max_chars: max_chars.unwrap_or(default_max_chars),
            },
            Self::MetadataOnly => StorageOutputPolicy::MetadataOnly,
            Self::None => StorageOutputPolicy::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", deny_unknown_fields)]
pub enum DynamicRecoveryOutputRequest {
    Evidence {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        include_exit_status: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        include_error_class: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        include_retry_hint: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diagnostic_excerpt: Option<DynamicDiagnosticExcerptRequest>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        include_fingerprints: Option<bool>,
    },
    MetadataOnly,
    None,
}

impl DynamicRecoveryOutputRequest {
    pub fn to_policy(&self) -> RecoveryOutputPolicy {
        match self {
            Self::Evidence {
                include_exit_status,
                include_error_class,
                include_retry_hint,
                diagnostic_excerpt,
                include_fingerprints,
            } => RecoveryOutputPolicy::Evidence {
                include_exit_status: include_exit_status.unwrap_or(true),
                include_error_class: include_error_class.unwrap_or(true),
                include_retry_hint: include_retry_hint.unwrap_or(true),
                diagnostic_excerpt: diagnostic_excerpt
                    .as_ref()
                    .map(DynamicDiagnosticExcerptRequest::to_policy)
                    .unwrap_or(DiagnosticExcerptPolicy::Disabled),
                include_fingerprints: include_fingerprints.unwrap_or(true),
            },
            Self::MetadataOnly => RecoveryOutputPolicy::MetadataOnly,
            Self::None => RecoveryOutputPolicy::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", deny_unknown_fields)]
pub enum DynamicDiagnosticExcerptRequest {
    Disabled,
    ErrorsOnly {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_chars: Option<usize>,
    },
    Output {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_chars: Option<usize>,
    },
}

impl DynamicDiagnosticExcerptRequest {
    pub fn to_policy(&self) -> DiagnosticExcerptPolicy {
        match self {
            Self::Disabled => DiagnosticExcerptPolicy::Disabled,
            Self::ErrorsOnly { max_chars } => DiagnosticExcerptPolicy::ErrorsOnly {
                max_chars: max_chars.unwrap_or(4_000),
            },
            Self::Output { max_chars } => DiagnosticExcerptPolicy::Output {
                max_chars: max_chars.unwrap_or(4_000),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", deny_unknown_fields)]
pub enum DynamicDeltaOutputRequest {
    PersistAndDisplay {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_chunk_bytes: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_total_bytes: Option<usize>,
    },
    ProgressOnly,
    Disabled,
}

impl DynamicDeltaOutputRequest {
    pub fn to_policy(
        &self,
        default_chunk_bytes: usize,
        default_total_bytes: usize,
    ) -> DeltaOutputPolicy {
        match self {
            Self::PersistAndDisplay {
                max_chunk_bytes,
                max_total_bytes,
            } => DeltaOutputPolicy::PersistAndDisplay {
                max_chunk_bytes: max_chunk_bytes.unwrap_or(default_chunk_bytes),
                max_total_bytes: max_total_bytes.unwrap_or(default_total_bytes),
            },
            Self::ProgressOnly => DeltaOutputPolicy::ProgressOnly,
            Self::Disabled => DeltaOutputPolicy::Disabled,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillRuntimeToolDefinition {
    pub tool_slug: String,
    pub description: String,
    pub kind: SkillRuntimeToolKind,
    pub parameters: JsonValue,
    #[serde(default)]
    pub execution_class: RuntimeExecutionClassHint,
    #[serde(default)]
    pub config: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_policy: Option<DynamicToolOutputPolicyDeclaration>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillRuntimeDescriptor {
    pub canonical_tool_name: String,
    pub skill_slug: String,
    pub skill_name: String,
    pub skill_fingerprint: String,
    pub source_kind: SkillSourceKind,
    pub trust_level: SkillTrustLevel,
    pub dependencies: crate::compile::SkillDependencySet,
    pub definition: SkillRuntimeToolDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSkillEntry {
    pub slug: String,
    pub name: String,
    pub description: String,
    pub body: String,
    pub fingerprint: String,
    pub source_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeToolExcludedReason {
    DynamicToolsDisabled,
    DisabledByConfig,
    TrustLevelTooLow,
    InvalidToolSlug,
    InvalidOutputPolicy,
    DuplicateCanonicalName,
    MaxDynamicToolsPerSkill,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcludedRuntimeTool {
    pub skill_slug: String,
    pub tool_slug: String,
    pub reason: RuntimeToolExcludedReason,
}

#[derive(Debug, Clone)]
pub struct SkillRuntimePlan {
    pub tools: Vec<SkillRuntimeDescriptor>,
    pub read_skill_index: HashMap<String, ReadSkillEntry>,
    pub excluded_tools: Vec<ExcludedRuntimeTool>,
}

#[derive(Debug, Clone)]
pub struct SkillRuntimeBudget {
    pub enable_dynamic_tools: bool,
    pub max_dynamic_tools_per_skill: usize,
    pub allow_shell_tools: bool,
    pub allow_http_tools: bool,
    pub allow_function_proxy_tools: bool,
    pub allow_untrusted_install: bool,
    pub min_trust_for_shell_tools: SkillTrustLevel,
    pub min_trust_for_http_tools: SkillTrustLevel,
    pub min_trust_for_function_proxy_tools: SkillTrustLevel,
}

impl SkillRuntimeBudget {
    pub fn normalized(self) -> Self {
        Self {
            enable_dynamic_tools: self.enable_dynamic_tools,
            max_dynamic_tools_per_skill: self.max_dynamic_tools_per_skill.max(1),
            allow_shell_tools: self.allow_shell_tools,
            allow_http_tools: self.allow_http_tools,
            allow_function_proxy_tools: self.allow_function_proxy_tools,
            allow_untrusted_install: self.allow_untrusted_install,
            min_trust_for_shell_tools: self.min_trust_for_shell_tools,
            min_trust_for_http_tools: self.min_trust_for_http_tools,
            min_trust_for_function_proxy_tools: self.min_trust_for_function_proxy_tools,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionCheck {
    pub allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    pub message: String,
    #[serde(default)]
    pub dependency_diagnostics: Vec<DependencyDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionRecheckPolicy {
    pub runtime_recheck_on_tool_call: bool,
    pub security: SkillSecurityPolicy,
}

impl Default for RuntimeExecutionRecheckPolicy {
    fn default() -> Self {
        Self {
            runtime_recheck_on_tool_call: true,
            security: SkillSecurityPolicy::default(),
        }
    }
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

fn canonical_runtime_tool_name(skill_slug: &str, tool_slug: &str) -> Option<(String, String)> {
    let normalized_skill = normalize_slug(skill_slug);
    let normalized_tool = normalize_slug(tool_slug);
    if normalized_skill.is_empty() || normalized_tool.is_empty() {
        return None;
    }

    Some((
        format!("{CANONICAL_PREFIX}.{normalized_skill}.{normalized_tool}"),
        normalized_tool,
    ))
}

fn kind_allowed(kind: &SkillRuntimeToolKind, budget: &SkillRuntimeBudget) -> bool {
    match kind {
        SkillRuntimeToolKind::Shell => budget.allow_shell_tools,
        SkillRuntimeToolKind::Http => budget.allow_http_tools,
        SkillRuntimeToolKind::FunctionProxy => budget.allow_function_proxy_tools,
    }
}

fn runtime_security_policy_from_budget(budget: &SkillRuntimeBudget) -> SkillSecurityPolicy {
    SkillSecurityPolicy {
        allow_untrusted_install: budget.allow_untrusted_install,
        min_trust_for_shell_tools: budget.min_trust_for_shell_tools.clone(),
        min_trust_for_http_tools: budget.min_trust_for_http_tools.clone(),
        min_trust_for_function_proxy_tools: budget.min_trust_for_function_proxy_tools.clone(),
        max_install_archive_bytes: 10 * 1024 * 1024,
        max_install_file_bytes: 1024 * 1024,
    }
}

fn trust_allowed_for_tool(
    trust_level: SkillTrustLevel,
    kind: &SkillRuntimeToolKind,
    policy: &SkillSecurityPolicy,
) -> bool {
    if !policy.allow_untrusted_install && matches!(trust_level, SkillTrustLevel::Untrusted) {
        return false;
    }
    let min_required = minimum_trust_for_tool_kind(kind, policy);
    trust_satisfies_minimum(trust_level, min_required)
}

pub fn recheck_runtime_tool_execution(
    descriptor: &SkillRuntimeDescriptor,
    policy: &RuntimeExecutionRecheckPolicy,
) -> RuntimeExecutionCheck {
    if !trust_allowed_for_tool(
        descriptor.trust_level.clone(),
        &descriptor.definition.kind,
        &policy.security,
    ) {
        return RuntimeExecutionCheck {
            allowed: false,
            reason_code: Some("runtime.trust_level_too_low".to_owned()),
            message: format!(
                "runtime trust policy blocked `{}` for skill `{}`",
                descriptor.canonical_tool_name, descriptor.skill_slug
            ),
            dependency_diagnostics: Vec::new(),
        };
    }

    if !policy.runtime_recheck_on_tool_call {
        return RuntimeExecutionCheck {
            allowed: true,
            reason_code: None,
            message: "runtime recheck disabled by policy".to_owned(),
            dependency_diagnostics: Vec::new(),
        };
    }

    let diagnostics =
        evaluate_dependency_set(&descriptor.dependencies, &DependencyCheckInput::baseline())
            .failing_diagnostics();

    if !diagnostics.is_empty() {
        return RuntimeExecutionCheck {
            allowed: false,
            reason_code: Some("runtime.dependency_missing".to_owned()),
            message: format!(
                "runtime dependency recheck failed for `{}`",
                descriptor.canonical_tool_name
            ),
            dependency_diagnostics: diagnostics,
        };
    }

    RuntimeExecutionCheck {
        allowed: true,
        reason_code: None,
        message: "runtime dependency/trust checks passed".to_owned(),
        dependency_diagnostics: Vec::new(),
    }
}

pub fn build_skill_runtime_plan(
    active: &[ResolvedSkill],
    budget: SkillRuntimeBudget,
) -> SkillRuntimePlan {
    let budget = budget.normalized();
    let trust_policy = runtime_security_policy_from_budget(&budget);

    let mut read_skill_index = HashMap::new();
    for skill in active {
        read_skill_index.insert(
            skill.slug.clone(),
            ReadSkillEntry {
                slug: skill.slug.clone(),
                name: skill.definition.identity.display_name.clone(),
                description: skill.definition.instructions.description.clone(),
                body: skill.definition.instructions.body.clone(),
                fingerprint: skill.definition.identity.fingerprint.clone(),
                source_kind: skill
                    .definition
                    .identity
                    .source_kind
                    .as_db_value()
                    .to_owned(),
            },
        );
    }

    let mut excluded_tools = Vec::new();

    if !budget.enable_dynamic_tools {
        for skill in active {
            for definition in &skill.definition.runtime.runtime_tools {
                excluded_tools.push(ExcludedRuntimeTool {
                    skill_slug: skill.slug.clone(),
                    tool_slug: definition.tool_slug.clone(),
                    reason: RuntimeToolExcludedReason::DynamicToolsDisabled,
                });
            }
        }
        excluded_tools.sort_by(|left, right| {
            left.skill_slug
                .as_str()
                .cmp(right.skill_slug.as_str())
                .then_with(|| left.tool_slug.as_str().cmp(right.tool_slug.as_str()))
        });
        return SkillRuntimePlan {
            tools: Vec::new(),
            read_skill_index,
            excluded_tools,
        };
    }

    let mut accepted = Vec::new();
    let mut used_names = HashSet::new();

    let mut ordered_skills = active.to_vec();
    ordered_skills.sort_by(|left, right| left.slug.as_str().cmp(right.slug.as_str()));

    for skill in ordered_skills {
        let mut definitions = skill.definition.runtime.runtime_tools.clone();
        definitions.sort_by(|left, right| {
            normalize_slug(left.tool_slug.as_str()).cmp(&normalize_slug(right.tool_slug.as_str()))
        });

        let mut accepted_for_skill = 0usize;

        for definition in definitions {
            if !kind_allowed(&definition.kind, &budget) {
                excluded_tools.push(ExcludedRuntimeTool {
                    skill_slug: skill.slug.clone(),
                    tool_slug: definition.tool_slug.clone(),
                    reason: RuntimeToolExcludedReason::DisabledByConfig,
                });
                continue;
            }

            if !trust_allowed_for_tool(
                skill.definition.runtime.trust_level.clone(),
                &definition.kind,
                &trust_policy,
            ) {
                excluded_tools.push(ExcludedRuntimeTool {
                    skill_slug: skill.slug.clone(),
                    tool_slug: definition.tool_slug.clone(),
                    reason: RuntimeToolExcludedReason::TrustLevelTooLow,
                });
                continue;
            }

            if accepted_for_skill >= budget.max_dynamic_tools_per_skill {
                excluded_tools.push(ExcludedRuntimeTool {
                    skill_slug: skill.slug.clone(),
                    tool_slug: definition.tool_slug.clone(),
                    reason: RuntimeToolExcludedReason::MaxDynamicToolsPerSkill,
                });
                continue;
            }

            let Some((canonical_tool_name, normalized_tool_slug)) =
                canonical_runtime_tool_name(skill.slug.as_str(), definition.tool_slug.as_str())
            else {
                excluded_tools.push(ExcludedRuntimeTool {
                    skill_slug: skill.slug.clone(),
                    tool_slug: definition.tool_slug.clone(),
                    reason: RuntimeToolExcludedReason::InvalidToolSlug,
                });
                continue;
            };

            if !used_names.insert(canonical_tool_name.clone()) {
                excluded_tools.push(ExcludedRuntimeTool {
                    skill_slug: skill.slug.clone(),
                    tool_slug: definition.tool_slug.clone(),
                    reason: RuntimeToolExcludedReason::DuplicateCanonicalName,
                });
                continue;
            }

            accepted.push(SkillRuntimeDescriptor {
                canonical_tool_name,
                skill_slug: skill.slug.clone(),
                skill_name: skill.definition.identity.display_name.clone(),
                skill_fingerprint: skill.definition.identity.fingerprint.clone(),
                source_kind: skill.definition.identity.source_kind.clone(),
                trust_level: skill.definition.runtime.trust_level.clone(),
                dependencies: skill.definition.dependencies.clone(),
                definition: SkillRuntimeToolDefinition {
                    tool_slug: normalized_tool_slug,
                    description: definition.description,
                    kind: definition.kind,
                    parameters: definition.parameters,
                    execution_class: definition.execution_class,
                    config: definition.config,
                    output_policy: definition.output_policy,
                },
            });

            accepted_for_skill = accepted_for_skill.saturating_add(1);
        }
    }

    accepted.sort_by(|left, right| {
        left.skill_slug
            .as_str()
            .cmp(right.skill_slug.as_str())
            .then_with(|| {
                left.definition
                    .tool_slug
                    .as_str()
                    .cmp(right.definition.tool_slug.as_str())
            })
    });
    excluded_tools.sort_by(|left, right| {
        left.skill_slug
            .as_str()
            .cmp(right.skill_slug.as_str())
            .then_with(|| left.tool_slug.as_str().cmp(right.tool_slug.as_str()))
    });

    SkillRuntimePlan {
        tools: accepted,
        read_skill_index,
        excluded_tools,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RuntimeExecutionClassHint, RuntimeExecutionRecheckPolicy, RuntimeToolExcludedReason,
        SkillRuntimeBudget, SkillRuntimeToolDefinition, SkillRuntimeToolKind,
        build_skill_runtime_plan, recheck_runtime_tool_execution,
    };
    use crate::compile::{CompileSkillInput, SkillDefinition, compile_skill_definition};
    use crate::contract::{
        SkillCatalogSnapshot, SkillDependencies, SkillSourceKind, SkillTrustLevel,
        default_skill_conformance,
    };
    use crate::dependencies::DependencyCheckInput;
    use crate::policy::SkillPolicySet;
    use crate::resolver::{
        SkillExplicitRef, SkillResolutionInput, SkillValidationPolicy, resolve_skills,
    };
    use serde_json::json;

    fn skill_with_runtime_tools(
        slug: &str,
        tools: Vec<SkillRuntimeToolDefinition>,
    ) -> SkillDefinition {
        let conformance = default_skill_conformance();
        let definition = compile_skill_definition(CompileSkillInput {
            owner: "workspace".to_owned(),
            slug: slug.to_owned(),
            name: slug.to_owned(),
            display_name: slug.to_owned(),
            description: format!("{slug} description"),
            body: "skill body".to_owned(),
            source_kind: SkillSourceKind::User,
            source_root: "/tmp".to_owned(),
            skill_dir: format!("/tmp/{slug}"),
            skill_file: format!("/tmp/{slug}/SKILL.md"),
            version_hint: None,
            fingerprint: "fp".to_owned(),
            user_invocable: true,
            disable_model_invocation: false,
            paths: Vec::new(),
            allowed_tools: Vec::new(),
            runtime_tools: tools.clone(),
            trust_level: SkillTrustLevel::Community,
            dependencies: SkillDependencies::default(),
            license: None,
            compatibility: None,
            metadata_raw: json!({}),
            conformance: conformance.clone(),
        });

        definition
    }

    fn resolved_active_with_tools(
        slug: &str,
        tools: Vec<SkillRuntimeToolDefinition>,
    ) -> Vec<crate::resolver::ResolvedSkill> {
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![skill_with_runtime_tools(slug, tools)],
        };
        resolve_skills(SkillResolutionInput {
            explicit_refs: &[SkillExplicitRef {
                capability_id: format!("skill:user:{slug}"),
                label: Some(slug.to_owned()),
                slug: slug.to_owned(),
                source_kind: "user".to_owned(),
            }],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &SkillPolicySet::default(),
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &DependencyCheckInput::baseline(),
        })
        .active
    }

    #[test]
    fn runtime_plan_is_sorted_and_canonicalized() {
        let active = resolved_active_with_tools(
            "my-skill",
            vec![
                SkillRuntimeToolDefinition {
                    tool_slug: "B Tool".to_owned(),
                    description: "desc".to_owned(),
                    kind: SkillRuntimeToolKind::Http,
                    parameters: json!({"type":"object"}),
                    execution_class: RuntimeExecutionClassHint::Shared,
                    config: json!({}),
                    output_policy: None,
                },
                SkillRuntimeToolDefinition {
                    tool_slug: "a_tool".to_owned(),
                    description: "desc".to_owned(),
                    kind: SkillRuntimeToolKind::Http,
                    parameters: json!({"type":"object"}),
                    execution_class: RuntimeExecutionClassHint::Shared,
                    config: json!({}),
                    output_policy: None,
                },
            ],
        );

        let plan = build_skill_runtime_plan(
            &active,
            SkillRuntimeBudget {
                enable_dynamic_tools: true,
                max_dynamic_tools_per_skill: 16,
                allow_shell_tools: false,
                allow_http_tools: true,
                allow_function_proxy_tools: true,
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Verified,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            },
        );

        assert_eq!(plan.tools.len(), 2);
        assert_eq!(
            plan.tools[0].canonical_tool_name,
            "skill.workspace-my-skill.a-tool"
        );
        assert_eq!(
            plan.tools[1].canonical_tool_name,
            "skill.workspace-my-skill.b-tool"
        );
    }

    #[test]
    fn runtime_plan_applies_kind_gate_and_limits() {
        let active = resolved_active_with_tools(
            "my-skill",
            vec![
                SkillRuntimeToolDefinition {
                    tool_slug: "shell-1".to_owned(),
                    description: "desc".to_owned(),
                    kind: SkillRuntimeToolKind::Shell,
                    parameters: json!({"type":"object"}),
                    execution_class: RuntimeExecutionClassHint::SessionScoped,
                    config: json!({}),
                    output_policy: None,
                },
                SkillRuntimeToolDefinition {
                    tool_slug: "http-1".to_owned(),
                    description: "desc".to_owned(),
                    kind: SkillRuntimeToolKind::Http,
                    parameters: json!({"type":"object"}),
                    execution_class: RuntimeExecutionClassHint::Shared,
                    config: json!({}),
                    output_policy: None,
                },
                SkillRuntimeToolDefinition {
                    tool_slug: "http-2".to_owned(),
                    description: "desc".to_owned(),
                    kind: SkillRuntimeToolKind::Http,
                    parameters: json!({"type":"object"}),
                    execution_class: RuntimeExecutionClassHint::Shared,
                    config: json!({}),
                    output_policy: None,
                },
            ],
        );

        let plan = build_skill_runtime_plan(
            &active,
            SkillRuntimeBudget {
                enable_dynamic_tools: true,
                max_dynamic_tools_per_skill: 1,
                allow_shell_tools: false,
                allow_http_tools: true,
                allow_function_proxy_tools: true,
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Verified,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            },
        );

        assert_eq!(plan.tools.len(), 1);
        assert_eq!(plan.tools[0].definition.tool_slug, "http-1");
        assert!(plan.excluded_tools.iter().any(|tool| {
            tool.tool_slug == "shell-1"
                && tool.reason == RuntimeToolExcludedReason::DisabledByConfig
        }));
        assert!(plan.excluded_tools.iter().any(|tool| {
            tool.tool_slug == "http-2"
                && tool.reason == RuntimeToolExcludedReason::MaxDynamicToolsPerSkill
        }));
    }

    #[test]
    fn runtime_plan_rejects_duplicate_canonical_names() {
        let active = resolved_active_with_tools(
            "my-skill",
            vec![
                SkillRuntimeToolDefinition {
                    tool_slug: "echo_tool".to_owned(),
                    description: "desc".to_owned(),
                    kind: SkillRuntimeToolKind::Http,
                    parameters: json!({"type":"object"}),
                    execution_class: RuntimeExecutionClassHint::Shared,
                    config: json!({}),
                    output_policy: None,
                },
                SkillRuntimeToolDefinition {
                    tool_slug: "echo-tool".to_owned(),
                    description: "desc".to_owned(),
                    kind: SkillRuntimeToolKind::Http,
                    parameters: json!({"type":"object"}),
                    execution_class: RuntimeExecutionClassHint::Shared,
                    config: json!({}),
                    output_policy: None,
                },
            ],
        );

        let plan = build_skill_runtime_plan(
            &active,
            SkillRuntimeBudget {
                enable_dynamic_tools: true,
                max_dynamic_tools_per_skill: 16,
                allow_shell_tools: false,
                allow_http_tools: true,
                allow_function_proxy_tools: true,
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Verified,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            },
        );

        assert_eq!(plan.tools.len(), 1);
        assert!(plan.excluded_tools.iter().any(|tool| {
            tool.reason == RuntimeToolExcludedReason::DuplicateCanonicalName
                && (tool.tool_slug == "echo_tool" || tool.tool_slug == "echo-tool")
        }));
    }

    #[test]
    fn runtime_plan_enforces_trust_gate_per_tool_kind() {
        let mut skill = skill_with_runtime_tools(
            "untrusted-shell",
            vec![SkillRuntimeToolDefinition {
                tool_slug: "shell-run".to_owned(),
                description: "desc".to_owned(),
                kind: SkillRuntimeToolKind::Shell,
                parameters: json!({"type":"object"}),
                execution_class: RuntimeExecutionClassHint::Shared,
                config: json!({}),
                output_policy: None,
            }],
        );
        skill.runtime.trust_level = SkillTrustLevel::Untrusted;

        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![skill],
        };
        let active = resolve_skills(SkillResolutionInput {
            explicit_refs: &[SkillExplicitRef {
                capability_id: "skill:user:untrusted-shell".to_owned(),
                label: Some("untrusted-shell".to_owned()),
                slug: "untrusted-shell".to_owned(),
                source_kind: "user".to_owned(),
            }],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &SkillPolicySet::default(),
            validation_policy: SkillValidationPolicy {
                allow_untrusted_install: true,
                ..SkillValidationPolicy::default()
            },
            dependency_input: &DependencyCheckInput::baseline(),
        })
        .active;

        let plan = build_skill_runtime_plan(
            &active,
            SkillRuntimeBudget {
                enable_dynamic_tools: true,
                max_dynamic_tools_per_skill: 16,
                allow_shell_tools: true,
                allow_http_tools: true,
                allow_function_proxy_tools: true,
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Community,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            },
        );

        assert!(plan.tools.is_empty());
        assert!(plan.excluded_tools.iter().any(|tool| {
            tool.tool_slug == "shell-run"
                && tool.reason == RuntimeToolExcludedReason::TrustLevelTooLow
        }));
    }

    #[test]
    fn runtime_recheck_blocks_when_dependency_disappears() {
        let active = resolved_active_with_tools(
            "my-skill",
            vec![SkillRuntimeToolDefinition {
                tool_slug: "shell-run".to_owned(),
                description: "desc".to_owned(),
                kind: SkillRuntimeToolKind::Shell,
                parameters: json!({"type":"object"}),
                execution_class: RuntimeExecutionClassHint::Shared,
                config: json!({}),
                output_policy: None,
            }],
        );

        let mut plan = build_skill_runtime_plan(
            &active,
            SkillRuntimeBudget {
                enable_dynamic_tools: true,
                max_dynamic_tools_per_skill: 16,
                allow_shell_tools: true,
                allow_http_tools: true,
                allow_function_proxy_tools: true,
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Community,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            },
        );
        assert_eq!(plan.tools.len(), 1);
        plan.tools[0].trust_level = SkillTrustLevel::Verified;
        plan.tools[0].dependencies.commands = vec!["definitely-missing-phase3-bin".to_owned()];

        let check = recheck_runtime_tool_execution(
            &plan.tools[0],
            &RuntimeExecutionRecheckPolicy::default(),
        );
        assert!(!check.allowed);
        assert_eq!(
            check.reason_code.as_deref(),
            Some("runtime.dependency_missing")
        );
        assert!(!check.dependency_diagnostics.is_empty());
    }
}
