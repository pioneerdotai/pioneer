use crate::output_policy::{
    DeltaOutputPolicy, DiagnosticExcerptPolicy, LlmOutputPolicy, LlmRetentionPolicy,
    RecoveryOutputPolicy, StorageOutputPolicy, TimelineOutputPolicy, ToolOutputPolicySnapshot,
    ToolOutputProjectionKind,
};
use pioneer_skills::{
    DynamicToolOutputPolicyDeclaration, SkillSourceKind, SkillTrustLevel, trust_satisfies_minimum,
};
use serde::{Deserialize, Serialize};

const DEFAULT_LLM_MAX_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_SUMMARY_CHARS: usize = 2_000;
const DEFAULT_SHELL_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_SHELL_TOTAL_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_DIAGNOSTIC_CHARS: usize = 4_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamicToolKind {
    Http,
    Shell,
    FunctionProxy,
    Generic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamicToolPolicyDiagnosticCode {
    InvalidDeclaration,
    UnsupportedMode,
    TrustNarrowedTimeline,
    TrustNarrowedStorage,
    TrustNarrowedDeltas,
    KindNarrowedHttpBody,
    KindNarrowedBlobOutput,
    ShellPersistenceNotAllowed,
    FunctionProxyTargetMissing,
    FunctionProxyTargetNarrowed,
    LimitClamped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DynamicToolPolicyDiagnostic {
    pub code: DynamicToolPolicyDiagnosticCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicToolOutputPolicyCaps {
    pub default_llm_max_bytes: usize,
    pub max_llm_retention_bytes: usize,
    pub max_summary_chars: usize,
    pub max_shell_storage_bytes: usize,
    pub max_delta_chunk_bytes: usize,
    pub max_delta_total_bytes: usize,
    pub shell_persist_min_trust: SkillTrustLevel,
    pub allow_dynamic_shell_full_output: bool,
    pub allow_dynamic_function_proxy_policy_inheritance: bool,
}

impl Default for DynamicToolOutputPolicyCaps {
    fn default() -> Self {
        Self {
            default_llm_max_bytes: DEFAULT_LLM_MAX_BYTES,
            max_llm_retention_bytes: DEFAULT_LLM_MAX_BYTES,
            max_summary_chars: DEFAULT_SUMMARY_CHARS,
            max_shell_storage_bytes: DEFAULT_SHELL_TOTAL_BYTES,
            max_delta_chunk_bytes: DEFAULT_SHELL_CHUNK_BYTES,
            max_delta_total_bytes: DEFAULT_SHELL_TOTAL_BYTES,
            shell_persist_min_trust: SkillTrustLevel::Verified,
            allow_dynamic_shell_full_output: false,
            allow_dynamic_function_proxy_policy_inheritance: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DynamicToolPolicyContext {
    pub canonical_tool_name: String,
    pub skill_slug: String,
    pub skill_fingerprint: String,
    pub source_kind: SkillSourceKind,
    pub trust_level: SkillTrustLevel,
    pub kind: DynamicToolKind,
    pub target_tool_name: Option<String>,
    pub target_output_policy: Option<ToolOutputPolicySnapshot>,
    pub target_projection_kind: Option<ToolOutputProjectionKind>,
    pub host_caps: DynamicToolOutputPolicyCaps,
}

#[derive(Debug, Clone)]
pub struct DynamicToolPolicyResolution {
    pub effective_policy: ToolOutputPolicySnapshot,
    pub projection_kind: ToolOutputProjectionKind,
    pub requested_policy: Option<DynamicToolOutputPolicyDeclaration>,
    pub diagnostics: Vec<DynamicToolPolicyDiagnostic>,
}

pub fn resolve_dynamic_tool_output_policy(
    context: DynamicToolPolicyContext,
    requested_policy: Option<DynamicToolOutputPolicyDeclaration>,
) -> DynamicToolPolicyResolution {
    let mut diagnostics = Vec::new();
    let mut policy = default_policy_for_kind(&context);

    if let Some(requested) = requested_policy.as_ref() {
        apply_requested_policy(&mut policy, requested, &context.host_caps);
    }

    clamp_policy_limits(&mut policy, &context.host_caps, &mut diagnostics);
    narrow_policy_for_kind_and_trust(&mut policy, &context, &mut diagnostics);

    let projection_kind = match context.kind {
        DynamicToolKind::Http => ToolOutputProjectionKind::DynamicHttp,
        DynamicToolKind::Shell => ToolOutputProjectionKind::DynamicShell,
        DynamicToolKind::Generic => ToolOutputProjectionKind::DynamicGeneric,
        DynamicToolKind::FunctionProxy => {
            let target_tool_name = context
                .target_tool_name
                .clone()
                .unwrap_or_else(|| "__missing_target__".to_owned());
            let target_policy = context
                .target_output_policy
                .clone()
                .unwrap_or_else(crate::dynamic_unknown_output_policy);
            let target_projection_kind = context
                .target_projection_kind
                .clone()
                .unwrap_or(ToolOutputProjectionKind::DynamicGeneric);
            if context.target_output_policy.is_none() {
                diagnostics.push(diag(
                    DynamicToolPolicyDiagnosticCode::FunctionProxyTargetMissing,
                    format!(
                        "function proxy `{}` target policy is missing; using dynamic safe default",
                        context.canonical_tool_name
                    ),
                ));
            }
            ToolOutputProjectionKind::DynamicFunctionProxy {
                target_tool_name,
                target_policy,
                target_projection_kind: Box::new(target_projection_kind),
            }
        }
    };

    DynamicToolPolicyResolution {
        effective_policy: policy,
        projection_kind,
        requested_policy,
        diagnostics,
    }
}

fn default_policy_for_kind(context: &DynamicToolPolicyContext) -> ToolOutputPolicySnapshot {
    let caps = &context.host_caps;
    match context.kind {
        DynamicToolKind::Http => ToolOutputPolicySnapshot {
            llm: LlmOutputPolicy::Structured {
                max_bytes: caps.default_llm_max_bytes,
            },
            llm_retention: retained_llm_policy(caps),
            timeline: TimelineOutputPolicy::Summary {
                max_chars: caps.max_summary_chars,
            },
            storage: StorageOutputPolicy::MetadataOnly,
            recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
            deltas: DeltaOutputPolicy::ProgressOnly,
        },
        DynamicToolKind::Shell => ToolOutputPolicySnapshot {
            llm: LlmOutputPolicy::Full {
                max_bytes: caps.default_llm_max_bytes,
            },
            llm_retention: retained_llm_policy(caps),
            timeline: TimelineOutputPolicy::Summary {
                max_chars: caps.max_summary_chars,
            },
            storage: StorageOutputPolicy::MetadataOnly,
            recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
            deltas: DeltaOutputPolicy::ProgressOnly,
        },
        DynamicToolKind::FunctionProxy
            if context
                .host_caps
                .allow_dynamic_function_proxy_policy_inheritance =>
        {
            context
                .target_output_policy
                .clone()
                .unwrap_or_else(crate::dynamic_unknown_output_policy)
        }
        DynamicToolKind::FunctionProxy | DynamicToolKind::Generic => {
            crate::dynamic_unknown_output_policy()
        }
    }
}

fn retained_llm_policy(caps: &DynamicToolOutputPolicyCaps) -> LlmRetentionPolicy {
    LlmRetentionPolicy::UntilTurnTerminal {
        max_bytes: caps.max_llm_retention_bytes,
    }
}

fn evidence_recovery_policy(diagnostic_excerpt: DiagnosticExcerptPolicy) -> RecoveryOutputPolicy {
    RecoveryOutputPolicy::Evidence {
        include_exit_status: true,
        include_error_class: true,
        include_retry_hint: true,
        diagnostic_excerpt,
        include_fingerprints: true,
    }
}

fn apply_requested_policy(
    policy: &mut ToolOutputPolicySnapshot,
    requested: &DynamicToolOutputPolicyDeclaration,
    caps: &DynamicToolOutputPolicyCaps,
) {
    if let Some(llm) = requested.llm.as_ref() {
        policy.llm = llm.to_policy(caps.default_llm_max_bytes);
    }
    if let Some(llm_retention) = requested.llm_retention.as_ref() {
        policy.llm_retention = llm_retention.to_policy(caps.max_llm_retention_bytes);
    }
    if let Some(timeline) = requested.timeline.as_ref() {
        policy.timeline = timeline.to_policy(caps.max_summary_chars, caps.max_shell_storage_bytes);
    }
    if let Some(storage) = requested.storage.as_ref() {
        policy.storage = storage.to_policy(caps.max_summary_chars, caps.max_shell_storage_bytes);
    }
    if let Some(recovery) = requested.recovery.as_ref() {
        policy.recovery = recovery.to_policy();
    }
    if let Some(deltas) = requested.deltas.as_ref() {
        policy.deltas = deltas.to_policy(caps.max_delta_chunk_bytes, caps.max_delta_total_bytes);
    }
}

fn clamp_policy_limits(
    policy: &mut ToolOutputPolicySnapshot,
    caps: &DynamicToolOutputPolicyCaps,
    diagnostics: &mut Vec<DynamicToolPolicyDiagnostic>,
) {
    match &mut policy.llm {
        LlmOutputPolicy::Full { max_bytes } | LlmOutputPolicy::Structured { max_bytes } => {
            clamp_usize(
                max_bytes,
                caps.default_llm_max_bytes,
                diagnostics,
                "llm max_bytes",
            );
        }
        LlmOutputPolicy::SummaryOnly => {}
    }

    if let LlmRetentionPolicy::UntilTurnTerminal { max_bytes, .. } = &mut policy.llm_retention {
        clamp_usize(
            max_bytes,
            caps.max_llm_retention_bytes,
            diagnostics,
            "llm retention max_bytes",
        );
    }

    match &mut policy.timeline {
        TimelineOutputPolicy::Full { max_bytes } => {
            clamp_usize(
                max_bytes,
                caps.max_shell_storage_bytes,
                diagnostics,
                "timeline max_bytes",
            );
        }
        TimelineOutputPolicy::Summary { max_chars } => {
            clamp_usize(
                max_chars,
                caps.max_summary_chars,
                diagnostics,
                "timeline max_chars",
            );
        }
        TimelineOutputPolicy::MetadataOnly | TimelineOutputPolicy::Hidden => {}
    }

    match &mut policy.storage {
        StorageOutputPolicy::Full { max_bytes } => {
            clamp_usize(
                max_bytes,
                caps.max_shell_storage_bytes,
                diagnostics,
                "storage max_bytes",
            );
        }
        StorageOutputPolicy::Summary { max_chars } => {
            clamp_usize(
                max_chars,
                caps.max_summary_chars,
                diagnostics,
                "storage max_chars",
            );
        }
        StorageOutputPolicy::MetadataOnly | StorageOutputPolicy::None => {}
    }

    if let RecoveryOutputPolicy::Evidence {
        diagnostic_excerpt, ..
    } = &mut policy.recovery
    {
        match diagnostic_excerpt {
            DiagnosticExcerptPolicy::ErrorsOnly { max_chars }
            | DiagnosticExcerptPolicy::Output { max_chars } => {
                clamp_usize(
                    max_chars,
                    DEFAULT_DIAGNOSTIC_CHARS,
                    diagnostics,
                    "recovery diagnostic max_chars",
                );
            }
            DiagnosticExcerptPolicy::Disabled => {}
        }
    }

    match &mut policy.deltas {
        DeltaOutputPolicy::PersistAndDisplay {
            max_chunk_bytes,
            max_total_bytes,
        } => {
            clamp_usize(
                max_chunk_bytes,
                caps.max_delta_chunk_bytes,
                diagnostics,
                "delta max_chunk_bytes",
            );
            clamp_usize(
                max_total_bytes,
                caps.max_delta_total_bytes,
                diagnostics,
                "delta max_total_bytes",
            );
            if *max_chunk_bytes > *max_total_bytes {
                *max_chunk_bytes = *max_total_bytes;
                diagnostics.push(diag(
                    DynamicToolPolicyDiagnosticCode::LimitClamped,
                    "delta max_chunk_bytes was clamped to max_total_bytes",
                ));
            }
        }
        DeltaOutputPolicy::ProgressOnly | DeltaOutputPolicy::Disabled => {}
    }
}

fn narrow_policy_for_kind_and_trust(
    policy: &mut ToolOutputPolicySnapshot,
    context: &DynamicToolPolicyContext,
    diagnostics: &mut Vec<DynamicToolPolicyDiagnostic>,
) {
    match context.kind {
        DynamicToolKind::Http => narrow_http_policy(policy, diagnostics),
        DynamicToolKind::Shell => narrow_shell_policy(policy, context, diagnostics),
        DynamicToolKind::FunctionProxy => {
            narrow_function_proxy_policy(policy, context, diagnostics);
            let target_is_shell_like =
                context.target_output_policy.as_ref().is_some_and(|target| {
                    matches!(target.storage, StorageOutputPolicy::Full { .. })
                        || matches!(target.deltas, DeltaOutputPolicy::PersistAndDisplay { .. })
                });
            if target_is_shell_like {
                narrow_shell_policy(policy, context, diagnostics);
            } else {
                narrow_generic_policy(policy, context, diagnostics);
            }
        }
        DynamicToolKind::Generic => narrow_generic_policy(policy, context, diagnostics),
    }
}

fn narrow_http_policy(
    policy: &mut ToolOutputPolicySnapshot,
    diagnostics: &mut Vec<DynamicToolPolicyDiagnostic>,
) {
    if matches!(policy.timeline, TimelineOutputPolicy::Full { .. }) {
        policy.timeline = TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        };
        diagnostics.push(diag(
            DynamicToolPolicyDiagnosticCode::KindNarrowedHttpBody,
            "dynamic HTTP timeline full output was narrowed to summary",
        ));
    }
    if !matches!(
        policy.storage,
        StorageOutputPolicy::MetadataOnly | StorageOutputPolicy::None
    ) {
        policy.storage = StorageOutputPolicy::MetadataOnly;
        diagnostics.push(diag(
            DynamicToolPolicyDiagnosticCode::KindNarrowedHttpBody,
            "dynamic HTTP storage was narrowed to metadata-only",
        ));
    }
    disable_recovery_excerpt(
        policy,
        DynamicToolPolicyDiagnosticCode::KindNarrowedHttpBody,
        diagnostics,
    );
    if matches!(policy.deltas, DeltaOutputPolicy::PersistAndDisplay { .. }) {
        policy.deltas = DeltaOutputPolicy::ProgressOnly;
        diagnostics.push(diag(
            DynamicToolPolicyDiagnosticCode::TrustNarrowedDeltas,
            "dynamic HTTP output deltas were narrowed to progress-only",
        ));
    }
}

fn narrow_generic_policy(
    policy: &mut ToolOutputPolicySnapshot,
    context: &DynamicToolPolicyContext,
    diagnostics: &mut Vec<DynamicToolPolicyDiagnostic>,
) {
    match context.trust_level {
        SkillTrustLevel::Untrusted => {
            if !matches!(
                policy.timeline,
                TimelineOutputPolicy::MetadataOnly | TimelineOutputPolicy::Hidden
            ) {
                policy.timeline = TimelineOutputPolicy::MetadataOnly;
                diagnostics.push(diag(
                    DynamicToolPolicyDiagnosticCode::TrustNarrowedTimeline,
                    "untrusted dynamic timeline output was narrowed to metadata-only",
                ));
            }
            if !matches!(
                policy.storage,
                StorageOutputPolicy::MetadataOnly | StorageOutputPolicy::None
            ) {
                policy.storage = StorageOutputPolicy::MetadataOnly;
                diagnostics.push(diag(
                    DynamicToolPolicyDiagnosticCode::TrustNarrowedStorage,
                    "untrusted dynamic storage was narrowed to metadata-only",
                ));
            }
        }
        SkillTrustLevel::Community | SkillTrustLevel::Verified | SkillTrustLevel::Internal => {
            if matches!(policy.timeline, TimelineOutputPolicy::Full { .. }) {
                policy.timeline = TimelineOutputPolicy::Summary {
                    max_chars: context.host_caps.max_summary_chars,
                };
                diagnostics.push(diag(
                    DynamicToolPolicyDiagnosticCode::TrustNarrowedTimeline,
                    "generic dynamic timeline full output was narrowed to summary",
                ));
            }
            if matches!(policy.storage, StorageOutputPolicy::Full { .. }) {
                policy.storage = StorageOutputPolicy::Summary {
                    max_chars: context.host_caps.max_summary_chars,
                };
                diagnostics.push(diag(
                    DynamicToolPolicyDiagnosticCode::TrustNarrowedStorage,
                    "generic dynamic storage full output was narrowed to summary",
                ));
            }
        }
    }

    disable_recovery_excerpt(
        policy,
        DynamicToolPolicyDiagnosticCode::KindNarrowedBlobOutput,
        diagnostics,
    );
    if matches!(policy.deltas, DeltaOutputPolicy::PersistAndDisplay { .. }) {
        policy.deltas = DeltaOutputPolicy::ProgressOnly;
        diagnostics.push(diag(
            DynamicToolPolicyDiagnosticCode::TrustNarrowedDeltas,
            "generic dynamic output deltas were narrowed to progress-only",
        ));
    }
}

fn narrow_shell_policy(
    policy: &mut ToolOutputPolicySnapshot,
    context: &DynamicToolPolicyContext,
    diagnostics: &mut Vec<DynamicToolPolicyDiagnostic>,
) {
    let shell_full_allowed = context.host_caps.allow_dynamic_shell_full_output
        && trust_satisfies_minimum(
            context.trust_level.clone(),
            context.host_caps.shell_persist_min_trust.clone(),
        );

    if !shell_full_allowed {
        if matches!(policy.timeline, TimelineOutputPolicy::Full { .. }) {
            policy.timeline = TimelineOutputPolicy::Summary {
                max_chars: context.host_caps.max_summary_chars,
            };
            diagnostics.push(diag(
                DynamicToolPolicyDiagnosticCode::ShellPersistenceNotAllowed,
                "dynamic shell full timeline output is not allowed by host trust caps",
            ));
        }
        if matches!(policy.storage, StorageOutputPolicy::Full { .. }) {
            policy.storage = StorageOutputPolicy::MetadataOnly;
            diagnostics.push(diag(
                DynamicToolPolicyDiagnosticCode::ShellPersistenceNotAllowed,
                "dynamic shell full storage output is not allowed by host trust caps",
            ));
        }
        if matches!(policy.deltas, DeltaOutputPolicy::PersistAndDisplay { .. }) {
            policy.deltas = DeltaOutputPolicy::ProgressOnly;
            diagnostics.push(diag(
                DynamicToolPolicyDiagnosticCode::ShellPersistenceNotAllowed,
                "dynamic shell output deltas are not allowed by host trust caps",
            ));
        }
        disable_recovery_excerpt(
            policy,
            DynamicToolPolicyDiagnosticCode::ShellPersistenceNotAllowed,
            diagnostics,
        );
        return;
    }
}

fn narrow_function_proxy_policy(
    policy: &mut ToolOutputPolicySnapshot,
    context: &DynamicToolPolicyContext,
    diagnostics: &mut Vec<DynamicToolPolicyDiagnostic>,
) {
    let Some(target) = context.target_output_policy.as_ref() else {
        return;
    };

    if policy_rank_timeline(&policy.timeline) > policy_rank_timeline(&target.timeline) {
        policy.timeline = target.timeline.clone();
        diagnostics.push(diag(
            DynamicToolPolicyDiagnosticCode::FunctionProxyTargetNarrowed,
            "function proxy timeline policy was narrowed to target policy",
        ));
    }
    if policy_rank_storage(&policy.storage) > policy_rank_storage(&target.storage) {
        policy.storage = target.storage.clone();
        diagnostics.push(diag(
            DynamicToolPolicyDiagnosticCode::FunctionProxyTargetNarrowed,
            "function proxy storage policy was narrowed to target policy",
        ));
    }
    if policy_rank_deltas(&policy.deltas) > policy_rank_deltas(&target.deltas) {
        policy.deltas = target.deltas.clone();
        diagnostics.push(diag(
            DynamicToolPolicyDiagnosticCode::FunctionProxyTargetNarrowed,
            "function proxy delta policy was narrowed to target policy",
        ));
    }
}

fn disable_recovery_excerpt(
    policy: &mut ToolOutputPolicySnapshot,
    code: DynamicToolPolicyDiagnosticCode,
    diagnostics: &mut Vec<DynamicToolPolicyDiagnostic>,
) {
    if let RecoveryOutputPolicy::Evidence {
        diagnostic_excerpt, ..
    } = &mut policy.recovery
        && !matches!(diagnostic_excerpt, DiagnosticExcerptPolicy::Disabled)
    {
        *diagnostic_excerpt = DiagnosticExcerptPolicy::Disabled;
        diagnostics.push(diag(
            code,
            "dynamic recovery diagnostic excerpt was disabled",
        ));
    }
}

fn policy_rank_timeline(policy: &TimelineOutputPolicy) -> u8 {
    match policy {
        TimelineOutputPolicy::Hidden => 0,
        TimelineOutputPolicy::MetadataOnly => 1,
        TimelineOutputPolicy::Summary { .. } => 2,
        TimelineOutputPolicy::Full { .. } => 3,
    }
}

fn policy_rank_storage(policy: &StorageOutputPolicy) -> u8 {
    match policy {
        StorageOutputPolicy::None => 0,
        StorageOutputPolicy::MetadataOnly => 1,
        StorageOutputPolicy::Summary { .. } => 2,
        StorageOutputPolicy::Full { .. } => 3,
    }
}

fn policy_rank_deltas(policy: &DeltaOutputPolicy) -> u8 {
    match policy {
        DeltaOutputPolicy::Disabled => 0,
        DeltaOutputPolicy::ProgressOnly => 1,
        DeltaOutputPolicy::PersistAndDisplay { .. } => 3,
    }
}

fn clamp_usize(
    value: &mut usize,
    max: usize,
    diagnostics: &mut Vec<DynamicToolPolicyDiagnostic>,
    field: &str,
) {
    if *value <= max {
        return;
    }
    *value = max;
    diagnostics.push(diag(
        DynamicToolPolicyDiagnosticCode::LimitClamped,
        format!("{field} exceeded host cap and was clamped to {max}"),
    ));
}

fn diag(
    code: DynamicToolPolicyDiagnosticCode,
    message: impl Into<String>,
) -> DynamicToolPolicyDiagnostic {
    DynamicToolPolicyDiagnostic {
        code,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_skills::{
        DynamicDeltaOutputRequest, DynamicRecoveryOutputRequest, DynamicStorageOutputRequest,
        DynamicTimelineOutputRequest,
    };

    fn context(kind: DynamicToolKind, trust_level: SkillTrustLevel) -> DynamicToolPolicyContext {
        DynamicToolPolicyContext {
            canonical_tool_name: "skill.workspace-tool.test".to_owned(),
            skill_slug: "workspace/tool".to_owned(),
            skill_fingerprint: "fp".to_owned(),
            source_kind: SkillSourceKind::Workspace,
            trust_level,
            kind,
            target_tool_name: None,
            target_output_policy: None,
            target_projection_kind: None,
            host_caps: DynamicToolOutputPolicyCaps {
                allow_dynamic_shell_full_output: true,
                shell_persist_min_trust: SkillTrustLevel::Verified,
                ..DynamicToolOutputPolicyCaps::default()
            },
        }
    }

    #[test]
    fn missing_declaration_resolves_to_safe_dynamic_default() {
        let resolution = resolve_dynamic_tool_output_policy(
            context(DynamicToolKind::Generic, SkillTrustLevel::Community),
            None,
        );

        assert!(matches!(
            resolution.effective_policy.storage,
            StorageOutputPolicy::MetadataOnly
        ));
        assert!(matches!(
            resolution.effective_policy.timeline,
            TimelineOutputPolicy::MetadataOnly
        ));
        assert!(matches!(
            resolution.effective_policy.deltas,
            DeltaOutputPolicy::ProgressOnly
        ));
    }

    #[test]
    fn untrusted_full_storage_request_is_narrowed_to_metadata() {
        let requested = DynamicToolOutputPolicyDeclaration {
            llm: None,
            llm_retention: None,
            timeline: Some(DynamicTimelineOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            storage: Some(DynamicStorageOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            recovery: None,
            deltas: None,
        };

        let resolution = resolve_dynamic_tool_output_policy(
            context(DynamicToolKind::Generic, SkillTrustLevel::Untrusted),
            Some(requested),
        );

        assert!(matches!(
            resolution.effective_policy.storage,
            StorageOutputPolicy::MetadataOnly
        ));
        assert!(matches!(
            resolution.effective_policy.timeline,
            TimelineOutputPolicy::MetadataOnly
        ));
    }

    #[test]
    fn http_full_storage_request_is_narrowed_to_metadata() {
        let requested = DynamicToolOutputPolicyDeclaration {
            llm: None,
            llm_retention: None,
            timeline: None,
            storage: Some(DynamicStorageOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            recovery: None,
            deltas: Some(DynamicDeltaOutputRequest::PersistAndDisplay {
                max_chunk_bytes: Some(1024),
                max_total_bytes: Some(4096),
            }),
        };

        let resolution = resolve_dynamic_tool_output_policy(
            context(DynamicToolKind::Http, SkillTrustLevel::Verified),
            Some(requested),
        );

        assert!(matches!(
            resolution.effective_policy.storage,
            StorageOutputPolicy::MetadataOnly
        ));
        assert!(matches!(
            resolution.effective_policy.deltas,
            DeltaOutputPolicy::ProgressOnly
        ));
    }

    #[test]
    fn shell_full_output_requires_trust_and_host_cap() {
        let requested = DynamicToolOutputPolicyDeclaration {
            llm: None,
            llm_retention: None,
            timeline: Some(DynamicTimelineOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            storage: Some(DynamicStorageOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            recovery: None,
            deltas: Some(DynamicDeltaOutputRequest::PersistAndDisplay {
                max_chunk_bytes: Some(1024),
                max_total_bytes: Some(4096),
            }),
        };

        let denied = resolve_dynamic_tool_output_policy(
            context(DynamicToolKind::Shell, SkillTrustLevel::Community),
            Some(requested.clone()),
        );
        assert!(matches!(
            denied.effective_policy.storage,
            StorageOutputPolicy::MetadataOnly
        ));
        assert!(matches!(
            denied.effective_policy.deltas,
            DeltaOutputPolicy::ProgressOnly
        ));

        let allowed = resolve_dynamic_tool_output_policy(
            context(DynamicToolKind::Shell, SkillTrustLevel::Verified),
            Some(requested),
        );
        assert!(matches!(
            allowed.effective_policy.storage,
            StorageOutputPolicy::Full { .. }
        ));
        assert!(matches!(
            allowed.effective_policy.deltas,
            DeltaOutputPolicy::PersistAndDisplay { .. }
        ));
    }

    #[test]
    fn function_proxy_cannot_widen_target_policy() {
        let mut ctx = context(DynamicToolKind::FunctionProxy, SkillTrustLevel::Verified);
        ctx.target_tool_name = Some("read_file".to_owned());
        ctx.target_output_policy = Some(ToolOutputPolicySnapshot::for_tool_name("read_file"));
        ctx.target_projection_kind = Some(ToolOutputProjectionKind::Builtin);

        let requested = DynamicToolOutputPolicyDeclaration {
            llm: None,
            llm_retention: None,
            timeline: Some(DynamicTimelineOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            storage: Some(DynamicStorageOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            recovery: None,
            deltas: None,
        };

        let resolution = resolve_dynamic_tool_output_policy(ctx, Some(requested));

        assert!(matches!(
            resolution.effective_policy.storage,
            StorageOutputPolicy::MetadataOnly
        ));
    }

    #[test]
    fn community_generic_full_request_is_narrowed_to_summary() {
        let requested = DynamicToolOutputPolicyDeclaration {
            llm: None,
            llm_retention: None,
            timeline: Some(DynamicTimelineOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            storage: Some(DynamicStorageOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            recovery: None,
            deltas: None,
        };

        let resolution = resolve_dynamic_tool_output_policy(
            context(DynamicToolKind::Generic, SkillTrustLevel::Community),
            Some(requested),
        );

        assert!(matches!(
            resolution.effective_policy.timeline,
            TimelineOutputPolicy::Summary { .. }
        ));
        assert!(matches!(
            resolution.effective_policy.storage,
            StorageOutputPolicy::Summary { .. }
        ));
    }

    #[test]
    fn requested_limits_are_clamped_and_diagnosed() {
        let mut ctx = context(DynamicToolKind::Shell, SkillTrustLevel::Verified);
        ctx.host_caps.max_shell_storage_bytes = 128;
        ctx.host_caps.max_delta_chunk_bytes = 32;
        ctx.host_caps.max_delta_total_bytes = 64;

        let requested = DynamicToolOutputPolicyDeclaration {
            llm: None,
            llm_retention: None,
            timeline: Some(DynamicTimelineOutputRequest::Full {
                max_bytes: Some(4096),
            }),
            storage: Some(DynamicStorageOutputRequest::Full {
                max_bytes: Some(4096),
            }),
            recovery: Some(DynamicRecoveryOutputRequest::Evidence {
                include_exit_status: None,
                include_error_class: None,
                include_retry_hint: None,
                diagnostic_excerpt: None,
                include_fingerprints: None,
            }),
            deltas: Some(DynamicDeltaOutputRequest::PersistAndDisplay {
                max_chunk_bytes: Some(512),
                max_total_bytes: Some(1024),
            }),
        };

        let resolution = resolve_dynamic_tool_output_policy(ctx, Some(requested));

        assert!(matches!(
            resolution.effective_policy.timeline,
            TimelineOutputPolicy::Full { max_bytes: 128 }
        ));
        assert!(matches!(
            resolution.effective_policy.storage,
            StorageOutputPolicy::Full { max_bytes: 128 }
        ));
        assert!(matches!(
            resolution.effective_policy.deltas,
            DeltaOutputPolicy::PersistAndDisplay {
                max_chunk_bytes: 32,
                max_total_bytes: 64
            }
        ));
        assert!(resolution.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == DynamicToolPolicyDiagnosticCode::LimitClamped
        }));
    }

    #[test]
    fn function_proxy_to_exec_command_requires_shell_caps() {
        let mut requested_context =
            context(DynamicToolKind::FunctionProxy, SkillTrustLevel::Verified);
        requested_context.target_tool_name = Some("exec_command".to_owned());
        requested_context.target_output_policy =
            Some(ToolOutputPolicySnapshot::for_tool_name("exec_command"));
        requested_context.target_projection_kind = Some(ToolOutputProjectionKind::Builtin);

        let requested = DynamicToolOutputPolicyDeclaration {
            llm: None,
            llm_retention: None,
            timeline: Some(DynamicTimelineOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            storage: Some(DynamicStorageOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            recovery: None,
            deltas: Some(DynamicDeltaOutputRequest::PersistAndDisplay {
                max_chunk_bytes: Some(1024),
                max_total_bytes: Some(4096),
            }),
        };

        let allowed =
            resolve_dynamic_tool_output_policy(requested_context.clone(), Some(requested.clone()));
        assert!(matches!(
            allowed.effective_policy.storage,
            StorageOutputPolicy::Full { .. }
        ));

        requested_context.host_caps.allow_dynamic_shell_full_output = false;
        let denied = resolve_dynamic_tool_output_policy(requested_context, Some(requested));
        assert!(matches!(
            denied.effective_policy.storage,
            StorageOutputPolicy::MetadataOnly
        ));
        assert!(matches!(
            denied.effective_policy.deltas,
            DeltaOutputPolicy::ProgressOnly
        ));
    }

    #[test]
    fn function_proxy_missing_target_uses_safe_default_with_diagnostic() {
        let resolution = resolve_dynamic_tool_output_policy(
            context(DynamicToolKind::FunctionProxy, SkillTrustLevel::Verified),
            None,
        );

        assert!(matches!(
            resolution.effective_policy.storage,
            StorageOutputPolicy::MetadataOnly
        ));
        assert!(resolution.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == DynamicToolPolicyDiagnosticCode::FunctionProxyTargetMissing
        }));
    }

    #[test]
    fn resolver_output_is_deterministic_for_identical_inputs() {
        let requested = DynamicToolOutputPolicyDeclaration {
            llm: None,
            llm_retention: None,
            timeline: Some(DynamicTimelineOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            storage: Some(DynamicStorageOutputRequest::Full {
                max_bytes: Some(100_000),
            }),
            recovery: None,
            deltas: None,
        };
        let ctx = context(DynamicToolKind::Generic, SkillTrustLevel::Community);

        let first = resolve_dynamic_tool_output_policy(ctx.clone(), Some(requested.clone()));
        let second = resolve_dynamic_tool_output_policy(ctx, Some(requested));

        assert_eq!(first.effective_policy, second.effective_policy);
        assert_eq!(first.projection_kind, second.projection_kind);
        assert_eq!(first.diagnostics, second.diagnostics);
    }
}
