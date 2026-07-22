use super::*;
use anyhow::{Result, bail};
use pioneer_crud::{
    SkillAuditEventRecord, SkillInstallationPatch, SkillInstallationRecord,
    SkillPackInstallationRecord, WorkspaceSkillPolicyRecord,
};
use pioneer_protocol::constants::{events, methods};
use pioneer_protocol::{
    JsonRpcError, JsonRpcErrorResponse, JsonRpcResponse, RequestId, SkillAuditTimelineItem,
    SkillChangedItem, SkillDependencyDiagnostic, SkillHealthItem, SkillHealthSummary, SkillId,
    SkillInstallState, SkillLifecycleAuditSummary, SkillLifecycleRemovedSkill,
    SkillLifecycleResultSkill, SkillLifecycleSource, SkillListItem, SkillListParams,
    SkillListResponse, SkillPackChangedItem, SkillPackId, SkillPackInstallationItem,
    SkillPackMembership, SkillPolicyState, SkillSecurityFinding, SkillTrustGateStatus,
    SkillValidationDiagnostic, SkillWorkspacePolicy, SkillsChangedNotification, SkillsHealthParams,
    SkillsHealthResponse, SkillsInstallParams, SkillsInstallResponse, SkillsPackInstallParams,
    SkillsPackInstallResponse, SkillsPackUninstallParams, SkillsPackUninstallResponse,
    SkillsPackUpdateParams, SkillsPackUpdateResponse, SkillsPolicyListParams,
    SkillsPolicyListResponse, SkillsPolicySetParams, SkillsPolicySetResponse,
    SkillsUninstallParams, SkillsUninstallResponse, SkillsUpdateParams, SkillsUpdateResponse,
};
use pioneer_skills::{
    DependencyCheckInput, SkillCatalogLoadParams, SkillExplicitRef, SkillPolicy, SkillPolicyKey,
    SkillPolicySet, SkillResolutionInput, SkillSecurityPolicy, SkillSourceKind,
    SkillValidationPolicy, effective_policy_for_skill, explicit_ref_matches_skill, load_catalog,
    resolve_skills, skill_implicit_invocation_editable,
};
use serde_json::json;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};
use tracing::warn;

const WORKSPACE_ID_TOKEN: &str = "{workspaceId}";
const SKILLS_ERROR_NOT_FOUND: &str = "skills.not_found";
const SKILLS_ERROR_INVALID_REQUEST: &str = "skills.invalid_request";
const SKILLS_ERROR_SOURCE_NOT_SUPPORTED: &str = "skills.source_not_supported";
const SKILLS_ERROR_INSTALL_BLOCKED_SECURITY: &str = "skills.install_blocked.security";
const SKILLS_ERROR_INSTALL_BLOCKED_TRUST: &str = "skills.install_blocked.trust";
const SKILLS_ERROR_INSTALL_BLOCKED_DEPENDENCY: &str = "skills.install_blocked.dependency";
const SKILLS_ERROR_UPDATE_CONFLICT_FINGERPRINT: &str =
    "skills.update_conflict.fingerprint_mismatch";
const SKILLS_ERROR_UPLOAD_INVALID_REQUEST: &str = "skills.upload.invalid_request";
const SKILLS_ERROR_UPLOAD_NOT_FOUND: &str = "skills.upload.not_found";
const SKILLS_ERROR_UPLOAD_EXPIRED: &str = "skills.upload.expired";
const SKILLS_ERROR_UPLOAD_SIZE_LIMIT: &str = "skills.upload.size_limit";
const SKILLS_ERROR_UPLOAD_DIGEST_MISMATCH: &str = "skills.upload.digest_mismatch";
const SKILLS_ERROR_UPLOAD_INVALID_ARCHIVE: &str = "skills.upload.invalid_archive";
const SKILLS_ERROR_INTERNAL: &str = "skills.internal_error";
const SKILLS_WATCH_DEBOUNCE_MS: u64 = 1_500;

#[derive(Clone)]
pub(crate) struct SkillsRuntimeContext {
    catalog_params: SkillCatalogLoadParams,
    validation_policy: SkillValidationPolicy,
    security_policy: SkillSecurityPolicy,
    global_policy_defaults: SkillPolicy,
    user_root: PathBuf,
    user_lock_path: PathBuf,
    registry_root: PathBuf,
    registry_lock_path: PathBuf,
    upload_root: PathBuf,
    materialized_root: PathBuf,
    upload_ttl_secs: u64,
    upload_recommended_chunk_size_bytes: usize,
    upload_max_chunk_size_bytes: usize,
    max_upload_compressed_bytes: usize,
    max_upload_uncompressed_bytes: usize,
    max_upload_archive_entries: usize,
}

mod catalog;
mod lifecycle;
mod policy;
mod storage_relocation;
mod storage_startup;
mod upload;
mod watcher;
pub(crate) mod workspace;

pub(in crate::message) use upload::SKILL_UPLOAD_CHUNK_FRAME_MAGIC;

fn to_protocol_dependency(
    diagnostic: &pioneer_skills::DependencyDiagnostic,
) -> SkillDependencyDiagnostic {
    SkillDependencyDiagnostic {
        kind: format!("{:?}", diagnostic.kind).to_ascii_lowercase(),
        name: diagnostic.name.clone(),
        status: format!("{:?}", diagnostic.status).to_ascii_lowercase(),
        hint: diagnostic.hint.clone(),
    }
}

fn to_protocol_security_finding(finding: &pioneer_skills::SecurityFinding) -> SkillSecurityFinding {
    SkillSecurityFinding {
        rule_id: finding.rule_id.clone(),
        severity: finding.severity.clone(),
        message: finding.message.clone(),
        path: finding.path.clone(),
    }
}

fn to_protocol_validation_issue(
    issue: &pioneer_skills::SkillValidationIssue,
) -> SkillValidationDiagnostic {
    SkillValidationDiagnostic {
        code: issue.code.clone(),
        level: format!("{:?}", issue.level).to_ascii_lowercase(),
        message: issue.message.clone(),
        field_path: issue.field_path.clone(),
    }
}

fn blocking_validation_issues(
    skill: &pioneer_skills::SkillDefinition,
    policy: &SkillValidationPolicy,
) -> Vec<pioneer_skills::SkillValidationIssue> {
    if skill.conformance.agentskills_strict.compliant {
        return Vec::new();
    }

    let pick_errors = |issues: &[pioneer_skills::SkillValidationIssue]| {
        issues
            .iter()
            .filter(|issue| matches!(issue.level, pioneer_skills::IssueLevel::Error))
            .cloned()
            .collect::<Vec<_>>()
    };

    if policy.strict_agentskills {
        return pick_errors(&skill.conformance.agentskills_strict.issues);
    }

    if policy.accept_openclaw_profile && skill.conformance.openclaw_compat.compliant {
        return Vec::new();
    }

    if policy.accept_openclaw_profile {
        return pick_errors(&skill.conformance.openclaw_compat.issues);
    }

    pick_errors(&skill.conformance.agentskills_strict.issues)
}

fn parse_lifecycle_upload_id(source: SkillLifecycleSource) -> Result<String, String> {
    match source {
        SkillLifecycleSource::UploadedArchive { upload_id } => {
            let upload_id = upload_id.trim();
            if upload_id.is_empty() {
                return Err("upload_id is required".to_owned());
            }
            Ok(upload_id.to_owned())
        }
    }
}

#[derive(Clone)]
struct SkillInstallLocation {
    install_root: PathBuf,
    lock_path: PathBuf,
}

fn parse_installable_source_kind(raw: &str) -> Option<SkillSourceKind> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "user" => Some(SkillSourceKind::User),
        "registry" => Some(SkillSourceKind::Registry),
        _ => None,
    }
}

fn install_location_for_source_kind(
    context: &SkillsRuntimeContext,
    source_kind: &SkillSourceKind,
) -> Option<SkillInstallLocation> {
    match source_kind {
        SkillSourceKind::User => Some(SkillInstallLocation {
            install_root: context.user_root.clone(),
            lock_path: context.user_lock_path.clone(),
        }),
        SkillSourceKind::Registry => Some(SkillInstallLocation {
            install_root: context.registry_root.clone(),
            lock_path: context.registry_lock_path.clone(),
        }),
        SkillSourceKind::System => None,
    }
}

fn install_location_for_stored_source_kind(
    context: &SkillsRuntimeContext,
    stored_source_kind: &str,
) -> Result<(SkillSourceKind, SkillInstallLocation)> {
    let Some(source_kind) = parse_installable_source_kind(stored_source_kind) else {
        bail!("stored skill source kind `{stored_source_kind}` is not lifecycle-editable");
    };
    let Some(location) = install_location_for_source_kind(context, &source_kind) else {
        bail!("stored skill source kind `{stored_source_kind}` has no managed location");
    };
    Ok((source_kind, location))
}

fn pioneer_owned_install_path(
    location: &SkillInstallLocation,
    stored_install_path: &str,
) -> Option<PathBuf> {
    let path = PathBuf::from(stored_install_path);
    let containment = pioneer_skills::ensure_install_path_contained(
        location.install_root.as_path(),
        path.as_path(),
    );
    (!containment.has_blocking_findings()).then_some(path)
}

fn installer_policy(context: &SkillsRuntimeContext) -> pioneer_skills::SkillInstallerPolicy {
    pioneer_skills::SkillInstallerPolicy {
        security: context.security_policy.clone(),
        dependency_input: DependencyCheckInput::baseline(),
        block_on_dependency_failures: context.validation_policy.preflight_on_resolve,
        block_on_fingerprint_mismatch: true,
    }
}

struct LifecycleErrorMapping {
    jsonrpc_code: i64,
    code: &'static str,
    message: String,
}

fn map_lifecycle_error(error: &anyhow::Error, method: &str) -> LifecycleErrorMapping {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();

    if lower.contains("dependency failure") {
        return LifecycleErrorMapping {
            jsonrpc_code: INVALID_REQUEST_CODE,
            code: SKILLS_ERROR_INSTALL_BLOCKED_DEPENDENCY,
            message: format!("{method} blocked by dependency policy"),
        };
    }
    if lower.contains("untrusted skill") || lower.contains("trust") {
        return LifecycleErrorMapping {
            jsonrpc_code: INVALID_REQUEST_CODE,
            code: SKILLS_ERROR_INSTALL_BLOCKED_TRUST,
            message: format!("{method} blocked by trust policy"),
        };
    }
    if lower.contains("security scan")
        || lower.contains("containment")
        || lower.contains("symlink")
        || lower.contains("traversal")
    {
        return LifecycleErrorMapping {
            jsonrpc_code: INVALID_REQUEST_CODE,
            code: SKILLS_ERROR_INSTALL_BLOCKED_SECURITY,
            message: format!("{method} blocked by security policy"),
        };
    }
    if lower.contains("fingerprint") && lower.contains("expected") {
        return LifecycleErrorMapping {
            jsonrpc_code: INVALID_REQUEST_CODE,
            code: SKILLS_ERROR_UPDATE_CONFLICT_FINGERPRINT,
            message: format!("{method} fingerprint conflict"),
        };
    }
    if lower.contains("not installed") || lower.contains("was not found") {
        return LifecycleErrorMapping {
            jsonrpc_code: INVALID_REQUEST_CODE,
            code: SKILLS_ERROR_NOT_FOUND,
            message: format!("{method} target was not found"),
        };
    }
    if lower.contains("must be a directory")
        || lower.contains("missing skill.md")
        || lower.contains("canonicalize")
    {
        return LifecycleErrorMapping {
            jsonrpc_code: INVALID_PARAMS_CODE,
            code: SKILLS_ERROR_INVALID_REQUEST,
            message: format!("invalid source for {method}"),
        };
    }
    LifecycleErrorMapping {
        jsonrpc_code: INVALID_REQUEST_CODE,
        code: SKILLS_ERROR_INTERNAL,
        message: format!("internal error in {method}"),
    }
}

fn lifecycle_error_payload(
    error: &anyhow::Error,
    mapped: &LifecycleErrorMapping,
    preview_skill: Option<&pioneer_skills::SkillDefinition>,
    validation_policy: &SkillValidationPolicy,
) -> (String, serde_json::Value) {
    let mut message = mapped.message.clone();
    let mut details = json!({"error": format!("{error:#}")});

    if mapped.code == SKILLS_ERROR_INSTALL_BLOCKED_DEPENDENCY
        && validation_policy.preflight_on_resolve
        && let Some(skill) = preview_skill
    {
        let failures =
            pioneer_skills::evaluate_skill_dependencies(skill, &DependencyCheckInput::baseline())
                .failing_diagnostics();
        if !failures.is_empty() {
            let summary = summarize_dependency_failures(failures.as_slice());
            message = format!("{message}: {summary}");
            details["dependency_failures"] = serde_json::to_value(failures).unwrap_or(json!([]));
        }
    }

    (message, details)
}

fn summarize_dependency_failures(failures: &[pioneer_skills::DependencyDiagnostic]) -> String {
    let mut parts = failures
        .iter()
        .map(|failure| {
            let status = match failure.status {
                pioneer_skills::DependencyStatus::Blocked => "blocked",
                pioneer_skills::DependencyStatus::Missing => "missing",
                pioneer_skills::DependencyStatus::Satisfied => "satisfied",
            };
            let kind = match failure.kind {
                pioneer_skills::DependencyKind::Env => "env",
                pioneer_skills::DependencyKind::Bin => "bin",
                pioneer_skills::DependencyKind::Command => "command",
                pioneer_skills::DependencyKind::Mcp => "mcp",
                pioneer_skills::DependencyKind::ApiKey => "api_key",
            };
            format!("{status} {kind} `{}`", failure.name)
        })
        .collect::<Vec<_>>();
    parts.sort();
    parts.join(", ")
}

fn skill_audit_records(events: &[pioneer_skills::SkillAuditEvent]) -> Vec<SkillAuditEventRecord> {
    events
        .iter()
        .map(|event| SkillAuditEventRecord {
            turn_id: None,
            skill_id: event.skill_id.clone(),
            skill_owner: event.skill_owner.clone(),
            skill_slug: event.skill_slug.clone(),
            source_kind: event.source_kind.clone(),
            action: event.action.as_db_value().to_owned(),
            decision: event.decision.as_db_value().to_owned(),
            reason_code: event.reason_code.clone(),
            details_json: serde_json::to_string(&event.details).unwrap_or_else(|_| "{}".to_owned()),
            created_at_unix: event.created_at_unix,
        })
        .collect()
}

fn skills_error(
    id: Option<RequestId>,
    jsonrpc_code: i64,
    code: &str,
    message: impl Into<String>,
    details: serde_json::Value,
) -> JsonRpcErrorResponse {
    JsonRpcErrorResponse {
        jsonrpc: JSONRPC_VERSION.to_owned(),
        id,
        error: JsonRpcError {
            code: jsonrpc_code,
            message: format!("{code}: {}", message.into()),
            data: Some(json!({
                "code": code,
                "details": details,
            })),
        },
    }
}

fn trust_level_as_str(level: &pioneer_skills::SkillTrustLevel) -> &'static str {
    match level {
        pioneer_skills::SkillTrustLevel::Internal => "internal",
        pioneer_skills::SkillTrustLevel::Verified => "verified",
        pioneer_skills::SkillTrustLevel::Community => "community",
        pioneer_skills::SkillTrustLevel::Untrusted => "untrusted",
    }
}

fn parse_persisted_trust_level(value: &str) -> Result<pioneer_skills::SkillTrustLevel> {
    match value {
        "internal" => Ok(pioneer_skills::SkillTrustLevel::Internal),
        "verified" => Ok(pioneer_skills::SkillTrustLevel::Verified),
        "community" => Ok(pioneer_skills::SkillTrustLevel::Community),
        "untrusted" => Ok(pioneer_skills::SkillTrustLevel::Untrusted),
        other => bail!("unsupported persisted skill trust level `{other}`"),
    }
}

impl MessageProcessor {
    pub(crate) async fn ensure_skills_lock_v2_locked(
        &self,
        lock_path: &Path,
        source_kind: SkillSourceKind,
        scope_key: &str,
    ) -> Result<pioneer_skills::SkillsLock> {
        let candidates = self
            .crud_store
            .list_skill_installations()
            .await?
            .into_iter()
            .filter(|row| {
                row.source_kind == source_kind.as_db_value() && row.scope_key == scope_key
            })
            .map(|row| {
                Ok(pioneer_skills::SkillLockConversionCandidate {
                    skill_id: row.skill_id,
                    owner: row.owner,
                    slug: row.slug,
                    source_kind: row.source_kind,
                    source_ref: row.source_ref,
                    install_path: row.install_path,
                    version: row.version,
                    trust_level: parse_persisted_trust_level(row.trust_level.as_str())?,
                    fingerprint: row.fingerprint,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        pioneer_skills::ensure_skills_lock_v2(lock_path, candidates.as_slice())
    }
}

fn sanitize_workspace_id_component(workspace_id: &str) -> String {
    let mut sanitized = String::with_capacity(workspace_id.len());
    for ch in workspace_id.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        "workspace".to_owned()
    } else {
        sanitized
    }
}

fn expand_workspace_id_token(path: &str, workspace_id: &str) -> String {
    if !path.contains(WORKSPACE_ID_TOKEN) {
        return path.to_owned();
    }
    let sanitized_workspace_id = sanitize_workspace_id_component(workspace_id);
    path.replace(WORKSPACE_ID_TOKEN, sanitized_workspace_id.as_str())
}

fn expand_home(path: &str) -> String {
    if path == "~" {
        return std::env::var("HOME").unwrap_or_else(|_| path.to_owned());
    }
    if let Some(rest) = path.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return PathBuf::from(home).join(rest).display().to_string();
    }
    path.to_owned()
}

pub(crate) fn resolve_root_path(raw: &str, workspace_id: &str) -> PathBuf {
    let expanded_workspace = expand_workspace_id_token(raw, workspace_id);
    let expanded = expand_home(expanded_workspace.as_str());
    let candidate = PathBuf::from(expanded);
    if candidate.is_absolute() {
        candidate
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(candidate)
    }
}

fn hash_skill_roots(roots: &[PathBuf]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for root in roots {
        root.display().to_string().hash(&mut hasher);
        hash_skill_root(root.as_path(), &mut hasher);
    }
    hasher.finish()
}

fn hash_skill_root(root: &Path, hasher: &mut DefaultHasher) {
    let Ok(metadata) = std::fs::symlink_metadata(root) else {
        "missing".hash(hasher);
        return;
    };
    metadata.is_dir().hash(hasher);
    if !metadata.is_dir() {
        return;
    }

    let mut queue = vec![root.to_path_buf()];
    while let Some(current) = queue.pop() {
        let Ok(entries) = std::fs::read_dir(current.as_path()) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(meta) = std::fs::symlink_metadata(path.as_path()) else {
                continue;
            };
            if meta.file_type().is_symlink() {
                continue;
            }
            if meta.is_dir() {
                queue.push(path);
                continue;
            }
            if !meta.is_file() {
                continue;
            }
            path.display().to_string().hash(hasher);
            meta.len().hash(hasher);
            if let Ok(modified) = meta.modified()
                && let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH)
            {
                duration.as_secs().hash(hasher);
                duration.subsec_nanos().hash(hasher);
            }
        }
    }
}
