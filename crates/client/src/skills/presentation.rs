//! UI-neutral skill diagnostics and presentation rows.

use pioneer_protocol::{
    SkillAuditTimelineItem, SkillDependencyDiagnostic, SkillHealthItem, SkillListItem,
    SkillSecurityFinding, SkillTrustGateStatus, SkillValidationDiagnostic,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillDiagnosticsTone {
    Default,
    Muted,
    Success,
    Warning,
    Danger,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillDiagnosticsTableCell {
    pub text: String,
    pub tooltip: Option<String>,
    pub tone: SkillDiagnosticsTone,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillDiagnosticsTableRow {
    pub cells: Vec<SkillDiagnosticsTableCell>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillSlugPresentation {
    pub owner: Option<String>,
    pub slug: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillSummaryPresentation {
    pub slug: SkillSlugPresentation,
    pub version: Option<String>,
    pub source: SkillSourceKind,
    pub trust: SkillTrustLevel,
    pub fingerprint_short: String,
    pub status: SkillStatus,
    pub status_tone: SkillDiagnosticsTone,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillDetailDiagnostics {
    pub dependency_diagnostics: Vec<SkillDependencyDiagnostic>,
    pub security_findings: Vec<SkillSecurityFinding>,
    pub validation_issues: Vec<SkillValidationDiagnostic>,
    pub trust_gate: Vec<SkillTrustGateStatus>,
    pub recent_audit: Vec<SkillAuditTimelineItem>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillValidationRow {
    pub code: String,
    pub level: String,
    pub field_path: Option<String>,
    pub message: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillDependencyCard {
    pub kind: SkillDependencyKind,
    pub requirement_name: Option<String>,
    pub status: SkillDependencyStatus,
    pub action_hint: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillSecurityCard {
    pub severity: SkillSecuritySeverity,
    pub severity_tone: SkillDiagnosticsTone,
    pub rule_id: Option<String>,
    pub message: Option<String>,
    pub location: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillTrustGateCard {
    pub tool_kind: SkillTrustGateToolKind,
    pub minimum_trust: SkillTrustLevel,
    pub decision: SkillTrustGateDecision,
    pub decision_tone: SkillDiagnosticsTone,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillAuditRow {
    pub created_at: i64,
    pub action: SkillAuditAction,
    pub decision: SkillAuditDecision,
    pub decision_tone: SkillDiagnosticsTone,
    pub reason_code: Option<String>,
    pub details_summary: SkillAuditDetailsSummary,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillSourceKind {
    System,
    User,
    Registry,
    Other(String),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillTrustLevel {
    Internal,
    Verified,
    Community,
    Untrusted,
    Other(String),
    None,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillStatus {
    Active,
    Blocked,
    Disabled,
    Other(String),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillDependencyKind {
    Bin,
    Env,
    ApiKey,
    Command,
    Mcp,
    Tool,
    Other(String),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillDependencyStatus {
    Ready,
    Missing,
    Blocked,
    Warning,
    Unknown,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillSecuritySeverity {
    Critical,
    High,
    Medium,
    Low,
    Info,
    Other(String),
    None,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillTrustGateToolKind {
    Shell,
    Http,
    FunctionProxy,
    Mcp,
    Other(String),
    None,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillTrustGateDecision {
    Allowed,
    Blocked,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillAuditAction {
    Install,
    Update,
    Uninstall,
    ResolveAllowed,
    ResolveBlocked,
    RuntimeAllowed,
    RuntimeBlocked,
    SecurityWarn,
    None,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillAuditDecision {
    Allowed,
    Blocked,
    Warning,
    None,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillAuditDetailsSummary {
    Empty,
    Text(String),
    ObjectPairs(Vec<(String, SkillJsonValuePreview)>),
    ArrayLen(usize),
    Value(SkillJsonValuePreview),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillJsonValuePreview {
    Text(String),
    Bool(bool),
    Number(String),
    None,
    EmptyArray,
    ArrayLen(usize),
    EmptyObject,
    ObjectKeys(usize),
}

pub fn skill_summary_presentation(skill: &SkillListItem) -> SkillSummaryPresentation {
    SkillSummaryPresentation {
        slug: split_skill_slug_for_view(skill.slug.as_str()),
        version: non_empty_text(skill.version.as_deref().unwrap_or_default()),
        source: skill_source_kind(skill.source_kind.as_str()),
        trust: skill_trust_level(skill.trust_level.as_str()),
        fingerprint_short: short_fingerprint(skill.fingerprint.as_str()),
        status: skill_status(skill.status.as_str()),
        status_tone: skill_status_tone(skill.status.as_str()),
    }
}

pub fn split_skill_slug_for_view(skill_slug: &str) -> SkillSlugPresentation {
    let trimmed = skill_slug.trim();
    if let Some((owner, slug)) = trimmed.split_once('/') {
        let owner = owner.trim();
        let slug = slug.trim();
        if !owner.is_empty() && !slug.is_empty() {
            return SkillSlugPresentation {
                owner: Some(owner.to_owned()),
                slug: slug.to_owned(),
            };
        }
    }

    SkillSlugPresentation {
        owner: None,
        slug: trimmed.to_owned(),
    }
}

pub fn short_fingerprint(fingerprint: &str) -> String {
    let trimmed = fingerprint.trim();
    if trimmed.len() <= 16 {
        return trimmed.to_owned();
    }
    format!("{}...", &trimmed[..16])
}

pub fn skill_detail_diagnostics(
    skill: &SkillListItem,
    health_detail: Option<&SkillHealthItem>,
) -> SkillDetailDiagnostics {
    SkillDetailDiagnostics {
        dependency_diagnostics: health_detail
            .map(|health| health.dependency_diagnostics.clone())
            .unwrap_or_else(|| skill.health.dependency_failures.clone()),
        security_findings: health_detail
            .map(|health| health.security_findings.clone())
            .unwrap_or_else(|| skill.health.security_blocks.clone()),
        validation_issues: health_detail
            .map(|health| health.validation_issues.clone())
            .unwrap_or_else(|| skill.health.validation_issues.clone()),
        trust_gate: health_detail
            .map(|health| health.trust_gate.clone())
            .unwrap_or_default(),
        recent_audit: health_detail
            .map(|health| health.recent_audit.clone())
            .unwrap_or_default(),
    }
}

pub fn skill_validation_rows(issues: &[SkillValidationDiagnostic]) -> Vec<SkillValidationRow> {
    issues
        .iter()
        .map(|issue| SkillValidationRow {
            code: issue.code.clone(),
            level: issue.level.clone(),
            field_path: optional_non_empty_text(issue.field_path.as_deref()),
            message: issue.message.clone(),
        })
        .collect()
}

pub fn skill_dependency_cards(
    diagnostics: &[SkillDependencyDiagnostic],
) -> Vec<SkillDependencyCard> {
    diagnostics
        .iter()
        .map(|item| SkillDependencyCard {
            kind: skill_dependency_kind(item.kind.as_str()),
            requirement_name: non_empty_text(item.name.as_str()),
            status: skill_dependency_status(item.status.as_str()),
            action_hint: non_empty_text(item.hint.as_str()),
        })
        .collect()
}

pub fn skill_security_cards(findings: &[SkillSecurityFinding]) -> Vec<SkillSecurityCard> {
    findings
        .iter()
        .map(|item| {
            let severity = skill_security_severity(item.severity.as_str());
            SkillSecurityCard {
                severity_tone: skill_security_severity_tone(&severity),
                severity,
                rule_id: non_empty_text(item.rule_id.as_str()),
                message: non_empty_text(item.message.as_str()),
                location: optional_non_empty_text(item.path.as_deref()),
            }
        })
        .collect()
}

pub fn skill_trust_gate_cards(trust_gate: &[SkillTrustGateStatus]) -> Vec<SkillTrustGateCard> {
    trust_gate
        .iter()
        .map(|item| {
            let decision = skill_trust_gate_decision(item.allowed);
            SkillTrustGateCard {
                tool_kind: skill_trust_gate_tool_kind(item.tool_kind.as_str()),
                minimum_trust: skill_trust_level(item.minimum_trust.as_str()),
                decision,
                decision_tone: skill_trust_gate_decision_tone(decision),
            }
        })
        .collect()
}

pub fn skill_audit_rows(audit: &[SkillAuditTimelineItem], limit: usize) -> Vec<SkillAuditRow> {
    audit
        .iter()
        .take(limit)
        .map(|item| {
            let decision = skill_audit_decision(item.decision.as_str());
            SkillAuditRow {
                created_at: item.created_at,
                action: skill_audit_action(item.action.as_str()),
                decision,
                decision_tone: skill_audit_decision_tone(decision),
                reason_code: optional_non_empty_text(item.reason_code.as_deref()),
                details_summary: summarize_audit_details(item.details_json.as_str()),
            }
        })
        .collect()
}

pub fn skill_source_kind(source_kind: &str) -> SkillSourceKind {
    match source_kind.trim() {
        "system" => SkillSourceKind::System,
        "user" => SkillSourceKind::User,
        "registry" => SkillSourceKind::Registry,
        other => SkillSourceKind::Other(other.to_owned()),
    }
}

pub fn skill_trust_level(trust_level: &str) -> SkillTrustLevel {
    match trust_level.trim() {
        "internal" => SkillTrustLevel::Internal,
        "verified" => SkillTrustLevel::Verified,
        "community" => SkillTrustLevel::Community,
        "untrusted" => SkillTrustLevel::Untrusted,
        "" => SkillTrustLevel::None,
        other => SkillTrustLevel::Other(other.to_owned()),
    }
}

pub fn skill_status(status: &str) -> SkillStatus {
    match status.trim() {
        "active" => SkillStatus::Active,
        "blocked" => SkillStatus::Blocked,
        "disabled" => SkillStatus::Disabled,
        other => SkillStatus::Other(other.to_owned()),
    }
}

pub fn skill_status_tone(status: &str) -> SkillDiagnosticsTone {
    match skill_status(status) {
        SkillStatus::Active => SkillDiagnosticsTone::Success,
        SkillStatus::Blocked => SkillDiagnosticsTone::Warning,
        SkillStatus::Disabled | SkillStatus::Other(_) => SkillDiagnosticsTone::Default,
    }
}

pub fn skill_dependency_kind(kind: &str) -> SkillDependencyKind {
    match kind.trim() {
        "bin" => SkillDependencyKind::Bin,
        "env" => SkillDependencyKind::Env,
        "api_key" => SkillDependencyKind::ApiKey,
        "command" => SkillDependencyKind::Command,
        "mcp" => SkillDependencyKind::Mcp,
        "tool" | "" => SkillDependencyKind::Tool,
        other => SkillDependencyKind::Other(other.to_owned()),
    }
}

pub fn skill_dependency_status(status: &str) -> SkillDependencyStatus {
    match status.trim() {
        "satisfied" | "ok" | "available" => SkillDependencyStatus::Ready,
        "missing" => SkillDependencyStatus::Missing,
        "blocked" => SkillDependencyStatus::Blocked,
        "warning" => SkillDependencyStatus::Warning,
        _ => SkillDependencyStatus::Unknown,
    }
}

pub fn skill_dependency_status_tone(status: SkillDependencyStatus) -> SkillDiagnosticsTone {
    match status {
        SkillDependencyStatus::Ready => SkillDiagnosticsTone::Success,
        SkillDependencyStatus::Missing | SkillDependencyStatus::Blocked => {
            SkillDiagnosticsTone::Danger
        }
        SkillDependencyStatus::Warning => SkillDiagnosticsTone::Warning,
        SkillDependencyStatus::Unknown => SkillDiagnosticsTone::Muted,
    }
}

pub fn skill_security_severity(severity: &str) -> SkillSecuritySeverity {
    match severity.trim().to_lowercase().as_str() {
        "critical" => SkillSecuritySeverity::Critical,
        "high" => SkillSecuritySeverity::High,
        "medium" => SkillSecuritySeverity::Medium,
        "low" => SkillSecuritySeverity::Low,
        "info" | "informational" => SkillSecuritySeverity::Info,
        "" => SkillSecuritySeverity::None,
        other => SkillSecuritySeverity::Other(other.to_owned()),
    }
}

pub fn skill_security_severity_tone(severity: &SkillSecuritySeverity) -> SkillDiagnosticsTone {
    match severity {
        SkillSecuritySeverity::Critical | SkillSecuritySeverity::High => {
            SkillDiagnosticsTone::Danger
        }
        SkillSecuritySeverity::Medium => SkillDiagnosticsTone::Warning,
        SkillSecuritySeverity::Low
        | SkillSecuritySeverity::Info
        | SkillSecuritySeverity::Other(_)
        | SkillSecuritySeverity::None => SkillDiagnosticsTone::Muted,
    }
}

pub fn skill_trust_gate_tool_kind(tool_kind: &str) -> SkillTrustGateToolKind {
    match tool_kind.trim() {
        "shell" => SkillTrustGateToolKind::Shell,
        "http" => SkillTrustGateToolKind::Http,
        "function_proxy" => SkillTrustGateToolKind::FunctionProxy,
        "mcp" => SkillTrustGateToolKind::Mcp,
        "" => SkillTrustGateToolKind::None,
        other => SkillTrustGateToolKind::Other(other.to_owned()),
    }
}

pub fn skill_trust_gate_decision(allowed: bool) -> SkillTrustGateDecision {
    if allowed {
        SkillTrustGateDecision::Allowed
    } else {
        SkillTrustGateDecision::Blocked
    }
}

pub fn skill_trust_gate_decision_tone(decision: SkillTrustGateDecision) -> SkillDiagnosticsTone {
    match decision {
        SkillTrustGateDecision::Allowed => SkillDiagnosticsTone::Success,
        SkillTrustGateDecision::Blocked => SkillDiagnosticsTone::Danger,
    }
}

pub fn skill_audit_action(action: &str) -> SkillAuditAction {
    match action.trim() {
        "install" => SkillAuditAction::Install,
        "update" => SkillAuditAction::Update,
        "uninstall" => SkillAuditAction::Uninstall,
        "resolve_allowed" => SkillAuditAction::ResolveAllowed,
        "resolve_blocked" => SkillAuditAction::ResolveBlocked,
        "runtime_allowed" => SkillAuditAction::RuntimeAllowed,
        "runtime_blocked" => SkillAuditAction::RuntimeBlocked,
        "security_warn" => SkillAuditAction::SecurityWarn,
        _ => SkillAuditAction::None,
    }
}

pub fn skill_audit_decision(decision: &str) -> SkillAuditDecision {
    match decision.trim() {
        "allowed" => SkillAuditDecision::Allowed,
        "blocked" => SkillAuditDecision::Blocked,
        "warning" => SkillAuditDecision::Warning,
        _ => SkillAuditDecision::None,
    }
}

pub fn skill_audit_decision_tone(decision: SkillAuditDecision) -> SkillDiagnosticsTone {
    match decision {
        SkillAuditDecision::Allowed => SkillDiagnosticsTone::Success,
        SkillAuditDecision::Blocked => SkillDiagnosticsTone::Danger,
        SkillAuditDecision::Warning => SkillDiagnosticsTone::Warning,
        SkillAuditDecision::None => SkillDiagnosticsTone::Muted,
    }
}

pub fn summarize_audit_details(details_json: &str) -> SkillAuditDetailsSummary {
    let raw = details_json.trim();
    if raw.is_empty() || raw == "{}" || raw == "null" {
        return SkillAuditDetailsSummary::Empty;
    }

    let value: serde_json::Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => return SkillAuditDetailsSummary::Text(truncate_for_table(raw, 96)),
    };

    match value {
        serde_json::Value::Object(map) => {
            if map.is_empty() {
                return SkillAuditDetailsSummary::Empty;
            }

            SkillAuditDetailsSummary::ObjectPairs(
                map.iter()
                    .take(2)
                    .map(|(key, value)| (key.clone(), json_value_preview(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => SkillAuditDetailsSummary::ArrayLen(values.len()),
        other => SkillAuditDetailsSummary::Value(json_value_preview(&other)),
    }
}

pub fn json_value_preview(value: &serde_json::Value) -> SkillJsonValuePreview {
    match value {
        serde_json::Value::String(value) => {
            SkillJsonValuePreview::Text(truncate_for_table(value, 48))
        }
        serde_json::Value::Bool(value) => SkillJsonValuePreview::Bool(*value),
        serde_json::Value::Number(value) => SkillJsonValuePreview::Number(value.to_string()),
        serde_json::Value::Null => SkillJsonValuePreview::None,
        serde_json::Value::Array(values) => {
            if values.is_empty() {
                SkillJsonValuePreview::EmptyArray
            } else {
                SkillJsonValuePreview::ArrayLen(values.len())
            }
        }
        serde_json::Value::Object(map) => {
            if map.is_empty() {
                SkillJsonValuePreview::EmptyObject
            } else {
                SkillJsonValuePreview::ObjectKeys(map.len())
            }
        }
    }
}

pub fn truncate_for_table(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }

    let shortened = value.chars().take(max_chars).collect::<String>();
    format!("{shortened}...")
}

pub fn non_empty_text(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

pub fn optional_non_empty_text(value: Option<&str>) -> Option<String> {
    value.and_then(non_empty_text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        SkillHealthSummary, SkillInstallState, SkillPolicyState, SkillTrustGateStatus,
    };

    fn skill(slug: &str) -> SkillListItem {
        SkillListItem {
            slug: slug.to_owned(),
            source_kind: "user".to_owned(),
            display_name: slug.to_owned(),
            description: String::new(),
            version: Some(" 1.2.3 ".to_owned()),
            fingerprint: "1234567890abcdef9999".to_owned(),
            trust_level: "community".to_owned(),
            install: SkillInstallState {
                managed: true,
                installed: true,
                lifecycle_editable: true,
                install_path: None,
                updated_at: None,
            },
            policy: SkillPolicyState {
                enabled: true,
                allow_implicit_invocation: true,
                allow_implicit_invocation_editable: true,
            },
            health: SkillHealthSummary {
                status: "ok".to_owned(),
                dependency_failures: vec![SkillDependencyDiagnostic {
                    kind: "bin".to_owned(),
                    name: "node".to_owned(),
                    status: "missing".to_owned(),
                    hint: "install node".to_owned(),
                }],
                security_blocks: Vec::new(),
                validation_issues: Vec::new(),
            },
            status: "active".to_owned(),
            status_reason: None,
        }
    }

    #[test]
    fn skill_summary_normalizes_slug_version_fingerprint_and_status() {
        let summary = skill_summary_presentation(&skill("@owner/example"));

        assert_eq!(summary.slug.owner.as_deref(), Some("@owner"));
        assert_eq!(summary.slug.slug, "example");
        assert_eq!(summary.version.as_deref(), Some("1.2.3"));
        assert_eq!(summary.fingerprint_short, "1234567890abcdef...");
        assert_eq!(summary.source, SkillSourceKind::User);
        assert_eq!(summary.trust, SkillTrustLevel::Community);
        assert_eq!(summary.status, SkillStatus::Active);
        assert_eq!(summary.status_tone, SkillDiagnosticsTone::Success);
    }

    #[test]
    fn detail_diagnostics_prefers_health_detail_with_skill_fallback() {
        let skill = skill("alpha");
        let fallback = skill_detail_diagnostics(&skill, None);
        assert_eq!(fallback.dependency_diagnostics.len(), 1);

        let health = SkillHealthItem {
            slug: "alpha".to_owned(),
            source_kind: "user".to_owned(),
            trust_level: "community".to_owned(),
            dependency_diagnostics: Vec::new(),
            security_findings: vec![SkillSecurityFinding {
                rule_id: "rule".to_owned(),
                severity: "high".to_owned(),
                message: "blocked".to_owned(),
                path: None,
            }],
            validation_issues: Vec::new(),
            trust_gate: vec![SkillTrustGateStatus {
                tool_kind: "shell".to_owned(),
                minimum_trust: "verified".to_owned(),
                allowed: false,
            }],
            recent_audit: Vec::new(),
        };
        let detailed = skill_detail_diagnostics(&skill, Some(&health));

        assert!(detailed.dependency_diagnostics.is_empty());
        assert_eq!(detailed.security_findings.len(), 1);
        assert_eq!(detailed.trust_gate.len(), 1);
    }

    #[test]
    fn diagnostics_cards_preserve_values_and_tones_without_ui_copy() {
        let dependencies = skill_dependency_cards(&[SkillDependencyDiagnostic {
            kind: "env".to_owned(),
            name: "API_KEY".to_owned(),
            status: "warning".to_owned(),
            hint: "set env".to_owned(),
        }]);
        assert_eq!(dependencies[0].kind, SkillDependencyKind::Env);
        assert_eq!(dependencies[0].status, SkillDependencyStatus::Warning);
        assert_eq!(
            skill_dependency_status_tone(dependencies[0].status),
            SkillDiagnosticsTone::Warning
        );

        let security = skill_security_cards(&[SkillSecurityFinding {
            rule_id: "exec".to_owned(),
            severity: "critical".to_owned(),
            message: "unsafe".to_owned(),
            path: Some("SKILL.md".to_owned()),
        }]);
        assert_eq!(security[0].severity, SkillSecuritySeverity::Critical);
        assert_eq!(security[0].severity_tone, SkillDiagnosticsTone::Danger);

        let trust = skill_trust_gate_cards(&[SkillTrustGateStatus {
            tool_kind: "mcp".to_owned(),
            minimum_trust: "verified".to_owned(),
            allowed: true,
        }]);
        assert_eq!(trust[0].tool_kind, SkillTrustGateToolKind::Mcp);
        assert_eq!(trust[0].decision, SkillTrustGateDecision::Allowed);
        assert_eq!(trust[0].decision_tone, SkillDiagnosticsTone::Success);
    }

    #[test]
    fn audit_rows_summarize_json_and_classify_decisions() {
        let rows = skill_audit_rows(
            &[
                SkillAuditTimelineItem {
                    action: "runtime_blocked".to_owned(),
                    decision: "blocked".to_owned(),
                    reason_code: Some(" policy ".to_owned()),
                    created_at: 1_700_000_000,
                    details_json: r#"{"tool":"shell","args":["echo"]}"#.to_owned(),
                },
                SkillAuditTimelineItem {
                    action: "install".to_owned(),
                    decision: "allowed".to_owned(),
                    reason_code: None,
                    created_at: 1_700_000_001,
                    details_json: "{}".to_owned(),
                },
            ],
            1,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].action, SkillAuditAction::RuntimeBlocked);
        assert_eq!(rows[0].decision, SkillAuditDecision::Blocked);
        assert_eq!(rows[0].decision_tone, SkillDiagnosticsTone::Danger);
        assert_eq!(rows[0].reason_code.as_deref(), Some("policy"));
        assert!(matches!(
            rows[0].details_summary,
            SkillAuditDetailsSummary::ObjectPairs(_)
        ));
    }
}
