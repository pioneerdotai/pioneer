use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillAuditAction {
    Install,
    Update,
    Uninstall,
    ResolveAllowed,
    ResolveBlocked,
    RuntimeAllowed,
    RuntimeBlocked,
    SecurityWarn,
}

impl SkillAuditAction {
    pub fn as_db_value(&self) -> &'static str {
        match self {
            Self::Install => "install",
            Self::Update => "update",
            Self::Uninstall => "uninstall",
            Self::ResolveAllowed => "resolve_allowed",
            Self::ResolveBlocked => "resolve_blocked",
            Self::RuntimeAllowed => "runtime_allowed",
            Self::RuntimeBlocked => "runtime_blocked",
            Self::SecurityWarn => "security_warn",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillAuditDecision {
    Allowed,
    Blocked,
    Warning,
}

impl SkillAuditDecision {
    pub fn as_db_value(&self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Blocked => "blocked",
            Self::Warning => "warning",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillAuditEvent {
    pub skill_slug: String,
    pub source_kind: String,
    pub action: SkillAuditAction,
    pub decision: SkillAuditDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    #[serde(default)]
    pub details: JsonValue,
    pub created_at_unix: i64,
}

impl SkillAuditEvent {
    pub fn new(
        skill_slug: impl Into<String>,
        source_kind: impl Into<String>,
        action: SkillAuditAction,
        decision: SkillAuditDecision,
        reason_code: Option<String>,
        details: JsonValue,
        created_at_unix: i64,
    ) -> Self {
        Self {
            skill_slug: skill_slug.into(),
            source_kind: source_kind.into(),
            action,
            decision,
            reason_code,
            details,
            created_at_unix,
        }
    }

    pub fn install(
        skill_slug: impl Into<String>,
        source_kind: impl Into<String>,
        details: JsonValue,
        created_at_unix: i64,
    ) -> Self {
        Self::new(
            skill_slug,
            source_kind,
            SkillAuditAction::Install,
            SkillAuditDecision::Allowed,
            None,
            details,
            created_at_unix,
        )
    }

    pub fn update(
        skill_slug: impl Into<String>,
        source_kind: impl Into<String>,
        details: JsonValue,
        created_at_unix: i64,
    ) -> Self {
        Self::new(
            skill_slug,
            source_kind,
            SkillAuditAction::Update,
            SkillAuditDecision::Allowed,
            None,
            details,
            created_at_unix,
        )
    }

    pub fn uninstall(
        skill_slug: impl Into<String>,
        source_kind: impl Into<String>,
        details: JsonValue,
        created_at_unix: i64,
    ) -> Self {
        Self::new(
            skill_slug,
            source_kind,
            SkillAuditAction::Uninstall,
            SkillAuditDecision::Allowed,
            None,
            details,
            created_at_unix,
        )
    }

    pub fn resolution_allowed(
        skill_slug: impl Into<String>,
        source_kind: impl Into<String>,
        details: JsonValue,
        created_at_unix: i64,
    ) -> Self {
        Self::new(
            skill_slug,
            source_kind,
            SkillAuditAction::ResolveAllowed,
            SkillAuditDecision::Allowed,
            None,
            details,
            created_at_unix,
        )
    }

    pub fn resolution_blocked(
        skill_slug: impl Into<String>,
        source_kind: impl Into<String>,
        reason_code: impl Into<String>,
        details: JsonValue,
        created_at_unix: i64,
    ) -> Self {
        Self::new(
            skill_slug,
            source_kind,
            SkillAuditAction::ResolveBlocked,
            SkillAuditDecision::Blocked,
            Some(reason_code.into()),
            details,
            created_at_unix,
        )
    }

    pub fn runtime_blocked(
        skill_slug: impl Into<String>,
        source_kind: impl Into<String>,
        reason_code: impl Into<String>,
        details: JsonValue,
        created_at_unix: i64,
    ) -> Self {
        Self::new(
            skill_slug,
            source_kind,
            SkillAuditAction::RuntimeBlocked,
            SkillAuditDecision::Blocked,
            Some(reason_code.into()),
            details,
            created_at_unix,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{SkillAuditAction, SkillAuditDecision, SkillAuditEvent};
    use serde_json::json;

    #[test]
    fn audit_event_builders_are_stable() {
        let created = 1_700_000_000;
        let install = SkillAuditEvent::install(
            "agent-browser",
            "registry",
            json!({"fingerprint":"fp1"}),
            created,
        );
        let blocked = SkillAuditEvent::runtime_blocked(
            "agent-browser",
            "registry",
            "runtime.dependency_missing",
            json!({"kind":"bin","name":"node"}),
            created,
        );

        assert_eq!(install.action, SkillAuditAction::Install);
        assert_eq!(install.decision, SkillAuditDecision::Allowed);
        assert!(install.reason_code.is_none());

        assert_eq!(blocked.action, SkillAuditAction::RuntimeBlocked);
        assert_eq!(blocked.decision, SkillAuditDecision::Blocked);
        assert_eq!(
            blocked.reason_code.as_deref(),
            Some("runtime.dependency_missing")
        );
    }
}
