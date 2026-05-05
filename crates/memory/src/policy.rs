use crate::{MemoryOperationContext, MemoryReadPolicy, MemoryServiceConfig};
use anyhow::{Result, bail};
use pioneer_protocol::{MemoryRememberParams, MemorySensitivity, MemorySourceKind};

pub const POLICY_ACTION_REMEMBER: &str = "remember";
#[allow(dead_code)]
pub const POLICY_ACTION_GET: &str = "get";
#[allow(dead_code)]
pub const POLICY_ACTION_LIST: &str = "list";
#[allow(dead_code)]
pub const POLICY_ACTION_SEARCH: &str = "search";
pub const POLICY_ACTION_FORGET: &str = "forget";
#[allow(dead_code)]
pub const POLICY_ACTION_REPAIR: &str = "repair";

pub const POLICY_DECISION_ALLOW: &str = "allow";
#[allow(dead_code)]
pub const POLICY_DECISION_DENY: &str = "deny";
#[allow(dead_code)]
pub const POLICY_DECISION_FILTER: &str = "filter";
pub const POLICY_DECISION_ERROR: &str = "error";

#[derive(Debug, Clone)]
pub struct MemoryPolicyEngine {
    config: MemoryServiceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPolicyDecision {
    pub action: &'static str,
    pub decision: &'static str,
    pub reason_code: Option<&'static str>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedRememberPolicy {
    pub content: String,
    pub sensitivity: MemorySensitivity,
    pub confidence: f64,
    pub importance: f64,
}

impl MemoryPolicyEngine {
    pub fn new(config: MemoryServiceConfig) -> Self {
        Self { config }
    }

    pub fn read_policy(&self, context: &MemoryOperationContext) -> MemoryReadPolicy {
        context
            .read_policy
            .clone()
            .unwrap_or_else(|| self.config.default_read_policy.clone())
    }

    pub(crate) fn prepare_remember(
        &self,
        _context: &MemoryOperationContext,
        params: &MemoryRememberParams,
    ) -> Result<PreparedRememberPolicy> {
        let content = params.content.trim();
        if content.is_empty() {
            bail!("memory content cannot be empty");
        }
        if content.len() > self.config.max_content_bytes {
            bail!(
                "memory content is too large: {} bytes exceeds {} bytes",
                content.len(),
                self.config.max_content_bytes
            );
        }

        let source_kind = params
            .provenance
            .as_ref()
            .map(|provenance| provenance.source_kind)
            .unwrap_or(MemorySourceKind::ExplicitUserRequest);
        let confidence = params.confidence.unwrap_or_else(|| {
            if source_kind == MemorySourceKind::ExplicitUserRequest {
                1.0
            } else {
                0.5
            }
        });
        let importance = params.importance.unwrap_or(0.5);
        if !confidence.is_finite() || !(0.0..=1.0).contains(&confidence) {
            bail!("memory confidence must be a finite value between 0.0 and 1.0");
        }
        if !importance.is_finite() || !(0.0..=1.0).contains(&importance) {
            bail!("memory importance must be a finite value between 0.0 and 1.0");
        }

        Ok(PreparedRememberPolicy {
            content: content.to_owned(),
            sensitivity: params.sensitivity.unwrap_or(MemorySensitivity::Normal),
            confidence: f64::from(confidence),
            importance: f64::from(importance),
        })
    }

    pub fn allows_sensitivity(
        &self,
        context: &MemoryOperationContext,
        sensitivity: MemorySensitivity,
    ) -> bool {
        self.read_policy(context).allows(sensitivity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{MemoryCategory, MemoryScope, MemoryScopeKind};
    use std::collections::BTreeMap;

    #[test]
    fn default_read_policy_blocks_secret_like_and_regulated() {
        let policy = MemoryReadPolicy::default();
        assert!(policy.allows(MemorySensitivity::Normal));
        assert!(policy.allows(MemorySensitivity::Personal));
        assert!(!policy.allows(MemorySensitivity::SecretLike));
        assert!(!policy.allows(MemorySensitivity::Regulated));
    }

    #[test]
    fn remember_policy_rejects_empty_content() {
        let engine = MemoryPolicyEngine::new(MemoryServiceConfig::default());
        let params = MemoryRememberParams {
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".to_owned(),
            },
            category: MemoryCategory::Identity,
            namespace: None,
            key: None,
            content: "   ".to_owned(),
            sensitivity: None,
            confidence: None,
            importance: None,
            provenance: None,
            idempotency_key: None,
            supersedes: None,
            metadata: BTreeMap::new(),
        };

        assert!(
            engine
                .prepare_remember(&MemoryOperationContext::default(), &params)
                .is_err()
        );
    }
}
