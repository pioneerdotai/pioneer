use std::collections::{BTreeMap, HashSet};

use crate::memvid::lifecycle::Memvid;
use crate::types::{
    ACL_READ_GROUPS_KEY, ACL_READ_PRINCIPALS_KEY, ACL_READ_ROLES_KEY, ACL_TENANT_ID_KEY,
    ACL_VISIBILITY_KEY, AclContext, AclEnforcementMode, SearchHit,
};
use crate::{MemvidError, Result};

#[derive(Debug, Clone, Default)]
pub(crate) struct AclFilterStats {
    pub allowed: usize,
    pub denied: usize,
    pub cross_tenant_denied: usize,
    pub missing_metadata: usize,
}

impl AclFilterStats {
    fn record(&mut self, decision: AclDecision) {
        if decision.allowed {
            self.allowed += 1;
            return;
        }
        self.denied += 1;
        if decision.cross_tenant_denied {
            self.cross_tenant_denied += 1;
        }
        if decision.missing_metadata_denied {
            self.missing_metadata += 1;
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct NormalizedAclContext {
    tenant_id: String,
    subject_id: Option<String>,
    roles: HashSet<String>,
    group_ids: HashSet<String>,
}

#[derive(Debug, Clone)]
struct ParsedFrameAcl {
    tenant_id: String,
    visibility: FrameVisibility,
    roles: HashSet<String>,
    groups: HashSet<String>,
    principals: HashSet<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum FrameVisibility {
    Public,
    Restricted,
}

#[derive(Debug, Clone, Copy, Default)]
struct AclDecision {
    allowed: bool,
    cross_tenant_denied: bool,
    missing_metadata_denied: bool,
}

impl AclDecision {
    fn allow() -> Self {
        Self {
            allowed: true,
            ..Self::default()
        }
    }

    fn deny_cross_tenant() -> Self {
        Self {
            allowed: false,
            cross_tenant_denied: true,
            missing_metadata_denied: false,
        }
    }

    fn deny_missing_metadata() -> Self {
        Self {
            allowed: false,
            cross_tenant_denied: false,
            missing_metadata_denied: true,
        }
    }

    fn deny_restricted() -> Self {
        Self {
            allowed: false,
            ..Self::default()
        }
    }
}

impl Memvid {
    pub(crate) fn apply_acl_to_search_hits(
        &self,
        hits: &mut Vec<SearchHit>,
        acl_context: Option<&AclContext>,
        acl_enforcement_mode: AclEnforcementMode,
    ) -> Result<AclFilterStats> {
        let normalized_context = match acl_enforcement_mode {
            AclEnforcementMode::Audit => normalize_acl_context(acl_context),
            AclEnforcementMode::Enforce => Some(validate_enforce_acl_context(acl_context)?),
        };

        let mut stats = AclFilterStats::default();
        if normalized_context.is_none() {
            stats.allowed = hits.len();
            return Ok(stats);
        }

        let mut filtered_hits = Vec::with_capacity(hits.len());
        for hit in &*hits {
            let decision = match self.frame_by_id(hit.frame_id) {
                Ok(frame) => {
                    evaluate_acl_metadata(&frame.extra_metadata, normalized_context.as_ref())
                }
                Err(_) => AclDecision::deny_missing_metadata(),
            };
            stats.record(decision);
            if decision.allowed || acl_enforcement_mode == AclEnforcementMode::Audit {
                filtered_hits.push(hit.clone());
            }
        }

        if acl_enforcement_mode == AclEnforcementMode::Enforce {
            for (index, hit) in filtered_hits.iter_mut().enumerate() {
                hit.rank = index + 1;
            }
            *hits = filtered_hits;
        }

        Ok(stats)
    }
}

fn validate_enforce_acl_context(context: Option<&AclContext>) -> Result<NormalizedAclContext> {
    let Some(context) = context else {
        return Err(MemvidError::InvalidQuery {
            reason: "acl_context is required when acl_enforcement_mode is 'enforce'".to_string(),
        });
    };
    let Some(normalized) = normalize_acl_context(Some(context)) else {
        return Err(MemvidError::InvalidQuery {
            reason: "acl_context.tenant_id is required when acl_enforcement_mode is 'enforce'"
                .to_string(),
        });
    };
    Ok(normalized)
}

fn normalize_acl_context(context: Option<&AclContext>) -> Option<NormalizedAclContext> {
    let context = context?;
    let tenant_id = normalize_scalar(context.tenant_id.as_deref())?;
    let subject_id = context
        .subject_id
        .as_deref()
        .and_then(|value| normalize_scalar(Some(value)));
    let roles = context
        .roles
        .iter()
        .filter_map(|role| normalize_scalar(Some(role.as_str())))
        .collect();
    let group_ids = context
        .group_ids
        .iter()
        .filter_map(|group| normalize_scalar(Some(group.as_str())))
        .collect();
    Some(NormalizedAclContext {
        tenant_id,
        subject_id,
        roles,
        group_ids,
    })
}

fn evaluate_acl_metadata(
    metadata: &BTreeMap<String, String>,
    context: Option<&NormalizedAclContext>,
) -> AclDecision {
    let Some(context) = context else {
        return AclDecision::allow();
    };

    let parsed = match parse_acl_metadata(metadata) {
        Ok(parsed) => parsed,
        Err(()) => return AclDecision::deny_missing_metadata(),
    };

    if parsed.tenant_id != context.tenant_id {
        return AclDecision::deny_cross_tenant();
    }

    if parsed.visibility == FrameVisibility::Public {
        return AclDecision::allow();
    }

    let principal_allowed = context
        .subject_id
        .as_ref()
        .is_some_and(|subject| parsed.principals.contains(subject));
    let role_allowed = context.roles.iter().any(|role| parsed.roles.contains(role));
    let group_allowed = context
        .group_ids
        .iter()
        .any(|group| parsed.groups.contains(group));

    if principal_allowed || role_allowed || group_allowed {
        AclDecision::allow()
    } else {
        AclDecision::deny_restricted()
    }
}

fn parse_acl_metadata(
    metadata: &BTreeMap<String, String>,
) -> std::result::Result<ParsedFrameAcl, ()> {
    let tenant_id =
        normalize_scalar(metadata.get(ACL_TENANT_ID_KEY).map(String::as_str)).ok_or(())?;
    let visibility_raw =
        normalize_scalar(metadata.get(ACL_VISIBILITY_KEY).map(String::as_str)).ok_or(())?;
    let visibility = match visibility_raw.as_str() {
        "public" => FrameVisibility::Public,
        "restricted" => FrameVisibility::Restricted,
        _ => return Err(()),
    };
    let roles = parse_acl_list(metadata, ACL_READ_ROLES_KEY)?;
    let groups = parse_acl_list(metadata, ACL_READ_GROUPS_KEY)?;
    let principals = parse_acl_list(metadata, ACL_READ_PRINCIPALS_KEY)?;

    Ok(ParsedFrameAcl {
        tenant_id,
        visibility,
        roles,
        groups,
        principals,
    })
}

fn parse_acl_list(
    metadata: &BTreeMap<String, String>,
    key: &str,
) -> std::result::Result<HashSet<String>, ()> {
    let Some(raw) = metadata.get(key) else {
        return Ok(HashSet::new());
    };
    let values: Vec<String> = serde_json::from_str(raw).map_err(|_| ())?;
    let mut parsed = HashSet::with_capacity(values.len());
    for value in values {
        let normalized = normalize_scalar(Some(value.as_str())).ok_or(())?;
        parsed.insert(normalized);
    }
    Ok(parsed)
}

fn normalize_scalar(value: Option<&str>) -> Option<String> {
    let value = value?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        // Accept legacy/stringified metadata values emitted by some bindings,
        // e.g. acl_visibility stored as "\"restricted\"" instead of "restricted".
        let unwrapped = match serde_json::from_str::<String>(trimmed) {
            Ok(parsed) => parsed.trim().to_string(),
            Err(_) => trimmed.to_string(),
        };
        if unwrapped.is_empty() {
            None
        } else {
            Some(unwrapped.to_ascii_lowercase())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn restricted_metadata() -> BTreeMap<String, String> {
        BTreeMap::from([
            (ACL_TENANT_ID_KEY.to_string(), "tenant-a".to_string()),
            (ACL_VISIBILITY_KEY.to_string(), "restricted".to_string()),
            (
                ACL_READ_ROLES_KEY.to_string(),
                "[\"admin\",\"analyst\"]".to_string(),
            ),
            (ACL_READ_GROUPS_KEY.to_string(), "[\"eng\"]".to_string()),
            (
                ACL_READ_PRINCIPALS_KEY.to_string(),
                "[\"user-123\"]".to_string(),
            ),
        ])
    }

    fn context(tenant: &str) -> NormalizedAclContext {
        NormalizedAclContext {
            tenant_id: tenant.to_string(),
            subject_id: Some("user-123".to_string()),
            roles: HashSet::from(["viewer".to_string()]),
            group_ids: HashSet::from(["eng".to_string()]),
        }
    }

    #[test]
    fn parse_acl_metadata_rejects_invalid_list_encoding() {
        let mut metadata = restricted_metadata();
        metadata.insert(ACL_READ_GROUPS_KEY.to_string(), "eng,ops".to_string());
        assert!(parse_acl_metadata(&metadata).is_err());
    }

    #[test]
    fn evaluate_acl_denies_cross_tenant() {
        let metadata = restricted_metadata();
        let ctx = context("tenant-b");
        let decision = evaluate_acl_metadata(&metadata, Some(&ctx));
        assert!(!decision.allowed);
        assert!(decision.cross_tenant_denied);
    }

    #[test]
    fn evaluate_acl_allows_restricted_group_match() {
        let metadata = restricted_metadata();
        let ctx = context("tenant-a");
        let decision = evaluate_acl_metadata(&metadata, Some(&ctx));
        assert!(decision.allowed);
    }

    #[test]
    fn evaluate_acl_denies_missing_metadata() {
        let metadata = BTreeMap::new();
        let ctx = context("tenant-a");
        let decision = evaluate_acl_metadata(&metadata, Some(&ctx));
        assert!(!decision.allowed);
        assert!(decision.missing_metadata_denied);
    }
}
