//! ACL request context and metadata contract types.

use serde::{Deserialize, Serialize};

/// Required frame metadata key for tenant isolation.
pub const ACL_TENANT_ID_KEY: &str = "acl_tenant_id";
/// Optional resource lineage identifier.
pub const ACL_RESOURCE_ID_KEY: &str = "acl_resource_id";
/// Visibility policy (`public` or `restricted`).
pub const ACL_VISIBILITY_KEY: &str = "acl_visibility";
/// Read allow-list by role (canonical JSON string array).
pub const ACL_READ_ROLES_KEY: &str = "acl_read_roles";
/// Read allow-list by group ID (canonical JSON string array).
pub const ACL_READ_GROUPS_KEY: &str = "acl_read_groups";
/// Read allow-list by subject/principal ID (canonical JSON string array).
pub const ACL_READ_PRINCIPALS_KEY: &str = "acl_read_principals";
/// ACL policy schema version marker.
pub const ACL_POLICY_VERSION_KEY: &str = "acl_policy_version";

/// Enforcement mode for ACL checks.
///
/// - `audit`: evaluate ACL and collect deny signals, but do not block hits.
/// - `enforce`: deny-by-default when ACL metadata is missing/invalid or not allowed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AclEnforcementMode {
    #[default]
    Audit,
    Enforce,
}

/// Caller identity context used to evaluate ACL policies at retrieval time.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct AclContext {
    /// Tenant ID for strict cross-tenant isolation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// Subject/principal ID of the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_id: Option<String>,
    /// Caller roles used for RBAC checks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<String>,
    /// Caller group IDs used for group-based ACL checks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_ids: Vec<String>,
}
