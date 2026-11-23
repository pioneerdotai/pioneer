use pioneer_protocol::{SkillId, SkillPackId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSkillPolicyRecord {
    pub id: String,
    pub workspace_id: String,
    pub skill_id: SkillId,
    pub enabled: Option<bool>,
    pub allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillInstallationRecord {
    pub skill_id: SkillId,
    pub owner: Option<String>,
    pub slug: String,
    pub version: Option<String>,
    pub source_kind: String,
    pub scope_key: String,
    pub source_ref: String,
    pub install_path: String,
    pub trust_level: String,
    pub fingerprint: String,
    pub updated_at_unix: i64,
    pub pack_id: Option<SkillPackId>,
    pub pack_member_key: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkillInstallationPatch {
    pub owner: Option<Option<String>>,
    pub slug: Option<String>,
    pub version: Option<Option<String>>,
    pub source_kind: Option<String>,
    pub scope_key: Option<String>,
    pub source_ref: Option<String>,
    pub install_path: Option<String>,
    pub trust_level: Option<String>,
    pub fingerprint: Option<String>,
    pub pack_id: Option<Option<SkillPackId>>,
    pub pack_member_key: Option<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillUploadSessionRecord {
    pub purpose: String,
    pub upload_id: String,
    pub workspace_id: String,
    pub connection_id: u64,
    pub status: String,
    pub file_name: String,
    pub archive_format: String,
    pub compressed_size_bytes: u64,
    pub received_bytes: u64,
    pub sha256: String,
    pub payload_path: String,
    pub created_at_unix: i64,
    pub expires_at_unix: i64,
    pub finalized_at_unix: Option<i64>,
    pub consumed_at_unix: Option<i64>,
    pub aborted_at_unix: Option<i64>,
}
