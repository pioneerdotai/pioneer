use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::SkillId;

fn default_true() -> bool {
    true
}

fn default_health_audit_limit() -> u64 {
    16
}

fn default_user_skill_source_kind() -> String {
    "user".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SkillLifecycleSource {
    UploadedArchive { upload_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillArchiveFormat {
    TarGz,
}

impl SkillArchiveFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TarGz => "tar_gz",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUploadStartParams {
    pub workspace_id: String,
    pub file_name: String,
    pub archive_format: SkillArchiveFormat,
    pub compressed_size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncompressed_size_hint_bytes: Option<u64>,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUploadStartResponse {
    pub upload_id: String,
    pub recommended_chunk_size_bytes: u64,
    pub max_chunk_size_bytes: u64,
    pub max_compressed_size_bytes: u64,
    pub max_uncompressed_size_bytes: u64,
    pub expires_at_unix: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUploadChunkHeader {
    pub workspace_id: String,
    pub upload_id: String,
    pub offset: u64,
    pub len: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUploadChunkAckNotification {
    pub upload_id: String,
    pub offset: u64,
    pub len: u64,
    pub received_bytes: u64,
    pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUploadFinishParams {
    pub workspace_id: String,
    pub upload_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUploadFinishResponse {
    pub upload_id: String,
    pub status: String,
    pub sha256: String,
    pub compressed_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUploadAbortParams {
    pub workspace_id: String,
    pub upload_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUploadAbortResponse {
    pub upload_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillListParams {
    pub workspace_id: String,
    #[serde(default = "default_true")]
    pub include_health: bool,
    #[serde(default = "default_true")]
    pub include_policy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillListResponse {
    pub snapshot_version: u64,
    pub generated_at: i64,
    #[serde(default)]
    pub skills: Vec<SkillListItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillListItem {
    pub skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
    pub display_name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub fingerprint: String,
    pub trust_level: String,
    pub install: SkillInstallState,
    pub policy: SkillPolicyState,
    pub health: SkillHealthSummary,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillInstallState {
    pub managed: bool,
    pub installed: bool,
    #[serde(default = "default_skill_install_lifecycle_editable")]
    pub lifecycle_editable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub install_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
}

const fn default_skill_install_lifecycle_editable() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillPolicyState {
    pub enabled: bool,
    pub allow_implicit_invocation: bool,
    #[serde(default = "default_true")]
    pub allow_implicit_invocation_editable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillHealthSummary {
    pub status: String,
    #[serde(default)]
    pub dependency_failures: Vec<SkillDependencyDiagnostic>,
    #[serde(default)]
    pub security_blocks: Vec<SkillSecurityFinding>,
    #[serde(default)]
    pub validation_issues: Vec<SkillValidationDiagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillDependencyDiagnostic {
    pub kind: String,
    pub name: String,
    pub status: String,
    pub hint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillSecurityFinding {
    pub rule_id: String,
    pub severity: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillValidationDiagnostic {
    pub code: String,
    pub level: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsInstallParams {
    pub workspace_id: String,
    pub source: SkillLifecycleSource,
    #[serde(default = "default_user_skill_source_kind")]
    pub target_source_kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsInstallResponse {
    pub status: String,
    pub skill: SkillLifecycleResultSkill,
    pub audit: SkillLifecycleAuditSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUpdateParams {
    pub workspace_id: String,
    pub skill_id: SkillId,
    pub source: SkillLifecycleSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_previous_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUpdateResponse {
    pub status: String,
    pub skill: SkillLifecycleResultSkill,
    pub audit: SkillLifecycleAuditSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUninstallParams {
    pub workspace_id: String,
    pub skill_id: SkillId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsUninstallResponse {
    pub status: String,
    pub skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_install_path: Option<String>,
    pub audit: SkillLifecycleAuditSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillLifecycleResultSkill {
    pub skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub fingerprint: String,
    pub trust_level: String,
    pub install_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillLifecycleAuditSummary {
    pub events_written: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsPolicyListParams {
    pub workspace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsPolicyListResponse {
    #[serde(default)]
    pub policies: Vec<SkillWorkspacePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillWorkspacePolicy {
    pub workspace_id: String,
    pub skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsPolicySetParams {
    pub workspace_id: String,
    pub skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsPolicySetResponse {
    pub policy: SkillWorkspacePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsHealthParams {
    pub workspace_id: String,
    #[serde(default)]
    pub skills: Vec<SkillHealthTarget>,
    #[serde(default = "default_health_audit_limit")]
    pub audit_limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillHealthTarget {
    pub skill_id: SkillId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsHealthResponse {
    pub snapshot_version: u64,
    pub generated_at: i64,
    #[serde(default)]
    pub skills: Vec<SkillHealthItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillHealthItem {
    pub skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
    pub trust_level: String,
    #[serde(default)]
    pub dependency_diagnostics: Vec<SkillDependencyDiagnostic>,
    #[serde(default)]
    pub security_findings: Vec<SkillSecurityFinding>,
    #[serde(default)]
    pub validation_issues: Vec<SkillValidationDiagnostic>,
    #[serde(default)]
    pub trust_gate: Vec<SkillTrustGateStatus>,
    #[serde(default)]
    pub recent_audit: Vec<SkillAuditTimelineItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillTrustGateStatus {
    pub tool_kind: String,
    pub minimum_trust: String,
    pub allowed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillAuditTimelineItem {
    pub action: String,
    pub decision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    pub created_at: i64,
    pub details_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillsChangedNotification {
    pub workspace_id: String,
    pub snapshot_version: u64,
    pub reason: String,
    #[serde(default)]
    pub changes: Vec<SkillChangedItem>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SkillChangedItem {
    pub skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
    pub change_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_before: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_after: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn skill_id(character: char) -> SkillId {
        SkillId::new(character.to_string().repeat(21)).expect("valid test skill id")
    }

    #[test]
    fn uploaded_archive_lifecycle_source_round_trips_and_rejects_path() {
        let source = SkillLifecycleSource::UploadedArchive {
            upload_id: "upload_00000000000001".to_owned(),
        };
        let value = serde_json::to_value(&source).expect("source should encode");
        assert_eq!(
            value,
            json!({
                "type": "uploaded_archive",
                "upload_id": "upload_00000000000001"
            })
        );
        let decoded: SkillLifecycleSource =
            serde_json::from_value(value).expect("source should decode");
        assert_eq!(decoded, source);

        let legacy = json!({"type": "path", "path": "/tmp/skill"});
        assert!(
            serde_json::from_value::<SkillLifecycleSource>(legacy).is_err(),
            "path lifecycle source must not be accepted"
        );
    }

    #[test]
    fn install_params_default_to_user_target_source_kind() {
        let params: SkillsInstallParams = serde_json::from_value(json!({
            "workspace_id": "ws_000000000000000001",
            "source": {
                "type": "uploaded_archive",
                "upload_id": "upload_00000000000001"
            }
        }))
        .expect("install params should decode without target_source_kind");

        assert_eq!(params.target_source_kind, "user");
    }

    #[test]
    fn management_mutations_require_exact_skill_id() {
        let update: SkillsUpdateParams = serde_json::from_value(json!({
            "workspace_id": "ws_000000000000000001",
            "skill_id": "AAAAAAAAAAAAAAAAAAAAA",
            "source": {
                "type": "uploaded_archive",
                "upload_id": "upload_00000000000001"
            },
            "expected_previous_fingerprint": "previous"
        }))
        .expect("ID-based update should decode");
        assert_eq!(update.skill_id, skill_id('A'));

        let uninstall: SkillsUninstallParams = serde_json::from_value(json!({
            "workspace_id": "ws_000000000000000001",
            "skill_id": "BBBBBBBBBBBBBBBBBBBBB"
        }))
        .expect("ID-based uninstall should decode");
        assert_eq!(uninstall.skill_id, skill_id('B'));

        let policy: SkillsPolicySetParams = serde_json::from_value(json!({
            "workspace_id": "ws_000000000000000001",
            "skill_id": "CCCCCCCCCCCCCCCCCCCCC",
            "enabled": true
        }))
        .expect("ID-based policy should decode");
        assert_eq!(policy.skill_id, skill_id('C'));

        let health: SkillsHealthParams = serde_json::from_value(json!({
            "workspace_id": "ws_000000000000000001",
            "skills": [{"skill_id": "DDDDDDDDDDDDDDDDDDDDD"}]
        }))
        .expect("ID-based health request should decode");
        assert_eq!(health.skills[0].skill_id, skill_id('D'));

        let mut update_without_id = serde_json::to_value(&update).expect("update should encode");
        update_without_id
            .as_object_mut()
            .expect("update must encode as object")
            .remove("skill_id");
        assert!(serde_json::from_value::<SkillsUpdateParams>(update_without_id).is_err());

        let mut uninstall_without_id =
            serde_json::to_value(&uninstall).expect("uninstall should encode");
        uninstall_without_id
            .as_object_mut()
            .expect("uninstall must encode as object")
            .remove("skill_id");
        assert!(serde_json::from_value::<SkillsUninstallParams>(uninstall_without_id).is_err());

        let mut policy_without_id = serde_json::to_value(&policy).expect("policy should encode");
        policy_without_id
            .as_object_mut()
            .expect("policy must encode as object")
            .remove("skill_id");
        assert!(serde_json::from_value::<SkillsPolicySetParams>(policy_without_id).is_err());

        let mut health_without_id = serde_json::to_value(&health).expect("health should encode");
        health_without_id["skills"][0]
            .as_object_mut()
            .expect("health target must encode as object")
            .remove("skill_id");
        assert!(serde_json::from_value::<SkillsHealthParams>(health_without_id).is_err());
    }

    #[test]
    fn management_results_and_notifications_carry_id_and_presentation() {
        let list_item = SkillListItem {
            skill_id: skill_id('E'),
            owner: Some("owner".to_owned()),
            slug: "skill".to_owned(),
            source_kind: "user".to_owned(),
            display_name: "Skill".to_owned(),
            description: "Description".to_owned(),
            version: Some("1.0.0".to_owned()),
            fingerprint: "fingerprint".to_owned(),
            trust_level: "community".to_owned(),
            install: SkillInstallState {
                managed: true,
                installed: true,
                lifecycle_editable: true,
                install_path: Some("/managed/E/skill".to_owned()),
                updated_at: Some(1),
            },
            policy: SkillPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
                allow_implicit_invocation_editable: true,
            },
            health: SkillHealthSummary {
                status: "ready".to_owned(),
                dependency_failures: Vec::new(),
                security_blocks: Vec::new(),
                validation_issues: Vec::new(),
            },
            status: "available".to_owned(),
            status_reason: None,
        };
        let list_json = serde_json::to_value(&list_item).expect("list item encodes");
        assert_eq!(list_json["skill_id"], "EEEEEEEEEEEEEEEEEEEEE");
        assert_eq!(list_json["owner"], "owner");
        assert_eq!(list_json["slug"], "skill");

        let lifecycle = SkillLifecycleResultSkill {
            skill_id: skill_id('F'),
            owner: None,
            slug: "authorless".to_owned(),
            source_kind: "registry".to_owned(),
            version: None,
            fingerprint: "fingerprint".to_owned(),
            trust_level: "community".to_owned(),
            install_path: "/managed/F/authorless".to_owned(),
        };
        let install = SkillsInstallResponse {
            status: "installed".to_owned(),
            skill: lifecycle,
            audit: SkillLifecycleAuditSummary { events_written: 1 },
        };
        let install_json = serde_json::to_value(&install).expect("install response encodes");
        assert_eq!(install_json["skill"]["skill_id"], "FFFFFFFFFFFFFFFFFFFFF");
        assert!(install_json["skill"].get("owner").is_none());

        let uninstall = SkillsUninstallResponse {
            status: "uninstalled".to_owned(),
            skill_id: skill_id('J'),
            owner: Some("owner".to_owned()),
            slug: "skill".to_owned(),
            source_kind: "user".to_owned(),
            removed_install_path: Some("/managed/J/skill".to_owned()),
            audit: SkillLifecycleAuditSummary { events_written: 1 },
        };
        let uninstall_json = serde_json::to_value(&uninstall).expect("uninstall response encodes");
        assert_eq!(uninstall_json["skill_id"], "JJJJJJJJJJJJJJJJJJJJJ");
        assert_eq!(uninstall_json["owner"], "owner");
        assert_eq!(uninstall_json["slug"], "skill");

        let policy = SkillWorkspacePolicy {
            workspace_id: "ws_000000000000000001".to_owned(),
            skill_id: skill_id('G'),
            owner: Some("owner".to_owned()),
            slug: "skill".to_owned(),
            source_kind: "user".to_owned(),
            enabled: Some(true),
            allow_implicit_invocation: Some(false),
        };
        assert_eq!(
            serde_json::to_value(&policy).unwrap()["skill_id"],
            "GGGGGGGGGGGGGGGGGGGGG"
        );

        let health = SkillHealthItem {
            skill_id: skill_id('H'),
            owner: None,
            slug: "health".to_owned(),
            source_kind: "system".to_owned(),
            trust_level: "internal".to_owned(),
            dependency_diagnostics: Vec::new(),
            security_findings: Vec::new(),
            validation_issues: Vec::new(),
            trust_gate: Vec::new(),
            recent_audit: Vec::new(),
        };
        assert_eq!(
            serde_json::to_value(&health).unwrap()["skill_id"],
            "HHHHHHHHHHHHHHHHHHHHH"
        );

        let changed = SkillChangedItem {
            skill_id: skill_id('I'),
            owner: Some("owner".to_owned()),
            slug: "skill".to_owned(),
            source_kind: "user".to_owned(),
            change_type: "updated".to_owned(),
            fingerprint_before: Some("before".to_owned()),
            fingerprint_after: Some("after".to_owned()),
        };
        let changed_json = serde_json::to_value(&changed).expect("change encodes");
        assert_eq!(changed_json["skill_id"], "IIIIIIIIIIIIIIIIIIIII");
        assert_eq!(changed_json["owner"], "owner");
        assert_eq!(changed_json["slug"], "skill");
    }

    #[test]
    fn install_upload_id_is_not_skill_id() {
        let source = SkillLifecycleSource::UploadedArchive {
            upload_id: "UUUUUUUUUUUUUUUUUUUUU".to_owned(),
        };
        let result = SkillLifecycleResultSkill {
            skill_id: skill_id('S'),
            owner: None,
            slug: "skill".to_owned(),
            source_kind: "user".to_owned(),
            version: None,
            fingerprint: "fingerprint".to_owned(),
            trust_level: "community".to_owned(),
            install_path: "/managed/S/skill".to_owned(),
        };

        let source_json = serde_json::to_value(source).unwrap();
        let result_json = serde_json::to_value(result).unwrap();
        assert_ne!(source_json["upload_id"], result_json["skill_id"]);
    }

    #[test]
    fn upload_control_params_round_trip() {
        let start = SkillsUploadStartParams {
            workspace_id: "ws_000000000000000001".to_owned(),
            file_name: "skill.tar.gz".to_owned(),
            archive_format: SkillArchiveFormat::TarGz,
            compressed_size_bytes: 128,
            uncompressed_size_hint_bytes: Some(256),
            sha256: "a".repeat(64),
        };
        assert_eq!(
            serde_json::from_value::<SkillsUploadStartParams>(
                serde_json::to_value(&start).expect("start params encode")
            )
            .expect("start params decode"),
            start
        );

        let finish = SkillsUploadFinishParams {
            workspace_id: start.workspace_id.clone(),
            upload_id: "upload_00000000000001".to_owned(),
        };
        assert_eq!(
            serde_json::from_value::<SkillsUploadFinishParams>(
                serde_json::to_value(&finish).expect("finish params encode")
            )
            .expect("finish params decode"),
            finish
        );

        let abort = SkillsUploadAbortParams {
            workspace_id: start.workspace_id,
            upload_id: finish.upload_id,
        };
        assert_eq!(
            serde_json::from_value::<SkillsUploadAbortParams>(
                serde_json::to_value(&abort).expect("abort params encode")
            )
            .expect("abort params decode"),
            abort
        );
    }

    #[test]
    fn upload_chunk_header_and_ack_round_trip() {
        let header = SkillsUploadChunkHeader {
            workspace_id: "ws_000000000000000001".to_owned(),
            upload_id: "upload_00000000000001".to_owned(),
            offset: 1024,
            len: 512,
            chunk_sha256: Some("b".repeat(64)),
        };
        assert_eq!(
            serde_json::from_value::<SkillsUploadChunkHeader>(
                serde_json::to_value(&header).expect("header encode")
            )
            .expect("header decode"),
            header
        );

        let ack = SkillsUploadChunkAckNotification {
            upload_id: header.upload_id,
            offset: header.offset,
            len: header.len,
            received_bytes: 1536,
            next_offset: 1536,
        };
        assert_eq!(
            serde_json::from_value::<SkillsUploadChunkAckNotification>(
                serde_json::to_value(&ack).expect("ack encode")
            )
            .expect("ack decode"),
            ack
        );
    }

    #[test]
    fn generated_lifecycle_source_schema_does_not_expose_path_source() {
        let schema = crate::protocol_schema_documents()
            .into_iter()
            .find(|document| document.file_name == "skill_lifecycle_source.json")
            .expect("skill lifecycle source schema should be exported");
        let schema_json = serde_json::to_string(&schema.schema).expect("schema should encode");

        assert!(schema_json.contains("uploaded_archive"));
        assert!(!schema_json.contains("\"path\""));
    }
}
