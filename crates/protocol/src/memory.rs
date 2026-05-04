use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScopeKind {
    User,
    Workspace,
    Thread,
    Agent,
    Task,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryScope {
    pub kind: MemoryScopeKind,
    pub key: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCategory {
    Identity,
    Preference,
    Biography,
    Relationship,
    ProjectFact,
    ProjectDecision,
    Procedure,
    Todo,
    Constraint,
    CommunicationStyle,
    Custom,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryStatus {
    Active,
    Superseded,
    Deleted,
    Expired,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemorySensitivity {
    Normal,
    Personal,
    SecretLike,
    Regulated,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemorySourceKind {
    ExplicitUserRequest,
    UserCorrection,
    AssistantInference,
    BackgroundExtractor,
    ToolObservation,
    Import,
    System,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryActorKind {
    User,
    Assistant,
    Extractor,
    System,
    Tool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryActor {
    pub kind: MemoryActorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryProvenance {
    pub source_kind: MemorySourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<MemoryActor>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryRecord {
    pub id: String,
    pub scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub category: MemoryCategory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub content: String,
    pub status: MemoryStatus,
    pub confidence: f32,
    pub importance: f32,
    pub sensitivity: MemorySensitivity,
    pub provenance: MemoryProvenance,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_accessed_at: Option<i64>,
    #[serde(default)]
    pub access_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_reason: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemorySearchParams {
    pub query: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<MemoryScope>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<MemoryCategory>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<MemoryStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default)]
    pub include_provenance: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemorySearchHit {
    pub record: MemoryRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matched_terms: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemorySearchResponse {
    #[serde(default)]
    pub hits: Vec<MemorySearchHit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryGetParams {
    pub memory_id: String,
    #[serde(default)]
    pub include_deleted: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryGetResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<MemoryRecord>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryListParams {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<MemoryScope>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<MemoryCategory>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<MemoryStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryListResponse {
    #[serde(default)]
    pub records: Vec<MemoryRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryRememberParams {
    pub scope: MemoryScope,
    pub category: MemoryCategory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sensitivity: Option<MemorySensitivity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub importance: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<MemoryProvenance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryRememberResponse {
    pub record: MemoryRecord,
    #[serde(default)]
    pub created: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_memory_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MemoryForgetTarget {
    #[serde(rename_all = "snake_case")]
    Id { memory_id: String },
    #[serde(rename_all = "snake_case")]
    ScopedKey {
        scope: MemoryScope,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
        key: String,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryForgetParams {
    pub target: MemoryForgetTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<MemoryActor>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryForgetResponse {
    #[serde(default)]
    pub forgotten_memory_ids: Vec<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCandidateStatus {
    Pending,
    Approved,
    Rejected,
    Expired,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidate {
    pub id: String,
    pub scope: MemoryScope,
    pub category: MemoryCategory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub candidate_text: String,
    pub confidence: f32,
    pub reason: String,
    pub provenance: MemoryProvenance,
    pub status: MemoryCandidateStatus,
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decided_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_reason: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCandidateDecision {
    Approve,
    Reject,
    Expire,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryCandidatesListParams {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<MemoryScope>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<MemoryCandidateStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryCandidatesListResponse {
    #[serde(default)]
    pub candidates: Vec<MemoryCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryCandidatesDecideParams {
    pub candidate_id: String,
    pub decision: MemoryCandidateDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<MemoryActor>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidatesDecideResponse {
    pub candidate: MemoryCandidate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<MemoryRecord>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryChangeKind {
    Created,
    Updated,
    Superseded,
    Deleted,
    Restored,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryChangedNotification {
    pub memory_id: String,
    pub scope: MemoryScope,
    pub change_kind: MemoryChangeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<MemoryRecord>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidateCreatedNotification {
    pub candidate: MemoryCandidate,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryForgottenNotification {
    #[serde(default)]
    pub memory_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        MemoryCandidateDecision, MemoryCategory, MemoryForgetTarget, MemoryRememberParams,
        MemoryScope, MemoryScopeKind, MemorySearchParams,
    };
    use crate::constants;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn memory_scope_kind_uses_snake_case() {
        let encoded = serde_json::to_value(MemoryScopeKind::Workspace).expect("scope kind encode");
        assert_eq!(encoded, json!("workspace"));
    }

    #[test]
    fn memory_category_uses_snake_case() {
        let encoded =
            serde_json::to_value(MemoryCategory::ProjectDecision).expect("category encode");
        assert_eq!(encoded, json!("project_decision"));
    }

    #[test]
    fn remember_params_skip_absent_optional_fields() {
        let encoded = serde_json::to_value(MemoryRememberParams {
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".to_owned(),
            },
            category: MemoryCategory::Identity,
            namespace: None,
            key: None,
            content: "User birthday is September 12.".to_owned(),
            sensitivity: None,
            confidence: None,
            importance: None,
            provenance: None,
            idempotency_key: None,
            supersedes: None,
            metadata: BTreeMap::new(),
        })
        .expect("remember params encode");

        assert_eq!(
            encoded,
            json!({
                "scope": {
                    "kind": "user",
                    "key": "default"
                },
                "category": "identity",
                "content": "User birthday is September 12."
            })
        );
    }

    #[test]
    fn search_params_default_collections_are_empty() {
        let params: MemorySearchParams =
            serde_json::from_value(json!({"query": "birthday"})).expect("search params decode");

        assert_eq!(params.query, "birthday");
        assert!(params.scopes.is_empty());
        assert!(params.categories.is_empty());
        assert!(params.statuses.is_empty());
        assert_eq!(params.limit, None);
        assert_eq!(params.cursor, None);
        assert!(!params.include_provenance);
    }

    #[test]
    fn forget_target_scoped_key_roundtrips() {
        let target = MemoryForgetTarget::ScopedKey {
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".to_owned(),
            },
            namespace: None,
            key: "user.birthday".to_owned(),
        };

        let encoded = serde_json::to_value(&target).expect("forget target encode");
        assert_eq!(
            encoded,
            json!({
                "kind": "scoped_key",
                "scope": {
                    "kind": "user",
                    "key": "default"
                },
                "key": "user.birthday"
            })
        );

        let decoded: MemoryForgetTarget =
            serde_json::from_value(encoded).expect("forget target decode");
        assert_eq!(decoded, target);
    }

    #[test]
    fn candidate_decision_roundtrips() {
        let encoded =
            serde_json::to_value(MemoryCandidateDecision::Approve).expect("decision encode");
        assert_eq!(encoded, json!("approve"));

        let decoded: MemoryCandidateDecision =
            serde_json::from_value(encoded).expect("decision decode");
        assert_eq!(decoded, MemoryCandidateDecision::Approve);
    }

    #[test]
    fn constants_include_memory_methods_and_events() {
        assert_eq!(constants::methods::MEMORY_SEARCH, "memory/search");
        assert_eq!(
            constants::methods::MEMORY_CANDIDATES_DECIDE,
            "memory/candidates/decide"
        );
        assert_eq!(constants::events::MEMORY_CHANGED, "memory/changed");
        assert_eq!(
            constants::events::MEMORY_CANDIDATE_CREATED,
            "memory/candidate_created"
        );
    }

    #[test]
    fn schema_documents_include_memory_contracts() {
        let schema_names = crate::protocol_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "memory_record.json",
            "memory_search_params.json",
            "memory_search_response.json",
            "memory_remember_params.json",
            "memory_forget_params.json",
            "memory_candidate.json",
            "memory_candidates_decide_params.json",
            "memory_changed_notification.json",
        ] {
            assert!(
                schema_names.iter().any(|name| *name == expected),
                "missing schema document {expected}"
            );
        }
    }
}
