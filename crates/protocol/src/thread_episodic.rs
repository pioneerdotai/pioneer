use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ThreadEpisodicWorkspaceId(pub String);

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ThreadEpisodicThreadId(pub String);

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ThreadEpisodicTurnId(pub String);

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ThreadEpisodicItemId(pub String);

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ThreadEpisodicChunkId(pub String);

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicSourceActorRole {
    User,
    Assistant,
    ToolSummary,
    TaskSummary,
    GeneratedSummary,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicSourceContext {
    UserVisibleThreadItem,
    UserVisibleToolSummary,
    UserVisibleTaskSummary,
    ThreadCompactionSummary,
    HiddenPrompt,
    ReasoningTrace,
    RawToolOutput,
    RawTaskRuntime,
    InternalHookRuntime,
    SystemPrompt,
    DeveloperPrompt,
    #[serde(other)]
    Unknown,
}

impl ThreadEpisodicSourceContext {
    pub const fn is_user_visible(self) -> bool {
        matches!(
            self,
            Self::UserVisibleThreadItem
                | Self::UserVisibleToolSummary
                | Self::UserVisibleTaskSummary
                | Self::ThreadCompactionSummary
        )
    }

    pub const fn is_hidden_or_internal(self) -> bool {
        !self.is_user_visible()
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicChunkStatus {
    PendingIndex,
    Indexed,
    Active,
    IndexFailed,
    Deleted,
    Excluded,
    #[serde(other)]
    Unknown,
}

impl ThreadEpisodicChunkStatus {
    pub const fn is_recallable(self) -> bool {
        matches!(self, Self::Indexed | Self::Active)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicVisibility {
    UserVisible,
    Hidden,
    Internal,
    #[serde(other)]
    Unknown,
}

impl ThreadEpisodicVisibility {
    pub const fn is_user_visible(self) -> bool {
        matches!(self, Self::UserVisible)
    }

    pub const fn is_hidden_or_internal(self) -> bool {
        matches!(self, Self::Hidden | Self::Internal)
    }
}

/// Evidence pointer for a thread episodic chunk.
///
/// This is not a durable-memory identity. It points to where recalled
/// conversation context came from so later layers can filter, debug, rebuild,
/// and cite the exact source chunk without reusing durable memory record ids.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicSourceProvenance {
    pub source_id: String,
    pub workspace_id: ThreadEpisodicWorkspaceId,
    pub thread_id: ThreadEpisodicThreadId,
    pub turn_id: ThreadEpisodicTurnId,
    pub item_id: ThreadEpisodicItemId,
    pub chunk_id: ThreadEpisodicChunkId,
    pub chunk_index: u32,
    pub source_actor_role: ThreadEpisodicSourceActorRole,
    pub source_context: ThreadEpisodicSourceContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicChunk {
    pub provenance: ThreadEpisodicSourceProvenance,
    pub status: ThreadEpisodicChunkStatus,
    pub visibility: ThreadEpisodicVisibility,
    pub text_hash: String,
    pub source_text_hash: String,
    pub char_start: u32,
    pub char_end: u32,
    pub byte_start: u32,
    pub byte_end: u32,
    pub token_estimate: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicSearchMode {
    Auto,
    Semantic,
    Lexical,
    Temporal,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicAdaptiveStrategy {
    Combined,
    Relative,
    Absolute,
    Cliff,
    Elbow,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ThreadEpisodicScoreBreakdown {
    pub final_score: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memvid_score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lexical_score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal_score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_source_boost: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recency_boost: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_role_boost: Option<f32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ThreadEpisodicAdaptiveDiagnostics {
    pub search_mode: ThreadEpisodicSearchMode,
    pub strategy: ThreadEpisodicAdaptiveStrategy,
    pub min_relevancy: f32,
    pub max_candidates: u32,
    pub total_candidates: u32,
    pub results_returned: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cutoff_score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cutoff_reason: Option<String>,
    #[serde(default)]
    pub native_memvid_adaptive_used: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ThreadEpisodicHit {
    pub provenance: ThreadEpisodicSourceProvenance,
    pub text: String,
    pub score: f32,
    pub score_breakdown: ThreadEpisodicScoreBreakdown,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adaptive_diagnostics: Option<ThreadEpisodicAdaptiveDiagnostics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicRecallPolicyContext {
    pub context_recall_allowed: bool,
    #[serde(default)]
    pub include_sensitive_context: bool,
}

impl Default for ThreadEpisodicRecallPolicyContext {
    fn default() -> Self {
        Self {
            context_recall_allowed: true,
            include_sensitive_context: false,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ThreadEpisodicRecallInput {
    pub workspace_id: ThreadEpisodicWorkspaceId,
    pub thread_id: ThreadEpisodicThreadId,
    pub turn_id: ThreadEpisodicTurnId,
    pub query_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recent_context_summary: Option<String>,
    #[serde(default)]
    pub policy_context: ThreadEpisodicRecallPolicyContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_prompt_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_candidates: Option<u32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicRecallDiagnosticCode {
    Completed,
    SkippedByPolicy,
    BackendUnavailable,
    InvalidInput,
    PromptBudgetExceeded,
    SuppressedByBoundary,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicRecallDiagnostic {
    pub code: ThreadEpisodicRecallDiagnosticCode,
    pub message: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ThreadEpisodicRecallOutput {
    #[serde(default)]
    pub hits: Vec<ThreadEpisodicHit>,
    #[serde(default)]
    pub diagnostics: Vec<ThreadEpisodicRecallDiagnostic>,
    #[serde(default)]
    pub fallback_used: bool,
}

#[cfg(test)]
mod tests {
    use super::{
        ThreadEpisodicAdaptiveDiagnostics, ThreadEpisodicAdaptiveStrategy, ThreadEpisodicChunkId,
        ThreadEpisodicChunkStatus, ThreadEpisodicHit, ThreadEpisodicItemId,
        ThreadEpisodicRecallDiagnostic, ThreadEpisodicRecallDiagnosticCode,
        ThreadEpisodicRecallInput, ThreadEpisodicRecallOutput, ThreadEpisodicScoreBreakdown,
        ThreadEpisodicSearchMode, ThreadEpisodicSourceActorRole, ThreadEpisodicSourceContext,
        ThreadEpisodicSourceProvenance, ThreadEpisodicThreadId, ThreadEpisodicTurnId,
        ThreadEpisodicVisibility, ThreadEpisodicWorkspaceId,
    };
    use crate::{MemoryRecord, PromptManifestHookContributionKind};
    use serde_json::json;
    use std::any::TypeId;

    fn sample_provenance() -> ThreadEpisodicSourceProvenance {
        ThreadEpisodicSourceProvenance {
            source_id: "thread:turn_41/item_1/chunk_0".to_owned(),
            workspace_id: ThreadEpisodicWorkspaceId("ws_1".to_owned()),
            thread_id: ThreadEpisodicThreadId("thread_1".to_owned()),
            turn_id: ThreadEpisodicTurnId("turn_41".to_owned()),
            item_id: ThreadEpisodicItemId("item_1".to_owned()),
            chunk_id: ThreadEpisodicChunkId("chunk_0".to_owned()),
            chunk_index: 0,
            source_actor_role: ThreadEpisodicSourceActorRole::Assistant,
            source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
            created_at: Some(123),
        }
    }

    #[test]
    fn thread_episodic_provenance_roundtrips() {
        let provenance = sample_provenance();
        let encoded = serde_json::to_value(&provenance).expect("encode provenance");
        assert_eq!(
            encoded,
            json!({
                "source_id": "thread:turn_41/item_1/chunk_0",
                "workspace_id": "ws_1",
                "thread_id": "thread_1",
                "turn_id": "turn_41",
                "item_id": "item_1",
                "chunk_id": "chunk_0",
                "chunk_index": 0,
                "source_actor_role": "assistant",
                "source_context": "user_visible_thread_item",
                "created_at": 123
            })
        );

        let decoded: ThreadEpisodicSourceProvenance =
            serde_json::from_value(encoded).expect("decode provenance");
        assert_eq!(decoded, provenance);
    }

    #[test]
    fn hidden_and_internal_contexts_are_representable() {
        for context in [
            ThreadEpisodicSourceContext::HiddenPrompt,
            ThreadEpisodicSourceContext::ReasoningTrace,
            ThreadEpisodicSourceContext::RawToolOutput,
            ThreadEpisodicSourceContext::InternalHookRuntime,
            ThreadEpisodicSourceContext::SystemPrompt,
            ThreadEpisodicSourceContext::DeveloperPrompt,
        ] {
            assert!(context.is_hidden_or_internal());
            assert!(!context.is_user_visible());
        }
    }

    #[test]
    fn chunk_status_roundtrips_and_marks_recallable_states() {
        let encoded = serde_json::to_value(ThreadEpisodicChunkStatus::PendingIndex)
            .expect("encode chunk status");
        assert_eq!(encoded, json!("pending_index"));

        let decoded: ThreadEpisodicChunkStatus =
            serde_json::from_value(json!("indexed")).expect("decode chunk status");
        assert_eq!(decoded, ThreadEpisodicChunkStatus::Indexed);
        assert!(decoded.is_recallable());
        assert!(ThreadEpisodicChunkStatus::Active.is_recallable());
        assert!(!ThreadEpisodicChunkStatus::Deleted.is_recallable());
        assert!(!ThreadEpisodicChunkStatus::Excluded.is_recallable());
        assert!(!ThreadEpisodicChunkStatus::IndexFailed.is_recallable());
    }

    #[test]
    fn visibility_roundtrips_without_collapsing_hidden_internal_to_user_visible() {
        let hidden: ThreadEpisodicVisibility =
            serde_json::from_value(json!("hidden")).expect("decode hidden visibility");
        let internal: ThreadEpisodicVisibility =
            serde_json::from_value(json!("internal")).expect("decode internal visibility");

        assert_eq!(hidden, ThreadEpisodicVisibility::Hidden);
        assert_eq!(internal, ThreadEpisodicVisibility::Internal);
        assert!(hidden.is_hidden_or_internal());
        assert!(internal.is_hidden_or_internal());
        assert!(!hidden.is_user_visible());
        assert!(!internal.is_user_visible());
        assert_eq!(
            serde_json::to_value(ThreadEpisodicVisibility::UserVisible)
                .expect("encode user-visible visibility"),
            json!("user_visible")
        );
    }

    #[test]
    fn unknown_boundary_values_fail_safely() {
        let context: ThreadEpisodicSourceContext =
            serde_json::from_value(json!("provider_added_new_context"))
                .expect("unknown context maps to safe unknown");
        let status: ThreadEpisodicChunkStatus =
            serde_json::from_value(json!("provider_added_new_status"))
                .expect("unknown status maps to safe unknown");
        let visibility: ThreadEpisodicVisibility =
            serde_json::from_value(json!("provider_added_new_visibility"))
                .expect("unknown visibility maps to safe unknown");

        assert_eq!(context, ThreadEpisodicSourceContext::Unknown);
        assert!(context.is_hidden_or_internal());
        assert_eq!(status, ThreadEpisodicChunkStatus::Unknown);
        assert!(!status.is_recallable());
        assert_eq!(visibility, ThreadEpisodicVisibility::Unknown);
        assert!(!visibility.is_user_visible());
        assert!(!visibility.is_hidden_or_internal());
    }

    #[test]
    fn prompt_manifest_can_distinguish_thread_context_from_durable_memory_recall() {
        let encoded = serde_json::to_value(PromptManifestHookContributionKind::ThreadContext)
            .expect("encode thread-context manifest kind");
        assert_eq!(encoded, json!("thread_context"));
        assert_ne!(
            PromptManifestHookContributionKind::ThreadContext,
            PromptManifestHookContributionKind::PromptContext
        );
    }

    #[test]
    fn thread_episodic_ids_are_not_durable_memory_ids() {
        assert_ne!(
            TypeId::of::<ThreadEpisodicChunkId>(),
            TypeId::of::<String>()
        );
        assert_ne!(
            TypeId::of::<ThreadEpisodicChunkId>(),
            TypeId::of::<MemoryRecord>()
        );
    }

    #[test]
    fn thread_episodic_recall_input_roundtrips_with_policy_context() {
        let input = ThreadEpisodicRecallInput {
            workspace_id: ThreadEpisodicWorkspaceId("ws_1".to_owned()),
            thread_id: ThreadEpisodicThreadId("thread_1".to_owned()),
            turn_id: ThreadEpisodicTurnId("turn_42".to_owned()),
            query_text: "continue the phase".to_owned(),
            recent_context_summary: Some("phase discussion".to_owned()),
            policy_context: Default::default(),
            max_prompt_chars: Some(2048),
            max_candidates: Some(40),
        };

        let encoded = serde_json::to_value(&input).expect("encode input");
        assert_eq!(encoded["workspace_id"], "ws_1");
        assert_eq!(encoded["thread_id"], "thread_1");
        assert_eq!(encoded["turn_id"], "turn_42");
        assert_eq!(encoded["policy_context"]["context_recall_allowed"], true);

        let decoded: ThreadEpisodicRecallInput =
            serde_json::from_value(encoded).expect("decode input");
        assert_eq!(decoded, input);
    }

    #[test]
    fn thread_episodic_recall_output_preserves_source_provenance() {
        let provenance = sample_provenance();
        let output = ThreadEpisodicRecallOutput {
            hits: vec![ThreadEpisodicHit {
                provenance: provenance.clone(),
                text: "Phase 17 uses canonical keys.".to_owned(),
                score: 0.87,
                score_breakdown: ThreadEpisodicScoreBreakdown {
                    final_score: 0.87,
                    memvid_score: Some(0.75),
                    semantic_score: Some(0.72),
                    lexical_score: None,
                    temporal_score: None,
                    exact_source_boost: Some(0.1),
                    recency_boost: Some(0.02),
                    source_role_boost: None,
                },
                adaptive_diagnostics: Some(ThreadEpisodicAdaptiveDiagnostics {
                    search_mode: ThreadEpisodicSearchMode::Auto,
                    strategy: ThreadEpisodicAdaptiveStrategy::Combined,
                    min_relevancy: 0.45,
                    max_candidates: 40,
                    total_candidates: 12,
                    results_returned: 3,
                    cutoff_score: Some(0.45),
                    cutoff_reason: Some("combined_cutoff".to_owned()),
                    native_memvid_adaptive_used: true,
                }),
                created_at: Some(123),
            }],
            diagnostics: vec![ThreadEpisodicRecallDiagnostic {
                code: ThreadEpisodicRecallDiagnosticCode::Completed,
                message: "ok".to_owned(),
            }],
            fallback_used: false,
        };

        let encoded = serde_json::to_value(&output).expect("encode output");
        assert_eq!(encoded["hits"][0]["provenance"]["turn_id"], "turn_41");
        assert_eq!(encoded["hits"][0]["provenance"]["item_id"], "item_1");
        assert_eq!(encoded["hits"][0]["provenance"]["chunk_id"], "chunk_0");

        let decoded: ThreadEpisodicRecallOutput =
            serde_json::from_value(encoded).expect("decode output");
        assert_eq!(decoded.hits[0].provenance, provenance);
    }
}
