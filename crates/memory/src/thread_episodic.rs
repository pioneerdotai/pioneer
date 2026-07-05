use async_trait::async_trait;
use memvid_core::{
    AclEnforcementMode, DocMetadata, FrameStatus, Memvid, MemvidError, PutOptions,
    SearchEngineKind, SearchHit, SearchRequest, TemporalFilter,
};
use pioneer_protocol::{
    ThreadEpisodicAdaptiveStrategy, ThreadEpisodicItemStatus, ThreadEpisodicScoreBreakdown,
    ThreadEpisodicSearchMode, ThreadEpisodicSourceContext, ThreadEpisodicVisibility,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const THREAD_EPISODIC_SCHEMA_VERSION: &str = "1";
const THREAD_EPISODIC_TRACK: &str = "pioneer_thread_episodic";
const THREAD_EPISODIC_KIND: &str = "pioneer.thread_episodic.item";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicMemvidCapabilityState {
    Supported,
    Disabled,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadEpisodicMemvidBackendCapabilities {
    pub adaptive_retrieval: ThreadEpisodicMemvidCapabilityState,
    pub adaptive_retrieval_implementation: ThreadEpisodicAdaptiveRetrievalImplementation,
    pub semantic_search: ThreadEpisodicMemvidCapabilityState,
    pub lexical_search: ThreadEpisodicMemvidCapabilityState,
    pub temporal_search: ThreadEpisodicMemvidCapabilityState,
    pub graph_search: ThreadEpisodicMemvidCapabilityState,
}

impl ThreadEpisodicMemvidBackendCapabilities {
    pub const fn memvid_default() -> Self {
        Self {
            adaptive_retrieval: ThreadEpisodicMemvidCapabilityState::Supported,
            adaptive_retrieval_implementation:
                ThreadEpisodicAdaptiveRetrievalImplementation::PioneerFallback,
            semantic_search: ThreadEpisodicMemvidCapabilityState::Unsupported,
            lexical_search: ThreadEpisodicMemvidCapabilityState::Supported,
            temporal_search: ThreadEpisodicMemvidCapabilityState::Supported,
            graph_search: ThreadEpisodicMemvidCapabilityState::Disabled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicAdaptiveRetrievalImplementation {
    NativeMemvid,
    PioneerFallback,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadEpisodicMemvidFeatureAudit {
    pub memvid_core_version: String,
    pub enabled_features: Vec<String>,
    pub disabled_features: Vec<String>,
    pub adaptive_retrieval_implementation: ThreadEpisodicAdaptiveRetrievalImplementation,
    pub graph_search_note: String,
}

impl ThreadEpisodicMemvidFeatureAudit {
    pub fn current() -> Self {
        Self {
            memvid_core_version: "2.0.139".to_owned(),
            enabled_features: vec!["lex".to_owned(), "temporal_track".to_owned(), "simd".to_owned()],
            disabled_features: vec!["vec".to_owned(), "logic_mesh".to_owned()],
            adaptive_retrieval_implementation:
                ThreadEpisodicAdaptiveRetrievalImplementation::PioneerFallback,
            graph_search_note: "logic_mesh/graph search is intentionally not enabled for thread episodic memory in this phase".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicMemvidFailureKind {
    Retryable,
    NonRetryable,
    CapacityExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadEpisodicMemvidError {
    pub kind: ThreadEpisodicMemvidFailureKind,
    pub message: String,
}

impl ThreadEpisodicMemvidError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            kind: ThreadEpisodicMemvidFailureKind::Retryable,
            message: message.into(),
        }
    }

    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            kind: ThreadEpisodicMemvidFailureKind::NonRetryable,
            message: message.into(),
        }
    }

    pub fn capacity_exceeded(message: impl Into<String>) -> Self {
        Self {
            kind: ThreadEpisodicMemvidFailureKind::CapacityExceeded,
            message: message.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self.kind, ThreadEpisodicMemvidFailureKind::Retryable)
    }
}

impl Display for ThreadEpisodicMemvidError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl Error for ThreadEpisodicMemvidError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ThreadEpisodicMemvidStats {
    pub active_frame_count: Option<i64>,
    pub frame_count: Option<i64>,
    pub size_bytes: Option<i64>,
    pub capacity_bytes: Option<i64>,
    pub remaining_capacity_bytes: Option<i64>,
    pub utilization_percent: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicMemvidIndexRequest {
    pub storage_uri: String,
    pub capsule_id: String,
    pub capsule_ref: String,
    pub workspace_capsule: bool,
    pub index_item_id: String,
    pub frame_uri: String,
    pub text: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadEpisodicMemvidIndexOutput {
    pub frame_id: i64,
    pub frame_uri: String,
    pub stats: ThreadEpisodicMemvidStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicSearchProfileKind {
    DefaultContext,
    ExactReference,
    HighRecallContinuation,
    RecentContext,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicSearchProfile {
    pub kind: ThreadEpisodicSearchProfileKind,
    pub mode: ThreadEpisodicSearchMode,
    pub adaptive_strategy: ThreadEpisodicAdaptiveStrategy,
    pub min_relevancy: f32,
    pub max_candidates: u32,
    pub min_results: u32,
    pub max_segments: u32,
    pub snippet_chars: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recent_start_unix: Option<i64>,
}

impl ThreadEpisodicSearchProfile {
    pub fn for_kind(kind: ThreadEpisodicSearchProfileKind) -> Self {
        match kind {
            ThreadEpisodicSearchProfileKind::DefaultContext => Self {
                kind,
                mode: ThreadEpisodicSearchMode::Auto,
                adaptive_strategy: ThreadEpisodicAdaptiveStrategy::Combined,
                min_relevancy: 0.25,
                max_candidates: 32,
                min_results: 1,
                max_segments: 4,
                snippet_chars: 360,
                recent_start_unix: None,
            },
            ThreadEpisodicSearchProfileKind::ExactReference => Self {
                kind,
                mode: ThreadEpisodicSearchMode::Lexical,
                adaptive_strategy: ThreadEpisodicAdaptiveStrategy::Absolute,
                min_relevancy: 0.55,
                max_candidates: 16,
                min_results: 1,
                max_segments: 6,
                snippet_chars: 420,
                recent_start_unix: None,
            },
            ThreadEpisodicSearchProfileKind::HighRecallContinuation => Self {
                kind,
                mode: ThreadEpisodicSearchMode::Auto,
                adaptive_strategy: ThreadEpisodicAdaptiveStrategy::Relative,
                min_relevancy: 0.20,
                max_candidates: 64,
                min_results: 2,
                max_segments: 8,
                snippet_chars: 420,
                recent_start_unix: None,
            },
            ThreadEpisodicSearchProfileKind::RecentContext => Self {
                kind,
                mode: ThreadEpisodicSearchMode::Temporal,
                adaptive_strategy: ThreadEpisodicAdaptiveStrategy::Combined,
                min_relevancy: 0.20,
                max_candidates: 40,
                min_results: 1,
                max_segments: 4,
                snippet_chars: 360,
                recent_start_unix: None,
            },
        }
    }

    pub fn validate(&self) -> Result<(), ThreadEpisodicMemvidError> {
        if !(0.0..=1.0).contains(&self.min_relevancy) {
            return Err(ThreadEpisodicMemvidError::non_retryable(format!(
                "thread episodic min_relevancy {} must be in 0..=1",
                self.min_relevancy
            )));
        }
        if self.max_candidates == 0 || self.max_candidates > 256 {
            return Err(ThreadEpisodicMemvidError::non_retryable(format!(
                "thread episodic max_candidates {} must be in 1..=256",
                self.max_candidates
            )));
        }
        if self.min_results > self.max_candidates {
            return Err(ThreadEpisodicMemvidError::non_retryable(
                "thread episodic min_results cannot exceed max_candidates",
            ));
        }
        if self.max_segments == 0 || self.max_segments > 32 {
            return Err(ThreadEpisodicMemvidError::non_retryable(format!(
                "thread episodic max_segments {} must be in 1..=32",
                self.max_segments
            )));
        }
        if self.snippet_chars == 0 || self.snippet_chars > 4_000 {
            return Err(ThreadEpisodicMemvidError::non_retryable(format!(
                "thread episodic snippet_chars {} must be in 1..=4000",
                self.snippet_chars
            )));
        }
        Ok(())
    }

    pub fn adaptive_cutoff_config(&self) -> PioneerAdaptiveCutoffConfig {
        PioneerAdaptiveCutoffConfig {
            strategy: self.adaptive_strategy,
            min_relevancy: self.min_relevancy,
            max_candidates: self.max_candidates,
            min_results: self.min_results,
            relative_min_ratio: 0.50,
            cliff_drop_ratio: 0.40,
            elbow_sensitivity: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PioneerAdaptiveCutoffConfig {
    pub strategy: ThreadEpisodicAdaptiveStrategy,
    pub min_relevancy: f32,
    pub max_candidates: u32,
    pub min_results: u32,
    pub relative_min_ratio: f32,
    pub cliff_drop_ratio: f32,
    pub elbow_sensitivity: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PioneerAdaptiveCutoffReason {
    NoCandidates,
    NoScores,
    MaxCandidates,
    AbsoluteThreshold,
    RelativeThreshold,
    ScoreCliff,
    Elbow,
    Combined,
    MinResults,
}

impl PioneerAdaptiveCutoffReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoCandidates => "no_candidates",
            Self::NoScores => "no_scores",
            Self::MaxCandidates => "max_candidates",
            Self::AbsoluteThreshold => "absolute_threshold",
            Self::RelativeThreshold => "relative_threshold",
            Self::ScoreCliff => "score_cliff",
            Self::Elbow => "elbow",
            Self::Combined => "combined",
            Self::MinResults => "min_results",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PioneerAdaptiveCutoffDiagnostics {
    pub strategy: ThreadEpisodicAdaptiveStrategy,
    pub min_relevancy: f32,
    pub cutoff_score: Option<f32>,
    pub cutoff_reason: PioneerAdaptiveCutoffReason,
    pub candidate_count: u32,
    pub result_count: u32,
}

pub struct PioneerAdaptiveCutoff;

impl PioneerAdaptiveCutoff {
    pub fn cutoff_len(
        scores: &[Option<f32>],
        config: PioneerAdaptiveCutoffConfig,
    ) -> (usize, PioneerAdaptiveCutoffDiagnostics) {
        let bounded_count = scores.len().min(config.max_candidates as usize);
        if bounded_count == 0 {
            return (
                0,
                PioneerAdaptiveCutoffDiagnostics {
                    strategy: config.strategy,
                    min_relevancy: config.min_relevancy,
                    cutoff_score: None,
                    cutoff_reason: PioneerAdaptiveCutoffReason::NoCandidates,
                    candidate_count: 0,
                    result_count: 0,
                },
            );
        }

        let bounded_scores = &scores[..bounded_count];
        let numeric_scores = bounded_scores
            .iter()
            .map(|score| score.unwrap_or(0.0))
            .collect::<Vec<_>>();
        let has_any_score = bounded_scores.iter().any(Option::is_some);
        if !has_any_score {
            let result_count = bounded_count.min(config.min_results.max(1) as usize).max(1);
            return (
                result_count,
                PioneerAdaptiveCutoffDiagnostics {
                    strategy: config.strategy,
                    min_relevancy: config.min_relevancy,
                    cutoff_score: None,
                    cutoff_reason: PioneerAdaptiveCutoffReason::NoScores,
                    candidate_count: bounded_count as u32,
                    result_count: result_count as u32,
                },
            );
        }

        let raw_cutoff = match config.strategy {
            ThreadEpisodicAdaptiveStrategy::Absolute => {
                absolute_cutoff(&numeric_scores, config.min_relevancy)
            }
            ThreadEpisodicAdaptiveStrategy::Relative => relative_cutoff(
                &numeric_scores,
                config.min_relevancy,
                config.relative_min_ratio,
            ),
            ThreadEpisodicAdaptiveStrategy::Cliff => cliff_cutoff(
                &numeric_scores,
                config.min_relevancy,
                config.cliff_drop_ratio,
            ),
            ThreadEpisodicAdaptiveStrategy::Elbow => elbow_cutoff(
                &numeric_scores,
                config.min_relevancy,
                config.elbow_sensitivity,
            ),
            ThreadEpisodicAdaptiveStrategy::Combined => combined_cutoff(&numeric_scores, config),
        };
        let min_results = config.min_results as usize;
        let adjusted_cutoff = raw_cutoff
            .max(min_results.min(bounded_count))
            .min(bounded_count);
        let reason = if adjusted_cutoff != raw_cutoff
            && adjusted_cutoff == min_results.min(bounded_count)
        {
            PioneerAdaptiveCutoffReason::MinResults
        } else if adjusted_cutoff == bounded_count {
            PioneerAdaptiveCutoffReason::MaxCandidates
        } else {
            match config.strategy {
                ThreadEpisodicAdaptiveStrategy::Absolute => {
                    PioneerAdaptiveCutoffReason::AbsoluteThreshold
                }
                ThreadEpisodicAdaptiveStrategy::Relative => {
                    PioneerAdaptiveCutoffReason::RelativeThreshold
                }
                ThreadEpisodicAdaptiveStrategy::Cliff => PioneerAdaptiveCutoffReason::ScoreCliff,
                ThreadEpisodicAdaptiveStrategy::Elbow => PioneerAdaptiveCutoffReason::Elbow,
                ThreadEpisodicAdaptiveStrategy::Combined => PioneerAdaptiveCutoffReason::Combined,
            }
        };
        let cutoff_score = adjusted_cutoff
            .checked_sub(1)
            .and_then(|index| numeric_scores.get(index).copied());
        (
            adjusted_cutoff,
            PioneerAdaptiveCutoffDiagnostics {
                strategy: config.strategy,
                min_relevancy: config.min_relevancy,
                cutoff_score,
                cutoff_reason: reason,
                candidate_count: bounded_count as u32,
                result_count: adjusted_cutoff as u32,
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadEpisodicMemvidSearchSegment {
    pub capsule_id: String,
    pub capsule_ref: String,
    pub storage_uri: String,
    pub segment_index: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ThreadEpisodicExactSourceTarget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_item_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicMemvidSearchRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub profile: ThreadEpisodicSearchProfile,
    #[serde(default)]
    pub segments: Vec<ThreadEpisodicMemvidSearchSegment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_source: Option<ThreadEpisodicExactSourceTarget>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicMemvidSearchHit {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub index_item_id: String,
    pub source_actor_role: String,
    pub source_runtime_kind: String,
    pub source_context: ThreadEpisodicSourceContext,
    pub visibility: ThreadEpisodicVisibility,
    pub status: ThreadEpisodicItemStatus,
    pub segment_index: i64,
    pub capsule_id: String,
    pub capsule_ref: String,
    pub frame_id: u64,
    pub frame_uri: String,
    pub text: String,
    pub memvid_score: Option<f32>,
    pub lexical_score: Option<f32>,
    pub semantic_score: Option<f32>,
    pub temporal_score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at_unix: Option<i64>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicRankedSearchHit {
    pub hit: ThreadEpisodicMemvidSearchHit,
    pub score_breakdown: ThreadEpisodicScoreBreakdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicCandidateSuppressionReason {
    HiddenOrInternal,
    Deleted,
    Excluded,
    IndexFailed,
    PendingIndex,
    WrongWorkspace,
    WrongThread,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadEpisodicCandidateSuppression {
    pub index_item_id: String,
    pub reason: ThreadEpisodicCandidateSuppressionReason,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicFilteredSearchCandidates {
    pub hits: Vec<ThreadEpisodicMemvidSearchHit>,
    pub suppressions: Vec<ThreadEpisodicCandidateSuppression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicSearchDiagnostics {
    pub profile_kind: ThreadEpisodicSearchProfileKind,
    pub search_mode: ThreadEpisodicSearchMode,
    pub adaptive: PioneerAdaptiveCutoffDiagnostics,
    pub searched_segment_ids: Vec<String>,
    pub searched_segment_count: u32,
    pub unavailable_segment_ids: Vec<String>,
    pub raw_candidate_count: u32,
    pub filtered_candidate_count: u32,
    pub returned_count: u32,
    pub native_memvid_adaptive_used: bool,
    #[serde(default)]
    pub suppressions: Vec<ThreadEpisodicCandidateSuppression>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicMemvidSearchOutput {
    pub hits: Vec<ThreadEpisodicRankedSearchHit>,
    pub diagnostics: ThreadEpisodicSearchDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ThreadEpisodicRankingContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_source: Option<ThreadEpisodicExactSourceTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_unix: Option<i64>,
}

#[async_trait]
pub trait ThreadEpisodicMemvidBackend: Send + Sync {
    fn capabilities(&self) -> ThreadEpisodicMemvidBackendCapabilities;

    async fn index_item(
        &self,
        request: ThreadEpisodicMemvidIndexRequest,
    ) -> Result<ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidError>;

    async fn search(
        &self,
        request: ThreadEpisodicMemvidSearchRequest,
    ) -> Result<ThreadEpisodicMemvidSearchOutput, ThreadEpisodicMemvidError>;
}

pub struct MemvidThreadEpisodicBackend {
    capabilities: ThreadEpisodicMemvidBackendCapabilities,
    locks: Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>,
}

impl MemvidThreadEpisodicBackend {
    pub fn new() -> Self {
        Self {
            capabilities: ThreadEpisodicMemvidBackendCapabilities::memvid_default(),
            locks: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn with_capabilities(capabilities: ThreadEpisodicMemvidBackendCapabilities) -> Self {
        Self {
            capabilities,
            locks: Mutex::new(BTreeMap::new()),
        }
    }

    async fn lock_for_path(&self, path: &Path) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().await;
        locks
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

impl Default for MemvidThreadEpisodicBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ThreadEpisodicMemvidBackend for MemvidThreadEpisodicBackend {
    fn capabilities(&self) -> ThreadEpisodicMemvidBackendCapabilities {
        self.capabilities.clone()
    }

    async fn index_item(
        &self,
        request: ThreadEpisodicMemvidIndexRequest,
    ) -> Result<ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidError> {
        let path = path_from_storage_uri(request.storage_uri.as_str())?;
        let parent = path.parent().ok_or_else(|| {
            ThreadEpisodicMemvidError::non_retryable(format!(
                "thread episodic memvid path `{}` has no parent",
                path.display()
            ))
        })?;
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            ThreadEpisodicMemvidError::retryable(format!(
                "failed to create thread episodic memvid dir `{}`: {error}",
                parent.display()
            ))
        })?;

        let lock = self.lock_for_path(path.as_path()).await;
        let _guard = lock.lock().await;
        let path_for_task = path.clone();
        let request_for_task = request.clone();

        tokio::task::spawn_blocking(move || index_item_blocking(path_for_task, request_for_task))
            .await
            .map_err(|error| {
                ThreadEpisodicMemvidError::retryable(format!(
                    "thread episodic memvid index task failed: {error}"
                ))
            })?
    }

    async fn search(
        &self,
        request: ThreadEpisodicMemvidSearchRequest,
    ) -> Result<ThreadEpisodicMemvidSearchOutput, ThreadEpisodicMemvidError> {
        request.profile.validate()?;
        let mut segments = request.segments.clone();
        segments.sort_by(|left, right| right.segment_index.cmp(&left.segment_index));
        segments.truncate(request.profile.max_segments as usize);

        let mut warnings = Vec::new();
        if request.profile.mode == ThreadEpisodicSearchMode::Semantic {
            warnings.push("semantic search requested but memvid vec feature is not enabled; using lexical search".to_owned());
        }

        let mut searched_segment_ids = Vec::new();
        let mut unavailable_segment_ids = Vec::new();
        let mut raw_hits = Vec::new();
        for segment in &segments {
            let path = path_from_storage_uri(segment.storage_uri.as_str())?;
            if !path.exists() {
                unavailable_segment_ids.push(segment.capsule_id.clone());
                continue;
            }

            let lock = self.lock_for_path(path.as_path()).await;
            let _guard = lock.lock().await;
            let path_for_task = path.clone();
            let search_request = memvid_thread_episodic_search_request(
                &request.profile,
                &request.query,
                request.scope.as_deref(),
            );
            let segment_for_task = segment.clone();
            let workspace_id = request.workspace_id.clone();
            let thread_id = request.thread_id.clone();
            let segment_hits = tokio::task::spawn_blocking(move || {
                search_segment_blocking(
                    path_for_task,
                    search_request,
                    segment_for_task,
                    workspace_id.as_str(),
                    thread_id.as_str(),
                )
            })
            .await
            .map_err(|error| {
                ThreadEpisodicMemvidError::retryable(format!(
                    "thread episodic memvid search task failed: {error}"
                ))
            })?;

            match segment_hits {
                Ok(hits) => {
                    searched_segment_ids.push(segment.capsule_id.clone());
                    raw_hits.extend(hits);
                }
                Err(error) if error.kind == ThreadEpisodicMemvidFailureKind::NonRetryable => {
                    unavailable_segment_ids.push(segment.capsule_id.clone());
                    warnings.push(error.message);
                }
                Err(error) => return Err(error),
            }
        }

        raw_hits.sort_by(|left, right| {
            score_or_zero(right.memvid_score)
                .partial_cmp(&score_or_zero(left.memvid_score))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.segment_index.cmp(&left.segment_index))
                .then_with(|| left.index_item_id.cmp(&right.index_item_id))
        });

        let scores = raw_hits
            .iter()
            .map(|hit| hit.memvid_score)
            .collect::<Vec<_>>();
        let (cutoff_len, adaptive) =
            PioneerAdaptiveCutoff::cutoff_len(&scores, request.profile.adaptive_cutoff_config());
        let raw_hits = raw_hits.into_iter().take(cutoff_len).collect::<Vec<_>>();
        let filtered = filter_thread_episodic_search_candidates(
            request.workspace_id.as_str(),
            request.thread_id.as_str(),
            raw_hits,
        );
        let mut ranked = rank_thread_episodic_search_hits(
            filtered.hits,
            ThreadEpisodicRankingContext {
                exact_source: request.exact_source,
                now_unix: None,
            },
        );
        ranked.truncate(request.profile.max_candidates as usize);

        let diagnostics = ThreadEpisodicSearchDiagnostics {
            profile_kind: request.profile.kind,
            search_mode: request.profile.mode,
            searched_segment_count: searched_segment_ids.len() as u32,
            searched_segment_ids,
            unavailable_segment_ids,
            raw_candidate_count: adaptive.candidate_count,
            filtered_candidate_count: ranked.len() as u32,
            returned_count: ranked.len() as u32,
            adaptive,
            native_memvid_adaptive_used: false,
            suppressions: filtered.suppressions,
            warnings,
        };

        Ok(ThreadEpisodicMemvidSearchOutput {
            hits: ranked,
            diagnostics,
        })
    }
}

fn index_item_blocking(
    path: PathBuf,
    request: ThreadEpisodicMemvidIndexRequest,
) -> Result<ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidError> {
    let mut memvid = open_or_create_memvid(path.as_path(), request.workspace_capsule)?;
    let options = index_put_options(&request);
    let payload = request.text.as_bytes().to_vec();
    match memvid.frame_by_uri(request.frame_uri.as_str()) {
        Ok(frame) if frame.status == FrameStatus::Active => {
            memvid
                .update_frame(frame.id, Some(payload), options, None)
                .map_err(classify_memvid_error)?;
        }
        _ => {
            memvid
                .put_bytes_with_options(payload.as_slice(), options)
                .map_err(classify_memvid_error)?;
        }
    }
    memvid.commit().map_err(classify_memvid_error)?;
    let frame = memvid
        .frame_by_uri(request.frame_uri.as_str())
        .map_err(classify_memvid_error)?;
    let stats = memvid.stats().map_err(classify_memvid_error)?;
    let stats = thread_episodic_stats_from_memvid(stats)?;

    Ok(ThreadEpisodicMemvidIndexOutput {
        frame_id: i64::try_from(frame.id).map_err(|_| {
            ThreadEpisodicMemvidError::non_retryable("thread episodic frame id does not fit i64")
        })?,
        frame_uri: request.frame_uri,
        stats,
    })
}

fn search_segment_blocking(
    path: PathBuf,
    request: SearchRequest,
    segment: ThreadEpisodicMemvidSearchSegment,
    workspace_id: &str,
    thread_id: &str,
) -> Result<Vec<ThreadEpisodicMemvidSearchHit>, ThreadEpisodicMemvidError> {
    let mut memvid = Memvid::open_read_only(path.as_path()).map_err(classify_memvid_error)?;
    let response = memvid.search(request).map_err(classify_memvid_error)?;
    let engine = response.engine;
    Ok(response
        .hits
        .into_iter()
        .filter_map(|hit| {
            search_hit_to_thread_episodic_hit(
                hit,
                engine.clone(),
                &segment,
                workspace_id,
                thread_id,
            )
        })
        .collect())
}

fn search_hit_to_thread_episodic_hit(
    hit: SearchHit,
    engine: SearchEngineKind,
    segment: &ThreadEpisodicMemvidSearchSegment,
    workspace_id: &str,
    thread_id: &str,
) -> Option<ThreadEpisodicMemvidSearchHit> {
    let metadata = hit.metadata.as_ref()?.extra_metadata.clone();
    let candidate_workspace_id = metadata
        .get("pioneer.thread_episodic.workspace_id")
        .cloned()
        .unwrap_or_else(|| workspace_id.to_owned());
    let candidate_thread_id = metadata
        .get("pioneer.thread_episodic.thread_id")
        .cloned()
        .unwrap_or_else(|| thread_id.to_owned());
    let index_item_id = metadata
        .get("pioneer.thread_episodic.index_item_id")
        .cloned()
        .or_else(|| hit.title.clone())
        .unwrap_or_else(|| hit.uri.clone());
    let source_context = metadata
        .get("pioneer.thread_episodic.source_context")
        .and_then(|value| serde_json::from_str::<ThreadEpisodicSourceContext>(value).ok())
        .unwrap_or(ThreadEpisodicSourceContext::Unknown);
    let visibility = metadata
        .get("pioneer.thread_episodic.visibility")
        .and_then(|value| parse_thread_episodic_visibility(value))
        .unwrap_or(ThreadEpisodicVisibility::UserVisible);
    let status = metadata
        .get("pioneer.thread_episodic.status")
        .and_then(|value| parse_thread_episodic_item_status(value))
        .unwrap_or(ThreadEpisodicItemStatus::Active);
    let score = hit.score;
    let lexical_score = match engine {
        SearchEngineKind::Tantivy | SearchEngineKind::LexFallback => score,
        SearchEngineKind::Hybrid => score,
    };
    Some(ThreadEpisodicMemvidSearchHit {
        workspace_id: candidate_workspace_id,
        thread_id: candidate_thread_id,
        turn_id: metadata
            .get("pioneer.thread_episodic.turn_id")
            .cloned()
            .unwrap_or_default(),
        item_id: metadata
            .get("pioneer.thread_episodic.item_id")
            .cloned()
            .unwrap_or_default(),
        index_item_id,
        source_actor_role: metadata
            .get("pioneer.thread_episodic.source_actor_role")
            .cloned()
            .unwrap_or_default(),
        source_runtime_kind: metadata
            .get("pioneer.thread_episodic.source_runtime_kind")
            .cloned()
            .unwrap_or_default(),
        source_context,
        visibility,
        status,
        segment_index: segment.segment_index,
        capsule_id: segment.capsule_id.clone(),
        capsule_ref: segment.capsule_ref.clone(),
        frame_id: hit.frame_id,
        frame_uri: hit.uri,
        text: hit.chunk_text.unwrap_or(hit.text),
        memvid_score: score,
        lexical_score,
        semantic_score: None,
        temporal_score: None,
        created_at_unix: parse_i64_metadata(&metadata, "pioneer.thread_episodic.created_at_unix"),
        metadata,
    })
}

fn memvid_thread_episodic_search_request(
    profile: &ThreadEpisodicSearchProfile,
    query: &str,
    scope: Option<&str>,
) -> SearchRequest {
    SearchRequest {
        query: query.to_owned(),
        top_k: profile.max_candidates as usize,
        snippet_chars: profile.snippet_chars as usize,
        uri: None,
        scope: scope.map(str::to_owned),
        cursor: None,
        temporal: profile.recent_start_unix.map(|start_utc| TemporalFilter {
            start_utc: Some(start_utc),
            end_utc: None,
            phrase: None,
            tz: None,
        }),
        as_of_frame: None,
        as_of_ts: None,
        no_sketch: true,
        acl_context: None,
        acl_enforcement_mode: AclEnforcementMode::default(),
    }
}

pub fn filter_thread_episodic_search_candidates(
    workspace_id: &str,
    thread_id: &str,
    hits: Vec<ThreadEpisodicMemvidSearchHit>,
) -> ThreadEpisodicFilteredSearchCandidates {
    let mut accepted = Vec::new();
    let mut suppressions = Vec::new();
    for hit in hits {
        let reason = if hit.workspace_id != workspace_id {
            Some(ThreadEpisodicCandidateSuppressionReason::WrongWorkspace)
        } else if hit.thread_id != thread_id {
            Some(ThreadEpisodicCandidateSuppressionReason::WrongThread)
        } else if hit.source_context.is_hidden_or_internal()
            || hit.visibility.is_hidden_or_internal()
        {
            Some(ThreadEpisodicCandidateSuppressionReason::HiddenOrInternal)
        } else {
            match hit.status {
                ThreadEpisodicItemStatus::Deleted => {
                    Some(ThreadEpisodicCandidateSuppressionReason::Deleted)
                }
                ThreadEpisodicItemStatus::Excluded => {
                    Some(ThreadEpisodicCandidateSuppressionReason::Excluded)
                }
                ThreadEpisodicItemStatus::IndexFailed => {
                    Some(ThreadEpisodicCandidateSuppressionReason::IndexFailed)
                }
                ThreadEpisodicItemStatus::PendingIndex | ThreadEpisodicItemStatus::Unknown => {
                    Some(ThreadEpisodicCandidateSuppressionReason::PendingIndex)
                }
                ThreadEpisodicItemStatus::Indexed | ThreadEpisodicItemStatus::Active => None,
            }
        };
        if let Some(reason) = reason {
            suppressions.push(ThreadEpisodicCandidateSuppression {
                index_item_id: hit.index_item_id,
                reason,
            });
        } else {
            accepted.push(hit);
        }
    }
    ThreadEpisodicFilteredSearchCandidates {
        hits: accepted,
        suppressions,
    }
}

pub fn rank_thread_episodic_search_hits(
    hits: Vec<ThreadEpisodicMemvidSearchHit>,
    context: ThreadEpisodicRankingContext,
) -> Vec<ThreadEpisodicRankedSearchHit> {
    let mut ranked = hits
        .into_iter()
        .map(|hit| {
            let memvid_score = hit.memvid_score;
            let exact_source_boost = exact_source_boost(&hit, context.exact_source.as_ref());
            let recency_boost = recency_boost(&hit, context.now_unix);
            let source_role_boost = source_role_boost(hit.source_actor_role.as_str());
            let final_score = score_or_zero(memvid_score)
                + exact_source_boost.unwrap_or(0.0)
                + recency_boost.unwrap_or(0.0)
                + source_role_boost.unwrap_or(0.0);
            let score_breakdown = ThreadEpisodicScoreBreakdown {
                final_score,
                memvid_score,
                semantic_score: hit.semantic_score,
                lexical_score: hit.lexical_score,
                temporal_score: hit.temporal_score,
                exact_source_boost,
                recency_boost,
                source_role_boost,
            };
            ThreadEpisodicRankedSearchHit {
                hit,
                score_breakdown,
            }
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .score_breakdown
            .final_score
            .partial_cmp(&left.score_breakdown.final_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.hit.segment_index.cmp(&left.hit.segment_index))
            .then_with(|| left.hit.index_item_id.cmp(&right.hit.index_item_id))
    });
    ranked
}

fn index_put_options(request: &ThreadEpisodicMemvidIndexRequest) -> PutOptions {
    let mut builder = PutOptions::builder()
        .uri(request.frame_uri.as_str())
        .title(request.index_item_id.as_str())
        .track(THREAD_EPISODIC_TRACK)
        .kind(THREAD_EPISODIC_KIND)
        .tag(
            "pioneer.thread_episodic.schema_version",
            THREAD_EPISODIC_SCHEMA_VERSION,
        )
        .tag(
            "pioneer.thread_episodic.capsule_id",
            request.capsule_id.as_str(),
        )
        .tag(
            "pioneer.thread_episodic.capsule_ref",
            request.capsule_ref.as_str(),
        )
        .tag(
            "pioneer.thread_episodic.index_item_id",
            request.index_item_id.as_str(),
        )
        .metadata(DocMetadata {
            mime: Some("text/plain; charset=utf-8".to_owned()),
            ..DocMetadata::default()
        })
        .search_text(request.text.as_str())
        .auto_tag(false)
        .extract_dates(false)
        .extract_triplets(false)
        .instant_index(false)
        .extraction_budget_ms(0);

    for (key, value) in &request.metadata {
        builder = builder.tag(key.as_str(), value.as_str());
    }

    builder.build()
}

fn absolute_cutoff(scores: &[f32], min_relevancy: f32) -> usize {
    first_below(scores, min_relevancy).unwrap_or(scores.len())
}

fn relative_cutoff(scores: &[f32], min_relevancy: f32, min_ratio: f32) -> usize {
    let Some(top_score) = scores.first().copied() else {
        return 0;
    };
    let threshold = min_relevancy.max(top_score * min_ratio.clamp(0.0, 1.0));
    first_below(scores, threshold).unwrap_or(scores.len())
}

fn cliff_cutoff(scores: &[f32], min_relevancy: f32, max_drop_ratio: f32) -> usize {
    if let Some(index) = first_below(scores, min_relevancy) {
        return index;
    }
    for index in 1..scores.len() {
        let previous = scores[index - 1];
        if previous <= f32::EPSILON {
            continue;
        }
        let drop_ratio = (previous - scores[index]) / previous;
        if drop_ratio > max_drop_ratio.clamp(0.0, 1.0) {
            return index;
        }
    }
    scores.len()
}

fn elbow_cutoff(scores: &[f32], min_relevancy: f32, sensitivity: f32) -> usize {
    if let Some(index) = first_below(scores, min_relevancy) {
        return index;
    }
    if scores.len() <= 2 {
        return scores.len();
    }
    let first = scores[0];
    let last = *scores.last().unwrap_or(&first);
    let span = (scores.len() - 1) as f32;
    let mut best_index = 0usize;
    let mut best_distance = 0.0f32;
    for (index, score) in scores.iter().enumerate().skip(1).take(scores.len() - 2) {
        let expected = first + (last - first) * (index as f32 / span);
        let distance = (expected - *score).abs();
        if distance > best_distance {
            best_distance = distance;
            best_index = index;
        }
    }
    let minimum_distance = 0.05 / sensitivity.max(0.1);
    if best_distance >= minimum_distance {
        best_index + 1
    } else {
        scores.len()
    }
}

fn combined_cutoff(scores: &[f32], config: PioneerAdaptiveCutoffConfig) -> usize {
    [
        absolute_cutoff(scores, config.min_relevancy),
        relative_cutoff(scores, config.min_relevancy, config.relative_min_ratio),
        cliff_cutoff(scores, config.min_relevancy, config.cliff_drop_ratio),
        elbow_cutoff(scores, config.min_relevancy, config.elbow_sensitivity),
    ]
    .into_iter()
    .filter(|cutoff| *cutoff > 0)
    .min()
    .unwrap_or(scores.len())
}

fn first_below(scores: &[f32], threshold: f32) -> Option<usize> {
    scores.iter().position(|score| *score < threshold)
}

fn score_or_zero(score: Option<f32>) -> f32 {
    score.unwrap_or(0.0)
}

fn parse_i64_metadata(metadata: &BTreeMap<String, String>, key: &str) -> Option<i64> {
    metadata.get(key)?.parse::<i64>().ok()
}

fn parse_thread_episodic_visibility(value: &str) -> Option<ThreadEpisodicVisibility> {
    match value {
        "user_visible" => Some(ThreadEpisodicVisibility::UserVisible),
        "hidden" => Some(ThreadEpisodicVisibility::Hidden),
        "internal" | "internal_hidden" => Some(ThreadEpisodicVisibility::Internal),
        _ => None,
    }
}

fn parse_thread_episodic_item_status(value: &str) -> Option<ThreadEpisodicItemStatus> {
    match value {
        "indexed" => Some(ThreadEpisodicItemStatus::Indexed),
        "active" => Some(ThreadEpisodicItemStatus::Active),
        "pending_index" => Some(ThreadEpisodicItemStatus::PendingIndex),
        "deleted" => Some(ThreadEpisodicItemStatus::Deleted),
        "excluded" => Some(ThreadEpisodicItemStatus::Excluded),
        "index_failed" | "failed" => Some(ThreadEpisodicItemStatus::IndexFailed),
        _ => None,
    }
}

fn exact_source_boost(
    hit: &ThreadEpisodicMemvidSearchHit,
    exact_source: Option<&ThreadEpisodicExactSourceTarget>,
) -> Option<f32> {
    let exact_source = exact_source?;
    if exact_source
        .index_item_id
        .as_deref()
        .is_some_and(|index_item_id| index_item_id == hit.index_item_id)
    {
        return Some(2.0);
    }
    if exact_source
        .item_id
        .as_deref()
        .is_some_and(|item_id| item_id == hit.item_id)
    {
        return Some(1.0);
    }
    if exact_source
        .turn_id
        .as_deref()
        .is_some_and(|turn_id| turn_id == hit.turn_id)
    {
        return Some(0.5);
    }
    None
}

fn recency_boost(hit: &ThreadEpisodicMemvidSearchHit, now_unix: Option<i64>) -> Option<f32> {
    let (Some(created_at), Some(now_unix)) = (hit.created_at_unix, now_unix) else {
        return None;
    };
    let age_seconds = now_unix.saturating_sub(created_at).max(0) as f32;
    let day = 86_400.0;
    let boost = if age_seconds <= day {
        0.30
    } else if age_seconds <= 7.0 * day {
        0.20
    } else if age_seconds <= 30.0 * day {
        0.10
    } else {
        0.0
    };
    (boost > 0.0).then_some(boost)
}

fn source_role_boost(source_actor_role: &str) -> Option<f32> {
    match source_actor_role {
        "user" => Some(0.08),
        "assistant" => Some(0.05),
        "task" | "task_summary" => Some(0.03),
        "system_visible" | "generated_summary" => Some(0.04),
        _ => None,
    }
}

fn open_or_create_memvid(
    path: &Path,
    _workspace_capsule: bool,
) -> Result<Memvid, ThreadEpisodicMemvidError> {
    if path.exists() {
        Memvid::open(path).map_err(classify_memvid_error)
    } else {
        Memvid::create(path).map_err(classify_memvid_error)
    }
}

fn thread_episodic_stats_from_memvid(
    stats: memvid_core::Stats,
) -> Result<ThreadEpisodicMemvidStats, ThreadEpisodicMemvidError> {
    let capacity_bytes = optional_i64_from_unlimited_capacity(stats.capacity_bytes)?;
    let remaining_capacity_bytes = optional_i64_from_capacity_bound_stat(
        stats.remaining_capacity_bytes,
        stats.capacity_bytes,
    )?;
    Ok(ThreadEpisodicMemvidStats {
        active_frame_count: Some(i64_from_u64(stats.active_frame_count)?),
        frame_count: Some(i64_from_u64(stats.frame_count)?),
        size_bytes: Some(i64_from_u64(stats.size_bytes)?),
        capacity_bytes,
        remaining_capacity_bytes,
        utilization_percent: Some(stats.storage_utilisation_percent),
    })
}

fn classify_memvid_error(error: MemvidError) -> ThreadEpisodicMemvidError {
    match error {
        MemvidError::CapacityExceeded { .. } => {
            ThreadEpisodicMemvidError::capacity_exceeded(error.to_string())
        }
        MemvidError::Lock(_)
        | MemvidError::Locked(_)
        | MemvidError::CheckpointFailed { .. }
        | MemvidError::Io { .. } => ThreadEpisodicMemvidError::retryable(error.to_string()),
        MemvidError::InvalidHeader { .. }
        | MemvidError::InvalidToc { .. }
        | MemvidError::InvalidTimeIndex { .. }
        | MemvidError::InvalidSketchTrack { .. }
        | MemvidError::InvalidLogicMesh { .. }
        | MemvidError::LogicMeshNotEnabled
        | MemvidError::LexNotEnabled
        | MemvidError::VecNotEnabled
        | MemvidError::ClipNotEnabled
        | MemvidError::VecDimensionMismatch { .. }
        | MemvidError::InvalidTier
        | MemvidError::EncryptedFile { .. }
        | MemvidError::AuxiliaryFileDetected { .. }
        | MemvidError::RequiresSealed
        | MemvidError::RequiresOpen => ThreadEpisodicMemvidError::non_retryable(error.to_string()),
        _ => ThreadEpisodicMemvidError::retryable(error.to_string()),
    }
}

fn i64_from_u64(value: u64) -> Result<i64, ThreadEpisodicMemvidError> {
    i64::try_from(value)
        .map_err(|_| ThreadEpisodicMemvidError::non_retryable("memvid stat does not fit i64"))
}

fn optional_i64_from_unlimited_capacity(
    value: u64,
) -> Result<Option<i64>, ThreadEpisodicMemvidError> {
    if value == u64::MAX {
        Ok(None)
    } else {
        i64_from_u64(value).map(Some)
    }
}

fn optional_i64_from_capacity_bound_stat(
    value: u64,
    capacity_bytes: u64,
) -> Result<Option<i64>, ThreadEpisodicMemvidError> {
    if capacity_bytes == u64::MAX {
        Ok(None)
    } else {
        i64_from_u64(value).map(Some)
    }
}

fn path_from_storage_uri(storage_uri: &str) -> Result<PathBuf, ThreadEpisodicMemvidError> {
    let path = storage_uri.strip_prefix("file://").ok_or_else(|| {
        ThreadEpisodicMemvidError::non_retryable(format!(
            "unsupported thread episodic memvid storage URI `{storage_uri}`"
        ))
    })?;
    Ok(PathBuf::from(path))
}

pub fn thread_episodic_storage_uri_from_path(path: &Path) -> String {
    format!("file://{}", path.display())
}

pub fn thread_episodic_memvid_metadata(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    source_actor_role: &str,
    source_runtime_kind: &str,
    source_context: &str,
    text_hash: &str,
    source_text_hash: &str,
) -> BTreeMap<String, String> {
    [
        ("pioneer.thread_episodic.workspace_id", workspace_id),
        ("pioneer.thread_episodic.thread_id", thread_id),
        ("pioneer.thread_episodic.turn_id", turn_id),
        ("pioneer.thread_episodic.item_id", item_id),
        (
            "pioneer.thread_episodic.source_actor_role",
            source_actor_role,
        ),
        (
            "pioneer.thread_episodic.source_runtime_kind",
            source_runtime_kind,
        ),
        ("pioneer.thread_episodic.source_context", source_context),
        ("pioneer.thread_episodic.visibility", "user_visible"),
        ("pioneer.thread_episodic.status", "active"),
        ("pioneer.thread_episodic.text_hash", text_hash),
        ("pioneer.thread_episodic.source_text_hash", source_text_hash),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeThreadEpisodicMemvidBackend {
        capabilities: ThreadEpisodicMemvidBackendCapabilities,
    }

    #[async_trait]
    impl ThreadEpisodicMemvidBackend for FakeThreadEpisodicMemvidBackend {
        fn capabilities(&self) -> ThreadEpisodicMemvidBackendCapabilities {
            self.capabilities.clone()
        }

        async fn index_item(
            &self,
            request: ThreadEpisodicMemvidIndexRequest,
        ) -> Result<ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidError> {
            Ok(ThreadEpisodicMemvidIndexOutput {
                frame_id: 7,
                frame_uri: request.frame_uri,
                stats: ThreadEpisodicMemvidStats::default(),
            })
        }

        async fn search(
            &self,
            _request: ThreadEpisodicMemvidSearchRequest,
        ) -> Result<ThreadEpisodicMemvidSearchOutput, ThreadEpisodicMemvidError> {
            Ok(ThreadEpisodicMemvidSearchOutput {
                hits: Vec::new(),
                diagnostics: ThreadEpisodicSearchDiagnostics {
                    profile_kind: ThreadEpisodicSearchProfileKind::DefaultContext,
                    search_mode: ThreadEpisodicSearchMode::Auto,
                    adaptive: PioneerAdaptiveCutoffDiagnostics {
                        strategy: ThreadEpisodicAdaptiveStrategy::Combined,
                        min_relevancy: 0.25,
                        cutoff_score: None,
                        cutoff_reason: PioneerAdaptiveCutoffReason::NoCandidates,
                        candidate_count: 0,
                        result_count: 0,
                    },
                    searched_segment_ids: Vec::new(),
                    searched_segment_count: 0,
                    unavailable_segment_ids: Vec::new(),
                    raw_candidate_count: 0,
                    filtered_candidate_count: 0,
                    returned_count: 0,
                    native_memvid_adaptive_used: false,
                    suppressions: Vec::new(),
                    warnings: Vec::new(),
                },
            })
        }
    }

    #[tokio::test]
    async fn fake_backend_uses_typed_capabilities() {
        let backend = FakeThreadEpisodicMemvidBackend {
            capabilities: ThreadEpisodicMemvidBackendCapabilities::memvid_default(),
        };

        let capabilities = backend.capabilities();
        assert_eq!(
            capabilities.adaptive_retrieval,
            ThreadEpisodicMemvidCapabilityState::Supported
        );
        assert_eq!(
            capabilities.adaptive_retrieval_implementation,
            ThreadEpisodicAdaptiveRetrievalImplementation::PioneerFallback
        );
        assert_eq!(
            capabilities.semantic_search,
            ThreadEpisodicMemvidCapabilityState::Unsupported
        );
        assert_eq!(
            capabilities.lexical_search,
            ThreadEpisodicMemvidCapabilityState::Supported
        );
        assert_eq!(
            capabilities.temporal_search,
            ThreadEpisodicMemvidCapabilityState::Supported
        );
        assert_eq!(
            capabilities.graph_search,
            ThreadEpisodicMemvidCapabilityState::Disabled
        );
    }

    #[test]
    fn capabilities_serialize_as_typed_states() {
        let json = serde_json::to_value(ThreadEpisodicMemvidBackendCapabilities::memvid_default())
            .expect("capabilities should serialize");

        assert_eq!(json["adaptive_retrieval"], "supported");
        assert_eq!(
            json["adaptive_retrieval_implementation"],
            "pioneer_fallback"
        );
        assert_eq!(json["semantic_search"], "unsupported");
        assert_eq!(json["lexical_search"], "supported");
        assert_eq!(json["temporal_search"], "supported");
        assert_eq!(json["graph_search"], "disabled");
    }

    #[test]
    fn feature_audit_records_current_memvid_feature_set() {
        let audit = ThreadEpisodicMemvidFeatureAudit::current();
        assert_eq!(audit.memvid_core_version, "2.0.139");
        assert!(audit.enabled_features.contains(&"lex".to_owned()));
        assert!(
            audit
                .enabled_features
                .contains(&"temporal_track".to_owned())
        );
        assert!(audit.disabled_features.contains(&"vec".to_owned()));
        assert_eq!(
            audit.adaptive_retrieval_implementation,
            ThreadEpisodicAdaptiveRetrievalImplementation::PioneerFallback
        );
        assert!(audit.graph_search_note.contains("not enabled"));
    }

    #[test]
    fn adaptive_absolute_cuts_low_score_tail() {
        let (cutoff, diagnostics) = PioneerAdaptiveCutoff::cutoff_len(
            &[Some(0.95), Some(0.82), Some(0.40), Some(0.10)],
            PioneerAdaptiveCutoffConfig {
                strategy: ThreadEpisodicAdaptiveStrategy::Absolute,
                min_relevancy: 0.50,
                max_candidates: 10,
                min_results: 1,
                relative_min_ratio: 0.5,
                cliff_drop_ratio: 0.4,
                elbow_sensitivity: 1.0,
            },
        );
        assert_eq!(cutoff, 2);
        assert_eq!(
            diagnostics.cutoff_reason,
            PioneerAdaptiveCutoffReason::AbsoluteThreshold
        );
        assert_eq!(diagnostics.result_count, 2);
    }

    #[test]
    fn adaptive_relative_retains_high_relevance_cluster() {
        let (cutoff, diagnostics) = PioneerAdaptiveCutoff::cutoff_len(
            &[Some(1.0), Some(0.91), Some(0.87), Some(0.35)],
            PioneerAdaptiveCutoffConfig {
                strategy: ThreadEpisodicAdaptiveStrategy::Relative,
                min_relevancy: 0.20,
                max_candidates: 10,
                min_results: 1,
                relative_min_ratio: 0.80,
                cliff_drop_ratio: 0.4,
                elbow_sensitivity: 1.0,
            },
        );
        assert_eq!(cutoff, 3);
        assert_eq!(
            diagnostics.cutoff_reason,
            PioneerAdaptiveCutoffReason::RelativeThreshold
        );
    }

    #[test]
    fn adaptive_cliff_and_elbow_are_deterministic() {
        let config = PioneerAdaptiveCutoffConfig {
            strategy: ThreadEpisodicAdaptiveStrategy::Cliff,
            min_relevancy: 0.10,
            max_candidates: 10,
            min_results: 1,
            relative_min_ratio: 0.5,
            cliff_drop_ratio: 0.30,
            elbow_sensitivity: 1.0,
        };
        let (cliff_cutoff, cliff_diagnostics) = PioneerAdaptiveCutoff::cutoff_len(
            &[Some(0.96), Some(0.92), Some(0.88), Some(0.42)],
            config,
        );
        assert_eq!(cliff_cutoff, 3);
        assert_eq!(
            cliff_diagnostics.cutoff_reason,
            PioneerAdaptiveCutoffReason::ScoreCliff
        );

        let (elbow_cutoff, elbow_diagnostics) = PioneerAdaptiveCutoff::cutoff_len(
            &[Some(1.0), Some(0.88), Some(0.60), Some(0.58), Some(0.57)],
            PioneerAdaptiveCutoffConfig {
                strategy: ThreadEpisodicAdaptiveStrategy::Elbow,
                ..config
            },
        );
        assert!(elbow_cutoff > 1);
        assert_eq!(
            elbow_diagnostics.cutoff_reason,
            PioneerAdaptiveCutoffReason::Elbow
        );
    }

    #[test]
    fn search_profiles_are_typed_and_validated() {
        let default_profile =
            ThreadEpisodicSearchProfile::for_kind(ThreadEpisodicSearchProfileKind::DefaultContext);
        assert_eq!(default_profile.mode, ThreadEpisodicSearchMode::Auto);
        assert_eq!(
            default_profile.adaptive_strategy,
            ThreadEpisodicAdaptiveStrategy::Combined
        );
        default_profile.validate().expect("default profile");

        let exact =
            ThreadEpisodicSearchProfile::for_kind(ThreadEpisodicSearchProfileKind::ExactReference);
        assert_eq!(exact.mode, ThreadEpisodicSearchMode::Lexical);
        assert_eq!(
            exact.adaptive_strategy,
            ThreadEpisodicAdaptiveStrategy::Absolute
        );

        let mut invalid = exact.clone();
        invalid.max_candidates = 0;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn recent_profile_builds_temporal_search_request() {
        let mut profile =
            ThreadEpisodicSearchProfile::for_kind(ThreadEpisodicSearchProfileKind::RecentContext);
        profile.recent_start_unix = Some(1_700_000_000);
        let request =
            memvid_thread_episodic_search_request(&profile, "architecture decision", None);
        assert_eq!(request.query, "architecture decision");
        assert_eq!(request.top_k, profile.max_candidates as usize);
        assert_eq!(
            request
                .temporal
                .as_ref()
                .and_then(|filter| filter.start_utc),
            Some(1_700_000_000)
        );
    }

    #[test]
    fn search_request_preserves_optional_scope() {
        let profile =
            ThreadEpisodicSearchProfile::for_kind(ThreadEpisodicSearchProfileKind::DefaultContext);
        let scope = "mv2://workspace/workspace_a/thread/thread_a/";

        let scoped = memvid_thread_episodic_search_request(&profile, "memory limit", Some(scope));
        assert_eq!(scoped.scope.as_deref(), Some(scope));

        let unscoped = memvid_thread_episodic_search_request(&profile, "memory limit", None);
        assert_eq!(unscoped.scope, None);
    }

    #[test]
    fn hard_filter_suppresses_hidden_deleted_excluded_and_wrong_scope() {
        let mut hits = vec![
            test_hit("workspace_a", "thread_a", "active"),
            test_hit("workspace_b", "thread_a", "wrong_workspace"),
            test_hit("workspace_a", "thread_b", "wrong_thread"),
            test_hit("workspace_a", "thread_a", "deleted"),
            test_hit("workspace_a", "thread_a", "excluded"),
            test_hit("workspace_a", "thread_a", "hidden"),
        ];
        hits[3].status = ThreadEpisodicItemStatus::Deleted;
        hits[4].status = ThreadEpisodicItemStatus::Excluded;
        hits[5].visibility = ThreadEpisodicVisibility::Internal;

        let filtered = filter_thread_episodic_search_candidates("workspace_a", "thread_a", hits);

        assert_eq!(filtered.hits.len(), 1);
        assert_eq!(filtered.hits[0].index_item_id, "active");
        assert_eq!(filtered.suppressions.len(), 5);
        assert!(filtered.suppressions.iter().any(|suppression| {
            suppression.reason == ThreadEpisodicCandidateSuppressionReason::WrongWorkspace
        }));
        assert!(filtered.suppressions.iter().any(|suppression| {
            suppression.reason == ThreadEpisodicCandidateSuppressionReason::Deleted
        }));
    }

    #[test]
    fn ranking_returns_explainable_score_breakdown() {
        let mut generic = test_hit("workspace_a", "thread_a", "generic");
        generic.memvid_score = Some(0.70);
        generic.created_at_unix = Some(100);
        let mut exact = test_hit("workspace_a", "thread_a", "exact");
        exact.memvid_score = Some(0.30);
        exact.turn_id = "turn_1".to_owned();
        exact.item_id = "item_1".to_owned();
        exact.created_at_unix = Some(1_000);

        let ranked = rank_thread_episodic_search_hits(
            vec![generic, exact],
            ThreadEpisodicRankingContext {
                exact_source: Some(ThreadEpisodicExactSourceTarget {
                    turn_id: Some("turn_1".to_owned()),
                    item_id: Some("item_1".to_owned()),
                    index_item_id: None,
                }),
                now_unix: Some(1_000),
            },
        );

        assert_eq!(ranked[0].hit.index_item_id, "exact");
        assert_eq!(ranked[0].score_breakdown.exact_source_boost, Some(1.0));
        assert_eq!(ranked[0].score_breakdown.recency_boost, Some(0.30));
        assert_eq!(ranked[0].score_breakdown.source_role_boost, Some(0.08));
    }

    #[test]
    fn rejects_non_file_storage_uri() {
        let error = path_from_storage_uri("s3://bucket/capsule.mv2").expect_err("must reject");
        assert_eq!(error.kind, ThreadEpisodicMemvidFailureKind::NonRetryable);
    }

    #[test]
    fn workspace_index_creates_capacity_bounded_capsule() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("segment-000001.mv2");
        let frame_uri =
            "mv2://workspace/workspace_1/thread/thread_1/turn/turn_1/item/item_1/index/index_1";

        let output = index_item_blocking(
            path.clone(),
            ThreadEpisodicMemvidIndexRequest {
                storage_uri: thread_episodic_storage_uri_from_path(path.as_path()),
                capsule_id: "capsule_1".to_owned(),
                capsule_ref:
                    "mv2://pioneer/thread_episodic/workspace/workspace_hash/segments/000001/capsules/capsule_1".to_owned(),
                workspace_capsule: true,
                index_item_id: "index_item_1".to_owned(),
                frame_uri: frame_uri.to_owned(),
                text: "workspace memory item".to_owned(),
                metadata: BTreeMap::new(),
            },
        )
        .expect("workspace item should index");

        assert_eq!(output.frame_uri, frame_uri);
        assert_eq!(output.stats.capacity_bytes, Some(50 * 1024 * 1024));
        assert!(output.stats.remaining_capacity_bytes.is_some());
    }

    fn test_hit(
        workspace_id: &str,
        thread_id: &str,
        index_item_id: &str,
    ) -> ThreadEpisodicMemvidSearchHit {
        ThreadEpisodicMemvidSearchHit {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: "turn".to_owned(),
            item_id: "item".to_owned(),
            index_item_id: index_item_id.to_owned(),
            source_actor_role: "user".to_owned(),
            source_runtime_kind: "user_turn".to_owned(),
            source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
            visibility: ThreadEpisodicVisibility::UserVisible,
            status: ThreadEpisodicItemStatus::Active,
            segment_index: 1,
            capsule_id: "capsule".to_owned(),
            capsule_ref: "capsule_ref".to_owned(),
            frame_id: 1,
            frame_uri: format!("mv2://frame/{index_item_id}"),
            text: "text".to_owned(),
            memvid_score: Some(0.5),
            lexical_score: Some(0.5),
            semantic_score: None,
            temporal_score: None,
            created_at_unix: None,
            metadata: BTreeMap::new(),
        }
    }
}
