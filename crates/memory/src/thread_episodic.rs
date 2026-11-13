use crate::thread_episodic_embedding::{
    ThreadEpisodicEmbeddingIdentity, ThreadEpisodicEmbeddingProvider,
};
use async_trait::async_trait;
use pioneer_protocol::{
    ThreadEpisodicAdaptiveStrategy, ThreadEpisodicItemStatus, ThreadEpisodicScoreBreakdown,
    ThreadEpisodicSearchMode, ThreadEpisodicSourceContext, ThreadEpisodicVisibility,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, OnceLock, Weak};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicRetrievalMode {
    Hybrid,
    Semantic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicSearchFailureKind {
    Retryable,
    NonRetryable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadEpisodicSearchError {
    pub kind: ThreadEpisodicSearchFailureKind,
    pub message: String,
}

impl ThreadEpisodicSearchError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            kind: ThreadEpisodicSearchFailureKind::Retryable,
            message: message.into(),
        }
    }

    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            kind: ThreadEpisodicSearchFailureKind::NonRetryable,
            message: message.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self.kind, ThreadEpisodicSearchFailureKind::Retryable)
    }
}

impl Display for ThreadEpisodicSearchError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl Error for ThreadEpisodicSearchError {}

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadEpisodicIndexEmbedding {
    pub identity: ThreadEpisodicEmbeddingIdentity,
    pub vector: Vec<f32>,
}

impl ThreadEpisodicIndexEmbedding {
    pub fn new(
        identity: ThreadEpisodicEmbeddingIdentity,
        vector: Vec<f32>,
    ) -> Result<Self, ThreadEpisodicSearchError> {
        if vector.len() != identity.dimension {
            return Err(ThreadEpisodicSearchError::non_retryable(format!(
                "thread episodic embedding dimension mismatch for `{}`/`{}`: expected {}, got {}",
                identity.provider_id,
                identity.model,
                identity.dimension,
                vector.len()
            )));
        }
        if identity.dimension == 0
            || vector.iter().any(|value| !value.is_finite())
            || !vector.iter().any(|value| *value != 0.0)
        {
            return Err(ThreadEpisodicSearchError::non_retryable(
                "invalid embedding vector",
            ));
        }

        Ok(Self { identity, vector })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadEpisodicIndexDocument {
    pub workspace_id: String,
    pub index_item_id: String,
    pub text: String,
    pub metadata: BTreeMap<String, String>,
    pub embedding: Option<ThreadEpisodicIndexEmbedding>,
}

/// Limits for prepared documents retained by one existing executor dispatch.
pub const THREAD_EPISODIC_INDEX_BATCH_ITEMS: usize = 8;
pub const THREAD_EPISODIC_INDEX_BATCH_BYTES: usize = 256 * 1024;

impl ThreadEpisodicIndexDocument {
    pub fn prepared_bytes(&self) -> usize {
        let strings = [&self.workspace_id, &self.index_item_id, &self.text];
        let mut bytes = strings
            .into_iter()
            .fold(0usize, |n, value| n.saturating_add(value.len()));
        for (key, value) in &self.metadata {
            bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
        }
        if let Some(embedding) = &self.embedding {
            bytes = bytes
                .saturating_add(
                    embedding
                        .vector
                        .len()
                        .saturating_mul(std::mem::size_of::<f32>()),
                )
                .saturating_add(embedding.identity.provider_id.len())
                .saturating_add(embedding.identity.model.len());
        }
        bytes
    }

    pub fn batch_compatible(&self, other: &Self) -> bool {
        self.workspace_id == other.workspace_id
            && self.embedding.as_ref().map(|value| &value.identity)
                == other.embedding.as_ref().map(|value| &value.identity)
            && self.index_item_id != other.index_item_id
    }

    pub fn validate_prepared(&self) -> Result<(), ThreadEpisodicSearchError> {
        if let Some(embedding) = &self.embedding {
            validate_index_embedding(embedding)?;
        }
        Ok(())
    }
}

pub type ThreadEpisodicValidationResult = ();

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
                snippet_chars: 360,
                recent_start_unix: None,
            },
            ThreadEpisodicSearchProfileKind::ExactReference => Self {
                kind,
                mode: ThreadEpisodicSearchMode::Lexical,
                adaptive_strategy: ThreadEpisodicAdaptiveStrategy::Absolute,
                min_relevancy: 0.25,
                max_candidates: 16,
                min_results: 1,
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
                snippet_chars: 360,
                recent_start_unix: None,
            },
        }
    }

    pub fn validate(&self) -> Result<(), ThreadEpisodicSearchError> {
        if !(0.0..=1.0).contains(&self.min_relevancy) {
            return Err(ThreadEpisodicSearchError::non_retryable(format!(
                "thread episodic min_relevancy {} must be in 0..=1",
                self.min_relevancy
            )));
        }
        if self.max_candidates == 0 || self.max_candidates > 256 {
            return Err(ThreadEpisodicSearchError::non_retryable(format!(
                "thread episodic max_candidates {} must be in 1..=256",
                self.max_candidates
            )));
        }
        if self.min_results > self.max_candidates {
            return Err(ThreadEpisodicSearchError::non_retryable(
                "thread episodic min_results cannot exceed max_candidates",
            ));
        }
        if self.snippet_chars == 0 || self.snippet_chars > 4_000 {
            return Err(ThreadEpisodicSearchError::non_retryable(format!(
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicSearchRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub query: String,
    pub profile: ThreadEpisodicSearchProfile,
    #[serde(default)]
    pub access: pioneer_crud::ThreadEpisodicSearchAccess,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicSearchHit {
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
    pub text: String,
    /// One-based positions within each eligible branch, independent of its
    /// engine's raw similarity/distance scale.
    pub lexical_rank: Option<i64>,
    pub semantic_rank: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at_unix: Option<i64>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl ThreadEpisodicSearchHit {
    /// Equal-weight RRF is product policy. The adapter returns only branch
    /// ranks; its storage query may push down this order to bound pagination.
    pub fn rank_score(&self) -> f32 {
        pioneer_crud::thread_search_rank_score(self.lexical_rank, self.semantic_rank)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicRankedSearchHit {
    pub hit: ThreadEpisodicSearchHit,
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
    pub hits: Vec<ThreadEpisodicSearchHit>,
    pub suppressions: Vec<ThreadEpisodicCandidateSuppression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicSearchDiagnostics {
    pub profile_kind: ThreadEpisodicSearchProfileKind,
    pub search_mode: ThreadEpisodicSearchMode,
    pub adaptive: PioneerAdaptiveCutoffDiagnostics,
    pub raw_candidate_count: u32,
    pub filtered_candidate_count: u32,
    pub returned_count: u32,
    #[serde(default)]
    pub suppressions: Vec<ThreadEpisodicCandidateSuppression>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadEpisodicSearchOutput {
    pub hits: Vec<ThreadEpisodicRankedSearchHit>,
    pub diagnostics: ThreadEpisodicSearchDiagnostics,
}

/// One bounded retrieval page. Eligibility is applied before candidate selection;
/// the caller revalidates current source access before disclosure.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadEpisodicSearchPage {
    pub hits: Vec<ThreadEpisodicSearchHit>,
    pub next_cursor: Option<String>,
    pub warnings: Vec<String>,
}

#[async_trait]
pub trait ThreadEpisodicSearchBackend: Send + Sync {
    async fn validate_document(
        &self,
        request: ThreadEpisodicIndexDocument,
    ) -> Result<ThreadEpisodicValidationResult, ThreadEpisodicSearchError> {
        request.validate_prepared()
    }

    async fn validate_document_with_workspace_ownership(
        &self,
        request: ThreadEpisodicIndexDocument,
        ownership: ThreadEpisodicWorkspaceOwnership,
    ) -> Result<ThreadEpisodicValidationResult, ThreadEpisodicSearchError> {
        let _ownership = ownership;
        self.validate_document(request).await
    }

    /// Stable per-request results. Implementations may retain the single-item
    /// API; an error for one request must not skip its already admitted peers.
    async fn validate_documents_with_workspace_ownership(
        &self,
        requests: Vec<ThreadEpisodicIndexDocument>,
        ownership: ThreadEpisodicWorkspaceOwnership,
    ) -> Vec<Result<ThreadEpisodicValidationResult, ThreadEpisodicSearchError>> {
        let mut outputs = Vec::with_capacity(requests.len());
        for request in requests {
            outputs.push(
                self.validate_document_with_workspace_ownership(request, ownership.clone())
                    .await,
            );
        }
        outputs
    }

    async fn search(
        &self,
        request: ThreadEpisodicSearchRequest,
    ) -> Result<ThreadEpisodicSearchOutput, ThreadEpisodicSearchError> {
        request.profile.validate()?;
        let page = self
            .search_page(
                request.clone(),
                request.profile.max_candidates as usize,
                None,
                None,
            )
            .await?;
        Ok(finalize_thread_episodic_search_output(
            request,
            page.hits,
            page.warnings,
        ))
    }

    /// Return a bounded ranked page within the caller's eligibility conditions.
    /// Cursors are internal; the product exposes only its own signed cursor.
    async fn search_page(
        &self,
        request: ThreadEpisodicSearchRequest,
        page_size: usize,
        cursor: Option<String>,
        embedding: Option<ThreadEpisodicIndexEmbedding>,
    ) -> Result<ThreadEpisodicSearchPage, ThreadEpisodicSearchError>;

    async fn ask_retrieval(
        &self,
        mut request: ThreadEpisodicSearchRequest,
        mode: ThreadEpisodicRetrievalMode,
        embedding_provider: Arc<dyn ThreadEpisodicEmbeddingProvider>,
    ) -> Result<ThreadEpisodicSearchOutput, ThreadEpisodicSearchError> {
        request.profile.validate()?;
        request.profile.mode = match mode {
            ThreadEpisodicRetrievalMode::Semantic => ThreadEpisodicSearchMode::Semantic,
            ThreadEpisodicRetrievalMode::Hybrid
                if request.profile.mode == ThreadEpisodicSearchMode::Temporal =>
            {
                ThreadEpisodicSearchMode::Temporal
            }
            ThreadEpisodicRetrievalMode::Hybrid => ThreadEpisodicSearchMode::Auto,
        };
        let identity = embedding_provider.identity();
        let query = request.query.clone();
        let prepared = async {
            let vector =
                tokio::task::spawn_blocking(move || embedding_provider.embed_query_checked(&query))
                    .await
                    .map_err(|_| {
                        ThreadEpisodicSearchError::retryable("embedding worker interrupted")
                    })?
                    .map_err(|error| {
                        if error.is_retryable() {
                            ThreadEpisodicSearchError::retryable(error.to_string())
                        } else {
                            ThreadEpisodicSearchError::non_retryable(error.to_string())
                        }
                    })?;
            ThreadEpisodicIndexEmbedding::new(identity, vector)
        }
        .await;
        let (embedding, warning) = match prepared {
            Ok(embedding) => (Some(embedding), None),
            Err(error) if mode == ThreadEpisodicRetrievalMode::Hybrid => {
                let warning = if error.is_retryable() {
                    "query_embedding_retryable_failure"
                } else {
                    "query_embedding_unavailable"
                };
                (None, Some(warning.to_owned()))
            }
            Err(error) => return Err(error),
        };
        let mut page = self
            .search_page(
                request.clone(),
                request.profile.max_candidates as usize,
                None,
                embedding,
            )
            .await?;
        if let Some(warning) = warning {
            page.warnings.push(warning);
        }
        Ok(finalize_thread_episodic_search_output(
            request,
            page.hits,
            page.warnings,
        ))
    }
}

// Shared by ordinary indexing and destructive projection replacement. The
// ownership fences publication against replacement until the commit settles.
pub type ThreadEpisodicWorkspaceOwnership = Arc<ThreadEpisodicWorkspaceGuard>;

pub struct ThreadEpisodicWorkspaceGuard {
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

fn workspace_releases() -> &'static tokio::sync::watch::Sender<()> {
    static RELEASES: OnceLock<tokio::sync::watch::Sender<()>> = OnceLock::new();
    RELEASES.get_or_init(|| tokio::sync::watch::channel(()).0)
}

// A single coalesced release signal covers all workspaces, including releases
// by attempts interrupted while a serialized writer commit was settling.
pub fn thread_episodic_workspace_releases() -> tokio::sync::watch::Receiver<()> {
    workspace_releases().subscribe()
}

impl Drop for ThreadEpisodicWorkspaceGuard {
    fn drop(&mut self) {
        drop(self.guard.take());
        workspace_releases().send_replace(());
    }
}
type WorkspaceLockRegistry = Mutex<BTreeMap<String, Weak<Mutex<()>>>>;

async fn workspace_lock(workspace_id: &str) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<WorkspaceLockRegistry> = OnceLock::new();
    let mut locks = LOCKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .await;
    if let Some(lock) = locks.get(workspace_id).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(workspace_id.to_owned(), Arc::downgrade(&lock));
    lock
}

pub async fn lock_thread_episodic_workspace(
    workspace_id: &str,
) -> ThreadEpisodicWorkspaceOwnership {
    Arc::new(ThreadEpisodicWorkspaceGuard {
        guard: Some(workspace_lock(workspace_id).await.lock_owned().await),
    })
}

pub async fn try_lock_thread_episodic_workspace(
    workspace_id: &str,
) -> Option<ThreadEpisodicWorkspaceOwnership> {
    workspace_lock(workspace_id)
        .await
        .try_lock_owned()
        .ok()
        .map(|guard| Arc::new(ThreadEpisodicWorkspaceGuard { guard: Some(guard) }))
}

mod sqlite;
pub use sqlite::SqliteThreadEpisodicBackend;

fn finalize_thread_episodic_search_output(
    request: ThreadEpisodicSearchRequest,
    mut raw_hits: Vec<ThreadEpisodicSearchHit>,
    warnings: Vec<String>,
) -> ThreadEpisodicSearchOutput {
    raw_hits.sort_by(|left, right| {
        right
            .rank_score()
            .partial_cmp(&left.rank_score())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.index_item_id.cmp(&right.index_item_id))
    });

    let filtered = filter_thread_episodic_search_candidates(
        request.workspace_id.as_str(),
        request.thread_id.as_str(),
        raw_hits,
    );
    let scores = filtered
        .hits
        .iter()
        .map(|hit| Some(hit.rank_score()))
        .collect::<Vec<_>>();
    let (cutoff_len, adaptive) =
        PioneerAdaptiveCutoff::cutoff_len(&scores, request.profile.adaptive_cutoff_config());
    let filtered = ThreadEpisodicFilteredSearchCandidates {
        hits: filtered.hits.into_iter().take(cutoff_len).collect(),
        suppressions: filtered.suppressions,
    };
    let mut ranked = rank_thread_episodic_search_hits(filtered.hits);
    ranked.truncate(request.profile.max_candidates as usize);

    let diagnostics = ThreadEpisodicSearchDiagnostics {
        profile_kind: request.profile.kind,
        search_mode: request.profile.mode,
        raw_candidate_count: adaptive.candidate_count,
        filtered_candidate_count: ranked.len() as u32,
        returned_count: ranked.len() as u32,
        adaptive,
        suppressions: filtered.suppressions,
        warnings,
    };

    ThreadEpisodicSearchOutput {
        hits: ranked,
        diagnostics,
    }
}

fn validate_index_embedding(
    embedding: &ThreadEpisodicIndexEmbedding,
) -> Result<(), ThreadEpisodicSearchError> {
    if embedding.identity.dimension > 0
        && embedding.vector.len() == embedding.identity.dimension
        && embedding.vector.iter().all(|value| value.is_finite())
        && embedding.vector.iter().any(|value| *value != 0.0)
    {
        return Ok(());
    }

    Err(ThreadEpisodicSearchError::non_retryable(format!(
        "thread episodic embedding dimension mismatch for `{}`/`{}`: expected {}, got {}",
        embedding.identity.provider_id,
        embedding.identity.model,
        embedding.identity.dimension,
        embedding.vector.len()
    )))
}

pub fn filter_thread_episodic_search_candidates(
    workspace_id: &str,
    thread_id: &str,
    hits: Vec<ThreadEpisodicSearchHit>,
) -> ThreadEpisodicFilteredSearchCandidates {
    let mut accepted = Vec::new();
    let mut suppressions = Vec::new();
    for hit in hits {
        let reason = if hit.workspace_id != workspace_id {
            Some(ThreadEpisodicCandidateSuppressionReason::WrongWorkspace)
        } else if !thread_id.is_empty() && hit.thread_id != thread_id {
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
    hits: Vec<ThreadEpisodicSearchHit>,
) -> Vec<ThreadEpisodicRankedSearchHit> {
    let mut ranked = hits
        .into_iter()
        .map(|hit| {
            let final_score = hit.rank_score();
            let branch_score = |rank| pioneer_crud::thread_search_rank_score(rank, None) * 2.0;
            let score_breakdown = ThreadEpisodicScoreBreakdown {
                final_score,
                rank_score: Some(final_score),
                semantic_score: hit.semantic_rank.map(|rank| branch_score(Some(rank))),
                lexical_score: hit.lexical_rank.map(|rank| branch_score(Some(rank))),
                temporal_score: None,
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
            .then_with(|| left.hit.index_item_id.cmp(&right.hit.index_item_id))
    });
    ranked
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

pub fn thread_episodic_source_metadata(
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
    fn rank_scores_have_deterministic_ties_and_no_source_boosts() {
        let mut lower = test_hit("workspace", "thread", "a");
        lower.lexical_rank = Some(1);
        lower.semantic_rank = None;
        lower.source_actor_role = "user".into();
        lower.created_at_unix = Some(1_000);
        let mut higher = test_hit("workspace", "thread", "b");
        higher.lexical_rank = Some(1);
        higher.semantic_rank = Some(1);
        higher.created_at_unix = Some(1);
        let mut tied = higher.clone();
        tied.index_item_id = "c".into();
        let ranked = rank_thread_episodic_search_hits(vec![tied, lower, higher]);
        assert_eq!(
            ranked
                .iter()
                .map(|hit| hit.hit.index_item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c", "a"]
        );
        assert_eq!(ranked[0].score_breakdown.final_score, 1.0);
        assert_eq!(ranked[2].score_breakdown.rank_score, Some(0.5));
    }

    #[test]
    fn rank_fusion_keeps_strong_single_branch_matches_ahead_of_two_moderate_matches() {
        let mut strong = test_hit("workspace", "thread", "strong");
        strong.semantic_rank = Some(1);
        strong.lexical_rank = Some(52_612);
        let mut moderate = test_hit("workspace", "thread", "moderate");
        moderate.lexical_rank = Some(6);
        moderate.semantic_rank = Some(28);
        let ranked = rank_thread_episodic_search_hits(vec![moderate, strong]);
        assert_eq!(ranked[0].hit.index_item_id, "strong");
        assert!(ranked[0].score_breakdown.final_score > ranked[1].score_breakdown.final_score);
    }

    #[test]
    fn single_branch_recall_preserves_absolute_cutoff_and_monotonic_scores() {
        for semantic in [false, true] {
            let scores = (1..=80)
                .map(|rank| {
                    let mut hit = test_hit("workspace", "thread", "source");
                    hit.lexical_rank = (!semantic).then_some(rank);
                    hit.semantic_rank = semantic.then_some(rank);
                    Some(hit.rank_score())
                })
                .collect::<Vec<_>>();
            assert_eq!(scores[0], Some(0.5));
            assert_eq!(scores[61], Some(0.25));
            assert!(scores.windows(2).all(|pair| pair[0] > pair[1]));
            let mut profile = ThreadEpisodicSearchProfile::for_kind(
                ThreadEpisodicSearchProfileKind::DefaultContext,
            );
            profile.adaptive_strategy = ThreadEpisodicAdaptiveStrategy::Absolute;
            profile.max_candidates = 80;
            let (count, _) =
                PioneerAdaptiveCutoff::cutoff_len(&scores, profile.adaptive_cutoff_config());
            assert_eq!(count, 62);
        }
    }

    #[test]
    fn index_document_rejects_invalid_vectors_before_publication() {
        let identity =
            ThreadEpisodicEmbeddingIdentity::new("openai", "text-embedding-3-small", 3, true);
        let document = ThreadEpisodicIndexDocument {
            workspace_id: "workspace".into(),
            index_item_id: "validate_document".into(),
            text: "document".into(),
            metadata: BTreeMap::new(),
            embedding: None,
        };
        document.validate_prepared().unwrap();
        for vector in [
            vec![0.1, 0.2],
            vec![0.1, f32::NAN, 0.3],
            vec![f32::INFINITY, 0.2, 0.3],
            vec![0.0, 0.0, 0.0],
        ] {
            assert_eq!(
                ThreadEpisodicIndexEmbedding::new(identity.clone(), vector)
                    .unwrap_err()
                    .kind,
                ThreadEpisodicSearchFailureKind::NonRetryable
            );
        }
        let valid = ThreadEpisodicIndexDocument {
            embedding: Some(
                ThreadEpisodicIndexEmbedding::new(identity, vec![0.1, 0.2, 0.3]).unwrap(),
            ),
            ..document
        };
        valid.validate_prepared().unwrap();
    }

    fn test_hit(
        workspace_id: &str,
        thread_id: &str,
        index_item_id: &str,
    ) -> ThreadEpisodicSearchHit {
        ThreadEpisodicSearchHit {
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
            text: "text".to_owned(),
            lexical_rank: Some(1),
            semantic_rank: None,
            created_at_unix: None,
            metadata: BTreeMap::new(),
        }
    }
}
