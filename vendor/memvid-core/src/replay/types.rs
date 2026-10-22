//! Types for time-travel replay of agent sessions.
//!
//! This module defines the core data structures for recording, storing,
//! and replaying agent sessions in a deterministic manner.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Magic bytes for replay segment identification
pub const REPLAY_SEGMENT_MAGIC: &[u8; 8] = b"MV2RPLY!";

/// Current version of the replay segment format
pub const REPLAY_SEGMENT_VERSION: u32 = 1;

/// Maximum preview length for input/output strings
pub const MAX_PREVIEW_LENGTH: usize = 512;

/// A recorded action in an agent session
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReplayAction {
    /// Monotonic sequence number within session
    pub sequence: u64,
    /// Unix timestamp (seconds) for determinism
    pub timestamp_secs: i64,
    /// Type of action performed
    pub action_type: ActionType,
    /// Blake3 hash of input data
    pub input_hash: [u8; 32],
    /// Blake3 hash of output data
    pub output_hash: [u8; 32],
    /// Preview of input (truncated for storage efficiency)
    pub input_preview: String,
    /// Preview of output (truncated for storage efficiency)
    pub output_preview: String,
    /// Frames affected by this action
    pub affected_frames: Vec<u64>,
    /// Duration of the action in milliseconds
    #[serde(default)]
    pub duration_ms: u64,
}

impl ReplayAction {
    /// Create a new replay action with the current timestamp
    #[must_use]
    pub fn new(sequence: u64, action_type: ActionType) -> Self {
        Self {
            sequence,
            timestamp_secs: chrono::Utc::now().timestamp(),
            action_type,
            input_hash: [0; 32],
            output_hash: [0; 32],
            input_preview: String::new(),
            output_preview: String::new(),
            affected_frames: Vec::new(),
            duration_ms: 0,
        }
    }

    /// Set the input hash and preview
    ///
    /// # Security
    /// This function implements multiple layers of defense against malicious input:
    /// - **Size Validation**: Enforces strict 10MB limit, rejecting larger payloads
    /// - **Content Sanitization**: Removes control characters that could enable injection
    /// - **Memory Safety**: Uses safe UTF-8 conversion with lossy handling
    /// - **`DoS` Prevention**: Prevents memory exhaustion and resource abuse
    ///
    /// Data exceeding limits is rejected by storing empty values.
    #[must_use]
    pub fn with_input(mut self, data: &[u8]) -> Self {
        // Security: Multi-layer validation to prevent exploitation
        const MAX_INPUT_SIZE: usize = 10 * 1024 * 1024; // 10MB hard limit
        const WARN_INPUT_SIZE: usize = 1024 * 1024; // 1MB warning threshold

        // Reject oversized data completely to prevent DoS
        if data.is_empty() {
            // Empty data is safe, use zero hash
            self.input_hash = [0; 32];
            self.input_preview = String::new();
        } else if data.len() > MAX_INPUT_SIZE {
            // SECURITY: Reject oversized payloads completely
            // Store error indicator instead of processing malicious data
            self.input_hash = [0xFF; 32]; // Error sentinel value
            self.input_preview = format!(
                "[ERROR: Input size {} exceeds maximum {}]",
                data.len(),
                MAX_INPUT_SIZE
            );
        } else {
            // Valid size: process with sanitization
            if data.len() > WARN_INPUT_SIZE {
                // Log large but acceptable inputs
                eprintln!(
                    "[SECURITY WARNING] Large input detected: {} bytes",
                    data.len()
                );
            }
            self.input_hash = blake3::hash(data).into();
            self.input_preview = Self::sanitize_preview(data);
        }
        self
    }

    /// Sanitize input data for preview display
    /// Removes control characters and limits length for security
    fn sanitize_preview(data: &[u8]) -> String {
        let preview_len = data.len().min(MAX_PREVIEW_LENGTH);
        String::from_utf8_lossy(&data[..preview_len])
            .chars()
            .filter(|c| !c.is_control() || *c == '\n' || *c == '\t')
            .take(MAX_PREVIEW_LENGTH)
            .collect()
    }

    /// Set the output hash and preview
    ///
    /// # Security
    /// This function implements multiple layers of defense against malicious output:
    /// - **Size Validation**: Enforces strict 10MB limit, rejecting larger payloads
    /// - **Content Sanitization**: Removes control characters that could enable injection
    /// - **Memory Safety**: Uses safe UTF-8 conversion with lossy handling
    /// - **`DoS` Prevention**: Prevents memory exhaustion and resource abuse
    ///
    /// Data exceeding limits is rejected by storing empty values.
    #[must_use]
    pub fn with_output(mut self, data: &[u8]) -> Self {
        // Security: Multi-layer validation to prevent exploitation
        const MAX_OUTPUT_SIZE: usize = 10 * 1024 * 1024; // 10MB hard limit
        const WARN_OUTPUT_SIZE: usize = 1024 * 1024; // 1MB warning threshold

        // Reject oversized data completely to prevent DoS
        if data.is_empty() {
            // Empty data is safe, use zero hash
            self.output_hash = [0; 32];
            self.output_preview = String::new();
        } else if data.len() > MAX_OUTPUT_SIZE {
            // SECURITY: Reject oversized payloads completely
            // Store error indicator instead of processing malicious data
            self.output_hash = [0xFF; 32]; // Error sentinel value
            self.output_preview = format!(
                "[ERROR: Output size {} exceeds maximum {}]",
                data.len(),
                MAX_OUTPUT_SIZE
            );
        } else {
            // Valid size: process with sanitization
            if data.len() > WARN_OUTPUT_SIZE {
                // Log large but acceptable outputs
                eprintln!(
                    "[SECURITY WARNING] Large output detected: {} bytes",
                    data.len()
                );
            }
            self.output_hash = blake3::hash(data).into();
            self.output_preview = Self::sanitize_preview(data);
        }
        self
    }

    /// Set the affected frames
    #[must_use]
    pub fn with_affected_frames(mut self, frames: Vec<u64>) -> Self {
        self.affected_frames = frames;
        self
    }

    /// Set the duration
    #[must_use]
    pub fn with_duration_ms(mut self, duration_ms: u64) -> Self {
        self.duration_ms = duration_ms;
        self
    }
}

/// Type of action recorded in a replay session
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ActionType {
    /// Frame insertion
    Put { frame_id: u64 },
    /// Batch frame insertion
    PutMany { frame_ids: Vec<u64>, count: usize },
    /// Search query
    Find {
        query: String,
        mode: String, // "lexical", "semantic", "hybrid"
        result_count: usize,
    },
    /// RAG query
    Ask {
        query: String,
        provider: String,
        model: String,
    },
    /// Explicit checkpoint
    Checkpoint { checkpoint_id: u64 },
    /// Frame update
    Update { frame_id: u64 },
    /// Frame deletion
    Delete { frame_id: u64 },
    /// Custom tool call (for agent frameworks)
    ToolCall { name: String, args_hash: [u8; 32] },
}

impl ActionType {
    /// Get a human-readable name for the action type
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Put { .. } => "PUT",
            Self::PutMany { .. } => "PUT_MANY",
            Self::Find { .. } => "FIND",
            Self::Ask { .. } => "ASK",
            Self::Checkpoint { .. } => "CHECKPOINT",
            Self::Update { .. } => "UPDATE",
            Self::Delete { .. } => "DELETE",
            Self::ToolCall { .. } => "TOOL_CALL",
        }
    }
}

/// A checkpoint captures complete state at a point in time
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Checkpoint {
    /// Unique ID within session
    pub id: u64,
    /// Sequence number at checkpoint time
    pub at_sequence: u64,
    /// Unix timestamp
    pub timestamp_secs: i64,
    /// Blake3 hash of complete state
    pub state_hash: [u8; 32],
    /// Snapshot of critical state for restoration
    pub snapshot: StateSnapshot,
}

impl Checkpoint {
    /// Create a new checkpoint at the current time
    #[must_use]
    pub fn new(id: u64, at_sequence: u64, snapshot: StateSnapshot) -> Self {
        let state_hash = blake3::hash(
            &bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())
                .unwrap_or_default(),
        )
        .into();
        Self {
            id,
            at_sequence,
            timestamp_secs: chrono::Utc::now().timestamp(),
            state_hash,
            snapshot,
        }
    }
}

/// State snapshot for checkpoint restoration
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StateSnapshot {
    /// Total frame count at checkpoint
    pub frame_count: usize,
    /// List of all frame IDs
    pub frame_ids: Vec<u64>,
    /// Hash of lexical index state
    pub lex_index_hash: Option<[u8; 32]>,
    /// Hash of vector index state
    pub vec_index_hash: Option<[u8; 32]>,
    /// WAL sequence number
    pub wal_sequence: u64,
    /// Generation number
    pub generation: u64,
}

/// A complete replay session
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReplaySession {
    /// Unique session identifier
    pub session_id: Uuid,
    /// Human-readable name (optional)
    pub name: Option<String>,
    /// Creation timestamp
    pub created_secs: i64,
    /// End timestamp (None if still recording)
    pub ended_secs: Option<i64>,
    /// All checkpoints in order
    pub checkpoints: Vec<Checkpoint>,
    /// All actions in order
    pub actions: Vec<ReplayAction>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
    /// Session version for future compatibility
    #[serde(default = "default_session_version")]
    pub version: u32,
}

fn default_session_version() -> u32 {
    1
}

impl ReplaySession {
    /// Create a new session with the given name
    #[must_use]
    pub fn new(name: Option<String>) -> Self {
        Self {
            session_id: Uuid::new_v4(),
            name,
            created_secs: chrono::Utc::now().timestamp(),
            ended_secs: None,
            checkpoints: Vec::new(),
            actions: Vec::new(),
            metadata: HashMap::new(),
            version: 1,
        }
    }

    /// Check if the session is still recording
    #[must_use]
    pub fn is_recording(&self) -> bool {
        self.ended_secs.is_none()
    }

    /// Get the duration of the session in seconds
    #[must_use]
    pub fn duration_secs(&self) -> u64 {
        match self.ended_secs {
            Some(end) => u64::try_from((end - self.created_secs).max(0)).unwrap_or(0),
            None => u64::try_from((chrono::Utc::now().timestamp() - self.created_secs).max(0))
                .unwrap_or(0),
        }
    }

    /// Get the next sequence number for a new action
    #[must_use]
    pub fn next_sequence(&self) -> u64 {
        self.actions.last().map_or(0, |a| a.sequence + 1)
    }

    /// Add an action to the session
    pub fn add_action(&mut self, action: ReplayAction) {
        self.actions.push(action);
    }

    /// Add a checkpoint to the session
    pub fn add_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.checkpoints.push(checkpoint);
    }

    /// End the session
    pub fn end(&mut self) {
        if self.ended_secs.is_none() {
            self.ended_secs = Some(chrono::Utc::now().timestamp());
        }
    }
}

/// Summary for listing sessions
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SessionSummary {
    pub session_id: Uuid,
    pub name: Option<String>,
    pub created_secs: i64,
    pub ended_secs: Option<i64>,
    pub action_count: usize,
    pub checkpoint_count: usize,
    pub duration_secs: u64,
}

impl From<&ReplaySession> for SessionSummary {
    fn from(session: &ReplaySession) -> Self {
        Self {
            session_id: session.session_id,
            name: session.name.clone(),
            created_secs: session.created_secs,
            ended_secs: session.ended_secs,
            action_count: session.actions.len(),
            checkpoint_count: session.checkpoints.len(),
            duration_secs: session.duration_secs(),
        }
    }
}

/// Manifest stored in TOC for replay segment location
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ReplayManifest {
    /// Offset to replay segment in file
    pub segment_offset: u64,
    /// Size of replay segment
    pub segment_size: u64,
    /// Number of sessions
    pub session_count: u32,
    /// Total actions across all sessions
    pub total_actions: u64,
    /// Version of the replay segment format
    #[serde(default = "default_replay_version")]
    pub version: u32,
}

fn default_replay_version() -> u32 {
    REPLAY_SEGMENT_VERSION
}

impl ReplayManifest {
    /// Check if there are any sessions in the manifest
    #[must_use]
    pub fn has_sessions(&self) -> bool {
        self.session_count > 0
    }
}

/// Options for replay execution
#[derive(Clone, Debug, Default)]
pub struct ReplayOptions {
    /// Start from this checkpoint (0 = beginning)
    pub from_checkpoint: u64,
    /// Override the LLM model
    pub model: Option<String>,
    /// Override the provider
    pub provider: Option<String>,
    /// Override temperature (0.0 for deterministic)
    pub temperature: Option<f32>,
    /// Stop at this sequence number
    pub stop_at: Option<u64>,
    /// Dry run: compare without re-executing LLM calls
    pub dry_run: bool,
    /// Override search mode
    pub search_mode: Option<String>,
    /// Override top-k for retrieval
    pub top_k: Option<usize>,
}

/// Result of replay execution
#[derive(Clone, Debug)]
pub struct ReplayResult {
    pub session_id: Uuid,
    pub from_checkpoint: u64,
    pub original_actions: Vec<ReplayAction>,
    pub replay_actions: Vec<ReplayAction>,
    pub divergences: Vec<Divergence>,
    pub completed: bool,
    pub error: Option<String>,
}

/// A detected divergence between original and replay
#[derive(Clone, Debug)]
pub struct Divergence {
    pub at_sequence: u64,
    pub action_type: String,
    pub divergence_type: DivergenceType,
    pub original_preview: String,
    pub replay_preview: String,
    pub details: Option<String>,
}

/// Type of divergence detected
#[derive(Clone, Debug, PartialEq)]
pub enum DivergenceType {
    /// Different output content
    OutputMismatch,
    /// Different number of search results
    ResultCountDiff { original: usize, replay: usize },
    /// One succeeded, one failed
    ErrorVsSuccess,
    /// Hash mismatch (unexpected state change)
    StateHashMismatch,
}

/// Result of multi-model comparison
#[derive(Clone, Debug)]
pub struct ComparisonReport {
    pub session_id: Uuid,
    pub from_checkpoint: u64,
    pub models: Vec<ModelResult>,
    pub summary: ComparisonSummary,
}

/// Result for a single model in comparison
#[derive(Clone, Debug)]
pub struct ModelResult {
    pub provider: String,
    pub model: String,
    pub actions: Vec<ReplayAction>,
    pub divergence_count: usize,
    pub first_divergence: Option<u64>,
}

/// Summary of model comparison
#[derive(Clone, Debug)]
pub struct ComparisonSummary {
    pub total_actions: usize,
    pub models_compared: usize,
    pub unanimous_actions: usize, // All models agreed
    pub divergent_actions: usize, // At least one disagreed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_lifecycle() {
        let mut session = ReplaySession::new(Some("Test Session".to_string()));
        assert!(session.is_recording());
        assert_eq!(session.next_sequence(), 0);

        let action = ReplayAction::new(0, ActionType::Put { frame_id: 1 });
        session.add_action(action);
        assert_eq!(session.next_sequence(), 1);

        session.end();
        assert!(!session.is_recording());
        assert!(session.ended_secs.is_some());
    }

    #[test]
    fn test_action_type_names() {
        assert_eq!(ActionType::Put { frame_id: 0 }.name(), "PUT");
        assert_eq!(
            ActionType::Find {
                query: "test".into(),
                mode: "lexical".into(),
                result_count: 0
            }
            .name(),
            "FIND"
        );
        assert_eq!(
            ActionType::Ask {
                query: "test".into(),
                provider: "openai".into(),
                model: "gpt-4".into()
            }
            .name(),
            "ASK"
        );
    }

    #[test]
    fn test_session_summary() {
        let mut session = ReplaySession::new(Some("Summary Test".to_string()));
        session.add_action(ReplayAction::new(0, ActionType::Put { frame_id: 1 }));
        session.add_checkpoint(Checkpoint::new(0, 0, StateSnapshot::default()));

        let summary = SessionSummary::from(&session);
        assert_eq!(summary.action_count, 1);
        assert_eq!(summary.checkpoint_count, 1);
        assert_eq!(summary.name, Some("Summary Test".to_string()));
    }
}
