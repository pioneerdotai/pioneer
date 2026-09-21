//! Deterministic working-context rules. No database, Gateway, network or process ownership.
pub mod budget;
pub mod canonical;
pub mod frozen;
pub mod history;
pub mod projection;
pub mod results;
pub mod runner;
pub mod selection;
pub mod summary;
pub use budget::*;
pub use history::*;
pub use selection::*;

pub const FORMAT_VERSION: u32 = 1;
pub const TAIL_TOKENS: u64 = 20_000;
pub const ATTEMPT_MILLIS: u64 = 5 * 60 * 1_000;
pub const OPERATION_MILLIS: u64 = 15 * 60 * 1_000;
pub const RESULT_TOKENS: u64 = 8_192;
pub const RESULT_BYTES: usize = 64 * 1_024;
pub const PAGE_TOKENS: u64 = 4_096;
pub const PAGE_BYTES: usize = 32 * 1_024;

pub fn text_tokens(text: &str) -> u64 {
    tiktoken_rs::cl100k_base_singleton()
        .encode_ordinary(text)
        .len() as u64
}

/// Immutable input to one bounded operation; append-only history is not its basis.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OperationSnapshot {
    pub id: String,
    pub owner: String,
    pub expected_checkpoint: Option<String>,
    pub projection_version: u64,
    /// Epochs captured before resolving directly selected raw sources.
    /// Appends do not change them; an edit in a raw dependency fences publication.
    /// Published checkpoint inputs are immutable objects and do not inherit the
    /// current epochs of the historical sources that they cover.
    #[serde(default)]
    pub source_epochs: std::collections::BTreeMap<String, u64>,
    pub admission: OperationAdmission,
    pub plan: CompactionPlan,
}
