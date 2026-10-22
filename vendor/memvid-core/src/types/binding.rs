//! Memory binding types for linking MV2 files to dashboard memories.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents a binding between an MV2 file and a dashboard memory.
///
/// This is stored in the file header and tracks which dashboard memory
/// this file is associated with for capacity management.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryBinding {
    /// Dashboard memory ID
    pub memory_id: Uuid,
    /// Human-readable memory name
    pub memory_name: String,
    /// When the binding was created
    pub bound_at: DateTime<Utc>,
    /// API URL used for this binding
    pub api_url: String,
}

/// Information about the MV2 file for reporting to dashboard.
///
/// This is sent to the API when syncing tickets to track
/// which files are using a memory's capacity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    /// File name (e.g., "`support_docs.mv2`")
    pub file_name: String,
    /// Full file path
    pub file_path: String,
    /// Current file size in bytes
    pub file_size: u64,
    /// Unique machine identifier
    pub machine_id: String,
    /// Last sync timestamp
    pub last_synced: DateTime<Utc>,
}
