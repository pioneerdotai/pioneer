use std::collections::BTreeMap;
use std::path::PathBuf;

use pioneer_protocol::{ArtifactCreatedByKind, ArtifactKind};

use crate::models::ArtifactBindingTarget;
use crate::security::ArtifactLocalPathPolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactSource {
    Bytes(Vec<u8>),
    LocalPath(PathBuf),
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestArtifactSourceRequest {
    pub workspace_id: String,
    pub primary_thread_id: Option<String>,
    pub source: ArtifactSource,
    pub display_name: Option<String>,
    pub kind: Option<ArtifactKind>,
    pub mime_type: Option<String>,
    pub created_by_kind: ArtifactCreatedByKind,
    pub created_by_actor_id: Option<String>,
    pub binding: Option<ArtifactBindingTarget>,
    pub metadata: BTreeMap<String, serde_json::Value>,
    pub local_path_policy: Option<ArtifactLocalPathPolicy>,
}
