#[cfg(feature = "schema")]
use pioneer_client::composer::model_selection::ComposerModelSelection;
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadTreeRefreshRequest {
    pub workspace_id: String,
}

#[cfg(feature = "schema")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadTreeQueryData {
    pub snapshot: ClientThreadTreeSnapshot,
    pub composer_model_selection: Option<ComposerModelSelection>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadTreeLevelRequest {
    pub snapshot: ClientThreadTreeSnapshot,
    #[serde(default)]
    pub folder_id: Option<String>,
}

pub use pioneer_client::workspaces::projection::{
    ThreadTreeLevel as ClientThreadTreeLevel, ThreadTreeSnapshot as ClientThreadTreeSnapshot,
};

pub fn client_thread_tree_level(request: ThreadTreeLevelRequest) -> ClientThreadTreeLevel {
    pioneer_client::workspaces::projection::thread_tree_level(&request.snapshot, request.folder_id)
}
