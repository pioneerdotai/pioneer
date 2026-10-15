//! Thin mobile boundary adapters for skill pack Composer intent.

use pioneer_client::composer::skill_selection::{
    ComposerSkillChip, ComposerSkillPickerProjection, ComposerSkillSelection,
    project_composer_skill_chips,
};
use serde::Deserialize;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSkillPackPickerRequest {
    pub thread_id: String,
    pub draft_id: pioneer_client::composer::store::DraftId,
    #[serde(default)]
    pub query: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSkillChipsRequest {
    pub selections: Vec<ComposerSkillSelection>,
    pub picker: ComposerSkillPickerProjection,
}

pub fn composer_skill_pack_picker(
    core: &pioneer_client::core::ClientCore,
    request: ClientComposerSkillPackPickerRequest,
) -> ComposerSkillPickerProjection {
    core.composer_catalog_skill_picker(&request.thread_id, request.draft_id, &request.query)
}

pub fn composer_skill_chips(request: ClientComposerSkillChipsRequest) -> Vec<ComposerSkillChip> {
    project_composer_skill_chips(request.selections.as_slice(), &request.picker)
}
