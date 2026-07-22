//! Thin mobile boundary adapters for skill pack Composer intent.

use pioneer_client::{
    composer::skill_selection::{
        ComposerSkillChip, ComposerSkillPickerProjection, ComposerSkillSelection,
        ComposerSkillSelectionReduction, project_composer_skill_chips,
        project_composer_skill_picker, reduce_composer_skill_selection_toggle,
    },
    skills::catalog as skill_catalog,
    transport::ws::GatewayWsCommandSender,
};
use pioneer_protocol::SkillListResponse;
use serde::Deserialize;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSkillPackPickerRequest {
    pub workspace_id: String,
    #[serde(default)]
    pub query: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSkillSelectionToggleRequest {
    pub selections: Vec<ComposerSkillSelection>,
    pub picker: ComposerSkillPickerProjection,
    pub selection: ComposerSkillSelection,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSkillChipsRequest {
    pub selections: Vec<ComposerSkillSelection>,
    pub picker: ComposerSkillPickerProjection,
}

fn skills_management_projection_from_response(
    response: SkillListResponse,
) -> skill_catalog::SkillManagementProjection {
    let split = skill_catalog::derive_skills_catalog_and_installed(response.skills);
    skill_catalog::project_skill_management(split.installed.as_slice(), response.packs)
}

pub fn composer_skill_pack_picker(
    ws_sender: &GatewayWsCommandSender,
    request: ClientComposerSkillPackPickerRequest,
) -> anyhow::Result<ComposerSkillPickerProjection> {
    let response = ws_sender.skills_list(skill_catalog::skill_list_params(request.workspace_id))?;
    let management = skills_management_projection_from_response(response);
    Ok(project_composer_skill_picker(
        &management,
        request.query.as_str(),
    ))
}

pub fn composer_skill_selection_toggle(
    request: ClientComposerSkillSelectionToggleRequest,
) -> ComposerSkillSelectionReduction {
    reduce_composer_skill_selection_toggle(
        request.selections.as_slice(),
        &request.picker,
        request.selection,
    )
}

pub fn composer_skill_chips(request: ClientComposerSkillChipsRequest) -> Vec<ComposerSkillChip> {
    project_composer_skill_chips(request.selections.as_slice(), &request.picker)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        SkillHealthSummary, SkillId, SkillInstallState, SkillListItem, SkillPackId,
        SkillPackInstallationItem, SkillPackMembership, SkillPolicyState,
    };

    fn skill_id(character: char) -> SkillId {
        SkillId::new(character.to_string().repeat(21)).expect("skill id")
    }

    fn pack_id(character: char) -> SkillPackId {
        SkillPackId::new(character.to_string().repeat(21)).expect("pack id")
    }

    fn skill(character: char, pack_id: Option<SkillPackId>) -> SkillListItem {
        SkillListItem {
            skill_id: skill_id(character),
            pack: pack_id.map(|pack_id| SkillPackMembership {
                pack_id,
                member_key: "member".to_owned(),
            }),
            owner: None,
            slug: "skill".to_owned(),
            source_kind: "user".to_owned(),
            display_name: "Skill".to_owned(),
            description: String::new(),
            version: None,
            fingerprint: "fingerprint".to_owned(),
            trust_level: "community".to_owned(),
            install: SkillInstallState {
                managed: true,
                installed: true,
                lifecycle_editable: true,
                install_path: None,
                updated_at: None,
            },
            policy: SkillPolicyState {
                enabled: true,
                allow_implicit_invocation: true,
                allow_implicit_invocation_editable: true,
            },
            health: SkillHealthSummary {
                status: "ok".to_owned(),
                dependency_failures: Vec::new(),
                security_blocks: Vec::new(),
                validation_issues: Vec::new(),
            },
            status: "active".to_owned(),
            status_reason: None,
        }
    }

    #[test]
    fn ffi_selection_and_chip_wrappers_delegate_to_shared_reducers() {
        let parent_id = pack_id('P');
        let management = skills_management_projection_from_response(SkillListResponse {
            snapshot_version: 1,
            generated_at: 2,
            skills: vec![skill('C', Some(parent_id.clone()))],
            packs: vec![SkillPackInstallationItem {
                id: parent_id.clone(),
                name: "Pack".to_owned(),
                source_kind: "user".to_owned(),
                created_at: 1,
                updated_at: 2,
            }],
        });
        let picker = project_composer_skill_picker(&management, "");
        let selection = ComposerSkillSelection::SkillPack { pack_id: parent_id };

        let reduction =
            composer_skill_selection_toggle(ClientComposerSkillSelectionToggleRequest {
                selections: Vec::new(),
                picker: picker.clone(),
                selection,
            });
        assert!(reduction.changed);
        let chips = composer_skill_chips(ClientComposerSkillChipsRequest {
            selections: reduction.selections,
            picker,
        });
        assert_eq!(chips.len(), 1);
        assert_eq!(chips[0].label, "Pack");
    }
}
