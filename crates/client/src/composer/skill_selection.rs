//! Shell-neutral skill pack picker, selection, and chip projections.

use super::capabilities::{SelectableSkillCapability, selectable_skill_from_item};
use crate::skills::catalog::SkillManagementProjection;
use pioneer_protocol::{SkillId, SkillPackId};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComposerSkillSelection {
    Skill {
        skill_id: SkillId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pack_id: Option<SkillPackId>,
    },
    SkillPack {
        pack_id: SkillPackId,
    },
}

impl ComposerSkillSelection {
    pub fn key(&self) -> String {
        match self {
            Self::Skill { skill_id, .. } => pioneer_protocol::skill_capability_key(skill_id),
            Self::SkillPack { pack_id } => pioneer_protocol::skill_pack_capability_key(pack_id),
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerSkillPickerProjection {
    pub standalone: Vec<SelectableSkillCapability>,
    pub packs: Vec<SelectableSkillPackCapability>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SelectableSkillPackCapability {
    pub key: String,
    pub pack_id: SkillPackId,
    pub label: String,
    pub children: Vec<SelectablePackedSkillCapability>,
    pub selectable: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SelectablePackedSkillCapability {
    pub pack_id: SkillPackId,
    pub member_key: String,
    pub skill: SelectableSkillCapability,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerSkillSelectionReduction {
    pub selections: Vec<ComposerSkillSelection>,
    pub changed: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComposerSkillChipKind {
    SkillPack,
    PackedSkill,
    StandaloneSkill,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerSkillChip {
    pub key: String,
    pub label: String,
    pub kind: ComposerSkillChipKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_id: Option<SkillId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pack_id: Option<SkillPackId>,
}

pub fn project_composer_skill_picker(
    management: &SkillManagementProjection,
    query: &str,
) -> ComposerSkillPickerProjection {
    let query = query.trim().to_lowercase();
    let standalone = management
        .standalone
        .iter()
        .map(selectable_skill_from_item)
        .filter(|row| skill_row_matches_query(row, query.as_str()))
        .collect();
    let packs = management
        .packs
        .iter()
        .filter_map(|row| {
            let pack_matches =
                query.is_empty() || row.pack.name.to_lowercase().contains(query.as_str());
            let children = row
                .children
                .iter()
                .map(|skill| {
                    let membership = skill.pack.as_ref().expect("management packed child");
                    SelectablePackedSkillCapability {
                        pack_id: membership.pack_id.clone(),
                        member_key: membership.member_key.clone(),
                        skill: selectable_skill_from_item(skill),
                    }
                })
                .filter(|child| {
                    pack_matches || skill_row_matches_query(&child.skill, query.as_str())
                })
                .collect::<Vec<_>>();
            if !pack_matches && children.is_empty() {
                return None;
            }

            Some(SelectableSkillPackCapability {
                key: pioneer_protocol::skill_pack_capability_key(&row.pack.id),
                pack_id: row.pack.id.clone(),
                label: row.pack.name.clone(),
                selectable: row.attachable,
                children,
            })
        })
        .collect();

    ComposerSkillPickerProjection { standalone, packs }
}

pub fn normalize_composer_skill_selections(
    selections: impl IntoIterator<Item = ComposerSkillSelection>,
) -> Vec<ComposerSkillSelection> {
    let mut normalized = Vec::new();
    for selection in selections {
        add_composer_skill_selection(&mut normalized, selection);
    }
    normalized
}

pub fn reduce_composer_skill_selection_toggle(
    current: &[ComposerSkillSelection],
    picker: &ComposerSkillPickerProjection,
    selection: ComposerSkillSelection,
) -> ComposerSkillSelectionReduction {
    let mut selections = normalize_composer_skill_selections(current.iter().cloned());
    let initial = selections.clone();
    if let Some(index) = selections
        .iter()
        .position(|existing| existing == &selection)
    {
        selections.remove(index);
        return ComposerSkillSelectionReduction {
            changed: selections != initial,
            selections,
        };
    }

    if !composer_skill_selection_is_selectable(picker, &selection) {
        return ComposerSkillSelectionReduction {
            changed: selections != current,
            selections,
        };
    }

    add_composer_skill_selection(&mut selections, selection);

    ComposerSkillSelectionReduction {
        changed: selections != initial,
        selections,
    }
}

pub fn project_composer_skill_chips(
    selections: &[ComposerSkillSelection],
    picker: &ComposerSkillPickerProjection,
) -> Vec<ComposerSkillChip> {
    normalize_composer_skill_selections(selections.iter().cloned())
        .into_iter()
        .filter_map(|selection| match selection {
            ComposerSkillSelection::SkillPack { pack_id } => {
                let pack = picker.packs.iter().find(|pack| pack.pack_id == pack_id)?;
                Some(ComposerSkillChip {
                    key: pioneer_protocol::skill_pack_capability_key(&pack_id),
                    label: pack.label.clone(),
                    kind: ComposerSkillChipKind::SkillPack,
                    skill_id: None,
                    pack_id: Some(pack_id),
                })
            }
            ComposerSkillSelection::Skill {
                skill_id,
                pack_id: Some(pack_id),
            } => {
                let pack = picker.packs.iter().find(|pack| pack.pack_id == pack_id)?;
                let child = pack
                    .children
                    .iter()
                    .find(|child| child.skill.skill_id == skill_id)?;
                Some(ComposerSkillChip {
                    key: pioneer_protocol::skill_capability_key(&skill_id),
                    label: format!("{} / {}", pack.label, child.skill.label),
                    kind: ComposerSkillChipKind::PackedSkill,
                    skill_id: Some(skill_id),
                    pack_id: Some(pack_id),
                })
            }
            ComposerSkillSelection::Skill {
                skill_id,
                pack_id: None,
            } => {
                let skill = picker
                    .standalone
                    .iter()
                    .find(|skill| skill.skill_id == skill_id)?;
                Some(ComposerSkillChip {
                    key: pioneer_protocol::skill_capability_key(&skill_id),
                    label: skill.label.clone(),
                    kind: ComposerSkillChipKind::StandaloneSkill,
                    skill_id: Some(skill_id),
                    pack_id: None,
                })
            }
        })
        .collect()
}

fn add_composer_skill_selection(
    selections: &mut Vec<ComposerSkillSelection>,
    incoming: ComposerSkillSelection,
) {
    if selections.iter().any(|existing| existing == &incoming) {
        return;
    }

    selections.retain(|existing| !composer_skill_selections_conflict(existing, &incoming));
    selections.push(incoming);
}

fn composer_skill_selections_conflict(
    existing: &ComposerSkillSelection,
    incoming: &ComposerSkillSelection,
) -> bool {
    match (existing, incoming) {
        (
            ComposerSkillSelection::SkillPack {
                pack_id: existing_pack,
            },
            ComposerSkillSelection::Skill {
                pack_id: Some(incoming_pack),
                ..
            },
        )
        | (
            ComposerSkillSelection::Skill {
                pack_id: Some(existing_pack),
                ..
            },
            ComposerSkillSelection::SkillPack {
                pack_id: incoming_pack,
            },
        ) => existing_pack == incoming_pack,
        (
            ComposerSkillSelection::Skill {
                skill_id: existing_skill,
                ..
            },
            ComposerSkillSelection::Skill {
                skill_id: incoming_skill,
                ..
            },
        ) => existing_skill == incoming_skill,
        _ => false,
    }
}

fn composer_skill_selection_is_selectable(
    picker: &ComposerSkillPickerProjection,
    selection: &ComposerSkillSelection,
) -> bool {
    match selection {
        ComposerSkillSelection::SkillPack { pack_id } => picker
            .packs
            .iter()
            .any(|pack| &pack.pack_id == pack_id && pack.selectable),
        ComposerSkillSelection::Skill {
            skill_id,
            pack_id: Some(pack_id),
        } => picker.packs.iter().any(|pack| {
            &pack.pack_id == pack_id
                && pack
                    .children
                    .iter()
                    .any(|child| &child.skill.skill_id == skill_id && child.skill.selectable)
        }),
        ComposerSkillSelection::Skill {
            skill_id,
            pack_id: None,
        } => picker
            .standalone
            .iter()
            .any(|skill| &skill.skill_id == skill_id && skill.selectable),
    }
}

fn skill_row_matches_query(row: &SelectableSkillCapability, query: &str) -> bool {
    query.is_empty()
        || row.label.to_lowercase().contains(query)
        || row.display_name.to_lowercase().contains(query)
        || row
            .owner
            .as_deref()
            .unwrap_or_default()
            .to_lowercase()
            .contains(query)
        || row.slug.to_lowercase().contains(query)
        || row.description.to_lowercase().contains(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skills::catalog::{SkillManagementProjection, SkillPackManagementRow};
    use pioneer_protocol::{
        SkillHealthSummary, SkillInstallState, SkillListItem, SkillPackInstallationItem,
        SkillPackMembership, SkillPolicyState,
    };

    fn skill_id(character: char) -> SkillId {
        SkillId::new(character.to_string().repeat(21)).expect("skill id")
    }

    fn pack_id(character: char) -> SkillPackId {
        SkillPackId::new(character.to_string().repeat(21)).expect("pack id")
    }

    fn skill(character: char, slug: &str, pack: Option<(SkillPackId, &str)>) -> SkillListItem {
        SkillListItem {
            skill_id: skill_id(character),
            pack: pack.map(|(pack_id, member_key)| SkillPackMembership {
                pack_id,
                member_key: member_key.to_owned(),
            }),
            owner: None,
            slug: slug.to_owned(),
            source_kind: "user".to_owned(),
            display_name: slug.to_owned(),
            description: format!("{slug} description"),
            version: None,
            fingerprint: format!("{slug}-fingerprint"),
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

    fn management() -> SkillManagementProjection {
        let research_id = pack_id('P');
        SkillManagementProjection {
            standalone: vec![skill('S', "standalone", None)],
            packs: vec![
                SkillPackManagementRow {
                    pack: SkillPackInstallationItem {
                        id: research_id.clone(),
                        name: "Research".to_owned(),
                        source_kind: "user".to_owned(),
                        created_at: 1,
                        updated_at: 2,
                    },
                    children: vec![
                        skill('B', "browser", Some((research_id.clone(), "browser"))),
                        skill('R', "reviewer", Some((research_id, "reviewer"))),
                    ],
                    attachable: true,
                },
                SkillPackManagementRow {
                    pack: SkillPackInstallationItem {
                        id: pack_id('E'),
                        name: "Empty".to_owned(),
                        source_kind: "user".to_owned(),
                        created_at: 1,
                        updated_at: 2,
                    },
                    children: Vec::new(),
                    attachable: false,
                },
            ],
        }
    }

    #[test]
    fn picker_projects_standalone_pack_children_and_disabled_empty_parent() {
        let picker = project_composer_skill_picker(&management(), "");

        assert_eq!(picker.standalone.len(), 1);
        assert_eq!(picker.packs.len(), 2);
        assert_eq!(picker.packs[0].children.len(), 2);
        assert!(picker.packs[0].selectable);
        assert!(picker.packs[1].children.is_empty());
        assert!(!picker.packs[1].selectable);

        let filtered = project_composer_skill_picker(&management(), "reviewer");
        assert!(filtered.standalone.is_empty());
        assert_eq!(filtered.packs.len(), 1);
        assert_eq!(filtered.packs[0].children.len(), 1);
        assert_eq!(filtered.packs[0].children[0].skill.slug, "reviewer");
    }

    #[test]
    fn picker_search_is_case_insensitive_for_unicode_pack_names() {
        let mut management = management();
        management.packs[0].pack.name = "Исследования".to_owned();

        let picker = project_composer_skill_picker(&management, "ИССЛЕДОВАНИЯ");

        assert_eq!(picker.packs.len(), 1);
        assert_eq!(picker.packs[0].label, "Исследования");
        assert_eq!(picker.packs[0].children.len(), 2);
    }

    #[test]
    fn full_and_partial_selection_are_mutually_exclusive() {
        let picker = project_composer_skill_picker(&management(), "");
        let full = ComposerSkillSelection::SkillPack {
            pack_id: pack_id('P'),
        };
        let child = ComposerSkillSelection::Skill {
            skill_id: skill_id('B'),
            pack_id: Some(pack_id('P')),
        };

        let selected = reduce_composer_skill_selection_toggle(&[], &picker, full.clone());
        assert_eq!(selected.selections, vec![full]);
        let partial =
            reduce_composer_skill_selection_toggle(&selected.selections, &picker, child.clone());
        assert_eq!(partial.selections, vec![child]);
    }

    #[test]
    fn manually_selecting_every_child_remains_partial() {
        let picker = project_composer_skill_picker(&management(), "");
        let browser = ComposerSkillSelection::Skill {
            skill_id: skill_id('B'),
            pack_id: Some(pack_id('P')),
        };
        let reviewer = ComposerSkillSelection::Skill {
            skill_id: skill_id('R'),
            pack_id: Some(pack_id('P')),
        };

        let first = reduce_composer_skill_selection_toggle(&[], &picker, browser.clone());
        let second =
            reduce_composer_skill_selection_toggle(&first.selections, &picker, reviewer.clone());

        assert_eq!(second.selections, vec![browser, reviewer]);
        assert!(
            second
                .selections
                .iter()
                .all(|selection| matches!(selection, ComposerSkillSelection::Skill { .. }))
        );
    }

    #[test]
    fn standalone_selection_survives_pack_selection_and_empty_pack_is_rejected() {
        let picker = project_composer_skill_picker(&management(), "");
        let standalone = ComposerSkillSelection::Skill {
            skill_id: skill_id('S'),
            pack_id: None,
        };
        let first = reduce_composer_skill_selection_toggle(&[], &picker, standalone.clone());
        let second = reduce_composer_skill_selection_toggle(
            &first.selections,
            &picker,
            ComposerSkillSelection::SkillPack {
                pack_id: pack_id('P'),
            },
        );
        assert!(second.selections.contains(&standalone));
        assert_eq!(second.selections.len(), 2);

        let rejected = reduce_composer_skill_selection_toggle(
            &second.selections,
            &picker,
            ComposerSkillSelection::SkillPack {
                pack_id: pack_id('E'),
            },
        );
        assert!(!rejected.changed);
        assert_eq!(rejected.selections, second.selections);
    }

    #[test]
    fn an_existing_selection_can_be_removed_after_it_becomes_unavailable() {
        let mut picker = project_composer_skill_picker(&management(), "");
        let child = ComposerSkillSelection::Skill {
            skill_id: skill_id('B'),
            pack_id: Some(pack_id('P')),
        };
        let selected = reduce_composer_skill_selection_toggle(&[], &picker, child.clone());
        picker.packs[0].children[0].skill.selectable = false;

        let removed = reduce_composer_skill_selection_toggle(&selected.selections, &picker, child);

        assert!(removed.changed);
        assert!(removed.selections.is_empty());
    }

    #[test]
    fn chip_projection_uses_pack_pack_skill_and_standalone_labels() {
        let picker = project_composer_skill_picker(&management(), "");
        let full = project_composer_skill_chips(
            &[ComposerSkillSelection::SkillPack {
                pack_id: pack_id('P'),
            }],
            &picker,
        );
        assert_eq!(full[0].kind, ComposerSkillChipKind::SkillPack);
        assert_eq!(full[0].label, "Research");

        let partial_and_standalone = project_composer_skill_chips(
            &[
                ComposerSkillSelection::Skill {
                    skill_id: skill_id('R'),
                    pack_id: Some(pack_id('P')),
                },
                ComposerSkillSelection::Skill {
                    skill_id: skill_id('S'),
                    pack_id: None,
                },
            ],
            &picker,
        );
        assert_eq!(
            partial_and_standalone
                .iter()
                .map(|chip| (chip.kind, chip.label.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (ComposerSkillChipKind::PackedSkill, "Research / reviewer"),
                (ComposerSkillChipKind::StandaloneSkill, "standalone"),
            ]
        );
    }
}
