//! Skills catalog state.

use super::{health as skill_health, presentation};
use anyhow::Result;
use pioneer_protocol::{
    SkillHealthItem, SkillId, SkillListItem, SkillListParams, SkillListResponse,
    SkillPackInstallationItem, SkillsHealthParams, SkillsHealthResponse,
};
use std::collections::{HashMap, HashSet};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillCatalogState {
    pub installed: Vec<SkillListItem>,
    pub catalog: Vec<SkillListItem>,
    #[serde(default)]
    pub management: SkillManagementProjection,
    pub health_details: HashMap<SkillId, SkillHealthItem>,
    pub loading: bool,
    pub error: Option<String>,
    pub refresh_requested: bool,
    pub poller_started: bool,
    pub pending_actions: HashSet<SkillId>,
    pub selected_target: Option<SkillId>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillsCatalogSnapshot {
    pub catalog: Vec<SkillListItem>,
    pub installed: Vec<SkillListItem>,
    #[serde(default)]
    pub management: SkillManagementProjection,
    pub health_details: HashMap<SkillId, SkillHealthItem>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillManagementProjection {
    pub standalone: Vec<SkillListItem>,
    pub packs: Vec<SkillPackManagementRow>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillPackManagementRow {
    pub pack: SkillPackInstallationItem,
    pub children: Vec<SkillListItem>,
    pub attachable: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillsCatalogSplit {
    pub catalog: Vec<SkillListItem>,
    pub installed: Vec<SkillListItem>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ReconciledSkillsSnapshot {
    pub snapshot: SkillsCatalogSnapshot,
    pub selected_target: Option<SkillId>,
    pub selected_target_cleared: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillsCatalogRefreshSuccessReduction {
    pub catalog: Vec<SkillListItem>,
    pub installed: Vec<SkillListItem>,
    pub management: SkillManagementProjection,
    pub health_details: HashMap<SkillId, SkillHealthItem>,
    pub pending_actions: HashSet<SkillId>,
    pub selected_target: Option<SkillId>,
    pub selected_target_cleared: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillsCatalogRefreshFailureReduction {
    pub error: String,
}

pub trait SkillSnapshotTransport {
    fn skills_list(&self, params: SkillListParams) -> Result<SkillListResponse>;

    fn skills_health(&self, params: SkillsHealthParams) -> Result<SkillsHealthResponse>;
}

pub fn skill_key(skill_id: &SkillId) -> SkillId {
    skill_id.clone()
}

/// Returns whether a skill has a user-controlled invocation policy.
///
/// Source origin is intentionally irrelevant here: bundled system skills such
/// as Browser are user-controlled, while required system skills such as Memory,
/// Tasks, and Subagents are always active and therefore do not belong in
/// user-managed catalog or composer-selection surfaces.
pub fn skill_is_user_selectable(skill: &SkillListItem) -> bool {
    skill.policy.allow_implicit_invocation_editable
}

pub fn skill_list_params(workspace_id: impl Into<String>) -> SkillListParams {
    SkillListParams {
        workspace_id: workspace_id.into(),
        include_health: true,
        include_policy: true,
    }
}

pub fn skill_matches_search(skill: &SkillListItem, query: &str) -> bool {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return true;
    }

    let label = presentation::compact_skill_label(skill.owner.as_deref(), skill.slug.as_str());
    [
        skill.owner.as_deref(),
        Some(skill.slug.as_str()),
        Some(label.as_str()),
        Some(skill.display_name.as_str()),
        Some(skill.description.as_str()),
    ]
    .into_iter()
    .flatten()
    .any(|field| field.to_lowercase().contains(query.as_str()))
}

pub fn filter_skills_by_search<'a>(
    skills: &'a [SkillListItem],
    query: &str,
) -> Vec<&'a SkillListItem> {
    skills
        .iter()
        .filter(|skill| skill_matches_search(skill, query))
        .collect()
}

pub fn derive_skills_catalog_and_installed(mut catalog: Vec<SkillListItem>) -> SkillsCatalogSplit {
    catalog.retain(skill_is_user_selectable);
    catalog.sort_by(|left, right| {
        left.source_kind
            .cmp(&right.source_kind)
            .then_with(|| left.slug.cmp(&right.slug))
    });

    let installed = catalog
        .iter()
        .filter(|skill| skill.install.installed)
        .cloned()
        .collect::<Vec<_>>();

    SkillsCatalogSplit { catalog, installed }
}

pub fn project_skills_snapshot(
    catalog: Vec<SkillListItem>,
    health_items: Vec<SkillHealthItem>,
) -> SkillsCatalogSnapshot {
    project_skills_list_snapshot(
        SkillListResponse {
            snapshot_version: 0,
            generated_at: 0,
            skills: catalog,
            packs: Vec::new(),
        },
        health_items,
    )
}

pub fn project_skills_list_snapshot(
    list: SkillListResponse,
    health_items: Vec<SkillHealthItem>,
) -> SkillsCatalogSnapshot {
    let SkillListResponse { skills, packs, .. } = list;
    let SkillsCatalogSplit { catalog, installed } = derive_skills_catalog_and_installed(skills);
    let management = project_skill_management(installed.as_slice(), packs);
    let health_details = skill_health::health_details_map(health_items);

    SkillsCatalogSnapshot {
        catalog,
        installed,
        management,
        health_details,
    }
}

pub fn project_skill_management(
    installed: &[SkillListItem],
    mut packs: Vec<SkillPackInstallationItem>,
) -> SkillManagementProjection {
    let mut standalone = installed
        .iter()
        .filter(|skill| skill.pack.is_none())
        .cloned()
        .collect::<Vec<_>>();
    standalone.sort_by(skill_management_order);

    packs.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.id.cmp(&right.id))
    });
    let mut children_by_pack = HashMap::<_, Vec<_>>::new();
    for skill in installed.iter().filter(|skill| skill.pack.is_some()) {
        let membership = skill.pack.as_ref().expect("membership checked above");
        children_by_pack
            .entry(membership.pack_id.clone())
            .or_default()
            .push(skill.clone());
    }

    let packs = packs
        .into_iter()
        .map(|pack| {
            let mut children = children_by_pack.remove(&pack.id).unwrap_or_default();
            children.sort_by(|left, right| {
                let left_membership = left.pack.as_ref().expect("packed child membership");
                let right_membership = right.pack.as_ref().expect("packed child membership");
                left_membership
                    .member_key
                    .cmp(&right_membership.member_key)
                    .then_with(|| left.skill_id.cmp(&right.skill_id))
            });
            SkillPackManagementRow {
                attachable: !children.is_empty(),
                pack,
                children,
            }
        })
        .collect();

    SkillManagementProjection { standalone, packs }
}

fn skill_management_order(left: &SkillListItem, right: &SkillListItem) -> std::cmp::Ordering {
    left.source_kind
        .cmp(&right.source_kind)
        .then_with(|| left.slug.cmp(&right.slug))
        .then_with(|| left.skill_id.cmp(&right.skill_id))
}

pub fn load_skills_snapshot<TTransport>(
    transport: &TTransport,
    workspace_id: impl Into<String>,
    include_management_health: bool,
) -> Result<SkillsCatalogSnapshot>
where
    TTransport: SkillSnapshotTransport,
{
    let workspace_id = workspace_id.into();
    let list = transport.skills_list(skill_list_params(workspace_id.clone()))?;
    let targets = skill_health::skill_health_targets(list.skills.as_slice());
    let health_items = if !include_management_health || targets.is_empty() {
        Vec::new()
    } else {
        transport
            .skills_health(skill_health::skills_health_params(workspace_id, targets))?
            .skills
    };

    Ok(project_skills_list_snapshot(list, health_items))
}

pub fn reconcile_skills_snapshot(
    snapshot: SkillsCatalogSnapshot,
    pending_actions: &mut HashSet<SkillId>,
    selected_target: Option<SkillId>,
) -> ReconciledSkillsSnapshot {
    retain_pending_actions_for_catalog(pending_actions, snapshot.catalog.as_slice());

    let selected_target_cleared = selected_target.as_ref().is_some_and(|skill_id| {
        !selected_skill_still_present(snapshot.installed.as_slice(), Some(skill_id))
    });
    let selected_target = if selected_target_cleared {
        None
    } else {
        selected_target
    };

    ReconciledSkillsSnapshot {
        snapshot,
        selected_target,
        selected_target_cleared,
    }
}

pub fn reduce_skills_catalog_refresh_success(
    snapshot: SkillsCatalogSnapshot,
    mut pending_actions: HashSet<SkillId>,
    selected_target: Option<SkillId>,
) -> SkillsCatalogRefreshSuccessReduction {
    let reconciled = reconcile_skills_snapshot(snapshot, &mut pending_actions, selected_target);
    let snapshot = reconciled.snapshot;

    SkillsCatalogRefreshSuccessReduction {
        catalog: snapshot.catalog,
        installed: snapshot.installed,
        management: snapshot.management,
        health_details: snapshot.health_details,
        pending_actions,
        selected_target: reconciled.selected_target,
        selected_target_cleared: reconciled.selected_target_cleared,
    }
}

pub fn reduce_skills_catalog_refresh_failure(
    error: impl Into<String>,
) -> SkillsCatalogRefreshFailureReduction {
    SkillsCatalogRefreshFailureReduction {
        error: error.into(),
    }
}

pub fn retain_pending_actions_for_catalog(
    pending_actions: &mut HashSet<SkillId>,
    catalog: &[SkillListItem],
) {
    let catalog_keys = catalog
        .iter()
        .map(|skill| skill_key(&skill.skill_id))
        .collect::<HashSet<_>>();

    pending_actions.retain(|key| catalog_keys.contains(key));
}

pub fn is_skill_pending(pending_actions: &HashSet<SkillId>, skill_id: &SkillId) -> bool {
    pending_actions.contains(skill_id)
}

pub fn mark_skill_pending(
    pending_actions: &mut HashSet<SkillId>,
    skill_id: &SkillId,
    pending: bool,
) {
    if pending {
        pending_actions.insert(skill_id.clone());
    } else {
        pending_actions.remove(skill_id);
    }
}

pub fn find_skill<'a>(
    skills: &'a [SkillListItem],
    skill_id: &SkillId,
) -> Option<&'a SkillListItem> {
    skills.iter().find(|skill| &skill.skill_id == skill_id)
}

pub fn skill_exists(skills: &[SkillListItem], skill_id: &SkillId) -> bool {
    find_skill(skills, skill_id).is_some()
}

pub fn selected_skill_still_present(
    installed: &[SkillListItem],
    selected_target: Option<&SkillId>,
) -> bool {
    let Some(skill_id) = selected_target else {
        return true;
    };

    skill_exists(installed, skill_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        SkillHealthSummary, SkillInstallState, SkillPackId, SkillPackMembership, SkillPolicyState,
    };
    use std::cell::RefCell;

    fn skill_id(slug: &str, source_kind: &str) -> SkillId {
        let seed = format!("{slug}{source_kind}")
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .collect::<String>();
        SkillId::new(seed.chars().cycle().take(21).collect::<String>()).expect("test skill id")
    }

    fn skill(slug: &str, source_kind: &str, installed: bool) -> SkillListItem {
        SkillListItem {
            skill_id: skill_id(slug, source_kind),
            pack: None,
            owner: None,
            slug: slug.to_owned(),
            source_kind: source_kind.to_owned(),
            display_name: slug.to_owned(),
            description: String::new(),
            version: None,
            fingerprint: format!("{slug}:{source_kind}:fingerprint"),
            trust_level: "community".to_owned(),
            install: SkillInstallState {
                managed: true,
                installed,
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

    fn pack(character: char, name: &str) -> SkillPackInstallationItem {
        SkillPackInstallationItem {
            id: SkillPackId::new(character.to_string().repeat(21)).expect("test pack id"),
            name: name.to_owned(),
            source_kind: "user".to_owned(),
            created_at: 1,
            updated_at: 2,
        }
    }

    fn packed_skill(
        slug: &str,
        pack: &SkillPackInstallationItem,
        member_key: &str,
    ) -> SkillListItem {
        let mut skill = skill(slug, "user", true);
        skill.pack = Some(SkillPackMembership {
            pack_id: pack.id.clone(),
            member_key: member_key.to_owned(),
        });
        skill
    }

    fn health(slug: &str, source_kind: &str) -> SkillHealthItem {
        SkillHealthItem {
            skill_id: skill_id(slug, source_kind),
            owner: None,
            slug: slug.to_owned(),
            source_kind: source_kind.to_owned(),
            trust_level: "community".to_owned(),
            dependency_diagnostics: Vec::new(),
            security_findings: Vec::new(),
            validation_issues: Vec::new(),
            trust_gate: Vec::new(),
            recent_audit: Vec::new(),
        }
    }

    #[test]
    fn skill_targets_are_keyed_only_by_exact_id() {
        let id = skill_id("imagegen", "user");
        assert_eq!(skill_key(&id), id);
    }

    #[test]
    fn catalog_split_uses_invocation_policy_instead_of_system_origin() {
        let browser = skill("browser", "system", true);
        let mut memory = skill("memory", "system", true);
        memory.policy.allow_implicit_invocation_editable = false;

        let split = derive_skills_catalog_and_installed(vec![
            skill("zeta", "user", true),
            browser,
            memory,
            skill("beta", "user", true),
        ]);

        assert_eq!(
            split
                .catalog
                .iter()
                .map(|skill| (skill.slug.as_str(), skill.source_kind.as_str()))
                .collect::<Vec<_>>(),
            vec![("browser", "system"), ("beta", "user"), ("zeta", "user")]
        );
        assert_eq!(split.installed.len(), 3);
        assert!(skill_exists(&split.catalog, &skill_id("browser", "system")));
        assert!(skill_exists(
            &split.installed,
            &skill_id("browser", "system")
        ));
        assert!(skill_exists(&split.installed, &skill_id("beta", "user")));
        assert!(!skill_exists(&split.catalog, &skill_id("memory", "system")));
        assert!(!skill_exists(
            &split.installed,
            &skill_id("memory", "system")
        ));
    }

    #[test]
    fn snapshot_projects_health_map_and_prunes_pending_actions() {
        let snapshot = project_skills_snapshot(
            vec![skill("alpha", "user", true), skill("beta", "user", false)],
            vec![health("alpha", "user")],
        );
        let alpha_id = skill_id("alpha", "user");
        assert!(snapshot.health_details.contains_key(&alpha_id));

        let missing_id = skill_id("missing", "user");
        let mut pending = HashSet::from([alpha_id.clone(), missing_id.clone()]);
        retain_pending_actions_for_catalog(&mut pending, &snapshot.catalog);
        assert!(pending.contains(&alpha_id));
        assert!(!pending.contains(&missing_id));
    }

    #[test]
    fn management_projection_groups_current_children_and_preserves_empty_parents() {
        let populated = pack('P', "Research");
        let empty = pack('Z', "Empty");
        let standalone = skill("standalone", "user", true);
        let mut first = packed_skill("browser", &populated, "z-browser");
        first.skill_id = SkillId::new("B".repeat(21)).expect("skill id");
        let mut second = packed_skill("reviewer", &populated, "a-reviewer");
        second.skill_id = SkillId::new("R".repeat(21)).expect("skill id");

        let projection = project_skill_management(
            &[first, standalone.clone(), second],
            vec![populated.clone(), empty.clone()],
        );

        assert_eq!(projection.standalone, vec![standalone]);
        assert_eq!(projection.packs.len(), 2);
        assert_eq!(
            projection
                .packs
                .iter()
                .map(|row| row.pack.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Empty", "Research"]
        );
        let empty_row = projection
            .packs
            .iter()
            .find(|row| row.pack.id == empty.id)
            .expect("empty pack row");
        assert!(empty_row.children.is_empty());
        assert!(!empty_row.attachable);

        let populated_row = projection
            .packs
            .iter()
            .find(|row| row.pack.id == populated.id)
            .expect("populated pack row");
        assert!(populated_row.attachable);
        assert_eq!(
            populated_row
                .children
                .iter()
                .map(|skill| { skill.pack.as_ref().expect("membership").member_key.as_str() })
                .collect::<Vec<_>>(),
            vec!["a-reviewer", "z-browser"]
        );
        assert_eq!(populated_row.pack.name, "Research");
    }

    #[test]
    fn management_projection_orders_equal_member_keys_by_skill_id() {
        let parent = pack('P', "Research");
        let mut second = packed_skill("same", &parent, "member");
        second.skill_id = SkillId::new("B".repeat(21)).expect("skill id");
        let mut first = packed_skill("same", &parent, "member");
        first.skill_id = SkillId::new("A".repeat(21)).expect("skill id");

        let projection = project_skill_management(&[second, first], vec![parent]);

        assert_eq!(
            projection.packs[0]
                .children
                .iter()
                .map(|skill| skill.skill_id.as_str())
                .collect::<Vec<_>>(),
            vec!["AAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBB"]
        );
    }

    #[test]
    fn old_snapshot_payload_defaults_management_projection() {
        let value = serde_json::json!({
            "catalog": [],
            "installed": [],
            "health_details": {}
        });

        let snapshot: SkillsCatalogSnapshot =
            serde_json::from_value(value).expect("old snapshot payload");

        assert_eq!(snapshot.management, SkillManagementProjection::default());
    }

    #[test]
    fn pending_and_selected_target_helpers_use_canonical_keys() {
        let mut pending = HashSet::new();
        let alpha_id = skill_id("alpha", "user");
        let missing_id = skill_id("missing", "user");

        mark_skill_pending(&mut pending, &alpha_id, true);
        assert!(is_skill_pending(&pending, &alpha_id));
        mark_skill_pending(&mut pending, &alpha_id, false);
        assert!(!is_skill_pending(&pending, &alpha_id));

        let installed = vec![skill("alpha", "user", true)];
        assert!(selected_skill_still_present(&installed, Some(&alpha_id)));
        assert!(!selected_skill_still_present(&installed, Some(&missing_id)));
        assert!(selected_skill_still_present(&installed, None));
    }

    #[test]
    fn load_snapshot_orchestrates_list_then_health() {
        let parent = pack('P', "Research");
        let mut transport = FakeSkillSnapshotTransport::new(
            vec![
                packed_skill("alpha", &parent, "alpha"),
                skill("beta", "registry", false),
            ],
            vec![health("alpha", "user")],
        );
        transport.packs.push(parent);

        let snapshot = load_skills_snapshot(&transport, "workspace", true).expect("snapshot");

        assert_eq!(
            transport.requests.borrow().as_slice(),
            &["list:workspace".to_owned(), "health:workspace:2".to_owned()]
        );
        assert_eq!(snapshot.catalog.len(), 2);
        assert_eq!(snapshot.installed.len(), 1);
        assert_eq!(snapshot.management.packs.len(), 1);
        assert_eq!(snapshot.management.packs[0].children.len(), 1);
        assert!(
            snapshot
                .health_details
                .contains_key(&skill_id("alpha", "user"))
        );
    }

    #[test]
    fn reconcile_snapshot_prunes_pending_and_clears_missing_selection() {
        let snapshot = project_skills_snapshot(
            vec![skill("alpha", "user", true)],
            vec![health("alpha", "user")],
        );
        let alpha_id = skill_id("alpha", "user");
        let missing_id = skill_id("missing", "user");
        let mut pending = HashSet::from([alpha_id.clone(), missing_id.clone()]);

        let reconciled = reconcile_skills_snapshot(snapshot, &mut pending, Some(missing_id));

        assert_eq!(pending, HashSet::from([alpha_id]));
        assert!(reconciled.selected_target.is_none());
        assert!(reconciled.selected_target_cleared);
    }

    #[test]
    fn refresh_success_reduction_applies_snapshot_and_returns_owned_pending_actions() {
        let snapshot = project_skills_snapshot(
            vec![skill("alpha", "user", true), skill("beta", "user", false)],
            vec![health("alpha", "user")],
        );
        let alpha_id = skill_id("alpha", "user");
        let pending = HashSet::from([alpha_id.clone(), skill_id("missing", "user")]);

        let reduction =
            reduce_skills_catalog_refresh_success(snapshot, pending, Some(alpha_id.clone()));

        assert_eq!(reduction.catalog.len(), 2);
        assert_eq!(reduction.installed.len(), 1);
        assert!(reduction.health_details.contains_key(&alpha_id));
        assert_eq!(reduction.pending_actions, HashSet::from([alpha_id.clone()]));
        assert_eq!(reduction.selected_target, Some(alpha_id));
        assert!(!reduction.selected_target_cleared);
    }

    #[test]
    fn duplicate_labels_remain_distinct_and_reconcile_independently_by_id() {
        let mut first = skill("humanizer", "user", true);
        first.skill_id = SkillId::new("A".repeat(21)).unwrap();
        first.owner = Some("alex".to_owned());
        let mut second = first.clone();
        second.skill_id = SkillId::new("B".repeat(21)).unwrap();

        let snapshot = project_skills_snapshot(vec![first.clone(), second.clone()], Vec::new());
        assert_eq!(snapshot.catalog.len(), 2);
        assert_eq!(snapshot.catalog[0].slug, snapshot.catalog[1].slug);

        let mut pending = HashSet::from([first.skill_id.clone(), second.skill_id.clone()]);
        let reconciled =
            reconcile_skills_snapshot(snapshot, &mut pending, Some(second.skill_id.clone()));

        assert_eq!(pending.len(), 2);
        assert_eq!(reconciled.selected_target, Some(second.skill_id));
        assert!(!reconciled.selected_target_cleared);
    }

    #[test]
    fn search_preserves_presentation_fields_and_duplicate_rows() {
        let mut first = skill("humanizer", "user", true);
        first.skill_id = SkillId::new("A".repeat(21)).unwrap();
        first.owner = Some("alex".to_owned());
        first.display_name = "Natural writing".to_owned();
        first.description = "Remove robotic wording".to_owned();
        let mut second = first.clone();
        second.skill_id = SkillId::new("B".repeat(21)).unwrap();

        let skills = vec![first, second];
        for query in ["alex", "humanizer", "alex/humanizer", "natural", "robotic"] {
            assert_eq!(filter_skills_by_search(&skills, query).len(), 2, "{query}");
        }
        assert!(filter_skills_by_search(&skills, "missing").is_empty());
    }

    #[test]
    fn refresh_failure_reduction_carries_display_error() {
        let reduction = reduce_skills_catalog_refresh_failure("load failed");

        assert_eq!(reduction.error, "load failed");
    }

    #[test]
    fn skill_list_params_include_health_and_policy() {
        let params = skill_list_params("workspace");

        assert_eq!(params.workspace_id, "workspace");
        assert!(params.include_health);
        assert!(params.include_policy);
    }

    struct FakeSkillSnapshotTransport {
        skills: Vec<SkillListItem>,
        packs: Vec<SkillPackInstallationItem>,
        health: Vec<SkillHealthItem>,
        requests: RefCell<Vec<String>>,
    }

    impl FakeSkillSnapshotTransport {
        fn new(skills: Vec<SkillListItem>, health: Vec<SkillHealthItem>) -> Self {
            Self {
                skills,
                packs: Vec::new(),
                health,
                requests: RefCell::new(Vec::new()),
            }
        }
    }

    impl SkillSnapshotTransport for FakeSkillSnapshotTransport {
        fn skills_list(&self, params: SkillListParams) -> Result<SkillListResponse> {
            self.requests
                .borrow_mut()
                .push(format!("list:{}", params.workspace_id));
            Ok(SkillListResponse {
                snapshot_version: 1,
                generated_at: 1,
                skills: self.skills.clone(),
                packs: self.packs.clone(),
            })
        }

        fn skills_health(&self, params: SkillsHealthParams) -> Result<SkillsHealthResponse> {
            self.requests.borrow_mut().push(format!(
                "health:{}:{}",
                params.workspace_id,
                params.skills.len()
            ));
            Ok(SkillsHealthResponse {
                snapshot_version: 1,
                generated_at: 1,
                skills: self.health.clone(),
            })
        }
    }
}
