//! Skills catalog state.

use super::health as skill_health;
use anyhow::Result;
use pioneer_protocol::{
    SkillHealthItem, SkillListItem, SkillListParams, SkillListResponse, SkillsHealthParams,
    SkillsHealthResponse,
};
use std::collections::{HashMap, HashSet};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillCatalogState {
    pub installed: Vec<SkillListItem>,
    pub catalog: Vec<SkillListItem>,
    pub health_details: HashMap<String, SkillHealthItem>,
    pub loading: bool,
    pub error: Option<String>,
    pub refresh_requested: bool,
    pub poller_started: bool,
    pub pending_actions: HashSet<String>,
    pub selected_target: Option<(String, String)>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillsCatalogSnapshot {
    pub catalog: Vec<SkillListItem>,
    pub installed: Vec<SkillListItem>,
    pub health_details: HashMap<String, SkillHealthItem>,
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
    pub selected_target: Option<(String, String)>,
    pub selected_target_cleared: bool,
}

pub trait SkillSnapshotTransport {
    fn skills_list(&self, params: SkillListParams) -> Result<SkillListResponse>;

    fn skills_health(&self, params: SkillsHealthParams) -> Result<SkillsHealthResponse>;
}

pub fn skill_key(slug: &str, source_kind: &str) -> String {
    format!("{}::{}", slug.trim(), source_kind.trim())
}

pub fn normalize_skill_target(slug: &str, source_kind: &str) -> Option<(String, String)> {
    let slug = slug.trim();
    let source_kind = source_kind.trim();

    if slug.is_empty() || source_kind.is_empty() {
        return None;
    }

    Some((slug.to_owned(), source_kind.to_owned()))
}

pub fn skill_list_params(workspace_id: impl Into<String>) -> SkillListParams {
    SkillListParams {
        workspace_id: workspace_id.into(),
        include_health: true,
        include_policy: true,
    }
}

pub fn derive_skills_catalog_and_installed(mut catalog: Vec<SkillListItem>) -> SkillsCatalogSplit {
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
    let SkillsCatalogSplit { catalog, installed } = derive_skills_catalog_and_installed(catalog);
    let health_details = skill_health::health_details_map(health_items);

    SkillsCatalogSnapshot {
        catalog,
        installed,
        health_details,
    }
}

pub fn load_skills_snapshot<TTransport>(
    transport: &TTransport,
    workspace_id: impl Into<String>,
) -> Result<SkillsCatalogSnapshot>
where
    TTransport: SkillSnapshotTransport,
{
    let workspace_id = workspace_id.into();
    let list = transport.skills_list(skill_list_params(workspace_id.clone()))?;
    let targets = skill_health::skill_health_targets(list.skills.as_slice());
    let health_items = if targets.is_empty() {
        Vec::new()
    } else {
        transport
            .skills_health(skill_health::skills_health_params(workspace_id, targets))?
            .skills
    };

    Ok(project_skills_snapshot(list.skills, health_items))
}

pub fn reconcile_skills_snapshot(
    snapshot: SkillsCatalogSnapshot,
    pending_actions: &mut HashSet<String>,
    selected_target: Option<(String, String)>,
) -> ReconciledSkillsSnapshot {
    retain_pending_actions_for_catalog(pending_actions, snapshot.catalog.as_slice());

    let selected_target_cleared = selected_target.as_ref().is_some_and(|(slug, source_kind)| {
        !selected_skill_still_present(
            snapshot.installed.as_slice(),
            Some((slug.as_str(), source_kind.as_str())),
        )
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

pub fn retain_pending_actions_for_catalog(
    pending_actions: &mut HashSet<String>,
    catalog: &[SkillListItem],
) {
    let catalog_keys = catalog
        .iter()
        .map(|skill| skill_key(skill.slug.as_str(), skill.source_kind.as_str()))
        .collect::<HashSet<_>>();

    pending_actions.retain(|key| catalog_keys.contains(key));
}

pub fn is_skill_pending(pending_actions: &HashSet<String>, slug: &str, source_kind: &str) -> bool {
    pending_actions.contains(skill_key(slug, source_kind).as_str())
}

pub fn mark_skill_pending(
    pending_actions: &mut HashSet<String>,
    slug: &str,
    source_kind: &str,
    pending: bool,
) {
    let key = skill_key(slug, source_kind);
    if pending {
        pending_actions.insert(key);
    } else {
        pending_actions.remove(key.as_str());
    }
}

pub fn find_skill<'a>(
    skills: &'a [SkillListItem],
    slug: &str,
    source_kind: &str,
) -> Option<&'a SkillListItem> {
    let (slug, source_kind) = normalize_skill_target(slug, source_kind)?;
    skills
        .iter()
        .find(|skill| skill.slug == slug && skill.source_kind == source_kind)
}

pub fn skill_exists(skills: &[SkillListItem], slug: &str, source_kind: &str) -> bool {
    find_skill(skills, slug, source_kind).is_some()
}

pub fn selected_skill_still_present(
    installed: &[SkillListItem],
    selected_target: Option<(&str, &str)>,
) -> bool {
    let Some((slug, source_kind)) = selected_target else {
        return true;
    };

    skill_exists(installed, slug, source_kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{SkillHealthSummary, SkillInstallState, SkillPolicyState};
    use std::cell::RefCell;

    fn skill(slug: &str, source_kind: &str, installed: bool) -> SkillListItem {
        SkillListItem {
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

    fn health(slug: &str, source_kind: &str) -> SkillHealthItem {
        SkillHealthItem {
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
    fn skill_targets_are_normalized_and_keyed() {
        assert_eq!(skill_key(" imagegen ", " user "), "imagegen::user");
        assert_eq!(
            normalize_skill_target(" imagegen ", " user "),
            Some(("imagegen".to_owned(), "user".to_owned()))
        );
        assert_eq!(normalize_skill_target(" ", "user"), None);
    }

    #[test]
    fn catalog_split_sorts_and_keeps_installed_subset() {
        let split = derive_skills_catalog_and_installed(vec![
            skill("zeta", "user", true),
            skill("alpha", "system", false),
            skill("beta", "user", true),
        ]);

        assert_eq!(
            split
                .catalog
                .iter()
                .map(|skill| skill_key(&skill.slug, &skill.source_kind))
                .collect::<Vec<_>>(),
            vec!["alpha::system", "beta::user", "zeta::user"]
        );
        assert_eq!(split.installed.len(), 2);
        assert!(skill_exists(&split.installed, "beta", "user"));
    }

    #[test]
    fn snapshot_projects_health_map_and_prunes_pending_actions() {
        let snapshot = project_skills_snapshot(
            vec![skill("alpha", "user", true), skill("beta", "user", false)],
            vec![health("alpha", "user")],
        );
        assert!(snapshot.health_details.contains_key("alpha::user"));

        let mut pending = HashSet::from(["alpha::user".to_owned(), "missing::user".to_owned()]);
        retain_pending_actions_for_catalog(&mut pending, &snapshot.catalog);
        assert!(pending.contains("alpha::user"));
        assert!(!pending.contains("missing::user"));
    }

    #[test]
    fn pending_and_selected_target_helpers_use_canonical_keys() {
        let mut pending = HashSet::new();

        mark_skill_pending(&mut pending, " alpha ", " user ", true);
        assert!(is_skill_pending(&pending, "alpha", "user"));
        mark_skill_pending(&mut pending, "alpha", "user", false);
        assert!(!is_skill_pending(&pending, "alpha", "user"));

        let installed = vec![skill("alpha", "user", true)];
        assert!(selected_skill_still_present(
            &installed,
            Some(("alpha", "user"))
        ));
        assert!(!selected_skill_still_present(
            &installed,
            Some(("missing", "user"))
        ));
        assert!(selected_skill_still_present(&installed, None));
    }

    #[test]
    fn load_snapshot_orchestrates_list_then_health() {
        let transport = FakeSkillSnapshotTransport::new(
            vec![
                skill("alpha", "user", true),
                skill("beta", "registry", false),
            ],
            vec![health("alpha", "user")],
        );

        let snapshot = load_skills_snapshot(&transport, "workspace").expect("snapshot");

        assert_eq!(
            transport.requests.borrow().as_slice(),
            &["list:workspace".to_owned(), "health:workspace:2".to_owned()]
        );
        assert_eq!(snapshot.catalog.len(), 2);
        assert_eq!(snapshot.installed.len(), 1);
        assert!(snapshot.health_details.contains_key("alpha::user"));
    }

    #[test]
    fn reconcile_snapshot_prunes_pending_and_clears_missing_selection() {
        let snapshot = project_skills_snapshot(
            vec![skill("alpha", "user", true)],
            vec![health("alpha", "user")],
        );
        let mut pending = HashSet::from([skill_key("alpha", "user"), skill_key("missing", "user")]);

        let reconciled = reconcile_skills_snapshot(
            snapshot,
            &mut pending,
            Some(("missing".to_owned(), "user".to_owned())),
        );

        assert_eq!(pending, HashSet::from([skill_key("alpha", "user")]));
        assert!(reconciled.selected_target.is_none());
        assert!(reconciled.selected_target_cleared);
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
        health: Vec<SkillHealthItem>,
        requests: RefCell<Vec<String>>,
    }

    impl FakeSkillSnapshotTransport {
        fn new(skills: Vec<SkillListItem>, health: Vec<SkillHealthItem>) -> Self {
            Self {
                skills,
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
