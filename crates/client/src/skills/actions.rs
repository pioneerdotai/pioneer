//! Skill action orchestration.

use super::catalog;
use pioneer_protocol::{
    SkillId, SkillLifecycleSource, SkillListItem, SkillsInstallParams, SkillsPolicySetParams,
    SkillsUninstallParams, SkillsUpdateParams,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillActionUnavailable {
    GatewayNotConnected,
    WorkspaceNotSelected,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SkillActionScope {
    pub connection_id: u64,
    pub workspace_id: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillActionScopePlan {
    Send(SkillActionScope),
    Unavailable(SkillActionUnavailable),
}

pub fn normalize_skill_source_path(source_path: &str) -> Option<String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return None;
    }

    Some(source_path.to_owned())
}

pub fn plan_skill_action_scope(
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
) -> SkillActionScopePlan {
    let Some(connection_id) = available_connection_id(gateway_connected, connection_id) else {
        return SkillActionScopePlan::Unavailable(SkillActionUnavailable::GatewayNotConnected);
    };
    let Some(workspace_id) = workspace_id else {
        return SkillActionScopePlan::Unavailable(SkillActionUnavailable::WorkspaceNotSelected);
    };

    SkillActionScopePlan::Send(SkillActionScope {
        connection_id,
        workspace_id,
    })
}

pub fn skill_lifecycle_editable(
    installed: &[SkillListItem],
    catalog: &[SkillListItem],
    skill_id: &SkillId,
) -> Option<bool> {
    find_skill_in_installed_or_catalog(installed, catalog, skill_id)
        .map(|skill| skill.install.lifecycle_editable)
}

pub fn skill_policy_implicit_editable(
    installed: &[SkillListItem],
    catalog: &[SkillListItem],
    skill_id: &SkillId,
) -> Option<bool> {
    find_skill_in_installed_or_catalog(installed, catalog, skill_id)
        .map(|skill| skill.policy.allow_implicit_invocation_editable)
}

pub fn skill_policy_values(catalog: &[SkillListItem], skill_id: &SkillId) -> Option<(bool, bool)> {
    catalog::find_skill(catalog, skill_id)
        .map(|skill| (skill.policy.enabled, skill.policy.allow_implicit_invocation))
}

pub fn effective_allow_implicit_invocation(
    allow_implicit_invocation: bool,
    implicit_editable: Option<bool>,
) -> bool {
    if implicit_editable.unwrap_or(true) {
        allow_implicit_invocation
    } else {
        true
    }
}

pub fn apply_local_skill_policy(
    catalog: &mut [SkillListItem],
    installed: &mut [SkillListItem],
    skill_id: &SkillId,
    enabled: bool,
    allow_implicit_invocation: bool,
) {
    apply_local_skill_policy_to_slice(catalog, skill_id, enabled, allow_implicit_invocation);
    apply_local_skill_policy_to_slice(installed, skill_id, enabled, allow_implicit_invocation);
}

pub fn skills_install_uploaded_archive_params(
    workspace_id: impl Into<String>,
    upload_id: impl Into<String>,
) -> SkillsInstallParams {
    SkillsInstallParams {
        workspace_id: workspace_id.into(),
        source: SkillLifecycleSource::UploadedArchive {
            upload_id: upload_id.into(),
        },
        target_source_kind: "user".to_owned(),
    }
}

pub fn skills_update_uploaded_archive_params(
    workspace_id: impl Into<String>,
    skill_id: SkillId,
    upload_id: impl Into<String>,
    expected_previous_fingerprint: Option<String>,
) -> SkillsUpdateParams {
    SkillsUpdateParams {
        workspace_id: workspace_id.into(),
        skill_id,
        source: SkillLifecycleSource::UploadedArchive {
            upload_id: upload_id.into(),
        },
        expected_previous_fingerprint,
    }
}

pub fn skills_uninstall_params(
    workspace_id: impl Into<String>,
    skill_id: SkillId,
) -> SkillsUninstallParams {
    SkillsUninstallParams {
        workspace_id: workspace_id.into(),
        skill_id,
    }
}

pub fn skills_policy_set_params(
    workspace_id: impl Into<String>,
    skill_id: SkillId,
    enabled: bool,
    allow_implicit_invocation: bool,
) -> SkillsPolicySetParams {
    SkillsPolicySetParams {
        workspace_id: workspace_id.into(),
        skill_id,
        enabled: Some(enabled),
        allow_implicit_invocation: Some(allow_implicit_invocation),
    }
}

pub fn skill_action_matches_connection(
    action_connection_id: u64,
    current_connection_id: Option<u64>,
) -> bool {
    current_connection_id == Some(action_connection_id)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillActionTarget {
    pub skill_id: SkillId,
}

impl SkillActionTarget {
    pub fn new(skill_id: SkillId) -> Self {
        Self { skill_id }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkillActionFinishKind {
    Install,
    Update(SkillActionTarget),
    Uninstall(SkillActionTarget),
    Policy(SkillActionTarget),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkillActionFinishOutcome {
    Success,
    Failure { error: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillPendingReduction {
    pub target: SkillActionTarget,
    pub pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillActionFinishReduction {
    pub loading: Option<bool>,
    pub clear_upload_state: bool,
    pub pending: Option<SkillPendingReduction>,
    pub error: Option<String>,
    pub queue_refresh: bool,
    pub rollback_policy: bool,
}

pub fn reduce_skill_action_finish(
    kind: SkillActionFinishKind,
    outcome: SkillActionFinishOutcome,
) -> SkillActionFinishReduction {
    let (loading, clear_upload_state, pending, rollbackable_policy) = match kind {
        SkillActionFinishKind::Install => (Some(false), true, None, false),
        SkillActionFinishKind::Update(target) => (
            None,
            true,
            Some(SkillPendingReduction {
                target,
                pending: false,
            }),
            false,
        ),
        SkillActionFinishKind::Uninstall(target) => (
            None,
            false,
            Some(SkillPendingReduction {
                target,
                pending: false,
            }),
            false,
        ),
        SkillActionFinishKind::Policy(target) => (
            None,
            false,
            Some(SkillPendingReduction {
                target,
                pending: false,
            }),
            true,
        ),
    };

    match outcome {
        SkillActionFinishOutcome::Success => SkillActionFinishReduction {
            loading,
            clear_upload_state,
            pending,
            error: None,
            queue_refresh: true,
            rollback_policy: false,
        },
        SkillActionFinishOutcome::Failure { error } => SkillActionFinishReduction {
            loading,
            clear_upload_state,
            pending,
            error: Some(error),
            queue_refresh: false,
            rollback_policy: rollbackable_policy,
        },
    }
}

fn available_connection_id(gateway_connected: bool, connection_id: Option<u64>) -> Option<u64> {
    gateway_connected.then_some(connection_id).flatten()
}

fn find_skill_in_installed_or_catalog<'a>(
    installed: &'a [SkillListItem],
    catalog: &'a [SkillListItem],
    skill_id: &SkillId,
) -> Option<&'a SkillListItem> {
    catalog::find_skill(installed, skill_id).or_else(|| catalog::find_skill(catalog, skill_id))
}

fn apply_local_skill_policy_to_slice(
    skills: &mut [SkillListItem],
    skill_id: &SkillId,
    enabled: bool,
    allow_implicit_invocation: bool,
) {
    for skill in skills {
        if &skill.skill_id == skill_id {
            let allow_implicit_invocation = if skill.policy.allow_implicit_invocation_editable {
                allow_implicit_invocation
            } else {
                true
            };
            skill.policy.enabled = enabled;
            skill.policy.allow_implicit_invocation = allow_implicit_invocation;
            skill.status = if enabled {
                if skill.health.status == "blocked" {
                    "blocked".to_owned()
                } else {
                    "active".to_owned()
                }
            } else {
                "disabled".to_owned()
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{SkillHealthSummary, SkillInstallState, SkillPolicyState};

    fn test_id(slug: &str, source_kind: &str) -> SkillId {
        let seed = format!("{slug}{source_kind}");
        SkillId::new(seed.chars().cycle().take(21).collect::<String>()).unwrap()
    }

    fn skill(
        slug: &str,
        source_kind: &str,
        lifecycle_editable: bool,
        implicit_editable: bool,
        health_status: &str,
    ) -> SkillListItem {
        SkillListItem {
            skill_id: test_id(slug, source_kind),
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
                installed: true,
                lifecycle_editable,
                install_path: None,
                updated_at: None,
            },
            policy: SkillPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
                allow_implicit_invocation_editable: implicit_editable,
            },
            health: SkillHealthSummary {
                status: health_status.to_owned(),
                dependency_failures: Vec::new(),
                security_blocks: Vec::new(),
                validation_issues: Vec::new(),
            },
            status: "active".to_owned(),
            status_reason: None,
        }
    }

    #[test]
    fn skill_action_helpers_normalize_path_and_target_state() {
        assert_eq!(
            normalize_skill_source_path(" /tmp/skill "),
            Some("/tmp/skill".to_owned())
        );
        assert_eq!(normalize_skill_source_path(" "), None);

        let installed = vec![skill("alpha", "user", false, true, "ok")];
        let catalog = vec![skill("beta", "registry", true, false, "ok")];
        let alpha_id = test_id("alpha", "user");
        let beta_id = test_id("beta", "registry");

        assert_eq!(
            skill_lifecycle_editable(&installed, &catalog, &alpha_id),
            Some(false)
        );
        assert_eq!(
            skill_policy_implicit_editable(&installed, &catalog, &beta_id),
            Some(false)
        );
        assert_eq!(skill_policy_values(&catalog, &beta_id), Some((true, false)));
        assert!(effective_allow_implicit_invocation(false, Some(true)) == false);
        assert!(effective_allow_implicit_invocation(false, Some(false)));
    }

    #[test]
    fn skill_action_scope_plan_reports_connection_and_workspace_availability() {
        assert_eq!(
            plan_skill_action_scope(true, Some(7), Some("workspace".to_owned())),
            SkillActionScopePlan::Send(SkillActionScope {
                connection_id: 7,
                workspace_id: "workspace".to_owned(),
            })
        );
        assert!(matches!(
            plan_skill_action_scope(false, Some(7), Some("workspace".to_owned())),
            SkillActionScopePlan::Unavailable(SkillActionUnavailable::GatewayNotConnected)
        ));
        assert!(matches!(
            plan_skill_action_scope(true, None, Some("workspace".to_owned())),
            SkillActionScopePlan::Unavailable(SkillActionUnavailable::GatewayNotConnected)
        ));
        assert!(matches!(
            plan_skill_action_scope(true, Some(7), None),
            SkillActionScopePlan::Unavailable(SkillActionUnavailable::WorkspaceNotSelected)
        ));
    }

    #[test]
    fn skill_action_finish_reducer_projects_install_success() {
        let reduction = reduce_skill_action_finish(
            SkillActionFinishKind::Install,
            SkillActionFinishOutcome::Success,
        );

        assert_eq!(reduction.loading, Some(false));
        assert!(reduction.clear_upload_state);
        assert!(reduction.pending.is_none());
        assert!(reduction.error.is_none());
        assert!(reduction.queue_refresh);
        assert!(!reduction.rollback_policy);
    }

    #[test]
    fn skill_action_finish_reducer_projects_update_failure() {
        let target = SkillActionTarget::new(test_id("alpha", "user"));
        let reduction = reduce_skill_action_finish(
            SkillActionFinishKind::Update(target.clone()),
            SkillActionFinishOutcome::Failure {
                error: "update failed".to_owned(),
            },
        );

        assert_eq!(reduction.loading, None);
        assert!(reduction.clear_upload_state);
        assert_eq!(
            reduction.pending,
            Some(SkillPendingReduction {
                target,
                pending: false,
            })
        );
        assert_eq!(reduction.error.as_deref(), Some("update failed"));
        assert!(!reduction.queue_refresh);
        assert!(!reduction.rollback_policy);
    }

    #[test]
    fn skill_action_finish_reducer_requests_policy_rollback_on_failure() {
        let target = SkillActionTarget::new(test_id("alpha", "user"));
        let reduction = reduce_skill_action_finish(
            SkillActionFinishKind::Policy(target),
            SkillActionFinishOutcome::Failure {
                error: "policy failed".to_owned(),
            },
        );

        assert!(!reduction.clear_upload_state);
        assert!(reduction.pending.is_some());
        assert_eq!(reduction.error.as_deref(), Some("policy failed"));
        assert!(!reduction.queue_refresh);
        assert!(reduction.rollback_policy);
    }

    #[test]
    fn local_policy_projection_updates_catalog_and_installed() {
        let mut catalog = vec![skill("alpha", "user", true, false, "blocked")];
        let mut installed = vec![skill("alpha", "user", true, true, "ok")];
        let alpha_id = test_id("alpha", "user");

        apply_local_skill_policy(&mut catalog, &mut installed, &alpha_id, true, false);

        assert!(catalog[0].policy.allow_implicit_invocation);
        assert_eq!(catalog[0].status, "blocked");
        assert!(!installed[0].policy.allow_implicit_invocation);
        assert_eq!(installed[0].status, "active");

        apply_local_skill_policy(&mut catalog, &mut installed, &alpha_id, false, true);
        assert_eq!(catalog[0].status, "disabled");
        assert_eq!(installed[0].status, "disabled");
    }

    #[test]
    fn duplicate_labels_are_independently_actionable_by_id() {
        let mut first = skill("humanizer", "user", true, true, "ok");
        first.skill_id = SkillId::new("A".repeat(21)).unwrap();
        first.owner = Some("alex".to_owned());
        let mut second = first.clone();
        second.skill_id = SkillId::new("B".repeat(21)).unwrap();
        let mut catalog = vec![first.clone(), second.clone()];
        let mut installed = catalog.clone();

        apply_local_skill_policy(&mut catalog, &mut installed, &first.skill_id, false, false);

        assert_eq!(catalog[0].status, "disabled");
        assert_eq!(installed[0].status, "disabled");
        assert_eq!(catalog[1].status, "active");
        assert_eq!(installed[1].status, "active");
        assert_eq!(catalog[0].slug, catalog[1].slug);
    }

    #[test]
    fn skill_action_params_preserve_identity_and_upload_source() {
        let install = skills_install_uploaded_archive_params("workspace", "upload");
        assert_eq!(install.workspace_id, "workspace");
        assert_eq!(install.target_source_kind, "user");
        assert!(matches!(
            install.source,
            SkillLifecycleSource::UploadedArchive { ref upload_id } if upload_id == "upload"
        ));

        let alpha_id = test_id("alpha", "user");
        let update = skills_update_uploaded_archive_params(
            "workspace",
            alpha_id.clone(),
            "upload",
            Some("fingerprint".to_owned()),
        );
        assert_eq!(update.skill_id, alpha_id);
        assert_eq!(
            update.expected_previous_fingerprint.as_deref(),
            Some("fingerprint")
        );

        let uninstall = skills_uninstall_params("workspace", alpha_id.clone());
        assert_eq!(uninstall.skill_id, alpha_id);

        let policy = skills_policy_set_params("workspace", alpha_id.clone(), true, false);
        assert_eq!(policy.skill_id, alpha_id);
        assert_eq!(policy.enabled, Some(true));
        assert_eq!(policy.allow_implicit_invocation, Some(false));
    }

    #[test]
    fn skill_action_connection_guard_detects_stale_results() {
        assert!(skill_action_matches_connection(5, Some(5)));
        assert!(!skill_action_matches_connection(5, Some(6)));
        assert!(!skill_action_matches_connection(5, None));
    }
}
