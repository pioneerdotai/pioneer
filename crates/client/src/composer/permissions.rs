//! Shared composer permission-mode state.

use pioneer_protocol::{AuthorizationAgentPermissionOption, TurnPermissionProfileSelection};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerPermissionModeOption {
    pub mode: TurnPermissionMode,
    pub label: String,
    pub description: String,
}

pub type TurnPermissionModeDisplay = ComposerPermissionModeOption;

pub fn default_composer_permission_mode() -> TurnPermissionMode {
    TurnPermissionMode::FullAccess
}

/// Project the Gateway-owned permission presets without deriving policy from
/// a role name. The Gateway still revalidates the selected mode and immutable
/// execution ceiling when a turn starts.
pub fn authorized_composer_permission_mode_options(
    options: &[AuthorizationAgentPermissionOption],
) -> Vec<ComposerPermissionModeOption> {
    options
        .iter()
        .map(|option| ComposerPermissionModeOption {
            mode: option.mode,
            label: option.label.clone(),
            description: option.description.clone(),
        })
        .collect()
}

pub fn turn_permission_mode_display(mode: TurnPermissionMode) -> TurnPermissionModeDisplay {
    match mode {
        TurnPermissionMode::FullAccess => ComposerPermissionModeOption {
            mode,
            label: "Full access".to_owned(),
            description: "Allow commands and edits without prompts.".to_owned(),
        },
        TurnPermissionMode::AutoAcceptEdits => ComposerPermissionModeOption {
            mode,
            label: "Auto-accept edits".to_owned(),
            description: "Auto-approve edits, ask before other actions.".to_owned(),
        },
        TurnPermissionMode::Supervised => ComposerPermissionModeOption {
            mode,
            label: "Supervised".to_owned(),
            description: "Ask before commands and file changes.".to_owned(),
        },
    }
}

pub fn set_composer_permission_mode(
    current: &mut TurnPermissionMode,
    mode: TurnPermissionMode,
) -> bool {
    if *current == mode {
        return false;
    }

    *current = mode;
    true
}

pub fn turn_permission_profile_selection_from_composer_mode(
    mode: TurnPermissionMode,
) -> TurnPermissionProfileSelection {
    TurnPermissionProfileSelection { mode }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_composer_permission_mode_is_full_access() {
        assert_eq!(
            default_composer_permission_mode(),
            TurnPermissionMode::FullAccess
        );
    }

    #[test]
    fn set_composer_permission_mode_reports_changes() {
        let mut current = default_composer_permission_mode();

        assert!(!set_composer_permission_mode(
            &mut current,
            TurnPermissionMode::FullAccess
        ));
        assert!(set_composer_permission_mode(
            &mut current,
            TurnPermissionMode::Supervised
        ));
        assert_eq!(current, TurnPermissionMode::Supervised);
    }

    #[test]
    fn composer_mode_builds_turn_permission_selection() {
        let selection = turn_permission_profile_selection_from_composer_mode(
            TurnPermissionMode::AutoAcceptEdits,
        );

        assert_eq!(selection.mode, TurnPermissionMode::AutoAcceptEdits);
    }

    #[test]
    fn turn_permission_mode_display_reuses_composer_copy() {
        let display = turn_permission_mode_display(TurnPermissionMode::Supervised);

        assert_eq!(display.label, "Supervised");
        assert_eq!(display.description, "Ask before commands and file changes.");
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use crate::{
        composer::{
            state_machine::ComposerDomainState,
            store::{ComposerIntent, ComposerOperationCompletion, ComposerOperationKind},
        },
        core::{ClientCore, ClientMutationAuthority, ClientTransitionOutcome},
    };
    use pioneer_protocol::*;
    use std::sync::Arc;
    fn policy(restricted: bool) -> AuthorizationCapabilitySnapshot {
        let options = vec![AuthorizationAgentPermissionOption {
            id: "synthetic".into(),
            effective_policy: default_turn_permission_profile_snapshot().effective_policy,
            locked: vec![],
            mode: if restricted {
                TurnPermissionMode::Supervised
            } else {
                TurnPermissionMode::FullAccess
            },
            label: "Synthetic permission".into(),
            description: "Synthetic description".into(),
        }];
        AuthorizationCapabilitySnapshot {
            schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision: if restricted { 2 } else { 1 },
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            role_key: "member".into(),
            role: AuthorizationRolePresentation {
                key: "member".into(),
                display_name: "Synthetic".into(),
                description: String::new(),
                built_in: false,
            },
            global: Default::default(),
            workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
                workspace_id: "ws".into(),
                capabilities: AuthorizationWorkspaceCapabilities {
                    agent_permission_options: options.clone(),
                    ..Default::default()
                },
                operational_resources: Default::default(),
                execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                    fingerprint: if restricted { "restricted" } else { "initial" }.into(),
                    resources: Default::default(),
                    permission_options: options,
                    can_attach_artifacts: !restricted,
                    mcp_invocation_limits: Default::default(),
                },
            }),
            thread: Some(AuthorizationThreadCapabilitySnapshot {
                thread_id: "a".into(),
                workspace_id: "ws".into(),
                capabilities: Default::default(),
            }),
        }
    }
    fn fixture(shared: bool) -> Arc<ClientCore> {
        let core = if shared {
            ClientCore::shared()
        } else {
            Arc::new(ClientCore::new())
        };
        core.upsert_thread(serde_json::from_value(serde_json::json!({"workspace_id":"ws","id":"a","preview":"","mode":"Message","model":"","model_provider":"","created_at":1,"updated_at":1,"status":"Idle","origin_kind":"user","sidebar_visibility":"visible","turns":[]})).unwrap());
        ClientMutationAuthority { _private: () }
            .accept_thread_capabilities_for_test(&core, policy(false));
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: ComposerDomainState::default(),
        });
        let draft = core.composer_snapshot("a").unwrap();
        core.composer_intent(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: draft.draft_id(),
            text: "keep the draft".into(),
        });
        core.composer_intent(ComposerIntent::Domain {
            thread_id: "a".into(),
            draft_id: draft.draft_id(),
            action: super::super::state_machine::ComposerDomainAction::AddAttachment {
                attachment: super::super::attachments::composer_attachment_from_path(
                    std::path::Path::new("synthetic.txt"),
                )
                .unwrap(),
            },
        });
        core
    }
    #[test]
    fn accepted_policy_updates_only_its_draft_without_shell_reconciliation_or_echo() {
        let core = fixture(true);
        core.composer_intent(ComposerIntent::Open {
            thread_id: "b".into(),
            defaults: Default::default(),
        });
        let b = core.composer_snapshot("b").unwrap();
        ClientMutationAuthority { _private: () }
            .accept_thread_capabilities_for_test(&core, policy(true));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while core
            .composer_snapshot("a")
            .unwrap()
            .authorization_fingerprint()
            != Some("restricted")
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let current = core.composer_snapshot("a").unwrap();
        assert_eq!(current.authorization_fingerprint(), Some("restricted"));
        assert_eq!(current.draft().text, "keep the draft");
        assert!(current.domain().attachments.is_empty());
        assert_eq!(
            current.domain().selected_permission_mode,
            TurnPermissionMode::Supervised
        );
        assert!(current.selected_permission_mode_allowed());
        assert_eq!(
            current.permission_options()[0].label,
            "Synthetic permission"
        );
        assert!(Arc::ptr_eq(&b, &core.composer_snapshot("b").unwrap()));
        assert_eq!(
            core.composer_intent(ComposerIntent::EditText {
                thread_id: "a".into(),
                draft_id: current.draft_id(),
                text: current.draft().text.clone()
            })
            .outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(&current, &core.composer_snapshot("a").unwrap()));
        assert!(serde_json::from_value::<ComposerIntent>(serde_json::json!({"kind":"reconcile_policy","thread_id":"a","draft_id":current.draft_id(),"policy":{}})).is_err());
        core.shutdown();
    }
    #[test]
    fn authorization_eviction_removes_old_draft_and_permission_publications() {
        let core = fixture(false);
        let draft = core.composer_snapshot("a").unwrap();
        core.composer_intent(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id: draft.draft_id(),
            operation: ComposerOperationKind::Send,
        });
        let old = core
            .composer_snapshot("a")
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        core.clear_authorization_projections();
        assert!(core.composer_snapshot("a").is_none());
        core.complete_composer_operation(old, ComposerOperationCompletion::Sent);
        assert!(core.composer_snapshot("a").is_none());
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: Default::default(),
        });
        let next = core.composer_snapshot("a").unwrap();
        assert_ne!(next.draft_id(), draft.draft_id());
        assert!(next.draft().text.is_empty());
        assert!(next.permission_options().is_empty());
    }
    #[test]
    fn send_preflight_reconciles_a_new_policy_once_and_rejects_old_completion() {
        let core = fixture(false);
        let draft = core.composer_snapshot("a").unwrap().draft_id();
        core.composer_intent(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id: draft,
            operation: ComposerOperationKind::Send,
        });
        let old = core
            .composer_snapshot("a")
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        ClientMutationAuthority { _private: () }
            .accept_thread_capabilities_for_test(&core, policy(true));
        core.composer_intent(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id: draft,
            operation: ComposerOperationKind::Send,
        });
        let reconciled = core.composer_snapshot("a").unwrap();
        assert!(!reconciled.operation().unwrap().pending());
        assert!(reconciled.domain().attachments.is_empty());
        assert_eq!(reconciled.draft().text, "keep the draft");
        core.complete_composer_operation(old, ComposerOperationCompletion::Sent);
        assert!(Arc::ptr_eq(
            &reconciled,
            &core.composer_snapshot("a").unwrap()
        ));
        assert_eq!(
            core.composer_intent(ComposerIntent::BeginOperation {
                thread_id: "a".into(),
                draft_id: draft,
                operation: ComposerOperationKind::Send
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        assert!(
            core.composer_snapshot("a")
                .unwrap()
                .operation()
                .unwrap()
                .pending()
        );
    }
}

pub use pioneer_protocol::TurnPermissionMode;
