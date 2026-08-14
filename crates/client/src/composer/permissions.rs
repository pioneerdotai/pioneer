//! Shared composer permission-mode state.

use pioneer_protocol::{
    AuthorizationAgentPermissionOption, TurnPermissionMode, TurnPermissionProfileSelection,
};

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
