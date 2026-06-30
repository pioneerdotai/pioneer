//! Shared composer permission-mode state.

use pioneer_protocol::{TurnPermissionMode, TurnPermissionProfileSelection};

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

pub fn composer_permission_mode_options() -> [ComposerPermissionModeOption; 3] {
    [
        ComposerPermissionModeOption {
            mode: TurnPermissionMode::FullAccess,
            label: "Full access".to_owned(),
            description: "Allow commands and edits without prompts.".to_owned(),
        },
        ComposerPermissionModeOption {
            mode: TurnPermissionMode::AutoAcceptEdits,
            label: "Auto-accept edits".to_owned(),
            description: "Auto-approve edits, ask before other actions.".to_owned(),
        },
        ComposerPermissionModeOption {
            mode: TurnPermissionMode::Supervised,
            label: "Supervised".to_owned(),
            description: "Ask before commands and file changes.".to_owned(),
        },
    ]
}

pub fn composer_permission_mode_option(mode: TurnPermissionMode) -> ComposerPermissionModeOption {
    composer_permission_mode_options()
        .into_iter()
        .find(|option| option.mode == mode)
        .unwrap_or(ComposerPermissionModeOption {
            mode,
            label: "Full access".to_owned(),
            description: "Allow commands and edits without prompts.".to_owned(),
        })
}

pub fn turn_permission_mode_display(mode: TurnPermissionMode) -> TurnPermissionModeDisplay {
    composer_permission_mode_option(mode)
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
    fn permission_mode_options_match_product_modes() {
        let options = composer_permission_mode_options();

        assert_eq!(options.len(), 3);
        assert_eq!(options[0].mode, TurnPermissionMode::Supervised);
        assert_eq!(options[1].mode, TurnPermissionMode::AutoAcceptEdits);
        assert_eq!(options[2].mode, TurnPermissionMode::FullAccess);
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
