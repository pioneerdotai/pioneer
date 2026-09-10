//! Desktop platform preference values; persistence remains in the shell adapter.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowThemePreference {
    #[default]
    System,
    Light,
    Dark,
}

impl WindowThemePreference {
    pub(crate) fn identity_key(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Light => "light",
            Self::Dark => "dark",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppLanguagePreference {
    #[default]
    System,
    English,
    Russian,
    Chinese,
    Hindi,
    Spanish,
    German,
    French,
    Japanese,
}

impl AppLanguagePreference {
    pub(crate) fn identity_key(self) -> &'static str {
        self.explicit_locale().unwrap_or("system")
    }

    pub fn explicit_locale(self) -> Option<&'static str> {
        match self {
            Self::System => None,
            Self::English => Some("en"),
            Self::Russian => Some("ru"),
            Self::Chinese => Some("zh"),
            Self::Hindi => Some("hi"),
            Self::Spanish => Some("es"),
            Self::German => Some("de"),
            Self::French => Some("fr"),
            Self::Japanese => Some("jp"),
        }
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    #[test]
    fn option_identity_is_locale_independent_and_survives_reordering() {
        let before = [
            AppLanguagePreference::English,
            AppLanguagePreference::Russian,
            AppLanguagePreference::System,
        ]
        .map(AppLanguagePreference::identity_key);
        let after = [
            AppLanguagePreference::System,
            AppLanguagePreference::English,
            AppLanguagePreference::Russian,
        ]
        .map(AppLanguagePreference::identity_key);
        assert_eq!(before, [after[1], after[2], after[0]]);
        assert_eq!(before, ["en", "ru", "system"]);
        assert_eq!(
            [
                WindowThemePreference::Dark,
                WindowThemePreference::System,
                WindowThemePreference::Light
            ]
            .map(WindowThemePreference::identity_key),
            ["dark", "system", "light"]
        );
    }
}
