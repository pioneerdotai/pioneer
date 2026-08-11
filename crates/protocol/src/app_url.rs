use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const PIONEER_PRODUCTION_URL_SCHEME: &str = "pioneer";
pub const PIONEER_DEVELOPMENT_URL_SCHEME: &str = "pioneer-dev";

/// The custom URL scheme owned by a Pioneer application build.
///
/// Debug builds default to the development scheme so they can coexist with an
/// installed production application. Release packaging can override the
/// default at compile time with `PIONEER_APP_URL_SCHEME`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum PioneerAppUrlScheme {
    #[serde(rename = "pioneer")]
    Production,
    #[serde(rename = "pioneer-dev")]
    Development,
}

impl PioneerAppUrlScheme {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Production => PIONEER_PRODUCTION_URL_SCHEME,
            Self::Development => PIONEER_DEVELOPMENT_URL_SCHEME,
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            PIONEER_PRODUCTION_URL_SCHEME => Some(Self::Production),
            PIONEER_DEVELOPMENT_URL_SCHEME => Some(Self::Development),
            _ => None,
        }
    }

    pub fn for_current_build() -> Self {
        match option_env!("PIONEER_APP_URL_SCHEME") {
            Some(PIONEER_PRODUCTION_URL_SCHEME) => Self::Production,
            Some(PIONEER_DEVELOPMENT_URL_SCHEME) => Self::Development,
            Some(value) => panic!(
                "invalid PIONEER_APP_URL_SCHEME `{value}`; expected `pioneer` or `pioneer-dev`"
            ),
            None if cfg!(debug_assertions) => Self::Development,
            None => Self::Production,
        }
    }
}

impl std::fmt::Display for PioneerAppUrlScheme {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_owned_application_schemes() {
        assert_eq!(
            PioneerAppUrlScheme::parse("pioneer"),
            Some(PioneerAppUrlScheme::Production)
        );
        assert_eq!(
            PioneerAppUrlScheme::parse("pioneer-dev"),
            Some(PioneerAppUrlScheme::Development)
        );
        assert_eq!(PioneerAppUrlScheme::parse("https"), None);
        assert_eq!(PioneerAppUrlScheme::parse("pioneer-preview"), None);
    }
}
