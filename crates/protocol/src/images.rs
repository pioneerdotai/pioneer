//! Shared native image representations; domain authorization stays with each resource.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const IMAGE_MAX_BYTES: usize = 256 * 1024;
pub const IMAGE_MAX_DIMENSION: u32 = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum ImageMediaType {
    #[serde(rename = "image/png")]
    Png,
    #[serde(rename = "image/jpeg")]
    Jpeg,
    #[serde(rename = "image/webp")]
    Webp,
}

impl ImageMediaType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
        }
    }
}

/// Immutable image reference. Published paths are authenticated Gateway storage links.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImageReference {
    pub path: String,
    pub revision: String,
}
impl ImageReference {
    pub fn member_avatar(principal: &crate::PrincipalId, revision: impl Into<String>) -> Self {
        let revision = revision.into();
        Self {
            path: format!("storage/members/{principal}/avatar/{revision}"),
            revision,
        }
    }
    pub fn agent_avatar(revision: impl Into<String>) -> Self {
        let revision = revision.into();
        Self {
            path: format!("storage/system/agent/avatar/{revision}"),
            revision,
        }
    }
    pub fn plugin_icon(workspace: &str, plugin: &str, revision: impl Into<String>) -> Self {
        let revision = revision.into();
        Self {
            path: format!("storage/workspaces/{workspace}/plugins/{plugin}/icons/{revision}"),
            revision,
        }
    }
}
