//! Authored runtime requirements. Image versions, execution identity and grants are host-owned.
use crate::{ManifestError, canonical_package_path};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    Node,
    Python,
    Native,
    Wasm,
}
impl Environment {
    pub fn name(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Python => "python",
            Self::Native => "native",
            Self::Wasm => "wasm",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub path: String,
    /// SHA-256 of the canonical package member snapshot, including modes.
    /// A checksum is not a publisher signature.
    pub sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Readiness {
    pub path: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    pub environment: Environment,
    #[serde(default)]
    pub artifacts: BTreeMap<String, Artifact>,
    pub cwd: String,
    pub command: Vec<String>,
    pub port: u16,
    pub readiness: Readiness,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ui {
    pub path: String,
}
impl Runtime {
    pub(crate) fn validate(&mut self) -> Result<(), ManifestError> {
        let reject = |field| ManifestError::new("invalid_runtime", field);
        self.cwd = canonical_package_path(&self.cwd, "app.json/runtime/cwd")?;
        for artifact in self.artifacts.values_mut() {
            artifact.path =
                canonical_package_path(&artifact.path, "app.json/runtime/artifacts/path")?;
        }
        if self.command.is_empty()
            || self.command.len() > 128
            || self
                .command
                .iter()
                .any(|v| v.is_empty() || v.len() > 16384 || v.contains('\0'))
        {
            return Err(reject("app.json/runtime/command"));
        }
        if self.port == 0 || !valid_http_path(&self.readiness.path) {
            return Err(reject("app.json/runtime/readiness"));
        }
        if self.artifacts.len() > 2 {
            return Err(reject("app.json/runtime/artifacts"));
        }
        let paths: Vec<_> = self
            .artifacts
            .values()
            .map(|a| a.path.as_str())
            .chain(std::iter::once(self.cwd.as_str()))
            .collect();
        for (i, a) in paths.iter().enumerate() {
            for b in paths.iter().skip(i + 1) {
                if a == b || a.starts_with(&format!("{b}/")) || b.starts_with(&format!("{a}/")) {
                    return Err(reject("app.json/runtime/artifacts"));
                }
            }
        }
        for (architecture, artifact) in &self.artifacts {
            if !matches!(architecture.as_str(), "linux/amd64" | "linux/arm64") {
                return Err(reject("app.json/runtime/artifacts"));
            }
            canonical_package_path(&artifact.path, "app.json/runtime/artifacts/path")?;
            if artifact.sha256.len() != 64
                || !artifact
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(reject("app.json/runtime/artifacts/sha256"));
            }
        }
        Ok(())
    }
    pub(crate) fn validate_executable(
        &self,
        snapshot: &pioneer_packages::Snapshot,
        architecture: Option<&str>,
    ) -> Result<(), ManifestError> {
        let command = &self.command[0];
        let common = command
            .strip_prefix("/app/")
            .map(str::to_owned)
            .or_else(|| {
                command
                    .strip_prefix("./")
                    .map(|v| format!("{}/{v}", self.cwd))
            });
        let mut files = Vec::new();
        if let Some(common) = common {
            files.push(canonical_package_path(&common, "app.json/runtime/command")?);
        } else if let Some(member) = command.strip_prefix("/payload/") {
            let member = canonical_package_path(member, "app.json/runtime/command")?;
            for (arch, artifact) in &self.artifacts {
                if architecture.is_none_or(|selected| arch == selected) {
                    files.push(format!("{}/{member}", artifact.path));
                }
            }
            if files.is_empty() {
                return Err(ManifestError::new(
                    "missing_runtime_payload",
                    "app.json/runtime/command",
                ));
            }
        } else if self.environment == Environment::Native {
            return Err(ManifestError::new(
                "invalid_native_executable",
                "app.json/runtime/command",
            ));
        }
        for key in files {
            if !matches!(snapshot.entries().get(&key), Some(pioneer_packages::Entry::File { mode, .. }) if mode & 0o100 != 0 && mode & 0o400 != 0)
            {
                return Err(ManifestError::new(
                    "runtime_executable_mode",
                    "app.json/runtime/command",
                ));
            }
        }
        Ok(())
    }
    pub fn artifact(&self, architecture: &str) -> Result<Option<&Artifact>, ManifestError> {
        if self.artifacts.is_empty() {
            return Ok(None);
        }
        self.artifacts.get(architecture).map(Some).ok_or_else(|| {
            ManifestError::new(
                "unsupported_guest_architecture",
                "app.json/runtime/artifacts",
            )
        })
    }
}
pub fn valid_http_path(value: &str) -> bool {
    value.starts_with('/')
        && !value.contains("//")
        && value.len() <= 2048
        && !value.contains(['\\', '\0', '?', '#', '%'])
        && !value.bytes().any(|b| b.is_ascii_control())
        && !value.split('/').any(|s| matches!(s, "." | ".."))
}
