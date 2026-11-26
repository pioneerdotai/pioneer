//! App-only preparation over shared component primitives. No vendor discovery.
use super::{AppManifest, ManifestError, SelectionPlan, manifest::path};
use pioneer_packages::package_mcp::{PackageRootKind, load_mcp, validate_snapshot_paths_for};
use pioneer_packages::{Boundary, ComponentPlan, Diagnostic, Entry, Snapshot};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct LoadedAppPackage {
    pub manifest: AppManifest,
    pub localizations: BTreeMap<String, Value>,
    pub tree_digest: String,
    pub components: Vec<ComponentPlan>,
    pub diagnostics: Vec<Diagnostic>,
    /// Validated references keyed by canonical package path, covering both themes.
    pub images: BTreeMap<String, pioneer_protocol::ImageReference>,
}
/// Index release payloads for streaming; retain only documents the App
/// loaders interpret. The second manifest read must match the first.
pub fn capture_release(
    root: &std::path::Path,
    limits: pioneer_packages::Limits,
    cancelled: impl Fn() -> bool,
) -> anyhow::Result<Snapshot> {
    let mut manifest_limits = limits;
    manifest_limits.entry_bytes = manifest_limits.entry_bytes.min(1024 * 1024);
    let manifest_entry =
        BTreeMap::from([("app.json".to_owned(), pioneer_packages::SelectionKind::File)]);
    let first = Snapshot::capture_selected(root, manifest_limits, &manifest_entry, &cancelled)?;
    let manifest_bytes = first
        .file("app.json")
        .ok_or_else(|| ManifestError::new("missing_manifest", "app.json"))?;
    let manifest = AppManifest::parse(manifest_bytes)?;
    let document = manifest.document();
    let mut retain = std::collections::BTreeSet::from(["app.json".to_owned()]);
    retain.extend(manifest.locales().values().cloned());
    for reference in [
        document.get("icon"),
        document.get("composerIcon"),
        document["developer"].get("logo"),
    ]
    .into_iter()
    .flatten()
    {
        for theme in ["light", "dark"] {
            retain.insert(path(
                reference[theme].as_str().expect("validated path"),
                "image",
            )?);
        }
    }
    if let Some(config) = document["mcp"]["config"].as_str() {
        retain.insert(path(config, "app.json/mcp/config")?);
    }
    let skills = document["skills"]
        .as_str()
        .map(|directory| path(directory, "app.json/skills").map(|key| format!("{key}/")))
        .transpose()?;
    let snapshot = Snapshot::capture_file_backed(
        root,
        limits,
        |key| {
            retain.contains(key)
                || skills.as_ref().is_some_and(|prefix| {
                    key.strip_prefix(prefix)
                        .and_then(|relative| relative.split_once('/'))
                        .is_some_and(|(_, file)| file == "SKILL.md")
                })
        },
        cancelled,
    )?;
    anyhow::ensure!(
        snapshot.file("app.json") == Some(manifest_bytes),
        "source_changed"
    );
    Ok(snapshot)
}

pub fn load(snapshot: &Snapshot) -> Result<LoadedAppPackage, ManifestError> {
    let manifest = SelectionPlan::validate_package(snapshot)?;
    load_validated(snapshot, manifest, || false)
}
/// Prepare an authoring snapshot without applying release executable/checksum rules.
/// Component and presentation validation is shared with ordinary installation.
/// Callers must keep this result out of the release installation path.
pub fn load_source(snapshot: &Snapshot) -> Result<LoadedAppPackage, ManifestError> {
    let manifest = AppManifest::parse(
        snapshot
            .file("app.json")
            .ok_or_else(|| ManifestError::new("missing_manifest", "app.json"))?,
    )?;
    SelectionPlan::for_source(&manifest)?.validate_snapshot(snapshot)?;
    let loaded = load_validated(snapshot, manifest, || false)?;
    if let Some(diagnostic) = loaded.diagnostics.first() {
        return Err(ManifestError::new(
            "invalid_source_component",
            &diagnostic.pointer,
        ));
    }
    Ok(loaded)
}

pub fn load_for_architecture(
    snapshot: &Snapshot,
    architecture: &str,
) -> Result<LoadedAppPackage, ManifestError> {
    load_for_architecture_cancellable(snapshot, architecture, || false)
}

pub fn load_for_architecture_cancellable(
    snapshot: &Snapshot,
    architecture: &str,
    cancelled: impl Fn() -> bool,
) -> Result<LoadedAppPackage, ManifestError> {
    let manifest =
        SelectionPlan::validate_for_architecture_cancellable(snapshot, architecture, &cancelled)?;
    load_validated(snapshot, manifest, cancelled)
}

/// Select and validate the published tree in one operation.
pub fn select_release(
    snapshot: &Snapshot,
    architecture: &str,
    cancelled: impl Fn() -> bool,
) -> anyhow::Result<(Snapshot, LoadedAppPackage)> {
    let selected = SelectionPlan::prune_other_architectures(snapshot, architecture)?;
    let plan = load_for_architecture_cancellable(&selected, architecture, cancelled)?;
    Ok((selected, plan))
}

fn load_validated(
    snapshot: &Snapshot,
    manifest: AppManifest,
    cancelled: impl Fn() -> bool,
) -> Result<LoadedAppPackage, ManifestError> {
    let localizations = manifest.load_localizations(|p| snapshot.file(p))?;
    let mut components = Vec::new();
    let mut diagnostics = Vec::new();
    let mut images = BTreeMap::new();
    let document = manifest.document();
    for (reference, field) in [
        (document.get("icon"), "icon"),
        (document.get("composerIcon"), "composerIcon"),
        (document["developer"].get("logo"), "developer/logo"),
    ] {
        if let Some(reference) = reference {
            for theme in ["light", "dark"] {
                let field = format!("app.json/{field}/{theme}");
                let key = path(reference[theme].as_str().expect("validated path"), &field)?;
                if !images.contains_key(&key) {
                    let bytes = snapshot
                        .file(&key)
                        .ok_or_else(|| ManifestError::new("missing_image", &field))?;
                    let image = pioneer_packages::images::prepare_package_image(bytes)
                        .map_err(|_| ManifestError::new("invalid_image", &field))?;
                    images.insert(
                        key.clone(),
                        pioneer_protocol::ImageReference {
                            path: key,
                            revision: image.revision(),
                        },
                    );
                }
            }
        }
    }
    let mut examined = 0usize;
    let limit = snapshot.limits().components.min(256);
    if let Some(directory) = document.get("skills").and_then(Value::as_str) {
        let directory = path(directory, "app.json/skills")?;
        let prefix = format!("{directory}/");
        for (key, entry) in snapshot.entries() {
            let Some(name) = key.strip_prefix(&prefix).filter(|s| !s.contains('/')) else {
                continue;
            };
            if !matches!(entry, Entry::Directory { .. }) {
                continue;
            }
            let skill_file = format!("{key}/SKILL.md");
            if !snapshot.entries().contains_key(&skill_file) {
                continue;
            }
            examined += 1;
            if examined > limit {
                return Err(ManifestError::new("component_limit", "app.json/skills"));
            }
            if snapshot
                .file(&skill_file)
                .and_then(|b| std::str::from_utf8(b).ok())
                .is_some_and(|text| pioneer_packages::valid_skill_at(text, name, key))
            {
                components.push(ComponentPlan::Skill {
                    member_key: name.into(),
                    member_path: key.clone(),
                    tree_digest: snapshot
                        .checked_member_digest_cancellable(key, &cancelled)
                        .map_err(|_| ManifestError::new("source_changed", key))?,
                });
            } else {
                diagnostics.push(Diagnostic::new(
                    "invalid_skill",
                    Boundary::Skill,
                    &skill_file,
                    "Declared Skill could not be read through the native parser",
                ));
            }
        }
    }
    if let Some(config) = document
        .get("mcp")
        .and_then(|v| v.get("config"))
        .and_then(Value::as_str)
    {
        let config = path(config, "app.json/mcp/config")?;
        let bytes = snapshot
            .file(&config)
            .ok_or_else(|| ManifestError::new("missing_mcp_config", "app.json/mcp/config"))?;
        let servers = load_mcp(bytes, limit.saturating_sub(examined)).map_err(|d| {
            let code = if d.code == "component_limit" {
                "component_limit"
            } else {
                "invalid_mcp_document"
            };
            ManifestError::new(code, &config)
        })?;
        for (name, server) in servers {
            let field = format!(
                "{config}/mcpServers/{}",
                name.replace('~', "~0").replace('/', "~1")
            );
            match server {
                Ok(server)
                    if validate_snapshot_paths_for(&server, snapshot, PackageRootKind::App) =>
                {
                    components.push(ComponentPlan::Mcp {
                        member_key: name.clone(),
                        server,
                    })
                }
                Ok(_) => diagnostics.push(Diagnostic::new(
                    "invalid_mcp_path",
                    Boundary::Server,
                    &field,
                    "Local MCP path is absent from the selected package",
                )),
                Err(mut diagnostic) => {
                    diagnostic.pointer = field;
                    diagnostics.push(diagnostic);
                }
            }
        }
    }
    let tree_digest = snapshot
        .checked_tree_digest_cancellable(cancelled)
        .map_err(|_| ManifestError::new("source_changed", "app.json"))?;
    Ok(LoadedAppPackage {
        manifest,
        localizations,
        tree_digest,
        components,
        diagnostics,
        images,
    })
}
