use super::{AppManifest, ManifestError, manifest::path};
use crate::{Entry, Limits, Snapshot};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionKind {
    File,
    Directory,
    FileOrDirectory,
}
#[derive(Debug, Clone)]
pub struct SelectionPlan {
    entries: BTreeMap<String, SelectionKind>,
}
impl SelectionPlan {
    pub fn from_manifest(manifest: &AppManifest) -> Result<Self, ManifestError> {
        let mut entries = BTreeMap::from([("app.json".into(), SelectionKind::File)]);
        let mut add = |value: &Value, field: &str, kind| -> Result<(), ManifestError> {
            let p = path(value.as_str().expect("validated path"), field)?;
            if let Some(old) = entries.get(&p) {
                if *old != kind
                    && *old != SelectionKind::FileOrDirectory
                    && kind != SelectionKind::FileOrDirectory
                {
                    return Err(ManifestError::new("conflicting_path_kind", field));
                }
                if kind == SelectionKind::FileOrDirectory {
                    return Ok(());
                }
            }
            entries.insert(p, kind);
            Ok(())
        };
        let m = manifest.document();
        for (value, field) in [
            (m.get("icon"), "icon"),
            (m.get("composerIcon"), "composerIcon"),
            (m["developer"].get("logo"), "developer/logo"),
        ] {
            if let Some(value) = value {
                for theme in ["light", "dark"] {
                    add(
                        &value[theme],
                        &format!("app.json/{field}/{theme}"),
                        SelectionKind::File,
                    )?;
                }
            }
        }
        if let Some(v) = m.get("skills") {
            add(v, "app.json/skills", SelectionKind::Directory)?;
        }
        if let Some(v) = m.get("mcp") {
            add(&v["config"], "app.json/mcp/config", SelectionKind::File)?;
            if let Some(include) = v.get("include").and_then(Value::as_array) {
                for (i, v) in include.iter().enumerate() {
                    add(
                        v,
                        &format!("app.json/mcp/include/{i}"),
                        SelectionKind::FileOrDirectory,
                    )?;
                }
            }
        }
        for (locale, p) in manifest.locales() {
            add(
                &Value::String(p.clone()),
                &format!("app.json/locales/{locale}"),
                SelectionKind::File,
            )?;
        }
        Ok(Self { entries })
    }
    pub fn entries(&self) -> &BTreeMap<String, SelectionKind> {
        &self.entries
    }
    /// Independent validation of immutable archive entries, after bounded extraction.
    /// A caller must still validate images and native component documents before install.
    pub fn validate_snapshot(&self, snapshot: &Snapshot) -> Result<(), ManifestError> {
        for (p, kind) in &self.entries {
            let valid = matches!(
                (kind, snapshot.entries().get(p)),
                (SelectionKind::File, Some(Entry::File { .. }))
                    | (SelectionKind::Directory, Some(Entry::Directory { .. }))
                    | (
                        SelectionKind::FileOrDirectory,
                        Some(Entry::File { .. } | Entry::Directory { .. })
                    )
            );
            if !valid {
                return Err(ManifestError::new("selected_path_kind", p));
            }
        }
        for (p, entry) in snapshot.entries() {
            if matches!(entry, Entry::Denied) {
                return Err(ManifestError::new("denied_entry", p));
            }
            let allowed = self.entries.iter().any(|(selected, _)| {
                p == selected
                    || (matches!(
                        snapshot.entries().get(selected),
                        Some(Entry::Directory { .. })
                    ) && p.starts_with(&format!("{selected}/")))
                    || (matches!(entry, Entry::Directory { .. })
                        && selected.starts_with(&format!("{p}/")))
            });
            if !allowed {
                return Err(ManifestError::new("unselected_entry", p));
            }
        }
        Ok(())
    }
    /// Parse the captured manifest again: a caller-provided plan is never authority.
    pub fn validate_package(snapshot: &Snapshot) -> Result<AppManifest, ManifestError> {
        let manifest = AppManifest::parse(
            snapshot
                .file("app.json")
                .ok_or_else(|| ManifestError::new("missing_manifest", "app.json"))?,
        )?;
        Self::from_manifest(&manifest)?.validate_snapshot(snapshot)?;
        manifest.load_localizations(|p| snapshot.file(p))?;
        Ok(manifest)
    }
    /// Only selected paths and their ancestors are visited. Excluded siblings are never scanned.
    pub fn capture(
        root: &Path,
        limits: Limits,
        cancelled: impl Fn() -> bool,
    ) -> anyhow::Result<(AppManifest, Snapshot)> {
        let manifest_limits = Limits {
            entry_bytes: limits.entry_bytes.min(1024 * 1024),
            expanded_bytes: limits.expanded_bytes.min(1024 * 1024),
            ..limits
        };
        let first = Snapshot::capture_selected(
            root,
            manifest_limits,
            &BTreeMap::from([("app.json".into(), SelectionKind::File)]),
            &cancelled,
        )?;
        let bytes = first.file("app.json").expect("selected manifest");
        let manifest = AppManifest::parse(bytes)?;
        let plan = Self::from_manifest(&manifest)?;
        let snapshot = Snapshot::capture_selected(root, limits, &plan.entries, &cancelled)?;
        // The manifest used for planning must be byte-for-byte the captured manifest.
        if snapshot.file("app.json") != Some(bytes) {
            anyhow::bail!("source_changed: app.json");
        }
        plan.validate_snapshot(&snapshot)?;
        manifest.load_localizations(|p| snapshot.file(p))?;
        Ok((manifest, snapshot))
    }
}
