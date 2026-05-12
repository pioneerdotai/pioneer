use crate::constants::files::{BootstrapFileKind, CANONICAL_FILE_ORDER};
use crate::diagnostics::PromptDiagnostic;
use crate::profile::PromptProfile;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedBootstrapFile {
    pub kind: BootstrapFileKind,
    pub name: String,
    pub path: PathBuf,
    pub content: String,
}

fn is_allowed_by_profile(profile: PromptProfile, kind: BootstrapFileKind) -> bool {
    use PromptProfile as P;
    match profile {
        P::AssistantFull | P::AssistantMinimal => matches!(
            kind,
            BootstrapFileKind::Soul | BootstrapFileKind::Identity | BootstrapFileKind::User
        ),
        P::AssistantNone => false,
    }
}

fn resolve_file_path(workspace_root: &Path, kind: BootstrapFileKind) -> PathBuf {
    workspace_root.join(kind.canonical_name())
}

fn file_exists_exact(workspace_root: &Path, file_name: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(workspace_root) else {
        return false;
    };
    entries.flatten().any(|entry| {
        entry
            .file_name()
            .to_str()
            .is_some_and(|value| value == file_name)
    })
}

fn resolve_existing_file_path(workspace_root: &Path, kind: BootstrapFileKind) -> PathBuf {
    let primary = resolve_file_path(workspace_root, kind);
    if file_exists_exact(workspace_root, kind.canonical_name()) {
        return primary;
    }
    if primary.exists() {
        return primary;
    }
    primary
}

pub fn load_bootstrap_files(
    workspace_root: &Path,
    profile: PromptProfile,
) -> (Vec<LoadedBootstrapFile>, Vec<PromptDiagnostic>) {
    let mut files = Vec::new();
    let mut diagnostics = Vec::new();

    for kind in CANONICAL_FILE_ORDER {
        let canonical_path = resolve_file_path(workspace_root, kind);
        if !is_allowed_by_profile(profile, kind) {
            diagnostics.push(PromptDiagnostic::filtered_by_profile(
                kind.canonical_name(),
                canonical_path.display().to_string(),
            ));
            continue;
        }

        let path = resolve_existing_file_path(workspace_root, kind);
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let name = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or(kind.canonical_name())
                    .to_owned();
                files.push(LoadedBootstrapFile {
                    kind,
                    name,
                    path,
                    content,
                });
            }
            Err(error) => {
                if error.kind() == std::io::ErrorKind::NotFound {
                    diagnostics.push(PromptDiagnostic::missing_file(
                        kind.canonical_name(),
                        path.display().to_string(),
                    ));
                } else {
                    diagnostics.push(PromptDiagnostic::file_read_error(
                        kind.canonical_name(),
                        path.display().to_string(),
                        error.to_string().as_str(),
                    ));
                }
            }
        }
    }

    (files, diagnostics)
}

#[cfg(test)]
mod tests {
    use super::load_bootstrap_files;
    use crate::constants::files::BootstrapFileKind;
    use crate::profile::PromptProfile;

    fn temp_workspace(name: &str) -> std::path::PathBuf {
        let root =
            std::env::temp_dir().join(format!("pioneer_promt_files_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp workspace");
        root
    }

    #[test]
    fn loads_only_identity_files_in_canonical_order() {
        let root = temp_workspace("identity_only");
        std::fs::write(root.join("SOUL.md"), "voice").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "assistant").expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "alex").expect("write USER");
        std::fs::write(root.join("AGENTS.md"), "legacy ignored").expect("write AGENTS");

        let (files, diagnostics) = load_bootstrap_files(&root, PromptProfile::AssistantFull);
        assert!(
            diagnostics.is_empty(),
            "unexpected diagnostics: {diagnostics:?}"
        );

        assert_eq!(files.len(), 3);
        assert_eq!(
            files.iter().map(|f| f.kind).collect::<Vec<_>>(),
            vec![
                BootstrapFileKind::Soul,
                BootstrapFileKind::Identity,
                BootstrapFileKind::User
            ]
        );
    }

    #[test]
    fn agents_md_bootstrap_file_is_ignored() {
        let root = temp_workspace("agents_md_ignored");
        std::fs::write(root.join("SOUL.md"), "voice").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "assistant").expect("write IDENTITY");
        std::fs::write(root.join("USER.md"), "alex").expect("write USER");
        std::fs::write(root.join("AGENTS.md"), "must not be loaded by bootstrap")
            .expect("write AGENTS");

        let (files, diagnostics) = load_bootstrap_files(&root, PromptProfile::AssistantFull);
        assert!(
            diagnostics.is_empty(),
            "unexpected diagnostics: {diagnostics:?}"
        );
        assert_eq!(files.len(), 3);
        assert!(files.iter().all(|file| file.name.as_str() != "AGENTS.md"));
        assert!(
            files
                .iter()
                .all(|file| !file.content.contains("must not be loaded"))
        );
    }

    #[test]
    fn missing_file_reports_missing_diagnostic() {
        let root = temp_workspace("missing");
        let (_files, diagnostics) = load_bootstrap_files(&root, PromptProfile::AssistantFull);
        assert!(diagnostics.iter().any(|d| {
            d.code == crate::diagnostics::PromptDiagnosticCode::MissingFile
                && d.file
                    .as_deref()
                    .is_some_and(|file| file.ends_with("SOUL.md"))
        }));
    }

    #[test]
    fn read_error_reports_file_read_error_diagnostic() {
        let root = temp_workspace("read_error");
        std::fs::create_dir_all(root.join("SOUL.md")).expect("create directory named SOUL.md");

        let (_files, diagnostics) = load_bootstrap_files(&root, PromptProfile::AssistantFull);
        assert!(diagnostics.iter().any(|d| {
            d.code == crate::diagnostics::PromptDiagnosticCode::FileReadError
                && d.file
                    .as_deref()
                    .is_some_and(|file| file.ends_with("SOUL.md"))
        }));
    }
}
