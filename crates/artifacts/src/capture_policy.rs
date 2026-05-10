use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactCapturePolicy {
    pub capture_user_uploads: bool,
    pub capture_new_workspace_files: bool,
    pub capture_modified_workspace_files: bool,
    pub capture_generated_media: bool,
    pub capture_tool_outputs: bool,
    pub capture_task_results: bool,
    pub output_roots: Vec<PathBuf>,
    pub ignored_globs: Vec<String>,
    pub max_files_per_turn: usize,
    pub max_bytes_per_file: u64,
    pub max_total_bytes_per_turn: u64,
}

impl Default for ArtifactCapturePolicy {
    fn default() -> Self {
        Self {
            capture_user_uploads: true,
            capture_new_workspace_files: true,
            capture_modified_workspace_files: false,
            capture_generated_media: true,
            capture_tool_outputs: true,
            capture_task_results: true,
            output_roots: Vec::new(),
            ignored_globs: default_ignored_globs(),
            max_files_per_turn: 32,
            max_bytes_per_file: 50 * 1024 * 1024,
            max_total_bytes_per_turn: 128 * 1024 * 1024,
        }
    }
}

impl ArtifactCapturePolicy {
    pub fn output_roots_or_default(&self, default_root: PathBuf) -> Vec<PathBuf> {
        if self.output_roots.is_empty() {
            vec![default_root]
        } else {
            self.output_roots.clone()
        }
    }

    pub fn ignores_path(&self, path: &Path) -> bool {
        self.ignored_globs
            .iter()
            .any(|pattern| path_matches_ignored_pattern(path, pattern))
    }
}

fn default_ignored_globs() -> Vec<String> {
    [
        ".git",
        "target",
        "node_modules",
        "dist",
        "build",
        ".next",
        ".cache",
        ".DS_Store",
        "*.tmp",
        "*.swp",
        "*~",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn path_matches_ignored_pattern(path: &Path, pattern: &str) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return false;
    }
    let path_text = path.to_string_lossy();
    if let Some(suffix) = pattern.strip_prefix("*.") {
        return path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| name.ends_with(format!(".{suffix}").as_str()));
    }
    if let Some(prefix) = pattern.strip_suffix("/**") {
        return path_text == prefix || path_text.starts_with(format!("{prefix}/").as_str());
    }
    path.components().any(|component| match component {
        Component::Normal(value) => value == pattern,
        _ => false,
    }) || path
        .file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| name == pattern)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_policy_ignores_dependency_and_temp_paths() {
        let policy = ArtifactCapturePolicy::default();

        assert!(policy.ignores_path(Path::new("target/debug/app")));
        assert!(policy.ignores_path(Path::new("node_modules/pkg/index.js")));
        assert!(policy.ignores_path(Path::new("notes.tmp")));
        assert!(!policy.ignores_path(Path::new("src/report.txt")));
    }

    #[test]
    fn capture_policy_uses_workspace_root_when_outputs_are_empty() {
        let policy = ArtifactCapturePolicy::default();

        assert_eq!(
            policy.output_roots_or_default(PathBuf::from("/workspace")),
            vec![PathBuf::from("/workspace")]
        );
    }

    #[test]
    fn capture_policy_defaults_enable_all_artifact_sources() {
        let policy = ArtifactCapturePolicy::default();

        assert!(policy.capture_user_uploads);
        assert!(policy.capture_new_workspace_files);
        assert!(policy.capture_generated_media);
        assert!(policy.capture_tool_outputs);
        assert!(policy.capture_task_results);
    }
}
