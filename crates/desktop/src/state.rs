use crate::settings::WindowOpenState;
use anyhow::{Context as _, Result};
use gpui_kit::{App, Global};
use pioneer_config::AppConfig;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::PathBuf};
use tracing::warn;

const DESKTOP_STATE_FILE_NAME: &str = "desktop-state.toml";
const DESKTOP_STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DesktopStateFile {
    #[serde(default = "default_state_version")]
    version: u32,
    #[serde(default)]
    window: Option<WindowState>,
    #[serde(default)]
    sidebar: SidebarState,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct WindowState {
    pub(crate) state: WindowOpenState,
    pub(crate) x: f32,
    pub(crate) y: f32,
    pub(crate) width: f32,
    pub(crate) height: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SidebarState {
    #[serde(default)]
    threads: ThreadsState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ThreadsState {
    #[serde(default)]
    folders: ThreadFoldersState,
    #[serde(default)]
    workspaces: HashMap<String, WorkspaceThreadsState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct WorkspaceThreadsState {
    #[serde(default)]
    folders: ThreadFoldersState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ThreadFoldersState {
    #[serde(default)]
    expanded: HashMap<String, bool>,
}

struct DesktopStateStore {
    path: PathBuf,
    state: DesktopStateFile,
}

impl Global for DesktopStateStore {}

const fn default_state_version() -> u32 {
    DESKTOP_STATE_VERSION
}

pub(crate) fn window(cx: &mut App) -> Result<Option<WindowState>> {
    ensure_loaded(cx)?;
    Ok(cx
        .try_global::<DesktopStateStore>()
        .and_then(|state| state.state.window))
}

pub(crate) fn set_window(cx: &mut App, window: WindowState) -> Result<()> {
    ensure_loaded(cx)?;

    let (path, serialized) = {
        let state = cx.global_mut::<DesktopStateStore>();
        state.state.version = DESKTOP_STATE_VERSION;
        state.state.window = Some(window);
        (state.path.clone(), serialize_state(&state.state)?)
    };

    write_state_file(path.as_path(), serialized)?;
    Ok(())
}

pub(crate) fn thread_folders_expanded_for_workspace(
    cx: &mut App,
    workspace_id: Option<&str>,
) -> HashMap<String, bool> {
    if let Err(error) = ensure_loaded(cx) {
        warn!(
            error = %format!("{error:#}"),
            "failed to load desktop state; defaulting sidebar expansion state"
        );
        return HashMap::new();
    }

    cx.try_global::<DesktopStateStore>()
        .map(|state| thread_folders_expanded_from_state(&state.state, workspace_id))
        .unwrap_or_default()
}

pub(crate) fn set_thread_folders_expanded_for_workspace(
    cx: &mut App,
    workspace_id: &str,
    expanded: HashMap<String, bool>,
) -> Result<()> {
    ensure_loaded(cx)?;

    let (path, serialized) = {
        let state = cx.global_mut::<DesktopStateStore>();
        state.state.version = DESKTOP_STATE_VERSION;
        set_thread_folders_expanded_for_workspace_in_state(
            &mut state.state,
            workspace_id,
            expanded,
        );
        (state.path.clone(), serialize_state(&state.state)?)
    };

    write_state_file(path.as_path(), serialized)?;
    Ok(())
}

fn thread_folders_expanded_from_state(
    state: &DesktopStateFile,
    workspace_id: Option<&str>,
) -> HashMap<String, bool> {
    if let Some(expanded) = workspace_id.and_then(|workspace_id| {
        state
            .sidebar
            .threads
            .workspaces
            .get(workspace_id)
            .map(|workspace| workspace.folders.expanded.clone())
    }) {
        return expanded;
    }

    state.sidebar.threads.folders.expanded.clone()
}

fn set_thread_folders_expanded_for_workspace_in_state(
    state: &mut DesktopStateFile,
    workspace_id: &str,
    expanded: HashMap<String, bool>,
) {
    state
        .sidebar
        .threads
        .workspaces
        .entry(workspace_id.to_owned())
        .or_default()
        .folders
        .expanded = expanded;
}

fn ensure_loaded(cx: &mut App) -> Result<()> {
    if cx.has_global::<DesktopStateStore>() {
        return Ok(());
    }

    cx.set_global(DesktopStateStore::load()?);
    Ok(())
}

impl DesktopStateStore {
    fn load() -> Result<Self> {
        let path = state_path()?;

        let state = if path.is_file() {
            load_state_file_from_path(path.as_path())?
        } else {
            DesktopStateFile {
                version: DESKTOP_STATE_VERSION,
                ..Default::default()
            }
        };

        Ok(Self { path, state })
    }
}

fn state_path() -> Result<PathBuf> {
    Ok(runtime_home_dir()?.join(DESKTOP_STATE_FILE_NAME))
}

pub(crate) fn runtime_home_dir() -> Result<PathBuf> {
    let config = AppConfig::load().context("failed to load app config for desktop state")?;
    config
        .ensure_runtime_home_dir()
        .context("failed to ensure runtime home dir for desktop state")
}

fn load_state_file_from_path(path: &std::path::Path) -> Result<DesktopStateFile> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read desktop state `{}`", path.display()))?;
    toml::from_str::<DesktopStateFile>(raw.as_str())
        .with_context(|| format!("failed to parse desktop state `{}`", path.display()))
}

fn serialize_state(state: &DesktopStateFile) -> Result<String> {
    toml::to_string_pretty(state).context("failed to serialize desktop state")
}

fn write_state_file(path: &std::path::Path, serialized: String) -> Result<()> {
    fs::write(path, serialized)
        .with_context(|| format!("failed to write desktop state `{}`", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expanded(entries: &[(&str, bool)]) -> HashMap<String, bool> {
        entries
            .iter()
            .map(|(folder_id, expanded)| ((*folder_id).to_owned(), *expanded))
            .collect()
    }

    #[::core::prelude::v1::test]
    fn thread_folders_expanded_reads_legacy_global_shape_as_workspace_fallback() {
        let state: DesktopStateFile = toml::from_str(
            r#"
version = 1

[sidebar.threads.folders.expanded]
fld_legacy = true
"#,
        )
        .expect("legacy desktop state should parse");

        assert_eq!(
            thread_folders_expanded_from_state(&state, Some("ws_a")),
            expanded(&[("fld_legacy", true)])
        );
    }

    #[::core::prelude::v1::test]
    fn thread_folders_expanded_prefers_workspace_shape_over_legacy_global_shape() {
        let state: DesktopStateFile = toml::from_str(
            r#"
version = 1

[sidebar.threads.folders.expanded]
fld_legacy = true

[sidebar.threads.workspaces.ws_a.folders.expanded]
fld_a = true
"#,
        )
        .expect("workspace desktop state should parse");

        assert_eq!(
            thread_folders_expanded_from_state(&state, Some("ws_a")),
            expanded(&[("fld_a", true)])
        );
    }

    #[::core::prelude::v1::test]
    fn thread_folders_expanded_writes_workspace_scoped_shape() {
        let mut state = DesktopStateFile::default();

        set_thread_folders_expanded_for_workspace_in_state(
            &mut state,
            "ws_a",
            expanded(&[("fld_a", true), ("fld_b", false)]),
        );

        assert_eq!(
            thread_folders_expanded_from_state(&state, Some("ws_a")),
            expanded(&[("fld_a", true), ("fld_b", false)])
        );

        let serialized = serialize_state(&state).expect("state should serialize");
        assert!(serialized.contains("[sidebar.threads.workspaces.ws_a.folders.expanded]"));
        assert!(!serialized.contains("[sidebar.threads.folders.expanded]\nfld_a"));
    }

    #[::core::prelude::v1::test]
    fn thread_folders_expanded_loads_requested_workspace_set() {
        let mut state = DesktopStateFile::default();
        set_thread_folders_expanded_for_workspace_in_state(
            &mut state,
            "ws_a",
            expanded(&[("fld_a", true)]),
        );
        set_thread_folders_expanded_for_workspace_in_state(
            &mut state,
            "ws_b",
            expanded(&[("fld_b", true)]),
        );

        assert_eq!(
            thread_folders_expanded_from_state(&state, Some("ws_a")),
            expanded(&[("fld_a", true)])
        );
        assert_eq!(
            thread_folders_expanded_from_state(&state, Some("ws_b")),
            expanded(&[("fld_b", true)])
        );
    }
}
