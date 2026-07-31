//! Artifact read model state.

use crate::artifacts::{
    actions::{
        ArtifactActionStatus, ArtifactLocalFile, ArtifactVersionKey, ThreadArtifactActionState,
        artifact_version_key,
    },
    preview::{ArtifactPreviewImagePaths, ThreadArtifactPreviewState},
};
use pioneer_protocol::{
    ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind, ArtifactListForThreadParams,
    ArtifactRef, ArtifactSummary,
};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::{Duration, Instant},
};

const THREAD_ARTIFACTS_TRANSIENT_RETRY_LIMIT: u8 = 5;
const THREAD_ARTIFACTS_TRANSIENT_RETRY_DELAYS_MS: [u64; 5] = [250, 500, 1_000, 2_000, 4_000];
pub const THREAD_ARTIFACT_LIST_LIMIT: u64 = 250;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ThreadArtifactFilter {
    #[default]
    All,
    Uploaded,
    Generated,
    TaskOutput,
    Images,
    Documents,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ThreadArtifactCacheEntry {
    pub items: Vec<ArtifactSummary>,
    pub loaded: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ThreadArtifactsState {
    cache: ThreadArtifactCacheState,
    preview: ThreadArtifactPreviewState,
    actions: ThreadArtifactActionState,
    local_files_by_artifact: HashMap<ArtifactVersionKey, ArtifactLocalFile>,
}

#[derive(Clone, Debug, Default)]
pub struct ThreadArtifactCacheState {
    active_thread_id: Option<String>,
    loading: bool,
    loading_thread_id: Option<String>,
    loading_thread_ids: HashSet<String>,
    refresh_requested_thread_ids: HashSet<String>,
    retry_after_by_thread: HashMap<String, Instant>,
    transient_retry_count_by_thread: HashMap<String, u8>,
    error: Option<String>,
    selected_artifact_id: Option<String>,
    filter: ThreadArtifactFilter,
    cache_by_thread: HashMap<String, ThreadArtifactCacheEntry>,
}

impl ThreadArtifactFilter {
    pub const fn all() -> [Self; 6] {
        [
            Self::All,
            Self::Uploaded,
            Self::Generated,
            Self::TaskOutput,
            Self::Images,
            Self::Documents,
        ]
    }
}

pub fn artifact_list_for_thread_params(
    workspace_id: impl Into<String>,
    thread_id: impl Into<String>,
) -> ArtifactListForThreadParams {
    ArtifactListForThreadParams {
        workspace_id: workspace_id.into(),
        thread_id: Some(thread_id.into()),
        turn_id: None,
        message_id: None,
        task_id: None,
        task_run_id: None,
        kinds: Vec::new(),
        include_deleted: false,
        cursor: None,
        limit: Some(THREAD_ARTIFACT_LIST_LIMIT),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadArtifactsRefreshRequest {
    pub connection_id: u64,
    pub params: ArtifactListForThreadParams,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadArtifactsRefreshPlan {
    Send(ThreadArtifactsRefreshRequest),
    RequestRefreshAfterCurrent,
    ClearError,
    Skip,
}

pub fn plan_thread_artifacts_refresh(
    state: &ThreadArtifactsState,
    thread_id: &str,
    force: bool,
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
    thread_materialized: bool,
) -> ThreadArtifactsRefreshPlan {
    if !gateway_connected {
        return ThreadArtifactsRefreshPlan::Skip;
    }
    if !thread_materialized {
        return ThreadArtifactsRefreshPlan::ClearError;
    }
    let Some(connection_id) = connection_id else {
        return ThreadArtifactsRefreshPlan::Skip;
    };
    let Some(workspace_id) = workspace_id else {
        return ThreadArtifactsRefreshPlan::Skip;
    };
    if !force && !state.needs_load(thread_id) {
        return ThreadArtifactsRefreshPlan::Skip;
    }
    if state.is_loading_thread(thread_id) {
        return if force {
            ThreadArtifactsRefreshPlan::RequestRefreshAfterCurrent
        } else {
            ThreadArtifactsRefreshPlan::Skip
        };
    }

    ThreadArtifactsRefreshPlan::Send(ThreadArtifactsRefreshRequest {
        connection_id,
        params: artifact_list_for_thread_params(workspace_id, thread_id.to_owned()),
    })
}

impl ThreadArtifactsState {
    pub fn activate_thread(&mut self, thread_id: Option<&str>) {
        self.cache.activate_thread(thread_id);
    }

    pub fn needs_load(&self, thread_id: &str) -> bool {
        self.cache.needs_load(thread_id)
    }

    pub fn is_loading_thread(&self, thread_id: &str) -> bool {
        self.cache.is_loading_thread(thread_id)
    }

    pub fn request_refresh_after_current(&mut self, thread_id: &str) {
        self.cache.request_refresh_after_current(thread_id);
    }

    pub fn take_refresh_after_current(&mut self, thread_id: &str) -> bool {
        self.cache.take_refresh_after_current(thread_id)
    }

    pub fn mark_loading(&mut self, thread_id: &str) {
        self.cache.mark_loading(thread_id);
    }

    pub fn apply_loaded(&mut self, thread_id: &str, items: Vec<ArtifactSummary>) {
        self.cache.apply_loaded(thread_id, items);
    }

    pub fn apply_failed(&mut self, thread_id: &str, error: String) {
        self.cache.apply_failed(thread_id, error);
    }

    pub fn defer_transient_load_retry(&mut self, thread_id: &str) -> Option<Duration> {
        self.cache.defer_transient_load_retry(thread_id)
    }

    pub fn clear_error(&mut self, thread_id: &str) {
        self.cache.clear_error(thread_id);
    }

    /// Removes every in-memory artifact projection owned by the supplied
    /// threads while preserving unrelated thread caches.
    ///
    /// Access-loss handling intentionally drops preview paths, action state,
    /// and local-file references together with the summaries so no shell can
    /// continue presenting a protected artifact through a secondary cache.
    pub fn remove_threads(&mut self, thread_ids: &[String]) {
        let removed_artifact_keys = self.cache.remove_threads(thread_ids);
        self.preview.remove_keys(&removed_artifact_keys);
        self.actions.remove_keys(&removed_artifact_keys);
        self.local_files_by_artifact
            .retain(|key, _| !removed_artifact_keys.contains(key));
    }

    pub fn set_filter(&mut self, filter: ThreadArtifactFilter) {
        self.cache.set_filter(filter);
    }

    pub fn select_artifact(&mut self, artifact_id: String) {
        self.cache.select_artifact(artifact_id);
    }

    pub fn items_for_active_thread(&self) -> &[ArtifactSummary] {
        self.cache.items_for_active_thread()
    }

    pub fn visible_items(&self) -> Vec<&ArtifactSummary> {
        self.cache.visible_items()
    }

    pub fn active_thread_id(&self) -> Option<&str> {
        self.cache.active_thread_id()
    }

    pub fn loading(&self) -> bool {
        self.cache.loading()
    }

    pub fn loading_thread_id(&self) -> Option<&str> {
        self.cache.loading_thread_id()
    }

    pub fn error(&self) -> Option<&str> {
        self.cache.error()
    }

    pub fn selected_artifact_id(&self) -> Option<&str> {
        self.cache.selected_artifact_id()
    }

    pub fn filter(&self) -> ThreadArtifactFilter {
        self.cache.filter()
    }

    pub fn is_loading_active_thread(&self) -> bool {
        self.loading()
            && self.loading_thread_id() == self.active_thread_id()
            && self.active_thread_id().is_some()
    }

    pub fn local_file(&self, artifact: &ArtifactRef) -> Option<&ArtifactLocalFile> {
        self.local_files_by_artifact
            .get(&artifact_version_key(artifact))
    }

    pub fn set_local_file(&mut self, artifact: &ArtifactRef, local_file: ArtifactLocalFile) {
        self.local_files_by_artifact
            .insert(artifact_version_key(artifact), local_file);
    }

    pub fn clear_local_file(&mut self, artifact: &ArtifactRef) {
        self.local_files_by_artifact
            .remove(&artifact_version_key(artifact));
    }

    pub fn action_status(&self, artifact: &ArtifactRef) -> Option<&ArtifactActionStatus> {
        self.actions.status(artifact)
    }

    pub fn set_action_status(&mut self, artifact: &ArtifactRef, status: ArtifactActionStatus) {
        self.actions.set_status(artifact, status);
    }

    pub fn clear_action_status(&mut self, artifact: &ArtifactRef) {
        self.actions.clear_status(artifact);
    }

    pub fn action_in_progress(&self, artifact: &ArtifactRef) -> bool {
        self.actions.in_progress(artifact)
    }

    pub fn preview_square_image_path(&self, artifact: &ArtifactRef) -> Option<&Path> {
        self.preview.square_image_path(artifact)
    }

    pub fn preview_detail_image_path(&self, artifact: &ArtifactRef) -> Option<&Path> {
        self.preview.detail_image_path(artifact)
    }

    pub fn has_loadable_preview(&self, artifact: &ArtifactRef) -> bool {
        self.preview.has_loadable_preview(artifact)
    }

    pub fn should_load_preview(&self, artifact: &ArtifactRef) -> bool {
        self.preview.should_load_preview(artifact)
    }

    pub fn mark_preview_loading_if_needed(&mut self, artifact: &ArtifactRef) -> bool {
        self.preview.mark_loading_if_needed(artifact)
    }

    pub fn apply_preview_loaded(
        &mut self,
        artifact: &ArtifactRef,
        image_paths: ArtifactPreviewImagePaths,
    ) {
        self.preview.apply_loaded(artifact, image_paths);
    }

    pub fn apply_preview_failed(&mut self, artifact: &ArtifactRef) {
        self.preview.apply_failed(artifact);
    }
}

impl ThreadArtifactCacheState {
    pub fn active_thread_id(&self) -> Option<&str> {
        self.active_thread_id.as_deref()
    }

    pub fn loading(&self) -> bool {
        self.loading
    }

    pub fn loading_thread_id(&self) -> Option<&str> {
        self.loading_thread_id.as_deref()
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn selected_artifact_id(&self) -> Option<&str> {
        self.selected_artifact_id.as_deref()
    }

    pub fn filter(&self) -> ThreadArtifactFilter {
        self.filter
    }

    pub fn activate_thread(&mut self, thread_id: Option<&str>) {
        let next_thread_id = thread_id.map(str::to_owned);
        if self.active_thread_id != next_thread_id {
            self.error = next_thread_id
                .as_deref()
                .and_then(|thread_id| self.cache_by_thread.get(thread_id))
                .and_then(|entry| entry.error.clone());
            self.active_thread_id = next_thread_id;
            self.selected_artifact_id = None;
        }
    }

    pub fn needs_load(&self, thread_id: &str) -> bool {
        if self.loading_thread_ids.contains(thread_id) {
            return false;
        }
        if self
            .retry_after_by_thread
            .get(thread_id)
            .is_some_and(|retry_after| Instant::now() < *retry_after)
        {
            return false;
        }

        !self
            .cache_by_thread
            .get(thread_id)
            .is_some_and(|entry| entry.loaded || entry.error.is_some())
    }

    pub fn is_loading_thread(&self, thread_id: &str) -> bool {
        self.loading_thread_ids.contains(thread_id)
    }

    pub fn request_refresh_after_current(&mut self, thread_id: &str) {
        self.refresh_requested_thread_ids
            .insert(thread_id.to_owned());
    }

    pub fn take_refresh_after_current(&mut self, thread_id: &str) -> bool {
        self.refresh_requested_thread_ids.remove(thread_id)
    }

    pub fn mark_loading(&mut self, thread_id: &str) {
        self.retry_after_by_thread.remove(thread_id);
        self.loading_thread_ids.insert(thread_id.to_owned());
        self.sync_loading_state(thread_id);
        self.loading_thread_id = Some(thread_id.to_owned());
        self.error = None;
        let entry = self
            .cache_by_thread
            .entry(thread_id.to_owned())
            .or_default();
        entry.error = None;
    }

    pub fn apply_loaded(&mut self, thread_id: &str, items: Vec<ArtifactSummary>) {
        self.retry_after_by_thread.remove(thread_id);
        self.transient_retry_count_by_thread.remove(thread_id);
        self.cache_by_thread.insert(
            thread_id.to_owned(),
            ThreadArtifactCacheEntry {
                items,
                loaded: true,
                error: None,
            },
        );
        if self.loading_thread_id.as_deref() == Some(thread_id) {
            self.loading_thread_id = None;
        }
        self.loading_thread_ids.remove(thread_id);
        self.sync_loading_state(thread_id);
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.error = None;
            self.ensure_selected_artifact_exists();
        }
    }

    pub fn apply_failed(&mut self, thread_id: &str, error: String) {
        self.retry_after_by_thread.remove(thread_id);
        self.transient_retry_count_by_thread.remove(thread_id);
        let entry = self
            .cache_by_thread
            .entry(thread_id.to_owned())
            .or_default();
        entry.loaded = false;
        entry.error = Some(error.clone());
        if self.loading_thread_id.as_deref() == Some(thread_id) {
            self.loading_thread_id = None;
        }
        self.loading_thread_ids.remove(thread_id);
        self.sync_loading_state(thread_id);
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.error = Some(error);
        }
    }

    pub fn defer_transient_load_retry(&mut self, thread_id: &str) -> Option<Duration> {
        let retry_count = self
            .transient_retry_count_by_thread
            .entry(thread_id.to_owned())
            .or_default();
        if *retry_count >= THREAD_ARTIFACTS_TRANSIENT_RETRY_LIMIT {
            return None;
        }

        let delay = thread_artifacts_transient_retry_delay(*retry_count);
        *retry_count = retry_count.saturating_add(1);
        self.retry_after_by_thread
            .insert(thread_id.to_owned(), Instant::now() + delay);

        if let Some(entry) = self.cache_by_thread.get_mut(thread_id) {
            entry.loaded = false;
            entry.error = None;
        }
        if self.loading_thread_id.as_deref() == Some(thread_id) {
            self.loading_thread_id = None;
        }
        self.loading_thread_ids.remove(thread_id);
        self.sync_loading_state(thread_id);
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.error = None;
        }

        Some(delay)
    }

    pub fn clear_error(&mut self, thread_id: &str) {
        self.retry_after_by_thread.remove(thread_id);
        if let Some(entry) = self.cache_by_thread.get_mut(thread_id) {
            entry.error = None;
        }
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.error = None;
        }
    }

    fn remove_threads(&mut self, thread_ids: &[String]) -> HashSet<ArtifactVersionKey> {
        let thread_ids = thread_ids
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        if thread_ids.is_empty() {
            return HashSet::new();
        }

        let mut removed_artifact_keys = HashSet::new();
        for thread_id in &thread_ids {
            if let Some(entry) = self.cache_by_thread.remove(*thread_id) {
                removed_artifact_keys.extend(
                    entry
                        .items
                        .iter()
                        .map(|summary| artifact_version_key(&summary.artifact)),
                );
            }
            self.loading_thread_ids.remove(*thread_id);
            self.refresh_requested_thread_ids.remove(*thread_id);
            self.retry_after_by_thread.remove(*thread_id);
            self.transient_retry_count_by_thread.remove(*thread_id);
        }

        if self
            .active_thread_id
            .as_deref()
            .is_some_and(|thread_id| thread_ids.contains(thread_id))
        {
            self.active_thread_id = None;
            self.selected_artifact_id = None;
            self.error = None;
        }
        if self
            .loading_thread_id
            .as_deref()
            .is_some_and(|thread_id| thread_ids.contains(thread_id))
        {
            self.loading_thread_id = None;
        }
        self.loading = !self.loading_thread_ids.is_empty();

        removed_artifact_keys
    }

    pub fn set_filter(&mut self, filter: ThreadArtifactFilter) {
        self.filter = filter;
        self.ensure_selected_artifact_exists();
    }

    pub fn select_artifact(&mut self, artifact_id: String) {
        self.selected_artifact_id = Some(artifact_id);
    }

    pub fn items_for_active_thread(&self) -> &[ArtifactSummary] {
        let Some(thread_id) = self.active_thread_id.as_deref() else {
            return &[];
        };
        self.cache_by_thread
            .get(thread_id)
            .map(|entry| entry.items.as_slice())
            .unwrap_or(&[])
    }

    pub fn visible_items(&self) -> Vec<&ArtifactSummary> {
        self.items_for_active_thread()
            .iter()
            .filter(|artifact| artifact_matches_filter(artifact, self.filter))
            .collect()
    }

    pub fn selected_artifact(&self) -> Option<&ArtifactSummary> {
        let selected_artifact_id = self.selected_artifact_id.as_deref()?;
        self.items_for_active_thread()
            .iter()
            .find(|summary| summary.artifact.artifact_id == selected_artifact_id)
    }

    fn ensure_selected_artifact_exists(&mut self) {
        if self.selected_artifact_id.is_none() {
            return;
        }
        if self
            .selected_artifact()
            .is_none_or(|summary| !artifact_matches_filter(summary, self.filter))
        {
            self.selected_artifact_id = None;
        }
    }

    fn sync_loading_state(&mut self, changed_thread_id: &str) {
        self.loading = !self.loading_thread_ids.is_empty();
        if self
            .loading_thread_id
            .as_deref()
            .is_some_and(|thread_id| self.loading_thread_ids.contains(thread_id))
        {
            return;
        }
        self.loading_thread_id = if self.loading_thread_ids.contains(changed_thread_id) {
            Some(changed_thread_id.to_owned())
        } else {
            self.loading_thread_ids.iter().next().cloned()
        };
    }
}

pub fn is_artifact_thread_not_found_error(thread_id: &str, error: &str) -> bool {
    let needle = format!("thread `{thread_id}` not found");
    error.contains(needle.as_str())
}

pub fn artifact_matches_filter(summary: &ArtifactSummary, filter: ThreadArtifactFilter) -> bool {
    match filter {
        ThreadArtifactFilter::All => true,
        ThreadArtifactFilter::Uploaded => matches!(
            summary.created_by_kind,
            ArtifactCreatedByKind::User | ArtifactCreatedByKind::Import
        ),
        ThreadArtifactFilter::Generated => matches!(
            summary.created_by_kind,
            ArtifactCreatedByKind::Agent
                | ArtifactCreatedByKind::Tool
                | ArtifactCreatedByKind::System
                | ArtifactCreatedByKind::ExternalAgent
        ),
        ThreadArtifactFilter::TaskOutput => {
            summary.created_by_kind == ArtifactCreatedByKind::Task
                || summary
                    .bindings
                    .iter()
                    .any(|binding| binding.binding_kind == ArtifactBindingKind::TaskResult)
        }
        ThreadArtifactFilter::Images => matches!(
            summary.artifact.kind,
            ArtifactKind::Image | ArtifactKind::GeneratedImage | ArtifactKind::Screenshot
        ),
        ThreadArtifactFilter::Documents => matches!(
            summary.artifact.kind,
            ArtifactKind::File
                | ArtifactKind::Text
                | ArtifactKind::Pdf
                | ArtifactKind::Spreadsheet
                | ArtifactKind::Json
                | ArtifactKind::WorkspaceFile
                | ArtifactKind::DirectoryManifest
        ),
    }
}

fn thread_artifacts_transient_retry_delay(retry_count: u8) -> Duration {
    THREAD_ARTIFACTS_TRANSIENT_RETRY_DELAYS_MS
        .get(retry_count as usize)
        .copied()
        .map(Duration::from_millis)
        .unwrap_or_else(|| {
            Duration::from_millis(
                *THREAD_ARTIFACTS_TRANSIENT_RETRY_DELAYS_MS
                    .last()
                    .unwrap_or(&4_000),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ArtifactBindingDirection, ArtifactBindingSummary, ArtifactRef, ArtifactRole, ArtifactStatus,
    };
    use std::collections::BTreeMap;

    #[test]
    fn artifacts_state_loads_stores_selects_and_filters_items() {
        let uploaded = artifact_summary(
            "art_upload",
            "upload.txt",
            ArtifactKind::Text,
            ArtifactCreatedByKind::User,
            Vec::new(),
        );
        let task_output = artifact_summary(
            "art_task",
            "task.json",
            ArtifactKind::Json,
            ArtifactCreatedByKind::Agent,
            vec![ArtifactBindingSummary {
                binding_id: "bind_task".to_owned(),
                workspace_id: "ws".to_owned(),
                thread_id: Some("thread".to_owned()),
                turn_id: Some("turn".to_owned()),
                message_id: None,
                turn_item_id: None,
                tool_call_id: None,
                task_id: Some("task".to_owned()),
                task_run_id: Some("run".to_owned()),
                binding_kind: ArtifactBindingKind::TaskResult,
                direction: ArtifactBindingDirection::Output,
                item_index: None,
                role: Some(ArtifactRole::Task),
                created_at: 1,
            }],
        );

        let mut state = ThreadArtifactCacheState::default();
        state.activate_thread(Some("thread"));
        state.mark_loading("thread");
        state.apply_loaded("thread", vec![uploaded, task_output]);

        assert!(!state.loading());
        assert_eq!(state.items_for_active_thread().len(), 2);

        state.select_artifact("art_task".to_owned());
        assert_eq!(
            state
                .selected_artifact()
                .map(|item| item.artifact.artifact_id.as_str()),
            Some("art_task")
        );

        state.set_filter(ThreadArtifactFilter::TaskOutput);
        assert_eq!(state.visible_items().len(), 1);
        assert_eq!(
            state.visible_items()[0].artifact.artifact_id.as_str(),
            "art_task"
        );
    }

    #[test]
    fn artifacts_state_clears_selected_artifact_when_filter_hides_it() {
        let uploaded = artifact_summary(
            "art_upload",
            "upload.txt",
            ArtifactKind::Text,
            ArtifactCreatedByKind::User,
            Vec::new(),
        );
        let generated = artifact_summary(
            "art_generated",
            "generated.png",
            ArtifactKind::Image,
            ArtifactCreatedByKind::Agent,
            Vec::new(),
        );

        let mut state = ThreadArtifactCacheState::default();
        state.activate_thread(Some("thread"));
        state.apply_loaded("thread", vec![uploaded, generated]);
        state.select_artifact("art_upload".to_owned());

        state.set_filter(ThreadArtifactFilter::Generated);

        assert!(state.selected_artifact().is_none());
        assert_eq!(state.visible_items().len(), 1);
        assert_eq!(
            state.visible_items()[0].artifact.artifact_id.as_str(),
            "art_generated"
        );
    }

    #[test]
    fn artifacts_state_keeps_failed_load_for_active_thread() {
        let mut state = ThreadArtifactCacheState::default();
        state.activate_thread(Some("thread"));
        state.mark_loading("thread");
        state.apply_failed("thread", "boom".to_owned());

        assert!(!state.loading());
        assert_eq!(state.error(), Some("boom"));
        assert!(!state.needs_load("thread"));

        state.mark_loading("thread");
        assert!(state.loading());
        assert!(state.error().is_none());
    }

    #[test]
    fn artifacts_state_defers_transient_load_retry_without_caching_error() {
        let mut state = ThreadArtifactCacheState::default();
        state.activate_thread(Some("thread"));
        state.mark_loading("thread");

        let delay = state.defer_transient_load_retry("thread");

        assert_eq!(delay, Some(Duration::from_millis(250)));
        assert!(!state.loading());
        assert!(state.error().is_none());
        assert!(!state.needs_load("thread"));

        state.retry_after_by_thread.insert(
            "thread".to_owned(),
            Instant::now() - Duration::from_millis(1),
        );
        assert!(state.needs_load("thread"));
    }

    #[test]
    fn artifacts_state_caps_transient_load_retries() {
        let mut state = ThreadArtifactCacheState::default();
        state.activate_thread(Some("thread"));

        for _ in 0..THREAD_ARTIFACTS_TRANSIENT_RETRY_LIMIT {
            state.mark_loading("thread");
            assert!(state.defer_transient_load_retry("thread").is_some());
            state.retry_after_by_thread.insert(
                "thread".to_owned(),
                Instant::now() - Duration::from_millis(1),
            );
        }

        state.mark_loading("thread");
        assert!(state.defer_transient_load_retry("thread").is_none());
    }

    #[test]
    fn artifact_thread_not_found_detection_is_thread_scoped() {
        assert!(is_artifact_thread_not_found_error(
            "thread_a",
            "thread `thread_a` not found"
        ));
        assert!(!is_artifact_thread_not_found_error(
            "thread_a",
            "thread `thread_b` not found"
        ));
    }

    #[test]
    fn artifact_refresh_plan_requires_connection_and_materialized_thread() {
        let state = ThreadArtifactsState::default();

        assert_eq!(
            plan_thread_artifacts_refresh(
                &state,
                "thread",
                false,
                false,
                Some(7),
                Some("ws".to_owned()),
                true,
            ),
            ThreadArtifactsRefreshPlan::Skip
        );
        assert_eq!(
            plan_thread_artifacts_refresh(
                &state,
                "thread",
                false,
                true,
                Some(7),
                Some("ws".to_owned()),
                false,
            ),
            ThreadArtifactsRefreshPlan::ClearError
        );
        assert_eq!(
            plan_thread_artifacts_refresh(
                &state,
                "thread",
                false,
                true,
                None,
                Some("ws".to_owned()),
                true,
            ),
            ThreadArtifactsRefreshPlan::Skip
        );
    }

    #[test]
    fn artifact_refresh_plan_builds_request_or_defers_force_refresh() {
        let mut state = ThreadArtifactsState::default();

        let plan = plan_thread_artifacts_refresh(
            &state,
            "thread",
            false,
            true,
            Some(7),
            Some("ws".to_owned()),
            true,
        );
        let ThreadArtifactsRefreshPlan::Send(request) = plan else {
            panic!("expected artifact refresh request");
        };
        assert_eq!(request.connection_id, 7);
        assert_eq!(request.params.workspace_id, "ws");
        assert_eq!(request.params.thread_id.as_deref(), Some("thread"));

        state.mark_loading("thread");
        assert_eq!(
            plan_thread_artifacts_refresh(
                &state,
                "thread",
                false,
                true,
                Some(7),
                Some("ws".to_owned()),
                true,
            ),
            ThreadArtifactsRefreshPlan::Skip
        );
        assert_eq!(
            plan_thread_artifacts_refresh(
                &state,
                "thread",
                true,
                true,
                Some(7),
                Some("ws".to_owned()),
                true,
            ),
            ThreadArtifactsRefreshPlan::RequestRefreshAfterCurrent
        );
    }

    #[test]
    fn access_loss_removes_only_invalidated_thread_artifact_projections() {
        let protected = artifact_summary(
            "art_protected",
            "protected.txt",
            ArtifactKind::Text,
            ArtifactCreatedByKind::Agent,
            Vec::new(),
        );
        let unrelated = artifact_summary(
            "art_unrelated",
            "unrelated.txt",
            ArtifactKind::Text,
            ArtifactCreatedByKind::Agent,
            Vec::new(),
        );
        let protected_ref = protected.artifact.clone();
        let unrelated_ref = unrelated.artifact.clone();
        let mut state = ThreadArtifactsState::default();
        state.apply_loaded("thread_protected", vec![protected]);
        state.apply_loaded("thread_unrelated", vec![unrelated]);
        state.set_action_status(&protected_ref, ArtifactActionStatus::Queued);
        state.set_action_status(&unrelated_ref, ArtifactActionStatus::Queued);
        state.set_local_file(
            &protected_ref,
            ArtifactLocalFile {
                path: "protected-cache".into(),
                sha256: "protected".to_owned(),
                size_bytes: Some(1),
            },
        );
        state.set_local_file(
            &unrelated_ref,
            ArtifactLocalFile {
                path: "unrelated-cache".into(),
                sha256: "unrelated".to_owned(),
                size_bytes: Some(1),
            },
        );
        state.activate_thread(Some("thread_unrelated"));

        state.remove_threads(&["thread_protected".to_owned()]);

        assert!(state.needs_load("thread_protected"));
        assert!(!state.needs_load("thread_unrelated"));
        assert_eq!(state.items_for_active_thread().len(), 1);
        assert!(state.action_status(&protected_ref).is_none());
        assert!(state.local_file(&protected_ref).is_none());
        assert_eq!(
            state.action_status(&unrelated_ref),
            Some(&ArtifactActionStatus::Queued)
        );
        assert!(state.local_file(&unrelated_ref).is_some());
    }

    fn artifact_summary(
        artifact_id: &str,
        display_name: &str,
        kind: ArtifactKind,
        created_by_kind: ArtifactCreatedByKind,
        bindings: Vec<ArtifactBindingSummary>,
    ) -> ArtifactSummary {
        ArtifactSummary {
            artifact: ArtifactRef {
                artifact_id: artifact_id.to_owned(),
                version_id: Some(format!("{artifact_id}_v1")),
                display_name: display_name.to_owned(),
                kind,
                mime_type: None,
                size_bytes: Some(2048),
                sha256: None,
                status: ArtifactStatus::Ready,
                preview: None,
            },
            workspace_id: "ws".to_owned(),
            primary_thread_id: Some("thread".to_owned()),
            created_by_kind,
            created_by_actor_id: None,
            created_at: 1,
            updated_at: 1,
            bindings,
            metadata: BTreeMap::new(),
        }
    }
}
