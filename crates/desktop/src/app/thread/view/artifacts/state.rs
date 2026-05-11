use crate::{
    app::root::{
        GatewayConnectionState, PioneerDesktop, ThreadArtifactActionStatus,
        ThreadArtifactCacheEntry, ThreadArtifactFilter, ThreadArtifactLocalFile,
        ThreadArtifactPreviewImagePaths, ThreadArtifactVersionKey, ThreadArtifactsState,
    },
    state as desktop_state,
};
use anyhow::{Context as _, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use gpui::{AppContext, AsyncApp, Context, WeakEntity};
use image::{DynamicImage, GenericImageView as _, ImageFormat, imageops::FilterType};
use pioneer_protocol::{
    ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind, ArtifactListForThreadParams,
    ArtifactProjectionKind, ArtifactProjectionStatus, ArtifactReadParams, ArtifactReadResponse,
    ArtifactRef, ArtifactSummary, ThreadArtifactsChangedNotification,
};
use std::{
    fs,
    io::Cursor,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};
use tracing::{debug, warn};

const THREAD_ARTIFACTS_TRANSIENT_RETRY_LIMIT: u8 = 5;
const THREAD_ARTIFACTS_TRANSIENT_RETRY_DELAYS_MS: [u64; 5] = [250, 500, 1_000, 2_000, 4_000];
const THREAD_ARTIFACT_PREVIEW_MAX_BYTES: u64 = 512 * 1024;
const THREAD_ARTIFACT_PREVIEW_CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024;
const THREAD_ARTIFACT_PREVIEW_SQUARE_EDGE_PX: u32 = 128;
const THREAD_ARTIFACT_PREVIEW_DETAIL_WIDTH_PX: u32 = 640;
const THREAD_ARTIFACT_PREVIEW_DETAIL_HEIGHT_PX: u32 = 320;

impl ThreadArtifactFilter {
    pub(super) const fn all() -> [Self; 6] {
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

impl ThreadArtifactsState {
    pub(super) fn activate_thread(&mut self, thread_id: Option<&str>) {
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

    pub(super) fn needs_load(&self, thread_id: &str) -> bool {
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

    pub(super) fn is_loading_thread(&self, thread_id: &str) -> bool {
        self.loading_thread_ids.contains(thread_id)
    }

    pub(super) fn request_refresh_after_current(&mut self, thread_id: &str) {
        self.refresh_requested_thread_ids
            .insert(thread_id.to_owned());
    }

    pub(super) fn take_refresh_after_current(&mut self, thread_id: &str) -> bool {
        self.refresh_requested_thread_ids.remove(thread_id)
    }

    pub(super) fn mark_loading(&mut self, thread_id: &str) {
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

    pub(super) fn apply_loaded(&mut self, thread_id: &str, items: Vec<ArtifactSummary>) {
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

    pub(super) fn apply_failed(&mut self, thread_id: &str, error: String) {
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

    pub(super) fn defer_transient_load_retry(&mut self, thread_id: &str) -> Option<Duration> {
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

    pub(super) fn clear_error(&mut self, thread_id: &str) {
        self.retry_after_by_thread.remove(thread_id);
        if let Some(entry) = self.cache_by_thread.get_mut(thread_id) {
            entry.error = None;
        }
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.error = None;
        }
    }

    pub(super) fn set_filter(&mut self, filter: ThreadArtifactFilter) {
        self.filter = filter;
        self.ensure_selected_artifact_exists();
    }

    pub(super) fn select_artifact(&mut self, artifact_id: String) {
        self.selected_artifact_id = Some(artifact_id);
    }

    pub(super) fn items_for_active_thread(&self) -> &[ArtifactSummary] {
        let Some(thread_id) = self.active_thread_id.as_deref() else {
            return &[];
        };
        self.cache_by_thread
            .get(thread_id)
            .map(|entry| entry.items.as_slice())
            .unwrap_or(&[])
    }

    pub(super) fn visible_items(&self) -> Vec<&ArtifactSummary> {
        self.items_for_active_thread()
            .iter()
            .filter(|artifact| artifact_matches_filter(artifact, self.filter))
            .collect()
    }

    pub(super) fn selected_artifact(&self) -> Option<&ArtifactSummary> {
        let selected_artifact_id = self.selected_artifact_id.as_deref()?;
        self.items_for_active_thread()
            .iter()
            .find(|summary| summary.artifact.artifact_id == selected_artifact_id)
    }

    pub(super) fn local_file(&self, artifact: &ArtifactRef) -> Option<&ThreadArtifactLocalFile> {
        self.local_files_by_artifact
            .get(&thread_artifact_version_key(artifact))
    }

    pub(super) fn set_local_file(
        &mut self,
        artifact: &ArtifactRef,
        local_file: ThreadArtifactLocalFile,
    ) {
        self.local_files_by_artifact
            .insert(thread_artifact_version_key(artifact), local_file);
    }

    pub(super) fn clear_local_file(&mut self, artifact: &ArtifactRef) {
        self.local_files_by_artifact
            .remove(&thread_artifact_version_key(artifact));
    }

    pub(super) fn action_status(
        &self,
        artifact: &ArtifactRef,
    ) -> Option<&ThreadArtifactActionStatus> {
        self.action_status_by_artifact
            .get(&thread_artifact_version_key(artifact))
    }

    pub(super) fn set_action_status(
        &mut self,
        artifact: &ArtifactRef,
        status: ThreadArtifactActionStatus,
    ) {
        self.action_status_by_artifact
            .insert(thread_artifact_version_key(artifact), status);
    }

    pub(super) fn clear_action_status(&mut self, artifact: &ArtifactRef) {
        self.action_status_by_artifact
            .remove(&thread_artifact_version_key(artifact));
    }

    pub(super) fn action_in_progress(&self, artifact: &ArtifactRef) -> bool {
        self.action_status(artifact)
            .is_some_and(|status| !matches!(status, ThreadArtifactActionStatus::Failed(_)))
    }

    pub(in crate::app) fn preview_square_image_path(
        &self,
        artifact: &ArtifactRef,
    ) -> Option<&Path> {
        self.preview_image_path_by_artifact
            .get(&thread_artifact_version_key(artifact))
            .map(|paths| paths.square_path.as_path())
            .filter(|path| path.is_file())
    }

    pub(in crate::app) fn preview_detail_image_path(
        &self,
        artifact: &ArtifactRef,
    ) -> Option<&Path> {
        self.preview_image_path_by_artifact
            .get(&thread_artifact_version_key(artifact))
            .map(|paths| paths.detail_path.as_path())
            .filter(|path| path.is_file())
    }

    pub(super) fn has_loadable_preview(&self, artifact: &ArtifactRef) -> bool {
        artifact_thumbnail_preview(artifact).is_some()
    }

    pub(super) fn should_load_preview(&self, artifact: &ArtifactRef) -> bool {
        if !self.has_loadable_preview(artifact) {
            return false;
        }
        let key = thread_artifact_version_key(artifact);
        !self
            .preview_image_path_by_artifact
            .get(&key)
            .is_some_and(|paths| paths.square_path.is_file() && paths.detail_path.is_file())
            && !self.preview_loading_by_artifact.contains(&key)
            && !self.preview_failed_by_artifact.contains(&key)
    }

    pub(super) fn mark_preview_loading_if_needed(&mut self, artifact: &ArtifactRef) -> bool {
        if !self.should_load_preview(artifact) {
            return false;
        }
        let key = thread_artifact_version_key(artifact);
        self.preview_failed_by_artifact.remove(&key);
        self.preview_loading_by_artifact.insert(key);
        true
    }

    pub(super) fn apply_preview_loaded(
        &mut self,
        artifact: &ArtifactRef,
        image_paths: ThreadArtifactPreviewImagePaths,
    ) {
        let key = thread_artifact_version_key(artifact);
        self.preview_loading_by_artifact.remove(&key);
        self.preview_failed_by_artifact.remove(&key);
        self.preview_image_path_by_artifact.insert(key, image_paths);
    }

    pub(super) fn apply_preview_failed(&mut self, artifact: &ArtifactRef) {
        let key = thread_artifact_version_key(artifact);
        self.preview_loading_by_artifact.remove(&key);
        self.preview_failed_by_artifact.insert(key);
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

impl PioneerDesktop {
    pub(in crate::app) fn ensure_active_thread_artifacts_loaded(&mut self, cx: &mut Context<Self>) {
        let active_thread_id = self.current_active_thread_id().map(str::to_owned);
        self.thread_artifacts
            .activate_thread(active_thread_id.as_deref());

        let Some(thread_id) = active_thread_id else {
            return;
        };
        if !self.is_thread_materialized_for_artifacts(thread_id.as_str()) {
            self.thread_artifacts.clear_error(thread_id.as_str());
            return;
        }
        self.refresh_thread_artifacts(thread_id, false, cx);
    }

    pub(in crate::app) fn refresh_thread_artifacts(
        &mut self,
        thread_id: String,
        force: bool,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        if !self.is_thread_materialized_for_artifacts(thread_id.as_str()) {
            self.thread_artifacts.clear_error(thread_id.as_str());
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(workspace_id) = self
            .thread_workspace_id(thread_id.as_str())
            .map(str::to_owned)
        else {
            return;
        };
        if !force && !self.thread_artifacts.needs_load(thread_id.as_str()) {
            return;
        }
        if self.thread_artifacts.is_loading_thread(thread_id.as_str()) {
            if force {
                self.thread_artifacts
                    .request_refresh_after_current(thread_id.as_str());
            }
            return;
        }

        self.thread_artifacts.mark_loading(thread_id.as_str());
        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let workspace_id_for_request = workspace_id.clone();
            let thread_id_for_request = thread_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.artifact_list_for_thread(ArtifactListForThreadParams {
                            workspace_id: workspace_id_for_request,
                            thread_id: Some(thread_id_for_request),
                            turn_id: None,
                            message_id: None,
                            task_id: None,
                            task_run_id: None,
                            kinds: Vec::new(),
                            include_deleted: false,
                            cursor: None,
                            limit: Some(250),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }
                    if !view.is_thread_materialized_for_artifacts(thread_id.as_str()) {
                        view.thread_artifacts.clear_error(thread_id.as_str());
                        cx.notify();
                        return;
                    }

                    match result {
                        Ok(response) => {
                            view.thread_artifacts
                                .apply_loaded(thread_id.as_str(), response.items);
                        }
                        Err(error) => {
                            let error_message = format!("{error:#}");
                            if is_artifact_thread_not_found_error(
                                thread_id.as_str(),
                                error_message.as_str(),
                            ) {
                                if let Some(delay) = view
                                    .thread_artifacts
                                    .defer_transient_load_retry(thread_id.as_str())
                                {
                                    warn!(
                                        thread_id = thread_id.as_str(),
                                        retry_after_ms = delay.as_millis(),
                                        error = %error_message,
                                        "thread artifacts list raced thread materialization; scheduling retry"
                                    );
                                    view.schedule_thread_artifacts_retry_after(
                                        connection_id,
                                        thread_id,
                                        delay,
                                        cx,
                                    );
                                    cx.notify();
                                    return;
                                }
                            }
                            warn!(
                                thread_id = thread_id.as_str(),
                                error = %error_message,
                                "failed to refresh thread artifacts"
                            );
                            view.thread_artifacts
                                .apply_failed(thread_id.as_str(), error_message);
                        }
                    }

                    if view
                        .thread_artifacts
                        .take_refresh_after_current(thread_id.as_str())
                    {
                        view.refresh_thread_artifacts(thread_id, true, cx);
                        return;
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn schedule_thread_artifacts_retry_after(
        &self,
        connection_id: u64,
        thread_id: String,
        delay: Duration,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                cx.background_executor().timer(delay).await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id)
                        || view.gateway.connection_state != GatewayConnectionState::Connected
                    {
                        return;
                    }
                    if !view.is_thread_materialized_for_artifacts(thread_id.as_str()) {
                        view.thread_artifacts.clear_error(thread_id.as_str());
                        cx.notify();
                        return;
                    }

                    view.refresh_thread_artifacts(thread_id, true, cx);
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn select_thread_artifact(
        &mut self,
        artifact_id: String,
        cx: &mut Context<Self>,
    ) {
        self.thread_artifacts.select_artifact(artifact_id);
        cx.notify();
    }

    pub(in crate::app) fn open_thread_artifact_in_sidebar(
        &mut self,
        artifact_id: String,
        cx: &mut Context<Self>,
    ) {
        self.show_thread_artifacts_sidebar = true;
        self.thread_artifacts.select_artifact(artifact_id);
        cx.notify();
    }

    pub(in crate::app) fn set_thread_artifact_filter(
        &mut self,
        filter: ThreadArtifactFilter,
        cx: &mut Context<Self>,
    ) {
        self.thread_artifacts.set_filter(filter);
        cx.notify();
    }

    pub(in crate::app) fn apply_thread_artifacts_changed_notification(
        &mut self,
        notification: ThreadArtifactsChangedNotification,
        cx: &mut Context<Self>,
    ) {
        if !self.thread_workspace_matches(
            notification.thread_id.as_str(),
            notification.workspace_id.as_str(),
        ) {
            return;
        }
        self.refresh_thread_artifacts(notification.thread_id, true, cx);
    }

    pub(in crate::app) fn refresh_current_thread_artifacts_if_contains(
        &mut self,
        artifact_id: &str,
        cx: &mut Context<Self>,
    ) {
        let Some(active_thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        if self
            .thread_artifacts
            .items_for_active_thread()
            .iter()
            .any(|summary| summary.artifact.artifact_id == artifact_id)
        {
            self.refresh_thread_artifacts(active_thread_id, true, cx);
        }
    }

    pub(in crate::app) fn request_thread_artifact_preview_load(
        &self,
        workspace_id: &str,
        artifact: &ArtifactRef,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || !self.thread_artifacts.should_load_preview(artifact)
        {
            return;
        }

        let Some(preview) = artifact_thumbnail_preview(artifact) else {
            return;
        };
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        let workspace_id = workspace_id.to_owned();
        let artifact = artifact.clone();
        let expected_preview_sha256 = preview.sha256.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                let should_start = this
                    .update(&mut cx, |view, cx| {
                        if view.gateway.ws_connection_id != Some(connection_id)
                            || view.gateway.connection_state != GatewayConnectionState::Connected
                            || !view
                                .thread_artifacts
                                .mark_preview_loading_if_needed(&artifact)
                        {
                            return false;
                        }
                        cx.notify();
                        true
                    })
                    .unwrap_or(false);
                if !should_start {
                    return;
                }

                let request_artifact = artifact.clone();
                let request_workspace_id = workspace_id.clone();
                let result = cx
                    .background_spawn(async move {
                        let response = ws_sender.artifact_read(ArtifactReadParams {
                            workspace_id: request_workspace_id,
                            artifact_id: request_artifact.artifact_id.clone(),
                            version_id: request_artifact.version_id.clone(),
                            projection_kind: Some(ArtifactProjectionKind::Thumbnail),
                            offset: Some(0),
                            max_bytes: Some(THREAD_ARTIFACT_PREVIEW_MAX_BYTES),
                        })?;
                        let image_path = write_artifact_preview_cache_file(
                            workspace_id.as_str(),
                            &request_artifact,
                            expected_preview_sha256.as_deref(),
                            &response,
                        )?;
                        Ok::<_, anyhow::Error>(image_path)
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    match result {
                        Ok(image_path) => {
                            view.thread_artifacts
                                .apply_preview_loaded(&artifact, image_path);
                        }
                        Err(error) => {
                            debug!(
                                artifact_id = artifact.artifact_id.as_str(),
                                error = %error,
                                "failed to load artifact thumbnail preview"
                            );
                            view.thread_artifacts.apply_preview_failed(&artifact);
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn prune_thread_artifact_preview_cache(&self, cx: &mut Context<Self>) {
        cx.spawn(move |_this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let cx = cx.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        let runtime_home = desktop_state::runtime_home_dir()?;
                        prune_artifact_preview_cache(
                            runtime_home.as_path(),
                            THREAD_ARTIFACT_PREVIEW_CACHE_MAX_BYTES,
                            &[],
                        )
                    })
                    .await;

                match result {
                    Ok(removed_bytes) if removed_bytes > 0 => {
                        debug!(
                            removed_bytes,
                            max_bytes = THREAD_ARTIFACT_PREVIEW_CACHE_MAX_BYTES,
                            "pruned artifact preview cache"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        debug!(
                            error = %format!("{error:#}"),
                            "failed to prune artifact preview cache"
                        );
                    }
                }
            }
        })
        .detach();
    }

    fn is_thread_materialized_for_artifacts(&self, thread_id: &str) -> bool {
        self.draft_thread_id() != Some(thread_id)
            && self
                .thread_coordinator(thread_id)
                .is_some_and(|coordinator| coordinator.thread().is_some())
    }
}

pub(super) fn thread_artifact_version_key(artifact: &ArtifactRef) -> ThreadArtifactVersionKey {
    ThreadArtifactVersionKey {
        artifact_id: artifact.artifact_id.clone(),
        version_id: artifact.version_id.clone(),
    }
}

pub(super) fn artifact_thumbnail_preview(
    artifact: &ArtifactRef,
) -> Option<&pioneer_protocol::ArtifactPreviewRef> {
    let preview = artifact.preview.as_ref()?;
    if preview.projection_kind == ArtifactProjectionKind::Thumbnail
        && preview.status == ArtifactProjectionStatus::Ready
        && preview.blob_id.is_some()
    {
        Some(preview)
    } else {
        None
    }
}

fn write_artifact_preview_cache_file(
    workspace_id: &str,
    artifact: &ArtifactRef,
    expected_sha256: Option<&str>,
    response: &ArtifactReadResponse,
) -> Result<ThreadArtifactPreviewImagePaths> {
    let runtime_home = desktop_state::runtime_home_dir()?;
    write_artifact_preview_cache_files(
        runtime_home.as_path(),
        workspace_id,
        artifact,
        expected_sha256,
        response,
    )
}

fn write_artifact_preview_cache_files(
    runtime_home: &Path,
    workspace_id: &str,
    artifact: &ArtifactRef,
    expected_sha256: Option<&str>,
    response: &ArtifactReadResponse,
) -> Result<ThreadArtifactPreviewImagePaths> {
    if response.truncated {
        bail!("artifact thumbnail preview read was truncated");
    }
    if response.len == 0 {
        bail!("artifact thumbnail preview was empty");
    }
    if expected_sha256.is_some_and(|sha256| sha256 != response.sha256) {
        bail!("artifact thumbnail preview sha256 mismatch");
    }

    let bytes = BASE64
        .decode(response.content_base64.as_bytes())
        .context("failed to decode artifact thumbnail preview")?;
    let decoded_len = u64::try_from(bytes.len()).unwrap_or_default();
    if decoded_len != response.len || decoded_len != response.total_size_bytes {
        bail!("artifact thumbnail preview length mismatch");
    }

    let source_image = image::load_from_memory(bytes.as_slice())
        .context("failed to decode artifact thumbnail preview image")?;
    let image_paths = artifact_preview_cache_paths(
        runtime_home,
        workspace_id,
        artifact.artifact_id.as_str(),
        artifact.version_id.as_deref(),
        response.sha256.as_str(),
    )?;
    if let Some(parent) = image_paths.square_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create artifact preview cache dir `{}`",
                parent.display()
            )
        })?;
    }
    write_artifact_preview_variant(
        &source_image,
        image_paths.square_path.as_path(),
        THREAD_ARTIFACT_PREVIEW_SQUARE_EDGE_PX,
        THREAD_ARTIFACT_PREVIEW_SQUARE_EDGE_PX,
    )?;
    write_artifact_preview_variant(
        &source_image,
        image_paths.detail_path.as_path(),
        THREAD_ARTIFACT_PREVIEW_DETAIL_WIDTH_PX,
        THREAD_ARTIFACT_PREVIEW_DETAIL_HEIGHT_PX,
    )?;
    let removed_bytes = prune_artifact_preview_cache(
        runtime_home,
        THREAD_ARTIFACT_PREVIEW_CACHE_MAX_BYTES,
        &[
            image_paths.square_path.clone(),
            image_paths.detail_path.clone(),
        ],
    )?;
    if removed_bytes > 0 {
        debug!(
            removed_bytes,
            max_bytes = THREAD_ARTIFACT_PREVIEW_CACHE_MAX_BYTES,
            "pruned artifact preview cache after writing thumbnail"
        );
    }

    Ok(image_paths)
}

fn write_artifact_preview_variant(
    source_image: &DynamicImage,
    image_path: &Path,
    target_width: u32,
    target_height: u32,
) -> Result<()> {
    let resized = cover_crop_resize_image(source_image, target_width, target_height)?;
    let mut encoded = Cursor::new(Vec::new());
    resized
        .write_to(&mut encoded, ImageFormat::Png)
        .context("failed to encode artifact preview cache image")?;

    let temp_path = image_path.with_extension("png.tmp");
    fs::write(temp_path.as_path(), encoded.into_inner()).with_context(|| {
        format!(
            "failed to write artifact preview cache file `{}`",
            temp_path.display()
        )
    })?;
    fs::rename(temp_path.as_path(), image_path).with_context(|| {
        format!(
            "failed to publish artifact preview cache file `{}`",
            image_path.display()
        )
    })?;
    Ok(())
}

fn cover_crop_resize_image(
    source_image: &DynamicImage,
    target_width: u32,
    target_height: u32,
) -> Result<DynamicImage> {
    if target_width == 0 || target_height == 0 {
        bail!("artifact preview target size must be non-zero");
    }

    let (source_width, source_height) = source_image.dimensions();
    if source_width == 0 || source_height == 0 {
        bail!("artifact thumbnail preview image has invalid dimensions");
    }

    let source_ratio = f64::from(source_width) / f64::from(source_height);
    let target_ratio = f64::from(target_width) / f64::from(target_height);
    let (crop_x, crop_y, crop_width, crop_height) = if source_ratio > target_ratio {
        let crop_width =
            ((f64::from(source_height) * target_ratio).round() as u32).clamp(1, source_width);
        (
            (source_width - crop_width) / 2,
            0,
            crop_width,
            source_height,
        )
    } else {
        let crop_height =
            ((f64::from(source_width) / target_ratio).round() as u32).clamp(1, source_height);
        (
            0,
            (source_height - crop_height) / 2,
            source_width,
            crop_height,
        )
    };

    Ok(source_image
        .crop_imm(crop_x, crop_y, crop_width, crop_height)
        .resize_exact(target_width, target_height, FilterType::Lanczos3))
}

#[derive(Debug)]
struct ArtifactPreviewCacheFile {
    path: PathBuf,
    size: u64,
    modified: SystemTime,
}

fn prune_artifact_preview_cache(
    runtime_home: &Path,
    max_bytes: u64,
    protected_files: &[PathBuf],
) -> Result<u64> {
    let cache_root = artifact_preview_cache_root(runtime_home)?;
    if !cache_root.exists() {
        return Ok(0);
    }

    let mut files = Vec::new();
    collect_artifact_preview_cache_files(cache_root.as_path(), &mut files)?;
    let mut total_size = files
        .iter()
        .fold(0_u64, |total, file| total.saturating_add(file.size));
    if total_size <= max_bytes {
        return Ok(0);
    }

    files.sort_by(|left, right| {
        left.modified
            .cmp(&right.modified)
            .then_with(|| left.path.cmp(&right.path))
    });

    let mut removed_bytes = 0_u64;
    for file in files {
        if total_size <= max_bytes {
            break;
        }
        if protected_files
            .iter()
            .any(|protected_file| file.path == *protected_file)
        {
            continue;
        }

        match fs::remove_file(file.path.as_path()) {
            Ok(()) => {
                total_size = total_size.saturating_sub(file.size);
                removed_bytes = removed_bytes.saturating_add(file.size);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to remove artifact preview cache file `{}`",
                        file.path.display()
                    )
                });
            }
        }
    }

    remove_empty_artifact_preview_cache_dirs(cache_root.as_path(), cache_root.as_path())?;
    Ok(removed_bytes)
}

fn collect_artifact_preview_cache_files(
    dir: &Path,
    files: &mut Vec<ArtifactPreviewCacheFile>,
) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| {
        format!(
            "failed to read artifact preview cache dir `{}`",
            dir.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "failed to read artifact preview cache entry under `{}`",
                dir.display()
            )
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read file type `{}`", path.display()))?;
        if file_type.is_dir() {
            collect_artifact_preview_cache_files(path.as_path(), files)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let metadata = entry.metadata().with_context(|| {
            format!("failed to stat artifact preview cache `{}`", path.display())
        })?;
        files.push(ArtifactPreviewCacheFile {
            path,
            size: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        });
    }
    Ok(())
}

fn remove_empty_artifact_preview_cache_dirs(dir: &Path, root: &Path) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| {
        format!(
            "failed to read artifact preview cache dir `{}`",
            dir.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "failed to read artifact preview cache entry under `{}`",
                dir.display()
            )
        })?;
        let path = entry.path();
        if entry
            .file_type()
            .with_context(|| format!("failed to read file type `{}`", path.display()))?
            .is_dir()
        {
            remove_empty_artifact_preview_cache_dirs(path.as_path(), root)?;
        }
    }

    if dir != root
        && fs::read_dir(dir)
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false)
    {
        let _ = fs::remove_dir(dir);
    }
    Ok(())
}

fn artifact_preview_cache_root(runtime_home: &Path) -> Result<PathBuf> {
    let cache_root = runtime_home.join("previews").join("artifacts");
    if !cache_root.starts_with(runtime_home) {
        bail!("artifact preview cache path escaped runtime home");
    }
    Ok(cache_root)
}

fn artifact_preview_cache_paths(
    runtime_home: &Path,
    workspace_id: &str,
    artifact_id: &str,
    version_id: Option<&str>,
    sha256: &str,
) -> Result<ThreadArtifactPreviewImagePaths> {
    Ok(ThreadArtifactPreviewImagePaths {
        square_path: artifact_preview_cache_path(
            runtime_home,
            workspace_id,
            artifact_id,
            version_id,
            sha256,
            "square",
        )?,
        detail_path: artifact_preview_cache_path(
            runtime_home,
            workspace_id,
            artifact_id,
            version_id,
            sha256,
            "detail",
        )?,
    })
}

fn artifact_preview_cache_path(
    runtime_home: &Path,
    workspace_id: &str,
    artifact_id: &str,
    version_id: Option<&str>,
    sha256: &str,
    variant: &str,
) -> Result<PathBuf> {
    let safe_workspace_id = artifact_preview_safe_path_segment(workspace_id, "workspace");
    let safe_artifact_id = artifact_preview_safe_path_segment(artifact_id, "artifact");
    let safe_version_id =
        artifact_preview_safe_path_segment(version_id.unwrap_or("latest"), "version");
    let safe_sha256 = artifact_preview_safe_path_segment(sha256, "thumbnail");
    let safe_variant = artifact_preview_safe_path_segment(variant, "preview");
    let image_path = artifact_preview_cache_root(runtime_home)?
        .join(safe_workspace_id)
        .join(safe_artifact_id)
        .join(safe_version_id)
        .join(format!("{safe_sha256}.{safe_variant}.png"));
    if !image_path.starts_with(runtime_home) {
        bail!("artifact preview cache path escaped runtime home");
    }
    Ok(image_path)
}

fn artifact_preview_safe_path_segment(value: &str, fallback: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    let trimmed = sanitized
        .trim_matches([' ', '\t', '\n', '\r'])
        .trim_matches('.');
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        fallback.to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn thread_artifacts_transient_retry_delay(retry_count: u8) -> Duration {
    let index =
        retry_count.min((THREAD_ARTIFACTS_TRANSIENT_RETRY_DELAYS_MS.len() - 1) as u8) as usize;
    Duration::from_millis(THREAD_ARTIFACTS_TRANSIENT_RETRY_DELAYS_MS[index])
}

fn is_artifact_thread_not_found_error(thread_id: &str, error: &str) -> bool {
    let needle = format!("thread `{thread_id}` not found");
    error.contains(needle.as_str())
}

fn artifact_matches_filter(summary: &ArtifactSummary, filter: ThreadArtifactFilter) -> bool {
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

pub(super) fn format_artifact_size(size_bytes: Option<u64>) -> String {
    let Some(size_bytes) = size_bytes else {
        return t!("artifacts.size_unknown").to_string();
    };
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    if size_bytes < 1024 {
        format!("{size_bytes} B")
    } else if size_bytes < 1024 * 1024 {
        format!("{:.1} KB", size_bytes as f64 / KB)
    } else {
        format!("{:.1} MB", size_bytes as f64 / MB)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ArtifactBindingDirection, ArtifactBindingSummary, ArtifactPreviewRef, ArtifactRef,
        ArtifactRole, ArtifactStatus,
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

        let mut state = ThreadArtifactsState::default();
        state.activate_thread(Some("thread"));
        state.mark_loading("thread");
        state.apply_loaded("thread", vec![uploaded, task_output]);

        assert!(!state.loading);
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

        let mut state = ThreadArtifactsState::default();
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
        let mut state = ThreadArtifactsState::default();
        state.activate_thread(Some("thread"));
        state.mark_loading("thread");
        state.apply_failed("thread", "boom".to_owned());

        assert!(!state.loading);
        assert_eq!(state.error.as_deref(), Some("boom"));
        assert!(!state.needs_load("thread"));

        state.mark_loading("thread");
        assert!(state.loading);
        assert!(state.error.is_none());
    }

    #[test]
    fn artifacts_state_defers_transient_load_retry_without_caching_error() {
        let mut state = ThreadArtifactsState::default();
        state.activate_thread(Some("thread"));
        state.mark_loading("thread");

        let delay = state.defer_transient_load_retry("thread");

        assert_eq!(delay, Some(Duration::from_millis(250)));
        assert!(!state.loading);
        assert!(state.error.is_none());
        assert!(!state.needs_load("thread"));

        state.retry_after_by_thread.insert(
            "thread".to_owned(),
            Instant::now() - Duration::from_millis(1),
        );
        assert!(state.needs_load("thread"));
    }

    #[test]
    fn artifacts_state_caps_transient_load_retries() {
        let mut state = ThreadArtifactsState::default();
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
    fn artifact_preview_cache_prune_keeps_cache_under_size_limit() {
        let temp = tempfile::tempdir().expect("temp dir");
        let old_path = artifact_preview_cache_path(
            temp.path(),
            "ws",
            "old_art",
            Some("v1"),
            "old_sha",
            "square",
        )
        .expect("old path");
        fs::create_dir_all(old_path.parent().expect("old parent")).expect("create old parent");
        fs::write(old_path.as_path(), vec![0_u8; 40]).expect("write old preview");

        std::thread::sleep(Duration::from_millis(5));

        let new_path = artifact_preview_cache_path(
            temp.path(),
            "ws",
            "new_art",
            Some("v1"),
            "new_sha",
            "square",
        )
        .expect("new path");
        fs::create_dir_all(new_path.parent().expect("new parent")).expect("create new parent");
        fs::write(new_path.as_path(), vec![1_u8; 40]).expect("write new preview");

        let removed =
            prune_artifact_preview_cache(temp.path(), 50, &[new_path.clone()]).expect("prune");

        assert_eq!(removed, 40);
        assert!(!old_path.exists());
        assert!(new_path.exists());
    }

    #[test]
    fn artifact_preview_cache_write_creates_cropped_variants() {
        let temp = tempfile::tempdir().expect("temp dir");
        let artifact = preview_artifact("art_preview");
        let source_image =
            image::ImageBuffer::from_pixel(400, 300, image::Rgba([20_u8, 40, 60, 255]));
        let mut encoded = std::io::Cursor::new(Vec::new());
        DynamicImage::ImageRgba8(source_image)
            .write_to(&mut encoded, ImageFormat::Png)
            .expect("encode source");
        let bytes = encoded.into_inner();
        let response = ArtifactReadResponse {
            artifact: artifact.clone(),
            offset: 0,
            len: bytes.len() as u64,
            total_size_bytes: bytes.len() as u64,
            sha256: "thumb_sha".to_owned(),
            content_base64: BASE64.encode(bytes.as_slice()),
            truncated: false,
        };

        let image_paths = write_artifact_preview_cache_files(
            temp.path(),
            "ws",
            &artifact,
            Some("thumb_sha"),
            &response,
        )
        .expect("write preview variants");

        let square = image::open(image_paths.square_path.as_path()).expect("open square");
        let detail = image::open(image_paths.detail_path.as_path()).expect("open detail");
        assert_eq!(
            square.dimensions(),
            (
                THREAD_ARTIFACT_PREVIEW_SQUARE_EDGE_PX,
                THREAD_ARTIFACT_PREVIEW_SQUARE_EDGE_PX
            )
        );
        assert_eq!(
            detail.dimensions(),
            (
                THREAD_ARTIFACT_PREVIEW_DETAIL_WIDTH_PX,
                THREAD_ARTIFACT_PREVIEW_DETAIL_HEIGHT_PX
            )
        );
    }

    #[test]
    fn artifacts_state_reloads_preview_when_cached_file_was_pruned() {
        let artifact = preview_artifact("art_preview");
        let mut state = ThreadArtifactsState::default();
        state.preview_image_path_by_artifact.insert(
            thread_artifact_version_key(&artifact),
            ThreadArtifactPreviewImagePaths {
                square_path: PathBuf::from("/tmp/pioneer-missing-preview-square.png"),
                detail_path: PathBuf::from("/tmp/pioneer-missing-preview-detail.png"),
            },
        );

        assert!(state.preview_square_image_path(&artifact).is_none());
        assert!(state.preview_detail_image_path(&artifact).is_none());
        assert!(state.should_load_preview(&artifact));
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

    fn preview_artifact(artifact_id: &str) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact_id.to_owned(),
            version_id: Some("v1".to_owned()),
            display_name: "image.png".to_owned(),
            kind: ArtifactKind::Image,
            mime_type: Some("image/png".to_owned()),
            size_bytes: Some(2048),
            sha256: Some("artifact_sha".to_owned()),
            status: ArtifactStatus::Ready,
            preview: Some(ArtifactPreviewRef {
                projection_kind: ArtifactProjectionKind::Thumbnail,
                status: ArtifactProjectionStatus::Ready,
                artifact_id: artifact_id.to_owned(),
                version_id: "v1".to_owned(),
                blob_id: Some("blob_1".to_owned()),
                mime_type: Some("image/png".to_owned()),
                size_bytes: Some(512),
                sha256: Some("thumb_sha".to_owned()),
            }),
        }
    }
}
