use crate::{
    app::root::{
        GatewayConnectionState, PioneerDesktop, ThreadArtifactFilter,
        ThreadArtifactPreviewImagePaths,
    },
    state as desktop_state,
};
use anyhow::{Context as _, Result, bail};
use gpui_kit::{AppContext, AsyncApp, Context, WeakEntity};
use image::{DynamicImage, GenericImageView as _, ImageFormat, imageops::FilterType};
use pioneer_client::artifacts::{
    presentation as client_artifact_presentation, preview as client_artifact_preview,
    state as client_artifact_state,
};
use pioneer_protocol::ArtifactRef;
use std::{fs, io::Cursor, path::Path, time::Duration};
use tracing::{debug, warn};

impl PioneerDesktop {
    pub(in crate::app) fn ensure_active_thread_artifacts_loaded(&mut self, cx: &mut Context<Self>) {
        let active_thread_id = self.current_active_thread_id().map(str::to_owned);
        self.thread_artifacts
            .activate_thread(active_thread_id.as_deref());

        let Some(thread_id) = active_thread_id else {
            return;
        };
        if !self
            .artifact_presentation_policy_for_thread(thread_id.as_str())
            .can_list
        {
            self.thread_artifacts.clear_error(thread_id.as_str());
            return;
        }
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
        if !self
            .artifact_presentation_policy_for_thread(thread_id.as_str())
            .can_list
        {
            self.thread_artifacts.clear_error(thread_id.as_str());
            return;
        }
        let plan = client_artifact_state::plan_thread_artifacts_refresh(
            &self.thread_artifacts,
            thread_id.as_str(),
            force,
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.thread_workspace_id(thread_id.as_str())
                .map(str::to_owned),
            self.is_thread_materialized_for_artifacts(thread_id.as_str()),
        );
        let request = match plan {
            client_artifact_state::ThreadArtifactsRefreshPlan::Send(request) => request,
            client_artifact_state::ThreadArtifactsRefreshPlan::ClearError => {
                self.thread_artifacts.clear_error(thread_id.as_str());
                return;
            }
            client_artifact_state::ThreadArtifactsRefreshPlan::RequestRefreshAfterCurrent => {
                self.thread_artifacts
                    .request_refresh_after_current(thread_id.as_str());
                return;
            }
            client_artifact_state::ThreadArtifactsRefreshPlan::Skip => return,
        };
        let connection_id = request.connection_id;
        let params = request.params;

        self.thread_artifacts.mark_loading(thread_id.as_str());
        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.artifact_list_for_thread(params)
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
                            if client_artifact_state::is_artifact_thread_not_found_error(
                                thread_id.as_str(),
                                error_message.as_str(),
                            ) {
                                if let Some(delay) = view
                                    .thread_artifacts
                                    .defer_transient_load_retry(thread_id.as_str())
                                {
                                    debug!(
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
        pioneer_observability::record_qualification_diagnostic!(record_animation_activity(
            pioneer_observability::AnimationSourceId::ThreadArtifactRefreshRetry,
            pioneer_observability::DiagnosticAction::Scheduled,
            pioneer_observability::Visibility::NotApplicable,
        ));
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                cx.background_executor().timer(delay).await;
                pioneer_observability::record_qualification_diagnostic!(
                    record_animation_activity(
                        pioneer_observability::AnimationSourceId::ThreadArtifactRefreshRetry,
                        pioneer_observability::DiagnosticAction::Woke,
                        pioneer_observability::Visibility::NotApplicable,
                    )
                );

                #[cfg(not(feature = "qualification-diagnostics"))]
                {
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
                #[cfg(feature = "qualification-diagnostics")]
                {
                    let handoff = this.update(&mut cx, |view, cx| {
                        if view.gateway.ws_connection_id != Some(connection_id)
                            || view.gateway.connection_state != GatewayConnectionState::Connected
                        {
                            return false;
                        }
                        if !view.is_thread_materialized_for_artifacts(thread_id.as_str()) {
                            view.thread_artifacts.clear_error(thread_id.as_str());
                            pioneer_observability::record_qualification_diagnostic!(
                                record_animation_activity(
                                    pioneer_observability::AnimationSourceId::ThreadArtifactRefreshRetry,
                                    pioneer_observability::DiagnosticAction::Requested,
                                    pioneer_observability::Visibility::NotApplicable,
                                )
                            );
                            cx.notify();
                            return true;
                        }

                        pioneer_observability::record_qualification_diagnostic!(
                            record_animation_activity(
                                pioneer_observability::AnimationSourceId::ThreadArtifactRefreshRetry,
                                pioneer_observability::DiagnosticAction::Requested,
                                pioneer_observability::Visibility::NotApplicable,
                            )
                        );
                        view.refresh_thread_artifacts(thread_id, true, cx);
                        true
                    });
                    pioneer_observability::record_qualification_diagnostic!(
                        record_animation_activity(
                            pioneer_observability::AnimationSourceId::ThreadArtifactRefreshRetry,
                            if matches!(handoff, Ok(true)) {
                                pioneer_observability::DiagnosticAction::Completed
                            } else {
                                pioneer_observability::DiagnosticAction::Cancelled
                            },
                            pioneer_observability::Visibility::NotApplicable,
                        )
                    );
                }
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
        if !self.active_artifact_presentation_policy().can_open {
            return;
        }
        let active_thread_id = self.current_active_thread_id().map(str::to_owned);
        let artifact_missing_from_cache = !self
            .thread_artifacts
            .items_for_active_thread()
            .iter()
            .any(|summary| summary.artifact.artifact_id == artifact_id);

        self.show_thread_artifacts_sidebar = true;
        self.show_thread_members_sidebar = false;
        self.thread_artifacts.select_artifact(artifact_id);
        if let Some(thread_id) = active_thread_id {
            self.refresh_thread_artifacts(thread_id, artifact_missing_from_cache, cx);
        }
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

    pub(in crate::app) fn request_thread_artifact_preview_load(
        &self,
        workspace_id: &str,
        artifact: &ArtifactRef,
        cx: &mut Context<Self>,
    ) {
        if !self.active_artifact_presentation_policy().can_open
            || self.gateway.connection_state != GatewayConnectionState::Connected
            || !self.thread_artifacts.should_load_preview(artifact)
        {
            return;
        }

        let Some(_) = client_artifact_preview::thumbnail_preview(artifact) else {
            return;
        };
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };

        let workspace_id = workspace_id.to_owned();
        let artifact = artifact.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                let http_client = this
                    .update(&mut cx, |view, cx| {
                        if view.gateway.ws_connection_id != Some(connection_id)
                            || view.gateway.connection_state != GatewayConnectionState::Connected
                            || !view
                                .thread_artifacts
                                .mark_preview_loading_if_needed(&artifact)
                        {
                            return None;
                        }
                        let http_client = match view.active_gateway_http_client() {
                            Ok(http_client) => http_client,
                            Err(_) => {
                                view.thread_artifacts.apply_preview_failed(&artifact);
                                cx.notify();
                                return None;
                            }
                        };
                        cx.notify();
                        Some(http_client)
                    })
                    .ok()
                    .flatten();
                let Some(http_client) = http_client else {
                    return;
                };

                let request_artifact = artifact.clone();
                let request_workspace_id = workspace_id.clone();
                let result = cx
                    .background_spawn(async move {
                        let preview_data = http_client.fetch_artifact_thumbnail(
                            request_workspace_id.as_str(),
                            &request_artifact,
                            tokio_util::sync::CancellationToken::new(),
                        )?;
                        let image_path = write_artifact_preview_cache_file(
                            workspace_id.as_str(),
                            &request_artifact,
                            &preview_data,
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
                        client_artifact_preview::prune_artifact_preview_cache(
                            runtime_home.as_path(),
                            client_artifact_preview::ARTIFACT_PREVIEW_CACHE_MAX_BYTES,
                            &[],
                        )
                    })
                    .await;

                match result {
                    Ok(removed_bytes) if removed_bytes > 0 => {
                        debug!(
                            removed_bytes,
                            max_bytes = client_artifact_preview::ARTIFACT_PREVIEW_CACHE_MAX_BYTES,
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

fn write_artifact_preview_cache_file(
    workspace_id: &str,
    artifact: &ArtifactRef,
    preview_data: &client_artifact_preview::ArtifactPreviewReadData,
) -> Result<ThreadArtifactPreviewImagePaths> {
    let runtime_home = desktop_state::runtime_home_dir()?;
    write_artifact_preview_cache_files(runtime_home.as_path(), workspace_id, artifact, preview_data)
}

fn write_artifact_preview_cache_files(
    runtime_home: &Path,
    workspace_id: &str,
    artifact: &ArtifactRef,
    preview_data: &client_artifact_preview::ArtifactPreviewReadData,
) -> Result<ThreadArtifactPreviewImagePaths> {
    client_artifact_preview::write_artifact_preview_cache_files(
        &DesktopArtifactPreviewImageRenderer,
        runtime_home,
        workspace_id,
        artifact,
        preview_data,
    )
}

struct DesktopArtifactPreviewImageRenderer;

impl client_artifact_preview::ArtifactPreviewImageRenderer for DesktopArtifactPreviewImageRenderer {
    fn write_preview_variants(
        &self,
        source_bytes: &[u8],
        targets: &[client_artifact_preview::ArtifactPreviewVariantTarget],
    ) -> Result<()> {
        let source_image = image::load_from_memory(source_bytes)
            .context("failed to decode artifact thumbnail preview image")?;
        for target in targets {
            write_artifact_preview_variant(
                &source_image,
                target.path.as_path(),
                target.width_px,
                target.height_px,
            )?;
        }
        Ok(())
    }
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

pub(super) fn format_artifact_size(size_bytes: Option<u64>) -> String {
    let Some(size_bytes) = size_bytes else {
        return t!("artifacts.size_unknown").to_string();
    };
    client_artifact_presentation::format_artifact_size_bytes(size_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ArtifactKind, ArtifactPreviewRef, ArtifactProjectionKind, ArtifactProjectionStatus,
        ArtifactRef, ArtifactStatus,
    };

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
        let preview_data = client_artifact_preview::ArtifactPreviewReadData {
            bytes,
            sha256: "thumb_sha".to_owned(),
        };

        let image_paths =
            write_artifact_preview_cache_files(temp.path(), "ws", &artifact, &preview_data)
                .expect("write preview variants");

        let square = image::open(image_paths.square_path.as_path()).expect("open square");
        let detail = image::open(image_paths.detail_path.as_path()).expect("open detail");
        assert_eq!(
            square.dimensions(),
            (
                client_artifact_preview::ARTIFACT_PREVIEW_SQUARE_EDGE_PX,
                client_artifact_preview::ARTIFACT_PREVIEW_SQUARE_EDGE_PX
            )
        );
        assert_eq!(
            detail.dimensions(),
            (
                client_artifact_preview::ARTIFACT_PREVIEW_DETAIL_WIDTH_PX,
                client_artifact_preview::ARTIFACT_PREVIEW_DETAIL_HEIGHT_PX
            )
        );
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
