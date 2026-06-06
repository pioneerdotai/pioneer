use super::state::format_artifact_size;
use crate::app::root::{
    GatewayConnectionState, PioneerDesktop, ThreadArtifactActionStatus, ThreadArtifactFilter,
};
use chrono::{Local, TimeZone};
use gpui::{StatefulInteractiveElement as _, prelude::*, *};
use gpui_component::{
    Disableable, Icon, IconName, Selectable, Sizable, StyledExt,
    button::{Button, ButtonVariants},
    h_flex,
    spinner::Spinner,
    theme::ActiveTheme,
    v_flex,
};
use pioneer_client::artifacts::presentation as client_artifact_presentation;
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
    ArtifactRef, ArtifactStatus, ArtifactSummary,
};
use std::path::PathBuf;

const THREAD_ARTIFACT_DETAIL_PREVIEW_ASPECT_RATIO: f32 = 2.0;

impl PioneerDesktop {
    pub(crate) fn render_thread_artifacts_panel(&self, cx: &mut Context<Self>) -> AnyElement {
        let total_count = self.thread_artifacts.items_for_active_thread().len();
        let visible_items = self.thread_artifacts.visible_items();
        let visible_count = visible_items.len();
        let selected_artifact_id = self
            .thread_artifacts
            .selected_artifact_id()
            .map(str::to_owned);
        let is_loading_active_thread = self.thread_artifacts.is_loading_active_thread();

        let mut list = v_flex().w_full().gap_2();
        for summary in visible_items {
            let row = self.render_thread_artifact_row(summary, cx);
            if selected_artifact_id.as_deref() == Some(summary.artifact.artifact_id.as_str()) {
                list = list.child(
                    v_flex()
                        .w_full()
                        .gap_3()
                        .child(row)
                        .child(self.render_thread_artifact_detail(summary, cx)),
                );
            } else {
                list = list.child(row);
            }
        }

        v_flex()
            .id("thread-artifacts-panel")
            .h_full()
            .w_full()
            .min_w_0()
            .border_l_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .child(
                h_flex()
                    .px_4()
                    .pt_2p5()
                    .pb_1()
                    .items_center()
                    .justify_between()
                    .child(
                        h_flex().items_center().gap_2().opacity(0.4).child(
                            div()
                                .text_sm()
                                .font_semibold()
                                .child(t!("artifacts.title").to_string()),
                        ),
                    ),
            )
            .child(self.render_thread_artifact_filters(cx))
            .child(
                v_flex()
                    .id("thread-artifacts-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .p_3()
                    .gap_3()
                    .when(is_loading_active_thread, |this| {
                        this.child(
                            h_flex()
                                .gap_2()
                                .items_center()
                                .text_xs()
                                .child(Icon::new(IconName::LoaderCircle).size_3())
                                .child(t!("artifacts.loading").to_string()),
                        )
                    })
                    .when_some(
                        self.thread_artifacts.error().map(str::to_owned),
                        |this, error| {
                            this.child(
                                v_flex()
                                    .gap_1()
                                    .rounded_md()
                                    .border_1()
                                    .border_color(cx.theme().danger.opacity(0.45))
                                    .bg(cx.theme().danger.opacity(0.08))
                                    .p_3()
                                    .text_sm()
                                    .child(
                                        div()
                                            .font_medium()
                                            .child(t!("artifacts.failed").to_string()),
                                    )
                                    .child(div().text_xs().child(error)),
                            )
                        },
                    )
                    .child(if total_count == 0 && !is_loading_active_thread {
                        self.render_thread_artifact_empty(cx)
                    } else if visible_count == 0 {
                        v_flex()
                            .w_full()
                            .items_center()
                            .gap_2()
                            .py_8()
                            .text_center()
                            .opacity(0.6)
                            .child(Icon::new(IconName::File).size_4())
                            .child(
                                div()
                                    .text_xs()
                                    .child(t!("artifacts.empty_filter").to_string()),
                            )
                            .into_any_element()
                    } else {
                        list.into_any_element()
                    }),
            )
            .into_any_element()
    }

    fn render_thread_artifact_filters(&self, cx: &mut Context<Self>) -> AnyElement {
        h_flex()
            .id("thread-artifact-filters")
            .w_full()
            .min_w_0()
            .gap_1()
            .px_3()
            .pt_1()
            .pb_0()
            .overflow_x_scroll()
            .overflow_y_hidden()
            .scrollbar_width(px(0.))
            .children(ThreadArtifactFilter::all().into_iter().map(|filter| {
                Button::new((
                    "thread-artifact-filter",
                    client_artifact_presentation::thread_artifact_filter_id(filter),
                ))
                .flex_none()
                .ghost()
                .small()
                .compact()
                .selected(self.thread_artifacts.filter() == filter)
                .child(
                    div()
                        .text_xs()
                        .whitespace_nowrap()
                        .child(filter_label(filter)),
                )
                .on_click(cx.listener(move |view, _, _, cx| {
                    view.set_thread_artifact_filter(filter, cx);
                }))
            }))
            .into_any_element()
    }

    fn render_thread_artifact_empty(&self, _cx: &mut Context<Self>) -> AnyElement {
        v_flex()
            .w_full()
            .items_center()
            .gap_2()
            .py_8()
            .text_center()
            .opacity(0.6)
            .child(Icon::new(IconName::File).size_4())
            .child(div().text_xs().child(t!("artifacts.empty").to_string()))
            .into_any_element()
    }

    fn render_thread_artifact_row(
        &self,
        summary: &ArtifactSummary,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let artifact = &summary.artifact;
        let artifact_id = artifact.artifact_id.clone();
        let selected =
            self.thread_artifacts.selected_artifact_id() == Some(artifact.artifact_id.as_str());
        self.request_thread_artifact_preview_load(summary.workspace_id.as_str(), artifact, cx);
        let preview_image_path = self
            .thread_artifacts
            .preview_square_image_path(artifact)
            .map(PathBuf::from);

        h_flex()
            .id((
                "thread-artifact-row",
                client_artifact_presentation::stable_artifact_row_id(artifact.artifact_id.as_str()),
            ))
            .w_full()
            .items_center()
            .gap_3()
            .rounded_md()
            .px_1()
            .py_1p5()
            .bg(if selected {
                cx.theme().muted
            } else {
                cx.theme().background
            })
            .hover(|this| this.bg(cx.theme().muted.opacity(1.)))
            .child(self.render_thread_artifact_thumbnail(artifact, px(34.), preview_image_path, cx))
            .child(
                v_flex()
                    .min_w_0()
                    .flex_1()
                    .gap_1p5()
                    .child(
                        div()
                            .text_sm()
                            .line_height(relative(1.))
                            .font_medium()
                            .whitespace_nowrap()
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(artifact.display_name.clone()),
                    )
                    .child(
                        h_flex()
                            .gap_1()
                            .opacity(0.6)
                            .text_xs()
                            .line_height(relative(1.))
                            .child(format_artifact_size(artifact.size_bytes))
                            .child("·")
                            .child(created_by_label(summary.created_by_kind))
                            .child("·")
                            .child(status_label(artifact.status)),
                    ),
            )
            .on_click(cx.listener(move |view, _, _, cx| {
                view.select_thread_artifact(artifact_id.clone(), cx);
            }))
            .into_any_element()
    }

    fn render_thread_artifact_detail(
        &self,
        summary: &ArtifactSummary,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let artifact = &summary.artifact;
        let created_at = format_timestamp(summary.created_at);
        let action_status = self.thread_artifacts.action_status(artifact).cloned();
        let action_in_progress = self.thread_artifacts.action_in_progress(artifact);
        let can_download = artifact.status == ArtifactStatus::Ready
            && self.gateway.connection_state == GatewayConnectionState::Connected
            && !action_in_progress;
        let reveal_enabled = !action_in_progress
            && self
                .thread_artifacts
                .local_file(artifact)
                .is_some_and(|local_file| local_file.path.is_file());
        self.request_thread_artifact_preview_load(summary.workspace_id.as_str(), artifact, cx);
        let preview_image_path = self
            .thread_artifacts
            .preview_detail_image_path(artifact)
            .map(PathBuf::from);

        let mut metadata_rows = v_flex().w_full().gap_1();
        for (key, value) in &summary.metadata {
            metadata_rows = metadata_rows.child(detail_row(key.to_string(), value.to_string(), cx));
        }

        let mut binding_rows = v_flex().w_full().gap_1();
        for binding in &summary.bindings {
            binding_rows = binding_rows.child(
                v_flex()
                    .w_full()
                    .gap_1()
                    .rounded_md()
                    .bg(cx.theme().muted.opacity(0.45))
                    .p_2()
                    .child(
                        h_flex()
                            .justify_between()
                            .text_xs()
                            .child(binding_kind_label(binding.binding_kind))
                            .child(binding_direction_label(binding.direction)),
                    )
                    .child(div().text_xs().child(binding_target_label(
                        binding.thread_id.as_deref(),
                        binding.turn_id.as_deref(),
                        binding.message_id.as_deref(),
                        binding.task_id.as_deref(),
                        binding.tool_call_id.as_deref(),
                    ))),
            );
        }

        v_flex()
            .w_full()
            .gap_2()
            .mb_1()
            .bg(cx.theme().muted)
            .p_1p5()
            .rounded_md()
            .when(
                self.thread_artifacts.has_loadable_preview(artifact),
                |this| {
                    this.child(self.render_thread_artifact_large_preview(
                        artifact,
                        preview_image_path,
                        cx,
                    ))
                },
            )
            .child(
                v_flex()
                    .gap_1()
                    .child(detail_row(
                        t!("artifacts.version").to_string(),
                        artifact
                            .version_id
                            .clone()
                            .unwrap_or_else(|| t!("artifacts.version_latest").to_string()),
                        cx,
                    ))
                    .child(detail_row(
                        t!("artifacts.kind").to_string(),
                        artifact_kind_label(artifact.kind),
                        cx,
                    ))
                    .child(detail_row(
                        t!("artifacts.size").to_string(),
                        format_artifact_size(artifact.size_bytes),
                        cx,
                    ))
                    .child(detail_row(
                        t!("artifacts.source_label").to_string(),
                        created_by_label(summary.created_by_kind),
                        cx,
                    ))
                    .child(detail_row(
                        t!("artifacts.created").to_string(),
                        created_at,
                        cx,
                    )),
            )
            .when_some(action_status.as_ref(), |this, status| {
                this.child(render_thread_artifact_action_status(status, cx))
            })
            .child(h_flex().w_full().gap_1().children([
                {
                    let summary = summary.clone();
                    artifact_action_button(
                        Button::new("artifact-download-action")
                            .icon(IconName::ArrowDown)
                            .tooltip(t!("artifacts.action.download_tooltip").to_string())
                            .on_click(cx.listener(move |view, _, window, cx| {
                                view.choose_thread_artifact_download_destination(
                                    summary.clone(),
                                    window,
                                    cx,
                                );
                            })),
                        can_download,
                        cx,
                    )
                },
                {
                    let summary = summary.clone();
                    artifact_action_button(
                        Button::new("artifact-open-action")
                            .icon(IconName::ExternalLink)
                            .tooltip(t!("artifacts.action.open_tooltip").to_string())
                            .on_click(cx.listener(move |view, _, _, cx| {
                                view.open_thread_artifact(summary.clone(), cx);
                            })),
                        can_download,
                        cx,
                    )
                },
                {
                    let artifact = artifact.clone();
                    artifact_action_button(
                        Button::new("artifact-reveal-action")
                            .icon(IconName::FolderOpen)
                            .tooltip(t!("artifacts.action.reveal_tooltip").to_string())
                            .on_click(cx.listener(move |view, _, _, cx| {
                                view.reveal_thread_artifact(artifact.clone(), cx);
                            })),
                        reveal_enabled,
                        cx,
                    )
                },
                {
                    let artifact = artifact.clone();
                    let attach_enabled = artifact.status == ArtifactStatus::Ready;
                    artifact_action_button(
                        Button::new("artifact-attach-action")
                            .icon(IconName::Plus)
                            .tooltip(t!("artifacts.action.attach_tooltip").to_string())
                            .on_click(cx.listener(move |view, _, _, cx| {
                                view.attach_artifact_to_composer(artifact.clone(), cx);
                            })),
                        attach_enabled,
                        cx,
                    )
                },
            ]))
            .into_any_element()
    }

    fn render_thread_artifact_thumbnail(
        &self,
        artifact: &ArtifactRef,
        size: Pixels,
        preview_image_path: Option<PathBuf>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let fallback_icon = kind_icon(artifact.kind);

        if let Some(image_path) = preview_image_path {
            div()
                .size(size)
                .flex_none()
                .overflow_hidden()
                .rounded_full()
                .bg(cx.theme().muted)
                .child(
                    img(image_path)
                        .size(size)
                        .rounded_full()
                        .with_fallback(move || {
                            Icon::new(fallback_icon.clone())
                                .size_4()
                                .opacity(0.8)
                                .into_any_element()
                        }),
                )
                .into_any_element()
        } else {
            div()
                .size(size)
                .flex_none()
                .flex()
                .items_center()
                .justify_center()
                .child(Icon::new(fallback_icon.clone()).size_4().opacity(0.65))
                .into_any_element()
        }
    }

    fn render_thread_artifact_large_preview(
        &self,
        artifact: &ArtifactRef,
        preview_image_path: Option<PathBuf>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let fallback_icon = kind_icon(artifact.kind);
        let mut container = div()
            .w_full()
            .relative()
            .overflow_hidden()
            .rounded_md()
            .bg(cx.theme().muted.opacity(0.35))
            .flex()
            .items_center()
            .justify_center();
        container.style().aspect_ratio = Some(THREAD_ARTIFACT_DETAIL_PREVIEW_ASPECT_RATIO);

        if let Some(image_path) = preview_image_path {
            container = container.child(
                img(image_path)
                    .absolute()
                    .top_0()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .w_full()
                    .h_full()
                    .rounded_md()
                    .object_fit(ObjectFit::Fill)
                    .with_fallback(move || {
                        Icon::new(fallback_icon.clone())
                            .size_6()
                            .opacity(0.65)
                            .into_any_element()
                    }),
            );
        } else {
            container = container.child(Icon::new(fallback_icon).size_6().opacity(0.55));
        }

        container.into_any_element()
    }
}

fn detail_row(label: String, value: String, _cx: &mut Context<PioneerDesktop>) -> AnyElement {
    h_flex()
        .w_full()
        .items_start()
        .gap_2()
        .text_xs()
        .child(div().w(px(92.)).flex_none().opacity(0.6).child(label))
        .child(
            div()
                .min_w_0()
                .flex_1()
                .overflow_hidden()
                .text_ellipsis()
                .child(value),
        )
        .into_any_element()
}

fn artifact_action_button(
    button: Button,
    enabled: bool,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let normal_fg = if enabled {
        cx.theme().muted_foreground
    } else {
        cx.theme().muted_foreground.opacity(0.45)
    };
    let hover_bg = cx.theme().muted_foreground.opacity(0.15);

    div()
        .size_6()
        .flex_none()
        .flex()
        .items_center()
        .justify_center()
        .rounded_md()
        .when(enabled, |this| {
            this.cursor_pointer().hover(move |this| this.bg(hover_bg))
        })
        .child(
            button
                .w_full()
                .h_full()
                .text()
                .disabled(!enabled)
                .text_color(normal_fg)
                .small(),
        )
        .into_any_element()
}

fn render_thread_artifact_action_status(
    status: &ThreadArtifactActionStatus,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let failed = matches!(status, ThreadArtifactActionStatus::Failed(_));
    h_flex()
        .mt_1()
        .w_full()
        .gap_2()
        .items_center()
        .rounded_md()
        .bg(if failed {
            cx.theme().danger.opacity(0.1)
        } else {
            cx.theme().muted.opacity(0.8)
        })
        .px_2()
        .py_1p5()
        .child(if failed {
            Icon::new(IconName::TriangleAlert)
                .size_3()
                .text_color(cx.theme().danger)
                .into_any_element()
        } else {
            Spinner::new()
                .icon(IconName::Loader)
                .color(cx.theme().muted_foreground)
                .into_any_element()
        })
        .child(
            div()
                .min_w_0()
                .flex_1()
                .text_xs()
                .line_height(relative(1.25))
                .text_color(if failed {
                    cx.theme().danger
                } else {
                    cx.theme().muted_foreground
                })
                .child(action_status_label(status)),
        )
        .into_any_element()
}

fn action_status_label(status: &ThreadArtifactActionStatus) -> String {
    match status {
        ThreadArtifactActionStatus::Queued => t!("artifacts.action.status.queued").to_string(),
        ThreadArtifactActionStatus::Downloading => {
            t!("artifacts.action.status.downloading").to_string()
        }
        ThreadArtifactActionStatus::Verifying => {
            t!("artifacts.action.status.verifying").to_string()
        }
        ThreadArtifactActionStatus::Opening => t!("artifacts.action.status.opening").to_string(),
        ThreadArtifactActionStatus::Revealing => {
            t!("artifacts.action.status.revealing").to_string()
        }
        ThreadArtifactActionStatus::Failed(error) => {
            format!("{}: {error}", t!("artifacts.action.status.failed"))
        }
    }
}

fn filter_label(filter: ThreadArtifactFilter) -> String {
    match filter {
        ThreadArtifactFilter::All => t!("artifacts.filter.all").to_string(),
        ThreadArtifactFilter::Uploaded => t!("artifacts.filter.uploaded").to_string(),
        ThreadArtifactFilter::Generated => t!("artifacts.filter.generated").to_string(),
        ThreadArtifactFilter::TaskOutput => t!("artifacts.filter.task_output").to_string(),
        ThreadArtifactFilter::Images => t!("artifacts.filter.images").to_string(),
        ThreadArtifactFilter::Documents => t!("artifacts.filter.documents").to_string(),
    }
}

fn kind_icon(_kind: ArtifactKind) -> IconName {
    IconName::File
}

fn artifact_kind_label(kind: ArtifactKind) -> String {
    client_artifact_presentation::artifact_kind_code(kind)
}

fn created_by_label(kind: ArtifactCreatedByKind) -> String {
    match kind {
        ArtifactCreatedByKind::User => t!("artifacts.source.user").to_string(),
        ArtifactCreatedByKind::Agent => t!("artifacts.source.agent").to_string(),
        ArtifactCreatedByKind::Tool => t!("artifacts.source.tool").to_string(),
        ArtifactCreatedByKind::Task => t!("artifacts.source.task").to_string(),
        ArtifactCreatedByKind::System => t!("artifacts.source.system").to_string(),
        ArtifactCreatedByKind::Import => t!("artifacts.source.import").to_string(),
        ArtifactCreatedByKind::ExternalAgent => t!("artifacts.source.external_agent").to_string(),
    }
}

fn status_label(status: ArtifactStatus) -> String {
    match status {
        ArtifactStatus::Ready => t!("artifacts.status.ready").to_string(),
        ArtifactStatus::Pending => t!("artifacts.status.pending").to_string(),
        ArtifactStatus::Quarantined => t!("artifacts.status.quarantined").to_string(),
        ArtifactStatus::Deleted => t!("artifacts.status.deleted").to_string(),
        ArtifactStatus::MissingExternalSource => {
            t!("artifacts.status.missing_external_source").to_string()
        }
        ArtifactStatus::Failed => t!("artifacts.status.failed").to_string(),
    }
}

fn binding_kind_label(kind: ArtifactBindingKind) -> String {
    client_artifact_presentation::artifact_binding_kind_code(kind)
}

fn binding_direction_label(direction: ArtifactBindingDirection) -> String {
    client_artifact_presentation::artifact_binding_direction_code(direction)
}

fn binding_target_label(
    thread_id: Option<&str>,
    turn_id: Option<&str>,
    message_id: Option<&str>,
    task_id: Option<&str>,
    tool_call_id: Option<&str>,
) -> String {
    let summary = client_artifact_presentation::artifact_binding_target_summary(
        thread_id,
        turn_id,
        message_id,
        task_id,
        tool_call_id,
    );
    if summary.is_empty() {
        t!("artifacts.provenance_unknown").to_string()
    } else {
        summary
    }
}

fn format_timestamp(timestamp_ms: i64) -> String {
    Local
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .map(|dt| dt.format("%d.%m.%Y %H:%M").to_string())
        .unwrap_or_else(|| timestamp_ms.to_string())
}
