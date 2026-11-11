use crate::assets::PioneerIconName;
use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::mcp::{
    presentation as mcp_presentation,
    types::{McpListItem, McpServerStatus},
};

/// Shared catalogue card. Navigation and domain commands belong to the caller.
#[derive(gpui_kit::IntoElement)]
pub struct McpServerCard {
    title: String,
    capability_labels: Vec<String>,
    status_label: String,
    status_color: Hsla,
    pending: bool,
}
impl McpServerCard {
    pub const HEIGHT: f32 = 78.0;

    pub fn new(server: &McpListItem, cx: &mut App) -> Self {
        Self {
            title: mcp_presentation::mcp_display_name(server),
            capability_labels: Self::mcp_capability_labels(
                mcp_presentation::mcp_capability_counts(server).as_slice(),
            ),
            status_label: Self::mcp_status_label(server.status),
            status_color: Self::mcp_status_color(server.status, cx),
            pending: false,
        }
    }

    /// Counts stay absent when the component has no native installation.
    pub fn missing(title: String, status_label: String, status_color: Hsla) -> Self {
        Self {
            title,
            capability_labels: Vec::new(),
            status_label,
            status_color,
            pending: false,
        }
    }
    pub fn pending(mut self, pending: bool) -> Self {
        self.pending = pending;
        self
    }

    pub fn status(mut self, label: String, color: Hsla) -> Self {
        self.status_label = label;
        self.status_color = color;
        self
    }

    pub(crate) fn mcp_status_label(status: McpServerStatus) -> String {
        Self::mcp_status_label_from_kind(mcp_presentation::mcp_status_label(status))
    }

    pub(crate) fn mcp_status_label_from_kind(status: mcp_presentation::McpStatusLabel) -> String {
        match status {
            mcp_presentation::McpStatusLabel::NotStarted => {
                t!("mcp.status.not_started").to_string()
            }
            mcp_presentation::McpStatusLabel::Disabled => t!("mcp.status.disabled").to_string(),
            mcp_presentation::McpStatusLabel::Starting => t!("mcp.status.starting").to_string(),
            mcp_presentation::McpStatusLabel::Ready => t!("mcp.status.ready").to_string(),
            mcp_presentation::McpStatusLabel::Degraded => t!("mcp.status.degraded").to_string(),
            mcp_presentation::McpStatusLabel::AuthRequired => {
                t!("mcp.status.auth_required").to_string()
            }
            mcp_presentation::McpStatusLabel::Failed => t!("mcp.status.failed").to_string(),
            mcp_presentation::McpStatusLabel::Stopping => t!("mcp.status.stopping").to_string(),
            mcp_presentation::McpStatusLabel::Stopped => t!("mcp.status.stopped").to_string(),
            mcp_presentation::McpStatusLabel::Restarting => t!("mcp.status.restarting").to_string(),
        }
    }

    pub(crate) fn mcp_status_color(status: McpServerStatus, cx: &mut App) -> Hsla {
        match mcp_presentation::mcp_status_tone(status) {
            mcp_presentation::McpPresentationTone::Success => cx.theme().success,
            mcp_presentation::McpPresentationTone::Warning => cx.theme().warning,
            mcp_presentation::McpPresentationTone::Danger => cx.theme().danger,
            mcp_presentation::McpPresentationTone::Muted => cx.theme().muted_foreground,
            mcp_presentation::McpPresentationTone::Default => cx.theme().foreground.opacity(0.84),
        }
    }

    pub(crate) fn mcp_capability_labels(
        counts: &[mcp_presentation::McpCapabilityCount],
    ) -> Vec<String> {
        counts
            .iter()
            .map(|count| {
                let label = match count.kind {
                    mcp_presentation::McpCapabilityKind::Tools => {
                        t!("mcp.capabilities.tools").to_string()
                    }
                    mcp_presentation::McpCapabilityKind::Resources => {
                        t!("mcp.capabilities.resources").to_string()
                    }
                    mcp_presentation::McpCapabilityKind::ResourceTemplates => {
                        t!("mcp.capabilities.templates").to_string()
                    }
                    mcp_presentation::McpCapabilityKind::Prompts => {
                        t!("mcp.capabilities.prompts").to_string()
                    }
                };
                format!("{} {label}", count.count)
            })
            .collect()
    }
}
impl RenderOnce for McpServerCard {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        h_flex()
            .w_full()
            .h(px(Self::HEIGHT))
            .pt_3()
            .px_4()
            .pb_3()
            .rounded_lg()
            .bg(cx.theme().background)
            .border_1()
            .border_color(cx.theme().border)
            .gap_4()
            .items_start()
            .hover(|this| this.bg(cx.theme().secondary.opacity(0.45)))
            .child(
                v_flex()
                    .flex_1()
                    .h_full()
                    .gap_1()
                    .child(
                        h_flex()
                            .w_full()
                            .justify_between()
                            .items_center()
                            .gap_2()
                            .child(div().text_sm().font_semibold().child(self.title)),
                    )
                    .child(div().flex_1())
                    .child(
                        h_flex().justify_between().items_center().gap_2().child(
                            h_flex()
                                .items_center()
                                .gap_2()
                                .children(
                                    self.capability_labels
                                        .into_iter()
                                        .map(|label| div().text_xs().opacity(0.6).child(label)),
                                )
                                .when(self.pending, |this| {
                                    this.child(
                                        Icon::new(PioneerIconName::RefreshCw)
                                            .size_3()
                                            .text_color(cx.theme().warning),
                                    )
                                }),
                        ),
                    ),
            )
            .child(
                pioneer_desktop_foundation::catalog_status_badge(
                    self.status_label,
                    self.status_color,
                )
                .mt_1p5(),
            )
    }
}
