use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::mcp::{
    presentation as mcp_presentation,
    types::{McpListItem, McpServerStatus},
};

/// Shared catalogue card. Navigation and domain commands belong to the caller.
#[derive(gpui_kit::IntoElement)]
pub(crate) struct McpServerCard {
    title: String,
    capability_labels: Vec<String>,
    status_label: String,
    status_color: Hsla,
    pending: bool,
}
impl McpServerCard {
    pub const HEIGHT: f32 = pioneer_desktop_foundation::CatalogSummaryCard::HEIGHT;

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

    pub fn pending(mut self, pending: bool) -> Self {
        self.pending = pending;
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
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        pioneer_desktop_foundation::CatalogSummaryCard::new(
            self.title,
            self.capability_labels,
            self.status_label,
            self.status_color,
        )
        .pending(self.pending)
    }
}
