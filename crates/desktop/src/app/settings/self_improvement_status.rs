use crate::app::root::{
    GatewayConnectionState, MainContentView, PioneerDesktop, SettingsContentView,
};
use crate::components::buttonts::small_outline_button;
use gpui_kit::component::{ActiveTheme, Disableable, StyledExt};
use gpui_kit::{prelude::*, *};
use pioneer_protocol::{SelfImprovementPhase as Phase, SelfImprovementStatusReason as Reason};
use std::time::Duration;

impl PioneerDesktop {
    pub(super) fn start_self_improvement_status_poll(&mut self, cx: &mut Context<Self>) {
        self.self_improvement_status_poll =
            Some(cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                let mut cx = cx.clone();
                async move {
                    loop {
                        cx.background_executor().timer(Duration::from_secs(5)).await;
                        let keep_polling = this.update(&mut cx, |view, cx| {
                            if view.main_content_view != MainContentView::Settings
                                || view.settings_content_view
                                    != SettingsContentView::SelfImprovement
                            {
                                return false;
                            }
                            // The existing refresh owner gates concurrent requests and rejects stale
                            // workspace/connection responses. Never retain a workspace in this timer.
                            if view.gateway.connection_state == GatewayConnectionState::Connected {
                                view.refresh_gateway_settings(cx);
                                cx.notify();
                            }
                            true
                        });
                        if !matches!(keep_polling, Ok(true)) {
                            break;
                        }
                    }
                }
            }));
    }

    pub(super) fn render_self_improvement_status(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let status = pioneer_client::settings::self_improvement::status_for_workspace(
            self.gateway.settings.as_ref(),
            self.active_workspace_id(),
        );
        let mut content = div().flex().flex_col().w_full().min_w_0().gap_2().child(
            div()
                .flex()
                .items_center()
                .justify_between()
                .gap_3()
                .child(
                    div()
                        .text_sm()
                        .font_semibold()
                        .child(t!("settings.self_improvement.status.title").to_string()),
                )
                .child(
                    small_outline_button("self-improvement-status-refresh")
                        .label(t!("settings.self_improvement.status.refresh").to_string())
                        .disabled(
                            self.gateway.settings_loading
                                || self.gateway.connection_state
                                    != GatewayConnectionState::Connected,
                        )
                        .on_click(cx.listener(|view, _, _, cx| {
                            view.refresh_gateway_settings(cx);
                            cx.notify();
                        })),
                ),
        );
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            content = content.child(
                div()
                    .text_sm()
                    .child(t!("settings.self_improvement.status.offline").to_string()),
            );
        } else if self.gateway.settings_error.is_some() {
            content = content.child(
                div()
                    .text_sm()
                    .text_color(theme.warning)
                    .child(t!("settings.self_improvement.status.refresh_failed").to_string()),
            );
        } else if self.gateway.settings_loading && status.is_none() {
            content = content.child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(t!("settings.self_improvement.status.loading").to_string()),
            );
        }
        if let Some(status) = status {
            content = content
                .child(
                    div()
                        .text_sm()
                        .font_medium()
                        .child(phase_label(status.phase)),
                )
                .child(
                    div()
                        .text_sm()
                        .whitespace_normal()
                        .child(reason_label(status.reason)),
                );
            if let Some(progress) = status.progress {
                content = content.child(
                    div().text_sm().child(
                        t!(
                            "settings.self_improvement.status.progress",
                            processed = progress.processed_chunks.to_string(),
                            total = progress.total_chunks.to_string()
                        )
                        .to_string(),
                    ),
                );
            }
            content = content
                .child(status_time(
                    "settings.self_improvement.status.last_run",
                    status.last_run_at_unix,
                ))
                .child(status_time(
                    "settings.self_improvement.status.next_run",
                    status.next_scheduled_at_unix,
                ));
            if let Some(result) = status.last_result.filter(|reason| *reason != status.reason) {
                content = content.child(
                    div().text_sm().whitespace_normal().child(
                        t!(
                            "settings.self_improvement.status.last_result",
                            result = reason_label(result)
                        )
                        .to_string(),
                    ),
                );
            }
            if status.next_retry_at_unix.is_some() {
                content = content.child(status_time(
                    "settings.self_improvement.status.next_retry",
                    status.next_retry_at_unix,
                ));
            }
            content = content.child(status_time(
                "settings.self_improvement.status.observed",
                Some(status.observed_at_unix),
            ));
        } else if !self.gateway.settings_loading {
            content = content.child(
                div()
                    .text_sm()
                    .child(t!("settings.self_improvement.status.unavailable").to_string()),
            );
        }
        content.into_any_element()
    }
}

fn status_time(key: &'static str, value: Option<i64>) -> AnyElement {
    let value = value
        .and_then(|unix| chrono::DateTime::from_timestamp(unix, 0))
        .map(|time| {
            time.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M:%S %Z")
                .to_string()
        })
        .unwrap_or_else(|| t!("settings.self_improvement.status.not_scheduled").to_string());
    div()
        .text_xs()
        .child(t!(key, time = value).to_string())
        .into_any_element()
}

fn phase_label(phase: Phase) -> String {
    let key = match phase {
        Phase::Disabled => "disabled",
        Phase::Unavailable => "unavailable",
        Phase::Waiting => "waiting",
        Phase::Running => "running",
        Phase::Retrying => "retrying",
        Phase::Failed => "failed",
        Phase::NoChange => "no_change",
        Phase::Completed => "completed",
        Phase::Cancelled => "cancelled",
    };
    let key = format!("settings.self_improvement.status.phase_{key}");
    t!(&key).to_string()
}

fn reason_label(reason: Reason) -> String {
    let key = match reason {
        Reason::Disabled => "disabled",
        Reason::ModelUnavailable => "model_unavailable",
        Reason::WorkerUnavailable => "worker_unavailable",
        Reason::Preparing => "preparing",
        Reason::NoNewSources => "no_new_sources",
        Reason::AwaitingSchedule => "awaiting_schedule",
        Reason::Analyzing => "analyzing",
        Reason::Finalizing => "finalizing",
        Reason::Pending => "pending",
        Reason::Recovering => "recovering",
        Reason::Timeout => "timeout",
        Reason::OutputLimit => "output_limit",
        Reason::InvalidResponse => "invalid_response",
        Reason::ProviderError => "provider_error",
        Reason::ResponseFiltered => "response_filtered",
        Reason::NoCandidate => "no_candidate",
        Reason::ReviewerRejected => "reviewer_rejected",
        Reason::ValidationRejected => "validation_rejected",
        Reason::Created => "created",
        Reason::Updated => "updated",
        Reason::RolledBack => "rolled_back",
        Reason::Cancelled => "cancelled",
        Reason::Unknown => "unknown",
    };
    let key = format!("settings.self_improvement.status.reason_{key}");
    t!(&key).to_string()
}

#[cfg(test)]
mod tests {
    use super::{Phase, Reason, phase_label, reason_label};

    #[test]
    fn operational_status_labels_are_translated_and_distinguish_outcomes() {
        for phase in [
            Phase::Disabled,
            Phase::Unavailable,
            Phase::Waiting,
            Phase::Running,
            Phase::Retrying,
            Phase::Failed,
            Phase::NoChange,
            Phase::Completed,
            Phase::Cancelled,
        ] {
            let label = phase_label(phase);
            assert!(!label.is_empty());
            assert!(!label.starts_with("settings.self_improvement.status."));
        }
        for reason in [
            Reason::Disabled,
            Reason::ModelUnavailable,
            Reason::WorkerUnavailable,
            Reason::Preparing,
            Reason::NoNewSources,
            Reason::AwaitingSchedule,
            Reason::Analyzing,
            Reason::Finalizing,
            Reason::Pending,
            Reason::Recovering,
            Reason::Timeout,
            Reason::OutputLimit,
            Reason::InvalidResponse,
            Reason::ProviderError,
            Reason::ResponseFiltered,
            Reason::NoCandidate,
            Reason::ReviewerRejected,
            Reason::ValidationRejected,
            Reason::Created,
            Reason::Updated,
            Reason::RolledBack,
            Reason::Cancelled,
            Reason::Unknown,
        ] {
            let label = reason_label(reason);
            assert!(!label.is_empty());
            assert!(!label.starts_with("settings.self_improvement.status."));
        }
        assert_ne!(phase_label(Phase::Failed), phase_label(Phase::NoChange));
        assert_ne!(reason_label(Reason::Created), reason_label(Reason::Updated));
    }
}
