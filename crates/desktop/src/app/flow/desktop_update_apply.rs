use super::*;
use crate::updater::plan::{DesktopUpdatePlanInput, prepare_desktop_update_apply};
use anyhow::Context as _;
use std::process::Command;

struct DesktopUpdateApplyFailedNotification;

impl PioneerDesktop {
    pub(crate) fn restart_to_apply_desktop_update(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.desktop_update.is_style_preview() {
            return;
        }

        let input = match &self.desktop_update {
            DesktopUpdateUiState::Ready {
                version,
                current_version,
                tag,
                asset_path,
                asset_name,
                sha256,
                os,
                arch,
                kind,
                ..
            } => DesktopUpdatePlanInput {
                target_version: version.clone(),
                current_version: current_version.clone(),
                tag: tag.clone(),
                os: os.clone(),
                arch: arch.clone(),
                asset_kind: kind.clone(),
                asset_path: asset_path.clone(),
                asset_name: asset_name.clone(),
                asset_sha256: sha256.clone(),
            },
            _ => return,
        };

        let prepared = match crate::state::runtime_home_dir()
            .context("failed to resolve Pioneer runtime home")
            .and_then(|runtime_home| prepare_desktop_update_apply(runtime_home.as_path(), input))
        {
            Ok(prepared) => prepared,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to prepare desktop update apply plan"
                );
                self.push_desktop_update_apply_failed_notification(
                    format!("{error:#}"),
                    window,
                    cx,
                );
                return;
            }
        };

        let spawn_result = Command::new(prepared.helper_path.as_path())
            .arg("apply")
            .arg("--plan")
            .arg(prepared.plan_path.as_path())
            .spawn()
            .with_context(|| {
                format!(
                    "failed to start desktop updater helper `{}`",
                    prepared.helper_path.display()
                )
            });

        match spawn_result {
            Ok(_) => {
                let target_version = prepared.plan.target_version.clone();
                self.desktop_update = DesktopUpdateUiState::Applying {
                    version: target_version,
                };
                cx.notify();
                cx.quit();
            }
            Err(error) => {
                warn!(
                    plan_path = %prepared.plan_path.display(),
                    helper_path = %prepared.helper_path.display(),
                    error = %format!("{error:#}"),
                    "failed to spawn desktop update helper"
                );
                self.push_desktop_update_apply_failed_notification(
                    format!("{error:#}"),
                    window,
                    cx,
                );
            }
        }
    }

    fn push_desktop_update_apply_failed_notification(
        &mut self,
        details: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let message = t!("desktop_update.apply_failed", error = details.as_str()).to_string();

        window.push_notification(
            Notification::new()
                .with_type(NotificationType::Warning)
                .id1::<DesktopUpdateApplyFailedNotification>(("desktop-update-apply-failed", 0u64))
                .content(move |_, _, _| {
                    v_flex()
                        .child(
                            div()
                                .text_sm()
                                .opacity(0.8)
                                .line_height(relative(1.4))
                                .whitespace_normal()
                                .child(message.clone()),
                        )
                        .into_any_element()
                }),
            cx,
        );
    }
}
