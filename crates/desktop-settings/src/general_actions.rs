use crate::{binding::SettingsBinding, screen::SettingsConfig};
use gpui_kit::component::{button::*, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::ClientScope,
    settings::runtime::{SettingsIntent, SettingsPage, SettingsPagePublication, SettingsPageValue},
};
use std::sync::Arc;
pub(crate) struct GeneralActionsView {
    config: SettingsConfig,
    value: Option<bool>,
    _binding: Arc<SettingsBinding>,
    _task: Task<()>,
}
impl GeneralActionsView {
    pub fn new(config: SettingsConfig, cx: &mut App) -> Entity<Self> {
        cx.new(|cx| {
            let binding = SettingsBinding::new(
                vec![ClientScope::SettingsPage {
                    page: SettingsPage::General,
                }],
                &config.bindings,
            );
            let mut changed = binding.changed.subscribe();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if view
                        .update(cx, |view, cx| {
                            let value = view.read();
                            if value != view.value {
                                view.value = value;
                                cx.notify();
                            }
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let mut view = Self {
                config,
                value: None,
                _binding: binding,
                _task: task,
            };
            view.value = view.read();
            view
        })
    }
    fn read(&self) -> Option<bool> {
        let publication = self
            .config
            .client
            .snapshot(&ClientScope::SettingsPage {
                page: SettingsPage::General,
            })?
            .typed::<SettingsPagePublication>()?;
        match &publication.payload().value {
            Some(SettingsPageValue::General { settings }) => Some(settings.keepawake),
            _ => None,
        }
    }
}
impl Render for GeneralActionsView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        Button::new("toggle-keepawake")
            .ghost()
            .small()
            .compact()
            .disabled(self.value.is_none())
            .tooltip(t!("settings.option.keepawake.tooltip").to_string())
            .child(
                Icon::empty()
                    .path("icons/power-off.svg")
                    .size_3p5()
                    .opacity(0.6)
                    .when(self.value == Some(true), |icon| {
                        icon.opacity(1.).text_color(cx.theme().blue)
                    }),
            )
            .on_click(cx.listener(|view, _, _, _| {
                if let Some(enabled) = view.value {
                    view.config
                        .client
                        .settings_intent(SettingsIntent::Keepawake { enabled: !enabled });
                } else {
                    view.config.client.settings_intent(SettingsIntent::Refresh);
                }
            }))
    }
}
