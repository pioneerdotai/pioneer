mod composer;
mod header;
pub(crate) mod timeline;

use super::super::{conversation::ConversationViewState, root::PioneerDesktop};
use gpui::{prelude::*, *};
use gpui_component::{theme::ActiveTheme, *};

impl PioneerDesktop {
    pub(crate) fn render_thread(&self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let active_thread_id = self.current_active_thread_id().map(str::to_owned);

        let default_projection = ConversationViewState::default();
        let projection = self
            .active_thread_conversation()
            .map(|conversation| conversation.projection())
            .unwrap_or(&default_projection);

        let timeline = self.render_timeline(active_thread_id.as_deref(), projection, window, cx);

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .child(self.render_thread_header(cx))
            .child(div().flex_1().child(timeline))
            .child(self.render_composer(cx))
            .into_any_element()
    }
}
