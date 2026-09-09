use crate::app::root::{MainContentView, PioneerDesktop};
use gpui_kit::*;
impl PioneerDesktop {
    pub(in crate::app) fn open_skills_screen_from_bottom_bar(&mut self, cx: &mut Context<Self>) {
        if self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            self.set_main_content_view(MainContentView::Skills, cx);
        }
    }
}
