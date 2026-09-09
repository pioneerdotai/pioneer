use super::*;

impl PioneerDesktop {
    pub(in crate::app) fn refresh_workspace_bound_screens_after_switch(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        // Feature bindings reacquire demand from the Client workspace publication.
        let _ = cx;
    }
}
