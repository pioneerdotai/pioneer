use super::{ADMINISTRATION_CONTENT_INVITATIONS_NODE_ID, ADMINISTRATION_CONTENT_MEMBERS_NODE_ID};
use crate::app::root::{
    AdministrationContentView, GatewayConnectionState, MainContentView, PioneerDesktop,
};
use gpui::*;
use gpui_component::tree::TreeItem;
use pioneer_client::authorization::{
    PrincipalPresentationCapabilities, principal_presentation_capabilities_from_auth,
};

impl PioneerDesktop {
    pub(super) fn principal_presentation_capabilities(&self) -> PrincipalPresentationCapabilities {
        self.gateway
            .current_auth
            .as_ref()
            .map(principal_presentation_capabilities_from_auth)
            .unwrap_or_default()
    }

    pub(in crate::app) fn refresh_current_principal(&mut self, cx: &mut Context<Self>) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.gateway.current_auth = None;
            self.sync_settings_sidebar_tree_state(cx);
            self.sync_administration_sidebar_tree_state(cx);
            return;
        }
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx.background_spawn(async move { sender.auth_me() }).await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.gateway.current_auth = result.ok();
                    view.resolve_current_principal_avatar(cx);
                    view.sync_settings_sidebar_tree_state(cx);
                    view.sync_administration_sidebar_tree_state(cx);
                    if view.main_content_view == MainContentView::Administration {
                        view.refresh_current_administration_content(cx);
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn open_administration_screen_from_bottom_bar(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        self.sync_administration_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Administration, cx);
        self.refresh_current_administration_content(cx);
    }

    pub(in crate::app) fn open_administration_content(
        &mut self,
        content_view: AdministrationContentView,
        cx: &mut Context<Self>,
    ) {
        self.administration_content_view = content_view;
        self.sync_administration_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Administration, cx);
        self.refresh_current_administration_content(cx);
    }

    pub(in crate::app) fn sync_administration_sidebar_tree_state(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let capabilities = self.principal_presentation_capabilities();
        let mut items = Vec::with_capacity(2);
        if capabilities.can_view_member_directory {
            items.push((
                AdministrationContentView::Members,
                TreeItem::new(ADMINISTRATION_CONTENT_MEMBERS_NODE_ID, "members"),
            ));
        }
        if capabilities.can_view_invitations {
            items.push((
                AdministrationContentView::Invitations,
                TreeItem::new(ADMINISTRATION_CONTENT_INVITATIONS_NODE_ID, "invitations"),
            ));
        }

        if !items
            .iter()
            .any(|(content_view, _)| *content_view == self.administration_content_view)
        {
            self.administration_content_view = items
                .first()
                .map(|(content_view, _)| *content_view)
                .unwrap_or(AdministrationContentView::Members);
        }

        let selected_ix = items
            .iter()
            .position(|(content_view, _)| *content_view == self.administration_content_view);
        let administration_tree_state = self.administration_tree_state.clone();
        administration_tree_state.update(cx, |state, cx| {
            state.set_items(
                items.into_iter().map(|(_, item)| item).collect::<Vec<_>>(),
                cx,
            );
            state.set_selected_index(selected_ix, cx);
        });
    }

    pub(in crate::app) fn refresh_current_administration_content(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        match self.administration_content_view {
            AdministrationContentView::Members => {
                self.refresh_members(false, cx);
                self.refresh_all_workspace_members(cx);
            }
            AdministrationContentView::Invitations => self.refresh_invitations(false, cx),
        }
    }
}
