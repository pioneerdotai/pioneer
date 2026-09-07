//! Retained workspace navigation over scoped Client publications.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod binding;
mod buttons;
mod catalog_dialogs;
mod catalog_view;
mod dialogs;
mod drag_preview;
mod sidebar;
mod tree_view;
use gpui_kit::component::dialog::Dialog;
use gpui_kit::{prelude::*, *};
use pioneer_client::{agents_doc::scope::AgentsDocEditorScope, core::ClientCore};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::{collections::HashMap, rc::Rc, sync::Arc};
type DialogBuilder = Rc<dyn Fn(Dialog, &mut Window, &mut App) -> Dialog>;
type DialogPresenter = Rc<dyn Fn(DialogBuilder, &mut Window, &mut App)>;

pub enum WorkspaceNavigationEvent {
    OpenThread { thread_id: Option<String> },
    OpenAgentsDocument { scope: AgentsDocEditorScope },
}
pub struct WorkspaceNavigationConfig {
    client: Arc<ClientCore>,
    workspace_preference: Rc<dyn Fn(&App) -> Option<String>>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    load_expansion: Rc<dyn Fn(&str, &mut App) -> HashMap<String, bool>>,
    save_expansion: Rc<dyn Fn(&str, HashMap<String, bool>, &mut App)>,
    context_locked: Rc<dyn Fn(&App) -> bool>,
    rename_thread_dialog: DialogPresenter,
    rename_folder_dialog: DialogPresenter,
    rename_workspace_dialog: DialogPresenter,
    create_workspace_dialog: DialogPresenter,
}
impl WorkspaceNavigationConfig {
    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        load_expansion: impl Fn(&str, &mut App) -> HashMap<String, bool> + 'static,
        save_expansion: impl Fn(&str, HashMap<String, bool>, &mut App) + 'static,
        context_locked: impl Fn(&App) -> bool + 'static,
        rename_thread_dialog: impl Fn(DialogBuilder, &mut Window, &mut App) + 'static,
        rename_folder_dialog: impl Fn(DialogBuilder, &mut Window, &mut App) + 'static,
        rename_workspace_dialog: impl Fn(DialogBuilder, &mut Window, &mut App) + 'static,
        create_workspace_dialog: impl Fn(DialogBuilder, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            client,
            workspace_preference: Rc::new(|_| None),
            registrar,
            load_expansion: Rc::new(load_expansion),
            save_expansion: Rc::new(save_expansion),
            context_locked: Rc::new(context_locked),
            rename_thread_dialog: Rc::new(rename_thread_dialog),
            rename_folder_dialog: Rc::new(rename_folder_dialog),
            rename_workspace_dialog: Rc::new(rename_workspace_dialog),
            create_workspace_dialog: Rc::new(create_workspace_dialog),
        }
    }
    pub fn with_workspace_preference(
        mut self,
        read: impl Fn(&App) -> Option<String> + 'static,
    ) -> Self {
        self.workspace_preference = Rc::new(read);
        self
    }
}
pub struct WorkspaceNavigationView {
    sidebar: Entity<sidebar::ThreadSidebarView>,
    _events: Subscription,
}
impl EventEmitter<WorkspaceNavigationEvent> for WorkspaceNavigationView {}
impl WorkspaceNavigationView {
    pub fn new(config: WorkspaceNavigationConfig, cx: &mut Context<Self>) -> Self {
        let sidebar = cx.new(|cx| sidebar::ThreadSidebarView::new(config, cx));
        let events = cx.subscribe(
            &sidebar,
            |_, _, event: &WorkspaceNavigationEvent, cx| match event {
                WorkspaceNavigationEvent::OpenThread { thread_id } => {
                    cx.emit(WorkspaceNavigationEvent::OpenThread {
                        thread_id: thread_id.clone(),
                    })
                }
                WorkspaceNavigationEvent::OpenAgentsDocument { scope } => {
                    cx.emit(WorkspaceNavigationEvent::OpenAgentsDocument {
                        scope: scope.clone(),
                    })
                }
            },
        );
        Self {
            sidebar,
            _events: events,
        }
    }
    pub fn refresh_presentation(&mut self, cx: &mut Context<Self>) {
        self.sidebar
            .update(cx, |view, cx| view.refresh_presentation(cx));
    }
    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.sidebar.update(cx, |view, _| view.close());
    }
}
impl Render for WorkspaceNavigationView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .key_context("WorkspaceNavigation")
            .on_action(cx.listener(|view, _: &NewThread, window, cx| {
                view.sidebar.update(cx, |sidebar, cx| {
                    sidebar.open_or_create_new_thread_from_sidebar(window, cx)
                })
            }))
            .on_action(cx.listener(|view, action: &RenameThread, window, cx| {
                view.rename_thread(action, window, cx)
            }))
            .on_action(cx.listener(|view, action: &SelectThread, window, cx| {
                view.sidebar.update(cx, |sidebar, cx| {
                    sidebar.open_thread_from_sidebar(action.thread_id.clone(), window, cx)
                })
            }))
            .on_action(cx.listener(|view, action: &SelectWorkspace, _, cx| {
                view.sidebar.update(cx, |sidebar, cx| {
                    sidebar.switch_workspace_from_popover(action.workspace_id.clone(), cx)
                })
            }))
            .on_action(cx.listener(|view, action: &DeleteThread, _, cx| {
                view.sidebar.update(cx, |sidebar, cx| {
                    sidebar.delete_thread(&action.thread_id, cx)
                })
            }))
            .on_action(cx.listener(|view, action: &MoveThread, _, cx| {
                view.sidebar.update(cx, |sidebar, cx| {
                    sidebar.move_thread(&action.thread_id, action.folder_id.clone(), cx)
                })
            }))
            .child(self.sidebar.clone())
    }
}

#[derive(Clone, Debug, PartialEq, gpui_kit::Action, serde::Deserialize)]
#[action(no_json)]
pub struct RenameThread {
    pub thread_id: String,
}
impl WorkspaceNavigationView {
    pub fn rename_thread(
        &mut self,
        action: &RenameThread,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.sidebar.update(cx, |sidebar, cx| {
            sidebar.open_rename_thread_dialog(action.thread_id.clone(), window, cx)
        });
    }
}

actions!(workspace_navigation, [NewThread]);
#[derive(Clone, Debug, PartialEq, gpui_kit::Action, serde::Deserialize)]
#[action(no_json)]
pub struct SelectThread {
    pub thread_id: String,
}
#[derive(Clone, Debug, PartialEq, gpui_kit::Action, serde::Deserialize)]
#[action(no_json)]
pub struct SelectWorkspace {
    pub workspace_id: String,
}
#[derive(Clone, Debug, PartialEq, gpui_kit::Action, serde::Deserialize)]
#[action(no_json)]
pub struct DeleteThread {
    pub thread_id: String,
}
#[derive(Clone, Debug, PartialEq, gpui_kit::Action, serde::Deserialize)]
#[action(no_json)]
pub struct MoveThread {
    pub thread_id: String,
    pub folder_id: Option<String>,
}

#[cfg(test)]
mod context_tests;
