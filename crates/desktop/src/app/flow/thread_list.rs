use super::*;
use pioneer_client::threads::tree as thread_tree;

impl PioneerDesktop {
    pub(crate) fn upsert_thread_for_workspace(&mut self, thread_id: &str, workspace_id: &str) {
        self.upsert_thread_coordinator(thread_id, workspace_id);
    }

    pub(crate) fn thread_workspace_matches(&self, thread_id: &str, workspace_id: &str) -> bool {
        self.thread_workspace_id(thread_id).as_deref() == Some(workspace_id)
    }

    pub(crate) fn open_task_child_thread(
        &mut self,
        child_thread_id: String,
        title: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(parent_thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        if parent_thread_id == child_thread_id {
            return;
        }
        let Some(workspace_id) = self
            .thread_workspace_id(parent_thread_id.as_str())
            .or_else(|| self.active_workspace_id().map(str::to_owned))
        else {
            return;
        };

        self.navigation_intent(
            pioneer_client::navigation::NavigationIntent::PushTaskThread {
                entry: TaskThreadNavigationEntry::new(
                    parent_thread_id,
                    child_thread_id.clone(),
                    workspace_id.clone(),
                    title,
                ),
            },
        );
        self.set_main_content_view(MainContentView::Threads, cx);
        self.set_active_thread_id(Some(child_thread_id.clone()));
        self.set_preferred_workspace_id(Some(workspace_id.clone()));

        cx.notify();
    }

    pub(crate) fn close_task_child_thread(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(entry) = self.navigation_input.lineage().last().cloned() else {
            return;
        };
        self.navigation_intent(pioneer_client::navigation::NavigationIntent::PopTaskThread);
        self.set_active_thread_id(Some(entry.parent_thread_id().to_owned()));
        self.set_preferred_workspace_id(Some(entry.workspace_id().to_owned()));

        cx.notify();
    }
}

pub(crate) fn resolve_thread_tree_workspace_id(
    active_workspace_id: Option<&str>,
    preferred_workspace_id: Option<&str>,
    runtime_workspace_id: Option<&str>,
) -> Option<String> {
    pioneer_client::workspaces::selectors::resolve_workspace_scope(
        active_workspace_id,
        preferred_workspace_id,
        runtime_workspace_id,
    )
}
