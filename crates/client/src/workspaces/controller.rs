//! Workspace request lifecycle, independent from shell transport delivery.
use super::intents::WorkspaceIntent;
use crate::core::{ClientCore, ClientMutationAuthority, ClientTransition};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, mpsc},
    thread::JoinHandle,
};

const MAX_QUEUED_COMMANDS: usize = 64;
enum WorkspaceRequest {
    Bootstrap {
        preferred: Option<String>,
        connection: Option<u64>,
    },
    Refresh {
        workspace: String,
        connection: Option<u64>,
    },
    Command {
        intent: WorkspaceIntent,
        connection: Option<u64>,
    },
}
#[derive(Default)]
pub(crate) struct WorkspaceController {
    sender: Option<mpsc::SyncSender<()>>,
    // At most one refresh per retained workspace; hints during a request leave
    // one follow-up. The wake channel carries no domain data and cannot lose it.
    refresh: BTreeMap<String, Option<u64>>,
    bootstrap: Option<(Option<String>, Option<u64>)>,
    commands: VecDeque<(WorkspaceIntent, Option<u64>)>,
    task: Option<JoinHandle<()>>,
}
impl WorkspaceController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.refresh.clear();
        self.bootstrap = None;
        self.commands.clear();
    }
    fn wake(&self) {
        if let Some(sender) = &self.sender {
            let _ = sender.try_send(());
        }
    }
    fn next(&mut self) -> Option<WorkspaceRequest> {
        if let Some((preferred, connection)) = self.bootstrap.take() {
            Some(WorkspaceRequest::Bootstrap {
                preferred,
                connection,
            })
        } else if let Some((intent, connection)) = self.commands.pop_front() {
            Some(WorkspaceRequest::Command { intent, connection })
        } else {
            self.refresh
                .pop_first()
                .map(|(workspace, connection)| WorkspaceRequest::Refresh {
                    workspace,
                    connection,
                })
        }
    }
}
impl Drop for WorkspaceController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
impl ClientCore {
    pub(crate) fn start_workspace_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel(1);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-workspace-directory".into())
            .spawn(move || {
                while receiver.recv().is_ok() {
                    loop {
                        let Some(core) = weak.upgrade() else {
                            return;
                        };
                        if core.is_stopped() {
                            return;
                        }
                        let request = core
                            .workspace_controller
                            .lock()
                            .expect("workspace controller poisoned")
                            .next();
                        match request {
                            Some(WorkspaceRequest::Bootstrap {
                                preferred,
                                connection,
                            }) if core.gateway_http_generation() == connection => {
                                if let Ok(reduction) = core.bootstrap_workspace_catalog(preferred) {
                                    core.load_selected_workspace_directory(
                                        &reduction.selected.workspace_id,
                                    );
                                }
                            }
                            Some(WorkspaceRequest::Refresh {
                                workspace,
                                connection,
                            }) if core.gateway_http_generation() == connection
                                && core.workspace_refresh_is_demanded(&workspace) =>
                            {
                                let _ = core.refresh_workspace_tree(&workspace);
                            }
                            Some(WorkspaceRequest::Command { intent, connection })
                                if core.gateway_http_generation() == connection =>
                            {
                                let operation = core.begin_directory_action(&intent);
                                let result = core.execute_workspace_intent(intent);
                                if core.gateway_http_generation() == connection {
                                    if let Some(operation) = operation {
                                        core.complete_directory_action(
                                            operation,
                                            result.err().map(|error| format!("{error:#}")),
                                        );
                                    }
                                }
                            }
                            Some(_) => {}
                            None => break,
                        }
                    }
                }
            })
            .expect("workspace worker could not start");
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
    pub fn request_workspace_bootstrap(&self, preferred: Option<String>) {
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        if owner.sender.is_some() && !self.is_stopped() {
            owner.bootstrap = Some((preferred, self.gateway_http_generation()));
            owner.wake();
        }
    }
    pub(crate) fn observe_workspace_notification(
        &self,
        notification: &pioneer_protocol::GatewayNotification,
    ) -> bool {
        use pioneer_protocol::GatewayNotification;
        match notification {
            GatewayNotification::WorkspaceChanged(_) => {
                self.observe_workspace_catalog(notification);
                true
            }
            GatewayNotification::ThreadReadCursorChanged(change) => {
                self.apply_directory_read(
                    &change.workspace_id,
                    &change.thread_id,
                    &change.cursor,
                    change.unread_count,
                );
                true
            }
            GatewayNotification::ThreadTreeChanged(change) => {
                self.queue_directory_refresh(&change.workspace_id);
                true
            }
            GatewayNotification::ThreadAgentsDocChanged(change) => {
                self.queue_directory_refresh(&change.workspace_id);
                false
            }
            _ => false,
        }
    }
    /// Explicit refresh from a producer that changed thread metadata outside the directory UI.
    pub fn request_workspace_tree_refresh(&self, workspace: &str) {
        self.queue_directory_refresh(workspace);
    }
    pub(crate) fn load_selected_workspace_directory(&self, workspace: &str) {
        let Ok(directory) = self.refresh_workspace_tree(workspace) else {
            return;
        };
        if directory.error().is_some() {
            return;
        }
        let navigation = self.navigation_snapshot();
        if navigation.workspace_id() != Some(workspace) || navigation.active_thread_id().is_some() {
            return;
        }
        let restored = crate::threads::tree::restore_workspace_thread_state(
            workspace,
            navigation.last_active(workspace),
            navigation.draft(workspace),
            |id, workspace| {
                self.thread_snapshot(id)
                    .is_some_and(|snapshot| snapshot.coordinator().workspace_id == workspace)
            },
        );
        if let Some(thread_id) = restored.active_thread_id {
            self.navigate(
                crate::navigation::NavigationIntent::SelectThread {
                    workspace_id: Some(workspace.to_owned()),
                    thread_id: Some(thread_id),
                },
                None,
            );
        } else {
            let _ = self.execute_workspace_intent(WorkspaceIntent::NewThread {
                workspace_id: workspace.to_owned(),
            });
        }
    }
    pub(crate) fn cancel_workspace_requests(&self, workspace: &str) {
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        owner.refresh.remove(workspace);
        owner
            .commands
            .retain(|(intent, _)| intent.workspace_id() != Some(workspace));
    }
    pub(crate) fn queue_directory_refresh(&self, workspace: &str) {
        if !self.workspace_refresh_is_demanded(workspace) {
            return;
        }
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        if owner.sender.is_some() && !self.is_stopped() {
            owner
                .refresh
                .insert(workspace.to_owned(), self.gateway_http_generation());
            owner.wake();
        }
    }
    pub(crate) fn dispatch_workspace_intent(&self, intent: WorkspaceIntent) -> ClientTransition {
        let authority = ClientMutationAuthority { _private: () };
        if matches!(intent, WorkspaceIntent::SelectThread { .. }) {
            return if self.execute_workspace_intent(intent).is_ok() {
                self.transition(&authority, vec![], vec![])
            } else {
                self.reject_intent()
            };
        }
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        if self.is_stopped()
            || owner.sender.is_none()
            || owner.commands.len() >= MAX_QUEUED_COMMANDS
        {
            return self.reject_intent();
        }
        if owner.commands.iter().any(|(queued, connection)| {
            queued == &intent && *connection == self.gateway_http_generation()
        }) {
            return self.transition(&authority, vec![], vec![]);
        }
        let selection = if let WorkspaceIntent::NewThread { workspace_id } = &intent {
            let allowed = self
                .authorization_snapshot(Some(workspace_id), None)
                .and_then(|snapshot| snapshot.workspace)
                .is_some_and(|workspace| workspace.capabilities.can_create_thread);
            if !allowed {
                return self.reject_intent();
            }
            let draft = self
                .navigation_snapshot()
                .draft(workspace_id)
                .map(str::to_owned);
            Some(self.open_workspace_thread(workspace_id.clone(), draft, None))
        } else {
            None
        };
        owner
            .commands
            .push_back((intent, self.gateway_http_generation()));
        owner.wake();
        selection.unwrap_or_else(|| self.transition(&authority, vec![], vec![]))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repeated_hints_keep_one_follow_up_and_teardown_discards_pending_work() {
        let mut owner = WorkspaceController::default();
        for _ in 0..1000 {
            owner.refresh.insert("workspace".into(), Some(1));
        }
        assert_eq!(owner.refresh.len(), 1);
        assert!(matches!(
            owner.next(),
            Some(WorkspaceRequest::Refresh { .. })
        ));
        assert!(owner.next().is_none());
        owner.refresh.insert("workspace".into(), Some(2));
        owner.stop();
        assert!(owner.next().is_none());
    }
}
