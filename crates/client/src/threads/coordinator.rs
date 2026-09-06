//! Per-thread coordinator state.

use crate::{conversation::Conversation, threads::resume::ThreadResumeCoordinator};
use pioneer_protocol::Thread;

pub struct ThreadCoordinator {
    pub workspace_id: String,
    pub conversation: Conversation,
    pub resume: ThreadResumeCoordinator,
    pub history_loaded: bool,
    pub history_loading: bool,
    thread_state: ThreadState,
}

#[derive(Clone)]
enum ThreadState {
    Pending,
    Ready(Thread),
}

impl ThreadCoordinator {
    pub(crate) fn snapshot_copy(&self) -> Self {
        Self {
            workspace_id: self.workspace_id.clone(),
            conversation: self.conversation.snapshot_copy(),
            resume: self.resume.clone(),
            history_loaded: self.history_loaded,
            history_loading: self.history_loading,
            thread_state: self.thread_state.clone(),
        }
    }

    pub fn new(thread: Thread) -> Self {
        let workspace_id = thread.workspace_id.clone();
        let mut conversation = Conversation::new(thread.id.clone());
        conversation.sync_thread_snapshot(&thread);

        Self {
            workspace_id,
            thread_state: ThreadState::Ready(thread),
            conversation,
            resume: ThreadResumeCoordinator::default(),
            history_loaded: false,
            history_loading: false,
        }
    }

    pub fn pending(thread_id: &str, workspace_id: &str) -> Self {
        Self {
            workspace_id: workspace_id.to_owned(),
            thread_state: ThreadState::Pending,
            conversation: Conversation::new(thread_id),
            resume: ThreadResumeCoordinator::default(),
            history_loaded: false,
            history_loading: false,
        }
    }

    pub fn set_workspace_id(&mut self, workspace_id: &str) {
        self.workspace_id = workspace_id.to_owned();
        if let ThreadState::Ready(thread) = &mut self.thread_state {
            thread.workspace_id = workspace_id.to_owned();
        }
    }

    pub fn set_snapshot(&mut self, thread: Thread) {
        self.workspace_id = thread.workspace_id.clone();
        self.conversation.sync_thread_snapshot(&thread);
        self.thread_state = ThreadState::Ready(thread);
    }

    pub fn thread(&self) -> Option<&Thread> {
        match &self.thread_state {
            ThreadState::Pending => None,
            ThreadState::Ready(thread) => Some(thread),
        }
    }

    pub fn thread_mut(&mut self) -> Option<&mut Thread> {
        match &mut self.thread_state {
            ThreadState::Pending => None,
            ThreadState::Ready(thread) => Some(thread),
        }
    }

    pub fn updated_at(&self) -> i64 {
        self.thread()
            .map(|thread| thread.updated_at)
            .unwrap_or_default()
    }
}
