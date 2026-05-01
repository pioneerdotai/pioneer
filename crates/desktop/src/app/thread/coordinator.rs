use crate::app::{conversation::Conversation, root::ThreadResumeCoordinator};
use pioneer_protocol::Thread;

pub(crate) struct ThreadCoordinator {
    pub(crate) workspace_id: String,
    pub(crate) conversation: Conversation,
    pub(crate) resume: ThreadResumeCoordinator,
    pub(crate) history_loaded: bool,
    pub(crate) history_loading: bool,
    thread_state: ThreadState,
}

enum ThreadState {
    Pending,
    Ready(Thread),
}

impl ThreadCoordinator {
    pub(crate) fn new(thread: Thread) -> Self {
        let workspace_id = thread.workspace_id.clone();
        let conversation = Conversation::new(thread.id.clone());

        Self {
            workspace_id,
            thread_state: ThreadState::Ready(thread),
            conversation,
            resume: ThreadResumeCoordinator::default(),
            history_loaded: false,
            history_loading: false,
        }
    }

    pub(crate) fn pending(thread_id: &str, workspace_id: &str) -> Self {
        Self {
            workspace_id: workspace_id.to_owned(),
            thread_state: ThreadState::Pending,
            conversation: Conversation::new(thread_id),
            resume: ThreadResumeCoordinator::default(),
            history_loaded: false,
            history_loading: false,
        }
    }

    pub(crate) fn set_workspace_id(&mut self, workspace_id: &str) {
        self.workspace_id = workspace_id.to_owned();
        if let ThreadState::Ready(thread) = &mut self.thread_state {
            thread.workspace_id = workspace_id.to_owned();
        }
    }

    pub(crate) fn set_snapshot(&mut self, thread: Thread) {
        self.workspace_id = thread.workspace_id.clone();
        self.thread_state = ThreadState::Ready(thread);
    }

    pub(crate) fn thread(&self) -> Option<&Thread> {
        match &self.thread_state {
            ThreadState::Pending => None,
            ThreadState::Ready(thread) => Some(thread),
        }
    }

    pub(crate) fn thread_mut(&mut self) -> Option<&mut Thread> {
        match &mut self.thread_state {
            ThreadState::Pending => None,
            ThreadState::Ready(thread) => Some(thread),
        }
    }

    pub(crate) fn updated_at(&self) -> i64 {
        self.thread()
            .map(|thread| thread.updated_at)
            .unwrap_or_default()
    }
}
