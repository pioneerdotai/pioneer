//! Per-thread coordinator state.

use crate::{conversation::Conversation, threads::resume::ThreadResumeCoordinator};
use pioneer_protocol::{Thread, Turn};

pub struct ThreadCoordinator {
    pub workspace_id: String,
    pub conversation: Conversation,
    pub resume: ThreadResumeCoordinator,
    pub history_loaded: bool,
    pub history_loading: bool,
    thread_state: ThreadState,
    // thread/start omits historical turns. Keep the directory's last-turn
    // evidence separately from runtime turns and the legacy Conversation,
    // which intentionally does not project TaskRun/Message markers.
    last_known_turn: Option<Turn>,
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
            last_known_turn: self.last_known_turn.clone(),
        }
    }

    pub fn new(thread: Thread) -> Self {
        let workspace_id = thread.workspace_id.clone();
        let mut conversation = Conversation::new(thread.id.clone());
        conversation.sync_thread_snapshot(&thread);

        Self {
            workspace_id,
            last_known_turn: thread.turns.last().cloned(),
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
            last_known_turn: None,
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
        self.last_known_turn = thread.turns.last().cloned().or_else(|| {
            self.thread()
                .filter(|previous| previous.id == thread.id)
                .and_then(|_| self.last_known_turn())
                .cloned()
        });
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

    pub(crate) fn last_known_turn(&self) -> Option<&Turn> {
        self.thread()
            .and_then(|thread| thread.turns.last())
            .or(self.last_known_turn.as_ref())
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
