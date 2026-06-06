//! Agents.md autosave state machine.

use crate::agents_doc::content::agents_doc_content_hash;
use pioneer_protocol::ThreadAgentsDocPayload;
use std::time::Duration;

pub const AGENTS_DOC_AUTOSAVE_DELAY: Duration = Duration::from_millis(700);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentsDocEditorLoadState {
    Loading,
    Loaded,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentsDocEditorSaveState {
    Clean,
    Dirty,
    Saving,
    Saved {
        saved_at: i64,
    },
    Error {
        message: String,
    },
    Conflict {
        local_content: String,
        remote_doc: ThreadAgentsDocPayload,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentsDocAutosaveDecision {
    Noop,
    Schedule { generation: u64 },
    SaveNow { generation: u64 },
    InFlight,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentsDocAutosaveState {
    pub save_state: AgentsDocEditorSaveState,
    pub last_saved_hash: Option<String>,
    pub last_saved_version: Option<i64>,
    pub pending_hash: Option<String>,
    pub generation: u64,
    pub save_in_flight: bool,
}

impl AgentsDocAutosaveState {
    pub fn new() -> Self {
        Self {
            save_state: AgentsDocEditorSaveState::Clean,
            last_saved_hash: None,
            last_saved_version: None,
            pending_hash: None,
            generation: 0,
            save_in_flight: false,
        }
    }

    pub fn reset_from_explicit(&mut self, explicit_doc: Option<&ThreadAgentsDocPayload>) {
        self.save_state = AgentsDocEditorSaveState::Clean;
        self.last_saved_hash = explicit_doc.map(|doc| doc.content_sha256.clone());
        self.last_saved_version = explicit_doc.map(|doc| doc.version);
        self.pending_hash = None;
        self.generation = self.generation.saturating_add(1);
        self.save_in_flight = false;
    }

    pub fn mark_changed(&mut self, content: &str) -> AgentsDocAutosaveDecision {
        if let AgentsDocEditorSaveState::Conflict { local_content, .. } = &mut self.save_state {
            *local_content = content.to_owned();
            self.pending_hash = Some(agents_doc_content_hash(content));
            return AgentsDocAutosaveDecision::Noop;
        }

        let hash = agents_doc_content_hash(content);
        if self.last_saved_hash.as_deref() == Some(hash.as_str()) {
            self.pending_hash = None;
            if !self.save_in_flight {
                self.save_state = AgentsDocEditorSaveState::Clean;
            }
            self.generation = self.generation.saturating_add(1);
            return AgentsDocAutosaveDecision::Noop;
        }

        self.pending_hash = Some(hash);
        self.save_state = AgentsDocEditorSaveState::Dirty;
        self.generation = self.generation.saturating_add(1);
        AgentsDocAutosaveDecision::Schedule {
            generation: self.generation,
        }
    }

    pub fn flush(&mut self, content: &str) -> AgentsDocAutosaveDecision {
        if matches!(self.save_state, AgentsDocEditorSaveState::Conflict { .. }) {
            self.pending_hash = Some(agents_doc_content_hash(content));
            return AgentsDocAutosaveDecision::Noop;
        }

        let hash = agents_doc_content_hash(content);
        if self.last_saved_hash.as_deref() == Some(hash.as_str()) {
            self.pending_hash = None;
            if !self.save_in_flight {
                self.save_state = AgentsDocEditorSaveState::Clean;
            }
            self.generation = self.generation.saturating_add(1);
            return AgentsDocAutosaveDecision::Noop;
        }

        self.pending_hash = Some(hash);
        self.save_state = AgentsDocEditorSaveState::Dirty;
        self.generation = self.generation.saturating_add(1);

        if self.save_in_flight {
            AgentsDocAutosaveDecision::InFlight
        } else {
            AgentsDocAutosaveDecision::SaveNow {
                generation: self.generation,
            }
        }
    }

    pub fn debounce_due(&self, generation: u64) -> bool {
        self.generation == generation
            && self.pending_hash.is_some()
            && !self.save_in_flight
            && !matches!(self.save_state, AgentsDocEditorSaveState::Conflict { .. })
    }

    pub fn mark_saving(&mut self) {
        self.save_in_flight = true;
        self.save_state = AgentsDocEditorSaveState::Saving;
    }

    pub fn finish_success(
        &mut self,
        doc: &ThreadAgentsDocPayload,
        current_hash: &str,
        saved_at: i64,
    ) -> AgentsDocAutosaveDecision {
        self.save_in_flight = false;
        self.last_saved_hash = Some(doc.content_sha256.clone());
        self.last_saved_version = Some(doc.version);

        if current_hash == doc.content_sha256 {
            self.pending_hash = None;
            self.save_state = AgentsDocEditorSaveState::Saved { saved_at };
            return AgentsDocAutosaveDecision::Noop;
        }

        self.pending_hash = Some(current_hash.to_owned());
        self.save_state = AgentsDocEditorSaveState::Dirty;
        self.generation = self.generation.saturating_add(1);
        AgentsDocAutosaveDecision::Schedule {
            generation: self.generation,
        }
    }

    pub fn finish_error(&mut self, message: String) {
        self.save_in_flight = false;
        self.save_state = AgentsDocEditorSaveState::Error { message };
    }

    pub fn enter_conflict(&mut self, local_content: String, remote_doc: ThreadAgentsDocPayload) {
        self.save_in_flight = false;
        self.last_saved_hash = Some(remote_doc.content_sha256.clone());
        self.last_saved_version = Some(remote_doc.version);
        self.pending_hash = Some(agents_doc_content_hash(local_content.as_str()));
        self.generation = self.generation.saturating_add(1);
        self.save_state = AgentsDocEditorSaveState::Conflict {
            local_content,
            remote_doc,
        };
    }

    pub fn reload_remote(&mut self, remote_doc: &ThreadAgentsDocPayload) {
        self.save_in_flight = false;
        self.last_saved_hash = Some(remote_doc.content_sha256.clone());
        self.last_saved_version = Some(remote_doc.version);
        self.pending_hash = None;
        self.generation = self.generation.saturating_add(1);
        self.save_state = AgentsDocEditorSaveState::Clean;
    }

    pub fn prepare_conflict_overwrite(&mut self, content: &str) -> AgentsDocAutosaveDecision {
        let hash = agents_doc_content_hash(content);
        self.pending_hash = Some(hash);
        self.save_state = AgentsDocEditorSaveState::Dirty;
        self.save_in_flight = false;
        self.generation = self.generation.saturating_add(1);
        AgentsDocAutosaveDecision::SaveNow {
            generation: self.generation,
        }
    }
}

impl Default for AgentsDocAutosaveState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents_doc::content::agents_doc_content_hash;
    use pioneer_protocol::{ThreadAgentsDocPayload, ThreadAgentsDocStatus};

    fn payload(status: ThreadAgentsDocStatus, content: &str) -> ThreadAgentsDocPayload {
        ThreadAgentsDocPayload {
            id: "agd_1".to_owned(),
            workspace_id: "ws_1".to_owned(),
            folder_id: Some("fld_1".to_owned()),
            status,
            title: "AGENTS.md".to_owned(),
            content: content.to_owned(),
            content_sha256: agents_doc_content_hash(content),
            version: 1,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_000,
        }
    }

    #[test]
    fn change_marks_dirty_and_schedules() {
        let mut state = AgentsDocAutosaveState::new();
        state.reset_from_explicit(Some(&payload(ThreadAgentsDocStatus::Active, "old")));

        let decision = state.mark_changed("new");

        assert_eq!(
            decision,
            AgentsDocAutosaveDecision::Schedule { generation: 2 }
        );
        assert_eq!(state.save_state, AgentsDocEditorSaveState::Dirty);
        assert_eq!(state.pending_hash, Some(agents_doc_content_hash("new")));
    }

    #[test]
    fn same_hash_skips_save_after_line_ending_normalization() {
        let mut state = AgentsDocAutosaveState::new();
        state.reset_from_explicit(Some(&payload(
            ThreadAgentsDocStatus::Active,
            "line\nending",
        )));

        let decision = state.mark_changed("line\r\nending");

        assert_eq!(decision, AgentsDocAutosaveDecision::Noop);
        assert_eq!(state.save_state, AgentsDocEditorSaveState::Clean);
        assert_eq!(state.pending_hash, None);
    }

    #[test]
    fn success_updates_version_and_hash() {
        let mut state = AgentsDocAutosaveState::new();
        state.reset_from_explicit(Some(&payload(ThreadAgentsDocStatus::Active, "old")));
        assert!(matches!(
            state.flush("new"),
            AgentsDocAutosaveDecision::SaveNow { .. }
        ));
        state.mark_saving();
        let mut saved = payload(ThreadAgentsDocStatus::Active, "new");
        saved.version = 2;

        let decision = state.finish_success(&saved, agents_doc_content_hash("new").as_str(), 42);

        assert_eq!(decision, AgentsDocAutosaveDecision::Noop);
        assert_eq!(state.last_saved_hash, Some(agents_doc_content_hash("new")));
        assert_eq!(state.last_saved_version, Some(2));
        assert_eq!(
            state.save_state,
            AgentsDocEditorSaveState::Saved { saved_at: 42 }
        );
    }

    #[test]
    fn failed_save_keeps_dirty_hash_and_error() {
        let mut state = AgentsDocAutosaveState::new();
        state.reset_from_explicit(Some(&payload(ThreadAgentsDocStatus::Active, "old")));
        assert!(matches!(
            state.flush("new"),
            AgentsDocAutosaveDecision::SaveNow { .. }
        ));
        state.mark_saving();

        state.finish_error("offline".to_owned());

        assert_eq!(state.pending_hash, Some(agents_doc_content_hash("new")));
        assert_eq!(state.last_saved_version, Some(1));
        assert_eq!(
            state.save_state,
            AgentsDocEditorSaveState::Error {
                message: "offline".to_owned()
            }
        );
    }

    #[test]
    fn stale_debounce_generation_noops() {
        let mut state = AgentsDocAutosaveState::new();
        state.reset_from_explicit(Some(&payload(ThreadAgentsDocStatus::Active, "old")));
        let first = match state.mark_changed("new 1") {
            AgentsDocAutosaveDecision::Schedule { generation } => generation,
            other => panic!("expected schedule, got {other:?}"),
        };
        let second = match state.mark_changed("new 2") {
            AgentsDocAutosaveDecision::Schedule { generation } => generation,
            other => panic!("expected schedule, got {other:?}"),
        };

        assert!(!state.debounce_due(first));
        assert!(state.debounce_due(second));
    }

    #[test]
    fn flush_runs_pending_save_immediately() {
        let mut state = AgentsDocAutosaveState::new();
        state.reset_from_explicit(Some(&payload(ThreadAgentsDocStatus::Active, "old")));
        assert!(matches!(
            state.mark_changed("new"),
            AgentsDocAutosaveDecision::Schedule { .. }
        ));

        let decision = state.flush("new");

        assert_eq!(
            decision,
            AgentsDocAutosaveDecision::SaveNow { generation: 3 }
        );
    }

    #[test]
    fn conflict_stops_autosave() {
        let mut state = AgentsDocAutosaveState::new();
        let remote = payload(ThreadAgentsDocStatus::Active, "remote");
        state.enter_conflict("local".to_owned(), remote);

        let decision = state.mark_changed("local edited");

        assert_eq!(decision, AgentsDocAutosaveDecision::Noop);
        assert!(!state.debounce_due(state.generation));
        assert!(matches!(
            state.save_state,
            AgentsDocEditorSaveState::Conflict { .. }
        ));
    }

    #[test]
    fn conflict_reload_remote_replaces_local_state() {
        let mut state = AgentsDocAutosaveState::new();
        let remote = payload(ThreadAgentsDocStatus::Active, "remote");
        state.enter_conflict("local".to_owned(), remote.clone());

        state.reload_remote(&remote);

        assert_eq!(state.save_state, AgentsDocEditorSaveState::Clean);
        assert_eq!(state.last_saved_hash, Some(remote.content_sha256.clone()));
        assert_eq!(state.last_saved_version, Some(remote.version));
    }

    #[test]
    fn conflict_overwrite_remote_resumes_saved_state() {
        let mut state = AgentsDocAutosaveState::new();
        let remote = payload(ThreadAgentsDocStatus::Active, "remote");
        state.enter_conflict("local".to_owned(), remote);

        let decision = state.prepare_conflict_overwrite("local");
        state.mark_saving();
        let mut saved = payload(ThreadAgentsDocStatus::Active, "local");
        saved.version = 3;
        let finish = state.finish_success(&saved, agents_doc_content_hash("local").as_str(), 99);

        assert!(matches!(
            decision,
            AgentsDocAutosaveDecision::SaveNow { .. }
        ));
        assert_eq!(finish, AgentsDocAutosaveDecision::Noop);
        assert_eq!(
            state.save_state,
            AgentsDocEditorSaveState::Saved { saved_at: 99 }
        );
        assert_eq!(state.last_saved_version, Some(3));
    }
}
