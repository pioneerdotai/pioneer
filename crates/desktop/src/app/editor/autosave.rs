use super::content::agents_doc_content_hash;
use pioneer_protocol::ThreadAgentsDocPayload;
use std::time::Duration;

pub(super) const AGENTS_DOC_AUTOSAVE_DELAY: Duration = Duration::from_millis(700);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum AgentsDocEditorLoadState {
    Loading,
    Loaded,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum AgentsDocEditorSaveState {
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
pub(super) enum AgentsDocAutosaveDecision {
    Noop,
    Schedule { generation: u64 },
    SaveNow { generation: u64 },
    InFlight,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AgentsDocAutosaveState {
    pub(super) save_state: AgentsDocEditorSaveState,
    pub(super) last_saved_hash: Option<String>,
    pub(super) last_saved_version: Option<i64>,
    pub(super) pending_hash: Option<String>,
    pub(super) generation: u64,
    pub(super) save_in_flight: bool,
}

impl AgentsDocAutosaveState {
    pub(super) fn new() -> Self {
        Self {
            save_state: AgentsDocEditorSaveState::Clean,
            last_saved_hash: None,
            last_saved_version: None,
            pending_hash: None,
            generation: 0,
            save_in_flight: false,
        }
    }

    pub(super) fn reset_from_explicit(&mut self, explicit_doc: Option<&ThreadAgentsDocPayload>) {
        self.save_state = AgentsDocEditorSaveState::Clean;
        self.last_saved_hash = explicit_doc.map(|doc| doc.content_sha256.clone());
        self.last_saved_version = explicit_doc.map(|doc| doc.version);
        self.pending_hash = None;
        self.generation = self.generation.saturating_add(1);
        self.save_in_flight = false;
    }

    pub(super) fn mark_changed(&mut self, content: &str) -> AgentsDocAutosaveDecision {
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

    pub(super) fn flush(&mut self, content: &str) -> AgentsDocAutosaveDecision {
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

    pub(super) fn debounce_due(&self, generation: u64) -> bool {
        self.generation == generation
            && self.pending_hash.is_some()
            && !self.save_in_flight
            && !matches!(self.save_state, AgentsDocEditorSaveState::Conflict { .. })
    }

    pub(super) fn mark_saving(&mut self) {
        self.save_in_flight = true;
        self.save_state = AgentsDocEditorSaveState::Saving;
    }

    pub(super) fn finish_success(
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

    pub(super) fn finish_error(&mut self, message: String) {
        self.save_in_flight = false;
        self.save_state = AgentsDocEditorSaveState::Error { message };
    }

    pub(super) fn enter_conflict(
        &mut self,
        local_content: String,
        remote_doc: ThreadAgentsDocPayload,
    ) {
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

    pub(super) fn reload_remote(&mut self, remote_doc: &ThreadAgentsDocPayload) {
        self.save_in_flight = false;
        self.last_saved_hash = Some(remote_doc.content_sha256.clone());
        self.last_saved_version = Some(remote_doc.version);
        self.pending_hash = None;
        self.generation = self.generation.saturating_add(1);
        self.save_state = AgentsDocEditorSaveState::Clean;
    }

    pub(super) fn prepare_conflict_overwrite(
        &mut self,
        content: &str,
    ) -> AgentsDocAutosaveDecision {
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
