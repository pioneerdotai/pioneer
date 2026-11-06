//! Short, synchronous retirement handoff for a late cold ThreadClosed.
//! The existing store retains only this ticket until local operation cleanup
//! completes. No event queue, worker, detached task or domain snapshot is kept.
use super::*;
use std::sync::{Condvar, Mutex};

#[derive(Default)]
enum CleanupState {
    #[default]
    Pending,
    Running(std::thread::ThreadId),
    Done,
}
pub(super) struct ThreadRetirement {
    id: String,
    generation: u64,
    state: Mutex<CleanupState>,
    changed: Condvar,
}
struct CleanupAttempt<'a>(&'a ThreadRetirement, bool);
impl Drop for CleanupAttempt<'_> {
    fn drop(&mut self) {
        // An unwinding helper must not strand replacement admission forever.
        *self.0.state.lock().unwrap_or_else(|p| p.into_inner()) = if self.1 {
            CleanupState::Done
        } else {
            CleanupState::Pending
        };
        self.0.changed.notify_all();
    }
}
impl ClientCore {
    pub(super) fn begin_thread_retirement_locked(
        &self,
        registry: &mut ThreadRegistry,
        id: &str,
    ) -> (Arc<ThreadRetirement>, Vec<ClientPublicationDraft>) {
        let generation = registry.stores[id].generation;
        let workspace = registry.stores[id].coordinator.workspace_id.clone();
        let ticket = Arc::new(ThreadRetirement {
            id: id.to_owned(),
            generation,
            state: Mutex::new(CleanupState::Pending),
            changed: Condvar::new(),
        });
        let drafts = self.retire_thread_state_locked(registry, id, true);
        // Old operation tokens fail immediately. This private, unpublished slot
        // prevents admission from racing the ID-scoped legacy operation owners.
        registry.clock = registry
            .clock
            .checked_add(1)
            .expect("thread generation exhausted");
        let mut slot = ThreadDomainStore::new(id, &workspace);
        slot.generation = registry.clock;
        slot.presentation_blocked = true;
        slot.retirement = Some(ticket.clone());
        registry.stores.insert(id.to_owned(), slot);
        (ticket, drafts)
    }

    pub(super) fn finish_pending_thread_retirement(&self, id: &str) -> bool {
        let ticket = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .stores
            .get(id)
            .and_then(|store| store.retirement.clone());
        ticket.is_none_or(|ticket| self.finish_thread_retirement(&ticket))
    }

    pub(super) fn finish_thread_retirement(&self, ticket: &Arc<ThreadRetirement>) -> bool {
        let thread = std::thread::current().id();
        let mut state = ticket.state.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            match &*state {
                CleanupState::Done => return true,
                // A cleanup helper can consult an existing thread mutation.
                // It must neither wait for itself nor recreate the retired slot.
                CleanupState::Running(owner) if *owner == thread => return false,
                CleanupState::Running(_) => {
                    state = ticket
                        .changed
                        .wait(state)
                        .unwrap_or_else(|p| p.into_inner());
                }
                CleanupState::Pending => {
                    *state = CleanupState::Running(thread);
                    break;
                }
            }
        }
        drop(state);
        let mut attempt = CleanupAttempt(ticket, false);
        // No identity, transport, registry or ticket mutex is held here.
        // Replacement admission helps this exact ticket before creating its new
        // store/operations. Late callers of a completed ticket do nothing.
        self.cancel_retired_thread_operations(&ticket.id);
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if registry
            .stores
            .get(&ticket.id)
            .filter(|store| store.generation > ticket.generation)
            .and_then(|store| store.retirement.as_ref())
            .is_some_and(|current| Arc::ptr_eq(current, ticket))
        {
            registry.stores.remove(&ticket.id);
        }
        drop(registry);
        attempt.1 = true;
        self.notify_send_readiness();
        self.retire_thread_controller_jobs();
        true
    }
}
