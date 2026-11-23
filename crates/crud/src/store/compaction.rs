//! Compaction entry points and coordination with other domains.
use crate::CrudStore;
use pioneer_crud_compaction::store::CompactionStore;

mod checkpoints;
pub(crate) mod frozen_history;
mod history;
pub(crate) mod lifecycle;
pub(crate) mod runner;
mod task_outputs;

impl CrudStore {
    pub(crate) fn compaction_store(&self) -> CompactionStore {
        CompactionStore::new(self.connection.clone(), self.frozen_readers.clone())
    }
}
