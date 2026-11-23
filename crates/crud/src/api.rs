//! Public types and repository entry points exposed by the CRUD facade.

// Private domain modules organize the existing top-level exports.
mod access;
mod artifacts;
pub mod compaction;
mod conversations;
mod execution;
mod memory;
mod packages;
mod self_improvement;
mod tasks;
mod timeline;

pub use access::*;
pub use artifacts::*;
pub use compaction::root_exports::*;
pub use conversations::*;
pub use execution::*;
pub use memory::*;
pub use packages::*;
pub use self_improvement::*;
pub use tasks::*;
pub use timeline::*;

pub use crate::store::connection::CrudStore;

pub use pioneer_postgres_pools::{WorkSignal, WorkSubscription};
