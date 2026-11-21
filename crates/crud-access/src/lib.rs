//! Access repositories that use the caller's scoped connection or transaction.

pub mod actor_nickname;
pub mod administrative_audit;
pub mod policy_generation;
pub mod principal_avatar;

pub mod auth_session;
pub mod authorization_persistence;
pub mod authorization_scope;
pub mod identity;
pub mod membership;
pub mod task_activity;
pub mod task_scope;
pub mod thread_participants;
pub mod turn_execution;
pub mod workspace;

pub const AUTHORIZATION_FACTS_BATCH_LIMIT: usize = 128;

#[cfg(feature = "test-support")]
use crate as domain;
#[cfg(feature = "test-support")]
#[path = "../tests/support/membership.rs"]
pub mod test_support;

pub mod invitation;
pub mod policy;
