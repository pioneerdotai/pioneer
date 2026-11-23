//! Compaction repositories that participate in the caller's transaction.

pub mod accepted_import;
pub mod background;
pub mod canonical_history;
pub mod checkpoint_apply;
pub mod checkpoint_candidate;
pub mod checkpoint_projection;
pub mod checkpoint_proofs;
pub mod checkpoint_topology;
pub mod child_history;
pub mod frozen;
pub mod frozen_capture;
pub mod frozen_copy;
pub mod frozen_expiry;
pub mod frozen_geometry;
pub mod frozen_import;
pub mod frozen_lifetime;
pub mod frozen_reachability;
pub mod frozen_storage;
pub mod frozen_storage_reads;
pub mod frozen_views;
pub mod history;
pub mod history_check_result;
pub mod history_fence;
pub mod history_turn_pages;
pub mod input_copy_aliases;
pub mod lifecycle;
pub mod lifecycle_pending;
pub mod live_sources;
pub mod manifest;
pub mod operations;
#[cfg(any(test, feature = "test-support"))]
pub mod plan_test_support;
pub mod preview_frozen_index;
pub mod proof_admission;
pub mod proof_discovery;
pub mod proof_graph;
pub mod proof_publication;
pub mod proof_staging;
#[cfg(any(test, feature = "test-support"))]
pub mod proof_test_metrics;
mod retained_snapshots;
pub mod runner_repository;
pub mod runner_resume;
pub mod runner_state;
pub mod runner_writes;

pub mod delivery_sources;
pub mod source_payloads;
pub mod source_projection;
pub mod source_revision;
pub mod sweep_work;
pub mod task_basis;
pub mod task_output;
pub mod tool_sources;

pub const SOURCE_PAGE_ROWS: u64 = 128;
pub const SOURCE_PAGE_BYTES: usize = 256 * 1024;

#[cfg(any(test, feature = "test-support"))]
pub mod publication_test_hook;
pub mod stop_execution;
pub mod store;

pub mod frozen_maintenance;
pub const CHECKPOINT_SOURCE_LIMIT: usize = pioneer_compaction::REPLAY_ALIAS_LIMIT;

pub mod runner_publication;

#[cfg(test)]
mod history_regression_tests;

#[cfg(test)]
mod source_revision_regressions;

#[cfg(test)]
mod payload_reference_regressions;

#[cfg(test)]
mod cli_binding_retention_tests;
