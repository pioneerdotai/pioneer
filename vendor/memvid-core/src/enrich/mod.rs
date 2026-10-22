//! Enrichment engine framework for extracting memory cards from frames.
//!
//! This module provides the trait and utilities for building enrichment engines
//! that process MV2 frames and extract structured memory cards.

pub mod engine;
pub mod rules;

pub use engine::{EnrichmentContext, EnrichmentEngine, EnrichmentResult};
pub use rules::RulesEngine;
