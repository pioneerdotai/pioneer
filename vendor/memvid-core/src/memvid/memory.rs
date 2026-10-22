//! Memory card extensions for `Memvid`.
//!
//! This module provides methods for managing structured memory cards within
//! an MV2 file, including adding cards, querying by entity/slot, temporal
//! lookups, and enrichment tracking.

use crate::error::Result;
use crate::memvid::lifecycle::Memvid;
use crate::types::{
    Cardinality, EntityKind, FrameId, MemoriesStats, MemoriesTrack, MemoryCard, MemoryCardId,
    PredicateSchema, SchemaError, SchemaRegistry,
};
use serde::Serialize;

/// Summary entry for an inferred schema.
#[derive(Debug, Clone, Serialize)]
pub struct SchemaSummaryEntry {
    /// The predicate (slot) name.
    pub predicate: String,
    /// The inferred value type (e.g., "string", "number", "datetime").
    pub inferred_type: String,
    /// Whether the predicate allows multiple values per entity.
    pub cardinality: Cardinality,
    /// Number of unique entities with this predicate.
    pub entity_count: usize,
    /// Total number of values across all entities.
    pub value_count: usize,
    /// Number of unique values.
    pub unique_values: usize,
    /// Whether this predicate has a built-in schema definition.
    pub is_builtin: bool,
}

/// Internal stats for predicate inference.
struct PredicateStats {
    _entity_count: usize,
    value_count: usize,
    unique_values: std::collections::HashSet<String>,
    entities: std::collections::HashSet<String>,
}

impl Memvid {
    /// Get an immutable reference to the memories track.
    ///
    /// Returns the in-memory memories track. Changes are persisted when
    /// the file is sealed.
    #[must_use]
    pub fn memories(&self) -> &MemoriesTrack {
        &self.memories_track
    }

    /// Get a mutable reference to the memories track.
    ///
    /// Returns the in-memory memories track for direct manipulation.
    /// Changes are persisted when the file is sealed.
    pub fn memories_mut(&mut self) -> &mut MemoriesTrack {
        self.dirty = true;
        &mut self.memories_track
    }

    /// Add a memory card to the memories track.
    ///
    /// The card is assigned a unique ID and stored in memory. Changes
    /// are persisted when the file is sealed.
    ///
    /// If schema validation is enabled (strict mode), invalid cards will
    /// be rejected with an error. In non-strict mode (default), validation
    /// warnings are logged but the card is still inserted.
    ///
    /// # Arguments
    /// * `card` - The memory card to add (ID will be overwritten)
    ///
    /// # Returns
    /// The assigned card ID.
    ///
    /// # Errors
    /// Returns an error if strict schema validation is enabled and the card is invalid.
    pub fn put_memory_card(&mut self, card: MemoryCard) -> Result<MemoryCardId> {
        // Validate against schema
        if let Err(e) = self.validate_card(&card) {
            if self.schema_strict {
                return Err(crate::error::MemvidError::SchemaValidation {
                    reason: e.to_string(),
                });
            }
            // Non-strict mode: log warning but continue
            tracing::warn!(
                entity = %card.entity,
                slot = %card.slot,
                value = %card.value,
                error = %e,
                "Schema validation warning"
            );
        }

        self.dirty = true;
        let id = self.memories_track.add_card(card);
        Ok(id)
    }

    /// Add multiple memory cards at once.
    ///
    /// If schema validation is enabled (strict mode), all cards are validated
    /// before any are inserted. If any card fails validation, an error is
    /// returned and no cards are inserted.
    ///
    /// In non-strict mode (default), validation warnings are logged but
    /// all cards are still inserted.
    ///
    /// # Arguments
    /// * `cards` - The memory cards to add
    ///
    /// # Returns
    /// The assigned card IDs in order.
    ///
    /// # Errors
    /// Returns an error if strict schema validation is enabled and any card is invalid.
    pub fn put_memory_cards(&mut self, cards: Vec<MemoryCard>) -> Result<Vec<MemoryCardId>> {
        // Validate all cards first
        let validation_errors = self.validate_cards(&cards);

        if !validation_errors.is_empty() {
            if self.schema_strict {
                // In strict mode, reject all if any are invalid
                let errors: Vec<String> = validation_errors
                    .iter()
                    .map(|(i, e)| format!("Card {i}: {e}"))
                    .collect();
                return Err(crate::error::MemvidError::SchemaValidation {
                    reason: format!(
                        "{} cards failed validation: {}",
                        errors.len(),
                        errors.join("; ")
                    ),
                });
            }

            // Non-strict mode: log warnings but continue
            for (i, e) in &validation_errors {
                let card = &cards[*i];
                tracing::warn!(
                    index = i,
                    entity = %card.entity,
                    slot = %card.slot,
                    value = %card.value,
                    error = %e,
                    "Schema validation warning"
                );
            }
        }

        self.dirty = true;
        let ids = self.memories_track.add_cards(cards);
        Ok(ids)
    }

    /// Record that a frame was enriched by an engine.
    ///
    /// This is used to track which frames have been processed by which
    /// enrichment engines, enabling incremental enrichment.
    ///
    /// # Arguments
    /// * `frame_id` - The frame that was enriched
    /// * `engine_kind` - The engine identifier (e.g., "rules-v1")
    /// * `engine_version` - The engine version (e.g., "1.0.0")
    /// * `card_ids` - The IDs of cards produced from this frame
    pub fn record_enrichment(
        &mut self,
        frame_id: FrameId,
        engine_kind: &str,
        engine_version: &str,
        card_ids: Vec<MemoryCardId>,
    ) -> Result<()> {
        self.dirty = true;
        self.memories_track
            .record_enrichment(frame_id, engine_kind, engine_version, card_ids);
        Ok(())
    }

    /// Get frames that haven't been enriched by a specific engine version.
    ///
    /// # Arguments
    /// * `engine_kind` - The engine identifier
    /// * `engine_version` - The engine version
    ///
    /// # Returns
    /// A list of frame IDs that need enrichment.
    #[must_use]
    pub fn get_unenriched_frames(&self, engine_kind: &str, engine_version: &str) -> Vec<FrameId> {
        (0..self.toc.frames.len() as FrameId)
            .filter(|id| {
                self.memories_track.enrichment_manifest().needs_enrichment(
                    *id,
                    engine_kind,
                    engine_version,
                )
            })
            .collect()
    }

    /// Check if a frame has been enriched by a specific engine version.
    #[must_use]
    pub fn is_frame_enriched(
        &self,
        frame_id: FrameId,
        engine_kind: &str,
        engine_version: &str,
    ) -> bool {
        self.memories_track
            .is_enriched_by(frame_id, engine_kind, engine_version)
    }

    /// Get the current (most recent, non-retracted) memory for an entity:slot.
    ///
    /// # Arguments
    /// * `entity` - The entity (e.g., "user")
    /// * `slot` - The slot/attribute (e.g., "employer")
    ///
    /// # Returns
    /// The most recent non-retracted card, if any.
    #[must_use]
    pub fn get_current_memory(&self, entity: &str, slot: &str) -> Option<&MemoryCard> {
        self.memories_track.get_current(entity, slot)
    }

    /// Get the memory value at a specific point in time.
    ///
    /// # Arguments
    /// * `entity` - The entity
    /// * `slot` - The slot/attribute
    /// * `timestamp` - Unix timestamp to query
    ///
    /// # Returns
    /// The most recent non-retracted card at that time, if any.
    #[must_use]
    pub fn get_memory_at_time(
        &self,
        entity: &str,
        slot: &str,
        timestamp: i64,
    ) -> Option<&MemoryCard> {
        self.memories_track.get_at_time(entity, slot, timestamp)
    }

    /// Get all memory cards for an entity.
    ///
    /// # Arguments
    /// * `entity` - The entity to query
    ///
    /// # Returns
    /// All cards associated with the entity.
    #[must_use]
    pub fn get_entity_memories(&self, entity: &str) -> Vec<&MemoryCard> {
        self.memories_track.get_entity_cards(entity)
    }

    /// Aggregate all values for a slot across all occurrences.
    ///
    /// Useful for multi-session scenarios where the same slot may have
    /// multiple values across different conversations.
    ///
    /// # Arguments
    /// * `entity` - The entity
    /// * `slot` - The slot/attribute
    ///
    /// # Returns
    /// All unique values for the slot.
    #[must_use]
    pub fn aggregate_memory_slot(&self, entity: &str, slot: &str) -> Vec<String> {
        self.memories_track.aggregate_slot(entity, slot)
    }

    /// Count occurrences of a slot, optionally filtered by value.
    ///
    /// Useful for questions like "how many times did I mention X?".
    ///
    /// # Arguments
    /// * `entity` - The entity
    /// * `slot` - The slot/attribute
    /// * `value_filter` - Optional substring to filter values
    ///
    /// # Returns
    /// The count of matching cards.
    #[must_use]
    pub fn count_memory_occurrences(
        &self,
        entity: &str,
        slot: &str,
        value_filter: Option<&str>,
    ) -> usize {
        self.memories_track
            .count_occurrences(entity, slot, value_filter)
    }

    /// Get the timeline of events for an entity.
    ///
    /// Returns event-type cards sorted chronologically.
    ///
    /// # Arguments
    /// * `entity` - The entity
    ///
    /// # Returns
    /// Event cards in chronological order.
    #[must_use]
    pub fn get_memory_timeline(&self, entity: &str) -> Vec<&MemoryCard> {
        self.memories_track.get_timeline(entity)
    }

    /// Get all preferences for an entity.
    #[must_use]
    pub fn get_preferences(&self, entity: &str) -> Vec<&MemoryCard> {
        self.memories_track.get_preferences(entity)
    }

    /// Get statistics about the memories track.
    #[must_use]
    pub fn memories_stats(&self) -> MemoriesStats {
        self.memories_track.stats()
    }

    /// Get the total number of memory cards.
    #[must_use]
    pub fn memory_card_count(&self) -> usize {
        self.memories_track.card_count()
    }

    /// Get all unique entities with memory cards.
    #[must_use]
    pub fn memory_entities(&self) -> Vec<String> {
        self.memories_track.entities()
    }

    /// Clear all memory cards and enrichment records.
    ///
    /// This is destructive and cannot be undone.
    pub fn clear_memories(&mut self) {
        self.dirty = true;
        self.memories_track.clear();
    }

    // ========================================================================
    // Schema Validation
    // ========================================================================

    /// Get an immutable reference to the schema registry.
    #[must_use]
    pub fn schema_registry(&self) -> &SchemaRegistry {
        &self.schema_registry
    }

    /// Get a mutable reference to the schema registry.
    ///
    /// Use this to register custom predicate schemas.
    pub fn schema_registry_mut(&mut self) -> &mut SchemaRegistry {
        &mut self.schema_registry
    }

    /// Enable or disable strict schema validation.
    ///
    /// When strict mode is enabled:
    /// - `put_memory_card` and `put_memory_cards` will return errors for invalid cards
    /// - Unknown predicates are rejected
    ///
    /// When strict mode is disabled (default):
    /// - Validation warnings are logged but cards are still inserted
    /// - Unknown predicates are allowed
    pub fn set_schema_strict(&mut self, strict: bool) {
        self.schema_strict = strict;
    }

    /// Check if strict schema validation is enabled.
    #[must_use]
    pub fn is_schema_strict(&self) -> bool {
        self.schema_strict
    }

    /// Register a custom predicate schema.
    ///
    /// # Arguments
    /// * `schema` - The predicate schema to register
    pub fn register_schema(&mut self, schema: PredicateSchema) {
        self.schema_registry.register(schema);
    }

    /// Validate a memory card against the schema.
    ///
    /// # Arguments
    /// * `card` - The memory card to validate
    ///
    /// # Returns
    /// `Ok(())` if valid, `Err(SchemaError)` if invalid.
    pub fn validate_card(&self, card: &MemoryCard) -> std::result::Result<(), SchemaError> {
        // Infer entity kind from the card's kind field
        let entity_kind = match card.kind {
            crate::types::MemoryKind::Fact
            | crate::types::MemoryKind::Preference
            | crate::types::MemoryKind::Profile
            | crate::types::MemoryKind::Relationship => Some(EntityKind::Person),
            // Events, goals, and other kinds can apply to any entity type
            crate::types::MemoryKind::Event
            | crate::types::MemoryKind::Goal
            | crate::types::MemoryKind::Other => None,
        };

        self.schema_registry
            .validate(&card.slot, &card.value, entity_kind)
    }

    /// Validate multiple memory cards against the schema.
    ///
    /// # Arguments
    /// * `cards` - The memory cards to validate
    ///
    /// # Returns
    /// A vector of (index, error) tuples for invalid cards.
    #[must_use]
    pub fn validate_cards(&self, cards: &[MemoryCard]) -> Vec<(usize, SchemaError)> {
        cards
            .iter()
            .enumerate()
            .filter_map(|(i, card)| self.validate_card(card).err().map(|e| (i, e)))
            .collect()
    }

    /// Infer schemas from existing memory cards.
    ///
    /// Analyzes all predicates (slots) in the memories track and infers
    /// type information (Number, `DateTime`, Boolean, String) and cardinality
    /// (Single vs Multiple) from the actual values.
    ///
    /// # Returns
    /// A vector of inferred predicate schemas.
    #[must_use]
    pub fn infer_schemas(&self) -> Vec<PredicateSchema> {
        use std::collections::HashMap;

        // Collect all values per predicate, grouped by entity
        let mut predicate_values: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

        for entity in self.memories_track.entities() {
            for card in self.memories_track.get_entity_cards(&entity) {
                predicate_values
                    .entry(card.slot.clone())
                    .or_default()
                    .entry(card.entity.clone())
                    .or_default()
                    .push(card.value.clone());
            }
        }

        // Infer schema for each predicate
        let mut schemas: Vec<PredicateSchema> = Vec::new();

        for (predicate, entity_values) in predicate_values {
            // Collect all values across all entities
            let all_values: Vec<&str> = entity_values
                .values()
                .flatten()
                .map(std::string::String::as_str)
                .collect();

            // Use the registry's inference method
            let mut schema = self
                .schema_registry
                .infer_from_values(&predicate, &all_values);

            // Determine cardinality: if any entity has multiple values, it's Multiple
            let has_multiple = entity_values.values().any(|vals| vals.len() > 1);
            if has_multiple {
                schema.cardinality = crate::types::Cardinality::Multiple;
            }

            // Try to infer domain from entity patterns
            // (In a real implementation, you'd analyze entity kinds from the LogicMesh)
            // For now, we'll leave domain empty (any entity can have this predicate)

            schemas.push(schema);
        }

        // Sort alphabetically for consistent output
        schemas.sort_by(|a, b| a.id.cmp(&b.id));
        schemas
    }

    /// Infer schemas and register them in the schema registry.
    ///
    /// This analyzes all existing memory cards and registers inferred
    /// schemas for predicates that don't already have a schema defined.
    ///
    /// # Arguments
    /// * `overwrite` - If true, overwrite existing schemas; otherwise skip them
    ///
    /// # Returns
    /// The number of schemas registered.
    pub fn register_inferred_schemas(&mut self, overwrite: bool) -> usize {
        let inferred = self.infer_schemas();
        let mut count = 0;

        for schema in inferred {
            if overwrite || !self.schema_registry.contains(&schema.id) {
                self.schema_registry.register(schema);
                count += 1;
            }
        }

        count
    }

    /// Get a summary of inferred schemas for display.
    ///
    /// Returns a structured summary suitable for CLI output.
    #[must_use]
    pub fn schema_summary(&self) -> Vec<SchemaSummaryEntry> {
        use std::collections::HashMap;

        // Collect stats per predicate
        let mut predicate_stats: HashMap<String, PredicateStats> = HashMap::new();

        for entity in self.memories_track.entities() {
            for card in self.memories_track.get_entity_cards(&entity) {
                let stats =
                    predicate_stats
                        .entry(card.slot.clone())
                        .or_insert_with(|| PredicateStats {
                            _entity_count: 0,
                            value_count: 0,
                            unique_values: std::collections::HashSet::new(),
                            entities: std::collections::HashSet::new(),
                        });

                stats.value_count += 1;
                stats.unique_values.insert(card.value.clone());
                stats.entities.insert(card.entity.clone());
            }
        }

        // Build summary entries
        let inferred = self.infer_schemas();
        let mut entries: Vec<SchemaSummaryEntry> = inferred
            .into_iter()
            .map(|schema| {
                let stats = predicate_stats.get(&schema.id);
                let (entity_count, value_count, unique_values) = stats.map_or((0, 0, 0), |s| {
                    (s.entities.len(), s.value_count, s.unique_values.len())
                });

                // Check if there's an existing (builtin) schema
                let is_builtin = self
                    .schema_registry
                    .get(&schema.id)
                    .is_some_and(|s| s.builtin);

                SchemaSummaryEntry {
                    predicate: schema.id.clone(),
                    inferred_type: schema.range.description(),
                    cardinality: schema.cardinality,
                    entity_count,
                    value_count,
                    unique_values,
                    is_builtin,
                }
            })
            .collect();

        entries.sort_by(|a, b| a.predicate.cmp(&b.predicate));
        entries
    }

    /// Run an enrichment engine over unenriched frames.
    ///
    /// This method:
    /// 1. Finds frames not yet processed by this engine version
    /// 2. Creates enrichment contexts for each frame
    /// 3. Runs the engine and collects memory cards
    /// 4. Stores cards and records enrichment
    ///
    /// # Arguments
    /// * `engine` - The enrichment engine to run
    ///
    /// # Returns
    /// A tuple of (`frames_processed`, `cards_extracted`).
    pub fn run_enrichment(
        &mut self,
        engine: &dyn crate::enrich::EnrichmentEngine,
    ) -> Result<(usize, usize)> {
        use crate::enrich::EnrichmentContext;

        let unenriched = self.get_unenriched_frames(engine.kind(), engine.version());
        let mut frames_processed = 0;
        let mut total_cards = 0;

        for frame_id in unenriched {
            // Get frame data
            // Safe frame lookup
            let Ok(index) = usize::try_from(frame_id) else {
                continue;
            };
            let Some(frame) = self.toc.frames.get(index) else {
                continue;
            };
            let frame = frame.clone();

            // Get frame content
            let text = match self.frame_content(&frame) {
                Ok(t) => t,
                Err(_) => continue,
            };

            // Create enrichment context
            let uri = frame
                .uri
                .clone()
                .unwrap_or_else(|| crate::default_uri(frame_id));
            let metadata_json = frame
                .metadata
                .as_ref()
                .and_then(|m| serde_json::to_string(m).ok());
            let ctx = EnrichmentContext::new(
                frame_id,
                uri,
                text,
                frame.title.clone(),
                frame.timestamp,
                metadata_json,
            );

            // Run enrichment
            let result = engine.enrich(&ctx);

            if result.success {
                let cards = result.cards;
                let card_count = cards.len();

                // Store cards
                let card_ids = if cards.is_empty() {
                    Vec::new()
                } else {
                    self.put_memory_cards(cards)?
                };

                // Record enrichment
                self.record_enrichment(frame_id, engine.kind(), engine.version(), card_ids)?;

                total_cards += card_count;
            }

            frames_processed += 1;
        }

        Ok((frames_processed, total_cards))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MemoryCardBuilder;
    use tempfile::NamedTempFile;

    #[test]
    fn test_put_and_get_memory_card() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();
        std::fs::remove_file(path).ok();

        let mut memvid = Memvid::create(path).unwrap();

        let card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("employer")
            .value("Anthropic")
            .source(0, Some("mv2://test".to_string()))
            .engine("test", "1.0.0")
            .build(0)
            .unwrap();

        let id = memvid.put_memory_card(card).unwrap();

        let current = memvid.get_current_memory("user", "employer");
        assert!(current.is_some());
        assert_eq!(current.unwrap().value, "Anthropic");
        assert_eq!(current.unwrap().id, id);
    }

    #[test]
    fn test_enrichment_tracking() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();
        std::fs::remove_file(path).ok();

        let mut memvid = Memvid::create(path).unwrap();

        // Initially all frames need enrichment
        assert!(!memvid.is_frame_enriched(1, "rules-v1", "1.0.0"));

        // Record enrichment
        memvid
            .record_enrichment(1, "rules-v1", "1.0.0", vec![0, 1])
            .unwrap();

        // Now frame 1 is enriched by rules-v1
        assert!(memvid.is_frame_enriched(1, "rules-v1", "1.0.0"));

        // But not by a different engine
        assert!(!memvid.is_frame_enriched(1, "llm:phi-3.5-mini", "1.0.0"));
    }

    #[test]
    fn test_memory_stats() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();
        std::fs::remove_file(path).ok();

        let mut memvid = Memvid::create(path).unwrap();

        // Add some cards
        for slot in ["employer", "location", "hobby"] {
            let card = MemoryCardBuilder::new()
                .fact()
                .entity("user")
                .slot(slot)
                .value("test")
                .source(0, None)
                .engine("test", "1.0.0")
                .build(0)
                .unwrap();
            memvid.put_memory_card(card).unwrap();
        }

        let stats = memvid.memories_stats();
        assert_eq!(stats.card_count, 3);
        assert_eq!(stats.entity_count, 1);
    }

    #[test]
    fn test_run_enrichment() {
        use crate::PutOptions;
        use crate::enrich::RulesEngine;

        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();
        std::fs::remove_file(path).ok();

        let mut memvid = Memvid::create(path).unwrap();

        // Add some frames with personal info (disable auto-extraction to test manual enrichment)
        let opts = PutOptions::builder().extract_triplets(false).build();
        memvid
            .put_bytes_with_options(b"Hello! I work at Anthropic.", opts.clone())
            .unwrap();
        memvid
            .put_bytes_with_options(b"I live in San Francisco.", opts.clone())
            .unwrap();
        memvid
            .put_bytes_with_options(b"The weather is nice today.", opts)
            .unwrap();
        memvid.commit().unwrap();

        // Run rules engine
        let engine = RulesEngine::new();
        let (frames, cards) = memvid.run_enrichment(&engine).unwrap();

        assert_eq!(frames, 3);
        assert_eq!(cards, 2); // employer + location

        // Verify cards were stored
        let employer = memvid.get_current_memory("user", "employer");
        assert!(employer.is_some());
        assert_eq!(employer.unwrap().value, "Anthropic");

        let location = memvid.get_current_memory("user", "location");
        assert!(location.is_some());
        assert_eq!(location.unwrap().value, "San Francisco");

        // Re-running should not process any frames (already enriched)
        let (frames2, cards2) = memvid.run_enrichment(&engine).unwrap();
        assert_eq!(frames2, 0);
        assert_eq!(cards2, 0);
    }

    #[test]
    fn test_schema_validation_strict() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();
        std::fs::remove_file(path).ok();

        let mut memvid = Memvid::create(path).unwrap();

        // Enable strict schema validation
        memvid.set_schema_strict(true);

        // Valid card - age with numeric value
        let valid_card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("age")
            .value("25") // Valid number
            .source(0, None)
            .engine("test", "1.0.0")
            .build(0)
            .unwrap();

        assert!(memvid.put_memory_card(valid_card).is_ok());

        // Invalid card - age with non-numeric value
        let invalid_card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("age")
            .value("twenty-five") // Invalid - not a number
            .source(0, None)
            .engine("test", "1.0.0")
            .build(0)
            .unwrap();

        let result = memvid.put_memory_card(invalid_card);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_validation_non_strict() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();
        std::fs::remove_file(path).ok();

        let mut memvid = Memvid::create(path).unwrap();

        // Non-strict mode (default)
        assert!(!memvid.is_schema_strict());

        // Invalid card - age with non-numeric value
        let invalid_card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("age")
            .value("twenty-five") // Invalid - not a number
            .source(0, None)
            .engine("test", "1.0.0")
            .build(0)
            .unwrap();

        // In non-strict mode, card should still be inserted (with warning logged)
        let result = memvid.put_memory_card(invalid_card);
        assert!(result.is_ok());

        // Verify the card was stored
        let cards = memvid.get_entity_memories("user");
        assert_eq!(cards.len(), 1);
    }

    #[test]
    fn test_schema_registry_custom() {
        use crate::types::{PredicateSchema, ValueType};

        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();
        std::fs::remove_file(path).ok();

        let mut memvid = Memvid::create(path).unwrap();
        memvid.set_schema_strict(true);

        // Register a custom schema for a "status" predicate with enum values
        let status_schema = PredicateSchema::new("status", "Status").with_range(ValueType::Enum {
            values: vec!["active".to_string(), "inactive".to_string()],
        });
        memvid.register_schema(status_schema);

        // Valid card with allowed enum value
        let valid_card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("status")
            .value("active")
            .source(0, None)
            .engine("test", "1.0.0")
            .build(0)
            .unwrap();

        assert!(memvid.put_memory_card(valid_card).is_ok());

        // Invalid card with disallowed enum value
        let invalid_card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("status")
            .value("pending") // Not in enum
            .source(0, None)
            .engine("test", "1.0.0")
            .build(0)
            .unwrap();

        assert!(memvid.put_memory_card(invalid_card).is_err());
    }
}
