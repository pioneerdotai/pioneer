//! Memories track for storing structured memory cards within MV2 files.
//!
//! The memories track is a specialized track within an MV2 file that stores
//! extracted memory cards along with indices for fast lookup and enrichment
//! tracking metadata.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::{MemvidError, Result};
use crate::types::FrameId;
use crate::types::memory_card::{MemoryCard, MemoryCardId, MemoryKind, Polarity, VersionRelation};

/// Magic bytes identifying the memories track.
pub const MEMORIES_TRACK_MAGIC: &[u8; 4] = b"MVMC";

/// Current version of the memories track format.
pub const MEMORIES_TRACK_VERSION: u16 = 1;

/// Slot key combining entity and slot name.
fn slot_key(entity: &str, slot: &str) -> String {
    format!("{}:{}", entity.to_lowercase(), slot.to_lowercase())
}

/// Parse a slot key back to entity and slot.
fn parse_slot_key(key: &str) -> Option<(&str, &str)> {
    key.split_once(':')
}

/// Index for fast slot-based lookups.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SlotIndex {
    /// Maps "entity:slot" -> list of card IDs (most recent first).
    entries: HashMap<String, Vec<MemoryCardId>>,
}

impl SlotIndex {
    /// Create a new empty slot index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a card into the index.
    pub fn insert(&mut self, card: &MemoryCard) {
        let key = slot_key(&card.entity, &card.slot);
        self.entries.entry(key).or_default().insert(0, card.id);
    }

    /// Get card IDs for a given entity and slot (case-insensitive).
    #[must_use]
    pub fn get(&self, entity: &str, slot: &str) -> Option<&[MemoryCardId]> {
        let key = slot_key(entity, slot); // Already lowercased
        // Try exact match first (new files with lowercase keys)
        if let Some(ids) = self.entries.get(&key) {
            return Some(ids.as_slice());
        }
        // Fall back to case-insensitive search (backwards compatibility)
        self.entries
            .iter()
            .find(|(k, _)| k.to_lowercase() == key)
            .map(|(_, ids)| ids.as_slice())
    }

    /// Get all card IDs for a given entity (case-insensitive).
    #[must_use]
    pub fn get_by_entity(&self, entity: &str) -> Vec<MemoryCardId> {
        let prefix = format!("{}:", entity.to_lowercase());
        self.entries
            .iter()
            .filter(|(k, _)| k.to_lowercase().starts_with(&prefix))
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect()
    }

    /// Get all unique entities in the index.
    #[must_use]
    pub fn entities(&self) -> Vec<String> {
        let mut entities: Vec<_> = self
            .entries
            .keys()
            .filter_map(|k| parse_slot_key(k).map(|(e, _)| e.to_string()))
            .collect();
        entities.sort();
        entities.dedup();
        entities
    }

    /// Get all unique slots for an entity.
    #[must_use]
    pub fn slots_for_entity(&self, entity: &str) -> Vec<String> {
        let prefix = format!("{}:", entity.to_lowercase());
        let mut slots: Vec<_> = self
            .entries
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .filter_map(|k| parse_slot_key(k).map(|(_, s)| s.to_string()))
            .collect();
        slots.sort();
        slots.dedup();
        slots
    }

    /// Clear the index.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Get total number of card references.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.values().map(Vec::len).sum()
    }

    /// Check if the index is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Record of which engines have enriched a frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentRecord {
    /// The frame that was enriched.
    pub frame_id: FrameId,
    /// Stamps for each engine that processed this frame.
    pub stamps: Vec<EngineStamp>,
}

/// Stamp recording when an engine processed a frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineStamp {
    /// Engine identifier (e.g., "rules-v1", "llm:phi-3.5-mini").
    pub engine_kind: String,
    /// Engine version (e.g., "1.0.0").
    pub engine_version: String,
    /// Unix timestamp when enrichment occurred.
    pub enriched_at: i64,
    /// Card IDs produced from this frame by this engine.
    pub card_ids: Vec<MemoryCardId>,
}

/// Enrichment manifest tracking which frames have been processed.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EnrichmentManifest {
    /// Per-frame enrichment records.
    frames: HashMap<FrameId, EnrichmentRecord>,
    /// Total frames that have been enriched.
    pub total_frames_enriched: usize,
    /// Total cards created across all enrichments.
    pub total_cards_created: usize,
    /// Timestamp of last enrichment run.
    pub last_enrichment: Option<i64>,
}

impl EnrichmentManifest {
    /// Create a new empty manifest.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a frame needs enrichment by a specific engine version.
    #[must_use]
    pub fn needs_enrichment(
        &self,
        frame_id: FrameId,
        engine_kind: &str,
        engine_version: &str,
    ) -> bool {
        match self.frames.get(&frame_id) {
            None => true, // Never enriched
            Some(record) => {
                // Check if this specific engine+version has run
                !record
                    .stamps
                    .iter()
                    .any(|s| s.engine_kind == engine_kind && s.engine_version == engine_version)
            }
        }
    }

    /// Get frames that need enrichment by a specific engine.
    pub fn frames_needing_enrichment<I>(
        &self,
        all_frame_ids: I,
        engine_kind: &str,
        engine_version: &str,
    ) -> Vec<FrameId>
    where
        I: Iterator<Item = FrameId>,
    {
        all_frame_ids
            .filter(|id| self.needs_enrichment(*id, engine_kind, engine_version))
            .collect()
    }

    /// Record that a frame was enriched.
    pub fn record_enrichment(
        &mut self,
        frame_id: FrameId,
        engine_kind: &str,
        engine_version: &str,
        card_ids: Vec<MemoryCardId>,
    ) {
        let stamp = EngineStamp {
            engine_kind: engine_kind.to_string(),
            engine_version: engine_version.to_string(),
            enriched_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            card_ids: card_ids.clone(),
        };

        let record = self.frames.entry(frame_id).or_insert_with(|| {
            self.total_frames_enriched += 1;
            EnrichmentRecord {
                frame_id,
                stamps: Vec::new(),
            }
        });

        self.total_cards_created += card_ids.len();
        self.last_enrichment = Some(stamp.enriched_at);
        record.stamps.push(stamp);
    }

    /// Get the enrichment record for a frame.
    #[must_use]
    pub fn get_record(&self, frame_id: FrameId) -> Option<&EnrichmentRecord> {
        self.frames.get(&frame_id)
    }

    /// Get all enriched frame IDs.
    #[must_use]
    pub fn enriched_frames(&self) -> Vec<FrameId> {
        self.frames.keys().copied().collect()
    }

    /// Clear all enrichment records.
    pub fn clear(&mut self) {
        self.frames.clear();
        self.total_frames_enriched = 0;
        self.total_cards_created = 0;
        self.last_enrichment = None;
    }
}

/// The memories track stored within an MV2 file.
///
/// This track stores all extracted memory cards along with indices for
/// fast lookup and metadata for tracking enrichment progress.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MemoriesTrack {
    /// All memory cards.
    cards: Vec<MemoryCard>,
    /// Next card ID to assign.
    next_id: MemoryCardId,
    /// Fast lookup by (entity, slot).
    slot_index: SlotIndex,
    /// Enrichment tracking.
    enrichment_manifest: EnrichmentManifest,
}

impl MemoriesTrack {
    /// Create a new empty memories track.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new memory card, returns the assigned ID.
    pub fn add_card(&mut self, mut card: MemoryCard) -> MemoryCardId {
        let id = self.next_id;
        self.next_id += 1;
        card.id = id;

        // Set version key if not provided
        if card.version_key.is_none() {
            card.version_key = Some(card.default_version_key());
        }

        self.slot_index.insert(&card);
        self.cards.push(card);
        id
    }

    /// Add multiple cards at once.
    pub fn add_cards(&mut self, cards: Vec<MemoryCard>) -> Vec<MemoryCardId> {
        cards.into_iter().map(|c| self.add_card(c)).collect()
    }

    /// Get a card by ID.
    #[must_use]
    pub fn get_card(&self, id: MemoryCardId) -> Option<&MemoryCard> {
        self.cards.iter().find(|c| c.id == id)
    }

    /// Get all cards.
    #[must_use]
    pub fn cards(&self) -> &[MemoryCard] {
        &self.cards
    }

    /// Get total number of cards.
    #[must_use]
    pub fn card_count(&self) -> usize {
        self.cards.len()
    }

    /// Access the enrichment manifest.
    #[must_use]
    pub fn enrichment_manifest(&self) -> &EnrichmentManifest {
        &self.enrichment_manifest
    }

    /// Access the enrichment manifest mutably.
    pub fn enrichment_manifest_mut(&mut self) -> &mut EnrichmentManifest {
        &mut self.enrichment_manifest
    }

    /// Record that a frame was enriched by an engine.
    pub fn record_enrichment(
        &mut self,
        frame_id: FrameId,
        engine_kind: &str,
        engine_version: &str,
        card_ids: Vec<MemoryCardId>,
    ) {
        self.enrichment_manifest
            .record_enrichment(frame_id, engine_kind, engine_version, card_ids);
    }

    /// Check if a frame has been enriched by a specific engine version.
    #[must_use]
    pub fn is_enriched_by(
        &self,
        frame_id: FrameId,
        engine_kind: &str,
        engine_version: &str,
    ) -> bool {
        !self
            .enrichment_manifest
            .needs_enrichment(frame_id, engine_kind, engine_version)
    }

    /// Get all cards for a given entity and slot.
    #[must_use]
    pub fn get_cards(&self, entity: &str, slot: &str) -> Vec<&MemoryCard> {
        self.slot_index
            .get(entity, slot)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.cards.iter().find(|c| c.id == *id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the current (most recent, non-retracted) value for an entity:slot.
    #[must_use]
    pub fn get_current(&self, entity: &str, slot: &str) -> Option<&MemoryCard> {
        let mut cards = self.get_cards(entity, slot);

        // Sort by effective timestamp descending
        cards.sort_by(|a, b| {
            let a_time = a.effective_timestamp();
            let b_time = b.effective_timestamp();
            b_time.cmp(&a_time)
        });

        // Find first non-retracted card
        cards.into_iter().find(|c| !c.is_retracted())
    }

    /// Get value at a specific point in time.
    #[must_use]
    pub fn get_at_time(&self, entity: &str, slot: &str, timestamp: i64) -> Option<&MemoryCard> {
        let mut cards: Vec<_> = self
            .get_cards(entity, slot)
            .into_iter()
            .filter(|c| c.effective_timestamp() <= timestamp)
            .collect();

        cards.sort_by(|a, b| {
            let a_time = a.effective_timestamp();
            let b_time = b.effective_timestamp();
            b_time.cmp(&a_time)
        });

        cards.into_iter().find(|c| !c.is_retracted())
    }

    /// Get all cards for an entity.
    #[must_use]
    pub fn get_entity_cards(&self, entity: &str) -> Vec<&MemoryCard> {
        self.slot_index
            .get_by_entity(entity)
            .iter()
            .filter_map(|id| self.cards.iter().find(|c| c.id == *id))
            .collect()
    }

    /// Aggregate all values for a slot (for multi-session scenarios).
    #[must_use]
    pub fn aggregate_slot(&self, entity: &str, slot: &str) -> Vec<String> {
        let cards = self.get_cards(entity, slot);
        let mut values: Vec<String> = Vec::new();

        for card in cards {
            match card.version_relation {
                VersionRelation::Extends => {
                    if !values.contains(&card.value) {
                        values.push(card.value.clone());
                    }
                }
                VersionRelation::Retracts => {
                    values.retain(|v| v != &card.value);
                }
                VersionRelation::Sets | VersionRelation::Updates => {
                    if !values.contains(&card.value) {
                        values.push(card.value.clone());
                    }
                }
            }
        }

        values
    }

    /// Count occurrences (for "how many times did I mention X?" questions).
    #[must_use]
    pub fn count_occurrences(&self, entity: &str, slot: &str, value_filter: Option<&str>) -> usize {
        self.get_cards(entity, slot)
            .iter()
            .filter(|c| {
                if let Some(v) = value_filter {
                    c.value.to_lowercase().contains(&v.to_lowercase())
                } else {
                    true
                }
            })
            .count()
    }

    /// Get timeline of events for an entity.
    #[must_use]
    pub fn get_timeline(&self, entity: &str) -> Vec<&MemoryCard> {
        let mut cards: Vec<_> = self
            .get_entity_cards(entity)
            .into_iter()
            .filter(|c| c.kind == MemoryKind::Event)
            .collect();

        cards.sort_by_key(|c| c.effective_timestamp());
        cards
    }

    /// Get all preferences for an entity.
    #[must_use]
    pub fn get_preferences(&self, entity: &str) -> Vec<&MemoryCard> {
        self.get_entity_cards(entity)
            .into_iter()
            .filter(|c| c.kind == MemoryKind::Preference)
            .collect()
    }

    /// Get positive preferences for an entity.
    #[must_use]
    pub fn get_positive_preferences(&self, entity: &str) -> Vec<&MemoryCard> {
        self.get_preferences(entity)
            .into_iter()
            .filter(|c| c.polarity == Some(Polarity::Positive))
            .collect()
    }

    /// Get all unique entities.
    #[must_use]
    pub fn entities(&self) -> Vec<String> {
        self.slot_index.entities()
    }

    /// Get all unique slots for an entity.
    #[must_use]
    pub fn slots_for_entity(&self, entity: &str) -> Vec<String> {
        self.slot_index.slots_for_entity(entity)
    }

    /// Serialize the track for storage using JSON.
    /// We use JSON for complex nested structures to ensure compatibility.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(MEMORIES_TRACK_MAGIC);
        buf.extend_from_slice(&MEMORIES_TRACK_VERSION.to_le_bytes());

        let json_data = serde_json::to_vec(self).map_err(|e| MemvidError::InvalidHeader {
            reason: format!("failed to serialize memories track: {e}").into(),
        })?;

        // Compress the JSON data
        let compressed =
            zstd::encode_all(json_data.as_slice(), 3).map_err(|e| MemvidError::InvalidHeader {
                reason: format!("failed to compress memories track: {e}").into(),
            })?;

        buf.extend_from_slice(&(compressed.len() as u64).to_le_bytes());
        buf.extend(compressed);

        Ok(buf)
    }

    /// Deserialize from storage.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 14 {
            return Err(MemvidError::InvalidHeader {
                reason: "memories track too short".into(),
            });
        }

        if &data[0..4] != MEMORIES_TRACK_MAGIC {
            return Err(MemvidError::InvalidHeader {
                reason: "invalid memories track magic".into(),
            });
        }

        let version = u16::from_le_bytes([data[4], data[5]]);
        if version != MEMORIES_TRACK_VERSION {
            return Err(MemvidError::InvalidHeader {
                reason: format!("unsupported memories version: {version}").into(),
            });
        }

        let len = usize::try_from(u64::from_le_bytes([
            data[6], data[7], data[8], data[9], data[10], data[11], data[12],
            data[13],
            // Safe: checked on next line that data.len() >= 14 + len, so len fits in available memory
        ]))
        .unwrap_or(0);
        if data.len() < 14 + len {
            return Err(MemvidError::InvalidHeader {
                reason: "memories track data truncated".into(),
            });
        }

        // Decompress the data
        let decompressed =
            zstd::decode_all(&data[14..14 + len]).map_err(|e| MemvidError::InvalidHeader {
                reason: format!("failed to decompress memories track: {e}").into(),
            })?;

        let track: MemoriesTrack =
            serde_json::from_slice(&decompressed).map_err(|e| MemvidError::InvalidHeader {
                reason: format!("failed to deserialize memories track: {e}").into(),
            })?;

        Ok(track)
    }

    /// Clear all cards and reset the track.
    pub fn clear(&mut self) {
        self.cards.clear();
        self.next_id = 0;
        self.slot_index.clear();
        self.enrichment_manifest.clear();
    }
}

/// Statistics about the memories track.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoriesStats {
    /// Total number of cards.
    pub card_count: usize,
    /// Number of unique entities.
    pub entity_count: usize,
    /// Number of unique (entity, slot) pairs.
    pub slot_count: usize,
    /// Cards by kind.
    pub cards_by_kind: HashMap<String, usize>,
    /// Number of enriched frames.
    pub enriched_frames: usize,
    /// Last enrichment timestamp.
    pub last_enrichment: Option<i64>,
}

impl MemoriesTrack {
    /// Get statistics about the track.
    #[must_use]
    pub fn stats(&self) -> MemoriesStats {
        let mut cards_by_kind: HashMap<String, usize> = HashMap::new();
        for card in &self.cards {
            *cards_by_kind
                .entry(card.kind.as_str().to_string())
                .or_default() += 1;
        }

        MemoriesStats {
            card_count: self.cards.len(),
            entity_count: self.entities().len(),
            slot_count: self.slot_index.len(),
            cards_by_kind,
            enriched_frames: self.enrichment_manifest.total_frames_enriched,
            last_enrichment: self.enrichment_manifest.last_enrichment,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::memory_card::MemoryCardBuilder;

    #[test]
    fn test_add_and_retrieve_card() {
        let mut track = MemoriesTrack::new();

        let card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("employer")
            .value("Anthropic")
            .source(1, None)
            .engine("rules-v1", "1.0.0")
            .build(0)
            .unwrap();

        let id = track.add_card(card);
        assert_eq!(id, 0);

        let retrieved = track.get_card(id).unwrap();
        assert_eq!(retrieved.value, "Anthropic");
    }

    #[test]
    fn test_get_current() {
        let mut track = MemoriesTrack::new();

        // Add old value
        let old_card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("location")
            .value("New York")
            .document_date(1000)
            .source(1, None)
            .engine("rules-v1", "1.0.0")
            .build(0)
            .unwrap();
        track.add_card(old_card);

        // Add new value
        let new_card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("location")
            .value("San Francisco")
            .document_date(2000)
            .source(2, None)
            .engine("rules-v1", "1.0.0")
            .updates()
            .build(0)
            .unwrap();
        track.add_card(new_card);

        let current = track.get_current("user", "location").unwrap();
        assert_eq!(current.value, "San Francisco");
    }

    #[test]
    fn test_get_at_time() {
        let mut track = MemoriesTrack::new();

        let card1 = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("location")
            .value("New York")
            .document_date(1000)
            .source(1, None)
            .engine("rules-v1", "1.0.0")
            .build(0)
            .unwrap();
        track.add_card(card1);

        let card2 = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("location")
            .value("San Francisco")
            .document_date(2000)
            .source(2, None)
            .engine("rules-v1", "1.0.0")
            .updates()
            .build(0)
            .unwrap();
        track.add_card(card2);

        // Query at time 1500 should return New York
        let at_1500 = track.get_at_time("user", "location", 1500).unwrap();
        assert_eq!(at_1500.value, "New York");

        // Query at time 2500 should return San Francisco
        let at_2500 = track.get_at_time("user", "location", 2500).unwrap();
        assert_eq!(at_2500.value, "San Francisco");
    }

    #[test]
    fn test_enrichment_tracking() {
        let mut track = MemoriesTrack::new();

        // Frame 1 is not enriched
        assert!(
            track
                .enrichment_manifest()
                .needs_enrichment(1, "rules-v1", "1.0.0")
        );

        // Enrich frame 1
        track.record_enrichment(1, "rules-v1", "1.0.0", vec![0, 1]);

        // Frame 1 is now enriched by rules-v1
        assert!(
            !track
                .enrichment_manifest()
                .needs_enrichment(1, "rules-v1", "1.0.0")
        );

        // But not by llm
        assert!(
            track
                .enrichment_manifest()
                .needs_enrichment(1, "llm:phi-3.5-mini", "1.0.0")
        );
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut track = MemoriesTrack::new();

        let card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("name")
            .value("Alice")
            .source(1, None)
            .engine("rules-v1", "1.0.0")
            .build(0)
            .unwrap();
        track.add_card(card);

        let serialized = track.serialize().unwrap();
        let deserialized = MemoriesTrack::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.card_count(), 1);
        let card = deserialized.get_card(0).unwrap();
        assert_eq!(card.value, "Alice");
    }

    #[test]
    fn test_aggregate_slot() {
        let mut track = MemoriesTrack::new();

        // Add multiple hobbies
        for hobby in ["reading", "hiking", "coding"] {
            let card = MemoryCardBuilder::new()
                .preference()
                .entity("user")
                .slot("hobby")
                .value(hobby)
                .extends()
                .positive()
                .source(1, None)
                .engine("rules-v1", "1.0.0")
                .build(0)
                .unwrap();
            track.add_card(card);
        }

        let hobbies = track.aggregate_slot("user", "hobby");
        assert_eq!(hobbies.len(), 3);
        assert!(hobbies.contains(&"reading".to_string()));
        assert!(hobbies.contains(&"hiking".to_string()));
        assert!(hobbies.contains(&"coding".to_string()));
    }

    #[test]
    fn test_count_occurrences() {
        let mut track = MemoriesTrack::new();

        // Add mentions of gym
        for i in 0..5 {
            let card = MemoryCardBuilder::new()
                .event()
                .entity("user")
                .slot("activity")
                .value("went to the gym")
                .source(i, None)
                .engine("rules-v1", "1.0.0")
                .build(0)
                .unwrap();
            track.add_card(card);
        }

        assert_eq!(track.count_occurrences("user", "activity", Some("gym")), 5);
        assert_eq!(track.count_occurrences("user", "activity", Some("pool")), 0);
    }

    #[test]
    fn test_timeline() {
        let mut track = MemoriesTrack::new();

        let events = [
            ("started job", 1000),
            ("moved to SF", 2000),
            ("got promoted", 3000),
        ];

        for (event, ts) in events {
            let card = MemoryCardBuilder::new()
                .event()
                .entity("user")
                .slot("life_event")
                .value(event)
                .event_date(ts)
                .source(1, None)
                .engine("rules-v1", "1.0.0")
                .build(0)
                .unwrap();
            track.add_card(card);
        }

        let timeline = track.get_timeline("user");
        assert_eq!(timeline.len(), 3);
        assert_eq!(timeline[0].value, "started job");
        assert_eq!(timeline[1].value, "moved to SF");
        assert_eq!(timeline[2].value, "got promoted");
    }
}
