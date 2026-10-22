//! Memory card types for structured memory extraction and storage.
//!
//! Memory cards are atomic, structured representations of memories extracted
//! from conversation content. Unlike raw chunks which are text fragments,
//! cards are semantic units with identity, value, temporality, provenance,
//! and versioning information.

use serde::{Deserialize, Serialize};

use crate::types::FrameId;

/// Unique identifier for a memory card within an MV2 file.
pub type MemoryCardId = u64;

/// The kind of memory being stored.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum MemoryKind {
    /// Factual information: "User works at Anthropic"
    Fact = 0,
    /// User preference: "User prefers dark mode"
    Preference = 1,
    /// Discrete event: "User moved to San Francisco on 2024-03-15"
    Event = 2,
    /// Background/profile information: "User is a software engineer"
    Profile = 3,
    /// Relationship between entities: "User's manager is Alice"
    Relationship = 4,
    /// Goal or intent: "User wants to learn Rust"
    Goal = 5,
    /// Other/custom kind
    Other = 6,
}

impl MemoryKind {
    /// Returns the string representation of this kind.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Fact => "fact",
            Self::Preference => "preference",
            Self::Event => "event",
            Self::Profile => "profile",
            Self::Relationship => "relationship",
            Self::Goal => "goal",
            Self::Other => "other",
        }
    }

    /// Parse a string into a `MemoryKind`.
    #[must_use]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "fact" => Self::Fact,
            "preference" => Self::Preference,
            "event" => Self::Event,
            "profile" => Self::Profile,
            "relationship" => Self::Relationship,
            "goal" => Self::Goal,
            _ => Self::Other,
        }
    }
}

impl Default for MemoryKind {
    fn default() -> Self {
        Self::Fact
    }
}

/// How this card relates to prior versions of the same slot.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum VersionRelation {
    /// First time this slot is being set.
    #[default]
    Sets = 0,
    /// Replaces a previous value entirely.
    Updates = 1,
    /// Adds to existing value (e.g., list of hobbies).
    Extends = 2,
    /// Negates/removes a previous value.
    Retracts = 3,
}

impl VersionRelation {
    /// Returns the string representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Sets => "sets",
            Self::Updates => "updates",
            Self::Extends => "extends",
            Self::Retracts => "retracts",
        }
    }

    /// Parse a string into a `VersionRelation`.
    #[must_use]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "updates" => Self::Updates,
            "extends" => Self::Extends,
            "retracts" => Self::Retracts,
            _ => Self::Sets,
        }
    }
}

/// Polarity for preferences and boolean facts.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Polarity {
    /// "likes", "prefers", "wants"
    Positive = 0,
    /// "dislikes", "avoids", "doesn't want"
    Negative = 1,
    /// Factual, no sentiment
    Neutral = 2,
}

impl Polarity {
    /// Returns the string representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Positive => "positive",
            Self::Negative => "negative",
            Self::Neutral => "neutral",
        }
    }

    /// Parse a string into a Polarity.
    #[must_use]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "positive" => Some(Self::Positive),
            "negative" => Some(Self::Negative),
            "neutral" => Some(Self::Neutral),
            _ => None,
        }
    }
}

impl Default for Polarity {
    fn default() -> Self {
        Self::Neutral
    }
}

impl std::fmt::Display for Polarity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::fmt::Display for MemoryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A structured memory unit extracted from conversation content.
///
/// Memory cards represent atomic facts, preferences, events, or other
/// information extracted from raw text. They support:
/// - **Identity**: What entity/slot this card describes
/// - **Value**: The actual information
/// - **Temporality**: When this was true (event time) vs when recorded (document time)
/// - **Provenance**: Which frame/chunk it came from, which engine extracted it
/// - **Versioning**: How this card relates to prior knowledge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryCard {
    /// Unique identifier within this MV2 file.
    pub id: MemoryCardId,

    /// What kind of memory this represents.
    pub kind: MemoryKind,

    /// The entity this memory is about (e.g., "user", "user.team", "project.memvid").
    pub entity: String,

    /// The attribute/slot being described (e.g., "employer", "`favorite_food`", "location").
    pub slot: String,

    /// The actual value (always stored as string, can be JSON for complex values).
    pub value: String,

    /// Sentiment/polarity for preferences.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub polarity: Option<Polarity>,

    /// When the event/fact occurred (not when it was recorded).
    /// For events: the event date.
    /// For facts: when this became true (if known).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_date: Option<i64>,

    /// When this information was recorded (from the source document/conversation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_date: Option<i64>,

    /// Versioning: key to group related cards (usually entity:slot).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_key: Option<String>,

    /// How this relates to prior versions.
    #[serde(default)]
    pub version_relation: VersionRelation,

    /// Reference to the source frame this was extracted from.
    pub source_frame_id: FrameId,

    /// URI of the source (for provenance).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_uri: Option<String>,

    /// Character offset within source frame (start, end) for highlighting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_offset: Option<(usize, usize)>,

    /// Which engine produced this card.
    pub engine: String,

    /// Version of the engine.
    pub engine_version: String,

    /// Confidence score (0.0-1.0) if from probabilistic engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,

    /// When this card was created (Unix timestamp).
    pub created_at: i64,
}

impl MemoryCard {
    /// Generate the default version key from entity and slot.
    #[must_use]
    pub fn default_version_key(&self) -> String {
        format!("{}:{}", self.entity, self.slot)
    }

    /// Check if this card supersedes another based on version relation and timestamps.
    #[must_use]
    pub fn supersedes(&self, other: &MemoryCard) -> bool {
        // Must have same version key to supersede
        let self_key = self
            .version_key
            .as_ref()
            .map_or_else(|| self.default_version_key(), std::clone::Clone::clone);
        let other_key = other
            .version_key
            .as_ref()
            .map_or_else(|| other.default_version_key(), std::clone::Clone::clone);

        if self_key != other_key {
            return false;
        }

        match self.version_relation {
            VersionRelation::Updates | VersionRelation::Retracts => {
                // Compare by event_date if available, else document_date
                let self_time = self.event_date.or(self.document_date).unwrap_or(0);
                let other_time = other.event_date.or(other.document_date).unwrap_or(0);
                self_time > other_time
            }
            VersionRelation::Sets | VersionRelation::Extends => false,
        }
    }

    /// Get the effective timestamp for temporal ordering.
    #[must_use]
    pub fn effective_timestamp(&self) -> i64 {
        self.event_date
            .or(self.document_date)
            .unwrap_or(self.created_at)
    }

    /// Check if this card is a retraction.
    #[must_use]
    pub fn is_retracted(&self) -> bool {
        self.version_relation == VersionRelation::Retracts
    }
}

/// Builder for constructing `MemoryCards`.
#[derive(Debug, Default)]
pub struct MemoryCardBuilder {
    kind: Option<MemoryKind>,
    entity: Option<String>,
    slot: Option<String>,
    value: Option<String>,
    polarity: Option<Polarity>,
    event_date: Option<i64>,
    document_date: Option<i64>,
    version_key: Option<String>,
    version_relation: VersionRelation,
    source_frame_id: Option<FrameId>,
    source_uri: Option<String>,
    source_offset: Option<(usize, usize)>,
    engine: Option<String>,
    engine_version: Option<String>,
    confidence: Option<f32>,
}

impl MemoryCardBuilder {
    /// Create a new builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the memory kind.
    #[must_use]
    pub fn kind(mut self, kind: MemoryKind) -> Self {
        self.kind = Some(kind);
        self
    }

    /// Set kind to Fact.
    #[must_use]
    pub fn fact(self) -> Self {
        self.kind(MemoryKind::Fact)
    }

    /// Set kind to Preference.
    #[must_use]
    pub fn preference(self) -> Self {
        self.kind(MemoryKind::Preference)
    }

    /// Set kind to Event.
    #[must_use]
    pub fn event(self) -> Self {
        self.kind(MemoryKind::Event)
    }

    /// Set kind to Profile.
    #[must_use]
    pub fn profile(self) -> Self {
        self.kind(MemoryKind::Profile)
    }

    /// Set kind to Relationship.
    #[must_use]
    pub fn relationship(self) -> Self {
        self.kind(MemoryKind::Relationship)
    }

    /// Set kind to Goal.
    #[must_use]
    pub fn goal(self) -> Self {
        self.kind(MemoryKind::Goal)
    }

    /// Set the entity.
    #[must_use]
    pub fn entity(mut self, entity: impl Into<String>) -> Self {
        self.entity = Some(entity.into());
        self
    }

    /// Set the slot.
    #[must_use]
    pub fn slot(mut self, slot: impl Into<String>) -> Self {
        self.slot = Some(slot.into());
        self
    }

    /// Set the value.
    #[must_use]
    pub fn value(mut self, value: impl Into<String>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Set the polarity.
    #[must_use]
    pub fn polarity(mut self, polarity: Polarity) -> Self {
        self.polarity = Some(polarity);
        self
    }

    /// Set polarity to Positive.
    #[must_use]
    pub fn positive(self) -> Self {
        self.polarity(Polarity::Positive)
    }

    /// Set polarity to Negative.
    #[must_use]
    pub fn negative(self) -> Self {
        self.polarity(Polarity::Negative)
    }

    /// Set the event date.
    #[must_use]
    pub fn event_date(mut self, ts: i64) -> Self {
        self.event_date = Some(ts);
        self
    }

    /// Set the document date.
    #[must_use]
    pub fn document_date(mut self, ts: i64) -> Self {
        self.document_date = Some(ts);
        self
    }

    /// Set the version key explicitly.
    #[must_use]
    pub fn version_key(mut self, key: impl Into<String>) -> Self {
        self.version_key = Some(key.into());
        self
    }

    /// Set version relation to Updates.
    #[must_use]
    pub fn updates(mut self) -> Self {
        self.version_relation = VersionRelation::Updates;
        self
    }

    /// Set version relation to Extends.
    #[must_use]
    pub fn extends(mut self) -> Self {
        self.version_relation = VersionRelation::Extends;
        self
    }

    /// Set version relation to Retracts.
    #[must_use]
    pub fn retracts(mut self) -> Self {
        self.version_relation = VersionRelation::Retracts;
        self
    }

    /// Set the source frame and optional URI.
    #[must_use]
    pub fn source(mut self, frame_id: FrameId, uri: Option<String>) -> Self {
        self.source_frame_id = Some(frame_id);
        self.source_uri = uri;
        self
    }

    /// Set the source offset within the frame.
    #[must_use]
    pub fn source_offset(mut self, start: usize, end: usize) -> Self {
        self.source_offset = Some((start, end));
        self
    }

    /// Set the engine name and version.
    #[must_use]
    pub fn engine(mut self, name: impl Into<String>, version: impl Into<String>) -> Self {
        self.engine = Some(name.into());
        self.engine_version = Some(version.into());
        self
    }

    /// Set the confidence score.
    #[must_use]
    pub fn confidence(mut self, conf: f32) -> Self {
        self.confidence = Some(conf.clamp(0.0, 1.0));
        self
    }

    /// Build the `MemoryCard`.
    ///
    /// # Arguments
    /// * `id` - The ID to assign (usually 0, will be reassigned on insert)
    ///
    /// # Errors
    /// Returns an error if required fields are missing.
    pub fn build(self, id: MemoryCardId) -> Result<MemoryCard, MemoryCardBuilderError> {
        let kind = self
            .kind
            .ok_or(MemoryCardBuilderError::MissingField("kind"))?;
        let entity = self
            .entity
            .ok_or(MemoryCardBuilderError::MissingField("entity"))?;
        let slot = self
            .slot
            .ok_or(MemoryCardBuilderError::MissingField("slot"))?;
        let value = self
            .value
            .ok_or(MemoryCardBuilderError::MissingField("value"))?;
        let source_frame_id = self
            .source_frame_id
            .ok_or(MemoryCardBuilderError::MissingField("source_frame_id"))?;
        let engine = self
            .engine
            .ok_or(MemoryCardBuilderError::MissingField("engine"))?;
        let engine_version = self
            .engine_version
            .ok_or(MemoryCardBuilderError::MissingField("engine_version"))?;

        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        Ok(MemoryCard {
            id,
            kind,
            entity,
            slot,
            value,
            polarity: self.polarity,
            event_date: self.event_date,
            document_date: self.document_date,
            version_key: self.version_key,
            version_relation: self.version_relation,
            source_frame_id,
            source_uri: self.source_uri,
            source_offset: self.source_offset,
            engine,
            engine_version,
            confidence: self.confidence,
            created_at,
        })
    }
}

/// Error type for `MemoryCardBuilder`.
#[derive(Debug, Clone)]
pub enum MemoryCardBuilderError {
    /// A required field is missing.
    MissingField(&'static str),
}

impl std::fmt::Display for MemoryCardBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(field) => write!(f, "missing required field: {field}"),
        }
    }
}

impl std::error::Error for MemoryCardBuilderError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_card_builder() {
        let card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("employer")
            .value("Anthropic")
            .source(42, Some("mv2://chat/session1".to_string()))
            .engine("rules-v1", "1.0.0")
            .build(1)
            .unwrap();

        assert_eq!(card.id, 1);
        assert_eq!(card.kind, MemoryKind::Fact);
        assert_eq!(card.entity, "user");
        assert_eq!(card.slot, "employer");
        assert_eq!(card.value, "Anthropic");
        assert_eq!(card.source_frame_id, 42);
        assert_eq!(card.engine, "rules-v1");
    }

    #[test]
    fn test_preference_with_polarity() {
        let card = MemoryCardBuilder::new()
            .preference()
            .entity("user")
            .slot("beverage")
            .value("coffee")
            .positive()
            .source(1, None)
            .engine("rules-v1", "1.0.0")
            .build(1)
            .unwrap();

        assert_eq!(card.kind, MemoryKind::Preference);
        assert_eq!(card.polarity, Some(Polarity::Positive));
    }

    #[test]
    fn test_version_key_default() {
        let card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("location")
            .value("San Francisco")
            .source(1, None)
            .engine("rules-v1", "1.0.0")
            .build(1)
            .unwrap();

        assert_eq!(card.default_version_key(), "user:location");
    }

    #[test]
    fn test_supersedes() {
        let old_card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("location")
            .value("New York")
            .document_date(1000)
            .source(1, None)
            .engine("rules-v1", "1.0.0")
            .build(1)
            .unwrap();

        let mut new_card = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            .slot("location")
            .value("San Francisco")
            .document_date(2000)
            .source(2, None)
            .engine("rules-v1", "1.0.0")
            .updates()
            .build(2)
            .unwrap();

        assert!(new_card.supersedes(&old_card));

        // Sets doesn't supersede
        new_card.version_relation = VersionRelation::Sets;
        assert!(!new_card.supersedes(&old_card));
    }

    #[test]
    fn test_builder_missing_field() {
        let result = MemoryCardBuilder::new()
            .fact()
            .entity("user")
            // missing slot, value, source, engine
            .build(1);

        assert!(result.is_err());
    }

    #[test]
    fn test_memory_kind_from_str() {
        assert_eq!(MemoryKind::from_str("fact"), MemoryKind::Fact);
        assert_eq!(MemoryKind::from_str("PREFERENCE"), MemoryKind::Preference);
        assert_eq!(MemoryKind::from_str("custom_type"), MemoryKind::Other);
    }
}
