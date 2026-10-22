//! Temporal mention primitives (feature-gated by `temporal_track`).

use serde::{Deserialize, Serialize};

use super::{common::FrameId, frame::AnchorSource};

/// Bit flag indicating the temporal track stores per-frame anchors.
pub const TEMPORAL_TRACK_FLAG_HAS_ANCHORS: u32 = 0b0000_0001;
/// Bit flag indicating the temporal track stores normalised mentions.
pub const TEMPORAL_TRACK_FLAG_HAS_MENTIONS: u32 = 0b0000_0010;

/// Distinguishes the category of a temporal mention.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TemporalMentionKind {
    Date,
    DateTime,
    RangeStart,
    RangeEnd,
    Recurring,
    Approximate,
}

impl TemporalMentionKind {
    #[must_use]
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Date => 0,
            Self::DateTime => 1,
            Self::RangeStart => 2,
            Self::RangeEnd => 3,
            Self::Recurring => 4,
            Self::Approximate => 5,
        }
    }

    #[must_use]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Date),
            1 => Some(Self::DateTime),
            2 => Some(Self::RangeStart),
            3 => Some(Self::RangeEnd),
            4 => Some(Self::Recurring),
            5 => Some(Self::Approximate),
            _ => None,
        }
    }
}

/// Bitflags describing additional mention semantics.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporalMentionFlags(pub u8);

impl TemporalMentionFlags {
    pub const HAS_RANGE: u8 = 0b0000_0001;
    pub const DERIVED: u8 = 0b0000_0010;
    pub const AMBIGUOUS: u8 = 0b0000_0100;
    pub const LOW_CONFIDENCE: u8 = 0b0000_1000;

    #[must_use]
    pub fn empty() -> Self {
        Self(0)
    }

    #[must_use]
    pub fn contains(self, flag: u8) -> bool {
        (self.0 & flag) != 0
    }

    #[must_use]
    pub fn set(mut self, flag: u8, enabled: bool) -> Self {
        if enabled {
            self.0 |= flag;
        } else {
            self.0 &= !flag;
        }
        self
    }
}

/// Canonical representation of a single mention tied to a frame.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporalMention {
    pub ts_utc: i64,
    pub frame_id: FrameId,
    pub byte_start: u32,
    pub byte_len: u32,
    pub kind: TemporalMentionKind,
    pub confidence: u16,
    pub tz_hint_minutes: i16,
    pub flags: TemporalMentionFlags,
}

impl TemporalMention {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ts_utc: i64,
        frame_id: FrameId,
        byte_start: u32,
        byte_len: u32,
        kind: TemporalMentionKind,
        confidence: u16,
        tz_hint_minutes: i16,
        flags: TemporalMentionFlags,
    ) -> Self {
        Self {
            ts_utc,
            frame_id,
            byte_start,
            byte_len,
            kind,
            confidence,
            tz_hint_minutes,
            flags,
        }
    }
}

/// Persisted anchor metadata per frame.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporalAnchor {
    pub frame_id: FrameId,
    pub anchor_ts: i64,
    pub source: AnchorSource,
}

impl TemporalAnchor {
    #[must_use]
    pub fn new(frame_id: FrameId, anchor_ts: i64, source: AnchorSource) -> Self {
        Self {
            frame_id,
            anchor_ts,
            source,
        }
    }
}

/// In-memory representation of the temporal track payload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TemporalTrack {
    pub mentions: Vec<TemporalMention>,
    pub anchors: Vec<TemporalAnchor>,
    pub flags: u32,
}

impl TemporalTrack {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mentions.is_empty()
    }

    #[must_use]
    pub fn capabilities(&self) -> TemporalCapabilities {
        TemporalCapabilities {
            has_anchors: self.flags & TEMPORAL_TRACK_FLAG_HAS_ANCHORS != 0
                || !self.anchors.is_empty(),
            has_mentions: self.flags & TEMPORAL_TRACK_FLAG_HAS_MENTIONS != 0
                || !self.mentions.is_empty(),
        }
    }

    #[must_use]
    pub fn anchor_for_frame(&self, frame_id: FrameId) -> Option<&TemporalAnchor> {
        self.anchors
            .binary_search_by_key(&frame_id, |anchor| anchor.frame_id)
            .ok()
            .map(|idx| &self.anchors[idx])
    }
}

/// Request-time temporal window applied to queries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporalFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_utc: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_utc: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phrase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tz: Option<String>,
}

impl TemporalFilter {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let phrase_empty = self
            .phrase
            .as_ref()
            .map(|phrase| phrase.trim().is_empty())
            .unwrap_or(true);
        self.start_utc.is_none() && self.end_utc.is_none() && phrase_empty
    }
}

/// Convenience wrapper describing which temporal features are available on a track.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TemporalCapabilities {
    pub has_anchors: bool,
    pub has_mentions: bool,
}
