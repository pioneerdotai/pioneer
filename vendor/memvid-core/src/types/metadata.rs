//! Rich metadata structures describing document, audio, and textual attributes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MediaManifest {
    pub kind: String,
    pub mime: String,
    pub bytes: u64,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub codec: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct DocMetadata {
    #[serde(default)]
    pub mime: Option<String>,
    #[serde(default)]
    pub bytes: Option<u64>,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub colors: Option<Vec<String>>,
    #[serde(default)]
    pub caption: Option<String>,
    #[serde(default)]
    pub exif: Option<DocExifMetadata>,
    #[serde(default)]
    pub audio: Option<DocAudioMetadata>,
    #[serde(default)]
    pub media: Option<MediaManifest>,
}

impl DocMetadata {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mime.is_none()
            && self.bytes.is_none()
            && self.hash.is_none()
            && self.width.is_none()
            && self.height.is_none()
            && self.colors.as_ref().is_none_or(Vec::is_empty)
            && self.caption.is_none()
            && self.exif.is_none()
            && self.audio.as_ref().is_none_or(DocAudioMetadata::is_empty)
            && self.media.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct DocAudioMetadata {
    #[serde(default)]
    pub duration_secs: Option<f32>,
    #[serde(default)]
    pub sample_rate_hz: Option<u32>,
    #[serde(default)]
    pub channels: Option<u8>,
    #[serde(default)]
    pub bitrate_kbps: Option<u32>,
    #[serde(default)]
    pub codec: Option<String>,
    #[serde(default)]
    pub segments: Vec<AudioSegmentMetadata>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

impl DocAudioMetadata {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.duration_secs.is_none()
            && self.sample_rate_hz.is_none()
            && self.channels.is_none()
            && self.bitrate_kbps.is_none()
            && self.codec.is_none()
            && self.segments.is_empty()
            && self.tags.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AudioSegmentMetadata {
    pub start_seconds: f32,
    pub end_seconds: f32,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TextChunkRange {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TextChunkManifest {
    pub chunk_chars: usize,
    #[serde(default)]
    pub chunks: Vec<TextChunkRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct DocExifMetadata {
    #[serde(default)]
    pub make: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub lens: Option<String>,
    #[serde(default)]
    pub datetime: Option<String>,
    #[serde(default)]
    pub gps: Option<DocGpsMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct DocGpsMetadata {
    pub latitude: f64,
    pub longitude: f64,
}
