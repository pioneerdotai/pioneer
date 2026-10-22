//! Timeline assembly helpers for `Memvid`.

use crate::Result;
use crate::io::time_index::{TimeIndexEntry, read_track as time_index_read};
use crate::memvid::lifecycle::Memvid;
#[cfg(feature = "temporal_track")]
use crate::memvid::search::frame_ids_for_temporal_filter;
use crate::types::{FrameId, FrameRole, FrameStatus, TimelineEntry};
#[cfg(feature = "temporal_track")]
use crate::types::{
    SearchHitTemporal, SearchHitTemporalAnchor, SearchHitTemporalMention, TemporalFilter,
    TemporalTrack,
};
#[cfg(feature = "temporal_track")]
use std::collections::HashSet;
use std::num::NonZeroU64;
#[cfg(feature = "temporal_track")]
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub(crate) fn build_timeline(
    memvid: &mut Memvid,
    limit: Option<NonZeroU64>,
    since: Option<i64>,
    until: Option<i64>,
    reverse: bool,
    #[cfg(feature = "temporal_track")] temporal: Option<&TemporalFilter>,
) -> Result<Vec<TimelineEntry>> {
    #[cfg(feature = "temporal_track")]
    let temporal_candidates: Option<HashSet<FrameId>> = if let Some(filter) = temporal {
        if filter.is_empty() {
            None
        } else {
            match frame_ids_for_temporal_filter(memvid, filter)? {
                Some(ids) => {
                    let set: HashSet<FrameId> = ids.into_iter().collect();
                    if set.is_empty() {
                        return Ok(Vec::new());
                    }
                    Some(set)
                }
                None => None,
            }
        }
    } else {
        None
    };

    let mut entries = if let Some(manifest) = &memvid.toc.time_index {
        let mut indexed = time_index_read(
            &mut memvid.file,
            manifest.bytes_offset,
            manifest.bytes_length,
        )?;
        // Also include ExtractedImage frames (child frames) which may not be in time index
        let indexed_ids: std::collections::HashSet<FrameId> =
            indexed.iter().map(|e| e.frame_id).collect();
        for frame in &memvid.toc.frames {
            if frame.status == FrameStatus::Active
                && frame.role == FrameRole::ExtractedImage
                && !indexed_ids.contains(&frame.id)
            {
                indexed.push(TimeIndexEntry::new(frame.timestamp, frame.id));
            }
        }
        indexed
    } else {
        memvid
            .toc
            .frames
            .iter()
            .filter(|frame| frame.status == FrameStatus::Active)
            .map(|frame| TimeIndexEntry::new(frame.timestamp, frame.id))
            .collect()
    };

    #[cfg(feature = "temporal_track")]
    if let Some(ref candidates) = temporal_candidates {
        entries.retain(|entry| candidates.contains(&entry.frame_id));
    }

    entries.retain(|entry| {
        let after_since = since.is_none_or(|s| entry.timestamp >= s);
        let before_until = until.is_none_or(|u| entry.timestamp <= u);
        after_since && before_until
    });

    if reverse {
        entries.reverse();
    }

    let limit = limit.map_or(entries.len(), |nz| {
        usize::try_from(nz.get()).unwrap_or(usize::MAX)
    });
    let mut result = Vec::with_capacity(entries.len().min(limit));
    #[cfg(feature = "temporal_track")]
    let temporal_track_snapshot = memvid.temporal_track_ref()?.cloned();
    for entry in entries.into_iter().take(limit) {
        let Some(frame) = memvid
            .toc
            .frames
            .get(usize::try_from(entry.frame_id).unwrap_or(usize::MAX))
            .cloned()
        else {
            tracing::warn!(
                frame_id = entry.frame_id,
                "skipping time index entry with out-of-range frame id"
            );
            continue;
        };
        if frame.status != FrameStatus::Active {
            continue;
        }
        let preview = memvid.frame_preview(&frame)?;
        let uri = frame
            .uri
            .clone()
            .or_else(|| Some(crate::default_uri(frame.id)));
        let child_frames: Vec<FrameId> = memvid
            .toc
            .frames
            .iter()
            .filter(|candidate| {
                candidate.status == FrameStatus::Active && candidate.parent_id == Some(frame.id)
            })
            .map(|candidate| candidate.id)
            .collect();
        #[cfg(feature = "temporal_track")]
        let temporal_info = if let Some(track) = temporal_track_snapshot.as_ref() {
            build_timeline_temporal_metadata(memvid, track, &frame)?
        } else {
            None
        };

        result.push(TimelineEntry {
            frame_id: frame.id,
            timestamp: frame.timestamp,
            preview,
            uri,
            child_frames,
            #[cfg(feature = "temporal_track")]
            temporal: temporal_info,
        });
    }
    Ok(result)
}

#[cfg(feature = "temporal_track")]
fn build_timeline_temporal_metadata(
    memvid: &mut Memvid,
    track: &TemporalTrack,
    frame: &crate::types::Frame,
) -> Result<Option<SearchHitTemporal>> {
    let mut temporal = SearchHitTemporal::default();

    if let Some(anchor) = track.anchor_for_frame(frame.id) {
        temporal.anchor = Some(SearchHitTemporalAnchor {
            ts_utc: anchor.anchor_ts,
            iso_8601: timestamp_to_rfc3339(anchor.anchor_ts),
            source: anchor.source,
        });
    }

    let mentions: Vec<_> = track
        .mentions
        .iter()
        .filter(|mention| mention.frame_id == frame.id)
        .collect();

    if !mentions.is_empty() {
        let canonical = memvid.frame_content(frame)?;
        let bytes = canonical.as_bytes();
        let mut collected = Vec::new();
        for mention in mentions.into_iter().take(8) {
            let start = mention.byte_start as usize;
            let end = start
                .saturating_add(mention.byte_len as usize)
                .min(bytes.len());
            if start >= end {
                continue;
            }
            let snippet = String::from_utf8_lossy(&bytes[start..end])
                .trim()
                .to_owned();
            collected.push(SearchHitTemporalMention {
                ts_utc: mention.ts_utc,
                iso_8601: timestamp_to_rfc3339(mention.ts_utc),
                kind: mention.kind,
                confidence: mention.confidence,
                flags: mention.flags,
                text: if snippet.is_empty() {
                    None
                } else {
                    Some(snippet)
                },
                byte_start: mention.byte_start,
                byte_len: mention.byte_len,
            });
        }
        if !collected.is_empty() {
            temporal.mentions = collected;
        }
    }

    if temporal.anchor.is_some() || !temporal.mentions.is_empty() {
        Ok(Some(temporal))
    } else {
        Ok(None)
    }
}

#[cfg(feature = "temporal_track")]
fn timestamp_to_rfc3339(ts: i64) -> Option<String> {
    OffsetDateTime::from_unix_timestamp(ts)
        .ok()
        .and_then(|dt| dt.format(&Rfc3339).ok())
}
