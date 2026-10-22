use crate::Result;
use crate::io::time_index::read_track as time_index_read;
use crate::memvid::lifecycle::Memvid;

#[cfg(feature = "temporal_track")]
use crate::MemvidError;
#[cfg(feature = "temporal_track")]
use crate::analysis::temporal::{
    TemporalContext, TemporalNormalizer, TemporalResolution, TemporalResolutionValue,
};
#[cfg(feature = "temporal_track")]
use crate::types::{TemporalFilter, TemporalMention, TemporalMentionKind};
#[cfg(feature = "temporal_track")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "temporal_track")]
use time::{Date, OffsetDateTime, Time, UtcOffset};

pub(super) fn frame_ids_in_date_range(
    memvid: &mut Memvid,
    range: &crate::search::DateRange,
) -> Result<Option<Vec<u64>>> {
    if range.is_empty() {
        return Ok(Some(Vec::new()));
    }
    #[cfg(feature = "temporal_track")]
    if let Some(track) = memvid.temporal_track_ref()? {
        if track.capabilities().has_anchors {
            let ids: Vec<u64> = track
                .anchors
                .iter()
                .filter(|anchor| range.contains(anchor.anchor_ts))
                .map(|anchor| anchor.frame_id)
                .collect();
            if !ids.is_empty() {
                return Ok(Some(ids));
            }
        }
    }
    let manifest = match &memvid.toc.time_index {
        Some(manifest) => manifest.clone(),
        None => return Ok(None),
    };
    let entries = time_index_read(
        &mut memvid.file,
        manifest.bytes_offset,
        manifest.bytes_length,
    )?;
    let mut ids = Vec::new();
    for entry in entries {
        if range.contains(entry.timestamp) {
            ids.push(entry.frame_id);
        }
    }
    Ok(Some(ids))
}

#[cfg(feature = "temporal_track")]
pub fn frame_ids_for_temporal_filter(
    memvid: &mut Memvid,
    filter: &TemporalFilter,
) -> Result<Option<Vec<u64>>> {
    if filter.is_empty() {
        return Ok(None);
    }

    let Some(bounds) = resolve_temporal_bounds(filter)? else {
        return Ok(None);
    };

    if let (Some(start), Some(end)) = (bounds.start, bounds.end) {
        if start > end {
            return Ok(Some(Vec::new()));
        }
    }

    if let Some(track) = memvid.temporal_track_ref()? {
        let capabilities = track.capabilities();
        if capabilities.has_mentions {
            let ids = frame_ids_from_mentions(&track.mentions, &bounds);
            if !ids.is_empty() || !capabilities.has_anchors {
                return Ok(Some(ids));
            }
        }
        if capabilities.has_anchors {
            let ids = frame_ids_from_anchors(&track.anchors, &bounds);
            return Ok(Some(ids));
        }
    }

    let fallback = crate::search::DateRange {
        start: bounds.start,
        end: bounds.end,
    };
    frame_ids_in_date_range(memvid, &fallback)
}

#[cfg(feature = "temporal_track")]
#[derive(Clone, Copy, Debug)]
struct Bounds {
    start: Option<i64>,
    end: Option<i64>,
}

#[cfg(feature = "temporal_track")]
impl Bounds {
    fn new(start: Option<i64>, end: Option<i64>) -> Self {
        Self { start, end }
    }

    fn intersect(self, other: Bounds) -> Option<Bounds> {
        let start = match (self.start, other.start) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let end = match (self.end, other.end) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        if let (Some(s), Some(e)) = (start, end) {
            if s > e {
                return None;
            }
        }
        Some(Bounds { start, end })
    }

    fn contains(&self, ts: i64) -> bool {
        if let Some(start) = self.start {
            if ts < start {
                return false;
            }
        }
        if let Some(end) = self.end {
            if ts > end {
                return false;
            }
        }
        true
    }

    fn overlaps(&self, start: i64, end: i64) -> bool {
        let range_start = start.min(end);
        let range_end = start.max(end);
        let window_start = self.start.unwrap_or(i64::MIN);
        let window_end = self.end.unwrap_or(i64::MAX);
        range_end >= window_start && range_start <= window_end
    }
}

#[cfg(feature = "temporal_track")]
fn resolve_temporal_bounds(filter: &TemporalFilter) -> Result<Option<Bounds>> {
    let mut bounds = if filter.start_utc.is_some() || filter.end_utc.is_some() {
        Some(Bounds::new(filter.start_utc, filter.end_utc))
    } else {
        None
    };

    if let Some(phrase_bounds) = resolve_phrase_bounds(filter)? {
        bounds = match bounds {
            Some(existing) => existing.intersect(phrase_bounds),
            None => Some(phrase_bounds),
        };
        if bounds.is_none() {
            return Ok(Some(Bounds::new(Some(1), Some(0))));
        }
    }

    Ok(bounds)
}

#[cfg(feature = "temporal_track")]
fn resolve_phrase_bounds(filter: &TemporalFilter) -> Result<Option<Bounds>> {
    let phrase = match filter
        .phrase
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        Some(value) => value,
        None => return Ok(None),
    };

    let offset = parse_utc_offset(filter.tz.as_deref())?;
    let anchor = OffsetDateTime::now_utc().to_offset(offset);
    let context = TemporalContext::new(anchor, filter.tz.clone().unwrap_or_else(|| "UTC".into()));
    let normalizer = TemporalNormalizer::new(context);
    let resolution = normalizer.resolve(phrase)?;
    let bounds = resolution_to_bounds(&resolution)?;
    Ok(Some(bounds))
}

#[cfg(feature = "temporal_track")]
fn resolution_to_bounds(resolution: &TemporalResolution) -> Result<Bounds> {
    match &resolution.value {
        TemporalResolutionValue::Date(date) => {
            let ts = date_to_timestamp(*date);
            Ok(Bounds::new(Some(ts), Some(ts)))
        }
        TemporalResolutionValue::DateTime(dt) => {
            let ts = dt.unix_timestamp();
            Ok(Bounds::new(Some(ts), Some(ts)))
        }
        TemporalResolutionValue::DateRange { start, end } => {
            let start_ts = date_to_timestamp(*start);
            let end_ts = date_to_timestamp(*end);
            Ok(Bounds::new(Some(start_ts), Some(end_ts)))
        }
        TemporalResolutionValue::DateTimeRange { start, end } => Ok(Bounds::new(
            Some(start.unix_timestamp()),
            Some(end.unix_timestamp()),
        )),
        TemporalResolutionValue::Month { year, month } => {
            let start_date = Date::from_calendar_date(*year, *month, 1).map_err(|_| {
                MemvidError::InvalidQuery {
                    reason: "invalid month in temporal resolution".into(),
                }
            })?;
            let end_date = last_day_in_month(*year, *month);
            Ok(Bounds::new(
                Some(date_to_timestamp(start_date)),
                Some(date_to_timestamp(end_date)),
            ))
        }
    }
}

#[cfg(feature = "temporal_track")]
fn parse_utc_offset(spec: Option<&str>) -> Result<UtcOffset> {
    let Some(value) = spec.filter(|s| !s.trim().is_empty()) else {
        return Ok(UtcOffset::UTC);
    };
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("utc") || trimmed.eq_ignore_ascii_case("z") {
        return Ok(UtcOffset::UTC);
    }
    if let Some(stripped) = trimmed
        .strip_prefix('+')
        .or_else(|| trimmed.strip_prefix('-'))
    {
        let sign = if trimmed.starts_with('-') { -1 } else { 1 };
        let digits: String = stripped.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() == 2 || digits.len() == 4 {
            let hours: i32 = digits[0..2]
                .parse()
                .map_err(|_| MemvidError::InvalidQuery {
                    reason: format!("invalid timezone offset: {value}"),
                })?;
            let minutes: i32 = if digits.len() == 4 {
                digits[2..4]
                    .parse()
                    .map_err(|_| MemvidError::InvalidQuery {
                        reason: format!("invalid timezone offset: {value}"),
                    })?
            } else {
                0
            };
            let total_minutes = sign * (hours * 60 + minutes);
            return UtcOffset::from_whole_seconds(total_minutes * 60).map_err(|_| {
                MemvidError::InvalidQuery {
                    reason: format!("invalid timezone offset: {value}"),
                }
            });
        }
    }
    Err(MemvidError::InvalidQuery {
        reason: format!("unsupported timezone specifier: {value}"),
    })
}

#[cfg(feature = "temporal_track")]
fn frame_ids_from_mentions(mentions: &[TemporalMention], bounds: &Bounds) -> Vec<u64> {
    if mentions.is_empty() {
        return Vec::new();
    }

    let mut frames = HashSet::new();
    let mut ranges: HashMap<(u64, u32, u32), Vec<i64>> = HashMap::new();

    for mention in mentions {
        let key = (mention.frame_id, mention.byte_start, mention.byte_len);
        match mention.kind {
            TemporalMentionKind::RangeStart => {
                ranges.entry(key).or_default().push(mention.ts_utc);
                if bounds.contains(mention.ts_utc) {
                    frames.insert(mention.frame_id);
                }
            }
            TemporalMentionKind::RangeEnd => {
                let mut matched = false;
                if let Some(starts) = ranges.get_mut(&key) {
                    if let Some(start_ts) = starts.pop() {
                        if bounds.overlaps(start_ts, mention.ts_utc) {
                            frames.insert(mention.frame_id);
                            matched = true;
                        }
                    }
                    if starts.is_empty() {
                        ranges.remove(&key);
                    }
                }
                if !matched && bounds.contains(mention.ts_utc) {
                    frames.insert(mention.frame_id);
                }
            }
            _ => {
                if bounds.contains(mention.ts_utc) {
                    frames.insert(mention.frame_id);
                }
            }
        }
    }

    frames.into_iter().collect()
}

#[cfg(feature = "temporal_track")]
fn frame_ids_from_anchors(anchors: &[crate::TemporalAnchor], bounds: &Bounds) -> Vec<u64> {
    anchors
        .iter()
        .filter(|anchor| bounds.contains(anchor.anchor_ts))
        .map(|anchor| anchor.frame_id)
        .collect()
}

#[cfg(feature = "temporal_track")]
fn date_to_timestamp(date: Date) -> i64 {
    time::PrimitiveDateTime::new(date, Time::MIDNIGHT)
        .assume_offset(UtcOffset::UTC)
        .unix_timestamp()
}

#[cfg(feature = "temporal_track")]
fn last_day_in_month(year: i32, month: time::Month) -> Date {
    let mut date = Date::from_calendar_date(year, month, 1).expect("valid month");
    while let Some(next) = date.next_day() {
        if next.month() == month {
            date = next;
        } else {
            break;
        }
    }
    date
}
