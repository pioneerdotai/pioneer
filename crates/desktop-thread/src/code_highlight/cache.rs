use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use sha2::{Digest, Sha256};

use super::{
    CanonicalLanguage, CodeThemeId, HIGHLIGHT_ENGINE_REVISION, HighlightError,
    HighlightFallbackReason, HighlightKey, HighlightLimits, HighlightOutcome, HighlightSpan,
    HighlightedCode, normalize_language_hint,
};

const DEFAULT_MAX_ENTRIES: usize = 128;
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_ACTIVE_JOBS: usize = 4;

#[derive(Clone, Debug)]
pub(crate) struct CodeHighlightJob {
    pub(crate) key: HighlightKey,
    pub(crate) generation: u64,
    pub(crate) source: Arc<str>,
    pub(crate) language_hint: Option<String>,
    pub(crate) theme: CodeThemeId,
    pub(crate) limits: HighlightLimits,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CodeHighlightLookup {
    Ready(Arc<HighlightedCode>),
    Fallback(HighlightFallbackReason),
    Pending,
    Unavailable,
}

#[derive(Debug)]
pub(crate) struct CodeHighlightRequest {
    pub(crate) lookup: CodeHighlightLookup,
    pub(crate) jobs: Vec<CodeHighlightJob>,
    pub(crate) observe_immediate_fallback: bool,
    pub(crate) observe_cache_hit: bool,
}

#[derive(Debug)]
pub(crate) struct CodeHighlightCompletion {
    pub(crate) accepted: bool,
    pub(crate) visible_output_changed: bool,
    pub(crate) jobs: Vec<CodeHighlightJob>,
}

#[derive(Clone, Debug)]
enum DesktopCodeHighlightCacheEntry {
    Pending { generation: u64 },
    Ready(Arc<HighlightedCode>),
    Fallback(HighlightFallbackReason),
}

#[derive(Clone, Debug)]
struct CacheRecord {
    entry: DesktopCodeHighlightCacheEntry,
    last_used: u64,
    payload_bytes: usize,
    hit_observed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ImmediateFallbackKind {
    Empty,
    SourceTooLarge,
    CacheCapacity,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ImmediateFallbackObservationKey {
    kind: ImmediateFallbackKind,
    canonical_language: String,
    theme: CodeThemeId,
    source_size_bucket: u8,
}

pub(crate) struct DesktopCodeHighlightCache {
    entries: HashMap<HighlightKey, CacheRecord>,
    queued_jobs: VecDeque<CodeHighlightJob>,
    active_generations: HashSet<u64>,
    immediate_fallback_observations: HashSet<ImmediateFallbackObservationKey>,
    next_generation: u64,
    usage_clock: u64,
    payload_bytes: usize,
    max_entries: usize,
    max_payload_bytes: usize,
    max_active_jobs: usize,
}

impl Default for DesktopCodeHighlightCache {
    fn default() -> Self {
        Self::with_limits(
            DEFAULT_MAX_ENTRIES,
            DEFAULT_MAX_PAYLOAD_BYTES,
            DEFAULT_MAX_ACTIVE_JOBS,
        )
    }
}

impl DesktopCodeHighlightCache {
    fn with_limits(max_entries: usize, max_payload_bytes: usize, max_active_jobs: usize) -> Self {
        Self {
            entries: HashMap::new(),
            queued_jobs: VecDeque::new(),
            active_generations: HashSet::new(),
            immediate_fallback_observations: HashSet::new(),
            next_generation: 1,
            usage_clock: 0,
            payload_bytes: 0,
            max_entries,
            max_payload_bytes,
            max_active_jobs,
        }
    }

    pub(crate) fn request(
        &mut self,
        source: &str,
        language_hint: Option<&str>,
        theme: CodeThemeId,
        limits: HighlightLimits,
    ) -> CodeHighlightRequest {
        let language = normalize_language_hint(language_hint);
        if source.is_empty() {
            let observe_immediate_fallback = self.observe_immediate_fallback_once(
                ImmediateFallbackKind::Empty,
                language,
                theme,
                source.len(),
            );
            return fallback_request(HighlightFallbackReason::Empty, observe_immediate_fallback);
        }
        if source.len() > limits.max_source_bytes {
            let observe_immediate_fallback = self.observe_immediate_fallback_once(
                ImmediateFallbackKind::SourceTooLarge,
                language,
                theme,
                source.len(),
            );
            return fallback_request(
                HighlightFallbackReason::SourceTooLarge,
                observe_immediate_fallback,
            );
        }
        let key = make_highlight_key(source, language, theme);
        self.usage_clock = self.usage_clock.wrapping_add(1);

        if self.entries.contains_key(&key) {
            let (lookup, observe_cache_hit, queued_generation) = {
                let record = self.entries.get_mut(&key).expect("entry checked above");
                record.last_used = self.usage_clock;
                let observe_cache_hit = !record.hit_observed
                    && !matches!(record.entry, DesktopCodeHighlightCacheEntry::Pending { .. });
                record.hit_observed |= observe_cache_hit;
                let queued_generation = match &record.entry {
                    DesktopCodeHighlightCacheEntry::Pending { generation }
                        if !self.active_generations.contains(generation) =>
                    {
                        Some(*generation)
                    }
                    _ => None,
                };
                (
                    lookup_for_entry(&record.entry),
                    observe_cache_hit,
                    queued_generation,
                )
            };
            if let Some(generation) = queued_generation {
                self.promote_queued_job(generation);
            }
            return CodeHighlightRequest {
                lookup,
                jobs: Vec::new(),
                observe_immediate_fallback: false,
                observe_cache_hit,
            };
        }

        if !self.ensure_entry_capacity(None) {
            let observe_immediate_fallback = self.observe_immediate_fallback_once(
                ImmediateFallbackKind::CacheCapacity,
                language,
                theme,
                source.len(),
            );
            return CodeHighlightRequest {
                lookup: CodeHighlightLookup::Unavailable,
                jobs: Vec::new(),
                observe_immediate_fallback,
                observe_cache_hit: false,
            };
        }

        if let Some(reason) = immediate_fallback(language) {
            self.entries.insert(
                key,
                CacheRecord {
                    entry: DesktopCodeHighlightCacheEntry::Fallback(reason),
                    last_used: self.usage_clock,
                    payload_bytes: 0,
                    hit_observed: false,
                },
            );
            return CodeHighlightRequest {
                lookup: CodeHighlightLookup::Fallback(reason),
                jobs: Vec::new(),
                observe_immediate_fallback: true,
                observe_cache_hit: false,
            };
        }

        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        let job = CodeHighlightJob {
            key: key.clone(),
            generation,
            source: Arc::from(source),
            language_hint: Some(language.cache_name().to_owned()),
            theme,
            limits,
        };
        self.entries.insert(
            key,
            CacheRecord {
                entry: DesktopCodeHighlightCacheEntry::Pending { generation },
                last_used: self.usage_clock,
                payload_bytes: 0,
                hit_observed: false,
            },
        );

        let jobs = if self.active_generations.len() < self.max_active_jobs {
            self.active_generations.insert(generation);
            vec![job]
        } else {
            self.queued_jobs.push_front(job);
            Vec::new()
        };
        CodeHighlightRequest {
            lookup: CodeHighlightLookup::Pending,
            jobs,
            observe_immediate_fallback: false,
            observe_cache_hit: false,
        }
    }

    pub(crate) fn complete(
        &mut self,
        key: &HighlightKey,
        generation: u64,
        result: Result<HighlightOutcome, HighlightError>,
    ) -> CodeHighlightCompletion {
        if !self.active_generations.remove(&generation) {
            return CodeHighlightCompletion {
                accepted: false,
                visible_output_changed: false,
                jobs: Vec::new(),
            };
        }

        let matching_pending = self.entries.get(key).is_some_and(|record| {
            matches!(
                record.entry,
                DesktopCodeHighlightCacheEntry::Pending {
                    generation: pending_generation
                } if pending_generation == generation
            )
        });
        let mut visible_output_changed = false;
        if matching_pending {
            self.usage_clock = self.usage_clock.wrapping_add(1);
            let (entry, payload_bytes) = match result {
                Ok(HighlightOutcome::Highlighted(code)) if code.key == *key => {
                    let payload_bytes = estimated_payload_bytes(&code);
                    visible_output_changed = !code.spans.is_empty();
                    (
                        DesktopCodeHighlightCacheEntry::Ready(Arc::new(code)),
                        payload_bytes,
                    )
                }
                Ok(HighlightOutcome::Fallback(reason)) => {
                    (DesktopCodeHighlightCacheEntry::Fallback(reason), 0)
                }
                Ok(HighlightOutcome::Highlighted(_)) => (
                    DesktopCodeHighlightCacheEntry::Fallback(HighlightFallbackReason::ParserError),
                    0,
                ),
                Err(_) => (
                    DesktopCodeHighlightCacheEntry::Fallback(HighlightFallbackReason::ParserError),
                    0,
                ),
            };
            if let Some(record) = self.entries.get_mut(key) {
                record.entry = entry;
                record.last_used = self.usage_clock;
                record.payload_bytes = payload_bytes;
                self.payload_bytes = self.payload_bytes.saturating_add(payload_bytes);
            }
            self.evict_to_limits(Some(key));
            if self.payload_bytes > self.max_payload_bytes
                && let Some(record) = self.entries.get_mut(key)
            {
                self.payload_bytes = self.payload_bytes.saturating_sub(record.payload_bytes);
                record.entry =
                    DesktopCodeHighlightCacheEntry::Fallback(HighlightFallbackReason::SpanLimit);
                record.payload_bytes = 0;
                visible_output_changed = false;
            }
        }

        CodeHighlightCompletion {
            accepted: matching_pending,
            visible_output_changed,
            jobs: self.start_queued_jobs(),
        }
    }

    fn start_queued_jobs(&mut self) -> Vec<CodeHighlightJob> {
        let mut jobs = Vec::new();
        while self.active_generations.len() < self.max_active_jobs {
            let Some(job) = self.queued_jobs.pop_front() else {
                break;
            };
            let still_pending = self.entries.get(&job.key).is_some_and(|record| {
                matches!(
                    record.entry,
                    DesktopCodeHighlightCacheEntry::Pending { generation }
                        if generation == job.generation
                )
            });
            if still_pending {
                self.active_generations.insert(job.generation);
                jobs.push(job);
            }
        }
        jobs
    }

    fn promote_queued_job(&mut self, generation: u64) {
        let Some(position) = self
            .queued_jobs
            .iter()
            .position(|job| job.generation == generation)
        else {
            return;
        };
        if let Some(job) = self.queued_jobs.remove(position) {
            self.queued_jobs.push_front(job);
        }
    }

    fn observe_immediate_fallback_once(
        &mut self,
        kind: ImmediateFallbackKind,
        language: CanonicalLanguage,
        theme: CodeThemeId,
        source_bytes: usize,
    ) -> bool {
        let key = ImmediateFallbackObservationKey {
            kind,
            canonical_language: language.cache_name().to_owned(),
            theme,
            source_size_bucket: source_size_bucket(source_bytes),
        };
        if self.immediate_fallback_observations.contains(&key)
            || self.immediate_fallback_observations.len() >= self.max_entries
        {
            return false;
        }
        self.immediate_fallback_observations.insert(key)
    }

    fn ensure_entry_capacity(&mut self, protected: Option<&HighlightKey>) -> bool {
        while self.entries.len() >= self.max_entries {
            if !self.evict_one(protected) {
                return false;
            }
        }
        true
    }

    fn evict_to_limits(&mut self, protected: Option<&HighlightKey>) {
        while self.entries.len() > self.max_entries || self.payload_bytes > self.max_payload_bytes {
            if !self.evict_one(protected) {
                break;
            }
        }
    }

    fn evict_one(&mut self, protected: Option<&HighlightKey>) -> bool {
        let candidate = self
            .entries
            .iter()
            .filter(|(key, record)| {
                protected != Some(*key)
                    && !matches!(record.entry, DesktopCodeHighlightCacheEntry::Pending { .. })
            })
            .min_by_key(|(_, record)| record.last_used)
            .map(|(key, _)| key.clone());
        let Some(candidate) = candidate else {
            return false;
        };
        if let Some(record) = self.entries.remove(&candidate) {
            self.payload_bytes = self.payload_bytes.saturating_sub(record.payload_bytes);
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn test_cache(max_entries: usize, max_payload_bytes: usize) -> Self {
        Self::with_limits(max_entries, max_payload_bytes, DEFAULT_MAX_ACTIVE_JOBS)
    }

    #[cfg(test)]
    pub(crate) fn test_entry_count(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(crate) fn test_payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    #[cfg(test)]
    pub(crate) fn test_contains(&self, key: &HighlightKey) -> bool {
        self.entries.contains_key(key)
    }
}

pub(crate) fn make_highlight_key(
    source: &str,
    language: CanonicalLanguage,
    theme: CodeThemeId,
) -> HighlightKey {
    HighlightKey {
        source_sha256: Sha256::digest(source.as_bytes()).into(),
        canonical_language: language.cache_name().to_owned(),
        theme,
        engine_revision: HIGHLIGHT_ENGINE_REVISION,
    }
}

pub(crate) fn estimated_payload_bytes(code: &HighlightedCode) -> usize {
    code.source_bytes
        .saturating_add(
            code.spans
                .len()
                .saturating_mul(std::mem::size_of::<HighlightSpan>()),
        )
        .saturating_add(code.resolved_language.as_ref().map_or(0, String::len))
}

fn lookup_for_entry(entry: &DesktopCodeHighlightCacheEntry) -> CodeHighlightLookup {
    match entry {
        DesktopCodeHighlightCacheEntry::Pending { .. } => CodeHighlightLookup::Pending,
        DesktopCodeHighlightCacheEntry::Ready(code) => CodeHighlightLookup::Ready(code.clone()),
        DesktopCodeHighlightCacheEntry::Fallback(reason) => CodeHighlightLookup::Fallback(*reason),
    }
}

fn immediate_fallback(language: CanonicalLanguage) -> Option<HighlightFallbackReason> {
    match language {
        CanonicalLanguage::Plaintext => Some(HighlightFallbackReason::Plaintext),
        CanonicalLanguage::Unknown => Some(HighlightFallbackReason::UnknownLanguage),
        CanonicalLanguage::Known(_) => None,
    }
}

fn fallback_request(
    reason: HighlightFallbackReason,
    observe_immediate_fallback: bool,
) -> CodeHighlightRequest {
    CodeHighlightRequest {
        lookup: CodeHighlightLookup::Fallback(reason),
        jobs: Vec::new(),
        observe_immediate_fallback,
        observe_cache_hit: false,
    }
}

fn source_size_bucket(source_bytes: usize) -> u8 {
    match source_bytes {
        0 => 0,
        1..=4_096 => 1,
        4_097..=32_768 => 2,
        32_769..=262_144 => 3,
        _ => 4,
    }
}
