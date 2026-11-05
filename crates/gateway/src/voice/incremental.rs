//! Session-owned finalized audio chunks over the existing one-shot engines.
//! Pauses propose audio boundaries; each disjoint owned PCM range is decoded once.
#[cfg(test)]
use super::transcription::{VoiceTranscriptionErrorKind, transcription_error};
use super::{
    finalization::{VoiceFinalizationLease, VoiceFinalizations},
    session_buffer::{GatewayVoiceSessionBufferStore, VoiceChunkIngestError},
    silero::create_voice_detector,
    supervisor::VoiceInputSupervisor,
    transcription::{
        PreparedSpeechBuffer, VoiceSpeechHypothesis, VoiceSpeechTranscriber, VoiceTranscript,
        VoiceTranscriptionDiagnostics, VoiceTranscriptionError, VoiceTranscriptionNoSpeech,
        VoiceTranscriptionNoSpeechReason, VoiceTranscriptionStrategy,
    },
    vad::{
        SmoothedVadFrame, SmoothedVoiceVad, VOICE_VAD_FRAME_SAMPLES, VadInterval,
        VoiceActivityDetector, VoiceSpeechSegment, VoiceVadConfig,
    },
};
use pioneer_protocol::{
    VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ, VoiceAudioFormat, VoiceError, VoiceErrorKind,
};
use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

// One algorithm for every one-shot adapter. Durations count real prepared PCM,
// not source silence, packets or native-only padding (Handy initial defaults).
const RATE: usize = VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ as usize;
const TARGET_CHUNK_SAMPLES: usize = 30 * RATE;
const MAX_CHUNK_SAMPLES: usize = 40 * RATE;
const MIN_CHUNK_SAMPLES: usize = 5 * RATE;
const LOOKAHEAD_SAMPLES: usize = 5 * RATE;
const PLANNER_CAP_SAMPLES: usize = MAX_CHUNK_SAMPLES + LOOKAHEAD_SAMPLES;
// transcribe-rs 0.3.12 onnx/moonshine/model.rs::generate rejects audio shorter
// than 0.1 s at 16 kHz. This applies to Pioneer's moonshine-base adapter only;
// other engines keep their existing input and inference parameters.
const MIN_MOONSHINE_BASE_INPUT_SAMPLES: usize = VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ as usize / 10;

pub(crate) enum GatewayVoiceSessionPipelineOutcome {
    Transcript {
        transcript: VoiceTranscript,
        signal_stats: VoiceSignalStats,
    },
    NoSpeech {
        no_speech: VoiceTranscriptionNoSpeech,
        signal_stats: VoiceSignalStats,
    },
}

pub(crate) struct VoiceProcessingCompletion {
    pub(crate) outcome: Result<GatewayVoiceSessionPipelineOutcome, VoiceError>,
    // Transfer ownership through completion/publication. Cancel drops the receiver,
    // but an in-flight native call retains this lease until its actual return.
    pub(crate) lease: VoiceFinalizationLease,
}

pub(crate) struct SessionSpeechTranscriber {
    pub(crate) supervisor: Arc<VoiceInputSupervisor>,
    pub(crate) generation: u64,
    pub(crate) is_moonshine_base: bool,
    pub(crate) cancellation: CancellationToken,
    #[cfg(test)]
    pub(crate) fake: Option<super::transcription::TestVoiceSpeechTranscriber>,
}

impl VoiceSpeechTranscriber for SessionSpeechTranscriber {
    fn transcribe_speech(
        &self,
        buffer: &PreparedSpeechBuffer,
    ) -> Result<String, VoiceTranscriptionError> {
        self.transcribe_hypothesis(buffer).map(|result| result.text)
    }

    fn transcribe_hypothesis(
        &self,
        buffer: &PreparedSpeechBuffer,
    ) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError> {
        #[cfg(test)]
        if let Some(fake) = &self.fake {
            if self.cancellation.is_cancelled() || self.supervisor.generation() != self.generation {
                return Err(transcription_error(
                    VoiceTranscriptionErrorKind::ModelUnavailable,
                    "voice session model is no longer available",
                ));
            }
            return fake(buffer).map(VoiceSpeechHypothesis::text_only);
        }
        self.supervisor
            .transcribe_for_session(buffer, self.generation, &self.cancellation)
    }
}

pub(crate) fn start(
    store: GatewayVoiceSessionBufferStore,
    session_id: String,
    format: VoiceAudioFormat,
    workers: &VoiceFinalizations,
    lease: VoiceFinalizationLease,
    transcriber: SessionSpeechTranscriber,
    transcribe_while_recording: bool,
) -> Result<(), VoiceChunkIngestError> {
    #[cfg(test)]
    if transcriber.fake.is_some() {
        return start_with_detector(
            store,
            session_id,
            format,
            workers,
            lease,
            transcriber,
            transcribe_while_recording,
            |_| super::vad::EnergyVoiceActivityDetector::new(0.02),
        );
    }
    start_with_detector(
        store,
        session_id,
        format,
        workers,
        lease,
        transcriber,
        transcribe_while_recording,
        create_voice_detector,
    )
}

fn start_with_detector<D: VoiceActivityDetector + Send + 'static>(
    store: GatewayVoiceSessionBufferStore,
    session_id: String,
    format: VoiceAudioFormat,
    workers: &VoiceFinalizations,
    lease: VoiceFinalizationLease,
    transcriber: SessionSpeechTranscriber,
    transcribe_while_recording: bool,
    make_detector: impl FnOnce(&CancellationToken) -> anyhow::Result<D> + Send + 'static,
) -> Result<(), VoiceChunkIngestError> {
    let cancellation = transcriber.cancellation.clone();
    let (sender, receiver) = oneshot::channel();
    store.start_processing(session_id.clone(), format, cancellation.clone(), receiver)?;
    // Shutdown may close admission and clear buffers concurrently with start.
    // Never leave a post-clear completion (and its tracked lease) in the store.
    if cancellation.is_cancelled() {
        let _ = store.remove_session(&session_id);
        return Err(super::session_buffer::VoiceChunkIngestError {
            kind: super::session_buffer::VoiceChunkIngestErrorKind::UnknownSession,
            message: "voice processing is shutting down".to_owned(),
        });
    }
    // Exactly one blocking worker per admitted session. It waits on the existing
    // inbox without an engine lock, and is explicitly joined by the gateway's
    // tracker at shutdown. No task or second audio queue per network chunk.
    let store = store.worker_view();
    workers
        .tasks
        .spawn_blocking(pioneer_observability::work::blocking(move || {
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                ensure_active(&cancellation)?;
                // One Session per recording, built on this blocking worker,
                // never in ingress or under the shared session-store lock.
                let detector = make_detector(&cancellation)
                    .map_err(|_| worker_error("failed to initialize required Silero VAD"))?;
                ensure_active(&cancellation)?;
                let result = if transcribe_while_recording {
                    process(
                        &store,
                        &session_id,
                        &cancellation,
                        &transcriber,
                        transcriber.is_moonshine_base,
                        detector,
                    )
                } else {
                    process_buffered(
                        &store,
                        &session_id,
                        &cancellation,
                        &transcriber,
                        transcriber.is_moonshine_base,
                        detector,
                    )
                };
                // Completed fragments must also respect replacement generations,
                // even when Commit needs no additional native invocation.
                ensure_active(&cancellation)?;
                if transcriber.supervisor.generation() != transcriber.generation {
                    return Err(worker_error("voice session model is no longer available"));
                }
                result
            }))
            .unwrap_or_else(|_| Err(worker_error("voice processing worker panicked")));
            store.processing_finished(&session_id);
            let _ = sender.send(VoiceProcessingCompletion { outcome, lease });
        }));
    Ok(())
}

// Whole-session preparation uses the same VAD filtering and final-onset
// handling as incremental, but never invokes its audio chunk planner.
// All accepted chunks transfer out of the inbox only after Commit.
fn process_buffered<D: VoiceActivityDetector>(
    store: &GatewayVoiceSessionBufferStore,
    session_id: &str,
    cancellation: &CancellationToken,
    transcriber: &impl VoiceSpeechTranscriber,
    is_moonshine_base: bool,
    detector: D,
) -> Result<GatewayVoiceSessionPipelineOutcome, VoiceError> {
    ensure_active(cancellation)?;
    let chunks = store
        .take_closed_processing_chunks(session_id)
        .map_err(|error| error.into_voice_error())?;
    ensure_active(cancellation)?;
    let key = store.processing_startup_key(session_id);
    pioneer_observability::turn_startup::scope_sync(key, || {
        let mut vad = SmoothedVoiceVad::new(detector, VoiceVadConfig::default())
            .map_err(|_| worker_error("failed to initialize voice VAD"))?;
        let mut signal = SignalAccumulator::default();
        let mut pending = Vec::with_capacity(VOICE_VAD_FRAME_SAMPLES);
        // At most one extra normalized recording, bounded by the existing
        // lifetime ingress budget (raw + normalized). Reserve exactly once to
        // avoid geometric capacity growth; source chunks drop during the pass.
        let source_samples: usize = chunks
            .iter()
            .map(|chunk| chunk.normalized_samples.len())
            .sum();
        let mut source_map = PreparedSourceMap::default();
        let mut speech = Vec::with_capacity(source_samples.max(if is_moonshine_base {
            MIN_MOONSHINE_BASE_INPUT_SAMPLES
        } else {
            0
        }));
        {
            let _stage = pioneer_observability::turn_startup::current_stage(
                pioneer_observability::turn_startup::Stage::VoiceVad,
            );
            for chunk in chunks {
                ensure_active(cancellation)?;
                signal.add_samples(&chunk.normalized_samples);
                for sample in chunk.normalized_samples {
                    pending.push(sample);
                    if pending.len() == VOICE_VAD_FRAME_SAMPLES {
                        ensure_active(cancellation)?;
                        let classified = vad
                            .push_incremental_frame(&pending)
                            .map_err(|_| worker_error("failed to segment voice audio"))?;
                        ensure_active(cancellation)?;
                        if let SmoothedVadFrame::Speech(samples) = classified {
                            source_map.append(&vad.take_emitted_intervals());
                            speech.extend(samples);
                        }
                        pending.clear();
                    }
                }
            }
            ensure_active(cancellation)?;
            let tail = vad
                .finish_incremental_tail(&pending)
                .map_err(|_| worker_error("failed to segment final voice audio"))?;
            if !tail.is_empty() {
                source_map.append(&vad.take_emitted_intervals());
                speech.extend(tail);
            }
        }
        ensure_active(cancellation)?;
        let signal_stats = signal.finish();
        let speech_samples = source_map.real_samples;
        debug_assert_eq!(speech_samples, speech.len());
        if speech_samples == 0 {
            return Ok(GatewayVoiceSessionPipelineOutcome::NoSpeech {
                no_speech: VoiceTranscriptionNoSpeech {
                    reason: VoiceTranscriptionNoSpeechReason::NoSpeechDetected,
                    total_samples: signal_stats.total_samples,
                },
                signal_stats,
            });
        }
        if is_moonshine_base {
            prepare_moonshine_base_samples(&mut speech);
        }
        let native_samples = speech.len();
        // Keep the exact real-source mapping alive through this one call. The
        // range [speech_samples, native_samples) is synthetic native input only.
        debug_assert_eq!(
            source_map.source_at(speech_samples.saturating_sub(1)),
            source_map.spans.last().map(|span| span.source_end - 1)
        );
        let buffer = PreparedSpeechBuffer {
            sample_rate_hz: VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ,
            total_samples: signal_stats.total_samples,
            segments: vec![VoiceSpeechSegment {
                start_sample: 0,
                end_sample: native_samples,
                samples: speech,
            }],
        };
        // The session adapter checks generation and cancellation after waiting
        // for the engine lock and immediately before entering native inference.
        ensure_active(cancellation)?;
        let text = transcriber
            .transcribe_hypothesis(&buffer)
            .map_err(|error| error.into_voice_error())?
            .text
            .trim()
            .to_owned();
        ensure_active(cancellation)?;
        if text.is_empty() {
            return Ok(GatewayVoiceSessionPipelineOutcome::NoSpeech {
                no_speech: VoiceTranscriptionNoSpeech {
                    reason: VoiceTranscriptionNoSpeechReason::EmptyTranscript,
                    total_samples: signal_stats.total_samples,
                },
                signal_stats,
            });
        }
        Ok(GatewayVoiceSessionPipelineOutcome::Transcript {
            transcript: VoiceTranscript {
                text,
                diagnostics: VoiceTranscriptionDiagnostics {
                    sample_rate_hz: VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ,
                    total_samples: signal_stats.total_samples,
                    speech_samples,
                    segment_count: 1,
                    strategy: VoiceTranscriptionStrategy::BufferedGatewaySession,
                },
            },
            signal_stats,
        })
    })
}

fn process<D: VoiceActivityDetector>(
    store: &GatewayVoiceSessionBufferStore,
    session_id: &str,
    cancellation: &CancellationToken,
    transcriber: &impl VoiceSpeechTranscriber,
    is_moonshine_base: bool,
    detector: D,
) -> Result<GatewayVoiceSessionPipelineOutcome, VoiceError> {
    ensure_active(cancellation)?;
    let mut state = IncrementalSpeech::new(is_moonshine_base, detector)?;
    loop {
        ensure_active(cancellation)?;
        let Some(chunk) = store
            .next_processing_chunk(session_id)
            .map_err(|e| e.into_voice_error())?
        else {
            break;
        };
        state.signal.add_samples(&chunk.normalized_samples);
        for sample in chunk.normalized_samples {
            state.pending.push(sample);
            if state.pending.len() == VOICE_VAD_FRAME_SAMPLES {
                ensure_active(cancellation)?;
                let key = store.processing_startup_key(session_id);
                let classified = pioneer_observability::turn_startup::scope_sync(key, || {
                    let _stage = pioneer_observability::turn_startup::current_stage(
                        pioneer_observability::turn_startup::Stage::VoiceVad,
                    );
                    state.vad.push_incremental_frame(&state.pending)
                })
                .map_err(|_| worker_error("failed to segment voice audio"))?;
                ensure_active(cancellation)?;
                state.pending.clear();
                state.source_samples += VOICE_VAD_FRAME_SAMPLES;
                match classified {
                    SmoothedVadFrame::Speech(samples) => {
                        state.accept_speech(
                            &samples,
                            store,
                            session_id,
                            cancellation,
                            transcriber,
                        )?;
                    }
                    SmoothedVadFrame::Noise => {}
                }
            }
        }
    }
    ensure_active(cancellation)?;
    let key = store.processing_startup_key(session_id);
    let tail = pioneer_observability::turn_startup::scope_sync(key, || {
        let _stage = pioneer_observability::turn_startup::current_stage(
            pioneer_observability::turn_startup::Stage::VoiceVad,
        );
        state.vad.finish_incremental_tail(&state.pending)
    })
    .map_err(|_| worker_error("failed to segment final voice audio"))?;
    ensure_active(cancellation)?;
    state.source_samples += state.pending.len();
    if !tail.is_empty() {
        state.accept_speech(&tail, store, session_id, cancellation, transcriber)?;
    }
    state.finish(store, session_id, cancellation, transcriber)?;
    let signal_stats = state.signal.finish();
    if state.text.is_empty() {
        return Ok(GatewayVoiceSessionPipelineOutcome::NoSpeech {
            no_speech: VoiceTranscriptionNoSpeech {
                reason: if state.speech_samples == 0 {
                    VoiceTranscriptionNoSpeechReason::NoSpeechDetected
                } else {
                    VoiceTranscriptionNoSpeechReason::EmptyTranscript
                },
                total_samples: signal_stats.total_samples,
            },
            signal_stats,
        });
    }
    Ok(GatewayVoiceSessionPipelineOutcome::Transcript {
        transcript: VoiceTranscript {
            text: state.text,
            diagnostics: VoiceTranscriptionDiagnostics {
                sample_rate_hz: VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ,
                total_samples: signal_stats.total_samples,
                speech_samples: state.speech_samples,
                segment_count: state.fragments,
                strategy: VoiceTranscriptionStrategy::IncrementalGatewaySession,
            },
        },
        signal_stats,
    })
}

// Buffered mode merges adjacent source spans through its single ASR call.
#[derive(Clone, Copy)]
struct PreparedSourceSpan {
    prepared: usize,
    source_start: usize,
    source_end: usize,
}

#[derive(Default)]
struct PreparedSourceMap {
    spans: Vec<PreparedSourceSpan>,
    real_samples: usize,
}

impl PreparedSourceMap {
    fn append(&mut self, intervals: &[VadInterval]) {
        for interval in intervals {
            if let Some(last) = self
                .spans
                .last_mut()
                .filter(|last| last.source_end == interval.start)
            {
                last.source_end = interval.end;
            } else {
                self.spans.push(PreparedSourceSpan {
                    prepared: self.real_samples,
                    source_start: interval.start,
                    source_end: interval.end,
                });
            }
            self.real_samples += interval.end - interval.start;
        }
    }

    fn source_at(&self, prepared: usize) -> Option<usize> {
        self.spans.iter().find_map(|span| {
            let offset = prepared.checked_sub(span.prepared)?;
            (offset < span.source_end - span.source_start).then_some(span.source_start + offset)
        })
    }
}

// Mapping is private preparation metadata. Coordinates in `prepared` count
// unique real ASR PCM, before native padding; source counts ALL input.
// Only intervals supporting the bounded pending prepared PCM are retained.
#[derive(Clone, Copy, Debug)]
struct PreparedInterval {
    prepared: usize,
    source: VadInterval,
}

#[derive(Default)]
struct AudioTimeline {
    intervals: VecDeque<PreparedInterval>,
}

impl AudioTimeline {
    fn append(&mut self, prepared: usize, start: usize, end: usize, intervals: &[VadInterval]) {
        for interval in intervals {
            let first = start.max(interval.start);
            let last = end.min(interval.end);
            if first < last {
                self.intervals.push_back(PreparedInterval {
                    prepared: prepared + first - start,
                    source: VadInterval {
                        start: first,
                        end: last,
                        ..*interval
                    },
                });
            }
        }
    }

    fn retain_from(&mut self, prepared: usize) {
        while self.intervals.front().is_some_and(|interval| {
            interval.prepared + interval.source.end - interval.source.start <= prepared
        }) {
            self.intervals.pop_front();
        }
        if let Some(interval) = self.intervals.front_mut() {
            if interval.prepared < prepared {
                interval.source.start += prepared - interval.prepared;
                interval.prepared = prepared;
            }
        }
    }

    // Exact sample mapping. Deleted source silence and native padding have no
    // entry. At a concatenation seam the next real sample owns the coordinate.
    fn source_at(&self, prepared: usize) -> Option<usize> {
        self.intervals.iter().find_map(|interval| {
            let offset = prepared.checked_sub(interval.prepared)?;
            (offset < interval.source.end - interval.source.start)
                .then_some(interval.source.start + offset)
        })
    }

    fn prepared_at(&self, source: usize) -> Option<usize> {
        self.intervals.iter().find_map(|interval| {
            (source >= interval.source.start && source < interval.source.end)
                .then(|| interval.prepared + source - interval.source.start)
        })
    }

    // Closed quiet runs only during push: an open pause has no stable midpoint.
    // At finish its real source end is known. Gaps removed by VAD map to the
    // concatenation seam, never to invented PCM. quiet_run_start distinguishes
    // pauses separated by positive frames that VAD did not emit.
    fn candidates(&self, finish: Option<(usize, usize)>) -> Vec<(usize, usize)> {
        let mut result = Vec::new();
        let mut quiet: Option<(usize, usize)> = None; // source start, retained end
        let mut close = |start: usize, end: usize| {
            let middle = start + (end - start) / 2;
            let cut = self
                .prepared_at(middle)
                .or_else(|| {
                    self.intervals
                        .iter()
                        .find(|i| i.source.start >= middle)
                        .map(|i| i.prepared)
                })
                .unwrap_or_else(|| {
                    self.intervals
                        .back()
                        .map_or(0, |i| i.prepared + i.source.end - i.source.start)
                });
            result.push((cut, end - start));
        };
        for interval in &self.intervals {
            let source = interval.source;
            if quiet.is_some_and(|(start, _)| source.voice || start != source.quiet_run_start) {
                let (start, end) = quiet.take().unwrap();
                // Only VAD-proven quiet contributes duration; a source gap may
                // also hide a discarded positive onset frame.
                close(start, end);
            }
            if !source.voice {
                quiet = Some((source.quiet_run_start, source.end));
            }
        }
        if let (Some((start, retained_end)), Some((last_quiet_start, end))) = (quiet, finish) {
            // Full-input quiet identity prevents extending through a positive
            // frame that was subsequently evicted from VAD prefill.
            close(
                start,
                if start == last_quiet_start {
                    end
                } else {
                    retained_end
                },
            );
        }
        result
    }
}

struct IncrementalSpeech<D> {
    is_moonshine_base: bool,
    vad: SmoothedVoiceVad<D>,
    pending: Vec<f32>,
    speech: Vec<f32>,
    timeline: AudioTimeline,
    chunk_start_sample: usize,
    speech_samples: usize,
    fragments: usize,
    text: String,
    signal: SignalAccumulator,
    source_samples: usize,
}

impl<D: VoiceActivityDetector> IncrementalSpeech<D> {
    fn new(is_moonshine_base: bool, detector: D) -> Result<Self, VoiceError> {
        Ok(Self {
            is_moonshine_base,
            vad: SmoothedVoiceVad::new(detector, VoiceVadConfig::default())
                .map_err(|_| worker_error("failed to initialize voice VAD"))?,
            pending: Vec::with_capacity(VOICE_VAD_FRAME_SAMPLES),
            speech: Vec::with_capacity(PLANNER_CAP_SAMPLES),
            timeline: AudioTimeline::default(),
            chunk_start_sample: 0,
            speech_samples: 0,
            fragments: 0,
            text: String::new(),
            signal: SignalAccumulator::default(),
            source_samples: 0,
        })
    }

    fn accept_speech(
        &mut self,
        samples: &[f32],
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        let intervals = self.vad.take_emitted_intervals();
        self.push(samples, &intervals, store, id, cancellation, transcriber)
    }

    fn push(
        &mut self,
        mut samples: &[f32],
        intervals: &[VadInterval],
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        // Split metadata and PCM together, even if a VAD prefill or caller block
        // straddles the cap. Planning observes exactly the same horizon for any
        // packetization, and never appends an oversized block before cutting.
        for interval in intervals {
            let mut source = interval.start;
            while source < interval.end {
                ensure_active(cancellation)?;
                let take = (interval.end - source).min(PLANNER_CAP_SAMPLES - self.speech.len());
                self.timeline
                    .append(self.speech_samples, source, source + take, &[*interval]);
                self.speech.extend_from_slice(&samples[..take]);
                samples = &samples[take..];
                self.speech_samples += take;
                source += take;
                if self.speech.len() == PLANNER_CAP_SAMPLES {
                    let cut = self.plan_cut(false);
                    self.decode_until(cut, store, id, cancellation, transcriber)?;
                }
            }
        }
        debug_assert!(samples.is_empty());
        debug_assert_eq!(
            self.chunk_start_sample + self.speech.len(),
            self.speech_samples
        );
        Ok(())
    }

    // Handy plan_cuts: favor long pauses near 30 s and a useful >=5 s rest;
    // without a pause split at 40 s, or halfway to avoid a tiny final rest.
    fn plan_cut(&self, finish: bool) -> usize {
        let len = self.speech.len();
        let mut best: Option<(usize, f64)> = None;
        let mut fallback: Option<(usize, f64)> = None;
        for (absolute, gap) in self.timeline.candidates(finish.then_some((
            self.source_samples - self.vad.quiet_samples(),
            self.source_samples,
        ))) {
            let Some(cut) = absolute.checked_sub(self.chunk_start_sample) else {
                continue;
            };
            if !(MIN_CHUNK_SAMPLES..=MAX_CHUNK_SAMPLES).contains(&cut) || cut > len {
                continue;
            }
            let position = cut as f64 / RATE as f64;
            let target = TARGET_CHUNK_SAMPLES as f64 / RATE as f64;
            let penalty = if position > target {
                0.1 * (position - target)
            } else {
                0.05 * (target - position)
            };
            let score = (gap as f64 / RATE as f64).min(1.0) - penalty;
            if len - cut >= MIN_CHUNK_SAMPLES && best.is_none_or(|(_, prior)| score > prior) {
                best = Some((cut, score));
            }
            if fallback.is_none_or(|(_, prior)| score > prior) {
                fallback = Some((cut, score));
            }
        }
        best.or(fallback).map_or_else(
            || {
                if len < MAX_CHUNK_SAMPLES + MIN_CHUNK_SAMPLES {
                    len / 2
                } else {
                    MAX_CHUNK_SAMPLES
                }
            },
            |(cut, _)| cut,
        )
    }

    fn finish(
        &mut self,
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        while self.speech.len() > MAX_CHUNK_SAMPLES {
            self.decode_until(self.plan_cut(true), store, id, cancellation, transcriber)?;
        }
        if !self.speech.is_empty() {
            self.decode_until(self.speech.len(), store, id, cancellation, transcriber)?;
        }
        Ok(())
    }

    fn decode_until(
        &mut self,
        count: usize,
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        ensure_active(cancellation)?;
        debug_assert!(count > 0 && count <= MAX_CHUNK_SAMPLES);
        debug_assert!(self.timeline.source_at(self.chunk_start_sample).is_some());
        let mut rest = self.speech.split_off(count);
        rest.reserve_exact(PLANNER_CAP_SAMPLES - rest.len());
        let mut samples = std::mem::replace(&mut self.speech, rest);
        if self.is_moonshine_base {
            prepare_moonshine_base_samples(&mut samples);
        }
        debug_assert!(samples.len() <= MAX_CHUNK_SAMPLES);
        let native_count = samples.len();
        let buffer = PreparedSpeechBuffer {
            sample_rate_hz: VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ,
            total_samples: count, // real prepared samples; padding is native-only
            segments: vec![VoiceSpeechSegment {
                start_sample: 0,
                end_sample: native_count,
                samples,
            }],
        };
        let key = store.processing_startup_key(id);
        let result = pioneer_observability::turn_startup::scope_sync(key, || {
            transcriber.transcribe_hypothesis(&buffer)
        })
        .map_err(|error| error.into_voice_error())?;
        ensure_active(cancellation)?;
        append_fragment(&mut self.text, &result.text);
        self.fragments += 1;
        self.chunk_start_sample += count;
        self.timeline.retain_from(self.chunk_start_sample);
        Ok(())
    }
}

// Preserve repetitions and recognizer punctuation. A technical cut is not a
// sentence boundary. Reuse the existing Unicode script helper, without the
// removed text-alignment/projection machinery.
fn append_fragment(joined: &mut String, text: &str) {
    let text = text.trim();
    let Some(first) = text.chars().next() else {
        return;
    };
    if let Some(last) = joined.chars().last() {
        if !is_unspaced_character(last)
            && !is_unspaced_character(first)
            && !matches!(
                first,
                '.' | ',' | '!' | '?' | ':' | ';' | ')' | ']' | '}' | '…'
            )
        {
            joined.push(' ');
        }
    }
    joined.push_str(text);
}

fn is_unspaced_character(c: char) -> bool {
    matches!(c,
        '\u{3400}'..='\u{4dbf}' | '\u{4e00}'..='\u{9fff}' |
        '\u{f900}'..='\u{faff}' | '\u{20000}'..='\u{323af}' |
        '\u{3040}'..='\u{30ff}' | '\u{3100}'..='\u{312f}' |
        '\u{31a0}'..='\u{31bf}' | '\u{31f0}'..='\u{31ff}' |
        '\u{0e00}'..='\u{0eff}' | '\u{1000}'..='\u{109f}' |
        '\u{1780}'..='\u{17ff}' | '\u{a9e0}'..='\u{a9ff}' |
        '\u{aa60}'..='\u{aa7f}' | '\u{3000}'..='\u{303f}' |
        '\u{ff00}'..='\u{ffef}'
    )
}

fn prepare_moonshine_base_samples(samples: &mut Vec<f32>) {
    // Append only synthetic silence;
    // padding never contributes to unique recorded/speech sample counts.
    if !samples.is_empty() && samples.len() < MIN_MOONSHINE_BASE_INPUT_SAMPLES {
        samples.resize(MIN_MOONSHINE_BASE_INPUT_SAMPLES, 0.0);
    }
}

fn ensure_active(cancellation: &CancellationToken) -> Result<(), VoiceError> {
    if cancellation.is_cancelled() {
        return Err(VoiceError {
            kind: VoiceErrorKind::Cancelled,
            message: "voice processing cancelled".to_owned(),
            public_error: None,
        });
    }
    Ok(())
}
fn worker_error(message: &str) -> VoiceError {
    VoiceError {
        kind: VoiceErrorKind::TranscriptionFailed,
        message: message.to_owned(),
        public_error: None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct VoiceSignalStats {
    pub(crate) total_samples: usize,
    pub(crate) non_zero_samples: usize,
    pub(crate) rms: f32,
    pub(crate) peak: f32,
}
#[derive(Default)]
struct SignalAccumulator {
    total_samples: usize,
    non_zero_samples: usize,
    sum_squares: f64,
    peak: f32,
}
impl SignalAccumulator {
    fn add_samples(&mut self, samples: &[f32]) {
        self.total_samples += samples.len();
        for sample in samples {
            let abs = sample.abs();
            self.non_zero_samples += usize::from(abs > f32::EPSILON);
            self.peak = self.peak.max(abs);
            self.sum_squares += f64::from(*sample) * f64::from(*sample);
        }
    }
    fn finish(self) -> VoiceSignalStats {
        VoiceSignalStats {
            total_samples: self.total_samples,
            non_zero_samples: self.non_zero_samples,
            peak: self.peak,
            rms: if self.total_samples == 0 {
                0.0
            } else {
                (self.sum_squares / self.total_samples as f64).sqrt() as f32
            },
        }
    }
}
impl VoiceSignalStats {
    #[cfg(test)]
    pub(crate) fn from_samples(samples: &[f32]) -> Self {
        let mut accumulator = SignalAccumulator::default();
        accumulator.add_samples(samples);
        accumulator.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::transcription::TestSpeechTranscriber;
    use super::*;
    use pioneer_protocol::{VoiceAudioEncoding, VoiceChunkFrameHeader};
    use std::sync::Mutex;

    fn test_detector() -> super::super::vad::EnergyVoiceActivityDetector {
        super::super::vad::EnergyVoiceActivityDetector::new(0.02).unwrap()
    }

    // Entering frame N proves that all handling of frame N-1, including its
    // successful decode and candidate acceptance, has returned to the owner.
    // Notifications inside fake ASR itself cannot establish that ordering.
    struct CheckpointDetector {
        inner: super::super::vad::EnergyVoiceActivityDetector,
        frames: usize,
        checkpoints: Vec<usize>,
        reached: std::sync::mpsc::Sender<usize>,
    }

    impl CheckpointDetector {
        fn new(checkpoints: Vec<usize>) -> (Self, std::sync::mpsc::Receiver<usize>) {
            let (reached, receive) = std::sync::mpsc::channel();
            (
                Self {
                    inner: test_detector(),
                    frames: 0,
                    checkpoints,
                    reached,
                },
                receive,
            )
        }
    }

    impl VoiceActivityDetector for CheckpointDetector {
        fn speech_probability(&mut self, frame: &[f32]) -> anyhow::Result<f32> {
            let probability = self.inner.speech_probability(frame)?;
            self.frames += 1;
            if self.checkpoints.contains(&self.frames) {
                let _ = self.reached.send(self.frames);
            }
            Ok(probability)
        }
    }

    fn process(
        store: &GatewayVoiceSessionBufferStore,
        session_id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
        is_moonshine_base: bool,
    ) -> Result<GatewayVoiceSessionPipelineOutcome, VoiceError> {
        super::process(
            store,
            session_id,
            cancellation,
            transcriber,
            is_moonshine_base,
            test_detector(),
        )
    }

    fn process_buffered(
        store: &GatewayVoiceSessionBufferStore,
        session_id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
        is_moonshine_base: bool,
    ) -> Result<GatewayVoiceSessionPipelineOutcome, VoiceError> {
        super::process_buffered(
            store,
            session_id,
            cancellation,
            transcriber,
            is_moonshine_base,
            test_detector(),
        )
    }

    // Feed the production processor with real PCM normalization and smoothing.
    // The deterministic energy fixture proves framing/lifecycle, not Silero quality.
    // Closed input makes this synchronous fixture deterministic; the ingress
    // regression separately proves inference occurs while input is still open.
    fn partitioned(
        audio: &[i16],
        packet_samples: usize,
    ) -> (GatewayVoiceSessionPipelineOutcome, Vec<Vec<f32>>) {
        partitioned_input(audio, packet_samples, false)
    }

    fn partitioned_input(
        audio: &[i16],
        packet_samples: usize,
        is_moonshine_base: bool,
    ) -> (GatewayVoiceSessionPipelineOutcome, Vec<Vec<f32>>) {
        let calls = Arc::new(Mutex::new(Vec::<Vec<f32>>::new()));
        let captured = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            // This fake is reached after production native-input preparation.
            if is_moonshine_base && buffer.speech_samples() < 1600 {
                return Err(transcription_error(
                    VoiceTranscriptionErrorKind::RuntimeFailure,
                    "Moonshine native input must contain at least 1600 samples",
                ));
            }
            let mut calls = captured.lock().unwrap();
            calls.push(buffer.flattened_samples());
            // Stable fake text for these short full-recording fixtures.
            Ok(if buffer.speech_samples() <= 48000 {
                "part1"
            } else if buffer.speech_samples() <= 96000 {
                "part1 part2"
            } else {
                "part1 part2 part3"
            }
            .to_owned())
        }));
        let result = closed_input(audio, packet_samples, is_moonshine_base, &fake).unwrap();
        let captured = calls.lock().unwrap().clone();
        (result, captured)
    }

    fn closed_input(
        audio: &[i16],
        packet_samples: usize,
        is_moonshine_base: bool,
        fake: &impl VoiceSpeechTranscriber,
    ) -> Result<GatewayVoiceSessionPipelineOutcome, VoiceError> {
        closed_input_mode(audio, packet_samples, is_moonshine_base, fake, false)
    }

    fn closed_input_mode(
        audio: &[i16],
        packet_samples: usize,
        is_moonshine_base: bool,
        fake: &impl VoiceSpeechTranscriber,
        buffered: bool,
    ) -> Result<GatewayVoiceSessionPipelineOutcome, VoiceError> {
        let store = GatewayVoiceSessionBufferStore::default();
        let cancellation = CancellationToken::new();
        let (_sender, receiver) = oneshot::channel();
        let format = VoiceAudioFormat::pioneer_streaming_target();
        store
            .start_processing("session".to_owned(), format, cancellation.clone(), receiver)
            .unwrap();
        for (sequence, samples) in audio.chunks(packet_samples).enumerate() {
            let pcm: Vec<_> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
            store
                .append_chunk(
                    VoiceChunkFrameHeader {
                        session_id: "session".to_owned(),
                        sequence: sequence as u64,
                        sample_rate_hz: 16000,
                        channels: 1,
                        encoding: VoiceAudioEncoding::PcmS16Le,
                        payload_len: pcm.len() as u64,
                        captured_at_unix_ms: None,
                        duration_ms: None,
                    },
                    pcm,
                )
                .unwrap();
        }
        let _completion = store.finish_processing("session").unwrap();
        if buffered {
            process_buffered(&store, "session", &cancellation, fake, is_moonshine_base)
        } else {
            process(&store, "session", &cancellation, fake, is_moonshine_base)
        }
    }

    #[test]
    fn buffered_full_recording_is_one_exact_call_across_packet_boundaries() {
        // Longer than the incremental window, with a residual shorter than one
        // VAD frame. Every sample is voiced; literal expected PCM is independent
        // of the normalization/segmentation implementation under test.
        let audio: Vec<i16> = (0..20 * 16000 + 137)
            .map(|ix| 8000 + (ix % 4000) as i16)
            .collect();
        let expected: Vec<f32> = audio
            .iter()
            .map(|sample| f32::from(*sample) / 32768.0)
            .collect();
        for packet in [137, 480, 1599, 1600] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let capture = calls.clone();
            let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                capture.lock().unwrap().push(buffer.flattened_samples());
                Ok(" full recording ".to_owned())
            }));
            let outcome = closed_input_mode(&audio, packet, false, &fake, true).unwrap();
            assert_eq!(*calls.lock().unwrap(), vec![expected.clone()]);
            let GatewayVoiceSessionPipelineOutcome::Transcript {
                transcript,
                signal_stats,
            } = outcome
            else {
                panic!("full transcript expected");
            };
            assert_eq!(transcript.text, "full recording");
            assert_eq!(transcript.diagnostics.total_samples, audio.len());
            assert_eq!(transcript.diagnostics.speech_samples, audio.len());
            assert_eq!(signal_stats.total_samples, audio.len());
            assert_eq!(
                transcript.diagnostics.strategy,
                VoiceTranscriptionStrategy::BufferedGatewaySession
            );
        }
    }

    #[test]
    fn buffered_vad_joins_speech_once_without_replaying_long_pauses() {
        let mut audio = vec![12000; 1024];
        audio.extend(vec![0; 512 * 40]);
        audio.extend(vec![16000; 1024 + 79]);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let capture = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            capture.lock().unwrap().push(buffer.flattened_samples());
            Ok("joined".to_owned())
        }));
        closed_input_mode(&audio, 137, false, &fake, true).unwrap();
        // Fourteen 32 ms hangover frames, then thirteen real quiet frames
        // preceding the two-frame onset. The middle of the long pause is removed.
        let mut expected = vec![12000.0 / 32768.0; 1024];
        expected.extend(vec![0.0; 512 * (14 + 13)]);
        expected.extend(vec![16000.0 / 32768.0; 1024 + 79]);
        assert_eq!(*calls.lock().unwrap(), vec![expected]);
    }

    #[test]
    fn buffered_short_onset_tail_padding_silence_empty_text_and_failure() {
        for source_samples in [1, 137, 479, 480, 481, 959, 960, 1599] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let capture = calls.clone();
            let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                capture.lock().unwrap().push(buffer.flattened_samples());
                Ok("short".to_owned())
            }));
            let outcome =
                closed_input_mode(&vec![12000; source_samples], 137, true, &fake, true).unwrap();
            let mut expected = vec![12000.0 / 32768.0; source_samples];
            expected.resize(1600, 0.0);
            assert_eq!(*calls.lock().unwrap(), vec![expected]);
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = outcome else {
                panic!("short speech expected");
            };
            assert_eq!(transcript.diagnostics.total_samples, source_samples);
            assert_eq!(transcript.diagnostics.speech_samples, source_samples);
        }
        let never = TestSpeechTranscriber(Arc::new(|_| panic!("silence must not call ASR")));
        for (amplitude, is_moonshine_base) in [(0, false), (50, false), (0, true), (50, true)] {
            for count in [0, 137, 960, 32000] {
                let outcome = closed_input_mode(
                    &vec![amplitude; count],
                    137,
                    is_moonshine_base,
                    &never,
                    true,
                )
                .unwrap();
                let GatewayVoiceSessionPipelineOutcome::NoSpeech { no_speech, .. } = outcome else {
                    panic!("silence or low-energy noise expected");
                };
                assert_eq!(no_speech.total_samples, count);
            }
        }
        let empty = TestSpeechTranscriber(Arc::new(|_| Ok(" \n ".to_owned())));
        assert!(matches!(
            closed_input_mode(&[12000; 960], 137, false, &empty, true).unwrap(),
            GatewayVoiceSessionPipelineOutcome::NoSpeech {
                no_speech: VoiceTranscriptionNoSpeech {
                    reason: VoiceTranscriptionNoSpeechReason::EmptyTranscript,
                    ..
                },
                ..
            }
        ));
        let failure = TestSpeechTranscriber(Arc::new(|_| {
            Err(transcription_error(
                VoiceTranscriptionErrorKind::RuntimeFailure,
                "native failed",
            ))
        }));
        assert!(closed_input_mode(&[12000; 960], 137, false, &failure, true).is_err());
    }

    fn append_test_packet(
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        sequence: u64,
        samples: &[i16],
    ) -> super::super::session_buffer::VoiceChunkIngestReport {
        let pcm: Vec<_> = samples
            .iter()
            .flat_map(|sample| sample.to_le_bytes())
            .collect();
        store
            .append_chunk(
                VoiceChunkFrameHeader {
                    session_id: id.to_owned(),
                    sequence,
                    sample_rate_hz: 16000,
                    channels: 1,
                    encoding: VoiceAudioEncoding::PcmS16Le,
                    payload_len: pcm.len() as u64,
                    captured_at_unix_ms: None,
                    duration_ms: None,
                },
                pcm,
            )
            .unwrap()
    }

    #[test]
    fn buffered_commit_drains_unique_ordered_backlog_and_rejects_late_packets() {
        let store = GatewayVoiceSessionBufferStore::default();
        let token = CancellationToken::new();
        let (_sender, receiver) = oneshot::channel();
        store
            .start_processing(
                "session".to_owned(),
                VoiceAudioFormat::pioneer_streaming_target(),
                token.clone(),
                receiver,
            )
            .unwrap();
        append_test_packet(&store, "session", 0, &[8000; 1600]);
        assert!(append_test_packet(&store, "session", 0, &[20000; 1600]).already_buffered);
        assert_eq!(
            append_test_packet(&store, "session", 1, &[12000; 1600]).missing_chunks,
            0
        );
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            captured.lock().unwrap().push(buffer.flattened_samples());
            Ok("unique".to_owned())
        }));
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| process_buffered(&store, "session", &token, &fake, false));
            append_test_packet(&store, "session", 2, &[16000; 79]);
            // The close and append boundary is the production inbox mutex.
            let _completion = store.finish_processing("session").unwrap();
            assert!(
                store
                    .append_chunk(
                        VoiceChunkFrameHeader {
                            session_id: "session".to_owned(),
                            sequence: 3,
                            sample_rate_hz: 16000,
                            channels: 1,
                            encoding: VoiceAudioEncoding::PcmS16Le,
                            payload_len: 2,
                            captured_at_unix_ms: None,
                            duration_ms: None,
                        },
                        vec![0, 0]
                    )
                    .is_err()
            );
            let outcome = worker.join().unwrap().unwrap();
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = outcome else {
                panic!("transcript expected");
            };
            assert_eq!(transcript.diagnostics.total_samples, 3279);
            assert_eq!(transcript.diagnostics.speech_samples, 3279);
        });
        let mut expected = vec![8000.0 / 32768.0; 1600];
        expected.extend(vec![12000.0 / 32768.0; 1600]);
        expected.extend(vec![16000.0 / 32768.0; 79]);
        assert_eq!(*calls.lock().unwrap(), vec![expected]);
    }

    #[tokio::test]
    async fn both_modes_retain_native_lease_on_cancel_or_shutdown() {
        use super::super::supervisor::{EagerVoiceEngineLoader, FilesystemVoiceModelInstaller};
        for incremental in [false, true] {
            for shutdown in [false, true] {
                let store = GatewayVoiceSessionBufferStore::default();
                let workers = VoiceFinalizations::default();
                let lease = workers.reserve(1).unwrap();
                let other_leases: Vec<_> = (2..=4).map(|id| workers.reserve(id).unwrap()).collect();
                let token = workers.shutdown.child_token();
                let supervisor = Arc::new(VoiceInputSupervisor::new(
                    Arc::new(FilesystemVoiceModelInstaller::new(
                        crate::isolated_test_app_config().unwrap(),
                        std::path::PathBuf::new(),
                    )),
                    Arc::new(EagerVoiceEngineLoader),
                ));
                let (entered, mut native_entered) = tokio::sync::mpsc::unbounded_channel();
                let (release, released) = std::sync::mpsc::channel();
                let released = Arc::new(Mutex::new(released));
                let fake = Arc::new(move |buffer: &PreparedSpeechBuffer| {
                    entered.send(buffer.flattened_samples()).unwrap();
                    released.lock().unwrap().recv().unwrap();
                    Ok("must not escape cancellation".to_owned())
                });
                start(
                    store.clone(),
                    "session".to_owned(),
                    VoiceAudioFormat::pioneer_streaming_target(),
                    &workers,
                    lease,
                    SessionSpeechTranscriber {
                        supervisor,
                        generation: 0,
                        is_moonshine_base: false,
                        cancellation: token.clone(),
                        fake: Some(fake),
                    },
                    incremental,
                )
                .unwrap();
                for sequence in 0..40 {
                    append_test_packet(&store, "session", sequence, &[12000; 1600]);
                }
                assert!(native_entered.try_recv().is_err());
                let completion = store.finish_processing("session").unwrap();
                let native_pcm =
                    tokio::time::timeout(std::time::Duration::from_secs(5), native_entered.recv())
                        .await
                        .unwrap()
                        .unwrap();
                assert_eq!(native_pcm, vec![12000.0 / 32768.0; 64000]);
                if shutdown {
                    workers.close();
                    store.cancel_all();
                } else {
                    store.remove_session("session").unwrap();
                }
                assert!(token.is_cancelled());
                drop(completion);
                // Even a dropped result receiver cannot free native capacity early.
                assert!(workers.reserve(1).is_none());
                assert!(workers.reserve(5).is_none());
                release.send(()).unwrap();
                drop(other_leases);
                workers.tasks.close();
                tokio::time::timeout(std::time::Duration::from_secs(5), workers.tasks.wait())
                    .await
                    .unwrap();
                assert!(!workers.has_active_work());
                assert!(native_entered.try_recv().is_err());
                if !shutdown {
                    assert!(workers.reserve(1).is_some());
                }
            }
        }
    }

    #[test]
    fn buffered_cancel_before_commit_does_not_enter_inference() {
        let store = GatewayVoiceSessionBufferStore::default();
        let token = CancellationToken::new();
        let (_sender, receiver) = oneshot::channel();
        store
            .start_processing(
                "session".to_owned(),
                VoiceAudioFormat::pioneer_streaming_target(),
                token.clone(),
                receiver,
            )
            .unwrap();
        append_test_packet(&store, "session", 0, &[12000; 960]);
        let fake = TestSpeechTranscriber(Arc::new(|_| panic!("cancelled input cannot call ASR")));
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| process_buffered(&store, "session", &token, &fake, false));
            store.remove_session("session").unwrap();
            assert!(worker.join().unwrap().is_err());
        });
    }

    fn open_store() -> GatewayVoiceSessionBufferStore {
        let store = GatewayVoiceSessionBufferStore::default();
        store
            .start_session("session", VoiceAudioFormat::pioneer_streaming_target())
            .unwrap();
        store
    }

    #[test]
    fn moonshine_context_keeps_unique_source_diagnostics() {
        let mut audio = vec![12000_i16; 48000];
        audio.extend(vec![6000; 48000]);
        audio.extend([3000; 7]);
        let (result, calls) = partitioned_input(&audio, 317, true);
        let (_, other_packets) = partitioned_input(&audio, 1600, true);
        assert_eq!(calls, other_packets);
        assert_eq!(calls.iter().map(Vec::len).collect::<Vec<_>>(), vec![96007]);
        // This complete short recording needs no padding. Separate short-tail
        // fixtures assert native padding.
        let recovered_source = calls.concat();
        assert_eq!(
            recovered_source,
            audio
                .iter()
                .map(|sample| f32::from(*sample) / 32768.0)
                .collect::<Vec<_>>()
        );
        let GatewayVoiceSessionPipelineOutcome::Transcript {
            transcript,
            signal_stats,
        } = result
        else {
            panic!("transcript expected");
        };
        assert_eq!(transcript.text, "part1 part2 part3");
        assert_eq!(transcript.diagnostics.total_samples, 96007);
        assert_eq!(transcript.diagnostics.speech_samples, 96007);
        assert_eq!(transcript.diagnostics.segment_count, 1);
        assert_eq!(signal_stats.total_samples, 96007);
        assert_eq!(signal_stats.non_zero_samples, 96007);
        assert_eq!(signal_stats.peak, 12000.0 / 32768.0);
        let [first, second, tail] = [12000.0_f64, 6000.0, 3000.0].map(|sample| sample / 32768.0);
        let source_rms = ((48000.0 * first * first + 48000.0 * second * second + 7.0 * tail * tail)
            / 96007.0)
            .sqrt() as f32;
        assert!((signal_stats.rms - source_rms).abs() < 1e-7);
    }

    #[test]
    fn moonshine_native_short_commit_preserves_source_at_minimum_boundary() {
        for count in [1, 7, 479, 512, 487, 959, 1024, 1031, 1599, 1600, 1601] {
            let audio: Vec<_> = (0..count)
                .map(|index| [12000_i16, -6000, 3000][index % 3])
                .collect();
            let (result, calls) = partitioned_input(&audio, 317, true);
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].len(), count.max(1600));
            let normalized: Vec<_> = audio
                .iter()
                .map(|sample| f32::from(*sample) / 32768.0)
                .collect();
            assert_eq!(&calls[0][..count], normalized.as_slice());
            assert!(calls[0][count..].iter().all(|sample| *sample == 0.0));
            let GatewayVoiceSessionPipelineOutcome::Transcript {
                transcript,
                signal_stats,
            } = result
            else {
                panic!("short voiced Commit must reach native inference");
            };
            assert_eq!(transcript.text, "part1");
            assert_eq!(transcript.diagnostics.total_samples, count);
            assert_eq!(transcript.diagnostics.speech_samples, count);
            assert_eq!(transcript.diagnostics.segment_count, 1);
            assert_eq!(signal_stats, VoiceSignalStats::from_samples(&normalized));
        }
    }

    #[test]
    fn moonshine_native_padding_does_not_admit_silence_to_inference() {
        for count in [0, 1, 7, 479, 1599, 1600, 48000] {
            let (result, calls) = partitioned_input(&vec![0; count], 317, true);
            let GatewayVoiceSessionPipelineOutcome::NoSpeech {
                signal_stats,
                no_speech,
            } = result
            else {
                panic!("no speech expected");
            };
            assert!(calls.is_empty());
            assert_eq!(no_speech.total_samples, count);
            assert_eq!(
                signal_stats,
                VoiceSignalStats::from_samples(&vec![0.0; count])
            );
        }
    }

    #[test]
    fn voice_incremental_packet_boundaries_do_not_repeat_prefix_or_drop_tail() {
        let mut audio = vec![12000; (3 * RATE)];
        audio.extend(vec![6000; (3 * RATE)]);
        audio.extend([3000; 7]);
        let (_, baseline) = partitioned(&audio, 1600);
        assert_eq!(
            baseline.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![96007]
        );
        let (result, fragmented) = partitioned(&audio, 317);
        assert_eq!(fragmented, baseline);
        let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
            panic!("transcript expected");
        };
        assert_eq!(transcript.text, "part1 part2 part3");
        assert_eq!(transcript.diagnostics.total_samples, audio.len());
        assert_eq!(transcript.diagnostics.speech_samples, audio.len());
    }

    #[test]
    fn voice_incremental_short_onset_and_subframe_recordings_are_preserved() {
        for count in [1, 137, 479, 512, 487, 959, 1024, 1031] {
            let audio = vec![12000; count];
            let (result, calls) = partitioned(&audio, 317);
            assert!(matches!(
                result,
                GatewayVoiceSessionPipelineOutcome::Transcript { .. }
            ));
            assert_eq!(calls.concat(), vec![12000.0 / 32768.0; count]);
        }
    }

    #[test]
    fn voice_incremental_silence_never_calls_inference() {
        for count in [0, 1, 479, 1024, 60 * RATE] {
            let (result, calls) = partitioned(&vec![0; count], 317);
            assert!(matches!(
                result,
                GatewayVoiceSessionPipelineOutcome::NoSpeech { .. }
            ));
            assert!(calls.is_empty());
        }
    }

    #[test]
    fn voice_incremental_two_sessions_keep_audio_and_results_separate() {
        for buffered in [false, true] {
            let store = GatewayVoiceSessionBufferStore::default();
            let cancellation = CancellationToken::new();
            for (id, amplitude, count) in [("first", 12000_i16, 1031), ("second", 6000_i16, 1)] {
                let (_sender, receiver) = oneshot::channel();
                store
                    .start_processing(
                        id.to_owned(),
                        VoiceAudioFormat::pioneer_streaming_target(),
                        cancellation.child_token(),
                        receiver,
                    )
                    .unwrap();
                let pcm: Vec<_> = std::iter::repeat_n(amplitude, count)
                    .flat_map(i16::to_le_bytes)
                    .collect();
                store
                    .append_chunk(
                        VoiceChunkFrameHeader {
                            session_id: id.to_owned(),
                            sequence: 0,
                            sample_rate_hz: 16000,
                            channels: 1,
                            encoding: VoiceAudioEncoding::PcmS16Le,
                            payload_len: pcm.len() as u64,
                            captured_at_unix_ms: None,
                            duration_ms: None,
                        },
                        pcm,
                    )
                    .unwrap();
                let _completion = store.finish_processing(id).unwrap();
            }
            let first = TestSpeechTranscriber(Arc::new(|buffer| {
                assert_eq!(buffer.flattened_samples(), vec![12000.0 / 32768.0; 1031]);
                Ok("first result".to_owned())
            }));
            let second = TestSpeechTranscriber(Arc::new(|buffer| {
                assert_eq!(buffer.flattened_samples(), vec![6000.0 / 32768.0]);
                Ok("second result".to_owned())
            }));
            let (first, second) = std::thread::scope(|scope| {
                let first = scope.spawn(|| {
                    if buffered {
                        process_buffered(&store, "first", &cancellation, &first, false)
                    } else {
                        process(&store, "first", &cancellation, &first, false)
                    }
                });
                let second = scope.spawn(|| {
                    if buffered {
                        process_buffered(&store, "second", &cancellation, &second, false)
                    } else {
                        process(&store, "second", &cancellation, &second, false)
                    }
                });
                (
                    first.join().unwrap().unwrap(),
                    second.join().unwrap().unwrap(),
                )
            });
            let GatewayVoiceSessionPipelineOutcome::Transcript {
                transcript: first, ..
            } = first
            else {
                panic!("first transcript");
            };
            let GatewayVoiceSessionPipelineOutcome::Transcript {
                transcript: second, ..
            } = second
            else {
                panic!("second transcript");
            };
            assert_eq!(first.text, "first result");
            assert_eq!(second.text, "second result");
            assert_eq!(first.diagnostics.total_samples, 1031);
            assert_eq!(second.diagnostics.total_samples, 1);
        }
    }

    #[test]
    fn voice_incremental_next_onset_does_not_replay_previous_hangover() {
        for buffered in [false, true] {
            for remainder in [0, 1, 137, 479] {
                let mut audio = vec![12000; 1024];
                audio.extend(vec![0; 16 * VOICE_VAD_FRAME_SAMPLES]);
                // Only one complete frame in the final onset. Commit must
                // recover it, with or without a short remainder.
                audio.extend(vec![6000; VOICE_VAD_FRAME_SAMPLES + remainder]);
                let expected: Vec<_> = audio.iter().map(|s| f32::from(*s) / 32768.0).collect();
                for packet in [1, 137, 479, 1600] {
                    let calls = Arc::new(Mutex::new(Vec::new()));
                    let captured = calls.clone();
                    let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                        captured.lock().unwrap().push(buffer.flattened_samples());
                        Ok("speech".to_owned())
                    }));
                    let outcome =
                        closed_input_mode(&audio, packet, false, &fake, buffered).unwrap();
                    let calls = calls.lock().unwrap();
                    assert_eq!(calls.len(), 1);
                    assert_eq!(calls.concat(), expected);
                    let GatewayVoiceSessionPipelineOutcome::Transcript {
                        transcript,
                        signal_stats,
                    } = outcome
                    else {
                        panic!("final onset expected");
                    };
                    assert_eq!(transcript.diagnostics.speech_samples, audio.len());
                    assert_eq!(transcript.diagnostics.total_samples, audio.len());
                    assert_eq!(signal_stats.total_samples, audio.len());
                    assert_eq!(signal_stats.non_zero_samples, 1536 + remainder);
                }
            }
        }
    }

    #[tokio::test]
    async fn detector_cancel_or_failure_retains_lease_until_return_and_publishes_no_transcript() {
        use super::super::supervisor::{EagerVoiceEngineLoader, FilesystemVoiceModelInstaller};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct BlockingDetector {
            entered: tokio::sync::mpsc::UnboundedSender<()>,
            release: std::sync::mpsc::Receiver<()>,
            fail: bool,
        }
        impl VoiceActivityDetector for BlockingDetector {
            fn speech_probability(&mut self, frame: &[f32]) -> anyhow::Result<f32> {
                assert_eq!(frame.len(), VOICE_VAD_FRAME_SAMPLES);
                self.entered.send(()).unwrap();
                self.release.recv().unwrap();
                if self.fail {
                    anyhow::bail!("detector failed");
                }
                Ok(0.9)
            }
        }
        for buffered in [false, true] {
            // Cancellation during synchronous inference cannot abort it. A
            // native error without cancellation must also fail closed.
            for cancel in [false, true] {
                let store = GatewayVoiceSessionBufferStore::default();
                let workers = VoiceFinalizations::default();
                let lease = workers.reserve(1).unwrap();
                let other_leases: Vec<_> = (2..=4).map(|id| workers.reserve(id).unwrap()).collect();
                let cancellation = CancellationToken::new();
                let supervisor = Arc::new(VoiceInputSupervisor::new(
                    Arc::new(FilesystemVoiceModelInstaller::new(
                        crate::isolated_test_app_config().unwrap(),
                        std::path::PathBuf::new(),
                    )),
                    Arc::new(EagerVoiceEngineLoader),
                ));
                let asr_calls = Arc::new(AtomicUsize::new(0));
                let worker_asr_calls = asr_calls.clone();
                let transcriber = SessionSpeechTranscriber {
                    supervisor,
                    generation: 0,
                    is_moonshine_base: false,
                    cancellation: cancellation.clone(),
                    fake: Some(Arc::new(move |_| {
                        worker_asr_calls.fetch_add(1, Ordering::SeqCst);
                        Ok("unexpected ASR result".to_owned())
                    })),
                };
                let (entered, mut receive) = tokio::sync::mpsc::unbounded_channel();
                let (release, released) = std::sync::mpsc::channel();
                start_with_detector(
                    store.clone(),
                    "session".to_owned(),
                    VoiceAudioFormat::pioneer_streaming_target(),
                    &workers,
                    lease,
                    transcriber,
                    !buffered,
                    move |_| {
                        Ok(BlockingDetector {
                            entered,
                            release: released,
                            fail: !cancel,
                        })
                    },
                )
                .unwrap();
                append_test_packet(&store, "session", 0, &[12000; 1024]);
                let completion = store.finish_processing("session").unwrap();
                tokio::time::timeout(std::time::Duration::from_secs(5), receive.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(workers.reserve(1).is_none());
                assert!(workers.reserve(5).is_none());
                if cancel {
                    store.remove_session("session").unwrap();
                    assert!(cancellation.is_cancelled());
                    drop(completion);
                    assert!(workers.reserve(1).is_none());
                    release.send(()).unwrap();
                } else {
                    release.send(()).unwrap();
                    let result = completion.await.unwrap();
                    assert!(matches!(
                        result.outcome,
                        Err(VoiceError {
                            kind: VoiceErrorKind::TranscriptionFailed,
                            ..
                        })
                    ));
                    // Publication still owns capacity until its completion is dropped.
                    assert!(workers.reserve(1).is_none());
                    drop(result.lease);
                }
                drop(other_leases);
                workers.tasks.close();
                tokio::time::timeout(std::time::Duration::from_secs(5), workers.tasks.wait())
                    .await
                    .unwrap();
                assert_eq!(asr_calls.load(Ordering::SeqCst), 0);
                assert!(!workers.has_active_work());
                assert!(workers.reserve(1).is_some());
                assert!(receive.try_recv().is_err());
            }
        }
    }

    #[test]
    fn real_samples_cross_packet_and_v4_frame_boundaries_once_in_both_modes() {
        struct RecordingVad(Arc<Mutex<Vec<Vec<f32>>>>);
        impl VoiceActivityDetector for RecordingVad {
            fn speech_probability(&mut self, frame: &[f32]) -> anyhow::Result<f32> {
                self.0.lock().unwrap().push(frame.to_vec());
                Ok(0.9)
            }
        }
        for count in [1usize, 511, 512, 513, 1024, 1615] {
            let audio: Vec<i16> = (0..count).map(|ix| 8000 + ix as i16).collect();
            let expected: Vec<_> = audio
                .iter()
                .map(|sample| f32::from(*sample) / 32768.0)
                .collect();
            for packet in [1, 137, 511, 512, 513, 1600] {
                for buffered in [false, true] {
                    let store = GatewayVoiceSessionBufferStore::default();
                    let (_sender, receiver) = oneshot::channel();
                    store
                        .start_processing(
                            "session".to_owned(),
                            VoiceAudioFormat::pioneer_streaming_target(),
                            CancellationToken::new(),
                            receiver,
                        )
                        .unwrap();
                    for (sequence, chunk) in audio.chunks(packet).enumerate() {
                        append_test_packet(&store, "session", sequence as u64, chunk);
                    }
                    let _completion = store.finish_processing("session").unwrap();
                    let vad_calls = Arc::new(Mutex::new(Vec::new()));
                    let asr_calls = Arc::new(Mutex::new(Vec::new()));
                    let captured = asr_calls.clone();
                    let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                        captured.lock().unwrap().push(buffer.flattened_samples());
                        Ok("short speech".to_owned())
                    }));
                    let detector = RecordingVad(vad_calls.clone());
                    let token = CancellationToken::new();
                    let result = if buffered {
                        super::process_buffered(&store, "session", &token, &fake, false, detector)
                    } else {
                        super::process(&store, "session", &token, &fake, false, detector)
                    }
                    .unwrap();
                    assert_eq!(*asr_calls.lock().unwrap(), vec![expected.clone()]);
                    let frames = vad_calls.lock().unwrap();
                    assert_eq!(frames.len(), count.div_ceil(512));
                    assert!(frames.iter().all(|frame| frame.len() == 512));
                    let classified = frames.concat();
                    assert_eq!(&classified[..count], expected);
                    assert!(classified[count..].iter().all(|sample| *sample == 0.0));
                    let GatewayVoiceSessionPipelineOutcome::Transcript {
                        transcript,
                        signal_stats,
                    } = result
                    else {
                        panic!("speech");
                    };
                    assert_eq!(transcript.diagnostics.speech_samples, count);
                    assert_eq!(transcript.diagnostics.total_samples, count);
                    assert_eq!(signal_stats.total_samples, count);
                }
            }
        }
    }

    // Prepared-stream tests isolate boundary selection from capture VAD policy.
    // The fake captures actual engine input, including any accidental overlap.
    fn prepared_chunks(pcm: &[f32], intervals: &[VadInterval], packet: usize) -> Vec<Vec<f32>> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            captured.lock().unwrap().push(buffer.flattened_samples());
            Ok("да".to_owned())
        }));
        let store = open_store();
        let token = CancellationToken::new();
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        let mut offset = 0;
        for interval in intervals {
            let length = interval.end - interval.start;
            for at in (0..length).step_by(packet) {
                let take = packet.min(length - at);
                let clipped = VadInterval {
                    start: interval.start + at,
                    end: interval.start + at + take,
                    ..*interval
                };
                state.source_samples = clipped.end;
                state
                    .push(
                        &pcm[offset + at..offset + at + take],
                        &[clipped],
                        &store,
                        "session",
                        &token,
                        &fake,
                    )
                    .unwrap();
                assert!(state.speech.len() < PLANNER_CAP_SAMPLES);
            }
            offset += length;
        }
        state.finish(&store, "session", &token, &fake).unwrap();
        assert_eq!(state.chunk_start_sample, pcm.len());
        assert!(state.timeline.intervals.is_empty());
        let result = calls.lock().unwrap().clone();
        result
    }

    fn interval(start: usize, end: usize, voice: bool, quiet_start: usize) -> VadInterval {
        VadInterval {
            start,
            end,
            voice,
            probability: if voice { 0.9 } else { 0.0 },
            quiet_run_start: quiet_start,
        }
    }

    #[test]
    fn finalized_ranges_are_packet_independent_disjoint_and_bounded() {
        // A continuous recording, one with competing closed pauses, and an
        // open pause whose moving midpoint must never become a candidate.
        for intervals in [
            vec![interval(0, 84 * RATE + 7, true, 0)],
            vec![
                interval(0, 28 * RATE, true, 0),
                interval(28 * RATE, 29 * RATE, false, 28 * RATE),
                interval(29 * RATE, 58 * RATE, true, 29 * RATE),
                interval(58 * RATE, 59 * RATE, false, 58 * RATE),
                interval(59 * RATE, 100 * RATE + 7, true, 59 * RATE),
            ],
            vec![
                interval(0, 34 * RATE, true, 0),
                interval(34 * RATE, 50 * RATE, false, 34 * RATE),
                interval(50 * RATE, 90 * RATE, true, 50 * RATE),
            ],
        ] {
            let length = intervals.last().unwrap().end;
            let pcm: Vec<_> = (0..length).map(|i| i as f32).collect();
            let baseline = prepared_chunks(&pcm, &intervals, length);
            let packetized = prepared_chunks(&pcm, &intervals, 317);
            assert_eq!(packetized, baseline);
            assert_eq!(baseline.concat(), pcm);
            assert!(baseline.iter().all(|chunk| chunk.len() <= 40 * RATE));
            let expected = match length {
                n if n == 84 * RATE + 7 => vec![
                    40 * RATE,
                    (44 * RATE + 7) / 2,
                    44 * RATE + 7 - (44 * RATE + 7) / 2,
                ],
                n if n == 90 * RATE => vec![40 * RATE, 40 * RATE, 10 * RATE],
                _ => vec![
                    285 * RATE / 10,
                    30 * RATE,
                    (415 * RATE / 10 + 7) / 2,
                    415 * RATE / 10 + 7 - (415 * RATE / 10 + 7) / 2,
                ],
            };
            assert_eq!(baseline.iter().map(Vec::len).collect::<Vec<_>>(), expected);
        }
    }

    #[test]
    fn finish_preserves_short_recordings_and_prefers_a_useful_rest() {
        for length in [1, 7, 5 * RATE, 40 * RATE] {
            let pcm = vec![0.5; length];
            let calls = prepared_chunks(&pcm, &[interval(0, length, true, 0)], 317);
            assert_eq!(calls, vec![pcm]);
        }
        let intervals = [
            interval(0, 10 * RATE, true, 0),
            interval(10 * RATE, 11 * RATE, false, 10 * RATE),
            interval(11 * RATE, 38 * RATE, true, 11 * RATE),
            interval(38 * RATE, 39 * RATE, false, 38 * RATE),
            interval(39 * RATE, 42 * RATE, true, 39 * RATE),
        ];
        let calls = prepared_chunks(&vec![0.5; 42 * RATE], &intervals, 317);
        assert_eq!(
            calls.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![105 * RATE / 10, 315 * RATE / 10]
        );
    }

    #[test]
    fn finish_keeps_a_short_tail_after_the_only_available_pause() {
        let length = 405 * RATE / 10;
        let intervals = [
            interval(0, 388 * RATE / 10, true, 0),
            interval(388 * RATE / 10, 392 * RATE / 10, false, 388 * RATE / 10),
            interval(392 * RATE / 10, length, true, 392 * RATE / 10),
        ];
        let pcm: Vec<_> = (0..length).map(|i| i as f32).collect();
        let chunks = prepared_chunks(&pcm, &intervals, 317);
        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![39 * RATE, 15 * RATE / 10]
        );
        assert_eq!(chunks.concat(), pcm);
    }

    #[test]
    fn open_pause_waits_and_evicted_positive_frames_separate_quiet_runs() {
        let mut open = AudioTimeline::default();
        open.append(0, 0, 512, &[interval(0, 512, false, 0)]);
        assert!(open.candidates(None).is_empty());
        assert_eq!(
            open.candidates(Some((0, 100 * RATE))),
            vec![(512, 100 * RATE)]
        );
        // A changed full-input quiet identity proves an intervening positive
        // frame, even though VAD removed it and the surrounding source gap.
        open.append(512, 8192, 8704, &[interval(8192, 8704, false, 8192)]);
        assert_eq!(open.candidates(None), vec![(256, 512)]);
        assert_eq!(
            open.candidates(Some((9000, 10000))),
            vec![(256, 512), (768, 512)]
        );
    }

    #[test]
    fn fragment_join_preserves_repeats_punctuation_and_unicode() {
        for (parts, expected) in [
            (vec![" да, да ", "да; ещё раз"], "да, да да; ещё раз"),
            (
                vec!["unfinished", "continuation"],
                "unfinished continuation",
            ),
            (vec!["Hello", ", world!"], "Hello, world!"),
            (vec!["你好𠀀", "我们。", "再见"], "你好𠀀我们。再见"),
            (vec!["สวัสดี", "ครับ"], "สวัสดีครับ"),
            (
                vec!["안녕하세요.", "반갑습니다."],
                "안녕하세요. 반갑습니다.",
            ),
            (vec!["", "   ", "écho", "écho"], "écho écho"),
        ] {
            let mut result = String::new();
            for part in parts {
                append_fragment(&mut result, part);
            }
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn long_vad_recording_matches_buffered_preparation_and_failure_is_not_partial_success() {
        let mut audio = vec![12000; 18 * RATE];
        audio.extend(vec![0; RATE]);
        audio.extend(vec![6000; 44 * RATE + 7]);
        let capture = |buffered| {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let captured = calls.clone();
            let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                captured.lock().unwrap().push(buffer.flattened_samples());
                Ok("да".to_owned())
            }));
            let outcome = closed_input_mode(&audio, 1600, false, &fake, buffered).unwrap();
            let result = calls.lock().unwrap().clone();
            (outcome, result)
        };
        let (_, buffered) = capture(true);
        let (result, chunks) = capture(false);
        assert_eq!(buffered.len(), 1);
        assert_eq!(chunks.concat(), buffered[0]);
        assert!(chunks.len() > 2);
        let GatewayVoiceSessionPipelineOutcome::Transcript {
            transcript,
            signal_stats,
        } = result
        else {
            panic!("speech")
        };
        assert_eq!(transcript.diagnostics.speech_samples, buffered[0].len());
        assert_eq!(transcript.diagnostics.segment_count, chunks.len());
        assert_eq!(signal_stats.total_samples, audio.len());
        assert_eq!(
            transcript.text,
            std::iter::repeat_n("да", chunks.len())
                .collect::<Vec<_>>()
                .join(" ")
        );
        let calls = Arc::new(Mutex::new(0));
        let count = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |_| {
            let mut count = count.lock().unwrap();
            *count += 1;
            if *count == 3 {
                Err(transcription_error(
                    VoiceTranscriptionErrorKind::RuntimeFailure,
                    "third chunk failed",
                ))
            } else {
                Ok("must not publish".to_owned())
            }
        }));
        assert!(closed_input(&audio, 1600, false, &fake).is_err());
        assert_eq!(*calls.lock().unwrap(), 3);
    }

    #[test]
    fn live_worker_decodes_finalized_audio_before_stop_and_retains_open_pause() {
        let store = GatewayVoiceSessionBufferStore::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            captured.lock().unwrap().push(buffer.flattened_samples());
            Ok("speech".to_owned())
        }));
        let (detector, reached) = CheckpointDetector::new(vec![1408, 1900]);
        let token = CancellationToken::new();
        let (_sender, receiver) = oneshot::channel();
        store
            .start_processing(
                "session".to_owned(),
                VoiceAudioFormat::pioneer_streaming_target(),
                token.clone(),
                receiver,
            )
            .unwrap();
        // Wake the inbox waiter before scope joins if feeding or closing panics.
        struct SessionCleanup<'a>(&'a GatewayVoiceSessionBufferStore);
        impl Drop for SessionCleanup<'_> {
            fn drop(&mut self) {
                let _ = self.0.remove_session("session");
            }
        }
        std::thread::scope(|scope| {
            let _cleanup = SessionCleanup(&store);
            let worker =
                scope.spawn(|| super::process(&store, "session", &token, &fake, false, detector));
            let mut audio = vec![12000; 34 * RATE];
            audio.extend(vec![0; 13 * RATE]);
            // VAD removes most quiet PCM: source 47 s alone cannot force a cut.
            for (sequence, piece) in audio.chunks(1600).enumerate() {
                append_test_packet(&store, "session", sequence as u64, piece);
            }
            let checkpoint = reached.recv_timeout(std::time::Duration::from_secs(5));
            let no_early_decode = calls.lock().unwrap().is_empty();
            // Resume and pass 45 s of prepared PCM, so one chunk is finalized.
            for sequence in 470..620 {
                append_test_packet(&store, "session", sequence, &[6000; 1600]);
            }
            let checkpoint_after = reached.recv_timeout(std::time::Duration::from_secs(5));
            let decoded_before_stop = calls.lock().unwrap().len();
            let _completion = store.finish_processing("session").unwrap();
            let result = worker.join().unwrap().unwrap();
            assert_eq!(checkpoint.unwrap(), 1408);
            assert_eq!(checkpoint_after.unwrap(), 1900);
            assert!(decoded_before_stop > 0);
            assert!(no_early_decode);
            assert!(matches!(
                result,
                GatewayVoiceSessionPipelineOutcome::Transcript { .. }
            ));
        });
        assert!(calls.lock().unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn generation_changed_during_last_native_call_rejects_completed_text() {
        use super::super::supervisor::{
            EagerVoiceEngineLoader, FilesystemVoiceModelInstaller, VoiceInputDesiredState,
        };
        use pioneer_protocol::GatewayVoiceInputProvider;
        let supervisor = Arc::new(VoiceInputSupervisor::new(
            Arc::new(FilesystemVoiceModelInstaller::new(
                crate::isolated_test_app_config().unwrap(),
                std::path::PathBuf::new(),
            )),
            Arc::new(EagerVoiceEngineLoader),
        ));
        let generation = supervisor
            .apply_desired(
                VoiceInputDesiredState {
                    enabled: true,
                    provider: Some(GatewayVoiceInputProvider::Local),
                    model: Some("small".to_owned()),
                },
                false,
            )
            .unwrap()
            .generation;
        let replacement = supervisor.clone();
        let calls = Arc::new(Mutex::new(0));
        let count = calls.clone();
        let store = GatewayVoiceSessionBufferStore::default();
        let workers = VoiceFinalizations::default();
        start(
            store.clone(),
            "session".to_owned(),
            VoiceAudioFormat::pioneer_streaming_target(),
            &workers,
            workers.reserve(1).unwrap(),
            SessionSpeechTranscriber {
                supervisor,
                generation,
                is_moonshine_base: false,
                cancellation: CancellationToken::new(),
                fake: Some(Arc::new(move |_| {
                    *count.lock().unwrap() += 1;
                    replacement
                        .apply_desired(VoiceInputDesiredState::default(), false)
                        .unwrap();
                    Ok("stale completed text".to_owned())
                })),
            },
            true,
        )
        .unwrap();
        append_test_packet(&store, "session", 0, &[12000; 1024]);
        let completion = store.finish_processing("session").unwrap().await.unwrap();
        assert_eq!(
            *calls.lock().unwrap(),
            1,
            "generation changed inside the actual final call"
        );
        assert!(matches!(
            completion.outcome,
            Err(VoiceError {
                kind: VoiceErrorKind::TranscriptionFailed,
                ..
            })
        ));
        drop(completion.lease);
        workers.tasks.close();
        workers.tasks.wait().await;
        assert!(!workers.has_active_work());
    }
    #[test]
    fn vad_short_pause_keeps_context_and_preserves_seven_sample_moonshine_tail() {
        let mut audio = vec![12000; 1024];
        audio.extend(vec![0; 16 * VOICE_VAD_FRAME_SAMPLES]);
        audio.extend([6000; 7]);
        let fake = TestSpeechTranscriber(Arc::new(|buffer| {
            assert!(buffer.speech_samples() >= MIN_MOONSHINE_BASE_INPUT_SAMPLES);
            let samples = buffer.flattened_samples();
            assert_eq!(&samples[samples.len() - 7..], &[6000.0 / 32768.0; 7]);
            Ok(if samples.contains(&(6000.0 / 32768.0)) {
                "да, да; ещё раз, ещё раз короткий хвост"
            } else {
                "да, да; ещё раз, ещё раз"
            }
            .to_owned())
        }));
        for packet_samples in [317, 1600] {
            let result = closed_input(&audio, packet_samples, true, &fake).unwrap();
            let GatewayVoiceSessionPipelineOutcome::Transcript {
                transcript,
                signal_stats,
            } = result
            else {
                panic!("VAD transcript expected");
            };
            assert_eq!(transcript.text, "да, да; ещё раз, ещё раз короткий хвост");
            assert_eq!(transcript.diagnostics.total_samples, audio.len());
            assert_eq!(transcript.diagnostics.speech_samples, audio.len());
            assert_eq!(transcript.diagnostics.segment_count, 1);
            assert_eq!(signal_stats.total_samples, audio.len());
            assert_eq!(signal_stats.non_zero_samples, 1024 + 7);
        }
    }

    #[test]
    fn prepared_mapping_excludes_removed_silence_and_survives_partial_evictions() {
        let mut timeline = AudioTimeline::default();
        let mut buffered = PreparedSourceMap::default();
        // Two real retained edges around a discarded source interval. ASR time
        // is contiguous at 512 while source jumps from 512 to 8192.
        let intervals = [
            VadInterval {
                start: 0,
                end: 512,
                probability: 0.0,
                voice: false,
                quiet_run_start: 0,
            },
            VadInterval {
                start: 8192,
                end: 8704,
                probability: 0.0,
                voice: false,
                quiet_run_start: 0,
            },
            VadInterval {
                start: 8704,
                end: 9216,
                probability: 0.9,
                voice: true,
                quiet_run_start: 8704,
            },
        ];
        buffered.append(&intervals);
        timeline.append(0, 0, 512, &intervals[..1]);
        timeline.append(512, 8192, 9216, &intervals[1..]);
        assert_eq!(buffered.spans.len(), 2);
        assert_eq!(buffered.real_samples, 1536);
        for (pcm, source) in [
            (0, 0),
            (511, 511),
            (512, 8192),
            (1023, 8703),
            (1024, 8704),
            (1535, 9215),
        ] {
            assert_eq!(timeline.source_at(pcm), Some(source));
            assert_eq!(timeline.prepared_at(source), Some(pcm));
            assert_eq!(buffered.source_at(pcm), Some(source));
        }
        assert_eq!(timeline.prepared_at(4096), None);
        assert_eq!(buffered.source_at(1536), None); // includes native-only padding
        assert_eq!(timeline.candidates(None), vec![(512, 8704)]);
        timeline.retain_from(700);
        assert_eq!(timeline.source_at(699), None);
        assert_eq!(timeline.source_at(700), Some(8380));
        for ix in 0..1600 {
            let prepared = 1536 + ix * 512;
            let source = 9216 + ix * 512;
            timeline.append(
                prepared,
                source,
                source + 512,
                &[VadInterval {
                    start: source,
                    end: source + 512,
                    probability: 0.9,
                    voice: true,
                    quiet_run_start: source,
                }],
            );
            let origin = (prepared + 512).saturating_sub(PLANNER_CAP_SAMPLES);
            timeline.retain_from(origin);
            assert_eq!(timeline.source_at(prepared + 511), Some(source + 511));
            if origin > 0 {
                assert_eq!(timeline.source_at(origin - 1), None);
            }
            assert!(timeline.intervals.len() <= PLANNER_CAP_SAMPLES.div_ceil(512) + 1);
        }
    }
}
