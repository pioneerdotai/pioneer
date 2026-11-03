//! Session-owned contextual ASR over the existing one-shot engines.
//! Replay is bounded to the unresolved span and a short confirmed left context.
//! Packet boundaries never confirm text. Long pauses/Commit or a bounded window
//! projection advances the confirmed prefix; projection is an ASR heuristic.
#[cfg(test)]
use super::transcription::{VoiceTranscriptionErrorKind, transcription_error};
use super::{
    finalization::{VoiceFinalizationLease, VoiceFinalizations},
    session_buffer::{GatewayVoiceSessionBufferStore, VoiceChunkIngestError},
    silero::create_voice_detector,
    supervisor::VoiceInputSupervisor,
    transcription::{
        DecodingCompletion, PreparedSpeechBuffer, VoiceNativeSession, VoiceSpeechHypothesis,
        VoiceSpeechTranscriber, VoiceTranscript, VoiceTranscriptionDiagnostics,
        VoiceTranscriptionError, VoiceTranscriptionNoSpeech, VoiceTranscriptionNoSpeechReason,
        VoiceTranscriptionStrategy,
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

// Source-duration engineering bounds, not measured quality/latency optima.
// Energy-only fallback scans real PCM at the original 30 ms resolution.
// This is a cut-search grid, NOT the neural detector input frame contract.
const BOUNDARY_SCAN_SAMPLES: usize = 480;
const REFRESH_SAMPLES: usize = 3 * VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ as usize;
const MIN_ADVANCE_SAMPLES: usize = 2 * REFRESH_SAMPLES;
const LEFT_CONTEXT_SAMPLES: usize = REFRESH_SAMPLES / 2;
const MAX_ALIGNMENT_UNITS: usize = 2048;
const MAX_CONTEXT_ANCHOR_BYTES: usize = 256;
const MAX_SPEECH_WINDOW_SAMPLES: usize = 6 * REFRESH_SAMPLES;
// Pause policy: retain 3 s unresolved + 1.5 s real confirmed left context,
// within the existing 18 s window. 1.2 s quiet includes VAD's 448 ms hangover.
// A carried span gets at most two continuations and 6 s of input-time lifetime.
// Oversized text is sealed instead of retaining an unbounded linguistic history.
const PAUSE_WINDOW_SAMPLES: usize = REFRESH_SAMPLES + LEFT_CONTEXT_SAMPLES;
// Keep the original 1.2 s source-time target, independent of VAD framing.
const PAUSE_SEAL_SAMPLES: usize = 16000 * 1200 / 1000;
const PAUSE_LIFETIME_SAMPLES: usize = 2 * REFRESH_SAMPLES;
const MAX_PAUSE_CONTINUATIONS: usize = 2;
const MAX_PAUSE_TEXT_BYTES: usize = 512;
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
    fn transcribe_prepared_hypothesis(
        &self,
        buffer: &PreparedSpeechBuffer,
        real_pcm_samples: usize,
    ) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError> {
        #[cfg(test)]
        if self.fake.is_some() {
            return self.transcribe_hypothesis(buffer);
        }
        self.supervisor.transcribe_prepared_for_session(
            buffer,
            real_pcm_samples,
            self.generation,
            &self.cancellation,
        )
    }

    fn begin_native_session(
        &self,
    ) -> Result<Option<Box<dyn VoiceNativeSession>>, VoiceTranscriptionError> {
        #[cfg(test)]
        if self.fake.is_some() {
            return Ok(None);
        }
        let state = self.supervisor.native_session_operation(
            self.generation,
            &self.cancellation,
            |engine| engine.begin_native_session(),
        )?;
        Ok(state.map(|state| {
            Box::new(SessionNativeTranscriber {
                supervisor: self.supervisor.clone(),
                generation: self.generation,
                cancellation: self.cancellation.clone(),
                state: Some(state),
            }) as Box<dyn VoiceNativeSession>
        }))
    }

    fn transcribe_speech(
        &self,
        buffer: &PreparedSpeechBuffer,
    ) -> Result<String, VoiceTranscriptionError> {
        self.transcribe_hypothesis(buffer)?
            .ensure_complete()
            .map(|result| result.text)
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

struct SessionNativeTranscriber {
    supervisor: Arc<VoiceInputSupervisor>,
    generation: u64,
    cancellation: CancellationToken,
    state: Option<super::runtime::NativeVoiceEngineSession>,
}

impl SessionNativeTranscriber {
    fn closed_error() -> VoiceTranscriptionError {
        super::transcription::transcription_error(
            super::transcription::VoiceTranscriptionErrorKind::RuntimeFailure,
            "native voice segment is closed",
        )
    }
}

impl VoiceNativeSession for SessionNativeTranscriber {
    fn feed(&mut self, samples: &[f32]) -> Result<(), VoiceTranscriptionError> {
        let state = self.state.as_mut().ok_or_else(Self::closed_error)?;
        let result = self.supervisor.native_session_operation(
            self.generation,
            &self.cancellation,
            |engine| engine.feed_native_session(state, samples),
        );
        if result.is_err() {
            self.state = None;
        }
        result
    }

    fn hypothesis(&mut self) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError> {
        let state = self.state.as_mut().ok_or_else(Self::closed_error)?;
        let result = self.supervisor.native_session_operation(
            self.generation,
            &self.cancellation,
            |engine| engine.native_hypothesis(state),
        );
        if result.is_err() {
            self.state = None;
        }
        result
    }

    fn finish(&mut self) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError> {
        let state = self.state.take().ok_or_else(Self::closed_error)?;
        let result = self.supervisor.native_session_operation(
            self.generation,
            &self.cancellation,
            |engine| engine.finish_native_session(state),
        )?;
        if result.completion != DecodingCompletion::Normal {
            return Err(super::transcription::transcription_error(
                super::transcription::VoiceTranscriptionErrorKind::RuntimeFailure,
                "native voice segment did not reach normal completion",
            ));
        }
        result.ensure_complete()
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
                // Cached candidates must also respect replacement generations,
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
// handling as incremental, but never invokes its bounded-window/text planner.
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
            .transcribe_prepared_hypothesis(&buffer, speech_samples)
            .and_then(VoiceSpeechHypothesis::ensure_complete)
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
    state.native = transcriber
        .begin_native_session()
        .map_err(|error| error.into_voice_error())?;
    state.native_enabled = state.native.is_some();
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
                        state.resume(store, session_id, cancellation, transcriber)?;
                        state.accept_speech(
                            &samples,
                            store,
                            session_id,
                            cancellation,
                            transcriber,
                        )?;
                    }
                    SmoothedVadFrame::Noise => {
                        state.pause(store, session_id, cancellation, transcriber)?
                    }
                }
            }
        }
        // A skipped preview must not wait for another full stride after the
        // worker catches up (e.g. 4 s already queued when a 3 s preview was due).
        // Decode the latest available span before waiting for more audio, only
        // after at least one stride of new source since the last hypothesis.
        if state.speech.len().saturating_sub(state.decoded_samples) >= REFRESH_SAMPLES
            && !store.processing_skip_preview(session_id)
        {
            state.refresh(store, session_id, cancellation, transcriber)?;
        }
        if matches!(state.pause, PauseState::Holding(_))
            && !store.processing_skip_preview(session_id)
        {
            state.prepare_pause(store, session_id, cancellation, transcriber)?;
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
        state.resume(store, session_id, cancellation, transcriber)?;
        state.accept_speech(&tail, store, session_id, cancellation, transcriber)?;
    }
    state.flush(store, session_id, cancellation, transcriber)?;
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

// The existing session owner also owns pause transitions. Holding is quiet;
// Resumed keeps the same deadline/replay budget across short repeated pauses.
// A first unconfirmed onset with no owned speech never creates a held context.
#[derive(Clone, Copy)]
struct PauseContext {
    started_at: usize,
    continuations: usize,
}

#[derive(Clone, Copy, Default)]
enum PauseState {
    #[default]
    Speech,
    Holding(PauseContext),
    Resumed(PauseContext),
}

// Buffered mode needs source spans through its single ASR call, but no VAD
// probability history. Merge adjacent emissions; space is bounded by admitted
// source samples and only genuinely removed gaps add another span.
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
// unique real ASR PCM, before per-call replay/padding; source counts ALL input.
// Only intervals still supporting the current <=18 s window are retained.
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

    fn pause_boundary(
        &self,
        origin: usize,
        earliest: usize,
        latest: usize,
        preferred: usize,
    ) -> Option<usize> {
        let mut best: Option<(usize, usize, f32)> = None; // cut, real quiet duration, mean probability
        let mut run: Option<(usize, usize, usize, usize, f32, usize)> = None;
        let mut run_start = None;
        let mut consider = |run: (usize, usize, usize, usize, f32, usize)| {
            let (source_start, source_end, pcm_start, pcm_end, sum, count) = run;
            let duration = source_end - source_start;
            if duration < 2 * VOICE_VAD_FRAME_SAMPLES {
                return;
            }
            let first = pcm_start.max(origin + earliest);
            let last = pcm_end.min(origin + latest);
            if first > last {
                return;
            }
            // Source midpoint can lie in removed silence. In that case use its
            // real concatenation seam, never pretend source time is ASR time.
            let middle = source_start + duration / 2;
            let mapped = self
                .prepared_at(middle)
                .or_else(|| {
                    self.intervals
                        .iter()
                        .find(|interval| interval.source.start >= middle)
                        .map(|interval| interval.prepared)
                })
                .unwrap_or(pcm_end);
            let cut = mapped.clamp(first, last) - origin;
            let mean = sum / count as f32;
            if best.is_none_or(|(prior, length, probability)| {
                duration > length
                    || (duration == length
                        && (cut.abs_diff(preferred), mean)
                            .partial_cmp(&(prior.abs_diff(preferred), probability))
                            == Some(std::cmp::Ordering::Less))
            }) {
                best = Some((cut, duration, mean));
            }
        };
        for interval in &self.intervals {
            let source = interval.source;
            // Use the same hysteretic VAD decision as smoothing/quiet clock.
            if source.voice {
                if let Some(quiet) = run.take() {
                    consider(quiet);
                }
                run_start = None;
            } else {
                // A source gap can hide an unconfirmed positive frame that
                // prefill later evicted. Only full-input VAD continuity proves
                // that retained quiet edges belong to the same pause.
                if run_start != Some(source.quiet_run_start) {
                    if let Some(quiet) = run.take() {
                        consider(quiet);
                    }
                    run_start = Some(source.quiet_run_start);
                }
                let pcm_end = interval.prepared + source.end - source.start;
                match run.as_mut() {
                    Some(quiet) => {
                        quiet.1 = source.end;
                        quiet.3 = pcm_end;
                        quiet.4 += source.probability;
                        quiet.5 += 1;
                    }
                    None => {
                        run = Some((
                            source.start,
                            source.end,
                            interval.prepared,
                            pcm_end,
                            source.probability,
                            1,
                        ))
                    }
                }
            }
        }
        if let Some(quiet) = run {
            consider(quiet);
        }
        best.map(|(cut, _, _)| cut)
    }
}

struct IncrementalSpeech<D> {
    native: Option<Box<dyn VoiceNativeSession>>,
    native_enabled: bool,
    is_moonshine_base: bool,
    vad: SmoothedVoiceVad<D>,
    pending: Vec<f32>,
    speech: Vec<f32>,
    timeline: AudioTimeline,
    window_start_sample: usize, // cursor in the unique VAD-emitted speech stream
    // Unique VAD-emitted source samples, independent of context replay/padding.
    speech_samples: usize,
    fragments: usize,
    text: String,       // confirmed prefix; never changed by a preview
    hypothesis: String, // replaceable text for the unconfirmed part only
    full_hypothesis: VoiceSpeechHypothesis,
    owned_text_start: usize,
    context_samples: usize, // already confirmed PCM at the start of speech
    context_text: String,
    // Preview cadence counts new source separately from candidate validity:
    // projection carries a stride origin, but changes PCM/ownership boundaries.
    decoded_samples: usize,
    candidate_current: bool,
    next_refresh_samples: usize,
    signal: SignalAccumulator,
    source_samples: usize, // processed input, including discarded silence
    pause: PauseState,
    pause_projection_deferred: bool,
    // A consensus prefix confirmed WITHOUT evicting any uncertain audio. This
    // bounded reference excludes it on the next changed whole hypothesis.
    held_prefix: Option<String>,
}
impl<D: VoiceActivityDetector> IncrementalSpeech<D> {
    fn new(is_moonshine_base: bool, detector: D) -> Result<Self, VoiceError> {
        Ok(Self {
            native: None,
            native_enabled: false,
            is_moonshine_base,
            vad: SmoothedVoiceVad::new(detector, VoiceVadConfig::default())
                .map_err(|_| worker_error("failed to initialize voice VAD"))?,
            pending: Vec::with_capacity(VOICE_VAD_FRAME_SAMPLES),
            speech: Vec::with_capacity(MAX_SPEECH_WINDOW_SAMPLES),
            timeline: AudioTimeline::default(),
            window_start_sample: 0,
            speech_samples: 0,
            fragments: 0,
            text: String::new(),
            hypothesis: String::new(),
            full_hypothesis: VoiceSpeechHypothesis::default(),
            owned_text_start: 0,
            context_samples: 0,
            context_text: String::new(),
            decoded_samples: 0,
            candidate_current: false,
            next_refresh_samples: REFRESH_SAMPLES,
            signal: SignalAccumulator::default(),
            source_samples: 0,
            pause: PauseState::Speech,
            pause_projection_deferred: false,
            held_prefix: None,
        })
    }

    fn pause(
        &mut self,
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        if self.speech.len() == self.context_samples {
            return Ok(());
        }
        let entering = !matches!(self.pause, PauseState::Holding(_));
        let context = match self.pause {
            PauseState::Speech => PauseContext {
                started_at: self.source_samples.saturating_sub(self.vad.quiet_samples()),
                continuations: 0,
            },
            PauseState::Holding(context) | PauseState::Resumed(context) => context,
        };
        self.pause = PauseState::Holding(context);
        if self.vad.quiet_samples() >= PAUSE_SEAL_SAMPLES
            || self.source_samples.saturating_sub(context.started_at) >= PAUSE_LIFETIME_SAMPLES
        {
            return self.flush(store, id, cancellation, transcriber);
        }
        if entering {
            self.prepare_pause(store, id, cancellation, transcriber)?;
        }
        Ok(())
    }

    fn prepare_pause(
        &mut self,
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        if self.native_enabled {
            // The native acoustic segment has no projected/committed prefix.
            // Keep the existing endpoint lifetime; no one-shot partition probes.
            if !store.processing_skip_preview(id) {
                self.refresh(store, id, cancellation, transcriber)?;
            }
            if self.full_hypothesis.text.len() > MAX_PAUSE_TEXT_BYTES {
                self.flush(store, id, cancellation, transcriber)?;
            }
            return Ok(());
        }
        if self.speech.len() > PAUSE_WINDOW_SAMPLES && !self.pause_projection_deferred {
            // The same ownership projection as a full window, with a fixed
            // source fence. At most whole + two partition probes + left probe;
            // never retry to force textual agreement.
            // Retained = left context + (len - cut) <= 4.5 s. Keep at
            // least 1.5 s unresolved, advance owned PCM by >= one frame, and
            // leave enough left context. Unlike a symmetric tolerance these
            // one-sided bounds cannot exceed the retained-window budget.
            let earliest = (self.speech.len() + LEFT_CONTEXT_SAMPLES - PAUSE_WINDOW_SAMPLES)
                .max(self.context_samples + VOICE_VAD_FRAME_SAMPLES)
                .max(LEFT_CONTEXT_SAMPLES);
            let latest = self.speech.len() - LEFT_CONTEXT_SAMPLES;
            let cut = self.preferred_boundary(earliest, latest);
            if self.project_window(
                cut,
                earliest,
                latest,
                false,
                store,
                id,
                cancellation,
                transcriber,
            )? {
                self.next_refresh_samples = self.speech.len() + REFRESH_SAMPLES;
            } else {
                // Keep the disputed text AND its supporting PCM. No retry on
                // every quiet frame; a changed span or mandatory cap ends this
                // deferral. Lifetime, text and final quiet budgets still apply.
                self.pause_projection_deferred = true;
            }
        }
        if !store.processing_skip_preview(id) {
            self.refresh(store, id, cancellation, transcriber)?;
        }
        if self.full_hypothesis.text.len() > MAX_PAUSE_TEXT_BYTES
            || self.context_text.len() > MAX_PAUSE_TEXT_BYTES
        {
            self.flush(store, id, cancellation, transcriber)?;
        }
        Ok(())
    }

    fn resume(
        &mut self,
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        let (context, entering) = match self.pause {
            PauseState::Speech => return Ok(()),
            PauseState::Holding(context) => (context, true),
            PauseState::Resumed(context) => (context, false),
        };
        if self.source_samples.saturating_sub(context.started_at) >= PAUSE_LIFETIME_SAMPLES
            || (entering && context.continuations == MAX_PAUSE_CONTINUATIONS)
        {
            self.flush(store, id, cancellation, transcriber)?;
        } else {
            self.pause = PauseState::Resumed(PauseContext {
                continuations: context.continuations + usize::from(entering),
                ..context
            });
        }
        Ok(())
    }

    fn accept_speech(
        &mut self,
        mut samples: &[f32],
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        self.pause_projection_deferred = false;
        let intervals = self.vad.take_emitted_intervals();
        let mut source = self.source_samples - samples.len();
        while !samples.is_empty() {
            if self.speech.len() == MAX_SPEECH_WINDOW_SAMPLES {
                // Mandatory resource work, independent of preview cadence and
                // Commit. At exactly the cap with no further audio, do nothing.
                self.advance_window(store, id, cancellation, transcriber)?;
                self.next_refresh_samples =
                    (self.speech.len() + REFRESH_SAMPLES).min(MAX_SPEECH_WINDOW_SAMPLES);
            }
            let room = MAX_SPEECH_WINDOW_SAMPLES - self.speech.len();
            let until_refresh = self.next_refresh_samples - self.speech.len();
            let take = samples.len().min(room).min(until_refresh);
            if self.native_enabled {
                if self.native.is_none() {
                    self.native = transcriber
                        .begin_native_session()
                        .map_err(|error| error.into_voice_error())?;
                }
                self.native
                    .as_mut()
                    .ok_or_else(|| worker_error("native voice capability disappeared"))?
                    .feed(&samples[..take])
                    .map_err(|error| error.into_voice_error())?;
                ensure_active(cancellation)?;
            }
            self.timeline
                .append(self.speech_samples, source, source + take, &intervals);
            debug_assert_eq!(self.timeline.source_at(self.speech_samples), Some(source));
            source += take;
            self.speech.extend_from_slice(&samples[..take]);
            self.candidate_current = false;
            self.speech_samples += take;
            samples = &samples[take..];
            if self.speech.len() == self.next_refresh_samples {
                if self.speech.len().saturating_sub(self.decoded_samples) >= REFRESH_SAMPLES
                    && !store.processing_skip_preview(id)
                {
                    self.refresh(store, id, cancellation, transcriber)?;
                }
                self.next_refresh_samples =
                    (self.speech.len() + REFRESH_SAMPLES).min(MAX_SPEECH_WINDOW_SAMPLES);
            }
        }
        debug_assert_eq!(
            self.window_start_sample + self.speech.len(),
            self.speech_samples
        );
        Ok(())
    }

    fn refresh(
        &mut self,
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        if !self.candidate_current || self.decoded_samples != self.speech.len() {
            if self.native_enabled {
                self.full_hypothesis = self
                    .native
                    .as_mut()
                    .ok_or_else(|| worker_error("native voice segment is missing"))?
                    .hypothesis()
                    .map_err(|error| error.into_voice_error())?;
                ensure_active(cancellation)?;
                self.hypothesis = self.full_hypothesis.text.clone();
                self.decoded_samples = self.speech.len();
                self.candidate_current = true;
                return Ok(());
            }
            let full = self.decode(&self.speech, store, id, cancellation, transcriber)?;
            let start = if let Some(prefix) = &self.held_prefix {
                // If native timing also validates this exact prefix, prefer it.
                // Otherwise the same occurrence-preserving alignment as context
                // exclusion maps the immutable text reference into the new whole.
                timed_boundaries(&full, self.speech.len())
                    .and_then(|boundaries| {
                        boundaries
                            .into_iter()
                            .find(|boundary| {
                                boundary.text_end == prefix.len()
                                    && full.text.get(..boundary.text_end) == Some(prefix.as_str())
                                    && is_text_boundary(&full.text, boundary.text_end)
                            })
                            .map(|boundary| boundary.text_end)
                    })
                    .unwrap_or_else(|| align_boundary(&full.text, prefix, &self.hypothesis))
            } else if self.context_samples == 0 {
                0
            } else {
                timed_boundaries(&full, self.speech.len())
                    .and_then(|boundaries| timed_text_start(&boundaries, self.context_samples))
                    .filter(|start| is_text_boundary(&full.text, *start))
                    .unwrap_or_else(|| {
                        align_boundary(&full.text, &self.context_text, &self.hypothesis)
                    })
            };
            self.hypothesis = full.text[start..].trim().to_owned();
            self.owned_text_start = start;
            self.full_hypothesis = full;
            self.decoded_samples = self.speech.len();
            self.candidate_current = true;
        }
        Ok(())
    }

    fn advance_window(
        &mut self,
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        if self.native_enabled {
            // Resource boundary: finalize the entire bounded native segment,
            // then start a fresh disjoint segment on the next accepted PCM.
            return self.flush(store, id, cancellation, transcriber);
        }
        let earliest = self.context_samples + MIN_ADVANCE_SAMPLES;
        let latest = self.speech.len() - REFRESH_SAMPLES;
        let preferred_cut = self.preferred_boundary(earliest, latest);
        self.project_window(
            preferred_cut,
            earliest,
            latest,
            true,
            store,
            id,
            cancellation,
            transcriber,
        )
        .map(|_| ())
    }

    fn preferred_boundary(&self, earliest: usize, latest: usize) -> usize {
        let preferred = (earliest + latest) / 2;
        self.timeline
            .pause_boundary(self.window_start_sample, earliest, latest, preferred)
            .unwrap_or_else(|| partition_boundary_in(&self.speech, earliest, latest))
    }

    fn project_window(
        &mut self,
        preferred_cut: usize,
        earliest: usize,
        latest: usize,
        mandatory: bool,
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<bool, VoiceError> {
        self.refresh(store, id, cancellation, transcriber)?;
        let timing = timed_boundaries(&self.full_hypothesis, self.speech.len());
        let timed_cut = timing.as_ref().and_then(|boundaries| {
            boundaries
                .iter()
                .filter(|boundary| {
                    boundary.end >= earliest
                        && boundary.end <= latest
                        && boundary.text_end >= self.owned_text_start
                        && is_text_boundary(&self.full_hypothesis.text, boundary.text_end)
                })
                // All text with the same reported end belongs on the same
                // side; retaining a later zero-duration word as tail would
                // lose it when that time is excluded as confirmed context.
                .min_by_key(|boundary| {
                    (
                        boundary.end.abs_diff(preferred_cut),
                        std::cmp::Reverse(boundary.text_end),
                    )
                })
        });
        let (cut, text_end) = if let Some(boundary) = timed_cut {
            (boundary.end, boundary.text_end)
        } else {
            // Both probes own known disjoint PCM intervals. Global sequence
            // alignment projects their partition into the contextual whole;
            // without overlap deduplication or rejecting disagreement.
            let left = self.decode(
                &self.speech[..preferred_cut],
                store,
                id,
                cancellation,
                transcriber,
            )?;
            let right = self.decode(
                &self.speech[preferred_cut..],
                store,
                id,
                cancellation,
                transcriber,
            )?;
            let projection = boundary_projection(
                &self.full_hypothesis.text,
                &left.text,
                &right.text,
                Some((preferred_cut, self.speech.len())),
            );
            if projection.ambiguous() && !mandatory {
                // Every optimal candidate assigns this prefix to the left. It
                // can become immutable while ALL supporting PCM stays owned.
                // Do not keep an unbounded textual reference across new audio.
                let common = projection.first.max(self.owned_text_start);
                if common > self.owned_text_start && common <= MAX_PAUSE_TEXT_BYTES {
                    append_text(
                        &mut self.text,
                        &self.full_hypothesis.text[self.owned_text_start..common],
                        self.owned_text_start > 0,
                    );
                    self.fragments += 1;
                    self.held_prefix = Some(self.full_hypothesis.text[..common].to_owned());
                    self.owned_text_start = common;
                    self.hypothesis = self.full_hypothesis.text[common..].trim().to_owned();
                }
                return Ok(false);
            }
            // At the hard cap the finite fallback is the best-cost candidate
            // nearest the PCM partition, then prefix fit and original order.
            // Equal-cost alternatives are evidence of ambiguity, not confidence.
            (preferred_cut, projection.selected)
        };
        let text_end = text_end.max(self.owned_text_start);
        // Keep actual left audio across the boundary, including a word that may
        // straddle the heuristic cut. Record its hypothesis for later alignment.
        let context_start = cut - LEFT_CONTEXT_SAMPLES;
        let context = self.decode(
            &self.speech[context_start..cut],
            store,
            id,
            cancellation,
            transcriber,
        )?;
        // An isolated probe's omission cannot revoke known ownership. Usable
        // timing can locate several confirmed words inside the retained PCM;
        // do not collapse that reference to just its last repeated word. Without
        // such evidence retain only a bounded nearest-unit heuristic, never the
        // entire prefix containing words outside the actual replayed context.
        let context_text = if context.text.is_empty() {
            timing
                .as_deref()
                .and_then(|boundaries| {
                    timed_context_anchor(&self.full_hypothesis.text, boundaries, context_start, cut)
                })
                .map(str::to_owned)
                .unwrap_or_else(|| confirmed_context_anchor(&self.full_hypothesis.text[..text_end]))
        } else {
            context.text
        };
        let tail = self.full_hypothesis.text[text_end..].to_owned();
        // Transfer the projected left text BEFORE discarding its PCM. Only the
        // projected right slice remains replaceable on the next refresh. A
        // textual tie here must not use the context-exclusion bias in refresh:
        // that could leave the entire old whole in a now unsupported tail.
        append_text(
            &mut self.text,
            &self.full_hypothesis.text[self.owned_text_start..text_end],
            self.owned_text_start > 0,
        );
        self.fragments += 1;
        self.speech.drain(..context_start);
        self.window_start_sample += context_start;
        self.timeline.retain_from(self.window_start_sample);
        self.context_samples = LEFT_CONTEXT_SAMPLES;
        self.context_text = context_text;
        self.held_prefix = None;
        // Carry the projected ownership reference, but it is not a final
        // candidate for the changed PCM/ownership boundaries. Refresh once
        // before Commit can reuse it, even if no new source arrived.
        let mut carried = self.context_text.clone();
        append_text(&mut carried, &tail, true);
        self.owned_text_start = self.context_text.len();
        self.full_hypothesis = VoiceSpeechHypothesis::text_only(carried);
        self.hypothesis = tail.trim().to_owned();
        self.decoded_samples = self.speech.len();
        self.candidate_current = false;
        Ok(true)
    }

    fn flush(
        &mut self,
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        if self.speech.len() == self.context_samples {
            return Ok(());
        }
        // Reuse only an identical completed span. A changed tail MUST succeed;
        // an error cannot publish earlier successful previews as a partial turn.
        if self.native_enabled {
            self.full_hypothesis = self
                .native
                .take()
                .ok_or_else(|| worker_error("native voice segment is missing"))?
                .finish()
                .and_then(VoiceSpeechHypothesis::ensure_complete)
                .map_err(|error| error.into_voice_error())?;
            ensure_active(cancellation)?;
        } else {
            self.refresh(store, id, cancellation, transcriber)?;
        }
        append_text(
            &mut self.text,
            &self.full_hypothesis.text[self.owned_text_start..],
            self.owned_text_start > 0,
        );
        self.fragments += 1;
        self.window_start_sample += self.speech.len();
        self.speech.clear();
        self.timeline.intervals.clear();
        self.hypothesis.clear();
        self.full_hypothesis = VoiceSpeechHypothesis::default();
        self.owned_text_start = 0;
        self.context_samples = 0;
        self.context_text.clear();
        self.decoded_samples = 0;
        self.candidate_current = false;
        self.next_refresh_samples = REFRESH_SAMPLES;
        self.pause = PauseState::Speech;
        self.pause_projection_deferred = false;
        self.held_prefix = None;
        Ok(())
    }

    fn decode(
        &self,
        source: &[f32],
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<VoiceSpeechHypothesis, VoiceError> {
        ensure_active(cancellation)?;
        let mut samples = source.to_vec();
        // Padding is native input only; source diagnostics are counted on accept.
        if self.is_moonshine_base {
            prepare_moonshine_base_samples(&mut samples);
        }
        let native_count = samples.len();
        let buffer = PreparedSpeechBuffer {
            sample_rate_hz: VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ,
            total_samples: native_count,
            segments: vec![VoiceSpeechSegment {
                start_sample: 0,
                end_sample: native_count,
                samples,
            }],
        };
        // A call already in progress at Commit keeps its original startup scope.
        let key = store.processing_startup_key(id);
        let mut raw = pioneer_observability::turn_startup::scope_sync(key, || {
            transcriber.transcribe_prepared_hypothesis(&buffer, source.len())
        })
        .map_err(|error| error.into_voice_error())?;
        ensure_active(cancellation)?;
        raw = raw
            .ensure_complete()
            .map_err(|error| error.into_voice_error())?;
        raw.text = raw.text.trim().to_owned();
        if raw
            .segments
            .as_ref()
            .is_some_and(|segments| segments.len() > MAX_ALIGNMENT_UNITS)
        {
            raw.segments = None;
        }
        Ok(raw)
    }
}

// Candidate only: low energy is not proof of a word boundary. Native timing
// takes priority; text-only engines use bounded full-sequence alignment below.
fn partition_boundary_in(samples: &[f32], earliest: usize, latest: usize) -> usize {
    let first = earliest.div_ceil(BOUNDARY_SCAN_SAMPLES);
    let last = latest / BOUNDARY_SCAN_SAMPLES;
    if first > last {
        return (earliest + latest) / 2;
    }
    let midpoint = (first + last) / 2;
    (first..=last)
        .min_by(|a, b| {
            let energy = |frame: usize| {
                let cut = frame * BOUNDARY_SCAN_SAMPLES;
                samples[cut - BOUNDARY_SCAN_SAMPLES..cut + BOUNDARY_SCAN_SAMPLES]
                    .iter()
                    .map(|sample| f64::from(*sample).powi(2))
                    .sum::<f64>()
            };
            energy(*a)
                .total_cmp(&energy(*b))
                .then_with(|| a.abs_diff(midpoint).cmp(&b.abs_diff(midpoint)))
        })
        .expect("partition interval is nonempty")
        * BOUNDARY_SCAN_SAMPLES
}

struct TimedBoundary {
    start: usize,
    end: usize,
    text_end: usize,
}

// SDK "segments" can be subword tokens. Timing alone must not confirm a
// fragment inside a whitespace-delimited word or separate its punctuation.
fn is_text_boundary(text: &str, end: usize) -> bool {
    text.is_char_boundary(end)
        && (end == 0 || end == text.len() || text[end..].starts_with(char::is_whitespace))
}

fn timed_boundaries(
    hypothesis: &VoiceSpeechHypothesis,
    samples: usize,
) -> Option<Vec<TimedBoundary>> {
    let segments = hypothesis.segments.as_ref()?;
    if segments.is_empty() || segments.len() > MAX_ALIGNMENT_UNITS {
        return None;
    }
    let mut result: Vec<TimedBoundary> = Vec::with_capacity(segments.len());
    let mut cursor = 0;
    let mut previous_end = 0.0;
    for segment in segments {
        if !segment.start.is_finite()
            || !segment.end.is_finite()
            || segment.start < previous_end
            || segment.end < segment.start
            || segment.start < 0.0
        {
            return None;
        }
        let start = (f64::from(segment.start) * 16000.0).round() as usize;
        let end = (f64::from(segment.end) * 16000.0).round() as usize;
        if end > samples {
            return None;
        }
        let rest = &hypothesis.text[cursor..];
        let trimmed = rest.trim_start();
        cursor += rest.len() - trimmed.len();
        let text = segment.text.trim();
        if text.is_empty() || !trimmed.starts_with(text) {
            return None;
        }
        cursor += text.len();
        // The audio fence cannot separate segments with the same end sample.
        // Coalesce BEFORE testing text boundaries, including when the final
        // coincident token ends inside the next word.
        match result.last_mut() {
            Some(previous) if previous.end == end => previous.text_end = cursor,
            _ => result.push(TimedBoundary {
                start,
                end,
                text_end: cursor,
            }),
        }
        previous_end = segment.end;
    }
    hypothesis.text[cursor..]
        .trim()
        .is_empty()
        .then_some(result)
}

// Coarse segments straddling the ownership fence cannot separate context.
fn timed_text_start(boundaries: &[TimedBoundary], context_samples: usize) -> Option<usize> {
    let mut text_end = 0;
    for boundary in boundaries {
        if boundary.end <= context_samples {
            text_end = boundary.text_end;
        } else if boundary.start >= context_samples {
            return Some(text_end);
        } else {
            return None;
        }
    }
    Some(text_end)
}

// Both PCM fences must map between complete text units in already validated
// metadata. A straddling SDK segment/subword or an over-budget span cannot give
// this bounded reference exact coverage; callers then use the textual heuristic.
fn timed_context_anchor<'a>(
    text: &'a str,
    boundaries: &[TimedBoundary],
    start: usize,
    end: usize,
) -> Option<&'a str> {
    let first = timed_text_start(boundaries, start)?;
    let last = timed_text_start(boundaries, end)?;
    if first > last || !is_text_boundary(text, first) || !is_text_boundary(text, last) {
        return None;
    }
    let reference = text[first..last].trim();
    // Some("") is evidence that retained PCM owns no text. Only None may
    // substitute a nearest-unit heuristic; doing so for an empty gap would
    // wrongly assign an earlier confirmed word to this context.
    (reference.len() <= MAX_CONTEXT_ANCHOR_BYTES).then_some(reference)
}

// Bounded global edit alignment. Every original token occurrence participates;
// only the comparison key is normalized. Output retains the whole hypothesis's
// spelling, punctuation and repetitions. No timestamps are inferred here.
struct AlignmentUnit<'a> {
    raw: &'a str,
    key: String,
    end: usize,
}

fn alignment_units(text: &str, scalar: bool) -> Vec<AlignmentUnit<'_>> {
    let count = alignment_spans(text, scalar).count();
    let group = count.div_ceil(MAX_ALIGNMENT_UNITS).max(1);
    let mut units = Vec::with_capacity(count.min(MAX_ALIGNMENT_UNITS));
    let mut start = 0;
    for (index, (word_start, end)) in alignment_spans(text, scalar).enumerate() {
        if index % group == 0 {
            start = word_start;
        }
        if index % group == group - 1 || index + 1 == count {
            let raw = &text[start..end];
            let key = raw
                .split_whitespace()
                .map(|word| {
                    word.trim_matches(|c: char| !c.is_alphanumeric())
                        .to_lowercase()
                })
                .collect::<Vec<_>>()
                .join(" ");
            units.push(AlignmentUnit { raw, key, end });
        }
    }
    units
}

// Coarse mode keeps spaced alphabetic words intact and separates CJK scalars,
// attaching following punctuation/combining kana marks. Refinement can split
// any script, including marks: comparison units are not asserted word, grapheme
// or SDK token boundaries. Both scans return original UTF-8 byte offsets.
fn alignment_spans(text: &str, scalar: bool) -> impl Iterator<Item = (usize, usize)> + '_ {
    let mut cursor = 0;
    text.split_whitespace().flat_map(move |word| {
        cursor += text[cursor..].find(word).expect("word comes from text");
        let base = cursor;
        cursor += word.len();
        let mut chars = word.char_indices().peekable();
        std::iter::from_fn(move || {
            let (start, first) = chars.next()?;
            while let Some(&(_, next)) = chars.peek() {
                if scalar
                    || is_unspaced_character(next)
                    || (is_unspaced_character(first) && next.is_alphanumeric())
                {
                    break;
                }
                chars.next();
            }
            let end = chars.peek().map_or(word.len(), |(index, _)| *index);
            Some((base + start, base + end))
        })
    })
}

fn is_unspaced_character(c: char) -> bool {
    matches!(c,
        '\u{3400}'..='\u{4dbf}' | '\u{4e00}'..='\u{9fff}' |
        '\u{f900}'..='\u{faff}' | '\u{20000}'..='\u{323af}' |
        '\u{3041}'..='\u{3096}' | '\u{309d}'..='\u{309f}' |
        '\u{30a1}'..='\u{30fa}' | '\u{30fc}'..='\u{30ff}' |
        '\u{31f0}'..='\u{31ff}' | '\u{1100}'..='\u{11ff}' |
        '\u{3130}'..='\u{318f}' | '\u{ac00}'..='\u{d7af}'
    )
}

fn confirmed_context_anchor(confirmed: &str) -> String {
    let Some((start, end)) = alignment_spans(confirmed, false).last() else {
        return String::new();
    };
    let mut start = start.max(end.saturating_sub(MAX_CONTEXT_ANCHOR_BYTES));
    while !confirmed.is_char_boundary(start) {
        start += 1;
    }
    confirmed[start..end].to_owned()
}

fn alignment_cost(a: &AlignmentUnit<'_>, b: &AlignmentUnit<'_>) -> usize {
    if a.raw == b.raw {
        0
    } else if !a.key.is_empty() && a.key == b.key {
        1
    } else {
        3
    }
}

// Last row for all prefixes of target, using O(target) memory.
fn edit_costs(
    source: &[AlignmentUnit<'_>],
    target: &[AlignmentUnit<'_>],
    reverse: bool,
) -> Vec<usize> {
    let mut row: Vec<_> = (0..=target.len()).map(|index| 2 * index).collect();
    for i in 0..source.len() {
        let a = &source[if reverse { source.len() - 1 - i } else { i }];
        let mut diagonal = row[0];
        row[0] = 2 * (i + 1);
        for j in 0..target.len() {
            let b = &target[if reverse { target.len() - 1 - j } else { j }];
            let above = row[j + 1];
            row[j + 1] = (diagonal + alignment_cost(a, b))
                .min(above + 2)
                .min(row[j] + 2);
            diagonal = above;
        }
    }
    row
}

fn align_boundary(whole: &str, left: &str, right: &str) -> usize {
    project_boundary(whole, left, right, None)
}

// A partition transfers ownership before PCM eviction; context exclusion only
// removes technical replay. They need different tie priors: favoring the best
// prefix fit is conservative for exclusion, but can discard left ownership at
// a partition. Neither prior establishes exact acoustic correspondence.
#[derive(Debug)]
struct BoundaryProjection {
    selected: usize,
    first: usize,
    last: usize,
    cost: usize,
    alternative_cost: Option<usize>,
}

impl BoundaryProjection {
    fn ambiguous(&self) -> bool {
        self.first != self.last && self.alternative_cost == Some(self.cost)
    }
}

fn project_boundary(
    whole: &str,
    left: &str,
    right: &str,
    partition: Option<(usize, usize)>,
) -> usize {
    boundary_projection(whole, left, right, partition).selected
}

fn boundary_projection(
    whole: &str,
    left: &str,
    right: &str,
    partition: Option<(usize, usize)>,
) -> BoundaryProjection {
    let mut whole_units = alignment_units(whole, false);
    let mut left_units = alignment_units(left, false);
    let mut right_units = alignment_units(right, false);
    // The partition can lie inside one coarse unit even with other correct
    // words around it. An unmatched boundary probe also needs refinement: a
    // typo must not prevent locating the other probe inside a coarse unit.
    // Matching coarse words stay intact (including long repeated sequences).
    // This is resolution selection, not evidence of confirmed ownership.
    let unmatched = |probe: &AlignmentUnit<'_>| {
        !whole_units
            .iter()
            .any(|unit| unit.raw == probe.raw || (!probe.key.is_empty() && unit.key == probe.key))
    };
    let scalar = left_units.last().is_some_and(unmatched)
        || right_units.first().is_some_and(unmatched)
        || left_units
            .last()
            .zip(right_units.first())
            .is_some_and(|(left, right)| {
                whole_units.len() == 1
                    || whole_units.iter().any(|unit| {
                        unit.raw
                            .strip_prefix(left.raw)
                            .is_some_and(|rest| rest.starts_with(right.raw))
                            || (!left.key.is_empty()
                                && !right.key.is_empty()
                                && unit
                                    .key
                                    .strip_prefix(left.key.as_str())
                                    .is_some_and(|rest| rest.starts_with(right.key.as_str())))
                    })
            });
    if scalar {
        // Use one shared resolution for ALL sequences, then only one global
        // edit-distance projection. Both modes retain the 2,048-unit cap and
        // original byte offsets. SDK timestamp subword guards are unchanged.
        whole_units = alignment_units(whole, true);
        left_units = alignment_units(left, true);
        right_units = alignment_units(right, true);
    }
    let prefix = edit_costs(&left_units, &whole_units, false);
    let suffix = edit_costs(&right_units, &whole_units, true);
    // At eviction, use the known PCM partition as a positional tie prior.
    // Probe omissions (even just one empty side) must not bias every unmatched
    // whole unit into the replaceable tail. This assumes roughly even text
    // density ONLY among equally good edit projections; it supplies no timing.
    // Context exclusion instead keeps the probe-count prior behind prefix fit.
    let probe_units = left_units.len() + right_units.len();
    let prior = partition.map_or_else(
        || whole_units.len() * left_units.len() / probe_units.max(1),
        |(left_samples, samples)| (whole_units.len() * left_samples + samples / 2) / samples.max(1),
    );
    let split = (0..=whole_units.len())
        .min_by_key(|index| {
            let (first_tie, second_tie) = if partition.is_some() {
                (index.abs_diff(prior), prefix[*index])
            } else {
                // A growing tail is not evidence of more old context: prefer
                // the better prefix fit before the positional prior, preserving
                // genuinely new occurrences of repeated words.
                (prefix[*index], index.abs_diff(prior))
            };
            (
                prefix[*index] + suffix[whole_units.len() - *index],
                first_tie,
                second_tie,
                *index,
            )
        })
        .unwrap_or(0);
    let offset = |index: usize| {
        if index == 0 {
            0
        } else {
            whole_units[index - 1].end
        }
    };
    let cost_at = |index: usize| prefix[index] + suffix[whole_units.len() - index];
    let cost = cost_at(split);
    let mut candidates = (0..=whole_units.len()).filter(|index| cost_at(*index) == cost);
    let first = candidates.next().unwrap_or(split);
    let last = candidates.last().unwrap_or(first);
    BoundaryProjection {
        selected: offset(split),
        first: offset(first),
        last: offset(last),
        cost,
        alternative_cost: (0..=whole_units.len())
            .filter(|index| *index != split)
            .map(cost_at)
            .min(),
    }
}

fn append_text(confirmed: &mut String, text: &str, continuation: bool) {
    let separated = text.starts_with(char::is_whitespace);
    let text = text.trim();
    if !text.is_empty() {
        // Inside a contextual whole, the original slice tells whether this
        // boundary had a separator, including after ASCII punctuation. Separate
        // VAD spans or a whole omitting all context keep the usual space join.
        if !confirmed.is_empty() && (!continuation || separated) {
            confirmed.push(' ');
        }
        confirmed.push_str(text);
    }
}

fn prepare_moonshine_base_samples(samples: &mut Vec<f32>) {
    // Append only synthetic silence. Context replay is handled separately;
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
    include!("engine_sessions_tests.rs");
    use super::super::transcription::TestSpeechTranscriber;
    use super::*;
    use pioneer_protocol::{VoiceAudioEncoding, VoiceChunkFrameHeader};
    use std::sync::Mutex;

    // A fixture word is 30 ms independent of the 32 ms neural VAD frame.
    const TEST_WORD_SAMPLES: usize = 480;

    fn test_detector() -> super::super::vad::EnergyVoiceActivityDetector {
        super::super::vad::EnergyVoiceActivityDetector::new(0.02).unwrap()
    }

    // These planner tests start AFTER preparation; their synthetic voiced PCM
    // has explicit source ownership. End-to-end tests below use the real VAD.
    impl IncrementalSpeech<super::super::vad::EnergyVoiceActivityDetector> {
        fn accept_prepared_for_test(
            &mut self,
            samples: &[f32],
            store: &GatewayVoiceSessionBufferStore,
            id: &str,
            cancellation: &CancellationToken,
            transcriber: &impl VoiceSpeechTranscriber,
        ) -> Result<(), VoiceError> {
            let interval = VadInterval {
                start: self.source_samples,
                end: self.source_samples + samples.len(),
                probability: 0.9,
                voice: true,
                quiet_run_start: self.source_samples,
            };
            self.timeline.append(
                self.speech_samples,
                interval.start,
                interval.end,
                &[interval],
            );
            self.source_samples += samples.len();
            self.accept_speech(samples, store, id, cancellation, transcriber)
        }
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

    struct HypothesisTranscriber<F>(F);
    impl<F> VoiceSpeechTranscriber for HypothesisTranscriber<F>
    where
        F: Fn(&PreparedSpeechBuffer) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError>
            + Send,
    {
        fn transcribe_speech(
            &self,
            buffer: &PreparedSpeechBuffer,
        ) -> Result<String, VoiceTranscriptionError> {
            (self.0)(buffer).map(|hypothesis| hypothesis.text)
        }
        fn transcribe_hypothesis(
            &self,
            buffer: &PreparedSpeechBuffer,
        ) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError> {
            (self.0)(buffer)
        }
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
            // Distinct hypotheses depend on available source context, not call
            // order. A larger span can replace the earlier boundary hypothesis.
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
    async fn buffered_worker_waits_for_commit_and_retains_native_lease_on_cancel_or_shutdown() {
        use super::super::supervisor::{EagerVoiceEngineLoader, FilesystemVoiceModelInstaller};
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
                false,
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
    fn coalesced_preview_runs_when_open_inbox_is_caught_up_before_commit() {
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
        // Preload four seconds and a seven-sample tail before starting the real
        // processor. At the 3 s tick there is still backlog; the next 3 s tick
        // will never arrive. Commit must retain the incomplete VAD frame.
        for sequence in 0..40 {
            let pcm: Vec<_> =
                std::iter::repeat_n(12000_i16, if sequence == 39 { 1607 } else { 1600 })
                    .flat_map(i16::to_le_bytes)
                    .collect();
            store
                .append_chunk(
                    VoiceChunkFrameHeader {
                        session_id: "session".to_owned(),
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
                .unwrap();
        }
        let (entered, receive) = std::sync::mpsc::channel();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            entered.send(buffer.speech_samples()).unwrap();
            Ok(if buffer.speech_samples() < 64007 {
                "ранняя ошибка"
            } else {
                "исправлено"
            }
            .to_owned())
        }));
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| process(&store, "session", &token, &fake, false));
            let before_commit = receive.recv_timeout(std::time::Duration::from_secs(5));
            // No Commit was needed to enter inference; the frame remainder is
            // still owned by the processor and must reach the final call.
            let _completion = store.finish_processing("session").unwrap();
            let result = worker.join().unwrap().unwrap();
            assert_eq!(before_commit.unwrap(), 64000);
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
                panic!("contextual transcript expected");
            };
            assert_eq!(receive.try_recv().unwrap(), 64007);
            assert!(receive.try_recv().is_err());
            assert_eq!(transcript.text, "исправлено");
            assert_eq!(transcript.diagnostics.speech_samples, 64007);
            assert_eq!(transcript.diagnostics.total_samples, 64007);
        });
    }

    #[test]
    fn contextual_hypothesis_repairs_old_three_second_word_boundary() {
        let store = open_store();
        let token = CancellationToken::new();
        let fake = TestSpeechTranscriber(Arc::new(|buffer| {
            Ok(if buffer.speech_samples() <= REFRESH_SAMPLES {
                "распо"
            } else {
                "распознавание речи, да, да; ещё раз, ещё раз"
            }
            .to_owned())
        }));
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        state
            .accept_prepared_for_test(
                &vec![0.5; REFRESH_SAMPLES],
                &store,
                "session",
                &token,
                &fake,
            )
            .unwrap();
        assert_eq!(state.hypothesis, "распо");
        assert!(state.text.is_empty());
        assert_eq!(state.window_start_sample, 0);
        state
            .accept_prepared_for_test(
                &vec![0.5; REFRESH_SAMPLES],
                &store,
                "session",
                &token,
                &fake,
            )
            .unwrap();
        assert_eq!(
            state.hypothesis,
            "распознавание речи, да, да; ещё раз, ещё раз"
        );
        assert!(state.text.is_empty());
        state.flush(&store, "session", &token, &fake).unwrap();
        assert_eq!(state.text, "распознавание речи, да, да; ещё раз, ещё раз");
        assert_eq!(state.speech_samples, 2 * REFRESH_SAMPLES);
        assert_eq!(state.window_start_sample, state.speech_samples);
        assert!(state.speech.is_empty());
    }

    #[test]
    fn confirmed_prefix_survives_tail_replacement_and_empty_hypothesis() {
        let store = open_store();
        let token = CancellationToken::new();
        let fake = TestSpeechTranscriber(Arc::new(|buffer| {
            let samples = buffer.flattened_samples();
            Ok(if samples[0] == 0.5 {
                "подтверждено"
            } else if samples.len() <= REFRESH_SAMPLES {
                "ошибочный хвост"
            } else if samples.last() == Some(&0.75) {
                "исправленный хвост"
            } else {
                ""
            }
            .to_owned())
        }));
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        state
            .accept_prepared_for_test(&[0.5; 7], &store, "session", &token, &fake)
            .unwrap();
        state.flush(&store, "session", &token, &fake).unwrap();
        let confirmed_end = state.window_start_sample;
        state
            .accept_prepared_for_test(
                &vec![0.25; REFRESH_SAMPLES],
                &store,
                "session",
                &token,
                &fake,
            )
            .unwrap();
        assert_eq!(state.hypothesis, "ошибочный хвост");
        state
            .accept_prepared_for_test(
                &vec![0.25; REFRESH_SAMPLES],
                &store,
                "session",
                &token,
                &fake,
            )
            .unwrap();
        assert!(
            state.hypothesis.is_empty(),
            "empty success replaces stale preview text"
        );
        assert_eq!(state.text, "подтверждено");
        assert_eq!(state.window_start_sample, confirmed_end);
        state
            .accept_prepared_for_test(&[0.75; 7], &store, "session", &token, &fake)
            .unwrap();
        state.flush(&store, "session", &token, &fake).unwrap();
        assert_eq!(state.text, "подтверждено исправленный хвост");
        assert_eq!(state.window_start_sample, state.speech_samples);
    }

    // Text-only oracle: no timestamps, confidence or streaming state. Each
    // source frame carries a word/phrase, deliberately with natural repetition.
    // Splitting a frame would change the output, so partitioning must preserve
    // source ownership as well as the exact full sequence of repeated tokens.
    fn frame_transcript(samples: &[f32]) -> String {
        samples
            .chunks(TEST_WORD_SAMPLES)
            .filter_map(|frame| {
                let sample = *frame.iter().find(|sample| **sample != 0.0)?;
                Some(if sample > 0.3 {
                    "да,"
                } else {
                    "ещё раз"
                })
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn continuous_speech_advances_bounded_windows_preserving_repetitions_and_packets() {
        let audio: Vec<i16> = (0..50 * 16000 + 7)
            .map(|index| {
                if index / TEST_WORD_SAMPLES % 4 < 2 {
                    12000
                } else {
                    6000
                }
            })
            .collect();
        let normalized: Vec<_> = audio
            .iter()
            .map(|sample| f32::from(*sample) / 32768.0)
            .collect();
        let expected = frame_transcript(&normalized);
        assert!(expected.contains("да, да, ещё раз ещё раз"));
        for (packet_samples, timestamps) in [(317, false), (960, true), (1600, false)] {
            let work = Arc::new(Mutex::new(Vec::new()));
            let captured = work.clone();
            let fake = HypothesisTranscriber(move |buffer: &PreparedSpeechBuffer| {
                captured.lock().unwrap().push(buffer.speech_samples());
                let samples = buffer.flattened_samples();
                let text = frame_transcript(&samples);
                let segments = timestamps.then(|| {
                    samples
                        .chunks(TEST_WORD_SAMPLES)
                        .enumerate()
                        .filter_map(|(index, frame)| {
                            let text = frame_transcript(frame);
                            (!text.is_empty()).then(|| transcribe_rs::TranscriptionSegment {
                                start: (index * TEST_WORD_SAMPLES) as f32 / 16000.0,
                                end: ((index * TEST_WORD_SAMPLES + frame.len()) as f32) / 16000.0,
                                text,
                            })
                        })
                        .collect()
                });
                Ok(VoiceSpeechHypothesis {
                    text,
                    segments,
                    ..Default::default()
                })
            });
            let result = closed_input(&audio, packet_samples, false, &fake).unwrap();
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
                panic!("continuous transcript expected");
            };
            assert_eq!(transcript.text, expected);
            assert_eq!(transcript.diagnostics.speech_samples, audio.len());
            assert_eq!(transcript.diagnostics.total_samples, audio.len());
            assert!(transcript.diagnostics.segment_count > 1);
            let work = work.lock().unwrap();
            assert!(work.iter().all(|count| *count <= MAX_SPEECH_WINDOW_SAMPLES));
            assert!(work.len() <= 3 * (audio.len() / REFRESH_SAMPLES + 1) + 1);
            assert!(work.iter().sum::<usize>() <= 12 * audio.len() + MAX_SPEECH_WINDOW_SAMPLES);
        }
    }

    #[test]
    fn unspaced_text_preserves_all_characters_and_repeats_across_window_advances() {
        fn recognize(samples: &[f32], spaced: bool) -> String {
            samples
                .chunks(TEST_WORD_SAMPLES)
                .map(|frame| match (frame[0] * 32768.0) as i16 {
                    12000 => "甲",
                    6000 => "乙.",
                    3000 => "丙",
                    15000 => "丁，",
                    18000 => "𠀀", // Four-byte UTF-8, including repeated occurrences.
                    _ => panic!("unexpected source sample"),
                })
                .collect::<Vec<_>>()
                .join(if spaced { " " } else { "" })
        }
        let pattern = [12000, 12000, 6000, 3000, 15000, 18000, 18000];
        let audio: Vec<i16> = (0..50 * 16000 + 7)
            .map(|index| pattern[index / TEST_WORD_SAMPLES % pattern.len()])
            .collect();
        let normalized: Vec<_> = audio.iter().map(|s| f32::from(*s) / 32768.0).collect();
        for spaced in [false, true] {
            for packet_samples in [317, 1600] {
                let calls = Arc::new(Mutex::new(Vec::new()));
                let captured = calls.clone();
                // Text-only production boundary: no invented character timing.
                let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                    let samples = buffer.flattened_samples();
                    let text = recognize(&samples, spaced);
                    captured.lock().unwrap().push(samples);
                    Ok(text)
                }));
                let result = closed_input(&audio, packet_samples, false, &fake).unwrap();
                let GatewayVoiceSessionPipelineOutcome::Transcript {
                    transcript,
                    signal_stats,
                } = result
                else {
                    panic!("unspaced transcript expected");
                };
                // Entire order and multiplicity, through several rolls AND the
                // changed final refresh. Also retain real spaces when supplied.
                assert_eq!(transcript.text, recognize(&normalized, spaced));
                assert!(transcript.diagnostics.segment_count >= 3);
                assert_eq!(transcript.diagnostics.total_samples, audio.len());
                assert_eq!(transcript.diagnostics.speech_samples, audio.len());
                assert_eq!(signal_stats, VoiceSignalStats::from_samples(&normalized));
                let calls = calls.lock().unwrap();
                assert!(
                    calls
                        .iter()
                        .all(|call| call.len() <= MAX_SPEECH_WINDOW_SAMPLES)
                );
                assert!(calls.len() <= 3 * (audio.len() / REFRESH_SAMPLES + 1) + 1);
                assert!(calls.iter().map(Vec::len).sum::<usize>() <= 12 * audio.len());
                // All context/probe inputs remain exact, frame-aligned source
                // slices; no padding, fabricated PCM or per-packet oracle state.
                for call in calls.iter() {
                    assert!(
                        normalized
                            .chunks(TEST_WORD_SAMPLES)
                            .enumerate()
                            .any(|(frame, _)| {
                                let start = frame * TEST_WORD_SAMPLES;
                                normalized.get(start..start + call.len()) == Some(call.as_slice())
                            })
                    );
                }
            }
        }
    }

    #[test]
    fn non_cjk_unspaced_text_refines_a_single_unit_through_multiple_advances() {
        fn recognize(samples: &[f32], symbols: &[char]) -> String {
            samples
                .chunks(TEST_WORD_SAMPLES)
                .map(|frame| {
                    let index = (((frame[0] * 32768.0) as i16 - 12000) / 500) as usize;
                    symbols[index]
                })
                .collect()
        }
        // Two scripts outside the enumerated ranges: resolution must depend
        // on indivisibility, not on adding another language to the table.
        for phrase in ["ทดสอบระบบ", "اختبارنظام"] {
            let symbols: Vec<_> = phrase.chars().collect();
            let audio: Vec<i16> = (0..50 * 16000 + 7)
                .map(|index| 12000 + 500 * (index / TEST_WORD_SAMPLES % symbols.len()) as i16)
                .collect();
            let normalized: Vec<_> = audio.iter().map(|s| f32::from(*s) / 32768.0).collect();
            let expected = recognize(&normalized, &symbols);
            for packet_samples in [317, 1600] {
                let symbols = symbols.clone();
                let calls = Arc::new(Mutex::new(Vec::new()));
                let captured = calls.clone();
                let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                    let samples = buffer.flattened_samples();
                    let text = recognize(&samples, &symbols);
                    captured.lock().unwrap().push(samples);
                    Ok(text)
                }));
                let result = closed_input(&audio, packet_samples, false, &fake).unwrap();
                let GatewayVoiceSessionPipelineOutcome::Transcript {
                    transcript,
                    signal_stats,
                } = result
                else {
                    panic!("unspaced text outside CJK must survive window progress");
                };
                assert_eq!(
                    transcript.text, expected,
                    "{phrase}: packet {packet_samples}"
                );
                assert!(transcript.diagnostics.segment_count >= 3);
                assert_eq!(transcript.diagnostics.total_samples, audio.len());
                assert_eq!(transcript.diagnostics.speech_samples, audio.len());
                assert_eq!(signal_stats, VoiceSignalStats::from_samples(&normalized));
                let calls = calls.lock().unwrap();
                assert!(
                    calls
                        .iter()
                        .all(|call| call.len() <= MAX_SPEECH_WINDOW_SAMPLES)
                );
                assert!(calls.len() <= 3 * (audio.len() / REFRESH_SAMPLES + 1) + 1);
                assert!(calls.iter().map(Vec::len).sum::<usize>() <= 12 * audio.len());
                for call in calls.iter() {
                    assert!(
                        normalized
                            .chunks(TEST_WORD_SAMPLES)
                            .enumerate()
                            .any(|(frame, _)| {
                                let start = frame * TEST_WORD_SAMPLES;
                                normalized.get(start..start + call.len()) == Some(call.as_slice())
                            })
                    );
                }
            }
        }
    }

    #[test]
    fn common_suffix_does_not_hide_an_interior_unspaced_partition() {
        for prefix in ["", "intro "] {
            for suffix in ["", " OK", " OK OK", " OK ещё раз"] {
                for part in ["ทดสอบ", "ทดสอบ,"] {
                    let whole = format!("{prefix}{part}ระบบ{suffix}");
                    let left = format!("{prefix}{part}");
                    let right = format!("ระบบ{suffix}");
                    assert_eq!(align_boundary(&whole, &left, &right), left.len(), "{whole}");
                }
            }
        }
    }

    #[test]
    fn unspaced_partition_with_spaced_suffix_survives_progress_and_final_refresh() {
        fn recognize(samples: &[f32], suffix: &str) -> String {
            let symbols: Vec<_> = "ทดสอบระบบ".chars().collect();
            let mut text = String::new();
            for frame in samples.chunks(TEST_WORD_SAMPLES) {
                let sample = (frame[0] * 32768.0) as i16;
                if sample == 26000 {
                    text.push_str(suffix);
                } else {
                    text.push(symbols[((sample - 12000) / 500) as usize]);
                }
            }
            text.trim().to_owned()
        }
        let mut audio: Vec<i16> = (0..20 * 16000 + 7)
            .map(|index| 12000 + 500 * (index / TEST_WORD_SAMPLES % 9) as i16)
            .collect();
        // The first whole and right probe end in OK; the left/context probes
        // do not. New audio extends beyond OK before the changed final refresh.
        audio[MAX_SPEECH_WINDOW_SAMPLES - TEST_WORD_SAMPLES..MAX_SPEECH_WINDOW_SAMPLES].fill(26000);
        let normalized: Vec<_> = audio.iter().map(|s| f32::from(*s) / 32768.0).collect();
        for suffix in [" OK ", " OK OK "] {
            for packet_samples in [317, 1600] {
                let calls = Arc::new(Mutex::new(Vec::new()));
                let captured = calls.clone();
                let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                    let samples = buffer.flattened_samples();
                    let text = recognize(&samples, suffix);
                    captured.lock().unwrap().push(samples);
                    Ok(text)
                }));
                let result = closed_input(&audio, packet_samples, false, &fake).unwrap();
                let GatewayVoiceSessionPipelineOutcome::Transcript {
                    transcript,
                    signal_stats,
                } = result
                else {
                    panic!("mixed spaced/unspaced transcript expected");
                };
                assert_eq!(transcript.text, recognize(&normalized, suffix));
                assert_eq!(transcript.diagnostics.segment_count, 2);
                assert_eq!(transcript.diagnostics.total_samples, audio.len());
                assert_eq!(transcript.diagnostics.speech_samples, audio.len());
                assert_eq!(signal_stats, VoiceSignalStats::from_samples(&normalized));
                let calls = calls.lock().unwrap();
                assert_eq!(calls.len(), 5);
                let cut = calls[1].len();
                let context_start = cut - LEFT_CONTEXT_SAMPLES;
                assert_eq!(calls[0], normalized[..MAX_SPEECH_WINDOW_SAMPLES]);
                assert_eq!(calls[1], normalized[..cut]);
                assert_eq!(calls[2], normalized[cut..MAX_SPEECH_WINDOW_SAMPLES]);
                assert_eq!(calls[3], normalized[context_start..cut]);
                assert_eq!(calls[4], normalized[context_start..]);
                assert!(
                    calls
                        .iter()
                        .all(|call| call.len() <= MAX_SPEECH_WINDOW_SAMPLES)
                );
                assert_eq!(
                    calls.iter().map(Vec::len).sum::<usize>(),
                    2 * MAX_SPEECH_WINDOW_SAMPLES + audio.len() - cut + 2 * LEFT_CONTEXT_SAMPLES
                );
            }
        }
    }

    #[test]
    fn inexact_mixed_probes_keep_ownership_through_two_advances_and_commit() {
        // Constant magnitude keeps VAD and the energy partition deterministic.
        // The negative context identifies the second whole by its actual PCM,
        // independently of invocation order. No decoder text is an expectation.
        let mut audio = vec![12000_i16; 30 * 16000 + 7];
        audio[144000..168000].fill(-12000);
        let normalized: Vec<_> = audio.iter().map(|s| f32::from(*s) / 32768.0).collect();
        for (left, right, context, timing) in [
            ("ทดสอป", "ระบบ OK", "อบ", "none"),
            ("ทดสอป", "ระบบ OK", "อบ", "coarse"),
            ("ทดสอป", "ระบบ OK", "อบ", "nonfinite"),
            ("", "ระบบ OK", "", "none"),
            ("ทดสอป", "", "", "none"),
            ("", "", "", "none"),
            ("XXXXX", "YYYYYYYYYY", "", "none"),
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let captured = calls.clone();
            let fake = HypothesisTranscriber(move |buffer: &PreparedSpeechBuffer| {
                let samples = buffer.flattened_samples();
                let count = samples.len();
                let negative = samples[0] < 0.0;
                let text = match (count, negative) {
                    // The left typo used to suppress refinement and assign
                    // zero bytes to the PCM about to be evicted.
                    (288000, false) => "ทดสอบระบบ OK",
                    (168000, false) => left,
                    (120000, false) => right,
                    (24000, true) => context,
                    // The later OK is a real new occurrence; the first OK in
                    // the final whole is technical replay of confirmed PCM.
                    (288000, true) => "อบระบบ OK OK FIN",
                    (180000, true) => "อบระบบ OK",
                    (108000, false) => "OK FIN",
                    (24000, false) => "OK",
                    (180007, false) => "OK OK FIN fin.",
                    _ => panic!("unexpected PCM span: {count}, negative={negative}"),
                }
                .to_owned();
                let segments = match timing {
                    "none" => None,
                    _ => Some(vec![transcribe_rs::TranscriptionSegment {
                        start: 0.0,
                        end: if timing == "nonfinite" {
                            f32::NAN
                        } else {
                            count as f32 / 16000.0
                        },
                        text: text.clone(),
                    }]),
                };
                captured.lock().unwrap().push(samples);
                Ok(VoiceSpeechHypothesis {
                    text,
                    segments,
                    ..Default::default()
                })
            });
            let result = closed_input(&audio, 317, false, &fake).unwrap();
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
                panic!("mixed transcript expected after two window advances");
            };
            assert_eq!(
                transcript.text, "ทดสอบระบบ OK OK FIN fin.",
                "{left:?}/{right:?}/{context:?}/{timing}"
            );
            assert_eq!(transcript.diagnostics.segment_count, 3);
            assert_eq!(transcript.diagnostics.speech_samples, 480007);
            assert_eq!(transcript.diagnostics.total_samples, 480007);
            let calls = calls.lock().unwrap();
            // Two disjoint partitions and their bounded context, then a changed
            // final refresh after new source. No retry-until-agreement calls.
            let spans = [
                (0, 288000),
                (0, 168000),
                (168000, 288000),
                (144000, 168000),
                (144000, 432000),
                (144000, 324000),
                (324000, 432000),
                (300000, 324000),
                (300000, 480007),
            ];
            assert_eq!(calls.len(), spans.len());
            for (call, (start, end)) in calls.iter().zip(spans) {
                assert_eq!(call.as_slice(), &normalized[start..end]);
                assert!(call.len() <= MAX_SPEECH_WINDOW_SAMPLES);
            }
            assert_eq!(calls.iter().map(Vec::len).sum::<usize>(), 1380007);
        }
    }

    #[test]
    fn empty_context_probe_keeps_ownership_without_consuming_a_new_repetition() {
        for (marker, expected) in [
            (12000_i16, Some("начало да конец дальше")),
            (6000, Some("начало да да конец дальше")),
            (3000, Some("начало да")), // Empty FINAL still replaces the old tail.
            (24000, None),             // A real late error must still fail the recording.
        ] {
            for packet_samples in [317, 1600] {
                let mut audio = vec![12000; 20 * 16000];
                audio.extend([marker; 7]);
                let normalized: Vec<_> = audio.iter().map(|s| f32::from(*s) / 32768.0).collect();
                let calls = Arc::new(Mutex::new(Vec::new()));
                let captured = calls.clone();
                let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                    let samples = buffer.flattened_samples();
                    let count = samples.len();
                    let last = (*samples.last().unwrap() * 32768.0) as i16;
                    captured.lock().unwrap().push(samples);
                    // Disjoint probes and the contextual whole intentionally
                    // disagree with the short context probe. The final PCM
                    // marker distinguishes a genuinely new repeated word.
                    Ok(match count {
                        MAX_SPEECH_WINDOW_SAMPLES => "начало да конец",
                        168000 => "начало да",
                        120000 => "конец",
                        LEFT_CONTEXT_SAMPLES => "",
                        176007 => match last {
                            12000 => "да конец дальше",
                            6000 => "да да конец дальше",
                            3000 => "",
                            24000 => {
                                return Err(transcription_error(
                                    VoiceTranscriptionErrorKind::RuntimeFailure,
                                    "late native failure",
                                ));
                            }
                            _ => panic!("unexpected final source marker"),
                        },
                        _ => panic!("unexpected native span: {count}"),
                    }
                    .to_owned())
                }));
                let result = closed_input(&audio, packet_samples, false, &fake);
                match expected {
                    Some(expected) => {
                        let GatewayVoiceSessionPipelineOutcome::Transcript {
                            transcript,
                            signal_stats,
                        } = result.unwrap()
                        else {
                            panic!("contextual transcript expected");
                        };
                        assert_eq!(transcript.text, expected);
                        assert_eq!(transcript.diagnostics.total_samples, audio.len());
                        assert_eq!(transcript.diagnostics.speech_samples, audio.len());
                        assert_eq!(transcript.diagnostics.segment_count, 2);
                        assert_eq!(signal_stats, VoiceSignalStats::from_samples(&normalized));
                    }
                    None => {
                        let Err(error) = result else {
                            panic!("late failure must not yield a partial transcript");
                        };
                        assert_eq!(error.kind, VoiceErrorKind::TranscriptionFailed);
                    }
                }
                let calls = calls.lock().unwrap();
                assert_eq!(calls.len(), 5, "no extra inference to recover ownership");
                let cut = 168000;
                let context_start = cut - LEFT_CONTEXT_SAMPLES;
                assert_eq!(calls[0], normalized[..MAX_SPEECH_WINDOW_SAMPLES]);
                assert_eq!(calls[1], normalized[..cut]);
                assert_eq!(calls[2], normalized[cut..MAX_SPEECH_WINDOW_SAMPLES]);
                assert_eq!(calls[3], normalized[context_start..cut]);
                assert_eq!(calls[4], normalized[context_start..]);
                assert_eq!(calls.iter().map(Vec::len).sum::<usize>(), 776007);
            }
        }
    }

    #[test]
    fn empty_context_probe_retains_all_words_located_by_previous_timing() {
        for next_timing in ["none", "coarse", "invalid"] {
            for (marker, expected) in [
                (12000_i16, "начало да да конец дальше"),
                (6000, "начало да да да конец дальше"),
            ] {
                for packet_samples in [317, 1600] {
                    // Vary sample order while keeping every sample's energy
                    // identical: the preferred cut stays at 10.5 s, and exact
                    // PCM assertions can detect more than just wrong lengths.
                    let mut audio: Vec<i16> = (0..20_usize * 16000)
                        .map(|index| {
                            if index.count_ones() % 2 == 0 {
                                12000
                            } else {
                                -12000
                            }
                        })
                        .collect();
                    audio.extend([marker; 7]);
                    let normalized: Vec<_> =
                        audio.iter().map(|s| f32::from(*s) / 32768.0).collect();
                    let calls = Arc::new(Mutex::new(Vec::new()));
                    let captured = calls.clone();
                    let fake = HypothesisTranscriber(move |buffer: &PreparedSpeechBuffer| {
                        let samples = buffer.flattened_samples();
                        let count = samples.len();
                        let last = (*samples.last().unwrap() * 32768.0) as i16;
                        captured.lock().unwrap().push(samples);
                        match count {
                            MAX_SPEECH_WINDOW_SAMPLES => Ok(VoiceSpeechHypothesis {
                                text: "начало да да конец".to_owned(),
                                segments: Some(
                                    [
                                        (0.0, 8.0, "начало"),
                                        (8.6, 9.2, "да"),
                                        (9.2, 10.0, "да"),
                                        (10.0, 18.0, "конец"),
                                    ]
                                    .into_iter()
                                    .map(|(start, end, text)| transcribe_rs::TranscriptionSegment {
                                        start,
                                        end,
                                        text: text.to_owned(),
                                    })
                                    .collect(),
                                ),
                                ..Default::default()
                            }),
                            LEFT_CONTEXT_SAMPLES => {
                                Ok(VoiceSpeechHypothesis::text_only(String::new()))
                            }
                            184007 => {
                                let text = match last {
                                    12000 => "да да конец дальше",
                                    6000 => "да да да конец дальше",
                                    _ => panic!("unexpected final source marker"),
                                }
                                .to_owned();
                                let segments = match next_timing {
                                    "none" => None,
                                    _ => Some(vec![transcribe_rs::TranscriptionSegment {
                                        start: 0.0,
                                        end: if next_timing == "invalid" {
                                            f32::NAN
                                        } else {
                                            count as f32 / 16000.0
                                        },
                                        text: text.clone(),
                                    }]),
                                };
                                Ok(VoiceSpeechHypothesis {
                                    text,
                                    segments,
                                    ..Default::default()
                                })
                            }
                            _ => panic!(
                                "timed progress must not add partition/recovery calls: {count}"
                            ),
                        }
                    });
                    let result = closed_input(&audio, packet_samples, false, &fake).unwrap();
                    let GatewayVoiceSessionPipelineOutcome::Transcript {
                        transcript,
                        signal_stats,
                    } = result
                    else {
                        panic!("timed confirmed context must survive a text-only refresh");
                    };
                    assert_eq!(transcript.text, expected, "{next_timing}: marker {marker}");
                    assert_eq!(transcript.diagnostics.total_samples, audio.len());
                    assert_eq!(transcript.diagnostics.speech_samples, audio.len());
                    assert_eq!(transcript.diagnostics.segment_count, 2);
                    assert_eq!(signal_stats, VoiceSignalStats::from_samples(&normalized));
                    let calls = calls.lock().unwrap();
                    assert_eq!(
                        calls.len(),
                        3,
                        "whole, context, and current final tail only"
                    );
                    assert_eq!(calls[0], normalized[..MAX_SPEECH_WINDOW_SAMPLES]);
                    assert_eq!(calls[1], normalized[136000..160000]);
                    assert_eq!(calls[2], normalized[136000..]);
                    assert_eq!(calls.iter().map(Vec::len).sum::<usize>(), 496007);
                }
            }
        }
    }

    #[test]
    fn known_empty_timed_context_preserves_the_entire_current_tail() {
        fn hypothesis(
            samples: &[f32],
            coarse_final: bool,
        ) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError> {
            let text = match samples.len() {
                MAX_SPEECH_WINDOW_SAMPLES => {
                    return Ok(VoiceSpeechHypothesis {
                        text: "начало да конец".to_owned(),
                        segments: Some(
                            [
                                (0.0, 2.0, "начало"),
                                (2.0, 5.0, "да"),
                                (16.0, 18.0, "конец"),
                            ]
                            .into_iter()
                            .map(|(start, end, text)| transcribe_rs::TranscriptionSegment {
                                start,
                                end,
                                text: text.to_owned(),
                            })
                            .collect(),
                        ),
                        ..Default::default()
                    });
                }
                168000 => "начало да",
                120000 | 144000 => "конец",
                LEFT_CONTEXT_SAMPLES => "",
                176007 => match (*samples.last().unwrap() * 32768.0) as i16 {
                    12000 => "конец да дальше",
                    6000 => "конец дальше",
                    3000 => "",
                    24000 => {
                        return Err(transcription_error(
                            VoiceTranscriptionErrorKind::RuntimeFailure,
                            "late native failure after known empty context",
                        ));
                    }
                    _ => panic!("unexpected final marker"),
                },
                count => panic!("unexpected native span: {count}"),
            }
            .to_owned();
            let segments = (coarse_final && samples.len() == 176007).then(|| {
                vec![transcribe_rs::TranscriptionSegment {
                    start: 0.0,
                    end: samples.len() as f32 / 16000.0,
                    text: text.clone(),
                }]
            });
            Ok(VoiceSpeechHypothesis {
                text,
                segments,
                ..Default::default()
            })
        }
        // Speech energy stays constant even across the timestamp gap. Varying
        // signs makes the exact source-order assertions meaningful as well.
        let source: Vec<i16> = (0..20_usize * 16000)
            .map(|index| {
                if index.count_ones() % 2 == 0 {
                    12000
                } else {
                    -12000
                }
            })
            .collect();
        for coarse_final in [false, true] {
            for (marker, expected) in [
                (12000_i16, Some("начало да конец да дальше")),
                (6000, Some("начало да конец дальше")),
                (3000, Some("начало да")),
                (24000, None),
            ] {
                let mut audio = source.clone();
                audio.extend([marker; 7]);
                let normalized: Vec<_> = audio.iter().map(|s| f32::from(*s) / 32768.0).collect();
                for packet_samples in [317, 1600] {
                    let calls = Arc::new(Mutex::new(Vec::new()));
                    let captured = calls.clone();
                    let fake = HypothesisTranscriber(move |buffer: &PreparedSpeechBuffer| {
                        let samples = buffer.flattened_samples();
                        let result = hypothesis(&samples, coarse_final);
                        captured.lock().unwrap().push(samples);
                        result
                    });
                    let result = closed_input(&audio, packet_samples, false, &fake);
                    match expected {
                        Some(expected) => {
                            let GatewayVoiceSessionPipelineOutcome::Transcript {
                                transcript,
                                signal_stats,
                            } = result.unwrap()
                            else {
                                panic!("known empty context must not consume any current text");
                            };
                            assert_eq!(transcript.text, expected);
                            assert_eq!(transcript.diagnostics.total_samples, audio.len());
                            assert_eq!(transcript.diagnostics.speech_samples, audio.len());
                            assert_eq!(transcript.diagnostics.segment_count, 2);
                            assert_eq!(signal_stats, VoiceSignalStats::from_samples(&normalized));
                        }
                        None => {
                            let Err(error) = result else {
                                panic!("real final error must discard the recording outcome");
                            };
                            assert_eq!(error.kind, VoiceErrorKind::TranscriptionFailed);
                        }
                    }
                    let calls = calls.lock().unwrap();
                    assert_eq!(calls.len(), 5, "no additional recovery inference");
                    assert_eq!(calls[0], normalized[..MAX_SPEECH_WINDOW_SAMPLES]);
                    assert_eq!(calls[1], normalized[..168000]);
                    assert_eq!(calls[2], normalized[168000..MAX_SPEECH_WINDOW_SAMPLES]);
                    assert_eq!(calls[3], normalized[144000..168000]);
                    assert_eq!(calls[4], normalized[144000..]);
                    assert_eq!(calls.iter().map(Vec::len).sum::<usize>(), 776007);
                }
            }
        }
        // Also inspect the carried, unchanged-source hypothesis: an empty
        // reference contributes zero owned bytes and cannot become a prefix.
        let fake = HypothesisTranscriber(|buffer: &PreparedSpeechBuffer| {
            hypothesis(&buffer.flattened_samples(), false)
        });
        let store = open_store();
        let token = CancellationToken::new();
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        state.speech = source[..MAX_SPEECH_WINDOW_SAMPLES]
            .iter()
            .map(|s| f32::from(*s) / 32768.0)
            .collect();
        state.speech_samples = state.speech.len();
        state
            .advance_window(&store, "session", &token, &fake)
            .unwrap();
        assert_eq!(state.text, "начало да");
        assert!(state.context_text.is_empty());
        assert_eq!(state.owned_text_start, 0);
        assert_eq!(state.full_hypothesis.text, "конец");
        assert_eq!(state.hypothesis, "конец");
        state.flush(&store, "session", &token, &fake).unwrap();
        assert_eq!(state.text, "начало да конец");
        assert_eq!(state.speech_samples, MAX_SPEECH_WINDOW_SAMPLES);
        assert_eq!(state.window_start_sample, state.speech_samples);
    }

    #[test]
    fn window_limit_next_sample_and_vad_hangover_do_not_refuse_recording() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            captured.lock().unwrap().push(buffer.flattened_samples());
            Ok(if buffer.speech_samples() == MAX_SPEECH_WINDOW_SAMPLES {
                "Привет, мир."
            } else if buffer.speech_samples() > 9 * 16000 {
                "Привет."
            } else {
                "Мир."
            }
            .to_owned())
        }));
        for extra in [0, 1, 7] {
            calls.lock().unwrap().clear();
            let audio = vec![12000; MAX_SPEECH_WINDOW_SAMPLES + extra];
            let result = closed_input(&audio, 317, false, &fake).unwrap();
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
                panic!("continuous speech must not fail on disagreement");
            };
            assert_eq!(transcript.diagnostics.speech_samples, audio.len());
            let calls = calls.lock().unwrap();
            if extra == 0 {
                assert_eq!(calls.len(), 1, "Commit at cap needs no partition");
                assert_eq!(transcript.text, "Привет, мир.");
            } else {
                assert_eq!(calls.len(), 5);
                let cut = calls[1].len();
                assert_eq!(calls[0].len(), MAX_SPEECH_WINDOW_SAMPLES);
                assert_eq!(calls[1], calls[0][..cut]);
                assert_eq!(calls[2], calls[0][cut..]);
                assert_eq!(calls[3], calls[0][cut - LEFT_CONTEXT_SAMPLES..cut]);
                assert_eq!(
                    calls[4],
                    vec![12000.0 / 32768.0; audio.len() - cut + LEFT_CONTEXT_SAMPLES]
                );
                assert!(
                    calls
                        .iter()
                        .all(|call| call.len() <= MAX_SPEECH_WINDOW_SAMPLES)
                );
            }
        }
        // Hangover legitimately belongs to the window too, including when a
        // pause begins exactly at the cap. It must not trigger a content error.
        calls.lock().unwrap().clear();
        let mut audio = vec![12000; MAX_SPEECH_WINDOW_SAMPLES];
        audio.extend(vec![0; 16 * VOICE_VAD_FRAME_SAMPLES]);
        let result = closed_input(&audio, 317, false, &fake).unwrap();
        let GatewayVoiceSessionPipelineOutcome::Transcript {
            transcript,
            signal_stats,
        } = result
        else {
            panic!("VAD pause at cap must succeed");
        };
        assert_eq!(
            transcript.diagnostics.speech_samples,
            MAX_SPEECH_WINDOW_SAMPLES + 256 + 14 * VOICE_VAD_FRAME_SAMPLES
        );
        assert_eq!(signal_stats.total_samples, audio.len());
        let calls = calls.lock().unwrap();
        // The pause projects ownership with a whole-span decode, two partition
        // probes and a context probe. Commit refreshes the changed retained PCM.
        assert_eq!(calls.len(), 9);
        let normalized: Vec<_> = audio.iter().map(|s| f32::from(*s) / 32768.0).collect();
        assert_eq!(calls[4], normalized[144000..295424]);
        assert_eq!(calls[8], normalized[235200..295424]);
        assert!(
            calls
                .iter()
                .all(|call| call.len() <= MAX_SPEECH_WINDOW_SAMPLES)
        );
    }

    #[test]
    fn closed_thirteen_and_seventeen_second_backlogs_decode_once_without_probes() {
        for seconds in [13, 17] {
            let work = Arc::new(Mutex::new(Vec::new()));
            let captured = work.clone();
            let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                captured.lock().unwrap().push(buffer.speech_samples());
                Ok(if buffer.speech_samples() >= 12 * 16000 {
                    "Привет, мир."
                } else {
                    "МИР!"
                }
                .to_owned())
            }));
            let result = closed_input(&vec![12000; seconds * 16000], 1600, false, &fake).unwrap();
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
                panic!("transcript expected");
            };
            assert_eq!(transcript.text, "Привет, мир.");
            assert_eq!(*work.lock().unwrap(), vec![seconds * 16000]);
            assert_eq!(work.lock().unwrap().iter().sum::<usize>(), seconds * 16000);
        }
    }

    #[test]
    fn long_contextual_rewording_and_punctuation_keep_all_repeated_occurrences() {
        fn recognize(samples: &[f32]) -> String {
            let phrases: Vec<_> = samples
                .chunks(TEST_WORD_SAMPLES)
                .map(|frame| {
                    if frame[0] > 0.7 {
                        if samples.len() <= REFRESH_SAMPLES {
                            "ошибка"
                        } else if samples.len() < MAX_SPEECH_WINDOW_SAMPLES {
                            "новый черновик"
                        } else {
                            "черновик"
                        }
                    } else if frame[0] > 0.3 {
                        "да"
                    } else {
                        "ещё раз"
                    }
                })
                .collect();
            if samples.len() >= 12 * 16000 {
                format!("{}.", phrases.join(", "))
            } else {
                format!("{}.", phrases.join(". ").to_uppercase())
            }
        }
        fn words(text: &str) -> Vec<String> {
            text.split_whitespace()
                .map(|word| {
                    word.trim_matches(|c: char| !c.is_alphanumeric())
                        .to_lowercase()
                })
                .filter(|word| !word.is_empty())
                .collect()
        }
        let mut audio: Vec<i16> = (0..50 * 16000)
            .map(|index| {
                if index / TEST_WORD_SAMPLES % 4 < 2 {
                    12000
                } else {
                    6000
                }
            })
            .collect();
        audio[..TEST_WORD_SAMPLES].fill(24000);
        let normalized: Vec<_> = audio
            .iter()
            .map(|sample| f32::from(*sample) / 32768.0)
            .collect();
        let fake = TestSpeechTranscriber(Arc::new(|buffer| {
            Ok(recognize(&buffer.flattened_samples()))
        }));
        // This is the same incremental inference boundary used by process;
        // the early hypothesis changes words, not just punctuation/case.
        let store = open_store();
        let token = CancellationToken::new();
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        state
            .accept_prepared_for_test(
                &normalized[..REFRESH_SAMPLES],
                &store,
                "session",
                &token,
                &fake,
            )
            .unwrap();
        assert!(state.hypothesis.starts_with("ОШИБКА"));
        state
            .accept_prepared_for_test(
                &normalized[REFRESH_SAMPLES..MAX_SPEECH_WINDOW_SAMPLES],
                &store,
                "session",
                &token,
                &fake,
            )
            .unwrap();
        assert!(state.hypothesis.starts_with("черновик,"));
        assert!(state.text.is_empty());
        // Full production normalization/VAD/drain/finalization on long input.
        for packet_samples in [317, 1600] {
            let result = closed_input(&audio, packet_samples, false, &fake).unwrap();
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
                panic!("long speech expected");
            };
            assert_eq!(words(&transcript.text), words(&recognize(&normalized)));
            assert!(!transcript.text.to_lowercase().contains("ошибка"));
            assert!(
                !transcript.text.to_lowercase().contains("новый"),
                "probe-only rephrasing must not replace the contextual whole"
            );
            assert_eq!(transcript.diagnostics.speech_samples, audio.len());
            assert!(transcript.diagnostics.segment_count > 1);
        }
    }

    #[test]
    fn unusable_and_coarse_timestamps_use_text_alignment_without_failure() {
        let audio = vec![12000; MAX_SPEECH_WINDOW_SAMPLES + 7];
        let expected = frame_transcript(&vec![12000.0 / 32768.0; audio.len()]);
        for mode in ["coarse", "nonfinite", "mismatched_text"] {
            let fake = HypothesisTranscriber(move |buffer: &PreparedSpeechBuffer| {
                let text = frame_transcript(&buffer.flattened_samples());
                Ok(VoiceSpeechHypothesis {
                    segments: Some(vec![transcribe_rs::TranscriptionSegment {
                        start: 0.0,
                        end: if mode == "nonfinite" {
                            f32::NAN
                        } else {
                            buffer.speech_samples() as f32 / 16000.0
                        },
                        text: if mode == "mismatched_text" {
                            "different text".to_owned()
                        } else {
                            text.clone()
                        },
                    }]),
                    text,
                    ..Default::default()
                })
            });
            let result = closed_input(&audio, 317, false, &fake).unwrap();
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
                panic!("fallback transcript expected");
            };
            assert_eq!(transcript.text, expected, "{mode}");
            assert_eq!(transcript.diagnostics.speech_samples, audio.len());
        }
    }

    #[test]
    fn coincident_native_word_ends_keep_repeated_words_on_the_same_side() {
        let fake = HypothesisTranscriber(|buffer: &PreparedSpeechBuffer| {
            if buffer.speech_samples() != MAX_SPEECH_WINDOW_SAMPLES {
                return Ok(VoiceSpeechHypothesis::text_only("да, да.".to_owned()));
            }
            Ok(VoiceSpeechHypothesis {
                text: "да, да. хвост".to_owned(),
                segments: Some(vec![
                    transcribe_rs::TranscriptionSegment {
                        start: 0.0,
                        end: 10.0,
                        text: "да,".to_owned(),
                    },
                    transcribe_rs::TranscriptionSegment {
                        start: 10.0,
                        end: 10.0,
                        text: "да.".to_owned(),
                    },
                    transcribe_rs::TranscriptionSegment {
                        start: 10.0,
                        end: 18.0,
                        text: "хвост".to_owned(),
                    },
                ]),
                ..Default::default()
            })
        });
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        state.speech.extend(vec![0.5; MAX_SPEECH_WINDOW_SAMPLES]);
        state.speech_samples = state.speech.len();
        state
            .advance_window(&open_store(), "session", &CancellationToken::new(), &fake)
            .unwrap();
        assert_eq!(state.text, "да, да.");
        assert_eq!(state.hypothesis, "хвост");
        assert_eq!(
            state.window_start_sample + state.context_samples,
            10 * 16000
        );
    }

    #[test]
    fn sdk_subword_timing_does_not_confirm_a_partial_word() {
        let work = Arc::new(Mutex::new(Vec::new()));
        let captured = work.clone();
        let fake = HypothesisTranscriber(move |buffer: &PreparedSpeechBuffer| {
            let count = buffer.speech_samples();
            captured.lock().unwrap().push(count);
            if count != MAX_SPEECH_WINDOW_SAMPLES {
                return Ok(VoiceSpeechHypothesis::text_only("знавание".to_owned()));
            }
            Ok(VoiceSpeechHypothesis {
                text: "да, распознавание речи".to_owned(),
                segments: Some(vec![
                    transcribe_rs::TranscriptionSegment {
                        start: 0.0,
                        end: 10.0,
                        text: "да,".to_owned(),
                    },
                    transcribe_rs::TranscriptionSegment {
                        start: 10.0,
                        end: 10.0,
                        text: "распо".to_owned(),
                    },
                    transcribe_rs::TranscriptionSegment {
                        start: 10.0,
                        end: 12.0,
                        text: "знавание".to_owned(),
                    },
                    transcribe_rs::TranscriptionSegment {
                        start: 12.0,
                        end: 18.0,
                        text: "речи".to_owned(),
                    },
                ]),
                ..Default::default()
            })
        });
        let store = open_store();
        let token = CancellationToken::new();
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        state.speech.extend(vec![0.5; MAX_SPEECH_WINDOW_SAMPLES]);
        state.speech_samples = state.speech.len();
        state
            .advance_window(&store, "session", &token, &fake)
            .unwrap();
        assert_eq!(state.text, "да, распознавание");
        assert_eq!(state.hypothesis, "речи");
        assert_eq!(
            state.window_start_sample + state.context_samples,
            12 * 16000
        );
        assert_eq!(
            *work.lock().unwrap(),
            vec![MAX_SPEECH_WINDOW_SAMPLES, LEFT_CONTEXT_SAMPLES]
        );
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
    fn short_pause_revises_only_owned_tail_with_packet_independent_source() {
        for resume in [false, true] {
            let mut audio = vec![12000; 96000];
            audio.extend(vec![0; 512 * 16]);
            if resume {
                audio.extend(vec![6000; 1031]);
            }
            for packet in [137, 512, 1600] {
                let calls = Arc::new(Mutex::new(Vec::new()));
                let capture = calls.clone();
                let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                    let samples = buffer.flattened_samples();
                    let text = if samples.contains(&(6000.0 / 32768.0)) {
                        "prefix corrected tail"
                    } else {
                        match samples.len() {
                            103424 => "immutable prefix wrong tail",
                            67200 => "immutable prefix",
                            36224 => "wrong tail",
                            24000 => "prefix",
                            60224 => "prefix wrong tail",
                            count => panic!("unexpected source span {count}"),
                        }
                    };
                    capture.lock().unwrap().push(samples);
                    Ok(text.to_owned())
                }));
                let result = closed_input(&audio, packet, false, &fake).unwrap();
                let GatewayVoiceSessionPipelineOutcome::Transcript {
                    transcript,
                    signal_stats,
                } = result
                else {
                    panic!("contextual transcript expected");
                };
                assert_eq!(
                    transcript.text,
                    if resume {
                        "immutable prefix corrected tail"
                    } else {
                        "immutable prefix wrong tail"
                    }
                );
                let normalized: Vec<_> = audio.iter().map(|s| f32::from(*s) / 32768.0).collect();
                let calls = calls.lock().unwrap();
                // A real final decode of the retained span, after the ownership
                // fence changed. The dropped silence is recovered once as prefill
                // only when continuation is present; old hangover is not reissued.
                let source_end = if resume { audio.len() } else { 103424 };
                assert_eq!(calls.last().unwrap(), &normalized[43200..source_end]);
                assert!(calls.iter().all(|call| call.len() <= 288000));
                assert_eq!(transcript.diagnostics.speech_samples, source_end);
                assert_eq!(transcript.diagnostics.total_samples, audio.len());
                assert_eq!(signal_stats.total_samples, audio.len());
            }
        }
    }

    #[test]
    fn long_silence_seals_once_without_decoding_or_retaining_each_noise_frame() {
        for quiet_frames in [40, 1000] {
            let mut audio = vec![12000; 1024];
            audio.extend(vec![0; quiet_frames * 512]);
            audio.extend(vec![6000; 1031]);
            for packet in [137, 1600] {
                let calls = Arc::new(Mutex::new(Vec::new()));
                let capture = calls.clone();
                let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                    let samples = buffer.flattened_samples();
                    let text = if samples.contains(&(12000.0 / 32768.0)) {
                        "first"
                    } else {
                        "second"
                    };
                    capture.lock().unwrap().push(samples);
                    Ok(text.to_owned())
                }));
                let result = closed_input(&audio, packet, false, &fake).unwrap();
                let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result
                else {
                    panic!("both spans expected");
                };
                assert_eq!(transcript.text, "first second");
                let calls = calls.lock().unwrap();
                let mut first = vec![12000.0 / 32768.0; 1024];
                first.extend(vec![0.0; 14 * 512]);
                let mut second = vec![0.0; 13 * 512];
                second.extend(vec![6000.0 / 32768.0; 1031]);
                assert_eq!(*calls, vec![first, second]);
                assert_eq!(
                    transcript.diagnostics.speech_samples,
                    1024 + 27 * 512 + 1031
                );
                assert_eq!(transcript.diagnostics.total_samples, audio.len());
            }
        }
    }

    #[test]
    fn real_repetition_across_short_pause_is_not_text_deduplicated() {
        let mut audio = vec![12000; 960];
        audio.extend(vec![0; 16 * TEST_WORD_SAMPLES]);
        audio.extend(vec![12000; 960]);
        let fake = TestSpeechTranscriber(Arc::new(|buffer| {
            Ok(frame_transcript(&buffer.flattened_samples()))
        }));
        let result = closed_input(&audio, 137, false, &fake).unwrap();
        let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
            panic!("repeated phrase expected");
        };
        assert_eq!(transcript.text, "да, да, да, да,");
        assert_eq!(transcript.diagnostics.speech_samples, audio.len());
    }

    #[test]
    fn pause_candidate_commit_resume_error_and_cancel_use_current_source() {
        for action in [
            "commit",
            "resume",
            "error",
            "cancel",
            "cancel_paused",
            "oversized",
            "empty",
            "seal",
        ] {
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
            let cancel = token.clone();
            let (entered, receive) = std::sync::mpsc::channel();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let capture = calls.clone();
            let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                let samples = buffer.flattened_samples();
                let resumed = samples.contains(&(6000.0 / 32768.0));
                capture.lock().unwrap().push(samples);
                if !resumed {
                    entered.send(()).unwrap();
                    return Ok(if action == "oversized" {
                        "я".repeat(300)
                    } else {
                        "early mistake".to_owned()
                    });
                }
                if action == "error" {
                    return Err(transcription_error(
                        VoiceTranscriptionErrorKind::RuntimeFailure,
                        "late decode failed",
                    ));
                }
                if action == "cancel" {
                    cancel.cancel();
                }
                if action == "empty" {
                    return Ok(String::new());
                }
                Ok("corrected continuation".to_owned())
            }));
            let mut audio = vec![12000; 1024];
            audio.extend(vec![0; 16 * 512]);
            append_test_packet(&store, "session", 0, &audio);
            let (detector, checkpoints) = CheckpointDetector::new(vec![19, 29, 43]);
            std::thread::scope(|scope| {
                let worker = scope
                    .spawn(|| super::process(&store, "session", &token, &fake, false, detector));
                let before_commit = receive.recv_timeout(std::time::Duration::from_secs(5));
                // The first pause decode is frame 17 (2 speech + 15 quiet).
                // Frame 19 can only enter after the candidate was accepted.
                append_test_packet(&store, "session", 1, &[0; 512]);
                audio.extend([0; 512]);
                let accepted = checkpoints.recv_timeout(std::time::Duration::from_secs(5));
                let mut quiet_progress = Vec::new();
                if action == "seal" {
                    // Remain open: frame 29 proves ten additional quiet frames
                    // have completed after acceptance, with no changed ASR PCM.
                    append_test_packet(&store, "session", 2, &[0; 10 * 512]);
                    audio.extend([0; 10 * 512]);
                    quiet_progress.push((
                        checkpoints.recv_timeout(std::time::Duration::from_secs(5)),
                        calls.lock().unwrap().len(),
                    ));
                    // Seal is frame 40 (2 speech + 38 quiet); entering frame 43
                    // proves seal has run before Commit, not because of close.
                    append_test_packet(&store, "session", 3, &[0; 14 * 512]);
                    audio.extend([0; 14 * 512]);
                    quiet_progress.push((
                        checkpoints.recv_timeout(std::time::Duration::from_secs(5)),
                        calls.lock().unwrap().len(),
                    ));
                }
                if action == "cancel_paused" {
                    token.cancel();
                } else if !matches!(action, "commit" | "seal") {
                    append_test_packet(&store, "session", 2, &[6000; 1031]);
                }
                let _completion = store.finish_processing("session").unwrap();
                assert!(store.finish_processing("session").is_err());
                let result = worker.join().unwrap();
                // Always close/join before asserting a missing preview, so a
                // regression cannot strand the scoped worker on its inbox.
                before_commit.unwrap();
                assert_eq!(accepted.unwrap(), 19);
                for ((progress, count), expected_frame) in quiet_progress.into_iter().zip([29, 43])
                {
                    assert_eq!(progress.unwrap(), expected_frame);
                    assert_eq!(
                        count, 1,
                        "open-inbox quiet frames must reuse the accepted candidate"
                    );
                }
                let mut first_source = vec![12000.0 / 32768.0; 1024];
                first_source.extend([0.0; 7168]);
                assert_eq!(calls.lock().unwrap()[0], first_source);
                match action {
                    "empty" => {
                        let GatewayVoiceSessionPipelineOutcome::NoSpeech { no_speech, .. } =
                            result.unwrap()
                        else {
                            panic!("empty current decode must replace the candidate");
                        };
                        assert_eq!(
                            no_speech.reason,
                            VoiceTranscriptionNoSpeechReason::EmptyTranscript
                        );
                        assert_eq!(no_speech.total_samples, audio.len() + 1031);
                    }
                    "error" | "cancel" | "cancel_paused" => {
                        let Err(error) = result else {
                            panic!("earlier candidate must not become a successful recording");
                        };
                        assert_eq!(
                            error.kind,
                            if action != "error" {
                                VoiceErrorKind::Cancelled
                            } else {
                                VoiceErrorKind::TranscriptionFailed
                            }
                        );
                    }
                    _ => {
                        let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } =
                            result.unwrap()
                        else {
                            panic!("current transcript expected");
                        };
                        assert_eq!(
                            transcript.text,
                            match action {
                                "commit" | "seal" => "early mistake".to_owned(),
                                "oversized" =>
                                    format!("{} corrected continuation", "я".repeat(300)),
                                _ => "corrected continuation".to_owned(),
                            }
                        );
                        assert_eq!(
                            transcript.diagnostics.speech_samples,
                            if matches!(action, "commit" | "seal") {
                                8192
                            } else {
                                audio.len() + 1031
                            }
                        );
                        assert_eq!(
                            transcript.diagnostics.total_samples,
                            audio.len()
                                + if matches!(action, "commit" | "seal") {
                                    0
                                } else {
                                    1031
                                }
                        );
                        assert_eq!(
                            transcript.diagnostics.segment_count,
                            if action == "oversized" { 2 } else { 1 }
                        );
                    }
                }
            });
            assert_eq!(
                calls.lock().unwrap().len(),
                if matches!(action, "commit" | "cancel_paused" | "seal") {
                    1
                } else {
                    2
                }
            );
            if !matches!(action, "commit" | "cancel_paused" | "seal") {
                let mut expected: Vec<_> = if action == "oversized" {
                    vec![0.0; 1536]
                } else {
                    audio.iter().map(|s| f32::from(*s) / 32768.0).collect()
                };
                expected.extend([6000.0 / 32768.0; 1031]);
                assert_eq!(calls.lock().unwrap()[1], expected);
            }
        }
    }

    #[test]
    fn pause_context_expires_after_two_continuations_or_source_lifetime() {
        for lifetime in [false, true] {
            let mut audio = vec![12000; 1024];
            audio.extend(vec![0; 16 * 512]);
            if lifetime {
                audio.extend(vec![6000; 6 * 16000]);
            } else {
                for amplitude in [10000, 8000, 6000] {
                    audio.extend(vec![amplitude; 1024]);
                    if amplitude != 6000 {
                        audio.extend(vec![0; 16 * 512]);
                    }
                }
            }
            let calls = Arc::new(Mutex::new(Vec::new()));
            let capture = calls.clone();
            let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
                let samples = buffer.flattened_samples();
                let text = if samples.contains(&(12000.0 / 32768.0)) {
                    "held"
                } else {
                    "fresh"
                };
                capture.lock().unwrap().push(samples);
                Ok(text.to_owned())
            }));
            let result = closed_input(&audio, 137, false, &fake).unwrap();
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
                panic!("finite context expected");
            };
            assert_eq!(transcript.text, "held fresh");
            let calls = calls.lock().unwrap();
            assert_eq!(calls.len(), 2);
            // Holding survives the first continuation, but cannot survive all
            // source time / the third continuation until Commit.
            assert!(calls[0].contains(&(if lifetime { 6000.0 } else { 10000.0 } / 32768.0)));
            assert!(!calls[1].contains(&(12000.0 / 32768.0)));
            assert_eq!(transcript.diagnostics.speech_samples, audio.len());
        }
    }

    #[test]
    fn cancellation_between_partition_probes_prevents_next_native_call() {
        let store = open_store();
        let token = CancellationToken::new();
        let cancel = token.clone();
        let calls = Arc::new(Mutex::new(0));
        let count = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            *count.lock().unwrap() += 1;
            if buffer.speech_samples() < MAX_SPEECH_WINDOW_SAMPLES {
                cancel.cancel(); // left probe has actually returned
            }
            Ok("слово".to_owned())
        }));
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        // Skip previews so cancellation occurs specifically in the first probe.
        state.speech = vec![0.5; MAX_SPEECH_WINDOW_SAMPLES];
        state.speech_samples = state.speech.len();
        let error = state
            .advance_window(&store, "session", &token, &fake)
            .unwrap_err();
        assert_eq!(error.kind, VoiceErrorKind::Cancelled);
        assert_eq!(*calls.lock().unwrap(), 2);
        assert_eq!(state.window_start_sample, 0);
        assert!(state.text.is_empty());
    }

    #[test]
    fn contextual_windows_and_confirmed_text_are_session_local() {
        let fake = TestSpeechTranscriber(Arc::new(|buffer| {
            Ok(frame_transcript(&buffer.flattened_samples()))
        }));
        let first = vec![12000; 25 * 16000];
        let second = vec![6000; 31 * 16000];
        let (first_result, second_result) = std::thread::scope(|scope| {
            let first_worker = scope.spawn(|| closed_input(&first, 317, false, &fake));
            let second_worker = scope.spawn(|| closed_input(&second, 1600, true, &fake));
            (
                first_worker.join().unwrap().unwrap(),
                second_worker.join().unwrap().unwrap(),
            )
        });
        for (result, audio) in [(first_result, first), (second_result, second)] {
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
                panic!("session transcript expected");
            };
            let normalized: Vec<_> = audio
                .iter()
                .map(|sample| f32::from(*sample) / 32768.0)
                .collect();
            assert_eq!(transcript.text, frame_transcript(&normalized));
            assert_eq!(transcript.diagnostics.speech_samples, audio.len());
            assert!(transcript.diagnostics.segment_count > 1);
        }
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
        // Closed ingress coalesces previews; this complete outstanding span
        // needs no padding. Separate short-tail fixtures assert native padding.
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
        let mut audio = vec![12000; REFRESH_SAMPLES];
        audio.extend(vec![6000; REFRESH_SAMPLES]);
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
        for count in [0, 1, 479, 1024, 50000] {
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
    async fn cached_completion_does_not_wait_for_peer_native_and_rejects_stale_generation() {
        use super::super::{
            runtime::LoadedVoiceEngine,
            supervisor::{
                EagerVoiceEngineLoader, FilesystemVoiceModelInstaller, VoiceInputDesiredState,
                VoiceModelIdentity,
            },
        };
        use pioneer_protocol::GatewayVoiceInputProvider;

        // Release even on an assertion/timeout failure; the simulated native
        // call must never strand the runtime's blocking-worker shutdown.
        struct NativeRelease(Option<std::sync::mpsc::Sender<()>>);
        impl NativeRelease {
            fn release(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        impl Drop for NativeRelease {
            fn drop(&mut self) {
                self.release();
            }
        }

        for mode in ["cached", "no_speech", "stale"] {
            let store = GatewayVoiceSessionBufferStore::default();
            let workers = VoiceFinalizations::default();
            let supervisor = Arc::new(VoiceInputSupervisor::new(
                Arc::new(FilesystemVoiceModelInstaller::new(
                    crate::isolated_test_app_config().unwrap(),
                    std::path::PathBuf::new(),
                )),
                Arc::new(EagerVoiceEngineLoader),
            ));
            let identity = VoiceModelIdentity {
                provider: GatewayVoiceInputProvider::Local,
                model: "small".to_owned(),
            };
            let generation = supervisor
                .apply_desired(
                    VoiceInputDesiredState {
                        enabled: true,
                        provider: Some(identity.provider),
                        model: Some(identity.model.clone()),
                    },
                    false,
                )
                .unwrap()
                .generation;
            supervisor.mark_loading(generation).unwrap();
            let (entered_a, receive_a) = std::sync::mpsc::channel();
            let (entered_b, receive_b) = std::sync::mpsc::channel();
            let (release_b, released_b) = std::sync::mpsc::channel();
            let mut native_release = NativeRelease(Some(release_b));
            let released_b = Mutex::new(released_b);
            let calls = Arc::new(Mutex::new(Vec::new()));
            let captured = calls.clone();
            assert!(
                supervisor
                    .mark_ready(
                        generation,
                        identity,
                        LoadedVoiceEngine::TestStub(Arc::new(move |buffer| {
                            let samples = buffer.flattened_samples();
                            let is_a = samples.contains(&(12000.0 / 32768.0));
                            captured.lock().unwrap().push(samples);
                            if is_a {
                                entered_a.send(()).unwrap();
                                Ok("accepted pause candidate".to_owned())
                            } else {
                                // This callback runs through transcribe_for_session and
                                // transcribe_inner, while the real engine mutex is held.
                                entered_b.send(()).unwrap();
                                released_b.lock().unwrap().recv().unwrap();
                                Ok("peer result".to_owned())
                            }
                        }))
                    )
                    .unwrap()
            );
            let (detector_a, checkpoint_a) = CheckpointDetector::new(vec![19]);
            let (detector_b, _) = CheckpointDetector::new(Vec::new());
            let peer_cancellation = CancellationToken::new();
            for (id, connection, detector, cancellation) in [
                ("candidate", 1, detector_a, CancellationToken::new()),
                ("peer", 2, detector_b, peer_cancellation.clone()),
            ] {
                start_with_detector(
                    store.clone(),
                    id.to_owned(),
                    VoiceAudioFormat::pioneer_streaming_target(),
                    &workers,
                    workers.reserve(connection).unwrap(),
                    SessionSpeechTranscriber {
                        supervisor: supervisor.clone(),
                        generation,
                        is_moonshine_base: false,
                        cancellation,
                        fake: None,
                    },
                    true,
                    move |_| Ok(detector),
                )
                .unwrap();
            }
            let mut audio = vec![if mode == "no_speech" { 0 } else { 12000 }; 1024];
            audio.extend([0; 16 * 512]);
            append_test_packet(&store, "candidate", 0, &audio);
            if mode != "no_speech" {
                receive_a
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            }
            append_test_packet(&store, "candidate", 1, &[0; 512]);
            assert_eq!(
                checkpoint_a
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap(),
                19
            );
            if mode == "stale" {
                // A's successful candidate is already owned; no changed PCM
                // can force another decode to discover the generation change.
                supervisor
                    .apply_desired(VoiceInputDesiredState::default(), false)
                    .unwrap();
            }
            append_test_packet(&store, "peer", 0, &[6000; 1024]);
            let peer_completion = store.finish_processing("peer").unwrap();
            if mode != "stale" {
                receive_b
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            }
            let candidate_completion = store.finish_processing("candidate").unwrap();
            // Wait before releasing B; release B even on timeout, before
            // asserting it. The old mutex getter cannot finish this await.
            let before_peer_return =
                tokio::time::timeout(std::time::Duration::from_secs(5), candidate_completion).await;
            let peer_kept_lease = workers.reserve(2).is_none();
            if mode != "stale" {
                peer_cancellation.cancel();
            }
            let cancelled_peer_kept_lease = workers.reserve(2).is_none();
            native_release.release();
            let peer = tokio::time::timeout(std::time::Duration::from_secs(5), peer_completion)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                peer.outcome
                    .as_ref()
                    .err()
                    .expect("peer must be fenced")
                    .kind,
                if mode == "stale" {
                    VoiceErrorKind::TranscriptionFailed
                } else {
                    VoiceErrorKind::Cancelled
                }
            );
            drop(peer);
            let candidate = before_peer_return
                .expect("completion must arrive before B's native return")
                .unwrap();
            match mode {
                "stale" => {
                    let Err(error) = candidate.outcome else {
                        panic!("a cached old-generation result must fail");
                    };
                    assert_eq!(error.kind, VoiceErrorKind::TranscriptionFailed);
                    assert!(
                        receive_b.try_recv().is_err(),
                        "old-generation B must also be fenced before native entry"
                    );
                }
                "no_speech" => {
                    let GatewayVoiceSessionPipelineOutcome::NoSpeech { no_speech, .. } =
                        candidate.outcome.unwrap()
                    else {
                        panic!("silence expected");
                    };
                    assert_eq!(
                        no_speech.reason,
                        VoiceTranscriptionNoSpeechReason::NoSpeechDetected
                    );
                    assert_eq!(no_speech.total_samples, 19 * 512);
                }
                _ => {
                    let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } =
                        candidate.outcome.unwrap()
                    else {
                        panic!("cached transcript expected");
                    };
                    assert_eq!(transcript.text, "accepted pause candidate");
                    assert_eq!(transcript.diagnostics.total_samples, 19 * 512);
                    assert_eq!(transcript.diagnostics.speech_samples, 8192);
                    assert_eq!(transcript.diagnostics.segment_count, 1);
                }
            }
            if mode != "stale" {
                assert!(
                    peer_kept_lease && cancelled_peer_kept_lease,
                    "B must retain its lease until actual native return"
                );
            }
            drop(candidate.lease);
            workers.tasks.close();
            tokio::time::timeout(std::time::Duration::from_secs(5), workers.tasks.wait())
                .await
                .unwrap();
            let calls = calls.lock().unwrap();
            assert_eq!(calls.len(), if mode == "cached" { 2 } else { 1 });
            if mode != "no_speech" {
                let mut expected = vec![12000.0 / 32768.0; 1024];
                expected.extend([0.0; 7168]);
                assert_eq!(calls[0], expected);
            }
            if mode != "stale" {
                assert_eq!(calls.last().unwrap(), &vec![6000.0 / 32768.0; 1024]);
            }
            assert!(!workers.has_active_work());
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

    #[test]
    fn pause_uses_nearest_admissible_word_end_and_coincident_repetitions() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let fake = HypothesisTranscriber(move |buffer: &PreparedSpeechBuffer| {
            let count = buffer.speech_samples();
            captured.lock().unwrap().push(count);
            Ok(if count == 112000 {
                VoiceSpeechHypothesis {
                    text: "prefix да да tail".to_owned(),
                    segments: Some(
                        [
                            (0.0, 2.0, "prefix"),
                            (2.0, 4.8, "да"),
                            (4.8, 4.8, "да"),
                            (4.8, 7.0, "tail"),
                        ]
                        .into_iter()
                        .map(|(start, end, text)| transcribe_rs::TranscriptionSegment {
                            start,
                            end,
                            text: text.to_owned(),
                        })
                        .collect(),
                    ),
                    ..Default::default()
                }
            } else {
                VoiceSpeechHypothesis::text_only(
                    if count == LEFT_CONTEXT_SAMPLES {
                        "да да"
                    } else {
                        "да да tail"
                    }
                    .to_owned(),
                )
            })
        });
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        state.speech = vec![0.5; 112000];
        state.speech_samples = 112000;
        state
            .prepare_pause(&open_store(), "session", &CancellationToken::new(), &fake)
            .unwrap();
        assert_eq!(state.text, "prefix да да");
        assert_eq!(state.hypothesis, "tail");
        assert_eq!(state.window_start_sample, 52800);
        assert_eq!(state.speech.len(), 59200);
        assert!(state.speech.len() <= PAUSE_WINDOW_SAMPLES);
        assert_eq!(*calls.lock().unwrap(), vec![112000, 24000, 59200]);
    }

    #[test]
    fn equal_alignment_costs_hold_text_with_audio_once_then_force_finite_progress() {
        let tied = boundary_projection("a a a", "a", "a", Some((50, 100)));
        assert_eq!((tied.first, tied.last, tied.selected), (1, 3, 3));
        assert_eq!((tied.cost, tied.alternative_cost), (2, Some(2)));
        assert!(tied.ambiguous());
        let unique = boundary_projection("a b", "a", "b", Some((50, 100)));
        assert!(!unique.ambiguous());
        assert_eq!((unique.first, unique.last, unique.cost), (1, 1, 0));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            let count = buffer.speech_samples();
            captured.lock().unwrap().push(count);
            Ok(if count == 112000 {
                "a a a a"
            } else if count == 96000 || count == MAX_SPEECH_WINDOW_SAMPLES {
                "a a a"
            } else {
                "a"
            }
            .to_owned())
        }));
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        state.speech = vec![0.5; 96000];
        state.speech_samples = 96000;
        let store = open_store();
        let token = CancellationToken::new();
        state
            .prepare_pause(&store, "session", &token, &fake)
            .unwrap();
        state
            .prepare_pause(&store, "session", &token, &fake)
            .unwrap();
        assert_eq!(calls.lock().unwrap().len(), 3);
        assert_eq!(state.text, "a");
        assert_eq!(state.hypothesis, "a a");
        assert_eq!(state.speech.len(), 96000);
        assert_eq!(state.window_start_sample, 0);
        state.speech.resize(112000, 0.5);
        state.speech_samples = 112000;
        state.refresh(&store, "session", &token, &fake).unwrap();
        assert_eq!(state.text, "a");
        assert_eq!(state.hypothesis, "a a a"); // a new occurrence survives exclusion
        state.speech.resize(MAX_SPEECH_WINDOW_SAMPLES, 0.5);
        state.speech_samples = MAX_SPEECH_WINDOW_SAMPLES;
        state
            .advance_window(&store, "session", &token, &fake)
            .unwrap();
        assert_eq!(state.text, "a a");
        assert_eq!(state.hypothesis, "a");
        assert_eq!(state.window_start_sample, 144000);
        assert_eq!(calls.lock().unwrap().len(), 8);
        assert!(
            calls
                .lock()
                .unwrap()
                .iter()
                .all(|count| *count <= MAX_SPEECH_WINDOW_SAMPLES)
        );
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
        assert_eq!(timeline.pause_boundary(0, 100, 1000, 700), Some(512));
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
            let origin = (prepared + 512).saturating_sub(MAX_SPEECH_WINDOW_SAMPLES);
            timeline.retain_from(origin);
            assert_eq!(timeline.source_at(prepared + 511), Some(source + 511));
            if origin > 0 {
                assert_eq!(timeline.source_at(origin - 1), None);
            }
            assert!(timeline.intervals.len() <= MAX_SPEECH_WINDOW_SAMPLES.div_ceil(512) + 1);
        }
    }

    #[test]
    fn timing_validation_rejects_bad_order_padding_and_non_utf8_text_fences() {
        for (start, end) in [
            (f32::NAN, 1.0),
            (0.0, f32::INFINITY),
            (-0.1, 0.5),
            (1.0, 0.5),
            (0.0, 1.1),
        ] {
            let hypothesis = VoiceSpeechHypothesis {
                text: "да".to_owned(),
                segments: Some(vec![transcribe_rs::TranscriptionSegment {
                    start,
                    end,
                    text: "да".to_owned(),
                }]),
                ..Default::default()
            };
            assert!(timed_boundaries(&hypothesis, 16000).is_none());
        }
        let hypothesis = VoiceSpeechHypothesis {
            text: "да да".to_owned(),
            segments: Some(vec![
                transcribe_rs::TranscriptionSegment {
                    start: 0.0,
                    end: 0.7,
                    text: "да".to_owned(),
                },
                transcribe_rs::TranscriptionSegment {
                    start: 0.6,
                    end: 1.0,
                    text: "да".to_owned(),
                },
            ]),
            ..Default::default()
        };
        assert!(timed_boundaries(&hypothesis, 16000).is_none());
        assert!(!is_text_boundary("да tail", 1));
        assert!(!is_text_boundary("да tail", usize::MAX));
    }

    #[test]
    fn timestamp_cut_cannot_escape_the_retained_window_and_tail_bounds() {
        let fake = HypothesisTranscriber(|buffer: &PreparedSpeechBuffer| {
            Ok(if buffer.speech_samples() == 112000 {
                VoiceSpeechHypothesis {
                    text: "early admissible late".to_owned(),
                    segments: Some(
                        [
                            (0.0, 3.999, "early"),
                            (3.999, 4.0625, "admissible"),
                            (4.0625, 7.0, "late"),
                        ]
                        .into_iter()
                        .map(|(start, end, text)| transcribe_rs::TranscriptionSegment {
                            start,
                            end,
                            text: text.to_owned(),
                        })
                        .collect(),
                    ),
                    ..Default::default()
                }
            } else {
                assert_eq!(buffer.speech_samples(), 24000); // no partition probes
                VoiceSpeechHypothesis::text_only("admissible".to_owned())
            })
        });
        let mut state = IncrementalSpeech::new(false, test_detector()).unwrap();
        state.speech = vec![0.5; 112000];
        state.speech_samples = 112000;
        assert!(
            state
                .project_window(
                    64000,
                    64000,
                    88000,
                    false,
                    &open_store(),
                    "session",
                    &CancellationToken::new(),
                    &fake
                )
                .unwrap()
        );
        assert_eq!(state.window_start_sample + state.context_samples, 65000);
        assert_eq!(state.text, "early admissible");
        assert_eq!(state.hypothesis, "late");
        assert!(state.speech.len() <= 72000);
        assert!(state.speech.len() - state.context_samples >= 24000);
    }

    #[test]
    fn vad_pause_duration_beats_nearby_low_energy_during_window_planning() {
        struct Pauses(usize);
        impl VoiceActivityDetector for Pauses {
            fn speech_probability(&mut self, _: &[f32]) -> anyhow::Result<f32> {
                let index = self.0;
                self.0 += 1;
                Ok(
                    if (220..230).contains(&index) || (329..333).contains(&index) {
                        0.1
                    } else {
                        0.9
                    },
                )
            }
        }
        let mut state = IncrementalSpeech::new(false, Pauses(0)).unwrap();
        let fake = TestSpeechTranscriber(Arc::new(|_| Ok("speech".to_owned())));
        let store = open_store();
        let token = CancellationToken::new();
        for index in 0..562 {
            // Real noisy pauses have HIGHER energy than nearby voiced samples.
            let quiet = (220..230).contains(&index) || (329..333).contains(&index);
            let frame = vec![if quiet { 0.5 } else { 0.01 }; 512];
            let classified = state.vad.push_incremental_frame(&frame).unwrap();
            state.source_samples += 512;
            if let SmoothedVadFrame::Speech(samples) = classified {
                state
                    .accept_speech(&samples, &store, "session", &token, &fake)
                    .unwrap();
            }
        }
        let earliest = 96000;
        let latest = state.speech.len() - 48000;
        assert_eq!(state.preferred_boundary(earliest, latest), 115200);
        assert_ne!(
            partition_boundary_in(&state.speech, earliest, latest),
            115200
        );
        assert_eq!(state.timeline.source_at(115200), Some(115200));
        assert!(state.timeline.intervals.len() <= MAX_SPEECH_WINDOW_SAMPLES.div_ceil(512) + 6);
    }

    #[test]
    fn evicted_positive_vad_frame_separates_pauses_but_a_quiet_gap_keeps_continuity() {
        struct Pauses {
            frame: usize,
            middle_probability: f32,
        }
        impl VoiceActivityDetector for Pauses {
            fn speech_probability(&mut self, _: &[f32]) -> anyhow::Result<f32> {
                let index = self.frame;
                self.frame += 1;
                Ok(if index == 215 {
                    self.middle_probability
                } else if (200..232).contains(&index) || (274..294).contains(&index) {
                    0.0
                } else {
                    0.9
                })
            }
        }
        // Established speech, 15 quiet frames, one disputed frame, 16 quiet,
        // two onset frames, 40 voiced frames, then a competing 20-frame pause.
        // Only the middle probability changes; prefill removes the same five
        // source frames, including that middle frame, in both cases.
        for (middle_probability, expected_cut, expected_source) in [
            (0.9, 142848, 145408), // the actual 20-frame pause wins
            (0.0, 109568, 112128), // a continuous 32-frame pause wins at its seam
        ] {
            let mut state = IncrementalSpeech::new(
                false,
                Pauses {
                    frame: 0,
                    middle_probability,
                },
            )
            .unwrap();
            let fake = TestSpeechTranscriber(Arc::new(|_| Ok("speech".to_owned())));
            let store = open_store();
            let token = CancellationToken::new();
            for _ in 0..562 {
                let classified = state.vad.push_incremental_frame(&[0.5; 512]).unwrap();
                state.source_samples += 512;
                if let SmoothedVadFrame::Speech(samples) = classified {
                    // Production acceptance transfers emitted VAD metadata,
                    // including any splits at a refresh boundary, to the planner.
                    state
                        .accept_speech(&samples, &store, "session", &token, &fake)
                        .unwrap();
                }
            }
            assert_eq!(state.speech_samples, 557 * 512);
            assert_eq!(state.timeline.prepared_at(215 * 512), None);
            let latest = state.speech.len() - REFRESH_SAMPLES;
            assert_eq!(state.preferred_boundary(96000, latest), expected_cut);
            assert_eq!(
                state.timeline.source_at(expected_cut),
                Some(expected_source)
            );

            // Clip inside the first quiet edge. Its run identity must survive
            // while sample coordinates and the returned local cut move together.
            let origin = 201 * 512 + 137;
            state.timeline.retain_from(origin);
            assert_eq!(state.timeline.source_at(origin), Some(origin));
            assert_eq!(
                state.timeline.pause_boundary(
                    origin,
                    0,
                    latest - origin,
                    (96000 + latest) / 2 - origin,
                ),
                Some(expected_cut - origin),
            );
        }
    }

    #[test]
    fn final_pause_seal_keeps_the_original_source_time_target_with_32ms_frames() {
        struct SpeechThenQuiet(usize);
        impl VoiceActivityDetector for SpeechThenQuiet {
            fn speech_probability(&mut self, _: &[f32]) -> anyhow::Result<f32> {
                self.0 += 1;
                Ok(if self.0 <= 2 { 0.9 } else { 0.0 })
            }
        }
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            captured.lock().unwrap().push(buffer.flattened_samples());
            Ok("speech".to_owned())
        }));
        let mut state = IncrementalSpeech::new(false, SpeechThenQuiet(0)).unwrap();
        let store = open_store();
        let token = CancellationToken::new();
        for index in 0..40 {
            let frame = vec![if index < 2 { 0.5 } else { 0.0 }; 512];
            let classified = state.vad.push_incremental_frame(&frame).unwrap();
            state.source_samples += 512;
            match classified {
                SmoothedVadFrame::Speech(samples) => state
                    .accept_speech(&samples, &store, "session", &token, &fake)
                    .unwrap(),
                SmoothedVadFrame::Noise => state.pause(&store, "session", &token, &fake).unwrap(),
            }
            if index == 38 {
                assert_eq!(state.vad.quiet_samples(), 18944); // 1184 ms <1200
                assert!(state.text.is_empty());
                assert_eq!(state.speech.len(), 8192);
            }
        }
        assert_eq!(state.vad.quiet_samples(), 19456); // 1216 ms: first >=1200
        assert_eq!(state.text, "speech");
        assert!(state.speech.is_empty());
        assert_eq!(state.speech_samples, 8192);
        let mut expected = vec![0.5; 1024];
        expected.extend([0.0; 7168]);
        assert_eq!(*calls.lock().unwrap(), vec![expected]); // cached seal, no repeat ASR
    }
}
