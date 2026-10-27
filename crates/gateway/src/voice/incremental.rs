//! Session-owned contextual ASR over the existing one-shot engines.
//! Replay is bounded to the unresolved span and a short confirmed left context.
//! Packet boundaries never confirm text. VAD/Commit or a bounded window
//! projection advances the confirmed prefix; projection is an ASR heuristic.
#[cfg(test)]
use super::transcription::{VoiceTranscriptionErrorKind, transcription_error};
use super::{
    finalization::{VoiceFinalizationLease, VoiceFinalizations},
    session_buffer::{GatewayVoiceSessionBufferStore, VoiceChunkIngestError},
    supervisor::VoiceInputSupervisor,
    transcription::{
        PreparedSpeechBuffer, VoiceSpeechHypothesis, VoiceSpeechTranscriber, VoiceTranscript,
        VoiceTranscriptionDiagnostics, VoiceTranscriptionError, VoiceTranscriptionNoSpeech,
        VoiceTranscriptionNoSpeechReason, VoiceTranscriptionStrategy,
    },
    vad::{
        EnergyVoiceActivityDetector, SmoothedVadFrame, SmoothedVoiceVad, VOICE_VAD_FRAME_SAMPLES,
        VoiceSpeechSegment, VoiceVadConfig,
    },
};
use pioneer_protocol::{
    VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ, VoiceAudioFormat, VoiceError, VoiceErrorKind,
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

// Frame-aligned engineering bounds, not measured quality/latency optima.
const REFRESH_SAMPLES: usize = 3 * VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ as usize;
const MIN_ADVANCE_SAMPLES: usize = 2 * REFRESH_SAMPLES;
const LEFT_CONTEXT_SAMPLES: usize = REFRESH_SAMPLES / 2;
const MAX_ALIGNMENT_UNITS: usize = 2048;
const MAX_CONTEXT_ANCHOR_BYTES: usize = 256;
const MAX_SPEECH_WINDOW_SAMPLES: usize = 6 * REFRESH_SAMPLES;
const ENERGY_VAD_THRESHOLD_FLOOR: f32 = 0.02;
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
                if transcribe_while_recording {
                    process(
                        &store,
                        &session_id,
                        &cancellation,
                        &transcriber,
                        transcriber.is_moonshine_base,
                    )
                } else {
                    process_buffered(
                        &store,
                        &session_id,
                        &cancellation,
                        &transcriber,
                        transcriber.is_moonshine_base,
                    )
                }
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
fn process_buffered(
    store: &GatewayVoiceSessionBufferStore,
    session_id: &str,
    cancellation: &CancellationToken,
    transcriber: &impl VoiceSpeechTranscriber,
    is_moonshine_base: bool,
) -> Result<GatewayVoiceSessionPipelineOutcome, VoiceError> {
    ensure_active(cancellation)?;
    let chunks = store
        .take_closed_processing_chunks(session_id)
        .map_err(|error| error.into_voice_error())?;
    ensure_active(cancellation)?;
    let key = store.processing_startup_key(session_id);
    pioneer_observability::turn_startup::scope_sync(key, || {
        let detector = EnergyVoiceActivityDetector::new(ENERGY_VAD_THRESHOLD_FLOOR)
            .map_err(|_| worker_error("failed to initialize voice VAD"))?;
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
                        if let SmoothedVadFrame::Speech(samples) = vad
                            .push_incremental_frame(&pending)
                            .map_err(|_| worker_error("failed to segment voice audio"))?
                        {
                            speech.extend(samples);
                        }
                        pending.clear();
                    }
                }
            }
            speech.extend(
                vad.finish_incremental_tail(&pending)
                    .map_err(|_| worker_error("failed to segment final voice audio"))?,
            );
        }
        ensure_active(cancellation)?;
        let signal_stats = signal.finish();
        let speech_samples = speech.len();
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
            .transcribe_speech(&buffer)
            .map_err(|error| error.into_voice_error())?
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

fn process(
    store: &GatewayVoiceSessionBufferStore,
    session_id: &str,
    cancellation: &CancellationToken,
    transcriber: &impl VoiceSpeechTranscriber,
    is_moonshine_base: bool,
) -> Result<GatewayVoiceSessionPipelineOutcome, VoiceError> {
    let mut state = IncrementalSpeech::new(is_moonshine_base)?;
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
                state.pending.clear();
                match classified {
                    SmoothedVadFrame::Speech(samples) => state.accept_speech(
                        &samples,
                        store,
                        session_id,
                        cancellation,
                        transcriber,
                    )?,
                    SmoothedVadFrame::Noise => {
                        state.flush(store, session_id, cancellation, transcriber)?
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
    state.accept_speech(&tail, store, session_id, cancellation, transcriber)?;
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

struct IncrementalSpeech {
    is_moonshine_base: bool,
    vad: SmoothedVoiceVad<EnergyVoiceActivityDetector>,
    pending: Vec<f32>,
    speech: Vec<f32>,
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
    decoded_samples: usize,
    next_refresh_samples: usize,
    signal: SignalAccumulator,
}
impl IncrementalSpeech {
    fn new(is_moonshine_base: bool) -> Result<Self, VoiceError> {
        let detector = EnergyVoiceActivityDetector::new(ENERGY_VAD_THRESHOLD_FLOOR)
            .map_err(|_| worker_error("failed to initialize voice VAD"))?;
        Ok(Self {
            is_moonshine_base,
            vad: SmoothedVoiceVad::new(detector, VoiceVadConfig::default())
                .map_err(|_| worker_error("failed to initialize voice VAD"))?,
            pending: Vec::with_capacity(VOICE_VAD_FRAME_SAMPLES),
            speech: Vec::with_capacity(MAX_SPEECH_WINDOW_SAMPLES),
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
            next_refresh_samples: REFRESH_SAMPLES,
            signal: SignalAccumulator::default(),
        })
    }

    fn accept_speech(
        &mut self,
        mut samples: &[f32],
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
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
            self.speech.extend_from_slice(&samples[..take]);
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
        if self.decoded_samples != self.speech.len() {
            let full = self.decode(&self.speech, store, id, cancellation, transcriber)?;
            let start = if self.context_samples == 0 {
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
        self.refresh(store, id, cancellation, transcriber)?;
        let preferred_cut = partition_boundary(&self.speech, self.context_samples);
        let earliest = self.context_samples + MIN_ADVANCE_SAMPLES;
        let latest = self.speech.len() - REFRESH_SAMPLES;
        let timing = timed_boundaries(&self.full_hypothesis, self.speech.len());
        let timed_cut = timing.as_ref().and_then(|boundaries| {
            boundaries
                .iter()
                .filter(|boundary| {
                    boundary.end >= earliest
                        && boundary.end <= latest
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
            (
                preferred_cut,
                align_boundary(&self.full_hypothesis.text, &left.text, &right.text),
            )
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
        append_text(
            &mut self.text,
            &self.full_hypothesis.text[self.owned_text_start..text_end],
            self.owned_text_start > 0,
        );
        self.fragments += 1;
        self.speech.drain(..context_start);
        self.window_start_sample += context_start;
        self.context_samples = LEFT_CONTEXT_SAMPLES;
        self.context_text = context_text;
        // The tail was decoded with at least as much context and the identical
        // source end. Reuse it if Commit adds no source; no fabricated timings.
        let mut carried = self.context_text.clone();
        append_text(&mut carried, &tail, true);
        self.owned_text_start = self.context_text.len();
        self.full_hypothesis = VoiceSpeechHypothesis::text_only(carried);
        self.hypothesis = tail.trim().to_owned();
        self.decoded_samples = self.speech.len();
        Ok(())
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
        self.refresh(store, id, cancellation, transcriber)?;
        append_text(
            &mut self.text,
            &self.full_hypothesis.text[self.owned_text_start..],
            self.owned_text_start > 0,
        );
        self.fragments += 1;
        self.window_start_sample += self.speech.len();
        self.speech.clear();
        self.hypothesis.clear();
        self.full_hypothesis = VoiceSpeechHypothesis::default();
        self.owned_text_start = 0;
        self.context_samples = 0;
        self.context_text.clear();
        self.decoded_samples = 0;
        self.next_refresh_samples = REFRESH_SAMPLES;
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
            transcriber.transcribe_hypothesis(&buffer)
        })
        .map_err(|error| error.into_voice_error())?;
        ensure_active(cancellation)?;
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
fn partition_boundary(samples: &[f32], context_samples: usize) -> usize {
    let first = (context_samples + MIN_ADVANCE_SAMPLES).div_ceil(VOICE_VAD_FRAME_SAMPLES);
    let last = (samples.len() - REFRESH_SAMPLES) / VOICE_VAD_FRAME_SAMPLES;
    let midpoint = (first + last) / 2;
    (first..=last)
        .min_by(|a, b| {
            let energy = |frame: usize| {
                let cut = frame * VOICE_VAD_FRAME_SAMPLES;
                samples[cut - VOICE_VAD_FRAME_SAMPLES..cut + VOICE_VAD_FRAME_SAMPLES]
                    .iter()
                    .map(|sample| f64::from(*sample).powi(2))
                    .sum::<f64>()
            };
            energy(*a)
                .total_cmp(&energy(*b))
                .then_with(|| a.abs_diff(midpoint).cmp(&b.abs_diff(midpoint)))
        })
        .expect("partition interval is nonempty")
        * VOICE_VAD_FRAME_SAMPLES
}

struct TimedBoundary {
    start: usize,
    end: usize,
    text_end: usize,
}

// SDK "segments" can be subword tokens. Timing alone must not confirm a
// fragment inside a whitespace-delimited word or separate its punctuation.
fn is_text_boundary(text: &str, end: usize) -> bool {
    end == 0 || end == text.len() || text[end..].starts_with(char::is_whitespace)
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
    let mut whole_units = alignment_units(whole, false);
    let mut left_units = alignment_units(left, false);
    let mut right_units = alignment_units(right, false);
    // The partition can lie inside one coarse unit even with other correct
    // words around it. Boundary probe units that form a prefix of that unit
    // indicate insufficient resolution, NOT confirmed ownership. Keep the
    // one-unit fallback for disagreement with no such textual match.
    let scalar = left_units
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
    // A tie cannot establish true acoustic ownership. Prefer the better prefix
    // fit so new repeated tail words cannot be assigned to old context merely
    // because the tail grew; then use probe token counts as a deterministic
    // prior. Neither rule is evidence of correct acoustic ownership.
    let prior =
        whole_units.len() * left_units.len() / (left_units.len() + right_units.len()).max(1);
    let split = (0..=whole_units.len())
        .min_by_key(|index| {
            (
                prefix[*index] + suffix[whole_units.len() - *index],
                prefix[*index],
                index.abs_diff(prior),
                *index,
            )
        })
        .unwrap_or(0);
    if split == 0 {
        0
    } else {
        whole_units[split - 1].end
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
    use super::super::transcription::TestSpeechTranscriber;
    use super::*;
    use pioneer_protocol::{VoiceAudioEncoding, VoiceChunkFrameHeader};
    use std::sync::Mutex;

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

    // Feed the production processor with real PCM normalization and segmentation.
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
        let mut audio = vec![12000; 960];
        audio.extend(vec![0; 480 * 40]);
        audio.extend(vec![16000; 960 + 79]);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let capture = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            capture.lock().unwrap().push(buffer.flattened_samples());
            Ok("joined".to_owned())
        }));
        closed_input_mode(&audio, 137, false, &fake, true).unwrap();
        // Existing VAD: fifteen frames of hangover after first speech; fifteen
        // preceding frames (fourteen noise + first onset) at the next onset.
        let mut expected = vec![12000.0 / 32768.0; 960];
        expected.extend(vec![0.0; 480 * (15 + 14)]);
        expected.extend(vec![16000.0 / 32768.0; 960 + 79]);
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
            append_test_packet(&store, "session", 3, &[12000; 1600]).missing_chunks,
            2
        );
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let fake = TestSpeechTranscriber(Arc::new(move |buffer| {
            captured.lock().unwrap().push(buffer.flattened_samples());
            Ok("unique".to_owned())
        }));
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| process_buffered(&store, "session", &token, &fake, false));
            append_test_packet(&store, "session", 4, &[16000; 79]);
            // The close and append boundary is the production inbox mutex.
            let _completion = store.finish_processing("session").unwrap();
            assert!(
                store
                    .append_chunk(
                        VoiceChunkFrameHeader {
                            session_id: "session".to_owned(),
                            sequence: 5,
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
        // Preload four seconds before starting the real processor. At the 3 s
        // tick there is still backlog; the next 3 s tick will never arrive.
        for sequence in 0..40 {
            let pcm: Vec<_> = std::iter::repeat_n(12000_i16, 1600)
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
            Ok(if buffer.speech_samples() < 64000 {
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
            assert_eq!(before_commit.unwrap(), 63840);
            let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
                panic!("contextual transcript expected");
            };
            assert_eq!(receive.try_recv().unwrap(), 64000);
            assert!(receive.try_recv().is_err());
            assert_eq!(transcript.text, "исправлено");
            assert_eq!(transcript.diagnostics.speech_samples, 64000);
            assert_eq!(transcript.diagnostics.total_samples, 64000);
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
        let mut state = IncrementalSpeech::new(false).unwrap();
        state
            .accept_speech(
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
            .accept_speech(
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
        let mut state = IncrementalSpeech::new(false).unwrap();
        state
            .accept_speech(&[0.5; 7], &store, "session", &token, &fake)
            .unwrap();
        state.flush(&store, "session", &token, &fake).unwrap();
        let confirmed_end = state.window_start_sample;
        state
            .accept_speech(
                &vec![0.25; REFRESH_SAMPLES],
                &store,
                "session",
                &token,
                &fake,
            )
            .unwrap();
        assert_eq!(state.hypothesis, "ошибочный хвост");
        state
            .accept_speech(
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
            .accept_speech(&[0.75; 7], &store, "session", &token, &fake)
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
            .chunks(VOICE_VAD_FRAME_SAMPLES)
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
                if index / VOICE_VAD_FRAME_SAMPLES % 4 < 2 {
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
                        .chunks(VOICE_VAD_FRAME_SAMPLES)
                        .enumerate()
                        .filter_map(|(index, frame)| {
                            let text = frame_transcript(frame);
                            (!text.is_empty()).then(|| transcribe_rs::TranscriptionSegment {
                                start: (index * VOICE_VAD_FRAME_SAMPLES) as f32 / 16000.0,
                                end: ((index * VOICE_VAD_FRAME_SAMPLES + frame.len()) as f32)
                                    / 16000.0,
                                text,
                            })
                        })
                        .collect()
                });
                Ok(VoiceSpeechHypothesis { text, segments })
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
                .chunks(VOICE_VAD_FRAME_SAMPLES)
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
            .map(|index| pattern[index / VOICE_VAD_FRAME_SAMPLES % pattern.len()])
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
                    assert!(normalized.chunks(VOICE_VAD_FRAME_SAMPLES).enumerate().any(
                        |(frame, _)| {
                            let start = frame * VOICE_VAD_FRAME_SAMPLES;
                            normalized.get(start..start + call.len()) == Some(call.as_slice())
                        }
                    ));
                }
            }
        }
    }

    #[test]
    fn non_cjk_unspaced_text_refines_a_single_unit_through_multiple_advances() {
        fn recognize(samples: &[f32], symbols: &[char]) -> String {
            samples
                .chunks(VOICE_VAD_FRAME_SAMPLES)
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
                .map(|index| 12000 + 500 * (index / VOICE_VAD_FRAME_SAMPLES % symbols.len()) as i16)
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
                    assert!(normalized.chunks(VOICE_VAD_FRAME_SAMPLES).enumerate().any(
                        |(frame, _)| {
                            let start = frame * VOICE_VAD_FRAME_SAMPLES;
                            normalized.get(start..start + call.len()) == Some(call.as_slice())
                        }
                    ));
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
            for frame in samples.chunks(VOICE_VAD_FRAME_SAMPLES) {
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
            .map(|index| 12000 + 500 * (index / VOICE_VAD_FRAME_SAMPLES % 9) as i16)
            .collect();
        // The first whole and right probe end in OK; the left/context probes
        // do not. New audio extends beyond OK before the changed final refresh.
        audio[MAX_SPEECH_WINDOW_SAMPLES - VOICE_VAD_FRAME_SAMPLES..MAX_SPEECH_WINDOW_SAMPLES]
            .fill(26000);
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
                                Ok(VoiceSpeechHypothesis { text, segments })
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
                    });
                }
                168000 => "начало да",
                120000 => "конец",
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
            Ok(VoiceSpeechHypothesis { text, segments })
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
        let mut state = IncrementalSpeech::new(false).unwrap();
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
            MAX_SPEECH_WINDOW_SAMPLES + 15 * VOICE_VAD_FRAME_SAMPLES
        );
        assert_eq!(signal_stats.total_samples, audio.len());
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 5);
        let cut = calls[1].len();
        let mut final_source =
            vec![12000.0 / 32768.0; MAX_SPEECH_WINDOW_SAMPLES - cut + LEFT_CONTEXT_SAMPLES];
        final_source.extend(vec![0.0; 15 * VOICE_VAD_FRAME_SAMPLES]);
        assert_eq!(calls[4], final_source);
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
                .chunks(VOICE_VAD_FRAME_SAMPLES)
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
                if index / VOICE_VAD_FRAME_SAMPLES % 4 < 2 {
                    12000
                } else {
                    6000
                }
            })
            .collect();
        audio[..VOICE_VAD_FRAME_SAMPLES].fill(24000);
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
        let mut state = IncrementalSpeech::new(false).unwrap();
        state
            .accept_speech(
                &normalized[..REFRESH_SAMPLES],
                &store,
                "session",
                &token,
                &fake,
            )
            .unwrap();
        assert!(state.hypothesis.starts_with("ОШИБКА"));
        state
            .accept_speech(
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
            })
        });
        let mut state = IncrementalSpeech::new(false).unwrap();
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
            })
        });
        let store = open_store();
        let token = CancellationToken::new();
        let mut state = IncrementalSpeech::new(false).unwrap();
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
    fn vad_pause_seals_contextual_span_and_preserves_short_moonshine_tail() {
        let mut audio = vec![12000; 4 * REFRESH_SAMPLES];
        audio.extend(vec![0; 16 * VOICE_VAD_FRAME_SAMPLES]);
        audio.extend([6000; 7]);
        let fake = TestSpeechTranscriber(Arc::new(|buffer| {
            assert!(buffer.speech_samples() >= MIN_MOONSHINE_BASE_INPUT_SAMPLES);
            let samples = buffer.flattened_samples();
            Ok(if samples.iter().any(|sample| *sample > 0.3) {
                "да, да; ещё раз, ещё раз"
            } else {
                "короткий хвост"
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
            assert_eq!(transcript.diagnostics.segment_count, 2);
            assert_eq!(signal_stats.total_samples, audio.len());
            assert_eq!(signal_stats.non_zero_samples, 4 * REFRESH_SAMPLES + 7);
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
        let mut state = IncrementalSpeech::new(false).unwrap();
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
    fn moonshine_native_preparation_pads_only_short_input_with_silence() {
        for count in [0, 1, 7, 1599, 1600, 1601, 48000] {
            let source: Vec<_> = (0..count)
                .map(|index| [0.25, -0.5, 0.75][index % 3])
                .collect();
            let mut samples = source.clone();
            let pointer = samples.as_ptr();
            let capacity = samples.capacity();
            prepare_moonshine_base_samples(&mut samples);
            assert_eq!(samples.len(), if count == 0 { 0 } else { count.max(1600) });
            assert_eq!(&samples[..count], source.as_slice());
            assert!(samples[count..].iter().all(|sample| *sample == 0.0));
            if count >= 1600 {
                assert_eq!(
                    samples.as_ptr(),
                    pointer,
                    "adequate input must keep its allocation"
                );
                assert_eq!(samples.capacity(), capacity);
            }
            assert!(samples.len() <= MAX_SPEECH_WINDOW_SAMPLES);
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
        for count in [1, 7, 479, 480, 487, 959, 960, 967, 1599, 1600, 1601] {
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
        for count in [1, 479, 480, 487, 959, 960, 967] {
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
        for count in [0, 1, 479, 960, 50000] {
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
            for (id, amplitude, count) in [("first", 12000_i16, 967), ("second", 6000_i16, 1)] {
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
                assert_eq!(buffer.flattened_samples(), vec![12000.0 / 32768.0; 967]);
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
            assert_eq!(first.diagnostics.total_samples, 967);
            assert_eq!(second.diagnostics.total_samples, 1);
        }
    }

    #[test]
    fn voice_incremental_next_onset_does_not_replay_previous_hangover() {
        let mut audio = vec![12000; 960];
        audio.extend(vec![0; 16 * VOICE_VAD_FRAME_SAMPLES]);
        audio.extend(vec![6000; 967]);
        let (_, calls) = partitioned(&audio, 317);
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls.concat(),
            audio
                .iter()
                .map(|s| f32::from(*s) / 32768.0)
                .collect::<Vec<_>>()
        );
    }
}
