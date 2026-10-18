//! Session-owned incremental chunked ASR, using the existing one-shot engines.
//! This is not a native neural decoder stream. Network packet boundaries never
//! delimit inference, and processed audio is not replayed at Commit.
#[cfg(test)]
use super::transcription::{VoiceTranscriptionErrorKind, transcription_error};
use super::{
    finalization::{VoiceFinalizationLease, VoiceFinalizations},
    session_buffer::{GatewayVoiceSessionBufferStore, VoiceChunkIngestError},
    supervisor::VoiceInputSupervisor,
    transcription::{
        PreparedSpeechBuffer, VoiceSpeechTranscriber, VoiceTranscript,
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

// Bound speech accumulated before scheduling inference to three seconds, even
// without a pause. Waiting for the shared engine can still delay the call.
// An engineering tradeoff, not a measured quality/latency optimum. Frame aligned.
const MAX_SPEECH_WINDOW_SAMPLES: usize = 3 * VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ as usize;
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
        #[cfg(test)]
        if let Some(fake) = &self.fake {
            if self.cancellation.is_cancelled() || self.supervisor.generation() != self.generation {
                return Err(transcription_error(
                    VoiceTranscriptionErrorKind::ModelUnavailable,
                    "voice session model is no longer available",
                ));
            }
            return fake(buffer);
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
    workers.tasks.spawn_blocking(move || {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            process(
                &store,
                &session_id,
                &cancellation,
                &transcriber,
                transcriber.is_moonshine_base,
            )
        }))
        .unwrap_or_else(|_| Err(worker_error("voice processing worker panicked")));
        store.processing_finished(&session_id);
        let _ = sender.send(VoiceProcessingCompletion { outcome, lease });
    });
    Ok(())
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
    speech_samples: usize,
    fragments: usize,
    text: String,
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
            speech: Vec::new(),
            speech_samples: 0,
            fragments: 0,
            text: String::new(),
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
            let take = samples
                .len()
                .min(MAX_SPEECH_WINDOW_SAMPLES - self.speech.len());
            self.speech.extend_from_slice(&samples[..take]);
            samples = &samples[take..];
            if self.speech.len() == MAX_SPEECH_WINDOW_SAMPLES {
                self.flush(store, id, cancellation, transcriber)?;
            }
        }
        Ok(())
    }

    fn flush(
        &mut self,
        store: &GatewayVoiceSessionBufferStore,
        id: &str,
        cancellation: &CancellationToken,
        transcriber: &impl VoiceSpeechTranscriber,
    ) -> Result<(), VoiceError> {
        if self.speech.is_empty() {
            return Ok(());
        }
        ensure_active(cancellation)?;
        let mut samples = std::mem::take(&mut self.speech);
        // Count source speech before native-only padding. Signal stats already
        // come from ingested PCM; neither includes artificial silence.
        let source_count = samples.len();
        if self.is_moonshine_base {
            prepare_moonshine_base_samples(&mut samples);
        }
        // This buffer describes native input; final diagnostics use source_count above.
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
        // Snapshot Commit's scope before starting this call. A call already in
        // progress at Commit keeps None and is never charged to turn startup.
        let key = store.processing_startup_key(id);
        let raw = pioneer_observability::turn_startup::scope_sync(key, || {
            transcriber.transcribe_speech(&buffer)
        })
        .map_err(|error| error.into_voice_error())?;
        ensure_active(cancellation)?;
        self.speech_samples += source_count;
        self.fragments += 1;
        let text = raw.trim();
        if !text.is_empty() {
            if !self.text.is_empty() {
                self.text.push(' ');
            }
            self.text.push_str(text);
        }
        Ok(())
    }
}

fn prepare_moonshine_base_samples(samples: &mut Vec<f32>) {
    // Append only synthetic silence, never replay an earlier fragment. Resize
    // the owned short fragment in place; full windows need no copy or padding.
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
            Ok(format!("part{}", calls.len()))
        }));
        let result = process(&store, "session", &cancellation, &fake, is_moonshine_base).unwrap();
        let captured = calls.lock().unwrap().clone();
        (result, captured)
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
    fn moonshine_native_tail_keeps_source_diagnostics_and_each_prefix_once() {
        let mut audio = vec![12000_i16; 48000];
        audio.extend(vec![6000; 48000]);
        audio.extend([3000; 7]);
        let (result, calls) = partitioned_input(&audio, 317, true);
        let (_, other_packets) = partitioned_input(&audio, 1600, true);
        assert_eq!(calls, other_packets);
        assert_eq!(
            calls.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![48000, 48000, 1600]
        );
        let mut recovered_source = Vec::new();
        for (call, source_count) in calls.iter().zip([48000, 48000, 7]) {
            recovered_source.extend_from_slice(&call[..source_count]);
            assert!(call[source_count..].iter().all(|sample| *sample == 0.0));
        }
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
        assert_eq!(transcript.diagnostics.segment_count, 3);
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
        let mut audio = vec![12000; MAX_SPEECH_WINDOW_SAMPLES];
        audio.extend(vec![6000; MAX_SPEECH_WINDOW_SAMPLES]);
        audio.extend([3000; 7]);
        let (_, baseline) = partitioned(&audio, 1600);
        assert_eq!(
            baseline.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![48000, 48000, 7]
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
            let first = scope.spawn(|| process(&store, "first", &cancellation, &first, false));
            let second = scope.spawn(|| process(&store, "second", &cancellation, &second, false));
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
