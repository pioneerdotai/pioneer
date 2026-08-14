// Gateway transcription boundary. The concrete Parakeet backend plugs in here.

use super::vad::{VoiceNoSpeechReason, VoiceSpeechSegment, VoiceVadSegmentationOutcome};
use pioneer_config::GatewayVoiceTranscriptionStrategy;
use pioneer_protocol::{VoiceError, VoiceErrorKind};
#[cfg(test)]
use std::panic::{AssertUnwindSafe, catch_unwind};
#[cfg(test)]
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PreparedSpeechBuffer {
    pub(crate) sample_rate_hz: u32,
    pub(crate) total_samples: usize,
    pub(crate) segments: Vec<VoiceSpeechSegment>,
}

impl PreparedSpeechBuffer {
    pub(crate) fn from_segments(
        sample_rate_hz: u32,
        total_samples: usize,
        segments: Vec<VoiceSpeechSegment>,
    ) -> VoiceTranscriptionOutcome {
        if segments.is_empty() || segments.iter().all(|segment| segment.samples.is_empty()) {
            return VoiceTranscriptionOutcome::NoSpeech(VoiceTranscriptionNoSpeech {
                reason: VoiceTranscriptionNoSpeechReason::EmptyBuffer,
                total_samples,
            });
        }

        VoiceTranscriptionOutcome::Ready(Self {
            sample_rate_hz,
            total_samples,
            segments,
        })
    }

    pub(crate) fn from_vad_outcome(
        sample_rate_hz: u32,
        outcome: VoiceVadSegmentationOutcome,
    ) -> VoiceTranscriptionOutcome {
        match outcome {
            VoiceVadSegmentationOutcome::NoSpeech {
                total_samples,
                reason,
            } => VoiceTranscriptionOutcome::NoSpeech(VoiceTranscriptionNoSpeech {
                reason: reason.into(),
                total_samples,
            }),
            VoiceVadSegmentationOutcome::Speech {
                total_samples,
                segments,
            } => Self::from_segments(sample_rate_hz, total_samples, segments),
        }
    }

    pub(crate) fn speech_samples(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| segment.samples.len())
            .sum()
    }

    pub(crate) fn flattened_samples(&self) -> Vec<f32> {
        let mut samples = Vec::with_capacity(self.speech_samples());
        for segment in &self.segments {
            samples.extend_from_slice(segment.samples.as_slice());
        }
        samples
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum VoiceTranscriptionOutcome {
    Ready(PreparedSpeechBuffer),
    NoSpeech(VoiceTranscriptionNoSpeech),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VoiceTranscriptionNoSpeech {
    pub(crate) reason: VoiceTranscriptionNoSpeechReason,
    pub(crate) total_samples: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceTranscriptionNoSpeechReason {
    TooShort,
    NoSpeechDetected,
    EmptyBuffer,
    EmptyTranscript,
}

impl From<VoiceNoSpeechReason> for VoiceTranscriptionNoSpeechReason {
    fn from(reason: VoiceNoSpeechReason) -> Self {
        match reason {
            VoiceNoSpeechReason::TooShort => Self::TooShort,
            VoiceNoSpeechReason::NoSpeechDetected => Self::NoSpeechDetected,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VoiceTranscript {
    pub(crate) text: String,
    pub(crate) diagnostics: VoiceTranscriptionDiagnostics,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VoiceTranscriptionDiagnostics {
    pub(crate) sample_rate_hz: u32,
    pub(crate) total_samples: usize,
    pub(crate) speech_samples: usize,
    pub(crate) segment_count: usize,
    pub(crate) strategy: VoiceTranscriptionStrategy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceTranscriptionStrategy {
    BufferedGatewaySession,
    ExperimentalStreaming,
}

impl From<GatewayVoiceTranscriptionStrategy> for VoiceTranscriptionStrategy {
    fn from(strategy: GatewayVoiceTranscriptionStrategy) -> Self {
        match strategy {
            GatewayVoiceTranscriptionStrategy::BufferedGatewaySession => {
                Self::BufferedGatewaySession
            }
            GatewayVoiceTranscriptionStrategy::ExperimentalStreaming => Self::ExperimentalStreaming,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceTranscriptionErrorKind {
    ModelUnavailable,
    EngineNotImplemented,
    UnsupportedAudioFormat,
    RuntimeFailure,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VoiceTranscriptionError {
    pub(crate) kind: VoiceTranscriptionErrorKind,
    pub(crate) message: String,
}

impl VoiceTranscriptionError {
    pub(crate) fn into_voice_error(self) -> VoiceError {
        let (kind, public_code) = match self.kind {
            VoiceTranscriptionErrorKind::ModelUnavailable
            | VoiceTranscriptionErrorKind::EngineNotImplemented => (
                VoiceErrorKind::ModelUnavailable,
                pioneer_protocol::PublicErrorCode::Unavailable,
            ),
            VoiceTranscriptionErrorKind::UnsupportedAudioFormat => (
                VoiceErrorKind::TranscriptionFailed,
                pioneer_protocol::PublicErrorCode::InvalidInput,
            ),
            VoiceTranscriptionErrorKind::RuntimeFailure => (
                VoiceErrorKind::TranscriptionFailed,
                pioneer_protocol::PublicErrorCode::Internal,
            ),
        };
        let public_error = crate::public_error::map_agent_failure(
            public_code,
            pioneer_protocol::PublicErrorStage::Execution,
            self.message,
        );
        VoiceError {
            kind,
            message: public_error.message.clone(),
            public_error: Some(public_error),
        }
    }
}

impl std::fmt::Display for VoiceTranscriptionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message.as_str())
    }
}

impl std::error::Error for VoiceTranscriptionError {}

pub(crate) trait VoiceSpeechTranscriber: Send {
    fn transcribe_speech(
        &self,
        buffer: &PreparedSpeechBuffer,
    ) -> Result<String, VoiceTranscriptionError>;
}

impl VoiceSpeechTranscriber for Box<dyn VoiceSpeechTranscriber> {
    fn transcribe_speech(
        &self,
        buffer: &PreparedSpeechBuffer,
    ) -> Result<String, VoiceTranscriptionError> {
        self.as_ref().transcribe_speech(buffer)
    }
}

#[cfg(test)]
pub(crate) struct GatewayVoiceTranscriptionRuntime<T> {
    transcriber: Arc<Mutex<T>>,
    strategy: VoiceTranscriptionStrategy,
}

#[cfg(test)]
impl<T> Clone for GatewayVoiceTranscriptionRuntime<T> {
    fn clone(&self) -> Self {
        Self {
            transcriber: Arc::clone(&self.transcriber),
            strategy: self.strategy,
        }
    }
}

#[cfg(test)]
impl<T> GatewayVoiceTranscriptionRuntime<T>
where
    T: VoiceSpeechTranscriber + 'static,
{
    pub(crate) fn new(transcriber: T) -> Self {
        Self::new_with_strategy(
            transcriber,
            VoiceTranscriptionStrategy::BufferedGatewaySession,
        )
    }

    pub(crate) fn new_with_strategy(transcriber: T, strategy: VoiceTranscriptionStrategy) -> Self {
        Self {
            transcriber: Arc::new(Mutex::new(transcriber)),
            strategy,
        }
    }

    pub(crate) fn transcribe(
        &self,
        buffer: PreparedSpeechBuffer,
    ) -> Result<Result<VoiceTranscript, VoiceTranscriptionNoSpeech>, VoiceTranscriptionError> {
        let transcriber = match self.transcriber.lock() {
            Ok(transcriber) => transcriber,
            Err(poisoned) => poisoned.into_inner(),
        };
        catch_unwind(AssertUnwindSafe(|| {
            transcribe_prepared_speech_buffer_with_strategy(&*transcriber, buffer, self.strategy)
        }))
        .unwrap_or_else(|_| {
            Err(transcription_error(
                VoiceTranscriptionErrorKind::RuntimeFailure,
                "voice transcription runtime panicked and was recovered",
            ))
        })
    }
}

pub(crate) fn transcribe_prepared_speech_buffer<T>(
    transcriber: &T,
    buffer: PreparedSpeechBuffer,
) -> Result<Result<VoiceTranscript, VoiceTranscriptionNoSpeech>, VoiceTranscriptionError>
where
    T: VoiceSpeechTranscriber,
{
    transcribe_prepared_speech_buffer_with_strategy(
        transcriber,
        buffer,
        VoiceTranscriptionStrategy::BufferedGatewaySession,
    )
}

pub(crate) fn transcribe_prepared_speech_buffer_with_strategy<T>(
    transcriber: &T,
    buffer: PreparedSpeechBuffer,
    strategy: VoiceTranscriptionStrategy,
) -> Result<Result<VoiceTranscript, VoiceTranscriptionNoSpeech>, VoiceTranscriptionError>
where
    T: VoiceSpeechTranscriber,
{
    let diagnostics = VoiceTranscriptionDiagnostics {
        sample_rate_hz: buffer.sample_rate_hz,
        total_samples: buffer.total_samples,
        speech_samples: buffer.speech_samples(),
        segment_count: buffer.segments.len(),
        strategy,
    };
    if diagnostics.speech_samples == 0 {
        return Ok(Err(VoiceTranscriptionNoSpeech {
            reason: VoiceTranscriptionNoSpeechReason::EmptyBuffer,
            total_samples: diagnostics.total_samples,
        }));
    }

    let raw_text = transcriber.transcribe_speech(&buffer)?;
    let text = raw_text.trim().to_owned();
    if text.is_empty() {
        return Ok(Err(VoiceTranscriptionNoSpeech {
            reason: VoiceTranscriptionNoSpeechReason::EmptyTranscript,
            total_samples: diagnostics.total_samples,
        }));
    }

    Ok(Ok(VoiceTranscript { text, diagnostics }))
}

pub(crate) fn transcription_error(
    kind: VoiceTranscriptionErrorKind,
    message: impl Into<String>,
) -> VoiceTranscriptionError {
    VoiceTranscriptionError {
        kind,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Clone, Debug)]
    struct StaticTranscriber {
        text: &'static str,
    }

    impl VoiceSpeechTranscriber for StaticTranscriber {
        fn transcribe_speech(
            &self,
            _buffer: &PreparedSpeechBuffer,
        ) -> Result<String, VoiceTranscriptionError> {
            Ok(self.text.to_owned())
        }
    }

    #[derive(Clone, Debug)]
    struct FailingTranscriber;

    impl VoiceSpeechTranscriber for FailingTranscriber {
        fn transcribe_speech(
            &self,
            _buffer: &PreparedSpeechBuffer,
        ) -> Result<String, VoiceTranscriptionError> {
            Err(transcription_error(
                VoiceTranscriptionErrorKind::RuntimeFailure,
                "synthetic transcription failure",
            ))
        }
    }

    #[derive(Clone, Debug)]
    struct CountingTranscriber {
        calls: Arc<AtomicUsize>,
    }

    impl VoiceSpeechTranscriber for CountingTranscriber {
        fn transcribe_speech(
            &self,
            _buffer: &PreparedSpeechBuffer,
        ) -> Result<String, VoiceTranscriptionError> {
            let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(format!("phrase {call_index}"))
        }
    }

    #[derive(Clone, Debug)]
    struct PanicOnceTranscriber {
        calls: Arc<AtomicUsize>,
    }

    impl VoiceSpeechTranscriber for PanicOnceTranscriber {
        fn transcribe_speech(
            &self,
            _buffer: &PreparedSpeechBuffer,
        ) -> Result<String, VoiceTranscriptionError> {
            let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
            if call_index == 0 {
                panic!("synthetic panic");
            }
            Ok("recovered transcript".to_owned())
        }
    }

    #[derive(Clone, Debug)]
    struct SerializingTranscriber {
        active_calls: Arc<AtomicUsize>,
        max_active_calls: Arc<AtomicUsize>,
    }

    impl VoiceSpeechTranscriber for SerializingTranscriber {
        fn transcribe_speech(
            &self,
            _buffer: &PreparedSpeechBuffer,
        ) -> Result<String, VoiceTranscriptionError> {
            let active = self.active_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active_calls.fetch_max(active, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(10));
            self.active_calls.fetch_sub(1, Ordering::SeqCst);
            Ok("serialized".to_owned())
        }
    }

    #[test]
    fn empty_segments_return_typed_no_speech() {
        let outcome = PreparedSpeechBuffer::from_segments(16_000, 0, Vec::new());

        assert_eq!(
            outcome,
            VoiceTranscriptionOutcome::NoSpeech(VoiceTranscriptionNoSpeech {
                reason: VoiceTranscriptionNoSpeechReason::EmptyBuffer,
                total_samples: 0,
            })
        );
    }

    #[test]
    fn vad_no_speech_maps_to_transcription_no_speech() {
        let outcome = PreparedSpeechBuffer::from_vad_outcome(
            16_000,
            VoiceVadSegmentationOutcome::NoSpeech {
                total_samples: 160,
                reason: VoiceNoSpeechReason::TooShort,
            },
        );

        assert_eq!(
            outcome,
            VoiceTranscriptionOutcome::NoSpeech(VoiceTranscriptionNoSpeech {
                reason: VoiceTranscriptionNoSpeechReason::TooShort,
                total_samples: 160,
            })
        );
    }

    #[test]
    fn transcribe_trims_only_outer_whitespace() {
        let VoiceTranscriptionOutcome::Ready(buffer) =
            PreparedSpeechBuffer::from_segments(16_000, 4, vec![speech_segment([0.1, 0.2])])
        else {
            panic!("expected ready buffer");
        };

        let result = transcribe_prepared_speech_buffer(
            &StaticTranscriber {
                text: "  hello   world  ",
            },
            buffer,
        )
        .expect("transcribe")
        .expect("speech");

        assert_eq!(result.text, "hello   world");
        assert_eq!(result.diagnostics.segment_count, 1);
        assert_eq!(result.diagnostics.speech_samples, 2);
        assert_eq!(
            result.diagnostics.strategy,
            VoiceTranscriptionStrategy::BufferedGatewaySession
        );
    }

    #[test]
    fn transcribe_records_configured_strategy_in_diagnostics() {
        let VoiceTranscriptionOutcome::Ready(buffer) =
            PreparedSpeechBuffer::from_segments(16_000, 4, vec![speech_segment([0.1, 0.2])])
        else {
            panic!("expected ready buffer");
        };

        let result = transcribe_prepared_speech_buffer_with_strategy(
            &StaticTranscriber { text: "hello" },
            buffer,
            VoiceTranscriptionStrategy::ExperimentalStreaming,
        )
        .expect("transcribe")
        .expect("speech");

        assert_eq!(
            result.diagnostics.strategy,
            VoiceTranscriptionStrategy::ExperimentalStreaming
        );
    }

    #[test]
    fn whitespace_transcript_returns_no_speech() {
        let VoiceTranscriptionOutcome::Ready(buffer) =
            PreparedSpeechBuffer::from_segments(16_000, 4, vec![speech_segment([0.1, 0.2])])
        else {
            panic!("expected ready buffer");
        };

        let result =
            transcribe_prepared_speech_buffer(&StaticTranscriber { text: " \n\t " }, buffer)
                .expect("transcribe")
                .expect_err("empty transcript");

        assert_eq!(
            result.reason,
            VoiceTranscriptionNoSpeechReason::EmptyTranscript
        );
    }

    #[test]
    fn transcription_errors_are_typed_and_mappable() {
        let VoiceTranscriptionOutcome::Ready(buffer) =
            PreparedSpeechBuffer::from_segments(16_000, 4, vec![speech_segment([0.1, 0.2])])
        else {
            panic!("expected ready buffer");
        };

        let error = transcribe_prepared_speech_buffer(&FailingTranscriber, buffer)
            .expect_err("runtime failure");

        assert_eq!(error.kind, VoiceTranscriptionErrorKind::RuntimeFailure);
        assert_eq!(
            error.into_voice_error().kind,
            VoiceErrorKind::TranscriptionFailed
        );
    }

    #[test]
    fn runtime_reuses_warm_transcriber_without_reload() {
        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = GatewayVoiceTranscriptionRuntime::new(CountingTranscriber {
            calls: Arc::clone(&calls),
        });

        let first = runtime
            .transcribe(ready_buffer())
            .expect("first")
            .expect("first transcript");
        let second = runtime
            .transcribe(ready_buffer())
            .expect("second")
            .expect("second transcript");

        assert_eq!(first.text, "phrase 0");
        assert_eq!(second.text, "phrase 1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn runtime_recovers_from_transcription_panic() {
        let runtime = GatewayVoiceTranscriptionRuntime::new(PanicOnceTranscriber {
            calls: Arc::new(AtomicUsize::new(0)),
        });

        let panic_error = runtime
            .transcribe(ready_buffer())
            .expect_err("panic maps to error");
        let recovered = runtime
            .transcribe(ready_buffer())
            .expect("recovered")
            .expect("recovered transcript");

        assert_eq!(
            panic_error.kind,
            VoiceTranscriptionErrorKind::RuntimeFailure
        );
        assert_eq!(recovered.text, "recovered transcript");
    }

    #[test]
    fn runtime_serializes_concurrent_transcription_work() {
        let active_calls = Arc::new(AtomicUsize::new(0));
        let max_active_calls = Arc::new(AtomicUsize::new(0));
        let runtime = GatewayVoiceTranscriptionRuntime::new(SerializingTranscriber {
            active_calls: Arc::clone(&active_calls),
            max_active_calls: Arc::clone(&max_active_calls),
        });

        let first_runtime = runtime.clone();
        let second_runtime = runtime.clone();
        let first = std::thread::spawn(move || first_runtime.transcribe(ready_buffer()));
        let second = std::thread::spawn(move || second_runtime.transcribe(ready_buffer()));

        first
            .join()
            .expect("first join")
            .expect("first result")
            .expect("first transcript");
        second
            .join()
            .expect("second join")
            .expect("second result")
            .expect("second transcript");

        assert_eq!(max_active_calls.load(Ordering::SeqCst), 1);
    }

    fn speech_segment(samples: impl IntoIterator<Item = f32>) -> VoiceSpeechSegment {
        let samples = samples.into_iter().collect::<Vec<_>>();
        VoiceSpeechSegment {
            start_sample: 0,
            end_sample: samples.len(),
            samples,
        }
    }

    fn ready_buffer() -> PreparedSpeechBuffer {
        let VoiceTranscriptionOutcome::Ready(buffer) =
            PreparedSpeechBuffer::from_segments(16_000, 4, vec![speech_segment([0.1, 0.2])])
        else {
            panic!("expected ready buffer");
        };
        buffer
    }
}
