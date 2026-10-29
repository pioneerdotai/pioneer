use super::microphone::{
    DesktopMicrophoneFormatRequest, DesktopMicrophoneGateReport, DesktopMicrophoneGateState,
};
use pioneer_client::composer::voice::{ComposerVoiceFinalizeRequest, ComposerVoiceStartRequest};
use pioneer_client::voice::resampling::VoiceResampler;
use pioneer_protocol::{
    VoiceAudioEncoding, VoiceAudioFormat, VoiceSessionFinalizeResponse, VoiceSessionStartContext,
    VoiceTurnContext,
};
use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DesktopVoiceCaptureErrorKind {
    PermissionDenied,
    NoInputDevice,
    DeviceBusy,
    UnsupportedFormat,
    DeviceInterrupted,
    AlreadyCapturing,
    GatewaySession,
    GatewayChunk,
    GatewayFinalize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DesktopVoiceCaptureError {
    pub(crate) kind: DesktopVoiceCaptureErrorKind,
    pub(crate) message: String,
}

impl DesktopVoiceCaptureError {
    pub(crate) fn new(kind: DesktopVoiceCaptureErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn composer_message(&self) -> &str {
        self.message.as_str()
    }
}

impl std::fmt::Display for DesktopVoiceCaptureError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message.as_str())
    }
}

impl std::error::Error for DesktopVoiceCaptureError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DesktopVoiceCaptureConfig {
    pub(crate) format: DesktopMicrophoneFormatRequest,
}

impl DesktopVoiceCaptureConfig {
    pub(crate) fn gateway_audio_format(self) -> VoiceAudioFormat {
        VoiceAudioFormat {
            sample_rate_hz: self.format.sample_rate_hz,
            channels: self.format.channels,
            encoding: VoiceAudioEncoding::PcmS16Le,
        }
    }
}

impl Default for DesktopVoiceCaptureConfig {
    fn default() -> Self {
        Self {
            format: DesktopMicrophoneFormatRequest::default(),
        }
    }
}

/// `stop` must fence callbacks and wait for any callback already using the sink.
/// A platform pause alone is not a callback barrier.
pub(crate) trait DesktopAudioInputStream: Send {
    fn stop(&mut self) -> Result<(), DesktopVoiceCaptureError>;
}

pub(crate) enum DesktopVoiceInput<'a> {
    Samples {
        samples: &'a [f32],
        channels: u16,
        sample_rate_hz: u32,
        captured_at_unix_ms: Option<u64>,
    },
    /// A route discontinuity can retain the same reported sample format.
    Discontinuity,
}

pub(crate) type DesktopVoiceChunkSink =
    Box<dyn FnMut(DesktopVoiceInput<'_>) -> Result<(), DesktopVoiceCaptureError> + Send + 'static>;

pub(crate) trait DesktopAudioInputBackend {
    fn open_input_stream(
        &mut self,
        config: DesktopVoiceCaptureConfig,
        chunk_sink: DesktopVoiceChunkSink,
    ) -> Result<Box<dyn DesktopAudioInputStream>, DesktopVoiceCaptureError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PlatformDesktopAudioInputBackend;

impl DesktopAudioInputBackend for PlatformDesktopAudioInputBackend {
    fn open_input_stream(
        &mut self,
        config: DesktopVoiceCaptureConfig,
        chunk_sink: DesktopVoiceChunkSink,
    ) -> Result<Box<dyn DesktopAudioInputStream>, DesktopVoiceCaptureError> {
        cpal_audio::open_input_stream(config, chunk_sink)
    }
}

struct ActiveDesktopCapture {
    stream: Box<dyn DesktopAudioInputStream>,
}

pub(crate) struct DesktopVoiceCaptureController<B> {
    backend: B,
    active: Option<ActiveDesktopCapture>,
}

impl<B> DesktopVoiceCaptureController<B>
where
    B: DesktopAudioInputBackend,
{
    pub(crate) fn new(backend: B) -> Self {
        Self {
            backend,
            active: None,
        }
    }

    pub(crate) fn is_capturing(&self) -> bool {
        self.active.is_some()
    }

    #[cfg(test)]
    pub(crate) fn start(
        &mut self,
        gate: &DesktopMicrophoneGateReport,
        config: DesktopVoiceCaptureConfig,
    ) -> Result<(), DesktopVoiceCaptureError> {
        self.start_with_chunk_sink(gate, config, Box::new(|_| Ok(())))
    }

    pub(crate) fn start_with_chunk_sink(
        &mut self,
        gate: &DesktopMicrophoneGateReport,
        config: DesktopVoiceCaptureConfig,
        chunk_sink: DesktopVoiceChunkSink,
    ) -> Result<(), DesktopVoiceCaptureError> {
        if self.active.is_some() {
            return Err(DesktopVoiceCaptureError::new(
                DesktopVoiceCaptureErrorKind::AlreadyCapturing,
                t!("chat.composer.voice.capture_already_active").to_string(),
            ));
        }

        ensure_microphone_gate_allows_capture(gate)?;
        validate_capture_format(config)?;
        let stream = self.backend.open_input_stream(config, chunk_sink)?;
        self.active = Some(ActiveDesktopCapture { stream });
        Ok(())
    }

    pub(crate) fn stop(&mut self) -> Result<(), DesktopVoiceCaptureError> {
        let Some(mut active) = self.active.take() else {
            return Ok(());
        };
        active.stream.stop()
    }

    pub(crate) fn cancel(&mut self) {
        let Some(mut active) = self.active.take() else {
            return;
        };
        let _ = active.stream.stop();
    }
}

pub(crate) trait DesktopVoiceGateway {
    fn start_voice_session(
        &self,
        context: VoiceSessionStartContext,
        audio_format: VoiceAudioFormat,
    ) -> Result<String, DesktopVoiceCaptureError>;

    fn send_voice_audio_chunk(
        &self,
        session_id: String,
        sequence: u64,
        audio_format: VoiceAudioFormat,
        captured_at_unix_ms: Option<u64>,
        duration_ms: Option<u32>,
        pcm_chunk: Vec<u8>,
    ) -> Result<(), DesktopVoiceCaptureError>;

    fn finalize_voice_session(
        &self,
        session_id: String,
        context: VoiceTurnContext,
    ) -> Result<VoiceSessionFinalizeResponse, DesktopVoiceCaptureError>;

    fn capture_failed(&self, _error: &DesktopVoiceCaptureError) {}

    fn cancel_voice_session(
        &self,
        session_id: String,
        reason: Option<String>,
    ) -> Result<(), DesktopVoiceCaptureError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DesktopVoiceFinalizeAck {
    pub(crate) session_id: String,
    pub(crate) response: VoiceSessionFinalizeResponse,
}

#[derive(Clone)]
pub(crate) struct DesktopComposerVoiceGateway {
    client: std::sync::Arc<pioneer_client::core::ClientCore>,
    operation: pioneer_client::composer::store::ComposerOperationIdentity,
}
impl DesktopComposerVoiceGateway {
    pub(crate) fn new(
        client: std::sync::Arc<pioneer_client::core::ClientCore>,
        operation: pioneer_client::composer::store::ComposerOperationIdentity,
    ) -> Self {
        Self { client, operation }
    }
}
impl DesktopVoiceGateway for DesktopComposerVoiceGateway {
    fn start_voice_session(
        &self,
        _context: VoiceSessionStartContext,
        audio_format: VoiceAudioFormat,
    ) -> Result<String, DesktopVoiceCaptureError> {
        self.client
            .start_composer_voice_session(ComposerVoiceStartRequest {
                operation: self.operation.clone(),
                audio_format,
            })
            .map(|response| response.session_id)
            .map_err(|error| {
                DesktopVoiceCaptureError::new(
                    DesktopVoiceCaptureErrorKind::GatewaySession,
                    t!(
                        "chat.composer.voice.gateway_start_failed",
                        error = format!("{error:#}").as_str()
                    )
                    .to_string(),
                )
            })
    }

    fn send_voice_audio_chunk(
        &self,
        session_id: String,
        sequence: u64,
        audio_format: VoiceAudioFormat,
        captured_at_unix_ms: Option<u64>,
        duration_ms: Option<u32>,
        pcm_chunk: Vec<u8>,
    ) -> Result<(), DesktopVoiceCaptureError> {
        self.client
            .send_composer_voice_audio_chunk(
                &self.operation,
                session_id,
                sequence,
                audio_format,
                captured_at_unix_ms,
                duration_ms,
                pcm_chunk,
            )
            .map_err(|error| {
                DesktopVoiceCaptureError::new(
                    DesktopVoiceCaptureErrorKind::GatewayChunk,
                    t!(
                        "chat.composer.voice.gateway_stream_failed",
                        error = format!("{error:#}").as_str()
                    )
                    .to_string(),
                )
            })
    }

    fn finalize_voice_session(
        &self,
        _session_id: String,
        _context: VoiceTurnContext,
    ) -> Result<VoiceSessionFinalizeResponse, DesktopVoiceCaptureError> {
        self.client
            .finalize_composer_voice_session(ComposerVoiceFinalizeRequest {
                operation: self.operation.clone(),
            })
            .map_err(|error| {
                DesktopVoiceCaptureError::new(
                    DesktopVoiceCaptureErrorKind::GatewayFinalize,
                    t!(
                        "chat.composer.voice.gateway_finalize_failed",
                        error = format!("{error:#}").as_str()
                    )
                    .to_string(),
                )
            })
    }

    fn capture_failed(&self, error: &DesktopVoiceCaptureError) {
        self.client.complete_composer_operation(
            self.operation.clone(),
            pioneer_client::composer::store::ComposerOperationCompletion::Failed {
                message: error.composer_message().to_owned(),
            },
        );
    }

    fn cancel_voice_session(
        &self,
        _session_id: String,
        _reason: Option<String>,
    ) -> Result<(), DesktopVoiceCaptureError> {
        // Native Drop releases capture immediately; the Client's exact-session
        // cleanup worker owns the network request after operation retirement.
        self.client.complete_composer_operation(
            self.operation.clone(),
            pioneer_client::composer::store::ComposerOperationCompletion::Cancelled,
        );
        Ok(())
    }
}

const VOICE_MAX_CHUNK_SAMPLES: usize = 1_600;

/// Operation-owned output sequence and physical-stream-owned DSP. The callback
/// and commit share this mutex; no borrowed audio buffer escapes a callback.
struct DesktopVoiceStream<G> {
    gateway: G,
    session_id: String,
    audio_format: VoiceAudioFormat,
    resampler: Option<VoiceResampler>,
    input_channels: u16,
    next_sequence: u64,
    captured_at_unix_ms: Option<u64>,
    finished: bool,
    error: Option<DesktopVoiceCaptureError>,
}

impl<G: DesktopVoiceGateway> DesktopVoiceStream<G> {
    fn push(&mut self, input: DesktopVoiceInput<'_>) -> Result<(), DesktopVoiceCaptureError> {
        if self.finished {
            return Ok(());
        }
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        let result = self.process_input(input);
        if let Err(error) = &result {
            self.error = Some(error.clone());
        }
        result
    }

    fn process_input(
        &mut self,
        input: DesktopVoiceInput<'_>,
    ) -> Result<(), DesktopVoiceCaptureError> {
        let DesktopVoiceInput::Samples {
            samples,
            channels,
            sample_rate_hz,
            captured_at_unix_ms,
        } = input
        else {
            // Preserve old real-sample tail and sequence, but never filter across
            // an OS route discontinuity, even when the format is unchanged.
            return self.finish_segment();
        };
        let mono = interleaved_f32_to_mono(samples, channels)?;
        if self.resampler.as_ref().is_some_and(|resampler| {
            resampler.input_rate_hz() != sample_rate_hz || self.input_channels != channels
        }) {
            self.finish_segment()?;
        }
        if self.resampler.is_none() {
            self.resampler = Some(VoiceResampler::new(sample_rate_hz).map_err(resampling_error)?);
            self.input_channels = channels;
        }
        if !mono.is_empty() {
            self.captured_at_unix_ms = captured_at_unix_ms.or_else(now_unix_ms);
        }
        let pcm = self
            .resampler
            .as_mut()
            .expect("physical stream DSP")
            .process(&mono)
            .map_err(resampling_error)?;
        self.send_pcm(&pcm)
    }

    fn send_pcm(&mut self, samples: &[i16]) -> Result<(), DesktopVoiceCaptureError> {
        for chunk in samples.chunks(VOICE_MAX_CHUNK_SAMPLES) {
            self.gateway.send_voice_audio_chunk(
                self.session_id.clone(),
                self.next_sequence,
                self.audio_format,
                self.captured_at_unix_ms,
                chunk_duration_ms(chunk.len(), 16_000).filter(|duration| *duration > 0),
                chunk
                    .iter()
                    .flat_map(|sample| sample.to_le_bytes())
                    .collect(),
            )?;
            self.next_sequence = self.next_sequence.saturating_add(1);
        }
        Ok(())
    }

    fn finish_segment(&mut self) -> Result<(), DesktopVoiceCaptureError> {
        let Some(mut resampler) = self.resampler.take() else {
            return Ok(());
        };
        self.send_pcm(&resampler.finish())
    }

    fn finish(&mut self) -> Result<(), DesktopVoiceCaptureError> {
        if let Some(error) = &self.error {
            self.finished = true;
            self.resampler = None;
            return Err(error.clone());
        }
        if self.finished {
            return Ok(());
        }
        // Fence callbacks permanently, including if tail transmission fails.
        self.finished = true;
        let result = self.finish_segment();
        if let Err(error) = &result {
            self.error = Some(error.clone());
        }
        result
    }

    fn discard(&mut self) {
        self.finished = true;
        self.resampler = None;
    }
}

fn resampling_error(_detail: &'static str) -> DesktopVoiceCaptureError {
    DesktopVoiceCaptureError::new(
        DesktopVoiceCaptureErrorKind::UnsupportedFormat,
        t!("chat.composer.voice.microphone_sample_rate_unsupported").to_string(),
    )
}

struct ActiveGatewayVoiceSession<G> {
    session_id: String,
    output: Arc<Mutex<DesktopVoiceStream<G>>>,
    failure_reported: bool,
}

pub(crate) struct DesktopVoiceCaptureFlow<
    B: DesktopAudioInputBackend,
    G: Clone + DesktopVoiceGateway + Send + 'static,
> {
    capture: DesktopVoiceCaptureController<B>,
    gateway: G,
    active_gateway_session: Option<ActiveGatewayVoiceSession<G>>,
}

impl<B, G> DesktopVoiceCaptureFlow<B, G>
where
    B: DesktopAudioInputBackend,
    G: Clone + DesktopVoiceGateway + Send + 'static,
{
    pub(crate) fn new(backend: B, gateway: G) -> Self {
        Self {
            capture: DesktopVoiceCaptureController::new(backend),
            gateway,
            active_gateway_session: None,
        }
    }

    pub(crate) fn start(
        &mut self,
        gate: &DesktopMicrophoneGateReport,
        config: DesktopVoiceCaptureConfig,
        context: VoiceSessionStartContext,
    ) -> Result<(), DesktopVoiceCaptureError> {
        if self.active_gateway_session.is_some() || self.capture.is_capturing() {
            return Err(DesktopVoiceCaptureError::new(
                DesktopVoiceCaptureErrorKind::AlreadyCapturing,
                t!("chat.composer.voice.capture_already_active").to_string(),
            ));
        }
        ensure_microphone_gate_allows_capture(gate)?;
        validate_capture_format(config)?;

        let audio_format = config.gateway_audio_format();
        let session_id = self
            .gateway
            .start_voice_session(context, audio_format)
            .map_err(|error| DesktopVoiceCaptureError::new(error.kind, error.message))?;
        let output = Arc::new(Mutex::new(DesktopVoiceStream {
            gateway: self.gateway.clone(),
            session_id: session_id.clone(),
            audio_format,
            resampler: None,
            input_channels: 0,
            next_sequence: 0,
            captured_at_unix_ms: None,
            finished: false,
            error: None,
        }));
        let callback_output = output.clone();
        let chunk_sink: DesktopVoiceChunkSink =
            Box::new(move |input| callback_output.lock().expect("voice output").push(input));

        if let Err(error) = self.capture.start_with_chunk_sink(gate, config, chunk_sink) {
            output.lock().expect("voice output").discard();
            self.gateway.capture_failed(&error);
            let _ = self
                .gateway
                .cancel_voice_session(session_id, Some("desktop_capture_start_failed".to_owned()));
            return Err(error);
        }
        self.active_gateway_session = Some(ActiveGatewayVoiceSession {
            session_id,
            output,
            failure_reported: false,
        });
        Ok(())
    }

    pub(crate) fn stop_recording(&mut self) -> Result<(), DesktopVoiceCaptureError> {
        // Stop fences the OS callbacks and waits for any sink invocation. Only
        // then can commit send the delayed real-sample tail exactly once.
        let stop_result = self.capture.stop();
        let (result, notify_failure) = if let Some(session) = &mut self.active_gateway_session {
            let mut output = session.output.lock().expect("voice output");
            if let Err(error) = stop_result {
                output.error.get_or_insert(error);
                output.discard();
            }
            let result = output.finish();
            let notify_failure = result.is_err() && !session.failure_reported;
            session.failure_reported |= notify_failure;
            (result, notify_failure)
        } else {
            (stop_result, false)
        };
        if notify_failure && let Err(error) = &result {
            self.gateway.capture_failed(error);
        }
        result
    }

    pub(crate) fn finalize_send(
        &mut self,
        context: VoiceTurnContext,
    ) -> Result<DesktopVoiceFinalizeAck, DesktopVoiceCaptureError> {
        // Also enforce this ordering for callers that did not explicitly stop.
        self.stop_recording()?;
        let Some(session) = self.active_gateway_session.take() else {
            return Ok(DesktopVoiceFinalizeAck {
                session_id: String::new(),
                response: VoiceSessionFinalizeResponse {
                    status: pioneer_protocol::VoiceStatus::Ready,
                },
            });
        };
        let session_id = session.session_id.clone();
        let response = match self
            .gateway
            .finalize_voice_session(session_id.clone(), context)
        {
            Ok(response) => response,
            Err(error) => {
                // Keep exact-session cleanup owned until the failed flow drops.
                // Client may reject finalization before issuing an RPC.
                self.active_gateway_session = Some(session);
                return Err(error);
            }
        };
        Ok(DesktopVoiceFinalizeAck {
            session_id,
            response,
        })
    }

    pub(crate) fn release_cancel(&mut self) -> Result<(), DesktopVoiceCaptureError> {
        // Fence the DSP before stopping: an in-flight callback may finish, but
        // a queued/stale callback cannot send or flush after cancellation.
        if let Some(session) = &self.active_gateway_session {
            session.output.lock().expect("voice output").discard();
        }
        self.capture.cancel();
        let Some(session) = self.active_gateway_session.take() else {
            return Ok(());
        };
        self.gateway.cancel_voice_session(
            session.session_id,
            Some("desktop_release_cancel".to_owned()),
        )
    }
}

impl<B: DesktopAudioInputBackend, G: Clone + DesktopVoiceGateway + Send + 'static> Drop
    for DesktopVoiceCaptureFlow<B, G>
{
    fn drop(&mut self) {
        let _ = self.release_cancel();
    }
}

pub(crate) fn ensure_microphone_gate_allows_capture(
    gate: &DesktopMicrophoneGateReport,
) -> Result<(), DesktopVoiceCaptureError> {
    if gate.can_open_gateway_voice_session() {
        return Ok(());
    }

    let message = gate
        .composer_error_message()
        .unwrap_or_else(|| t!("chat.composer.voice.microphone_not_ready").to_string());
    let kind = match gate.state {
        DesktopMicrophoneGateState::DeniedRetryable | DesktopMicrophoneGateState::DeniedBlocked => {
            DesktopVoiceCaptureErrorKind::PermissionDenied
        }
        DesktopMicrophoneGateState::NoDevice => DesktopVoiceCaptureErrorKind::NoInputDevice,
        DesktopMicrophoneGateState::DeviceBusy => DesktopVoiceCaptureErrorKind::DeviceBusy,
        DesktopMicrophoneGateState::UnsupportedFormat => {
            DesktopVoiceCaptureErrorKind::UnsupportedFormat
        }
        DesktopMicrophoneGateState::Unknown => DesktopVoiceCaptureErrorKind::PermissionDenied,
        DesktopMicrophoneGateState::Granted => unreachable!("granted gate returned false"),
    };

    Err(DesktopVoiceCaptureError::new(kind, message))
}

pub(crate) fn validate_capture_format(
    config: DesktopVoiceCaptureConfig,
) -> Result<(), DesktopVoiceCaptureError> {
    if config.format.sample_rate_hz != 16_000 || config.format.channels == 0 {
        return Err(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::UnsupportedFormat,
            t!("chat.composer.voice.microphone_unsupported_format").to_string(),
        ));
    }
    if config.format.channels != 1 {
        return Err(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::UnsupportedFormat,
            t!("chat.composer.voice.microphone_mono_required").to_string(),
        ));
    }
    Ok(())
}

fn interleaved_f32_to_mono(
    samples: &[f32],
    input_channels: u16,
) -> Result<Vec<f32>, DesktopVoiceCaptureError> {
    if input_channels == 0 {
        return Err(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::UnsupportedFormat,
            t!("chat.composer.voice.microphone_channel_count_unsupported").to_string(),
        ));
    }
    let input_channels = usize::from(input_channels);
    if samples.len() % input_channels != 0 {
        return Err(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::UnsupportedFormat,
            t!("chat.composer.voice.microphone_frame_misaligned").to_string(),
        ));
    }

    let frames = samples.len() / input_channels;
    let mut mono = Vec::with_capacity(frames);
    for frame in samples.chunks_exact(input_channels) {
        let mixed = frame.iter().copied().sum::<f32>() / input_channels as f32;
        mono.push(mixed);
    }
    Ok(mono)
}

fn chunk_duration_ms(sample_count: usize, sample_rate_hz: u32) -> Option<u32> {
    if sample_count == 0 || sample_rate_hz == 0 {
        return None;
    }
    Some(((sample_count as u64 * 1000) / u64::from(sample_rate_hz)) as u32)
}

fn now_unix_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
}

mod cpal_audio {
    use super::*;
    use cpal::{
        SampleFormat, SupportedStreamConfig,
        traits::{DeviceTrait, HostTrait, StreamTrait},
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    pub(super) fn open_input_stream(
        config: DesktopVoiceCaptureConfig,
        chunk_sink: DesktopVoiceChunkSink,
    ) -> Result<Box<dyn DesktopAudioInputStream>, DesktopVoiceCaptureError> {
        let host = cpal::default_host();
        let device = host.default_input_device().ok_or_else(|| {
            DesktopVoiceCaptureError::new(
                DesktopVoiceCaptureErrorKind::NoInputDevice,
                t!("chat.composer.voice.microphone_no_device_available").to_string(),
            )
        })?;
        let stream_config = select_input_config(&device, config)?;
        let channels = stream_config.channels();
        let sample_format = stream_config.sample_format();
        let cpal_config = stream_config.config();
        let sample_rate_hz = cpal_config.sample_rate;

        let callbacks = Arc::new(CallbackGate::new(chunk_sink));

        let stream = match sample_format {
            SampleFormat::I8 => build_stream::<i8>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                callbacks.clone(),
                i8_to_f32,
            ),
            SampleFormat::I16 => build_stream::<i16>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                callbacks.clone(),
                i16_to_f32,
            ),
            SampleFormat::I32 => build_stream::<i32>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                callbacks.clone(),
                i32_to_f32,
            ),
            SampleFormat::U8 => build_stream::<u8>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                callbacks.clone(),
                u8_to_f32,
            ),
            SampleFormat::U16 => build_stream::<u16>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                callbacks.clone(),
                u16_to_f32,
            ),
            SampleFormat::U32 => build_stream::<u32>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                callbacks.clone(),
                u32_to_f32,
            ),
            SampleFormat::F32 => build_stream::<f32>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                callbacks.clone(),
                f32_to_f32,
            ),
            SampleFormat::F64 => build_stream::<f64>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                callbacks.clone(),
                f64_to_f32,
            ),
            other => {
                let sample_format = other.to_string();
                Err(DesktopVoiceCaptureError::new(
                    DesktopVoiceCaptureErrorKind::UnsupportedFormat,
                    t!(
                        "chat.composer.voice.microphone_sample_format_unsupported",
                        format = sample_format.as_str()
                    )
                    .to_string(),
                ))
            }
        }?;

        if let Err(error) = stream.play() {
            callbacks.fence();
            let _ = callbacks.wait();
            return Err(map_cpal_error(error));
        }

        Ok(Box::new(CpalDesktopAudioInputStream { stream, callbacks }))
    }

    fn select_input_config(
        device: &cpal::Device,
        _config: DesktopVoiceCaptureConfig,
    ) -> Result<SupportedStreamConfig, DesktopVoiceCaptureError> {
        if let Ok(config) = device.default_input_config()
            && config.channels() > 0
            && config.sample_rate() > 0
        {
            return Ok(config);
        }

        let mut supported_configs = device.supported_input_configs().map_err(map_cpal_error)?;
        supported_configs
            .find(|supported| supported.channels() > 0)
            .map(|supported| supported.with_max_sample_rate())
            .ok_or_else(|| {
                DesktopVoiceCaptureError::new(
                    DesktopVoiceCaptureErrorKind::UnsupportedFormat,
                    t!("chat.composer.voice.microphone_no_input_stream").to_string(),
                )
            })
    }

    fn build_stream<T>(
        device: &cpal::Device,
        config: cpal::StreamConfig,
        channels: u16,
        sample_rate_hz: u32,
        callbacks: Arc<CallbackGate>,
        convert: fn(T) -> f32,
    ) -> Result<cpal::Stream, DesktopVoiceCaptureError>
    where
        T: cpal::SizedSample + Copy + Send + 'static,
    {
        let errors = callbacks.clone();
        device
            .build_input_stream(
                config,
                move |samples: &[T], _info| {
                    let samples = samples.iter().copied().map(convert).collect::<Vec<_>>();
                    let _ = callbacks.deliver(DesktopVoiceInput::Samples {
                        samples: &samples,
                        channels,
                        sample_rate_hz,
                        captured_at_unix_ms: now_unix_ms(),
                    });
                },
                move |error| errors.record_stream_error(error),
                None,
            )
            .map_err(map_cpal_error)
    }

    struct CallbackState {
        sink: DesktopVoiceChunkSink,
        error: Option<DesktopVoiceCaptureError>,
    }

    /// Data and error callbacks serialize through the same gate. The second
    /// stopped check, under the mutex, rejects callbacks that raced with pause.
    pub(super) struct CallbackGate {
        stopped: AtomicBool,
        state: Mutex<CallbackState>,
    }

    impl CallbackGate {
        pub(super) fn new(sink: DesktopVoiceChunkSink) -> Self {
            Self {
                stopped: AtomicBool::new(false),
                state: Mutex::new(CallbackState { sink, error: None }),
            }
        }

        pub(super) fn deliver(
            &self,
            input: DesktopVoiceInput<'_>,
        ) -> Result<(), DesktopVoiceCaptureError> {
            if self.stopped.load(Ordering::Acquire) {
                return Ok(());
            }
            let mut state = self.state.lock().expect("capture callback");
            if self.stopped.load(Ordering::Acquire) {
                return Ok(());
            }
            if let Some(error) = &state.error {
                return Err(error.clone());
            }
            let result = (state.sink)(input);
            if let Err(error) = &result {
                state.error = Some(error.clone());
            }
            result
        }

        /// Returns whether the gate was already closed. No callback may start
        /// using the sink after this fence; `wait` joins the one already inside.
        pub(super) fn fence(&self) -> bool {
            self.stopped.swap(true, Ordering::AcqRel)
        }

        pub(super) fn wait(&self) -> Result<(), DesktopVoiceCaptureError> {
            let state = self.state.lock().expect("capture callback");
            state.error.clone().map_or(Ok(()), Err)
        }

        pub(super) fn record_stream_error(&self, error: cpal::Error) {
            match error.kind() {
                // A dropped slice or device switch is a physical discontinuity,
                // but must not discard the entire recording.
                cpal::ErrorKind::Xrun | cpal::ErrorKind::DeviceChanged => {
                    let _ = self.deliver(DesktopVoiceInput::Discontinuity);
                }
                cpal::ErrorKind::RealtimeDenied => {}
                _ => {
                    let mut state = self.state.lock().expect("capture callback");
                    if !self.stopped.load(Ordering::Acquire) {
                        state.error.get_or_insert_with(|| map_cpal_error(error));
                    }
                }
            }
        }
    }

    struct CpalDesktopAudioInputStream {
        stream: cpal::Stream,
        callbacks: Arc<CallbackGate>,
    }

    impl DesktopAudioInputStream for CpalDesktopAudioInputStream {
        fn stop(&mut self) -> Result<(), DesktopVoiceCaptureError> {
            let pause = if self.callbacks.fence() {
                Ok(())
            } else {
                self.stream.pause().map_err(map_cpal_error)
            };
            // Always synchronize, including after a pause failure. Callback
            // error publication is inside this barrier, not after unlocking.
            let callbacks = self.callbacks.wait();
            callbacks.and(pause)
        }
    }

    impl Drop for CpalDesktopAudioInputStream {
        fn drop(&mut self) {
            let _ = self.stop();
        }
    }

    fn map_cpal_error(error: cpal::Error) -> DesktopVoiceCaptureError {
        let kind = match error.kind() {
            cpal::ErrorKind::DeviceBusy => DesktopVoiceCaptureErrorKind::DeviceBusy,
            cpal::ErrorKind::DeviceNotAvailable | cpal::ErrorKind::HostUnavailable => {
                DesktopVoiceCaptureErrorKind::NoInputDevice
            }
            cpal::ErrorKind::PermissionDenied => DesktopVoiceCaptureErrorKind::PermissionDenied,
            cpal::ErrorKind::UnsupportedConfig | cpal::ErrorKind::UnsupportedOperation => {
                DesktopVoiceCaptureErrorKind::UnsupportedFormat
            }
            _ => DesktopVoiceCaptureErrorKind::DeviceInterrupted,
        };
        DesktopVoiceCaptureError::new(
            kind,
            t!(
                "chat.composer.voice.microphone_capture_failed",
                error = error.to_string().as_str()
            )
            .to_string(),
        )
    }

    fn i8_to_f32(sample: i8) -> f32 {
        sample as f32 / i8::MAX as f32
    }

    fn i16_to_f32(sample: i16) -> f32 {
        sample as f32 / i16::MAX as f32
    }

    fn i32_to_f32(sample: i32) -> f32 {
        sample as f32 / i32::MAX as f32
    }

    fn u8_to_f32(sample: u8) -> f32 {
        (sample as f32 - 128.0) / 128.0
    }

    fn u16_to_f32(sample: u16) -> f32 {
        (sample as f32 - 32_768.0) / 32_768.0
    }

    fn u32_to_f32(sample: u32) -> f32 {
        ((sample as f64 - 2_147_483_648.0) / 2_147_483_648.0) as f32
    }

    fn f32_to_f32(sample: f32) -> f32 {
        sample
    }

    fn f64_to_f32(sample: f64) -> f32 {
        sample as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::ThreadMode;
    use std::sync::{Arc, Mutex};

    #[test]
    fn recoverable_cpal_stream_events_do_not_fail_capture() {
        let callbacks = cpal_audio::CallbackGate::new(Box::new(|_| Ok(())));
        for kind in [
            cpal::ErrorKind::Xrun,
            cpal::ErrorKind::DeviceChanged,
            cpal::ErrorKind::RealtimeDenied,
        ] {
            callbacks.record_stream_error(cpal::Error::new(kind));
        }
        callbacks.wait().unwrap();
    }

    #[test]
    fn terminal_cpal_stream_events_still_fail_capture() {
        let callbacks = cpal_audio::CallbackGate::new(Box::new(|_| Ok(())));
        callbacks.record_stream_error(cpal::Error::new(cpal::ErrorKind::DeviceNotAvailable));
        assert_eq!(
            callbacks.wait().unwrap_err().kind,
            DesktopVoiceCaptureErrorKind::NoInputDevice
        );
    }

    type FakeInput = Arc<Mutex<Option<Arc<cpal_audio::CallbackGate>>>>;

    fn emit(
        input: &FakeInput,
        samples: &[f32],
        channels: u16,
        sample_rate_hz: u32,
        timestamp: u64,
    ) -> Result<(), DesktopVoiceCaptureError> {
        let callbacks = input.lock().unwrap().as_ref().unwrap().clone();
        callbacks.deliver(DesktopVoiceInput::Samples {
            samples,
            channels,
            sample_rate_hz,
            captured_at_unix_ms: Some(timestamp),
        })
    }

    struct FakeStream {
        stops: Arc<Mutex<usize>>,
        callbacks: Arc<cpal_audio::CallbackGate>,
    }

    impl DesktopAudioInputStream for FakeStream {
        fn stop(&mut self) -> Result<(), DesktopVoiceCaptureError> {
            if !self.callbacks.fence() {
                *self.stops.lock().unwrap() += 1;
            }
            self.callbacks.wait()
        }
    }

    struct FakeBackend {
        opens: usize,
        stops: Arc<Mutex<usize>>,
        callbacks: FakeInput,
        open_error: Option<DesktopVoiceCaptureError>,
        emit_on_open: Vec<Vec<f32>>,
        emitted_channels: u16,
        emitted_sample_rate_hz: u32,
    }

    impl Default for FakeBackend {
        fn default() -> Self {
            Self {
                opens: 0,
                stops: Arc::new(Mutex::new(0)),
                callbacks: Arc::new(Mutex::new(None)),
                open_error: None,
                emit_on_open: Vec::new(),
                emitted_channels: 1,
                emitted_sample_rate_hz: 16_000,
            }
        }
    }

    impl DesktopAudioInputBackend for FakeBackend {
        fn open_input_stream(
            &mut self,
            _config: DesktopVoiceCaptureConfig,
            chunk_sink: DesktopVoiceChunkSink,
        ) -> Result<Box<dyn DesktopAudioInputStream>, DesktopVoiceCaptureError> {
            self.opens += 1;
            if let Some(error) = self.open_error.clone() {
                return Err(error);
            }
            let callbacks = Arc::new(cpal_audio::CallbackGate::new(chunk_sink));
            *self.callbacks.lock().unwrap() = Some(callbacks.clone());
            for samples in self.emit_on_open.drain(..) {
                callbacks.deliver(DesktopVoiceInput::Samples {
                    samples: &samples,
                    channels: self.emitted_channels,
                    sample_rate_hz: self.emitted_sample_rate_hz,
                    captured_at_unix_ms: Some(777),
                })?;
            }
            Ok(Box::new(FakeStream {
                stops: self.stops.clone(),
                callbacks,
            }))
        }
    }

    #[derive(Clone)]
    struct SentChunk {
        format: VoiceAudioFormat,
        timestamp: Option<u64>,
        duration_ms: Option<u32>,
        pcm: Vec<u8>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum GatewayEvent {
        CaptureFailed,
        Start {
            turn_id: String,
        },
        Chunk {
            sequence: u64,
            bytes: usize,
        },
        Finalize {
            session_id: String,
            turn_id: String,
        },
        Cancel {
            session_id: String,
            reason: Option<String>,
        },
    }

    #[derive(Clone, Default)]
    struct FakeGateway {
        events: Arc<Mutex<Vec<GatewayEvent>>>,
        chunks: Arc<Mutex<Vec<SentChunk>>>,
        chunk_error: Arc<Mutex<Option<DesktopVoiceCaptureError>>>,
        finalize_error: Arc<Mutex<Option<DesktopVoiceCaptureError>>>,
    }

    impl DesktopVoiceGateway for FakeGateway {
        fn capture_failed(&self, _error: &DesktopVoiceCaptureError) {
            self.events
                .lock()
                .unwrap()
                .push(GatewayEvent::CaptureFailed);
        }
        fn start_voice_session(
            &self,
            context: VoiceSessionStartContext,
            _audio_format: VoiceAudioFormat,
        ) -> Result<String, DesktopVoiceCaptureError> {
            self.events
                .lock()
                .expect("events")
                .push(GatewayEvent::Start {
                    turn_id: context.turn_id,
                });
            Ok("voice_session_1".to_owned())
        }

        fn send_voice_audio_chunk(
            &self,
            _session_id: String,
            sequence: u64,
            audio_format: VoiceAudioFormat,
            captured_at_unix_ms: Option<u64>,
            duration_ms: Option<u32>,
            pcm_chunk: Vec<u8>,
        ) -> Result<(), DesktopVoiceCaptureError> {
            if let Some(error) = self.chunk_error.lock().expect("chunk error").clone() {
                return Err(error);
            }
            self.events
                .lock()
                .expect("events")
                .push(GatewayEvent::Chunk {
                    sequence,
                    bytes: pcm_chunk.len(),
                });
            self.chunks.lock().unwrap().push(SentChunk {
                format: audio_format,
                timestamp: captured_at_unix_ms,
                duration_ms,
                pcm: pcm_chunk,
            });
            Ok(())
        }

        fn finalize_voice_session(
            &self,
            session_id: String,
            context: VoiceTurnContext,
        ) -> Result<VoiceSessionFinalizeResponse, DesktopVoiceCaptureError> {
            self.events
                .lock()
                .expect("events")
                .push(GatewayEvent::Finalize {
                    session_id,
                    turn_id: context.turn_id,
                });
            if let Some(error) = self.finalize_error.lock().expect("finalize error").clone() {
                return Err(error);
            }
            Ok(VoiceSessionFinalizeResponse {
                status: pioneer_protocol::VoiceStatus::Transcribing,
            })
        }

        fn cancel_voice_session(
            &self,
            session_id: String,
            reason: Option<String>,
        ) -> Result<(), DesktopVoiceCaptureError> {
            self.events
                .lock()
                .expect("events")
                .push(GatewayEvent::Cancel { session_id, reason });
            Ok(())
        }
    }

    fn granted_gate() -> DesktopMicrophoneGateReport {
        DesktopMicrophoneGateReport {
            state: DesktopMicrophoneGateState::Granted,
            strategy:
                super::super::microphone::DesktopMicrophonePermissionRequestStrategy::DeviceProbe,
            device_name: Some("Built-in Microphone".to_owned()),
            message: None,
        }
    }

    fn voice_context() -> VoiceTurnContext {
        VoiceTurnContext {
            workspace_id: "workspace_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            prepared_input: Vec::new(),
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: Some(ThreadMode::Chat),
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        }
    }

    fn voice_start_context() -> VoiceSessionStartContext {
        VoiceSessionStartContext {
            workspace_id: "workspace_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
        }
    }

    #[test]
    fn backend_callback_downmixes_and_preserves_wire_limits_and_timestamp() {
        let gateway = FakeGateway::default();
        let chunks = gateway.chunks.clone();
        let backend = FakeBackend::default();
        let input = backend.callbacks.clone();
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);
        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .unwrap();
        let mut samples = vec![0.0; 4_004];
        samples[..6].copy_from_slice(&[1.0, -1.0, -1.0, -1.0, 1.0, 1.0]);
        emit(&input, &samples, 2, 16_000, 42).unwrap();
        emit(&input, &[], 2, 16_000, 99).unwrap();
        flow.stop_recording().unwrap();
        let chunks = chunks.lock().unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.pcm.len())
                .collect::<Vec<_>>(),
            vec![3_200, 804]
        );
        assert_eq!(&chunks[0].pcm[..6], &[0, 0, 0, 128, 255, 127]);
        assert!(chunks.iter().all(|chunk| chunk.format
            == DesktopVoiceCaptureConfig::default().gateway_audio_format()
            && chunk.timestamp == Some(42)));
        assert_eq!(chunks[0].duration_ms, Some(100));
        assert_eq!(chunks[1].duration_ms, Some(25));
    }

    #[test]
    fn backend_callback_tail_flushes_once_before_finalize_and_stale_callback_is_fenced() {
        let gateway = FakeGateway::default();
        let events = gateway.events.clone();
        let chunks = gateway.chunks.clone();
        let backend = FakeBackend::default();
        let input = backend.callbacks.clone();
        let stops = backend.stops.clone();
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);
        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .unwrap();
        emit(&input, &[0.25; 480], 1, 48_000, 42).unwrap();
        emit(&input, &[], 1, 48_000, 77).unwrap();
        let before_stop = chunks
            .lock()
            .unwrap()
            .iter()
            .map(|chunk| chunk.pcm.len())
            .sum::<usize>();
        assert!(before_stop < 320, "delayed tail must still be pending");
        flow.stop_recording().unwrap();
        let flushed_count = chunks.lock().unwrap().len();
        flow.stop_recording().unwrap();
        emit(&input, &[0.0; 480], 1, 48_000, 99).unwrap();
        assert_eq!(chunks.lock().unwrap().len(), flushed_count);
        flow.finalize_send(voice_context()).unwrap();
        drop(flow);
        let chunks = chunks.lock().unwrap();
        assert_eq!(
            chunks.iter().map(|chunk| chunk.pcm.len()).sum::<usize>(),
            320
        );
        assert!(chunks.iter().all(|chunk| chunk.timestamp == Some(42)));
        assert_eq!(*stops.lock().unwrap(), 1);
        let events = events.lock().unwrap();
        let sequences = events
            .iter()
            .filter_map(|event| match event {
                GatewayEvent::Chunk { sequence, .. } => Some(*sequence),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(sequences, (0..flushed_count as u64).collect::<Vec<_>>());
        assert!(matches!(events.last(), Some(GatewayEvent::Finalize { .. })));
    }

    #[test]
    fn capture_controller_serializes_double_start_and_cancel() {
        let backend = FakeBackend::default();
        let stops = Arc::clone(&backend.stops);
        let mut controller = DesktopVoiceCaptureController::new(backend);

        controller
            .start(&granted_gate(), DesktopVoiceCaptureConfig::default())
            .expect("start capture");
        let error = controller
            .start(&granted_gate(), DesktopVoiceCaptureConfig::default())
            .expect_err("second start should fail");
        controller.cancel();

        assert_eq!(error.kind, DesktopVoiceCaptureErrorKind::AlreadyCapturing);
        assert!(!controller.is_capturing());
        assert_eq!(*stops.lock().expect("stops"), 1);
    }

    #[test]
    fn failed_finalize_retains_exact_session_cleanup_until_drop() {
        let gateway = FakeGateway::default();
        *gateway.finalize_error.lock().unwrap() = Some(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::GatewayFinalize,
            "synthetic rejected operation",
        ));
        let events = gateway.events.clone();
        let backend = FakeBackend::default();
        let stops = backend.stops.clone();
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);
        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .unwrap();
        flow.stop_recording().unwrap();
        assert!(flow.finalize_send(voice_context()).is_err());
        drop(flow);
        assert_eq!(*stops.lock().unwrap(), 1);
        let events = events.lock().unwrap();
        assert_eq!(events.iter().filter(|event| matches!(event, GatewayEvent::Cancel { session_id, .. } if session_id == "voice_session_1")).count(), 1);
    }

    #[test]
    fn voice_flow_streams_chunks_while_held_then_finalizes() {
        let gateway = FakeGateway::default();
        let events = Arc::clone(&gateway.events);
        let backend = FakeBackend::default();
        let input = backend.callbacks.clone();
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);

        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .expect("start");
        emit(&input, &[0.0; 320], 1, 16_000, 100).expect("chunk 0");
        emit(&input, &[0.1; 320], 1, 16_000, 120).expect("chunk 1");
        flow.stop_recording().expect("stop recording");
        flow.finalize_send(voice_context()).expect("finalize");

        assert_eq!(
            *events.lock().expect("events"),
            vec![
                GatewayEvent::Start {
                    turn_id: "turn_1".to_owned(),
                },
                GatewayEvent::Chunk {
                    sequence: 0,
                    bytes: 640,
                },
                GatewayEvent::Chunk {
                    sequence: 1,
                    bytes: 640,
                },
                GatewayEvent::Finalize {
                    session_id: "voice_session_1".to_owned(),
                    turn_id: "turn_1".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn voice_flow_streams_backend_emitted_chunks_without_waiting_for_release() {
        let gateway = FakeGateway::default();
        let events = Arc::clone(&gateway.events);
        let backend = FakeBackend {
            emit_on_open: vec![vec![0.0; 320], vec![0.25; 160]],
            ..FakeBackend::default()
        };
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);

        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .expect("start");

        assert_eq!(
            *events.lock().expect("events"),
            vec![
                GatewayEvent::Start {
                    turn_id: "turn_1".to_owned(),
                },
                GatewayEvent::Chunk {
                    sequence: 0,
                    bytes: 640,
                },
                GatewayEvent::Chunk {
                    sequence: 1,
                    bytes: 320,
                },
            ]
        );

        flow.stop_recording().expect("stop recording");
        flow.finalize_send(voice_context()).expect("finalize");
    }

    #[test]
    fn dropped_voice_flow_stops_capture_and_cancels_its_session_once() {
        let gateway = FakeGateway::default();
        let events = Arc::clone(&gateway.events);
        let backend = FakeBackend {
            emit_on_open: vec![vec![0.25; 3]],
            emitted_sample_rate_hz: 48_000,
            ..Default::default()
        };
        let input = backend.callbacks.clone();
        let stops = Arc::clone(&backend.stops);
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);
        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .unwrap();
        drop(flow);
        emit(&input, &[0.0; 480], 1, 48_000, 99).unwrap();
        assert_eq!(*stops.lock().unwrap(), 1);
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                GatewayEvent::Start {
                    turn_id: "turn_1".into()
                },
                GatewayEvent::Cancel {
                    session_id: "voice_session_1".into(),
                    reason: Some("desktop_release_cancel".into())
                },
            ]
        );
    }

    #[test]
    fn voice_flow_cancel_discards_tail_and_sends_control_cancel_once() {
        let gateway = FakeGateway::default();
        let events = gateway.events.clone();
        let backend = FakeBackend {
            emit_on_open: vec![vec![0.25; 3]],
            emitted_sample_rate_hz: 48_000,
            ..Default::default()
        };
        let input = backend.callbacks.clone();
        let stops = backend.stops.clone();
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);
        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .unwrap();
        flow.release_cancel().unwrap();
        flow.release_cancel().unwrap();
        emit(&input, &[0.0; 480], 1, 48_000, 99).unwrap();
        drop(flow);
        assert_eq!(*stops.lock().unwrap(), 1);
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                GatewayEvent::Start {
                    turn_id: "turn_1".into()
                },
                GatewayEvent::Cancel {
                    session_id: "voice_session_1".into(),
                    reason: Some("desktop_release_cancel".into())
                },
            ]
        );
    }

    #[test]
    fn flush_failure_prevents_finalize_and_releases_session_once() {
        let gateway = FakeGateway::default();
        let events = gateway.events.clone();
        let chunk_error = gateway.chunk_error.clone();
        let backend = FakeBackend {
            emit_on_open: vec![vec![0.25; 3]],
            emitted_sample_rate_hz: 48_000,
            ..Default::default()
        };
        let stops = backend.stops.clone();
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);
        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .unwrap();
        *chunk_error.lock().unwrap() = Some(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::GatewayChunk,
            "tail failed",
        ));
        assert_eq!(
            flow.stop_recording().unwrap_err().kind,
            DesktopVoiceCaptureErrorKind::GatewayChunk
        );
        assert!(flow.finalize_send(voice_context()).is_err());
        flow.release_cancel().unwrap();
        flow.release_cancel().unwrap();
        drop(flow);
        assert_eq!(*stops.lock().unwrap(), 1);
        let events = events.lock().unwrap();
        assert!(!events.iter().any(|event| matches!(
            event,
            GatewayEvent::Finalize { .. } | GatewayEvent::Chunk { .. }
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, GatewayEvent::Cancel { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, GatewayEvent::CaptureFailed))
                .count(),
            1
        );
    }

    #[test]
    fn callback_send_failure_is_sticky_at_commit_and_cleanup() {
        let gateway = FakeGateway::default();
        let events = gateway.events.clone();
        let chunk_error = gateway.chunk_error.clone();
        let backend = FakeBackend::default();
        let input = backend.callbacks.clone();
        let stops = backend.stops.clone();
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);
        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .unwrap();
        *chunk_error.lock().unwrap() = Some(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::GatewayChunk,
            "callback failed",
        ));
        assert!(emit(&input, &[0.0; 320], 1, 16_000, 100).is_err());
        *chunk_error.lock().unwrap() = None;
        assert!(emit(&input, &[0.0; 320], 1, 16_000, 120).is_err());
        assert!(flow.finalize_send(voice_context()).is_err());
        drop(flow);
        assert_eq!(*stops.lock().unwrap(), 1);
        let events = events.lock().unwrap();
        assert!(!events.iter().any(|event| matches!(
            event,
            GatewayEvent::Finalize { .. } | GatewayEvent::Chunk { .. }
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, GatewayEvent::Cancel { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn route_and_format_boundaries_finish_old_tail_without_sharing_filter_history() {
        let gateway = FakeGateway::default();
        let chunks = gateway.chunks.clone();
        let events = gateway.events.clone();
        let backend = FakeBackend::default();
        let input = backend.callbacks.clone();
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);
        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .unwrap();
        emit(&input, &[0.5; 3], 1, 48_000, 10).unwrap();
        input
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .record_stream_error(cpal::Error::new(cpal::ErrorKind::DeviceChanged));
        assert_eq!(
            chunks
                .lock()
                .unwrap()
                .iter()
                .map(|chunk| chunk.pcm.len())
                .sum::<usize>(),
            2
        );
        emit(&input, &[0.0; 3], 1, 48_000, 20).unwrap();
        // A changed callback format also terminates the physical segment.
        emit(&input, &[0.0; 16], 1, 16_000, 30).unwrap();
        flow.stop_recording().unwrap();
        let chunks = chunks.lock().unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].timestamp, Some(10));
        assert_eq!(chunks[1].timestamp, Some(20));
        assert_eq!(chunks[2].timestamp, Some(30));
        assert!(
            chunks[1..]
                .iter()
                .all(|chunk| chunk.pcm.iter().all(|byte| *byte == 0))
        );
        let sequences = events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                GatewayEvent::Chunk { sequence, .. } => Some(*sequence),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![0, 1, 2]);
    }

    #[test]
    fn stop_gate_waits_for_callback_error_and_rejects_queued_delivery() {
        use std::sync::mpsc;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let gate = Arc::new(cpal_audio::CallbackGate::new(Box::new(move |_| {
            entered_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            Err(DesktopVoiceCaptureError::new(
                DesktopVoiceCaptureErrorKind::GatewayChunk,
                "in-flight failure",
            ))
        })));
        let callback_gate = gate.clone();
        let callback =
            std::thread::spawn(move || callback_gate.deliver(DesktopVoiceInput::Discontinuity));
        entered_rx.recv().unwrap();
        assert!(!gate.fence());
        // This late callback must not enter the occupied sink or deadlock.
        gate.deliver(DesktopVoiceInput::Discontinuity).unwrap();
        let waiter_gate = gate.clone();
        let (waiting_tx, waiting_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            waiting_tx.send(()).unwrap();
            done_tx.send(waiter_gate.wait()).unwrap();
        });
        waiting_rx.recv().unwrap();
        assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        resume_tx.send(()).unwrap();
        assert_eq!(
            done_rx.recv().unwrap().unwrap_err().kind,
            DesktopVoiceCaptureErrorKind::GatewayChunk
        );
        assert!(callback.join().unwrap().is_err());
        waiter.join().unwrap();
    }

    #[test]
    fn local_gate_failure_creates_no_gateway_session() {
        let gateway = FakeGateway::default();
        let events = Arc::clone(&gateway.events);
        let mut flow = DesktopVoiceCaptureFlow::new(FakeBackend::default(), gateway);
        let gate = DesktopMicrophoneGateReport {
            state: DesktopMicrophoneGateState::NoDevice,
            strategy:
                super::super::microphone::DesktopMicrophonePermissionRequestStrategy::DeviceProbe,
            device_name: None,
            message: None,
        };

        let error = flow
            .start(
                &gate,
                DesktopVoiceCaptureConfig::default(),
                voice_start_context(),
            )
            .expect_err("gate failure");

        assert_eq!(error.kind, DesktopVoiceCaptureErrorKind::NoInputDevice);
        assert!(events.lock().expect("events").is_empty());
    }
    #[test]
    fn recorder_start_failure_is_published_before_exact_session_cleanup() {
        let gateway = FakeGateway::default();
        let events = gateway.events.clone();
        let backend = FakeBackend {
            open_error: Some(DesktopVoiceCaptureError::new(
                DesktopVoiceCaptureErrorKind::DeviceBusy,
                "synthetic device busy",
            )),
            ..Default::default()
        };
        let mut flow = DesktopVoiceCaptureFlow::new(backend, gateway);
        assert!(
            flow.start(
                &granted_gate(),
                DesktopVoiceCaptureConfig::default(),
                voice_start_context()
            )
            .is_err()
        );
        drop(flow);
        let events = events.lock().unwrap();
        assert!(
            matches!(events.as_slice(), [GatewayEvent::Start { .. }, GatewayEvent::CaptureFailed, GatewayEvent::Cancel { session_id, .. }] if session_id == "voice_session_1")
        );
    }
}
