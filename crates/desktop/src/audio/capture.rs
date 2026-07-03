use super::microphone::{
    DesktopMicrophoneFormatRequest, DesktopMicrophoneGateReport, DesktopMicrophoneGateState,
};
use crate::gateway::GatewayWsCommandSender;
use pioneer_protocol::{
    VoiceAudioEncoding, VoiceAudioFormat, VoiceSessionCancelParams, VoiceSessionFinalizeParams,
    VoiceSessionStartContext, VoiceSessionStartParams, VoiceTurnContext,
};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DesktopVoiceCaptureErrorKind {
    PermissionDenied,
    NoInputDevice,
    DeviceBusy,
    UnsupportedFormat,
    DeviceInterrupted,
    AlreadyCapturing,
    #[cfg(test)]
    NotCapturing,
    GatewaySession,
    GatewayChunk,
    GatewayFinalize,
    GatewayCancel,
    NoSpeech,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DesktopVoicePcmChunk {
    pub(crate) sequence: u64,
    pub(crate) captured_at_unix_ms: Option<u64>,
    pub(crate) duration_ms: Option<u32>,
    pub(crate) pcm_s16le_mono: Vec<u8>,
}

pub(crate) trait DesktopAudioInputStream: Send {
    fn stop(&mut self) -> Result<(), DesktopVoiceCaptureError>;
}

pub(crate) type DesktopVoiceChunkSink = Box<
    dyn FnMut(&[f32], u16, u32, Option<u64>) -> Result<(), DesktopVoiceCaptureError>
        + Send
        + 'static,
>;

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
    #[cfg(test)]
    config: DesktopVoiceCaptureConfig,
    #[cfg(test)]
    next_sequence: u64,
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
        self.start_with_chunk_sink(gate, config, Box::new(|_, _, _, _| Ok(())))
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
        self.active = Some(ActiveDesktopCapture {
            stream,
            #[cfg(test)]
            config,
            #[cfg(test)]
            next_sequence: 0,
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn push_interleaved_f32_chunk(
        &mut self,
        samples: &[f32],
        input_channels: u16,
        input_sample_rate_hz: u32,
        captured_at_unix_ms: Option<u64>,
    ) -> Result<DesktopVoicePcmChunk, DesktopVoiceCaptureError> {
        let active = self.active.as_mut().ok_or_else(|| {
            DesktopVoiceCaptureError::new(
                DesktopVoiceCaptureErrorKind::NotCapturing,
                t!("chat.composer.voice.capture_not_active").to_string(),
            )
        })?;
        let sequence = active.next_sequence;
        active.next_sequence = active.next_sequence.saturating_add(1);
        desktop_voice_chunk_from_samples(
            samples,
            input_channels,
            input_sample_rate_hz,
            captured_at_unix_ms,
            sequence,
            active.config,
        )
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
    ) -> Result<(), DesktopVoiceCaptureError>;

    fn cancel_voice_session(
        &self,
        session_id: String,
        reason: Option<String>,
    ) -> Result<(), DesktopVoiceCaptureError>;
}

impl DesktopVoiceGateway for GatewayWsCommandSender {
    fn start_voice_session(
        &self,
        context: VoiceSessionStartContext,
        audio_format: VoiceAudioFormat,
    ) -> Result<String, DesktopVoiceCaptureError> {
        self.voice_session_start(VoiceSessionStartParams {
            context,
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
        self.send_voice_audio_chunk(
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
        session_id: String,
        context: VoiceTurnContext,
    ) -> Result<(), DesktopVoiceCaptureError> {
        self.voice_session_finalize(VoiceSessionFinalizeParams {
            session_id,
            context,
        })
        .map(|_| ())
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

    fn cancel_voice_session(
        &self,
        session_id: String,
        reason: Option<String>,
    ) -> Result<(), DesktopVoiceCaptureError> {
        self.voice_session_cancel(VoiceSessionCancelParams { session_id, reason })
            .map(|_| ())
            .map_err(|error| {
                DesktopVoiceCaptureError::new(
                    DesktopVoiceCaptureErrorKind::GatewayCancel,
                    t!(
                        "chat.composer.voice.gateway_cancel_failed",
                        error = format!("{error:#}").as_str()
                    )
                    .to_string(),
                )
            })
    }
}

struct ActiveGatewayVoiceSession {
    session_id: String,
    #[cfg(test)]
    audio_format: VoiceAudioFormat,
}

pub(crate) struct DesktopVoiceCaptureFlow<B, G> {
    capture: DesktopVoiceCaptureController<B>,
    gateway: G,
    active_gateway_session: Option<ActiveGatewayVoiceSession>,
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

    #[cfg(test)]
    pub(crate) fn is_capturing(&self) -> bool {
        self.capture.is_capturing()
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
        let mut next_sequence = 0_u64;
        let gateway = self.gateway.clone();
        let session_id_for_chunks = session_id.clone();
        let audio_format_for_chunks = audio_format;
        let config_for_chunks = config;
        let chunk_sink: DesktopVoiceChunkSink = Box::new(
            move |samples, input_channels, input_sample_rate_hz, captured_at| {
                let sequence = next_sequence;
                next_sequence = next_sequence.saturating_add(1);
                let chunk = desktop_voice_chunk_from_samples(
                    samples,
                    input_channels,
                    input_sample_rate_hz,
                    captured_at.or_else(now_unix_ms),
                    sequence,
                    config_for_chunks,
                )?;
                gateway.send_voice_audio_chunk(
                    session_id_for_chunks.clone(),
                    chunk.sequence,
                    audio_format_for_chunks,
                    chunk.captured_at_unix_ms,
                    chunk.duration_ms,
                    chunk.pcm_s16le_mono,
                )
            },
        );

        if let Err(error) = self.capture.start_with_chunk_sink(gate, config, chunk_sink) {
            let _ = self
                .gateway
                .cancel_voice_session(session_id, Some("desktop_capture_start_failed".to_owned()));
            return Err(error);
        }
        self.active_gateway_session = Some(ActiveGatewayVoiceSession {
            session_id,
            #[cfg(test)]
            audio_format,
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn push_interleaved_f32_chunk(
        &mut self,
        samples: &[f32],
        input_channels: u16,
        captured_at_unix_ms: Option<u64>,
    ) -> Result<(), DesktopVoiceCaptureError> {
        let chunk = self.capture.push_interleaved_f32_chunk(
            samples,
            input_channels,
            self.active_gateway_session
                .as_ref()
                .map(|session| session.audio_format.sample_rate_hz)
                .unwrap_or_else(|| DesktopVoiceCaptureConfig::default().format.sample_rate_hz),
            captured_at_unix_ms.or_else(now_unix_ms),
        )?;
        let Some(session) = self.active_gateway_session.as_ref() else {
            self.capture.cancel();
            return Err(DesktopVoiceCaptureError::new(
                DesktopVoiceCaptureErrorKind::GatewaySession,
                t!("chat.composer.voice.gateway_session_not_active").to_string(),
            ));
        };

        if let Err(error) = self.gateway.send_voice_audio_chunk(
            session.session_id.clone(),
            chunk.sequence,
            session.audio_format,
            chunk.captured_at_unix_ms,
            chunk.duration_ms,
            chunk.pcm_s16le_mono,
        ) {
            let session_id = session.session_id.clone();
            self.capture.cancel();
            self.active_gateway_session = None;
            let _ = self
                .gateway
                .cancel_voice_session(session_id, Some("desktop_chunk_send_failed".to_owned()));
            return Err(error);
        }

        Ok(())
    }

    pub(crate) fn stop_recording(&mut self) -> Result<(), DesktopVoiceCaptureError> {
        self.capture.stop()
    }

    pub(crate) fn finalize_send(
        &mut self,
        context: VoiceTurnContext,
    ) -> Result<(), DesktopVoiceCaptureError> {
        let Some(session) = self.active_gateway_session.take() else {
            return Ok(());
        };
        self.gateway
            .finalize_voice_session(session.session_id, context)
    }

    pub(crate) fn release_cancel(&mut self) -> Result<(), DesktopVoiceCaptureError> {
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
    if config.format.sample_rate_hz == 0 || config.format.channels == 0 {
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

pub(crate) fn desktop_voice_chunk_from_samples(
    samples: &[f32],
    input_channels: u16,
    input_sample_rate_hz: u32,
    captured_at_unix_ms: Option<u64>,
    sequence: u64,
    config: DesktopVoiceCaptureConfig,
) -> Result<DesktopVoicePcmChunk, DesktopVoiceCaptureError> {
    if input_sample_rate_hz == 0 {
        return Err(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::UnsupportedFormat,
            t!("chat.composer.voice.microphone_sample_rate_unsupported").to_string(),
        ));
    }
    let mono = interleaved_f32_to_mono(samples, input_channels)?;
    let mono = resample_mono_f32(&mono, input_sample_rate_hz, config.format.sample_rate_hz)?;
    let pcm_s16le_mono = f32_mono_to_pcm_s16le(&mono);
    let sample_count = pcm_s16le_mono.len() / 2;
    let duration_ms =
        chunk_duration_ms(sample_count, config.format.sample_rate_hz).filter(|v| *v > 0);

    Ok(DesktopVoicePcmChunk {
        sequence,
        captured_at_unix_ms,
        duration_ms,
        pcm_s16le_mono,
    })
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

fn resample_mono_f32(
    samples: &[f32],
    input_sample_rate_hz: u32,
    output_sample_rate_hz: u32,
) -> Result<Vec<f32>, DesktopVoiceCaptureError> {
    if input_sample_rate_hz == 0 || output_sample_rate_hz == 0 {
        return Err(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::UnsupportedFormat,
            t!("chat.composer.voice.microphone_sample_rate_unsupported").to_string(),
        ));
    }
    if samples.is_empty() || input_sample_rate_hz == output_sample_rate_hz {
        return Ok(samples.to_vec());
    }

    let output_len = ((samples.len() as u64 * u64::from(output_sample_rate_hz))
        + u64::from(input_sample_rate_hz)
        - 1)
        / u64::from(input_sample_rate_hz);
    let output_len = usize::try_from(output_len).map_err(|_| {
        DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::UnsupportedFormat,
            t!("chat.composer.voice.microphone_resample_too_large").to_string(),
        )
    })?;
    let input_per_output = input_sample_rate_hz as f64 / output_sample_rate_hz as f64;
    let mut output = Vec::with_capacity(output_len);
    for index in 0..output_len {
        let source = index as f64 * input_per_output;
        let base = source.floor() as usize;
        let next = (base + 1).min(samples.len() - 1);
        let fraction = (source - base as f64) as f32;
        let sample = samples[base] + (samples[next] - samples[base]) * fraction;
        output.push(sample);
    }
    Ok(output)
}

fn f32_mono_to_pcm_s16le(samples: &[f32]) -> Vec<u8> {
    let mut pcm = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        let clamped = sample.clamp(-1.0, 1.0);
        let scaled = if clamped < 0.0 {
            (clamped * 32768.0).round()
        } else {
            (clamped * 32767.0).round()
        };
        let sample = scaled.clamp(i16::MIN as f32, i16::MAX as f32) as i16;
        pcm.extend_from_slice(&sample.to_le_bytes());
    }
    pcm
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

        let sink = Arc::new(Mutex::new(chunk_sink));
        let callback_error = Arc::new(Mutex::new(None));
        let stopped = Arc::new(AtomicBool::new(false));

        let stream = match sample_format {
            SampleFormat::I8 => build_stream::<i8>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                Arc::clone(&stopped),
                Arc::clone(&callback_error),
                sink,
                i8_to_f32,
            ),
            SampleFormat::I16 => build_stream::<i16>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                Arc::clone(&stopped),
                Arc::clone(&callback_error),
                sink,
                i16_to_f32,
            ),
            SampleFormat::I32 => build_stream::<i32>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                Arc::clone(&stopped),
                Arc::clone(&callback_error),
                sink,
                i32_to_f32,
            ),
            SampleFormat::U8 => build_stream::<u8>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                Arc::clone(&stopped),
                Arc::clone(&callback_error),
                sink,
                u8_to_f32,
            ),
            SampleFormat::U16 => build_stream::<u16>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                Arc::clone(&stopped),
                Arc::clone(&callback_error),
                sink,
                u16_to_f32,
            ),
            SampleFormat::U32 => build_stream::<u32>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                Arc::clone(&stopped),
                Arc::clone(&callback_error),
                sink,
                u32_to_f32,
            ),
            SampleFormat::F32 => build_stream::<f32>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                Arc::clone(&stopped),
                Arc::clone(&callback_error),
                sink,
                f32_to_f32,
            ),
            SampleFormat::F64 => build_stream::<f64>(
                &device,
                cpal_config,
                channels,
                sample_rate_hz,
                Arc::clone(&stopped),
                Arc::clone(&callback_error),
                sink,
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

        stream.play().map_err(map_cpal_error)?;

        Ok(Box::new(CpalDesktopAudioInputStream {
            stream,
            callback_error,
            stopped,
        }))
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
        stopped: Arc<AtomicBool>,
        callback_error: Arc<Mutex<Option<DesktopVoiceCaptureError>>>,
        sink: Arc<Mutex<DesktopVoiceChunkSink>>,
        convert: fn(T) -> f32,
    ) -> Result<cpal::Stream, DesktopVoiceCaptureError>
    where
        T: cpal::SizedSample + Copy + Send + 'static,
    {
        let callback_error_for_data = Arc::clone(&callback_error);
        let callback_error_for_errors = Arc::clone(&callback_error);
        let stopped_for_data = Arc::clone(&stopped);
        device
            .build_input_stream(
                config,
                move |samples: &[T], _info| {
                    if stopped_for_data.load(Ordering::Relaxed) {
                        return;
                    }
                    let samples = samples.iter().copied().map(convert).collect::<Vec<_>>();
                    let result = sink.lock().expect("chunk sink")(
                        &samples,
                        channels,
                        sample_rate_hz,
                        now_unix_ms(),
                    );
                    if let Err(error) = result {
                        *callback_error_for_data.lock().expect("callback error") = Some(error);
                    }
                },
                move |error| {
                    *callback_error_for_errors.lock().expect("callback error") =
                        Some(map_cpal_error(error));
                },
                None,
            )
            .map_err(map_cpal_error)
    }

    struct CpalDesktopAudioInputStream {
        stream: cpal::Stream,
        callback_error: Arc<Mutex<Option<DesktopVoiceCaptureError>>>,
        stopped: Arc<AtomicBool>,
    }

    impl DesktopAudioInputStream for CpalDesktopAudioInputStream {
        fn stop(&mut self) -> Result<(), DesktopVoiceCaptureError> {
            self.stopped.store(true, Ordering::Relaxed);
            self.stream.pause().map_err(map_cpal_error)?;

            if let Ok(mut error) = self.callback_error.lock()
                && let Some(error) = error.take()
            {
                return Err(error);
            }
            Ok(())
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

    #[derive(Default)]
    struct FakeStream {
        stops: Arc<Mutex<usize>>,
    }

    impl DesktopAudioInputStream for FakeStream {
        fn stop(&mut self) -> Result<(), DesktopVoiceCaptureError> {
            *self.stops.lock().expect("stops") += 1;
            Ok(())
        }
    }

    struct FakeBackend {
        opens: usize,
        stops: Arc<Mutex<usize>>,
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
            mut chunk_sink: DesktopVoiceChunkSink,
        ) -> Result<Box<dyn DesktopAudioInputStream>, DesktopVoiceCaptureError> {
            self.opens += 1;
            if let Some(error) = self.open_error.clone() {
                return Err(error);
            }
            for samples in self.emit_on_open.drain(..) {
                chunk_sink(
                    &samples,
                    self.emitted_channels,
                    self.emitted_sample_rate_hz,
                    Some(777),
                )?;
            }
            Ok(Box::new(FakeStream {
                stops: Arc::clone(&self.stops),
            }))
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum GatewayEvent {
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
        chunk_error: Arc<Mutex<Option<DesktopVoiceCaptureError>>>,
    }

    impl DesktopVoiceGateway for FakeGateway {
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
            _audio_format: VoiceAudioFormat,
            _captured_at_unix_ms: Option<u64>,
            _duration_ms: Option<u32>,
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
            Ok(())
        }

        fn finalize_voice_session(
            &self,
            session_id: String,
            context: VoiceTurnContext,
        ) -> Result<(), DesktopVoiceCaptureError> {
            self.events
                .lock()
                .expect("events")
                .push(GatewayEvent::Finalize {
                    session_id,
                    turn_id: context.turn_id,
                });
            Ok(())
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
    fn capture_controller_starts_converts_chunks_and_stops_without_gateway() {
        let backend = FakeBackend::default();
        let stops = Arc::clone(&backend.stops);
        let mut controller = DesktopVoiceCaptureController::new(backend);

        controller
            .start(
                &granted_gate(),
                DesktopVoiceCaptureConfig {
                    format: DesktopMicrophoneFormatRequest {
                        sample_rate_hz: 16_000,
                        channels: 1,
                    },
                },
            )
            .expect("start capture");
        let chunk = controller
            .push_interleaved_f32_chunk(&[0.0, 1.0, -1.0], 1, 16_000, Some(42))
            .expect("chunk");
        controller.stop().expect("stop capture");

        assert_eq!(chunk.sequence, 0);
        assert_eq!(chunk.captured_at_unix_ms, Some(42));
        assert_eq!(chunk.duration_ms, None);
        assert_eq!(chunk.pcm_s16le_mono.len(), 6);
        assert_eq!(*stops.lock().expect("stops"), 1);
    }

    #[test]
    fn capture_controller_resamples_native_input_rate_to_gateway_rate() {
        let mut controller = DesktopVoiceCaptureController::new(FakeBackend::default());

        controller
            .start(&granted_gate(), DesktopVoiceCaptureConfig::default())
            .expect("start capture");
        let chunk = controller
            .push_interleaved_f32_chunk(&[0.5; 480], 1, 48_000, Some(42))
            .expect("chunk");

        assert_eq!(chunk.sequence, 0);
        assert_eq!(chunk.duration_ms, Some(10));
        assert_eq!(chunk.pcm_s16le_mono.len(), 320);
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
    fn voice_flow_streams_chunks_while_held_then_finalizes() {
        let gateway = FakeGateway::default();
        let events = Arc::clone(&gateway.events);
        let mut flow = DesktopVoiceCaptureFlow::new(FakeBackend::default(), gateway);

        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .expect("start");
        flow.push_interleaved_f32_chunk(&[0.0; 320], 1, Some(100))
            .expect("chunk 0");
        flow.push_interleaved_f32_chunk(&[0.1; 320], 1, Some(120))
            .expect("chunk 1");
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
    fn voice_flow_cancel_sends_control_cancel_without_finalize() {
        let gateway = FakeGateway::default();
        let events = Arc::clone(&gateway.events);
        let mut flow = DesktopVoiceCaptureFlow::new(FakeBackend::default(), gateway);

        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .expect("start");
        flow.release_cancel().expect("cancel");

        assert_eq!(
            *events.lock().expect("events"),
            vec![
                GatewayEvent::Start {
                    turn_id: "turn_1".to_owned(),
                },
                GatewayEvent::Cancel {
                    session_id: "voice_session_1".to_owned(),
                    reason: Some("desktop_release_cancel".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn chunk_send_failure_stops_capture_and_cancels_gateway_session() {
        let gateway = FakeGateway::default();
        *gateway.chunk_error.lock().expect("chunk error") = Some(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::GatewayChunk,
            "chunk failed",
        ));
        let events = Arc::clone(&gateway.events);
        let mut flow = DesktopVoiceCaptureFlow::new(FakeBackend::default(), gateway);

        flow.start(
            &granted_gate(),
            DesktopVoiceCaptureConfig::default(),
            voice_start_context(),
        )
        .expect("start");
        let error = flow
            .push_interleaved_f32_chunk(&[0.0; 320], 1, Some(100))
            .expect_err("chunk failure");

        assert_eq!(error.kind, DesktopVoiceCaptureErrorKind::GatewayChunk);
        assert!(!flow.is_capturing());
        assert_eq!(
            *events.lock().expect("events"),
            vec![
                GatewayEvent::Start {
                    turn_id: "turn_1".to_owned(),
                },
                GatewayEvent::Cancel {
                    session_id: "voice_session_1".to_owned(),
                    reason: Some("desktop_chunk_send_failed".to_owned()),
                },
            ]
        );
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
}
