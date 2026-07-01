use super::model_catalog::{parakeet_v3_int8_catalog_entry, parakeet_v3_int8_install_layout};
use super::model_install::is_voice_model_installed_and_verified;
use super::transcription::{
    PreparedSpeechBuffer, VoiceSpeechTranscriber, VoiceTranscriptionError,
    VoiceTranscriptionErrorKind, transcription_error,
};
use pioneer_config::AppConfig;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use transcribe_rs::onnx::Quantization;
use transcribe_rs::onnx::parakeet::{ParakeetModel, ParakeetParams, TimestampGranularity};

pub(crate) struct TranscribeRsParakeetSpeechTranscriber {
    config: AppConfig,
    runtime_home: PathBuf,
    model: Mutex<Option<ParakeetModel>>,
}

impl TranscribeRsParakeetSpeechTranscriber {
    pub(crate) fn new(config: AppConfig, runtime_home: PathBuf) -> Self {
        Self {
            config,
            runtime_home,
            model: Mutex::new(None),
        }
    }

    fn lock_model(&self) -> MutexGuard<'_, Option<ParakeetModel>> {
        self.model
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn load_model(&self) -> Result<ParakeetModel, VoiceTranscriptionError> {
        let entry = parakeet_v3_int8_catalog_entry();
        let layout = parakeet_v3_int8_install_layout(&self.config, self.runtime_home.as_path())
            .map_err(|error| {
                transcription_error(
                    VoiceTranscriptionErrorKind::ModelUnavailable,
                    format!("failed to resolve local Parakeet model layout: {error:#}"),
                )
            })?;

        if !layout
            .model_data_dir
            .starts_with(self.runtime_home.as_path())
        {
            return Err(transcription_error(
                VoiceTranscriptionErrorKind::ModelUnavailable,
                format!(
                    "local Parakeet model path {} is outside runtime home {}",
                    layout.model_data_dir.display(),
                    self.runtime_home.display()
                ),
            ));
        }

        if !is_voice_model_installed_and_verified(&entry, &layout) {
            return Err(transcription_error(
                VoiceTranscriptionErrorKind::ModelUnavailable,
                format!(
                    "local Parakeet model `{}` is not installed and verified at {}",
                    entry.id,
                    layout.model_data_dir.display()
                ),
            ));
        }

        ParakeetModel::load(layout.model_data_dir.as_path(), &Quantization::Int8).map_err(|error| {
            transcription_error(
                VoiceTranscriptionErrorKind::ModelUnavailable,
                format!(
                    "failed to load local Parakeet model `{}` from {}: {error}",
                    entry.id,
                    layout.model_data_dir.display()
                ),
            )
        })
    }
}

impl VoiceSpeechTranscriber for TranscribeRsParakeetSpeechTranscriber {
    fn transcribe_speech(
        &self,
        buffer: &PreparedSpeechBuffer,
    ) -> Result<String, VoiceTranscriptionError> {
        if buffer.sample_rate_hz != 16_000 {
            return Err(transcription_error(
                VoiceTranscriptionErrorKind::RuntimeFailure,
                format!(
                    "local Parakeet expects 16000 Hz speech samples, got {} Hz",
                    buffer.sample_rate_hz
                ),
            ));
        }

        let samples = buffer.flattened_samples();
        if samples.is_empty() {
            return Ok(String::new());
        }

        let mut model = {
            let mut model_guard = self.lock_model();
            if model_guard.is_none() {
                *model_guard = Some(self.load_model()?);
            }
            model_guard.take().ok_or_else(|| {
                transcription_error(
                    VoiceTranscriptionErrorKind::ModelUnavailable,
                    "local Parakeet model is not loaded",
                )
            })?
        };

        let params = ParakeetParams {
            timestamp_granularity: Some(TimestampGranularity::Segment),
            ..Default::default()
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            model.transcribe_with(samples.as_slice(), &params)
        }));

        match result {
            Ok(Ok(transcription)) => {
                *self.lock_model() = Some(model);
                Ok(transcription.text)
            }
            Ok(Err(error)) => {
                *self.lock_model() = Some(model);
                Err(transcription_error(
                    VoiceTranscriptionErrorKind::RuntimeFailure,
                    format!("local Parakeet transcription failed: {error}"),
                ))
            }
            Err(_) => Err(transcription_error(
                VoiceTranscriptionErrorKind::RuntimeFailure,
                "local Parakeet transcription runtime panicked and the model was unloaded",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::vad::VoiceSpeechSegment;
    use super::*;

    fn speech_buffer(sample_rate_hz: u32) -> PreparedSpeechBuffer {
        PreparedSpeechBuffer {
            sample_rate_hz,
            total_samples: 3,
            segments: vec![VoiceSpeechSegment {
                start_sample: 0,
                end_sample: 3,
                samples: vec![0.1, 0.2, 0.1],
            }],
        }
    }

    #[test]
    fn rejects_non_16khz_buffers_before_loading_model() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let transcriber = TranscribeRsParakeetSpeechTranscriber::new(
            AppConfig::load().unwrap(),
            temp_dir.path().to_path_buf(),
        );

        let error = transcriber
            .transcribe_speech(&speech_buffer(48_000))
            .expect_err("unsupported sample rate should fail before load");

        assert_eq!(error.kind, VoiceTranscriptionErrorKind::RuntimeFailure);
        assert!(error.message.contains("16000 Hz"));
    }

    #[test]
    fn missing_verified_model_is_model_unavailable() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let transcriber = TranscribeRsParakeetSpeechTranscriber::new(
            AppConfig::load().unwrap(),
            temp_dir.path().to_path_buf(),
        );

        let error = transcriber
            .transcribe_speech(&speech_buffer(16_000))
            .expect_err("missing model should fail");

        assert_eq!(error.kind, VoiceTranscriptionErrorKind::ModelUnavailable);
        assert!(error.message.contains("not installed and verified"));
    }
}
