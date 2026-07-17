use super::model_catalog::{VoiceModelCatalogEntry, VoiceModelInstallLayout};
use super::model_install::is_voice_model_installed_and_verified;
use super::transcription::{
    PreparedSpeechBuffer, VoiceTranscriptionError, VoiceTranscriptionErrorKind, transcription_error,
};
use pioneer_protocol::VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ;
use pioneer_provider::providers::LocalTranscriptionEngine;
use std::panic::{AssertUnwindSafe, catch_unwind};
use transcribe_rs::onnx::Quantization;
use transcribe_rs::onnx::canary::{CanaryModel, CanaryParams};
use transcribe_rs::onnx::cohere::{CohereModel, CohereParams};
use transcribe_rs::onnx::gigaam::{GigaAMModel, GigaAMParams};
use transcribe_rs::onnx::moonshine::{
    MoonshineModel, MoonshineParams, MoonshineStreamingParams, MoonshineVariant, StreamingModel,
};
use transcribe_rs::onnx::parakeet::{ParakeetModel, ParakeetParams, TimestampGranularity};
use transcribe_rs::onnx::sense_voice::{SenseVoiceModel, SenseVoiceParams};
use transcribe_rs::whisper_cpp::{WhisperEngine, WhisperInferenceParams, WhisperLoadParams};

#[cfg(test)]
const PREPARED_AUDIO_CHANNELS: u16 = 1;

pub(crate) enum LoadedVoiceEngine {
    Whisper(WhisperEngine),
    Parakeet(ParakeetModel),
    Moonshine(MoonshineModel),
    MoonshineStreaming(StreamingModel),
    SenseVoice(SenseVoiceModel),
    GigaAm(GigaAMModel),
    Canary(CanaryModel),
    Cohere(CohereModel),
    #[cfg(test)]
    TestStub(String),
}

impl LoadedVoiceEngine {
    pub(crate) fn load(
        entry: &VoiceModelCatalogEntry,
        layout: &VoiceModelInstallLayout,
    ) -> Result<Self, VoiceTranscriptionError> {
        if !is_voice_model_installed_and_verified(entry, layout) {
            return Err(transcription_error(
                VoiceTranscriptionErrorKind::ModelUnavailable,
                format!(
                    "local voice model `{}` is not installed and verified at {}",
                    entry.id,
                    layout.model_data_dir.display()
                ),
            ));
        }

        let Some(runtime_engine) = runtime_engine_for_model_id(entry.id) else {
            return Err(engine_not_implemented(entry.engine, entry.id));
        };
        if runtime_engine != entry.engine {
            return Err(transcription_error(
                VoiceTranscriptionErrorKind::EngineNotImplemented,
                format!(
                    "local voice model `{}` is cataloged as `{}` but routes to `{}`",
                    entry.id,
                    engine_id(entry.engine),
                    engine_id(runtime_engine)
                ),
            ));
        }

        match runtime_engine {
            LocalTranscriptionEngine::Whisper => load_whisper(entry, layout),
            LocalTranscriptionEngine::Parakeet => load_parakeet(entry, layout),
            LocalTranscriptionEngine::Moonshine => load_moonshine_base(entry, layout),
            LocalTranscriptionEngine::MoonshineStreaming => load_moonshine_streaming(entry, layout),
            LocalTranscriptionEngine::SenseVoice => load_sense_voice(entry, layout),
            LocalTranscriptionEngine::GigaAm => load_gigaam(entry, layout),
            LocalTranscriptionEngine::Canary => load_canary(entry, layout),
            LocalTranscriptionEngine::Cohere => load_cohere(entry, layout),
        }
    }

    pub(crate) fn transcribe(
        &mut self,
        buffer: &PreparedSpeechBuffer,
    ) -> Result<String, VoiceTranscriptionError> {
        let input = PreparedVoiceEngineInput::from_buffer(buffer)?;
        match self {
            Self::Whisper(model) => transcribe_whisper(model, &input),
            Self::Parakeet(model) => transcribe_parakeet(model, &input),
            Self::Moonshine(model) => transcribe_moonshine_base(model, &input),
            Self::MoonshineStreaming(model) => transcribe_moonshine_streaming(model, &input),
            Self::SenseVoice(model) => transcribe_sense_voice(model, &input),
            Self::GigaAm(model) => transcribe_gigaam(model, &input),
            Self::Canary(model) => transcribe_canary(model, &input),
            Self::Cohere(model) => transcribe_cohere(model, &input),
            #[cfg(test)]
            Self::TestStub(text) => Ok(text.clone()),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_stub() -> Self {
        Self::test_stub_with_transcript("test transcript")
    }

    #[cfg(test)]
    pub(crate) fn test_stub_with_transcript(text: impl Into<String>) -> Self {
        Self::TestStub(text.into())
    }
}

fn runtime_engine_for_model_id(model_id: &str) -> Option<LocalTranscriptionEngine> {
    if whisper_catalog_model(model_id).is_some() {
        Some(LocalTranscriptionEngine::Whisper)
    } else if parakeet_version(model_id).is_some() {
        Some(LocalTranscriptionEngine::Parakeet)
    } else if is_moonshine_base_model(model_id) {
        Some(LocalTranscriptionEngine::Moonshine)
    } else if moonshine_streaming_size(model_id).is_some() {
        Some(LocalTranscriptionEngine::MoonshineStreaming)
    } else if is_sense_voice_model(model_id) {
        Some(LocalTranscriptionEngine::SenseVoice)
    } else if is_gigaam_model(model_id) {
        Some(LocalTranscriptionEngine::GigaAm)
    } else if canary_catalog_model(model_id).is_some() {
        Some(LocalTranscriptionEngine::Canary)
    } else if is_cohere_model(model_id) {
        Some(LocalTranscriptionEngine::Cohere)
    } else {
        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WhisperCatalogModel {
    Small,
    Medium,
    Turbo,
    Large,
    BreezeAsr,
}

fn whisper_catalog_model(model_id: &str) -> Option<WhisperCatalogModel> {
    match model_id {
        "small" => Some(WhisperCatalogModel::Small),
        "medium" => Some(WhisperCatalogModel::Medium),
        "turbo" => Some(WhisperCatalogModel::Turbo),
        "large" => Some(WhisperCatalogModel::Large),
        "breeze-asr" => Some(WhisperCatalogModel::BreezeAsr),
        _ => None,
    }
}

fn whisper_load_params() -> WhisperLoadParams {
    WhisperLoadParams {
        use_gpu: false,
        flash_attn: false,
        gpu_device: 0,
    }
}

fn whisper_inference_params() -> WhisperInferenceParams {
    WhisperInferenceParams::default()
}

fn load_whisper(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<LoadedVoiceEngine, VoiceTranscriptionError> {
    let Some(_model) = whisper_catalog_model(entry.id) else {
        return Err(transcription_error(
            VoiceTranscriptionErrorKind::EngineNotImplemented,
            format!("unsupported Whisper model id `{}`", entry.id),
        ));
    };

    let model =
        WhisperEngine::load_with_params(layout.model_data_dir.as_path(), whisper_load_params())
            .map_err(|error| {
                transcription_error(
                    VoiceTranscriptionErrorKind::ModelUnavailable,
                    format!(
                        "failed to eagerly load local Whisper model `{}` from {}: {error}",
                        entry.id,
                        layout.model_data_dir.display()
                    ),
                )
            })?;
    Ok(LoadedVoiceEngine::Whisper(model))
}

fn transcribe_whisper(
    model: &mut WhisperEngine,
    input: &PreparedVoiceEngineInput,
) -> Result<String, VoiceTranscriptionError> {
    match catch_unwind(AssertUnwindSafe(|| {
        model.transcribe_with(input.samples.as_slice(), &whisper_inference_params())
    })) {
        Ok(Ok(result)) => Ok(result.text),
        Ok(Err(error)) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            format!("local Whisper transcription failed: {error}"),
        )),
        Err(_) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            "local Whisper transcription runtime panicked",
        )),
    }
}

fn is_cohere_model(model_id: &str) -> bool {
    model_id == "cohere-int8"
}

fn cohere_params() -> CohereParams {
    CohereParams {
        language: None,
        translate: false,
        max_new_tokens: None,
    }
}

fn load_cohere(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<LoadedVoiceEngine, VoiceTranscriptionError> {
    if !is_cohere_model(entry.id) {
        return Err(transcription_error(
            VoiceTranscriptionErrorKind::EngineNotImplemented,
            format!("unsupported Cohere model id `{}`", entry.id),
        ));
    }

    let model = CohereModel::load(layout.model_data_dir.as_path(), &Quantization::Int8).map_err(
        |error| {
            transcription_error(
                VoiceTranscriptionErrorKind::ModelUnavailable,
                format!(
                    "failed to eagerly load local Cohere model `{}` from {}: {error}",
                    entry.id,
                    layout.model_data_dir.display()
                ),
            )
        },
    )?;
    Ok(LoadedVoiceEngine::Cohere(model))
}

fn transcribe_cohere(
    model: &mut CohereModel,
    input: &PreparedVoiceEngineInput,
) -> Result<String, VoiceTranscriptionError> {
    match catch_unwind(AssertUnwindSafe(|| {
        model.transcribe_with(input.samples.as_slice(), &cohere_params())
    })) {
        Ok(Ok(result)) => Ok(result.text),
        Ok(Err(error)) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            format!("local Cohere transcription failed: {error}"),
        )),
        Err(_) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            "local Cohere transcription runtime panicked",
        )),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CanaryCatalogModel {
    Flash180M,
    V2_1B,
}

fn canary_catalog_model(model_id: &str) -> Option<CanaryCatalogModel> {
    match model_id {
        "canary-180m-flash" => Some(CanaryCatalogModel::Flash180M),
        "canary-1b-v2" => Some(CanaryCatalogModel::V2_1B),
        _ => None,
    }
}

fn canary_params() -> CanaryParams {
    CanaryParams::default()
}

fn load_canary(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<LoadedVoiceEngine, VoiceTranscriptionError> {
    let Some(_model) = canary_catalog_model(entry.id) else {
        return Err(transcription_error(
            VoiceTranscriptionErrorKind::EngineNotImplemented,
            format!("unsupported Canary model id `{}`", entry.id),
        ));
    };

    let model = CanaryModel::load(layout.model_data_dir.as_path(), &Quantization::Int8).map_err(
        |error| {
            transcription_error(
                VoiceTranscriptionErrorKind::ModelUnavailable,
                format!(
                    "failed to eagerly load local Canary model `{}` from {}: {error}",
                    entry.id,
                    layout.model_data_dir.display()
                ),
            )
        },
    )?;
    Ok(LoadedVoiceEngine::Canary(model))
}

fn transcribe_canary(
    model: &mut CanaryModel,
    input: &PreparedVoiceEngineInput,
) -> Result<String, VoiceTranscriptionError> {
    match catch_unwind(AssertUnwindSafe(|| {
        model.transcribe_with(input.samples.as_slice(), &canary_params())
    })) {
        Ok(Ok(result)) => Ok(result.text),
        Ok(Err(error)) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            format!("local Canary transcription failed: {error}"),
        )),
        Err(_) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            "local Canary transcription runtime panicked",
        )),
    }
}

fn is_gigaam_model(model_id: &str) -> bool {
    model_id == "gigaam-v3-e2e-ctc"
}

fn load_gigaam(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<LoadedVoiceEngine, VoiceTranscriptionError> {
    if !is_gigaam_model(entry.id) {
        return Err(transcription_error(
            VoiceTranscriptionErrorKind::EngineNotImplemented,
            format!("unsupported GigaAM model id `{}`", entry.id),
        ));
    }

    let model = GigaAMModel::load(layout.model_data_dir.as_path(), &Quantization::Int8).map_err(
        |error| {
            transcription_error(
                VoiceTranscriptionErrorKind::ModelUnavailable,
                format!(
                    "failed to eagerly load local GigaAM model `{}` from {}: {error}",
                    entry.id,
                    layout.model_data_dir.display()
                ),
            )
        },
    )?;
    Ok(LoadedVoiceEngine::GigaAm(model))
}

fn transcribe_gigaam(
    model: &mut GigaAMModel,
    input: &PreparedVoiceEngineInput,
) -> Result<String, VoiceTranscriptionError> {
    match catch_unwind(AssertUnwindSafe(|| {
        model.transcribe_with(input.samples.as_slice(), &GigaAMParams::default())
    })) {
        Ok(Ok(result)) => Ok(result.text),
        Ok(Err(error)) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            format!("local GigaAM transcription failed: {error}"),
        )),
        Err(_) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            "local GigaAM transcription runtime panicked",
        )),
    }
}

fn is_sense_voice_model(model_id: &str) -> bool {
    model_id == "sense-voice-int8"
}

fn sense_voice_params() -> SenseVoiceParams {
    SenseVoiceParams {
        language: None,
        use_itn: Some(true),
    }
}

fn load_sense_voice(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<LoadedVoiceEngine, VoiceTranscriptionError> {
    if !is_sense_voice_model(entry.id) {
        return Err(transcription_error(
            VoiceTranscriptionErrorKind::EngineNotImplemented,
            format!("unsupported SenseVoice model id `{}`", entry.id),
        ));
    }

    let model = SenseVoiceModel::load(layout.model_data_dir.as_path(), &Quantization::Int8)
        .map_err(|error| {
            transcription_error(
                VoiceTranscriptionErrorKind::ModelUnavailable,
                format!(
                    "failed to eagerly load local SenseVoice model `{}` from {}: {error}",
                    entry.id,
                    layout.model_data_dir.display()
                ),
            )
        })?;
    Ok(LoadedVoiceEngine::SenseVoice(model))
}

fn transcribe_sense_voice(
    model: &mut SenseVoiceModel,
    input: &PreparedVoiceEngineInput,
) -> Result<String, VoiceTranscriptionError> {
    match catch_unwind(AssertUnwindSafe(|| {
        model.transcribe_with(input.samples.as_slice(), &sense_voice_params())
    })) {
        Ok(Ok(result)) => Ok(result.text),
        Ok(Err(error)) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            format!("local SenseVoice transcription failed: {error}"),
        )),
        Err(_) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            "local SenseVoice transcription runtime panicked",
        )),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MoonshineStreamingSize {
    Tiny,
    Small,
    Medium,
}

fn moonshine_streaming_size(model_id: &str) -> Option<MoonshineStreamingSize> {
    match model_id {
        "moonshine-tiny-streaming-en" => Some(MoonshineStreamingSize::Tiny),
        "moonshine-small-streaming-en" => Some(MoonshineStreamingSize::Small),
        "moonshine-medium-streaming-en" => Some(MoonshineStreamingSize::Medium),
        _ => None,
    }
}

fn load_moonshine_streaming(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<LoadedVoiceEngine, VoiceTranscriptionError> {
    let Some(_size) = moonshine_streaming_size(entry.id) else {
        return Err(transcription_error(
            VoiceTranscriptionErrorKind::EngineNotImplemented,
            format!("unsupported Moonshine streaming model id `{}`", entry.id),
        ));
    };

    let model = StreamingModel::load(layout.model_data_dir.as_path(), 0, &Quantization::Int8)
        .map_err(|error| {
            transcription_error(
                VoiceTranscriptionErrorKind::ModelUnavailable,
                format!(
                    "failed to eagerly load local Moonshine streaming model `{}` from {}: {error}",
                    entry.id,
                    layout.model_data_dir.display()
                ),
            )
        })?;
    Ok(LoadedVoiceEngine::MoonshineStreaming(model))
}

fn transcribe_moonshine_streaming(
    model: &mut StreamingModel,
    input: &PreparedVoiceEngineInput,
) -> Result<String, VoiceTranscriptionError> {
    match catch_unwind(AssertUnwindSafe(|| {
        model.transcribe_with(
            input.samples.as_slice(),
            &MoonshineStreamingParams::default(),
        )
    })) {
        Ok(Ok(result)) => Ok(result.text),
        Ok(Err(error)) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            format!("local Moonshine streaming transcription failed: {error}"),
        )),
        Err(_) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            "local Moonshine streaming transcription runtime panicked",
        )),
    }
}

fn is_moonshine_base_model(model_id: &str) -> bool {
    model_id == "moonshine-base"
}

fn load_moonshine_base(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<LoadedVoiceEngine, VoiceTranscriptionError> {
    if !is_moonshine_base_model(entry.id) {
        return Err(transcription_error(
            VoiceTranscriptionErrorKind::EngineNotImplemented,
            format!("unsupported Moonshine Base model id `{}`", entry.id),
        ));
    }

    let model = MoonshineModel::load(
        layout.model_data_dir.as_path(),
        MoonshineVariant::Base,
        &Quantization::Int8,
    )
    .map_err(|error| {
        transcription_error(
            VoiceTranscriptionErrorKind::ModelUnavailable,
            format!(
                "failed to eagerly load local Moonshine Base model `{}` from {}: {error}",
                entry.id,
                layout.model_data_dir.display()
            ),
        )
    })?;
    Ok(LoadedVoiceEngine::Moonshine(model))
}

fn transcribe_moonshine_base(
    model: &mut MoonshineModel,
    input: &PreparedVoiceEngineInput,
) -> Result<String, VoiceTranscriptionError> {
    match catch_unwind(AssertUnwindSafe(|| {
        model.transcribe_with(input.samples.as_slice(), &MoonshineParams::default())
    })) {
        Ok(Ok(result)) => Ok(result.text),
        Ok(Err(error)) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            format!("local Moonshine Base transcription failed: {error}"),
        )),
        Err(_) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            "local Moonshine Base transcription runtime panicked",
        )),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParakeetVersion {
    V2,
    V3,
}

fn parakeet_version(model_id: &str) -> Option<ParakeetVersion> {
    match model_id {
        "parakeet-tdt-0.6b-v2" => Some(ParakeetVersion::V2),
        "parakeet-tdt-0.6b-v3" => Some(ParakeetVersion::V3),
        _ => None,
    }
}

fn load_parakeet(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<LoadedVoiceEngine, VoiceTranscriptionError> {
    let Some(_version) = parakeet_version(entry.id) else {
        return Err(transcription_error(
            VoiceTranscriptionErrorKind::EngineNotImplemented,
            format!("unsupported Parakeet model id `{}`", entry.id),
        ));
    };

    let model = ParakeetModel::load(layout.model_data_dir.as_path(), &Quantization::Int8).map_err(
        |error| {
            transcription_error(
                VoiceTranscriptionErrorKind::ModelUnavailable,
                format!(
                    "failed to eagerly load local Parakeet model `{}` from {}: {error}",
                    entry.id,
                    layout.model_data_dir.display()
                ),
            )
        },
    )?;
    Ok(LoadedVoiceEngine::Parakeet(model))
}

fn transcribe_parakeet(
    model: &mut ParakeetModel,
    input: &PreparedVoiceEngineInput,
) -> Result<String, VoiceTranscriptionError> {
    let params = ParakeetParams {
        timestamp_granularity: Some(TimestampGranularity::Segment),
        ..Default::default()
    };
    match catch_unwind(AssertUnwindSafe(|| {
        model.transcribe_with(input.samples.as_slice(), &params)
    })) {
        Ok(Ok(result)) => Ok(result.text),
        Ok(Err(error)) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            format!("local Parakeet transcription failed: {error}"),
        )),
        Err(_) => Err(transcription_error(
            VoiceTranscriptionErrorKind::RuntimeFailure,
            "local Parakeet transcription runtime panicked",
        )),
    }
}

#[derive(Debug)]
struct PreparedVoiceEngineInput {
    #[cfg(test)]
    sample_rate_hz: u32,
    #[cfg(test)]
    channels: u16,
    samples: Vec<f32>,
}

impl PreparedVoiceEngineInput {
    fn from_buffer(buffer: &PreparedSpeechBuffer) -> Result<Self, VoiceTranscriptionError> {
        if buffer.sample_rate_hz != VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ {
            return Err(transcription_error(
                VoiceTranscriptionErrorKind::UnsupportedAudioFormat,
                format!(
                    "local voice engines require {} Hz mono audio, got {} Hz",
                    VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ, buffer.sample_rate_hz
                ),
            ));
        }

        let samples = buffer.flattened_samples();
        if samples.is_empty() {
            return Err(transcription_error(
                VoiceTranscriptionErrorKind::UnsupportedAudioFormat,
                "local voice engine received an empty prepared speech buffer",
            ));
        }

        Ok(Self {
            #[cfg(test)]
            sample_rate_hz: buffer.sample_rate_hz,
            #[cfg(test)]
            channels: PREPARED_AUDIO_CHANNELS,
            samples,
        })
    }
}

fn engine_not_implemented(
    engine: LocalTranscriptionEngine,
    model_id: &str,
) -> VoiceTranscriptionError {
    transcription_error(
        VoiceTranscriptionErrorKind::EngineNotImplemented,
        format!(
            "local transcription engine `{}` is not implemented for model `{model_id}`",
            engine_id(engine)
        ),
    )
}

const fn engine_id(engine: LocalTranscriptionEngine) -> &'static str {
    match engine {
        LocalTranscriptionEngine::Whisper => "whisper",
        LocalTranscriptionEngine::Parakeet => "parakeet",
        LocalTranscriptionEngine::Moonshine => "moonshine",
        LocalTranscriptionEngine::MoonshineStreaming => "moonshine_streaming",
        LocalTranscriptionEngine::SenseVoice => "sense_voice",
        LocalTranscriptionEngine::GigaAm => "gigaam",
        LocalTranscriptionEngine::Canary => "canary",
        LocalTranscriptionEngine::Cohere => "cohere",
    }
}

#[cfg(test)]
mod tests {
    use super::super::model_catalog::{
        VoiceModelArchiveType, parakeet_v3_int8_catalog_entry, voice_model_catalog,
        voice_model_catalog_entry, voice_model_install_layout,
    };
    use super::super::vad::VoiceSpeechSegment;
    use super::*;
    use pioneer_config::AppConfig;

    #[test]
    fn voice_runtime_audio_adaptation_flattens_segments_as_target_mono_input() {
        let buffer = PreparedSpeechBuffer {
            sample_rate_hz: VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ,
            total_samples: 8,
            segments: vec![segment(&[0.1, 0.2]), segment(&[-0.3, 0.4, 0.5])],
        };

        let input = PreparedVoiceEngineInput::from_buffer(&buffer).expect("engine input");

        assert_eq!(input.sample_rate_hz, VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ);
        assert_eq!(input.channels, 1);
        assert_eq!(input.samples, vec![0.1, 0.2, -0.3, 0.4, 0.5]);
    }

    #[test]
    fn voice_runtime_audio_adaptation_rejects_non_target_rate_and_empty_buffer() {
        let wrong_rate = PreparedSpeechBuffer {
            sample_rate_hz: 48_000,
            total_samples: 1,
            segments: vec![segment(&[0.1])],
        };
        let wrong_rate_error =
            PreparedVoiceEngineInput::from_buffer(&wrong_rate).expect_err("unsupported rate");
        assert_eq!(
            wrong_rate_error.kind,
            VoiceTranscriptionErrorKind::UnsupportedAudioFormat
        );

        let empty = PreparedSpeechBuffer {
            sample_rate_hz: VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ,
            total_samples: 0,
            segments: Vec::new(),
        };
        let empty_error = PreparedVoiceEngineInput::from_buffer(&empty).expect_err("empty input");
        assert_eq!(
            empty_error.kind,
            VoiceTranscriptionErrorKind::UnsupportedAudioFormat
        );
    }

    #[test]
    fn voice_runtime_unimplemented_engine_is_typed_and_not_loaded() {
        let error = engine_not_implemented(LocalTranscriptionEngine::Moonshine, "moonshine-base");

        assert_eq!(
            error.kind,
            VoiceTranscriptionErrorKind::EngineNotImplemented
        );
        assert!(error.message.contains("moonshine-base"));
        assert_eq!(
            error.into_voice_error().kind,
            pioneer_protocol::VoiceErrorKind::ModelUnavailable
        );
    }

    #[test]
    fn voice_runtime_phase_gate_routes_all_catalog_layouts_and_engine_families() {
        let expected = [
            (
                "small",
                VoiceModelArchiveType::SingleFile,
                LocalTranscriptionEngine::Whisper,
            ),
            (
                "medium",
                VoiceModelArchiveType::SingleFile,
                LocalTranscriptionEngine::Whisper,
            ),
            (
                "turbo",
                VoiceModelArchiveType::SingleFile,
                LocalTranscriptionEngine::Whisper,
            ),
            (
                "large",
                VoiceModelArchiveType::SingleFile,
                LocalTranscriptionEngine::Whisper,
            ),
            (
                "breeze-asr",
                VoiceModelArchiveType::SingleFile,
                LocalTranscriptionEngine::Whisper,
            ),
            (
                "parakeet-tdt-0.6b-v2",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::Parakeet,
            ),
            (
                "parakeet-tdt-0.6b-v3",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::Parakeet,
            ),
            (
                "moonshine-base",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::Moonshine,
            ),
            (
                "moonshine-tiny-streaming-en",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::MoonshineStreaming,
            ),
            (
                "moonshine-small-streaming-en",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::MoonshineStreaming,
            ),
            (
                "moonshine-medium-streaming-en",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::MoonshineStreaming,
            ),
            (
                "sense-voice-int8",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::SenseVoice,
            ),
            (
                "gigaam-v3-e2e-ctc",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::GigaAm,
            ),
            (
                "canary-180m-flash",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::Canary,
            ),
            (
                "canary-1b-v2",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::Canary,
            ),
            (
                "cohere-int8",
                VoiceModelArchiveType::TarGzDirectory,
                LocalTranscriptionEngine::Cohere,
            ),
        ];
        let catalog = voice_model_catalog();
        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut seen_engine_family = [false; 8];

        assert_eq!(catalog.len(), expected.len());
        for (entry, (expected_id, expected_archive, expected_engine)) in
            catalog.iter().zip(expected)
        {
            assert_eq!(entry.id, expected_id);
            assert_eq!(entry.archive_type, expected_archive);
            assert_eq!(entry.engine, expected_engine);
            assert_eq!(runtime_engine_for_model_id(entry.id), Some(expected_engine));

            let layout = voice_model_install_layout(entry, &config, temp_dir.path())
                .expect("trusted catalog layout");
            match expected_archive {
                VoiceModelArchiveType::SingleFile => assert_eq!(
                    layout.model_data_dir,
                    layout.install_dir.join(entry.model_data_dir_name)
                ),
                VoiceModelArchiveType::TarGzDirectory => {
                    assert_eq!(layout.model_data_dir, layout.install_dir)
                }
            }
            assert!(layout.archive_path.starts_with(&layout.downloads_dir));
            assert!(layout.install_dir.starts_with(&layout.models_root));
            seen_engine_family[engine_family_index(expected_engine)] = true;
        }

        assert!(seen_engine_family.into_iter().all(|seen| seen));
        assert_eq!(runtime_engine_for_model_id("custom-whisper.bin"), None);
    }

    #[test]
    fn voice_runtime_whisper_routes_all_catalog_models_to_exact_single_file_layouts() {
        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let expected = [
            ("small", WhisperCatalogModel::Small),
            ("medium", WhisperCatalogModel::Medium),
            ("turbo", WhisperCatalogModel::Turbo),
            ("large", WhisperCatalogModel::Large),
            ("breeze-asr", WhisperCatalogModel::BreezeAsr),
        ];

        for (model_id, expected_route) in expected {
            assert_eq!(whisper_catalog_model(model_id), Some(expected_route));
            let entry = voice_model_catalog_entry(model_id).expect("Whisper catalog entry");
            let layout = voice_model_install_layout(&entry, &config, temp_dir.path())
                .expect("Whisper install layout");

            assert_eq!(entry.engine, LocalTranscriptionEngine::Whisper);
            assert_eq!(entry.archive_type, VoiceModelArchiveType::SingleFile);
            assert_eq!(
                layout.model_data_dir,
                layout.install_dir.join(entry.model_data_dir_name)
            );
        }

        assert_eq!(whisper_catalog_model("whisper-arbitrary"), None);
        assert_eq!(whisper_catalog_model("parakeet-tdt-0.6b-v3"), None);
    }

    #[test]
    fn voice_runtime_whisper_uses_portable_cpu_and_neutral_inference_defaults() {
        let load = whisper_load_params();
        assert!(!load.use_gpu);
        assert!(!load.flash_attn);
        assert_eq!(load.gpu_device, 0);

        let inference = whisper_inference_params();
        assert_eq!(inference.language, None);
        assert!(!inference.translate);
        assert!(!inference.print_special);
        assert!(!inference.print_progress);
        assert!(!inference.print_realtime);
        assert!(!inference.print_timestamps);
        assert!(inference.suppress_blank);
        assert!(inference.suppress_non_speech_tokens);
        assert_eq!(inference.initial_prompt, None);
    }

    #[test]
    fn voice_runtime_whisper_constructs_eagerly_from_the_verified_runtime_file() {
        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let entry = voice_model_catalog_entry("small").expect("Whisper catalog entry");
        let layout =
            voice_model_install_layout(&entry, &config, temp_dir.path()).expect("model layout");

        let missing_error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("missing Whisper file must not load"),
            Err(error) => error,
        };
        assert_eq!(
            missing_error.kind,
            VoiceTranscriptionErrorKind::ModelUnavailable
        );

        write_verified_single_file_fixture(&entry, &layout);
        let load_error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("invalid Whisper fixture must not create a loaded engine"),
            Err(error) => error,
        };
        assert_eq!(
            load_error.kind,
            VoiceTranscriptionErrorKind::ModelUnavailable
        );
        assert!(load_error.message.contains("eagerly load local Whisper"));
        assert!(
            load_error
                .message
                .contains(layout.model_data_dir.to_string_lossy().as_ref())
        );
    }

    #[test]
    fn voice_runtime_load_requires_verified_install_and_completed_engine_construction() {
        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let entry = parakeet_v3_int8_catalog_entry();
        let layout =
            voice_model_install_layout(&entry, &config, temp_dir.path()).expect("model layout");

        let missing_error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("missing install must not create a loaded engine"),
            Err(error) => error,
        };
        assert_eq!(
            missing_error.kind,
            VoiceTranscriptionErrorKind::ModelUnavailable
        );

        std::fs::create_dir_all(layout.install_dir.as_path()).expect("install dir");
        for file_name in [
            "encoder-model.int8.onnx",
            "decoder_joint-model.int8.onnx",
            "nemo128.onnx",
            "vocab.txt",
        ] {
            std::fs::write(layout.install_dir.join(file_name), b"fixture").expect("runtime file");
        }
        std::fs::write(
            layout.ready_marker_path.as_path(),
            serde_json::to_vec(&serde_json::json!({
                "id": entry.id,
                "version": entry.version,
                "sha256": entry.sha256,
            }))
            .expect("marker json"),
        )
        .expect("ready marker");

        let load_error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("invalid model fixture must not create a loaded value"),
            Err(error) => error,
        };
        assert_eq!(
            load_error.kind,
            VoiceTranscriptionErrorKind::ModelUnavailable
        );
        assert!(load_error.message.contains("eagerly load"));
    }

    #[test]
    fn voice_runtime_parakeet_routes_v2_and_v3_only() {
        assert_eq!(
            parakeet_version("parakeet-tdt-0.6b-v2"),
            Some(ParakeetVersion::V2)
        );
        assert_eq!(
            parakeet_version("parakeet-tdt-0.6b-v3"),
            Some(ParakeetVersion::V3)
        );
        assert_eq!(parakeet_version("parakeet-unknown"), None);
        assert_eq!(parakeet_version("moonshine-base"), None);
    }

    #[test]
    fn voice_runtime_moonshine_base_routes_and_fails_during_eager_construction() {
        assert!(is_moonshine_base_model("moonshine-base"));
        assert!(!is_moonshine_base_model("moonshine-tiny-streaming-en"));

        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let entry = voice_model_catalog_entry("moonshine-base").expect("catalog entry");
        let layout =
            voice_model_install_layout(&entry, &config, temp_dir.path()).expect("model layout");
        write_verified_fixture(
            &entry,
            &layout,
            &[
                "encoder_model.int8.onnx",
                "decoder_model_merged.int8.onnx",
                "tokenizer.json",
            ],
        );

        let error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("invalid Moonshine fixture must not load"),
            Err(error) => error,
        };

        assert_eq!(error.kind, VoiceTranscriptionErrorKind::ModelUnavailable);
        assert!(error.message.contains("eagerly load local Moonshine Base"));
    }

    #[test]
    fn voice_runtime_moonshine_streaming_routes_all_sizes_and_loads_eagerly() {
        assert_eq!(
            moonshine_streaming_size("moonshine-tiny-streaming-en"),
            Some(MoonshineStreamingSize::Tiny)
        );
        assert_eq!(
            moonshine_streaming_size("moonshine-small-streaming-en"),
            Some(MoonshineStreamingSize::Small)
        );
        assert_eq!(
            moonshine_streaming_size("moonshine-medium-streaming-en"),
            Some(MoonshineStreamingSize::Medium)
        );
        assert_eq!(moonshine_streaming_size("moonshine-base"), None);

        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let entry =
            voice_model_catalog_entry("moonshine-tiny-streaming-en").expect("catalog entry");
        let layout =
            voice_model_install_layout(&entry, &config, temp_dir.path()).expect("model layout");
        write_verified_fixture(
            &entry,
            &layout,
            &[
                "streaming_config.json",
                "tokenizer.bin",
                "frontend.int8.onnx",
                "encoder.int8.onnx",
                "adapter.int8.onnx",
                "cross_kv.int8.onnx",
                "decoder_kv.int8.onnx",
            ],
        );

        let error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("invalid streaming fixture must not load"),
            Err(error) => error,
        };

        assert_eq!(error.kind, VoiceTranscriptionErrorKind::ModelUnavailable);
        assert!(
            error
                .message
                .contains("eagerly load local Moonshine streaming")
        );
    }

    #[test]
    fn voice_runtime_sensevoice_uses_neutral_defaults_and_loads_eagerly() {
        assert!(is_sense_voice_model("sense-voice-int8"));
        assert!(!is_sense_voice_model("cohere-int8"));
        let params = sense_voice_params();
        assert_eq!(params.language, None);
        assert_eq!(params.use_itn, Some(true));

        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let entry = voice_model_catalog_entry("sense-voice-int8").expect("catalog entry");
        let layout =
            voice_model_install_layout(&entry, &config, temp_dir.path()).expect("model layout");
        write_verified_fixture(&entry, &layout, &["model.int8.onnx", "tokens.txt"]);

        let error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("invalid SenseVoice fixture must not load"),
            Err(error) => error,
        };

        assert_eq!(error.kind, VoiceTranscriptionErrorKind::ModelUnavailable);
        assert!(error.message.contains("eagerly load local SenseVoice"));
    }

    #[test]
    fn voice_runtime_gigaam_routes_validated_directory_and_loads_eagerly() {
        assert!(is_gigaam_model("gigaam-v3-e2e-ctc"));
        assert!(!is_gigaam_model("sense-voice-int8"));

        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let entry = voice_model_catalog_entry("gigaam-v3-e2e-ctc").expect("catalog entry");
        let layout =
            voice_model_install_layout(&entry, &config, temp_dir.path()).expect("model layout");
        write_verified_fixture(&entry, &layout, &["model.int8.onnx", "vocab.txt"]);

        let error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("invalid GigaAM fixture must not load"),
            Err(error) => error,
        };

        assert_eq!(error.kind, VoiceTranscriptionErrorKind::ModelUnavailable);
        assert!(error.message.contains("eagerly load local GigaAM"));
    }

    #[test]
    fn voice_runtime_canary_routes_both_models_without_external_language_state() {
        assert_eq!(
            canary_catalog_model("canary-180m-flash"),
            Some(CanaryCatalogModel::Flash180M)
        );
        assert_eq!(
            canary_catalog_model("canary-1b-v2"),
            Some(CanaryCatalogModel::V2_1B)
        );
        assert_eq!(canary_catalog_model("cohere-int8"), None);
        let params = canary_params();
        assert_eq!(params.language, None);
        assert_eq!(params.target_language, None);

        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let entry = voice_model_catalog_entry("canary-180m-flash").expect("catalog entry");
        let layout =
            voice_model_install_layout(&entry, &config, temp_dir.path()).expect("model layout");
        write_verified_fixture(
            &entry,
            &layout,
            &[
                "encoder-model.int8.onnx",
                "decoder-model.int8.onnx",
                "nemo128.onnx",
                "vocab.txt",
            ],
        );

        let error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("invalid Canary fixture must not load"),
            Err(error) => error,
        };

        assert_eq!(error.kind, VoiceTranscriptionErrorKind::ModelUnavailable);
        assert!(error.message.contains("eagerly load local Canary"));
    }

    #[test]
    fn voice_runtime_cohere_is_explicit_neutral_and_eagerly_owned() {
        assert!(is_cohere_model("cohere-int8"));
        assert!(!is_cohere_model("canary-1b-v2"));
        let params = cohere_params();
        assert_eq!(params.language, None);
        assert!(!params.translate);
        assert_eq!(params.max_new_tokens, None);

        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let entry = voice_model_catalog_entry("cohere-int8").expect("catalog entry");
        let layout =
            voice_model_install_layout(&entry, &config, temp_dir.path()).expect("model layout");

        let missing_error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("missing Cohere layout must not load"),
            Err(error) => error,
        };
        assert_eq!(
            missing_error.kind,
            VoiceTranscriptionErrorKind::ModelUnavailable
        );

        write_verified_fixture(
            &entry,
            &layout,
            &[
                "cohere-encoder.int8.onnx",
                "cohere-decoder.int8.onnx",
                "tokens.txt",
            ],
        );
        let load_error = match LoadedVoiceEngine::load(&entry, &layout) {
            Ok(_) => panic!("invalid Cohere fixture must not load"),
            Err(error) => error,
        };
        assert_eq!(
            load_error.kind,
            VoiceTranscriptionErrorKind::ModelUnavailable
        );
        assert!(load_error.message.contains("eagerly load local Cohere"));
    }

    fn segment(samples: &[f32]) -> VoiceSpeechSegment {
        VoiceSpeechSegment {
            start_sample: 0,
            end_sample: samples.len(),
            samples: samples.to_vec(),
        }
    }

    const fn engine_family_index(engine: LocalTranscriptionEngine) -> usize {
        match engine {
            LocalTranscriptionEngine::Whisper => 0,
            LocalTranscriptionEngine::Parakeet => 1,
            LocalTranscriptionEngine::Moonshine => 2,
            LocalTranscriptionEngine::MoonshineStreaming => 3,
            LocalTranscriptionEngine::SenseVoice => 4,
            LocalTranscriptionEngine::GigaAm => 5,
            LocalTranscriptionEngine::Canary => 6,
            LocalTranscriptionEngine::Cohere => 7,
        }
    }

    fn write_verified_fixture(
        entry: &VoiceModelCatalogEntry,
        layout: &VoiceModelInstallLayout,
        runtime_files: &[&str],
    ) {
        std::fs::create_dir_all(layout.model_data_dir.as_path()).expect("model dir");
        for file_name in runtime_files {
            std::fs::write(layout.model_data_dir.join(file_name), b"fixture")
                .expect("runtime file");
        }
        std::fs::write(
            layout.ready_marker_path.as_path(),
            serde_json::to_vec(&serde_json::json!({
                "id": entry.id,
                "version": entry.version,
                "sha256": entry.sha256,
            }))
            .expect("marker json"),
        )
        .expect("ready marker");
    }

    fn write_verified_single_file_fixture(
        entry: &VoiceModelCatalogEntry,
        layout: &VoiceModelInstallLayout,
    ) {
        std::fs::create_dir_all(layout.install_dir.as_path()).expect("model install dir");
        std::fs::write(layout.model_data_dir.as_path(), b"invalid Whisper fixture")
            .expect("runtime file");
        std::fs::write(
            layout.ready_marker_path.as_path(),
            serde_json::to_vec(&serde_json::json!({
                "id": entry.id,
                "version": entry.version,
                "sha256": entry.sha256,
            }))
            .expect("marker json"),
        )
        .expect("ready marker");
    }
}
