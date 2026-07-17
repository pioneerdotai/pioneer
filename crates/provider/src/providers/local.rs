use anyhow::{Result, bail};
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use pioneer_protocol::{
    ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits,
    ProviderTranscriptionModelMetadata,
};

use crate::traits::Provider;
use crate::types::{
    ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, ProviderCapabilities,
    ProviderInputCapabilities, StreamChunk,
};

const LOCAL_PROVIDER_ID: &str = "local";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalEmbeddingModelInfo {
    pub id: &'static str,
    pub display_name: &'static str,
    pub description: &'static str,
    pub dimension: usize,
    pub max_tokens: usize,
    pub model_url: &'static str,
    pub tokenizer_url: &'static str,
    pub default: bool,
}

pub const LOCAL_EMBEDDING_MODELS: &[LocalEmbeddingModelInfo] = &[
    LocalEmbeddingModelInfo {
        id: "bge-small-en-v1.5",
        display_name: "BGE Small EN v1.5",
        description: "Local 384-dimensional embedding model for fast memory recall.",
        dimension: 384,
        max_tokens: 512,
        model_url: "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/onnx/model.onnx",
        tokenizer_url: "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/tokenizer.json",
        default: true,
    },
    LocalEmbeddingModelInfo {
        id: "bge-base-en-v1.5",
        display_name: "BGE Base EN v1.5",
        description: "Local 768-dimensional embedding model with higher recall quality.",
        dimension: 768,
        max_tokens: 512,
        model_url: "https://huggingface.co/BAAI/bge-base-en-v1.5/resolve/main/onnx/model.onnx",
        tokenizer_url: "https://huggingface.co/BAAI/bge-base-en-v1.5/resolve/main/tokenizer.json",
        default: false,
    },
    LocalEmbeddingModelInfo {
        id: "nomic-embed-text-v1.5",
        display_name: "Nomic Embed Text v1.5",
        description: "Local 768-dimensional embedding model for general semantic retrieval.",
        dimension: 768,
        max_tokens: 512,
        model_url: "https://huggingface.co/nomic-ai/nomic-embed-text-v1.5/resolve/main/onnx/model.onnx",
        tokenizer_url: "https://huggingface.co/nomic-ai/nomic-embed-text-v1.5/resolve/main/tokenizer.json",
        default: false,
    },
    LocalEmbeddingModelInfo {
        id: "gte-large",
        display_name: "GTE Large",
        description: "Local 1024-dimensional embedding model for higher quality retrieval.",
        dimension: 1024,
        max_tokens: 512,
        model_url: "https://huggingface.co/thenlper/gte-large/resolve/main/onnx/model.onnx",
        tokenizer_url: "https://huggingface.co/thenlper/gte-large/resolve/main/tokenizer.json",
        default: false,
    },
];

pub fn local_embedding_model_info(model: &str) -> Option<&'static LocalEmbeddingModelInfo> {
    LOCAL_EMBEDDING_MODELS
        .iter()
        .find(|candidate| candidate.id == model)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalTranscriptionEngine {
    Whisper,
    Parakeet,
    Moonshine,
    MoonshineStreaming,
    SenseVoice,
    GigaAm,
    Canary,
    Cohere,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalTranscriptionArtifactKind {
    SingleFile,
    TarGzDirectory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalTranscriptionModelInfo {
    pub id: &'static str,
    pub display_name: &'static str,
    pub description: &'static str,
    pub engine: LocalTranscriptionEngine,
    pub artifact_kind: LocalTranscriptionArtifactKind,
    pub artifact_file_name: &'static str,
    pub install_dir_name: &'static str,
    pub runtime_file_name: Option<&'static str>,
    pub url: &'static str,
    pub sha256: &'static str,
    pub size_mb: u64,
    pub accuracy_score: u8,
    pub speed_score: u8,
    pub supports_translation: bool,
    pub supported_languages: &'static [&'static str],
    pub supports_language_selection: bool,
    pub recommended: bool,
}

const WHISPER_LANGUAGES: &[&str] = &[
    "en", "zh", "zh-Hans", "zh-Hant", "de", "es", "ru", "ko", "fr", "ja", "pt", "tr", "pl", "ca",
    "nl", "ar", "sv", "it", "id", "hi", "fi", "vi", "he", "uk", "el", "ms", "cs", "ro", "da", "hu",
    "ta", "no", "th", "ur", "hr", "bg", "lt", "la", "mi", "ml", "cy", "sk", "te", "fa", "lv", "bn",
    "sr", "az", "sl", "kn", "et", "mk", "br", "eu", "is", "hy", "ne", "mn", "bs", "kk", "sq", "sw",
    "gl", "mr", "pa", "si", "km", "sn", "yo", "so", "af", "oc", "ka", "be", "tg", "sd", "gu", "am",
    "yi", "lo", "uz", "fo", "ht", "ps", "tk", "nn", "mt", "sa", "lb", "my", "bo", "tl", "mg", "as",
    "tt", "haw", "ln", "ha", "ba", "jw", "su", "yue",
];

const PARAKEET_V3_LANGUAGES: &[&str] = &[
    "bg", "hr", "cs", "da", "nl", "en", "et", "fi", "fr", "de", "el", "hu", "it", "lv", "lt", "mt",
    "pl", "pt", "ro", "sk", "sl", "es", "sv", "ru", "uk",
];

const SENSE_VOICE_LANGUAGES: &[&str] = &["zh", "zh-Hans", "zh-Hant", "en", "yue", "ja", "ko"];

const CANARY_FLASH_LANGUAGES: &[&str] = &["en", "de", "es", "fr"];

const COHERE_LANGUAGES: &[&str] = &[
    "en", "fr", "de", "it", "es", "pt", "el", "nl", "pl", "zh", "zh-Hans", "zh-Hant", "ja", "ko",
    "vi", "ar",
];

pub const LOCAL_TRANSCRIPTION_MODELS: &[LocalTranscriptionModelInfo] = &[
    LocalTranscriptionModelInfo {
        id: "small",
        display_name: "Whisper Small",
        description: "Fast and fairly accurate.",
        engine: LocalTranscriptionEngine::Whisper,
        artifact_kind: LocalTranscriptionArtifactKind::SingleFile,
        artifact_file_name: "ggml-small.bin",
        install_dir_name: "whisper-small",
        runtime_file_name: Some("ggml-small.bin"),
        url: "https://blob.handy.computer/ggml-small.bin",
        sha256: "1be3a9b2063867b937e64e2ec7483364a79917e157fa98c5d94b5c1fffea987b",
        size_mb: 465,
        accuracy_score: 60,
        speed_score: 85,
        supports_translation: true,
        supported_languages: WHISPER_LANGUAGES,
        supports_language_selection: true,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "medium",
        display_name: "Whisper Medium",
        description: "Good accuracy, medium speed",
        engine: LocalTranscriptionEngine::Whisper,
        artifact_kind: LocalTranscriptionArtifactKind::SingleFile,
        artifact_file_name: "whisper-medium-q4_1.bin",
        install_dir_name: "whisper-medium",
        runtime_file_name: Some("whisper-medium-q4_1.bin"),
        url: "https://blob.handy.computer/whisper-medium-q4_1.bin",
        sha256: "79283fc1f9fe12ca3248543fbd54b73292164d8df5a16e095e2bceeaaabddf57",
        size_mb: 469,
        accuracy_score: 75,
        speed_score: 60,
        supports_translation: true,
        supported_languages: WHISPER_LANGUAGES,
        supports_language_selection: true,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "turbo",
        display_name: "Whisper Turbo",
        description: "Balanced accuracy and speed.",
        engine: LocalTranscriptionEngine::Whisper,
        artifact_kind: LocalTranscriptionArtifactKind::SingleFile,
        artifact_file_name: "ggml-large-v3-turbo.bin",
        install_dir_name: "whisper-turbo",
        runtime_file_name: Some("ggml-large-v3-turbo.bin"),
        url: "https://blob.handy.computer/ggml-large-v3-turbo.bin",
        sha256: "1fc70f774d38eb169993ac391eea357ef47c88757ef72ee5943879b7e8e2bc69",
        size_mb: 1549,
        accuracy_score: 80,
        speed_score: 40,
        supports_translation: false,
        supported_languages: WHISPER_LANGUAGES,
        supports_language_selection: true,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "large",
        display_name: "Whisper Large",
        description: "Good accuracy, but slow.",
        engine: LocalTranscriptionEngine::Whisper,
        artifact_kind: LocalTranscriptionArtifactKind::SingleFile,
        artifact_file_name: "ggml-large-v3-q5_0.bin",
        install_dir_name: "whisper-large",
        runtime_file_name: Some("ggml-large-v3-q5_0.bin"),
        url: "https://blob.handy.computer/ggml-large-v3-q5_0.bin",
        sha256: "d75795ecff3f83b5faa89d1900604ad8c780abd5739fae406de19f23ecd98ad1",
        size_mb: 1031,
        accuracy_score: 85,
        speed_score: 30,
        supports_translation: true,
        supported_languages: WHISPER_LANGUAGES,
        supports_language_selection: true,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "breeze-asr",
        display_name: "Breeze ASR",
        description: "Optimized for Taiwanese Mandarin. Code-switching support.",
        engine: LocalTranscriptionEngine::Whisper,
        artifact_kind: LocalTranscriptionArtifactKind::SingleFile,
        artifact_file_name: "breeze-asr-q5_k.bin",
        install_dir_name: "whisper-breeze-asr",
        runtime_file_name: Some("breeze-asr-q5_k.bin"),
        url: "https://blob.handy.computer/breeze-asr-q5_k.bin",
        sha256: "8efbf0ce8a3f50fe332b7617da787fb81354b358c288b008d3bdef8359df64c6",
        size_mb: 1030,
        accuracy_score: 85,
        speed_score: 35,
        supports_translation: false,
        supported_languages: WHISPER_LANGUAGES,
        supports_language_selection: true,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "parakeet-tdt-0.6b-v2",
        display_name: "Parakeet V2",
        description: "English only. The best model for English speakers.",
        engine: LocalTranscriptionEngine::Parakeet,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "parakeet-v2-int8.tar.gz",
        install_dir_name: "parakeet-tdt-0.6b-v2-int8",
        runtime_file_name: None,
        url: "https://blob.handy.computer/parakeet-v2-int8.tar.gz",
        sha256: "ac9b9429984dd565b25097337a887bb7f0f8ac393573661c651f0e7d31563991",
        size_mb: 451,
        accuracy_score: 85,
        speed_score: 85,
        supports_translation: false,
        supported_languages: &["en"],
        supports_language_selection: false,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "parakeet-tdt-0.6b-v3",
        display_name: "Parakeet V3",
        description: "Fast and accurate. Supports 25 European languages.",
        engine: LocalTranscriptionEngine::Parakeet,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "parakeet-v3-int8.tar.gz",
        install_dir_name: "parakeet-tdt-0.6b-v3-int8",
        runtime_file_name: None,
        url: "https://blob.handy.computer/parakeet-v3-int8.tar.gz",
        sha256: "43d37191602727524a7d8c6da0eef11c4ba24320f5b4730f1a2497befc2efa77",
        size_mb: 456,
        accuracy_score: 80,
        speed_score: 85,
        supports_translation: false,
        supported_languages: PARAKEET_V3_LANGUAGES,
        supports_language_selection: false,
        recommended: true,
    },
    LocalTranscriptionModelInfo {
        id: "moonshine-base",
        display_name: "Moonshine Base",
        description: "Very fast, English only. Handles accents well.",
        engine: LocalTranscriptionEngine::Moonshine,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "moonshine-base.tar.gz",
        install_dir_name: "moonshine-base",
        runtime_file_name: None,
        url: "https://blob.handy.computer/moonshine-base.tar.gz",
        sha256: "04bf6ab012cfceebd4ac7cf88c1b31d027bbdd3cd704649b692e2e935236b7e8",
        size_mb: 55,
        accuracy_score: 70,
        speed_score: 90,
        supports_translation: false,
        supported_languages: &["en"],
        supports_language_selection: false,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "moonshine-tiny-streaming-en",
        display_name: "Moonshine V2 Tiny",
        description: "Ultra-fast, English only",
        engine: LocalTranscriptionEngine::MoonshineStreaming,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "moonshine-tiny-streaming-en.tar.gz",
        install_dir_name: "moonshine-tiny-streaming-en",
        runtime_file_name: None,
        url: "https://blob.handy.computer/moonshine-tiny-streaming-en.tar.gz",
        sha256: "465addcfca9e86117415677dfdc98b21edc53537210333a3ecdb58509a80abaf",
        size_mb: 31,
        accuracy_score: 55,
        speed_score: 95,
        supports_translation: false,
        supported_languages: &["en"],
        supports_language_selection: false,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "moonshine-small-streaming-en",
        display_name: "Moonshine V2 Small",
        description: "Fast, English only. Good balance of speed and accuracy.",
        engine: LocalTranscriptionEngine::MoonshineStreaming,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "moonshine-small-streaming-en.tar.gz",
        install_dir_name: "moonshine-small-streaming-en",
        runtime_file_name: None,
        url: "https://blob.handy.computer/moonshine-small-streaming-en.tar.gz",
        sha256: "dbb3e1c1832bd88a4ac712f7449a136cc2c9a18c5fe33a12ed1b7cb1cfe9cdd5",
        size_mb: 99,
        accuracy_score: 65,
        speed_score: 90,
        supports_translation: false,
        supported_languages: &["en"],
        supports_language_selection: false,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "moonshine-medium-streaming-en",
        display_name: "Moonshine V2 Medium",
        description: "English only. High quality.",
        engine: LocalTranscriptionEngine::MoonshineStreaming,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "moonshine-medium-streaming-en.tar.gz",
        install_dir_name: "moonshine-medium-streaming-en",
        runtime_file_name: None,
        url: "https://blob.handy.computer/moonshine-medium-streaming-en.tar.gz",
        sha256: "07a66f3bff1c77e75a2f637e5a263928a08baae3c29c4c053fc968a9a9373d13",
        size_mb: 192,
        accuracy_score: 75,
        speed_score: 80,
        supports_translation: false,
        supported_languages: &["en"],
        supports_language_selection: false,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "sense-voice-int8",
        display_name: "SenseVoice",
        description: "Very fast. Chinese, English, Japanese, Korean, Cantonese.",
        engine: LocalTranscriptionEngine::SenseVoice,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "sense-voice-int8.tar.gz",
        install_dir_name: "sense-voice-int8",
        runtime_file_name: None,
        url: "https://blob.handy.computer/sense-voice-int8.tar.gz",
        sha256: "171d611fe5d353a50bbb741b6f3ef42559b1565685684e9aa888ef563ba3e8a4",
        size_mb: 152,
        accuracy_score: 65,
        speed_score: 95,
        supports_translation: false,
        supported_languages: SENSE_VOICE_LANGUAGES,
        supports_language_selection: true,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "gigaam-v3-e2e-ctc",
        display_name: "GigaAM v3",
        description: "Russian speech recognition. Fast and accurate.",
        engine: LocalTranscriptionEngine::GigaAm,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "giga-am-v3-int8.tar.gz",
        install_dir_name: "giga-am-v3-int8",
        runtime_file_name: None,
        url: "https://blob.handy.computer/giga-am-v3-int8.tar.gz",
        sha256: "d872462268430db140b69b72e0fc4b787b194c1dbe51b58de39444d55b6da45b",
        size_mb: 151,
        accuracy_score: 85,
        speed_score: 75,
        supports_translation: false,
        supported_languages: &["ru"],
        supports_language_selection: false,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "canary-180m-flash",
        display_name: "Canary 180M Flash",
        description: "Very fast. English, German, Spanish, French. Supports translation.",
        engine: LocalTranscriptionEngine::Canary,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "canary-180m-flash.tar.gz",
        install_dir_name: "canary-180m-flash",
        runtime_file_name: None,
        url: "https://blob.handy.computer/canary-180m-flash.tar.gz",
        sha256: "6d9cfca6118b296e196eaedc1c8fa9788305a7b0f1feafdb6dc91932ab6e53f7",
        size_mb: 146,
        accuracy_score: 75,
        speed_score: 85,
        supports_translation: true,
        supported_languages: CANARY_FLASH_LANGUAGES,
        supports_language_selection: true,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "canary-1b-v2",
        display_name: "Canary 1B v2",
        description: "Accurate multilingual. 25 European languages. Supports translation.",
        engine: LocalTranscriptionEngine::Canary,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "canary-1b-v2.tar.gz",
        install_dir_name: "canary-1b-v2",
        runtime_file_name: None,
        url: "https://blob.handy.computer/canary-1b-v2.tar.gz",
        sha256: "02305b2a25f9cf3e7deaffa7f94df00efa44f442cd55c101c2cb9c000f904666",
        size_mb: 691,
        accuracy_score: 85,
        speed_score: 70,
        supports_translation: true,
        supported_languages: PARAKEET_V3_LANGUAGES,
        supports_language_selection: true,
        recommended: false,
    },
    LocalTranscriptionModelInfo {
        id: "cohere-int8",
        display_name: "Cohere",
        description: "A large, slower, but very accurate multilingual model.",
        engine: LocalTranscriptionEngine::Cohere,
        artifact_kind: LocalTranscriptionArtifactKind::TarGzDirectory,
        artifact_file_name: "cohere-int8.tar.gz",
        install_dir_name: "cohere-int8",
        runtime_file_name: None,
        url: "https://blob.handy.computer/cohere-int8.tar.gz",
        sha256: "ea2257d52434f3644574f187dcdcf666e302cd11b92866116ab8e14cd9c887f0",
        size_mb: 1708,
        accuracy_score: 90,
        speed_score: 60,
        supports_translation: false,
        supported_languages: COHERE_LANGUAGES,
        supports_language_selection: true,
        recommended: false,
    },
];

pub fn local_transcription_model_info(model: &str) -> Option<&'static LocalTranscriptionModelInfo> {
    LOCAL_TRANSCRIPTION_MODELS
        .iter()
        .find(|candidate| candidate.id == model)
}

#[derive(Debug, Clone, Default)]
pub struct LocalProvider;

impl LocalProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Provider for LocalProvider {
    fn name(&self) -> &str {
        LOCAL_PROVIDER_ID
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: false,
            embeddings: true,
            transcription: true,
            input_types: ProviderInputCapabilities::disabled_for_all_file_types(),
        }
    }

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse> {
        bail!("provider '{LOCAL_PROVIDER_ID}' does not support chat")
    }

    async fn stream_chat(
        &self,
        _request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        bail!("provider '{LOCAL_PROVIDER_ID}' does not support chat streaming")
    }

    async fn list_embedding_models(&self) -> Result<Vec<ProviderModelInfo>> {
        Ok(LOCAL_EMBEDDING_MODELS
            .iter()
            .map(local_embedding_model_to_provider_model)
            .collect())
    }

    async fn list_transcription_models(&self) -> Result<Vec<ProviderModelInfo>> {
        Ok(LOCAL_TRANSCRIPTION_MODELS
            .iter()
            .map(local_transcription_model_to_provider_model)
            .collect())
    }

    async fn embed(&self, _request: EmbeddingRequest) -> Result<EmbeddingResponse> {
        bail!(
            "provider '{LOCAL_PROVIDER_ID}' uses the gateway local embedding runtime for embedding execution"
        )
    }
}

fn local_embedding_model_to_provider_model(model: &LocalEmbeddingModelInfo) -> ProviderModelInfo {
    ProviderModelInfo {
        id: model.id.to_owned(),
        name: Some(model.display_name.to_owned()),
        description: Some(model.description.to_owned()),
        created: None,
        provider: LOCAL_PROVIDER_ID.to_owned(),
        owned_by: Some("local".to_owned()),
        limits: ProviderModelLimits {
            max_input_tokens: Some(model.max_tokens as u64),
            max_output_tokens: None,
            context_window: Some(model.max_tokens as u64),
        },
        capabilities: ProviderModelCapabilities {
            embeddings: Some(true),
            ..ProviderModelCapabilities::default()
        },
        transcription: None,
        pricing: None,
        active: Some(true),
        family: Some("embedding".to_owned()),
        lifecycle_status: None,
    }
}

fn local_transcription_model_to_provider_model(
    model: &LocalTranscriptionModelInfo,
) -> ProviderModelInfo {
    let engine = local_transcription_engine_id(model.engine);

    ProviderModelInfo {
        id: model.id.to_owned(),
        name: Some(model.display_name.to_owned()),
        description: Some(model.description.to_owned()),
        created: None,
        provider: LOCAL_PROVIDER_ID.to_owned(),
        owned_by: Some("local".to_owned()),
        limits: ProviderModelLimits::default(),
        capabilities: ProviderModelCapabilities {
            transcription: Some(true),
            input_modalities: Some(vec!["audio".to_owned()]),
            output_modalities: Some(vec!["text".to_owned()]),
            ..ProviderModelCapabilities::default()
        },
        transcription: Some(ProviderTranscriptionModelMetadata {
            engine: engine.to_owned(),
            download_size_mb: model.size_mb,
            accuracy_score: model.accuracy_score,
            speed_score: model.speed_score,
            supports_translation: model.supports_translation,
            supported_languages: model
                .supported_languages
                .iter()
                .map(|language| (*language).to_owned())
                .collect(),
            supports_language_selection: model.supports_language_selection,
            recommended: model.recommended,
        }),
        pricing: None,
        active: Some(true),
        family: Some(engine.to_owned()),
        lifecycle_status: None,
    }
}

const fn local_transcription_engine_id(engine: LocalTranscriptionEngine) -> &'static str {
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
    use std::collections::HashSet;

    use super::*;

    const EXPECTED_TRANSCRIPTION_MODEL_IDS: &[&str] = &[
        "small",
        "medium",
        "turbo",
        "large",
        "breeze-asr",
        "parakeet-tdt-0.6b-v2",
        "parakeet-tdt-0.6b-v3",
        "moonshine-base",
        "moonshine-tiny-streaming-en",
        "moonshine-small-streaming-en",
        "moonshine-medium-streaming-en",
        "sense-voice-int8",
        "gigaam-v3-e2e-ctc",
        "canary-180m-flash",
        "canary-1b-v2",
        "cohere-int8",
    ];

    #[test]
    fn local_transcription_catalog_has_exact_deterministic_model_order() {
        let actual = LOCAL_TRANSCRIPTION_MODELS
            .iter()
            .map(|model| model.id)
            .collect::<Vec<_>>();

        assert_eq!(actual, EXPECTED_TRANSCRIPTION_MODEL_IDS);
    }

    #[test]
    fn local_transcription_catalog_has_unique_ids_and_paths() {
        let mut ids = HashSet::new();
        let mut artifacts = HashSet::new();
        let mut install_dirs = HashSet::new();

        for model in LOCAL_TRANSCRIPTION_MODELS {
            assert!(ids.insert(model.id), "duplicate model id: {}", model.id);
            assert!(
                artifacts.insert(model.artifact_file_name),
                "duplicate artifact name: {}",
                model.artifact_file_name
            );
            assert!(
                install_dirs.insert(model.install_dir_name),
                "duplicate install directory: {}",
                model.install_dir_name
            );
        }
    }

    #[test]
    fn local_transcription_catalog_uses_trusted_artifacts() {
        for model in LOCAL_TRANSCRIPTION_MODELS {
            assert!(
                model.url.starts_with("https://blob.handy.computer/"),
                "untrusted artifact URL for {}: {}",
                model.id,
                model.url
            );
            assert_eq!(model.sha256.len(), 64, "invalid SHA-256 for {}", model.id);
            assert!(
                model.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "non-hex SHA-256 for {}",
                model.id
            );
            assert!(
                model.url.ends_with(model.artifact_file_name),
                "artifact filename does not match URL for {}",
                model.id
            );
        }
    }

    #[test]
    fn local_transcription_catalog_has_one_recommended_model() {
        let recommended = LOCAL_TRANSCRIPTION_MODELS
            .iter()
            .filter(|model| model.recommended)
            .collect::<Vec<_>>();

        assert_eq!(recommended.len(), 1);
        assert_eq!(recommended[0].id, "parakeet-tdt-0.6b-v3");
    }

    #[test]
    fn local_transcription_catalog_lookup_is_exact() {
        assert_eq!(
            local_transcription_model_info("parakeet-tdt-0.6b-v3").map(|model| model.id),
            Some("parakeet-tdt-0.6b-v3")
        );
        assert!(local_transcription_model_info("PARAKEET-TDT-0.6B-V3").is_none());
        assert!(local_transcription_model_info("unknown").is_none());
    }

    #[tokio::test]
    async fn local_provider_lists_embedding_models_as_records() {
        let provider = LocalProvider::new();
        let models = provider
            .list_embedding_models()
            .await
            .expect("local embedding models");

        let small = models
            .iter()
            .find(|model| model.id == "bge-small-en-v1.5")
            .expect("bge small model");
        assert_eq!(small.provider, "local");
        assert_eq!(small.name.as_deref(), Some("BGE Small EN v1.5"));
        assert_eq!(small.capabilities.embeddings, Some(true));
    }

    #[tokio::test]
    async fn local_provider_lists_transcription_models_as_client_safe_records() {
        let provider = LocalProvider::new();
        let models = provider
            .list_transcription_models()
            .await
            .expect("local transcription models");

        assert_eq!(models.len(), 16);
        assert_eq!(
            models
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            EXPECTED_TRANSCRIPTION_MODEL_IDS
        );
        assert!(provider.capabilities().transcription);

        let parakeet = models
            .iter()
            .find(|model| model.id == "parakeet-tdt-0.6b-v3")
            .expect("Parakeet V3 model");
        assert_eq!(parakeet.provider, "local");
        assert_eq!(parakeet.family.as_deref(), Some("parakeet"));
        assert_eq!(parakeet.capabilities.transcription, Some(true));
        assert_eq!(
            parakeet.capabilities.input_modalities.as_deref(),
            Some(["audio".to_owned()].as_slice())
        );
        assert_eq!(
            parakeet.capabilities.output_modalities.as_deref(),
            Some(["text".to_owned()].as_slice())
        );
        assert_eq!(parakeet.active, Some(true));
        assert!(
            parakeet
                .transcription
                .as_ref()
                .is_some_and(|metadata| metadata.recommended)
        );

        let serialized = serde_json::to_string(&models).expect("models should serialize");
        for trusted_value in [
            "blob.handy.computer",
            "sha256",
            "artifact_file_name",
            "install_dir_name",
            "runtime_file_name",
        ] {
            assert!(
                !serialized.contains(trusted_value),
                "trusted catalog value leaked: {trusted_value}"
            );
        }
    }
}
