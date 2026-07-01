use pioneer_protocol::{VoiceAudioFormat, validate_voice_streaming_audio_format};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NormalizedVoiceAudioChunk {
    pub(crate) sample_rate_hz: u32,
    pub(crate) channels: u16,
    pub(crate) samples: Vec<f32>,
}

impl NormalizedVoiceAudioChunk {
    pub(crate) fn memory_bytes(&self) -> usize {
        self.samples.len() * std::mem::size_of::<f32>()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum VoiceAudioNormalizationError {
    UnsupportedFormat(String),
    IncompletePcmSample,
}

impl std::fmt::Display for VoiceAudioNormalizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedFormat(message) => formatter.write_str(message),
            Self::IncompletePcmSample => {
                formatter.write_str("pcm_s16le payload must contain complete samples")
            }
        }
    }
}

impl std::error::Error for VoiceAudioNormalizationError {}

pub(crate) fn normalize_voice_pcm_chunk(
    format: VoiceAudioFormat,
    pcm_payload: &[u8],
) -> Result<NormalizedVoiceAudioChunk, VoiceAudioNormalizationError> {
    validate_voice_streaming_audio_format(&format)
        .map_err(|error| VoiceAudioNormalizationError::UnsupportedFormat(error.to_string()))?;
    if pcm_payload.len() % 2 != 0 {
        return Err(VoiceAudioNormalizationError::IncompletePcmSample);
    }

    let samples = pcm_payload
        .chunks_exact(2)
        .map(|bytes| i16::from_le_bytes([bytes[0], bytes[1]]) as f32 / 32768.0)
        .collect();

    Ok(NormalizedVoiceAudioChunk {
        sample_rate_hz: format.sample_rate_hz,
        channels: format.channels,
        samples,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{VoiceAudioEncoding, VoiceAudioFormat};

    #[test]
    fn normalizes_pcm_s16le_to_f32_samples() {
        let normalized = normalize_voice_pcm_chunk(
            target_format(),
            &[
                0x00, 0x00, // 0
                0x00, 0x40, // 16384
                0xff, 0x7f, // 32767
                0x00, 0x80, // -32768
            ],
        )
        .expect("normalize");

        assert_eq!(normalized.sample_rate_hz, 16_000);
        assert_eq!(normalized.channels, 1);
        assert_eq!(normalized.samples[0], 0.0);
        assert_eq!(normalized.samples[1], 0.5);
        assert!((normalized.samples[2] - 0.9999695).abs() < 0.000001);
        assert_eq!(normalized.samples[3], -1.0);
    }

    #[test]
    fn rejects_unsupported_format() {
        let error = normalize_voice_pcm_chunk(
            VoiceAudioFormat {
                sample_rate_hz: 48_000,
                channels: 1,
                encoding: VoiceAudioEncoding::PcmS16Le,
            },
            &[0x00, 0x00],
        )
        .expect_err("unsupported format should fail");

        assert!(matches!(
            error,
            VoiceAudioNormalizationError::UnsupportedFormat(_)
        ));
    }

    fn target_format() -> VoiceAudioFormat {
        VoiceAudioFormat {
            sample_rate_hz: 16_000,
            channels: 1,
            encoding: VoiceAudioEncoding::PcmS16Le,
        }
    }
}
