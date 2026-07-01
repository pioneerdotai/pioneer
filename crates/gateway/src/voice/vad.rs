use anyhow::{Result, bail};
#[cfg(test)]
use pioneer_protocol::VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ;
use std::collections::VecDeque;

pub(crate) const VOICE_VAD_FRAME_MS: u32 = 30;
pub(crate) const VOICE_VAD_FRAME_SAMPLES: usize = 480;
pub(crate) const VOICE_VAD_SPEECH_THRESHOLD: f32 = 0.3;
pub(crate) const VOICE_VAD_PREFILL_FRAMES: usize = 15;
pub(crate) const VOICE_VAD_HANGOVER_FRAMES: usize = 15;
pub(crate) const VOICE_VAD_ONSET_FRAMES: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct VoiceVadConfig {
    pub(crate) frame_ms: u32,
    pub(crate) frame_samples: usize,
    pub(crate) speech_threshold: f32,
    pub(crate) prefill_frames: usize,
    pub(crate) hangover_frames: usize,
    pub(crate) onset_frames: usize,
}

impl Default for VoiceVadConfig {
    fn default() -> Self {
        Self {
            frame_ms: VOICE_VAD_FRAME_MS,
            frame_samples: VOICE_VAD_FRAME_SAMPLES,
            speech_threshold: VOICE_VAD_SPEECH_THRESHOLD,
            prefill_frames: VOICE_VAD_PREFILL_FRAMES,
            hangover_frames: VOICE_VAD_HANGOVER_FRAMES,
            onset_frames: VOICE_VAD_ONSET_FRAMES,
        }
    }
}

impl VoiceVadConfig {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.frame_ms == 0 || self.frame_samples == 0 {
            bail!("voice VAD frame size must be positive");
        }
        if !(0.0..=1.0).contains(&self.speech_threshold) {
            bail!("voice VAD speech threshold must be between 0.0 and 1.0");
        }
        if self.onset_frames == 0 {
            bail!("voice VAD onset_frames must be positive");
        }
        Ok(self)
    }
}

pub(crate) trait VoiceActivityDetector {
    fn speech_probability(&mut self, frame: &[f32]) -> Result<f32>;
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EnergyVoiceActivityDetector {
    threshold_floor: f32,
}

impl EnergyVoiceActivityDetector {
    pub(crate) fn new(threshold_floor: f32) -> Result<Self> {
        if !(0.0..=1.0).contains(&threshold_floor) {
            bail!("energy VAD threshold_floor must be between 0.0 and 1.0");
        }
        Ok(Self { threshold_floor })
    }
}

impl VoiceActivityDetector for EnergyVoiceActivityDetector {
    fn speech_probability(&mut self, frame: &[f32]) -> Result<f32> {
        if frame.is_empty() {
            return Ok(0.0);
        }
        let rms = (frame.iter().map(|sample| sample * sample).sum::<f32>() / frame.len() as f32)
            .sqrt()
            .clamp(0.0, 1.0);
        Ok((rms / self.threshold_floor.max(0.000_001)).clamp(0.0, 1.0))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VoiceSpeechSegment {
    pub(crate) start_sample: usize,
    pub(crate) end_sample: usize,
    pub(crate) samples: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum VoiceVadSegmentationOutcome {
    NoSpeech {
        total_samples: usize,
        reason: VoiceNoSpeechReason,
    },
    Speech {
        total_samples: usize,
        segments: Vec<VoiceSpeechSegment>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceNoSpeechReason {
    TooShort,
    NoSpeechDetected,
}

pub(crate) struct SmoothedVoiceVad<D> {
    detector: D,
    config: VoiceVadConfig,
    frame_buffer: VecDeque<Vec<f32>>,
    hangover_counter: usize,
    onset_counter: usize,
    in_speech: bool,
}

impl<D> SmoothedVoiceVad<D>
where
    D: VoiceActivityDetector,
{
    pub(crate) fn new(detector: D, config: VoiceVadConfig) -> Result<Self> {
        Ok(Self {
            detector,
            config: config.validate()?,
            frame_buffer: VecDeque::new(),
            hangover_counter: 0,
            onset_counter: 0,
            in_speech: false,
        })
    }

    pub(crate) fn segment_samples(
        &mut self,
        samples: &[f32],
    ) -> Result<VoiceVadSegmentationOutcome> {
        if samples.len() < self.config.frame_samples {
            return Ok(VoiceVadSegmentationOutcome::NoSpeech {
                total_samples: samples.len(),
                reason: VoiceNoSpeechReason::TooShort,
            });
        }

        let mut segments = Vec::new();
        let mut current_segment = Vec::new();
        let mut current_start_sample = 0usize;

        for (frame_index, frame) in samples.chunks_exact(self.config.frame_samples).enumerate() {
            match self.push_frame(frame)? {
                SmoothedVadFrame::Speech(speech_samples) => {
                    if current_segment.is_empty() {
                        current_start_sample = frame_index
                            .saturating_add(1)
                            .saturating_sub(self.config.prefill_frames.saturating_add(1))
                            * self.config.frame_samples;
                    }
                    current_segment.extend_from_slice(speech_samples.as_slice());
                }
                SmoothedVadFrame::Noise => {
                    if !current_segment.is_empty() && !self.in_speech {
                        let samples = std::mem::take(&mut current_segment);
                        let end_sample = current_start_sample + samples.len();
                        segments.push(VoiceSpeechSegment {
                            start_sample: current_start_sample,
                            end_sample,
                            samples,
                        });
                    }
                }
            }
        }

        if !current_segment.is_empty() {
            let samples = std::mem::take(&mut current_segment);
            let end_sample = current_start_sample + samples.len();
            segments.push(VoiceSpeechSegment {
                start_sample: current_start_sample,
                end_sample,
                samples,
            });
        }

        if segments.is_empty() {
            Ok(VoiceVadSegmentationOutcome::NoSpeech {
                total_samples: samples.len(),
                reason: VoiceNoSpeechReason::NoSpeechDetected,
            })
        } else {
            Ok(VoiceVadSegmentationOutcome::Speech {
                total_samples: samples.len(),
                segments,
            })
        }
    }

    fn push_frame(&mut self, frame: &[f32]) -> Result<SmoothedVadFrame> {
        self.frame_buffer.push_back(frame.to_vec());
        while self.frame_buffer.len() > self.config.prefill_frames + 1 {
            self.frame_buffer.pop_front();
        }

        let probability = self.detector.speech_probability(frame)?;
        let is_voice = probability > self.config.speech_threshold;
        match (self.in_speech, is_voice) {
            (false, true) => {
                self.onset_counter += 1;
                if self.onset_counter >= self.config.onset_frames {
                    self.in_speech = true;
                    self.hangover_counter = self.config.hangover_frames;
                    self.onset_counter = 0;
                    let mut speech = Vec::new();
                    for buffered_frame in &self.frame_buffer {
                        speech.extend_from_slice(buffered_frame.as_slice());
                    }
                    Ok(SmoothedVadFrame::Speech(speech))
                } else {
                    Ok(SmoothedVadFrame::Noise)
                }
            }
            (true, true) => {
                self.hangover_counter = self.config.hangover_frames;
                Ok(SmoothedVadFrame::Speech(frame.to_vec()))
            }
            (true, false) => {
                if self.hangover_counter > 0 {
                    self.hangover_counter -= 1;
                    Ok(SmoothedVadFrame::Speech(frame.to_vec()))
                } else {
                    self.in_speech = false;
                    Ok(SmoothedVadFrame::Noise)
                }
            }
            (false, false) => {
                self.onset_counter = 0;
                Ok(SmoothedVadFrame::Noise)
            }
        }
    }
}

enum SmoothedVadFrame {
    Speech(Vec<f32>),
    Noise,
}

#[cfg(test)]
pub(crate) fn voice_vad_frame_samples_for_target_rate() -> usize {
    (VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ as usize * VOICE_VAD_FRAME_MS as usize) / 1000
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct ScriptedVad {
        probabilities: VecDeque<f32>,
    }

    impl ScriptedVad {
        fn new(probabilities: impl IntoIterator<Item = f32>) -> Self {
            Self {
                probabilities: probabilities.into_iter().collect(),
            }
        }
    }

    impl VoiceActivityDetector for ScriptedVad {
        fn speech_probability(&mut self, _frame: &[f32]) -> Result<f32> {
            Ok(self.probabilities.pop_front().unwrap_or(0.0))
        }
    }

    #[test]
    fn smoothed_vad_segments_speech_with_prefill_and_hangover() {
        let config = VoiceVadConfig {
            frame_samples: 2,
            prefill_frames: 1,
            hangover_frames: 1,
            onset_frames: 2,
            ..VoiceVadConfig::default()
        };
        let mut vad = SmoothedVoiceVad::new(ScriptedVad::new([0.0, 0.9, 0.95, 0.0, 0.0]), config)
            .expect("vad");
        let outcome = vad
            .segment_samples(&[
                0.0, 0.0, // noise prefill
                0.1, 0.1, // onset 1
                0.2, 0.2, // onset 2 emits prefill + current
                0.0, 0.0, // hangover
                0.0, 0.0, // end
            ])
            .expect("segment");

        let VoiceVadSegmentationOutcome::Speech { segments, .. } = outcome else {
            panic!("speech expected");
        };
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].start_sample, 2);
        assert_eq!(segments[0].samples, vec![0.1, 0.1, 0.2, 0.2, 0.0, 0.0]);
    }

    #[test]
    fn smoothed_vad_reports_no_speech_without_segments() {
        let mut vad = SmoothedVoiceVad::new(
            ScriptedVad::new([0.0, 0.0, 0.0]),
            VoiceVadConfig {
                frame_samples: 2,
                ..VoiceVadConfig::default()
            },
        )
        .expect("vad");
        let outcome = vad
            .segment_samples(&[0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
            .expect("segment");

        assert_eq!(
            outcome,
            VoiceVadSegmentationOutcome::NoSpeech {
                total_samples: 6,
                reason: VoiceNoSpeechReason::NoSpeechDetected,
            }
        );
    }

    #[test]
    fn too_short_audio_reports_no_speech() {
        let mut vad = SmoothedVoiceVad::new(
            ScriptedVad::new([]),
            VoiceVadConfig {
                frame_samples: 4,
                ..VoiceVadConfig::default()
            },
        )
        .expect("vad");
        let outcome = vad.segment_samples(&[0.0, 0.0]).expect("segment");

        assert_eq!(
            outcome,
            VoiceVadSegmentationOutcome::NoSpeech {
                total_samples: 2,
                reason: VoiceNoSpeechReason::TooShort,
            }
        );
    }

    #[test]
    fn default_frame_samples_match_target_sample_rate() {
        assert_eq!(
            voice_vad_frame_samples_for_target_rate(),
            VOICE_VAD_FRAME_SAMPLES
        );
    }
}
