use anyhow::{Result, bail};
#[cfg(test)]
use pioneer_protocol::VOICE_AUDIO_TARGET_SAMPLE_RATE_HZ;
use std::collections::VecDeque;

pub(crate) const VOICE_VAD_FRAME_MS: u32 = 32;
pub(crate) const VOICE_VAD_FRAME_SAMPLES: usize = 512;
pub(crate) const VOICE_VAD_SPEECH_THRESHOLD: f32 = 0.3;
pub(crate) const VOICE_VAD_PREFILL_FRAMES: usize = 14;
pub(crate) const VOICE_VAD_HANGOVER_FRAMES: usize = 14;
pub(crate) const VOICE_VAD_ONSET_FRAMES: usize = 2;
// 450 ms prefill/hangover round down to 448 ms; 60 ms onset rounds up
// to 64 ms. The pending PCM bound stays <= the old 16 * 480 samples.
// After hangover, the same bounded real prefill bridges inter-speech gaps up
// to 864 ms (448 + 13 * 32) completely. Longer gaps retain real edges only.
pub(crate) const VOICE_VAD_CONTINUATION_THRESHOLD: f32 = 0.2;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct VadInterval {
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) probability: f32,
    pub(super) voice: bool,
    // Meaningful for quiet intervals: source start of this uninterrupted run
    // on the full VAD input, even if a separating positive frame is not emitted.
    pub(super) quiet_run_start: usize,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct VoiceVadConfig {
    pub(crate) frame_ms: u32,
    pub(crate) frame_samples: usize,
    pub(crate) speech_threshold: f32,
    pub(crate) continuation_threshold: f32,
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
            continuation_threshold: VOICE_VAD_CONTINUATION_THRESHOLD,
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
        if !self.continuation_threshold.is_finite()
            || !(0.0..=self.speech_threshold).contains(&self.continuation_threshold)
        {
            bail!("voice VAD continuation threshold must not exceed onset threshold");
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

// Deterministic pipeline fixture only; never a production fallback.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EnergyVoiceActivityDetector {
    threshold_floor: f32,
}

#[cfg(test)]
impl EnergyVoiceActivityDetector {
    pub(crate) fn new(threshold_floor: f32) -> Result<Self> {
        if !(0.0..=1.0).contains(&threshold_floor) {
            bail!("energy VAD threshold_floor must be between 0.0 and 1.0");
        }
        Ok(Self { threshold_floor })
    }
}

#[cfg(test)]
impl VoiceActivityDetector for EnergyVoiceActivityDetector {
    fn speech_probability(&mut self, frame: &[f32]) -> Result<f32> {
        if frame.len() != VOICE_VAD_FRAME_SAMPLES {
            bail!("test detector requires a full v4 frame");
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

#[cfg(test)]
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

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceNoSpeechReason {
    TooShort,
    NoSpeechDetected,
}

pub(crate) struct SmoothedVoiceVad<D> {
    detector: D,
    config: VoiceVadConfig,
    frame_buffer: VecDeque<Vec<f32>>,
    recent_intervals: VecDeque<VadInterval>,
    emitted_intervals: Vec<VadInterval>,
    source_cursor: usize,
    hangover_counter: usize,
    onset_counter: usize,
    in_speech: bool,
    // Source time since the last positive detector frame, including hangover.
    // This is independent of how much PCM smoothing emits or retains as prefill.
    quiet_samples: usize,
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
            recent_intervals: VecDeque::new(),
            emitted_intervals: Vec::new(),
            source_cursor: 0,
            hangover_counter: 0,
            onset_counter: 0,
            in_speech: false,
            quiet_samples: 0,
        })
    }

    #[cfg(test)]
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

    // Unlike one-shot segmentation, an incremental caller owns emitted audio.
    // Never return that prefix again as prefill at the next onset.
    pub(super) fn push_incremental_frame(&mut self, frame: &[f32]) -> Result<SmoothedVadFrame> {
        let result = self.push_frame(frame)?;
        self.emitted_intervals.clear();
        if let SmoothedVadFrame::Speech(samples) = &result {
            let start = self.source_cursor - samples.len();
            self.emitted_intervals.extend(
                self.recent_intervals
                    .iter()
                    .copied()
                    .filter(|interval| interval.end > start),
            );
            self.frame_buffer.clear();
            self.recent_intervals.clear();
        }
        Ok(result)
    }

    pub(super) fn quiet_samples(&self) -> usize {
        self.quiet_samples
    }

    pub(super) fn take_emitted_intervals(&mut self) -> Vec<VadInterval> {
        std::mem::take(&mut self.emitted_intervals)
    }

    pub(super) fn finish_incremental_tail(&mut self, tail: &[f32]) -> Result<Vec<f32>> {
        if tail.len() >= self.config.frame_samples {
            bail!("voice VAD tail must be shorter than a frame");
        }
        self.emitted_intervals.clear();
        // Only the final partial input is padded, even while already in speech.
        // Its probability/state sees padding; its clock and downstream PCM do not.
        let voiced_tail = if tail.is_empty() {
            false
        } else {
            let mut frame = vec![0.0; self.config.frame_samples];
            frame[..tail.len()].copy_from_slice(tail);
            self.classify(&frame, tail.len())?
        };
        if self.in_speech {
            self.emitted_intervals
                .extend(self.recent_intervals.drain(..));
            self.frame_buffer.clear();
            return Ok(tail.to_vec());
        }
        if self.onset_counter == 0 && !voiced_tail {
            return Ok(Vec::new());
        }
        let mut samples: Vec<_> = self.frame_buffer.drain(..).flatten().collect();
        self.emitted_intervals
            .extend(self.recent_intervals.drain(..));
        self.onset_counter = 0;
        samples.extend_from_slice(tail);
        Ok(samples)
    }

    fn classify(&mut self, frame: &[f32], real_samples: usize) -> Result<bool> {
        let probability = self.detector.speech_probability(frame)?;
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            bail!("voice VAD returned an invalid probability");
        }
        // Onset remains 0.3; continuing established speech uses 0.2. Quiet time
        // and pause candidates use this SAME hysteretic decision, never hangover.
        let threshold = if self.in_speech {
            self.config.continuation_threshold
        } else {
            self.config.speech_threshold
        };
        let voice = probability > threshold;
        self.recent_intervals.push_back(VadInterval {
            start: self.source_cursor,
            end: self.source_cursor + real_samples,
            probability,
            voice,
            quiet_run_start: if voice {
                self.source_cursor
            } else {
                self.source_cursor - self.quiet_samples
            },
        });
        self.source_cursor += real_samples;
        self.quiet_samples = if voice {
            0
        } else {
            self.quiet_samples.saturating_add(real_samples)
        };
        Ok(voice)
    }

    fn push_frame(&mut self, frame: &[f32]) -> Result<SmoothedVadFrame> {
        if frame.len() != self.config.frame_samples {
            bail!("voice VAD requires a complete frame");
        }
        let is_voice = self.classify(frame, frame.len())?;
        self.frame_buffer.push_back(frame.to_vec());
        // Keep un-emitted real PCM, including onset, only. This one queue both
        // preserves short pauses and supplies the trailing edge of long ones;
        // hangover already transferred to the caller cannot re-enter it.
        while self.frame_buffer.len() > self.config.prefill_frames + 1 {
            self.frame_buffer.pop_front();
            self.recent_intervals.pop_front();
        }
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

pub(super) enum SmoothedVadFrame {
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
        frames: Vec<Vec<f32>>,
    }

    impl ScriptedVad {
        fn new(probabilities: impl IntoIterator<Item = f32>) -> Self {
            Self {
                probabilities: probabilities.into_iter().collect(),
                frames: Vec::new(),
            }
        }
    }

    impl VoiceActivityDetector for ScriptedVad {
        fn speech_probability(&mut self, frame: &[f32]) -> Result<f32> {
            self.frames.push(frame.to_vec());
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

    #[test]
    fn final_onset_classifies_only_full_frames_and_emits_only_real_samples_once() {
        for tail_samples in [0, 1, 137, 479, 511] {
            let mut vad =
                SmoothedVoiceVad::new(ScriptedVad::new([0.0, 0.9, 0.9]), VoiceVadConfig::default())
                    .unwrap();
            let noise = vec![0.1; VOICE_VAD_FRAME_SAMPLES];
            let onset = vec![0.2; VOICE_VAD_FRAME_SAMPLES];
            assert!(matches!(
                vad.push_incremental_frame(&noise).unwrap(),
                SmoothedVadFrame::Noise
            ));
            assert!(matches!(
                vad.push_incremental_frame(&onset).unwrap(),
                SmoothedVadFrame::Noise
            ));
            let tail = vec![0.3; tail_samples];
            let mut expected = noise;
            expected.extend(onset);
            expected.extend(&tail);
            assert_eq!(vad.finish_incremental_tail(&tail).unwrap(), expected);
            assert!(vad.finish_incremental_tail(&[]).unwrap().is_empty());
            assert_eq!(
                vad.detector.frames.len(),
                if tail_samples == 0 { 2 } else { 3 }
            );
            for frame in &vad.detector.frames {
                assert_eq!(frame.len(), VOICE_VAD_FRAME_SAMPLES);
            }
            if tail_samples != 0 {
                let classified = &vad.detector.frames[2];
                assert_eq!(&classified[..tail_samples], tail);
                assert!(
                    classified[tail_samples..]
                        .iter()
                        .all(|sample| *sample == 0.0)
                );
            }
        }
    }

    #[test]
    fn empty_and_subframe_input_have_no_synthetic_audio_or_synthetic_next_frame() {
        for tail_samples in [0, 1, 137, 479, 511] {
            for probability in [0.0, 0.9] {
                let mut vad = SmoothedVoiceVad::new(
                    ScriptedVad::new([probability, 0.9, 0.9]),
                    VoiceVadConfig::default(),
                )
                .unwrap();
                let tail = vec![0.4; tail_samples];
                let result = vad.finish_incremental_tail(&tail).unwrap();
                assert_eq!(result, if probability == 0.0 { Vec::new() } else { tail });
                assert!(vad.finish_incremental_tail(&[]).unwrap().is_empty());
                assert!(vad.frame_buffer.is_empty());
                // A later real frame cannot contain VAD-only padding or old prefill.
                let real = vec![0.5; VOICE_VAD_FRAME_SAMPLES];
                let _ = vad.push_incremental_frame(&real).unwrap();
                assert_eq!(vad.detector.frames.last().unwrap(), &real);
                assert_eq!(vad.frame_buffer.front().unwrap(), &real);
            }
        }
    }

    #[test]
    fn pause_edges_preserve_real_short_gaps_and_bound_long_quiet_history() {
        for quiet_frames in [0, 1, 14, 15, 20, 27, 28, 10000] {
            let probabilities = [0.9, 0.9]
                .into_iter()
                .chain(std::iter::repeat_n(0.0, quiet_frames))
                .chain([0.9, 0.9]);
            let mut vad =
                SmoothedVoiceVad::new(ScriptedVad::new(probabilities), VoiceVadConfig::default())
                    .unwrap();
            let mut emitted = Vec::new();
            let mut mapping = Vec::new();
            for index in 0..quiet_frames + 4 {
                let frame = vec![(index + 1) as f32; 512];
                if let SmoothedVadFrame::Speech(samples) =
                    vad.push_incremental_frame(&frame).unwrap()
                {
                    emitted.extend(samples);
                    mapping.extend(vad.take_emitted_intervals());
                }
                assert!(vad.frame_buffer.len() * 512 <= 7680);
                assert!(vad.recent_intervals.len() <= 15);
            }
            assert!(vad.finish_incremental_tail(&[]).unwrap().is_empty());
            let source: Vec<_> = (0..quiet_frames + 4)
                .flat_map(|ix| vec![(ix + 1) as f32; 512])
                .collect();
            let expected = if quiet_frames <= 27 {
                source.clone()
            } else {
                // Leading 448 ms hangover, trailing 416 ms real prefill.
                [&source[..16 * 512], &source[(quiet_frames - 11) * 512..]].concat()
            };
            assert_eq!(emitted, expected, "quiet frames: {quiet_frames}");
            let recovered: Vec<_> = mapping
                .iter()
                .flat_map(|interval| source[interval.start..interval.end].iter().copied())
                .collect();
            assert_eq!(emitted, recovered);
            assert!(mapping.windows(2).all(|pair| pair[0].end <= pair[1].start));
        }
    }

    #[test]
    fn hysteresis_and_quiet_clock_share_the_detector_decision_not_hangover() {
        let probabilities = [0.0, 0.31, 0.31, 0.25]
            .into_iter()
            .chain(std::iter::repeat_n(0.19, 15))
            .chain([0.25, 0.31, 0.31]);
        let mut vad =
            SmoothedVoiceVad::new(ScriptedVad::new(probabilities), VoiceVadConfig::default())
                .unwrap();
        let mut emitted = Vec::new();
        for index in 0..22 {
            if let SmoothedVadFrame::Speech(samples) = vad
                .push_incremental_frame(&vec![(index + 1) as f32; 512])
                .unwrap()
            {
                emitted.extend(samples);
            }
            if index == 3 {
                assert_eq!(vad.quiet_samples(), 0);
            }
            if index == 17 {
                assert_eq!(vad.quiet_samples(), 14 * 512);
            }
            if index == 19 {
                assert_eq!(vad.quiet_samples(), 16 * 512);
            }
        }
        assert_eq!(
            emitted,
            (1..=22)
                .flat_map(|ix| vec![ix as f32; 512])
                .collect::<Vec<_>>()
        );
        assert_eq!(vad.quiet_samples(), 0);
    }

    #[test]
    fn pure_quiet_and_noise_never_become_speech_even_at_commit() {
        let mut vad = SmoothedVoiceVad::new(
            ScriptedVad::new(std::iter::repeat_n(0.29, 1001)),
            VoiceVadConfig::default(),
        )
        .unwrap();
        for _ in 0..1000 {
            assert!(matches!(
                vad.push_incremental_frame(&[0.02; 512]).unwrap(),
                SmoothedVadFrame::Noise
            ));
            assert!(vad.frame_buffer.len() <= 15);
            assert!(vad.recent_intervals.len() <= 15);
        }
        assert!(
            vad.finish_incremental_tail(&[0.02; 137])
                .unwrap()
                .is_empty()
        );
        assert_eq!(vad.quiet_samples(), 512000 + 137);
    }
}
