//! Streaming mono capture conversion to the voice wire rate, independent of any shell.
//!
//! A Blackman-windowed sinc with a 7.2 kHz cutoff (at downsampling rates) leaves
//! a transition band below the 8 kHz output Nyquist frequency. The kernel spans
//! 48 output periods on either side. A 256-phase table with linear interpolation
//! avoids trigonometry in the sample loop. No external DSP dependency is needed.
//!
//! Output sample m represents input time m * input_rate / 16000. Integer counters
//! determine both this position and the final length floor(N * 16000 / input_rate),
//! independent of callback boundaries. Non-identity conversion waits for right
//! context; finish supplies virtual zero context only for those pending samples.
//! It never emits the filter's ring-out beyond the recorded duration. Start context
//! is also zero. At 16 kHz, conversion is immediate and unfiltered.
//!
//! Retained input is at most two kernel radii plus one input step; neither process
//! nor finish retains a callback buffer. A physical input discontinuity requires
//! finishing (or discarding) this object and constructing a new one. Callers own
//! chunking, byte encoding, timestamps, callback barriers and transport errors.

use std::collections::VecDeque;
use std::f64::consts::PI;

const OUTPUT_RATE: u32 = 16_000;
const PHASES: usize = 256;
const HALF_WIDTH: f64 = 48.0;

/// One continuous mono capture stream. Shared by typed desktop and mobile FFI.
pub struct VoiceResampler {
    input_rate_hz: u32,
    radius: usize,
    kernels: Vec<f32>,
    history: VecDeque<f32>,
    history_start: u64,
    input_count: u64,
    output_count: u64,
    finished: bool,
}

impl VoiceResampler {
    /// Rates outside the capture range are rejected, never relabeled as 16 kHz.
    pub fn new(input_rate_hz: u32) -> Result<Self, &'static str> {
        if !(8_000..=384_000).contains(&input_rate_hz) {
            return Err("unsupported capture sample rate");
        }
        let ratio = f64::from(input_rate_hz) / f64::from(OUTPUT_RATE);
        let radius = (HALF_WIDTH * ratio.max(1.0)).ceil() as usize;
        let mut kernels = Vec::new();
        if input_rate_hz != OUTPUT_RATE {
            let cutoff = 0.45 / ratio.max(1.0);
            kernels.reserve((PHASES + 1) * (2 * radius + 1));
            for phase in 0..=PHASES {
                let fraction = phase as f64 / PHASES as f64;
                let mut weights = Vec::with_capacity(2 * radius + 1);
                let mut sum = 0.0;
                for tap in 0..=2 * radius {
                    let distance = tap as f64 - radius as f64 - fraction;
                    let normalized = distance / radius as f64;
                    let weight = if normalized.abs() >= 1.0 {
                        0.0
                    } else {
                        let x = 2.0 * cutoff * distance;
                        let sinc = if x.abs() < 1e-12 {
                            1.0
                        } else {
                            (PI * x).sin() / (PI * x)
                        };
                        let window = 0.42
                            + 0.5 * (PI * normalized).cos()
                            + 0.08 * (2.0 * PI * normalized).cos();
                        2.0 * cutoff * sinc * window
                    };
                    sum += weight;
                    weights.push(weight);
                }
                kernels.extend(weights.into_iter().map(|weight| (weight / sum) as f32));
            }
        }
        Ok(Self {
            input_rate_hz,
            radius,
            kernels,
            history: VecDeque::with_capacity(2 * radius + 2),
            history_start: 0,
            input_count: 0,
            output_count: 0,
            finished: false,
        })
    }

    pub fn input_rate_hz(&self) -> u32 {
        self.input_rate_hz
    }

    /// Consumes borrowed mono float samples synchronously. Nonfinite samples are
    /// silence, and clipping/PCM quantization is identical on both shells.
    pub fn process(&mut self, samples: &[f32]) -> Result<Vec<i16>, &'static str> {
        if self.finished {
            return Err("capture resampler already finished");
        }
        let new_count = self
            .input_count
            .checked_add(samples.len() as u64)
            .ok_or("capture sample count overflow")?;
        // This also bounds the u64 output counter for all accepted input rates.
        if u128::from(new_count) * u128::from(OUTPUT_RATE) / u128::from(self.input_rate_hz)
            > u128::from(u64::MAX)
        {
            return Err("capture sample count overflow");
        }
        if self.input_rate_hz == OUTPUT_RATE {
            self.input_count = new_count;
            self.output_count = new_count;
            return Ok(samples.iter().copied().map(to_pcm).collect());
        }
        let mut output = Vec::new();
        for &sample in samples {
            self.history
                .push_back(if sample.is_finite() { sample } else { 0.0 });
            self.input_count += 1;
            while self.output_count < self.target_output_count() {
                let (center, _) = self.position();
                if u128::from(center) + self.radius as u128 >= u128::from(self.input_count) {
                    break;
                }
                output.push(self.next_sample());
            }
            self.discard_old_context();
        }
        Ok(output)
    }

    /// Emits only pending real-duration samples once. Drop/cancel simply discard
    /// the object instead. Calling process after finish returns an error.
    pub fn finish(&mut self) -> Vec<i16> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        let mut output = Vec::new();
        while self.output_count < self.target_output_count() {
            output.push(self.next_sample());
            self.discard_old_context();
        }
        self.history.clear();
        output
    }

    fn target_output_count(&self) -> u64 {
        (u128::from(self.input_count) * u128::from(OUTPUT_RATE) / u128::from(self.input_rate_hz))
            as u64
    }

    fn position(&self) -> (u64, u32) {
        let position = u128::from(self.output_count) * u128::from(self.input_rate_hz);
        (
            (position / u128::from(OUTPUT_RATE)) as u64,
            (position % u128::from(OUTPUT_RATE)) as u32,
        )
    }

    fn next_sample(&mut self) -> i16 {
        let (center, remainder) = self.position();
        let phase = f64::from(remainder) * PHASES as f64 / f64::from(OUTPUT_RATE);
        let lower = phase.floor() as usize;
        let fraction = phase - lower as f64;
        let width = 2 * self.radius + 1;
        let mut sum = 0.0;
        for tap in 0..width {
            let index = i128::from(center) + tap as i128 - self.radius as i128;
            if index < i128::from(self.history_start) || index >= i128::from(self.input_count) {
                continue;
            }
            let sample = self.history[(index - i128::from(self.history_start)) as usize];
            let a = f64::from(self.kernels[lower * width + tap]);
            let b = f64::from(self.kernels[(lower + 1) * width + tap]);
            sum += f64::from(sample) * (a + fraction * (b - a));
        }
        self.output_count += 1;
        to_pcm(sum as f32)
    }

    fn discard_old_context(&mut self) {
        let (center, _) = self.position();
        let keep_from = center.saturating_sub(self.radius as u64);
        while self.history_start < keep_from && !self.history.is_empty() {
            self.history.pop_front();
            self.history_start += 1;
        }
    }
}

fn to_pcm(sample: f32) -> i16 {
    let sample = if sample.is_finite() {
        sample.clamp(-1.0, 1.0)
    } else {
        0.0
    };
    if sample < 0.0 {
        (sample * 32768.0).round() as i16
    } else {
        (sample * 32767.0).round() as i16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tone(rate: u32, count: usize, hz: f64) -> Vec<f32> {
        (0..count)
            .map(|i| (0.5 * (2.0 * PI * hz * i as f64 / f64::from(rate)).sin()) as f32)
            .collect()
    }

    fn convert(rate: u32, input: &[f32], blocks: &[usize]) -> (Vec<i16>, usize) {
        let mut converter = VoiceResampler::new(rate).unwrap();
        let mut result = Vec::new();
        let mut offset = 0;
        let mut block = 0;
        while offset < input.len() {
            let end = (offset + blocks[block % blocks.len()]).min(input.len());
            result.extend(converter.process(&input[offset..end]).unwrap());
            assert!(converter.history.len() <= 2 * converter.radius + 2);
            offset = end;
            block += 1;
        }
        let tail = converter.finish();
        let tail_count = tail.len();
        result.extend(tail);
        assert!(converter.finish().is_empty());
        assert!(converter.process(&[]).is_err());
        (result, tail_count)
    }

    #[test]
    fn callback_partition_does_not_change_signal_length_or_tail() {
        for rate in [44_100, 48_000] {
            // A duration deliberately not aligned to callback or rate conversion periods.
            let input: Vec<_> = (0..rate as usize + 137)
                .map(|i| {
                    let time = i as f64 / f64::from(rate);
                    (0.25 * (2.0 * PI * 997.0 * time).sin()
                        + 0.2 * (2.0 * PI * 5_321.0 * time).cos()) as f32
                })
                .collect();
            let (whole, tail) = convert(rate, &input, &[input.len()]);
            let (split, split_tail) = convert(rate, &input, &[1, 73, 509, 2, 997, 31]);
            assert_eq!(whole.len(), input.len() * 16_000 / rate as usize);
            assert_eq!(split, whole); // Same arithmetic order, bit-exact on one target.
            assert_eq!(split_tail, tail);
            assert!((1..=50).contains(&tail));
        }
    }

    #[test]
    fn empty_short_identity_and_finished_streams_account_only_real_samples() {
        for rate in [8_000, 16_000, 44_100, 48_000, 384_000] {
            for count in [0, 1, 2, 3, 17, 53] {
                let input = vec![0.25; count];
                let (output, _) = convert(rate, &input, &[1, 7]);
                assert_eq!(output.len(), count * 16_000 / rate as usize);
            }
        }
        let mut identity = VoiceResampler::new(16_000).unwrap();
        assert_eq!(
            identity
                .process(&[-2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, f32::NAN])
                .unwrap(),
            [-32768, -32768, -16384, 0, 16384, 32767, 32767, 0]
        );
        assert!(identity.finish().is_empty());
        for rate in [0, 7_999, 384_001, u32::MAX] {
            assert!(VoiceResampler::new(rate).is_err());
        }
    }

    #[test]
    fn finish_preserves_the_recorded_end_signal_without_extra_ring_out() {
        for rate in [44_100, 48_000] {
            let mut converter = VoiceResampler::new(rate).unwrap();
            // A constant signal has a known DC value through the interior of
            // the delayed tail. Keep eight samples from the zero-extended edge.
            let input = vec![0.5; rate as usize / 10];
            let streamed = converter.process(&input).unwrap();
            let tail = converter.finish();
            assert_eq!(streamed.len() + tail.len(), 1_600);
            assert!(tail.len() >= 40);
            for &sample in &tail[..tail.len() - 8] {
                // Conservative 3% bound allows the finite sinc's end transient;
                // replacing the pending tail with silence must fail independently.
                assert!((i32::from(sample) - 16_384).abs() < 500);
            }
            assert!(converter.finish().is_empty());
        }
    }

    #[test]
    fn useful_band_matches_independent_analytic_tones() {
        for rate in [44_100, 48_000] {
            for hz in [300.0, 1_000.0, 6_000.0] {
                let (output, _) = convert(rate, &tone(rate, rate as usize, hz), &[137, 521]);
                // Exclude zero-extension transients. Analytic reference, not a second
                // invocation of the resampler: <2% RMS error relative to amplitude.
                let mse = output[200..15_800]
                    .iter()
                    .enumerate()
                    .map(|(i, &pcm)| {
                        let expected = 0.5 * (2.0 * PI * hz * (i + 200) as f64 / 16_000.0).sin();
                        (f64::from(pcm) / 32768.0 - expected).powi(2)
                    })
                    .sum::<f64>()
                    / 15_600.0;
                assert!(
                    mse.sqrt() < 0.01,
                    "rate={rate}, hz={hz}, rms={}",
                    mse.sqrt()
                );
            }
        }
    }

    #[test]
    fn independent_out_of_band_tones_do_not_fold_into_speech() {
        for rate in [44_100, 48_000] {
            for hz in [8_500.0, 10_000.0, 13_000.0, 19_000.0] {
                let (output, _) = convert(rate, &tone(rate, rate as usize, hz), &[97, 1, 811]);
                let rms = (output[200..15_800]
                    .iter()
                    .map(|&pcm| (f64::from(pcm) / 32768.0).powi(2))
                    .sum::<f64>()
                    / 15_600.0)
                    .sqrt();
                // At least 43 dB below the input's 0.3536 RMS. Quantization and
                // phase-table interpolation are included in this conservative bound.
                assert!(rms < 0.0025, "rate={rate}, hz={hz}, rms={rms}");
            }
        }
    }
}
