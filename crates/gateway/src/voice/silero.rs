//! Mandatory gateway-only VAD resource. No model download or ASR selection dependency.
use super::vad::{VOICE_VAD_FRAME_SAMPLES, VoiceActivityDetector};
use anyhow::{Context, Result, bail};
use ort::{
    session::{Session, builder::GraphOptimizationLevel},
    value::TensorRef,
};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{fs, path::Path};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const MODEL: &[u8] = include_bytes!("../../../../resources/voice/silero-v4/silero_vad.onnx");
const LICENSE: &str = include_str!("../../../../resources/voice/silero-v4/LICENSE");
const NOTICE: &str = include_str!("../../../../resources/voice/silero-v4/NOTICE.md");
const MODEL_SHA256: &str = "a35ebf52fd3ce5f1469b2a36158dba761bc47b973ea3382b3186ca15b1f5af28";
const MODEL_BYTES: u64 = 1_807_522;

// Session and recurrent h/c are owned by exactly one recording's blocking worker.
// Keep the private materialization alive until native Session has been dropped.
pub(super) struct SileroVoiceActivityDetector {
    detector: SileroV4Session,
    _resource: TempDir,
}

// Local adapter for the pinned v4 graph, independent of the ASR SDK's 480
// sample VAD wrapper. v4 utils_vad.py accepts >=512 samples at 16 kHz and uses
// 512 by default. State is never reset on silence or at a packet boundary.
struct SileroV4Session {
    session: Session,
    h: [f32; 128],
    c: [f32; 128],
}

impl SileroV4Session {
    fn new(path: &Path) -> Result<Self> {
        let session = Session::builder()?
            // Drop the recoverable builder before converting errors to anyhow.
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(ort::Error::<()>::from)?
            .with_intra_threads(1)
            .map_err(ort::Error::<()>::from)?
            .with_inter_threads(1)
            .map_err(ort::Error::<()>::from)?
            .commit_from_file(path)?;
        Ok(Self {
            session,
            h: [0.0; 128],
            c: [0.0; 128],
        })
    }

    fn speech_probability(&mut self, frame: &[f32]) -> Result<f32> {
        if frame.len() != VOICE_VAD_FRAME_SAMPLES {
            bail!("Silero v4 requires exactly 512 samples at 16 kHz");
        }
        let sr = [16000i64];
        let outputs = self.session.run(ort::inputs![
            "input" => TensorRef::from_array_view(([1usize, VOICE_VAD_FRAME_SAMPLES], frame))?,
            "sr" => TensorRef::from_array_view(([] as [usize; 0], &sr[..]))?,
            "h" => TensorRef::from_array_view(([2usize, 1, 64], &self.h[..]))?,
            "c" => TensorRef::from_array_view(([2usize, 1, 64], &self.c[..]))?,
        ])?;
        let (shape, probability) = outputs
            .get("output")
            .context("Silero v4 missing output")?
            .try_extract_tensor::<f32>()?;
        if &shape[..] != [1, 1]
            || probability.len() != 1
            || !probability[0].is_finite()
            || !(0.0..=1.0).contains(&probability[0])
        {
            bail!("Silero v4 returned invalid probability tensor");
        }
        let (h_shape, h) = outputs
            .get("hn")
            .context("Silero v4 missing hn")?
            .try_extract_tensor::<f32>()?;
        let (c_shape, c) = outputs
            .get("cn")
            .context("Silero v4 missing cn")?
            .try_extract_tensor::<f32>()?;
        if &h_shape[..] != [2, 1, 64]
            || &c_shape[..] != [2, 1, 64]
            || h.len() != 128
            || c.len() != 128
            || h.iter().chain(c).any(|value| !value.is_finite())
        {
            bail!("Silero v4 returned invalid recurrent state");
        }
        self.h.copy_from_slice(h);
        self.c.copy_from_slice(c);
        Ok(probability[0])
    }
}

impl VoiceActivityDetector for SileroVoiceActivityDetector {
    fn speech_probability(&mut self, frame: &[f32]) -> Result<f32> {
        let probability = self.detector.speech_probability(frame)?;
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            bail!("Silero VAD returned an invalid probability");
        }
        Ok(probability)
    }
}

/// Only call from a blocking lifecycle/worker, outside supervisor/store locks.
/// Each call creates a fresh ONNX Session and zeroed h/c; pauses never reset it.
pub(super) fn create_voice_detector(
    cancellation: &CancellationToken,
) -> Result<SileroVoiceActivityDetector> {
    ensure_active(cancellation)?;
    // The ONNX loader opens a path. A private temporary directory avoids mutable
    // shared files/state and works with the existing single-binary packaging on
    // every gateway target. These bytes and notices are embedded in that binary.
    let builder = tempfile::Builder::new();
    #[cfg(unix)]
    let builder = {
        let mut builder = builder;
        // Restrict access at creation, before verification and the loader's reopen.
        builder.permissions(fs::Permissions::from_mode(0o700));
        builder
    };
    let resource = builder
        .tempdir()
        .context("failed to stage bundled Silero VAD")?;
    let path = resource.path().join("silero_vad.onnx");
    fs::write(&path, MODEL).context("failed to write bundled Silero VAD")?;
    fs::write(resource.path().join("LICENSE"), LICENSE)
        .context("failed to write Silero VAD license")?;
    fs::write(resource.path().join("NOTICE.md"), NOTICE)
        .context("failed to write Silero VAD notice")?;
    let detector = load_verified_detector(&path, cancellation)?;
    Ok(SileroVoiceActivityDetector {
        detector,
        _resource: resource,
    })
}

fn load_verified_detector(
    path: &Path,
    cancellation: &CancellationToken,
) -> Result<SileroV4Session> {
    ensure_active(cancellation)?;
    if fs::metadata(path)
        .context("required Silero VAD asset is unavailable")?
        .len()
        != MODEL_BYTES
    {
        bail!("required Silero VAD asset has an invalid size");
    }
    let bytes = fs::read(path).context("failed to read required Silero VAD asset")?;
    if hex::encode(Sha256::digest(&bytes)) != MODEL_SHA256 {
        bail!("required Silero VAD asset failed SHA256 verification");
    }
    ensure_active(cancellation)?;
    let detector = SileroV4Session::new(path).context("failed to load required Silero v4 VAD")?;
    ensure_active(cancellation)?;
    Ok(detector)
}

/// Readiness requires the exact adapter/model interface to work, not merely a file
/// or successful ASR load. The probe is discarded, never shared with recordings.
pub(super) fn prepare_voice_vad(cancellation: &CancellationToken) -> Result<()> {
    let mut detector = create_voice_detector(cancellation)?;
    ensure_active(cancellation)?;
    detector.speech_probability(&[0.0; VOICE_VAD_FRAME_SAMPLES])?;
    ensure_active(cancellation)?;
    Ok(())
}

fn ensure_active(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        bail!("Silero VAD preparation cancelled");
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn check_asset_failure_for_test(path: &Path) -> Result<()> {
    load_verified_detector(path, &CancellationToken::new()).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_truncated_and_same_size_corrupt_asset_fail_before_native_load() {
        let resource = tempfile::tempdir().unwrap();
        let path = resource.path().join("silero_vad.onnx");
        let cancellation = CancellationToken::new();
        let error = load_verified_detector(&path, &cancellation)
            .err()
            .expect("missing asset");
        assert!(error.to_string().contains("unavailable"));
        fs::write(&path, b"invalid ONNX").unwrap();
        let error = load_verified_detector(&path, &cancellation)
            .err()
            .expect("truncated asset");
        assert!(error.to_string().contains("invalid size"));
        let mut corrupt = MODEL.to_vec();
        let middle = corrupt.len() / 2;
        corrupt[middle] ^= 1;
        fs::write(&path, corrupt).unwrap();
        let error = load_verified_detector(&path, &cancellation)
            .err()
            .expect("corrupt asset");
        assert!(error.to_string().contains("SHA256"));
    }

    #[test]
    fn cancelled_preparation_never_stages_or_enters_native_runtime() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = prepare_voice_vad(&cancellation).unwrap_err();
        assert!(error.to_string().contains("cancelled"));
    }

    #[test]
    #[ignore = "external acceptance only: real Silero load/inference, never run during implementation"]
    fn silero_v4_bundled_load_inference_and_recording_state_isolation_smoke() {
        let cancellation = CancellationToken::new();
        prepare_voice_vad(&cancellation)
            .expect("readiness probe: actual 512-sample adapter/model contract");
        let mut first = create_voice_detector(&cancellation).unwrap();
        let mut second = create_voice_detector(&cancellation).unwrap();
        #[cfg(unix)]
        for detector in [&first, &second] {
            let permissions = fs::metadata(detector._resource.path())
                .unwrap()
                .permissions();
            assert_eq!(permissions.mode() & 0o077, 0);
        }
        // Separately loaded sessions check recording isolation. This is not an
        // independent v4 probability/state oracle or a speech-quality test.
        let mut first_reference = load_verified_detector(
            &first._resource.path().join("silero_vad.onnx"),
            &cancellation,
        )
        .unwrap();
        let mut second_reference = load_verified_detector(
            &second._resource.path().join("silero_vad.onnx"),
            &cancellation,
        )
        .unwrap();
        let frames: Vec<Vec<f32>> = (0..24)
            .map(|frame| {
                (0..VOICE_VAD_FRAME_SAMPLES)
                    .map(|sample| {
                        if (8..16).contains(&frame) {
                            0.0
                        } else {
                            ((frame * VOICE_VAD_FRAME_SAMPLES + sample) as f32 * 0.07).sin() * 0.2
                        }
                    })
                    .collect()
            })
            .collect();
        let silence = [0.0; VOICE_VAD_FRAME_SAMPLES];
        let first_expected: Vec<_> = frames
            .iter()
            .map(|frame| first_reference.speech_probability(frame).unwrap())
            .collect();
        let second_expected: Vec<_> = frames
            .iter()
            .map(|_| second_reference.speech_probability(&silence).unwrap())
            .collect();
        // Interleave two unrelated recordings, including ordinary pauses in A.
        // Compare with each recording processed alone, without resetting per frame.
        for (index, frame) in frames.iter().enumerate() {
            assert!(
                (first.speech_probability(frame).unwrap() - first_expected[index]).abs() < 1e-6
            );
            assert!(
                (second.speech_probability(&silence).unwrap() - second_expected[index]).abs()
                    < 1e-6
            );
        }
        first.detector.h.fill(0.0);
        first.detector.c.fill(0.0);
        let mut fresh = create_voice_detector(&cancellation).unwrap();
        assert!(
            (first.speech_probability(&frames[0]).unwrap()
                - fresh.speech_probability(&frames[0]).unwrap())
            .abs()
                < 1e-6
        );
    }
}
