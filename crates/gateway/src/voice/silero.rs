//! Mandatory gateway-only VAD resource. No model download or ASR selection dependency.
use super::vad::{VOICE_VAD_FRAME_SAMPLES, VOICE_VAD_SPEECH_THRESHOLD, VoiceActivityDetector};
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{fs, path::Path};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use transcribe_rs::vad::SileroVad;

const MODEL: &[u8] = include_bytes!("../../../../resources/voice/silero-v4/silero_vad.onnx");
const LICENSE: &str = include_str!("../../../../resources/voice/silero-v4/LICENSE");
const NOTICE: &str = include_str!("../../../../resources/voice/silero-v4/NOTICE.md");
const MODEL_SHA256: &str = "a35ebf52fd3ce5f1469b2a36158dba761bc47b973ea3382b3186ca15b1f5af28";
const MODEL_BYTES: u64 = 1_807_522;

// Session and recurrent h/c are owned by exactly one recording's blocking worker.
// Keep the private materialization alive until native Session has been dropped.
pub(super) struct SileroVoiceActivityDetector {
    detector: SileroVad,
    _resource: TempDir,
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
/// Each call creates a fresh SDK Session and zeroed h/c; pauses never reset it.
pub(super) fn create_voice_detector(
    cancellation: &CancellationToken,
) -> Result<SileroVoiceActivityDetector> {
    ensure_active(cancellation)?;
    // The SDK accepts a path only. A private temporary directory avoids mutable
    // shared files/state and works with the existing single-binary packaging on
    // every gateway target. These bytes and notices are embedded in that binary.
    let builder = tempfile::Builder::new();
    #[cfg(unix)]
    let builder = {
        let mut builder = builder;
        // Restrict access at creation, before verification and the SDK's reopen.
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

fn load_verified_detector(path: &Path, cancellation: &CancellationToken) -> Result<SileroVad> {
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
    let detector = SileroVad::new(path, VOICE_VAD_SPEECH_THRESHOLD)
        .context("failed to load required Silero v4 VAD")?;
    ensure_active(cancellation)?;
    Ok(detector)
}

/// Readiness requires the exact SDK/model interface to work, not merely a file
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
    use transcribe_rs::vad::Vad;

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
            .expect("readiness probe: actual 480-sample SDK/model contract");
        let mut first = create_voice_detector(&cancellation).unwrap();
        let mut second = create_voice_detector(&cancellation).unwrap();
        #[cfg(unix)]
        for detector in [&first, &second] {
            let permissions = fs::metadata(detector._resource.path())
                .unwrap()
                .permissions();
            assert_eq!(permissions.mode() & 0o077, 0);
        }
        // Raw pinned SDK instances are the independent recurrent-state oracle,
        // so an adapter that resets or shares state per frame cannot self-confirm.
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
        first.detector.reset();
        let mut fresh = create_voice_detector(&cancellation).unwrap();
        assert!(
            (first.speech_probability(&frames[0]).unwrap()
                - fresh.speech_probability(&frames[0]).unwrap())
            .abs()
                < 1e-6
        );
    }
}
