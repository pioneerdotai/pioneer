//! Binary mobile adapter. DSP belongs to the typed shared client resampler.
//! A runtime owns one exact-operation capture, from successful start to cancel/finalize.
use crate::ClientFfiVoiceAudioChunkParams;
use pioneer_client::{
    composer::store::ComposerOperationIdentity, voice::resampling::VoiceResampler,
};
use pioneer_protocol::{VoiceAudioEncoding, VoiceAudioFormat};
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VoiceCaptureAction {
    Process,
    Segment,
    Finish,
}

pub(super) fn audio_format() -> VoiceAudioFormat {
    VoiceAudioFormat {
        sample_rate_hz: 16_000,
        channels: 1,
        encoding: VoiceAudioEncoding::PcmS16Le,
    }
}

pub(super) struct MobileVoiceCapture {
    pub(super) operation: ComposerOperationIdentity,
    session_id: String,
    resampler: Option<VoiceResampler>,
    sequence: u64,
    captured_at: Option<u64>,
    finished: bool,
    failure: Option<String>,
}

impl MobileVoiceCapture {
    pub(super) fn new(operation: ComposerOperationIdentity, session_id: String) -> Self {
        Self {
            operation,
            session_id,
            resampler: None,
            sequence: 0,
            captured_at: None,
            finished: false,
            failure: None,
        }
    }
    pub(super) fn matches(&self, operation: &ComposerOperationIdentity, session_id: &str) -> bool {
        &self.operation == operation && self.session_id == session_id
    }
    pub(super) fn ensure_finished(&self) -> Result<(), String> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if !self.finished {
            return Err("Voice capture must finish before finalize".into());
        }
        Ok(())
    }
    pub(super) fn apply(
        &mut self,
        params: &ClientFfiVoiceAudioChunkParams,
        bytes: &[u8],
        mut send: impl FnMut(u64, Option<u64>, Vec<u8>) -> Result<(), String>,
    ) -> Result<(), String> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let result = self.apply_inner(params, bytes, &mut send);
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
    fn apply_inner(
        &mut self,
        params: &ClientFfiVoiceAudioChunkParams,
        bytes: &[u8],
        send: &mut impl FnMut(u64, Option<u64>, Vec<u8>) -> Result<(), String>,
    ) -> Result<(), String> {
        use VoiceCaptureAction::*;
        if params.action != Process {
            if !bytes.is_empty() {
                return Err("Voice capture control must not contain samples".into());
            }
            if self.finished {
                return Ok(());
            }
            self.finish_segment(send)?;
            self.finished = params.action == Finish;
            return Ok(());
        }
        if self.finished {
            return Err("Voice capture already finished".into());
        }
        if bytes.len() % 4 != 0 || bytes.len() > 4096 * 4 {
            return Err("Invalid voice f32 capture buffer size".into());
        }
        let rate = params
            .input_sample_rate_hz
            .ok_or("Missing actual voice sample rate")?;
        // Validate before finishing a prior format: invalid metadata cannot rename samples.
        let next = if self
            .resampler
            .as_ref()
            .is_none_or(|state| state.input_rate_hz() != rate)
        {
            Some(VoiceResampler::new(rate).map_err(str::to_owned)?)
        } else {
            None
        };
        let samples = bytes
            .chunks_exact(4)
            .map(|b| f32::from_ne_bytes([b[0], b[1], b[2], b[3]]))
            .collect::<Vec<_>>();
        if let Some(next) = next {
            // Input-format changes are physical boundaries, even without a route event.
            self.finish_segment(send)?;
            self.resampler = Some(next);
        }
        if samples.is_empty() {
            return Ok(());
        }
        self.captured_at = params.captured_at_unix_ms;
        let output = self
            .resampler
            .as_mut()
            .expect("validated resampler")
            .process(&samples)
            .map_err(str::to_owned)?;
        self.send(output, send)
    }
    fn finish_segment(
        &mut self,
        send: &mut impl FnMut(u64, Option<u64>, Vec<u8>) -> Result<(), String>,
    ) -> Result<(), String> {
        // Take before sending: failed or repeated flush cannot replay a tail.
        if let Some(mut resampler) = self.resampler.take() {
            self.send(resampler.finish(), send)?;
        }
        Ok(())
    }
    fn send(
        &mut self,
        samples: Vec<i16>,
        send: &mut impl FnMut(u64, Option<u64>, Vec<u8>) -> Result<(), String>,
    ) -> Result<(), String> {
        for chunk in samples.chunks(1600) {
            let pcm = chunk
                .iter()
                .flat_map(|sample| sample.to_le_bytes())
                .collect();
            send(self.sequence, self.captured_at, pcm)?;
            self.sequence = self
                .sequence
                .checked_add(1)
                .ok_or("Voice sequence overflow")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(action: VoiceCaptureAction, rate: Option<u32>) -> ClientFfiVoiceAudioChunkParams {
        ClientFfiVoiceAudioChunkParams {
            operation: ComposerOperationIdentity {
                thread_id: "thread".into(),
                draft_id: serde_json::from_value(serde_json::json!(1)).unwrap(),
                generation: 2,
            },
            session_id: "session".into(),
            action,
            input_sample_rate_hz: rate,
            captured_at_unix_ms: Some(123),
        }
    }
    fn capture() -> MobileVoiceCapture {
        let p = params(VoiceCaptureAction::Process, Some(16000));
        MobileVoiceCapture::new(p.operation, p.session_id)
    }
    fn bytes(samples: &[f32]) -> Vec<u8> {
        samples
            .iter()
            .flat_map(|sample| sample.to_ne_bytes())
            .collect()
    }

    #[test]
    fn binary_source_is_converted_and_split_directly_into_wire_packets() {
        let mut capture = capture();
        let p = params(VoiceCaptureAction::Process, Some(16000));
        let mut frames = Vec::new();
        capture
            .apply(&p, &bytes(&[1.0; 4096]), |sequence, captured_at, pcm| {
                frames.push(
                    pioneer_client::transport::ws::frames::encode_voice_audio_chunk_frame(
                        "session".into(),
                        sequence,
                        audio_format(),
                        captured_at,
                        None,
                        &pcm,
                    )
                    .map_err(|error| error.to_string())?,
                );
                Ok(())
            })
            .unwrap();
        let decoded = frames
            .iter()
            .map(|frame| pioneer_protocol::decode_voice_chunk_frame(frame).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            decoded
                .iter()
                .map(|frame| frame.header.sequence)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(
            decoded
                .iter()
                .map(|frame| frame.audio_payload.len())
                .collect::<Vec<_>>(),
            [3200, 3200, 1792]
        );
        assert!(
            decoded
                .iter()
                .all(|frame| frame.header.captured_at_unix_ms == Some(123))
        );
        assert!(decoded.iter().all(|frame| {
            frame
                .audio_payload
                .chunks_exact(2)
                .all(|sample| sample == [0xff, 0x7f])
        }));
    }

    #[test]
    fn route_and_format_boundaries_flush_once_and_continue_sequence_without_history() {
        let mut capture = capture();
        let mut output = Vec::new();
        let mut send = |sequence, _at, pcm: Vec<u8>| {
            output.push((sequence, pcm.len()));
            Ok(())
        };
        capture
            .apply(
                &params(VoiceCaptureAction::Process, Some(48000)),
                &bytes(&[0.5; 3]),
                &mut send,
            )
            .unwrap();
        capture
            .apply(&params(VoiceCaptureAction::Segment, None), &[], &mut send)
            .unwrap();
        capture
            .apply(&params(VoiceCaptureAction::Segment, None), &[], &mut send)
            .unwrap();
        capture
            .apply(
                &params(VoiceCaptureAction::Process, Some(48000)),
                &bytes(&[0.25; 3]),
                &mut send,
            )
            .unwrap();
        // Actual rate change without a route notification also drains the old segment.
        capture
            .apply(
                &params(VoiceCaptureAction::Process, Some(16000)),
                &bytes(&[1.0]),
                &mut send,
            )
            .unwrap();
        capture
            .apply(&params(VoiceCaptureAction::Finish, None), &[], &mut send)
            .unwrap();
        capture
            .apply(&params(VoiceCaptureAction::Finish, None), &[], &mut send)
            .unwrap();
        assert_eq!(output, [(0, 2), (1, 2), (2, 2)]);
        capture.ensure_finished().unwrap();
    }

    #[test]
    fn failed_tail_send_is_sticky_and_prevents_finalize_or_replay() {
        let mut capture = capture();
        capture
            .apply(
                &params(VoiceCaptureAction::Process, Some(48000)),
                &bytes(&[0.5; 3]),
                |_, _, _| panic!("short capture must wait for tail"),
            )
            .unwrap();
        assert!(capture.ensure_finished().is_err());
        let mut sends = 0;
        let mut fail = |_, _, _| {
            sends += 1;
            Err("tail rejected".to_owned())
        };
        assert_eq!(
            capture.apply(&params(VoiceCaptureAction::Finish, None), &[], &mut fail),
            Err("tail rejected".into())
        );
        assert_eq!(
            capture.apply(&params(VoiceCaptureAction::Finish, None), &[], &mut fail),
            Err("tail rejected".into())
        );
        assert_eq!(sends, 1);
        assert_eq!(capture.ensure_finished(), Err("tail rejected".into()));
        let runtime = crate::ClientFfiRuntime::default();
        let request = serde_json::json!({ "operation": capture.operation }).to_string();
        *runtime.voice_capture.lock().unwrap() = Some(capture);
        assert_eq!(
            runtime.voice_session_finalize(&request),
            Err("tail rejected".into())
        );
        assert!(runtime.voice_capture.lock().unwrap().is_some());
    }

    #[test]
    fn exact_session_cancel_discards_state_without_retiring_a_new_capture() {
        let runtime = crate::ClientFfiRuntime::default();
        let p = params(VoiceCaptureAction::Process, Some(48000));
        let mut capture = capture();
        capture
            .apply(&p, &bytes(&[0.5; 3]), |_, _, _| {
                panic!("pending samples must wait")
            })
            .unwrap();
        *runtime.voice_capture.lock().unwrap() = Some(capture);
        let mut stale = p.operation.clone();
        stale.generation += 1;
        runtime
            .voice_session_cancel(
                &serde_json::json!({
                    "operation": stale, "session_id": "session", "reason": null
                })
                .to_string(),
            )
            .unwrap();
        assert!(
            runtime
                .voice_capture
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .matches(&p.operation, "session")
        );
        runtime
            .voice_session_cancel(
                &serde_json::json!({
                    "operation": p.operation, "session_id": "other-session", "reason": null
                })
                .to_string(),
            )
            .unwrap();
        assert!(runtime.voice_capture.lock().unwrap().is_some());
        let cancel = serde_json::json!({
            "operation": p.operation, "session_id": "session", "reason": null
        })
        .to_string();
        runtime.voice_session_cancel(&cancel).unwrap();
        runtime.voice_session_cancel(&cancel).unwrap();
        assert!(runtime.voice_capture.lock().unwrap().is_none());
    }

    #[test]
    fn invalid_binary_format_fails_capture_without_claiming_identity_rate() {
        for (rate, payload) in [
            (Some(0), bytes(&[0.5])),
            (None, bytes(&[0.5])),
            (Some(16000), vec![0, 1, 2]),
            (Some(16000), vec![0; 4097 * 4]),
        ] {
            let mut capture = capture();
            assert!(
                capture
                    .apply(
                        &params(VoiceCaptureAction::Process, rate),
                        &payload,
                        |_, _, _| panic!("invalid input must not send")
                    )
                    .is_err()
            );
            assert!(
                capture
                    .apply(
                        &params(VoiceCaptureAction::Finish, None),
                        &[],
                        |_, _, _| panic!("failed capture must not flush")
                    )
                    .is_err()
            );
        }
    }
}
