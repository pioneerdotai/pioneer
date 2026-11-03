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
    Status,
}

pub(super) fn audio_format() -> VoiceAudioFormat {
    VoiceAudioFormat {
        sample_rate_hz: 16_000,
        channels: 1,
        encoding: VoiceAudioEncoding::PcmS16Le,
    }
}

pub(super) struct CaptureDsp {
    pub(super) operation: ComposerOperationIdentity,
    session_id: String,
    resampler: Option<VoiceResampler>,
    sequence: u64,
    captured_at: Option<u64>,
    finished: bool,
    failure: Option<String>,
}

impl CaptureDsp {
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
        if params.action == Status {
            return self.ensure_finished();
        }
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

// Source f32 bytes, including the item currently processed: 256 KiB. Controls
// have a separate item bound so repeated route events cannot allocate forever.
const MAX_PENDING_BYTES: usize = 65_536 * 4;
const MAX_PENDING_ITEMS: usize = 64;

struct WorkerStatus {
    failure: Option<String>,
    drained: bool,
}

pub(super) struct MobileVoiceCapture {
    pub(super) operation: ComposerOperationIdentity,
    session_id: String,
    sender: pioneer_client::voice::capture_worker::CaptureQueueSender<(
        ClientFfiVoiceAudioChunkParams,
        Vec<u8>,
    )>,
    status: std::sync::Arc<std::sync::Mutex<WorkerStatus>>,
    finishing: bool,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl MobileVoiceCapture {
    pub(super) fn new(
        operation: ComposerOperationIdentity,
        session_id: String,
        core: std::sync::Arc<pioneer_client::core::ClientCore>,
    ) -> Result<Self, String> {
        let owner = operation.clone();
        let session = session_id.clone();
        Self::from_dsp(
            CaptureDsp::new(operation, session_id),
            move |sequence, at, pcm| {
                core.send_composer_voice_audio_chunk(
                    &owner,
                    session.clone(),
                    sequence,
                    audio_format(),
                    at,
                    Some((((pcm.len() / 2 * 1000 + 8000) / 16000) as u32).max(1)),
                    pcm,
                )
                .map_err(|error| format!("{error:#}"))
            },
        )
    }

    fn from_dsp(
        mut dsp: CaptureDsp,
        mut send: impl FnMut(u64, Option<u64>, Vec<u8>) -> Result<(), String> + Send + 'static,
    ) -> Result<Self, String> {
        use std::sync::{Arc, Mutex};
        let (sender, receiver) = pioneer_client::voice::capture_worker::capture_queue::<(
            ClientFfiVoiceAudioChunkParams,
            Vec<u8>,
        )>(MAX_PENDING_ITEMS, MAX_PENDING_BYTES);
        let status = Arc::new(Mutex::new(WorkerStatus {
            failure: dsp.failure.clone(),
            drained: false,
        }));
        let worker_admission = sender.clone();
        let worker_status = status.clone();
        let operation = dsp.operation.clone();
        let session_id = dsp.session_id.clone();
        let worker = std::thread::Builder::new()
            .name("mobile-voice-capture".into())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    while let Some(item) = receiver.recv() {
                        if worker_admission.is_cancelled() || worker_admission.has_failed() {
                            break;
                        }
                        if worker_status
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .failure
                            .is_some()
                        {
                            break;
                        }
                        // Never hold status/registry mutex during DSP or transport.
                        let (params, bytes) = item.value();
                        let finish = params.action == VoiceCaptureAction::Finish;
                        let result = dsp.apply(params, bytes, &mut send);
                        drop(item);
                        let mut state = worker_status.lock().unwrap_or_else(|e| e.into_inner());
                        if let Err(error) = result {
                            state.failure.get_or_insert(error);
                            break;
                        }
                        if finish {
                            state.drained = true;
                            break;
                        }
                    }
                }));
                if outcome.is_err() {
                    worker_status
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .failure
                        .get_or_insert_with(|| "Voice capture worker failed".into());
                }
            })
            .map_err(|_| "Cannot start voice capture worker".to_owned())?;
        Ok(Self {
            operation,
            session_id,
            sender,
            status,
            finishing: false,
            worker: Some(worker),
        })
    }

    pub(super) fn matches(&self, operation: &ComposerOperationIdentity, session_id: &str) -> bool {
        &self.operation == operation && self.session_id == session_id
    }

    pub(super) fn ensure_finished(&self) -> Result<(), String> {
        let status = self
            .status
            .lock()
            .map_err(|_| "Voice capture status poisoned")?;
        if let Some(error) = &status.failure {
            return Err(error.clone());
        }
        if !status.drained
            || self
                .worker
                .as_ref()
                .is_some_and(|worker| !worker.is_finished())
        {
            return Err("Voice capture must drain before finalize".into());
        }
        Ok(())
    }

    fn fail(&self, error: &str) -> String {
        let mut status = self.status.lock().unwrap_or_else(|e| e.into_inner());
        self.sender.fail();
        self.sender.close();
        status.failure.get_or_insert_with(|| error.into()).clone()
    }

    pub(super) fn enqueue(
        &mut self,
        params: ClientFfiVoiceAudioChunkParams,
        bytes: &[u8],
    ) -> Result<bool, String> {
        {
            let state = self
                .status
                .lock()
                .map_err(|_| "Voice capture status poisoned")?;
            if let Some(error) = &state.failure {
                return Err(error.clone());
            }
            if params.action == VoiceCaptureAction::Status {
                if !bytes.is_empty() {
                    drop(state);
                    return Err(self.fail("Voice status must not contain samples"));
                }
                return Ok(state.drained
                    && self
                        .worker
                        .as_ref()
                        .is_none_or(|worker| worker.is_finished()));
            }
        }
        if self.finishing {
            if params.action == VoiceCaptureAction::Finish && bytes.is_empty() {
                return Ok(false);
            }
            return Err("Voice capture admission closed".into());
        }
        if params.action == VoiceCaptureAction::Process {
            if bytes.len() % 4 != 0
                || bytes.len() > 4096 * 4
                || params
                    .input_sample_rate_hz
                    .is_none_or(|rate| !(8_000..=384_000).contains(&rate))
            {
                return Err(self.fail("Invalid voice f32 capture buffer"));
            }
        } else if !bytes.is_empty() {
            return Err(self.fail("Voice capture control must not contain samples"));
        }
        let finish = params.action == VoiceCaptureAction::Finish;
        // Shared policy reserves bytes before copying and includes in-flight work.
        if let Err(error) = self
            .sender
            .try_push_with(bytes.len(), || (params, bytes.to_vec()))
        {
            let message = match error {
                pioneer_client::voice::capture_worker::CaptureQueueError::Overflow => {
                    "Voice capture queue capacity exceeded"
                }
                _ => "Voice capture worker queue unavailable",
            };
            return Err(self.fail(message));
        }
        if finish {
            self.sender.close();
        }
        self.finishing |= finish;
        Ok(false)
    }
}

impl Drop for MobileVoiceCapture {
    fn drop(&mut self) {
        self.sender.cancel();
        // No join while the registry mutex is held. The owned worker observes
        // cancellation before the next item; core also fences the exact operation.
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
    fn capture() -> CaptureDsp {
        let p = params(VoiceCaptureAction::Process, Some(16000));
        CaptureDsp::new(p.operation, p.session_id)
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
        *runtime.voice_capture.lock().unwrap() =
            Some(MobileVoiceCapture::from_dsp(capture, |_, _, _| Ok(())).unwrap());
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
        *runtime.voice_capture.lock().unwrap() =
            Some(MobileVoiceCapture::from_dsp(capture, |_, _, _| Ok(())).unwrap());
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
    #[test]
    fn worker_owns_reused_input_and_orders_backlog_segment_and_final_tail() {
        use std::sync::mpsc;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (pcm_tx, pcm_rx) = mpsc::channel();
        let mut worker = MobileVoiceCapture::from_dsp(capture(), move |sequence, _, pcm| {
            if sequence == 0 {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }
            pcm_tx.send((sequence, pcm)).unwrap();
            Ok(())
        })
        .unwrap();
        let mut source = bytes(&[1.0]);
        worker
            .enqueue(params(VoiceCaptureAction::Process, Some(16000)), &source)
            .unwrap();
        entered_rx.recv().unwrap();
        source.fill(0);
        worker
            .enqueue(params(VoiceCaptureAction::Segment, None), &[])
            .unwrap();
        worker
            .enqueue(
                params(VoiceCaptureAction::Process, Some(48000)),
                &bytes(&[0.0; 3]),
            )
            .unwrap();
        worker
            .enqueue(params(VoiceCaptureAction::Finish, None), &[])
            .unwrap();
        assert!(worker.ensure_finished().is_err());
        release_tx.send(()).unwrap();
        worker.worker.take().unwrap().join().unwrap();
        worker.ensure_finished().unwrap();
        let output = pcm_rx.try_iter().collect::<Vec<_>>();
        assert_eq!(output, [(0, vec![255, 127]), (1, vec![0, 0])]);
        // Repeated Finish never replays the tail.
        worker
            .enqueue(params(VoiceCaptureAction::Finish, None), &[])
            .unwrap();
        assert!(pcm_rx.try_recv().is_err());
    }

    #[test]
    fn source_budget_includes_inflight_samples_and_overflow_is_sticky() {
        use std::sync::mpsc;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mut first = true;
        let mut worker = MobileVoiceCapture::from_dsp(capture(), move |_, _, _| {
            if first {
                first = false;
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }
            Ok(())
        })
        .unwrap();
        let source = bytes(&[0.0; 4096]);
        worker
            .enqueue(params(VoiceCaptureAction::Process, Some(16000)), &source)
            .unwrap();
        entered_rx.recv().unwrap();
        for _ in 0..15 {
            worker
                .enqueue(params(VoiceCaptureAction::Process, Some(16000)), &source)
                .unwrap();
        }
        assert_eq!(
            worker.enqueue(params(VoiceCaptureAction::Process, Some(16000)), &source),
            Err("Voice capture queue capacity exceeded".into())
        );
        release_tx.send(()).unwrap();
        worker.worker.take().unwrap().join().unwrap();
        assert_eq!(
            worker.ensure_finished(),
            Err("Voice capture queue capacity exceeded".into())
        );
        assert!(
            worker
                .enqueue(params(VoiceCaptureAction::Finish, None), &[])
                .is_err()
        );
    }

    #[test]
    fn worker_tail_failure_and_panic_never_allow_finalize() {
        for panic_worker in [false, true] {
            let mut worker = MobileVoiceCapture::from_dsp(capture(), move |_, _, _| {
                if panic_worker {
                    panic!("synthetic worker panic");
                }
                Err("worker tail rejected".into())
            })
            .unwrap();
            worker
                .enqueue(
                    params(VoiceCaptureAction::Process, Some(48000)),
                    &bytes(&[0.5; 3]),
                )
                .unwrap();
            worker
                .enqueue(params(VoiceCaptureAction::Finish, None), &[])
                .unwrap();
            worker.worker.take().unwrap().join().unwrap();
            assert_eq!(
                worker.ensure_finished(),
                Err(if panic_worker {
                    "Voice capture worker failed".into()
                } else {
                    "worker tail rejected".into()
                })
            );
        }
    }
}
