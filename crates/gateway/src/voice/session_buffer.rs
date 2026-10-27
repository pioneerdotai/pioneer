use super::audio_normalization::{VoiceAudioNormalizationError, normalize_voice_pcm_chunk};
use super::incremental::VoiceProcessingCompletion;
use pioneer_protocol::{
    VOICE_AUDIO_MAX_CHUNK_BYTES, VoiceAudioFormat, VoiceChunkFrameHeader, VoiceError,
    VoiceErrorKind,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub(crate) const VOICE_SESSION_MAX_BUFFERED_BYTES: usize = VOICE_AUDIO_MAX_CHUNK_BYTES * 1_920;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BufferedVoiceChunk {
    pub(crate) sequence: u64,
    pub(crate) audio_payload: Vec<u8>,
    pub(crate) normalized_samples: Vec<f32>,
    pub(crate) captured_at_unix_ms: Option<u64>,
    pub(crate) duration_ms: Option<u32>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BufferedVoiceSessionAudio {
    pub(crate) session_id: String,
    pub(crate) audio_format: VoiceAudioFormat,
    pub(crate) chunks: Vec<BufferedVoiceChunk>,
    pub(crate) normalized_samples: Vec<f32>,
    pub(crate) buffered_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VoiceChunkIngestReport {
    pub(crate) session_id: String,
    pub(crate) sequence: u64,
    pub(crate) buffered_chunks: usize,
    pub(crate) buffered_bytes: usize,
    pub(crate) missing_chunks: u64,
    pub(crate) already_buffered: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceChunkIngestErrorKind {
    StoreUnavailable,
    UnknownSession,
    DuplicateSession,
    StaleChunk,
    AudioFormatMismatch,
    BufferLimitExceeded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VoiceChunkIngestError {
    pub(crate) kind: VoiceChunkIngestErrorKind,
    pub(crate) message: String,
}

impl VoiceChunkIngestError {
    pub(crate) fn into_voice_error(self) -> VoiceError {
        let (kind, public_code) = match self.kind {
            VoiceChunkIngestErrorKind::StoreUnavailable
            | VoiceChunkIngestErrorKind::UnknownSession
            | VoiceChunkIngestErrorKind::DuplicateSession => (
                VoiceErrorKind::InvalidSession,
                pioneer_protocol::PublicErrorCode::InvalidInput,
            ),
            VoiceChunkIngestErrorKind::StaleChunk => (
                VoiceErrorKind::StaleChunk,
                pioneer_protocol::PublicErrorCode::InvalidInput,
            ),
            VoiceChunkIngestErrorKind::AudioFormatMismatch => (
                VoiceErrorKind::DeviceUnavailable,
                pioneer_protocol::PublicErrorCode::InvalidInput,
            ),
            VoiceChunkIngestErrorKind::BufferLimitExceeded => (
                VoiceErrorKind::GatewayBusy,
                pioneer_protocol::PublicErrorCode::ResourceExhausted,
            ),
        };
        let public_error = crate::public_error::build_public_error(
            public_code,
            pioneer_protocol::PublicErrorStage::Admission,
        );
        match self.kind {
            VoiceChunkIngestErrorKind::StoreUnavailable => {
                crate::public_error::report_agent_failure(&public_error, self.message);
            }
            expected => {
                let failure_class = match expected {
                    VoiceChunkIngestErrorKind::UnknownSession => "unknown_session",
                    VoiceChunkIngestErrorKind::DuplicateSession => "duplicate_session",
                    VoiceChunkIngestErrorKind::StaleChunk => "stale_chunk",
                    VoiceChunkIngestErrorKind::AudioFormatMismatch => "audio_format_mismatch",
                    VoiceChunkIngestErrorKind::BufferLimitExceeded => "buffer_limit_exceeded",
                    VoiceChunkIngestErrorKind::StoreUnavailable => unreachable!(),
                };
                crate::public_error::report_expected_failure(
                    &public_error,
                    "voice_chunk_ingest",
                    failure_class,
                );
            }
        }
        VoiceError {
            kind,
            message: public_error.message.clone(),
            public_error: Some(public_error),
        }
    }
}

#[derive(Clone)]
pub(crate) struct GatewayVoiceSessionBufferStore {
    sessions: Arc<Mutex<HashMap<String, VoiceSessionBuffer>>>,
    ready: Arc<Condvar>,
    // Worker views deliberately do not own gateway lifetime. Dropping the last
    // gateway store also wakes/cancels idle workers (including fixture teardown).
    _lifetime: Option<Arc<VoiceBufferLifetime>>,
}

struct VoiceBufferLifetime {
    sessions: Arc<Mutex<HashMap<String, VoiceSessionBuffer>>>,
    ready: Arc<Condvar>,
}
impl Drop for VoiceBufferLifetime {
    fn drop(&mut self) {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for buffer in sessions.values() {
            buffer.cancel_processing();
        }
        sessions.clear();
        self.ready.notify_all();
    }
}
impl Default for GatewayVoiceSessionBufferStore {
    fn default() -> Self {
        let sessions = Arc::default();
        let ready = Arc::default();
        Self {
            _lifetime: Some(Arc::new(VoiceBufferLifetime {
                sessions: Arc::clone(&sessions),
                ready: Arc::clone(&ready),
            })),
            sessions,
            ready,
        }
    }
}

impl GatewayVoiceSessionBufferStore {
    pub(super) fn worker_view(&self) -> Self {
        Self {
            sessions: self.sessions.clone(),
            ready: self.ready.clone(),
            _lifetime: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn start_session(
        &self,
        session_id: impl Into<String>,
        audio_format: VoiceAudioFormat,
    ) -> Result<(), VoiceChunkIngestError> {
        let session_id = session_id.into();
        let mut sessions = self.sessions.lock().map_err(|_| {
            ingest_error(
                VoiceChunkIngestErrorKind::StoreUnavailable,
                "voice session buffer store is unavailable",
            )
        })?;
        if sessions.contains_key(session_id.as_str()) {
            return Err(ingest_error(
                VoiceChunkIngestErrorKind::DuplicateSession,
                format!("voice session `{session_id}` already exists"),
            ));
        }

        sessions.insert(session_id, VoiceSessionBuffer::new(audio_format));
        Ok(())
    }

    pub(crate) fn start_processing(
        &self,
        session_id: String,
        audio_format: VoiceAudioFormat,
        cancellation: CancellationToken,
        completion: oneshot::Receiver<VoiceProcessingCompletion>,
    ) -> Result<(), VoiceChunkIngestError> {
        let mut sessions = self.sessions.lock().map_err(|_| store_unavailable())?;
        if sessions.contains_key(&session_id) {
            return Err(ingest_error(
                VoiceChunkIngestErrorKind::DuplicateSession,
                "voice buffer already exists",
            ));
        }
        let mut buffer = VoiceSessionBuffer::new(audio_format);
        buffer.processing = Some(VoiceProcessingInput {
            cancellation,
            completion: Some(completion),
            startup_key: None,
        });
        sessions.insert(session_id, buffer);
        Ok(())
    }

    // Close and append share a mutex: every successful append before this
    // boundary is drained in sequence, including backlog behind native work.
    pub(crate) fn finish_processing(
        &self,
        session_id: &str,
    ) -> Result<oneshot::Receiver<VoiceProcessingCompletion>, VoiceChunkIngestError> {
        let mut sessions = self.sessions.lock().map_err(|_| store_unavailable())?;
        let buffer = sessions.get_mut(session_id).ok_or_else(inactive_session)?;
        let processing = buffer.processing.as_mut().ok_or_else(inactive_session)?;
        let completion = processing.completion.take().ok_or_else(inactive_session)?;
        processing.startup_key = pioneer_observability::turn_startup::current_key();
        buffer.closed = true;
        self.ready.notify_all();
        Ok(completion)
    }

    pub(super) fn next_processing_chunk(
        &self,
        session_id: &str,
    ) -> Result<Option<BufferedVoiceChunk>, VoiceChunkIngestError> {
        let mut sessions = self.sessions.lock().map_err(|_| store_unavailable())?;
        loop {
            let buffer = sessions.get_mut(session_id).ok_or_else(inactive_session)?;
            if let Some(chunk) = buffer.chunks.pop_front() {
                return Ok(Some(chunk));
            }
            if buffer.closed {
                return Ok(None);
            }
            sessions = self.ready.wait(sessions).map_err(|_| store_unavailable())?;
        }
    }

    // Buffered sessions keep the accepted inbox until Commit. Moving the deque
    // is constant-time; normalization ownership and heavy VAD work leave the
    // shared lock together. Removal/cancel wakes this wait and fails closed.
    pub(super) fn take_closed_processing_chunks(
        &self,
        session_id: &str,
    ) -> Result<VecDeque<BufferedVoiceChunk>, VoiceChunkIngestError> {
        let mut sessions = self.sessions.lock().map_err(|_| store_unavailable())?;
        loop {
            let buffer = sessions.get_mut(session_id).ok_or_else(inactive_session)?;
            if buffer.closed {
                return Ok(std::mem::take(&mut buffer.chunks));
            }
            sessions = self.ready.wait(sessions).map_err(|_| store_unavailable())?;
        }
    }

    pub(super) fn processing_startup_key(&self, session_id: &str) -> Option<String> {
        self.sessions
            .lock()
            .ok()?
            .get(session_id)?
            .processing
            .as_ref()?
            .startup_key
            .clone()
    }

    // Only a hint for skipping superseded preview inference, never for audio
    // ownership or Commit. Append/close and the actual drain stay serialized.
    pub(super) fn processing_skip_preview(&self, session_id: &str) -> bool {
        self.sessions
            .lock()
            .ok()
            .and_then(|sessions| {
                sessions
                    .get(session_id)
                    .map(|buffer| buffer.closed || !buffer.chunks.is_empty())
            })
            .unwrap_or(true)
    }

    pub(super) fn processing_finished(&self, session_id: &str) {
        if let Ok(mut sessions) = self.sessions.lock() {
            if let Some(buffer) = sessions.get_mut(session_id) {
                buffer.closed = true;
                buffer.chunks.clear();
            }
        }
    }

    pub(crate) fn cancel_all(&self) {
        // Teardown must still drop completion leases if another operation
        // poisoned the buffer mutex. Admission remains fail-closed.
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for buffer in sessions.values() {
            buffer.cancel_processing();
        }
        sessions.clear();
        self.ready.notify_all();
    }

    pub(crate) fn remove_session(&self, session_id: &str) -> Result<(), VoiceChunkIngestError> {
        let mut sessions = self.sessions.lock().map_err(|_| {
            ingest_error(
                VoiceChunkIngestErrorKind::StoreUnavailable,
                "voice session buffer store is unavailable",
            )
        })?;
        let result = sessions
            .remove(session_id)
            .map(|buffer| buffer.cancel_processing())
            .ok_or_else(|| {
                ingest_error(
                    VoiceChunkIngestErrorKind::UnknownSession,
                    format!("voice session `{session_id}` is not active"),
                )
            });
        self.ready.notify_all();
        result
    }

    #[cfg(test)]
    pub(crate) fn take_session_audio(
        &self,
        session_id: &str,
    ) -> Result<BufferedVoiceSessionAudio, VoiceChunkIngestError> {
        let mut sessions = self.sessions.lock().map_err(|_| {
            ingest_error(
                VoiceChunkIngestErrorKind::StoreUnavailable,
                "voice session buffer store is unavailable",
            )
        })?;
        let state = sessions.remove(session_id).ok_or_else(|| {
            ingest_error(
                VoiceChunkIngestErrorKind::UnknownSession,
                format!("voice session `{session_id}` is not active"),
            )
        })?;
        state.cancel_processing();
        self.ready.notify_all();
        Ok(state.into_session_audio(session_id))
    }

    pub(crate) fn append_chunk(
        &self,
        header: VoiceChunkFrameHeader,
        audio_payload: Vec<u8>,
    ) -> Result<VoiceChunkIngestReport, VoiceChunkIngestError> {
        let mut sessions = self.sessions.lock().map_err(|_| {
            ingest_error(
                VoiceChunkIngestErrorKind::StoreUnavailable,
                "voice session buffer store is unavailable",
            )
        })?;
        let state = sessions
            .get_mut(header.session_id.as_str())
            .ok_or_else(|| {
                ingest_error(
                    VoiceChunkIngestErrorKind::UnknownSession,
                    format!("voice session `{}` is not active", header.session_id),
                )
            })?;
        let result = state.append_chunk(header, audio_payload);
        self.ready.notify_all();
        result
    }

    #[cfg(test)]
    pub(crate) fn buffered_bytes_for_test(
        &self,
        session_id: &str,
    ) -> Result<usize, VoiceChunkIngestError> {
        let sessions = self.sessions.lock().map_err(|_| {
            ingest_error(
                VoiceChunkIngestErrorKind::StoreUnavailable,
                "voice session buffer store is unavailable",
            )
        })?;
        match sessions.get(session_id) {
            Some(buffer) => Ok(buffer.buffered_bytes),
            None => Err(ingest_error(
                VoiceChunkIngestErrorKind::UnknownSession,
                format!("voice session `{session_id}` is not active"),
            )),
        }
    }
}

struct VoiceProcessingInput {
    cancellation: CancellationToken,
    completion: Option<oneshot::Receiver<VoiceProcessingCompletion>>,
    startup_key: Option<String>,
}

struct VoiceSessionBuffer {
    audio_format: VoiceAudioFormat,
    next_sequence: u64,
    buffered_bytes: usize,
    chunks: VecDeque<BufferedVoiceChunk>,
    accepted_chunks: usize,
    closed: bool,
    processing: Option<VoiceProcessingInput>,
}

impl VoiceSessionBuffer {
    fn new(audio_format: VoiceAudioFormat) -> Self {
        Self {
            audio_format,
            next_sequence: 0,
            buffered_bytes: 0,
            chunks: VecDeque::new(),
            accepted_chunks: 0,
            closed: false,
            processing: None,
        }
    }

    fn cancel_processing(&self) {
        if let Some(processing) = &self.processing {
            processing.cancellation.cancel();
        }
    }

    fn append_chunk(
        &mut self,
        header: VoiceChunkFrameHeader,
        audio_payload: Vec<u8>,
    ) -> Result<VoiceChunkIngestReport, VoiceChunkIngestError> {
        if self.closed {
            return Err(inactive_session());
        }
        if header.audio_format() != self.audio_format {
            return Err(ingest_error(
                VoiceChunkIngestErrorKind::AudioFormatMismatch,
                format!(
                    "voice chunk format does not match session `{}`",
                    header.session_id
                ),
            ));
        }
        if header.sequence < self.next_sequence {
            return Ok(VoiceChunkIngestReport {
                session_id: header.session_id,
                sequence: header.sequence,
                buffered_chunks: self.accepted_chunks,
                buffered_bytes: self.buffered_bytes,
                missing_chunks: 0,
                already_buffered: true,
            });
        }

        let missing_chunks = header.sequence - self.next_sequence;

        let normalized = normalize_voice_pcm_chunk(header.audio_format(), audio_payload.as_slice())
            .map_err(|error| match error {
                VoiceAudioNormalizationError::UnsupportedFormat(message) => {
                    ingest_error(VoiceChunkIngestErrorKind::AudioFormatMismatch, message)
                }
                VoiceAudioNormalizationError::IncompletePcmSample => {
                    ingest_error(VoiceChunkIngestErrorKind::StaleChunk, error.to_string())
                }
            })?;
        let chunk_memory_bytes = audio_payload
            .len()
            .checked_add(normalized.memory_bytes())
            .ok_or_else(|| {
                ingest_error(
                    VoiceChunkIngestErrorKind::BufferLimitExceeded,
                    "voice session chunk memory byte count overflowed",
                )
            })?;
        let next_buffered_bytes = self
            .buffered_bytes
            .checked_add(chunk_memory_bytes)
            .ok_or_else(|| {
                ingest_error(
                    VoiceChunkIngestErrorKind::BufferLimitExceeded,
                    "voice session buffer byte count overflowed",
                )
            })?;
        if next_buffered_bytes > VOICE_SESSION_MAX_BUFFERED_BYTES {
            return Err(ingest_error(
                VoiceChunkIngestErrorKind::BufferLimitExceeded,
                format!(
                    "voice session buffer exceeds maximum: {} > {}",
                    next_buffered_bytes, VOICE_SESSION_MAX_BUFFERED_BYTES
                ),
            ));
        }

        let sequence = header.sequence;
        let session_id = header.session_id;
        let duration_ms = header.duration_ms;
        let captured_at_unix_ms = header.captured_at_unix_ms;
        // This is a lifetime budget, not a rolling allowance. Draining audio
        // never permits an infinitely long recording.
        self.buffered_bytes = next_buffered_bytes;
        self.accepted_chunks += 1;
        self.next_sequence = sequence.saturating_add(1);
        self.chunks.push_back(BufferedVoiceChunk {
            sequence,
            audio_payload,
            normalized_samples: normalized.samples,
            captured_at_unix_ms,
            duration_ms,
        });

        Ok(VoiceChunkIngestReport {
            session_id,
            sequence,
            buffered_chunks: self.accepted_chunks,
            buffered_bytes: self.buffered_bytes,
            missing_chunks,
            already_buffered: false,
        })
    }

    #[cfg(test)]
    fn into_session_audio(self, session_id: &str) -> BufferedVoiceSessionAudio {
        let chunks = self.chunks.into_iter().collect::<Vec<_>>();
        let normalized_samples = chunks
            .iter()
            .flat_map(|chunk| chunk.normalized_samples.iter().copied())
            .collect::<Vec<_>>();
        BufferedVoiceSessionAudio {
            session_id: session_id.to_owned(),
            audio_format: self.audio_format,
            chunks,
            normalized_samples,
            buffered_bytes: self.buffered_bytes,
        }
    }
}

fn store_unavailable() -> VoiceChunkIngestError {
    ingest_error(
        VoiceChunkIngestErrorKind::StoreUnavailable,
        "voice session buffer store is unavailable",
    )
}

fn inactive_session() -> VoiceChunkIngestError {
    ingest_error(
        VoiceChunkIngestErrorKind::UnknownSession,
        "voice processing input is closed or unavailable",
    )
}

fn ingest_error(
    kind: VoiceChunkIngestErrorKind,
    message: impl Into<String>,
) -> VoiceChunkIngestError {
    VoiceChunkIngestError {
        kind,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{VoiceAudioEncoding, VoiceAudioFormat};

    #[test]
    fn buffer_refusals_are_breadcrumbs_but_poisoned_store_is_an_error() {
        use crate::public_error::test_support::*;
        let store = GatewayVoiceSessionBufferStore::default();
        let (error, events) = capture_events(|| {
            store
                .take_session_audio("missing_session")
                .unwrap_err()
                .into_voice_error()
        });
        assert!(events.is_empty());
        assert_eq!(error.kind, VoiceErrorKind::InvalidSession);
        assert_eq!(
            error.public_error.unwrap().code,
            pioneer_protocol::PublicErrorCode::InvalidInput
        );
        let _ = std::panic::catch_unwind(|| {
            let _guard = store.sessions.lock().unwrap();
            panic!("poison test buffer store");
        });
        let (error, events) = capture_events(|| {
            let failure = store.take_session_audio("missing_session").unwrap_err();
            assert_eq!(failure.kind, VoiceChunkIngestErrorKind::StoreUnavailable);
            failure.into_voice_error()
        });
        // Preserve the original public contract even though the internal cause
        // now distinguishes infrastructure from a missing session.
        assert_eq!(error.kind, VoiceErrorKind::InvalidSession);
        assert_eq!(
            error.public_error.as_ref().unwrap().code,
            pioneer_protocol::PublicErrorCode::InvalidInput
        );
        assert_eq!(events.len(), 1);
        assert_correlated(&events[0], error.public_error.as_ref().unwrap());
    }

    #[test]
    fn active_session_buffers_ordered_chunks() {
        let store = GatewayVoiceSessionBufferStore::default();
        store
            .start_session("voice_session_1", target_format())
            .expect("start session");

        let first = store
            .append_chunk(header("voice_session_1", 0), vec![0; 640])
            .expect("first chunk");
        let second = store
            .append_chunk(header("voice_session_1", 1), vec![1; 640])
            .expect("second chunk");

        assert_eq!(first.buffered_chunks, 1);
        assert_eq!(second.buffered_chunks, 2);
        assert_eq!(second.buffered_bytes, 3_840);
    }

    #[test]
    fn take_session_audio_returns_samples_and_releases_buffer() {
        let store = GatewayVoiceSessionBufferStore::default();
        store
            .start_session("voice_session_1", target_format())
            .expect("start session");
        store
            .append_chunk(header("voice_session_1", 0), vec![0; 640])
            .expect("first chunk");
        store
            .append_chunk(header("voice_session_1", 1), vec![0; 640])
            .expect("second chunk");

        let audio = store
            .take_session_audio("voice_session_1")
            .expect("session audio");
        let late = store
            .append_chunk(header("voice_session_1", 2), vec![0; 640])
            .expect_err("late chunk should fail after take");

        assert_eq!(audio.session_id, "voice_session_1");
        assert_eq!(audio.chunks.len(), 2);
        assert_eq!(audio.normalized_samples.len(), 640);
        assert_eq!(late.kind, VoiceChunkIngestErrorKind::UnknownSession);
    }

    #[test]
    fn unknown_sessions_reject_chunks() {
        let store = GatewayVoiceSessionBufferStore::default();
        let unknown = store
            .append_chunk(header("voice_session_1", 0), vec![0; 640])
            .expect_err("unknown session should fail");
        assert_eq!(unknown.kind, VoiceChunkIngestErrorKind::UnknownSession);
    }

    #[test]
    fn stale_chunks_are_idempotent_and_sequence_gaps_resume_capture() {
        let store = GatewayVoiceSessionBufferStore::default();
        store
            .start_session("voice_session_1", target_format())
            .expect("start session");
        store
            .append_chunk(header("voice_session_1", 0), vec![0; 640])
            .expect("first chunk");

        let stale = store
            .append_chunk(header("voice_session_1", 0), vec![0; 640])
            .expect("a repeated chunk should be acknowledged idempotently");
        assert!(stale.already_buffered);
        assert_eq!(stale.buffered_chunks, 1);

        let gap = store
            .append_chunk(header("voice_session_1", 3), vec![0; 640])
            .expect("a sequence gap should resume capture");
        assert!(!gap.already_buffered);
        assert_eq!(gap.missing_chunks, 2);
        assert_eq!(gap.buffered_chunks, 2);

        let resumed = store
            .append_chunk(header("voice_session_1", 4), vec![0; 640])
            .expect("capture should continue after a gap");
        assert_eq!(resumed.missing_chunks, 0);
        assert_eq!(resumed.buffered_chunks, 3);
    }

    #[test]
    fn buffer_memory_limit_is_enforced() {
        let store = GatewayVoiceSessionBufferStore::default();
        store
            .start_session("voice_session_1", target_format())
            .expect("start session");
        store
            .append_chunk(
                header("voice_session_1", 0),
                vec![0; VOICE_SESSION_MAX_BUFFERED_BYTES / 3],
            )
            .expect("max chunk aggregate");

        let error = store
            .append_chunk(header("voice_session_1", 1), vec![0; 2])
            .expect_err("buffer cap should fail");

        assert_eq!(error.kind, VoiceChunkIngestErrorKind::BufferLimitExceeded);
        assert_eq!(
            store
                .buffered_bytes_for_test("voice_session_1")
                .expect("buffered bytes"),
            VOICE_SESSION_MAX_BUFFERED_BYTES
        );
    }

    #[test]
    fn voice_incremental_drain_retains_lifetime_budget_and_sequence_history() {
        let store = GatewayVoiceSessionBufferStore::default();
        store.start_session("session", target_format()).unwrap();
        store
            .append_chunk(
                header("session", 0),
                vec![0; VOICE_SESSION_MAX_BUFFERED_BYTES / 3],
            )
            .unwrap();
        assert_eq!(
            store
                .next_processing_chunk("session")
                .unwrap()
                .unwrap()
                .sequence,
            0
        );
        let duplicate = store
            .append_chunk(header("session", 0), vec![0; 2])
            .unwrap();
        assert!(duplicate.already_buffered);
        assert_eq!(duplicate.buffered_chunks, 1);
        assert_eq!(
            store
                .append_chunk(header("session", 1), vec![0; 2])
                .unwrap_err()
                .kind,
            VoiceChunkIngestErrorKind::BufferLimitExceeded
        );
    }

    #[test]
    fn voice_incremental_close_drains_accepted_gaps_once_and_rejects_late_input() {
        let store = GatewayVoiceSessionBufferStore::default();
        let token = CancellationToken::new();
        let (_sender, receiver) = oneshot::channel();
        store
            .start_processing(
                "session".to_owned(),
                target_format(),
                token.clone(),
                receiver,
            )
            .unwrap();
        store
            .append_chunk(header("session", 0), vec![0; 640])
            .unwrap();
        assert_eq!(
            store
                .append_chunk(header("session", 3), vec![0; 640])
                .unwrap()
                .missing_chunks,
            2
        );
        assert!(
            store
                .append_chunk(header("session", 0), vec![0; 640])
                .unwrap()
                .already_buffered
        );
        let _completion = store.finish_processing("session").unwrap();
        assert!(store.finish_processing("session").is_err());
        assert!(
            store
                .append_chunk(header("session", 4), vec![0; 640])
                .is_err()
        );
        assert_eq!(
            store
                .next_processing_chunk("session")
                .unwrap()
                .unwrap()
                .sequence,
            0
        );
        assert_eq!(
            store
                .next_processing_chunk("session")
                .unwrap()
                .unwrap()
                .sequence,
            3
        );
        assert!(store.next_processing_chunk("session").unwrap().is_none());
        store.remove_session("session").unwrap();
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn voice_incremental_last_owner_drop_wakes_idle_worker() {
        let store = GatewayVoiceSessionBufferStore::default();
        store.start_session("session", target_format()).unwrap();
        let worker = store.worker_view();
        let tasks = tokio_util::task::TaskTracker::new();
        let handle = tasks.spawn_blocking(move || worker.next_processing_chunk("session"));
        drop(store);
        tasks.close();
        tokio::time::timeout(std::time::Duration::from_secs(5), tasks.wait())
            .await
            .unwrap();
        assert!(handle.await.unwrap().is_err());
    }

    #[test]
    fn buffered_commit_racing_append_transfers_every_accepted_chunk_once() {
        let store = GatewayVoiceSessionBufferStore::default();
        let (_sender, receiver) = oneshot::channel();
        store
            .start_processing(
                "session".to_owned(),
                target_format(),
                CancellationToken::new(),
                receiver,
            )
            .unwrap();
        let packet_header = |sequence| VoiceChunkFrameHeader {
            payload_len: 2,
            duration_ms: None,
            ..header("session", sequence)
        };
        store.append_chunk(packet_header(0), vec![0, 32]).unwrap();
        let boundary = std::sync::Barrier::new(2);
        let accepted = std::thread::scope(|scope| {
            let append = scope.spawn(|| {
                boundary.wait();
                store.append_chunk(packet_header(1), vec![0, 64])
            });
            boundary.wait();
            let _completion = store.finish_processing("session").unwrap();
            match append.join().unwrap() {
                Ok(report) => {
                    assert!(!report.already_buffered);
                    true
                }
                Err(error) => {
                    assert_eq!(error.kind, VoiceChunkIngestErrorKind::UnknownSession);
                    false
                }
            }
        });
        let chunks = store.take_closed_processing_chunks("session").unwrap();
        assert_eq!(chunks.len(), if accepted { 2 } else { 1 });
        assert_eq!(chunks[0].sequence, 0);
        assert_eq!(chunks[0].normalized_samples, vec![0.25]);
        if accepted {
            assert_eq!(chunks[1].sequence, 1);
            assert_eq!(chunks[1].normalized_samples, vec![0.5]);
        }
        assert!(
            store
                .take_closed_processing_chunks("session")
                .unwrap()
                .is_empty()
        );
    }

    fn target_format() -> VoiceAudioFormat {
        VoiceAudioFormat {
            sample_rate_hz: 16_000,
            channels: 1,
            encoding: VoiceAudioEncoding::PcmS16Le,
        }
    }

    fn header(session_id: &str, sequence: u64) -> VoiceChunkFrameHeader {
        VoiceChunkFrameHeader {
            session_id: session_id.to_owned(),
            sequence,
            sample_rate_hz: 16_000,
            channels: 1,
            encoding: VoiceAudioEncoding::PcmS16Le,
            payload_len: 640,
            captured_at_unix_ms: None,
            duration_ms: Some(20),
        }
    }
}
