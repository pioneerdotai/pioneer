use super::audio_normalization::{VoiceAudioNormalizationError, normalize_voice_pcm_chunk};
use pioneer_protocol::{
    VOICE_AUDIO_MAX_CHUNK_BYTES, VoiceAudioFormat, VoiceChunkFrameHeader, VoiceError,
    VoiceErrorKind,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

pub(crate) const VOICE_SESSION_MAX_BUFFERED_BYTES: usize = VOICE_AUDIO_MAX_CHUNK_BYTES * 1_920;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BufferedVoiceChunk {
    pub(crate) sequence: u64,
    pub(crate) audio_payload: Vec<u8>,
    pub(crate) normalized_samples: Vec<f32>,
    pub(crate) captured_at_unix_ms: Option<u64>,
    pub(crate) duration_ms: Option<u32>,
}

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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceChunkIngestErrorKind {
    UnknownSession,
    DuplicateSession,
    StaleChunk,
    SequenceGap,
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
            VoiceChunkIngestErrorKind::UnknownSession
            | VoiceChunkIngestErrorKind::DuplicateSession => (
                VoiceErrorKind::InvalidSession,
                pioneer_protocol::PublicErrorCode::InvalidInput,
            ),
            VoiceChunkIngestErrorKind::StaleChunk => (
                VoiceErrorKind::StaleChunk,
                pioneer_protocol::PublicErrorCode::InvalidInput,
            ),
            VoiceChunkIngestErrorKind::SequenceGap => (
                VoiceErrorKind::SequenceGap,
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
        let public_error = crate::public_error::map_agent_failure(
            public_code,
            pioneer_protocol::PublicErrorStage::Admission,
            self.message,
        );
        VoiceError {
            kind,
            message: public_error.message.clone(),
            public_error: Some(public_error),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct GatewayVoiceSessionBufferStore {
    sessions: Arc<Mutex<HashMap<String, VoiceSessionBuffer>>>,
}

impl GatewayVoiceSessionBufferStore {
    pub(crate) fn start_session(
        &self,
        session_id: impl Into<String>,
        audio_format: VoiceAudioFormat,
    ) -> Result<(), VoiceChunkIngestError> {
        let session_id = session_id.into();
        let mut sessions = self.sessions.lock().map_err(|_| {
            ingest_error(
                VoiceChunkIngestErrorKind::UnknownSession,
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

    pub(crate) fn remove_session(&self, session_id: &str) -> Result<(), VoiceChunkIngestError> {
        let mut sessions = self.sessions.lock().map_err(|_| {
            ingest_error(
                VoiceChunkIngestErrorKind::UnknownSession,
                "voice session buffer store is unavailable",
            )
        })?;
        sessions.remove(session_id).map(|_| ()).ok_or_else(|| {
            ingest_error(
                VoiceChunkIngestErrorKind::UnknownSession,
                format!("voice session `{session_id}` is not active"),
            )
        })
    }

    pub(crate) fn take_session_audio(
        &self,
        session_id: &str,
    ) -> Result<BufferedVoiceSessionAudio, VoiceChunkIngestError> {
        let mut sessions = self.sessions.lock().map_err(|_| {
            ingest_error(
                VoiceChunkIngestErrorKind::UnknownSession,
                "voice session buffer store is unavailable",
            )
        })?;
        let state = sessions.remove(session_id).ok_or_else(|| {
            ingest_error(
                VoiceChunkIngestErrorKind::UnknownSession,
                format!("voice session `{session_id}` is not active"),
            )
        })?;
        Ok(state.into_session_audio(session_id))
    }

    pub(crate) fn append_chunk(
        &self,
        header: VoiceChunkFrameHeader,
        audio_payload: Vec<u8>,
    ) -> Result<VoiceChunkIngestReport, VoiceChunkIngestError> {
        let mut sessions = self.sessions.lock().map_err(|_| {
            ingest_error(
                VoiceChunkIngestErrorKind::UnknownSession,
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
        state.append_chunk(header, audio_payload)
    }

    #[cfg(test)]
    pub(crate) fn buffered_bytes_for_test(
        &self,
        session_id: &str,
    ) -> Result<usize, VoiceChunkIngestError> {
        let sessions = self.sessions.lock().map_err(|_| {
            ingest_error(
                VoiceChunkIngestErrorKind::UnknownSession,
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

#[derive(Clone, Debug, PartialEq)]
struct VoiceSessionBuffer {
    audio_format: VoiceAudioFormat,
    next_sequence: u64,
    buffered_bytes: usize,
    chunks: VecDeque<BufferedVoiceChunk>,
}

impl VoiceSessionBuffer {
    fn new(audio_format: VoiceAudioFormat) -> Self {
        Self {
            audio_format,
            next_sequence: 0,
            buffered_bytes: 0,
            chunks: VecDeque::new(),
        }
    }

    fn append_chunk(
        &mut self,
        header: VoiceChunkFrameHeader,
        audio_payload: Vec<u8>,
    ) -> Result<VoiceChunkIngestReport, VoiceChunkIngestError> {
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
            return Err(ingest_error(
                VoiceChunkIngestErrorKind::StaleChunk,
                format!(
                    "voice chunk sequence {} is stale; expected {}",
                    header.sequence, self.next_sequence
                ),
            ));
        }
        if header.sequence > self.next_sequence {
            return Err(ingest_error(
                VoiceChunkIngestErrorKind::SequenceGap,
                format!(
                    "voice chunk sequence gap: expected {}, got {}",
                    self.next_sequence, header.sequence
                ),
            ));
        }

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
        self.buffered_bytes = next_buffered_bytes;
        self.next_sequence = self.next_sequence.saturating_add(1);
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
            buffered_chunks: self.chunks.len(),
            buffered_bytes: self.buffered_bytes,
        })
    }

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
    fn stale_and_gap_sequences_are_rejected() {
        let store = GatewayVoiceSessionBufferStore::default();
        store
            .start_session("voice_session_1", target_format())
            .expect("start session");
        store
            .append_chunk(header("voice_session_1", 0), vec![0; 640])
            .expect("first chunk");

        let stale = store
            .append_chunk(header("voice_session_1", 0), vec![0; 640])
            .expect_err("stale sequence should fail");
        assert_eq!(stale.kind, VoiceChunkIngestErrorKind::StaleChunk);

        let gap = store
            .append_chunk(header("voice_session_1", 2), vec![0; 640])
            .expect_err("sequence gap should fail");
        assert_eq!(gap.kind, VoiceChunkIngestErrorKind::SequenceGap);
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
