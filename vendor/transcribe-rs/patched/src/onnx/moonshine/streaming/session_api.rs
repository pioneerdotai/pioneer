//! Recording/segment state without a borrow or lock on the shared ONNX model.
//! No VAD, endpoint detector, audio replay or implicit restart lives here.
use super::*;
use crate::{CompletedTranscription, DecodingCompletion};
use std::sync::Arc;

/// Engineering bound shared with the gateway's existing 18 s speech window.
pub const MAX_SESSION_SAMPLES: usize = 18 * SAMPLE_RATE as usize;
const MAX_STATE_BYTES: usize = 256 * 1024 * 1024;

pub struct StreamingSession {
    owner: Arc<()>,
    state: StreamingState,
    pending: Vec<f32>,
    real_samples: usize,
    padding_samples: usize,
    unavailable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deferred real-SDK coverage: must be explicitly admitted by the external
    // reviewer. This is not fake evidence for the acoustic model/export.
    #[test]
    #[ignore = "requires Moonshine Streaming export and a 16 kHz mono speech WAV; inference admission required"]
    fn real_export_preserves_acoustic_state_across_feed_partitions_and_final_tail() {
        let model_dir =
            std::env::var_os("PIONEER_MOONSHINE_STREAMING_MODEL").expect("model directory");
        let wav = std::env::var_os("PIONEER_VOICE_SPEECH_WAV").expect("speech WAV");
        let mut model =
            StreamingModel::load(Path::new(&model_dir), 1, &Quantization::Int8).unwrap();
        let audio = crate::audio::read_wav_samples(Path::new(&wav)).unwrap();
        assert!(
            audio.len() >= 3 * 16000 + 137,
            "provide at least 3.01 s speech"
        );
        let samples = &audio[..3 * 16000 + 137];
        let mut reference = None;
        for partition in [73, 1280, 4097, samples.len()] {
            let mut session = model.begin_session().unwrap();
            for chunk in samples.chunks(partition) {
                model.feed_session(&mut session, chunk).unwrap();
            }
            assert_eq!(session.real_samples(), samples.len());
            assert_eq!(session.pending.len(), samples.len() % 1280);
            // Preview re-decodes must not change frontend/encoder state.
            let features = session.state.accumulated_features.clone();
            let memory = session.state.memory.clone();
            let _ = model.session_hypothesis(&mut session).unwrap();
            assert_eq!(session.state.accumulated_features, features);
            assert_eq!(session.state.memory, memory);
            let result = model.finish_session(&mut session).unwrap();
            assert_eq!(session.padding_samples(), 1280 - samples.len() % 1280);
            assert!(session.pending.is_empty());
            assert_eq!(
                session.state.encoder_frames_emitted,
                session.state.accumulated_feature_count
            );
            assert!(session.state.cache_seq_len as usize <= model.config.max_seq_len);
            assert!(model.feed_session(&mut session, &[0.5]).is_err());
            assert!(model.finish_session(&mut session).is_err());
            let current = (
                result.result.text,
                result.completion,
                session.state.memory.clone(),
            );
            if let Some(expected) = &reference {
                assert_eq!(&current, expected);
            } else {
                reference = Some(current);
            }
        }
        let fresh = model.begin_session().unwrap();
        assert_eq!(fresh.real_samples(), 0);
        assert!(fresh.state.memory.is_empty());
        assert_eq!(fresh.state.cache_seq_len, 0);
        let mut oversized = model.begin_session().unwrap();
        assert!(model
            .feed_session(&mut oversized, &vec![0.0; MAX_SESSION_SAMPLES + 1])
            .is_err());
        assert_eq!(oversized.real_samples(), 0);
        assert!(model.feed_session(&mut oversized, &[0.5; 1280]).is_err());
        let mut other =
            StreamingModel::load(Path::new(&model_dir), 1, &Quantization::Int8).unwrap();
        let mut wrong_owner = fresh;
        assert!(other.feed_session(&mut wrong_owner, &[0.5; 1280]).is_err());
    }
}

impl StreamingSession {
    pub fn real_samples(&self) -> usize {
        self.real_samples
    }
    pub fn padding_samples(&self) -> usize {
        self.padding_samples
    }
}

impl StreamingModel {
    /// Begin one bounded acoustic segment. Retains no lock/model borrow.
    pub fn begin_session(&self) -> Result<StreamingSession, TranscribeError> {
        self.validate_session_config()?;
        let frames = MAX_SESSION_SAMPLES.div_ceil(320) + CHUNK_SIZE / 320;
        let mut state = self.create_state();
        // Reserve the bounded persistent tensors once. Geometric Vec growth
        // must not silently double the configuration's state estimate.
        state.accumulated_features = Vec::with_capacity(frames * self.config.encoder_dim);
        state.memory = Vec::with_capacity(frames * self.config.decoder_dim);
        let self_kv =
            self.config.depth * self.config.nheads * self.config.head_dim * self.config.max_seq_len;
        state.k_self = Vec::with_capacity(self_kv);
        state.v_self = Vec::with_capacity(self_kv);
        Ok(StreamingSession {
            owner: self.session_owner.clone(),
            state,
            pending: Vec::with_capacity(CHUNK_SIZE),
            real_samples: 0,
            padding_samples: 0,
            unavailable: false,
        })
    }

    fn validate_session_config(&self) -> Result<(), TranscribeError> {
        let c = &self.config;
        // The upstream frontend carries 79 samples and downsamples 4 x 80
        // samples per encoder frame. Do not silently apply that contract to a
        // different export. Bound all persistent tensors before allocation.
        if c.frame_len != 80
            || c.encoder_dim == 0
            || c.decoder_dim == 0
            || c.depth == 0
            || c.nheads == 0
            || c.head_dim == 0
            || c.d_model_frontend == 0
            || c.c1 == 0
            || c.max_seq_len == 0
            || c.max_seq_len > 8192
            || c.total_lookahead > 1024
        {
            return Err(TranscribeError::Config(
                "Unsupported streaming session config".into(),
            ));
        }
        let frames = MAX_SESSION_SAMPLES.div_ceil(320) + CHUNK_SIZE / 320;
        let estimate = (|| {
            let acoustic = frames.checked_mul(c.encoder_dim.checked_add(c.decoder_dim)?)?;
            let kv = c
                .depth
                .checked_mul(c.nheads)?
                .checked_mul(c.head_dim)?
                .checked_mul(frames.checked_add(c.max_seq_len)?)?
                .checked_mul(2)?;
            let frontend = c.d_model_frontend.checked_add(c.c1)?.checked_mul(4)?;
            acoustic
                .checked_add(kv)?
                .checked_add(frontend)?
                .checked_mul(4)
        })();
        if estimate.is_none_or(|bytes| bytes > MAX_STATE_BYTES) {
            return Err(TranscribeError::Config(
                "Streaming session state exceeds memory limit".into(),
            ));
        }
        Ok(())
    }

    fn check_session(&self, session: &StreamingSession) -> Result<(), TranscribeError> {
        if session.unavailable || !Arc::ptr_eq(&session.owner, &self.session_owner) {
            return Err(TranscribeError::Inference(
                "Streaming session closed or belongs to another model".into(),
            ));
        }
        Ok(())
    }

    /// Feed ordered, new prepared PCM exactly once. A partial chunk is retained
    /// until more audio or finish. Encoder lookahead remains un-emitted.
    pub fn feed_session(
        &mut self,
        session: &mut StreamingSession,
        samples: &[f32],
    ) -> Result<(), TranscribeError> {
        self.check_session(session)?;
        // Any failed operation poisons this session; no partial retry is safe.
        session.unavailable = true;
        let total = session
            .real_samples
            .checked_add(samples.len())
            .filter(|total| *total <= MAX_SESSION_SAMPLES)
            .ok_or_else(|| {
                TranscribeError::Inference("Streaming segment sample limit exceeded".into())
            })?;
        if samples.iter().any(|s| !s.is_finite()) {
            return Err(TranscribeError::Inference(
                "Non-finite streaming PCM".into(),
            ));
        }
        for slice in samples.chunks(CHUNK_SIZE) {
            let mut rest = slice;
            while !rest.is_empty() {
                let take = rest.len().min(CHUNK_SIZE - session.pending.len());
                session.pending.extend_from_slice(&rest[..take]);
                rest = &rest[take..];
                if session.pending.len() == CHUNK_SIZE {
                    self.process_audio_chunk(&mut session.state, &session.pending)?;
                    session.pending.clear();
                    self.encode_streaming(&mut session.state, false)?;
                }
            }
        }
        session.real_samples = total;
        session.unavailable = false;
        Ok(())
    }

    /// Tentative decode only. EOS here does not finalize the acoustic segment.
    pub fn session_hypothesis(
        &mut self,
        session: &mut StreamingSession,
    ) -> Result<CompletedTranscription, TranscribeError> {
        self.check_session(session)?;
        session.unavailable = true;
        let result = self.decode_session(session)?;
        session.unavailable = false;
        Ok(result)
    }

    /// Close the segment, flush the retained PCM (native padding is counted
    /// separately), release encoder lookahead, then decode against final memory.
    pub fn finish_session(
        &mut self,
        session: &mut StreamingSession,
    ) -> Result<CompletedTranscription, TranscribeError> {
        self.check_session(session)?;
        session.unavailable = true;
        if !session.pending.is_empty() {
            session.padding_samples = CHUNK_SIZE - session.pending.len();
            session.pending.resize(CHUNK_SIZE, 0.0);
            self.process_audio_chunk(&mut session.state, &session.pending)?;
            session.pending.clear();
        }
        self.encode_streaming(&mut session.state, true)?;
        self.decode_session(session)
    }

    fn decode_session(
        &mut self,
        session: &mut StreamingSession,
    ) -> Result<CompletedTranscription, TranscribeError> {
        let state = &mut session.state;
        // Self KV was conditioned on the previous acoustic memory. Continuing
        // that cache with new memory is not valid. Re-decode from BOS; retain
        // frontend/encoder/adapter state and recompute cross KV when invalidated.
        state.k_self.clear();
        state.v_self.clear();
        state.cache_seq_len = 0;
        let mut greedy = GreedyDecoder::new(self.config.eos_id);
        let mut tokens = Vec::new();
        if state.memory_len == 0 {
            return Ok(CompletedTranscription {
                result: TranscriptionResult {
                    text: String::new(),
                    segments: None,
                },
                completion: DecodingCompletion::Unknown,
            });
        }
        // The BOS occupies the first decoder input position; each call consumes
        // one position and predicts the next token, including the EOS attempt.
        // Count real prepared PCM, never native padding or gateway source gaps.
        let budget = ((session.real_samples as f64 / SAMPLE_RATE as f64 * 6.5).ceil() as usize)
            .min(self.config.max_seq_len);
        let mut token = self.config.bos_id;
        for _ in 0..budget {
            let logits = self.decode_step_logits(state, token)?;
            match greedy.next_token(&logits) {
                Some(next) => {
                    tokens.push(next);
                    token = next;
                }
                None => break,
            }
        }
        Ok(CompletedTranscription {
            result: TranscriptionResult {
                text: self.tokenizer.decode(&tokens)?,
                segments: None,
            },
            completion: greedy.completion(),
        })
    }
}
