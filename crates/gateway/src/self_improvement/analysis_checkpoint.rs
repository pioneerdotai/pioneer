use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use pioneer_crud::SelfImprovementRunRecord;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::history::{SelfImprovementHistoryChunk, validate_history_chunk_contract};
use super::learner::{
    ValidatedChunkDigest, validate_digest_against_processed_chunks, validate_persistable_digest,
};

const CHECKPOINT_SCHEMA_VERSION: u32 = 2;
const CURSOR_MAX_BYTES: usize = 64 * 1024;
const DIGEST_MAX_BYTES: usize = 1024 * 1024;
const CHUNK_TERMINAL_VALIDATED: u8 = 0;
const CHUNK_TERMINAL_OUTPUT_TOO_LARGE: u8 = 1;
const CHUNK_TERMINAL_MALFORMED_JSON: u8 = 2;
const CHUNK_TERMINAL_CONTRACT_REJECTED: u8 = 3;
const CHUNKS_PER_TERMINAL_CODE_BYTE: u32 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AnalysisCursor {
    schema_version: u32,
    source_lower_exclusive: i64,
    source_upper_inclusive: i64,
    plan_fingerprint: String,
    chunk_count: u32,
    next_chunk_index: u32,
    validated_chunk_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AnalysisDigest {
    schema_version: u32,
    validated: Option<ValidatedChunkDigest>,
    #[serde(with = "terminal_codes_base64")]
    chunk_terminal_codes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlanFingerprintInput<'a> {
    schema_version: u32,
    workspace_id: &'a str,
    source_lower_exclusive: i64,
    source_upper_inclusive: i64,
    chunk_fingerprints: Vec<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResumableHistoryAnalysis {
    cursor: AnalysisCursor,
    digest: AnalysisDigest,
}

impl ResumableHistoryAnalysis {
    pub(crate) fn restore(
        run: &SelfImprovementRunRecord,
        chunks: &[SelfImprovementHistoryChunk],
    ) -> Result<Self> {
        validate_plan(run, chunks)?;
        let plan_fingerprint = plan_fingerprint(chunks)?;
        let chunk_count =
            u32::try_from(chunks.len()).context("history chunk count exceeds checkpoint schema")?;
        let checkpoint = match (
            run.analysis_cursor_json.as_deref(),
            run.analysis_digest_json.as_deref(),
        ) {
            (None, None) => Self {
                cursor: AnalysisCursor {
                    schema_version: CHECKPOINT_SCHEMA_VERSION,
                    source_lower_exclusive: run.source_lower_exclusive,
                    source_upper_inclusive: run.source_upper_inclusive,
                    plan_fingerprint,
                    chunk_count,
                    next_chunk_index: 0,
                    validated_chunk_count: 0,
                },
                digest: AnalysisDigest {
                    schema_version: CHECKPOINT_SCHEMA_VERSION,
                    validated: None,
                    chunk_terminal_codes: Vec::new(),
                },
            },
            (Some(cursor), Some(digest)) => Self {
                cursor: serde_json::from_str(cursor)
                    .context("self-improvement analysis cursor is malformed")?,
                digest: serde_json::from_str(digest)
                    .context("self-improvement analysis digest is malformed")?,
            },
            _ => bail!("self-improvement run contains a partial analysis checkpoint"),
        };
        checkpoint.validate(run, chunks)?;
        Ok(checkpoint)
    }

    pub(crate) fn next_chunk_index(&self) -> u32 {
        self.cursor.next_chunk_index
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.cursor.next_chunk_index == self.cursor.chunk_count
    }

    pub(crate) fn validated_digest(&self) -> Option<&ValidatedChunkDigest> {
        self.digest.validated.as_ref()
    }

    pub(crate) fn chunk_rejection_reason_code(
        &self,
        chunk_index: u32,
    ) -> Result<Option<&'static str>> {
        if chunk_index >= self.cursor.next_chunk_index {
            bail!("analysis checkpoint chunk has not reached a terminal state");
        }
        let terminal_code = chunk_terminal_code(&self.digest.chunk_terminal_codes, chunk_index)?;
        Ok(rejection_reason_code(terminal_code))
    }

    pub(crate) fn record_validated(
        &mut self,
        chunk: &SelfImprovementHistoryChunk,
        digest: ValidatedChunkDigest,
    ) -> Result<()> {
        self.require_next_chunk(chunk)?;
        let expected_revision = self.cursor.validated_chunk_count.saturating_add(1);
        if digest.digest_revision != expected_revision {
            bail!("validated history digest revision does not match checkpoint cursor");
        }
        validate_persistable_digest(&digest)
            .map_err(|_| anyhow::anyhow!("validated history digest is not persistable"))?;
        append_chunk_terminal_code(
            &mut self.digest.chunk_terminal_codes,
            self.cursor.next_chunk_index,
            CHUNK_TERMINAL_VALIDATED,
        )?;
        self.digest.validated = Some(digest);
        self.cursor.validated_chunk_count = expected_revision;
        self.cursor.next_chunk_index = self.cursor.next_chunk_index.saturating_add(1);
        self.validate_counts()
    }

    #[cfg(test)]
    pub(crate) fn record_contract_rejected(
        &mut self,
        chunk: &SelfImprovementHistoryChunk,
        reason_code: &str,
    ) -> Result<()> {
        self.require_next_chunk(chunk)?;
        let terminal_code = rejection_terminal_code(reason_code)?;
        append_chunk_terminal_code(
            &mut self.digest.chunk_terminal_codes,
            self.cursor.next_chunk_index,
            terminal_code,
        )?;
        self.cursor.next_chunk_index = self.cursor.next_chunk_index.saturating_add(1);
        self.validate_counts()
    }

    pub(crate) fn encode(&self) -> Result<(String, String)> {
        let cursor =
            serde_json::to_string(&self.cursor).context("failed to encode analysis cursor")?;
        let digest =
            serde_json::to_string(&self.digest).context("failed to encode analysis digest")?;
        if cursor.len() > CURSOR_MAX_BYTES || digest.len() > DIGEST_MAX_BYTES {
            bail!("self-improvement analysis checkpoint exceeds its persistence bound");
        }
        Ok((cursor, digest))
    }

    fn require_next_chunk(&self, chunk: &SelfImprovementHistoryChunk) -> Result<()> {
        if self.is_complete()
            || chunk.chunk_index != self.cursor.next_chunk_index
            || chunk.chunk_count != self.cursor.chunk_count
        {
            bail!("history chunk does not match the exact checkpoint cursor");
        }
        Ok(())
    }

    fn validate(
        &self,
        run: &SelfImprovementRunRecord,
        chunks: &[SelfImprovementHistoryChunk],
    ) -> Result<()> {
        if self.cursor.schema_version != CHECKPOINT_SCHEMA_VERSION
            || self.digest.schema_version != CHECKPOINT_SCHEMA_VERSION
            || self.cursor.source_lower_exclusive != run.source_lower_exclusive
            || self.cursor.source_upper_inclusive != run.source_upper_inclusive
            || self.cursor.plan_fingerprint != plan_fingerprint(chunks)?
            || usize::try_from(self.cursor.chunk_count).ok() != Some(chunks.len())
            || self.cursor.next_chunk_index > self.cursor.chunk_count
        {
            bail!("self-improvement analysis checkpoint identity is stale");
        }
        self.validate_counts()?;

        let mut rejected_indexes = HashSet::new();
        for chunk_index in 0..self.cursor.next_chunk_index {
            let terminal_code =
                chunk_terminal_code(&self.digest.chunk_terminal_codes, chunk_index)?;
            if terminal_code != CHUNK_TERMINAL_VALIDATED {
                rejection_reason_code(terminal_code)
                    .context("analysis checkpoint contains an invalid rejection reason")?;
                let chunk = chunks
                    .get(
                        usize::try_from(chunk_index)
                            .context("rejected chunk index exceeds platform usize")?,
                    )
                    .context("analysis checkpoint rejected marker references an unknown chunk")?;
                if chunk.chunk_index != chunk_index {
                    bail!("analysis checkpoint rejected marker identity is stale");
                }
                rejected_indexes.insert(chunk_index);
            }
        }

        match (
            self.cursor.validated_chunk_count,
            self.digest.validated.as_ref(),
        ) {
            (0, None) => {}
            (count, Some(digest)) if count == digest.digest_revision => {
                let processed_validated = chunks
                    .iter()
                    .take(
                        usize::try_from(self.cursor.next_chunk_index)
                            .context("analysis cursor exceeds platform usize")?,
                    )
                    .filter(|chunk| !rejected_indexes.contains(&chunk.chunk_index))
                    .cloned()
                    .collect::<Vec<_>>();
                validate_digest_against_processed_chunks(digest, processed_validated.as_slice())
                    .map_err(|_| anyhow::anyhow!("checkpoint digest grounding is invalid"))?;
            }
            _ => bail!("analysis checkpoint digest revision is inconsistent"),
        }
        let (cursor, digest) = self.encode()?;
        if cursor.len() > CURSOR_MAX_BYTES || digest.len() > DIGEST_MAX_BYTES {
            bail!("analysis checkpoint exceeds persistence bounds");
        }
        Ok(())
    }

    fn validate_counts(&self) -> Result<()> {
        validate_chunk_terminal_codes(
            &self.digest.chunk_terminal_codes,
            self.cursor.next_chunk_index,
        )?;
        let mut validated_count = 0_u32;
        for chunk_index in 0..self.cursor.next_chunk_index {
            if chunk_terminal_code(&self.digest.chunk_terminal_codes, chunk_index)?
                == CHUNK_TERMINAL_VALIDATED
            {
                validated_count = validated_count.saturating_add(1);
            }
        }
        if validated_count != self.cursor.validated_chunk_count {
            bail!("analysis checkpoint terminal counts do not cover the cursor prefix");
        }
        Ok(())
    }
}

fn validate_plan(
    run: &SelfImprovementRunRecord,
    chunks: &[SelfImprovementHistoryChunk],
) -> Result<()> {
    if chunks.is_empty() {
        bail!("self-improvement history plan must not be empty");
    }
    let chunk_count =
        u32::try_from(chunks.len()).context("history chunk count exceeds plan contract")?;
    for (index, chunk) in chunks.iter().enumerate() {
        validate_history_chunk_contract(chunk, Default::default())?;
        if chunk.workspace_id != run.workspace_id
            || chunk.source_lower_exclusive != run.source_lower_exclusive
            || chunk.source_upper_inclusive != run.source_upper_inclusive
            || chunk.chunk_index
                != u32::try_from(index).context("history chunk index exceeds plan contract")?
            || chunk.chunk_count != chunk_count
        {
            bail!("self-improvement history plan does not match the frozen run");
        }
    }
    Ok(())
}

fn plan_fingerprint(chunks: &[SelfImprovementHistoryChunk]) -> Result<String> {
    let first = chunks
        .first()
        .context("cannot fingerprint an empty history plan")?;
    let encoded = serde_json::to_vec(&PlanFingerprintInput {
        schema_version: CHECKPOINT_SCHEMA_VERSION,
        workspace_id: first.workspace_id.as_str(),
        source_lower_exclusive: first.source_lower_exclusive,
        source_upper_inclusive: first.source_upper_inclusive,
        chunk_fingerprints: chunks
            .iter()
            .map(|chunk| chunk.fingerprint.as_str())
            .collect(),
    })
    .context("failed to encode history plan fingerprint")?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

/// Stores one terminal state in two bits. Its position is the exact
/// `chunk_index`; `AnalysisCursor::plan_fingerprint` commits the deterministic
/// chunk fingerprints and therefore the thread/turn/event/fragment identity
/// without duplicating every identifier into the bounded checkpoint.
fn append_chunk_terminal_code(codes: &mut Vec<u8>, chunk_index: u32, code: u8) -> Result<()> {
    if code > CHUNK_TERMINAL_CONTRACT_REJECTED {
        bail!("analysis checkpoint terminal code is invalid");
    }
    validate_chunk_terminal_codes(codes, chunk_index)?;
    let byte_index = usize::try_from(chunk_index / CHUNKS_PER_TERMINAL_CODE_BYTE)
        .context("analysis checkpoint terminal index exceeds platform usize")?;
    let slot = chunk_index % CHUNKS_PER_TERMINAL_CODE_BYTE;
    if slot == 0 {
        codes.push(0);
    }
    let shift = slot * 2;
    let byte = codes
        .get_mut(byte_index)
        .context("analysis checkpoint terminal byte is missing")?;
    *byte |= code << shift;
    Ok(())
}

fn chunk_terminal_code(codes: &[u8], chunk_index: u32) -> Result<u8> {
    let byte_index = usize::try_from(chunk_index / CHUNKS_PER_TERMINAL_CODE_BYTE)
        .context("analysis checkpoint terminal index exceeds platform usize")?;
    let shift = (chunk_index % CHUNKS_PER_TERMINAL_CODE_BYTE) * 2;
    let byte = codes
        .get(byte_index)
        .context("analysis checkpoint terminal code is missing")?;
    Ok((byte >> shift) & CHUNK_TERMINAL_CONTRACT_REJECTED)
}

fn validate_chunk_terminal_codes(codes: &[u8], chunk_count: u32) -> Result<()> {
    let expected_bytes = usize::try_from(
        chunk_count.saturating_add(CHUNKS_PER_TERMINAL_CODE_BYTE - 1)
            / CHUNKS_PER_TERMINAL_CODE_BYTE,
    )
    .context("analysis checkpoint terminal length exceeds platform usize")?;
    if codes.len() != expected_bytes {
        bail!("analysis checkpoint terminal code length does not match its cursor");
    }
    let used_slots = chunk_count % CHUNKS_PER_TERMINAL_CODE_BYTE;
    if used_slots != 0 {
        let used_bits = used_slots * 2;
        let unused_mask = u8::MAX << used_bits;
        if codes.last().is_some_and(|byte| byte & unused_mask != 0) {
            bail!("analysis checkpoint terminal code padding is nonzero");
        }
    }
    Ok(())
}

#[cfg(test)]
fn rejection_terminal_code(reason_code: &str) -> Result<u8> {
    match reason_code {
        "model_output_too_large" => Ok(CHUNK_TERMINAL_OUTPUT_TOO_LARGE),
        "malformed_model_json" => Ok(CHUNK_TERMINAL_MALFORMED_JSON),
        "chunk_contract_rejected" => Ok(CHUNK_TERMINAL_CONTRACT_REJECTED),
        _ => bail!("chunk contract rejection reason code is invalid"),
    }
}

fn rejection_reason_code(code: u8) -> Option<&'static str> {
    match code {
        CHUNK_TERMINAL_OUTPUT_TOO_LARGE => Some("model_output_too_large"),
        CHUNK_TERMINAL_MALFORMED_JSON => Some("malformed_model_json"),
        CHUNK_TERMINAL_CONTRACT_REJECTED => Some("chunk_contract_rejected"),
        _ => None,
    }
}

mod terminal_codes_base64 {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD_NO_PAD;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S>(value: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(STANDARD_NO_PAD.encode(value).as_str())
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        STANDARD_NO_PAD
            .decode(value.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::super::history::{
        HistoryEvidenceRole, SelfImprovementHistoryBlock, SelfImprovementHistoryContent,
        SelfImprovementHistoryThread, SelfImprovementHistoryTurn,
        compute_history_chunk_fingerprint,
    };
    use super::super::learner::{
        ObservationKind, ValidatedObservation, ValidatedObservationEvidence,
    };
    use super::*;

    fn chunk(index: u32, count: u32, text: &str) -> SelfImprovementHistoryChunk {
        let mut chunk = SelfImprovementHistoryChunk {
            schema_version: 2,
            workspace_id: "workspace".to_owned(),
            source_lower_exclusive: 0,
            source_upper_inclusive: 1,
            chunk_index: index,
            chunk_count: count,
            threads: vec![SelfImprovementHistoryThread {
                thread_id: "thread".to_owned(),
                turns: vec![SelfImprovementHistoryTurn {
                    turn_id: format!("turn-{index}"),
                    blocks: vec![SelfImprovementHistoryBlock {
                        block_key: format!("event-{index}:fragment"),
                        event_id: format!("event-{index}"),
                        event_thread_id: "thread".to_owned(),
                        event_turn_id: format!("turn-{index}"),
                        sequence: 1,
                        input_index: None,
                        fragment_index: index,
                        fragment_count: count,
                        evidence_role: HistoryEvidenceRole::NewAnchor,
                        content: SelfImprovementHistoryContent::UserText {
                            text: text.to_owned(),
                        },
                    }],
                }],
            }],
            fingerprint: String::new(),
        };
        chunk.fingerprint =
            compute_history_chunk_fingerprint(&chunk).expect("fixture chunk must fingerprint");
        chunk
    }

    fn run() -> SelfImprovementRunRecord {
        SelfImprovementRunRecord {
            id: "run".to_owned(),
            workspace_id: "workspace".to_owned(),
            activation_epoch: 1,
            scheduled_date_utc: "2026-07-23".to_owned(),
            source_lower_exclusive: 0,
            source_upper_inclusive: 1,
            status: "running".to_owned(),
            claim_token: Some("token".to_owned()),
            claimed_by: Some("worker".to_owned()),
            lease_expires_at_unix: Some(100),
            attempt_count: 1,
            next_attempt_at_unix: None,
            learner_provider: "provider".to_owned(),
            learner_model: "model".to_owned(),
            learner_reasoning_effort: None,
            reviewer_provider: "provider".to_owned(),
            reviewer_model: "model".to_owned(),
            reviewer_reasoning_effort: None,
            pipeline_contract_version: "contract".to_owned(),
            analysis_cursor_json: None,
            analysis_digest_json: None,
            outcome: None,
            applied_action: None,
            skill_id: None,
            previous_version_id: None,
            resulting_version_id: None,
            result_summary: None,
            last_error: None,
            created_at_unix: 1,
            updated_at_unix: 1,
        }
    }

    #[test]
    fn checkpoint_restores_exact_next_chunk_and_never_persists_history_text() {
        let chunks = vec![
            chunk(0, 2, "first visible evidence"),
            chunk(1, 2, "final visible tail"),
        ];
        let mut checkpoint =
            ResumableHistoryAnalysis::restore(&run(), chunks.as_slice()).expect("fresh checkpoint");
        checkpoint
            .record_validated(
                &chunks[0],
                ValidatedChunkDigest {
                    digest_revision: 1,
                    observations: vec![ValidatedObservation {
                        observation_key: "stable-key".to_owned(),
                        summary: "Bounded procedural summary".to_owned(),
                        evidence: vec![ValidatedObservationEvidence {
                            chunk_fingerprint: chunks[0].fingerprint.clone(),
                            turn_id: "turn-0".to_owned(),
                            event_id: "event-0".to_owned(),
                            normalized_start: 0,
                            normalized_end: 5,
                            evidence_role: HistoryEvidenceRole::NewAnchor,
                        }],
                        kind: ObservationKind::SuccessPattern,
                    }],
                },
            )
            .expect("first chunk must checkpoint");
        let (cursor, digest) = checkpoint.encode().expect("checkpoint must encode");
        assert!(!digest.contains("first visible evidence"));
        assert!(!digest.contains("final visible tail"));

        let mut persisted = run();
        persisted.analysis_cursor_json = Some(cursor);
        persisted.analysis_digest_json = Some(digest);
        let mut restored = ResumableHistoryAnalysis::restore(&persisted, chunks.as_slice())
            .expect("checkpoint must restore");
        assert_eq!(restored.next_chunk_index(), 1);
        restored
            .record_contract_rejected(&chunks[1], "chunk_contract_rejected")
            .expect("final rejection marker must persist");
        assert!(restored.is_complete());
        let (_, digest) = restored.encode().expect("terminal checkpoint must encode");
        let persisted_digest =
            serde_json::from_str::<AnalysisDigest>(digest.as_str()).expect("digest must decode");
        let code = chunk_terminal_code(&persisted_digest.chunk_terminal_codes, 1)
            .expect("final marker code");
        assert_eq!(rejection_reason_code(code), Some("chunk_contract_rejected"));
        assert_eq!(chunks[1].threads[0].turns[0].blocks[0].event_id, "event-1");
        assert!(!digest.contains("event-1:fragment"));
        assert!(!digest.contains("final visible tail"));
    }

    #[test]
    fn checkpoint_rejects_plan_drift_and_partial_columns() {
        let chunks = vec![chunk(0, 2, "first"), chunk(1, 2, "second")];
        let mut partial = run();
        partial.analysis_cursor_json = Some("{}".to_owned());
        assert!(
            ResumableHistoryAnalysis::restore(&partial, chunks.as_slice()).is_err(),
            "one checkpoint column cannot be treated as durable progress"
        );

        let checkpoint =
            ResumableHistoryAnalysis::restore(&run(), chunks.as_slice()).expect("fresh checkpoint");
        let (cursor, digest) = checkpoint.encode().expect("checkpoint must encode");
        let mut persisted = run();
        persisted.analysis_cursor_json = Some(cursor);
        persisted.analysis_digest_json = Some(digest);
        let drifted = vec![chunk(0, 2, "changed"), chunk(1, 2, "second")];
        assert!(
            ResumableHistoryAnalysis::restore(&persisted, drifted.as_slice()).is_err(),
            "a changed deterministic plan must not reuse the old cursor"
        );
    }

    #[test]
    fn terminal_markers_remain_bounded_for_a_very_long_history() {
        const CHUNK_COUNT: u32 = 1_000_000;
        let mut codes = Vec::new();
        for index in 0..CHUNK_COUNT {
            let code = match index % 4 {
                0 => CHUNK_TERMINAL_VALIDATED,
                1 => CHUNK_TERMINAL_OUTPUT_TOO_LARGE,
                2 => CHUNK_TERMINAL_MALFORMED_JSON,
                _ => CHUNK_TERMINAL_CONTRACT_REJECTED,
            };
            append_chunk_terminal_code(&mut codes, index, code)
                .expect("terminal marker must append");
        }
        validate_chunk_terminal_codes(codes.as_slice(), CHUNK_COUNT)
            .expect("terminal marker sequence must validate");
        assert_eq!(
            chunk_terminal_code(codes.as_slice(), CHUNK_COUNT - 1)
                .expect("last marker must decode"),
            CHUNK_TERMINAL_CONTRACT_REJECTED
        );

        let encoded = serde_json::to_string(&AnalysisDigest {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            validated: None,
            chunk_terminal_codes: codes,
        })
        .expect("compact terminal markers must encode");
        assert!(
            encoded.len() < DIGEST_MAX_BYTES,
            "one million terminal markers must fit the checkpoint bound"
        );
    }
}
