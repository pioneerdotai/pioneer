use crate::BackendPayload;
use anyhow::{Context, Result, bail};
use pioneer_crud::{AgentMemoryCandidateRecord, AgentMemoryControlRecord, MemoryActorRecord};
use pioneer_protocol::{
    MemoryActor, MemoryCandidate, MemoryProvenance, MemoryRecord, MemoryRememberParams,
};
use std::collections::BTreeMap;

pub(crate) fn protocol_actor_to_crud(actor: Option<MemoryActor>) -> Option<MemoryActorRecord> {
    actor.map(|actor| MemoryActorRecord {
        kind: actor.kind,
        id: actor.id,
    })
}

pub(crate) fn crud_actor_to_protocol(actor: Option<MemoryActorRecord>) -> Option<MemoryActor> {
    actor.map(|actor| MemoryActor {
        kind: actor.kind,
        id: actor.id,
    })
}

pub(crate) fn effective_provenance(
    params: &MemoryRememberParams,
    context_actor: Option<MemoryActor>,
) -> MemoryProvenance {
    match params.provenance.clone() {
        Some(mut provenance) => {
            if provenance.created_by.is_none() {
                provenance.created_by = context_actor;
            }
            provenance
        }
        None => MemoryProvenance {
            source_kind: pioneer_protocol::MemorySourceKind::ExplicitUserRequest,
            source_thread_id: None,
            source_turn_id: None,
            source_item_id: None,
            created_by: context_actor,
        },
    }
}

pub(crate) fn crud_record_to_protocol(
    record: AgentMemoryControlRecord,
    payload: BackendPayload,
) -> Result<MemoryRecord> {
    if payload.memory_id != record.id {
        bail!(
            "backend payload id `{}` does not match control-plane memory `{}`",
            payload.memory_id,
            record.id
        );
    }
    let metadata = merge_metadata(
        record.metadata_json.as_deref(),
        payload.metadata_json.as_deref(),
    )
    .with_context(|| format!("failed to parse metadata for memory `{}`", record.id))?;

    Ok(MemoryRecord {
        id: record.id,
        scope: record.scope,
        namespace: namespace_to_protocol(record.namespace),
        category: record.category,
        key: record.key,
        content: payload.content,
        status: record.status,
        confidence: checked_f32(record.confidence, "confidence")?,
        importance: checked_f32(record.importance, "importance")?,
        sensitivity: record.sensitivity,
        provenance: MemoryProvenance {
            source_kind: record.source_kind,
            source_thread_id: record.source_thread_id,
            source_turn_id: record.source_turn_id,
            source_item_id: record.source_item_id,
            created_by: crud_actor_to_protocol(record.created_by),
        },
        created_at: record.created_at_unix,
        updated_at: record.updated_at_unix,
        expires_at: record.expires_at_unix,
        last_accessed_at: record.last_accessed_at_unix,
        access_count: checked_u64(record.access_count, "access_count")?,
        superseded_by: record.superseded_by,
        deleted_at: record.deleted_at_unix,
        delete_reason: record.delete_reason,
        metadata,
    })
}

#[allow(dead_code)]
pub(crate) fn crud_candidate_to_protocol(
    candidate: AgentMemoryCandidateRecord,
) -> Result<MemoryCandidate> {
    Ok(MemoryCandidate {
        id: candidate.id,
        scope: candidate.scope,
        category: candidate.category,
        key: candidate.key,
        candidate_text: candidate.candidate_text,
        confidence: checked_f32(candidate.confidence, "confidence")?,
        reason: candidate.reason,
        provenance: MemoryProvenance {
            source_kind: candidate.source_kind,
            source_thread_id: candidate.source_thread_id,
            source_turn_id: candidate.source_turn_id,
            source_item_id: candidate.source_item_id,
            created_by: crud_actor_to_protocol(candidate.created_by),
        },
        status: candidate.status,
        created_at: candidate.created_at_unix,
        decided_at: candidate.decided_at_unix,
        decision_reason: candidate.decision_reason,
        metadata: parse_metadata_json(candidate.metadata_json.as_deref())?,
    })
}

pub(crate) fn metadata_to_json(
    metadata: &BTreeMap<String, serde_json::Value>,
) -> Result<Option<String>> {
    if metadata.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        serde_json::to_string(metadata).context("failed to encode memory metadata")?,
    ))
}

pub(crate) fn metadata_with_idempotency(
    metadata: &BTreeMap<String, serde_json::Value>,
    idempotency_key: Option<&str>,
) -> Result<Option<String>> {
    let mut metadata = metadata.clone();
    if let Some(idempotency_key) = idempotency_key {
        metadata.insert(
            "idempotency_key".to_owned(),
            serde_json::Value::String(idempotency_key.to_owned()),
        );
    }
    metadata_to_json(&metadata)
}

pub(crate) fn parse_metadata_json(
    metadata_json: Option<&str>,
) -> Result<BTreeMap<String, serde_json::Value>> {
    match metadata_json {
        Some(metadata_json) => serde_json::from_str(metadata_json)
            .with_context(|| format!("invalid memory metadata JSON `{metadata_json}`")),
        None => Ok(BTreeMap::new()),
    }
}

pub(crate) fn content_preview(content: &str, max_chars: usize) -> Option<String> {
    if max_chars == 0 {
        return None;
    }
    Some(content.chars().take(max_chars).collect())
}

fn merge_metadata(
    control_plane_json: Option<&str>,
    backend_json: Option<&str>,
) -> Result<BTreeMap<String, serde_json::Value>> {
    let mut metadata = parse_metadata_json(control_plane_json)?;
    for (key, value) in parse_metadata_json(backend_json)? {
        metadata.entry(key).or_insert(value);
    }
    Ok(metadata)
}

fn namespace_to_protocol(namespace: String) -> Option<String> {
    (namespace != "default").then_some(namespace)
}

fn checked_f32(value: f64, field: &str) -> Result<f32> {
    if !value.is_finite() {
        bail!("memory `{field}` must be finite");
    }
    if value > f32::MAX as f64 || value < f32::MIN as f64 {
        bail!("memory `{field}` is outside f32 range");
    }
    Ok(value as f32)
}

fn checked_u64(value: i64, field: &str) -> Result<u64> {
    if value < 0 {
        bail!("memory `{field}` cannot be negative");
    }
    Ok(value as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_roundtrips() {
        let mut metadata = BTreeMap::new();
        metadata.insert("a".to_owned(), serde_json::json!(1));
        let json = metadata_to_json(&metadata)
            .expect("metadata encode")
            .expect("metadata should encode");
        assert_eq!(parse_metadata_json(Some(json.as_str())).unwrap(), metadata);
    }

    #[test]
    fn preview_is_bounded_by_chars() {
        assert_eq!(content_preview("abcdef", 3).as_deref(), Some("abc"));
    }
}
