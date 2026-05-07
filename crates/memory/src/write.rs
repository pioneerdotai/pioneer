use anyhow::{Context, Result, bail};
use pioneer_protocol::{
    MemoryAttribute, MemoryAttributeCardinality, MemoryCanonicalKey, MemoryCategory, MemoryScope,
    MemoryScopeHint, MemorySemanticFields, MemorySubject, MemoryWriteEvidence,
};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(crate) const SEMANTIC_METADATA_KEY: &str = "semantic";
pub(crate) const EVIDENCE_METADATA_KEY: &str = "evidence";
pub(crate) const CLIENT_PROVIDED_KEY_METADATA_KEY: &str = "client_provided_key";

const SEMANTIC_FINGERPRINT_VERSION: &str = "semantic_fingerprint_v1";
const CUSTOM_HASH_CHARS: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticWritePrepared {
    pub canonical: MemoryCanonicalKey,
    pub semantic_fingerprint: String,
    pub dedupe_key: String,
    pub normalized_value: String,
}

pub(crate) fn prepare_semantic_write(
    scope: &MemoryScope,
    semantic: &MemorySemanticFields,
    value: &str,
) -> Result<SemanticWritePrepared> {
    let canonical = build_memory_canonical_key(scope, semantic)?;
    let normalized_value = normalize_semantic_text(value);
    if normalized_value.is_empty() {
        bail!("semantic memory value cannot be empty");
    }
    let semantic_fingerprint =
        semantic_memory_fingerprint(scope, &canonical, semantic, normalized_value.as_str());
    let dedupe_key = format!("semantic:{semantic_fingerprint}");
    Ok(SemanticWritePrepared {
        canonical,
        semantic_fingerprint,
        dedupe_key,
        normalized_value,
    })
}

pub fn build_memory_canonical_key(
    scope: &MemoryScope,
    semantic: &MemorySemanticFields,
) -> Result<MemoryCanonicalKey> {
    let category = semantic.category;
    let category_label = category_key(category);
    let subject_label = subject_key(scope, semantic)?;
    let attribute_label = attribute_key(semantic)?;
    let cardinality = attribute_cardinality(category, semantic.attribute);
    let scope_label = scope_key(scope, semantic.scope_hint);

    let key = match cardinality {
        MemoryAttributeCardinality::SingleValue => {
            format!("{scope_label}:{category_label}:{subject_label}:{attribute_label}")
        }
        MemoryAttributeCardinality::MultiValue | MemoryAttributeCardinality::SetMembership => {
            let item_key = semantic
                .subject_key
                .as_deref()
                .or(semantic.custom_subject.as_deref())
                .map(normalize_key_component)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| short_hash(subject_label.as_str()));
            format!("{scope_label}:{category_label}:{subject_label}:{attribute_label}:{item_key}")
        }
    };

    Ok(MemoryCanonicalKey {
        key,
        scope: scope.clone(),
        namespace: "default".to_owned(),
        category,
        cardinality,
    })
}

pub fn semantic_memory_fingerprint(
    scope: &MemoryScope,
    canonical: &MemoryCanonicalKey,
    semantic: &MemorySemanticFields,
    normalized_value: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(SEMANTIC_FINGERPRINT_VERSION.as_bytes());
    hasher.update([0]);
    hasher.update(memory_scope_kind_label(scope.kind).as_bytes());
    hasher.update([0]);
    hasher.update(normalize_semantic_text(scope.key.as_str()).as_bytes());
    hasher.update([0]);
    hasher.update(canonical.key.as_bytes());
    hasher.update([0]);
    hasher.update(category_key(semantic.category).as_bytes());
    hasher.update([0]);
    hasher.update(normalized_value.as_bytes());
    hex::encode(hasher.finalize())
}

pub(crate) fn semantic_metadata(
    semantic: &MemorySemanticFields,
    prepared: &SemanticWritePrepared,
    client_provided_key: Option<&str>,
    source: Option<&str>,
) -> BTreeMap<String, Value> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        SEMANTIC_METADATA_KEY.to_owned(),
        json!({
            "canonical_key": prepared.canonical.key,
            "canonical_namespace": prepared.canonical.namespace,
            "canonical": prepared.canonical,
            "fields": semantic,
            "semantic_fingerprint": prepared.semantic_fingerprint,
            "dedupe_key": prepared.dedupe_key,
            "normalized_value": prepared.normalized_value,
            "category": category_key(semantic.category),
            "subject": format!("{:?}", semantic.subject),
            "attribute": format!("{:?}", semantic.attribute),
            "cardinality": format!("{:?}", prepared.canonical.cardinality),
            "source": source,
        }),
    );
    if let Some(client_provided_key) = client_provided_key
        && !client_provided_key.trim().is_empty()
    {
        metadata.insert(
            CLIENT_PROVIDED_KEY_METADATA_KEY.to_owned(),
            json!(client_provided_key.trim()),
        );
    }
    metadata
}

pub(crate) fn merge_metadata(
    existing_json: Option<&str>,
    incoming: BTreeMap<String, Value>,
    evidence: Option<&MemoryWriteEvidence>,
    now_unix: i64,
) -> Result<String> {
    let mut root = match existing_json {
        Some(existing_json) if !existing_json.trim().is_empty() => {
            serde_json::from_str::<Map<String, Value>>(existing_json)
                .with_context(|| format!("invalid memory metadata JSON `{existing_json}`"))?
        }
        _ => Map::new(),
    };

    for (key, value) in incoming {
        root.insert(key, value);
    }

    merge_evidence_metadata(&mut root, evidence, now_unix);
    Ok(Value::Object(root).to_string())
}

pub(crate) fn metadata_normalized_value(metadata_json: Option<&str>) -> Option<String> {
    let metadata = serde_json::from_str::<Value>(metadata_json?).ok()?;
    metadata
        .get(SEMANTIC_METADATA_KEY)?
        .get("normalized_value")?
        .as_str()
        .map(ToOwned::to_owned)
}

pub(crate) fn normalize_semantic_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn merge_evidence_metadata(
    root: &mut Map<String, Value>,
    evidence: Option<&MemoryWriteEvidence>,
    now_unix: i64,
) {
    let evidence_entry = root
        .entry(EVIDENCE_METADATA_KEY.to_owned())
        .or_insert_with(|| json!({}));
    let evidence_object = match evidence_entry {
        Value::Object(object) => object,
        _ => {
            *evidence_entry = json!({});
            evidence_entry
                .as_object_mut()
                .expect("evidence object was just initialized")
        }
    };

    let count = evidence_object
        .get("count")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        + 1;
    evidence_object.insert("count".to_owned(), json!(count));
    evidence_object
        .entry("first_seen_at".to_owned())
        .or_insert_with(|| json!(now_unix));
    evidence_object.insert("last_seen_at".to_owned(), json!(now_unix));

    if let Some(evidence) = evidence {
        let source = json!({
            "source_thread_id": evidence.source_thread_id,
            "source_turn_id": evidence.source_turn_id,
            "source_item_id": evidence.source_item_id,
            "source_ref": evidence.source_ref,
            "quote_or_span": evidence.quote_or_span,
            "extractor_reason": evidence.extractor_reason,
        });
        let sources = evidence_object
            .entry("sources".to_owned())
            .or_insert_with(|| json!([]));
        if let Value::Array(sources) = sources
            && !sources.contains(&source)
        {
            sources.push(source);
        }
    }
}

fn attribute_cardinality(
    category: MemoryCategory,
    attribute: MemoryAttribute,
) -> MemoryAttributeCardinality {
    match (category, attribute) {
        (MemoryCategory::Identity, MemoryAttribute::Name)
        | (MemoryCategory::Identity, MemoryAttribute::Birthday)
        | (MemoryCategory::Preference, MemoryAttribute::PreferredLanguage)
        | (MemoryCategory::Preference, MemoryAttribute::CommunicationStyle)
        | (MemoryCategory::CommunicationStyle, MemoryAttribute::CommunicationStyle)
        | (MemoryCategory::ProjectPolicy, MemoryAttribute::MigrationPolicy)
        | (MemoryCategory::ProjectDecision, MemoryAttribute::PhaseNaming) => {
            MemoryAttributeCardinality::SingleValue
        }
        (MemoryCategory::Relationship, _) => MemoryAttributeCardinality::MultiValue,
        _ => MemoryAttributeCardinality::SingleValue,
    }
}

fn scope_key(scope: &MemoryScope, hint: MemoryScopeHint) -> String {
    match hint {
        MemoryScopeHint::UserGlobal => "user/global".to_owned(),
        MemoryScopeHint::UserWorkspace => format!("user/{}", normalize_key_component(&scope.key)),
        MemoryScopeHint::AgentGlobal => "agent/global".to_owned(),
        MemoryScopeHint::AgentWorkspace => format!("agent/{}", normalize_key_component(&scope.key)),
        MemoryScopeHint::ProjectWorkspace => {
            format!("workspace/{}", normalize_key_component(&scope.key))
        }
        MemoryScopeHint::Unknown => {
            format!(
                "{}/{}",
                memory_scope_kind_label(scope.kind),
                normalize_key_component(&scope.key)
            )
        }
    }
}

fn subject_key(scope: &MemoryScope, semantic: &MemorySemanticFields) -> Result<String> {
    let subject = match semantic.subject {
        MemorySubject::CurrentUser | MemorySubject::CurrentAgent => "self".to_owned(),
        MemorySubject::Workspace => normalize_key_component(&scope.key),
        MemorySubject::Project
        | MemorySubject::Person
        | MemorySubject::Organization
        | MemorySubject::Artifact => semantic
            .subject_key
            .as_deref()
            .map(normalize_key_component)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("semantic subject key is required"))?,
        MemorySubject::Custom => semantic
            .custom_subject
            .as_deref()
            .map(normalize_key_component)
            .filter(|value| !value.is_empty())
            .map(|value| format!("custom_{}", short_hash(value.as_str())))
            .ok_or_else(|| anyhow::anyhow!("custom semantic subject is required"))?,
    };
    Ok(subject)
}

fn attribute_key(semantic: &MemorySemanticFields) -> Result<String> {
    let attribute = match semantic.attribute {
        MemoryAttribute::Name => "name".to_owned(),
        MemoryAttribute::Birthday => "birthday".to_owned(),
        MemoryAttribute::PreferredLanguage => "preferred_language".to_owned(),
        MemoryAttribute::CommunicationStyle => "communication_style".to_owned(),
        MemoryAttribute::MigrationPolicy => "migration_file_policy".to_owned(),
        MemoryAttribute::ReviewStyle => "review_style".to_owned(),
        MemoryAttribute::PhaseNaming => "phase_naming".to_owned(),
        MemoryAttribute::Custom => semantic
            .custom_attribute
            .as_deref()
            .map(normalize_key_component)
            .filter(|value| !value.is_empty())
            .map(|value| format!("custom_{}", short_hash(value.as_str())))
            .ok_or_else(|| anyhow::anyhow!("custom semantic attribute is required"))?,
    };
    Ok(attribute)
}

fn category_key(category: MemoryCategory) -> &'static str {
    match category {
        MemoryCategory::Identity => "identity",
        MemoryCategory::Preference => "preference",
        MemoryCategory::Biography => "biography",
        MemoryCategory::Relationship => "relationship",
        MemoryCategory::RecurringInstruction => "recurring_instruction",
        MemoryCategory::ProjectPolicy => "project_policy",
        MemoryCategory::ProjectFact => "project_fact",
        MemoryCategory::ProjectDecision => "project_decision",
        MemoryCategory::Procedure => "procedure",
        MemoryCategory::Todo => "todo",
        MemoryCategory::Constraint => "constraint",
        MemoryCategory::CommunicationStyle => "communication_style",
        MemoryCategory::Custom => "custom",
    }
}

fn memory_scope_kind_label(kind: pioneer_protocol::MemoryScopeKind) -> &'static str {
    match kind {
        pioneer_protocol::MemoryScopeKind::User => "user",
        pioneer_protocol::MemoryScopeKind::Workspace => "workspace",
        pioneer_protocol::MemoryScopeKind::Thread => "thread",
        pioneer_protocol::MemoryScopeKind::Agent => "agent",
        pioneer_protocol::MemoryScopeKind::Task => "task",
    }
}

fn normalize_key_component(value: &str) -> String {
    let mut normalized = String::new();
    for ch in value.trim().chars() {
        if ch.is_alphanumeric() {
            for lower in ch.to_lowercase() {
                normalized.push(lower);
            }
        } else {
            normalized.push('_');
        }
    }
    normalized
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

fn short_hash(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
        .chars()
        .take(CUSTOM_HASH_CHARS)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        MemoryDurability, MemoryExplicitness, MemoryExtractorCertainty, MemoryIntent,
        MemoryScopeKind, MemorySensitivityHint,
    };

    fn semantic(
        category: MemoryCategory,
        subject: MemorySubject,
        attribute: MemoryAttribute,
    ) -> MemorySemanticFields {
        MemorySemanticFields {
            intent: MemoryIntent::ExplicitStore,
            explicitness: MemoryExplicitness::Explicit,
            category,
            subject,
            attribute,
            subject_key: None,
            custom_subject: None,
            custom_attribute: None,
            scope_hint: MemoryScopeHint::UserGlobal,
            durability: MemoryDurability::LongLived,
            sensitivity: MemorySensitivityHint::None,
            certainty: MemoryExtractorCertainty::High,
        }
    }

    #[test]
    fn canonical_user_identity_name_key_is_stable() {
        let scope = MemoryScope {
            kind: MemoryScopeKind::User,
            key: "default".to_owned(),
        };
        let key = build_memory_canonical_key(
            &scope,
            &semantic(
                MemoryCategory::Identity,
                MemorySubject::CurrentUser,
                MemoryAttribute::Name,
            ),
        )
        .expect("canonical key");
        assert_eq!(key.key, "user/global:identity:self:name");
        assert_eq!(key.cardinality, MemoryAttributeCardinality::SingleValue);
    }

    #[test]
    fn fingerprint_uses_semantic_value_not_surface_language() {
        let scope = MemoryScope {
            kind: MemoryScopeKind::User,
            key: "default".to_owned(),
        };
        let semantic = semantic(
            MemoryCategory::Identity,
            MemorySubject::CurrentUser,
            MemoryAttribute::Name,
        );
        let first = prepare_semantic_write(&scope, &semantic, "Александр").expect("first");
        let second = prepare_semantic_write(&scope, &semantic, "Александр").expect("second");
        assert_eq!(first.canonical.key, second.canonical.key);
        assert_eq!(first.semantic_fingerprint, second.semantic_fingerprint);
    }

    #[test]
    fn canonical_project_policy_examples_are_stable() {
        let scope = MemoryScope {
            kind: MemoryScopeKind::Workspace,
            key: "pioneer".to_owned(),
        };
        let mut policy = semantic(
            MemoryCategory::ProjectPolicy,
            MemorySubject::Project,
            MemoryAttribute::MigrationPolicy,
        );
        policy.scope_hint = MemoryScopeHint::ProjectWorkspace;
        policy.subject_key = Some("pioneer".to_owned());
        let policy_key = build_memory_canonical_key(&scope, &policy).expect("policy key");
        assert_eq!(
            policy_key.key,
            "workspace/pioneer:project_policy:pioneer:migration_file_policy"
        );

        let mut decision = semantic(
            MemoryCategory::ProjectDecision,
            MemorySubject::Project,
            MemoryAttribute::PhaseNaming,
        );
        decision.scope_hint = MemoryScopeHint::ProjectWorkspace;
        decision.subject_key = Some("proposal-07".to_owned());
        let decision_key = build_memory_canonical_key(&scope, &decision).expect("decision key");
        assert_eq!(
            decision_key.key,
            "workspace/pioneer:project_decision:proposal_07:phase_naming"
        );
    }

    #[test]
    fn single_value_canonical_key_does_not_include_value() {
        let scope = MemoryScope {
            kind: MemoryScopeKind::User,
            key: "default".to_owned(),
        };
        let semantic = semantic(
            MemoryCategory::Identity,
            MemorySubject::CurrentUser,
            MemoryAttribute::Name,
        );
        let first = prepare_semantic_write(&scope, &semantic, "Alexander").expect("first");
        let second = prepare_semantic_write(&scope, &semantic, "Alex").expect("second");
        assert_eq!(first.canonical.key, second.canonical.key);
        assert_ne!(first.semantic_fingerprint, second.semantic_fingerprint);
    }

    #[test]
    fn canonical_custom_fallback_accepts_unicode_components() {
        let scope = MemoryScope {
            kind: MemoryScopeKind::Workspace,
            key: "проект".to_owned(),
        };
        let mut semantic = semantic(
            MemoryCategory::Custom,
            MemorySubject::Custom,
            MemoryAttribute::Custom,
        );
        semantic.scope_hint = MemoryScopeHint::ProjectWorkspace;
        semantic.custom_subject = Some("Александр".to_owned());
        semantic.custom_attribute = Some("День рождения".to_owned());
        let key = build_memory_canonical_key(&scope, &semantic).expect("unicode custom key");
        assert!(key.key.starts_with("workspace/проект:custom:custom_"));
        assert!(key.key.contains(":custom_"));
    }
}
