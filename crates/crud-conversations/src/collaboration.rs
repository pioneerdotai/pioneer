//! Persisted Turn collaboration and immutable authorship.

use anyhow::{Context, Result, bail};
use pioneer_crud_access::identity::actor_ref_from_db;
use pioneer_entity::turn;
use pioneer_protocol::{
    NewMemberProfile, PersistedActorRef, ThreadMode, TurnAuthorSnapshot, TurnMention,
};

use crate::values::thread_mode_to_db;

pub const TURN_MENTIONS_JSON_MAX_BYTES: usize = 65_536;
pub const TURN_MENTION_MAX_COUNT: usize = 64;
pub const TURN_AUTHOR_AVATAR_REVISION_MAX_BYTES: usize = 64;

pub const TURN_AGENT_AUTHOR_SNAPSHOT_JSON_MAX_BYTES: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedTurnCollaboration {
    pub mode: PersistedTurnSendMode,
    pub author: Option<TurnAuthorSnapshot>,
    pub reply_to_turn_id: Option<String>,
    pub mentions: Vec<TurnMention>,
    pub message_revision: u64,
    pub message_deleted: bool,
}

/// Read-time interpretation of a persisted Turn mode.
///
/// A pre-Epic-6 null value renders as Chat for compatibility, but it never
/// becomes eligible for Message mutations. Eligibility is derived only from
/// the raw persisted `message` discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedTurnSendMode {
    pub effective_mode: ThreadMode,
    pub message_mutation_eligible: bool,
}

pub fn turn_send_mode_to_db(mode: ThreadMode) -> &'static str {
    thread_mode_to_db(mode)
}

pub fn turn_send_mode_from_db(value: Option<&str>) -> Result<PersistedTurnSendMode> {
    match value {
        None => Ok(PersistedTurnSendMode {
            effective_mode: ThreadMode::Chat,
            message_mutation_eligible: false,
        }),
        Some("message") => Ok(PersistedTurnSendMode {
            effective_mode: ThreadMode::Message,
            message_mutation_eligible: true,
        }),
        Some("chat") => Ok(PersistedTurnSendMode {
            effective_mode: ThreadMode::Chat,
            message_mutation_eligible: false,
        }),
        Some("agent") => Ok(PersistedTurnSendMode {
            effective_mode: ThreadMode::Agent,
            message_mutation_eligible: false,
        }),
        Some(unknown) => bail!("unknown persisted Turn send mode `{unknown}`"),
    }
}

pub fn canonical_turn_mentions_json(mentions: &[TurnMention]) -> Result<String> {
    if mentions.len() > TURN_MENTION_MAX_COUNT {
        bail!("Turn mentions exceed the maximum of {TURN_MENTION_MAX_COUNT} entries");
    }

    let mut canonical = mentions.to_vec();
    canonical.sort_by(|left, right| left.principal_id.cmp(&right.principal_id));
    canonical.dedup_by(|left, right| left.principal_id == right.principal_id);
    for mention in &canonical {
        validate_turn_mention(mention)?;
    }

    let encoded = serde_json::to_string(&canonical).context("failed to encode Turn mentions")?;
    if encoded.len() > TURN_MENTIONS_JSON_MAX_BYTES {
        bail!("encoded Turn mentions exceed {TURN_MENTIONS_JSON_MAX_BYTES} bytes");
    }
    Ok(encoded)
}

pub fn turn_mentions_from_db(value: &str) -> Result<Vec<TurnMention>> {
    if value.len() > TURN_MENTIONS_JSON_MAX_BYTES {
        bail!("persisted Turn mentions exceed {TURN_MENTIONS_JSON_MAX_BYTES} bytes");
    }
    let decoded = serde_json::from_str::<Vec<TurnMention>>(value)
        .context("failed to decode persisted Turn mentions")?;
    let canonical = canonical_turn_mentions_json(&decoded)?;
    let canonical_decoded = serde_json::from_str::<Vec<TurnMention>>(&canonical)
        .context("failed to decode canonical Turn mentions")?;
    Ok(canonical_decoded)
}

pub fn validate_turn_author_snapshot(snapshot: &TurnAuthorSnapshot) -> Result<()> {
    let validated = NewMemberProfile::new(
        snapshot.display_name.clone(),
        snapshot.nickname.clone(),
        None,
    )
    .context("invalid Turn author profile snapshot")?;
    if validated.display_name != snapshot.display_name {
        bail!("Turn author display-name snapshot is not canonical");
    }
    if snapshot.avatar_revision.as_ref().is_some_and(|revision| {
        revision.is_empty()
            || revision.len() > TURN_AUTHOR_AVATAR_REVISION_MAX_BYTES
            || revision.chars().any(char::is_control)
    }) {
        bail!("invalid Turn author avatar revision snapshot");
    }
    match (&snapshot.actor, snapshot.agent.as_ref()) {
        (PersistedActorRef::AgentExecution(execution_id), Some(agent)) => {
            if &agent.agent_execution_id != execution_id
                || agent.display_name != snapshot.display_name
                || agent.nickname != snapshot.nickname
                || agent.avatar_revision != snapshot.avatar_revision
            {
                bail!("Turn agent author snapshot does not match its immutable author fields");
            }
            if agent.identity_source_revision == 0 {
                bail!("Turn agent author snapshot has a zero source revision");
            }
            pioneer_protocol::AgentDisplayName::new(agent.display_name.clone())
                .context("invalid Turn agent display-name snapshot")?;
            pioneer_protocol::AgentNicknameKey::new(agent.nickname.clone())
                .context("invalid Turn agent nickname snapshot")?;
            if let Some(role_label) = agent.role_label.as_ref() {
                pioneer_protocol::AgentRoleLabel::new(role_label.clone())
                    .context("invalid Turn agent role-label snapshot")?;
            }
        }
        (PersistedActorRef::AgentExecution(_), None) => {
            bail!("AgentExecution Turn author has no immutable presentation snapshot")
        }
        (_, Some(_)) => bail!("non-agent Turn author carries an agent presentation snapshot"),
        (_, None) => {}
    }
    Ok(())
}

fn validate_turn_mention(mention: &TurnMention) -> Result<()> {
    NewMemberProfile::new("Mention snapshot", mention.nickname.clone(), None)
        .context("invalid Turn mention nickname snapshot")?;
    Ok(())
}

pub fn collaboration_from_model(model: &turn::Model) -> Result<PersistedTurnCollaboration> {
    let mode = turn_send_mode_from_db(model.send_mode.as_deref())?;
    let actor = actor_ref_from_db(
        model.initiated_by_actor_kind.as_deref(),
        model.initiated_by_actor_id.as_deref(),
    )
    .context("Turn has an invalid persisted initiator pair")?;
    if model.author_agent_snapshot_json.is_some()
        && (!matches!(actor.as_ref(), Some(PersistedActorRef::AgentExecution(_)))
            || model.author_display_name_snapshot.is_none()
            || model.author_nickname_snapshot.is_none())
    {
        anyhow::bail!("Turn has an agent author snapshot outside a complete AgentExecution author");
    }
    let author = match (
        actor,
        model.author_display_name_snapshot.as_ref(),
        model.author_nickname_snapshot.as_ref(),
    ) {
        (Some(actor), Some(display_name), Some(nickname)) => {
            let agent = model
                .author_agent_snapshot_json
                .as_deref()
                .map(|json| {
                    if json.len() > TURN_AGENT_AUTHOR_SNAPSHOT_JSON_MAX_BYTES {
                        anyhow::bail!(
                            "persisted Turn agent author snapshot exceeds {TURN_AGENT_AUTHOR_SNAPSHOT_JSON_MAX_BYTES} bytes"
                        );
                    }
                    serde_json::from_str(json)
                        .context("failed to decode persisted Turn agent author snapshot")
                })
                .transpose()?;
            let snapshot = TurnAuthorSnapshot {
                cli_runtime_kind: None,
                actor,
                display_name: display_name.clone(),
                nickname: nickname.clone(),
                avatar_revision: model.author_avatar_revision_snapshot.clone(),
                agent,
            };
            validate_turn_author_snapshot(&snapshot)?;
            Some(snapshot)
        }
        (None, None, None) => None,
        (Some(actor), None, None) => {
            let (display_name, nickname) = match &actor {
                PersistedActorRef::Principal(principal_id) => {
                    (principal_id.to_string(), principal_id.to_string())
                }
                PersistedActorRef::AgentExecution(execution_id) => {
                    (execution_id.to_string(), format!("agent-{}", execution_id))
                }
                PersistedActorRef::System => ("System".to_owned(), "system".to_owned()),
            };
            Some(TurnAuthorSnapshot {
                cli_runtime_kind: None,
                actor,
                display_name,
                nickname,
                avatar_revision: None,
                agent: None,
            })
        }
        _ => anyhow::bail!("Turn has an incomplete persisted author snapshot"),
    };
    let message_revision = u64::try_from(model.message_revision)
        .context("Turn has a negative persisted message revision")?;
    Ok(PersistedTurnCollaboration {
        mode,
        author,
        reply_to_turn_id: model.reply_to_turn_id.clone(),
        mentions: turn_mentions_from_db(model.mentions_json.as_str())?,
        message_revision,
        message_deleted: model.message_deleted_at.is_some(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::PrincipalId;

    #[test]
    fn persisted_turn_mode_keeps_legacy_null_history_immutable() {
        let legacy = turn_send_mode_from_db(None).expect("legacy null mode should remain readable");
        assert_eq!(legacy.effective_mode, ThreadMode::Chat);
        assert!(!legacy.message_mutation_eligible);

        let message = turn_send_mode_from_db(Some(turn_send_mode_to_db(ThreadMode::Message)))
            .expect("message mode should decode");
        assert_eq!(message.effective_mode, ThreadMode::Message);
        assert!(message.message_mutation_eligible);
        assert!(turn_send_mode_from_db(Some("Message")).is_err());
    }

    #[test]
    fn turn_mentions_are_deduplicated_and_encoded_in_principal_order() {
        let principal_a = PrincipalId::new("P00000000000000000001").unwrap();
        let principal_b = PrincipalId::new("P00000000000000000002").unwrap();
        let encoded = canonical_turn_mentions_json(&[
            TurnMention {
                principal_id: principal_b.clone(),
                nickname: "member_b".to_owned(),
            },
            TurnMention {
                principal_id: principal_a.clone(),
                nickname: "member_a".to_owned(),
            },
            TurnMention {
                principal_id: principal_b,
                nickname: "member_b".to_owned(),
            },
        ])
        .expect("mentions should encode");
        let decoded = turn_mentions_from_db(encoded.as_str()).expect("mentions should decode");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].principal_id, principal_a);
        assert_eq!(canonical_turn_mentions_json(&decoded).unwrap(), encoded);
    }

    #[test]
    fn legacy_null_send_mode_is_chat_compatible_but_not_message_mutable() {
        let model = collaboration_test_turn(None, None, None, "[]");
        let collaboration = collaboration_from_model(&model).expect("legacy Turn should decode");
        assert_eq!(collaboration.mode.effective_mode, ThreadMode::Chat);
        assert!(!collaboration.mode.message_mutation_eligible);
        assert_eq!(
            collaboration
                .author
                .expect("stable legacy fallback")
                .display_name,
            "System"
        );
    }

    #[test]
    fn persisted_message_collaboration_roundtrips_without_a_second_identity() {
        let model = collaboration_test_turn(
            Some("message"),
            Some("Member A"),
            Some("member_a"),
            r#"[{"principal_id":"P00000000000000000002","nickname":"member_b"}]"#,
        );
        let collaboration = collaboration_from_model(&model).expect("Message Turn should decode");
        assert_eq!(collaboration.mode.effective_mode, ThreadMode::Message);
        assert!(collaboration.mode.message_mutation_eligible);
        assert_eq!(collaboration.mentions.len(), 1);
        assert_eq!(collaboration.message_revision, 2);
        assert!(collaboration.message_deleted);
        assert_eq!(
            collaboration.author.expect("author snapshot").actor,
            PersistedActorRef::Principal(
                pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap()
            )
        );
    }

    #[test]
    fn persisted_agent_collaboration_roundtrips_the_exact_immutable_snapshot() {
        let mut model =
            collaboration_test_turn(Some("message"), Some("Builder"), Some("builder"), "[]");
        model.initiated_by_actor_kind = Some("agent_execution".to_owned());
        model.initiated_by_actor_id = Some("E00000000000000000001".to_owned());
        model.author_agent_snapshot_json = Some(
            serde_json::to_string(&pioneer_protocol::AgentPresentationSnapshot {
                agent_identity_id: pioneer_protocol::AgentIdentityId::new("I00000000000000000001")
                    .unwrap(),
                agent_execution_id: pioneer_protocol::AgentExecutionId::new(
                    "E00000000000000000001",
                )
                .unwrap(),
                identity_source_kind: pioneer_protocol::AgentIdentitySourceKind::NativeAgent,
                identity_source_revision: 7,
                display_name: "Builder".to_owned(),
                nickname: "builder".to_owned(),
                avatar_revision: Some("avatar-7".to_owned()),
                role_label: Some("Reviewer".to_owned()),
            })
            .unwrap(),
        );
        model.author_avatar_revision_snapshot = Some("avatar-7".to_owned());

        let author = collaboration_from_model(&model)
            .expect("exact Agent author should decode")
            .author
            .expect("Agent author");
        assert_eq!(author.display_name, "Builder");
        assert_eq!(author.nickname, "builder");
        assert_eq!(
            author
                .agent
                .expect("rich Agent snapshot")
                .role_label
                .as_deref(),
            Some("Reviewer")
        );
    }

    #[test]
    fn persisted_agent_snapshot_cannot_hide_under_an_incomplete_or_human_author() {
        let mut model =
            collaboration_test_turn(Some("message"), Some("Member A"), Some("member_a"), "[]");
        model.author_agent_snapshot_json = Some("{}".to_owned());
        assert!(collaboration_from_model(&model).is_err());

        model.initiated_by_actor_kind = Some("agent_execution".to_owned());
        model.initiated_by_actor_id = Some("E00000000000000000001".to_owned());
        model.author_display_name_snapshot = None;
        assert!(collaboration_from_model(&model).is_err());
    }

    fn collaboration_test_turn(
        send_mode: Option<&str>,
        display_name: Option<&str>,
        nickname: Option<&str>,
        mentions_json: &str,
    ) -> turn::Model {
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z").unwrap();
        turn::Model {
            insertion_ordinal: 1,
            publication_xid: "1".to_owned(),
            id: "T00000000000000000001".to_owned(),
            thread_id: "H00000000000000000001".to_owned(),
            package_selection_json: None,
            status: "completed".to_owned(),
            error: None,
            prompt_manifest_json: "{}".to_owned(),
            prompt_compiler_version: None,
            prompt_profile: None,
            prompt_fingerprint_stable: None,
            prompt_fingerprint_dynamic: None,
            prompt_fingerprint_full: None,
            created_at: now,
            updated_at: now,
            turn_kind: "conversation".to_owned(),
            origin: "user".to_owned(),
            work_owner: "turn".to_owned(),
            reasoning_effort: None,
            permission_profile_mode: None,
            permission_profile_source: None,
            permission_profile_snapshot_json: None,
            execution_security_snapshot_version: None,
            execution_security_snapshot_json: None,
            initiated_by_actor_id: send_mode.map(|_| "P00000000000000000001".to_owned()),
            initiated_by_actor_kind: Some(if send_mode.is_some() {
                "principal".to_owned()
            } else {
                "system".to_owned()
            }),
            execution_authorization_context_json: None,
            send_mode: send_mode.map(str::to_owned),
            author_display_name_snapshot: display_name.map(str::to_owned),
            author_nickname_snapshot: nickname.map(str::to_owned),
            author_avatar_revision_snapshot: None,
            author_agent_snapshot_json: None,
            reply_to_turn_id: send_mode.map(|_| "T00000000000000000000".to_owned()),
            mentions_json: mentions_json.to_owned(),
            message_revision: if send_mode.is_some() { 2 } else { 0 },
            message_deleted_at: send_mode.map(|_| now),
            message_deleted_by_actor_id: None,
            message_deleted_by_actor_kind: send_mode.map(|_| "system".to_owned()),
        }
    }
}
