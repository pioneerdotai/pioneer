use pioneer_protocol::{
    PersistedActorRef, PrincipalId, ThreadMode, Turn, TurnAuthorSnapshot, TurnMention, TurnOrigin,
    TurnStartParams, TurnStatus, UserInput, default_turn_permission_profile_snapshot,
};

fn turn_start(mode: ThreadMode) -> TurnStartParams {
    TurnStartParams {
        thread_id: "thread-baseline".to_owned(),
        turn_id: "turn-baseline".to_owned(),
        input: vec![UserInput::Text {
            text: "baseline".to_owned(),
            text_elements: Vec::new(),
        }],
        capabilities: Vec::new(),
        model: None,
        model_provider: None,
        sandbox_policy: None,
        mode: Some(mode),
        reply_to_turn_id: None,
        mentioned_principal_ids: Vec::new(),
        execution_backend: None,
        reasoning: None,
        permission_profile: None,
        cli_runtime_options: None,
    }
}

#[test]
fn epic6_baseline_turn_start_preserves_current_chat_and_agent_modes() {
    for mode in [ThreadMode::Message, ThreadMode::Chat, ThreadMode::Agent] {
        let encoded = serde_json::to_value(turn_start(mode)).expect("turn/start should encode");
        let decoded: TurnStartParams =
            serde_json::from_value(encoded).expect("turn/start should decode");

        assert_eq!(decoded.mode, Some(mode));
        assert_eq!(decoded.thread_id, "thread-baseline");
        assert_eq!(decoded.turn_id, "turn-baseline");
        assert_eq!(decoded.input.len(), 1);
    }
}

#[test]
fn epic6_turn_collaboration_fields_round_trip_on_the_canonical_turn() {
    let principal_id =
        PrincipalId::new("P00000000000000000001").expect("principal ID should be valid");
    let turn = Turn {
        id: "turn-message".to_owned(),
        status: TurnStatus::Completed,
        turn_kind: Default::default(),
        origin: TurnOrigin::User,
        mode: ThreadMode::Message,
        author: Some(TurnAuthorSnapshot {
            actor: PersistedActorRef::Principal(principal_id.clone()),
            display_name: "Member".to_owned(),
            nickname: "member".to_owned(),
            avatar_revision: Some("avatar-revision".to_owned()),
        }),
        reply_to_turn_id: Some("turn-parent".to_owned()),
        mentions: vec![TurnMention {
            principal_id,
            nickname: "member".to_owned(),
        }],
        message_revision: 2,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: default_turn_permission_profile_snapshot(),
    };

    let encoded = serde_json::to_value(&turn).expect("Turn should encode");
    let decoded: Turn = serde_json::from_value(encoded).expect("Turn should decode");

    assert_eq!(decoded, turn);
}

#[test]
fn epic6_pre_epic6_turn_decode_uses_chat_and_immutable_metadata_defaults() {
    let current = Turn {
        id: "turn-legacy".to_owned(),
        status: TurnStatus::Completed,
        turn_kind: Default::default(),
        origin: TurnOrigin::User,
        mode: ThreadMode::Agent,
        author: None,
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: default_turn_permission_profile_snapshot(),
    };
    let mut encoded = serde_json::to_value(current)
        .expect("Turn should encode")
        .as_object()
        .expect("Turn should be an object")
        .clone();
    for field in [
        "mode",
        "author",
        "reply_to_turn_id",
        "mentions",
        "message_revision",
        "message_deleted",
    ] {
        encoded.remove(field);
    }

    let decoded: Turn = serde_json::from_value(serde_json::Value::Object(encoded))
        .expect("legacy Turn should decode");

    assert_eq!(decoded.mode, ThreadMode::Chat);
    assert!(decoded.author.is_none());
    assert!(decoded.reply_to_turn_id.is_none());
    assert!(decoded.mentions.is_empty());
    assert_eq!(decoded.message_revision, 0);
    assert!(!decoded.message_deleted);
}

#[test]
fn epic6_baseline_turn_id_remains_the_single_send_identity() {
    let encoded =
        serde_json::to_value(turn_start(ThreadMode::Agent)).expect("turn/start should encode");
    let object = encoded.as_object().expect("turn/start should be an object");

    assert_eq!(
        object.get("turn_id").and_then(|value| value.as_str()),
        Some("turn-baseline")
    );
    assert!(!object.contains_key("message_id"));
    assert!(!object.contains_key("source_message_id"));
    assert!(!object.contains_key("idempotency_key"));
}
