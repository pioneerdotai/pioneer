use pioneer_protocol::{
    TURN_MESSAGE_INPUT_MAX_ITEMS, TURN_MESSAGE_MENTION_MAX_COUNT, ThreadMode, ThreadReadCursor,
    ThreadReadCursorChangedNotification, TimelineBlockKind, TurnMessageDeleteParams,
    TurnMessageEditParams, TurnMessageErrorReason, TurnMessageRevisionsPageParams, UserInput,
    validate_turn_message_content,
};

#[test]
fn epic6_message_send_and_edit_share_the_same_canonical_content_validation() {
    let valid = vec![UserInput::Text {
        text: "message".to_owned(),
        text_elements: Vec::new(),
    }];
    assert!(validate_turn_message_content(&valid, 0).is_ok());
    assert!(
        validate_turn_message_content(
            &[UserInput::Text {
                text: "   ".to_owned(),
                text_elements: Vec::new(),
            }],
            0,
        )
        .is_err()
    );
    assert!(
        validate_turn_message_content(
            &[UserInput::Artifact {
                artifact_id: "artifact".to_owned(),
                version_id: None,
            }],
            0,
        )
        .is_err()
    );
    assert!(
        validate_turn_message_content(
            &vec![
                UserInput::Text {
                    text: "message".to_owned(),
                    text_elements: Vec::new(),
                };
                TURN_MESSAGE_INPUT_MAX_ITEMS + 1
            ],
            0,
        )
        .is_err()
    );
    assert!(validate_turn_message_content(&valid, TURN_MESSAGE_MENTION_MAX_COUNT + 1).is_err());
}

#[test]
fn epic6_turn_message_operations_keep_turn_id_as_the_only_message_identity() {
    let edit = TurnMessageEditParams {
        thread_id: "thread-message".to_owned(),
        turn_id: "turn-message".to_owned(),
        expected_revision: 3,
        input: vec![UserInput::Text {
            text: "edited".to_owned(),
            text_elements: Vec::new(),
        }],
        mentioned_principal_ids: Vec::new(),
    };
    let delete = TurnMessageDeleteParams {
        thread_id: edit.thread_id.clone(),
        turn_id: edit.turn_id.clone(),
        expected_revision: 4,
    };

    let edit_json = serde_json::to_value(edit).expect("edit should encode");
    let delete_json = serde_json::to_value(delete).expect("delete should encode");

    assert_eq!(edit_json["turn_id"], "turn-message");
    assert_eq!(delete_json["turn_id"], "turn-message");
    assert!(edit_json.get("message_id").is_none());
    assert!(delete_json.get("message_id").is_none());
}

#[test]
fn epic6_revision_page_is_bounded_and_error_reasons_are_content_free() {
    assert_eq!(
        TurnMessageRevisionsPageParams {
            thread_id: "thread-message".to_owned(),
            turn_id: "turn-message".to_owned(),
            cursor: None,
            limit: None,
        }
        .validated_limit()
        .expect("default page should be valid"),
        50
    );
    assert!(
        TurnMessageRevisionsPageParams {
            thread_id: "thread-message".to_owned(),
            turn_id: "turn-message".to_owned(),
            cursor: None,
            limit: Some(101),
        }
        .validated_limit()
        .is_err()
    );

    let reason = serde_json::to_value(TurnMessageErrorReason::RevisionConflict)
        .expect("reason should encode");
    assert_eq!(reason, "revision_conflict");
}

#[test]
fn epic6_user_timeline_block_and_read_invalidation_use_existing_contracts() {
    assert_eq!(
        pioneer_protocol::constants::events::THREAD_READ_CURSOR_CHANGED,
        "thread/read/changed"
    );

    let block = TimelineBlockKind::UserMessage {
        item_id: None,
        inputs: Vec::new(),
        text: "message".to_owned(),
        attachments: Vec::new(),
        mode: ThreadMode::Message,
        author: None,
        reply: None,
        mentions: Vec::new(),
        revision: 1,
        edited: true,
        deleted: false,
    };
    let encoded = serde_json::to_value(block).expect("timeline block should encode");
    assert_eq!(encoded["kind"], "user_message");
    assert_eq!(encoded["mode"], "Message");
    assert_eq!(encoded["revision"], 1);
    assert_eq!(encoded["edited"], true);
    assert_eq!(encoded["deleted"], false);

    let notification = ThreadReadCursorChangedNotification {
        workspace_id: "workspace".to_owned(),
        thread_id: "thread-message".to_owned(),
        cursor: ThreadReadCursor {
            through_turn_id: "turn-message".to_owned(),
            sort_key: "sort-key".to_owned(),
        },
        unread_count: 0,
    };
    let encoded = serde_json::to_value(notification).expect("notification should encode");
    assert_eq!(encoded["unread_count"], 0);
}

#[test]
fn epic6_contracts_are_registered_in_the_authoritative_rust_schema_set() {
    let documents = pioneer_protocol::protocol_schema_documents();
    for required in [
        "turn.json",
        "turn_start_params.json",
        "turn_message_edit_params.json",
        "turn_message_delete_params.json",
        "turn_message_revision.json",
        "turn_message_revisions_page_response.json",
        "thread_read_params.json",
        "thread_read_cursor_changed_notification.json",
    ] {
        assert!(
            documents
                .iter()
                .any(|document| document.file_name == required),
            "missing authoritative schema document {required}"
        );
    }

    let turn_schema = documents
        .iter()
        .find(|document| document.file_name == "turn.json")
        .expect("Turn schema should exist");
    let schema_json = serde_json::to_value(&turn_schema.schema).expect("schema should encode");
    let schema_text = schema_json.to_string();
    for field in [
        "mode",
        "author",
        "reply_to_turn_id",
        "mentions",
        "message_revision",
        "message_deleted",
    ] {
        assert!(
            schema_text.contains(field),
            "Turn schema is missing {field}"
        );
    }
}
