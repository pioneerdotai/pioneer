use super::{start_test_turn, test_store_with_workspace};
use pioneer_entity::turn;
use pioneer_protocol::UserInput;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

#[tokio::test]
async fn canonical_history_keyset_handles_timestamp_ties_snapshot_appends_and_edits() {
    let mut store = test_store_with_workspace("saved-history").await;
    let at = crate::util::unix_to_datetime(1_700_000_000);
    for id in ["turn-a", "turn-c", "turn-b"] {
        (store, _, _) = start_test_turn(store, "saved-history", "parent", id).await;
        turn::Entity::update_many()
            .col_expr(turn::Column::CreatedAt, Expr::value(at))
            .filter(turn::Column::Id.eq(id))
            .exec(&store.connection)
            .await
            .unwrap();
    }
    let snapshot = store.saved_history_snapshot("parent").await.unwrap();
    let first = store
        .saved_history_page_at_snapshot("parent", None, None, 2, &snapshot)
        .await
        .unwrap();
    assert_eq!(
        first.iter().map(|r| r.turn_id.as_str()).collect::<Vec<_>>(),
        ["turn-c", "turn-b"]
    );
    let boundary = first[0].key.clone();
    let before = first[1].key.clone();
    let revision = store
        .saved_history_revision_at_snapshot("parent", &boundary, &snapshot)
        .await
        .unwrap();
    (store, _, _) = start_test_turn(store, "saved-history", "parent", "new-turn").await;
    // Even a newly saved record at the same timestamp and inside the old
    // lexicographic boundary is outside this traversal's insertion watermark.
    turn::Entity::update_many()
        .col_expr(turn::Column::CreatedAt, Expr::value(at))
        .filter(turn::Column::Id.eq("new-turn"))
        .exec(&store.connection)
        .await
        .unwrap();
    let old = store
        .saved_history_page_at_snapshot("parent", Some(&boundary), Some(&before), 2, &snapshot)
        .await
        .unwrap();
    assert_eq!(
        old.iter().map(|r| r.turn_id.as_str()).collect::<Vec<_>>(),
        ["turn-a"]
    );
    assert_eq!(
        store
            .saved_history_revision_at_snapshot("parent", &boundary, &snapshot)
            .await
            .unwrap(),
        revision
    );
    assert!(
        store
            .saved_history_page_at_snapshot("child", Some(&boundary), None, 2, &snapshot)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .replace_turn_input_projection_for_test(
            "turn-a",
            &[UserInput::Text {
                text: "edited".into(),
                text_elements: vec![],
            }],
            1,
            1_700_000_010,
        )
        .await
        .unwrap();
    assert_ne!(
        store
            .saved_history_revision_at_snapshot("parent", &boundary, &snapshot)
            .await
            .unwrap(),
        revision
    );
}

#[tokio::test]
async fn deleted_turn_inputs_are_not_returned_by_canonical_history_scan() {
    let store = test_store_with_workspace("deleted-history").await;
    let (store, _, _) = start_test_turn(store, "deleted-history", "thread", "turn").await;
    store
        .replace_turn_input_projection_for_test(
            "turn",
            &[UserInput::Text {
                text: "removed".into(),
                text_elements: vec![],
            }],
            1,
            1_700_000_010,
        )
        .await
        .unwrap();
    let snapshot = store.saved_history_snapshot("thread").await.unwrap();
    assert!(
        store
            .saved_history_page_at_snapshot("thread", None, None, 100, &snapshot)
            .await
            .unwrap()
            .iter()
            .any(|r| r.key.kind == "input")
    );
    turn::Entity::update_many()
        .col_expr(
            turn::Column::MessageDeletedAt,
            Expr::value(crate::util::unix_to_datetime(1_700_000_020)),
        )
        .filter(turn::Column::Id.eq("turn"))
        .exec(&store.connection)
        .await
        .unwrap();
    let rows = store
        .saved_history_page_at_snapshot("thread", None, None, 100, &snapshot)
        .await
        .unwrap();
    assert!(
        !rows
            .iter()
            .any(|r| r.key.kind == "input" || r.key.kind == "item")
    );
    assert_eq!(
        rows.len(),
        1,
        "the authorized deleted-turn tombstone remains readable"
    );
}
