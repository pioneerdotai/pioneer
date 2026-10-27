use super::test_store_with_workspace;
use pioneer_protocol::{
    PersistedActorRef, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
};
use sea_orm::{EntityTrait, Set};

fn thread(id: &str, workspace: &str) -> Thread {
    Thread {
        workspace_id: workspace.into(),
        id: id.into(),
        name: Some(id.into()),
        preview: "saved preview".into(),
        preview_author: None,
        mode: ThreadMode::Agent,
        model: "test".into(),
        model_provider: "test".into(),
        reasoning_effort: None,
        created_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        status: ThreadStatus::Closed,
        origin_kind: ThreadOriginKind::System,
        sidebar_visibility: ThreadSidebarVisibility::Hidden,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: vec![],
    }
}

#[tokio::test]
async fn observation_pages_include_hidden_archived_and_keep_position_after_updates() {
    let store = test_store_with_workspace("read-pages").await;
    for id in ["c", "a", "b"] {
        store
            .upsert_thread_model(&thread(id, "read-pages"), PersistedActorRef::System)
            .await
            .unwrap();
    }
    let first = store
        .list_thread_read_candidates_page("read-pages", None, 2)
        .await
        .unwrap();
    assert_eq!(
        first.iter().map(String::as_str).collect::<Vec<_>>(),
        ["a", "b"]
    );
    // Changes to mutable activity fields must not move rows across the cursor.
    let mut updated = thread("a", "read-pages");
    updated.updated_at += 100;
    store
        .upsert_thread_model(&updated, PersistedActorRef::System)
        .await
        .unwrap();
    let next = store
        .list_thread_read_candidates_page("read-pages", Some("b"), 2)
        .await
        .unwrap();
    assert_eq!(next.iter().map(String::as_str).collect::<Vec<_>>(), ["c"]);
    assert!(
        store
            .list_thread_read_candidates_page("other-workspace", None, 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_thread_read_candidates_page("read-pages", Some("c"), 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_thread_read_candidates_page("read-pages", None, 201)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn child_pages_continue_beyond_128_and_are_scoped_to_actual_parent() {
    let store = test_store_with_workspace("child-pages").await;
    let mut rows = Vec::new();
    for i in 0..151 {
        rows.push(pioneer_entity::thread_lineage::ActiveModel {
            child_thread_id: Set(format!("child-{i:03}")),
            parent_thread_id: Set("intermediate".into()),
            root_thread_id: Set("root".into()),
            depth: Set(2),
            created_at: Set(crate::util::unix_to_datetime(1_700_000_000)),
            origin_kind: Set(None),
            created_by_thread_id: Set(None),
            created_by_turn_id: Set(None),
        });
    }
    for (id, parent, depth) in [("sibling", "root", 1), ("grandchild", "child-000", 3)] {
        rows.push(pioneer_entity::thread_lineage::ActiveModel {
            child_thread_id: Set(id.into()),
            parent_thread_id: Set(parent.into()),
            root_thread_id: Set("root".into()),
            depth: Set(depth),
            created_at: Set(crate::util::unix_to_datetime(1_700_000_000)),
            origin_kind: Set(None),
            created_by_thread_id: Set(None),
            created_by_turn_id: Set(None),
        });
    }
    pioneer_entity::thread_lineage::Entity::insert_many(rows)
        .exec(&store.connection)
        .await
        .unwrap();
    let mut actual = Vec::new();
    let mut after = None;
    loop {
        let page = store
            .list_thread_child_lineage_page("intermediate", after.as_deref(), 17)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().map(|r| r.child_thread_id.clone());
        actual.extend(page.into_iter().map(|r| r.child_thread_id));
    }
    let expected = (0..151)
        .map(|i| format!("child-{i:03}"))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    let grandchildren = store
        .list_thread_child_lineage_page("child-000", None, 17)
        .await
        .unwrap();
    assert_eq!(grandchildren.len(), 1);
    assert_eq!(grandchildren[0].child_thread_id, "grandchild");
    assert!(
        store
            .list_thread_child_lineage_page("intermediate", None, 0)
            .await
            .is_err()
    );
}
