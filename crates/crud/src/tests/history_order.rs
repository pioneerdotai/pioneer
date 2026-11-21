use sea_orm::ConnectionTrait;

#[pioneer_postgres_test::postgres_test]
async fn compaction_history_and_cli_share_the_current_turn_order() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection().clone();
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE); \
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) \
             VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
         INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES \
             ('z','thread','completed','conversation','user','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
             ('a','thread','completed','conversation','user','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
             ('launch','thread','completed','conversation','user','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
             ('backdated','thread','completed','conversation','user','2026-01-01T09:00:00Z',CURRENT_TIMESTAMP), \
             ('occurrence','thread','completed','task_run','system','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
             ('future','thread','completed','conversation','user','2026-01-01T11:00:00Z',CURRENT_TIMESTAMP)",
    ).await.unwrap();
    let store = crate::CrudStore::new(db.clone());
    let history_fence = store.compaction_history_read_fence().await.unwrap();
    let mut history_turns = store
        .compaction_history_turn_page("ws", "thread", "", &history_fence)
        .await
        .unwrap();
    history_turns.sort_by(|left, right| {
        (
            &left.created_at,
            left.creation_order,
            left.legacy_creation_order,
            &left.id,
        )
            .cmp(&(
                &right.created_at,
                right.creation_order,
                right.legacy_creation_order,
                &right.id,
            ))
    });
    let history_order = history_turns
        .iter()
        .map(|turn| turn.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        history_order,
        vec!["backdated", "z", "a", "launch", "occurrence", "future"]
    );
    let launch_index = history_order
        .iter()
        .position(|turn| *turn == "launch")
        .unwrap();
    assert_eq!(history_order[launch_index - 1], "a");
    let boundary = store
        .turn_before_launch_and_intervening_by_creation_order("thread", "launch", "occurrence")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        boundary,
        (Some(history_order[launch_index - 1].into()), true)
    );
}
