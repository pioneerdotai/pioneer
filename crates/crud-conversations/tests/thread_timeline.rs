use anyhow::{Context, Result};
use chrono::DateTime;
use pioneer_crud_conversations::thread_state::{
    advance_thread_read_cursor, find_thread_read_cursor,
};
use pioneer_crud_conversations::thread_timeline_projection::{
    ThreadTimelineBlockRecord, count_unread_message_blocks,
    count_unread_message_blocks_for_threads, find_thread_timeline_block_by_id,
    find_turn_work_item_projection, new_message_sort_key,
    refresh_terminal_running_turn_work_item_projections, upsert_thread_timeline_block,
};
use pioneer_postgres_test::with_database;
use pioneer_protocol::PrincipalId;
use sea_orm::{ConnectionTrait, TransactionTrait};

const WORKSPACE: &str = "W00000000000000000001";
const OWNER: &str = "P00000000000000000001";
const MEMBER: &str = "P00000000000000000002";
const START: i64 = 1_700_000_000;

#[tokio::test]
async fn unread_respects_publication_time_access_own_messages_and_confirmed_read() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('W00000000000000000001', 'Workspace');
            INSERT INTO gateway_identity (id, singleton_key, unread_started_at)
                VALUES ('G00000000000000000001', 1, 1700000000);
            INSERT INTO gateway_principal
                (id, gateway_id, kind, role_key, status, display_name, nickname, nickname_key, created_at)
                VALUES ('P00000000000000000001', 'G00000000000000000001', 'superuser', NULL,
                        'active', 'Owner', 'owner', 'owner', to_timestamp(1700000000)),
                       ('P00000000000000000002', 'G00000000000000000001', 'user', 'member',
                        'active', 'Member', 'member', 'member', to_timestamp(1700000000));
            INSERT INTO workspace_membership
                (principal_id, workspace_id, granted_by_actor_kind, created_at, updated_at)
                VALUES ('P00000000000000000002', 'W00000000000000000001', 'system',
                    to_timestamp(1700000002.9), to_timestamp(1700000002.9));
            INSERT INTO thread
                (id, workspace_id, preview, mode, model, model_provider, status, access_class)
                VALUES ('thread', 'W00000000000000000001', '', 'agent', 'model', 'provider', 'active', 'workspace'),
                       ('private', 'W00000000000000000001', '', 'agent', 'model', 'provider', 'active', 'private');
            INSERT INTO "turn" (id, thread_id, status, initiated_by_actor_kind, initiated_by_actor_id)
                VALUES ('old', 'thread', 'completed', 'principal', 'P00000000000000000001'),
                       ('other', 'thread', 'completed', 'principal', 'P00000000000000000001'),
                       ('own', 'thread', 'completed', 'principal', 'P00000000000000000002'),
                       ('secret', 'private', 'completed', 'principal', 'P00000000000000000001');
            "#,
        )
        .await?;
        let mut own_key = String::new();
        for (thread, turn, at) in [
            ("thread", "old", START + 2),
            ("thread", "other", START + 4),
            ("thread", "own", START + 4),
            ("private", "secret", START + 4),
        ] {
            let tx = db.begin().await?;
            let key = new_message_sort_key(
                &tx,
                thread,
                turn,
                turn,
                format!("{:020}:{turn}", START * 1000),
                at,
            )
            .await?;
            let date = DateTime::from_timestamp(at, 0).unwrap().fixed_offset();
            upsert_thread_timeline_block(
                &tx,
                ThreadTimelineBlockRecord {
                    block_id: turn.into(),
                    workspace_id: WORKSPACE.into(),
                    thread_id: thread.into(),
                    turn_id: Some(turn.into()),
                    block_kind: "user_message".into(),
                    sort_key: key.clone(),
                    source_kind: Some("turn".into()),
                    source_key: Some(turn.into()),
                    started_at: None,
                    completed_at: None,
                    metadata_json: "{}".into(),
                    created_at: date,
                    updated_at: date,
                },
            )
            .await?;
            tx.commit().await?;
            if turn == "own" {
                own_key = key;
            }
        }
        let threads = ["thread".into(), "private".into(), "thread".into()];
        assert_eq!(count_unread_message_blocks(db, "thread", MEMBER, "").await?, 1);
        assert_eq!(count_unread_message_blocks_for_threads(db, &threads, MEMBER).await?.len(), 1);
        let owner_counts = count_unread_message_blocks_for_threads(db, &threads, OWNER).await?;
        assert_eq!(owner_counts.get("thread"), Some(&1));
        assert!(!owner_counts.contains_key("private"));
        assert_eq!(count_unread_message_blocks(db, "thread", MEMBER, &own_key).await?, 0);
        let member = PrincipalId::new(MEMBER)?;
        let confirmed_at = DateTime::from_timestamp(START + 5, 0).unwrap().fixed_offset();
        assert!(advance_thread_read_cursor(db, &member, "thread", &own_key, "own", confirmed_at).await?);
        assert!(!advance_thread_read_cursor(db, &member, "thread", "00000000000000000000:old", "old", confirmed_at).await?);
        let cursor = find_thread_read_cursor(db, &member, "thread").await?.context("missing confirmed read cursor")?;
        assert_eq!(cursor.last_read_turn_id, "own");
        assert!(!count_unread_message_blocks_for_threads(db, &threads, MEMBER).await?.contains_key("thread"));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn terminal_work_and_conflicting_assistant_block_change_in_one_commit() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'agent', 'model', 'provider', 'active');
            INSERT INTO "turn" (id, thread_id, status) VALUES ('turn', 'thread', 'completed');
            INSERT INTO turn_item (id, turn_id, item_id, item_type, status, payload, updated_at)
                VALUES ('row', 'turn', 'item', 'tool', 'failed', '{}', '2026-11-16T12:45:10.124Z');
            INSERT INTO turn_work_item_projection
                (work_item_id, workspace_id, thread_id, turn_id, item_id, order_key, item_type,
                 visibility, classification, status)
                VALUES ('work', 'w', 'thread', 'turn', 'item', '01', 'tool', 'visible', 'tool', 'running'),
                       ('missing', 'w', 'thread', 'turn', 'missing', '02', 'tool', 'visible', 'tool', 'running');
            INSERT INTO thread_timeline_block
                (block_id, workspace_id, thread_id, turn_id, block_kind, sort_key, source_kind, source_key)
                VALUES ('old', 'w', 'thread', 'turn', 'assistant_message', '01', 'turn_item', 'item');
            "#,
        )
        .await?;
        let date = DateTime::parse_from_rfc3339("2026-11-16T12:45:11Z")?;
        let item_done_at = DateTime::parse_from_rfc3339("2026-11-16T12:45:10.124Z")?;
        let writer = fixture.connect().await?;
        let tx = writer.begin().await?;
        assert_eq!(
            refresh_terminal_running_turn_work_item_projections(&tx, "turn", "event", 9, date)
                .await?,
            1
        );
        assert!(find_thread_timeline_block_by_id(&tx, "thread", "old").await?.is_none());
        assert!(find_thread_timeline_block_by_id(db, "thread", "old").await?.is_some());
        tx.rollback().await?;
        assert_eq!(find_turn_work_item_projection(db, "work").await?.context("missing work")?.status, "running");
        let tx = writer.begin().await?;
        refresh_terminal_running_turn_work_item_projections(&tx, "turn", "event", 9, date).await?;
        tx.commit().await?;
        assert!(find_thread_timeline_block_by_id(db, "thread", "old").await?.is_none());
        let changed = find_turn_work_item_projection(db, "work").await?.context("missing work")?;
        assert_eq!(changed.status, "failed");
        assert_eq!(changed.completed_at, Some(item_done_at));
        assert_eq!(changed.source_event_id.as_deref(), Some("event"));
        assert_eq!(changed.source_sequence, 9);
        assert_eq!(find_turn_work_item_projection(db, "missing").await?.context("missing row")?.status, "running");
        writer.close().await?;
        Ok(())
    })
    .await
}
