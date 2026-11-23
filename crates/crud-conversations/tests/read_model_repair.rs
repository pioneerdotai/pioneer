use anyhow::{Context, Result};
use pioneer_crud_conversations::read_model_repair as repair;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn repair_keeps_changes_that_arrive_after_an_interrupted_pass() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
             INSERT INTO turn (id, thread_id, status) VALUES ('turn', 'thread', 'completed');
             INSERT INTO turn_item (id, turn_id, item_id, item_type, status, payload)
             VALUES ('item', 'turn', 'item', 'command_execution', 'completed', '{}');",
        )
        .await?;
        let key = repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY;
        repair::reset_full_scan(db, key, 1, Some("item")).await?;
        repair::advance_full_scan_cursor(db, key, 1, "item").await?;
        repair::complete_full_scan(db, key, 1).await?;
        let first_generation = repair::current_change_generation(db).await?;
        repair::begin_incremental_pass(db, key, 1, first_generation).await?;
        let before = repair::list_dirty_turn_items(db, 0, first_generation, 10).await?;
        assert_eq!(before.len(), 1);

        let writer = fixture.connect().await?;
        let change = writer.begin().await?;
        change
            .execute_unprepared(
                r#"UPDATE turn_item SET payload = '{ "changed": true }' WHERE id = 'item'"#,
            )
            .await?;
        assert_eq!(
            repair::current_change_generation(db).await?,
            first_generation
        );
        change.commit().await?;
        let later_generation = repair::current_change_generation(db).await?;
        assert!(later_generation > first_generation);
        repair::clear_dirty_turn_item_if_unchanged(
            db,
            &before[0].turn_item_id,
            before[0].generation,
        )
        .await?;
        assert_eq!(
            repair::count_dirty_turn_items_in_window(db, 0, first_generation).await?,
            0
        );
        assert_eq!(
            repair::count_dirty_turn_items_in_window(db, first_generation, later_generation)
                .await?,
            1
        );

        let resumed = repair::load_checkpoint(&writer, key)
            .await?
            .context("checkpoint disappeared")?;
        assert_eq!(resumed.incremental_status, repair::STATUS_RUNNING);
        assert_eq!(
            resumed.incremental_high_watermark_generation,
            Some(first_generation)
        );
        repair::complete_incremental_pass(&writer, key, 1, first_generation).await?;
        repair::begin_incremental_pass(&writer, key, 1, later_generation).await?;
        let late =
            repair::list_dirty_turn_items(db, first_generation, later_generation, 10).await?;
        assert_eq!(late.len(), 1);
        assert_eq!(late[0].turn_item_id, "item");
        repair::advance_incremental_cursor(db, key, 1, later_generation, late[0].generation)
            .await?;
        repair::clear_dirty_turn_item(db, "item").await?;
        repair::complete_incremental_pass(db, key, 1, later_generation).await?;
        assert_eq!(
            repair::count_dirty_turn_items_in_window(db, 0, later_generation).await?,
            0
        );
        writer.close().await?;
        Ok(())
    })
    .await
}
