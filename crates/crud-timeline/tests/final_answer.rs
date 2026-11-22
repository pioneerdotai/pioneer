use pioneer_crud_conversations::{
    turn_item::{prepare_turn_item_projection, upsert_prepared_turn_item},
    turn_writes::update_turn_status,
};
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_timeline::conversation_display::ConversationDisplayStore;
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{AgentMessagePhase, TurnItem, TurnStatus};
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn legacy_final_selector_rejects_commentary_and_ambiguous_finals() {
    for final_count in 0..=2 {
        for final_id in ["a_final", "z_final"] {
            pioneer_postgres_test::with_database(async |fixture| {
                let db = PostgresDatabase::new(
                    fixture.connect_read_only().await?,
                    fixture.connection().clone(),
                );
                db.execute_unprepared(
                    "INSERT INTO workspace (id, name) VALUES ('ws_legacy_select', 'Final answer');
                     INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status)
                       VALUES ('thr_legacy_select','ws_legacy_select','','agent','model','provider','active');
                     INSERT INTO turn (id,thread_id,status)
                       VALUES ('turn_legacy_select','thr_legacy_select','in_progress')",
                ).await?;
                let store = ConversationDisplayStore::new(&db);
                let item = |id: &str, phase| TurnItem::AgentMessage {
                    id: id.into(),
                    text: id.into(),
                    phase,
                    markdown: None,
                    markdown_version: None,
                };
                let answer = item(final_id, AgentMessagePhase::FinalAnswer);
                let mut items = vec![
                    item("0_commentary", AgentMessagePhase::Commentary),
                    item("old_partial_final", AgentMessagePhase::Commentary),
                    item("zz_commentary", AgentMessagePhase::Commentary),
                ];
                if final_count > 0 {
                    items.push(answer.clone());
                }
                if final_count > 1 {
                    items.push(item("other_final", AgentMessagePhase::FinalAnswer));
                }
                items.push(item("late_commentary", AgentMessagePhase::Commentary));
                let at = unix_to_datetime(1_700_000_100);
                for item in items {
                    let prepared = prepare_turn_item_projection(
                        "turn_legacy_select", &item, Some("completed"), at, at,
                    )?;
                    let tx = db.begin().await?;
                    upsert_prepared_turn_item(&tx, prepared).await?;
                    tx.commit().await?;
                }
                let tx = db.begin().await?;
                update_turn_status(
                    &tx, "thr_legacy_select", "turn_legacy_select", TurnStatus::Completed, None, at,
                ).await?;
                tx.commit().await?;
                assert_eq!(
                    store
                        .completed_turn_final_answer(
                            "ws_legacy_select",
                            "thr_legacy_select",
                            "turn_legacy_select"
                        )
                        .await
                        .unwrap(),
                    (final_count == 1).then_some(answer)
                );
                Ok(())
            })
            .await
            .unwrap();
        }
    }
}
