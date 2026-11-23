use anyhow::Result;
use pioneer_crud_execution::turn_cli_runtime_instruction::{
    NewCliRuntimeInstructionProjection, find_cli_runtime_instruction_projection,
    insert_cli_runtime_instruction_projection_if_absent,
};
use sea_orm::ConnectionTrait;

#[pioneer_postgres_test::postgres_test]
async fn cli_runtime_instruction_projection_round_trips_without_debug_text_leakage() -> Result<()> {
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let db = fixture.connect_with_limit(1).await?;
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES('ws-cli-instruction','Fixture');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,created_at,updated_at)
         VALUES('thread-cli-instruction','ws-cli-instruction','','agent','m','p','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
         INSERT INTO turn(id,thread_id,status,created_at,updated_at)
         VALUES('turn-cli-instruction','thread-cli-instruction','in_progress',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
    ).await?;
    let turn_id = "turn-cli-instruction";
    let now = chrono::DateTime::from_timestamp(1_700_000_000, 0)
        .unwrap()
        .fixed_offset();
    let governing_text = "durable governing prompt canary";

    let projection = NewCliRuntimeInstructionProjection {
        turn_id: turn_id.to_owned(),
        runtime_kind: "codex".to_owned(),
        transport_kind: "codex_turn_collaboration_mode".to_owned(),
        instruction_text: governing_text.to_owned(),
        instruction_fingerprint: "a".repeat(64),
        section_ids_json: r#"["pioneer_cli_runtime_instructions"]"#.to_owned(),
        compiler_version: "test".to_owned(),
        created_at: now,
        updated_at: now,
    };
    let inserted = insert_cli_runtime_instruction_projection_if_absent(&db, projection.clone())
        .await
        .expect("instruction projection insert should succeed");
    assert_eq!(inserted.instruction_text, governing_text);
    assert!(!format!("{inserted:?}").contains(governing_text));

    let fetched = find_cli_runtime_instruction_projection(&db, turn_id)
        .await
        .expect("instruction projection read should succeed")
        .expect("instruction projection should exist");
    assert_eq!(fetched, inserted);

    let mut conflicting = projection;
    conflicting.instruction_text = "replacement governing prompt".to_owned();
    conflicting.instruction_fingerprint = "b".repeat(64);
    let preserved = insert_cli_runtime_instruction_projection_if_absent(&db, conflicting)
        .await
        .expect("idempotent projection insert should succeed");
    assert_eq!(preserved, inserted);
    assert_eq!(preserved.instruction_text, governing_text);
    db.close().await?;
    Ok(())
}
