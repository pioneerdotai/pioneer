use anyhow::Result;
use pioneer_crud_conversations::turn_input::post_turn_user_text;
use pioneer_postgres_test::with_database;
use sea_orm::ConnectionTrait;

#[tokio::test]
async fn original_message_limits_count_unicode_characters_and_skip_attachments() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'agent', 'model', 'provider', 'active');
            INSERT INTO "turn" (id, thread_id, status) VALUES ('turn', 'thread', 'completed');
            INSERT INTO turn_input (id, turn_id, input_index, input_type, text)
                VALUES ('second', 'turn', 3, 'text', '漢字'),
                       ('image', 'turn', 2, 'image', 'exclude'),
                       ('first', 'turn', 1, 'text', 'ё🙂');
            "#,
        )
        .await?;
        assert_eq!(
            post_turn_user_text(db, "turn", 3).await?,
            ("ё🙂\n".into(), true)
        );
        assert_eq!(
            post_turn_user_text(db, "turn", 4).await?,
            ("ё🙂\n漢".into(), true)
        );
        assert_eq!(
            post_turn_user_text(db, "turn", 5).await?,
            ("ё🙂\n漢字".into(), false)
        );
        Ok(())
    })
    .await
}
