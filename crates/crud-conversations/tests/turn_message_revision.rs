use anyhow::{Context, Result};
use pioneer_crud_conversations::turn_message_revision::{
    NewTurnMessageRevision, insert_prepared_turn_message_revision, insert_turn_message_revision,
    list_turn_message_revisions, prepare_turn_message_revision, turn_message_revision_from_model,
};
use pioneer_crud_conversations::turn_writes::{
    apply_prepared_turn_upsert, find_turn_collaboration, prepare_projected_turn_upsert,
};
use pioneer_postgres_test::with_database;
use pioneer_protocol::{
    PersistedActorRef, ThreadMode, Turn, TurnAuthorSnapshot, TurnKind,
    TurnMessageRevisionChangeKind, TurnStatus, UserInput,
};
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn revision_replay_preserves_original_receipt_and_rollback_preserves_paging() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'message', 'model', 'provider', 'active');
            INSERT INTO "turn" (id, thread_id, status) VALUES ('turn', 'thread', 'completed');
            "#,
        )
        .await?;
        let actor = PersistedActorRef::System;
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")?;
        let original = [UserInput::Text {
            text: "original\0内容".to_owned(),
            text_elements: Vec::new(),
        }];
        let first = NewTurnMessageRevision {
            turn_id: "turn",
            revision: 0,
            input: &original,
            mentions: &[],
            changed_by: &actor,
            change_kind: TurnMessageRevisionChangeKind::Edit,
            created_at: now,
        };
        let tx = db.begin().await?;
        insert_turn_message_revision(&tx, first.clone()).await?;
        insert_prepared_turn_message_revision(
            &tx,
            prepare_turn_message_revision(first.clone())?,
            true,
        )
        .await?;
        tx.commit().await?;
        let other = fixture.connect().await?;
        let tx = db.begin().await?;
        insert_turn_message_revision(
            &tx,
            NewTurnMessageRevision {
                revision: 1,
                ..first.clone()
            },
        )
        .await?;
        let first_page = list_turn_message_revisions(&tx, "turn", None, 1).await?;
        assert_eq!(first_page[0].revision, 1);
        let older = list_turn_message_revisions(&tx, "turn", Some(1), 1).await?;
        let revision = turn_message_revision_from_model(
            older
                .into_iter()
                .next()
                .context("missing previous revision")?,
        )?;
        assert_eq!(revision.turn_id, "turn");
        assert_eq!(revision.revision, 0);
        assert_eq!(revision.changed_by, actor);
        assert_eq!(revision.input.as_deref(), Some(original.as_slice()));
        tx.rollback().await?;
        assert!(insert_turn_message_revision(db, first).await.is_err());
        let persisted = list_turn_message_revisions(&other, "turn", None, 10).await?;
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].revision, 0);
        other.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn epic6_turn_columns_and_revision_use_existing_turn_repository() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();
        connection
            .execute_unprepared(
                "INSERT INTO workspace (id, name, is_active, is_current) \
                 VALUES ('workspace_epic6_repo', 'Epic 6', TRUE, FALSE); \
                 INSERT INTO thread \
                    (id, workspace_id, preview, mode, model, model_provider, status) \
                 VALUES \
                    ('H00000000000000000001', 'workspace_epic6_repo', '', 'chat', \
                     'model', 'provider', 'active');",
            )
            .await
            .expect("Turn owner fixture should insert");

        let now = "2026-01-01T00:00:00Z".parse()?;
        let actor = PersistedActorRef::System;
        let message = Turn {
            work_owner: Default::default(),
            id: "T00000000000000000001".to_owned(),
            status: TurnStatus::Completed,
            turn_kind: TurnKind::Conversation,
            origin: pioneer_protocol::TurnOrigin::User,
            mode: ThreadMode::Message,
            author: Some(TurnAuthorSnapshot {
                cli_runtime_kind: None,
                actor: actor.clone(),
                display_name: "System".to_owned(),
                nickname: "system".to_owned(),
                avatar_revision: None,
                agent: None,
            }),
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        let tx = connection.begin().await?;
        let prepared = prepare_projected_turn_upsert(
            &tx,
            message.id.as_str(),
            "H00000000000000000001",
            &message,
            None,
            None,
            None,
            Some(&actor),
            true,
            now,
            now,
        )
        .await
        .expect("Message Turn projection should prepare");
        apply_prepared_turn_upsert(&tx, prepared).await?;
        tx.commit().await?;

        let (_, collaboration) =
            find_turn_collaboration(&connection, "H00000000000000000001", message.id.as_str())
                .await
                .expect("Message Turn should load")
                .expect("Message Turn should exist");
        assert_eq!(collaboration.mode.effective_mode, ThreadMode::Message);
        assert!(collaboration.mode.message_mutation_eligible);

        insert_turn_message_revision(
            &connection,
            NewTurnMessageRevision {
                turn_id: message.id.as_str(),
                revision: 0,
                input: &[UserInput::Text {
                    text: "original".to_owned(),
                    text_elements: Vec::new(),
                }],
                mentions: &[],
                changed_by: &actor,
                change_kind: TurnMessageRevisionChangeKind::Edit,
                created_at: now,
            },
        )
        .await
        .expect("previous Turn version should persist");
        let revisions = list_turn_message_revisions(&connection, message.id.as_str(), None, 10)
            .await
            .expect("revisions should list");
        assert_eq!(revisions.len(), 1);
        let revision = turn_message_revision_from_model(revisions[0].clone())
            .expect("revision should map to protocol");
        assert_eq!(revision.turn_id, message.id);
        assert_eq!(revision.revision, 0);
        assert_eq!(revision.changed_by, actor);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stale_first_projection_cannot_replace_the_concurrent_winner() -> Result<()> {
    use sea_orm::{DbBackend, Statement};
    use std::time::Duration;

    for commit_first in [true, false] {
        with_database(async |fixture| {
            let db = fixture.connection();
            db.execute_unprepared(
                r#"
                INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
                INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                    VALUES ('thread', 'w', '', 'message', 'model', 'provider', 'active');
                "#,
            )
            .await?;
            let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")?;
            let turn = Turn {
                work_owner: Default::default(),
                id: "turn".into(),
                status: TurnStatus::Completed,
                turn_kind: TurnKind::Conversation,
                origin: pioneer_protocol::TurnOrigin::User,
                mode: ThreadMode::Message,
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            };
            let actor = PersistedActorRef::System;
            let winner = prepare_projected_turn_upsert(
                db,
                "turn",
                "thread",
                &turn,
                None,
                None,
                None,
                Some(&actor),
                true,
                now,
                now,
            )
            .await?;
            let contender_turn = Turn {
                mode: ThreadMode::Chat,
                ..turn
            };
            let contender = prepare_projected_turn_upsert(
                db,
                "turn",
                "thread",
                &contender_turn,
                None,
                None,
                None,
                Some(&actor),
                true,
                now,
                now,
            )
            .await?;

            let first = db.begin().await?;
            apply_prepared_turn_upsert(&first, winner).await?;
            let other = fixture.connect().await?;
            let second = other.begin().await?;
            let pid: i32 = second
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid()",
                ))
                .await?
                .context("missing contender backend")?
                .try_get_by_index(0)?;
            let writer = tokio::spawn(async move {
                let result = apply_prepared_turn_upsert(&second, contender).await;
                if result.is_ok() {
                    second.commit().await?;
                } else {
                    second.rollback().await?;
                }
                Ok::<_, anyhow::Error>(result)
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let waiting: bool = db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0",
                            [pid.into()],
                        ))
                        .await?
                        .context("missing lock observation")?
                        .try_get_by_index(0)?;
                    if waiting {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("contender did not reach the insert conflict")??;
            let expected_mode = if commit_first {
                first.commit().await?;
                let actual = writer
                    .await??
                    .err()
                    .context("stale projection overwrote the turn")?;
                assert!(
                    actual
                        .to_string()
                        .contains("changed during projection preparation")
                );
                ThreadMode::Message
            } else {
                first.rollback().await?;
                writer.await???;
                ThreadMode::Chat
            };
            let (_, persisted) = find_turn_collaboration(&other, "thread", "turn")
                .await?
                .context("turn did not persist")?;
            assert_eq!(persisted.mode.effective_mode, expected_mode);
            other.close().await?;
            Ok(())
        })
        .await?;
    }
    Ok(())
}
