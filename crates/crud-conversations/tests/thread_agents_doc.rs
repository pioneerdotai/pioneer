use anyhow::Result;
use pioneer_crud_conversations::thread_agents_doc::{
    ThreadAgentsDocError, ThreadAgentsDocRepository, ThreadAgentsDocSaveReason,
    ThreadAgentsDocStatus, now,
};
use pioneer_crud_conversations::thread_tree::{create_thread_folder, move_thread_to_folder};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::TestDatabase;
use sea_orm::{ConnectionTrait, DbBackend, Statement};

async fn create_workspace(fixture: &TestDatabase, workspace_id: &str) -> Result<PostgresDatabase> {
    let writer = fixture.connection();
    writer
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO workspace (id, name) VALUES ($1, 'Workspace')",
            [workspace_id.into()],
        ))
        .await?;
    Ok(PostgresDatabase::new(
        fixture.connect_read_only().await?,
        writer.clone(),
    ))
}

#[tokio::test]
async fn thread_agents_doc_repository_round_trips_draft_save_archive() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = create_workspace(fixture, "ws_agents_doc_crud").await?;
        let store = ThreadAgentsDocRepository::new();

        let draft = store
            .create_draft(&db, "ws_agents_doc_crud", None, now(), Some("user-1"))
            .await
            .expect("draft should create");
        assert_eq!(draft.status, ThreadAgentsDocStatus::Draft);
        assert_eq!(draft.version, 1);
        assert!(draft.content.is_empty());

        let duplicate = store
            .create_draft(&db, "ws_agents_doc_crud", None, now(), Some("user-1"))
            .await
            .expect("duplicate draft create should return existing draft");
        assert_eq!(duplicate.id, draft.id);

        let active = store
            .save_content(
                &db,
                "ws_agents_doc_crud",
                None,
                "Use cargo test.\r\nKeep docs short.",
                Some(draft.version),
                now(),
                Some("user-1"),
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("non-empty save should activate doc");
        assert_eq!(active.status, ThreadAgentsDocStatus::Active);
        assert_eq!(active.version, 2);
        assert_eq!(active.content, "Use cargo test.\nKeep docs short.");

        let revisions = store
            .list_revisions(&db, active.id.as_str())
            .await
            .expect("revisions should list");
        assert_eq!(revisions.len(), 1);
        assert_eq!(revisions[0].version, active.version);
        assert_eq!(revisions[0].save_reason, ThreadAgentsDocSaveReason::Manual);

        let unchanged = store
            .save_content(
                &db,
                "ws_agents_doc_crud",
                None,
                "Use cargo test.\nKeep docs short.",
                Some(active.version),
                now(),
                Some("user-1"),
                ThreadAgentsDocSaveReason::Autosave,
            )
            .await
            .expect("unchanged save should no-op");
        assert_eq!(unchanged.version, active.version);
        assert_eq!(
            store
                .list_revisions(&db, active.id.as_str())
                .await
                .expect("revisions should list")
                .len(),
            1
        );

        let conflict = store
            .save_content(
                &db,
                "ws_agents_doc_crud",
                None,
                "Changed",
                Some(1),
                now(),
                Some("user-2"),
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect_err("stale expected version should conflict");
        assert!(matches!(
            conflict,
            ThreadAgentsDocError::VersionConflict {
                expected: 1,
                actual: 2
            }
        ));

        let archived = store
            .archive(
                &db,
                "ws_agents_doc_crud",
                None,
                Some(active.version),
                now(),
                Some("user-1"),
            )
            .await
            .expect("archive should succeed")
            .expect("archive should return archived doc");
        assert_eq!(archived.status, ThreadAgentsDocStatus::Archived);
        assert_eq!(archived.version, 3);

        let after_archive = store
            .find_explicit(&db, "ws_agents_doc_crud", None)
            .await
            .expect("explicit lookup should succeed");
        assert!(after_archive.is_none());

        let replacement = store
            .create_draft(&db, "ws_agents_doc_crud", None, now(), Some("user-1"))
            .await
            .expect("archived doc should not block replacement draft");
        assert_eq!(replacement.status, ThreadAgentsDocStatus::Draft);
        assert_ne!(replacement.id, archived.id);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn thread_agents_doc_resolver_uses_nearest_active_ancestor() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = create_workspace(fixture, "ws_agents_doc_resolve").await?;
        let store = ThreadAgentsDocRepository::new();

        db.execute_unprepared(
            "INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread_agents_doc_resolve_thread', 'ws_agents_doc_resolve',
                     '', 'agent', 'model', 'provider', 'active')",
        )
        .await?;

        let root = store
            .save_content(
                &db,
                "ws_agents_doc_resolve",
                None,
                "root instructions",
                None,
                now(),
                Some("user-1"),
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("root doc should save");

        let parent = create_thread_folder(&db, "ws_agents_doc_resolve", None, "Parent")
            .await
            .expect("parent folder should create");
        let child = create_thread_folder(
            &db,
            "ws_agents_doc_resolve",
            Some(parent.id.as_str()),
            "Child",
        )
        .await
        .expect("child folder should create");

        let root_resolution = store
            .resolve_for_folder(&db, "ws_agents_doc_resolve", None)
            .await
            .expect("root resolution should succeed")
            .expect("root doc should resolve");
        assert_eq!(root_resolution.doc.id, root.id);
        assert!(!root_resolution.inherited);

        store
            .save_content(
                &db,
                "ws_agents_doc_resolve",
                Some(parent.id.as_str()),
                "parent instructions",
                None,
                now(),
                Some("user-1"),
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("parent doc should save");

        store
            .create_draft(
                &db,
                "ws_agents_doc_resolve",
                Some(child.id.as_str()),
                now(),
                Some("user-1"),
            )
            .await
            .expect("child blank draft should create");

        let inherited = store
            .resolve_for_folder(&db, "ws_agents_doc_resolve", Some(child.id.as_str()))
            .await
            .expect("child resolution should succeed")
            .expect("parent doc should resolve for child");
        assert_eq!(inherited.doc.content, "parent instructions");
        assert_eq!(
            inherited.source_folder_id.as_deref(),
            Some(parent.id.as_str())
        );
        assert_eq!(inherited.source_path, vec!["Parent".to_owned()]);
        assert!(inherited.inherited);

        let child_doc = store
            .save_content(
                &db,
                "ws_agents_doc_resolve",
                Some(child.id.as_str()),
                "child instructions",
                None,
                now(),
                Some("user-1"),
                ThreadAgentsDocSaveReason::Manual,
            )
            .await
            .expect("child doc should save");
        let child_resolution = store
            .resolve_for_folder(&db, "ws_agents_doc_resolve", Some(child.id.as_str()))
            .await
            .expect("child resolution should succeed")
            .expect("child doc should resolve");
        assert_eq!(child_resolution.doc.id, child_doc.id);
        assert_eq!(
            child_resolution.source_path,
            vec!["Parent".to_owned(), "Child".to_owned()]
        );
        assert!(!child_resolution.inherited);

        store
            .archive(
                &db,
                "ws_agents_doc_resolve",
                Some(child.id.as_str()),
                Some(child_doc.version),
                now(),
                Some("user-1"),
            )
            .await
            .expect("child archive should succeed");

        move_thread_to_folder(
            &db,
            "ws_agents_doc_resolve",
            "thread_agents_doc_resolve_thread",
            Some(child.id.as_str()),
        )
        .await
        .expect("thread placement should save");
        let thread_resolution = store
            .resolve_for_thread(
                &db,
                "ws_agents_doc_resolve",
                "thread_agents_doc_resolve_thread",
            )
            .await
            .expect("thread resolution should succeed")
            .expect("parent doc should resolve after child archive");
        assert_eq!(thread_resolution.doc.content, "parent instructions");
        assert_eq!(
            thread_resolution.resolved_for_folder_id,
            Some(child.id.clone())
        );

        let mismatch = store
            .resolve_for_folder(&db, "other_workspace", Some(child.id.as_str()))
            .await
            .expect_err("folder workspace mismatch should fail");
        assert!(matches!(
            mismatch,
            ThreadAgentsDocError::WorkspaceMismatch { .. }
        ));
        Ok(())
    })
    .await
    .unwrap();
}
