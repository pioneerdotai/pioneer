use anyhow::{Context, Result};
use pioneer_crud_conversations::thread_tree::{
    create_thread_folder, delete_thread_folder_promote, get_thread_placement, list_thread_folders,
    move_folder, move_thread_to_folder,
};
use pioneer_postgres_test::with_database;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::ConnectionTrait;

#[tokio::test]
async fn folder_topology_serializes_competing_moves_and_promotes_children() -> Result<()> {
    with_database(async |fixture| {
        let writer = fixture.connection();
        writer.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws', 'Workspace'), ('foreign', 'Foreign');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('local-thread', 'ws', '', 'chat', 'model', 'provider', 'active'),
                    ('foreign-thread', 'foreign', '', 'chat', 'model', 'provider', 'active');",
        ).await?;
        let db = SqliteDatabase::new(fixture.connect_read_only().await?, writer.clone());
        let other =
            SqliteDatabase::new(fixture.connect_read_only().await?, fixture.connect().await?);
        let left = create_thread_folder(&db, "ws", None, "Left").await?;
        let right = create_thread_folder(&db, "ws", None, "Right").await?;
        let (move_left, move_right) = tokio::join!(
            move_folder(&db, "ws", &left.id, Some(&right.id)),
            move_folder(&other, "ws", &right.id, Some(&left.id)),
        );
        let (root, child, refusal) = if move_left.is_ok() {
            (&right, &left, move_right)
        } else {
            (&left, &right, move_left)
        };
        assert!(
            refusal
                .expect_err("cycle was admitted")
                .to_string()
                .contains("descendant")
        );
        let folders = list_thread_folders(&db, "ws").await?;
        let child_row = folders
            .iter()
            .find(|folder| folder.id == child.id)
            .context("child folder was lost")?;
        assert_eq!(
            child_row.parent_folder_id.as_deref(),
            Some(root.id.as_str())
        );

        assert!(
            move_thread_to_folder(&db, "ws", "foreign-thread", Some(&root.id))
                .await
                .is_err()
        );
        assert!(get_thread_placement(&db, "foreign-thread").await?.is_none());
        move_thread_to_folder(&db, "ws", "local-thread", Some(&root.id)).await?;
        assert!(delete_thread_folder_promote(&db, "ws", &root.id).await?);
        assert!(!delete_thread_folder_promote(&db, "ws", &root.id).await?);
        let folders = list_thread_folders(&db, "ws").await?;
        assert_eq!(folders.len(), 1);
        assert_eq!(folders[0].id, child.id);
        assert_eq!(folders[0].parent_folder_id, None);
        let placement = get_thread_placement(&db, "local-thread")
            .await?
            .context("promotion lost the placement")?;
        assert_eq!(placement.workspace_id, "ws");
        assert_eq!(placement.folder_id, None);
        Ok(())
    })
    .await
}
