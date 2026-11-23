use anyhow::Result;
use pioneer_crud_access::policy_generation::{
    append_authorization_change, current_policy_generation, ensure_code_policy_generation,
    list_authorization_changes_after,
};
use pioneer_postgres_test::with_database;
use pioneer_protocol::{AuthorizationChangeKind, AuthorizationChangeScope, PolicyGeneration};
use sea_orm::DatabaseConnection;

#[tokio::test]
async fn generation_survives_reconnect_and_code_policy_change() -> Result<()> {
    with_database(async |fixture| {
        let database = fixture.connect().await?;
        let first = ensure_code_policy_generation(&database, &"a".repeat(64)).await?;
        assert_eq!(first.policy_generation, PolicyGeneration::INITIAL);
        let changed = append_authorization_change(
            &database,
            AuthorizationChangeKind::WorkspaceAcl,
            AuthorizationChangeScope::Workspace {
                workspace_id: "workspace-red".to_owned(),
            },
        )
        .await?;
        assert_eq!(changed.policy_generation.get(), 2);
        let changes =
            list_authorization_changes_after(&database, Some(PolicyGeneration::INITIAL), 10)
                .await?;
        assert_eq!(changes, vec![changed.clone()]);
        database.close().await?;

        let restarted = fixture.connect().await?;
        assert_eq!(current_policy_generation(&restarted).await?.get(), 2);
        let redeployed = ensure_code_policy_generation(&restarted, &"b".repeat(64)).await?;
        assert_eq!(redeployed.policy_generation.get(), 3);
        assert_eq!(
            list_authorization_changes_after(&restarted, None, 10).await?,
            vec![first, changed, redeployed]
        );
        restarted.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn concurrent_committed_changes_receive_unique_monotonic_generations() -> Result<()> {
    async fn append(database: &DatabaseConnection, workspace_id: &str) -> Result<u64> {
        let change = append_authorization_change(
            database,
            AuthorizationChangeKind::WorkspaceAcl,
            AuthorizationChangeScope::Workspace {
                workspace_id: workspace_id.to_owned(),
            },
        )
        .await?;
        Ok(change.policy_generation.get())
    }

    with_database(async |fixture| {
        let database = fixture.connection();
        ensure_code_policy_generation(database, &"c".repeat(64)).await?;
        let separate_pool = fixture.connect().await?;
        let (first, second, third, fourth) = tokio::try_join!(
            append(database, "workspace-a"),
            append(&separate_pool, "workspace-b"),
            append(database, "workspace-c"),
            append(&separate_pool, "workspace-d"),
        )?;
        let mut generations = [first, second, third, fourth];
        generations.sort_unstable();
        assert_eq!(generations, [2, 3, 4, 5]);
        assert_eq!(current_policy_generation(database).await?.get(), 5);
        separate_pool.close().await?;
        Ok(())
    })
    .await
}
