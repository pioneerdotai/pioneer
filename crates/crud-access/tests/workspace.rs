use anyhow::{Context, Result};
use pioneer_crud_access::workspace;
use pioneer_postgres_test::with_database;
use sea_orm::TransactionTrait;

#[tokio::test]
async fn workspace_identity_and_current_selection_survive_reconnect() -> Result<()> {
    with_database(async |fixture| {
        let database = fixture.connection();
        assert!(!workspace::insert_default(database).await?);
        let id = "W00000000000000000001";

        let create = database.begin().await?;
        let (first, inserted) = workspace::create(&create, id, Some("First name".into())).await?;
        assert!(inserted);
        assert!(!first.is_current);
        create.commit().await?;

        let independent = fixture.connect().await?;
        let select = independent.begin().await?;
        let (same, inserted) = workspace::create(&select, id, Some("Second name".into())).await?;
        assert!(!inserted);
        assert_eq!(same.name, "First name");
        workspace::set_current(&select, same, chrono::Utc::now().fixed_offset()).await?;
        select.commit().await?;
        independent.close().await?;

        let observed = workspace::find(database, id)
            .await?
            .context("missing workspace")?;
        assert_eq!(observed.name, "First name");
        assert!(observed.is_current);
        let all = workspace::list(database).await?;
        assert_eq!(
            all.iter()
                .filter(|row| row.is_current)
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            vec![id],
        );
        Ok(())
    })
    .await
}
