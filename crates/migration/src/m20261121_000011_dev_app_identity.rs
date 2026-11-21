use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // SeaQuery cannot replace PostgreSQL trigger functions.
        manager
            .get_connection()
            .execute_unprepared(include_str!("sql/m20261121_000011_dev_app_identity/up.sql"))
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        // SeaQuery cannot lock tables. Fence new installs until the previous
        // function is restored within the same migration transaction.
        connection
            .execute_unprepared(include_str!(
                "sql/m20261121_000011_dev_app_identity/lock_packages.sql"
            ))
            .await?;
        let dev_packages = Query::select()
            .column(Alias::new("id"))
            .from(Alias::new("package_installation"))
            .and_where(Expr::col(Alias::new("source_upload_id")).like("dev:%"))
            .limit(1)
            .to_owned();
        if connection.query_one(&dev_packages).await?.is_some() {
            return Err(DbErr::Custom(
                "Remove dev App connections before downgrading".into(),
            ));
        }
        connection
            .execute_unprepared(include_str!(
                "sql/m20261121_000011_dev_app_identity/down.sql"
            ))
            .await?;
        Ok(())
    }
}
