//! Schema lifecycle and native constraint proof, independent of CRUD preflight.
use anyhow::{Context, Result};
use migration::{Migrator, MigratorTrait, SchemaManager};
use pioneer_entity::{app_installation, package_installation, workspace};
use pioneer_postgres_test::with_empty_database;
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DatabaseConnection, DbBackend, DbErr, EntityTrait, Set,
    Statement, TransactionError, TransactionTrait,
};

async fn install(
    db: &DatabaseConnection,
    id: &str,
    workspace: &str,
) -> Result<app_installation::Model, TransactionError<DbErr>> {
    let id = id.to_owned();
    let workspace = workspace.to_owned();
    db.transaction::<_, _, DbErr>(|tx| {
        Box::pin(async move {
            let created = chrono::DateTime::from_timestamp(1_700_000_000, 123_000_000)
                .unwrap()
                .fixed_offset();
            package_installation::Entity::insert(package_installation::ActiveModel {
                id: Set(id.clone()),
                workspace_id: Set(workspace),
                source_upload_id: Set(format!("upload-{id}")),
                package_path: Set(format!("/managed/{id}/package")),
                data_path: Set(format!("/managed/{id}/data")),
                package_fingerprint: Set("package".into()),
                enabled: Set(true),
                allow_implicit_invocation: Set(false),
                state: Set("installed".into()),
                revision: Set(7),
                created_at: Set(created),
                updated_at: Set(created),
                kind: Set("app".into()),
                ..Default::default()
            })
            .exec(tx)
            .await?;
            app_installation::ActiveModel {
                id: Set(id),
                slug: Set("terminal".into()),
                manifest_json: Set(
                    r#"{ "slug": "terminal", "name": "Terminal", "version": "1.0.0" }"#.into(),
                ),
                localizations_json: Set(r#"{"ru":{"name":"Терминал"}}"#.into()),
            }
            .insert(tx)
            .await
        })
    })
    .await
}

async fn assert_no_parent(db: &DatabaseConnection, id: &str) {
    assert!(
        package_installation::Entity::find_by_id(id)
            .one(db)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn app_workspace_rules_reapply_enforces_scope_and_preserves_installations() -> Result<()> {
    with_empty_database(async |fixture| {
        let db = fixture.connection();
        Migrator::up(db, None).await?;
        for id in ["ws-one", "ws-two"] {
            let created = chrono::DateTime::from_timestamp(1_700_000_000, 0)
                .unwrap()
                .fixed_offset();
            workspace::ActiveModel {
                id: Set(id.into()),
                name: Set(id.into()),
                is_active: Set(true),
                is_current: Set(false),
                created_at: Set(created),
                updated_at: Set(created),
            }
            .insert(db)
            .await
            .unwrap();
        }
        let original = install(db, "app-one", "ws-one").await.unwrap();
        let second = install(db, "app-two", "ws-two").await.unwrap();
        assert!(
            install(db, "same-workspace-duplicate", "ws-one")
                .await
                .is_err(),
            "database must enforce workspace uniqueness without CRUD preflight"
        );
        assert_no_parent(db, "same-workspace-duplicate").await;
        // The insert-only scope guard relies on immutable App identity. Both
        // fields below satisfy the manifest CHECK; the identity guard rejects it.
        let mut renamed: app_installation::ActiveModel = original.clone().into();
        renamed.slug = Set("renamed".into());
        renamed.manifest_json = Set(r#"{"slug":"renamed"}"#.into());
        assert!(renamed.update(db).await.is_err());
        // The PostgreSQL rules migration owns triggers separately from table data.
        // Roll back those rules, verify that the table survives, then reinstall them.
        let applied = Migrator::get_applied_migrations(db).await?;
        let rules = applied
            .iter()
            .position(|migration| migration.name() == "m20261119_000010_database_rules")
            .context("app rules migration is applied")?;
        let rollback_steps = u32::try_from(applied.len() - rules)?;
        Migrator::down(db, Some(rollback_steps)).await?;
        assert!(SchemaManager::new(db).has_table("app_installation").await?);
        assert!(!slug_trigger_installed(db).await?);
        Migrator::up(db, None).await?;
        assert!(slug_trigger_installed(db).await?);
        assert!(
            install(db, "reapplied-existing-duplicate", "ws-one")
                .await
                .is_err()
        );
        assert_no_parent(db, "reapplied-existing-duplicate").await;
        for (id, expected) in [("app-one", original), ("app-two", second)] {
            assert_eq!(
                app_installation::Entity::find_by_id(id)
                    .one(db)
                    .await
                    .unwrap()
                    .unwrap(),
                expected
            );
            package_installation::Entity::delete_by_id(id)
                .exec(db)
                .await
                .unwrap();
            assert!(
                app_installation::Entity::find_by_id(id)
                    .one(db)
                    .await
                    .unwrap()
                    .is_none(),
                "package cleanup must remove only the owned App metadata"
            );
        }
        install(db, "reapplied-one", "ws-one").await.unwrap();
        install(db, "reapplied-two", "ws-two").await.unwrap();
        assert!(
            install(db, "reapplied-duplicate", "ws-one").await.is_err(),
            "reapply must restore workspace-scoped uniqueness"
        );
        assert_no_parent(db, "reapplied-duplicate").await;
        Ok(())
    })
    .await
}

async fn slug_trigger_installed(db: &DatabaseConnection) -> Result<bool> {
    let row = db.query_one_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'app_installation'::regclass AND tgname = $1 AND NOT tgisinternal) AS installed",
        ["app_workspace_slug_insert".into()],
    )).await?.expect("trigger catalog aggregate must return one row");
    Ok(row.try_get("", "installed")?)
}
