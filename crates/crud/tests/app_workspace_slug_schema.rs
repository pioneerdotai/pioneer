//! Schema lifecycle and native constraint proof, independent of CRUD preflight.
use migration::{Migrator, MigratorTrait, SchemaManager};
use pioneer_entity::{app_installation, package_installation, workspace};
use sea_orm::{
    ActiveModelTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbErr,
    EntityTrait, Set, TransactionError, TransactionTrait,
};

const MIGRATION: &str = "m20261112_000001_package_installations";

// Exercise this schema owner's down/up without traversing later irreversible storage.
struct AppSchemaMigrator;
impl MigratorTrait for AppSchemaMigrator {
    fn migrations() -> Vec<Box<dyn migration::MigrationTrait>> {
        let migrations = Migrator::migrations();
        let end = migrations
            .iter()
            .position(|m| m.name() == MIGRATION)
            .unwrap()
            + 1;
        migrations.into_iter().take(end).collect()
    }
}

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
async fn app_workspace_slug_fresh_down_reapply_enforces_scope_and_preserves_guards() {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options.max_connections(1);
    let db = Database::connect(options).await.unwrap();
    // Match runtime FK OFF on the fixture's single connection.
    db.execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    AppSchemaMigrator::up(&db, None).await.unwrap();
    let migrations = AppSchemaMigrator::migrations();
    let position = migrations
        .iter()
        .position(|m| m.name() == MIGRATION)
        .unwrap();
    let rollback_steps = (migrations.len() - position) as u32;
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
        .insert(&db)
        .await
        .unwrap();
    }
    let original = install(&db, "app-one", "ws-one").await.unwrap();
    let second = install(&db, "app-two", "ws-two").await.unwrap();
    assert!(
        install(&db, "same-workspace-duplicate", "ws-one")
            .await
            .is_err(),
        "database must enforce workspace uniqueness without CRUD preflight"
    );
    assert_no_parent(&db, "same-workspace-duplicate").await;
    // The insert-only scope guard relies on immutable App identity. Both
    // fields below satisfy the manifest CHECK; the identity guard rejects it.
    let mut renamed: app_installation::ActiveModel = original.clone().into();
    renamed.slug = Set("renamed".into());
    renamed.manifest_json = Set(r#"{"slug":"renamed"}"#.into());
    assert!(renamed.update(&db).await.is_err());
    assert!(
        AppSchemaMigrator::down(&db, Some(rollback_steps))
            .await
            .is_err(),
        "removing the Apps schema must refuse live installations"
    );
    for (id, expected) in [("app-one", original), ("app-two", second)] {
        assert_eq!(
            app_installation::Entity::find_by_id(id)
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            expected
        );
        package_installation::Entity::delete_by_id(id)
            .exec(&db)
            .await
            .unwrap();
        assert!(
            app_installation::Entity::find_by_id(id)
                .one(&db)
                .await
                .unwrap()
                .is_none(),
            "package cleanup must remove only the owned App metadata"
        );
    }
    AppSchemaMigrator::down(&db, Some(rollback_steps))
        .await
        .unwrap();
    assert!(
        !SchemaManager::new(&db)
            .has_table("app_installation")
            .await
            .unwrap()
    );
    AppSchemaMigrator::up(&db, None).await.unwrap();
    install(&db, "reapplied-one", "ws-one").await.unwrap();
    install(&db, "reapplied-two", "ws-two").await.unwrap();
    assert!(
        install(&db, "reapplied-duplicate", "ws-one").await.is_err(),
        "reapply must restore workspace-scoped uniqueness"
    );
    assert_no_parent(&db, "reapplied-duplicate").await;
}
