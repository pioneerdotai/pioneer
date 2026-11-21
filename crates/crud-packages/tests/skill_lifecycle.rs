use anyhow::Result;
use pioneer_crud_packages::{
    mcp_audit_event, mcp_installation_transactions, mcp_server_installation, plugins,
    skill_audit_event, skill_installation, skill_lifecycle, skill_workspace_policy,
};
use pioneer_postgres_test::TestDatabase;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[path = "support/plugin_ownership.rs"]
mod plugin_fixture;
use plugin_fixture::{mcp, mcp_audit, ownership, parent, policy, skill, skill_audit};

async fn package(fixture: &TestDatabase) -> Result<SqliteDatabase> {
    let connection = fixture.connection();
    connection
        .execute_unprepared("INSERT INTO workspace (id, name) VALUES ('ws','Workspace')")
        .await?;
    let publication = connection.begin().await?;
    plugins::insert_parent(&publication, &parent("plugin-a", "ws"), None).await?;
    publication.commit().await?;
    Ok(SqliteDatabase::new(
        fixture.connect_read_only().await?,
        connection.clone(),
    ))
}

#[tokio::test]
async fn native_skill_policy_audit_and_ownership_commit_together_without_upload() -> Result<()> {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = package(fixture).await?;
        let row = skill('A', "ws");
        let owner = ownership("plugin-a", row.skill_id.as_str(), "one");
        assert!(
            skill_lifecycle::install(
                &db,
                &row,
                &policy(&row),
                &[skill_audit(&row)],
                None,
                Some(&owner),
                1
            )
            .await?
        );
        assert_eq!(
            skill_installation::find_skill_installation(&db, &row.skill_id)
                .await?
                .unwrap()
                .id,
            row.skill_id.as_str()
        );
        assert_eq!(
            plugins::owner(&db, "skill", row.skill_id.as_str())
                .await?
                .unwrap()
                .package_id,
            "plugin-a"
        );
        assert_eq!(plugins::components(&db, "plugin-a").await?.len(), 1);
        let policy = skill_workspace_policy::find_workspace_skill_policy(&db, "ws", &row.skill_id)
            .await?
            .unwrap();
        assert_eq!(policy.enabled, Some(false));
        assert_eq!(policy.allow_implicit_invocation, Some(false));
        let events = skill_audit_event::list_skill_audit_events(&db, &row.skill_id, 10).await?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, "install");
        let restarted = fixture.connect_read_only().await?;
        assert!(
            plugins::owner(&restarted, "skill", row.skill_id.as_str())
                .await?
                .is_some()
        );
        restarted.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn failed_ownership_insert_rolls_back_native_rows_policies_and_audits() -> Result<()> {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = package(fixture).await?;
        db.execute_unprepared(
            "CREATE FUNCTION fail_plugin_link() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'injected link failure'; END $$;
             CREATE TRIGGER fail_plugin_link BEFORE INSERT ON package_component
             FOR EACH ROW EXECUTE FUNCTION fail_plugin_link()",
        )
        .await?;
        let row = skill('A', "ws");
        let owner = ownership("plugin-a", row.skill_id.as_str(), "one");
        let error = skill_lifecycle::install(
            &db,
            &row,
            &policy(&row),
            &[skill_audit(&row)],
            None,
            Some(&owner),
            1,
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("injected link failure"));
        assert!(
            skill_installation::find_skill_installation(&db, &row.skill_id)
                .await?
                .is_none()
        );
        assert!(plugins::components(&db, "plugin-a").await?.is_empty());
        assert!(
            skill_workspace_policy::list_workspace_skill_policies(&db, "ws")
                .await?
                .is_empty()
        );
        assert!(
            skill_audit_event::list_skill_audit_events(&db, &row.skill_id, 10)
                .await?
                .is_empty()
        );

        let row = mcp("mcp-a", "ws", "internal-a");
        let owner = ownership("plugin-a", "mcp-a", "server");
        let tx = db.begin().await?;
        let error = mcp_installation_transactions::publish(
            &tx,
            &row,
            &mcp_audit(&row),
            Some(&owner),
            None,
            Some(plugins::prepare_link(&owner, "mcp")),
            chrono::DateTime::from_timestamp(1, 0)
                .unwrap()
                .fixed_offset(),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("injected link failure"));
        tx.rollback().await?;
        assert!(
            mcp_server_installation::find_mcp_server_installation(
                &db,
                "workspace",
                "ws",
                "internal-a"
            )
            .await?
            .is_none()
        );
        assert!(
            mcp_audit_event::list_recent_mcp_audit_events(&db, "internal-a", 10)
                .await?
                .is_empty()
        );
        Ok(())
    })
    .await
}
