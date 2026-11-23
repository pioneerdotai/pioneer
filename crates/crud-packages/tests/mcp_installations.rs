use anyhow::Result;
use pioneer_crud_packages::{
    McpAuditEventRecord, McpServerCatalogSnapshotRecord, McpServerInstallationRecord,
    mcp_audit_event, mcp_installation_transactions, mcp_server_catalog_snapshot,
    mcp_server_installation,
};
use pioneer_postgres_test::with_database;
use sea_orm::TransactionTrait;
use sea_orm::prelude::{DateTimeUtc, DateTimeWithTimeZone};

fn at(offset: i64) -> DateTimeWithTimeZone {
    DateTimeUtc::from_timestamp(1_700_000_000 + offset, 0)
        .expect("fixture timestamp")
        .fixed_offset()
}

fn sample_mcp_installation(name: &str) -> McpServerInstallationRecord {
    McpServerInstallationRecord {
        id: None,
        scope_kind: "workspace".to_owned(),
        scope_key: "ws_mcp".to_owned(),
        name: name.to_owned(),
        display_name: None,
        source_kind: "config".to_owned(),
        source_ref: serde_json::json!({
            "source_kind": "config",
            "server": name,
            "transport": "stdio"
        })
        .to_string(),
        transport_kind: "stdio".to_owned(),
        transport_json: serde_json::json!({
            "type": "stdio",
            "command": "npx",
            "args": [],
            "env": {},
            "startup_timeout_ms": 10_000,
            "tool_timeout_ms": 60_000
        })
        .to_string(),
        auth_json: "{}".to_owned(),
        secret_refs_json: "[]".to_owned(),
        enabled: true,
        allow_implicit_invocation: true,
        required: false,
        fingerprint: format!("fingerprint-{name}"),
        updated_at_unix: 0,
    }
}
#[tokio::test]
async fn mcp_installation_catalog_and_audit_round_trip() -> Result<()> {
    with_database(async |fixture| {
        let database = fixture.connect().await?;
        let beta = sample_mcp_installation("beta");
        let beta_id =
            mcp_server_installation::upsert_mcp_server_installation(&database, &beta, at(0), at(0))
                .await
                .expect("first MCP installation upsert should succeed");
        let mut alpha = sample_mcp_installation("alpha");
        alpha.enabled = false;
        alpha.allow_implicit_invocation = false;
        mcp_server_installation::upsert_mcp_server_installation(&database, &alpha, at(1), at(1))
            .await
            .expect("second MCP installation upsert should succeed");

        let mut beta_updated = beta.clone();
        beta_updated.fingerprint = "fingerprint-beta-updated".to_owned();
        beta_updated.required = true;
        let beta_updated_id = mcp_server_installation::upsert_mcp_server_installation(
            &database,
            &beta_updated,
            at(2),
            at(2),
        )
        .await
        .expect("MCP installation update should succeed");
        assert_eq!(beta_id, beta_updated_id);

        let rows = mcp_server_installation::list_mcp_server_installations(
            &database,
            "workspace",
            "ws_mcp",
        )
        .await
        .expect("MCP installations list should succeed");
        assert_eq!(
            rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        assert!(!rows[0].enabled);
        assert!(!rows[0].allow_implicit_invocation);
        let beta_row = mcp_server_installation::find_mcp_server_installation(
            &database,
            "workspace",
            "ws_mcp",
            "beta",
        )
        .await
        .expect("MCP installation find should succeed")
        .expect("beta MCP installation should exist");
        assert_eq!(beta_row.fingerprint, "fingerprint-beta-updated");
        assert!(beta_row.required);

        let audit = McpAuditEventRecord {
            turn_id: None,
            server_installation_id: Some(beta_updated_id.clone()),
            server_name: "beta".to_owned(),
            raw_tool_name: None,
            callable_name: None,
            catalog_version: None,
            action: "install".to_owned(),
            decision: "allowed".to_owned(),
            reason_code: None,
            details_json: "{\"transport_kind\":\"stdio\"}".to_owned(),
            created_at_unix: 1_700_000_003,
        };
        let unpublished = sample_mcp_installation("unpublished");
        let mut invalid_audit = audit.clone();
        invalid_audit.details_json = "{".into();
        let transaction = database.begin().await?;
        assert!(
            mcp_installation_transactions::publish(
                &transaction,
                &unpublished,
                &invalid_audit,
                None,
                None,
                None,
                at(3),
            )
            .await
            .is_err()
        );
        transaction.rollback().await?;
        assert!(
            mcp_server_installation::find_mcp_server_installation(
                &database,
                "workspace",
                "ws_mcp",
                "unpublished",
            )
            .await?
            .is_none()
        );

        let transaction = database.begin().await?;
        let installed_id = mcp_installation_transactions::publish(
            &transaction,
            &beta_updated,
            &audit,
            None,
            None,
            None,
            at(3),
        )
        .await?;
        transaction.commit().await?;
        assert_eq!(installed_id, beta_updated_id);
        let audit_rows = mcp_audit_event::list_recent_mcp_audit_events(&database, "beta", 10)
            .await
            .expect("MCP audit list should succeed");
        assert_eq!(audit_rows.len(), 1);
        assert_eq!(audit_rows[0].action, "install");

        let catalog = McpServerCatalogSnapshotRecord {
            server_installation_id: beta_updated_id.clone(),
            catalog_version: "catalog-v1".to_owned(),
            server_info_json: "{\"name\":\"beta\"}".to_owned(),
            server_instructions_hash: Some("instructions-hash".to_owned()),
            tools_json: "[{\"name\":\"send\"}]".to_owned(),
            resources_json: "[]".to_owned(),
            resource_templates_json: "[]".to_owned(),
            prompts_json: "[]".to_owned(),
            generated_at_unix: 1_700_000_004,
        };
        mcp_server_catalog_snapshot::upsert_mcp_server_catalog_snapshot(
            &database,
            &catalog,
            at(4),
            at(4),
        )
        .await
        .expect("MCP catalog snapshot upsert should succeed");
        let read_catalog = mcp_server_catalog_snapshot::find_mcp_server_catalog_snapshot(
            &database,
            beta_updated_id.as_str(),
        )
        .await
        .expect("MCP catalog snapshot find should succeed")
        .expect("MCP catalog snapshot should exist");
        let read_catalog = McpServerCatalogSnapshotRecord::from(read_catalog);
        assert_eq!(read_catalog.catalog_version, "catalog-v1");
        assert_eq!(read_catalog.tools_json, "[{\"name\":\"send\"}]");

        let mut uninstall_audit = audit.clone();
        uninstall_audit.server_installation_id = None;
        uninstall_audit.server_name = "alpha".into();
        uninstall_audit.action = "uninstall".into();
        let transaction = database.begin().await?;
        mcp_installation_transactions::remove(&transaction, &alpha, &uninstall_audit, None, false)
            .await?;
        transaction.commit().await?;
        let removed = mcp_audit_event::list_recent_mcp_audit_events(&database, "alpha", 10).await?;
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].action, "uninstall");
        let rows = mcp_server_installation::list_mcp_server_installations(
            &database,
            "workspace",
            "ws_mcp",
        )
        .await
        .expect("MCP installations list after delete should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "beta");
        database.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn mcp_installation_id_lookup_preserves_workspace_and_scope_boundaries() -> Result<()> {
    with_database(async |fixture| {
        let database = fixture.connect().await?;
        let mut installation = sample_mcp_installation("saved-public-name");
        installation.id = Some("M".repeat(21));
        let id = mcp_server_installation::upsert_mcp_server_installation(
            &database,
            &installation,
            at(0),
            at(0),
        )
        .await?;
        let found = mcp_server_installation::find_mcp_server_installation_by_id(
            &database,
            &installation.scope_kind,
            &installation.scope_key,
            &id,
        )
        .await?
        .unwrap();
        assert_eq!(found.name, "saved-public-name");
        for (kind, scope) in [("workspace", "foreign-workspace"), ("user", "ws_mcp")] {
            assert!(
                mcp_server_installation::find_mcp_server_installation_by_id(
                    &database, kind, scope, &id,
                )
                .await?
                .is_none()
            );
        }
        Ok(())
    })
    .await
}
