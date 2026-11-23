use pioneer_crud_packages::turn_mcp_binding::{
    TurnMcpBindingRecord, list_turn_mcp_bindings, replace_turn_mcp_bindings,
    turn_mcp_binding_record_from_model,
};
use sea_orm::TransactionTrait;

#[tokio::test]
async fn turn_mcp_binding_repositories_round_trip_all_digest_fields() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();

        let beta_updated_id = "installed-beta".to_owned();

        let turn_bindings = vec![TurnMcpBindingRecord {
            server_installation_id: beta_updated_id.clone(),
            server_name: "beta".to_owned(),
            raw_tool_name: "send".to_owned(),
            callable_name: "mcp_beta_send".to_owned(),
            canonical_callable_name: "mcp_beta_send".to_owned(),
            provider_callable_name: "mcp__pioneer__mcp_beta_send".to_owned(),
            catalog_version: "catalog-v1".to_owned(),
            fingerprint: "fingerprint-beta-updated".to_owned(),
            canonical_schema_fingerprint: "canonical-schema-v1".to_owned(),
            provider_schema_fingerprint: "provider-schema-v1".to_owned(),
            annotations_json: r#"{"readOnlyHint":true}"#.to_owned(),
            annotations_digest: "annotations-v1".to_owned(),
            effective_timeout_ms: 20_000,
            runtime_generation: 7,
            projection_activation_generation: 2,
            selection_reason: "explicit_composer_capability".to_owned(),
            capability_id: Some("mcp:workspace:beta:send".to_owned()),
        }];
        let tx = db.begin().await?;
        replace_turn_mcp_bindings(
            &tx,
            "turn_mcp_roundtrip",
            &turn_bindings,
            chrono::DateTime::from_timestamp(1_700_000_005, 0)
                .unwrap()
                .fixed_offset(),
        )
        .await?;
        tx.commit().await?;
        let read_bindings = list_turn_mcp_bindings(db, "turn_mcp_roundtrip")
            .await?
            .into_iter()
            .map(turn_mcp_binding_record_from_model)
            .collect::<Vec<_>>();
        assert_eq!(read_bindings, turn_bindings);
        Ok(())
    })
    .await
    .unwrap();
}
