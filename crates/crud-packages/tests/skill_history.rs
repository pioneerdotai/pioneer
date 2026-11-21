use pioneer_crud_packages::package_store::PackageStore;
use pioneer_crud_packages::skill_audit_event::SkillAuditEventRecord;
use pioneer_crud_packages::skill_dependency_snapshot::SkillDependencySnapshotRecord;
use pioneer_crud_packages::skill_records::SkillInstallationRecord;
use pioneer_crud_packages::turn_skill_binding::TurnSkillBindingRecord;
use pioneer_protocol::SkillId;
use pioneer_sqlite::SqliteDatabase;

#[tokio::test]
async fn skill_history_survives_exact_installation_delete() {
    pioneer_postgres_test::with_database(async |fixture| {
        let reader = fixture.connect_read_only().await?;
        let store = PackageStore::new(SqliteDatabase::new(reader, fixture.connection().clone()));
        let skill_id = SkillId::new("G".repeat(21)).expect("valid skill id");
        let turn_id = "turn_000000000000000088";

        store
            .insert_skill_installation(
                &SkillInstallationRecord {
                    skill_id: skill_id.clone(),
                    owner: Some("pioneer".to_owned()),
                    slug: "historical".to_owned(),
                    version: Some("1.0.0".to_owned()),
                    source_kind: "user".to_owned(),
                    scope_key: "ws_history".to_owned(),
                    source_ref: "upload:history".to_owned(),
                    install_path: "/tmp/skills/G/historical".to_owned(),
                    trust_level: "community".to_owned(),
                    fingerprint: "fp-history".to_owned(),
                    updated_at_unix: 1_700_000_000,
                    pack_id: None,
                    pack_member_key: None,
                },
                1_700_000_000,
            )
            .await
            .expect("insert active installation");

        let binding = TurnSkillBindingRecord {
            skill_id: skill_id.clone(),
            skill_owner: Some("pioneer".to_owned()),
            skill_slug: "historical".to_owned(),
            skill_version: Some("1.0.0".to_owned()),
            fingerprint: "fp-history".to_owned(),
            source_kind: "user".to_owned(),
            resolved_reason: "explicit_composer_capability".to_owned(),
        };
        store
            .replace_turn_skill_bindings(turn_id, &[binding.clone()], 1_700_000_001)
            .await
            .expect("insert binding history");

        let audit = SkillAuditEventRecord {
            turn_id: Some(turn_id.to_owned()),
            skill_id: skill_id.clone(),
            skill_owner: Some("pioneer".to_owned()),
            skill_slug: "historical".to_owned(),
            source_kind: "user".to_owned(),
            action: "resolve".to_owned(),
            decision: "accepted".to_owned(),
            reason_code: None,
            details_json: "{}".to_owned(),
            created_at_unix: 1_700_000_002,
        };
        store
            .append_skill_audit_event_records(turn_id, &[audit.clone()])
            .await
            .expect("insert audit history");

        let dependency = SkillDependencySnapshotRecord {
            turn_id: Some(turn_id.to_owned()),
            skill_id: skill_id.clone(),
            skill_owner: Some("pioneer".to_owned()),
            skill_slug: "historical".to_owned(),
            source_kind: "user".to_owned(),
            diagnostics_json: "[]".to_owned(),
            created_at_unix: 1_700_000_003,
        };
        store
            .insert_skill_dependency_snapshot_record(&dependency)
            .await
            .expect("insert dependency history");

        assert!(
            store
                .delete_skill_installation(&skill_id)
                .await
                .expect("delete active installation")
        );
        assert_eq!(
            store
                .find_turn_skill_bindings(turn_id)
                .await
                .expect("read binding history"),
            vec![binding]
        );
        assert_eq!(
            store
                .list_skill_audit_event_records(&skill_id, 16)
                .await
                .expect("read audit history"),
            vec![audit]
        );
        assert_eq!(
            store
                .list_turn_skill_dependency_snapshot_records(turn_id)
                .await
                .expect("read dependency history"),
            vec![dependency]
        );
        Ok(())
    })
    .await
    .unwrap();
}
