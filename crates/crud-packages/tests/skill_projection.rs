use pioneer_crud_packages::skill_audit_event::{
    SkillAuditEventRecord, insert_skill_audit_events, list_skill_audit_events,
    list_turn_skill_audit_events, skill_audit_event_record_from_model,
};
use pioneer_crud_packages::skill_installation::find_skill_installation;
use pioneer_crud_packages::skill_records::WorkspaceSkillPolicyRecord;
use pioneer_crud_packages::skill_workspace_policy::{
    delete_workspace_skill_policy, find_workspace_skill_policy, list_workspace_skill_policies,
    upsert_workspace_skill_policy, workspace_skill_policy_record_from_model,
};
use pioneer_crud_packages::turn_skill_binding::{
    TurnSkillBindingRecord, find_turn_skill_bindings, prepare_turn_skill_bindings,
    replace_prepared_turn_skill_bindings, turn_skill_binding_record_from_model,
};
use pioneer_protocol::{SkillId, generate_id};
use sea_orm::{DatabaseConnection, TransactionTrait, entity::prelude::DateTimeWithTimeZone};

fn at(seconds: i64) -> DateTimeWithTimeZone {
    chrono::DateTime::from_timestamp(seconds, 0)
        .unwrap()
        .fixed_offset()
}

async fn replace_bindings(
    db: &DatabaseConnection,
    turn_id: &str,
    bindings: &[TurnSkillBindingRecord],
    seconds: i64,
) -> anyhow::Result<()> {
    let replacement = prepare_turn_skill_bindings(turn_id, bindings, at(seconds));
    let tx = db.begin().await?;
    replace_prepared_turn_skill_bindings(&tx, replacement).await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn skill_audit_event_persistence_orders_by_created_at() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        let turn_id = "turn_000000000000000077";

        let older = SkillAuditEventRecord {
            turn_id: Some(turn_id.to_owned()),
            skill_id: SkillId::new("E".repeat(21)).expect("valid skill id"),
            skill_owner: Some("pioneer".to_owned()),
            skill_slug: "agent-browser".to_owned(),
            source_kind: "registry".to_owned(),
            action: "resolve_blocked".to_owned(),
            decision: "blocked".to_owned(),
            reason_code: Some("dependency_missing".to_owned()),
            details_json: "{\"reason\":\"dependency_missing\"}".to_owned(),
            created_at_unix: 1_700_000_000,
        };
        let newer = SkillAuditEventRecord {
            turn_id: Some(turn_id.to_owned()),
            skill_id: older.skill_id.clone(),
            skill_owner: older.skill_owner.clone(),
            skill_slug: older.skill_slug.clone(),
            source_kind: "registry".to_owned(),
            action: "runtime_blocked".to_owned(),
            decision: "blocked".to_owned(),
            reason_code: Some("runtime.dependency_missing".to_owned()),
            details_json: "{\"reason\":\"runtime.dependency_missing\"}".to_owned(),
            created_at_unix: 1_700_000_100,
        };

        insert_skill_audit_events(db, Some(turn_id), &[older.clone(), newer.clone()])
            .await
            .expect("append audit events");

        let rows = list_turn_skill_audit_events(db, turn_id)
            .await
            .and_then(|rows| {
                rows.into_iter()
                    .map(skill_audit_event_record_from_model)
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .expect("list turn audit events");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].action, older.action);
        assert_eq!(rows[1].action, newer.action);

        let timeline = list_skill_audit_events(db, &older.skill_id, 16)
            .await
            .and_then(|rows| {
                rows.into_iter()
                    .map(skill_audit_event_record_from_model)
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .expect("list audit timeline");
        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0].action, newer.action);
        assert_eq!(timeline[1].action, older.action);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn workspace_skill_policy_upsert_and_delete_round_trip() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();

        let first = WorkspaceSkillPolicyRecord {
            id: generate_id(21),
            workspace_id: "ws_000000000000000001".to_owned(),
            skill_id: SkillId::new("D".repeat(21)).expect("valid bundled skill id"),
            enabled: Some(false),
            allow_implicit_invocation: Some(false),
        };

        assert!(
            find_skill_installation(db, &first.skill_id)
                .await
                .expect("bundled installation lookup")
                .is_none(),
            "bundled policy must not require an installation row"
        );

        upsert_workspace_skill_policy(db, &first, at(1_700_000_000), at(1_700_000_000))
            .await
            .expect("first policy upsert");

        let second = WorkspaceSkillPolicyRecord {
            enabled: Some(true),
            allow_implicit_invocation: Some(true),
            ..first.clone()
        };
        upsert_workspace_skill_policy(db, &second, at(1_700_000_100), at(1_700_000_100))
            .await
            .expect("second policy upsert");

        let rows = list_workspace_skill_policies(db, first.workspace_id.as_str())
            .await
            .and_then(|rows| {
                rows.into_iter()
                    .map(workspace_skill_policy_record_from_model)
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .expect("list workspace policies");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], second);
        assert_eq!(
            find_workspace_skill_policy(db, first.workspace_id.as_str(), &first.skill_id)
                .await
                .and_then(|row| row
                    .map(workspace_skill_policy_record_from_model)
                    .transpose())
                .expect("find exact policy"),
            Some(second.clone())
        );

        assert!(
            delete_workspace_skill_policy(db, first.workspace_id.as_str(), &first.skill_id)
                .await
                .expect("delete exact workspace policy")
        );

        let rows = list_workspace_skill_policies(db, first.workspace_id.as_str())
            .await
            .and_then(|rows| {
                rows.into_iter()
                    .map(workspace_skill_policy_record_from_model)
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .expect("list workspace policies after delete");
        assert!(rows.is_empty());
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn replace_and_find_turn_skill_bindings_round_trip() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        let turn_id = "turn_000000000000000001";

        let first = vec![
            TurnSkillBindingRecord {
                skill_id: SkillId::new("A".repeat(21)).expect("valid skill id"),
                skill_owner: Some("pioneer".to_owned()),
                skill_slug: "duplicate-label".to_owned(),
                skill_version: Some("1.0.0".to_owned()),
                fingerprint: "fp-alpha".to_owned(),
                source_kind: "registry".to_owned(),
                resolved_reason: "explicit_composer_capability".to_owned(),
            },
            TurnSkillBindingRecord {
                skill_id: SkillId::new("B".repeat(21)).expect("valid skill id"),
                skill_owner: Some("pioneer".to_owned()),
                skill_slug: "duplicate-label".to_owned(),
                skill_version: None,
                fingerprint: "fp-beta".to_owned(),
                source_kind: "user".to_owned(),
                resolved_reason: "path_match".to_owned(),
            },
        ];

        replace_bindings(db, turn_id, &first, 1_700_000_000)
            .await
            .expect("initial turn skill bindings should persist");

        let first_read = find_turn_skill_bindings(db, turn_id)
            .await
            .and_then(|rows| {
                rows.into_iter()
                    .map(turn_skill_binding_record_from_model)
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .expect("must read persisted turn skill bindings");
        assert_eq!(first_read, first);

        let second = vec![TurnSkillBindingRecord {
            skill_id: SkillId::new("C".repeat(21)).expect("valid skill id"),
            skill_owner: None,
            skill_slug: "gamma-skill".to_owned(),
            skill_version: Some("2.1.0".to_owned()),
            fingerprint: "fp-gamma".to_owned(),
            source_kind: "system".to_owned(),
            resolved_reason: "explicit_composer_capability".to_owned(),
        }];

        replace_bindings(db, turn_id, &second, 1_700_000_100)
            .await
            .expect("second replacement should overwrite prior bindings");

        let second_read = find_turn_skill_bindings(db, turn_id)
            .await
            .and_then(|rows| {
                rows.into_iter()
                    .map(turn_skill_binding_record_from_model)
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .expect("must read replaced turn skill bindings");
        assert_eq!(second_read, second);
        Ok(())
    })
    .await
    .unwrap();
}
