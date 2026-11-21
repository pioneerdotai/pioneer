//! Plugin ownership and native lifecycle regressions.
use super::tests::{postgres_store_with_workspace, test_store_with_workspace};
use super::*;

use sea_orm::ConnectionTrait;

#[path = "../../crud-packages/tests/support/plugin_ownership.rs"]
mod plugin_fixture;
use plugin_fixture::*;

#[tokio::test]
async fn package_update_retains_native_identity_policy_overrides_and_successful_sibling() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws-update").await;
        let id = "P".repeat(21);
        store
            .insert_plugin_installation(&parent(&id, "ws-update"))
            .await
            .unwrap();
        let first = skill('A', "ws-update");
        let sibling = skill('B', "ws-update");
        for (row, key) in [(&first, "one"), (&sibling, "two")] {
            store
                .install_skill_lifecycle_with_ownership(
                    row,
                    &policy(row),
                    &[skill_audit(row)],
                    None,
                    Some(&ownership(&id, row.skill_id.as_str(), key)),
                    1,
                )
                .await
                .unwrap();
        }
        store
            .settle_plugin_installation(&id, 1, "installed", None)
            .await
            .unwrap();
        let native = store
            .begin_plugin_mutation(
                "ws-update",
                &id,
                1,
                "updating",
                true,
                "{\"kind\":\"native\",\"action\":\"policy\",\"native_committed\":false}",
            )
            .await
            .unwrap();
        let write = PluginNativeWrite {
            kind: pioneer_protocol::PackageKind::Plugin,
            plugin_id: id.clone(),
            expected_revision: native.revision,
            member_key: "one".into(),
            child_id: first.skill_id.to_string(),
            override_fields_json: "[\"enabled\",\"allow_implicit_invocation\"]".into(),
            pending_after: "{\"kind\":\"native\",\"native_committed\":true}".into(),
        };
        store
            .upsert_workspace_skill_policy_with_plugin_change(&policy(&first), Some(&write), 2)
            .await
            .unwrap();
        store
            .finish_plugin_mutation(&id, native.revision, "installed", None)
            .await
            .unwrap();
        let gate = store
            .begin_plugin_mutation(
                "ws-update",
                &id,
                native.revision,
                "updating",
                true,
                "{\"kind\":\"update\",\"children\":[]}",
            )
            .await
            .unwrap();
        let mut owned = ownership(&id, first.skill_id.as_str(), "one");
        owned.expected_revision = gate.revision;
        owned.package_fingerprint = "new-assets".into();
        let patch = SkillInstallationPatch {
            fingerprint: Some("new-markdown".into()),
            trust_level: Some(first.trust_level.clone()),
            ..Default::default()
        };
        assert!(
            store
                .update_skill_lifecycle_with_plugin_change(
                    &first.skill_id,
                    &patch,
                    &[SkillAuditEventRecord {
                        action: "update".into(),
                        created_at_unix: 3,
                        ..skill_audit(&first)
                    }],
                    None,
                    Some(&owned),
                    None,
                    3
                )
                .await
                .unwrap()
        );
        let link = store
            .find_skill_plugin_owner(&first.skill_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(link.skill_id.as_deref(), Some(first.skill_id.as_str()));
        assert_eq!(link.package_fingerprint.as_deref(), Some("new-assets"));
        assert_eq!(link.override_fields_json, write.override_fields_json);
        let restrictions = store
            .list_workspace_skill_policies("ws-update")
            .await
            .unwrap();
        assert!(
            restrictions
                .iter()
                .all(|row| row.enabled == Some(false)
                    && row.allow_implicit_invocation == Some(false))
        );
        let untouched = store
            .find_skill_installation(&sibling.skill_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(untouched.fingerprint, sibling.fingerprint);
        assert_eq!(
            store
                .find_skill_plugin_owner(&sibling.skill_id)
                .await
                .unwrap()
                .unwrap()
                .package_fingerprint
                .as_deref(),
            Some("member-tree")
        );
        assert!(
            !store
                .plugin_child_available("skill", first.skill_id.as_str(), "ws-update")
                .await
                .unwrap()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn interrupted_package_removal_is_repairable_and_cannot_forget_a_live_native_child() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws-package-remove").await;
        let id = "P".repeat(21);
        store
            .insert_plugin_installation(&parent(&id, "ws-package-remove"))
            .await
            .unwrap();
        let child = skill('A', "ws-package-remove");
        store
            .install_skill_lifecycle_with_ownership(
                &child,
                &policy(&child),
                &[skill_audit(&child)],
                None,
                Some(&ownership(&id, child.skill_id.as_str(), "one")),
                1,
            )
            .await
            .unwrap();
        // Uninstall revalidates the full stored record, including installer timestamps.
        let child = store
            .find_skill_installation(&child.skill_id)
            .await
            .unwrap()
            .unwrap();
        store
            .settle_plugin_installation(&id, 1, "installed", None)
            .await
            .unwrap();
        let gate = store
            .begin_plugin_mutation(
                "ws-package-remove",
                &id,
                1,
                "updating",
                true,
                "{\"kind\":\"update\",\"children\":[]}",
            )
            .await
            .unwrap();
        assert!(
            store
                .forget_plugin_component(&id, gate.revision, "skill", "one")
                .await
                .is_err()
        );
        let write = PluginNativeWrite {
            kind: pioneer_protocol::PackageKind::Plugin,
            plugin_id: id.clone(),
            expected_revision: gate.revision,
            member_key: "one".into(),
            child_id: child.skill_id.to_string(),
            override_fields_json: "[]".into(),
            pending_after: gate.pending_json.clone().unwrap(),
        };
        assert!(
            store
                .uninstall_skill_installation_lifecycle_with_plugin_change(
                    &child,
                    &[SkillAuditEventRecord {
                        action: "uninstall".into(),
                        created_at_unix: 2,
                        ..skill_audit(&child)
                    }],
                    Some(&write),
                    2,
                )
                .await
                .unwrap()
        );
        let removed = store
            .list_plugin_components(&id)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(removed.skill_id.is_none());
        assert_eq!(removed.status, "failed");
        assert_eq!(
            removed.diagnostic.as_deref(),
            Some("component_removed_for_update")
        );
        store
            .finish_plugin_mutation(&id, gate.revision, "interrupted", None)
            .await
            .unwrap();
        let current = store.find_plugin_installation(&id).await.unwrap().unwrap();
        assert_eq!(current.pending_json, gate.pending_json);
        let resumed = store
            .resume_plugin_mutation(
                "ws-package-remove",
                &id,
                current.revision,
                current.pending_json.as_deref().unwrap(),
                "{\"kind\":\"update\",\"children\":[]}",
                "updating",
            )
            .await
            .unwrap();
        store
            .forget_plugin_component(&id, resumed.revision, "skill", "one")
            .await
            .unwrap();
        assert!(store.list_plugin_components(&id).await.unwrap().is_empty());
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn update_upload_consumption_and_repair_gate_share_one_authorized_write() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws-upload-update").await;
        let id = "P".repeat(21);
        store
            .insert_plugin_installation(&parent(&id, "ws-upload-update"))
            .await
            .unwrap();
        store
            .settle_plugin_installation(&id, 1, "installed", None)
            .await
            .unwrap();
        let upload = SkillUploadSessionRecord {
            purpose: "plugin".into(),
            upload_id: "U".repeat(21),
            workspace_id: "ws-upload-update".into(),
            connection_id: 7,
            status: "finalized".into(),
            file_name: "package.tar.gz".into(),
            archive_format: "tar_gz".into(),
            compressed_size_bytes: 1,
            received_bytes: 1,
            sha256: "a".repeat(64),
            payload_path: "/managed/upload".into(),
            created_at_unix: 1,
            expires_at_unix: 100,
            finalized_at_unix: Some(2),
            consumed_at_unix: None,
            aborted_at_unix: None,
        };
        store.insert_skill_upload_session(&upload).await.unwrap();
        let pending = "{\"kind\":\"update\",\"children\":[]}";
        for (workspace, connection, now, revision) in [
            ("foreign", 7, 3, 1),
            ("ws-upload-update", 8, 3, 1),
            ("ws-upload-update", 7, 100, 1),
            ("ws-upload-update", 7, 3, 2),
        ] {
            assert!(
                store
                    .begin_plugin_package_update(
                        workspace,
                        &id,
                        revision,
                        pending,
                        &upload.upload_id,
                        connection,
                        now,
                        None
                    )
                    .await
                    .is_err()
            );
            let current = store.find_plugin_installation(&id).await.unwrap().unwrap();
            assert_eq!(current.revision, 1);
            assert!(current.pending_json.is_none());
        }
        let gate = store
            .begin_plugin_package_update(
                "ws-upload-update",
                &id,
                1,
                pending,
                &upload.upload_id,
                7,
                3,
                None,
            )
            .await
            .unwrap();
        assert_eq!(gate.revision, 2);
        assert_eq!(gate.pending_json.as_deref(), Some(pending));
        assert!(
            store
                .begin_plugin_package_update(
                    "ws-upload-update",
                    &id,
                    2,
                    pending,
                    &upload.upload_id,
                    7,
                    3,
                    Some(pending)
                )
                .await
                .is_err()
        );
        assert_eq!(
            store
                .find_plugin_installation(&id)
                .await
                .unwrap()
                .unwrap()
                .revision,
            2
        );
        Ok(())
    })
    .await
    .unwrap();
}

// B-02: native resolver output is supplied separately from durable bindings.
// Missing bindings for selected leaves remain a commit failure; exclusions do
// not turn installed siblings into an all-or-nothing dependency.
#[pioneer_postgres_test::postgres_test]
async fn plugin_ready_keeps_resolved_siblings_and_rejects_real_missing_bindings() {
    use pioneer_protocol::{PluginSelectedChild, PluginSelectedParent, PluginSelectionSnapshot};
    let store = test_store_with_workspace("ws").await;
    let parent_id = "P".repeat(21);
    store
        .insert_plugin_installation(&parent(&parent_id, "ws"))
        .await
        .unwrap();
    let allowed = skill('A', "ws");
    let disabled = skill('D', "ws");
    for (row, enabled) in [(&allowed, true), (&disabled, false)] {
        let mut restriction = policy(row);
        restriction.enabled = Some(enabled);
        store
            .install_skill_lifecycle_with_ownership(
                row,
                &restriction,
                &[skill_audit(row)],
                None,
                Some(&ownership(
                    &parent_id,
                    row.skill_id.as_str(),
                    &row.skill_id.to_string(),
                )),
                1,
            )
            .await
            .unwrap();
    }
    store
        .settle_plugin_installation(&parent_id, 1, "installed", None)
        .await
        .unwrap();
    store.database_connection().execute_unprepared(
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,sidebar_visibility,access_class,created_at,updated_at) \
         VALUES('thread','ws','','chat','test','test','active','user','visible','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
         INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) \
         VALUES('turn','thread','in_progress','{}','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);"
    ).await.unwrap();
    let prepared = PluginSelectionSnapshot {
        parents: vec![PluginSelectedParent {
            kind: pioneer_protocol::PackageKind::Plugin,
            id: parent_id.clone(),
            revision: 1,
        }],
        children: [&allowed, &disabled]
            .iter()
            .map(|row| PluginSelectedChild {
                kind: "skill".into(),
                id: row.skill_id.to_string(),
                parent_id: parent_id.clone(),
            })
            .collect(),
        phase: "prepared".into(),
    };
    store
        .prepare_plugin_selection("turn", &prepared)
        .await
        .unwrap();
    // Historical prepared JSON predates the parent kind field. It must remain
    // usable without rewriting frozen selections during schema installation.
    let mut legacy = serde_json::to_value(&prepared).unwrap();
    for selected in legacy["parents"].as_array_mut().unwrap() {
        selected.as_object_mut().unwrap().remove("kind");
    }
    let legacy = legacy.to_string();
    store
        .run_serialized_write(|| async {
            let db = store.connection.begin().await?;
            crate::repositories::plugins::set_selection(&db, "turn", legacy.clone()).await?;
            db.commit().await?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        store
            .ready_plugin_selection("turn", &[allowed.skill_id.clone()])
            .await
            .is_err()
    );
    assert_eq!(
        store.get_plugin_selection("turn").await.unwrap().unwrap(),
        prepared
    );
    store
        .replace_turn_skill_bindings(
            "turn",
            &[TurnSkillBindingRecord {
                skill_id: allowed.skill_id.clone(),
                skill_owner: allowed.owner.clone(),
                skill_slug: allowed.slug.clone(),
                skill_version: None,
                fingerprint: allowed.fingerprint.clone(),
                source_kind: allowed.source_kind.clone(),
                resolved_reason: "explicit_capability".into(),
            }],
            1,
        )
        .await
        .unwrap();
    store
        .database_connection()
        .execute_unprepared(
            "CREATE FUNCTION fail_plugin_ready() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture ready write failure'; END $$; CREATE TRIGGER fail_plugin_ready BEFORE UPDATE OF package_selection_json ON turn FOR EACH ROW EXECUTE FUNCTION fail_plugin_ready()",
        )
        .await
        .unwrap();
    assert!(
        store
            .ready_plugin_selection("turn", &[allowed.skill_id.clone()])
            .await
            .is_err()
    );
    assert_eq!(
        store.get_plugin_selection("turn").await.unwrap().unwrap(),
        prepared
    );
    store
        .database_connection()
        .execute_unprepared(
            "DROP TRIGGER fail_plugin_ready ON turn; DROP FUNCTION fail_plugin_ready()",
        )
        .await
        .unwrap();
    store
        .ready_plugin_selection("turn", &[allowed.skill_id.clone()])
        .await
        .unwrap();
    let ready = store.get_plugin_selection("turn").await.unwrap().unwrap();
    assert_eq!(ready.phase, "ready");
    assert_eq!(ready.children, vec![prepared.children[0].clone()]);
    assert!(
        store
            .plugin_turn_child_available("turn", "skill", allowed.skill_id.as_str(), "ws")
            .await
            .unwrap()
    );
    assert!(
        !store
            .plugin_turn_child_available("turn", "skill", disabled.skill_id.as_str(), "ws")
            .await
            .unwrap()
    );
    // All-excluded is a valid parent selection, with no execution permission.
    store
        .prepare_plugin_selection("turn", &prepared)
        .await
        .unwrap();
    store
        .replace_turn_skill_bindings("turn", &[], 2)
        .await
        .unwrap();
    store.ready_plugin_selection("turn", &[]).await.unwrap();
    assert!(
        store
            .get_plugin_selection("turn")
            .await
            .unwrap()
            .unwrap()
            .children
            .is_empty()
    );
    // A foreign identity is never excused by being absent from resolver output.
    let mut foreign = prepared.clone();
    foreign.children[1].id = "F".repeat(21);
    store
        .prepare_plugin_selection("turn", &foreign)
        .await
        .unwrap();
    assert!(store.ready_plugin_selection("turn", &[]).await.is_err());
    assert_eq!(
        store
            .get_plugin_selection("turn")
            .await
            .unwrap()
            .unwrap()
            .phase,
        "prepared"
    );
}
#[tokio::test]
async fn foreign_scope_stale_revision_and_native_name_collision_cannot_publish() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws").await;
        store
            .insert_plugin_installation(&parent("plugin-a", "ws"))
            .await
            .unwrap();
        let row = skill('A', "foreign");
        let owner = ownership("plugin-a", row.skill_id.as_str(), "one");
        assert!(
            store
                .install_skill_lifecycle_with_ownership(
                    &row,
                    &policy(&row),
                    &[skill_audit(&row)],
                    None,
                    Some(&owner),
                    1
                )
                .await
                .is_err()
        );
        let row = skill('A', "ws");
        let mut owner = ownership("plugin-a", row.skill_id.as_str(), "one");
        owner.expected_revision = 2;
        assert!(
            store
                .install_skill_lifecycle_with_ownership(
                    &row,
                    &policy(&row),
                    &[skill_audit(&row)],
                    None,
                    Some(&owner),
                    1
                )
                .await
                .is_err()
        );
        let standalone = mcp("standalone", "ws", "same-name");
        store
            .upsert_mcp_server_installation_with_audit(&standalone, &mcp_audit(&standalone), 1)
            .await
            .unwrap();
        let mut attempted = standalone.clone();
        attempted.id = Some("owned".into());
        attempted.transport_json = "changed".into();
        let owner = ownership("plugin-a", "owned", "server");
        assert!(
            store
                .upsert_mcp_server_installation_with_audit_and_ownership(
                    &attempted,
                    &mcp_audit(&attempted),
                    Some(&owner),
                    2
                )
                .await
                .is_err()
        );
        assert_eq!(
            store
                .find_mcp_server_installation("workspace", "ws", "same-name")
                .await
                .unwrap()
                .unwrap()
                .transport_json,
            "{}"
        );
        assert!(
            store
                .list_plugin_components("plugin-a")
                .await
                .unwrap()
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}
#[pioneer_postgres_test::postgres_test]
async fn same_authored_skill_names_do_not_collide_and_child_delete_unlinks() {
    let store = test_store_with_workspace("ws").await;
    for (parent_id, id) in [("plugin-a", 'A'), ("plugin-b", 'B')] {
        store
            .insert_plugin_installation(&parent(parent_id, "ws"))
            .await
            .unwrap();
        let row = skill(id, "ws");
        let owner = ownership(parent_id, row.skill_id.as_str(), "same-key");
        store
            .install_skill_lifecycle_with_ownership(
                &row,
                &policy(&row),
                &[skill_audit(&row)],
                None,
                Some(&owner),
                1,
            )
            .await
            .unwrap();
    }
    let first = SkillId::new("A".repeat(21)).unwrap();
    store
        .delete_skill_installation_with_workspace_policy("ws", &first)
        .await
        .unwrap();
    assert_eq!(
        store.list_plugin_components("plugin-a").await.unwrap()[0].skill_id,
        None
    );
    assert!(
        store.list_plugin_components("plugin-b").await.unwrap()[0]
            .skill_id
            .is_some()
    );
    assert!(
        store
            .database_connection()
            .execute_unprepared("DELETE FROM package_installation WHERE id='plugin-a'")
            .await
            .is_err()
    );
    assert!(
        store
            .database_connection()
            .execute_unprepared("DELETE FROM workspace WHERE id='ws'")
            .await
            .is_err()
    );
}
#[tokio::test]
async fn ownership_does_not_reclassify_interactive_or_maintenance_access() {
    pioneer_postgres_test::with_database(async |fixture| {
        use pioneer_sqlite::{SqliteReadClass, SqliteWriteClass};
        let store = postgres_store_with_workspace(fixture, "ws").await;
        store
            .insert_plugin_installation(&parent("plugin-a", "ws"))
            .await
            .unwrap();
        let maintenance = store.with_maintenance_access();
        assert!(
            maintenance
                .find_plugin_installation("plugin-a")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store.database_connection().read_class(),
            SqliteReadClass::Interactive
        );
        assert_eq!(
            store.database_connection().write_class(),
            SqliteWriteClass::Interactive
        );
        assert_eq!(
            maintenance.database_connection().read_class(),
            SqliteReadClass::Maintenance
        );
        assert_eq!(
            maintenance.database_connection().write_class(),
            SqliteWriteClass::Maintenance
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn concurrent_interactive_and_maintenance_publication_cannot_share_child() {
    let store = test_store_with_workspace("ws").await;
    store
        .insert_plugin_installation(&parent("plugin-a", "ws"))
        .await
        .unwrap();
    store
        .insert_plugin_installation(&parent("plugin-b", "ws"))
        .await
        .unwrap();
    let maintenance = store.with_maintenance_access();
    let row = skill('A', "ws");
    let policy = policy(&row);
    let audits = [skill_audit(&row)];
    let a = ownership("plugin-a", row.skill_id.as_str(), "one");
    let b = ownership("plugin-b", row.skill_id.as_str(), "one");
    let (a, b) = tokio::join!(
        store.install_skill_lifecycle_with_ownership(&row, &policy, &audits, None, Some(&a), 1),
        maintenance.install_skill_lifecycle_with_ownership(
            &row,
            &policy,
            &audits,
            None,
            Some(&b),
            1
        ),
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(
        store
            .list_plugin_components("plugin-a")
            .await
            .unwrap()
            .len()
            + store
                .list_plugin_components("plugin-b")
                .await
                .unwrap()
                .len(),
        1
    );
    // Observe a real PostgreSQL row-lock wait on an independent writer.
    use pioneer_sqlite::SqliteDatabase;
    use sea_orm::{DbBackend, Set, Statement, TransactionTrait};
    use std::time::Duration;

    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let database = SqliteDatabase::new(reader, writer);
    database.validate_reader().await.unwrap();
    let store = CrudStore::new(database);
    let db = store.database_connection().begin().await.unwrap();
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws".into()),
        name: Set("CAS fixture".into()),
        is_active: Set(true),
        is_current: Set(false),
        created_at: Set(util::unix_to_datetime(1)),
        updated_at: Set(util::unix_to_datetime(1)),
    })
    .exec(&db)
    .await
    .unwrap();
    db.commit().await.unwrap();
    store
        .insert_plugin_installation(&parent("cas-parent", "ws"))
        .await
        .unwrap();
    store
        .settle_plugin_installation("cas-parent", 1, "installed", None)
        .await
        .unwrap();
    let db = store.database_connection().begin().await.unwrap();
    let first = repositories::plugins::begin_mutation(
        &db,
        "ws",
        "cas-parent",
        1,
        "updating",
        true,
        "{\"owner\":\"A\"}".into(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(first.revision, 2);
    // A owns a dispatched transition whose commit is still outstanding.
    assert_eq!(
        store
            .find_plugin_installation("cas-parent")
            .await
            .unwrap()
            .unwrap()
            .revision,
        1
    );
    let competing_writer = fixture.connect_with_limit(1).await.unwrap();
    let pid: i32 = competing_writer
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT pg_backend_pid() AS pid",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "pid")
        .unwrap();
    let maintenance = CrudStore::new(SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        competing_writer,
    ))
    .with_maintenance_access();
    let second = tokio::spawn(async move {
        maintenance
            .begin_plugin_mutation("ws", "cas-parent", 1, "updating", true, "{\"owner\":\"B\"}")
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = fixture
                .connection()
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT cardinality(pg_blocking_pids($1)) > 0 AS waiting",
                    [pid.into()],
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "waiting")
                .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the competing writer must wait on the plugin row");
    db.commit().await.unwrap();
    assert!(
        second
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("plugins.stale")
    );
    let retained = store
        .find_plugin_installation("cas-parent")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.revision, 2);
    assert_eq!(retained.pending_json.as_deref(), Some("{\"owner\":\"A\"}"));
    let next = store
        .resume_plugin_mutation(
            "ws",
            "cas-parent",
            2,
            retained.pending_json.as_deref().unwrap(),
            "{\"owner\":\"C\"}",
            "updating",
        )
        .await
        .unwrap();
    assert_eq!(next.revision, 3);
    assert!(
        store
            .finish_plugin_mutation("cas-parent", 2, "installed", None)
            .await
            .is_err()
    );
    assert!(
        store
            .replace_plugin_pending("cas-parent", 2, "{}")
            .await
            .is_err()
    );
    assert!(
        store
            .interrupt_plugin_mutation("cas-parent", 2, "{}", "late")
            .await
            .is_err()
    );
    assert!(store.prepare_plugin_reload("cas-parent", 2).await.is_err());
    assert!(
        store
            .publish_plugin_package("cas-parent", 2, "late", None, "late", None)
            .await
            .is_err()
    );
    assert!(
        store
            .forget_plugin_component("cas-parent", 2, "skill", "absent")
            .await
            .is_err()
    );
    assert_eq!(
        store
            .find_plugin_installation("cas-parent")
            .await
            .unwrap()
            .unwrap()
            .pending_json,
        next.pending_json
    );
    store
        .finish_plugin_mutation("cas-parent", 3, "installed", None)
        .await
        .unwrap();
    assert!(
        store
            .find_plugin_installation("cas-parent")
            .await
            .unwrap()
            .unwrap()
            .pending_json
            .is_none()
    );
}

#[tokio::test]
async fn standalone_mcp_names_work_while_owned_adoption_is_rejected() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws").await;
        let mut legacy = mcp("old", "ws", "custom");
        // Seed the pre-plugin native record through the existing low-level operation.
        store
            .upsert_mcp_server_installation(&legacy, 1)
            .await
            .unwrap();
        legacy.transport_json = r#"{"command":"updated"}"#.into();
        assert_eq!(
            store
                .upsert_mcp_server_installation_with_audit(&legacy, &mcp_audit(&legacy), 2,)
                .await
                .unwrap(),
            "old"
        );
        assert!(store.find_mcp_plugin_owner("old").await.unwrap().is_none());
        assert_eq!(
            store
                .find_mcp_server_installation("workspace", "ws", "custom")
                .await
                .unwrap()
                .unwrap()
                .transport_json,
            r#"{"command":"updated"}"#
        );

        let fresh = mcp("new", "ws", "new-server");
        assert_eq!(
            store
                .upsert_mcp_server_installation_with_audit(&fresh, &mcp_audit(&fresh), 3,)
                .await
                .unwrap(),
            "new"
        );

        store
            .insert_plugin_installation(&parent("plugin-a", "ws"))
            .await
            .unwrap();
        let same_id = ownership("plugin-a", "old", "server");
        assert!(
            store
                .upsert_mcp_server_installation_with_audit_and_ownership(
                    &legacy,
                    &mcp_audit(&legacy),
                    Some(&same_id),
                    3,
                )
                .await
                .is_err(),
            "a matching ID cannot adopt a standalone row"
        );
        let owned = mcp("child", "ws", "zenmoney");
        let owner = ownership("plugin-a", "child", "server");
        store
            .upsert_mcp_server_installation_with_audit_and_ownership(
                &owned,
                &mcp_audit(&owned),
                Some(&owner),
                3,
            )
            .await
            .unwrap();
        let mut owned_update = owned.clone();
        owned_update.transport_json = r#"{"command":"owned-update"}"#.into();
        assert_eq!(
            store
                .upsert_mcp_server_installation_with_audit_and_ownership(
                    &owned_update,
                    &mcp_audit(&owned_update),
                    Some(&owner),
                    4,
                )
                .await
                .unwrap(),
            "child",
            "the genuine owned update remains allowed"
        );
        let mut attempt = owned.clone();
        attempt.transport_json = r#"{"command":"foreign-change"}"#.into();
        assert!(
            store
                .upsert_mcp_server_installation_with_audit(&attempt, &mcp_audit(&attempt), 4,)
                .await
                .is_err()
        );
        store
            .insert_plugin_installation(&parent("plugin-b", "ws"))
            .await
            .unwrap();
        let foreign = ownership("plugin-b", "child", "server");
        assert!(
            store
                .upsert_mcp_server_installation_with_audit_and_ownership(
                    &attempt,
                    &mcp_audit(&attempt),
                    Some(&foreign),
                    4,
                )
                .await
                .is_err()
        );
        let mut name_collision = attempt.clone();
        name_collision.id = Some("another-child".into());
        let another_owner = ownership("plugin-b", "another-child", "server");
        assert!(
            store
                .upsert_mcp_server_installation_with_audit_and_ownership(
                    &name_collision,
                    &mcp_audit(&name_collision),
                    Some(&another_owner),
                    4,
                )
                .await
                .is_err(),
            "a second plugin cannot overwrite the same ordinary MCP name"
        );
        assert_eq!(
            store
                .find_mcp_server_installation("workspace", "ws", "zenmoney")
                .await
                .unwrap()
                .unwrap()
                .transport_json,
            r#"{"command":"owned-update"}"#
        );
        assert_eq!(
            store
                .find_mcp_plugin_owner("child")
                .await
                .unwrap()
                .unwrap()
                .package_id,
            "plugin-a"
        );
        Ok(())
    })
    .await
    .unwrap();
}

// Stage B sources: NOT_RUN / NOT_COMPILED.
#[tokio::test]
async fn plugin_reservation_consumes_only_its_finalized_owned_upload_atomically() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws").await;
        let mut reserved_parent = parent("P".repeat(21).as_str(), "ws");
        reserved_parent.pending_json = Some("{\"children\":[]}".into());
        let upload = SkillUploadSessionRecord {
            purpose: "plugin".into(),
            upload_id: reserved_parent.source_upload_id.clone(),
            workspace_id: "ws".into(),
            connection_id: 7,
            status: "finalized".into(),
            file_name: "plugin.tar.gz".into(),
            archive_format: "tar_gz".into(),
            compressed_size_bytes: 1,
            received_bytes: 1,
            sha256: "a".repeat(64),
            payload_path: "/managed/upload.tar.gz".into(),
            created_at_unix: 1,
            expires_at_unix: 100,
            finalized_at_unix: Some(2),
            consumed_at_unix: None,
            aborted_at_unix: None,
        };
        store.insert_skill_upload_session(&upload).await.unwrap();
        assert!(
            store
                .reserve_plugin_installation(&reserved_parent, 8, 3)
                .await
                .is_err()
        );
        assert!(
            store
                .find_plugin_by_upload(&upload.upload_id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .find_skill_upload_session(&upload.upload_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "finalized"
        );
        let maintenance = store.with_maintenance_access();
        let (a, b) = tokio::join!(
            store.reserve_plugin_installation(&reserved_parent, 7, 3),
            maintenance.reserve_plugin_installation(&reserved_parent, 7, 3)
        );
        assert_ne!(a.is_ok(), b.is_ok());
        assert_eq!(
            store.list_plugin_installations("ws").await.unwrap().len(),
            1
        );
        assert_eq!(
            store
                .find_skill_upload_session(&upload.upload_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "consumed"
        );
        assert!(
            store
                .reserve_plugin_installation(&reserved_parent, 7, 3)
                .await
                .is_err()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn install_failure_preserves_native_sibling_and_closes_failed_child() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws").await;
        store
            .insert_plugin_installation(&parent("plugin-a", "ws"))
            .await
            .unwrap();
        let good = skill('G', "ws");
        let good_owner = ownership("plugin-a", good.skill_id.as_str(), "good");
        store
            .install_skill_lifecycle_with_ownership(
                &good,
                &policy(&good),
                &[skill_audit(&good)],
                None,
                Some(&good_owner),
                1,
            )
            .await
            .unwrap();
        let failed = ownership("plugin-a", &"F".repeat(21), "failed");
        store
            .record_plugin_component_failure(&failed, "skill", "component_install_failed")
            .await
            .unwrap();
        assert!(
            !store
                .plugin_child_available("skill", good.skill_id.as_str(), "ws")
                .await
                .unwrap()
        );
        store
            .settle_plugin_installation("plugin-a", 1, "installed", None)
            .await
            .unwrap();
        assert!(
            store
                .plugin_child_available("skill", good.skill_id.as_str(), "ws")
                .await
                .unwrap()
        );
        assert!(
            !store
                .plugin_child_available("skill", good.skill_id.as_str(), "foreign")
                .await
                .unwrap()
        );
        assert!(
            !store
                .plugin_turn_child_available("no-ready-turn", "skill", good.skill_id.as_str(), "ws")
                .await
                .unwrap()
        );
        assert!(
            store
                .find_skill_installation(&good.skill_id)
                .await
                .unwrap()
                .is_some()
        );
        let links = store.list_plugin_components("plugin-a").await.unwrap();
        assert_eq!(links.len(), 2);
        assert!(
            links
                .iter()
                .any(|c| c.member_key == "failed" && c.status == "failed" && c.skill_id.is_none())
        );
        // The original child restriction was not relaxed by parent settlement.
        assert_eq!(
            store
                .list_workspace_skill_policies("ws")
                .await
                .unwrap()
                .iter()
                .find(|p| p.skill_id == good.skill_id)
                .unwrap()
                .enabled,
            Some(false)
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn closed_gate_reconciliation_and_enable_preserve_native_restrictions() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws-c1").await;
        let id = "P".repeat(21);
        store
            .insert_plugin_installation(&parent(&id, "ws-c1"))
            .await
            .unwrap();
        let child = skill('S', "ws-c1");
        let write = ownership(&id, child.skill_id.as_str(), "one");
        store
            .install_skill_lifecycle_with_ownership(
                &child,
                &policy(&child),
                &[skill_audit(&child)],
                None,
                Some(&write),
                1,
            )
            .await
            .unwrap();
        store
            .settle_plugin_installation(&id, 1, "installed", None)
            .await
            .unwrap();
        let restrictions = store.list_workspace_skill_policies("ws-c1").await.unwrap();
        let operation = store
            .begin_plugin_mutation(
                "ws-c1",
                &id,
                1,
                "updating",
                false,
                "{\"kind\":\"set_enabled\",\"children\":[]}",
            )
            .await
            .unwrap();
        assert_eq!(operation.revision, 2);
        assert!(
            store
                .begin_plugin_mutation(
                    "ws-c1",
                    &id,
                    2,
                    "updating",
                    true,
                    "{\"kind\":\"replacement\"}"
                )
                .await
                .is_err()
        );
        assert_eq!(
            store
                .find_plugin_installation(&id)
                .await
                .unwrap()
                .unwrap()
                .pending_json,
            operation.pending_json
        );
        assert!(
            !store
                .plugin_child_available("skill", child.skill_id.as_str(), "ws-c1")
                .await
                .unwrap()
        );
        assert!(
            store
                .begin_plugin_mutation("foreign", &id, 2, "updating", true, "{}")
                .await
                .is_err()
        );
        assert!(
            store
                .begin_plugin_mutation("ws-c1", &id, 1, "updating", true, "{}")
                .await
                .is_err()
        );
        store
            .with_maintenance_access()
            .interrupt_unfinished_plugins()
            .await
            .unwrap();
        let interrupted = store.find_plugin_installation(&id).await.unwrap().unwrap();
        assert_eq!(interrupted.state, "interrupted");
        assert_eq!(interrupted.pending_json, operation.pending_json);
        store
            .finish_plugin_mutation(&id, 2, "installed", None)
            .await
            .unwrap();
        let enable = store
            .begin_plugin_mutation("ws-c1", &id, 2, "updating", true, "{}")
            .await
            .unwrap();
        store
            .prepare_plugin_reload(&id, enable.revision)
            .await
            .unwrap();
        let reload = store.find_plugin_installation(&id).await.unwrap().unwrap();
        assert_eq!(reload.state, "installed");
        assert!(reload.pending_json.is_some());
        assert!(
            !store
                .plugin_child_available("skill", child.skill_id.as_str(), "ws-c1")
                .await
                .unwrap()
        );
        // Cancellation/restart during reload must retain an executable repair plan.
        store
            .with_maintenance_access()
            .interrupt_unfinished_plugins()
            .await
            .unwrap();
        assert_eq!(
            store
                .find_plugin_installation(&id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "interrupted"
        );
        store
            .finish_plugin_mutation(&id, enable.revision, "installed", None)
            .await
            .unwrap();
        assert_eq!(
            store.list_workspace_skill_policies("ws-c1").await.unwrap(),
            restrictions
        );
        assert!(
            store
                .delete_plugin_parent(&id, enable.revision)
                .await
                .is_err()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_assets_only_update_preserves_the_last_committed_tree_for_retry() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws-c1-assets").await;
        let id = "P".repeat(21);
        store
            .insert_plugin_installation(&parent(&id, "ws-c1-assets"))
            .await
            .unwrap();
        let child = skill('S', "ws-c1-assets");
        let write = ownership(&id, child.skill_id.as_str(), "one");
        store
            .install_skill_lifecycle_with_ownership(
                &child,
                &policy(&child),
                &[skill_audit(&child)],
                None,
                Some(&write),
                1,
            )
            .await
            .unwrap();
        let mut failed = write.clone();
        failed.package_fingerprint = "assets-only-new-tree".into();
        store
            .record_plugin_component_failure(&failed, "skill", "component_update_failed")
            .await
            .unwrap();
        let link = store
            .find_skill_plugin_owner(&child.skill_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(link.status, "failed");
        assert_eq!(
            link.package_fingerprint.as_deref(),
            Some(write.package_fingerprint.as_str())
        );
        assert_eq!(link.skill_id.as_deref(), Some(child.skill_id.as_str()));
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn native_policy_audit_override_and_parent_commit_marker_are_atomic() {
    let store = test_store_with_workspace("ws-c1-policy").await;
    let parent_id = "P".repeat(21);
    let child_id = "M".repeat(21);
    store
        .insert_plugin_installation(&parent(&parent_id, "ws-c1-policy"))
        .await
        .unwrap();
    let original = mcp(&child_id, "ws-c1-policy", "pplugin_native_policy");
    store
        .upsert_mcp_server_installation_with_audit_and_ownership(
            &original,
            &mcp_audit(&original),
            Some(&ownership(&parent_id, &child_id, "one")),
            1,
        )
        .await
        .unwrap();
    store
        .settle_plugin_installation(&parent_id, 1, "installed", None)
        .await
        .unwrap();
    let gate = store
        .begin_plugin_mutation(
            "ws-c1-policy",
            &parent_id,
            1,
            "updating",
            true,
            "{\"kind\":\"native\",\"native_committed\":false}",
        )
        .await
        .unwrap();
    let write = PluginNativeWrite {
        kind: pioneer_protocol::PackageKind::Plugin,
        plugin_id: parent_id.clone(),
        expected_revision: gate.revision,
        member_key: "one".into(),
        child_id: child_id.clone(),
        override_fields_json: "[\"enabled\"]".into(),
        pending_after: "{\"kind\":\"native\",\"native_committed\":true}".into(),
    };
    let mut edited = original.clone();
    edited.enabled = true;
    let mut stale = write.clone();
    stale.expected_revision -= 1;
    assert!(
        store
            .upsert_mcp_server_installation_with_plugin_native_change(
                &edited,
                &mcp_audit(&edited),
                None,
                Some(&stale),
                2
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .find_mcp_server_installation("workspace", "ws-c1-policy", &original.name)
            .await
            .unwrap()
            .unwrap(),
        original
    );
    assert_eq!(
        store
            .find_mcp_plugin_owner(&child_id)
            .await
            .unwrap()
            .unwrap()
            .override_fields_json,
        "[]"
    );
    assert_eq!(
        store
            .find_plugin_installation(&parent_id)
            .await
            .unwrap()
            .unwrap()
            .pending_json,
        gate.pending_json
    );
    store
        .database_connection()
        .execute_unprepared(
            "CREATE FUNCTION c1_native_audit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture audit failure'; END $$; CREATE TRIGGER c1_native_audit_failure BEFORE INSERT ON mcp_audit_event FOR EACH ROW EXECUTE FUNCTION c1_native_audit_failure()",
        )
        .await
        .unwrap();
    assert!(
        store
            .upsert_mcp_server_installation_with_plugin_native_change(
                &edited,
                &mcp_audit(&edited),
                None,
                Some(&write),
                2
            )
            .await
            .is_err()
    );
    // This failure occurs after the native row write, before link/parent effect
    // publication. The same transaction must roll all of them back.
    assert!(
        !store
            .find_mcp_server_installation("workspace", "ws-c1-policy", &original.name)
            .await
            .unwrap()
            .unwrap()
            .enabled
    );
    assert_eq!(
        store
            .find_mcp_plugin_owner(&child_id)
            .await
            .unwrap()
            .unwrap()
            .override_fields_json,
        "[]"
    );
    assert_eq!(
        store
            .find_plugin_installation(&parent_id)
            .await
            .unwrap()
            .unwrap()
            .pending_json,
        gate.pending_json
    );
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER c1_native_audit_failure ON mcp_audit_event; DROP FUNCTION c1_native_audit_failure()")
        .await
        .unwrap();
    let mut malformed = mcp_audit(&edited);
    malformed.decision = "invalid".into();
    // Foreign child binding is rejected without changing native policy/masks.
    let mut foreign = write.clone();
    foreign.child_id = "F".repeat(21);
    assert!(
        store
            .upsert_mcp_server_installation_with_plugin_native_change(
                &edited,
                &malformed,
                None,
                Some(&foreign),
                2
            )
            .await
            .is_err()
    );
    store
        .upsert_mcp_server_installation_with_plugin_native_change(
            &edited,
            &mcp_audit(&edited),
            None,
            Some(&write),
            2,
        )
        .await
        .unwrap();
    let link = store
        .find_mcp_plugin_owner(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(link.override_fields_json, write.override_fields_json);
    assert_eq!(link.package_fingerprint.as_deref(), Some("member-tree"));
    assert_eq!(
        store
            .find_plugin_installation(&parent_id)
            .await
            .unwrap()
            .unwrap()
            .pending_json,
        Some(write.pending_after.clone())
    );
    assert!(
        store
            .find_mcp_server_installation("workspace", "ws-c1-policy", &original.name)
            .await
            .unwrap()
            .unwrap()
            .enabled
    );
    assert!(
        !store
            .plugin_child_available("mcp", &child_id, "ws-c1-policy")
            .await
            .unwrap()
    );
    store
        .finish_plugin_mutation(
            &parent_id,
            gate.revision,
            "interrupted",
            Some("uncertain reply".into()),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .find_plugin_installation(&parent_id)
            .await
            .unwrap()
            .unwrap()
            .pending_json,
        Some(write.pending_after.clone())
    );
    // Runtime admission is distinct from model invocation during final reload.
    store
        .prepare_plugin_reload(&parent_id, gate.revision)
        .await
        .unwrap();
    assert!(
        store
            .plugin_child_runtime_available(&child_id, "ws-c1-policy")
            .await
            .unwrap()
    );
    assert!(
        !store
            .plugin_child_available("mcp", &child_id, "ws-c1-policy")
            .await
            .unwrap()
    );
    store
        .finish_plugin_mutation(&parent_id, gate.revision, "installed", None)
        .await
        .unwrap();
    assert!(
        store
            .plugin_child_available("mcp", &child_id, "ws-c1-policy")
            .await
            .unwrap()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn owned_skill_policy_and_user_removal_revalidate_in_the_native_writer() {
    let store = test_store_with_workspace("ws-c1-skill").await;
    let parent_id = "P".repeat(21);
    store
        .insert_plugin_installation(&parent(&parent_id, "ws-c1-skill"))
        .await
        .unwrap();
    let child = skill('S', "ws-c1-skill");
    store
        .install_skill_lifecycle_with_ownership(
            &child,
            &policy(&child),
            &[skill_audit(&child)],
            None,
            Some(&ownership(&parent_id, child.skill_id.as_str(), "one")),
            1,
        )
        .await
        .unwrap();
    let child = store
        .find_skill_installation(&child.skill_id)
        .await
        .unwrap()
        .unwrap();
    store
        .settle_plugin_installation(&parent_id, 1, "installed", None)
        .await
        .unwrap();
    let gate = store
        .begin_plugin_mutation(
            "ws-c1-skill",
            &parent_id,
            1,
            "updating",
            true,
            "{\"kind\":\"native\",\"action\":\"policy\",\"native_committed\":false}",
        )
        .await
        .unwrap();
    let mut write = PluginNativeWrite {
        kind: pioneer_protocol::PackageKind::Plugin,
        plugin_id: parent_id.clone(),
        expected_revision: gate.revision,
        member_key: "one".into(),
        child_id: child.skill_id.to_string(),
        override_fields_json: "[\"enabled\"]".into(),
        pending_after: "{\"kind\":\"native\",\"native_committed\":true}".into(),
    };
    let mut restricted = policy(&child);
    restricted.enabled = Some(true);
    assert!(
        store
            .upsert_workspace_skill_policy(&restricted, 2)
            .await
            .is_err()
    );
    store
        .upsert_workspace_skill_policy_with_plugin_change(&restricted, Some(&write), 2)
        .await
        .unwrap();
    assert_eq!(
        store
            .find_skill_plugin_owner(&child.skill_id)
            .await
            .unwrap()
            .unwrap()
            .override_fields_json,
        write.override_fields_json
    );
    store
        .finish_plugin_mutation(&parent_id, gate.revision, "installed", None)
        .await
        .unwrap();
    let remove = store
        .begin_plugin_mutation(
            "ws-c1-skill",
            &parent_id,
            gate.revision,
            "updating",
            true,
            "{\"kind\":\"native\",\"action\":\"uninstall\",\"native_committed\":false}",
        )
        .await
        .unwrap();
    write.expected_revision = remove.revision;
    let mut audit = skill_audit(&child);
    audit.action = "uninstall".into();
    assert!(
        store
            .uninstall_skill_installation_lifecycle(&child, &[audit.clone()], 3)
            .await
            .is_err()
    );
    assert!(
        store
            .find_skill_installation(&child.skill_id)
            .await
            .unwrap()
            .is_some()
    );
    store
        .database_connection()
        .execute_unprepared(
            "CREATE FUNCTION c1_skill_audit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture audit failure'; END $$; CREATE TRIGGER c1_skill_audit_failure BEFORE INSERT ON skill_audit_event FOR EACH ROW EXECUTE FUNCTION c1_skill_audit_failure()",
        )
        .await
        .unwrap();
    assert!(
        store
            .uninstall_skill_installation_lifecycle_with_plugin_change(
                &child,
                &[audit.clone()],
                Some(&write),
                3
            )
            .await
            .is_err()
    );
    assert!(
        store
            .find_skill_installation(&child.skill_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .find_skill_plugin_owner(&child.skill_id)
            .await
            .unwrap()
            .is_some()
    );
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER c1_skill_audit_failure ON skill_audit_event; DROP FUNCTION c1_skill_audit_failure()")
        .await
        .unwrap();
    assert!(
        store
            .uninstall_skill_installation_lifecycle_with_plugin_change(
                &child,
                &[audit],
                Some(&write),
                3
            )
            .await
            .unwrap()
    );
    assert!(
        store
            .find_skill_installation(&child.skill_id)
            .await
            .unwrap()
            .is_none()
    );
    let link = store
        .list_plugin_components(&parent_id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(link.status, "removed_by_user");
    assert!(link.skill_id.is_none());
    assert!(
        store
            .list_workspace_skill_policies("ws-c1-skill")
            .await
            .unwrap()
            .is_empty()
    );
}

// NOT_RUN: this extends the existing project DB fixture, including FK=OFF.
#[tokio::test]
async fn startup_reconciliation_finishes_more_than_one_thousand_parents_and_is_idempotent() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws-startup-batches").await;
        for index in 0..1001 {
            store
                .insert_plugin_installation(&parent(&format!("{index:021}"), "ws-startup-batches"))
                .await
                .unwrap();
        }
        assert_eq!(
            store
                .list_plugin_installations("ws-startup-batches")
                .await
                .unwrap()
                .len(),
            1001
        );
        store.interrupt_unfinished_plugins().await.unwrap();
        for index in 0..1001 {
            let row = store
                .find_plugin_installation(&format!("{index:021}"))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.state, "interrupted");
            assert_eq!(row.revision, 1);
        }
        store.interrupt_unfinished_plugins().await.unwrap();
        assert_eq!(
            store
                .find_plugin_installation(&format!("{:021}", 1000))
                .await
                .unwrap()
                .unwrap()
                .state,
            "interrupted"
        );
        Ok(())
    })
    .await
    .unwrap();
}

// App storage regressions: source-only, NOT_RUN / NOT_COMPILED.
fn app_parent(id: &str, workspace: &str) -> crate::PackageRecord {
    let mut record = parent(id, workspace);
    record.kind = pioneer_protocol::PackageKind::App;
    record.name = "shared-app-slug".into();
    record.app_manifest_json = Some(
        serde_json::json!({
            "slug": "shared-app-slug", "name": "Example", "version": "1.0.0",
            "shortDescription": "Example App", "developer": {"name": "Author"},
            "icon": {"light": "icon.png", "dark": "icon.png"}
        })
        .to_string(),
    );
    record.app_localizations_json = Some("{}".into());
    record
}

#[tokio::test]
async fn app_slug_is_workspace_scoped_and_failed_reservation_leaves_no_orphan_parent() {
    pioneer_postgres_test::with_database(async |fixture| {
        use sea_orm::{EntityTrait, Set};
        let store = postgres_store_with_workspace(fixture, "ws-app-one").await;
        store
            .run_serialized_write(|| async {
                for workspace in ["ws-app-two", "ws-app-three"] {
                    pioneer_entity::workspace::Entity::insert(
                        pioneer_entity::workspace::ActiveModel {
                            id: Set(workspace.into()),
                            name: Set(workspace.into()),
                            is_active: Set(true),
                            is_current: Set(false),
                            created_at: Set(util::unix_to_datetime(1)),
                            updated_at: Set(util::unix_to_datetime(1)),
                        },
                    )
                    .exec(&store.connection)
                    .await?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let first = app_parent("app-one", "ws-app-one");
        let second = app_parent("app-two", "ws-app-two");
        let (one, two) = tokio::join!(
            store.insert_package_installation(&first),
            store.insert_package_installation(&second)
        );
        one.unwrap();
        two.unwrap();
        let error = store
            .insert_package_installation(&app_parent("duplicate", "ws-app-one"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("apps.slug_conflict"),
            "wrong rejection: {error}"
        );
        assert!(
            store
                .find_package_installation("duplicate")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .find_plugin_installation("app-one")
                .await
                .unwrap()
                .is_none()
        );
        for (workspace, expected) in [
            ("ws-app-one", vec!["app-one"]),
            ("ws-app-two", vec!["app-two"]),
            ("ws-app-three", vec![]),
        ] {
            let apps = store
                .list_package_installations(workspace, pioneer_protocol::PackageKind::App)
                .await
                .unwrap();
            assert_eq!(
                apps.iter().map(|p| p.id.as_str()).collect::<Vec<_>>(),
                expected
            );
        }
        assert!(
            store
                .list_plugin_installations("ws-app-one")
                .await
                .unwrap()
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_same_workspace_app_reservations_publish_one_installation() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws-app-race").await;
        let first = app_parent("candidate-one", "ws-app-race");
        let second = app_parent("candidate-two", "ws-app-race");
        let (one, two) = tokio::join!(
            store.insert_package_installation(&first),
            store.insert_package_installation(&second)
        );
        assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
        let installed = store
            .list_package_installations("ws-app-race", pioneer_protocol::PackageKind::App)
            .await
            .unwrap();
        assert_eq!(installed.len(), 1);
        for (id, result) in [("candidate-one", one), ("candidate-two", two)] {
            let row = store.find_package_installation(id).await.unwrap();
            if let Err(error) = result {
                assert!(error.to_string().contains("apps.slug_conflict"));
                assert!(row.is_none(), "rejected reservation leaked its parent");
            } else {
                assert_eq!(installed[0].id, id);
                assert_eq!(row.unwrap().workspace_id, "ws-app-race");
            }
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn app_owned_native_child_cannot_be_adopted_by_plugin_or_published_with_wrong_kind() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws-app").await;
        let mut declared_app = app_parent("app-parent", "ws-app");
        let mut declared: serde_json::Value =
            serde_json::from_str(declared_app.app_manifest_json.as_deref().unwrap()).unwrap();
        declared["skills"] = serde_json::json!("skills");
        declared_app.app_manifest_json = Some(declared.to_string());
        store
            .insert_package_installation(&declared_app)
            .await
            .unwrap();
        store
            .insert_plugin_installation(&parent("plugin-parent", "ws-app"))
            .await
            .unwrap();
        let row = skill('A', "ws-app");
        let mut app_owner = ownership("app-parent", row.skill_id.as_str(), "one");
        // Same ID/revision with a forged kind must fail before any native write.
        assert!(
            store
                .install_skill_lifecycle_with_ownership(
                    &row,
                    &policy(&row),
                    &[skill_audit(&row)],
                    None,
                    Some(&app_owner),
                    1
                )
                .await
                .is_err()
        );
        assert!(
            store
                .find_skill_installation(&row.skill_id)
                .await
                .unwrap()
                .is_none()
        );
        app_owner.kind = pioneer_protocol::PackageKind::App;
        assert!(
            store
                .install_skill_lifecycle_with_ownership(
                    &row,
                    &policy(&row),
                    &[skill_audit(&row)],
                    None,
                    Some(&app_owner),
                    1
                )
                .await
                .unwrap()
        );
        let plugin_owner = ownership("plugin-parent", row.skill_id.as_str(), "one");
        assert!(
            store
                .install_skill_lifecycle_with_ownership(
                    &row,
                    &policy(&row),
                    &[skill_audit(&row)],
                    None,
                    Some(&plugin_owner),
                    1
                )
                .await
                .is_err()
        );
        let owner = store
            .find_skill_plugin_owner(&row.skill_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(owner.package_id, "app-parent");
        assert!(
            store
                .list_plugin_components("plugin-parent")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            !store
                .plugin_child_available("skill", row.skill_id.as_str(), "other-workspace")
                .await
                .unwrap()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn package_reload_preserves_structured_legacy_operation_diagnostic() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws-diagnostic").await;
        let mut record = parent("diagnostic-parent", "ws-diagnostic");
        let diagnostic = r#"{"legacy":"plugins.data_retained","detail":"preserved"}"#;
        record.last_error = Some("plugins.data_retained".into());
        record.operation_error_json = Some(diagnostic.into());
        record.pending_json = Some("{}".into());
        store.insert_plugin_installation(&record).await.unwrap();
        store.prepare_plugin_reload(&record.id, 1).await.unwrap();
        let reloaded = store
            .find_package_installation(&record.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reloaded.last_error.as_deref(),
            Some("plugins.data_retained")
        );
        assert_eq!(reloaded.operation_error_json.as_deref(), Some(diagnostic));
        assert_eq!(reloaded.state, "installed");
        assert_eq!(reloaded.pending_json.as_deref(), Some("{}"));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn app_runtime_discovery_has_a_finite_frontier_and_advances_past_a_poison_row() {
    pioneer_postgres_test::with_database(async |fixture| {
        use sea_orm::EntityTrait;

        let store = postgres_store_with_workspace(fixture, "ws-runtime").await;
        for index in 0..66 {
            let id = format!("app-{index:03}");
            let mut row = app_parent(&id, "ws-runtime");
            row.name = id.clone();
            let mut manifest: serde_json::Value =
                serde_json::from_str(row.app_manifest_json.as_deref().unwrap()).unwrap();
            manifest["slug"] = id.into();
            row.app_manifest_json = Some(manifest.to_string());
            if index == 32 {
                // Persist an interrupted parent-only aggregate. Ordinary CRUD
                // publishes both rows atomically; extension deletion stays guarded.
                pioneer_entity::package_installation::Entity::insert(row.operational())
                    .exec(&store.database_connection())
                    .await
                    .unwrap();
            } else {
                store.insert_package_installation(&row).await.unwrap();
            }
        }
        store
            .insert_plugin_installation(&parent("zzz-plugin", "ws-runtime"))
            .await
            .unwrap();
        let maintenance = store.with_maintenance_access();
        let frontier = maintenance.app_runtime_frontier().await.unwrap().unwrap();
        assert_eq!(frontier, "app-065");
        let mut later = app_parent("zzz-app", "ws-runtime");
        later.name = "later-app".into();
        let mut manifest: serde_json::Value =
            serde_json::from_str(later.app_manifest_json.as_deref().unwrap()).unwrap();
        manifest["slug"] = "later-app".into();
        later.app_manifest_json = Some(manifest.to_string());
        store.insert_package_installation(&later).await.unwrap();
        let first = maintenance.app_runtime_page("", &frontier).await.unwrap();
        assert_eq!(first.len(), 64);
        assert!(
            first
                .iter()
                .any(|(id, row)| id == "app-032" && row.is_none())
        );
        let second = maintenance
            .app_runtime_page(&first.last().unwrap().0, &frontier)
            .await
            .unwrap();
        assert_eq!(
            second.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            vec!["app-064", "app-065"]
        );
        assert!(
            maintenance
                .app_runtime_page("app-065", &frontier)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            maintenance.app_runtime_frontier().await.unwrap().as_deref(),
            Some("zzz-app")
        );
        assert_eq!(
            maintenance.database_connection().read_class(),
            pioneer_sqlite::SqliteReadClass::Maintenance
        );
        Ok(())
    })
    .await
    .unwrap();
}
