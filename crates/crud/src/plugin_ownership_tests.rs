//! Stage A regressions. NOT_RUN; test targets have not been compiled.
use super::tests::{pack_skill_record, skill_pack_record, test_store_with_workspace};
use super::*;
use pioneer_entity::plugin_installation;
use sea_orm::ConnectionTrait;

fn parent(id: &str, workspace: &str) -> plugin_installation::Model {
    plugin_installation::Model {
        id: id.into(),
        workspace_id: workspace.into(),
        name: "same-name".into(),
        version: None,
        source_upload_id: format!("upload-{id}"),
        package_path: format!("/managed/plugins/{id}/package"),
        data_path: format!("/managed/plugins/{id}/data"),
        package_fingerprint: "tree".into(),
        enabled: true,
        state: "installing".into(),
        revision: 1,
        pending_json: None,
        last_error: None,
        created_at: util::unix_to_datetime(1),
        updated_at: util::unix_to_datetime(1),
    }
}
fn ownership(parent: &str, child: &str, key: &str) -> PluginOwnershipWrite {
    PluginOwnershipWrite {
        plugin_id: parent.into(),
        expected_revision: 1,
        member_key: key.into(),
        member_path: Some(format!("skills/{key}")),
        package_fingerprint: "member-tree".into(),
        child_id: child.into(),
    }
}
fn skill(id: char, workspace: &str) -> SkillInstallationRecord {
    let mut row = pack_skill_record(
        id,
        &skill_pack_record('P', "unused", workspace),
        "same-slug",
    );
    row.pack_id = None;
    row.pack_member_key = None;
    row
}
fn policy(row: &SkillInstallationRecord) -> WorkspaceSkillPolicyRecord {
    WorkspaceSkillPolicyRecord {
        id: format!("policy-{}", row.skill_id),
        workspace_id: row.scope_key.clone(),
        skill_id: row.skill_id.clone(),
        enabled: Some(false),
        allow_implicit_invocation: Some(false),
    }
}
fn skill_audit(row: &SkillInstallationRecord) -> SkillAuditEventRecord {
    SkillAuditEventRecord {
        turn_id: None,
        skill_id: row.skill_id.clone(),
        skill_owner: None,
        skill_slug: row.slug.clone(),
        source_kind: row.source_kind.clone(),
        action: "install".into(),
        decision: "accepted".into(),
        reason_code: None,
        details_json: "{}".into(),
        created_at_unix: 1,
    }
}
fn mcp(id: &str, workspace: &str, name: &str) -> McpServerInstallationRecord {
    McpServerInstallationRecord {
        id: Some(id.into()),
        scope_kind: "workspace".into(),
        scope_key: workspace.into(),
        name: name.into(),
        display_name: None,
        source_kind: "config".into(),
        source_ref: "{}".into(),
        transport_kind: "stdio".into(),
        transport_json: "{}".into(),
        auth_json: "{}".into(),
        secret_refs_json: "[]".into(),
        enabled: false,
        allow_implicit_invocation: false,
        required: false,
        fingerprint: "native".into(),
        updated_at_unix: 1,
    }
}
fn mcp_audit(row: &McpServerInstallationRecord) -> McpAuditEventRecord {
    McpAuditEventRecord {
        turn_id: None,
        server_installation_id: None,
        server_name: row.name.clone(),
        raw_tool_name: None,
        callable_name: None,
        catalog_version: None,
        action: "install".into(),
        decision: "allowed".into(),
        reason_code: None,
        details_json: "{}".into(),
        created_at_unix: 1,
    }
}
#[tokio::test]
async fn native_skill_policy_audit_and_ownership_commit_together_without_upload() {
    let store = test_store_with_workspace("ws").await;
    store
        .insert_plugin_installation(&parent("plugin-a", "ws"))
        .await
        .unwrap();
    let row = skill('A', "ws");
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
            .unwrap()
    );
    assert_eq!(
        store
            .find_skill_installation(&row.skill_id)
            .await
            .unwrap()
            .unwrap()
            .skill_id,
        row.skill_id
    );
    assert_eq!(
        store
            .find_skill_plugin_owner(&row.skill_id)
            .await
            .unwrap()
            .unwrap()
            .plugin_id,
        "plugin-a"
    );
    // Genuine settings remain in the native row/policy, not in plugin_component.
    assert_eq!(
        store
            .list_plugin_components("plugin-a")
            .await
            .unwrap()
            .len(),
        1
    );
    let restarted = CrudStore::new(store.database_connection());
    assert!(
        restarted
            .find_skill_plugin_owner(&row.skill_id)
            .await
            .unwrap()
            .is_some()
    );
}
#[tokio::test]
async fn failed_ownership_insert_rolls_back_native_rows_policies_and_audits() {
    let store = test_store_with_workspace("ws").await;
    store
        .insert_plugin_installation(&parent("plugin-a", "ws"))
        .await
        .unwrap();
    // Inject a failure specifically at the final link publication, after the
    // native child/policy/audit writes, using the scoped SQLite writer.
    store.database_connection().execute_unprepared("CREATE TRIGGER fail_plugin_link BEFORE INSERT ON plugin_component BEGIN SELECT RAISE(ABORT,'injected link failure'); END;").await.unwrap();
    let row = skill('A', "ws");
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
    assert!(
        store
            .find_skill_installation(&row.skill_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .list_plugin_components("plugin-a")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_workspace_skill_policies("ws")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_skill_audit_event_records(&row.skill_id, 10)
            .await
            .unwrap()
            .is_empty()
    );
    let row = mcp("mcp-a", "ws", "internal-a");
    let owner = ownership("plugin-a", "mcp-a", "server");
    assert!(
        store
            .upsert_mcp_server_installation_with_audit_and_ownership(
                &row,
                &mcp_audit(&row),
                Some(&owner),
                1
            )
            .await
            .is_err()
    );
    assert!(
        store
            .find_mcp_server_installation("workspace", "ws", "internal-a")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .list_recent_mcp_audit_event_records("internal-a", 10)
            .await
            .unwrap()
            .is_empty()
    );
}
#[tokio::test]
async fn foreign_scope_stale_revision_and_native_name_collision_cannot_publish() {
    let store = test_store_with_workspace("ws").await;
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
}
#[tokio::test]
async fn same_authored_skill_names_do_not_collide_and_child_delete_unlinks_with_fk_off() {
    let store = test_store_with_workspace("ws").await;
    store
        .database_connection()
        .execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
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
            .execute_unprepared("DELETE FROM plugin_installation WHERE id='plugin-a'")
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
    use pioneer_sqlite::{SqliteReadClass, SqliteWriteClass};
    let store = test_store_with_workspace("ws").await;
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
}

#[tokio::test]
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
}

#[tokio::test]
async fn legacy_reserved_standalone_updates_but_new_names_and_owned_adoption_are_rejected() {
    let store = test_store_with_workspace("ws").await;
    let mut legacy = mcp("old", "ws", "pplugin_custom");
    // Seed the pre-plugin native record through the existing low-level operation.
    store
        .upsert_mcp_server_installation(&legacy, 1)
        .await
        .unwrap();
    legacy.transport_json = "updated".into();
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
            .find_mcp_server_installation("workspace", "ws", "pplugin_custom")
            .await
            .unwrap()
            .unwrap()
            .transport_json,
        "updated"
    );

    let fresh = mcp("new", "ws", "pplugin_new");
    assert!(
        store
            .upsert_mcp_server_installation_with_audit(&fresh, &mcp_audit(&fresh), 3,)
            .await
            .is_err()
    );
    assert!(
        store
            .find_mcp_server_installation("workspace", "ws", "pplugin_new")
            .await
            .unwrap()
            .is_none()
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
    let owned = mcp("child", "ws", "pplugin_owned");
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
    owned_update.transport_json = "owned-update".into();
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
    attempt.transport_json = "foreign-change".into();
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
    assert_eq!(
        store
            .find_mcp_server_installation("workspace", "ws", "pplugin_owned")
            .await
            .unwrap()
            .unwrap()
            .transport_json,
        "owned-update"
    );
    assert_eq!(
        store
            .find_mcp_plugin_owner("child")
            .await
            .unwrap()
            .unwrap()
            .plugin_id,
        "plugin-a"
    );
}

// Stage B sources: NOT_RUN / NOT_COMPILED.
#[tokio::test]
async fn plugin_reservation_consumes_only_its_finalized_owned_upload_atomically() {
    let store = test_store_with_workspace("ws").await;
    let mut parent = parent("P".repeat(21).as_str(), "ws");
    parent.pending_json = Some("{\"children\":[]}".into());
    let upload = SkillUploadSessionRecord {
        purpose: "plugin".into(),
        upload_id: parent.source_upload_id.clone(),
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
            .reserve_plugin_installation(&parent, 8, 3)
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
        store.reserve_plugin_installation(&parent, 7, 3),
        maintenance.reserve_plugin_installation(&parent, 7, 3)
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
            .reserve_plugin_installation(&parent, 7, 3)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn install_failure_preserves_native_sibling_and_closes_failed_child() {
    let store = test_store_with_workspace("ws").await;
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
}
