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

// B-02: native resolver output is supplied separately from durable bindings.
// Missing bindings for selected leaves remain a commit failure; exclusions do
// not turn installed siblings into an all-or-nothing dependency.
#[tokio::test]
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
                &[],
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
            "CREATE TRIGGER fail_plugin_ready BEFORE UPDATE OF plugin_selection_json ON turn \
         BEGIN SELECT RAISE(ABORT,'fixture ready write failure'); END;",
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
        .execute_unprepared("DROP TRIGGER fail_plugin_ready")
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

// C1 source regressions: NOT_RUN / NOT_COMPILED.
#[tokio::test]
async fn closed_gate_reconciliation_and_enable_preserve_native_restrictions() {
    let store = test_store_with_workspace("ws-c1").await;
    let id = "P".repeat(21);
    store
        .insert_plugin_installation(&parent(&id, "ws-c1"))
        .await
        .unwrap();
    let child = skill('S', "ws-c1");
    let write = ownership(&id, child.skill_id.as_str(), "one");
    store
        .install_skill_lifecycle_with_ownership(&child, &policy(&child), &[], None, Some(&write), 1)
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
}

#[tokio::test]
async fn failed_assets_only_update_preserves_the_last_committed_tree_for_retry() {
    let store = test_store_with_workspace("ws-c1-assets").await;
    let id = "P".repeat(21);
    store
        .insert_plugin_installation(&parent(&id, "ws-c1-assets"))
        .await
        .unwrap();
    let child = skill('S', "ws-c1-assets");
    let write = ownership(&id, child.skill_id.as_str(), "one");
    store
        .install_skill_lifecycle_with_ownership(&child, &policy(&child), &[], None, Some(&write), 1)
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
}

#[tokio::test]
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
            "CREATE TRIGGER c1_native_audit_failure BEFORE INSERT ON mcp_audit_event \
         BEGIN SELECT RAISE(ABORT, 'fixture audit failure'); END",
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
        .execute_unprepared("DROP TRIGGER c1_native_audit_failure")
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

#[tokio::test]
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
            &[],
            None,
            Some(&ownership(&parent_id, child.skill_id.as_str(), "one")),
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
            "CREATE TRIGGER c1_skill_audit_failure BEFORE INSERT ON skill_audit_event \
         BEGIN SELECT RAISE(ABORT, 'fixture audit failure'); END",
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
        .execute_unprepared("DROP TRIGGER c1_skill_audit_failure")
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
