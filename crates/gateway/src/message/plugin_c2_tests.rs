//! Plugin projections through in-memory Gateway/native-session harnesses.
//! No provider executable is launched.
use super::*;
use pioneer_protocol::{
    AgentExecutionProfileId, AgentExecutionProfileSelection, AgentExecutionSelection,
    AgentIdentitySelection, AgentLaunchSelection, SkillId, TurnCapability, TurnCapabilityKind,
};

async fn seed_owned_c2_skill(
    store: &CrudStore,
    workspace: &str,
    root: &std::path::Path,
) -> (String, SkillId, TurnCapability) {
    let parent = "P".repeat(21);
    let skill = SkillId::new("S".repeat(21)).unwrap();
    let member = root.join("skills/bundled");
    std::fs::create_dir_all(&member).unwrap();
    std::fs::create_dir_all(member.join("scripts")).unwrap();
    std::fs::write(member.join("SKILL.md"), "---\nname: bundled\ndescription: Selected packaged skill\n---\nRead scripts/helper.txt\n").unwrap();
    std::fs::write(member.join("scripts/helper.txt"), "supporting file").unwrap();
    let now = chrono::Utc::now().fixed_offset();
    store
        .insert_plugin_installation(&pioneer_entity::plugin_installation::Model {
            id: parent.clone(),
            workspace_id: workspace.into(),
            name: "authoritative-parent".into(),
            version: None,
            source_upload_id: "c2-upload".into(),
            package_path: root.to_string_lossy().into(),
            data_path: root.with_extension("data").to_string_lossy().into(),
            package_fingerprint: "context".into(),
            enabled: true, allow_implicit_invocation: false,
            state: "installing".into(),
            revision: 1,
        display_name: None,
        short_description: None,
        description: None,
        long_description: None,
        developer_name: None,
        author_name: None,
        author_email: None,
        author_url: None,
        homepage: None,
        repository: None,
        documentation_url: None,
        support_url: None,
        privacy_policy_url: None,
        terms_of_service_url: None,
        license: None,
        category: None,
        brand_color: None,
        logo_path: None,
        logo_revision: None,
        composer_icon_path: None,
        composer_icon_revision: None,
            pending_json: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    store
        .install_skill_lifecycle_with_ownership(
            &pioneer_crud::SkillInstallationRecord {
                skill_id: skill.clone(),
                owner: None,
                slug: "bundled".into(),
                version: None,
                source_kind: "user".into(),
                scope_key: workspace.into(),
                source_ref: "c2-package".into(),
                install_path: member.to_string_lossy().into(),
                trust_level: "community".into(),
                fingerprint: "member".into(),
                updated_at_unix: 1,
                pack_id: None,
                pack_member_key: None,
            },
            &pioneer_crud::WorkspaceSkillPolicyRecord {
                id: "c2-policy".into(),
                workspace_id: workspace.into(),
                skill_id: skill.clone(),
                enabled: Some(true),
                allow_implicit_invocation: Some(false),
            },
            &[fixture_skill_lifecycle_audit(
                &skill, "bundled", "install", 1,
            )],
            None,
            Some(&pioneer_crud::PluginOwnershipWrite {
                plugin_id: parent.clone(),
                expected_revision: 1,
                member_key: "bundled".into(),
                member_path: Some("skills/bundled".into()),
                package_fingerprint: "member".into(),
                child_id: skill.to_string(),
            }),
            1,
        )
        .await
        .unwrap();
    store
        .settle_plugin_installation(&parent, 1, "installed", None)
        .await
        .unwrap();
    let selection = TurnCapability {
        id: pioneer_protocol::plugin_capability_key(&parent),
        label: Some("fabricated-label".into()),
        kind: TurnCapabilityKind::Plugin {
            plugin_id: parent.clone(),
            expected_revision: 1,
        },
    };
    (parent, skill, selection)
}

fn task_parent_launch(selected: TurnCapability) -> AgentLaunchSelection {
    AgentLaunchSelection {
        agent: AgentIdentitySelection::DefaultPioneer,
        execution: AgentExecutionSelection {
            profile: AgentExecutionProfileSelection::Exact {
                profile_id: AgentExecutionProfileId::new("P".repeat(21)).unwrap(),
            },
            reasoning: None,
            permission_profile: None,
            skill_ids: vec![],
            mcp_server_ids: vec![],
            selected_capabilities: vec![selected],
        },
    }
}

#[tokio::test]
async fn task_parent_pinning_uses_server_expansion_and_preserves_a_native_ceiling() {
    let harness = setup_cli_runtime_security_harness(None).await;
    let root = unique_temp_dir("c2-task-parent");
    let (parent, skill, selected) =
        seed_owned_c2_skill(&harness.crud_store, &harness.workspace_id, &root).await;
    let mut forged = task_parent_launch(selected.clone());
    forged.execution.skill_ids = vec![skill.clone()];
    assert!(
        harness
            .processor
            .normalize_new_task_launch_capabilities(&harness.workspace_id, &mut forged)
            .await
            .is_err(),
        "public native fields cannot adopt owned children"
    );
    let mut launch = task_parent_launch(selected);
    let normalized = harness
        .processor
        .normalize_new_task_launch_capabilities(&harness.workspace_id, &mut launch)
        .await
        .unwrap();
    assert_eq!(normalized.execution.len(), 1);
    assert_eq!(launch.execution.skill_ids, [skill.clone()]);
    assert_eq!(launch.execution.selected_capabilities.len(), 1);
    assert!(
        matches!(&launch.execution.selected_capabilities[0].kind, TurnCapabilityKind::Plugin { plugin_id, expected_revision: 1 } if plugin_id == &parent)
    );
    assert_eq!(
        launch.execution.selected_capabilities[0].label.as_deref(),
        Some("authoritative-parent")
    );
    let restored = harness
        .processor
        .normalize_task_launch_capabilities(&harness.workspace_id, &launch.execution)
        .await
        .unwrap();
    assert_eq!(restored.execution, normalized.execution);
    let mut restricted = launch.execution.clone();
    restricted.skill_ids.clear();
    let restored = harness
        .processor
        .normalize_task_launch_capabilities(&harness.workspace_id, &restricted)
        .await
        .unwrap();
    assert!(
        restored.execution.is_empty(),
        "inheritance cannot widen a durable native grant"
    );
    assert_eq!(
        restored.presentation.len(),
        1,
        "an empty native grant retains one parent chip"
    );
    let mut forged = launch.execution.clone();
    forged.skill_ids.push(SkillId::new("F".repeat(21)).unwrap());
    assert!(
        harness
            .processor
            .normalize_task_launch_capabilities(&harness.workspace_id, &forged)
            .await
            .is_err()
    );
    harness
        .crud_store
        .begin_plugin_mutation(
            &harness.workspace_id,
            &parent,
            1,
            "updating",
            true,
            "{\"kind\":\"update\"}",
        )
        .await
        .unwrap();
    assert!(
        harness
            .processor
            .normalize_task_launch_capabilities(&harness.workspace_id, &launch.execution)
            .await
            .is_err(),
        "pending/revision change blocks Task recovery, without silent revision refresh"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn cli_restore_requires_committed_ready_bindings_and_current_parent_authority() {
    use sea_orm::ConnectionTrait;
    let harness = setup_cli_runtime_security_harness(None).await;
    let root = unique_temp_dir("c2-ready");
    let (parent, skill, selected) =
        seed_owned_c2_skill(&harness.crud_store, &harness.workspace_id, &root).await;
    let normalized = harness
        .processor
        .normalize_turn_skill_capabilities(&harness.workspace_id, &[selected])
        .await
        .unwrap();
    let snapshot = normalized.plugin_selection.unwrap();
    assert!(
        harness
            .crud_store
            .prepare_plugin_selection("c2-ready-turn", &snapshot)
            .await
            .is_err(),
        "prepared metadata requires a durable Turn"
    );
    harness.crud_store.database_connection().execute_unprepared(&format!(
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,sidebar_visibility,access_class,created_at,updated_at) VALUES('c2-ready-thread','{}','','chat','test','test','active','user','visible','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES('c2-ready-turn','c2-ready-thread','in_progress','{{}}','user','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);", harness.workspace_id
    )).await.unwrap();
    harness
        .crud_store
        .prepare_plugin_selection("c2-ready-turn", &snapshot)
        .await
        .unwrap();
    assert!(
        super::super::plugins::validate_cli_plugin_selection(
            &harness.crud_store,
            &harness.workspace_id,
            "c2-ready-turn"
        )
        .await
        .is_err()
    );
    assert!(
        harness
            .crud_store
            .ready_plugin_selection("c2-ready-turn", &[skill.clone()])
            .await
            .is_err(),
        "resolved event without committed binding is not ready"
    );
    assert_eq!(
        harness
            .crud_store
            .get_plugin_selection("c2-ready-turn")
            .await
            .unwrap()
            .unwrap()
            .phase,
        "prepared"
    );
    harness
        .crud_store
        .replace_turn_skill_bindings(
            "c2-ready-turn",
            &[pioneer_crud::TurnSkillBindingRecord {
                skill_id: skill,
                skill_owner: None,
                skill_slug: "bundled".into(),
                skill_version: None,
                fingerprint: "member".into(),
                source_kind: "user".into(),
                resolved_reason: "explicit_composer_capability".into(),
            }],
            1,
        )
        .await
        .unwrap();
    harness
        .crud_store
        .ready_plugin_selection("c2-ready-turn", &[SkillId::new("S".repeat(21)).unwrap()])
        .await
        .unwrap();
    let presentation = harness
        .processor
        .inherited_plugin_presentation(
            &harness.workspace_id,
            "c2-ready-turn",
            &normalized.execution,
        )
        .await
        .unwrap();
    assert_eq!(presentation.len(), 1);
    assert!(matches!(
        presentation[0].kind,
        TurnCapabilityKind::Plugin { .. }
    ));
    let mut omitted = snapshot.clone();
    omitted.children.clear();
    harness
        .crud_store
        .prepare_plugin_selection("c2-ready-turn", &omitted)
        .await
        .unwrap();
    harness
        .crud_store
        .ready_plugin_selection("c2-ready-turn", &[])
        .await
        .unwrap();
    assert!(
        super::super::plugins::validate_cli_plugin_selection(
            &harness.crud_store,
            &harness.workspace_id,
            "c2-ready-turn"
        )
        .await
        .is_err(),
        "partial metadata cannot authorize frozen owned bindings outside selection"
    );
    harness
        .crud_store
        .prepare_plugin_selection("c2-ready-turn", &snapshot)
        .await
        .unwrap();
    harness
        .crud_store
        .ready_plugin_selection("c2-ready-turn", &[SkillId::new("S".repeat(21)).unwrap()])
        .await
        .unwrap();
    let guards = harness
        .processor
        .acquire_plugin_launch_guards(&harness.workspace_id, "c2-ready-turn")
        .await
        .unwrap();
    assert!(
        harness
            .processor
            .acquire_plugin_mutation(&harness.workspace_id, &parent, 1)
            .await
            .is_err()
    );
    drop(guards);
    harness
        .crud_store
        .begin_plugin_mutation(
            &harness.workspace_id,
            &parent,
            1,
            "updating",
            false,
            "{\"kind\":\"enabled\"}",
        )
        .await
        .unwrap();
    assert!(
        super::super::plugins::validate_cli_plugin_selection(
            &harness.crud_store,
            &harness.workspace_id,
            "c2-ready-turn"
        )
        .await
        .is_err(),
        "historical ready binding does not authorize continuation after disable"
    );
    assert!(
        harness
            .processor
            .inherited_plugin_presentation(
                &harness.workspace_id,
                "c2-ready-turn",
                &normalized.execution
            )
            .await
            .is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn ordinary_owned_cli_start_uses_native_skill_and_ready_parent_for_both_providers() {
    run_standard_stack_message_test("C2 owned native projection", async {
        for kind in [CLIAgentRuntimeKind::Claude, CLIAgentRuntimeKind::Codex] {
            for explicit_agent in [false, true] {
                let mut harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
                let root = harness.user_root.join("owned-context");
                let (parent, skill, selected) =
                    seed_owned_c2_skill(&harness.crud_store, &harness.workspace_id, &root).await;
                let thread = format!("c2-owned-thread-{kind:?}");
                let turn = format!("c2-owned-turn-{kind:?}");
                seed_cli_runtime_skill_preflight_thread(&harness, &thread).await;
                let request = cli_runtime_turn_start_request_with_capabilities(
                    &generate_test_request_id("c2owned", "start"),
                    &thread,
                    &turn,
                    &harness.runtime_id,
                    kind,
                    vec![serde_json::to_value(&selected).unwrap()],
                );
                let mut envelope: JsonValue = serde_json::from_str(&request).unwrap();
                if explicit_agent {
                    let mut launch = exact_cli_task_launch_for_test(
                        &harness.processor,
                        &harness.workspace_id,
                        "openai",
                        if kind == CLIAgentRuntimeKind::Claude {
                            "claude-sonnet"
                        } else {
                            "o4-mini"
                        },
                        &harness.runtime_id,
                    )
                    .await
                    .unwrap();
                    launch.execution.permission_profile =
                        Some(pioneer_protocol::TurnPermissionProfileSelection::full_access());
                    envelope["params"]["agent_launch"] = serde_json::to_value(launch).unwrap();
                }
                let request = envelope.to_string();
                harness
                    .processor
                    .process_request_for_connection(harness.connection_id, &request)
                    .await;
                let request_id = envelope["id"].as_str().unwrap();
                let response = recv_jsonrpc_payload_by_id(&mut harness.rx, request_id).await;
                let response: JsonValue = serde_json::from_str(&response).unwrap();
                assert!(
                    response.get("error").is_none(),
                    "{kind:?}, explicit_agent={explicit_agent}: native plugin launch rejected: {response}"
                );
                let native = wait_for_recorded_cli_runtime_turn_start(&harness.cli_session).await;
                let selection = harness
                    .crud_store
                    .get_plugin_selection(&turn)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(selection.phase, "ready");
                assert_eq!(selection.parents[0].id, parent);
                assert_eq!(selection.children[0].id, skill.to_string());
                let bindings = harness
                    .crud_store
                    .list_turn_skill_bindings(&turn)
                    .await
                    .unwrap();
                assert_eq!(
                    bindings
                        .iter()
                        .filter(|binding| binding.skill_id == skill)
                        .count(),
                    1,
                    "the selected plugin skill has exactly one committed binding"
                );
                let install_name = "bundled";
                let receipt = pioneer_skills::read_external_runtime_receipt(
                    &harness
                        .runtime_home
                        .join(pioneer_skills::EXTERNAL_RUNTIME_RECEIPT_FILE_NAME),
                )
                .unwrap();
                let installed = receipt
                    .entries
                    .iter()
                    .find(|entry| {
                        entry.skill_id == skill
                            && entry.runtime_id == harness.runtime_id
                            && entry.plugin_id.as_deref() == Some(parent.as_str())
                    })
                    .unwrap();
                assert_eq!(installed.install_name, install_name);
                let context = std::path::PathBuf::from(&installed.install_path);
                assert_eq!(context, harness.native_home.join("skills").join(install_name));
                assert_eq!(
                    std::fs::read(context.join("scripts/helper.txt")).unwrap(),
                    b"supporting file"
                );
                assert_eq!(
                    std::fs::read(context.join("SKILL.md")).unwrap(),
                    std::fs::read(root.join("skills/bundled/SKILL.md")).unwrap()
                );
                if kind == CLIAgentRuntimeKind::Codex {
                    let items: Vec<pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem> =
                        serde_json::from_value(native.input).unwrap();
                    assert!(items.iter().any(|item| matches!(item, pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Skill { name, path } if name == install_name && path == &context.join("SKILL.md").to_string_lossy())), "Codex receives its genuine explicit native Skill item");
                }
                let owners = harness
                    .cli_manager
                    .plugin_stop_inventory(&harness.workspace_id, &parent);
                assert_eq!(owners.len(), 1);
                assert!(
                    owners[0].owns_turn(&turn),
                    "lifecycle owns the actual CLI instance used by this durable Turn"
                );
                assert_eq!(
                    harness
                        .cli_manager
                        .plugin_selection_for_instance(owners[0].instance())
                        .unwrap(),
                    Some(selection)
                );
            }
        }
    });
}

#[tokio::test]
async fn plugin_previews_and_update_require_the_full_authenticated_transfer_owner() {
    let mut harness = setup_cli_runtime_security_harness(None).await;
    let root = unique_temp_dir("plugin-transfer-owner");
    let package = root.join("parent/package");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(package.join("plugin.json"), serde_json::to_vec(&json!({
        "$schema": pioneer_plugins::PLUGIN_SCHEMA, "name": "owned-transfer"
    })).unwrap()).unwrap();
    let snapshot = pioneer_plugins::Snapshot::capture(&package, harness.processor.package_delivery_context().plugin_limits(), || false).unwrap();
    let plan = pioneer_plugins::load(&snapshot).unwrap();
    let archive = build_test_directory_archive(&package);
    let payload = root.join("valid.tar.gz");
    std::fs::write(&payload, &archive).unwrap();
    let now = now_timestamp_secs();
    let upload_id = "U".repeat(21);
    harness.crud_store.insert_skill_upload_session(&pioneer_crud::SkillUploadSessionRecord {
        purpose: "plugin".into(), upload_id: upload_id.clone(), workspace_id: harness.workspace_id.clone(),
        connection_id: harness.connection_id, status: "finalized".into(), file_name: "valid.tar.gz".into(),
        archive_format: "tar_gz".into(), compressed_size_bytes: archive.len() as u64, received_bytes: archive.len() as u64,
        sha256: hex::encode(Sha256::digest(&archive)), payload_path: payload.to_string_lossy().into(),
        created_at_unix: now, expires_at_unix: now + 3600, finalized_at_unix: Some(now), consumed_at_unix: None, aborted_at_unix: None,
    }).await.unwrap();
    let parent = "P".repeat(21);
    let timestamp = chrono::Utc::now().fixed_offset();
    harness.crud_store.insert_plugin_installation(&pioneer_entity::plugin_installation::Model {
        id: parent.clone(), workspace_id: harness.workspace_id.clone(), name: plan.manifest.name.clone(), version: None,
        source_upload_id: "old-source".into(), package_path: package.to_string_lossy().into(),
        data_path: root.join("parent/data").to_string_lossy().into(), package_fingerprint: plan.tree_digest.clone(), enabled: true, allow_implicit_invocation: false,
        state: "installing".into(), revision: 1,
        display_name: None,
        short_description: None,
        description: None,
        long_description: None,
        developer_name: None,
        author_name: None,
        author_email: None,
        author_url: None,
        homepage: None,
        repository: None,
        documentation_url: None,
        support_url: None,
        privacy_policy_url: None,
        terms_of_service_url: None,
        license: None,
        category: None,
        brand_color: None,
        logo_path: None,
        logo_revision: None,
        composer_icon_path: None,
        composer_icon_revision: None, pending_json: None, last_error: None, created_at: timestamp, updated_at: timestamp,
    }).await.unwrap();
    harness.crud_store.settle_plugin_installation(&parent, 1, "installed", None).await.unwrap();
    let context = registered_request_context(&harness.processor, harness.connection_id, methods::PLUGINS_PREVIEW).await;
    let owner = AuthenticatedTransferOwner::from_request_context(&context);
    // A genuine finalized archive and full owner form the positive control.
    // Missing/foreign owner negatives below keep all archive facts valid.
    let runtime = harness.processor.package_delivery_context();
    harness.processor.skill_upload_owners.lock().await.insert(upload_id.clone(), owner.clone());
    let source = harness.processor.materialize_uploaded_archive_source(
        &owner, &harness.workspace_id, &upload_id, &runtime,
        &RequestId::new(generate_test_request_id("transfer", "archive-control")).unwrap(), "plugin",
    ).await.unwrap();
    assert_eq!(source.upload.status, "finalized");
    drop(source);
    // Positive control through the actual preview RPC, with the same archive.
    harness.processor.skill_upload_owners.lock().await.insert(upload_id.clone(), owner.clone());
    let preview_id = generate_test_request_id("transfer", "owned-preview");
    let request = json!({"jsonrpc":"2.0","id":preview_id,"method":methods::PLUGINS_PREVIEW,
        "params":{"workspace_id":harness.workspace_id,"upload_id":upload_id}});
    harness.processor.process_request_for_connection(harness.connection_id, &request.to_string()).await;
    let preview = recv_response_by_id(&mut harness.rx, &preview_id).await;
    let fingerprint = preview.result["fingerprint"].as_str().unwrap().to_owned();
    for foreign in [false, true] {
        harness.processor.skill_upload_owners.lock().await.remove(&upload_id);
        if foreign {
            let mut foreign_owner = owner.clone();
            foreign_owner.auth_session_id = pioneer_protocol::AuthSessionId::new("F".repeat(21)).unwrap();
            harness.processor.skill_upload_owners.lock().await.insert(upload_id.clone(), foreign_owner);
        }
        for (suffix, method, params) in [
            ("preview", methods::PLUGINS_PREVIEW, json!({"workspace_id":harness.workspace_id,"upload_id":upload_id})),
            ("update-preview", methods::PLUGINS_PREVIEW, json!({"workspace_id":harness.workspace_id,"upload_id":upload_id,"target":{"plugin_id":parent,"expected_revision":1}})),
            ("update", methods::PLUGINS_UPDATE, json!({"workspace_id":harness.workspace_id,"plugin_id":parent,"expected_revision":1,"upload_id":upload_id,"expected_fingerprint":fingerprint,"confirm_changes":true})),
        ] {
            let id = generate_test_request_id(&format!("transfer-{foreign}"), suffix);
            let request = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
            harness.processor.process_request_for_connection(harness.connection_id, &request.to_string()).await;
            let _rejected = recv_error_by_id(&mut harness.rx, &id).await;
            let current = harness.crud_store.find_plugin_installation(&parent).await.unwrap().unwrap();
            assert_eq!(current.revision, 1);
            assert!(current.pending_json.is_none());
            assert_eq!(harness.crud_store.find_skill_upload_session(&upload_id).await.unwrap().unwrap().status, "finalized");
        }
    }
    // Alternate Skills entry must not read/abort a foreign malformed Plugin
    // transfer even when its persisted numeric owner matches this connection.
    let bad_id = "B".repeat(21);
    let bad_payload = root.join("malformed.tar.gz");
    std::fs::write(&bad_payload, b"finalized but not gzip").unwrap();
    let mut bad = harness.crud_store.find_skill_upload_session(&upload_id).await.unwrap().unwrap();
    bad.upload_id = bad_id.clone(); bad.payload_path = bad_payload.to_string_lossy().into();
    bad.sha256 = hex::encode(Sha256::digest(b"finalized but not gzip"));
    bad.compressed_size_bytes = b"finalized but not gzip".len() as u64; bad.received_bytes = bad.compressed_size_bytes;
    harness.crud_store.insert_skill_upload_session(&bad).await.unwrap();
    let request_id = RequestId::new(generate_test_request_id("transfer", "alternate-entry")).unwrap();
    assert!(harness.processor.materialize_uploaded_skill_source(&owner, &harness.workspace_id, &bad_id, &request_id).await.is_err());
    assert_eq!(harness.crud_store.find_skill_upload_session(&bad_id).await.unwrap().unwrap().status, "finalized");
    assert_eq!(std::fs::read(&bad_payload).unwrap(), b"finalized but not gzip");
    // Matching owner with wrong purpose is also rejected before extraction.
    harness.processor.skill_upload_owners.lock().await.insert(bad_id.clone(), owner.clone());
    assert!(harness.processor.materialize_uploaded_skill_pack_source(&owner, &harness.workspace_id, &bad_id, &request_id).await.is_err());
    assert_eq!(harness.crud_store.find_skill_upload_session(&bad_id).await.unwrap().unwrap().status, "finalized");
    // Genuine owner + matching Plugin purpose reaches extraction and may abort
    // its own invalid bytes; no competing transfer is touched.
    assert!(harness.processor.materialize_uploaded_archive_source(&owner, &harness.workspace_id, &bad_id, &runtime, &request_id, "plugin").await.is_err());
    assert_eq!(harness.crud_store.find_skill_upload_session(&bad_id).await.unwrap().unwrap().status, "aborted");
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn plugin_policy_rpc_preserves_independent_toggles_and_frozen_task_grants() {
    let mut harness = setup_cli_runtime_security_harness(None).await;
    let root = unique_temp_dir("plugin-implicit-policy");
    let (parent, skill, selected) =
        seed_owned_c2_skill(&harness.crud_store, &harness.workspace_id, &root).await;
    let mut frozen = task_parent_launch(selected.clone()).execution;
    frozen.skill_ids.clear();
    frozen.selected_capabilities.clear();
    let initial = harness.processor
        .normalize_turn_skill_capabilities(&harness.workspace_id, &[]).await.unwrap();
    assert!(initial.execution.is_empty());
    // These are the same RPC payloads as the two header buttons. The enabled
    // button deliberately omits implicit to preserve that independent setting.
    for (revision, enabled, implicit) in [
        (1, true, Some(true)),
        (2, false, None),
        (3, true, None),
        (4, true, Some(false)),
    ] {
        let id = generate_test_request_id("plugin-policy", &revision.to_string());
        let mut params = json!({
            "workspace_id":harness.workspace_id, "plugin_id":parent,
            "expected_revision":revision, "enabled":enabled,
        });
        if let Some(implicit) = implicit {
            params["allow_implicit_invocation"] = json!(implicit);
        }
        harness.processor.process_request_for_connection(harness.connection_id,
            &json!({"jsonrpc":"2.0","id":id,"method":methods::PLUGINS_SET_ENABLED,"params":params}).to_string(),
        ).await;
        let response = recv_response_by_id(&mut harness.rx, &id).await;
        let item: pioneer_protocol::PluginItem = serde_json::from_value(response.result).unwrap();
        assert_eq!(item.enabled, enabled);
        assert_eq!(item.allow_implicit_invocation, revision != 4);
        assert_eq!(item.revision, revision + 1);
        let stored = harness.crud_store.find_plugin_installation(&parent).await.unwrap().unwrap();
        assert_eq!(stored.enabled, enabled);
        assert_eq!(stored.allow_implicit_invocation, revision != 4);
        let automatic = harness.processor
            .normalize_turn_skill_capabilities(&harness.workspace_id, &[]).await.unwrap();
        if enabled && revision != 4 {
            assert!(automatic.execution.iter().any(|cap| matches!(&cap.kind,
                TurnCapabilityKind::Skill { skill_id, .. } if skill_id == &skill)));
            assert_eq!(automatic.plugin_selection.unwrap().parents[0].revision, revision + 1);
        } else {
            assert!(automatic.execution.is_empty());
            assert!(automatic.plugin_selection.is_none());
        }
        // Opting into a plugin cannot widen an already persisted task ceiling.
        let restored = harness.processor
            .normalize_task_launch_capabilities(&harness.workspace_id, &frozen).await.unwrap();
        assert!(restored.execution.is_empty());
        assert!(restored.plugin_selection.is_none());
    }
    // Turning implicit off keeps explicit selection available.
    let mut current = selected;
    if let TurnCapabilityKind::Plugin { expected_revision, .. } = &mut current.kind {
        *expected_revision = 5;
    }
    let explicit = harness.processor
        .normalize_turn_skill_capabilities(&harness.workspace_id, &[current]).await.unwrap();
    assert!(explicit.execution.iter().any(|cap| matches!(&cap.kind,
        TurnCapabilityKind::Skill { skill_id, .. } if skill_id == &skill)));
    std::fs::remove_dir_all(root).unwrap();
}
