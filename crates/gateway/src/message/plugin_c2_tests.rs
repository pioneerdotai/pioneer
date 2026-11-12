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
    seed_owned_c2_package(
        store,
        workspace,
        root,
        pioneer_protocol::PackageKind::Plugin,
    )
    .await
}
async fn seed_owned_c2_package(
    store: &CrudStore,
    workspace: &str,
    root: &std::path::Path,
    kind: pioneer_protocol::PackageKind,
) -> (String, SkillId, TurnCapability) {
    let app = kind == pioneer_protocol::PackageKind::App;
    let parent = if app { "A" } else { "P" }.repeat(21);
    let skill = SkillId::new(if app { "T" } else { "S" }.repeat(21)).unwrap();
    let member_name = if app { "bundled-app" } else { "bundled" };
    let member = root.join(format!("skills/{member_name}"));
    std::fs::create_dir_all(&member).unwrap();
    std::fs::create_dir_all(member.join("scripts")).unwrap();
    std::fs::write(
        member.join("SKILL.md"),
        format!("---\nname: {member_name}\ndescription: Selected packaged skill\n---\nRead scripts/helper.txt\n"),
    )
    .unwrap();
    std::fs::write(member.join("scripts/helper.txt"), "supporting file").unwrap();
    let now = chrono::Utc::now().fixed_offset();
    store
        .insert_package_installation(&pioneer_crud::PackageRecord {
            kind,
            app_manifest_json:app.then(||serde_json::json!({"slug":"authoritative-app","name":"Authoritative App","version":"1.0.0","shortDescription":"App regression","developer":{"name":"Author"},"icon":{"light":"icon.png","dark":"icon.png"},"skills":"skills"}).to_string()),
            app_localizations_json:app.then(||"{}".into()),
            operation_error_json: None,
            id: parent.clone(),
            workspace_id: workspace.into(),
            name:if app{"authoritative-app"}else{"authoritative-parent"}.into(),
            version: None,
            source_upload_id:format!("c2-upload-{}",kind.as_str()),
            package_path: root.to_string_lossy().into(),
            data_path: root.with_extension("data").to_string_lossy().into(),
            package_fingerprint: "context".into(),
            enabled: true,
            allow_implicit_invocation: false,
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
                slug: member_name.into(),
                version: None,
                source_kind: "user".into(),
                scope_key: workspace.into(),
                source_ref: if app {
                    format!("app:{parent}:{member_name}")
                } else {
                    "c2-package".into()
                },
                install_path: member.to_string_lossy().into(),
                trust_level: "community".into(),
                fingerprint: "member".into(),
                updated_at_unix: 1,
                pack_id: None,
                pack_member_key: None,
            },
            &pioneer_crud::WorkspaceSkillPolicyRecord {
                id: format!("c2-policy-{}", kind.as_str()),
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
                kind,
                plugin_id: parent.clone(),
                expected_revision: 1,
                member_key: member_name.into(),
                member_path: Some(format!("skills/{member_name}")),
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
    let mut selection = pioneer_protocol::package_capability(kind, &parent, 1);
    selection.label = Some("fabricated-label".into());
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
        TurnCapabilityKind::Plugin { .. } | TurnCapabilityKind::App { .. }
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
                assert_eq!(
                    context,
                    harness.native_home.join("skills").join(install_name)
                );
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
    let manifest = serde_json::to_vec(&json!({
        "$schema": pioneer_plugins::PLUGIN_SCHEMA, "name": "owned-transfer"
    }))
    .unwrap();
    std::fs::write(package.join("plugin.json"), &manifest).unwrap();
    let snapshot = pioneer_plugins::Snapshot::capture(
        &package,
        harness.processor.package_delivery_context().plugin_limits(),
        || false,
    )
    .unwrap();
    let plan = pioneer_plugins::load(&snapshot).unwrap();
    let archive = build_test_directory_archive(&package);
    let upload_id = "U".repeat(21);
    let payload = root.join("uploads").join(&upload_id).join("payload.tar.gz");
    std::fs::create_dir_all(payload.parent().unwrap()).unwrap();
    std::fs::write(&payload, &archive).unwrap();
    let now = now_timestamp_secs();
    harness
        .crud_store
        .insert_skill_upload_session(&pioneer_crud::SkillUploadSessionRecord {
            purpose: "plugin".into(),
            upload_id: upload_id.clone(),
            workspace_id: harness.workspace_id.clone(),
            connection_id: harness.connection_id,
            status: "finalized".into(),
            file_name: "valid.tar.gz".into(),
            archive_format: "tar_gz".into(),
            compressed_size_bytes: archive.len() as u64,
            received_bytes: archive.len() as u64,
            sha256: hex::encode(Sha256::digest(&archive)),
            payload_path: payload.to_string_lossy().into(),
            created_at_unix: now,
            expires_at_unix: now + 3600,
            finalized_at_unix: Some(now),
            consumed_at_unix: None,
            aborted_at_unix: None,
        })
        .await
        .unwrap();
    let parent = "P".repeat(21);
    let timestamp = chrono::Utc::now().fixed_offset();
    harness
        .crud_store
        .insert_plugin_installation(&pioneer_crud::PackageRecord {
            kind: pioneer_crud::PackageKind::Plugin,
            app_manifest_json: None,
            app_localizations_json: None,
            operation_error_json: None,
            id: parent.clone(),
            workspace_id: harness.workspace_id.clone(),
            name: plan.manifest.name.clone(),
            version: None,
            source_upload_id: "old-source".into(),
            package_path: package.to_string_lossy().into(),
            data_path: root.join("parent/data").to_string_lossy().into(),
            package_fingerprint: plan.tree_digest.clone(),
            enabled: true,
            allow_implicit_invocation: false,
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
            created_at: timestamp,
            updated_at: timestamp,
        })
        .await
        .unwrap();
    harness
        .crud_store
        .settle_plugin_installation(&parent, 1, "installed", None)
        .await
        .unwrap();
    let context = registered_request_context(
        &harness.processor,
        harness.connection_id,
        methods::PLUGINS_PREVIEW,
    )
    .await;
    let owner = AuthenticatedTransferOwner::from_request_context(&context);
    // A genuine finalized archive and full owner form the positive control.
    // Missing/foreign owner negatives below keep all archive facts valid.
    let runtime = harness.processor.package_delivery_context();
    harness
        .processor
        .skill_upload_owners
        .lock()
        .await
        .insert(upload_id.clone(), owner.clone());
    let source = harness
        .processor
        .materialize_uploaded_archive_source(
            &owner,
            &harness.workspace_id,
            &upload_id,
            &runtime,
            &RequestId::new(generate_test_request_id("transfer", "archive-control")).unwrap(),
            "plugin",
        )
        .await
        .unwrap();
    assert_eq!(source.upload.status, "finalized");
    drop(source);
    // Positive control through the actual preview RPC, with the same archive.
    harness
        .processor
        .skill_upload_owners
        .lock()
        .await
        .insert(upload_id.clone(), owner.clone());
    let preview_id = generate_test_request_id("transfer", "owned-preview");
    let request = json!({"jsonrpc":"2.0","id":preview_id,"method":methods::PLUGINS_PREVIEW,
        "params":{"workspace_id":harness.workspace_id,"upload_id":upload_id}});
    harness
        .processor
        .process_request_for_connection(harness.connection_id, &request.to_string())
        .await;
    let preview = recv_response_by_id(&mut harness.rx, &preview_id).await;
    let fingerprint = preview.result["fingerprint"].as_str().unwrap().to_owned();
    for foreign in [false, true] {
        harness
            .processor
            .skill_upload_owners
            .lock()
            .await
            .remove(&upload_id);
        if foreign {
            let mut foreign_owner = owner.clone();
            foreign_owner.auth_session_id =
                pioneer_protocol::AuthSessionId::new("F".repeat(21)).unwrap();
            harness
                .processor
                .skill_upload_owners
                .lock()
                .await
                .insert(upload_id.clone(), foreign_owner);
        }
        for (suffix, method, params) in [
            (
                "preview",
                methods::PLUGINS_PREVIEW,
                json!({"workspace_id":harness.workspace_id,"upload_id":upload_id}),
            ),
            (
                "update-preview",
                methods::PLUGINS_PREVIEW,
                json!({"workspace_id":harness.workspace_id,"upload_id":upload_id,"target":{"plugin_id":parent,"expected_revision":1}}),
            ),
            (
                "update",
                methods::PLUGINS_UPDATE,
                json!({"workspace_id":harness.workspace_id,"plugin_id":parent,"expected_revision":1,"upload_id":upload_id,"expected_fingerprint":fingerprint,"confirm_changes":true}),
            ),
        ] {
            let id = generate_test_request_id(&format!("transfer-{foreign}"), suffix);
            let request = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
            harness
                .processor
                .process_request_for_connection(harness.connection_id, &request.to_string())
                .await;
            let _rejected = recv_error_by_id(&mut harness.rx, &id).await;
            let current = harness
                .crud_store
                .find_plugin_installation(&parent)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(current.revision, 1);
            assert!(current.pending_json.is_none());
            assert_eq!(
                harness
                    .crud_store
                    .find_skill_upload_session(&upload_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "finalized"
            );
        }
    }
    // Alternate Skills entry must not read/abort a foreign malformed Plugin
    // transfer even when its persisted numeric owner matches this connection.
    let bad_id = "B".repeat(21);
    let bad_payload = root.join("uploads").join(&bad_id).join("payload.tar.gz");
    std::fs::create_dir_all(bad_payload.parent().unwrap()).unwrap();
    std::fs::write(&bad_payload, b"finalized but not gzip").unwrap();
    let mut bad = harness
        .crud_store
        .find_skill_upload_session(&upload_id)
        .await
        .unwrap()
        .unwrap();
    bad.upload_id = bad_id.clone();
    bad.payload_path = bad_payload.to_string_lossy().into();
    bad.sha256 = hex::encode(Sha256::digest(b"finalized but not gzip"));
    bad.compressed_size_bytes = b"finalized but not gzip".len() as u64;
    bad.received_bytes = bad.compressed_size_bytes;
    harness
        .crud_store
        .insert_skill_upload_session(&bad)
        .await
        .unwrap();
    let request_id =
        RequestId::new(generate_test_request_id("transfer", "alternate-entry")).unwrap();
    assert!(
        harness
            .processor
            .materialize_uploaded_skill_source(&owner, &harness.workspace_id, &bad_id, &request_id)
            .await
            .is_err()
    );
    assert_eq!(
        harness
            .crud_store
            .find_skill_upload_session(&bad_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "finalized"
    );
    assert_eq!(
        std::fs::read(&bad_payload).unwrap(),
        b"finalized but not gzip"
    );
    // Matching owner with wrong purpose is also rejected before extraction.
    harness
        .processor
        .skill_upload_owners
        .lock()
        .await
        .insert(bad_id.clone(), owner.clone());
    assert!(
        harness
            .processor
            .materialize_uploaded_skill_pack_source(
                &owner,
                &harness.workspace_id,
                &bad_id,
                &request_id
            )
            .await
            .is_err()
    );
    assert_eq!(
        harness
            .crud_store
            .find_skill_upload_session(&bad_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "finalized"
    );
    // Genuine owner + matching Plugin purpose reaches extraction and may abort
    // its own invalid bytes; no competing transfer is touched.
    assert!(
        harness
            .processor
            .materialize_uploaded_archive_source(
                &owner,
                &harness.workspace_id,
                &bad_id,
                &runtime,
                &request_id,
                "plugin"
            )
            .await
            .is_err()
    );
    assert_eq!(
        harness
            .crud_store
            .find_skill_upload_session(&bad_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "aborted"
    );
    assert!(!bad_payload.parent().unwrap().exists());
    assert_eq!(std::fs::read(&payload).unwrap(), archive);
    assert_eq!(
        std::fs::read(package.join("plugin.json")).unwrap(),
        manifest
    );
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
    let initial = harness
        .processor
        .normalize_turn_skill_capabilities(&harness.workspace_id, &[])
        .await
        .unwrap();
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
        let stored = harness
            .crud_store
            .find_plugin_installation(&parent)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.enabled, enabled);
        assert_eq!(stored.allow_implicit_invocation, revision != 4);
        let automatic = harness
            .processor
            .normalize_turn_skill_capabilities(&harness.workspace_id, &[])
            .await
            .unwrap();
        if enabled && revision != 4 {
            assert!(automatic.execution.iter().any(|cap| matches!(&cap.kind,
                TurnCapabilityKind::Skill { skill_id, .. } if skill_id == &skill)));
            assert_eq!(
                automatic.plugin_selection.unwrap().parents[0].revision,
                revision + 1
            );
        } else {
            assert!(automatic.execution.is_empty());
            assert!(automatic.plugin_selection.is_none());
        }
        // Opting into a plugin cannot widen an already persisted task ceiling.
        let restored = harness
            .processor
            .normalize_task_launch_capabilities(&harness.workspace_id, &frozen)
            .await
            .unwrap();
        assert!(restored.execution.is_empty());
        assert!(restored.plugin_selection.is_none());
    }
    // Turning implicit off keeps explicit selection available.
    let mut current = selected;
    if let TurnCapabilityKind::Plugin {
        expected_revision, ..
    } = &mut current.kind
    {
        *expected_revision = 5;
    }
    let explicit = harness
        .processor
        .normalize_turn_skill_capabilities(&harness.workspace_id, &[current])
        .await
        .unwrap();
    assert!(explicit.execution.iter().any(|cap| matches!(&cap.kind,
        TurnCapabilityKind::Skill { skill_id, .. } if skill_id == &skill)));
    std::fs::remove_dir_all(root).unwrap();
}

// NOT_RUN / NOT_COMPILED. Native catalog and owner admission, no provider process.
#[test]
fn app_selection_preserves_both_parent_domains_and_blocks_owned_leaf_bypass() {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async{
        let harness=setup_cli_runtime_security_harness(None).await;
        let root=unique_temp_dir("app-selection-owner");
        let (app,skill,selected)=seed_owned_c2_package(&harness.crud_store,&harness.workspace_id,&root.join("app"),pioneer_protocol::PackageKind::App).await;
        let (plugin,_,plugin_selected)=seed_owned_c2_skill(&harness.crud_store,&harness.workspace_id,&root.join("plugin")).await;
        let result=harness.processor.normalize_turn_skill_capabilities(&harness.workspace_id,&[selected.clone(),plugin_selected.clone()]).await.unwrap();
        assert_eq!(result.presentation.len(),2);
        assert_eq!(result.presentation[0].label.as_deref(),Some("Authoritative App"));
        assert!(matches!(&result.presentation[0].kind,TurnCapabilityKind::App{app_id,expected_revision:1} if app_id==&app));
        let snapshot=result.plugin_selection.unwrap();assert_eq!(snapshot.parents.len(),2);
        assert_eq!(snapshot.parents[0].kind,pioneer_protocol::PackageKind::App);
        assert_eq!(snapshot.parents[1].kind,pioneer_protocol::PackageKind::Plugin);
        assert!(snapshot.children.iter().any(|c|c.parent_id==app&&c.id==skill.as_str()));
        let raw=TurnCapability{id:pioneer_protocol::skill_capability_key(&skill),kind:TurnCapabilityKind::Skill{skill_id:skill,pack_id:None},label:None};
        assert!(harness.processor.normalize_turn_skill_capabilities(&harness.workspace_id,&[raw]).await.is_err());
        assert!(harness.processor.normalize_turn_skill_capabilities(&harness.workspace_id,&[pioneer_protocol::package_capability(pioneer_protocol::PackageKind::Plugin,&app,1)]).await.is_err());
        assert!(harness.processor.normalize_turn_skill_capabilities("foreign-workspace",&[selected.clone()]).await.is_err());
        let guards=super::super::plugins::acquire_plugin_selection_admission(&harness.crud_store,&harness.processor.plugin_mutation_locks,&harness.workspace_id,&snapshot).await.unwrap();drop(guards);
        harness.crud_store.begin_plugin_mutation(&harness.workspace_id,&app,1,"updating",false,"{\"kind\":\"enable\"}").await.unwrap();
        assert!(harness.processor.normalize_turn_skill_capabilities(&harness.workspace_id,&[selected]).await.is_err());
        assert!(harness.processor.normalize_turn_skill_capabilities(&harness.workspace_id,&[plugin_selected]).await.is_ok());
        assert!(harness.crud_store.find_package_installation(&plugin).await.unwrap().unwrap().enabled);
        std::fs::remove_dir_all(root).unwrap();
    });
}

// NOT_RUN / NOT_COMPILED. Real authenticated transfer and App facade, no providers.
async fn app_rpc(
    harness: &mut CliRuntimeSecurityHarness,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let id = pioneer_protocol::generate_id(21);
    harness
        .processor
        .process_request_for_connection(
            harness.connection_id,
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string(),
        )
        .await;
    recv_response_by_id(&mut harness.rx, &id).await.result
}

async fn install_rpc_app(
    harness: &mut CliRuntimeSecurityHarness,
    root: &std::path::Path,
    slug: &str,
    decorated_paths: bool,
) -> pioneer_protocol::AppItem {
    let package = root.join(slug);
    std::fs::create_dir_all(package.join("assets")).unwrap();
    std::fs::create_dir_all(package.join("skills").join(slug)).unwrap();
    let mut revisions = Vec::new();
    for (name, size) in [("light", 1), ("dark", 2)] {
        let mut png = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgba8(size, size)
            .write_to(&mut png, image::ImageFormat::Png)
            .unwrap();
        let bytes = png.into_inner();
        revisions.push(hex::encode(Sha256::digest(&bytes)));
        std::fs::write(package.join(format!("assets/{name}.png")), bytes).unwrap();
    }
    let path = |name| {
        if decorated_paths {
            format!("./assets/{name}.png/")
        } else {
            format!("assets/{name}.png")
        }
    };
    let icons = json!({"light":path("light"),"dark":path("dark")});
    std::fs::write(
        package.join("app.json"),
        json!({
            "slug":slug,"name":format!("App {slug}"),"version":"1.2.3",
            "shortDescription":"Saved description","developer":{"name":"Author","logo":icons},
            "icon":icons,"skills":"./skills/","locales":{"en":"en.json"},"defaultLocale":"en"
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        package.join("en.json"),
        r#"{"name":"Localized App","shortDescription":"Localized description"}"#,
    )
    .unwrap();
    std::fs::write(
        package.join("skills").join(slug).join("SKILL.md"),
        format!(
            "---\nname: {slug}\ndescription: App regression skill\n---\nUse this installed skill.\n"
        ),
    )
    .unwrap();
    let upload = create_finalized_archive_upload_with_purpose(
        &harness.processor,
        &mut harness.rx,
        harness.connection_id,
        &harness.workspace_id,
        build_test_directory_archive(&package),
        format!("{slug}.tar.gz"),
        slug,
        pioneer_protocol::SkillUploadPurpose::App,
    )
    .await;
    assert_eq!(
        harness
            .crud_store
            .find_skill_upload_session(&upload)
            .await
            .unwrap()
            .unwrap()
            .purpose,
        "app"
    );
    let workspace = harness.workspace_id.clone();
    let preview: pioneer_protocol::AppsPreviewResponse = serde_json::from_value(
        app_rpc(
            harness,
            methods::APPS_PREVIEW,
            json!({"workspace_id":workspace,"upload_id":upload}),
        )
        .await,
    )
    .unwrap();
    assert_eq!(preview.components.len(), 1);
    assert!(preview.diagnostics.is_empty());
    let installed_wire = app_rpc(
        harness,
        methods::APPS_INSTALL,
        json!({
            "workspace_id":workspace,"upload_id":upload,"expected_fingerprint":preview.fingerprint
        }),
    )
    .await;
    assert!(installed_wire.get("slug").is_some());
    assert!(installed_wire.get("plugin").is_none());
    let installed: pioneer_protocol::AppItem = serde_json::from_value(installed_wire).unwrap();
    assert_eq!(installed.slug, slug);
    assert_eq!(installed.name, "Localized App");
    assert_eq!(
        installed.description.as_deref(),
        Some("Localized description")
    );
    assert_eq!(installed.version.as_deref(), Some("1.2.3"));
    assert_eq!(installed.state, "installed");
    assert_eq!(installed.status, "installed");
    assert_eq!(installed.revision, 1);
    let artwork = installed.metadata.app.as_ref().unwrap();
    let icon = artwork
        .icon
        .as_ref()
        .expect("both validated themes must reach presentation");
    assert_eq!(
        icon.light,
        pioneer_protocol::ImageReference::app_icon(&workspace, &installed.id, &revisions[0])
    );
    assert_eq!(
        icon.dark,
        pioneer_protocol::ImageReference::app_icon(&workspace, &installed.id, &revisions[1])
    );
    assert_eq!(artwork.composer_icon.as_ref(), Some(icon));
    assert_eq!(artwork.developer_logo.as_ref(), Some(icon));
    assert_eq!(
        artwork.localizations["en"].name.as_deref(),
        Some("Localized App")
    );
    let stored = harness
        .crud_store
        .find_package_installation(&installed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.kind, pioneer_protocol::PackageKind::App);
    assert_eq!(stored.name, slug);
    let manifest: serde_json::Value =
        serde_json::from_str(stored.app_manifest_json.as_deref().unwrap()).unwrap();
    assert_eq!(manifest["name"], format!("App {slug}"));
    assert_eq!(stored.package_fingerprint, preview.fingerprint);
    let skill = installed.components[0].skill_id.as_ref().unwrap();
    let owner = harness
        .crud_store
        .native_package_owner("skill", skill.as_str())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owner.kind, pioneer_protocol::PackageKind::App);
    assert_eq!(owner.package_id, installed.id);
    let selected = pioneer_protocol::package_capability(
        pioneer_protocol::PackageKind::App,
        &installed.id,
        installed.revision,
    );
    let normalized = harness
        .processor
        .normalize_turn_skill_capabilities(&workspace, &[selected])
        .await
        .unwrap();
    assert_eq!(normalized.presentation.len(), 1);
    assert!(
        matches!(&normalized.presentation[0].kind, TurnCapabilityKind::App {app_id,expected_revision:1} if app_id==&installed.id)
    );
    assert!(
        normalized.execution.iter().any(
            |cap| matches!(&cap.kind,TurnCapabilityKind::Skill{skill_id,..} if skill_id==skill)
        )
    );
    let frozen = normalized.plugin_selection.unwrap();
    assert_eq!(frozen.parents[0].kind, pioneer_protocol::PackageKind::App);
    assert_eq!(frozen.parents[0].revision, 1);
    assert!(
        frozen
            .children
            .iter()
            .any(|child| child.parent_id == installed.id && child.id == skill.as_str())
    );
    installed
}

#[tokio::test]
async fn app_rpc_install_read_selection_and_damaged_inventory_removal() {
    let mut harness = setup_cli_runtime_security_harness(None).await;
    let root = unique_temp_dir("app-rpc-correction");
    // One scenario owns the real install contract and path→themed presentation
    // regression. No direct insertion of parent rows or fabricated fingerprints.
    let a = install_rpc_app(&mut harness, &root, "rpc-app-a", false).await;
    let b = install_rpc_app(&mut harness, &root, "rpc-app-b", true).await;
    let workspace = harness.workspace_id.clone();
    let list: pioneer_protocol::AppsListResponse = serde_json::from_value(
        app_rpc(
            &mut harness,
            methods::APPS_LIST,
            json!({"workspace_id":workspace}),
        )
        .await,
    )
    .unwrap();
    assert_eq!(list.apps.len(), 2);
    let details: pioneer_protocol::AppItem = serde_json::from_value(
        app_rpc(
            &mut harness,
            methods::APPS_DETAILS,
            json!({"workspace_id":workspace,"app_id":a.id}),
        )
        .await,
    )
    .unwrap();
    assert_eq!(details, a);
    let stored = harness
        .crud_store
        .find_package_installation(&a.id)
        .await
        .unwrap()
        .unwrap();
    for missing in [false, true] {
        if missing {
            std::fs::remove_dir_all(&stored.package_path).unwrap();
        } else {
            // Keep the manifest and both raster files valid, but change the
            // selected tree digest: this must hit fingerprint verification.
            std::fs::copy(
                std::path::Path::new(&stored.package_path).join("assets/dark.png"),
                std::path::Path::new(&stored.package_path).join("assets/light.png"),
            )
            .unwrap();
        }
        let list: pioneer_protocol::AppsListResponse = serde_json::from_value(
            app_rpc(
                &mut harness,
                methods::APPS_LIST,
                json!({"workspace_id":workspace}),
            )
            .await,
        )
        .unwrap();
        assert_eq!(list.apps.len(), 2);
        assert_eq!(list.apps.iter().find(|item| item.id == b.id).unwrap(), &b);
        let damaged = list.apps.iter().find(|item| item.id == a.id).unwrap();
        assert_eq!(damaged.slug, a.slug);
        assert_eq!(damaged.name, a.name);
        assert_eq!(damaged.version, a.version);
        assert_eq!(damaged.description, a.description);
        assert_eq!(damaged.revision, a.revision);
        assert_eq!(damaged.components, a.components);
        assert_eq!(damaged.status, "partial");
        let artwork = damaged.metadata.app.as_ref().unwrap();
        assert_eq!(
            artwork.localizations,
            a.metadata.app.as_ref().unwrap().localizations
        );
        assert!(
            artwork.icon.is_none()
                && artwork.composer_icon.is_none()
                && artwork.developer_logo.is_none()
        );
        assert!(damaged.metadata.logo.is_none() && damaged.metadata.composer_icon.is_none());
        let diagnostic = damaged
            .diagnostics
            .iter()
            .find(|d| d.code == "app_package_unavailable")
            .unwrap();
        assert!(diagnostic.path.is_empty());
        assert_eq!(
            diagnostic.message,
            "Installed App files could not be verified"
        );
        assert!(
            !serde_json::to_string(damaged)
                .unwrap()
                .contains(&stored.package_path)
        );
        let details: pioneer_protocol::AppItem = serde_json::from_value(
            app_rpc(
                &mut harness,
                methods::APPS_DETAILS,
                json!({"workspace_id":workspace,"app_id":a.id}),
            )
            .await,
        )
        .unwrap();
        assert_eq!(&details, damaged);
    }
    let removed: pioneer_protocol::AppsMutationResponse = serde_json::from_value(app_rpc(&mut harness,
        methods::APPS_REMOVE,json!({"workspace_id":workspace,"app_id":a.id,"expected_revision":a.revision,"purge_data":true})).await).unwrap();
    assert!(removed.removed && removed.app.is_none());
    assert!(
        harness
            .crud_store
            .find_package_installation(&a.id)
            .await
            .unwrap()
            .is_none()
    );
    let list: pioneer_protocol::AppsListResponse = serde_json::from_value(
        app_rpc(
            &mut harness,
            methods::APPS_LIST,
            json!({"workspace_id":workspace}),
        )
        .await,
    )
    .unwrap();
    assert_eq!(list.apps, vec![b]);
    std::fs::remove_dir_all(root).unwrap();
}
