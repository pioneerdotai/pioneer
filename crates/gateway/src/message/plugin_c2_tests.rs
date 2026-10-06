//! C2 regression sources only: NOT_RUN / NOT_COMPILED. Existing in-memory
//! Gateway/native-session harnesses; no provider executable is launched.
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
    std::fs::create_dir_all(root.join("scripts")).unwrap();
    std::fs::write(member.join("SKILL.md"), "---\nname: bundled\ndescription: Selected packaged skill\n---\nRead ../../scripts/helper.txt\n").unwrap();
    std::fs::write(root.join("scripts/helper.txt"), "package sibling").unwrap();
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
            enabled: true,
            state: "installing".into(),
            revision: 1,
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
            &[],
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
                let harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
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
                    let launch = exact_cli_task_launch_for_test(
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
                    envelope["params"]["agent_launch"] = serde_json::to_value(launch).unwrap();
                }
                let request = envelope.to_string();
                harness
                    .processor
                    .process_request_for_connection(harness.connection_id, &request)
                    .await;
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
                assert_eq!(bindings[0].skill_id, skill);
                let alias = crate::cli_runtime::skills::plugin_skill_alias(&parent, &skill);
                let context = harness
                    .native_home
                    .join(".pioneer-selected-contexts")
                    .join(&alias);
                assert_eq!(
                    std::fs::read(context.join("scripts/helper.txt")).unwrap(),
                    b"package sibling"
                );
                assert_eq!(
                    std::fs::read(context.join("skills/bundled/SKILL.md")).unwrap(),
                    std::fs::read(root.join("skills/bundled/SKILL.md")).unwrap()
                );
                assert!(
                    !harness.native_home.join("skills/bundled").exists(),
                    "owned children are not provider-autoloaded standalone skills"
                );
                if kind == CLIAgentRuntimeKind::Codex {
                    let items: Vec<pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem> =
                        serde_json::from_value(native.input).unwrap();
                    assert!(items.iter().any(|item| matches!(item, pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Skill { name, path } if name == &alias && path == &context.join("skills/bundled/SKILL.md").to_string_lossy())), "Codex receives its genuine explicit native Skill item");
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
