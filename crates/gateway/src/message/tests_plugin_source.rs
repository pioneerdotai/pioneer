//! Plugin native source overrides and package context regressions.
use super::*;
use std::path::Path;

#[tokio::test]
async fn owned_source_context_keeps_native_override_and_bundled_siblings_separate() {
    use pioneer_crud::{
        PluginNativeWrite, PluginOwnershipWrite, SkillInstallationRecord,
        WorkspaceSkillPolicyRecord,
    };
    use pioneer_skills::{
        SkillAvailability, SkillRuntimeBudget, SkillTrustLevel, SkillUnavailableReason,
    };
    let (_, store, workspace) = setup_workspace_manager().await;
    let fixture = tempfile::tempdir().unwrap();
    let package = fixture.path().join("package");
    let member = package.join("skills/member");
    let native = fixture.path().join("native/member");
    let uploaded = fixture.path().join("uploaded/member");
    let standalone = fixture.path().join("standalone/member");
    let markdown = |body: &str| {
        format!(
            "---\nname: member\ndescription: Source context regression\nruntime:\n  tools:\n    - tool_slug: helper\n      description: Read helper\n      kind: shell\n      parameters:\n        type: object\n      config:\n        command: scripts/helper\n---\n{body}\n"
        )
    };
    for (path, body, asset) in [
        (&member, "bundled body", "bundled script"),
        (&native, "bundled body", "bundled script"),
        (&uploaded, "uploaded body", "uploaded script"),
        (&standalone, "standalone body", "standalone script"),
    ] {
        std::fs::create_dir_all(path.join("scripts")).unwrap();
        std::fs::write(path.join("SKILL.md"), markdown(body)).unwrap();
        std::fs::write(path.join("scripts/helper"), asset).unwrap();
    }
    let sibling = package.join("skills/sibling");
    std::fs::create_dir_all(&sibling).unwrap();
    std::fs::write(sibling.join("asset"), "sibling asset").unwrap();
    let id = "P".repeat(21);
    let now = chrono::Utc::now().fixed_offset();
    store
        .insert_plugin_installation(&pioneer_entity::plugin_installation::Model {
            id: id.clone(),
            workspace_id: workspace.clone(),
            name: "fixture.tools".into(),
            version: None,
            source_upload_id: "fixture".into(),
            package_path: package.to_string_lossy().into(),
            data_path: fixture.path().join("data").to_string_lossy().into(),
            package_fingerprint: "v1".into(),
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
    let child = SkillInstallationRecord {
        skill_id: pioneer_protocol::SkillId::new("S".repeat(21)).unwrap(),
        owner: None,
        slug: "member".into(),
        version: None,
        source_kind: "user".into(),
        scope_key: workspace.clone(),
        source_ref: "plugin:fixture:member".into(),
        install_path: native.to_string_lossy().into(),
        trust_level: "community".into(),
        fingerprint: "native-v1".into(),
        updated_at_unix: 1,
        pack_id: None,
        pack_member_key: None,
    };
    let policy = WorkspaceSkillPolicyRecord {
        id: "policy".into(),
        workspace_id: workspace.clone(),
        skill_id: child.skill_id.clone(),
        enabled: Some(true),
        allow_implicit_invocation: Some(false),
    };
    store
        .install_skill_lifecycle_with_ownership(
            &child,
            &policy,
            &[fixture_skill_lifecycle_audit(
                &child.skill_id,
                "member",
                "install",
                1,
            )],
            None,
            Some(&PluginOwnershipWrite {
                plugin_id: id.clone(),
                expected_revision: 1,
                member_key: "member".into(),
                member_path: Some("skills/member".into()),
                package_fingerprint: "bundled-v1".into(),
                child_id: child.skill_id.to_string(),
            }),
            1,
        )
        .await
        .unwrap();
    store
        .settle_plugin_installation(&id, 1, "installed", None)
        .await
        .unwrap();
    let mut config = test_tool_loop_config();
    config.skills.system_roots.clear();
    let context =
        super::super::skills::workspace::skills_runtime_context_from_config(&config, &workspace)
            .unwrap();
    let catalog = super::super::skills::workspace::load_skills_catalog_from_store(
        &store, &workspace, &context,
    )
    .await
    .unwrap();
    let ordinary = catalog
        .skills
        .iter()
        .find(|skill| skill.identity.skill_id == child.skill_id)
        .unwrap();
    assert_eq!(
        Path::new(&ordinary.identity.skill_dir),
        member.canonicalize().unwrap()
    );
    assert_eq!(
        std::fs::read_to_string(Path::new(&ordinary.identity.skill_dir).join("../sibling/asset"))
            .unwrap(),
        "sibling asset"
    );
    assert!(ordinary.host_explicit_only);

    // Represent the genuine OwnedUpload commit via its shared native writer:
    // finalized upload, changed native folder/source and override mask atomically.
    let upload = pioneer_crud::SkillUploadSessionRecord {
        purpose: "skill".into(),
        upload_id: "U".repeat(21),
        workspace_id: workspace.clone(),
        connection_id: 7,
        status: "finalized".into(),
        file_name: "member.tar.gz".into(),
        archive_format: "tar_gz".into(),
        compressed_size_bytes: 1,
        received_bytes: 1,
        sha256: "a".repeat(64),
        payload_path: "/fixture/member.tar.gz".into(),
        created_at_unix: 1,
        expires_at_unix: 100,
        finalized_at_unix: Some(2),
        consumed_at_unix: None,
        aborted_at_unix: None,
    };
    store.insert_skill_upload_session(&upload).await.unwrap();
    let gate = store
        .begin_plugin_mutation(
            &workspace,
            &id,
            1,
            "updating",
            true,
            "{\"kind\":\"native\",\"action\":\"update\",\"native_committed\":false}",
        )
        .await
        .unwrap();
    let write = PluginNativeWrite {
        plugin_id: id.clone(),
        expected_revision: gate.revision,
        member_key: "member".into(),
        child_id: child.skill_id.to_string(),
        override_fields_json: "[\"skill_source\"]".into(),
        pending_after: "{\"kind\":\"native\",\"action\":\"update\",\"native_committed\":true}"
            .into(),
    };
    assert!(
        store
            .update_skill_lifecycle_with_plugin_change(
                &child.skill_id,
                &pioneer_crud::SkillInstallationPatch {
                    install_path: Some(uploaded.to_string_lossy().into()),
                    source_ref: Some(format!("upload:{}", upload.upload_id)),
                    fingerprint: Some("uploaded-v1".into()),
                    ..Default::default()
                },
                &[fixture_skill_lifecycle_audit(
                    &child.skill_id,
                    "member",
                    "update",
                    3
                )],
                Some(&upload.upload_id),
                None,
                Some(&write),
                3
            )
            .await
            .unwrap()
    );
    store
        .finish_plugin_mutation(&id, gate.revision, "installed", None)
        .await
        .unwrap();

    for package_version in ["v1", "v2"] {
        if package_version == "v2" {
            let gate = store
                .begin_plugin_mutation(
                    &workspace,
                    &id,
                    gate.revision,
                    "updating",
                    true,
                    "{\"kind\":\"update\",\"children\":[]}",
                )
                .await
                .unwrap();
            std::fs::write(member.join("SKILL.md"), markdown("bundled v2 body")).unwrap();
            std::fs::write(member.join("scripts/helper"), "bundled v2 script").unwrap();
            store
                .publish_plugin_package(
                    &id,
                    gate.revision,
                    "fixture.tools",
                    Some("2.0.0"),
                    "v2",
                    None,
                )
                .await
                .unwrap();
            store
                .finish_plugin_mutation(&id, gate.revision, "installed", None)
                .await
                .unwrap();
        }
        let catalog = super::super::skills::workspace::load_skills_catalog_from_store(
            &store, &workspace, &context,
        )
        .await
        .unwrap();
        let selected = catalog
            .skills
            .iter()
            .find(|skill| skill.identity.skill_id == child.skill_id)
            .unwrap();
        assert!(selected.is_available());
        assert!(selected.host_explicit_only);
        assert_eq!(Path::new(&selected.identity.skill_dir), &uploaded);
        assert_eq!(
            Path::new(&selected.identity.skill_file),
            uploaded.join("SKILL.md")
        );
        assert!(selected.instructions.body.contains("uploaded body"));
        assert_eq!(selected.runtime.trust_level, SkillTrustLevel::Community);
        let active = vec![pioneer_skills::ResolvedSkill {
            skill_id: child.skill_id.clone(),
            slug: "member".into(),
            reason: pioneer_skills::SkillResolvedReason::ExplicitCapability,
            definition: selected.clone(),
        }];
        let plan = pioneer_skills::build_skill_runtime_plan(
            &active,
            SkillRuntimeBudget {
                enable_dynamic_tools: true,
                max_dynamic_tools_per_skill: 4,
                allow_shell_tools: true,
                allow_http_tools: true,
                allow_function_proxy_tools: true,
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Community,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            },
        );
        let read = &plan.read_skill_index[&format!("skill:{}", child.skill_id)];
        assert!(read.body.contains("uploaded body"));
        assert_eq!(
            Path::new(read.source.package_asset_root().unwrap()),
            &uploaded
        );
        assert_eq!(
            std::fs::read_to_string(
                Path::new(read.source.package_asset_root().unwrap()).join("scripts/helper")
            )
            .unwrap(),
            "uploaded script"
        );
        assert_eq!(plan.tools.len(), 1);
        assert_eq!(Path::new(&plan.tools[0].skill_asset_root), &uploaded);
        assert_eq!(
            store
                .find_skill_installation(&child.skill_id)
                .await
                .unwrap()
                .unwrap()
                .install_path,
            uploaded.to_string_lossy()
        );
        let policies = store
            .list_workspace_skill_policies(&workspace)
            .await
            .unwrap();
        let persisted = policies
            .iter()
            .find(|row| row.skill_id == child.skill_id)
            .unwrap();
        assert_eq!(persisted.enabled, Some(true));
        assert_eq!(persisted.allow_implicit_invocation, Some(false));
    }
    // A missing/invalid override follows native unavailable semantics, even
    // while a valid, different bundled copy still exists.
    std::fs::write(uploaded.join("SKILL.md"), "---\nname: [invalid yaml\n---\n").unwrap();
    let catalog = super::super::skills::workspace::load_skills_catalog_from_store(
        &store, &workspace, &context,
    )
    .await
    .unwrap();
    let selected = catalog
        .skills
        .iter()
        .find(|skill| skill.identity.skill_id == child.skill_id)
        .unwrap();
    assert_eq!(
        selected.availability,
        SkillAvailability::Unavailable {
            reason: SkillUnavailableReason::InvalidPackage
        }
    );
    assert_eq!(Path::new(&selected.identity.skill_dir), &uploaded);
    std::fs::remove_file(uploaded.join("SKILL.md")).unwrap();
    let catalog = super::super::skills::workspace::load_skills_catalog_from_store(
        &store, &workspace, &context,
    )
    .await
    .unwrap();
    assert_eq!(
        catalog
            .skills
            .iter()
            .find(|skill| skill.identity.skill_id == child.skill_id)
            .unwrap()
            .availability,
        SkillAvailability::Unavailable {
            reason: SkillUnavailableReason::MissingPackage
        }
    );
    let mut standalone_record = child.clone();
    standalone_record.skill_id = pioneer_protocol::SkillId::new("T".repeat(21)).unwrap();
    standalone_record.slug = "standalone".into();
    std::fs::write(
        standalone.join("SKILL.md"),
        markdown("standalone body").replace("name: member", "name: standalone"),
    )
    .unwrap();
    standalone_record.install_path = standalone.to_string_lossy().into();
    standalone_record.source_ref = "native standalone".into();
    store
        .insert_skill_installation(&standalone_record, 4)
        .await
        .unwrap();
    let catalog = super::super::skills::workspace::load_skills_catalog_from_store(
        &store, &workspace, &context,
    )
    .await
    .unwrap();
    let standalone_skill = catalog
        .skills
        .iter()
        .find(|skill| skill.identity.skill_id == standalone_record.skill_id)
        .unwrap();
    assert_eq!(Path::new(&standalone_skill.identity.skill_dir), &standalone);
    assert!(
        standalone_skill
            .instructions
            .body
            .contains("standalone body")
    );
    assert!(!standalone_skill.host_explicit_only);
    std::fs::write(uploaded.join("SKILL.md"), markdown("uploaded body")).unwrap();
    let current = store.find_plugin_installation(&id).await.unwrap().unwrap();
    let disabled = store
        .begin_plugin_mutation(
            &workspace,
            &id,
            current.revision,
            "updating",
            false,
            "{\"kind\":\"set_enabled\",\"enabled\":false}",
        )
        .await
        .unwrap();
    store
        .finish_plugin_mutation(&id, disabled.revision, "installed", None)
        .await
        .unwrap();
    let catalog = super::super::skills::workspace::load_skills_catalog_from_store(
        &store, &workspace, &context,
    )
    .await
    .unwrap();
    let owned = catalog
        .skills
        .iter()
        .find(|skill| skill.identity.skill_id == child.skill_id)
        .unwrap();
    assert_eq!(Path::new(&owned.identity.skill_dir), &uploaded);
    assert_eq!(
        owned.availability,
        SkillAvailability::Unavailable {
            reason: SkillUnavailableReason::HostPolicy
        }
    );
    assert!(
        catalog
            .skills
            .iter()
            .find(|skill| skill.identity.skill_id == standalone_record.skill_id)
            .unwrap()
            .is_available()
    );
}
