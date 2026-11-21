use pioneer_crud_packages::{
    McpAuditEventRecord, McpServerInstallationRecord, PackageKind, PackageRecord,
    plugins::PluginOwnershipWrite,
    skill_audit_event::SkillAuditEventRecord,
    skill_records::{SkillInstallationRecord, WorkspaceSkillPolicyRecord},
};

pub fn parent(id: &str, workspace: &str) -> PackageRecord {
    PackageRecord {
        kind: PackageKind::Plugin,
        app_manifest_json: None,
        app_localizations_json: None,
        operation_error_json: None,
        id: id.into(),
        workspace_id: workspace.into(),
        name: "same-name".into(),
        version: None,
        source_upload_id: format!("upload-{id}"),
        package_path: format!("/managed/plugins/{id}/package"),
        data_path: format!("/managed/plugins/{id}/data"),
        package_fingerprint: "tree".into(),
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
        created_at: chrono::DateTime::from_timestamp(1, 0)
            .unwrap()
            .fixed_offset(),
        updated_at: chrono::DateTime::from_timestamp(1, 0)
            .unwrap()
            .fixed_offset(),
    }
}
pub fn ownership(parent: &str, child: &str, key: &str) -> PluginOwnershipWrite {
    PluginOwnershipWrite {
        kind: pioneer_protocol::PackageKind::Plugin,
        plugin_id: parent.into(),
        expected_revision: 1,
        member_key: key.into(),
        member_path: Some(format!("skills/{key}")),
        package_fingerprint: "member-tree".into(),
        child_id: child.into(),
    }
}
pub fn skill(id: char, workspace: &str) -> SkillInstallationRecord {
    SkillInstallationRecord {
        skill_id: pioneer_protocol::SkillId::new(id.to_string().repeat(21)).unwrap(),
        owner: None,
        slug: "same-slug-skill".into(),
        version: None,
        source_kind: "user".into(),
        scope_key: workspace.into(),
        source_ref: "upload:same-slug".into(),
        install_path: format!("/tmp/skills/{id}/same-slug"),
        trust_level: "community".into(),
        fingerprint: "fingerprint-same-slug".into(),
        updated_at_unix: 1_700_000_000,
        pack_id: None,
        pack_member_key: None,
    }
}
pub fn policy(row: &SkillInstallationRecord) -> WorkspaceSkillPolicyRecord {
    WorkspaceSkillPolicyRecord {
        id: format!("policy-{}", row.skill_id),
        workspace_id: row.scope_key.clone(),
        skill_id: row.skill_id.clone(),
        enabled: Some(false),
        allow_implicit_invocation: Some(false),
    }
}
pub fn skill_audit(row: &SkillInstallationRecord) -> SkillAuditEventRecord {
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
pub fn mcp(id: &str, workspace: &str, name: &str) -> McpServerInstallationRecord {
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
pub fn mcp_audit(row: &McpServerInstallationRecord) -> McpAuditEventRecord {
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
