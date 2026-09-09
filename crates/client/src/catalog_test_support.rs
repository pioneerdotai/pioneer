//! Synthetic catalog transport inputs for library and boundary replay tests.
use crate::core::*;
use pioneer_protocol::*;
use std::sync::Arc;
pub fn client() -> Arc<ClientCore> {
    let core = Arc::new(ClientCore::new());
    core.accept_authorization_projection(
        0,
        None,
        AuthorizationCapabilitySnapshot {
            schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision: 1,
            principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
            role_key: "admin".into(),
            role: AuthorizationRolePresentation {
                key: "admin".into(),
                display_name: "Administrator".into(),
                description: String::new(),
                built_in: false,
            },
            global: AuthorizationGlobalCapabilities {
                can_manage_capabilities: true,
                ..Default::default()
            },
            workspace: None,
            thread: None,
        },
    );
    let scope = ClientScope::Administration { workspace_id: None };
    let p = core.snapshot(&scope).unwrap();
    let mut identity = (*p
        .snapshot()
        .payload::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>()
        .unwrap())
    .clone();
    identity.connection_id = Some(7);
    core.publish(
        &ClientMutationAuthority { _private: () },
        scope,
        crate::threads::registry::revisions(p.revisions().scoped().get() + 1),
        Arc::new(identity),
        vec![],
    );
    core
}
pub fn skill(character: char) -> SkillListItem {
    let id = SkillId::new(character.to_string().repeat(21)).unwrap();
    SkillListItem {
        skill_id: id,
        pack: None,
        owner: None,
        slug: character.to_string(),
        source_kind: "user".into(),
        display_name: character.to_string(),
        description: String::new(),
        version: None,
        fingerprint: format!("{character}:fingerprint"),
        trust_level: "community".into(),
        install: SkillInstallState {
            managed: true,
            installed: true,
            lifecycle_editable: true,
            install_path: None,
            updated_at: None,
        },
        policy: SkillPolicyState {
            enabled: true,
            allow_implicit_invocation: true,
            allow_implicit_invocation_editable: true,
        },
        health: SkillHealthSummary {
            status: "ok".into(),
            dependency_failures: vec![],
            security_blocks: vec![],
            validation_issues: vec![],
        },
        status: "active".into(),
        status_reason: None,
    }
}
pub fn mcp(ids: &[&str]) -> McpListResponse {
    McpListResponse {
        snapshot_version: 1,
        generated_at: 1,
        servers: ids
            .iter()
            .map(|id| McpListItem {
                id: (*id).into(),
                name: (*id).into(),
                display_name: None,
                scope: McpScopeKind::Workspace,
                policy: McpPolicyState {
                    enabled: true,
                    allow_implicit_invocation: false,
                },
                required: false,
                runtime: McpRuntimeStatus {
                    state: McpRuntimeState::Ready,
                    live: true,
                    last_seen_at: None,
                },
                tools_count: 1,
                resources_count: 0,
                resource_templates_count: 0,
                prompts_count: 0,
                status: McpServerStatus::Ready,
            })
            .collect(),
    }
}

pub fn mcp_detail(id: &str) -> McpServerDetailsResponse {
    McpServerDetailsResponse {
        snapshot_version: 1,
        generated_at: 1,
        server: mcp(&[id]).servers.remove(0),
        catalog: McpServerCatalogDetails {
            catalog_version: None,
            generated_at: None,
            server_info: serde_json::Value::Null,
            server_instructions_hash: None,
            tools: vec![],
            resources: vec![],
            resource_templates: vec![],
            prompts: vec![],
        },
        management: None,
    }
}
