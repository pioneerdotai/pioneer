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

/// A current principal for direct/native-boundary profile replay; no transport.
pub fn settings_client() -> Arc<ClientCore> {
    let core = client();
    let device_id = DeviceId::new("DAAAAAAAAAAAAAAAAAAAA").unwrap();
    let (generation, connection) = core.current_auth_ticket();
    core.finish_current_auth(
        generation,
        connection,
        AuthMeResponse {
            gateway: AuthGatewaySnapshot {
                id: GatewayId::new("GAAAAAAAAAAAAAAAAAAAA").unwrap(),
            },
            principal: AuthPrincipalSnapshot {
                id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                kind: PrincipalKind::Superuser,
                display_name: "Synthetic User".into(),
                nickname: "synthetic".into(),
                avatar_revision: None,
            },
            device: AuthDeviceSnapshot {
                id: device_id.clone(),
                installation_id: "synthetic".into(),
                display_name: "Test device".into(),
                client_kind: ClientKind::Mobile,
                status: DeviceStatus::Active,
            },
            session: AuthSessionSnapshot {
                id: AuthSessionId::new("SAAAAAAAAAAAAAAAAAAAA").unwrap(),
                device_id,
                token_family_id: TokenFamilyId::new("FAAAAAAAAAAAAAAAAAAAA").unwrap(),
                status: AuthSessionStatus::Active,
                refresh_generation: 1,
                refresh_expires_at_unix: 1000,
            },
            role_key: None,
        },
    )
    .unwrap();
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

/// Settings dialog fixture with a selected workspace and server-settings permission.
pub fn settings_model_picker_client() -> Arc<ClientCore> {
    let core = settings_client();
    let mut capabilities = core.authorization_snapshot(None, None).unwrap();
    capabilities.authorization_revision += 1;
    capabilities.global.can_manage_gateway_settings = true;
    let (generation, connection) = core.current_auth_ticket();
    core.accept_authorization_projection(generation, connection, capabilities);
    core.navigate(
        crate::navigation::NavigationIntent::SelectWorkspace {
            workspace_id: Some("workspace".into()),
        },
        None,
    );
    core
}

/// A synthetic one-time reply for administration and rendered-dialog regression tests.
pub fn invitation_response() -> InvitationCreateResponse {
    let uri = format!(
        "pioneer://invite?gateway_base_url=https%3A%2F%2Fgateway.example.test%2F&gateway_id=G00000000000000000001#token=pinv1_{}",
        "A".repeat(43),
    );
    InvitationCreateResponse {
        invitation: InvitationSummary {
            invitation_id: InvitationId::new("IAAAAAAAAAAAAAAAAAAAA").unwrap(),
            role_key: RoleKey::new("admin").unwrap(),
            status: InvitationStatus::Pending,
            revoke_reason: None,
            inviter: InvitationInviterSummary {
                principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                kind: PrincipalKind::Superuser,
                display_name: "Synthetic".into(),
                nickname: "synthetic".into(),
            },
            workspaces: vec![InvitationWorkspaceSummary {
                workspace_id: WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA").unwrap(),
                name: "Synthetic".into(),
            }],
            created_at_unix: 1,
            expires_at_unix: 100,
            terminal_at_unix: None,
        },
        presentation: InvitationPresentation::parse(&uri).unwrap(),
    }
}

/// Replay the Gateway's invitation selector/list notifications before the typed
/// create response, through the real operation queue and JSON-RPC decoder.
/// This fixture never constructs a connection or sends network traffic.
pub fn replay_invitation_creation(core: &Arc<ClientCore>) -> Arc<std::sync::atomic::AtomicUsize> {
    use crate::rpc::{JsonRpcRequestTransport, JsonRpcResponseSender};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Transport {
        core: std::sync::Weak<ClientCore>,
        calls: Arc<AtomicUsize>,
    }
    impl JsonRpcRequestTransport for Transport {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            reply: JsonRpcResponseSender,
        ) -> Result<(), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let request: JsonRpcRequest = serde_json::from_str(&payload).unwrap();
            assert_eq!(request.method, constants::methods::INVITE_CREATE);
            let response = invitation_response();
            let core = self.core.upgrade().unwrap();
            let mut capabilities = core.authorization_snapshot(None, None).unwrap();
            capabilities.authorization_revision += 1;
            core.observe_policy_change(&AuthorizationProjectionChangedNotification {
                policy_generation: PolicyGeneration::new(capabilities.authorization_revision)
                    .unwrap(),
                change: AuthorizationChangeKind::ResourceSelector,
                affected: AuthorizationChangeScope::Invitation {
                    invitation_id: response.invitation.invitation_id.clone(),
                },
            });
            core.observe_administration_notification(&GatewayNotification::InvitationChanged(
                InvitationChangedNotification {
                    revision: capabilities.authorization_revision,
                    invitation_id: response.invitation.invitation_id.clone(),
                },
            ));
            let (generation, connection) = core.current_auth_ticket();
            core.accept_authorization_projection(generation, connection, capabilities);
            let wire = JsonRpcResponse::from_result(request.id, &response).unwrap();
            let (_, result) =
                crate::rpc::decode_json_rpc_response_value(&serde_json::to_value(wire).unwrap())
                    .unwrap();
            reply
                .send(result)
                .map_err(|_| "synthetic receiver closed".into())
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let transport = Transport {
        core: Arc::downgrade(core),
        calls: calls.clone(),
    };
    core.start_administration_operation_controller_with_executor(move |operation, client| {
        operation.execute_with_invitation_rpc(client, |params| {
            crate::transport::ws::command_sender::invitation_create(&transport, params)
        })
    });
    calls
}

/// Runs the real account queue/reducers with fake profile and device responses.
pub fn replay_account_requests(core: &ClientCore) -> (usize, usize) {
    core.replay_account_requests()
}

/// The post-save notifications published by Gateway for a changed own profile.
pub fn revalidate_saved_profile(core: &ClientCore) {
    let mut capabilities = core.authorization_snapshot(None, None).unwrap();
    capabilities.authorization_revision += 1;
    let principal_id = core.current_auth().unwrap().principal.id;
    core.observe_policy_change(&AuthorizationProjectionChangedNotification {
        policy_generation: PolicyGeneration::new(capabilities.authorization_revision).unwrap(),
        change: AuthorizationChangeKind::RoleAssignment,
        affected: AuthorizationChangeScope::Principal {
            principal_id: principal_id.clone(),
        },
    });
    core.observe_administration_notification(&GatewayNotification::MemberChanged(
        MemberChangedNotification {
            revision: capabilities.authorization_revision,
            principal_id,
        },
    ));
    let (request, connection) = core.current_auth_ticket();
    core.accept_authorization_projection(request, connection, capabilities);
}
