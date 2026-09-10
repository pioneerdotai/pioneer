use super::*;
use pioneer_client::{core::ClientIntent, providers::runtime::ProviderRuntimeIntent};

#[test]
fn provider_runtime_direct_and_versioned_dispatch_preserve_scope_and_cancellation() {
    // No transport/controller workers: this fixture exercises only the real dispatch adapter.
    let wire_core = Arc::new(ClientCore::new());
    let direct = ClientCore::new();
    let runtime = fixture(wire_core.clone());
    runtime.initialize(r#"{"platform":"ios"}"#).unwrap();
    let scope = ClientScope::ProviderRuntime {
        workspace_id: "synthetic".into(),
    };
    for operation in [
        ProviderRuntimeIntent::Observe {
            workspace_id: "synthetic".into(),
        },
        ProviderRuntimeIntent::Observe {
            workspace_id: "synthetic".into(),
        },
        ProviderRuntimeIntent::Release {
            workspace_id: "synthetic".into(),
        },
        ProviderRuntimeIntent::Release {
            workspace_id: "synthetic".into(),
        },
        ProviderRuntimeIntent::Release {
            workspace_id: "synthetic".into(),
        },
    ] {
        let intent = ClientIntent::ProviderRuntime { intent: operation };
        let wire = serde_json::to_string(&client_binding::ClientIntentDispatchDto {
            schema_version: 1,
            intent: intent.clone(),
        })
        .unwrap();
        assert_eq!(
            runtime.client_intent_dispatch(&wire).unwrap(),
            client_binding::transition_dto(direct.dispatch(intent))
        );
        assert_eq!(
            client_binding::snapshot_dto(wire_core.snapshot(&scope).unwrap()),
            client_binding::snapshot_dto(direct.snapshot(&scope).unwrap())
        );
    }
    assert!(runtime.client_intent_dispatch(r#"{"schema_version":2,"intent":{"kind":"provider_runtime","intent":{"kind":"observe","workspace_id":"synthetic"}}}"#).is_err());
}

fn fixture(wire_core: Arc<ClientCore>) -> ClientFfiRuntime {
    ClientFfiRuntime {
        active_thread: ClientFfiActiveThreadState::new(wire_core.clone()),
        client_runtime: ClientRuntimeCompatibility {
            core: wire_core.clone(),
        },
        config: Default::default(),
        client_subscriptions: Default::default(),
        active_connection_id: Default::default(),
        legacy_authorization_generation: Default::default(),
        legacy_authorization_change_sequence: Default::default(),
        diagnostics: Default::default(),
        avatar_cache: Default::default(),
    }
}

#[test]
fn provider_and_administration_intents_replay_identically_through_the_existing_versioned_boundary()
{
    use pioneer_client::administration::types::AuthorizationCapabilitySnapshot;
    let wire_core = Arc::new(ClientCore::new());
    let direct = ClientCore::new();
    let runtime = fixture(wire_core.clone());
    runtime.initialize(r#"{"platform":"android"}"#).unwrap();
    let auth: AuthorizationCapabilitySnapshot = serde_json::from_value(serde_json::json!({
        "schema_version": pioneer_protocol::AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION, "authorization_revision": 1, "principal_id": "PAAAAAAAAAAAAAAAAAAAA",
        "role_key": "admin", "role": { "key": "admin", "display_name": "Synthetic", "description": "", "built_in": false },
        "global": pioneer_protocol::AuthorizationGlobalCapabilities { can_manage_capabilities: true, can_view_member_directory: true, can_view_invitations: true, ..Default::default() },
        "workspace": null, "thread": null
    })).unwrap();
    for core in [&direct, wire_core.as_ref()] {
        assert_eq!(
            core.accept_authorization_projection(0, None, auth.clone()),
            pioneer_client::authorization::AuthorizationProjectionAcceptance::Accepted
        );
    }
    let intents = [
        serde_json::json!({"kind":"provider_collection","intent":{"kind":"observe","key":{"workspace_id":"synthetic","collection":{"kind":"catalog"}}}}),
        serde_json::json!({"kind":"provider_collection","intent":{"kind":"refresh","key":{"workspace_id":"synthetic","collection":{"kind":"catalog"}}}}),
        serde_json::json!({"kind":"provider_command","command":{"kind":"configure","params":{"workspace_id":"synthetic","provider":"openai","api_key":"synthetic-credential","proxy_url":null,"clear_proxy":false}}}),
        serde_json::json!({"kind":"provider_command","command":{"kind":"disconnect","params":{"workspace_id":"synthetic","provider":"openai"}}}),
        serde_json::json!({"kind":"administration_page","intent":{"kind":"observe","page":{"kind":"members"}}}),
        serde_json::json!({"kind":"administration_page","intent":{"kind":"next","page":{"kind":"members"}}}),
        serde_json::json!({"kind":"administration_page","intent":{"kind":"refresh","page":{"kind":"invitations"}}}),
        serde_json::json!({"kind":"administration_presentation","intent":{"kind":"copy_activation","generation":99}}),
    ];
    for value in intents {
        let intent: ClientIntent = serde_json::from_value(value).unwrap();
        let wire = serde_json::to_string(&client_binding::ClientIntentDispatchDto {
            schema_version: 1,
            intent: intent.clone(),
        })
        .unwrap();
        assert_eq!(
            runtime.client_intent_dispatch(&wire).unwrap(),
            client_binding::transition_dto(direct.dispatch(intent))
        );
    }
    // Each accepted command uses the same operation owner, including an explicit retry.
    for value in [
        serde_json::json!({"kind":"provider_command","command":{"kind":"disconnect","params":{"workspace_id":"synthetic","provider":"openai"}}}),
        serde_json::json!({"kind":"provider_command","command":{"kind":"connect","params":{"workspace_id":"synthetic","runtime_id":"codex"}}}),
        serde_json::json!({"kind":"provider_command","command":{"kind":"configure","params":{"workspace_id":"synthetic","provider":"openai","api_key":null,"proxy_url":null,"clear_proxy":true}}}),
        serde_json::json!({"kind":"provider_command","command":{"kind":"configure","params":{"workspace_id":"synthetic","provider":"openai","api_key":null,"proxy_url":null,"clear_proxy":true}}}),
        serde_json::json!({"kind":"navigation","intent":{"kind":"set_providers_route","filter":"Connected"},"expected_revision":null}),
        serde_json::json!({"kind":"navigation","intent":{"kind":"set_administration_route","route":pioneer_client::navigation::AdministrationRoute::Invitations},"expected_revision":null}),
    ] {
        direct.cancel_provider_operations("synthetic");
        wire_core.cancel_provider_operations("synthetic");
        let intent: ClientIntent = serde_json::from_value(value).unwrap();
        let wire = serde_json::to_string(&client_binding::ClientIntentDispatchDto {
            schema_version: 1,
            intent: intent.clone(),
        })
        .unwrap();
        let result = direct.dispatch(intent);
        assert_eq!(
            result.outcome(),
            pioneer_client::core::ClientTransitionOutcome::Changed
        );
        assert_eq!(
            runtime.client_intent_dispatch(&wire).unwrap(),
            client_binding::transition_dto(result)
        );
    }
    for scope in [
        ClientScope::Navigation,
        ClientScope::ProviderCollection {
            key: pioneer_client::providers::store::ProviderCollectionKey::catalog("synthetic"),
        },
        ClientScope::ProviderOperation {
            workspace_id: "synthetic".into(),
        },
        ClientScope::AdministrationPage {
            page: pioneer_client::administration::pages::AdministrationPage::Members,
        },
    ] {
        let wire = client_binding::snapshot_dto(wire_core.snapshot(&scope).unwrap());
        assert_eq!(
            wire,
            client_binding::snapshot_dto(direct.snapshot(&scope).unwrap())
        );
        assert!(
            !serde_json::to_string(&wire)
                .unwrap()
                .contains("synthetic-credential")
        );
    }
}
