use super::*;
use pioneer_client::{
    catalog_test_support::{client, mcp, skill},
    core::{ClientDemand, ClientGeneration, ClientIntent},
    navigation::{NavigationIntent, SemanticDestination},
};
fn runtime(core: Arc<ClientCore>) -> ClientFfiRuntime {
    ClientFfiRuntime {
        core,
        config: Default::default(),
        client_subscriptions: Default::default(),
        observed_scopes: Default::default(),

        diagnostics: Default::default(),
        avatar_cache: Default::default(),
    }
}
#[test]
fn catalog_direct_and_versioned_ffi_replay_preserve_routes_scopes_equality_and_release() {
    for platform in ["ios", "android"] {
        let direct = client();
        let wire = client();
        let runtime = runtime(wire.clone());
        runtime
            .initialize(&format!(r#"{{"platform":"{platform}"}}"#))
            .unwrap();
        for core in [&direct, &wire] {
            core.accept_mcp_catalog_for_test("workspace", mcp(&["a", "b"]));
            core.accept_skills_catalog_for_test(
                "workspace",
                pioneer_client::skills::catalog::project_skills_snapshot(
                    vec![skill('A'), skill('B')],
                    vec![],
                ),
            );
        }
        for intent in [
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("workspace".into()),
            },
            NavigationIntent::Navigate {
                destination: SemanticDestination::Mcp {
                    server_id: Some("a".into()),
                },
            },
            NavigationIntent::Navigate {
                destination: SemanticDestination::Mcp { server_id: None },
            },
            NavigationIntent::Navigate {
                destination: SemanticDestination::Skills {
                    skill_id: Some(skill('A').skill_id),
                },
            },
            NavigationIntent::Navigate {
                destination: SemanticDestination::Threads,
            },
        ] {
            let intent = ClientIntent::Navigation {
                intent,
                expected_revision: None,
            };
            let request = client_binding::ClientIntentDispatchDto {
                schema_version: 1,
                intent: intent.clone(),
            };
            assert_eq!(
                runtime
                    .client_intent_dispatch(&serde_json::to_string(&request).unwrap())
                    .unwrap(),
                client_binding::transition_dto(direct.dispatch(intent))
            );
        }
        let scopes = [
            ClientScope::Mcp {
                workspace_id: Some("workspace".into()),
            },
            ClientScope::Skills {
                workspace_id: Some("workspace".into()),
            },
            ClientScope::Navigation,
        ];
        for scope in &scopes {
            let request =
                serde_json::json!({"schema_version":1,"scope":scope,"after_revision":null});
            assert_eq!(
                runtime
                    .client_scoped_snapshot(&request.to_string())
                    .unwrap()
                    .unwrap(),
                client_binding::snapshot_dto(direct.snapshot(scope).unwrap())
            );
        }
        for core in [&direct, &wire] {
            core.accept_mcp_catalog_for_test("workspace", mcp(&["a", "b"]));
        }
        let before = wire.mcp_catalog_snapshot("workspace").unwrap();
        assert_eq!(*before, *direct.mcp_catalog_snapshot("workspace").unwrap());
        let mut update = mcp(&["b", "a"]);
        update.servers[1].status = pioneer_protocol::McpServerStatus::Restarting;
        let stable_b = before.servers()[1].clone();
        let skills = wire.skills_catalog_snapshot("workspace").unwrap();
        for core in [&direct, &wire] {
            core.accept_mcp_catalog_for_test("workspace", update.clone());
        }
        assert!(Arc::ptr_eq(
            &stable_b,
            &wire.mcp_catalog_snapshot("workspace").unwrap().servers()[0]
        ));
        assert!(Arc::ptr_eq(
            &skills,
            &wire.skills_catalog_snapshot("workspace").unwrap()
        ));
        let scope = scopes[0].clone();
        for (generation, demand) in [
            (1, ClientDemand::Visible),
            (2, ClientDemand::Suspended),
            (3, ClientDemand::Visible),
            (4, ClientDemand::Suspended),
        ] {
            let intent = ClientIntent::SetScopeDemand {
                scope: scope.clone(),
                demand,
                generation: ClientGeneration::new(generation),
            };
            let request = client_binding::ClientIntentDispatchDto {
                schema_version: 1,
                intent: intent.clone(),
            };
            assert_eq!(
                runtime
                    .client_intent_dispatch(&serde_json::to_string(&request).unwrap())
                    .unwrap(),
                client_binding::transition_dto(direct.dispatch(intent))
            );
        }
        assert!(runtime.client_intent_dispatch(r#"{"schema_version":2,"intent":{"kind":"navigation","intent":{"kind":"reset"},"expected_revision":null}}"#).is_err());
    }
}
