use super::*;
use pioneer_client::{
    core::{ClientCore, ClientIntent, ClientScope},
    gateway::{
        invitation_controller::InvitationIntent, onboarding_effects::OnboardingEnvironment,
        onboarding_runtime::OnboardingIntent, setup_controller::GatewaySetupIntent,
    },
};

#[test]
fn independent_direct_and_ffi_onboarding_replay_preserves_scope_and_single_submit() {
    for platform in ["ios", "android"] {
        let direct = Arc::new(ClientCore::new());
        let wire = Arc::new(ClientCore::new());
        let runtime = ClientFfiRuntime {
            active_thread: ClientFfiActiveThreadState::new(wire.clone()),
            client_runtime: ClientRuntimeCompatibility { core: wire.clone() },
            config: Default::default(),
            client_subscriptions: Default::default(),
            active_connection_id: Default::default(),
            legacy_authorization_generation: Default::default(),
            legacy_authorization_change_sequence: Default::default(),
            diagnostics: Default::default(),
            avatar_cache: Default::default(),
        };
        runtime
            .initialize(&format!(r#"{{"platform":"{platform}"}}"#))
            .unwrap();
        let mut registry = pioneer_client::gateway::registry::default_registry(
            &pioneer_client::gateway::registry::GatewayRegistryConfig { local: None },
        );
        registry.installation_id = Some("synthetic-installation".into());
        let environment = OnboardingEnvironment {
            registry,
            binding_journals: vec![],
            discard_unbound_remote_candidates: true,
            default_remote_name: "Remote".into(),
            remote_connect_timeout_min: std::time::Duration::ZERO,
            installation: pioneer_protocol::ClientInstallationDescriptor {
                installation_id: "synthetic-installation".into(),
                display_name: "Synthetic".into(),
                client_kind: pioneer_protocol::ClientKind::Mobile,
                platform: None,
                client_version: None,
            },
            timings: pioneer_client::gateway::timings::GatewayTimings::from_millis(10, 10, 10)
                .unwrap(),
            ws_timings: pioneer_client::gateway::timings::GatewayWsTimings::from_millis(
                10, 10, 10, 10, 20, 0,
            )
            .unwrap(),
            local_provisioned: false,
            local_install_required: false,
            local_update_required: false,
        };
        for core in [&direct, &wire] {
            core.install_onboarding_environment_for_test(environment.clone());
        }
        let owner = direct
            .snapshot(&ClientScope::GatewaySetup)
            .unwrap()
            .typed::<pioneer_client::gateway::setup_controller::GatewaySetupPublication>()
            .unwrap()
            .payload()
            .owner_generation;
        let setup = |intent| OnboardingIntent::Setup { intent };
        let fixture = vec![
            setup(GatewaySetupIntent::EditNameForOwner {
                expected_owner: owner + 1,
                value: "stale".into(),
            }),
            setup(GatewaySetupIntent::EditAddressForOwner {
                expected_owner: owner,
                value: "invalid".into(),
            }),
            setup(GatewaySetupIntent::SubmitForOwner {
                expected_owner: owner,
                local: false,
            }),
            setup(GatewaySetupIntent::EditAddressForOwner {
                expected_owner: owner,
                value: "https://gateway.invalid".into(),
            }),
            setup(GatewaySetupIntent::EditActivationForOwner {
                expected_owner: owner,
                value: pioneer_protocol::AuthSecretString::new("K7M4P9Q2"),
            }),
            setup(GatewaySetupIntent::SubmitForOwner {
                expected_owner: owner,
                local: false,
            }),
            setup(GatewaySetupIntent::SubmitForOwner {
                expected_owner: owner,
                local: false,
            }),
            setup(GatewaySetupIntent::Close {
                expected_owner: owner,
            }),
            OnboardingIntent::Invitation {
                intent: InvitationIntent::Open {
                    uri: pioneer_protocol::AuthSecretString::new("invalid invitation"),
                },
            },
            OnboardingIntent::Invitation {
                intent: InvitationIntent::Cancel,
            },
        ];
        for intent in fixture {
            let intent = ClientIntent::Onboarding { intent };
            let expected = client_binding::transition_dto(direct.dispatch(intent.clone()));
            let request = client_binding::ClientIntentDispatchDto {
                schema_version: 1,
                intent,
            };
            assert_eq!(
                runtime
                    .client_intent_dispatch(&serde_json::to_string(&request).unwrap())
                    .unwrap(),
                expected
            );
            for scope in [
                ClientScope::GatewaySetup,
                ClientScope::GatewayDestinations,
                ClientScope::OnboardingInvitation,
            ] {
                let request =
                    serde_json::json!({"schema_version":1,"scope":scope,"after_revision":null});
                let actual = runtime
                    .client_scoped_snapshot(&request.to_string())
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    actual,
                    client_binding::snapshot_dto(direct.snapshot(&scope).unwrap())
                );
                assert!(!serde_json::to_string(&actual).unwrap().contains("K7M4P9Q2"));
            }
        }
        direct.shutdown();
        wire.shutdown();
    }
}
