use super::*;
use pioneer_client::{
    core::{ClientIntent, ClientScope},
    settings::{
        profile::{ProfileEditorSection, ProfileField, ProfileIntent},
        runtime::SettingsIntent,
    },
};

#[test]
fn independent_direct_and_ffi_profile_replay_preserves_owner_and_page_isolation() {
    for platform in ["ios", "android"] {
        let direct = pioneer_client::catalog_test_support::settings_model_picker_client();
        let wire = pioneer_client::catalog_test_support::settings_model_picker_client();
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
        let picker_scope = ClientScope::SettingsModelPicker {
            picker_id: "settings-dialog".into(),
        };
        let _direct_consumers = [
            picker_scope.clone(),
            ClientScope::Profile,
            ClientScope::AuthSessions,
            ClientScope::Navigation,
        ]
        .into_iter()
        .map(|scope| {
            let subscription =
                direct.subscribe(scope.clone(), std::num::NonZeroUsize::new(64).unwrap());
            runtime.ensure_client_subscription(scope).unwrap();
            subscription
        })
        .collect::<Vec<_>>();
        let replay = |intent: ClientIntent| {
            let expected = client_binding::transition_dto(direct.dispatch(intent.clone()));
            let request = client_binding::ClientIntentDispatchDto {
                schema_version: 1,
                intent,
            };
            assert_eq!(
                runtime
                    .client_intent_dispatch(&serde_json::to_string(&request).unwrap())
                    .unwrap(),
                expected,
                "intent: {:?}",
                request.intent
            );
            for scope in [
                picker_scope.clone(),
                ClientScope::Profile,
                ClientScope::AuthSessions,
                ClientScope::Navigation,
            ] {
                let request =
                    serde_json::json!({"schema_version":1,"scope":scope,"after_revision":null});
                assert_eq!(
                    runtime
                        .client_scoped_snapshot(&request.to_string())
                        .unwrap(),
                    direct
                        .snapshot_if_newer(&scope, None)
                        .map(client_binding::snapshot_dto)
                );
            }
        };
        use pioneer_client::{
            composer::model_selection::ModelSelectorSelection,
            providers::list::ProviderModelSelectorMode,
            settings::model_picker::SettingsModelPickerIntent,
        };
        replay(ClientIntent::SettingsModelPicker {
            intent: SettingsModelPickerIntent::Open {
                picker_id: "settings-dialog".into(),
                workspace_id: "workspace".into(),
                mode: ProviderModelSelectorMode::Embeddings,
                selection: ModelSelectorSelection {
                    provider: None,
                    model: None,
                    selected_reasoning_effort: None,
                },
            },
        });
        let picker_owner = direct
            .settings_model_picker("settings-dialog")
            .unwrap()
            .owner_generation;
        replay(ClientIntent::SettingsModelPicker {
            intent: SettingsModelPickerIntent::SelectModel {
                picker_id: "settings-dialog".into(),
                expected_owner: picker_owner + 1,
                model: "late".into(),
            },
        });
        replay(ClientIntent::SettingsModelPicker {
            intent: SettingsModelPickerIntent::Close {
                picker_id: "settings-dialog".into(),
                expected_owner: picker_owner,
            },
        });
        assert!(
            direct
                .settings_model_picker("settings-dialog")
                .unwrap()
                .closed
        );
        replay(ClientIntent::Profile {
            intent: ProfileIntent::Open {
                section: ProfileEditorSection::Profile,
            },
        });
        let owner = direct.profile().owner_generation;
        let untouched = direct
            .snapshot(&ClientScope::AuthSessions)
            .map(|value| value.snapshot());
        for intent in [
            ProfileIntent::EditField {
                expected_owner: owner,
                field: ProfileField::FirstName,
                value: "Edited".into(),
            },
            ProfileIntent::EditField {
                expected_owner: owner,
                field: ProfileField::LastName,
                value: "Name".into(),
            },
            ProfileIntent::EditField {
                expected_owner: owner,
                field: ProfileField::FirstName,
                value: "Edited".into(),
            },
            ProfileIntent::SaveForOwner {
                expected_owner: owner + 1,
                section: ProfileEditorSection::Profile,
            },
            ProfileIntent::SaveForOwner {
                expected_owner: owner,
                section: ProfileEditorSection::Profile,
            },
            ProfileIntent::SaveForOwner {
                expected_owner: owner,
                section: ProfileEditorSection::Profile,
            },
        ] {
            replay(ClientIntent::Profile { intent });
        }
        assert!(direct.profile().pending);
        assert_eq!(direct.profile().first_name, "Edited");
        assert_eq!(direct.profile().last_name, "Name");
        assert_eq!(
            untouched.is_some(),
            direct.snapshot(&ClientScope::AuthSessions).is_some()
        );
        if let Some(untouched) = untouched {
            assert!(Arc::ptr_eq(
                &untouched,
                &direct
                    .snapshot(&ClientScope::AuthSessions)
                    .unwrap()
                    .snapshot()
            ));
        }
        for core in [&direct, &wire] {
            core.clear_authorization_projections();
        }
        // Desktop performs this existing thread compatibility teardown when it
        // receives access loss; FFI performs it at its next ingress. Replay both
        // producers so the full transition sequence remains comparable.
        direct.clear_thread_stores();
        replay(ClientIntent::Profile {
            intent: ProfileIntent::EditField {
                expected_owner: owner,
                field: ProfileField::FirstName,
                value: "late".into(),
            },
        });
        replay(ClientIntent::Profile {
            intent: ProfileIntent::SaveForOwner {
                expected_owner: owner,
                section: ProfileEditorSection::Profile,
            },
        });
        replay(ClientIntent::Settings {
            intent: SettingsIntent::RevokeSession {
                expected_owner: 0,
                session_id: pioneer_protocol::AuthSessionId::new("SAAAAAAAAAAAAAAAAAAAA").unwrap(),
                expected_status: None,
            },
        });
        assert!(direct.profile().principal_id.is_none());
        assert!(!direct.profile().pending);
        assert_eq!(direct.profile().first_name, "");
    }
}
