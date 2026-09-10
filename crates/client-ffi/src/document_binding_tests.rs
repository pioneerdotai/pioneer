use super::*;
use pioneer_client::{
    agents_doc::{
        content::agents_doc_content_hash,
        runtime::{AgentsDocumentAction, AgentsDocumentIntent, document_scope},
        scope::AgentsDocEditorScope,
    },
    core::{ClientDemand, ClientGeneration, ClientIntent},
};

#[test]
fn direct_and_ffi_document_replay_preserve_drafts_failure_retry_and_scope_release() {
    for platform in ["ios", "android"] {
        let direct = pioneer_client::catalog_test_support::client();
        let wire = pioneer_client::catalog_test_support::client();
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
        let scope = AgentsDocEditorScope::root("workspace");
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
                expected
            );
        };
        replay(ClientIntent::SetScopeDemand {
            scope: document_scope(&scope),
            demand: ClientDemand::Visible,
            generation: ClientGeneration::new(1),
        });
        for core in [&direct, &wire] {
            let request = core.next_agents_document_request_for_test().unwrap();
            assert!(core.complete_agents_document_load_for_test(
                request,
                pioneer_protocol::ThreadAgentsDocGetResponse {
                    explicit: None,
                    effective: None
                }
            ));
        }
        let owner = direct
            .agents_document_snapshot(&scope)
            .unwrap()
            .owner_generation();
        replay(ClientIntent::AgentsDocument {
            intent: AgentsDocumentIntent::Scoped {
                scope: scope.clone(),
                expected_owner: owner + 1,
                action: AgentsDocumentAction::Edit {
                    content: "late".into(),
                },
            },
        });
        replay(ClientIntent::AgentsDocument {
            intent: AgentsDocumentIntent::Scoped {
                scope: scope.clone(),
                expected_owner: owner,
                action: AgentsDocumentAction::Edit {
                    content: "draft".into(),
                },
            },
        });
        replay(ClientIntent::AgentsDocument {
            intent: AgentsDocumentIntent::Scoped {
                scope: scope.clone(),
                expected_owner: owner,
                action: AgentsDocumentAction::Edit {
                    content: "draft".into(),
                },
            },
        });
        replay(ClientIntent::AgentsDocument {
            intent: AgentsDocumentIntent::Scoped {
                scope: scope.clone(),
                expected_owner: owner,
                action: AgentsDocumentAction::Save,
            },
        });
        for core in [&direct, &wire] {
            let request = core.next_agents_document_request_for_test().unwrap();
            assert!(
                core.complete_agents_document_save_for_test(
                    request,
                    Err("synthetic offline".into())
                )
            );
            assert!(core.next_agents_document_request_for_test().is_none());
        }
        replay(ClientIntent::AgentsDocument {
            intent: AgentsDocumentIntent::Scoped {
                scope: scope.clone(),
                expected_owner: owner,
                action: AgentsDocumentAction::Save,
            },
        });
        for core in [&direct, &wire] {
            let request = core.next_agents_document_request_for_test().unwrap();
            assert!(core.complete_agents_document_save_for_test(
                request,
                Ok(pioneer_protocol::ThreadAgentsDocPayload {
                    id: "document".into(),
                    workspace_id: "workspace".into(),
                    folder_id: None,
                    status: pioneer_protocol::ThreadAgentsDocStatus::Active,
                    title: "AGENTS.md".into(),
                    content: "draft".into(),
                    content_sha256: agents_doc_content_hash("draft"),
                    version: 1,
                    created_at: 1,
                    updated_at: 1,
                })
            ));
        }
        let request = serde_json::json!({"schema_version":1,"scope":document_scope(&scope),"after_revision":null});
        assert_eq!(
            runtime
                .client_scoped_snapshot(&request.to_string())
                .unwrap()
                .unwrap(),
            client_binding::snapshot_dto(direct.snapshot(&document_scope(&scope)).unwrap())
        );
        replay(ClientIntent::SetScopeDemand {
            scope: document_scope(&scope),
            demand: ClientDemand::Suspended,
            generation: ClientGeneration::new(2),
        });
        assert!(direct.next_agents_document_request_for_test().is_none());
        assert!(wire.next_agents_document_request_for_test().is_none());
        replay(ClientIntent::SetScopeDemand {
            scope: document_scope(&scope),
            demand: ClientDemand::Visible,
            generation: ClientGeneration::new(3),
        });
        let next_owner = direct
            .agents_document_snapshot(&scope)
            .unwrap()
            .owner_generation();
        assert!(next_owner > owner);
        let before = direct.agents_document_snapshot(&scope).unwrap();
        replay(ClientIntent::AgentsDocument {
            intent: AgentsDocumentIntent::Scoped {
                scope: scope.clone(),
                expected_owner: owner,
                action: AgentsDocumentAction::Edit {
                    content: "stale previous editor".into(),
                },
            },
        });
        assert!(Arc::ptr_eq(
            &before,
            &direct.agents_document_snapshot(&scope).unwrap()
        ));
        assert_eq!(
            runtime
                .client_scoped_snapshot(&request.to_string())
                .unwrap()
                .unwrap(),
            client_binding::snapshot_dto(direct.snapshot(&document_scope(&scope)).unwrap())
        );
        replay(ClientIntent::SetScopeDemand {
            scope: document_scope(&scope),
            demand: ClientDemand::Suspended,
            generation: ClientGeneration::new(4),
        });
    }
}
