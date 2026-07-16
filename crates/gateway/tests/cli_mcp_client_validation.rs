use migration::{Migrator, MigratorTrait};
use pioneer_gateway::cli_mcp_client_validation::{
    CliMcpClientTarget, CliMcpClientValidationAuditContext, CliMcpClientValidationEvidence,
    CliMcpClientValidationRejection, CliMcpClientValidationRejectionCode,
    validate_cli_mcp_client_request_durably,
};
use pioneer_protocol::TurnCapabilityRejectedReason;
use sea_orm::Database;

#[derive(Debug, Default, PartialEq, Eq)]
struct GatewayObservationSpies {
    durable_rejections: usize,
    projection_binding_commits: usize,
    provider_acquisitions: usize,
    native_turn_starts: usize,
}

async fn exercise_gateway_gate(
    store: &pioneer_crud::CrudStore,
    runtime_id: &str,
    evidence: CliMcpClientValidationEvidence,
    observations: &mut GatewayObservationSpies,
) -> Result<(), CliMcpClientValidationRejection> {
    if let Err(rejection) = validate_cli_mcp_client_request_durably(
        store,
        CliMcpClientValidationAuditContext {
            workspace_id: Some("workspace"),
            thread_id: "thread",
            turn_id: "turn",
            runtime_id,
        },
        evidence,
    )
    .await
    {
        observations.durable_rejections = store
            .list_recent_mcp_audit_event_records(runtime_id, 16)
            .await
            .expect("durable client-rejection audit query")
            .into_iter()
            .filter(|record| {
                record.action == "cli_mcp_client_preflight"
                    && record.decision == "rejected"
                    && record.reason_code.as_deref() == Some(rejection.code.as_str())
            })
            .count();
        return Err(rejection);
    }

    if evidence.has_mcp_projection {
        observations.projection_binding_commits += 1;
    }
    observations.provider_acquisitions += 1;
    observations.native_turn_starts += 1;
    Ok(())
}

async fn test_store() -> pioneer_crud::CrudStore {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite");
    Migrator::up(&connection, None)
        .await
        .expect("Gateway client-validation migrations");
    pioneer_crud::CrudStore::new(connection)
}

fn valid_cli(target: CliMcpClientTarget) -> CliMcpClientValidationEvidence {
    CliMcpClientValidationEvidence {
        target,
        has_mcp_projection: true,
        provider_claim_matches: true,
        runtime_snapshot_current: true,
        runtime_supports_mcp_tools: true,
        projection_workspace_matches: true,
        explicit_capabilities_resolved: true,
    }
}

#[tokio::test]
async fn forged_stale_and_unsupported_requests_stop_before_all_side_effect_boundaries() {
    let store = test_store().await;
    let cases = [
        (
            "unsupported",
            CliMcpClientValidationEvidence {
                runtime_supports_mcp_tools: false,
                ..valid_cli(CliMcpClientTarget::Codex)
            },
            CliMcpClientValidationRejectionCode::RuntimeMcpUnsupported,
            TurnCapabilityRejectedReason::ProviderUnsupported,
        ),
        (
            "stale runtime",
            CliMcpClientValidationEvidence {
                runtime_snapshot_current: false,
                ..valid_cli(CliMcpClientTarget::Claude)
            },
            CliMcpClientValidationRejectionCode::StaleRuntimeSnapshot,
            TurnCapabilityRejectedReason::Unavailable,
        ),
        (
            "cross workspace",
            CliMcpClientValidationEvidence {
                projection_workspace_matches: false,
                ..valid_cli(CliMcpClientTarget::Codex)
            },
            CliMcpClientValidationRejectionCode::CrossWorkspaceProjection,
            TurnCapabilityRejectedReason::SecurityBlocked,
        ),
        (
            "unresolved explicit",
            CliMcpClientValidationEvidence {
                explicit_capabilities_resolved: false,
                ..valid_cli(CliMcpClientTarget::Claude)
            },
            CliMcpClientValidationRejectionCode::ExplicitCapabilityUnresolved,
            TurnCapabilityRejectedReason::ValidationRejected,
        ),
        (
            "provider switch race",
            CliMcpClientValidationEvidence {
                provider_claim_matches: false,
                ..valid_cli(CliMcpClientTarget::Codex)
            },
            CliMcpClientValidationRejectionCode::ProviderSwitchRace,
            TurnCapabilityRejectedReason::ProviderUnsupported,
        ),
    ];

    for (case, evidence, expected_code, expected_reason) in cases {
        let mut observations = GatewayObservationSpies::default();
        let rejection = exercise_gateway_gate(&store, case, evidence, &mut observations)
            .await
            .expect_err("invalid client MCP evidence must be rejected");

        assert_eq!(rejection.code, expected_code, "{case}");
        assert_eq!(rejection.reason, expected_reason, "{case}");
        assert!(
            rejection.to_string().starts_with(expected_code.as_str()),
            "{case}"
        );
        assert_eq!(observations.durable_rejections, 1, "{case}");
        assert_eq!(observations.projection_binding_commits, 0, "{case}");
        assert_eq!(observations.provider_acquisitions, 0, "{case}");
        assert_eq!(observations.native_turn_starts, 0, "{case}");
    }
}

#[tokio::test]
async fn valid_api_codex_and_claude_controls_reach_the_exact_side_effect_boundaries() {
    let store = test_store().await;
    for (case, evidence) in [
        ("native API MCP", CliMcpClientValidationEvidence::api(true)),
        ("Codex MCP", valid_cli(CliMcpClientTarget::Codex)),
        ("Claude MCP", valid_cli(CliMcpClientTarget::Claude)),
    ] {
        let mut observations = GatewayObservationSpies::default();
        exercise_gateway_gate(&store, case, evidence, &mut observations)
            .await
            .unwrap_or_else(|error| panic!("{case} should pass: {error}"));

        assert_eq!(observations.durable_rejections, 0, "{case}");
        assert_eq!(observations.projection_binding_commits, 1, "{case}");
        assert_eq!(observations.provider_acquisitions, 1, "{case}");
        assert_eq!(observations.native_turn_starts, 1, "{case}");
    }
}

#[tokio::test]
async fn no_mcp_cli_control_preserves_existing_provider_start_without_projection_commit() {
    let store = test_store().await;
    let evidence = CliMcpClientValidationEvidence {
        has_mcp_projection: false,
        runtime_snapshot_current: false,
        runtime_supports_mcp_tools: false,
        ..valid_cli(CliMcpClientTarget::Codex)
    };
    let mut observations = GatewayObservationSpies::default();

    exercise_gateway_gate(&store, "codex-no-mcp", evidence, &mut observations)
        .await
        .expect("no-MCP CLI control should pass");

    assert_eq!(observations.durable_rejections, 0);
    assert_eq!(observations.projection_binding_commits, 0);
    assert_eq!(observations.provider_acquisitions, 1);
    assert_eq!(observations.native_turn_starts, 1);
}

#[test]
fn production_turn_start_orders_validation_before_persistence_and_provider_acquisition() {
    let source = include_str!("../src/message/turn_handlers.rs");
    let validation = source
        .find("validate_cli_mcp_client_request_durably")
        .expect("production turn start must invoke the authoritative client gate");
    let persistence = source
        .find("persist_cli_resolved_mcp_turn_projection")
        .expect("production turn start must persist a validated projection");
    let acquisition = source
        .find("get_or_start_with_launch_spec")
        .expect("production turn start must acquire the provider session");

    assert!(validation < persistence);
    assert!(persistence < acquisition);
}
