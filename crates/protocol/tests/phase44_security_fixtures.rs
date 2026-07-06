use pioneer_protocol::{
    TurnExecutionSecuritySnapshot, TurnPermissionAuditDecision, TurnPermissionAuditEvent,
    TurnPermissionAuditEventKind, TurnPermissionDecisionReason, TurnPermissionMode,
    TurnSandboxMode, TurnSecurityEnforcementStatus,
};

fn fixture_snapshot(path: &str, raw: &str) -> TurnExecutionSecuritySnapshot {
    serde_json::from_str(raw).unwrap_or_else(|error| panic!("{path} should decode: {error}"))
}

#[test]
fn phase44_security_snapshot_fixtures_decode_product_modes() {
    let unrestricted = fixture_snapshot(
        "security_snapshot_unrestricted.json",
        include_str!("fixtures/phase44/security_snapshot_unrestricted.json"),
    );
    assert_eq!(
        unrestricted.permission_profile.mode,
        TurnPermissionMode::FullAccess
    );
    assert_eq!(unrestricted.sandbox.mode, TurnSandboxMode::Unrestricted);
    assert_eq!(
        unrestricted.enforcement,
        TurnSecurityEnforcementStatus::Active
    );

    let workspace_write = fixture_snapshot(
        "security_snapshot_workspace_write.json",
        include_str!("fixtures/phase44/security_snapshot_workspace_write.json"),
    );
    assert_eq!(
        workspace_write.permission_profile.mode,
        TurnPermissionMode::AutoAcceptEdits
    );
    assert_eq!(
        workspace_write.sandbox.mode,
        TurnSandboxMode::WorkspaceWrite
    );
    assert_eq!(
        workspace_write.enforcement,
        TurnSecurityEnforcementStatus::Active
    );
}

#[test]
fn phase44_security_snapshot_fixtures_decode_degraded_and_unavailable() {
    let degraded = fixture_snapshot(
        "security_snapshot_degraded.json",
        include_str!("fixtures/phase44/security_snapshot_degraded.json"),
    );
    assert!(matches!(
        degraded.enforcement,
        TurnSecurityEnforcementStatus::PartiallyActive { .. }
    ));

    let unavailable = fixture_snapshot(
        "security_snapshot_unavailable.json",
        include_str!("fixtures/phase44/security_snapshot_unavailable.json"),
    );
    assert_eq!(
        unavailable.permission_profile.mode,
        TurnPermissionMode::Supervised
    );
    assert!(matches!(
        unavailable.enforcement,
        TurnSecurityEnforcementStatus::Unavailable { .. }
    ));
}

#[test]
fn phase44_security_audit_fixture_links_decision_to_snapshot() {
    let audit: TurnPermissionAuditEvent = serde_json::from_str(include_str!(
        "fixtures/phase44/security_audit_decision_linked_snapshot.json"
    ))
    .expect("security audit fixture should decode");

    assert_eq!(
        audit.event_kind,
        TurnPermissionAuditEventKind::DecisionDenied
    );
    assert_eq!(audit.decision, Some(TurnPermissionAuditDecision::Deny));
    assert_eq!(
        audit.reason,
        Some(TurnPermissionDecisionReason::SandboxDenied)
    );
    assert_eq!(
        audit.security_snapshot_id.as_deref(),
        Some("turn_release_fixture:security:v1")
    );
    assert_eq!(audit.security_snapshot_version, Some(1));
}
