#[test]
fn cli_mcp_bridge_integration_keeps_transport_and_gateway_authority_separate() {
    let server = include_str!("../src/cli_runtime/mcp/server.rs");
    let facade = include_str!("../src/cli_runtime/mcp/facade.rs");
    let helper = include_str!("../../cli-mcp-bridge/src/helper.rs");
    let bridge_manifest = include_str!("../../cli-mcp-bridge/Cargo.toml");

    assert!(server.contains("CliMcpBridgeTransport"));
    assert!(server.contains("CliMcpToolFacade"));
    assert!(facade.contains("TurnMcpInvoker"));
    assert!(facade.contains("authorize_call"));
    assert!(!helper.contains("TurnMcpInvoker"));
    assert!(!bridge_manifest.contains("pioneer-gateway"));
    assert!(!bridge_manifest.contains("pioneer-crud"));
    assert!(!bridge_manifest.contains("pioneer-tools"));
}

#[test]
fn cli_mcp_secret_canary_has_no_literal_production_embedding() {
    const CANARY: &str = "pioneer-secret-canary-7c3278d3c7214b66965e";
    for production_source in [
        include_str!("../src/cli_runtime/mcp/facade.rs"),
        include_str!("../src/cli_runtime/mcp/server.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production server source"),
        include_str!("../../cli-mcp-bridge/src/helper.rs"),
    ] {
        assert!(!production_source.contains(CANARY));
    }
}
