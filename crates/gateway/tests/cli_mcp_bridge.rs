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
