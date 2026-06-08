use pioneer_client::gateway::{
    registry::{
        GatewayLocalRegistryConfig, GatewayRegistryConfig, default_registry, normalize_registry,
        setup_required,
    },
    types::{GatewayEndpoint, GatewayEndpointKind},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = GatewayRegistryConfig {
        version: 1,
        local: Some(GatewayLocalRegistryConfig {
            gateway_id: "local".to_owned(),
            name: "Local Gateway".to_owned(),
            address: "127.0.0.1:17878".to_owned(),
            auth_token_ref: Some("local-token".to_owned()),
            service_name: Some("com.pioneer.gateway".to_owned()),
        }),
    };

    let mut registry = default_registry(&config);
    assert!(setup_required(&registry));

    registry.active_gateway_id = Some("missing".to_owned());
    registry.remotes.push(GatewayEndpoint {
        id: "remote-a".to_owned(),
        name: "  ".to_owned(),
        address: "gateway.example.com".to_owned(),
        kind: GatewayEndpointKind::Local,
        auth_token_ref: Some("remote-token".to_owned()),
        workspace_id: Some("  ws_1  ".to_owned()),
        service_name: Some("ignored-for-remotes".to_owned()),
    });

    normalize_registry(
        &mut registry,
        &config,
        |endpoint_id| Ok(format!("secret:{endpoint_id}")),
        |index| format!("Remote Gateway {index}"),
    )?;

    assert_eq!(registry.active_gateway_id, None);
    assert_eq!(
        registry.local.as_ref().map(|endpoint| endpoint.kind),
        Some(GatewayEndpointKind::Local)
    );
    assert_eq!(registry.remotes.len(), 1);
    assert_eq!(registry.remotes[0].kind, GatewayEndpointKind::Remote);
    assert_eq!(registry.remotes[0].name, "Remote Gateway 1");
    assert_eq!(registry.remotes[0].address, "gateway.example.com:17878");
    assert_eq!(registry.remotes[0].workspace_id.as_deref(), Some("ws_1"));
    assert_eq!(registry.remotes[0].service_name, None);

    println!(
        "normalized registry with {} remote gateway",
        registry.remotes.len()
    );
    Ok(())
}
