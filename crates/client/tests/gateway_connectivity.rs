use pioneer_client::gateway::{
    connectivity::DEFAULT_GATEWAY_PORT,
    endpoint::{GatewayBaseUrl, GatewayTransportSecurity},
};

#[test]
fn gateway_base_url_rejects_legacy_or_unsafe_values() {
    for input in [
        " ",
        "ws://gateway.example.com",
        "wss://gateway.example.com/socket",
        "https://user:secret@gateway.example.com",
        "https://gateway.example.com?token=secret",
        "https://gateway.example.com#fragment",
        "http://0.0.0.0:17878",
    ] {
        assert!(GatewayBaseUrl::parse_presentation(input).is_err());
    }
}

#[test]
fn gateway_base_url_normalizes_presentation_input() {
    assert_eq!(
        GatewayBaseUrl::parse_presentation("127.0.0.1")
            .unwrap()
            .as_str(),
        format!("http://127.0.0.1:{DEFAULT_GATEWAY_PORT}/")
    );
    assert_eq!(
        GatewayBaseUrl::parse_presentation("https://gateway.example.com/pioneer")
            .unwrap()
            .as_str(),
        "https://gateway.example.com/pioneer/"
    );
    assert_eq!(
        GatewayBaseUrl::parse_presentation("http://[::1]:22000")
            .unwrap()
            .as_str(),
        "http://[::1]:22000/"
    );
}

#[test]
fn gateway_base_url_derives_transport_without_another_parser() {
    let base = GatewayBaseUrl::parse_presentation("https://gateway.example.com/pioneer").unwrap();
    assert_eq!(
        base.websocket_url().as_str(),
        "wss://gateway.example.com/pioneer/"
    );
    assert_eq!(base.socket_address_input(), "gateway.example.com:443");
    assert_eq!(base.transport_security(), GatewayTransportSecurity::Tls);
}
