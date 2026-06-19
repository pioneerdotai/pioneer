use pioneer_client::gateway::connectivity::{
    DEFAULT_GATEWAY_PORT, normalize_address, resolve_socket_address_input,
};

#[test]
fn gateway_normalize_address_rejects_empty_and_invalid_values() {
    assert!(normalize_address(" ").is_err());
    assert!(normalize_address("http://gateway.example.com").is_err());
    assert!(normalize_address("gateway.example.com/path").is_err());
    assert!(normalize_address("gateway.example.com:not-a-port").is_err());
}

#[test]
fn gateway_normalize_address_accepts_host_only_and_domains() {
    assert_eq!(
        normalize_address("127.0.0.1").expect("ip without port should be valid"),
        format!("127.0.0.1:{DEFAULT_GATEWAY_PORT}")
    );
    assert_eq!(
        normalize_address("localhost").expect("localhost without port should be valid"),
        format!("localhost:{DEFAULT_GATEWAY_PORT}")
    );
    assert_eq!(
        normalize_address("gateway.example.com").expect("domain without port should be valid"),
        format!("gateway.example.com:{DEFAULT_GATEWAY_PORT}")
    );
    assert_eq!(
        normalize_address("edge.gateway.example.co.uk:443")
            .expect("domain with port should be valid"),
        "edge.gateway.example.co.uk:443"
    );
}

#[test]
fn gateway_normalize_address_accepts_websocket_urls_and_ipv6() {
    assert_eq!(
        normalize_address("wss://gateway.example.com/socket")
            .expect("websocket URL should be valid"),
        "wss://gateway.example.com/socket"
    );
    assert_eq!(
        normalize_address("https://gateway.example.com").expect("HTTPS URL should be valid"),
        "https://gateway.example.com"
    );
    assert_eq!(
        normalize_address("::1").expect("ipv6 without port should be valid"),
        format!("[::1]:{DEFAULT_GATEWAY_PORT}")
    );
    assert_eq!(
        normalize_address("[::1]:22000").expect("ipv6 with port should be valid"),
        "[::1]:22000"
    );
}

#[test]
fn gateway_resolve_socket_address_input_uses_ws_default_ports() {
    assert_eq!(
        resolve_socket_address_input("ws://localhost").expect("ws default port"),
        "localhost:80"
    );
    assert_eq!(
        resolve_socket_address_input("wss://gateway.example.com").expect("wss default port"),
        "gateway.example.com:443"
    );
    assert_eq!(
        resolve_socket_address_input("https://gateway.example.com").expect("https default port"),
        "gateway.example.com:443"
    );
}
