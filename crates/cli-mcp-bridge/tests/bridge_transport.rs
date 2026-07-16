use pioneer_cli_mcp_bridge::{
    BRIDGE_FRAME_MAGIC, BRIDGE_FRAME_VERSION, BridgeFrame, BridgeFrameError, BridgeFrameTransport,
    BridgeFrameType, BridgeGeneration, BridgeSessionId, MAX_FRAME_PAYLOAD_BYTES,
    PrivateEndpointConfig, bind_private_endpoint, connect_private_endpoint,
    create_private_session_directory, decode_frame_with_limit,
};

#[tokio::test]
async fn bridge_transport_real_private_endpoint_round_trip_and_cleanup() {
    #[cfg(unix)]
    let temporary = tempfile::tempdir_in("/tmp").expect("short temporary directory");
    #[cfg(windows)]
    let temporary = tempfile::tempdir().expect("temporary directory");
    let session_id = BridgeSessionId::new("transport-fixture").expect("session id");
    let generation = BridgeGeneration::new(1).expect("generation");
    let mut directory = create_private_session_directory(temporary.path(), &session_id, generation)
        .expect("private directory");
    let config = PrivateEndpointConfig {
        managed_directory: directory.path().to_path_buf(),
        session_id,
        generation,
        expected_peer_pid: None,
    };
    let mut listener = bind_private_endpoint(&config).expect("private listener");
    let client_config = config.clone();
    let client = tokio::spawn(async move {
        let mut connection = connect_private_endpoint(&client_config)
            .await
            .expect("connect");
        let request =
            BridgeFrame::new(BridgeFrameType::Payload, b"request".to_vec()).expect("request");
        connection.send_frame(&request).await.expect("send request");
        let response = connection
            .receive_frame()
            .await
            .expect("receive response")
            .expect("response");
        assert_eq!(response.frame_type(), BridgeFrameType::Payload);
        assert_eq!(response.payload(), b"response");
        connection.shutdown().await.expect("client shutdown");
    });
    let mut server = listener.accept().await.expect("accept");
    let request = server
        .receive_frame()
        .await
        .expect("receive request")
        .expect("request");
    assert_eq!(request.payload(), b"request");
    server
        .send_frame(
            &BridgeFrame::new(BridgeFrameType::Payload, b"response".to_vec()).expect("response"),
        )
        .await
        .expect("send response");
    server.shutdown().await.expect("server shutdown");
    client.await.expect("client join");
    drop(listener);
    directory.cleanup().expect("directory cleanup");
}

#[test]
fn bridge_transport_rejects_malformed_and_oversized_wire_frames() {
    let mut oversized = Vec::new();
    oversized.extend_from_slice(&BRIDGE_FRAME_MAGIC);
    oversized.extend_from_slice(&BRIDGE_FRAME_VERSION.to_be_bytes());
    oversized.push(BridgeFrameType::Payload as u8);
    oversized.push(0);
    oversized.extend_from_slice(&1025_u32.to_be_bytes());
    oversized.resize(12 + 1025, 0);
    assert!(matches!(
        decode_frame_with_limit(&oversized, 1024),
        Err(BridgeFrameError::PayloadTooLarge {
            declared: 1025,
            max: 1024
        })
    ));

    let mut invalid_magic = oversized;
    invalid_magic[..4].copy_from_slice(b"NOPE");
    assert!(matches!(
        decode_frame_with_limit(&invalid_magic, MAX_FRAME_PAYLOAD_BYTES),
        Err(BridgeFrameError::InvalidMagic)
    ));
}

#[test]
fn bridge_transport_debug_surfaces_redact_nonce_canary() {
    use pioneer_cli_mcp_bridge::{BootstrapNonce, NONCE_BYTES};

    let nonce = BootstrapNonce::new([0x5a; NONCE_BYTES]).expect("nonce");
    let exposed = hex_for_test(nonce.expose_secret());
    assert!(!format!("{nonce:?}").contains(&exposed));
}

fn hex_for_test(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}
