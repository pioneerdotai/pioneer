use pioneer_cli_mcp_bridge::{
    BridgeFrame, BridgeFrameTransport, BridgeFrameType, BridgeGeneration, BridgeSessionId,
    PrivateEndpointConfig, bind_private_endpoint, connect_private_endpoint,
    create_private_session_directory,
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
