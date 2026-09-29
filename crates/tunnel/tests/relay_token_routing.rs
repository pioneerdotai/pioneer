//! Regression coverage for the user flow: enter a key, keep the default service name.
use std::{net::SocketAddr, time::Duration};

use pioneer_config::GatewayRemoteAccessConfig;
use pioneer_protocol::{GatewayRemoteAccessSettings, GatewayRemoteAccessState};
use pioneer_tunnel::{RemoteAccessDesiredState, RemoteAccessSupervisor};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast,
    task::JoinHandle,
    time::timeout,
};

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn token_id(key: &str) -> [u8; 32] {
    let mut bytes = b"relay/token-routing/v1\0".to_vec();
    bytes.extend(hash(key.as_bytes()));
    hash(&bytes)
}

fn tunnel(id: &str, host: &str, key: &str) -> String {
    format!(
        "\n[[relay.tunnels]]\nid = {id:?}\nurl = \"https://{host}\"\ntoken_hash = \"sha256:{}\"\n",
        hex::encode(hash(key.as_bytes()))
    )
}

struct Relay {
    control: SocketAddr,
    ingress: SocketAddr,
    shutdown: broadcast::Sender<bool>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Relay {
    async fn start() -> Self {
        // Reserve distinct ephemeral ports, releasing them immediately before Relay binds.
        let control = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ingress = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_addr = control.local_addr().unwrap();
        let ingress_addr = ingress.local_addr().unwrap();
        let config = format!(
            "[relay]\ncontrol_addr = \"{control_addr}\"\ningress_addr = \"{ingress_addr}\"\n{}{}",
            tunnel("oskin1", "first.example", "first-key"),
            tunnel("rechkin", "second.example", "second-key"),
        );
        let (shutdown, rx) = broadcast::channel(1);
        drop((control, ingress));
        let task = tokio::spawn(rathole::run_config(
            rathole::Config::from_str(&config).unwrap(),
            rathole::Cli::default(),
            rx,
        ));
        let relay = Self {
            control: control_addr,
            ingress: ingress_addr,
            shutdown,
            task,
        };
        timeout(Duration::from_secs(5), async {
            loop {
                if TcpStream::connect(relay.control).await.is_ok() {
                    break;
                }
                assert!(!relay.task.is_finished(), "Relay exited before listening");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        relay
    }

    async fn request(&self, host: &str) -> String {
        timeout(Duration::from_secs(5), async {
            let mut socket = TcpStream::connect(self.ingress).await.unwrap();
            socket
                .write_all(
                    format!("GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = String::new();
            socket.read_to_string(&mut response).await.unwrap();
            response
        })
        .await
        .unwrap()
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

async fn upstream(body: &'static str) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
                assert!(request.len() < 8192);
            }
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    (addr, task)
}

async fn connect_gateway(gateway: &RemoteAccessSupervisor, key: &str) {
    let mut status = gateway.subscribe_status();
    gateway
        .apply(RemoteAccessDesiredState {
            settings: GatewayRemoteAccessSettings {
                enabled: true,
                // Both installations use exactly the same default name.
                service_name: Some("pioneer_gateway".to_owned()),
                has_key: true,
                ..Default::default()
            },
            key: Some(key.to_owned()),
        })
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if status.borrow().state == GatewayRemoteAccessState::Connected {
                break;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn two_gateways_with_default_name_route_by_key_and_reconnect_independently() {
    let relay = Relay::start().await;
    let mut gateways = Vec::new();
    let mut upstreams = Vec::new();
    let mut homes = Vec::new();
    for (body, key) in [
        ("first-gateway", "first-key"),
        ("second-gateway", "second-key"),
    ] {
        let (local, task) = upstream(body).await;
        let home = tempfile::tempdir().unwrap();
        let gateway = RemoteAccessSupervisor::new(
            home.path(),
            GatewayRemoteAccessConfig {
                relay_addr: relay.control.to_string(),
                local_addr: local.to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        connect_gateway(&gateway, key).await;
        gateways.push(gateway);
        upstreams.push(task);
        homes.push(home);
    }
    assert!(
        relay
            .request("first.example")
            .await
            .ends_with("first-gateway")
    );
    assert!(
        relay
            .request("second.example")
            .await
            .ends_with("second-gateway")
    );
    gateways[0].shutdown().await;
    connect_gateway(&gateways[0], "first-key").await;
    assert!(
        relay
            .request("first.example")
            .await
            .ends_with("first-gateway")
    );
    assert!(
        relay
            .request("second.example")
            .await
            .ends_with("second-gateway")
    );
    for gateway in gateways {
        gateway.shutdown().await;
    }
    for task in upstreams {
        task.abort();
    }
}

async fn handshake(relay: &Relay, id: [u8; 32], proof_key: [u8; 32]) -> (TcpStream, u32) {
    timeout(Duration::from_secs(5), async {
        let mut socket = TcpStream::connect(relay.control).await.unwrap();
        socket.write_all(&0u32.to_le_bytes()).await.unwrap();
        socket.write_u8(1).await.unwrap();
        socket.write_all(&id).await.unwrap();
        let mut hello = [0; 37];
        socket.read_exact(&mut hello).await.unwrap();
        assert_eq!(&hello[..5], &[0, 0, 0, 0, 1]);
        let mut proof = proof_key.to_vec();
        proof.extend_from_slice(&hello[5..]);
        socket.write_all(&hash(&proof)).await.unwrap();
        let ack = socket.read_u32_le().await.unwrap();
        (socket, ack)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn token_lookup_requires_secret_proof_and_rejects_service_name_handshake() {
    let relay = Relay::start().await;
    let first_id = token_id("first-key");
    // A public routing id alone must not be sufficient to impersonate a gateway.
    assert_eq!(handshake(&relay, first_id, first_id).await.1, 2);
    // A different valid key must not authenticate the selected tunnel.
    assert_eq!(handshake(&relay, first_id, hash(b"second-key")).await.1, 2);
    assert_eq!(
        handshake(&relay, token_id("unknown"), hash(b"unknown"))
            .await
            .1,
        2
    );
    // A service-name digest is no longer a routing key, even for a registered id.
    assert_eq!(
        handshake(&relay, hash(b"oskin1"), hash(b"first-key"))
            .await
            .1,
        2
    );
    let (_first, ack) = handshake(&relay, first_id, hash(b"first-key")).await;
    assert_eq!(ack, 0);
}

#[test]
fn duplicate_keys_are_rejected_even_with_different_ids_and_hosts() {
    let config = format!(
        "[relay]\n{}{}",
        tunnel("one", "one.example", "same-key"),
        tunnel("two", "two.example", "same-key")
    );
    let error = rathole::Config::from_str(&config).unwrap_err();
    assert!(format!("{error:#}").contains("duplicate relay token"));
}
