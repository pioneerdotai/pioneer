use super::RemoteAccessSupervisor;
use anyhow::{Context, Result, ensure};
use pioneer_protocol::GatewayRemoteAccessState;
use reqwest::{StatusCode, Url, header};
use sha2::{Digest, Sha256};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};

const UPGRADE: &str = "pioneer-call-media-v1";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct CallMediaTarget {
    base_url: Url,
    key: String,
    client: reqwest::Client,
    shutdown: watch::Receiver<bool>,
}

pub struct CallMediaTransport {
    pub stream: reqwest::Upgraded,
    pub public_addr: SocketAddrV4,
    pub shutdown: watch::Receiver<bool>,
}

impl RemoteAccessSupervisor {
    pub async fn call_media_target(&self) -> Option<CallMediaTarget> {
        let state = self.state.lock().await;
        let shutdown = state.shutdown_tx.as_ref()?.subscribe();
        if *shutdown.borrow() {
            return None;
        }
        let status = self.status_tx.borrow();
        if status.state != GatewayRemoteAccessState::Connected {
            return None;
        }
        let base_url = Url::parse(status.public_base_url.as_ref()?.as_str()).ok()?;
        if base_url.scheme() != "https" {
            return None;
        }
        Some(CallMediaTarget {
            base_url,
            key: state.media_key.as_ref()?.clone(),
            client: self.media_client.clone(),
            shutdown,
        })
    }
}

impl CallMediaTarget {
    pub fn signaling_url(&self) -> Url {
        let mut url = self.base_url.clone();
        url.set_scheme("wss").expect("validated HTTPS relay URL");
        url.path_segments_mut()
            .expect("hierarchical relay URL")
            .pop_if_empty();
        url
    }

    pub async fn open(mut self) -> Result<CallMediaTransport> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid relay URL"))?
            .pop_if_empty()
            .extend(["_pioneer", "call-media", "v1"]);
        let handshake = async {
            let response = self
                .client
                .get(url)
                .header(header::CONNECTION, "Upgrade")
                .header(header::UPGRADE, UPGRADE)
                .send()
                .await
                .context("relay media connection failed")?;
            ensure!(
                response.status() == StatusCode::SWITCHING_PROTOCOLS
                    && response
                        .headers()
                        .get(header::UPGRADE)
                        .is_some_and(|value| value == UPGRADE),
                "relay media upgrade was rejected"
            );
            let mut stream = response.upgrade().await?;
            let mut nonce = [0; 32];
            stream.read_exact(&mut nonce).await?;
            let proof = Sha256::new()
                .chain_update(b"pioneer-call-media/v1\0")
                .chain_update(Sha256::digest(self.key.as_bytes()))
                .chain_update(nonce)
                .finalize();
            stream.write_all(&proof).await?;

            let mut address = [0; 6];
            stream.read_exact(&mut address).await?;
            let ip = Ipv4Addr::new(address[0], address[1], address[2], address[3]);
            let port = u16::from_be_bytes([address[4], address[5]]);
            ensure!(
                !ip.is_unspecified() && !ip.is_multicast() && !ip.is_broadcast() && port != 0,
                "invalid relay public media endpoint"
            );
            Ok::<_, anyhow::Error>((stream, SocketAddrV4::new(ip, port)))
        };
        let (stream, public_addr) = tokio::select! {
            _ = self.shutdown.changed() => anyhow::bail!("remote access configuration changed"),
            result = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake) => result??,
        };
        Ok(CallMediaTransport {
            stream,
            public_addr,
            shutdown: self.shutdown,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn signaling_base_allows_sdk_to_append_rtc_and_validation_paths() -> Result<()> {
        let (_, shutdown) = watch::channel(false);
        let client = reqwest::Client::builder().no_proxy().build()?;
        for (base, prefix) in [
            ("https://gateway.example.invalid/", ""),
            ("https://gateway.example.invalid/pioneer/", "/pioneer"),
            ("https://gateway.example.invalid/pioneer", "/pioneer"),
            (
                "https://gateway.example.invalid/space%20name/pioneer/",
                "/space%20name/pioneer",
            ),
        ] {
            let target = CallMediaTarget {
                base_url: Url::parse(base)?,
                key: String::new(),
                client: client.clone(),
                shutdown: shutdown.clone(),
            };
            let credentials_url = target.signaling_url();
            assert_eq!(credentials_url.scheme(), "wss");
            for segments in [&["rtc"][..], &["rtc", "v1"][..]] {
                // The Rust SDK parses the supplied URL and appends these segments.
                let mut sdk_url = Url::parse(credentials_url.as_str())?;
                sdk_url.path_segments_mut().unwrap().extend(segments);
                let expected = format!("{prefix}/{}", segments.join("/"));
                assert_eq!(sdk_url.path(), expected);
                sdk_url.path_segments_mut().unwrap().push("validate");
                assert_eq!(sdk_url.path(), format!("{expected}/validate"));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn call_media_upgrade_uses_public_prefix_and_proves_current_key() -> Result<()> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = listener.local_addr()?;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let target = CallMediaTarget {
            base_url: Url::parse(&format!("http://{addr}/pioneer/"))?,
            key: "test tunnel key".into(),
            client: reqwest::Client::builder().http1_only().no_proxy().build()?,
            shutdown: shutdown_rx,
        };
        let server = async {
            let (mut socket, _) = listener.accept().await?;
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await?);
                assert!(request.len() < 8192);
            }
            let request = String::from_utf8(request)?.to_ascii_lowercase();
            assert!(request.starts_with("get /pioneer/_pioneer/call-media/v1 http/1.1\r\n"));
            assert!(request.contains("\r\nupgrade: pioneer-call-media-v1\r\n"));
            assert!(!request.contains("test tunnel key"));
            let mut response = b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: pioneer-call-media-v1\r\n\r\n".to_vec();
            response.extend_from_slice(&[39; 32]);
            socket.write_all(&response).await?;
            let mut proof = [0; 32];
            socket.read_exact(&mut proof).await?;
            let expected: [u8; 32] = Sha256::new()
                .chain_update(b"pioneer-call-media/v1\0")
                .chain_update(Sha256::digest(b"test tunnel key"))
                .chain_update([39; 32])
                .finalize()
                .into();
            assert_eq!(proof, expected);
            socket.write_all(&[192, 0, 2, 1, 23, 42]).await?;
            Ok::<_, anyhow::Error>(socket)
        };
        let (transport, server) = tokio::join!(target.open(), server);
        let mut transport = transport?;
        let mut server = server?;
        assert_eq!(transport.public_addr, "192.0.2.1:5930".parse()?);
        transport.stream.write_all(b"frame").await?;
        let mut frame = [0; 5];
        server.read_exact(&mut frame).await?;
        assert_eq!(&frame, b"frame");
        shutdown.send(true)?;
        transport.shutdown.changed().await?;
        assert!(*transport.shutdown.borrow());
        Ok(())
    }
}
