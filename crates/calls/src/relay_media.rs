mod signaling;

pub use signaling::RelaySignal;

use anyhow::{Context, Result, ensure};
use std::{
    collections::{BTreeSet, HashMap},
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::UdpSocket,
    sync::{Mutex, mpsc, oneshot},
    task::{AbortHandle, Id, JoinError, JoinSet},
    time::{self, Instant},
};

// Wire version is negotiated by the Relay's HTTP Upgrade.
const REGISTER: u8 = 1;
const DATAGRAM: u8 = 2;
const ACK: u8 = 3;
const KEEPALIVE: u8 = 4;
const MAX_PACKET: usize = 4096;
const MAX_FRAME: usize = 8192;
const MAX_FLOWS: usize = 32;
const FLOW_IDLE: Duration = Duration::from_secs(100);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const ACK_TIMEOUT: Duration = Duration::from_secs(5);

type PendingAck = Arc<Mutex<Option<oneshot::Sender<()>>>>;

pub struct Registration {
    tx: mpsc::Sender<Vec<u8>>,
    pending: PendingAck,
}

impl Registration {
    pub async fn replace(&mut self, routes: BTreeSet<String>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            ensure!(pending.is_none(), "parallel relay route registration");
            *pending = Some(tx);
        }
        let mut body = Vec::new();
        for route in routes {
            body.extend_from_slice(route.as_bytes());
            body.push(b'\n');
        }
        time::timeout(ACK_TIMEOUT, async {
            self.tx.send(body).await.context("relay media closed")?;
            rx.await.context("relay media registration was rejected")
        })
        .await??;
        Ok(())
    }
}

pub fn session<T>(
    stream: T,
    local: SocketAddrV4,
) -> (Registration, impl Future<Output = Result<()>>)
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let (tx, routes) = mpsc::channel(1);
    let pending = PendingAck::default();
    let registration = Registration {
        tx,
        pending: pending.clone(),
    };
    (registration, run(stream, local, routes, pending))
}

struct Flow {
    socket: Arc<UdpSocket>,
    task: AbortHandle,
    last_sent: Instant,
}

impl Drop for Flow {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn remove_finished_flow(flows: &mut HashMap<u64, Flow>, completion: Result<(Id, ()), JoinError>) {
    let id = match completion {
        Ok((id, ())) => id,
        Err(error) => error.id(),
    };
    // A canceled reader may complete after a replacement has reused its flow ID.
    flows.retain(|_, flow| flow.task.id() != id);
}

async fn run(
    stream: impl AsyncRead + AsyncWrite + Unpin,
    local: SocketAddrV4,
    mut routes: mpsc::Receiver<Vec<u8>>,
    pending: PendingAck,
) -> Result<()> {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (media_tx, mut media_rx) = mpsc::channel(128);
    let from_relay = async {
        let mut flows: HashMap<u64, Flow> = HashMap::new();
        let mut sockets = JoinSet::new();
        loop {
            while let Some(completion) = sockets.try_join_next_with_id() {
                remove_finished_flow(&mut flows, completion);
            }
            let next_frame = time::timeout(Duration::from_secs(40), read_frame(&mut reader));
            tokio::pin!(next_frame);
            let (kind, body) = loop {
                tokio::select! {
                    result = &mut next_frame => break result??,
                    Some(completion) = sockets.join_next_with_id(), if !sockets.is_empty() => {
                        remove_finished_flow(&mut flows, completion);
                    }
                }
            };
            match kind {
                ACK if body.is_empty() => {
                    let tx = pending
                        .lock()
                        .await
                        .take()
                        .context("unexpected relay acknowledgment")?;
                    let _ = tx.send(());
                }
                KEEPALIVE if body.is_empty() => {}
                DATAGRAM if (9..=8 + MAX_PACKET).contains(&body.len()) => {
                    let now = Instant::now();
                    let flow_id = u64::from_be_bytes(body[..8].try_into()?);
                    if flows
                        .get(&flow_id)
                        .is_none_or(|flow| flow.task.is_finished())
                    {
                        flows.retain(|_, flow| {
                            now.duration_since(flow.last_sent) < FLOW_IDLE
                                && !flow.task.is_finished()
                        });
                        if flows.len() >= MAX_FLOWS {
                            continue;
                        }
                        let socket = Arc::new(UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?);
                        socket.connect(local).await?;
                        let task =
                            sockets.spawn(read_sidecar(socket.clone(), flow_id, media_tx.clone()));
                        flows.insert(
                            flow_id,
                            Flow {
                                socket,
                                task,
                                last_sent: now,
                            },
                        );
                    }
                    let flow = flows.get_mut(&flow_id).unwrap();
                    flow.last_sent = now;
                    // A full local UDP queue cannot block the framed stream.
                    let _ = time::timeout(Duration::from_millis(10), flow.socket.send(&body[8..]))
                        .await;
                }
                _ => anyhow::bail!("invalid media frame from relay"),
            }
        }
    };
    let to_relay = async {
        let mut heartbeat = time::interval(KEEPALIVE_INTERVAL);
        heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        loop {
            let (kind, body) = tokio::select! {
                biased;
                routes = routes.recv() => match routes {
                    Some(routes) => (REGISTER, routes),
                    None => return Ok(()),
                },
                _ = heartbeat.tick() => (KEEPALIVE, Vec::new()),
                packet = media_rx.recv() => match packet {
                    Some(packet) => (DATAGRAM, packet),
                    None => return Ok(()),
                },
            };
            write_frame(&mut writer, kind, &body).await?;
        }
    };
    tokio::select! {
        result = from_relay => result,
        result = to_relay => result,
    }
}

async fn read_sidecar(socket: Arc<UdpSocket>, flow_id: u64, tx: mpsc::Sender<Vec<u8>>) {
    let mut packet = [0; MAX_PACKET + 1];
    while let Ok(Ok(length)) = time::timeout(FLOW_IDLE, socket.recv(&mut packet)).await {
        if length == 0 || length > MAX_PACKET {
            continue;
        }
        let mut body = Vec::with_capacity(8 + length);
        body.extend_from_slice(&flow_id.to_be_bytes());
        body.extend_from_slice(&packet[..length]);
        let _ = tx.try_send(body);
    }
}

async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> Result<(u8, Vec<u8>)> {
    let kind = reader.read_u8().await?;
    let length = reader.read_u16().await? as usize;
    ensure!(length <= MAX_FRAME, "oversized frame from relay");
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await?;
    Ok((kind, body))
}

async fn write_frame(writer: &mut (impl AsyncWrite + Unpin), kind: u8, body: &[u8]) -> Result<()> {
    ensure!(body.len() <= MAX_FRAME, "oversized frame to relay");
    let mut frame = Vec::with_capacity(3 + body.len());
    frame.push(kind);
    frame.extend_from_slice(&(body.len() as u16).to_be_bytes());
    frame.extend_from_slice(body);
    time::timeout(ACK_TIMEOUT, writer.write_all(&frame)).await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completed_reader_releases_only_its_own_flow() -> Result<()> {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        let mut readers = JoinSet::new();
        let task = readers.spawn(async {});
        let mut flows = HashMap::from([(
            1,
            Flow {
                socket: socket.clone(),
                task,
                last_sent: Instant::now(),
            },
        )]);
        remove_finished_flow(&mut flows, readers.join_next_with_id().await.unwrap());
        assert!(flows.is_empty());

        let retired = readers.spawn(async {});
        let completion = readers.join_next_with_id().await.unwrap();
        assert_eq!(completion.as_ref().unwrap().0, retired.id());
        let replacement = readers.spawn(std::future::pending());
        flows.insert(
            1,
            Flow {
                socket,
                task: replacement,
                last_sent: Instant::now(),
            },
        );
        remove_finished_flow(&mut flows, completion);
        assert!(flows.contains_key(&1));
        Ok(())
    }

    #[tokio::test]
    async fn datagram_stream_keeps_separate_sources_for_livekit_transports() -> Result<()> {
        let sidecar = UdpSocket::bind("127.0.0.1:0").await?;
        let std::net::SocketAddr::V4(local) = sidecar.local_addr()? else {
            panic!("expected a loopback IPv4 sidecar");
        };
        let (gateway, mut peer) = tokio::io::duplex(8192);
        let (_registration, driver) = session(gateway, local);
        let check = async {
            for (id, payload) in [
                (1u64, b"publisher".as_slice()),
                (2u64, b"subscriber".as_slice()),
            ] {
                let mut frame = id.to_be_bytes().to_vec();
                frame.extend_from_slice(payload);
                write_frame(&mut peer, DATAGRAM, &frame).await?;
            }
            let mut sources = Vec::new();
            let mut packet = [0; 64];
            for payload in [b"publisher".as_slice(), b"subscriber".as_slice()] {
                let (len, from) = sidecar.recv_from(&mut packet).await?;
                assert_eq!(&packet[..len], payload);
                sources.push(from);
            }
            assert_ne!(sources[0], sources[1]);
            let replies = [
                b"publisher response".as_slice(),
                b"subscriber response".as_slice(),
            ];
            for (from, reply) in sources.into_iter().zip(replies) {
                sidecar.send_to(reply, from).await?;
            }
            let mut received = HashMap::new();
            while received.len() < 2 {
                let (kind, body) = read_frame(&mut peer).await?;
                if kind == DATAGRAM {
                    received.insert(
                        u64::from_be_bytes(body[..8].try_into()?),
                        body[8..].to_vec(),
                    );
                }
            }
            assert_eq!(received.get(&1).unwrap(), replies[0]);
            assert_eq!(received.get(&2).unwrap(), replies[1]);
            Ok::<_, anyhow::Error>(())
        };
        tokio::select! {
            result = driver => panic!("relay media closed early: {result:?}"),
            result = time::timeout(Duration::from_secs(2), check) => result?,
        }
    }
}
