use super::Registration;
use crate::livekit::{LiveKit, MAX_SIGNAL_BYTES};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{collections::BTreeSet, net::SocketAddrV4};

const JOIN: u32 = 1;
const ANSWER: u32 = 2;
const OFFER: u32 = 3;
const TRICKLE: u32 = 4;
const REFRESH: u32 = 16;
const RECONNECT: u32 = 19;
const ROOM_MOVED: u32 = 24;

/// The relay must acknowledge the full set of live ICE routes before an
/// offer or answer leaves this signaling socket.
pub struct RelaySignal<'a> {
    registration: Registration,
    state: SignalState<'a>,
}

struct SignalState<'a> {
    public: SocketAddrV4,
    media: &'a LiveKit,
    room: String,
    identity: String,
    publisher: BTreeSet<String>,
    subscriber: BTreeSet<String>,
}

impl<'a> RelaySignal<'a> {
    pub fn new(
        registration: Registration,
        public: SocketAddrV4,
        media: &'a LiveKit,
        room: String,
        identity: String,
    ) -> Self {
        Self {
            registration,
            state: SignalState {
                public,
                media,
                room,
                identity,
                publisher: BTreeSet::new(),
                subscriber: BTreeSet::new(),
            },
        }
    }

    pub async fn binary(&mut self, input: &[u8]) -> Result<Option<Vec<u8>>> {
        let before = self.state.routes();
        let mut output = Vec::with_capacity(input.len());
        for field in fields(input) {
            let field = field?;
            let child = match field.number {
                JOIN => filter_fields(field.bytes()?, &[5, 7])?, // ICE servers, alternate URL
                ANSWER | OFFER => self.state.binary_sdp(field.number, field.bytes()?)?,
                TRICKLE => {
                    let Some(trickle) = self.state.binary_trickle(field.bytes()?)? else {
                        return Ok(None);
                    };
                    trickle
                }
                REFRESH => self.state.refresh(field.text()?)?.into_bytes(),
                // /rtc/v1 encodes reconnect in a protobuf query. Reject the
                // response as well so every SDK must negotiate fresh ICE on a
                // replacement signaling socket.
                RECONNECT => anyhow::bail!("LiveKit signal resume has no relay ICE routes"),
                ROOM_MOVED => anyhow::bail!("LiveKit room move cannot reuse relay admission"),
                _ => {
                    output.extend_from_slice(field.raw);
                    continue;
                }
            };
            push_field(&mut output, field.number, &child);
        }
        ensure!(
            output.len() <= MAX_SIGNAL_BYTES,
            "rewritten LiveKit response is too large"
        );
        self.register(before).await?;
        Ok(Some(output))
    }

    pub async fn text(&mut self, input: &str) -> Result<Option<String>> {
        let before = self.state.routes();
        let mut message: Value = serde_json::from_str(input)?;
        let object = message
            .as_object_mut()
            .context("invalid JSON LiveKit response")?;
        for (name, field) in [("answer", ANSWER), ("offer", OFFER)] {
            if let Some(sdp) = object.get_mut(name).and_then(|value| value.get_mut("sdp")) {
                *sdp = self
                    .state
                    .sdp(field, sdp.as_str().context("invalid JSON SDP")?)?
                    .into();
            }
        }
        if let Some(join) = object.get_mut("join").and_then(Value::as_object_mut) {
            for name in [
                "iceServers",
                "ice_servers",
                "alternativeUrl",
                "alternative_url",
            ] {
                join.remove(name);
            }
        }
        if object.contains_key("reconnect") {
            anyhow::bail!("LiveKit signal resume has no relay ICE routes");
        }
        if object.contains_key("roomMoved") || object.contains_key("room_moved") {
            anyhow::bail!("LiveKit room move cannot reuse relay admission");
        }
        for name in ["refreshToken", "refresh_token"] {
            if let Some(token) = object.get_mut(name) {
                *token = self
                    .state
                    .refresh(token.as_str().context("invalid LiveKit token")?)?
                    .into();
            }
        }
        if let Some(candidate) = object
            .get_mut("trickle")
            .and_then(|value| value.get_mut("candidateInit"))
        {
            let Some(rewritten) = self
                .state
                .trickle(candidate.as_str().context("invalid JSON trickle")?)?
            else {
                return Ok(None);
            };
            *candidate = rewritten.into();
        }
        let output = serde_json::to_string(&message)?;
        ensure!(
            output.len() <= MAX_SIGNAL_BYTES,
            "rewritten LiveKit response is too large"
        );
        self.register(before).await?;
        Ok(Some(output))
    }

    async fn register(&mut self, before: BTreeSet<String>) -> Result<()> {
        let current = self.state.routes();
        if before != current {
            ensure!(
                current.len() <= 16,
                "too many ICE fragments for the signaling socket"
            );
            self.registration.replace(current).await?;
        }
        Ok(())
    }
}

impl SignalState<'_> {
    fn routes(&self) -> BTreeSet<String> {
        self.publisher.union(&self.subscriber).cloned().collect()
    }

    fn refresh(&self, token: &str) -> Result<String> {
        self.media
            .relay_refresh_token(token, &self.identity, &self.room)
    }

    fn sdp(&mut self, field: u32, sdp: &str) -> Result<String> {
        let (output, routes) = rewrite_sdp(sdp, self.public)?;
        if field == ANSWER {
            self.publisher = routes;
        } else {
            self.subscriber = routes;
        }
        Ok(output)
    }

    fn binary_sdp(&mut self, kind: u32, input: &[u8]) -> Result<Vec<u8>> {
        let mut output = Vec::with_capacity(input.len());
        let mut found = false;
        for field in fields(input) {
            let field = field?;
            if field.number == 2 {
                ensure!(!found, "duplicate LiveKit SDP");
                let sdp = self.sdp(kind, field.text()?)?;
                push_field(&mut output, 2, sdp.as_bytes());
                found = true;
            } else {
                output.extend_from_slice(field.raw);
            }
        }
        ensure!(found, "LiveKit SDP missing");
        Ok(output)
    }

    fn binary_trickle(&self, input: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut output = Vec::with_capacity(input.len());
        for field in fields(input) {
            let field = field?;
            if field.number == 1 {
                let Some(candidate) = self.trickle(field.text()?)? else {
                    return Ok(None);
                };
                push_field(&mut output, 1, candidate.as_bytes());
            } else {
                output.extend_from_slice(field.raw);
            }
        }
        Ok(Some(output))
    }

    fn trickle(&self, input: &str) -> Result<Option<String>> {
        if input.is_empty() {
            return Ok(Some(String::new()));
        }
        let mut json: Value = serde_json::from_str(input)?;
        let candidate = json
            .get_mut("candidate")
            .context("missing LiveKit ICE candidate")?;
        let value = candidate
            .as_str()
            .context("invalid LiveKit ICE candidate")?;
        if !value.is_empty() {
            let Some(rewritten) = rewrite_candidate(value, self.public) else {
                return Ok(None);
            };
            *candidate = rewritten.into();
        }
        Ok(Some(serde_json::to_string(&json)?))
    }
}

fn rewrite_sdp(sdp: &str, public: SocketAddrV4) -> Result<(String, BTreeSet<String>)> {
    // Relay confirms only SFU Binding Success responses. Do not publish
    // descriptions from a full ICE transport (including per-participant overrides).
    ensure!(
        sdp.lines()
            .take_while(|line| !line.starts_with("m="))
            .any(|line| line == "a=ice-lite"),
        "relay media requires session-level ICE Lite"
    );
    let mut routes = BTreeSet::new();
    let mut output = String::with_capacity(sdp.len());
    for line in sdp.lines() {
        if let Some(ufrag) = line.strip_prefix("a=ice-ufrag:") {
            ensure!(
                (4..=256).contains(&ufrag.len())
                    && ufrag
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/')),
                "invalid LiveKit ICE fragment"
            );
            routes.insert(ufrag.to_owned());
        }
        if let Some(candidate) = line.strip_prefix("a=candidate:") {
            if let Some(candidate) = rewrite_candidate(&format!("candidate:{candidate}"), public) {
                output.push_str("a=");
                output.push_str(&candidate);
                output.push_str("\r\n");
            }
            continue;
        }
        if line.starts_with("a=remote-candidates:") || line.starts_with("a=rtcp:") {
            continue;
        }
        if line.starts_with("c=") {
            output.push_str("c=IN IP4 0.0.0.0");
        } else {
            output.push_str(line);
        }
        output.push_str("\r\n");
    }
    ensure!(!routes.is_empty(), "LiveKit SDP lacks ICE fragments");
    Ok((output, routes))
}

fn rewrite_candidate(candidate: &str, public: SocketAddrV4) -> Option<String> {
    let mut parts = candidate.split_ascii_whitespace();
    let [foundation, component, transport, priority, _, _, kind, host] =
        std::array::from_fn(|_| parts.next().unwrap_or_default());
    if !foundation.starts_with("candidate:")
        || component != "1"
        || !transport.eq_ignore_ascii_case("udp")
        || priority.parse::<u32>().is_err()
        || kind != "typ"
        || host != "host"
    {
        return None;
    }
    // Never retain related-address or TCP extensions from the local SFU.
    Some(format!(
        "{foundation} 1 UDP {priority} {} {} typ host",
        public.ip(),
        public.port(),
    ))
}

// Edit only fields the proxy owns. Prost re-encoding a subset of LiveKit
// messages would discard unknown nested fields used by other SDK versions.
struct Field<'a> {
    number: u32,
    raw: &'a [u8],
    payload: Option<&'a [u8]>,
}

impl<'a> Field<'a> {
    fn bytes(&self) -> Result<&'a [u8]> {
        self.payload.context("invalid LiveKit protobuf wire type")
    }

    fn text(&self) -> Result<&'a str> {
        Ok(std::str::from_utf8(self.bytes()?)?)
    }
}

fn fields(mut input: &[u8]) -> impl Iterator<Item = Result<Field<'_>>> {
    std::iter::from_fn(move || {
        if input.is_empty() {
            return None;
        }
        let result = parse_field(&mut input);
        if result.is_err() {
            input = &[];
        }
        Some(result)
    })
}

fn parse_field<'a>(input: &mut &'a [u8]) -> Result<Field<'a>> {
    let raw = *input;
    let key = read_varint(input)?;
    let number = u32::try_from(key >> 3)?;
    ensure!(
        (1..=0x1fff_ffff).contains(&number),
        "invalid LiveKit protobuf field"
    );
    let payload = match key & 7 {
        0 => {
            read_varint(input)?;
            None
        }
        1 => {
            take(input, 8)?;
            None
        }
        2 => {
            let length = usize::try_from(read_varint(input)?)?;
            Some(take(input, length)?)
        }
        5 => {
            take(input, 4)?;
            None
        }
        _ => anyhow::bail!("invalid LiveKit protobuf wire type"),
    };
    Ok(Field {
        number,
        raw: &raw[..raw.len() - input.len()],
        payload,
    })
}

fn take<'a>(input: &mut &'a [u8], length: usize) -> Result<&'a [u8]> {
    let (head, tail) = input
        .split_at_checked(length)
        .context("truncated LiveKit protobuf")?;
    *input = tail;
    Ok(head)
}

fn read_varint(input: &mut &[u8]) -> Result<u64> {
    let mut value = 0;
    for shift in (0..64).step_by(7) {
        let byte = take(input, 1)?[0];
        ensure!(shift < 63 || byte <= 1, "oversized LiveKit protobuf varint");
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
    }
    anyhow::bail!("invalid LiveKit protobuf varint")
}

fn write_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn push_field(output: &mut Vec<u8>, number: u32, payload: &[u8]) {
    write_varint(output, (u64::from(number) << 3) | 2);
    write_varint(output, payload.len() as u64);
    output.extend_from_slice(payload);
}

fn filter_fields(input: &[u8], excluded: &[u32]) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(input.len());
    for field in fields(input) {
        let field = field?;
        if !excluded.contains(&field.number) {
            output.extend_from_slice(field.raw);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay_media;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn signaling_registers_before_emitting_sdp_and_preserves_wire_fields() -> Result<()> {
        let root = tempfile::tempdir()?;
        let livekit = LiveKit::new_relay(std::env::current_exe()?, root.path(), 1, 2)?;
        let (stream, mut peer) = tokio::io::duplex(4096);
        let (registration, driver) = relay_media::session(stream, "127.0.0.1:2".parse()?);
        let mut signal = RelaySignal::new(
            registration,
            "192.0.2.1:5555".parse()?,
            &livekit,
            "room".into(),
            "member".into(),
        );
        let mut offer = Vec::new();
        push_field(&mut offer, 1, b"offer");
        push_field(
            &mut offer,
            2,
            b"v=0\r\nc=IN IP4 127.0.0.1\r\na=ice-lite\r\na=ice-ufrag:local1\r\na=candidate:a 1 udp 1 127.0.0.1 2 typ host\r\na=candidate:b 1 tcp 1 127.0.0.1 2 typ host tcptype passive\r\n",
        );
        let unknown_sdp = [24, 13];
        offer.extend_from_slice(&unknown_sdp);
        let mut input = Vec::new();
        push_field(&mut input, OFFER, &offer);
        let mut offer = Box::pin(signal.binary(&input));
        tokio::pin!(driver);
        let (_, routes) = {
            let receive = async {
                let frame = loop {
                    let (kind, body) = relay_media::read_frame(&mut peer).await?;
                    if kind != relay_media::KEEPALIVE {
                        break (kind, body);
                    }
                };
                Ok::<_, anyhow::Error>(frame)
            };
            tokio::pin!(receive);
            tokio::select! {
                response = &mut offer => panic!("SDP left before the Relay acknowledged it: {response:?}"),
                result = &mut driver => panic!("relay lane closed: {result:?}"),
                frame = &mut receive => frame?,
            }
        };
        assert_eq!(routes, b"local1\n");
        tokio::select! {
            response = &mut offer => panic!("SDP left without an ACK: {response:?}"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        let ack = peer.write_all(&[relay_media::ACK, 0, 0]);
        tokio::pin!(ack);
        let output = tokio::select! {
            response = &mut offer => response?.unwrap(),
            result = &mut driver => panic!("relay lane closed: {result:?}"),
            result = &mut ack => {
                result?;
                tokio::select! {
                    response = &mut offer => response?.unwrap(),
                    result = &mut driver => panic!("relay lane closed: {result:?}"),
                }
            }
        };
        let description = fields(&output).next().unwrap()?.bytes()?;
        assert!(description.ends_with(&unknown_sdp));
        let sdp = fields(description).nth(1).unwrap()?.text()?;
        assert!(sdp.contains("candidate:a 1 UDP 1 192.0.2.1 5555 typ host"));
        assert!(!sdp.contains("127.0.0.1"));
        assert!(!sdp.contains("tcptype"));
        Ok(())
    }

    #[tokio::test]
    async fn full_ice_sdp_is_rejected_before_registering_in_both_signal_formats() -> Result<()> {
        let root = tempfile::tempdir()?;
        let livekit = LiveKit::new_relay(std::env::current_exe()?, root.path(), 1, 2)?;
        let (stream, _peer) = tokio::io::duplex(4096);
        let (registration, _driver) = relay_media::session(stream, "127.0.0.1:2".parse()?);
        let mut signal = RelaySignal::new(
            registration,
            "192.0.2.1:5555".parse()?,
            &livekit,
            "room".into(),
            "member".into(),
        );
        let full_ice = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=ice-ufrag:local1\r\n";
        let mut description = Vec::new();
        push_field(&mut description, 2, full_ice.as_bytes());
        let mut binary = Vec::new();
        push_field(&mut binary, ANSWER, &description);
        let error = signal.binary(&binary).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "relay media requires session-level ICE Lite"
        );

        // A marker under an m-line does not make the session ICE Lite.
        let text = serde_json::json!({ "offer": { "sdp": format!("{full_ice}a=ice-lite\r\n") } });
        let error = signal.text(&text.to_string()).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "relay media requires session-level ICE Lite"
        );
        assert!(signal.state.routes().is_empty());
        Ok(())
    }

    #[test]
    fn relay_credentials_and_refresh_tokens_cannot_be_used_at_livekit_directly() -> Result<()> {
        let root = tempfile::tempdir()?;
        let livekit = LiveKit::new_relay(std::env::current_exe()?, root.path(), 1, 2)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let credentials = livekit.credentials_with_relay_url(
            "room",
            "member",
            now,
            Some("wss://gateway.example.invalid/pioneer/"),
        )?;
        let upstream = livekit.upstream_token(&credentials.token)?;
        assert_ne!(upstream, credentials.token);
        assert!(livekit.admission(&credentials.token).is_ok());
        assert!(livekit.admission(&upstream).is_err());
        assert!(
            livekit
                .relay_refresh_token(&credentials.token, "member", "room")
                .is_err()
        );
        assert!(
            livekit
                .relay_refresh_token(&upstream, "member", "other")
                .is_err()
        );
        let refreshed = livekit.relay_refresh_token(&upstream, "member", "room")?;
        assert_eq!(
            livekit.admission(&refreshed)?,
            ("room".into(), "member".into())
        );
        assert!(
            livekit
                .relay_refresh_token(&refreshed, "member", "room")
                .is_err()
        );
        Ok(())
    }
}
