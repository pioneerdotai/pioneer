//! Authenticated, confidential stateless cursors. Candidate scan positions may
//! name inaccessible rows, so signing a plaintext/base64 cursor is insufficient.
use anyhow::{Result, anyhow, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::{
    aead,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

pub(super) struct ThreadReadCursorCodec(aead::LessSafeKey);
impl ThreadReadCursorCodec {
    pub(super) fn new(secret: &[u8]) -> Result<Self> {
        let mut hash = Sha256::new();
        hash.update(b"pioneer.thread-read.cursor.v1\0");
        hash.update(secret);
        let key = aead::UnboundKey::new(&aead::CHACHA20_POLY1305, &hash.finalize())
            .map_err(|_| anyhow!("cursor protection unavailable"))?;
        Ok(Self(aead::LessSafeKey::new(key)))
    }

    pub(super) fn encode<T: Serialize>(&self, state: &T, scope: &str) -> Result<String> {
        let mut nonce = [0u8; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| anyhow!("cursor protection unavailable"))?;
        let mut payload = serde_json::to_vec(state)?;
        self.0
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(scope.as_bytes()),
                &mut payload,
            )
            .map_err(|_| anyhow!("cursor protection unavailable"))?;
        let mut envelope = Vec::with_capacity(13 + payload.len());
        envelope.push(1);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&payload);
        let cursor = URL_SAFE_NO_PAD.encode(envelope);
        ensure!(
            cursor.len() <= 16384,
            "cursor exceeds 16 KiB; restart reading"
        );
        Ok(cursor)
    }

    pub(super) fn decode<T: DeserializeOwned>(&self, cursor: &str, scope: &str) -> Result<T> {
        ensure!(
            !cursor.is_empty() && cursor.len() <= 16384,
            "invalid cursor"
        );
        let mut bytes = URL_SAFE_NO_PAD
            .decode(cursor)
            .map_err(|_| anyhow!("invalid cursor"))?;
        ensure!(bytes.len() >= 29 && bytes[0] == 1, "invalid cursor");
        let nonce: [u8; 12] = bytes[1..13]
            .try_into()
            .map_err(|_| anyhow!("invalid cursor"))?;
        let plaintext = self
            .0
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(scope.as_bytes()),
                &mut bytes[13..],
            )
            .map_err(|_| anyhow!("invalid cursor or cursor scope mismatch"))?;
        serde_json::from_slice(plaintext).map_err(|_| anyhow!("invalid cursor"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_is_confidential_authenticated_and_bound_to_query_and_user() {
        let codec = ThreadReadCursorCodec::new(&[7; 64]).unwrap();
        let state = serde_json::json!({"after":"inaccessible-thread-name"});
        let cursor = codec.encode(&state, "user-a:list:workspace:20").unwrap();
        let bytes = URL_SAFE_NO_PAD.decode(&cursor).unwrap();
        assert!(!bytes.windows(24).any(|w| w == b"inaccessible-thread-name"));
        assert_eq!(
            codec
                .decode::<serde_json::Value>(&cursor, "user-a:list:workspace:20")
                .unwrap(),
            state
        );
        for scope in [
            "user-b:list:workspace:20",
            "user-a:list:workspace:21",
            "user-a:history:workspace:20",
        ] {
            assert!(codec.decode::<serde_json::Value>(&cursor, scope).is_err());
        }
        let mut tampered = bytes;
        *tampered.last_mut().unwrap() ^= 1;
        assert!(
            codec
                .decode::<serde_json::Value>(
                    &URL_SAFE_NO_PAD.encode(tampered),
                    "user-a:list:workspace:20"
                )
                .is_err()
        );
    }
}
