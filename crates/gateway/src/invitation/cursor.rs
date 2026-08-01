use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pioneer_crud::InvitationListCursor;
use pioneer_protocol::{ADMINISTRATION_DOMAIN_ID_LEN, InvitationId};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::auth::OpaqueCredentialFactory;
use crate::secrets::AuthKeyMaterial;

const CURSOR_VERSION: u8 = 1;
const CURSOR_FIXED_PAYLOAD_BYTES: usize = 1 + 8 + 4 + ADMINISTRATION_DOMAIN_ID_LEN + 32;
const CURSOR_MAC_BYTES: usize = 32;
const CURSOR_ENVELOPE_BYTES: usize = CURSOR_FIXED_PAYLOAD_BYTES + CURSOR_MAC_BYTES;

pub(crate) struct InvitationCursorCodec {
    factory: OpaqueCredentialFactory,
}

impl InvitationCursorCodec {
    pub(crate) fn new(key: &AuthKeyMaterial) -> Result<Self> {
        Ok(Self {
            factory: OpaqueCredentialFactory::new(key.as_bytes())
                .context("failed to initialize invitation cursor protection")?,
        })
    }

    pub(crate) fn encode(&self, cursor: &InvitationListCursor, scope: &str) -> String {
        let mut payload = Vec::with_capacity(CURSOR_FIXED_PAYLOAD_BYTES);
        payload.push(CURSOR_VERSION);
        payload.extend_from_slice(&cursor.created_at.timestamp().to_be_bytes());
        payload.extend_from_slice(&cursor.created_at.timestamp_subsec_nanos().to_be_bytes());
        payload.extend_from_slice(cursor.invitation_id.as_str().as_bytes());
        payload.extend_from_slice(&scope_hash(scope));
        debug_assert_eq!(payload.len(), CURSOR_FIXED_PAYLOAD_BYTES);
        let mac = self
            .factory
            .fingerprint_invitation_cursor(payload.as_slice());
        payload.extend_from_slice(&mac);
        URL_SAFE_NO_PAD.encode(payload)
    }

    pub(crate) fn decode(
        &self,
        encoded: &str,
        expected_scope: &str,
    ) -> Result<InvitationListCursor> {
        if encoded.is_empty() || encoded.len() > pioneer_protocol::INVITATION_CURSOR_MAX_BYTES {
            bail!("invalid invitation cursor");
        }
        let envelope = URL_SAFE_NO_PAD
            .decode(encoded.as_bytes())
            .context("invalid invitation cursor encoding")?;
        if envelope.len() != CURSOR_ENVELOPE_BYTES {
            bail!("invalid invitation cursor envelope");
        }
        let (payload, presented_mac) = envelope.split_at(CURSOR_FIXED_PAYLOAD_BYTES);
        let expected_mac = self.factory.fingerprint_invitation_cursor(payload);
        if !bool::from(expected_mac.as_slice().ct_eq(presented_mac)) {
            bail!("invalid invitation cursor signature");
        }
        if payload[0] != CURSOR_VERSION {
            bail!("unsupported invitation cursor version");
        }
        let seconds = i64::from_be_bytes(
            payload[1..9]
                .try_into()
                .context("invalid invitation cursor timestamp")?,
        );
        let nanos = u32::from_be_bytes(
            payload[9..13]
                .try_into()
                .context("invalid invitation cursor nanoseconds")?,
        );
        let id_end = 13 + ADMINISTRATION_DOMAIN_ID_LEN;
        let invitation_id = InvitationId::new(
            std::str::from_utf8(&payload[13..id_end])
                .context("invalid invitation cursor id encoding")?
                .to_owned(),
        )
        .context("invalid invitation cursor id")?;
        let presented_scope = &payload[id_end..id_end + 32];
        if !bool::from(scope_hash(expected_scope).as_slice().ct_eq(presented_scope)) {
            bail!("invitation cursor scope mismatch");
        }
        let created_at = chrono::DateTime::from_timestamp(seconds, nanos)
            .context("invalid invitation cursor timestamp")?
            .fixed_offset();
        Ok(InvitationListCursor {
            created_at,
            invitation_id,
        })
    }
}

fn scope_hash(scope: &str) -> [u8; 32] {
    Sha256::digest(scope.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_is_signed_and_bound_to_actor_filter_scope() {
        let key = AuthKeyMaterial::from_test_bytes(vec![7; 64]);
        let codec = InvitationCursorCodec::new(&key).unwrap();
        let cursor = InvitationListCursor {
            created_at: chrono::DateTime::from_timestamp(1_800_000_000, 123)
                .unwrap()
                .fixed_offset(),
            invitation_id: InvitationId::new("I00000000000000000001").unwrap(),
        };
        let encoded = codec.encode(&cursor, "member:P1");
        assert_eq!(codec.decode(&encoded, "member:P1").unwrap(), cursor);
        assert!(codec.decode(&encoded, "member:P2").is_err());
        let mut tampered = encoded.into_bytes();
        let index = tampered.len() / 2;
        tampered[index] = if tampered[index] == b'A' { b'B' } else { b'A' };
        assert!(
            codec
                .decode(std::str::from_utf8(&tampered).unwrap(), "member:P1")
                .is_err()
        );
    }
}
