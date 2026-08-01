use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pioneer_crud::MemberDirectoryCursor;
use pioneer_protocol::{ADMINISTRATION_DOMAIN_ID_LEN, PrincipalId};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::auth::OpaqueCredentialFactory;
use crate::secrets::AuthKeyMaterial;

const CURSOR_VERSION: u8 = 1;
const CURSOR_MAC_BYTES: usize = 32;

pub(crate) struct MemberCursorCodec {
    factory: OpaqueCredentialFactory,
}

impl MemberCursorCodec {
    pub(crate) fn new(key: &AuthKeyMaterial) -> Result<Self> {
        Ok(Self {
            factory: OpaqueCredentialFactory::new(key.as_bytes())
                .context("failed to initialize member cursor protection")?,
        })
    }

    pub(crate) fn encode(&self, cursor: &MemberDirectoryCursor, scope: &str) -> String {
        let nickname = cursor.nickname_key.as_bytes();
        let mut payload = Vec::with_capacity(
            1 + 2 + nickname.len() + ADMINISTRATION_DOMAIN_ID_LEN + 32 + CURSOR_MAC_BYTES,
        );
        payload.push(CURSOR_VERSION);
        payload.extend_from_slice(&(nickname.len() as u16).to_be_bytes());
        payload.extend_from_slice(nickname);
        payload.extend_from_slice(cursor.principal_id.as_str().as_bytes());
        payload.extend_from_slice(&scope_hash(scope));
        let mac = self.factory.fingerprint_member_cursor(payload.as_slice());
        payload.extend_from_slice(&mac);
        URL_SAFE_NO_PAD.encode(payload)
    }

    pub(crate) fn decode(
        &self,
        encoded: &str,
        expected_scope: &str,
    ) -> Result<MemberDirectoryCursor> {
        if encoded.is_empty() || encoded.len() > pioneer_protocol::MEMBER_DIRECTORY_CURSOR_MAX_BYTES
        {
            bail!("invalid member directory cursor");
        }
        let envelope = URL_SAFE_NO_PAD
            .decode(encoded.as_bytes())
            .context("invalid member directory cursor encoding")?;
        if envelope.len() < 1 + 2 + ADMINISTRATION_DOMAIN_ID_LEN + 32 + CURSOR_MAC_BYTES {
            bail!("invalid member directory cursor envelope");
        }
        let payload_len = envelope.len() - CURSOR_MAC_BYTES;
        let (payload, presented_mac) = envelope.split_at(payload_len);
        let expected_mac = self.factory.fingerprint_member_cursor(payload);
        if !bool::from(expected_mac.as_slice().ct_eq(presented_mac)) {
            bail!("invalid member directory cursor signature");
        }
        if payload[0] != CURSOR_VERSION {
            bail!("unsupported member directory cursor version");
        }
        let nickname_len = usize::from(u16::from_be_bytes([payload[1], payload[2]]));
        let expected_payload_len = 1 + 2 + nickname_len + ADMINISTRATION_DOMAIN_ID_LEN + 32;
        if nickname_len == 0
            || nickname_len > pioneer_protocol::MEMBER_NICKNAME_MAX_LEN
            || payload.len() != expected_payload_len
        {
            bail!("invalid member directory cursor payload");
        }
        let nickname_end = 3 + nickname_len;
        let id_end = nickname_end + ADMINISTRATION_DOMAIN_ID_LEN;
        let nickname_key = std::str::from_utf8(&payload[3..nickname_end])
            .context("invalid member directory cursor nickname")?
            .to_owned();
        let principal_id = PrincipalId::new(
            std::str::from_utf8(&payload[nickname_end..id_end])
                .context("invalid member directory cursor principal")?
                .to_owned(),
        )
        .context("invalid member directory cursor principal")?;
        if !bool::from(
            scope_hash(expected_scope)
                .as_slice()
                .ct_eq(&payload[id_end..]),
        ) {
            bail!("member directory cursor scope mismatch");
        }
        Ok(MemberDirectoryCursor {
            nickname_key,
            principal_id,
        })
    }
}

fn scope_hash(scope: &str) -> [u8; 32] {
    Sha256::digest(scope.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use crate::secrets::AuthKeyMaterial;

    use super::*;

    #[test]
    fn cursor_is_signed_and_bound_to_directory_actor() {
        let codec = MemberCursorCodec::new(&AuthKeyMaterial::from_test_bytes(vec![7; 64])).unwrap();
        let cursor = MemberDirectoryCursor {
            nickname_key: "member-a".to_owned(),
            principal_id: PrincipalId::new("P0000000000000000000A").unwrap(),
        };
        let encoded = codec.encode(&cursor, "member:P-A");
        assert_eq!(codec.decode(&encoded, "member:P-A").unwrap(), cursor);
        assert!(codec.decode(&encoded, "member:P-B").is_err());
        let mut tampered = encoded.into_bytes();
        let index = tampered.len() / 2;
        tampered[index] = if tampered[index] == b'A' { b'B' } else { b'A' };
        assert!(
            codec
                .decode(std::str::from_utf8(&tampered).unwrap(), "member:P-A")
                .is_err()
        );
    }
}
