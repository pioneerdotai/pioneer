use crate::cli_runtime::session_instance::CliSessionInstanceId;
use pioneer_cli_mcp_bridge::{BootstrapNonce, BridgeGeneration, BridgeSessionId};
use rand::fill;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use subtle::ConstantTimeEq;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CliMcpGrantId(u128);

impl CliMcpGrantId {
    fn random() -> Self {
        Self(rand::random())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CliMcpConnectionId(u128);

impl CliMcpConnectionId {
    pub(crate) fn random() -> Self {
        Self(rand::random())
    }

    #[cfg(test)]
    pub(crate) const fn for_test(value: u128) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CliMcpManifestHash(String);

impl CliMcpManifestHash {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, CliMcpGrantError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(CliMcpGrantError::InvalidScope);
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CliMcpGrantScope {
    pub(crate) process_instance: CliSessionInstanceId,
    pub(crate) manifest_hash: CliMcpManifestHash,
}

impl CliMcpGrantScope {
    pub(crate) fn new(
        process_instance: CliSessionInstanceId,
        manifest_hash: CliMcpManifestHash,
    ) -> Self {
        Self {
            process_instance,
            manifest_hash,
        }
    }
}

pub(crate) struct CliMcpIssuedGrant {
    pub(crate) grant_id: CliMcpGrantId,
    pub(crate) bridge_session_id: BridgeSessionId,
    pub(crate) nonce: BootstrapNonce,
    pub(crate) scope: CliMcpGrantScope,
    pub(crate) expires_at_unix_ms: u64,
}

impl fmt::Debug for CliMcpIssuedGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliMcpIssuedGrant")
            .field("grant_id", &self.grant_id)
            .field("bridge_session_id", &self.bridge_session_id)
            .field("nonce", &"[REDACTED]")
            .field("scope", &self.scope)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .finish()
    }
}

impl CliMcpIssuedGrant {
    pub(crate) fn grant_ref(&self) -> CliMcpGrantRef {
        CliMcpGrantRef {
            grant_id: self.grant_id,
            scope: self.scope.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CliMcpGrantRef {
    grant_id: CliMcpGrantId,
    scope: CliMcpGrantScope,
}

impl CliMcpGrantRef {
    pub(crate) const fn grant_id(&self) -> CliMcpGrantId {
        self.grant_id
    }

    pub(crate) fn scope(&self) -> &CliMcpGrantScope {
        &self.scope
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CliMcpBoundGrant {
    grant_id: CliMcpGrantId,
    connection_id: CliMcpConnectionId,
    scope: CliMcpGrantScope,
}

impl CliMcpBoundGrant {
    pub(crate) const fn grant_id(&self) -> CliMcpGrantId {
        self.grant_id
    }

    pub(crate) const fn connection_id(&self) -> CliMcpConnectionId {
        self.connection_id
    }

    pub(crate) fn scope(&self) -> &CliMcpGrantScope {
        &self.scope
    }

    pub(crate) fn grant_ref(&self) -> CliMcpGrantRef {
        CliMcpGrantRef {
            grant_id: self.grant_id,
            scope: self.scope.clone(),
        }
    }
}

struct CliMcpSessionGrant {
    grant_id: CliMcpGrantId,
    bridge_session_id: BridgeSessionId,
    token_hash: [u8; 32],
    nonce_consumed: bool,
    scope: CliMcpGrantScope,
    expires_at_unix_ms: u64,
    connection_id: Option<CliMcpConnectionId>,
    revoked: bool,
}

impl fmt::Debug for CliMcpSessionGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliMcpSessionGrant")
            .field("grant_id", &self.grant_id)
            .field("bridge_session_id", &self.bridge_session_id)
            .field("token_hash", &"[REDACTED]")
            .field("nonce_consumed", &self.nonce_consumed)
            .field("scope", &self.scope)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .field("connection_id", &self.connection_id)
            .field("revoked", &self.revoked)
            .finish()
    }
}

impl Drop for CliMcpSessionGrant {
    fn drop(&mut self) {
        self.token_hash.fill(0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliMcpGrantError {
    InvalidScope,
    InvalidExpiry,
    UnknownGrant,
    Expired,
    Revoked,
    Replay,
    CrossScope,
    WrongNonce,
    StaleGeneration,
    WrongConnection,
}

#[derive(Default)]
pub(crate) struct CliMcpGrantRegistryState {
    grants: HashMap<CliMcpGrantId, CliMcpSessionGrant>,
    grants_by_session: HashMap<BridgeSessionId, CliMcpGrantId>,
}

impl CliMcpGrantRegistryState {
    pub(crate) fn issue(
        &mut self,
        scope: CliMcpGrantScope,
        expires_at_unix_ms: u64,
        now_unix_ms: u64,
    ) -> Result<CliMcpIssuedGrant, CliMcpGrantError> {
        if expires_at_unix_ms <= now_unix_ms {
            return Err(CliMcpGrantError::InvalidExpiry);
        }
        let nonce = random_nonce();
        let grant_id = loop {
            let candidate = CliMcpGrantId::random();
            if !self.grants.contains_key(&candidate) {
                break candidate;
            }
        };
        let token_hash = nonce_hash(&nonce);
        let bridge_session_id =
            BridgeSessionId::new(format!("grant-{grant_id:032x}", grant_id = grant_id.0))
                .map_err(|_| CliMcpGrantError::InvalidScope)?;
        self.grants.insert(
            grant_id,
            CliMcpSessionGrant {
                grant_id,
                bridge_session_id: bridge_session_id.clone(),
                token_hash,
                nonce_consumed: false,
                scope: scope.clone(),
                expires_at_unix_ms,
                connection_id: None,
                revoked: false,
            },
        );
        self.grants_by_session
            .insert(bridge_session_id.clone(), grant_id);
        Ok(CliMcpIssuedGrant {
            grant_id,
            bridge_session_id,
            nonce,
            scope,
            expires_at_unix_ms,
        })
    }

    pub(crate) fn attach(
        &mut self,
        bridge_session_id: &BridgeSessionId,
        presented_generation: BridgeGeneration,
        presented_scope: &CliMcpGrantScope,
        nonce: &BootstrapNonce,
        connection_id: CliMcpConnectionId,
        now_unix_ms: u64,
    ) -> Result<CliMcpBoundGrant, CliMcpGrantError> {
        let grant_id = *self
            .grants_by_session
            .get(bridge_session_id)
            .ok_or(CliMcpGrantError::UnknownGrant)?;
        let grant = self
            .grants
            .get_mut(&grant_id)
            .ok_or(CliMcpGrantError::UnknownGrant)?;
        if grant.revoked {
            return Err(CliMcpGrantError::Revoked);
        }
        if grant.expires_at_unix_ms <= now_unix_ms {
            grant.revoked = true;
            grant.token_hash.fill(0);
            return Err(CliMcpGrantError::Expired);
        }
        if &grant.scope != presented_scope {
            return Err(CliMcpGrantError::CrossScope);
        }
        if grant.bridge_session_id != *bridge_session_id
            || grant.scope.process_instance.generation() != presented_generation.get()
        {
            return Err(CliMcpGrantError::StaleGeneration);
        }
        if grant.nonce_consumed || grant.connection_id.is_some() {
            return Err(CliMcpGrantError::Replay);
        }
        let presented_hash = nonce_hash(nonce);
        if !bool::from(grant.token_hash.ct_eq(&presented_hash)) {
            return Err(CliMcpGrantError::WrongNonce);
        }
        grant.token_hash.fill(0);
        grant.nonce_consumed = true;
        grant.connection_id = Some(connection_id);
        Ok(CliMcpBoundGrant {
            grant_id,
            connection_id,
            scope: grant.scope.clone(),
        })
    }

    pub(crate) fn validate_bound(
        &self,
        bound: &CliMcpBoundGrant,
        _now_unix_ms: u64,
    ) -> Result<(), CliMcpGrantError> {
        let grant = self
            .grants
            .get(&bound.grant_id)
            .ok_or(CliMcpGrantError::UnknownGrant)?;
        if grant.revoked {
            return Err(CliMcpGrantError::Revoked);
        }
        if grant.scope != bound.scope {
            return Err(CliMcpGrantError::CrossScope);
        }
        if grant.connection_id != Some(bound.connection_id) {
            return Err(CliMcpGrantError::WrongConnection);
        }
        Ok(())
    }

    pub(crate) fn validate_ref(
        &self,
        grant_ref: &CliMcpGrantRef,
        now_unix_ms: u64,
    ) -> Result<(), CliMcpGrantError> {
        let grant = self
            .grants
            .get(&grant_ref.grant_id)
            .ok_or(CliMcpGrantError::UnknownGrant)?;
        if grant.revoked {
            return Err(CliMcpGrantError::Revoked);
        }
        // The expiry bounds only the one-use bootstrap/attach window. Once
        // the nonce is consumed and the grant is connection-bound, the
        // connection plus explicit revoke/replacement own its lifetime.
        if grant.connection_id.is_none() && grant.expires_at_unix_ms <= now_unix_ms {
            return Err(CliMcpGrantError::Expired);
        }
        if grant.scope != grant_ref.scope {
            return Err(CliMcpGrantError::CrossScope);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn revoke_bound(
        &mut self,
        bound: &CliMcpBoundGrant,
    ) -> Result<(), CliMcpGrantError> {
        self.validate_bound(bound, 0)?;
        let grant = self
            .grants
            .get_mut(&bound.grant_id)
            .ok_or(CliMcpGrantError::UnknownGrant)?;
        grant.revoked = true;
        grant.token_hash.fill(0);
        Ok(())
    }

    pub(crate) fn revoke_ref(
        &mut self,
        grant_ref: &CliMcpGrantRef,
    ) -> Result<(), CliMcpGrantError> {
        self.validate_ref(grant_ref, 0)?;
        let grant = self
            .grants
            .get_mut(&grant_ref.grant_id)
            .ok_or(CliMcpGrantError::UnknownGrant)?;
        grant.revoked = true;
        grant.token_hash.fill(0);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn revoke_process(&mut self, process_instance: &CliSessionInstanceId) -> usize {
        let mut revoked = 0;
        for grant in self.grants.values_mut() {
            if &grant.scope.process_instance == process_instance && !grant.revoked {
                grant.revoked = true;
                grant.token_hash.fill(0);
                revoked += 1;
            }
        }
        revoked
    }

    pub(crate) fn revoke_all(&mut self) {
        for grant in self.grants.values_mut() {
            grant.revoked = true;
            grant.token_hash.fill(0);
        }
    }
}

fn random_nonce() -> BootstrapNonce {
    loop {
        let mut bytes = [0_u8; pioneer_cli_mcp_bridge::NONCE_BYTES];
        fill(&mut bytes);
        if let Ok(nonce) = BootstrapNonce::new(bytes) {
            return nonce;
        }
    }
}

fn nonce_hash(nonce: &BootstrapNonce) -> [u8; 32] {
    Sha256::digest(nonce.expose_secret()).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_runtime::manager::CLIAgentRuntimeSessionKey;

    fn scope() -> CliMcpGrantScope {
        let instance = CliSessionInstanceId::unmanaged_for_test(
            CLIAgentRuntimeSessionKey::new("workspace", "codex", "thread").expect("key"),
            1,
        )
        .expect("instance");
        CliMcpGrantScope::new(
            instance,
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest"),
        )
    }

    #[test]
    fn cli_mcp_grants_reject_wrong_nonce_and_expiry_without_leaking_secret() {
        let mut registry = CliMcpGrantRegistryState::default();
        let issued = registry.issue(scope(), 101, 100).expect("issue");
        let wrong_nonce =
            BootstrapNonce::new([0x44; pioneer_cli_mcp_bridge::NONCE_BYTES]).expect("nonce");
        assert_eq!(
            registry.attach(
                &issued.bridge_session_id,
                BridgeGeneration::new(1).expect("generation"),
                &issued.scope,
                &wrong_nonce,
                CliMcpConnectionId::for_test(1),
                100,
            ),
            Err(CliMcpGrantError::WrongNonce)
        );
        assert_eq!(
            registry.attach(
                &issued.bridge_session_id,
                BridgeGeneration::new(1).expect("generation"),
                &issued.scope,
                &issued.nonce,
                CliMcpConnectionId::for_test(1),
                101,
            ),
            Err(CliMcpGrantError::Expired)
        );

        let nonce_hex = issued
            .nonce
            .expose_secret()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert!(!format!("{issued:?}").contains(nonce_hex.as_str()));
    }

    #[test]
    fn cli_mcp_bound_grant_outlives_the_one_use_bootstrap_expiry() {
        let mut registry = CliMcpGrantRegistryState::default();
        let issued = registry.issue(scope(), 101, 100).expect("issue");
        let grant_ref = issued.grant_ref();
        let bound = registry
            .attach(
                &issued.bridge_session_id,
                BridgeGeneration::new(1).expect("generation"),
                &issued.scope,
                &issued.nonce,
                CliMcpConnectionId::for_test(7),
                100,
            )
            .expect("attach before bootstrap expiry");

        assert_eq!(registry.validate_bound(&bound, 10_000), Ok(()));
        assert_eq!(registry.validate_ref(&grant_ref, 10_000), Ok(()));
    }
}
