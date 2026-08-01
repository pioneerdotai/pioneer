use anyhow::{Context, Result};
use pioneer_crud::{InvitationWithGrants, PendingInvitationLookup};
use pioneer_entity::invitation;
use pioneer_protocol::InvitationCredential;
use sea_orm::{DatabaseTransaction, entity::prelude::DateTimeWithTimeZone};

use crate::{auth::OpaqueCredentialFactory, secrets::AuthKeyMaterial};

pub(crate) struct InvitationCredentialService {
    factory: OpaqueCredentialFactory,
}

pub(crate) struct IssuedInvitationCredential {
    credential: InvitationCredential,
    token_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InvitationCredentialLookup {
    Available(InvitationWithGrants),
    Expired(invitation::Model),
    Unavailable,
}

impl InvitationCredentialService {
    pub(crate) fn new(key: &AuthKeyMaterial) -> Result<Self> {
        Ok(Self {
            factory: OpaqueCredentialFactory::new(key.as_bytes())
                .context("failed to initialize invitation credential protection")?,
        })
    }

    pub(crate) fn issue(&self) -> IssuedInvitationCredential {
        let credential = self.factory.generate_invitation();
        let token_hash = self.factory.fingerprint_invitation(&credential);
        IssuedInvitationCredential {
            credential,
            token_hash,
        }
    }

    #[cfg(test)]
    pub(crate) async fn lookup_presented(
        &self,
        transaction: &DatabaseTransaction,
        raw: &str,
        now: DateTimeWithTimeZone,
    ) -> Result<InvitationCredentialLookup> {
        lookup_presented_with_factory(&self.factory, transaction, raw, now).await
    }
}

pub(crate) async fn lookup_presented_with_factory(
    factory: &OpaqueCredentialFactory,
    transaction: &DatabaseTransaction,
    raw: &str,
    now: DateTimeWithTimeZone,
) -> Result<InvitationCredentialLookup> {
    let Ok(credential) = InvitationCredential::parse(raw.to_owned()) else {
        return Ok(InvitationCredentialLookup::Unavailable);
    };
    let token_hash = factory.fingerprint_invitation(&credential);
    let lookup = pioneer_crud::load_effective_pending_invitation_by_token_hash(
        transaction,
        &token_hash,
        now,
    )
    .await
    .context("failed to resolve invitation credential")?;
    Ok(match lookup {
        PendingInvitationLookup::Available(invitation) => {
            InvitationCredentialLookup::Available(invitation)
        }
        PendingInvitationLookup::Expired(invitation) => {
            InvitationCredentialLookup::Expired(invitation)
        }
        PendingInvitationLookup::Unavailable => InvitationCredentialLookup::Unavailable,
    })
}

impl IssuedInvitationCredential {
    #[cfg(test)]
    pub(crate) fn credential(&self) -> &InvitationCredential {
        &self.credential
    }

    pub(crate) const fn token_hash(&self) -> &[u8; 32] {
        &self.token_hash
    }

    pub(crate) fn into_credential(self) -> InvitationCredential {
        self.credential
    }
}

impl std::fmt::Debug for IssuedInvitationCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IssuedInvitationCredential")
            .field("credential", &"[redacted]")
            .field("token_hash", &"[redacted]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use sea_orm::{Database, TransactionTrait};

    use super::*;

    #[test]
    fn issuance_is_canonical_nondeterministic_and_redacted() {
        let key = AuthKeyMaterial::from_test_bytes(vec![9; 64]);
        let service = InvitationCredentialService::new(&key).unwrap();
        let first = service.issue();
        let second = service.issue();
        assert!(InvitationCredential::parse(first.credential().expose_secret()).is_ok());
        assert_ne!(
            first.credential().expose_secret(),
            second.credential().expose_secret()
        );
        assert_ne!(first.token_hash(), second.token_hash());
        let rendered = format!("{first:?}");
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains(first.credential().expose_secret()));
        assert!(!rendered.contains(&hex::encode(first.token_hash())));
        assert!(std::mem::needs_drop::<IssuedInvitationCredential>());
    }

    #[tokio::test]
    async fn malformed_and_unknown_credentials_share_safe_unavailable_result() {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&database, None).await.unwrap();
        let transaction = database.begin().await.unwrap();
        let key = AuthKeyMaterial::from_test_bytes(vec![9; 64]);
        let service = InvitationCredentialService::new(&key).unwrap();
        let issued = service.issue();
        let now = chrono::DateTime::from_timestamp(
            crate::authorization_test_support::EPIC5_TEST_NOW_UNIX as i64,
            0,
        )
        .unwrap()
        .fixed_offset();

        assert_eq!(
            service
                .lookup_presented(&transaction, "malformed-secret", now)
                .await
                .unwrap(),
            InvitationCredentialLookup::Unavailable
        );
        assert_eq!(
            service
                .lookup_presented(&transaction, issued.credential().expose_secret(), now)
                .await
                .unwrap(),
            InvitationCredentialLookup::Unavailable
        );
        transaction.rollback().await.unwrap();
    }
}
