//! Recovery of existing native device-activation journals before registry publication.
use super::{
    session_refresh::GatewaySessionStorage,
    types::{GatewayEndpoint, GatewayRegistry},
};
use anyhow::Result;
use pioneer_protocol::{AuthSessionId, GatewayId};
use serde::{Deserialize, Serialize};
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayBindingJournalRecord {
    pub gateway_id: String,
    pub document: String,
}
impl std::fmt::Debug for GatewayBindingJournalRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayBindingJournalRecord")
            .finish_non_exhaustive()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingBinding {
    schema_version: u32,
    gateway_id: GatewayId,
    endpoint_id: String,
    session_ref: String,
    endpoint: GatewayEndpoint,
    previous_session_id: Option<AuthSessionId>,
}
/// A corrupt/unreadable journal remains intact; independent valid entries can still recover.
pub(crate) fn recover_gateway_bindings(
    registry: &mut GatewayRegistry,
    records: &[GatewayBindingJournalRecord],
    storage: &dyn GatewaySessionStorage,
    mut save: impl FnMut(&GatewayRegistry) -> Result<()>,
    mut clear: impl FnMut(&str) -> Result<()>,
) {
    let Some(installation) = registry.installation_id.clone() else {
        return;
    };
    for record in records {
        let Ok(pending) = serde_json::from_str::<PendingBinding>(&record.document) else {
            continue;
        };
        if pending.schema_version != 1
            || pending.gateway_id.as_str() != record.gateway_id
            || pending.endpoint_id != pending.endpoint.id
            || pending.session_ref.trim().is_empty()
            || pending.endpoint.session_ref.as_deref() != Some(pending.session_ref.as_str())
            || pending.endpoint.server_gateway_id.as_ref() != Some(&pending.gateway_id)
        {
            continue;
        }
        let existing = super::runtime::endpoint_by_id(registry, &pending.endpoint_id);
        if existing.is_some_and(|endpoint| {
            endpoint.gateway_base_url != pending.endpoint.gateway_base_url
                || endpoint.kind != pending.endpoint.kind
                || endpoint
                    .server_gateway_id
                    .as_ref()
                    .is_some_and(|id| id != &pending.gateway_id)
                || endpoint
                    .session_ref
                    .as_ref()
                    .is_some_and(|reference| reference != &pending.session_ref)
        }) {
            continue;
        }
        if existing.is_none() && pending.endpoint.kind != super::types::GatewayEndpointKind::Remote
        {
            continue;
        }
        if registry.endpoints().iter().any(|endpoint| {
            endpoint.id != pending.endpoint_id
                && (endpoint.gateway_base_url == pending.endpoint.gateway_base_url
                    || endpoint.server_gateway_id.as_ref() == Some(&pending.gateway_id)
                    || endpoint.session_ref.as_ref() == Some(&pending.session_ref))
        }) {
            continue;
        }
        let Ok(Some(envelope)) = storage.load(&pending.endpoint) else {
            continue;
        };
        if envelope.validate().is_err()
            || envelope.gateway_id != pending.gateway_id
            || envelope.installation_id != installation
            || Some(&envelope.session_id) == pending.previous_session_id.as_ref()
        {
            continue;
        }
        let mut endpoint = existing.cloned().unwrap_or(pending.endpoint);
        endpoint.session_ref = Some(pending.session_ref);
        endpoint.server_gateway_id = Some(pending.gateway_id);
        let mut recovered = registry.clone();
        if let Some(current) = super::runtime::endpoint_by_id_mut(&mut recovered, &endpoint.id) {
            *current = endpoint.clone();
        } else {
            recovered.remotes.push(endpoint.clone());
        }
        recovered.active_gateway_id = Some(endpoint.id);
        if save(&recovered).is_ok() {
            *registry = recovered;
            let _ = clear(&record.gateway_id);
        }
    }
}

/// Mobile historically discards unauthenticated candidates. A newly durable envelope
/// must first be adopted so a one-use activation survives process restart.
pub(crate) fn recover_unbound_remote_candidates(
    registry: &mut GatewayRegistry,
    storage: &dyn GatewaySessionStorage,
    mut save: impl FnMut(&GatewayRegistry) -> Result<()>,
) -> Result<()> {
    let Some(installation) = registry.installation_id.as_deref() else {
        return Ok(());
    };
    let mut recovered = registry.clone();
    let mut bound_gateways: std::collections::BTreeSet<_> = registry
        .endpoints()
        .iter()
        .filter_map(|endpoint| endpoint.server_gateway_id.clone())
        .collect();
    let mut bound_references: std::collections::BTreeSet<_> = registry
        .endpoints()
        .iter()
        .filter_map(|endpoint| endpoint.session_ref.clone())
        .collect();
    recovered.remotes.retain_mut(|endpoint| {
        if endpoint.session_ref.is_some() || endpoint.server_gateway_id.is_some() {
            return true;
        }
        let mut candidate = endpoint.clone();
        candidate.session_ref = Some(endpoint.id.clone());
        match storage.load(&candidate) {
            Ok(Some(envelope))
                if envelope.validate().is_ok() && envelope.installation_id == installation =>
            {
                if bound_gateways.contains(&envelope.gateway_id)
                    || bound_references.contains(&endpoint.id)
                {
                    return true;
                }
                bound_gateways.insert(envelope.gateway_id.clone());
                bound_references.insert(endpoint.id.clone());
                endpoint.session_ref = candidate.session_ref;
                endpoint.server_gateway_id = Some(envelope.gateway_id);
                true
            }
            Ok(None) => false,
            _ => true,
        }
    });
    if recovered
        .active_gateway_id
        .as_ref()
        .is_some_and(|id| super::runtime::endpoint_by_id(&recovered, id).is_none())
    {
        recovered.active_gateway_id = recovered
            .remotes
            .first()
            .or(recovered.local.as_ref())
            .map(|endpoint| endpoint.id.clone());
    }
    if &recovered != registry {
        save(&recovered)?;
        *registry = recovered;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::provisioning::{
        self,
        tests::{MemoryStorage, grant, registry},
    };
    use super::*;
    use serde_json::json;
    use std::cell::Cell;
    fn fixture() -> (GatewayRegistry, MemoryStorage, GatewayBindingJournalRecord) {
        let mut registry = registry();
        let storage = MemoryStorage::default();
        let installation = pioneer_protocol::ClientInstallationDescriptor {
            installation_id: registry.installation_id.clone().unwrap(),
            display_name: "Synthetic".into(),
            client_kind: pioneer_protocol::ClientKind::Desktop,
            platform: None,
            client_version: None,
        };
        provisioning::provision_endpoint_session(
            &mut registry,
            &installation,
            "local",
            "K7M4P9Q2",
            &storage,
            |_, _, _| Ok(grant()),
            |_, _, _| panic!(),
            |_| Ok(()),
        )
        .unwrap();
        let endpoint = registry.local.as_ref().unwrap();
        let record=GatewayBindingJournalRecord{gateway_id:grant().gateway.id.to_string(),document:json!({"schema_version":1,"gateway_id":grant().gateway.id,"endpoint_id":endpoint.id,"session_ref":endpoint.session_ref,"endpoint":endpoint,"previous_session_id":null}).to_string()};
        registry.local.as_mut().unwrap().session_ref = None;
        registry.local.as_mut().unwrap().server_gateway_id = None;
        (registry, storage, record)
    }
    #[test]
    fn unbound_candidate_recovers_durable_grant_before_discarding_empty_entries() {
        let (mut registry, storage, _) = fixture();
        let mut endpoint = registry.local.take().unwrap();
        endpoint.kind = super::super::types::GatewayEndpointKind::Remote;
        let mut empty = endpoint.clone();
        empty.id = "empty".into();
        empty.gateway_base_url =
            super::super::endpoint::GatewayBaseUrl::parse_presentation("https://empty.invalid")
                .unwrap();
        registry.remotes = vec![endpoint, empty];
        registry.active_gateway_id = Some("empty".into());
        let before = registry.clone();
        assert!(
            recover_unbound_remote_candidates(&mut registry, &storage, |_| anyhow::bail!(
                "synthetic write failure"
            ))
            .is_err()
        );
        assert_eq!(registry, before);
        recover_unbound_remote_candidates(&mut registry, &storage, |_| Ok(())).unwrap();
        assert_eq!(registry.remotes.len(), 1);
        assert!(registry.remotes[0].session_ref.is_some());
        assert_eq!(registry.active_gateway_id.as_deref(), Some("local"));
    }
    #[test]
    fn candidate_recovery_does_not_alias_a_bound_gateway_identity() {
        let (mut registry, storage, _) = fixture();
        registry.local.as_mut().unwrap().server_gateway_id = Some(grant().gateway.id.clone());
        registry.local.as_mut().unwrap().session_ref = Some("local".into());
        let mut candidate = registry.local.as_ref().unwrap().clone();
        candidate.id = "candidate".into();
        candidate.kind = super::super::types::GatewayEndpointKind::Remote;
        candidate.gateway_base_url =
            super::super::endpoint::GatewayBaseUrl::parse_presentation("https://candidate.invalid")
                .unwrap();
        candidate.session_ref = Some("candidate".into());
        let envelope = storage
            .load(registry.local.as_ref().unwrap())
            .unwrap()
            .unwrap();
        storage.persist(&candidate, &envelope).unwrap();
        candidate.session_ref = None;
        candidate.server_gateway_id = None;
        registry.remotes.push(candidate);
        let before = registry.clone();
        recover_unbound_remote_candidates(&mut registry, &storage, |_| {
            panic!("duplicate binding must remain private")
        })
        .unwrap();
        assert_eq!(registry, before);
    }
    #[test]
    fn recovers_durable_refresh_before_publication_and_clears_only_after_commit() {
        let (mut registry, storage, record) = fixture();
        let order = std::cell::RefCell::new(vec![]);
        recover_gateway_bindings(
            &mut registry,
            &[record],
            &storage,
            |next| {
                assert!(next.local.as_ref().unwrap().session_ref.is_some());
                order.borrow_mut().push("save");
                Ok(())
            },
            |_| {
                order.borrow_mut().push("clear");
                Ok(())
            },
        );
        assert_eq!(*order.borrow(), vec!["save", "clear"]);
        assert_eq!(registry.active_gateway_id.as_deref(), Some("local"));
    }
    #[test]
    fn failed_registry_commit_preserves_retry_without_exchange() {
        let (mut registry, storage, record) = fixture();
        let before = registry.clone();
        recover_gateway_bindings(
            &mut registry,
            std::slice::from_ref(&record),
            &storage,
            |_| anyhow::bail!("synthetic"),
            |_| panic!("must retain journal"),
        );
        assert_eq!(registry, before);
        recover_gateway_bindings(&mut registry, &[record], &storage, |_| Ok(()), |_| Ok(()));
        assert!(registry.local.unwrap().session_ref.is_some());
    }
    #[test]
    fn corrupt_journal_does_not_block_independent_valid_recovery() {
        let (mut registry, storage, record) = fixture();
        let cleared = Cell::new(0);
        recover_gateway_bindings(
            &mut registry,
            &[
                GatewayBindingJournalRecord {
                    gateway_id: "invalid".into(),
                    document: "invalid".into(),
                },
                record,
            ],
            &storage,
            |_| Ok(()),
            |_| {
                cleared.set(cleared.get() + 1);
                Ok(())
            },
        );
        assert_eq!(cleared.get(), 1);
    }
    #[test]
    fn scope_replacement_and_newer_bindings_cannot_be_overwritten() {
        for mutation in 0..4 {
            let (mut registry, storage, record) = fixture();
            match mutation {
                0 => {
                    registry.local.as_mut().unwrap().gateway_base_url =
                        super::super::endpoint::GatewayBaseUrl::parse_presentation(
                            "https://changed.invalid",
                        )
                        .unwrap()
                }
                1 => {
                    registry.local.as_mut().unwrap().session_ref = Some("newer-session-key".into())
                }
                2 => registry.installation_id = Some("different-installation".into()),
                _ => {
                    registry.local.as_mut().unwrap().server_gateway_id =
                        Some(GatewayId::new("G00000000000000000002").unwrap())
                }
            }
            let before = registry.clone();
            recover_gateway_bindings(
                &mut registry,
                &[record],
                &storage,
                |_| panic!("scope mismatch"),
                |_| panic!("must retain journal"),
            );
            assert_eq!(registry, before);
        }
    }
    #[test]
    fn rejects_unknown_secret_fields_nullable_endpoint_and_previous_terminal_envelope() {
        for mutation in 0..4 {
            let (mut registry, storage, mut record) = fixture();
            let mut value: serde_json::Value = serde_json::from_str(&record.document).unwrap();
            match mutation {
                0 => value["refresh_token"] = json!("synthetic forbidden field"),
                1 => value["endpoint"]["refresh_token"] = json!("synthetic forbidden field"),
                2 => value["endpoint"] = serde_json::Value::Null,
                _ => value["previous_session_id"] = json!(grant().session.id),
            }
            record.document = value.to_string();
            recover_gateway_bindings(
                &mut registry,
                &[record],
                &storage,
                |_| panic!("invalid journal"),
                |_| panic!("invalid journal"),
            );
            assert!(registry.local.unwrap().session_ref.is_none());
        }
    }
}
