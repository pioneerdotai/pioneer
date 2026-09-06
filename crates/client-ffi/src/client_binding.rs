//! Versioned DTO translation for the process-local Client binding.

use serde::{Deserialize, Serialize};

use pioneer_client::core::{
    ClientChangeSequence, ClientEffectCancellation, ClientEffectCompletion, ClientEffectPlan,
    ClientIntent, ClientPublicationReference, ClientScope, ClientSubscriptionEvent,
    ClientTransition, ClientTransitionOutcome,
};

pub const CLIENT_BINDING_SCHEMA_VERSION: u32 = 1;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientIntentDispatchDto {
    pub schema_version: u32,
    pub intent: ClientIntent,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientScopedSnapshotRequestDto {
    pub schema_version: u32,
    pub scope: ClientScope,
    pub after_revision: Option<pioneer_client::core::ScopedRevision>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientChangeBatchRequestDto {
    pub schema_version: u32,
    pub scope: ClientScope,
    pub maximum_items: u16,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientEffectCompletionDto {
    pub schema_version: u32,
    pub completion: ClientEffectCompletion,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientEffectCancellationDto {
    pub schema_version: u32,
    pub cancellation: ClientEffectCancellation,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientSequenceGapResnapshotDto {
    pub schema_version: u32,
    pub scope: ClientScope,
    pub received_predecessor: Option<ClientChangeSequence>,
    pub last_applied_sequence: Option<ClientChangeSequence>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ClientScopedSnapshotDto {
    pub schema_version: u32,
    pub sequence: ClientChangeSequence,
    pub scope: ClientScope,
    pub revisions: pioneer_client::core::ClientRevisions,
    pub payload: serde_json::Value,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientChangeDto {
    Publication {
        sequence: ClientChangeSequence,
        predecessor: Option<ClientChangeSequence>,
        snapshot: ClientScopedSnapshotDto,
    },
    ResnapshotRequired {
        scope: ClientScope,
        latest_sequence: ClientChangeSequence,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ClientChangeBatchDto {
    pub schema_version: u32,
    pub changes: Vec<ClientChangeDto>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientPublicationWaitRequestDto {
    pub schema_version: u32,
    pub after_sequence: ClientChangeSequence,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct ClientProcessChangeSetDto {
    pub sequence: ClientChangeSequence,
    pub predecessor: Option<ClientChangeSequence>,
    pub snapshots: Vec<ClientScopedSnapshotDto>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct ClientProcessChangeBatchDto {
    pub closed: bool,
    pub effects: Vec<ClientEffectPlan>,
    pub schema_version: u32,
    pub sequence: ClientChangeSequence,
    pub resnapshot: bool,
    pub changes: Vec<ClientProcessChangeSetDto>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ClientTransitionDto {
    pub schema_version: u32,
    pub sequence: pioneer_client::core::ClientTransitionSequence,
    pub outcome: ClientTransitionOutcome,
    pub effects: Vec<ClientEffectPlan>,
}

pub fn validate_schema_version(schema_version: u32) -> Result<(), String> {
    if schema_version != CLIENT_BINDING_SCHEMA_VERSION {
        return Err(format!(
            "unsupported Client binding schema version: {schema_version}"
        ));
    }
    Ok(())
}

pub fn snapshot_dto(publication: ClientPublicationReference) -> ClientScopedSnapshotDto {
    let snapshot = publication.snapshot();
    ClientScopedSnapshotDto {
        schema_version: CLIENT_BINDING_SCHEMA_VERSION,
        sequence: snapshot.sequence(),
        scope: snapshot.scope().clone(),
        revisions: snapshot.revisions(),
        payload: snapshot.serialized_payload().as_ref().clone(),
    }
}

pub fn transition_dto(transition: ClientTransition) -> ClientTransitionDto {
    ClientTransitionDto {
        schema_version: CLIENT_BINDING_SCHEMA_VERSION,
        sequence: transition.sequence(),
        outcome: transition.outcome(),
        effects: transition.effects().to_vec(),
    }
}

pub fn change_dto(event: ClientSubscriptionEvent) -> ClientChangeDto {
    match event {
        ClientSubscriptionEvent::Publication {
            sequence,
            predecessor,
            publication,
        } => ClientChangeDto::Publication {
            sequence,
            predecessor,
            snapshot: snapshot_dto(publication),
        },
        ClientSubscriptionEvent::ResnapshotRequired {
            scope,
            latest_sequence,
        } => ClientChangeDto::ResnapshotRequired {
            scope,
            latest_sequence,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_is_fail_closed() {
        assert!(validate_schema_version(CLIENT_BINDING_SCHEMA_VERSION).is_ok());
        assert!(validate_schema_version(CLIENT_BINDING_SCHEMA_VERSION + 1).is_err());
    }

    #[test]
    fn binding_source_contains_translation_only() {
        let source = include_str!("client_binding.rs");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("Client binding production source");
        for forbidden in [
            "ClientMutationAuthority",
            "DomainRevision::new",
            "ScopedRevision::new",
            "HashMap<",
            "Mutex<",
            ".publish(",
        ] {
            assert!(
                !production.contains(forbidden),
                "Client binding contains mutable domain ownership: {forbidden}"
            );
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTransportReserveRequestDto {
    pub schema_version: u32,
    pub exclusive: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTransportLeaseRequestDto {
    pub schema_version: u32,
    pub lease_id: u64,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientAccessChangePlanRequestDto {
    pub schema_version: u32,
    pub connection_generation: u64,
    pub change_sequence: u64,
    pub active_workspace_id: Option<String>,
    pub active_thread_id: Option<String>,
    pub known_threads: Vec<pioneer_client::authorization::ThreadAuthorizationScope>,
}
