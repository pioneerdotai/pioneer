//! Versioned DTO translation for the process-local Client binding.

use serde::{Deserialize, Serialize};

use pioneer_client::core::{
    ClientChangeSequence, ClientEffectCancellation, ClientEffectCompletion, ClientEffectPlan,
    ClientIntent, ClientPublicationReference, ClientScope, ClientSubscriptionEvent,
    ClientTransition, ClientTransitionOutcome,
};

pub const CLIENT_BINDING_SCHEMA_VERSION: u32 = 1;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientScopeLeaseRequestDto {
    pub schema_version: u32,
    pub scope: ClientScope,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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
    #[serde(default)]
    pub timeline_changes: Vec<pioneer_client::timeline::presentation::TimelineChangeSet>,
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
    /// Command receipt from Begin's own immutable publication, independent of delivery.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub composer_begin_plan: Option<pioneer_client::composer::store::ComposerOperationPlan>,
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
    let mut payload = snapshot.serialized_payload().as_ref().clone();
    if matches!(snapshot.scope(), ClientScope::Thread { .. }) {
        if let Some(fields) = payload.as_object_mut() {
            fields.remove("semantic");
        }
        if let Some(projection) = payload
            .get_mut("projection")
            .and_then(serde_json::Value::as_object_mut)
        {
            projection.remove("items");
            projection.remove("timeline");
        }
    }
    ClientScopedSnapshotDto {
        schema_version: CLIENT_BINDING_SCHEMA_VERSION,
        sequence: snapshot.sequence(),
        scope: snapshot.scope().clone(),
        revisions: snapshot.revisions(),
        payload,
    }
}

pub fn transition_dto(transition: ClientTransition) -> ClientTransitionDto {
    ClientTransitionDto {
        schema_version: CLIENT_BINDING_SCHEMA_VERSION,
        sequence: transition.sequence(),
        outcome: transition.outcome(),
        effects: transition.effects().to_vec(),
        composer_begin_plan: None,
    }
}

pub fn intent_transition_dto(
    intent: &ClientIntent,
    transition: ClientTransition,
) -> ClientTransitionDto {
    let mut result = transition_dto(transition.clone());
    if let ClientIntent::Composer {
        intent: pioneer_client::composer::store::ComposerIntent::BeginOperation { thread_id, .. },
    } = intent
    {
        result.composer_begin_plan =
            transition
                .changes()
                .publications()
                .iter()
                .find_map(|publication| {
                    if publication.scope()
                        != &(ClientScope::Composer {
                            thread_id: thread_id.clone(),
                        })
                    {
                        return None;
                    }
                    publication
                        .snapshot()
                        .payload::<pioneer_client::composer::store::ComposerPublication>()?
                        .operation()?
                        .plan
                        .clone()
                });
    }
    result
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

/// Steady-state timeline delivery carries a header plus exact row changes.
pub fn publication_dto(
    publication: ClientPublicationReference,
    incremental: bool,
) -> ClientScopedSnapshotDto {
    if !incremental {
        return snapshot_dto(publication);
    }
    let snapshot = publication.snapshot();
    let payload = publication
        .typed::<pioneer_client::timeline::presentation::TimelineSnapshot>()
        .map(|snapshot| snapshot.payload().serialized_header())
        .unwrap_or_else(|| snapshot.serialized_payload().as_ref().clone());
    ClientScopedSnapshotDto {
        schema_version: CLIENT_BINDING_SCHEMA_VERSION,
        sequence: snapshot.sequence(),
        scope: snapshot.scope().clone(),
        revisions: snapshot.revisions(),
        payload,
    }
}

#[cfg(test)]
mod begin_receipt_tests {
    use super::*;
    use pioneer_client::composer::store::{ComposerIntent, ComposerOperationKind};

    #[test]
    fn begin_receipt_uses_its_immutable_transition_even_after_replacement() {
        let core = pioneer_client::core::ClientCore::shared();
        core.composer_intent(ComposerIntent::Open {
            thread_id: "receipt".into(),
            defaults: Default::default(),
        });
        let draft_id = core.composer_snapshot("receipt").unwrap().draft_id();
        let edit = |text: &str| ComposerIntent::EditText {
            thread_id: "receipt".into(),
            draft_id,
            text: text.into(),
        };
        core.composer_intent(edit("captured payload"));
        let begin = ClientIntent::Composer {
            intent: ComposerIntent::BeginOperation {
                thread_id: "receipt".into(),
                draft_id,
                operation: ComposerOperationKind::Send,
            },
        };
        let accepted = core.dispatch(begin.clone());
        let captured = core
            .composer_snapshot("receipt")
            .unwrap()
            .operation()
            .unwrap()
            .plan
            .clone()
            .unwrap();
        assert_eq!(accepted.outcome(), ClientTransitionOutcome::Changed);
        assert_eq!(captured.draft.text, "captured payload");
        let duplicate = intent_transition_dto(&begin, core.dispatch(begin.clone()));
        assert_eq!(duplicate.outcome, ClientTransitionOutcome::Noop);
        assert!(duplicate.composer_begin_plan.is_none());
        let edit = ClientIntent::Composer {
            intent: edit("new payload"),
        };
        let edited = intent_transition_dto(&edit, core.dispatch(edit.clone()));
        assert!(edited.composer_begin_plan.is_none());
        let replacement = intent_transition_dto(&begin, core.dispatch(begin.clone()))
            .composer_begin_plan
            .unwrap();
        assert_ne!(captured.identity, replacement.identity);
        assert_eq!(replacement.draft.text, "new payload");
        core.shutdown();
        // Receipt serialization has no current snapshot read: teardown and a
        // newer operation cannot replace the accepted command's ownership.
        let receipt = intent_transition_dto(&begin, accepted);
        assert_eq!(receipt.composer_begin_plan.as_ref(), Some(&captured));
        assert_eq!(
            serde_json::to_value(receipt).unwrap()["composer_begin_plan"],
            serde_json::to_value(&captured).unwrap()
        );
    }
}
