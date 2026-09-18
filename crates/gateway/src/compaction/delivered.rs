//! Current authorization for exact, already acknowledged Task output snapshots.
//! Delivery identity is a locator, never a grant to read the child's originals.
use super::*;
use crate::{
    auth::AuthenticatedSessionPrincipal,
    authorization::{
        AuthorizationDecision, AuthorizationResolver, AuthorizationService, DenyReason,
        ProofResolution, ResourceAction,
    },
    message::MessageProcessor,
};
use pioneer_crud::compaction::{HistoryReadFence, TaskDeliveryOutputSnapshot};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct AuthorizedOutputBranch {
    pub(super) snapshot: TaskDeliveryOutputSnapshot,
    pub(super) acknowledgement: SourceRef,
    pub(super) acknowledgements: Vec<SourceRef>,
    pub(super) source_threads: BTreeSet<String>,
}

/// Kept inside this request; this is not a reusable permission token. The
/// consumer rechecks the authorization generation before publishing its result.
pub(crate) struct AuthorizedOutputSet {
    pub(super) workspace: String,
    pub(super) destination: String,
    pub(super) fence: HistoryReadFence,
    pub(super) authorization_revision: u64,
    pub(super) source_epochs: BTreeMap<String, u64>,
    pub(super) branches: Vec<AuthorizedOutputBranch>,
}

async fn can_read_originals(
    resolver: &AuthorizationResolver,
    principal: &AuthenticatedSessionPrincipal,
    workspace: &str,
    thread: &str,
) -> Result<bool> {
    let action = ResourceAction::ThreadRead;
    let gate = AuthorizationService::new().authorize_action(
        principal.kind,
        principal.role_key.as_ref(),
        action,
    );
    let proof = resolver
        .authorize_thread(principal, &gate, action, thread, Some(workspace))
        .await?;
    if matches!(
        proof.denial(),
        Some(AuthorizationDecision::Deny {
            reason: DenyReason::MissingAuthoritativeResource,
            ..
        })
    ) {
        return Ok(matches!(
            resolver
                .authorize_internal_thread_via_root(
                    principal,
                    &gate,
                    action,
                    thread,
                    Some(workspace)
                )
                .await?,
            ProofResolution::Authorized(_)
        ));
    }
    Ok(matches!(proof, ProofResolution::Authorized(_)))
}

impl MessageProcessor {
    /// Select only a queue-bound manifest acknowledged before the common fence,
    /// then authorize every original source scope before reading any payload.
    /// A denied branch leaves its previously delivered summary in the context.
    pub(crate) async fn authorize_delivered_output_branches(
        &self,
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        workspace: &str,
        destination: &str,
    ) -> Result<AuthorizedOutputSet> {
        let authorization_revision = self.current_authorization_revision().await?;
        let resolver = AuthorizationResolver::new(store.clone());
        let mut access = BTreeMap::new();
        let mut branches: Vec<AuthorizedOutputBranch> = Vec::new();
        let mut seen = BTreeMap::<String, Option<usize>>::new();
        let mut after = 0;
        let destination_read =
            can_read_originals(&resolver, principal, workspace, destination).await?;
        ensure!(
            destination_read,
            "destination history is unavailable or access is denied"
        );
        super::history::prepare_history(&store, workspace, destination).await?;
        let epoch = store
            .compaction_projection_version(workspace, destination)
            .await?;
        let fence = store.compaction_history_read_fence().await?;
        let mut source_epochs = BTreeMap::from([(destination.to_owned(), epoch)]);
        access.insert(destination.to_owned(), true);
        loop {
            let mut page = store
                .compaction_delivered_output_page(workspace, destination, after, &fence)
                .await?;
            if !page.unprojected_events.is_empty() {
                for source in &page.unprojected_events {
                    let payload =
                        super::history::reference_payload(&store, workspace, destination, source)
                            .await?;
                    let event: pioneer_crud::CanonicalTurnEventPayload =
                        serde_json::from_str(&payload)?;
                    ensure!(
                        store
                            .compaction_record_event_projection(
                                workspace,
                                destination,
                                source,
                                &event
                            )
                            .await?,
                        "delivery event changed while refreshing its metadata"
                    );
                }
                page = store
                    .compaction_delivered_output_page(workspace, destination, after, &fence)
                    .await?;
                ensure!(
                    page.unprojected_events.is_empty(),
                    "delivery metadata changed during discovery"
                );
            }
            for reference in page.entries {
                if let Some(accepted) = seen.get(&reference.delivery_id) {
                    if let Some(index) = accepted {
                        branches[*index]
                            .acknowledgements
                            .push(reference.acknowledgement);
                    }
                    continue;
                }
                seen.insert(reference.delivery_id.clone(), None);
                let snapshot = store
                    .compaction_delivery_output(workspace, &reference.delivery_id)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!("acknowledged Task source binding disappeared")
                    })?;
                ensure!(
                    snapshot.candidate_id == reference.candidate_id
                        && snapshot.output.task_run_turn_id == reference.task_run_turn_id
                        && snapshot.output.source_thread == reference.source_thread
                        && snapshot.output.source_turn == reference.source_turn,
                    "acknowledged Task source binding changed"
                );
                let source_threads = super::frozen::accepted_history_scopes(
                    &store,
                    workspace,
                    &snapshot.output.source_thread,
                    &serde_json::to_string(&snapshot.output.history)?,
                )
                .await?;
                let mut permitted = true;
                for thread in &source_threads {
                    if !access.contains_key(thread) {
                        access.insert(
                            thread.clone(),
                            can_read_originals(&resolver, principal, workspace, thread).await?,
                        );
                    }
                    if !access[thread] {
                        permitted = false;
                        break;
                    }
                }
                if !permitted {
                    continue;
                }
                for thread in &source_threads {
                    if !source_epochs.contains_key(thread) {
                        source_epochs.insert(
                            thread.clone(),
                            store
                                .compaction_projection_version(workspace, thread)
                                .await?,
                        );
                    }
                }
                seen.insert(reference.delivery_id, Some(branches.len()));
                branches.push(AuthorizedOutputBranch {
                    snapshot,
                    acknowledgements: vec![reference.acknowledgement.clone()],
                    acknowledgement: reference.acknowledgement,
                    source_threads,
                });
            }
            if page.done {
                break;
            }
            ensure!(
                page.scanned_through > after,
                "Task output discovery made no progress"
            );
            after = page.scanned_through;
        }
        ensure!(
            self.current_authorization_revision().await? == authorization_revision,
            "authorization changed while selecting Task originals"
        );
        ensure!(
            store
                .compaction_projection_version(workspace, destination)
                .await?
                == epoch,
            "delivery history changed while selecting Task originals"
        );
        Ok(AuthorizedOutputSet {
            workspace: workspace.into(),
            destination: destination.into(),
            fence,
            authorization_revision,
            source_epochs,
            branches,
        })
    }
    /// Return the descriptor only after rechecking the same authorization
    /// generation that admitted all foreign originals. Runtime callers switch
    /// to this entry point together with accepted-source CAS support.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn capture_authorized_task_basis(
        &self,
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        workspace: &str,
        destination: &str,
        basis_turn: Option<&str>,
        excluded_turn: Option<&str>,
        policy: Option<&pioneer_protocol::TaskAgentContextPolicy>,
    ) -> Result<String> {
        let prepared = self
            .capture_authorized_task_basis_prepared(
                store,
                principal,
                workspace,
                destination,
                basis_turn,
                excluded_turn,
                policy,
            )
            .await?;
        Ok(serde_json::to_string(&prepared.descriptor)?)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn capture_authorized_task_basis_prepared(
        &self,
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        workspace: &str,
        destination: &str,
        basis_turn: Option<&str>,
        excluded_turn: Option<&str>,
        policy: Option<&pioneer_protocol::TaskAgentContextPolicy>,
    ) -> Result<super::frozen::PreparedHistory> {
        if policy.is_some_and(|policy| {
            matches!(
                policy.mode,
                pioneer_protocol::TaskAgentContextMode::Empty
                    | pioneer_protocol::TaskAgentContextMode::Custom
            ) || (policy.mode == pioneer_protocol::TaskAgentContextMode::SummaryOnly
                && !policy.include_parent_summary)
        }) {
            return super::frozen::capture_execution_basis_prepared(
                store,
                workspace,
                destination,
                basis_turn,
                excluded_turn,
                policy,
            )
            .await;
        }
        let outputs = self
            .authorize_delivered_output_branches(store, principal, workspace, destination)
            .await?;
        let prepared = super::frozen::capture_execution_basis_prepared_with_outputs(
            store,
            workspace,
            destination,
            basis_turn,
            excluded_turn,
            policy,
            Some(&outputs),
        )
        .await?;
        ensure!(
            self.current_authorization_revision().await? == outputs.authorization_revision,
            "authorization changed while freezing Task originals"
        );
        Ok(prepared)
    }
}
