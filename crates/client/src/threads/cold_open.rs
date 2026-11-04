//! Bounded, request-owned ingress before an ID-only open establishes workspace.
//! These are domain reductions, not an alternative notification queue/worker.
use super::*;

// Defensive admission limits for pre-authority retention, not throughput claims.
// Eight flights retain at most 16 MiB of encoded event payload in total. Object
// overhead is additionally bounded by the per-flight event count. Lifetime is
// the existing subscription deadline; overflow fails the flight before receipt.
pub(super) const MAX_COLD_OPEN_FLIGHTS: usize = 8;
const MAX_EVENTS: usize = 128;
const MAX_BYTES: usize = 2 * 1024 * 1024;

pub(super) enum ColdIngress {
    Notification(GatewayNotification),
    Metadata(pioneer_protocol::ThreadMetadataPatch),
}
#[derive(Default)]
pub(super) struct ColdOpenStaging {
    pub(super) events: Vec<ColdIngress>,
    bytes: usize,
    pub(super) overflow: bool,
}
impl ColdOpenStaging {
    pub(super) fn push(&mut self, event: ColdIngress, bytes: usize) {
        if self.overflow {
            return;
        }
        if self.events.len() >= MAX_EVENTS || bytes > MAX_BYTES.saturating_sub(self.bytes) {
            self.events.clear();
            self.bytes = 0;
            self.overflow = true;
            return;
        }
        self.bytes += bytes;
        self.events.push(event);
    }
    pub(super) fn contains_closed(&self, id: &str, workspace: &str) -> bool {
        self.events.iter().any(|event| matches!(event, ColdIngress::Notification(GatewayNotification::ThreadClosed(n)) if n.thread_id == id && n.workspace_id == workspace))
    }
}

#[derive(Default)]
pub(super) struct ColdApplyEffects {
    pub(super) pending: Vec<PendingRequestsReduction>,
    pub(super) drafts: Vec<ClientPublicationDraft>,
    pub(super) actions: Vec<SemanticTimelineRequestAction>,
}

impl ClientCore {
    /// Return None only when this event never belonged to a cold flight. Once
    /// captured, replacement/retirement consumes it rather than recreating state.
    pub(super) fn capture_cold_thread_notification(
        &self,
        notification: &GatewayNotification,
    ) -> Option<bool> {
        use crate::runtime::ClientRuntimeNotification as R;
        if !matches!(
            crate::runtime::reduce_gateway_notification(
                notification.clone(),
                crate::runtime::ClientRuntimeNotificationContext {
                    notification_thread_workspace_matches: true,
                    ..Default::default()
                }
            ),
            Some(
                R::ThreadStarted(_)
                    | R::ThreadUpdated(_)
                    | R::ThreadClosed(_)
                    | R::ConversationEvent(_)
                    | R::TurnLifecycle(_)
                    | R::SemanticTimeline(_)
                    | R::CLIRuntimePendingRequests(_)
                    | R::PendingRequests { .. }
            )
        ) {
            return None;
        }
        let id = notification_thread_id(notification)?;
        let workspace = notification_workspace_id(notification)?;
        let request = {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let request = registry.stores.get(id)?.subscription_request.as_ref()?;
            if request
                .flight
                .lock()
                .expect("subscription flight poisoned")
                .cold_staging
                .is_none()
            {
                return None;
            }
            request.clone()
        };
        // Serialization is outside identity/registry/flight locks.
        let bytes = serde_json::to_vec(notification).map_or(usize::MAX, |bytes| bytes.len());
        let identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || identity.authorization_epoch() != request.authority
            || !identity.history_ticket_matches(request.auth.0)
        {
            return Some(true);
        }
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let (_transport_guard, current) = request.transport.history_apply_fence(request.auth.1);
        if !current || !*request.live.lock().expect("subscription receipt poisoned") {
            return Some(true);
        }
        let Some(store) = registry.stores.get(id).filter(|store| {
            store.generation == request.generation
                && (store.subscription_request.as_ref() == Some(&request)
                    || store
                        .subscription_receipt
                        .as_ref()
                        .is_some_and(|receipt| Arc::ptr_eq(&receipt.live, &request.live)))
        }) else {
            return Some(true);
        };
        let mut flight = request.flight.lock().expect("subscription flight poisoned");
        if let Some(staged) = &mut flight.cold_staging {
            staged.push(ColdIngress::Notification(notification.clone()), bytes);
            return Some(true);
        }
        // ACK won while serialization was in progress. Apply in this same
        // guarded commit, with the now authoritative scope, without re-entry.
        if store.coordinator.workspace_id != workspace {
            return Some(true);
        }
        drop(flight);
        let effects = self.apply_cold_ingress_locked(
            &mut registry,
            id,
            workspace,
            ColdIngress::Notification(notification.clone()),
        );
        let mut drafts = self
            .apply_pending_requests_batch_locked(&mut registry, effects.pending)
            .1;
        drafts.extend(effects.drafts);
        drafts.extend(registry.publish(id));
        drop(_transport_guard);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        drop(registry);
        drop(identity);
        for action in effects.actions {
            let _ = self.schedule_thread_semantic_request_inner(
                action,
                Some(request.generation),
                true,
                false,
            );
        }
        self.retire_thread_controller_jobs();
        self.refresh_thread_timeline_generation(id, Some(request.generation));
        self.refresh_approval_actions();
        Some(true)
    }

    pub(super) fn apply_cold_ingress_locked(
        &self,
        registry: &mut ThreadRegistry,
        id: &str,
        workspace: &str,
        event: ColdIngress,
    ) -> ColdApplyEffects {
        let notification = match event {
            ColdIngress::Metadata(patch) => {
                let Some(store) = registry.stores.get_mut(id) else {
                    return Default::default();
                };
                if patch.thread.id != id || patch.thread.workspace_id != workspace {
                    return Default::default();
                }
                if store
                    .coordinator
                    .thread()
                    .is_some_and(|old| old.updated_at > patch.thread.updated_at)
                {
                    return Default::default();
                }
                let mut thread = patch.thread;
                if patch.preview_truncated
                    && let Some(old) = store.coordinator.thread()
                    && old.preview.len() > thread.preview.len()
                    && old.preview.starts_with(&thread.preview)
                {
                    thread.preview = old.preview.clone();
                }
                store.set_snapshot(thread);
                return Default::default();
            }
            ColdIngress::Notification(notification) => notification,
        };
        if notification_thread_id(&notification) != Some(id)
            || notification_workspace_id(&notification) != Some(workspace)
        {
            return Default::default();
        }
        let context = crate::runtime::ClientRuntimeNotificationContext {
            notification_thread_workspace_matches: true,
            ..Default::default()
        };
        use crate::runtime::ClientRuntimeNotification::*;
        match crate::runtime::reduce_gateway_notification(notification, context) {
            Some(ThreadStarted(r)) => registry
                .stores
                .get_mut(id)
                .expect("open store")
                .reconcile_subscription_snapshot(None, r.thread),
            Some(ThreadUpdated(r)) => {
                let store = registry.stores.get_mut(id).expect("open store");
                if store
                    .coordinator
                    .thread()
                    .is_some_and(|old| old.updated_at > r.thread.updated_at)
                {
                    return Default::default();
                }
                store.set_snapshot(r.thread);
                if let Some(placement) = r
                    .placement
                    .filter(|p| p.thread_id == id && p.workspace_id == workspace)
                {
                    if registry.directory.placements.get(id) != Some(&placement) {
                        registry.directory.upsert_placement(placement);
                        registry
                            .directory
                            .dirty_workspaces
                            .insert(workspace.to_owned());
                    }
                }
            }
            Some(ThreadClosed(r)) => {
                if let Some(store) = registry.stores.remove(id) {
                    store.retire_subscription();
                }
                registry.timeline.invalidate(Some(id));
                let drafts = registry.retire(id);
                return ColdApplyEffects {
                    pending: r.pending_requests.into_iter().collect(),
                    drafts,
                    ..Default::default()
                };
            }
            Some(ConversationEvent(r)) => {
                let _ = registry
                    .stores
                    .get_mut(id)
                    .expect("open store")
                    .apply_conversation_event(workspace, r.conversation_event, None);
            }
            Some(TurnLifecycle(r)) => {
                let store = registry.stores.get_mut(id).expect("open store");
                if let (Some(status), Some(thread)) =
                    (r.thread_status, store.coordinator.thread_mut())
                {
                    thread.status = status;
                }
                if r.reset_thread_resume {
                    super::super::resume::reset_thread_resume_coordinator(
                        &mut store.coordinator.resume,
                    );
                }
                let actions = crate::timeline::semantic::terminal_turn_work_reconciliation(
                    &store.semantic,
                    &r.conversation_event,
                )
                .map(
                    |reconciliation| SemanticTimelineRequestAction::TurnWorkItemsGet {
                        key: SemanticTimelineRequestKey::TurnWorkItems {
                            thread_id: reconciliation.thread_id.clone(),
                            turn_id: reconciliation.turn_id.clone(),
                        },
                        params: pioneer_protocol::TurnWorkItemsGetParams {
                            thread_id: reconciliation.thread_id,
                            turn_id: reconciliation.turn_id,
                            work_item_ids: reconciliation.running_work_item_ids,
                        },
                    },
                )
                .into_iter()
                .collect();
                let _ = store.apply_conversation_event(workspace, r.conversation_event, None);
                if r.tick_conversation {
                    store.coordinator.conversation.tick();
                }
                if r.promote_thread_from_draft {
                    registry
                        .navigation
                        .apply(crate::navigation::NavigationIntent::PromoteThread {
                            thread_id: id.to_owned(),
                        });
                }
                let drafts = registry.navigation_change().into_iter().collect();
                return ColdApplyEffects {
                    pending: r.pending_requests.into_iter().collect(),
                    drafts,
                    actions,
                };
            }
            Some(SemanticTimeline(update)) => {
                use crate::timeline::semantic::SemanticTimelineLiveUpdate::*;
                let store = registry.stores.get_mut(id).expect("open store");
                match &update {
                    ThreadTimelineBlocksChanged(n) => {
                        registry
                            .timeline
                            .record_removals(id, None, &n.removed_block_ids)
                    }
                    TurnWorkItemsChanged(n)
                        if crate::timeline::semantic::accepts_turn_work_items_change(
                            &store.semantic,
                            id,
                            &n.turn_id,
                        ) =>
                    {
                        registry.timeline.record_removals(
                            id,
                            Some(&n.turn_id),
                            &n.removed_work_item_ids,
                        )
                    }
                    _ => {}
                }
                crate::timeline::semantic::apply_semantic_timeline_live_update(
                    &mut store.semantic,
                    update.clone(),
                );
                let actions = thread_reconcile_actions(&store.semantic, &update);
                registry.retire_unauthorized_targeted_requests(id);
                let store = &registry.stores[id];
                if !store.presentation_blocked && store.demand == ClientDemand::Visible {
                    registry.timeline.rearm_exhausted_refresh(id);
                }
                return ColdApplyEffects {
                    actions,
                    ..Default::default()
                };
            }
            Some(CLIRuntimePendingRequests(r)) | Some(PendingRequests { reduction: r }) => {
                return ColdApplyEffects {
                    pending: vec![r],
                    ..Default::default()
                };
            }
            _ => {}
        }
        Default::default()
    }
}
