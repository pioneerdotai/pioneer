//! Live immutable row ownership. Order is a coordinate, never retained identity.
use pioneer_client::timeline::presentation::{
    RowId, TimelineChangeSet, TimelineRowSnapshot, TimelineSnapshot,
};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub(crate) struct TimelineRowSlotView {
    snapshot: Arc<TimelineRowSnapshot>,
    pub(super) projection: pioneer_client::conversation::ConversationViewState,
    pub(super) content: super::TimelineItemPresentations,
    pub(super) view: Option<gpui_kit::Entity<super::row_view::TimelineRowView>>,
}
impl TimelineRowSlotView {
    fn new(snapshot: Arc<TimelineRowSnapshot>) -> Self {
        let mut projection = pioneer_client::conversation::ConversationViewState::default();
        let mut content = HashMap::new();
        if let Some(item) = snapshot.item() {
            projection
                .timeline
                .push(pioneer_client::conversation::TimelineEntry {
                    id: snapshot.id().as_str().to_owned(),
                    turn_id: item.turn_id.clone(),
                    item_id: item.id.clone(),
                    item_index: 0,
                });
            projection.items.push(item.clone());
            if snapshot.content().is_some() {
                content.insert(item.id.clone(), snapshot.clone());
            }
        }
        Self {
            snapshot,
            projection,
            content,
            view: None,
        }
    }

    pub(crate) fn render(
        &self,
        _height: gpui_kit::Pixels,
        _cx: &gpui_kit::App,
    ) -> gpui_kit::AnyElement {
        use gpui_kit::{prelude::*, *};
        let Some(view) = &self.view else {
            return div().into_any_element();
        };
        view.clone().into_any_element()
    }

    fn replacing(snapshot: Arc<TimelineRowSnapshot>, previous: Option<&Arc<Self>>) -> Self {
        let mut next = Self::new(snapshot);
        next.view = previous.and_then(|old| old.view.clone());
        next
    }
    pub(crate) fn snapshot(&self) -> &Arc<TimelineRowSnapshot> {
        &self.snapshot
    }
}

#[derive(Default)]
pub(crate) struct TimelineRowRegistry {
    thread_id: Option<String>,
    generation: u64,
    revision: u64,
    entries: HashMap<RowId, Arc<TimelineRowSlotView>>,
    order: Vec<RowId>,
}
impl TimelineRowRegistry {
    pub(crate) fn reset(&mut self, snapshot: &TimelineSnapshot) {
        let same_owner = self.thread_id.as_deref() == Some(snapshot.thread_id())
            && self.generation == snapshot.generation();
        let mut previous = if same_owner {
            std::mem::take(&mut self.entries)
        } else {
            HashMap::new()
        };
        self.entries = snapshot
            .rows()
            .iter()
            .map(|row| {
                let old = previous.remove(row.id());
                let slot = old
                    .as_ref()
                    .filter(|slot| slot.snapshot.revision() == row.revision())
                    .cloned()
                    .unwrap_or_else(|| {
                        Arc::new(TimelineRowSlotView::replacing(row.clone(), old.as_ref()))
                    });
                (row.id().clone(), slot)
            })
            .collect();
        self.thread_id = Some(snapshot.thread_id().to_owned());
        self.generation = snapshot.generation();
        self.revision = snapshot.revision();
        self.order = snapshot.rows().iter().map(|row| row.id().clone()).collect();
    }
    pub(crate) fn matches(&self, snapshot: &TimelineSnapshot) -> bool {
        self.thread_id.as_deref() == Some(snapshot.thread_id())
            && self.generation == snapshot.generation()
            && self.revision == snapshot.revision()
    }
    /// Reject the entire transaction before changing any retained owner.
    pub(crate) fn apply(&mut self, change: &TimelineChangeSet) -> bool {
        if self.thread_id.as_deref() != Some(&change.thread_id)
            || self.generation != change.generation
            || self.revision != change.from_revision
            || change.to_revision <= change.from_revision
        {
            return false;
        }
        for id in &change.removed {
            self.entries.remove(id);
        }
        for row in change.inserted.iter().chain(&change.replaced) {
            self.entries.insert(
                row.id().clone(),
                Arc::new(TimelineRowSlotView::replacing(
                    row.clone(),
                    self.entries.get(row.id()),
                )),
            );
        }
        if let Some(order) = &change.order {
            self.order.clone_from(order);
        }
        self.revision = change.to_revision;
        true
    }
    /// Attach a retained child only when its immutable row is still current.
    pub(crate) fn publish_slot(&mut self, slot: Arc<TimelineRowSlotView>) {
        if let Some(current) = self.entries.get_mut(slot.snapshot.id())
            && Arc::ptr_eq(&current.snapshot, &slot.snapshot)
        {
            *current = slot;
        }
    }
    pub(crate) fn slots(&self) -> Vec<Arc<TimelineRowSlotView>> {
        self.order
            .iter()
            .map(|id| self.entries.get(id).expect("live ordered row").clone())
            .collect()
    }
    pub(crate) fn get(&self, id: &RowId) -> Option<&Arc<TimelineRowSlotView>> {
        self.entries.get(id)
    }
}

#[cfg(test)]
impl TimelineRowSlotView {
    pub(crate) fn terminal_for_test(
        &self,
        cx: &gpui_kit::App,
    ) -> Option<gpui_kit::Entity<terminal::TerminalView>> {
        self.view
            .as_ref()
            .and_then(|view| view.read(cx).terminal_for_test())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::core::{ClientCore, ClientScope, ClientSubscriptionEvent};
    fn scope(thread: &str) -> ClientScope {
        ClientScope::Timeline {
            thread_id: thread.into(),
        }
    }
    fn snapshot(client: &ClientCore, thread: &str) -> Arc<TimelineSnapshot> {
        client
            .snapshot(&scope(thread))
            .unwrap()
            .snapshot()
            .payload()
            .unwrap()
    }
    fn page(client: &Arc<ClientCore>, thread: &str, items: &[(&str, &str)]) {
        let blocks = items.iter().enumerate().map(|(at, (id, text))| serde_json::json!({
            "workspaceId":"workspace", "threadId":thread, "blockId":id, "turnId":"turn", "sortKey":format!("{at:03}"),
            "kind":{"kind":"user_message","text":text,"mode":"Message"}
        })).collect::<Vec<_>>();
        client.apply_thread_timeline_page(serde_json::from_value(serde_json::json!({
            "workspaceId":"workspace", "threadId":thread, "projectionVersion":1, "blocks":blocks,
            "page":{"hasMoreBefore":false,"hasMoreAfter":false}
        })).unwrap(), pioneer_client::timeline::semantic::TopLevelPageMergeMode::Reset);
        let _flush = client.subscribe(scope(thread), std::num::NonZeroUsize::new(8).unwrap());
    }
    #[test]
    fn scoped_delivery_reconciles_only_named_slots_and_preserves_moved_identity() {
        let client = Arc::new(ClientCore::new());
        crate::test_support::install_thread_timeline(&client, "a", "initial");
        crate::test_support::install_thread_timeline(&client, "b", "unrelated");
        let _a_lease = client.subscribe(scope("a"), std::num::NonZeroUsize::new(8).unwrap());
        let _b_lease = client.subscribe(scope("b"), std::num::NonZeroUsize::new(8).unwrap());
        page(&client, "a", &[("one", "one"), ("two", "two")]);
        let mut a = TimelineRowRegistry::default();
        a.reset(&snapshot(&client, "a"));
        let mut b = TimelineRowRegistry::default();
        b.reset(&snapshot(&client, "b"));
        let original = a.slots();
        let other = b.slots();
        let subscription = client.subscribe(scope("a"), std::num::NonZeroUsize::new(8).unwrap());
        let b_subscription = client.subscribe(scope("b"), std::num::NonZeroUsize::new(8).unwrap());
        page(
            &client,
            "a",
            &[("new", "new"), ("two", "two"), ("one", "changed")],
        );
        let ClientSubscriptionEvent::Publication { publication, .. } =
            subscription.try_next().unwrap()
        else {
            panic!("incremental delivery")
        };
        let delta = publication
            .timeline_change()
            .expect("copied transaction on delivery");
        assert!(a.apply(&delta));
        assert!(!a.apply(&delta));
        assert!(!b.apply(&delta));
        assert!(
            client
                .snapshot(&scope("a"))
                .unwrap()
                .timeline_change()
                .is_none()
        );
        assert!(b_subscription.try_next().is_none());
        assert_eq!(delta.replaced.len(), 1);
        assert_eq!(delta.inserted.len(), 1);
        let current = a.slots();
        assert!(Arc::ptr_eq(&current[1], &original[1]));
        assert_eq!(current[2].snapshot.id(), original[0].snapshot.id());
        assert!(!Arc::ptr_eq(&current[2], &original[0]));
        assert!(Arc::ptr_eq(&other[0], &b.slots()[0]));
        // A resnapshot after a delivery gap retains the equal live owner too.
        a.reset(&snapshot(&client, "a"));
        assert!(Arc::ptr_eq(&current[1], &a.slots()[1]));
        page(&client, "a", &[("two", "two")]);
        let ClientSubscriptionEvent::Publication { publication, .. } =
            subscription.try_next().unwrap()
        else {
            panic!()
        };
        assert!(a.apply(&publication.timeline_change().unwrap()));
        assert_eq!(a.entries.len(), 1);
        assert!(Arc::ptr_eq(&current[1], &a.slots()[0]));
    }
}
