//! Window-local committed measurements and revision-guarded draw transactions.
use super::{TimelineRowLayout, layout_index::RowLayoutIndex};
use gpui_kit::{Pixels, Size, TextStyle, px, size};
use pioneer_client::timeline::presentation::RowId;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowMeasurementKey {
    pub id: RowId,
    pub dependencies_revision: u64,
    pub layout_revision: u64,
    pub content_revision: u64,
    pub presentation_revision: u64,
    pub content_width: Pixels,
    pub rem: Pixels,
    pub text_style: TextStyle,
    pub theme_revision: u64,
    pub locale: String,
    pub expanded: bool,
    pub grouping: TimelineRowLayout,
    pub last: bool,
    pub author_label: Option<String>,
    pub principal: Option<String>,
    pub task_child: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MeasurementTicket {
    pub presentation: u64,
    pub context: u64,
    pub generation: u64,
}
#[derive(Default)]
pub(crate) struct TimelineMeasurementCoordinator {
    generation: u64,
    pub draw:
        Option<Rc<RefCell<Option<Box<dyn FnOnce(&mut gpui_kit::Window, &mut gpui_kit::App)>>>>>,
    pending: Option<MeasurementTicket>,
}
impl TimelineMeasurementCoordinator {
    pub fn begin(&mut self, presentation: u64, context: u64) -> MeasurementTicket {
        self.draw = None;
        self.generation += 1;
        let ticket = MeasurementTicket {
            presentation,
            context,
            generation: self.generation,
        };
        self.pending = Some(ticket);
        ticket
    }
    pub fn is_pending(&self, ticket: MeasurementTicket) -> bool {
        self.pending == Some(ticket)
    }
    pub fn accept(&mut self, ticket: MeasurementTicket) -> bool {
        if self.pending != Some(ticket) {
            return false;
        }
        self.pending = None;
        true
    }
    pub fn cancel(&mut self) {
        self.draw = None;
        self.generation += 1;
        self.pending = None;
    }
}
struct Measurement {
    key: RowMeasurementKey,
    height: Pixels,
}
#[derive(Default)]
pub(crate) struct TimelineLayoutStore {
    entries: HashMap<RowId, Measurement>,
    pub body_heights: HashMap<RowId, Pixels>,
    pub index: Rc<RefCell<RowLayoutIndex>>,
    order: Vec<RowId>,
    pub context_revision: u64,
    pub theme_revision: u64,
}
impl TimelineLayoutStore {
    pub fn height(&self, key: &RowMeasurementKey) -> Option<Pixels> {
        self.entries
            .get(&key.id)
            .filter(|entry| entry.key == *key)
            .map(|entry| entry.height)
    }
    pub fn context_changed(&mut self) {
        self.context_revision += 1;
    }
    /// Called only by the normal update handler after its ticket has been accepted.
    pub fn commit(
        &mut self,
        order: Vec<RowId>,
        measured: Vec<(RowMeasurementKey, Pixels)>,
    ) -> Rc<Vec<Size<Pixels>>> {
        for (key, height) in measured {
            let height = height.max(px(1.));
            self.index.borrow_mut().update(&key.id, height);
            self.entries
                .insert(key.id.clone(), Measurement { key, height });
        }
        if self.order != order {
            let live: HashSet<_> = order.iter().collect();
            for id in self.order.iter().filter(|id| !live.contains(id)) {
                self.entries.remove(id);
                self.body_heights.remove(id);
                self.index.borrow_mut().remove(id);
            }
            let mut index = self.index.borrow_mut();
            for (at, id) in order.iter().enumerate() {
                if index.rank(id) != Some(at) {
                    index.remove(id);
                    index.insert(at, id.clone(), self.entries[id].height);
                }
            }
            self.order = order;
        }
        // The stock public list requires its complete size vector at each commit.
        Rc::new(
            self.order
                .iter()
                .map(|id| size(px(0.), self.entries[id].height))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_wrong_context_and_duplicate_batches_are_rejected() {
        let mut coordinator = TimelineMeasurementCoordinator::default();
        let old = coordinator.begin(1, 1);
        let current = coordinator.begin(2, 2);
        assert!(!coordinator.accept(old));
        assert!(!coordinator.accept(MeasurementTicket {
            context: 3,
            ..current
        }));
        assert!(!coordinator.accept(MeasurementTicket {
            presentation: 3,
            ..current
        }));
        assert!(coordinator.accept(current));
        assert!(!coordinator.accept(current));
        let late = coordinator.begin(3, 3);
        coordinator.cancel();
        assert!(!coordinator.accept(late));
    }
}

#[cfg(test)]
mod invalidation_tests {
    use super::*;
    fn key(id: &str) -> RowMeasurementKey {
        RowMeasurementKey {
            id: serde_json::from_value(serde_json::json!(id)).unwrap(),
            dependencies_revision: 0,
            layout_revision: 1,
            content_revision: 1,
            presentation_revision: 1,
            content_width: px(400.),
            rem: px(16.),
            text_style: TextStyle::default(),
            theme_revision: 1,
            locale: "en".into(),
            expanded: false,
            grouping: TimelineRowLayout::default(),
            last: false,
            author_label: None,
            principal: None,
            task_child: false,
        }
    }
    #[test]
    fn each_full_key_dimension_invalidates_independently_and_one_commit_updates_only_one_entry() {
        let a = key("a");
        let b = key("b");
        let mut store = TimelineLayoutStore::default();
        store.commit(
            vec![a.id.clone(), b.id.clone()],
            vec![(a.clone(), px(10.)), (b.clone(), px(20.))],
        );
        let unchanged = &store.entries[&b.id] as *const Measurement;
        let mut mutations: Vec<Box<dyn Fn(&mut RowMeasurementKey)>> = vec![
            Box::new(|k| k.dependencies_revision += 1),
            Box::new(|k| k.layout_revision += 1),
            Box::new(|k| k.content_revision += 1),
            Box::new(|k| k.presentation_revision += 1),
            Box::new(|k| k.content_width += px(0.25)),
            Box::new(|k| k.rem += px(1.)),
            Box::new(|k| k.text_style.font_weight = gpui_kit::FontWeight::BOLD),
            Box::new(|k| k.theme_revision += 1),
            Box::new(|k| k.locale = "ru".into()),
            Box::new(|k| k.expanded = true),
            Box::new(|k| k.last = true),
            Box::new(|k| k.author_label = Some("Author".into())),
            Box::new(|k| k.principal = Some("member".into())),
            Box::new(|k| k.task_child = true),
        ];
        mutations.push(Box::new(|k| k.grouping.starts_avatar_group = true));
        for change in mutations {
            let mut changed = a.clone();
            change(&mut changed);
            assert_ne!(changed, a);
            assert_eq!(store.height(&changed), None);
        }
        assert_eq!(store.height(&a), Some(px(10.))); // Equal complete input never enters measurement.
        let mut changed = a.clone();
        changed.content_width += px(0.25);
        let mut coordinator = TimelineMeasurementCoordinator::default();
        let ticket = coordinator.begin(2, 1);
        assert!(coordinator.accept(ticket));
        store.commit(
            vec![a.id.clone(), b.id.clone()],
            vec![(changed.clone(), px(30.))],
        );
        assert!(!coordinator.accept(ticket));
        assert_eq!(store.height(&a), None);
        assert_eq!(store.height(&changed), Some(px(30.)));
        assert_eq!(&store.entries[&b.id] as *const Measurement, unchanged);
        assert_eq!(store.index.borrow().origin(1), Some(px(30.)));
        assert_eq!(
            super::super::layout_index::visible_range(&store.index.borrow(), px(31.), px(5.)),
            1..2
        );
        store.commit(vec![b.id.clone()], vec![]);
        assert_eq!(store.entries.len(), 1);
        assert_eq!(store.index.borrow().origin(1), Some(px(20.)));
    }
}
