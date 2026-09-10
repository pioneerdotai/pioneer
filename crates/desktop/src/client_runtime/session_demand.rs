//! Process lifetime session demand over the selected endpoint publication.
use gpui_kit::{App, AppContext, Task};
use pioneer_client::{
    core::{ClientCore, ClientPublicationReference, ClientScope},
    gateway::{
        onboarding_runtime::GatewayDestinationsPublication,
        session_driver::{SessionDemand, SessionVisibility},
    },
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, sync::Arc};

struct DestinationBinding {
    selected: RefCell<Option<(u64, Option<String>)>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl ClientPublicationSink for DestinationBinding {
    fn publish(&self, publication: ClientPublicationReference) {
        if publication.scope() != &ClientScope::GatewayDestinations {
            return;
        }
        let Some(publication) = publication.typed::<GatewayDestinationsPublication>() else {
            return;
        };
        self.apply_selection(
            publication.revisions().scoped().get(),
            publication.payload().selected_endpoint.clone(),
        );
    }
}
impl DestinationBinding {
    fn apply_selection(&self, revision: u64, selected: Option<String>) {
        let mut current = self.selected.borrow_mut();
        if current
            .as_ref()
            .is_some_and(|(applied, _)| *applied >= revision)
        {
            return;
        }
        let changed = current.as_ref().is_none_or(|(_, old)| old != &selected);
        *current = Some((revision, selected));
        drop(current);
        if changed {
            self.changed.send_replace(revision);
        }
    }
}

pub(super) struct DesktopSessionDemand {
    _binding: Arc<DestinationBinding>,
    _registration: ClientBindingRegistration,
    _task: Task<()>,
}
impl DesktopSessionDemand {
    pub(super) fn new(
        core: Arc<ClientCore>,
        registrar: &dyn ClientBindingRegistrar,
        cx: &mut App,
    ) -> Self {
        let binding = Arc::new(DestinationBinding {
            selected: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        });
        let mut changed = binding.changed.subscribe();
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        let registration =
            registrar.register(ClientScope::GatewayDestinations, Arc::downgrade(&sink));
        let input = Arc::downgrade(&binding);
        let core = Arc::downgrade(&core);
        let task = cx.spawn(async move |_| {
            let mut generation = 0;
            while changed.changed().await.is_ok() {
                let (Some(input), Some(core)) = (input.upgrade(), core.upgrade()) else {
                    break;
                };
                if core.is_stopped() {
                    break;
                }
                let selected = input
                    .selected
                    .borrow()
                    .as_ref()
                    .and_then(|(_, selected)| selected.clone());
                generation += 1;
                core.session_demand(SessionDemand {
                    endpoint_id: selected,
                    visibility: SessionVisibility::Foreground,
                    network_available: true,
                    generation,
                });
            }
        });
        Self {
            _binding: binding,
            _registration: registration,
            _task: task,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, rc::Rc, sync::Weak};
    #[test]
    fn endpoint_demand_coalesces_equal_inputs_and_rejects_late_replacements() {
        let binding = DestinationBinding {
            selected: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        };
        let mut changes = binding.changed.subscribe();
        binding.apply_selection(4, Some("first".into()));
        assert_eq!(*changes.borrow_and_update(), 4);
        binding.apply_selection(5, Some("first".into()));
        assert!(!changes.has_changed().unwrap());
        binding.apply_selection(3, Some("stale".into()));
        assert!(!changes.has_changed().unwrap());
        binding.apply_selection(6, Some("second".into()));
        assert_eq!(*changes.borrow_and_update(), 6);
        binding.apply_selection(7, None);
        assert_eq!(*changes.borrow_and_update(), 7);
        assert_eq!(binding.selected.borrow().as_ref().unwrap().1, None);
    }
    struct Registrar {
        dropped: Rc<Cell<usize>>,
        sink: RefCell<Option<Weak<dyn ClientPublicationSink>>>,
    }
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            scope: ClientScope,
            sink: Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            assert_eq!(scope, ClientScope::GatewayDestinations);
            *self.sink.borrow_mut() = Some(sink);
            let dropped = self.dropped.clone();
            ClientBindingRegistration::new(move || dropped.set(dropped.get() + 1))
        }
    }
    #[gpui_kit::test]
    fn process_demand_drop_releases_registration_and_late_weak_delivery(
        cx: &mut gpui_kit::TestAppContext,
    ) {
        let dropped = Rc::new(Cell::new(0));
        let registrar = Registrar {
            dropped: dropped.clone(),
            sink: RefCell::default(),
        };
        let core = Arc::new(ClientCore::new());
        let owner = cx.update(|cx| DesktopSessionDemand::new(core.clone(), &registrar, cx));
        assert!(
            registrar
                .sink
                .borrow()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_some()
        );
        drop(owner);
        cx.run_until_parked();
        assert_eq!(dropped.get(), 1);
        assert!(
            registrar
                .sink
                .borrow()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_none()
        );
        assert_eq!(Arc::strong_count(&core), 1);
    }
}
