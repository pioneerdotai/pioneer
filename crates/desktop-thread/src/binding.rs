use pioneer_client::core::{ClientPublicationReference, ClientScope};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    sync::Arc,
};

/// Immutable-output registrations for exactly one mounted thread.
pub(crate) struct ThreadBindings {
    registrar: Arc<dyn ClientBindingRegistrar>,
    scopes: Vec<ClientScope>,
    active: Cell<bool>,
    registrations: RefCell<Vec<ClientBindingRegistration>>,
    latest: RefCell<HashMap<ClientScope, ClientPublicationReference>>,
    pending: RefCell<HashMap<ClientScope, ClientPublicationReference>>,
    changed: tokio::sync::watch::Sender<u64>,
    timeline_changes: RefCell<Vec<Arc<pioneer_client::timeline::presentation::TimelineChangeSet>>>,
    timeline: RefCell<Option<(String, crate::timeline::TimelineRenderModel)>>,
}
impl ThreadBindings {
    pub(crate) fn new(
        registrar: Arc<dyn ClientBindingRegistrar>,
        thread_id: &str,
        initial: Vec<ClientPublicationReference>,
    ) -> Arc<Self> {
        let id = thread_id.to_owned();
        let scopes = vec![
            ClientScope::Thread {
                thread_id: id.clone(),
            },
            ClientScope::Timeline {
                thread_id: id.clone(),
            },
            ClientScope::Composer {
                thread_id: id.clone(),
            },
            ClientScope::ComposerCatalog {
                thread_id: id.clone(),
            },
            ClientScope::TurnCancellation {
                thread_id: id.clone(),
            },
            ClientScope::ThreadCapability {
                thread_id: id.clone(),
            },
            ClientScope::Artifact {
                thread_id: id.clone(),
            },
            ClientScope::ThreadMember { thread_id: id },
            ClientScope::Navigation,
            ClientScope::Administration { workspace_id: None },
            ClientScope::Session,
        ];
        Self::scoped(registrar, scopes, initial)
    }
    pub(crate) fn scoped(
        registrar: Arc<dyn ClientBindingRegistrar>,
        scopes: Vec<ClientScope>,
        initial: Vec<ClientPublicationReference>,
    ) -> Arc<Self> {
        let binding = Arc::new(Self {
            registrar,
            scopes,
            active: Cell::new(true),
            registrations: RefCell::default(),
            latest: RefCell::default(),
            pending: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
            timeline: RefCell::default(),
            timeline_changes: RefCell::default(),
        });
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        for scope in &binding.scopes {
            let registration = binding
                .registrar
                .register(scope.clone(), Arc::downgrade(&sink));
            binding.registrations.borrow_mut().push(registration);
        }
        for publication in initial {
            binding.publish(publication);
        }
        binding
    }
    pub(crate) fn registrar(&self) -> Arc<dyn ClientBindingRegistrar> {
        self.registrar.clone()
    }
    pub(crate) fn publication(&self, scope: &ClientScope) -> Option<ClientPublicationReference> {
        self.latest.borrow().get(scope).cloned()
    }
    pub(crate) fn take_timeline_changes(
        &self,
    ) -> Vec<Arc<pioneer_client::timeline::presentation::TimelineChangeSet>> {
        self.timeline_changes.take()
    }
    pub(crate) fn timeline_model(
        &self,
        id: Option<&str>,
    ) -> Option<crate::timeline::TimelineRenderModel> {
        self.timeline
            .borrow()
            .as_ref()
            .filter(|(thread, _)| Some(thread.as_str()) == id)
            .map(|(_, model)| model.clone())
    }
    pub(crate) fn set_active(self: &Arc<Self>, active: bool) {
        if self.active.replace(active) == active {
            return;
        }
        self.registrations.borrow_mut().clear();
        self.pending.borrow_mut().clear();
        if !active {
            if let Some((_, model)) = self.timeline.borrow().as_ref() {
                model.update.retire(false);
            }
        }
        if active {
            let sink: Arc<dyn ClientPublicationSink> = self.clone();
            for scope in &self.scopes {
                let registration = self
                    .registrar
                    .register(scope.clone(), Arc::downgrade(&sink));
                self.registrations.borrow_mut().push(registration);
            }
        }
    }
    pub(crate) fn clear(&self) {
        self.active.set(false);
        if let Some((_, model)) = self.timeline.borrow_mut().take() {
            model.update.retire(false);
        }
        self.timeline_changes.borrow_mut().clear();
        self.registrations.borrow_mut().clear();
        self.pending.borrow_mut().clear();
        self.latest.borrow_mut().clear();
        if let Some((_, model)) = self.timeline.borrow_mut().take() {
            model.update.retire(false);
        }
        self.timeline_changes.borrow_mut().clear();
    }
    pub(crate) fn watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub(crate) fn drain(&self) -> Vec<ClientPublicationReference> {
        self.pending.borrow_mut().drain().map(|(_, p)| p).collect()
    }
}
impl ClientPublicationSink for ThreadBindings {
    fn publish(&self, publication: ClientPublicationReference) {
        let mut input = pioneer_observability::updates::StageObservation::start(
            pioneer_observability::updates::Stage::NativeInput,
        );
        let scope = publication.scope();
        if !self.active.get() || !self.scopes.contains(scope) {
            input.finish(pioneer_observability::updates::Fact::Rejected);
            return;
        }
        if self
            .latest
            .borrow()
            .get(scope)
            .is_some_and(|old| old.snapshot().sequence() >= publication.snapshot().sequence())
        {
            input.finish(pioneer_observability::updates::Fact::Superseded);
            return;
        }
        if let ClientScope::Timeline { thread_id } = scope {
            if let Some(change) = publication.timeline_change() {
                let mut pending = self.timeline_changes.borrow_mut();
                if pending.len() == 64 {
                    pending.clear();
                }
                pending.push(change);
            } else {
                self.timeline_changes.borrow_mut().clear();
            }
            let timeline = publication
                .typed::<pioneer_client::timeline::presentation::TimelineSnapshot>()
                .map(|snapshot| {
                    (thread_id.clone(), {
                        let snapshot = snapshot.payload();
                        let mut model =
                            crate::timeline::TimelineRenderModel::from_snapshot(&snapshot);
                        model.update = timeline_update(&publication, &snapshot);
                        if let Some((_, old)) = self
                            .timeline
                            .borrow()
                            .as_ref()
                            .filter(|(id, _)| id == thread_id)
                        {
                            old.update.retire(true);
                        }
                        model
                    })
                });
            *self.timeline.borrow_mut() = timeline;
        }
        self.latest
            .borrow_mut()
            .insert(scope.clone(), publication.clone());
        self.pending.borrow_mut().insert(scope.clone(), publication);
        input.finish(pioneer_observability::updates::Fact::Returned);
        self.changed.send_modify(|v| *v = v.saturating_add(1));
    }
}

impl Drop for ThreadBindings {
    fn drop(&mut self) {
        if let Some((_, model)) = self.timeline.get_mut().as_ref() {
            model.update.retire(false);
        }
    }
}

// Diagnostic subset of the actual immutable publication, with no payload copy.
fn timeline_update(
    publication: &ClientPublicationReference,
    snapshot: &pioneer_client::timeline::presentation::TimelineSnapshot,
) -> pioneer_observability::updates::RenderBatch {
    use pioneer_observability::updates::{RenderBatch, RenderTarget, RendererBoundary};
    if !pioneer_observability::telemetry_enabled() {
        return RenderBatch::default();
    }
    let Some(mut context) = publication.update_context() else {
        return RenderBatch::default();
    };
    if snapshot.thread_id().len() > 256 {
        let mut coverage = pioneer_observability::updates::StageObservation::start(
            pioneer_observability::updates::Stage::RenderCoverage,
        );
        coverage.finish(pioneer_observability::updates::Fact::Uncovered);
        return RenderBatch::default();
    }
    if snapshot.rows().is_empty() {
        return RenderBatch::new(
            Some(context),
            [RenderTarget {
                thread: snapshot.thread_id().to_owned(),
                generation: snapshot.generation(),
                row: String::new(),
                revision: snapshot.revision(),
                boundary: RendererBoundary::GpuiEmpty,
                part: None,
            }],
        );
    }
    let change = publication.timeline_change();
    let count = change.as_ref().map_or(snapshot.rows().len(), |change| {
        change.inserted.len().saturating_add(change.replaced.len())
    });
    if count > 4 {
        context.incomplete();
    }
    // Four retained targets plus one overflow witness, never a scan of history.
    let rows = if let Some(change) = &change {
        change
            .inserted
            .iter()
            .chain(&change.replaced)
            .take(5)
            .collect::<Vec<_>>()
    } else {
        snapshot.rows().iter().take(5).collect::<Vec<_>>()
    };
    let hint = context.hint();
    let mut targets = rows
        .into_iter()
        .filter(|row| {
            hint.is_none_or(|hint| {
                hint.thread == snapshot.thread_id()
                    && hint
                        .item
                        .as_ref()
                        .is_none_or(|id| row.item().is_some_and(|item| &item.id == id))
            })
        })
        .filter(|row| row.id().as_str().len() <= 256 && snapshot.thread_id().len() <= 256)
        .flat_map(|row| {
            let target = RenderTarget {
                thread: snapshot.thread_id().to_owned(),
                generation: snapshot.generation(),
                row: row.id().as_str().to_owned(),
                revision: row.revision(),
                boundary: if row.item().is_some_and(|item| {
                    matches!(
                        item.item,
                        pioneer_client::timeline::types::TurnItem::CommandExecution { .. }
                    )
                }) {
                    RendererBoundary::TerminalReader
                } else {
                    RendererBoundary::GpuiRender
                },
                part: None,
            };
            let html = row
                .content()
                .map(|content| content.html_publications.as_slice())
                .unwrap_or_default();
            // GPUI row composition and each inline document have distinct
            // boundaries; neither substitutes for the other.
            std::iter::once(target.clone())
                .chain(html.iter().take(4).map(|html| {
                    let mut target = target.clone();
                    target.boundary = RendererBoundary::HtmlVisibleLayout;
                    target.part = pioneer_observability::updates::html_part(
                        &html.artifact.artifact_id,
                        html.artifact.version_id.as_deref(),
                    );
                    target
                }))
                .collect::<Vec<_>>()
        })
        .take(5)
        .collect::<Vec<_>>();
    if change
        .as_ref()
        .is_some_and(|change| !change.removed.is_empty() || change.order.is_some())
    {
        // Membership/order evidence is separate from the content of any row.
        targets.insert(
            0,
            RenderTarget {
                thread: snapshot.thread_id().to_owned(),
                generation: snapshot.generation(),
                row: String::new(),
                revision: snapshot.revision(),
                boundary: RendererBoundary::GpuiStructureRender,
                part: None,
            },
        );
    }
    RenderBatch::new(Some(context), targets)
}
