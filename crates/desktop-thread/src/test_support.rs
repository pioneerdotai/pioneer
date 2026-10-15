use pioneer_client::core::{ClientCore, ClientScope, ClientSubscription, ClientSubscriptionEvent};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{
    cell::RefCell,
    collections::HashMap,
    num::NonZeroUsize,
    rc::Rc,
    sync::{Arc, Weak},
};

struct Target {
    subscription: ClientSubscription,
    sink: Weak<dyn ClientPublicationSink>,
    initial: Option<pioneer_client::core::ClientPublicationReference>,
}
struct Registrar {
    client: Weak<ClientCore>,
    targets: Rc<RefCell<HashMap<u64, Target>>>,
    next: RefCell<u64>,
}
impl ClientBindingRegistrar for Registrar {
    fn register(
        &self,
        scope: ClientScope,
        sink: Weak<dyn ClientPublicationSink>,
    ) -> ClientBindingRegistration {
        let Some(client) = self.client.upgrade() else {
            return ClientBindingRegistration::new(|| {});
        };
        let id = {
            let mut next = self.next.borrow_mut();
            *next += 1;
            *next
        };
        self.targets.borrow_mut().insert(
            id,
            Target {
                subscription: client.subscribe(scope.clone(), NonZeroUsize::new(64).unwrap()),
                initial: client.snapshot(&scope),
                sink,
            },
        );
        let targets = Rc::downgrade(&self.targets);
        ClientBindingRegistration::new(move || {
            if let Some(targets) = targets.upgrade() {
                targets.borrow_mut().remove(&id);
            }
        })
    }
}
pub(crate) fn binding_router(
    client: Arc<ClientCore>,
) -> (Arc<dyn ClientBindingRegistrar>, impl Fn()) {
    let targets = Rc::new(RefCell::new(HashMap::<u64, Target>::new()));
    let registrar = Arc::new(Registrar {
        client: Arc::downgrade(&client),
        targets: targets.clone(),
        next: RefCell::new(0),
    });
    (registrar, move || {
        let mut deliveries = Vec::new();
        for (id, target) in targets.borrow_mut().iter_mut() {
            if let Some(initial) = target.initial.take() {
                deliveries.push((*id, initial));
            }
            while let Some(event) = target.subscription.try_next() {
                let publication = match event {
                    ClientSubscriptionEvent::Publication { publication, .. } => Some(publication),
                    ClientSubscriptionEvent::ResnapshotRequired { scope, .. } => {
                        client.snapshot(&scope)
                    }
                };
                if let Some(publication) = publication {
                    deliveries.push((*id, publication));
                }
            }
        }
        deliveries.sort_by_key(|(_, publication)| publication.snapshot().sequence());
        for (id, publication) in deliveries {
            let sink = targets
                .borrow()
                .get(&id)
                .and_then(|target| target.sink.upgrade());
            if let Some(sink) = sink {
                sink.publish(publication);
            }
        }
    })
}

pub(crate) struct ThreadPorts;
impl pioneer_client::platform::ClientFileSystem for ThreadPorts {
    fn read_file(
        &self,
        _: &pioneer_client::platform::ClientPath,
    ) -> pioneer_client::ClientResult<Vec<u8>> {
        panic!("unexpected file read")
    }
    fn metadata(
        &self,
        _: &pioneer_client::platform::ClientPath,
    ) -> pioneer_client::ClientResult<pioneer_client::platform::ClientFileMetadata> {
        panic!("unexpected metadata read")
    }
    fn write_cache_file(
        &self,
        _: &str,
        _: &[u8],
    ) -> pioneer_client::ClientResult<pioneer_client::platform::ClientPath> {
        panic!("unexpected cache write")
    }
}
impl pioneer_client::artifacts::preview::ArtifactPreviewImageRenderer for ThreadPorts {
    fn write_preview_variants(
        &self,
        _: &[u8],
        _: &[pioneer_client::artifacts::preview::ArtifactPreviewVariantTarget],
    ) -> anyhow::Result<()> {
        panic!("unexpected preview encoding")
    }
}
impl crate::ports::ThreadFilePort for ThreadPorts {
    fn select_attachments(
        &self,
        _: crate::ports::ThreadPresentationOperation,
        _: pioneer_client::composer::store::ComposerOperationPlan,
        _: &mut gpui_kit::App,
    ) -> gpui_kit::Task<pioneer_client::composer::store::ComposerOperationCompletion> {
        panic!("unexpected picker")
    }
    fn select_download_destination(
        &self,
        _: crate::ports::ThreadPresentationOperation,
        _: pioneer_client::artifacts::workflow::ArtifactActionIdentity,
        _: &mut gpui_kit::App,
    ) -> gpui_kit::Task<pioneer_client::ClientResult<Option<pioneer_client::platform::ClientPath>>>
    {
        panic!("unexpected destination picker")
    }
    fn reveal_artifact(
        &self,
        _: crate::ports::ThreadPresentationOperation,
        _: pioneer_client::artifacts::local_presentation::ArtifactLocalPresentationPlan,
        _: &mut gpui_kit::App,
    ) -> gpui_kit::Task<pioneer_client::ClientResult<()>> {
        panic!("unexpected reveal")
    }
    fn file_openers(
        &self,
        _: &str,
        _: &gpui_kit::App,
    ) -> crate::ports::ThreadFileOpenerPresentation {
        let choice =
            crate::ports::ThreadFileOpenerChoice::new("file-manager".into(), "Files".into(), None);
        crate::ports::ThreadFileOpenerPresentation::new(
            vec![choice.clone()],
            choice.clone(),
            choice,
            None,
        )
    }
    fn select_file_opener(
        &self,
        _: &crate::ports::ThreadPresentationOperation,
        _: Option<&str>,
        _: &mut gpui_kit::App,
    ) -> pioneer_client::ClientResult<()> {
        panic!("unexpected preference write")
    }
    fn open_file(
        &self,
        _: &crate::ports::ThreadFileOpenRequest,
    ) -> pioneer_client::ClientResult<()> {
        panic!("unexpected file presentation")
    }
    fn preview_renderer(
        &self,
    ) -> Arc<dyn pioneer_client::artifacts::preview::ArtifactPreviewImageRenderer + Send + Sync>
    {
        Arc::new(Self)
    }
    fn runtime_root(&self) -> pioneer_client::platform::ClientPath {
        pioneer_client::platform::ClientPath::new("synthetic")
    }
    fn retire_mount(&self, _: &str, _: u64) {}
}
impl crate::ports::ThreadExternalNavigationPort for ThreadPorts {
    fn open_url(
        &self,
        _: &crate::ports::ThreadExternalNavigationRequest,
    ) -> pioneer_client::ClientResult<()> {
        panic!("unexpected browser presentation")
    }
    fn retire_mount(&self, _: &str, _: u64) {}
}

pub(crate) fn install_thread_timeline(client: &ClientCore, id: &str, text: &str) {
    client.upsert_thread(serde_json::from_value(serde_json::json!({
        "created_at":1,"id":id,"mode":"Chat","model":"synthetic-model","model_provider":"synthetic-provider","origin_kind":"user","preview":"","sidebar_visibility":"visible","status":"Idle","turns":[],"updated_at":2,"workspace_id":"workspace"
    })).unwrap());
    client.apply_thread_timeline_page(serde_json::from_value(serde_json::json!({
        "workspaceId":"workspace","threadId":id,"projectionVersion":1,
        "blocks":[{"workspaceId":"workspace","threadId":id,"blockId":"message","turnId":"turn","sortKey":"1","kind":{"kind":"user_message","text":text,"mode":"Message"}}],
        "page":{"hasMoreBefore":false,"hasMoreAfter":false}
    })).unwrap(), pioneer_client::timeline::semantic::TopLevelPageMergeMode::Reset);
}

use crate::ports::{ThreadAudioCompletion, ThreadAudioError, ThreadAudioRequest};
use gpui_kit::{App, Task};
impl crate::ports::ThreadAudioPort for ThreadPorts {
    fn start_capture(
        &self,
        _: ThreadAudioRequest,
        _: &mut App,
    ) -> Task<Result<ThreadAudioCompletion, ThreadAudioError>> {
        panic!("mount and controlled publication must not capture audio")
    }
    fn stop_recording(
        &self,
        _: &ThreadAudioRequest,
    ) -> Result<ThreadAudioCompletion, ThreadAudioError> {
        panic!("no capture was started")
    }
    fn finalize_capture(
        &self,
        _: ThreadAudioRequest,
        _: pioneer_client::composer::turn_prepare::PreparedVoiceComposerSnapshot,
        _: &mut App,
    ) -> Task<Result<ThreadAudioCompletion, ThreadAudioError>> {
        panic!("no capture was started")
    }
    fn cancel_capture(&self, _: &ThreadAudioRequest) {
        panic!("no capture was started")
    }
    fn retire_mount(&self, _: &str, _: u64) {}
}
