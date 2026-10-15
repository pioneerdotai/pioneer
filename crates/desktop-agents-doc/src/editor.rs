use crate::binding::DocumentBinding;
use gpui_kit::component::input::{EditorState, InputEvent};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    agents_doc::{
        controller::AgentsDocumentPublication,
        runtime::{AgentsDocumentAction, AgentsDocumentDemand, AgentsDocumentIntent},
        scope::AgentsDocEditorScope,
    },
    core::ClientCore,
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::sync::Arc;

pub struct AgentsDocumentConfig {
    client: Arc<ClientCore>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    scope: AgentsDocEditorScope,
}

#[cfg(test)]
mod tests {
    use super::{AgentsDocumentConfig, AgentsDocumentEditor, Arc, ClientBindingRegistrar};
    use gpui_kit::TestAppContext;
    use pioneer_client::agents_doc::scope::AgentsDocEditorScope;
    use pioneer_desktop_foundation::{
        ClientBindingRegistration, ClientPublicationSink, ClientScope,
    };
    struct Registrar;
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            _: ClientScope,
            _: std::sync::Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            ClientBindingRegistration::new(|| {})
        }
    }
    #[gpui_kit::test]
    fn retained_input_load_echo_and_save_do_not_replace_selection(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let client = pioneer_client::catalog_test_support::client();
        let scope = AgentsDocEditorScope::root("workspace");
        let (root, cx) = cx.add_window_view(|window, cx| {
            let editor = AgentsDocumentEditor::new(
                AgentsDocumentConfig::new(client.clone(), Arc::new(Registrar), scope.clone()),
                window,
                cx,
            );
            gpui_kit::component::Root::new(editor, window, cx)
        });
        let editor = root.read_with(cx, |root, _| {
            root.view()
                .clone()
                .downcast::<AgentsDocumentEditor>()
                .unwrap()
        });
        let request = client.next_agents_document_request_for_test().unwrap();
        let response = pioneer_client::agents_doc::controller::empty_document_response_for_test();
        assert!(client.complete_agents_document_load_for_test(request, response));
        cx.update(|window, cx| editor.update(cx, |editor, cx| editor.sync(window, cx)));
        let input = editor.read_with(cx, |editor, _| editor.input.clone());
        cx.update(|window, cx| {
            input.update(cx, |input, cx| {
                input.focus(window, cx);
            })
        });
        cx.simulate_input("draft");
        cx.run_until_parked();
        assert_eq!(
            client.agents_document_snapshot(&scope).unwrap().content(),
            "draft"
        );
        cx.update(|_, cx| input.update(cx, |input, cx| input.set_selected_range(1..3, cx)));
        let revision = client
            .agents_document_snapshot(&scope)
            .unwrap()
            .edit_revision();
        cx.update(|window, cx| {
            editor.update(cx, |editor, cx| editor.flush_pending_save(window, cx))
        });
        let save = client.next_agents_document_request_for_test().unwrap();
        client.complete_agents_document_save_for_test(save, Err("synthetic offline".into()));
        cx.update(|window, cx| editor.update(cx, |editor, cx| editor.sync(window, cx)));
        cx.run_until_parked();
        assert_eq!(input.read_with(cx, |input, _| input.selected_range()), 1..3);
        assert_eq!(
            client
                .agents_document_snapshot(&scope)
                .unwrap()
                .edit_revision(),
            revision
        );
        assert_eq!(
            input.entity_id(),
            editor.read_with(cx, |editor, _| editor.input.entity_id())
        );
    }
}
impl AgentsDocumentConfig {
    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        scope: AgentsDocEditorScope,
    ) -> Self {
        Self {
            client,
            registrar,
            scope,
        }
    }
}
pub struct AgentsDocumentEditor {
    client: Arc<ClientCore>,
    scope: AgentsDocEditorScope,
    pub(crate) publication: Arc<AgentsDocumentPublication>,
    pub(crate) input: Entity<EditorState>,
    binding: Arc<DocumentBinding>,
    _input_subscription: Subscription,
    _binding_task: Task<()>,
    _demand: AgentsDocumentDemand,
}
impl AgentsDocumentEditor {
    pub fn new(config: AgentsDocumentConfig, window: &mut Window, cx: &mut App) -> Entity<Self> {
        let demand = config.client.acquire_agents_document(config.scope.clone());
        let publication = config
            .client
            .agents_document_snapshot(&config.scope)
            .expect("document acquired before editor construction");
        cx.new(|cx| {
            let input = cx.new(|cx| {
                EditorState::new(window, cx)
                    .language("markdown")
                    .line_number(true)
                    .soft_wrap(true)
                    .default_value(publication.content())
            });
            let subscription = cx.subscribe_in(
                &input,
                window,
                |view: &mut Self, _, event, window, cx| match event {
                    InputEvent::Change => {
                        let content = view.input.read(cx).value().to_string();
                        if content != view.publication.content() {
                            view.intent(AgentsDocumentAction::Edit { content });
                            view.sync(window, cx);
                        }
                    }
                    InputEvent::Blur => view.flush_pending_save(window, cx),
                    _ => {}
                },
            );
            let binding = DocumentBinding::new(config.scope.clone(), config.registrar);
            let mut changed = binding.changed.subscribe();
            let handle = window.window_handle();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if cx
                        .update_window(handle, |_, window, cx| {
                            view.update(cx, |view, cx| view.sync(window, cx))
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            Self {
                client: config.client,
                scope: config.scope,
                publication,
                input,
                binding,
                _input_subscription: subscription,
                _binding_task: task,
                _demand: demand,
            }
        })
    }
    fn intent(&self, action: AgentsDocumentAction) {
        self.client
            .agents_document_intent(AgentsDocumentIntent::Scoped {
                scope: self.scope.clone(),
                expected_owner: self.publication.owner_generation(),
                action,
            });
    }
    pub fn scope(&self) -> &AgentsDocEditorScope {
        &self.scope
    }
    fn sync(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let next = self
            .binding
            .latest()
            .into_iter()
            .chain(self.client.agents_document_snapshot(&self.scope))
            .max_by_key(|p| p.revision());
        let Some(next) = next else { return };
        if next.revision() <= self.publication.revision() {
            return;
        }
        if self.input.read(cx).value().as_ref() != next.content() {
            self.input
                .update(cx, |input, cx| input.set_value(next.content(), window, cx));
        }
        self.publication = next;
        cx.notify();
    }
    pub fn flush_pending_save(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.intent(AgentsDocumentAction::Save);
        self.sync(window, cx);
    }
    pub(crate) fn retry_save_now(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.flush_pending_save(window, cx);
    }
    pub(crate) fn start_load(&mut self, _cx: &mut Context<Self>) {
        self.intent(AgentsDocumentAction::Reload);
    }
    pub(crate) fn reload_remote_conflict(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.intent(AgentsDocumentAction::ReloadRemote);
        self.sync(window, cx);
    }
    pub(crate) fn overwrite_remote_conflict(&mut self, _cx: &mut Context<Self>) {
        self.intent(AgentsDocumentAction::OverwriteRemote);
    }
}
