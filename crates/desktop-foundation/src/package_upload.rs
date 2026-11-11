use crate::CatalogBinding;
use crate::ClientBindingRegistrar;
use gpui_kit::component::{button::*, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::{ClientCore, ClientScope},
    package_delivery::{operations::*, upload as skill_upload, upload_flow::*},
};
use std::{path::PathBuf, sync::Arc};
/// Localized copy supplied by the feature adapter; transfer state stays shared.
pub trait UploadText: Send + Sync + 'static {
    fn failure_message(failure: SkillUploadFailure) -> String;
    fn preparing() -> String;
    fn uploading() -> String;
    fn cancel() -> String;
}
pub struct UploadVisibility;
pub struct UploadView<T: UploadText> {
    _text: std::marker::PhantomData<T>,
    client: Arc<ClientCore>,
    binding: Arc<CatalogBinding>,
    operation: Option<SkillUploadOperation>,
    workspace: String,
    publication: Option<Arc<SkillUploadPublication>>,
    _publication_task: Task<()>,
}
impl<T: UploadText> EventEmitter<UploadVisibility> for UploadView<T> {}
impl<T: UploadText> UploadView<T> {
    pub fn publication(&self) -> Option<&Arc<SkillUploadPublication>> {
        self.publication.as_ref()
    }
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.operation = None;
        self.binding.set_scopes(&[]);
        self.apply_publication(None, cx);
    }

    pub fn error(&self) -> Option<String> {
        let p = self
            .publication
            .as_ref()
            .filter(|p| p.state == SkillUploadState::Failed)?;
        Some(Self::failure_message(
            p.failure.unwrap_or(SkillUploadFailure::Unknown),
        ))
    }
    pub fn failure_message(failure: SkillUploadFailure) -> String {
        T::failure_message(failure)
    }

    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            let binding = CatalogBinding::new(registrar);
            let mut changed = binding.watch();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if view.update(cx, |view, cx| view.sync(cx)).is_err() {
                        break;
                    }
                }
            });
            Self {
                _text: std::marker::PhantomData,
                client,
                binding,
                operation: None,
                workspace: String::new(),
                publication: None,
                _publication_task: task,
            }
        })
    }
    pub fn active(&self) -> bool {
        self.publication
            .as_ref()
            .is_some_and(|p| !p.state.is_terminal())
    }
    pub fn start(
        &mut self,
        workspace: String,
        target: SkillUploadTarget,
        path: PathBuf,
        cx: &mut Context<Self>,
    ) -> Result<(), SkillUploadFailure> {
        if self.active() {
            return Err(SkillUploadFailure::Busy);
        }
        self.operation = None;
        let operation = self
            .client
            .start_skill_upload(&workspace, target, path)
            .map_err(|error| SkillUploadFailure::from_error(&error))?;
        {
            self.binding.set_scopes(&[ClientScope::SkillsUpload {
                workspace_id: workspace.clone(),
                operation_id: operation.id(),
            }]);
            self.workspace = workspace;
            self.operation = Some(operation);
            self.sync(cx);
        }
        Ok(())
    }
    pub fn cancel(&mut self, cx: &mut Context<Self>) {
        if let Some(operation) = &self.operation {
            operation.cancel();
        }
        self.sync(cx);
    }
    fn sync(&mut self, cx: &mut Context<Self>) {
        let next = self
            .operation
            .as_ref()
            .and_then(|o| self.client.skill_upload_snapshot(&self.workspace, o.id()));
        self.apply_publication(next, cx);
    }
    pub(crate) fn apply_publication(
        &mut self,
        next: Option<Arc<SkillUploadPublication>>,
        cx: &mut Context<Self>,
    ) {
        let active = self.active();
        if self.publication != next {
            self.publication = next;
            cx.notify();
        }
        if active != self.active() {
            cx.emit(UploadVisibility);
        }
    }
}
impl<T: UploadText> Render for UploadView<T> {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(publication) = self.publication.as_ref().filter(|p| !p.state.is_terminal()) else {
            return div().into_any_element();
        };
        let progress = skill_upload::skill_upload_progress(
            if publication.state == SkillUploadState::Preparing {
                T::preparing()
            } else {
                T::uploading()
            },
            publication.sent_bytes,
            publication.total_bytes,
        );
        let progress_fraction = skill_upload::skill_upload_progress_fraction(&progress);
        let progress_label = skill_upload::skill_upload_progress_text(&progress);
        let desktop_entity = cx.entity();
        h_flex()
            .w_full()
            .items_center()
            .justify_between()
            .gap_3()
            .p_3()
            .rounded_md()
            .bg(cx.theme().accent.opacity(0.08))
            .border_1()
            .border_color(cx.theme().accent.opacity(0.24))
            .child(
                h_flex()
                    .items_center()
                    .gap_3()
                    .child(
                        Icon::new(IconName::RefreshCw)
                            .size_4()
                            .text_color(cx.theme().accent),
                    )
                    .child(
                        v_flex()
                            .gap_1()
                            .child(div().text_sm().font_medium().child(progress_label))
                            .child(
                                div()
                                    .w(rems(15.))
                                    .h_1()
                                    .rounded_full()
                                    .bg(cx.theme().border)
                                    .child(
                                        div()
                                            .h_1()
                                            .w(relative(progress_fraction))
                                            .rounded_full()
                                            .bg(cx.theme().accent),
                                    ),
                            ),
                    ),
            )
            .child({
                let desktop_entity = desktop_entity.clone();
                Button::new(SharedString::from(format!(
                    "package:{}:upload:{}:cancel",
                    self.workspace, publication.operation_id
                )))
                .small()
                .outline()
                .label(T::cancel())
                .disabled(publication.state == SkillUploadState::Applying)
                .on_click(move |_, _, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.cancel(cx);
                    });
                })
            })
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::core::prelude::v1::test;
    use crate::{ClientBindingRegistration, ClientPublicationSink};
    use std::{cell::Cell, rc::Rc, sync::Weak};
    struct Text;
    impl UploadText for Text {
        fn failure_message(_: SkillUploadFailure) -> String {
            "failure".into()
        }
        fn preparing() -> String {
            "preparing".into()
        }
        fn uploading() -> String {
            "uploading".into()
        }
        fn cancel() -> String {
            "cancel".into()
        }
    }
    struct Registrar;
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            _: ClientScope,
            _: Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            ClientBindingRegistration::new(|| {})
        }
    }
    #[gpui_kit::test]
    fn progress_updates_child_and_terminal_visibility_once(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let upload = cx.update(|cx| {
            UploadView::<Text>::new(
                pioneer_client::catalog_test_support::client(),
                Arc::new(Registrar),
                cx,
            )
        });
        let publication = |revision, sent_bytes, state| {
            Some(Arc::new(SkillUploadPublication {
                failure: None,
                mutation_uncertain: false,
                plugin_preview: None,
                plugin_result: None,
                plugin_update_preview: None,
                operation_id: 7,
                generation: 7,
                revision,
                state,
                sent_bytes,
                total_bytes: 8,
                target: SkillUploadTarget::Install,
                pack: false,
            }))
        };
        cx.update(|cx| {
            upload.update(cx, |view, cx| {
                view.apply_publication(publication(1, 0, SkillUploadState::Uploading), cx)
            })
        });
        cx.run_until_parked();
        let updates = Rc::new(Cell::new(0));
        let visibility = Rc::new(Cell::new(0));
        let subscriptions = cx.update(|cx| {
            let updates = updates.clone();
            let visibility = visibility.clone();
            (
                cx.observe(&upload, move |_, _| updates.set(updates.get() + 1)),
                cx.subscribe(&upload, move |_, _: &UploadVisibility, _| {
                    visibility.set(visibility.get() + 1)
                }),
            )
        });
        cx.update(|cx| {
            upload.update(cx, |view, cx| {
                view.apply_publication(publication(2, 2, SkillUploadState::Uploading), cx)
            })
        });
        cx.run_until_parked();
        assert_eq!((updates.get(), visibility.get()), (1, 0));
        cx.update(|cx| {
            upload.update(cx, |view, cx| {
                view.apply_publication(publication(2, 2, SkillUploadState::Uploading), cx)
            })
        });
        cx.run_until_parked();
        assert_eq!((updates.get(), visibility.get()), (1, 0));
        cx.update(|cx| {
            upload.update(cx, |view, cx| {
                view.apply_publication(publication(3, 8, SkillUploadState::Succeeded), cx)
            })
        });
        cx.run_until_parked();
        assert_eq!((updates.get(), visibility.get()), (2, 1));
        assert!(!upload.read_with(cx, |view, _| view.active()));
        drop(subscriptions);
    }
}
