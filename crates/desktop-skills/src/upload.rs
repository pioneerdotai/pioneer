use crate::{assets::PioneerIconName, binding::CatalogBinding};
use gpui_kit::component::{button::*, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::{ClientCore, ClientScope},
    skills::{operations::*, upload as skill_upload, upload_flow::*},
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::{path::PathBuf, sync::Arc};
pub(crate) struct UploadVisibility;
pub(crate) struct UploadView {
    client: Arc<ClientCore>,
    binding: Arc<CatalogBinding>,
    operation: Option<SkillUploadOperation>,
    workspace: String,
    publication: Option<Arc<SkillUploadPublication>>,
    _publication_task: Task<()>,
}
impl EventEmitter<UploadVisibility> for UploadView {}
impl UploadView {
    pub(crate) fn error(&self) -> Option<String> {
        let p = self
            .publication
            .as_ref()
            .filter(|p| p.state == SkillUploadState::Failed)?;
        Some(match &p.target {
            SkillUploadTarget::Install if p.pack => {
                t!("skills.error.install_pack_failed").to_string()
            }
            SkillUploadTarget::Install => t!("skills.error.install_failed").to_string(),
            SkillUploadTarget::Update { .. } => t!("skills.error.update_failed").to_string(),
            SkillUploadTarget::UpdatePack { .. } => {
                t!("skills.error.update_pack_failed").to_string()
            }
        })
    }
    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            let binding = CatalogBinding::new(registrar);
            let mut changed = binding.changed.subscribe();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if view.update(cx, |view, cx| view.sync(cx)).is_err() {
                        break;
                    }
                }
            });
            Self {
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
    ) -> Result<(), ()> {
        if self.active() {
            return Err(());
        }
        self.operation = None;
        let operation = self
            .client
            .start_skill_upload(&workspace, target, path)
            .map_err(|_| ())?;
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
impl Render for UploadView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(publication) = self.publication.as_ref().filter(|p| !p.state.is_terminal()) else {
            return div().into_any_element();
        };
        let progress = skill_upload::skill_upload_progress(
            if publication.state == SkillUploadState::Preparing {
                t!("skills.upload.preparing").to_string()
            } else {
                t!("skills.upload.uploading").to_string()
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
                        Icon::new(PioneerIconName::RefreshCw)
                            .size_4()
                            .text_color(cx.theme().accent),
                    )
                    .child(
                        v_flex()
                            .gap_1()
                            .child(div().text_sm().font_medium().child(progress_label))
                            .child(
                                div()
                                    .w(px(240.0))
                                    .h(px(4.0))
                                    .rounded_full()
                                    .bg(cx.theme().border)
                                    .child(
                                        div()
                                            .h(px(4.0))
                                            .w(px(240.0 * progress_fraction))
                                            .rounded_full()
                                            .bg(cx.theme().accent),
                                    ),
                            ),
                    ),
            )
            .child({
                let desktop_entity = desktop_entity.clone();
                Button::new(SharedString::from(format!(
                    "skills:{}:upload:{}:cancel",
                    self.workspace, publication.operation_id
                )))
                .small()
                .outline()
                .label(t!("buttons.cancel").to_string())
                .on_click(move |_, _, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.cancel(cx);
                    });
                })
            })
            .into_any_element()
    }
}
