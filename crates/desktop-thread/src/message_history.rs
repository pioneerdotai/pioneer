#[path = "message_history_rows.rs"]
mod rows;
use crate::binding::ThreadBindings;
use gpui_kit::{
    component::{Disableable, StyledExt, WindowExt, button::Button, theme::ActiveTheme, v_flex},
    prelude::*,
    *,
};
use pioneer_client::{
    core::{ClientCore, ClientScope},
    threads::message_revisions::*,
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::{cell::Cell, rc::Rc, sync::Arc};

struct MoreRevisions;
struct MessageRevisionContent {
    input: Option<Arc<MessageRevisionPublication>>,
}
impl EventEmitter<MoreRevisions> for MessageRevisionContent {}
impl Render for MessageRevisionContent {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let input = self.input.as_ref();
        let page = input.and_then(|input| input.page.as_ref());
        let mut content = v_flex().w_full().min_w_0().pt_4().gap_2();
        if let Some(page) = page {
            for revision in &page.revisions {
                content = content.child(rows::render_revision(revision, cx));
            }
            if page.revisions.is_empty() {
                content = content.child(t!("timeline.message.revisions_empty").to_string());
            }
        }
        if input.is_some_and(|input| input.state == MessageRevisionReadState::Failed) {
            content = content.child(
                div()
                    .text_sm()
                    .text_color(cx.theme().danger)
                    .child(t!("timeline.message.revisions_failed").to_string()),
            );
        }
        if page.is_some_and(|page| page.next_cursor.is_some()) {
            content = content.child(
                Button::new("message-revisions-more")
                    .label(t!("timeline.message.revisions_more").to_string())
                    .disabled(
                        input.is_some_and(|input| input.state == MessageRevisionReadState::Loading),
                    )
                    .on_click(cx.listener(|_, _, _, cx| cx.emit(MoreRevisions))),
            );
        }
        content
    }
}
pub(crate) struct MessageRevisionView {
    client: Arc<ClientCore>,
    identity: Option<MessageRevisionIdentity>,
    binding: Arc<ThreadBindings>,
    content: Entity<MessageRevisionContent>,
    dialog_open: Rc<Cell<bool>>,
    presented: bool,
    _changes: Task<()>,
    _subscriptions: Vec<Subscription>,
}
impl MessageRevisionView {
    pub(crate) fn new(
        client: Arc<ClientCore>,
        thread_id: String,
        turn_id: String,
        registrar: Arc<dyn ClientBindingRegistrar>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        let scope = ClientScope::MessageRevisions {
            thread_id: thread_id.clone(),
        };
        let binding = ThreadBindings::scoped(registrar, vec![scope], vec![]);
        client.message_revision_intent(MessageRevisionIntent::Open {
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
        });
        let identity = client
            .message_revision_snapshot(&thread_id)
            .filter(|p| p.identity.turn_id == turn_id)
            .map(|p| p.identity.clone());
        cx.new(|cx: &mut Context<Self>| {
            let content = cx.new(|_| MessageRevisionContent { input: None });
            let subscriptions = vec![
                cx.subscribe(&content, |view, _, _: &MoreRevisions, _| {
                    if let Some(identity) = &view.identity {
                        view.client
                            .message_revision_intent(MessageRevisionIntent::More {
                                identity: identity.clone(),
                            });
                    }
                }),
                cx.on_release_in(window, |view, window, cx| {
                    if view.dialog_open.replace(false) {
                        window.close_dialog(cx);
                    }
                }),
            ];
            let mut changes = binding.watch();
            let input = binding.clone();
            let task = cx.spawn_in(window, async move |view, cx| {
                while changes.changed().await.is_ok() {
                    if input.drain().is_empty() {
                        continue;
                    }
                    if view
                        .update_in(cx, |view, window, cx| view.synchronize(window, cx))
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let mut view = Self {
                client,
                identity,
                binding,
                content,
                dialog_open: Rc::new(Cell::new(false)),
                presented: false,
                _changes: task,
                _subscriptions: subscriptions,
            };
            view.synchronize(window, cx);
            view
        })
    }
    fn synchronize(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let input = self.identity.as_ref().and_then(|identity| {
            self.client
                .message_revision_snapshot(&identity.thread_id)
                .filter(|input| input.identity == *identity)
        });
        let cancelled = input
            .as_ref()
            .is_none_or(|input| input.state == MessageRevisionReadState::Cancelled);
        if cancelled && self.dialog_open.replace(false) {
            window.close_dialog(cx);
        }
        let ready = input.as_ref().is_some_and(|input| input.page.is_some());
        self.content.update(cx, |view, cx| {
            view.input = input;
            cx.notify();
        });
        if ready && !self.presented {
            self.presented = true;
            self.dialog_open.set(true);
            let content = self.content.clone();
            let open = self.dialog_open.clone();
            let client = Arc::downgrade(&self.client);
            let identity = self.identity.clone();
            window.open_dialog(cx, move |dialog, window, _| {
                dialog
                    .w(px(480.))
                    .max_h(window.viewport_size().height * 0.8)
                    .gap_1()
                    .rounded_2xl()
                    .close_button(true)
                    .overlay_closable(true)
                    .keyboard(true)
                    .title(
                        div()
                            .text_base()
                            .font_semibold()
                            .child(t!("timeline.message.revisions_title").to_string()),
                    )
                    .on_close({
                        let open = open.clone();
                        let client = client.clone();
                        let identity = identity.clone();
                        move |_, _, _| {
                            open.set(false);
                            if let (Some(client), Some(identity)) =
                                (client.upgrade(), identity.clone())
                            {
                                client.message_revision_intent(MessageRevisionIntent::Close {
                                    identity,
                                });
                            }
                        }
                    })
                    .child(content.clone())
            });
        }
    }
}
impl Drop for MessageRevisionView {
    fn drop(&mut self) {
        if let Some(identity) = &self.identity {
            self.client
                .message_revision_intent(MessageRevisionIntent::Close {
                    identity: identity.clone(),
                });
        }
        self.binding.clear();
    }
}
