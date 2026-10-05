use super::*;
use gpui_kit::component::{
    dialog::DialogFooter,
    input::{Input, InputEvent, InputState},
};
use pioneer_client::composer::capabilities::ComposerCapability;
use std::{collections::BTreeSet, rc::Rc};

type Completion = Rc<dyn Fn(Option<Vec<ComposerCapability>>, &mut Window, &mut App)>;
struct PluginPicker {
    catalog: PluginCatalogState,
    selected: BTreeSet<String>,
    search: Entity<InputState>,
    _search_subscription: Subscription,
    request: Option<Task<()>>,
    done: bool,
}
/// Separate modal: its rows, selection and result contain parents only. The
/// dialog retains the Entity until dismissal; the shell retains draft identity.
pub fn open_plugin_picker(
    client: Arc<ClientCore>,
    workspace: String,
    selected: BTreeSet<String>,
    window: &mut Window,
    cx: &mut App,
    completion: impl Fn(Option<Vec<ComposerCapability>>, &mut Window, &mut App) + 'static,
) {
    let picker = cx.new(|cx| {
        let search =
            cx.new(|cx| InputState::new(window, cx).placeholder(t!("plugins.search").to_string()));
        let subscription = cx.subscribe(&search, |_, _, _: &InputEvent, cx| cx.notify());
        PluginPicker {
            catalog: PluginCatalogState {
                loading: true,
                ..Default::default()
            },
            selected,
            search,
            _search_subscription: subscription,
            request: None,
            done: false,
        }
    });
    picker.update(cx, |picker, cx| {
        picker.request = Some(cx.spawn(async move |view, cx| {
            let result = cx
                .background_spawn(async move { client.read_plugins(&workspace) })
                .await;
            let _ = view.update(cx, |view, cx| {
                match result {
                    Ok(response) => view.catalog.accept(response),
                    Err(_) => view.catalog.fail(),
                }
                cx.notify();
            });
        }));
    });
    let completion: Completion = Rc::new(completion);
    let search = picker.read(cx).search.clone();
    window.open_dialog(cx, move |dialog, window, cx| {
        let state = picker.read(cx);
        let query = state.search.read(cx).value().to_string();
        let rows = pioneer_client::plugins::selectable_plugins(&state.catalog.plugins, &query)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let selected = state.selected.clone();
        let loading = state.catalog.loading;
        let failed = state.catalog.failed;
        dialog
            .w(window.rem_size() * 32.)
            .title(t!("plugins.select").to_string())
            .keyboard(true)
            .overlay_closable(true)
            .close_button(true)
            .on_close({
                let picker = picker.clone();
                let completion = completion.clone();
                move |_, window, cx| {
                    let should_complete = picker.update(cx, |picker, _| {
                        let pending = !picker.done;
                        picker.done = true;
                        picker.request = None;
                        pending
                    });
                    if should_complete {
                        completion(None, window, cx);
                    }
                }
            })
            .child(
                v_flex()
                    .w_full()
                    .min_w_0()
                    .gap_2()
                    .child(Input::new(&state.search))
                    .when(loading, |view| {
                        view.child(t!("plugins.loading").to_string())
                    })
                    .when(failed, |view| view.child(t!("plugins.error").to_string()))
                    .child(
                        v_flex()
                            .w_full()
                            .max_h(rems(22.))
                            .min_h_0()
                            .gap_1()
                            .overflow_y_scrollbar()
                            .when(!loading && !failed && rows.is_empty(), |view| {
                                view.child(t!("plugins.empty").to_string())
                            })
                            .children(rows.into_iter().map(|plugin| {
                                let id = plugin.id.clone();
                                let checked = selected.contains(&id);
                                let picker = picker.clone();
                                Button::new(SharedString::from(format!(
                                    "plugin-picker-{}",
                                    plugin.id
                                )))
                                .ghost()
                                .w_full()
                                .justify_start()
                                .child(
                                    h_flex()
                                        .w_full()
                                        .gap_2()
                                        .child(
                                            Icon::new(if checked {
                                                IconName::Check
                                            } else {
                                                IconName::Plus
                                            })
                                            .size_4(),
                                        )
                                        .child(div().flex_1().child(plugin.name))
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(cx.theme().muted_foreground)
                                                .child(status_label(&plugin.status)),
                                        ),
                                )
                                .on_click(move |_, _, cx| {
                                    picker.update(cx, |picker, cx| {
                                        if !picker.selected.remove(&id)
                                            && picker.selected.len() < 64
                                        {
                                            picker.selected.insert(id.clone());
                                        }
                                        cx.notify();
                                    })
                                })
                                .into_any_element()
                            })),
                    ),
            )
            .footer(DialogFooter::new().children(vec![
                    Button::new("plugin-picker-cancel")
                        .outline()
                        .label(t!("plugins.cancel").to_string())
                        .on_click(|_, window, cx| window.close_dialog(cx))
                        .into_any_element(),
                    Button::new("plugin-picker-add")
                        .primary()
                        .label(t!("plugins.add").to_string())
                        .disabled(loading || failed)
                        .on_click({
                            let picker = picker.clone();
                            let completion = completion.clone();
                            move |_, window, cx| {
                                let result = picker.update(cx, |picker, _| {
                                    picker.done = true;
                                    pioneer_client::plugins::replace_selected_plugins(
                                        &[],
                                        &picker.catalog.plugins,
                                        &picker.selected,
                                    )
                                });
                                window.close_dialog(cx);
                                completion(Some(result), window, cx);
                            }
                        })
                        .into_any_element(),
                ]))
    });
    search.update(cx, |search, cx| search.focus(window, cx));
}
