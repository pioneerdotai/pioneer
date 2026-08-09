use gpui::{
    App, ClipboardItem, KeyBinding, Menu, MenuItem, PromptButton, PromptLevel, SystemMenuType,
    actions,
};

actions!(
    desktop_menu,
    [ShowAbout, HideApp, HideOtherApps, ShowAllApps, QuitApp]
);

pub(crate) fn init_system_menus(cx: &mut App) {
    cx.activate(true);
    cx.on_action(show_about);
    cx.on_action(hide_app);
    cx.on_action(hide_other_apps);
    cx.on_action(show_all_apps);
    cx.on_action(quit_app);
    cx.bind_keys([KeyBinding::new("secondary-h", HideApp, None)]);
    cx.bind_keys([KeyBinding::new("secondary-alt-h", HideOtherApps, None)]);
    cx.bind_keys([KeyBinding::new("secondary-q", QuitApp, None)]);
    cx.set_menus(vec![Menu {
        name: t!("menu.app.name").into(),
        disabled: false,
        items: vec![
            MenuItem::action(t!("menu.app.about"), ShowAbout),
            MenuItem::separator(),
            MenuItem::os_submenu(t!("menu.app.services"), SystemMenuType::Services),
            MenuItem::separator(),
            MenuItem::action(t!("menu.app.hide"), HideApp),
            MenuItem::action(t!("menu.app.hide_others"), HideOtherApps),
            MenuItem::action(t!("menu.app.show_all"), ShowAllApps),
            MenuItem::separator(),
            MenuItem::action(t!("menu.app.quit"), QuitApp),
        ],
    }]);
}

fn show_about(_: &ShowAbout, cx: &mut App) {
    let about_title = "Pioneer".to_string();
    let about_details = format!(
        "{}\n{}",
        t!("menu.about.version", version = env!("CARGO_PKG_VERSION")),
        t!("menu.about.copyright")
    );
    let copy_payload = format!("{}\n{}", t!("menu.app.name"), about_details);

    cx.defer(move |cx| {
        let Some(active_window) = cx.active_window() else {
            return;
        };

        let _ = active_window.update(cx, move |_, window, cx| {
            let answer = window.prompt(
                PromptLevel::Info,
                about_title.as_str(),
                Some(about_details.as_str()),
                &[
                    PromptButton::ok(t!("common.ok").to_string()),
                    PromptButton::new(t!("common.copy").to_string()),
                ],
                cx,
            );

            cx.spawn(async move |cx| {
                if let Ok(1) = answer.await {
                    let _ = cx.update(|cx| {
                        cx.write_to_clipboard(ClipboardItem::new_string(copy_payload));
                    });
                }
            })
            .detach();
        });
    });
}

fn hide_app(_: &HideApp, cx: &mut App) {
    cx.hide();
}

fn hide_other_apps(_: &HideOtherApps, cx: &mut App) {
    cx.hide_other_apps();
}

fn show_all_apps(_: &ShowAllApps, cx: &mut App) {
    cx.unhide_other_apps();
}

fn quit_app(_: &QuitApp, cx: &mut App) {
    cx.quit();
}
