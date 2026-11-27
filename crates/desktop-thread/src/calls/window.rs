use super::{
    Calls, Device,
    media::{DeviceChoice, DeviceInventory, DeviceSelection, Frame, ParticipantMedia},
    speech::{Speech, SpeechFrame},
};
use gpui_kit::{
    base::{Disableable, StyledExt},
    component::{
        Icon,
        button::*,
        h_flex,
        input::{Input, InputState},
        v_flex,
    },
    prelude::*,
    *,
};
use pioneer_client::{
    calls::{ThreadCallPhase, ThreadCallTransition},
    core::ClientCore,
};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};

actions!(thread_calls, [DismissCallPanel]);

pub(super) fn init(cx: &mut App) {
    cx.bind_keys([KeyBinding::new(
        "escape",
        DismissCallPanel,
        Some("ThreadCallPanel"),
    )]);
}

// Dark in-call colors from videocall-rs bea64063, tokens.css and components.css.
const CALL_BACKGROUND: u32 = 0x000000;
const TILE_BACKGROUND: u32 = 0x1c1c1e;
const LABEL_SECONDARY: u32 = 0xebebf599;
const SEPARATOR: u32 = 0x54545880;
// Opaque composites of the dark theme tokens on the participant/sidebar surface.
const SEARCH_FILL: u32 = 0x313136;
const ROSTER_SEARCH_FILL: u32 = 0x313135;
const ROSTER_SEARCH_HINT: u32 = 0xebebf54d;
const ROSTER_WIDTH: f32 = 400.;
const COMPACT_CALL_WIDTH: f32 = 600.;
const BAR_TINT: u32 = 0x1c1c1e5c;
const CONTROL_GAP: f32 = 12.;
const BAR_INSET: f32 = 9.;
const TILE_SECONDARY: u32 = 0x98989f;
const SPEECH_GREEN: u32 = 0x30d158;
const HANG_UP_RED: u32 = 0xff4245;

// Glyph outlines from videocall-rs bea64063, videocall-theme/src/icons.rs.
const SEARCH_ICON: &[u8] = br#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.35-4.35"/></svg>"#;
const SETTINGS_CLOSE_ICON: &[u8] = br#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="m18 6-12 12M6 6l12 12"/></svg>"#;

const SELECT_CARET: &[u8] = br#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10" fill="currentColor"><path d="M0 2.5h10l-5 5z"/></svg>"#;

// Filled glyph from videocall-rs bea64063, videocall-theme/src/icons.rs.
const CALL_AVATAR: &[u8] = br#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" fill="currentColor"><path d="M458.159,404.216c-18.93-33.65-49.934-71.764-100.409-93.431c-28.868,20.196-63.938,32.087-101.745,32.087 c-37.828,0-72.898-11.89-101.767-32.087c-50.474,21.667-81.479,59.782-100.398,93.431C28.731,448.848,48.417,512,91.842,512 c43.426,0,164.164,0,164.164,0s120.726,0,164.153,0C463.583,512,483.269,448.848,458.159,404.216z"/><path d="M256.005,300.641c74.144,0,134.231-60.108,134.231-134.242v-32.158C390.236,60.108,330.149,0,256.005,0 c-74.155,0-134.252,60.108-134.252,134.242V166.4C121.753,240.533,181.851,300.641,256.005,300.641z"/></svg>"#;

// Filled glyph from videocall-rs bea64063, videocall-theme/src/icons.rs.
const CALL_HANG_UP: &[u8] = br#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor"><path d="M12.017 6.995c-2.306 0-4.534.408-6.215 1.507-1.737 1.135-2.788 2.944-2.797 5.451a4.8 4.8 0 0 0 .01.62c.015.193.047.512.138.763a2.557 2.557 0 0 0 2.579 1.677H7.31a2.685 2.685 0 0 0 2.685-2.684v-.645a.684.684 0 0 1 .684-.684h2.647a.686.686 0 0 1 .686.687v.645c0 .712.284 1.395.787 1.898.478.478 1.101.787 1.847.787h1.647a2.555 2.555 0 0 0 2.575-1.674c.09-.25.123-.57.137-.763.015-.2.022-.433.01-.617-.002-2.508-1.049-4.32-2.785-5.458-1.68-1.1-3.907-1.51-6.213-1.51Z"/></svg>"#;

// The control glyph paths come from videocall-rs bea64063, videocall-theme/src/icons.rs.
fn call_control_icon(path: &'static str) -> Icon {
    let svg: &[u8] = match path {
        "icons/mic.svg" => include_bytes!("icons/mic.svg"),
        "icons/mic-off.svg" => include_bytes!("icons/mic-off.svg"),
        "icons/video.svg" => include_bytes!("icons/camera.svg"),
        "icons/video-off.svg" => include_bytes!("icons/camera-off.svg"),
        "icons/settings.svg" => include_bytes!("icons/settings.svg"),
        "icons/users.svg" => include_bytes!("icons/people.svg"),
        "icons/people-off.svg" => include_bytes!("icons/people-off.svg"),
        _ => return Icon::empty().path(path),
    };
    Icon::empty().data(svg)
}

impl Calls {
    // Keep the verified call owner during a Gateway outage, until the account or session changes.
    fn selected_call_owner(&self) -> Option<&super::CallOwner> {
        self.owner
            .as_ref()
            .filter(|account| account.matches(&self.core))
    }

    fn stay_in_call(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        self.closing = false;
        cx.notify();
    }

    pub(super) fn show(&mut self, cx: &mut Context<Self>) {
        let owner = cx.entity();
        cx.defer(move |cx| show_call_window(owner, cx));
    }
}

fn show_call_window(owner: Entity<Calls>, cx: &mut App) {
    if let Some(handle) = owner.read(cx).window
        && handle
            .update(cx, |_, window, _| window.activate_window())
            .is_ok()
    {
        return;
    }

    let window_owner = owner.clone();
    let bounds = Bounds::centered(None, size(px(800.), px(540.)), cx);
    let result = gpui_kit::open_window(
        WindowOptions {
            titlebar: Some(TitlebarOptions {
                title: Some(t!("thread.call.title").to_string().into()),
                appears_transparent: false,
                traffic_light_position: None,
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            window_min_size: Some(size(px(420.), px(360.))),
            ..Default::default()
        },
        cx,
        move |window, cx| {
            let owner = window_owner.clone();
            window.on_window_should_close(cx, {
                let owner = owner.downgrade();
                move |_, cx| {
                    owner
                        .update(cx, |owner, cx| {
                            if owner.active.is_none()
                                && owner.held.is_none()
                                && owner.incoming.is_none()
                                && !owner.opening
                            {
                                owner.window = None;
                                return true;
                            }
                            owner.closing = true;
                            cx.notify();
                            false
                        })
                        .unwrap_or(true)
                }
            });
            cx.new(|cx| {
                let search = cx.new(|cx| InputState::new(window, cx));
                let search_subscription = cx.observe(&search, |_, _, cx| cx.notify());
                let search_focus = search.read(cx).focus_handle(cx);
                let subscription =
                    cx.observe_in(&owner, window, |view: &mut CallWindow, _, window, cx| {
                        view.sync_call(window, cx);
                        cx.notify();
                    });
                let mut view = CallWindow {
                    owner,
                    focus: cx.focus_handle(),
                    textures: HashMap::new(),
                    speech: HashMap::new(),
                    tiles: HashMap::new(),
                    animation_pending: false,
                    _subscription: subscription,
                    _resize: cx.observe_window_bounds(
                        window,
                        |view: &mut CallWindow, window, cx| {
                            if view.panel == Some(Panel::More)
                                && f32::from(window.viewport_size().width) >= COMPACT_CALL_WIDTH
                            {
                                view.panel = None;
                            }
                            cx.notify();
                        },
                    ),
                    names_key: None,
                    names_connection: None,
                    search,
                    search_focus,
                    _search_subscription: search_subscription,
                    names: HashMap::new(),
                    names_task: None,
                    panel: None,
                    active_window_title: None,
                    inventory: None,
                    open_device_list: None,
                    inventory_task: None,
                };
                view.sync_call(window, cx);
                view
            })
        },
    );
    owner.update(cx, move |owner, cx| {
        match result {
            Ok((handle, _)) => owner.window = Some(handle),
            Err(_) => owner.failed = true,
        }
        cx.notify();
    });
}

struct Texture {
    frame: Arc<Frame>,
    gpu: DynamicTexture,
}

impl Drop for Texture {
    fn drop(&mut self) {
        self.gpu.remove();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Panel {
    More,
    Participants,
    Audio,
    Video,
}

struct ParticipantTile {
    media: ParticipantMedia,
    texture: Option<(DynamicTexture, u32, u32)>,
    voice: SpeechFrame,
}

struct CallWindow {
    owner: Entity<Calls>,
    focus: FocusHandle,
    textures: HashMap<String, Texture>,
    speech: HashMap<String, Speech>,
    tiles: HashMap<String, ParticipantTile>,
    animation_pending: bool,
    _subscription: Subscription,
    _resize: Subscription,
    names_key: Option<(super::CallOwner, String)>,
    search: Entity<InputState>,
    search_focus: FocusHandle,
    _search_subscription: Subscription,
    names_connection: Option<u64>,
    names: HashMap<String, String>,
    names_task: Option<Task<()>>,
    panel: Option<Panel>,
    active_window_title: Option<String>,
    inventory: Option<DeviceInventory>,
    open_device_list: Option<&'static str>,
    inventory_task: Option<Task<()>>,
}

impl CallWindow {
    fn sync_call(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let owner = self.owner.read(cx);
        let account = owner.selected_call_owner();
        let call = owner
            .active
            .as_ref()
            .map(|s| &s.call)
            .or(owner.incoming.as_ref())
            .or_else(|| owner.held.as_ref().map(|s| &s.call));
        let names_key = account
            .zip(call)
            .map(|(account, call)| (account.clone(), call.workspace_id.clone()));
        let active_window_title = (account.is_some()
            && owner.media.is_some()
            && owner.incoming.is_none()
            && !owner
                .active
                .as_ref()
                .is_some_and(|s| s.call.phase == ThreadCallPhase::Ringing))
        .then(|| {
            if owner.title.is_empty() {
                t!("thread.call.title").to_string()
            } else {
                owner.title.clone()
            }
        });
        self.load_names(owner.core.clone(), names_key, owner.connection, window, cx);
        if self.active_window_title != active_window_title {
            if let Some(title) = &active_window_title {
                window.set_window_title(title);
            } else {
                window.set_window_title(t!("thread.call.title").as_ref());
            }
            self.active_window_title = active_window_title;
        }

        self.sync_media(window, cx);
    }

    fn sync_media(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let owner = self.owner.read(cx);
        let account = owner.selected_call_owner();
        let (frames, media_participants) = owner
            .media
            .as_ref()
            .map(|media| {
                (
                    media.view.frames.lock().unwrap().clone(),
                    media.view.participants.lock().unwrap().clone(),
                )
            })
            .unwrap_or_default();
        self.textures.retain(|id, _| frames.contains_key(id));
        self.speech
            .retain(|id, _| media_participants.contains_key(id));
        let now = cx.background_executor().now();
        let reduce_motion = cx.reduce_motion();
        self.tiles.clear();
        let mut animate = false;
        if let Some((account, session)) = account.zip(owner.active.as_ref()) {
            for participant in session
                .call
                .participants
                .iter()
                .filter(|member| !member.held)
            {
                let local = account.principal == participant.principal_id;
                let media = media_participants
                    .get(&participant.media_identity)
                    .copied()
                    .unwrap_or_default();
                let camera = if local {
                    session.devices.camera
                } else {
                    media.camera
                };
                let texture = frames
                    .get(&participant.media_identity)
                    .filter(|_| camera)
                    .and_then(|frame| {
                        self.texture(&participant.media_identity, frame, window)
                            .map(|texture| (texture, frame.width, frame.height))
                    });
                let voice = if (!local || texture.is_some())
                    && media_participants.contains_key(&participant.media_identity)
                {
                    self.speech
                        .entry(participant.media_identity.clone())
                        .or_insert_with(|| Speech::new(now))
                        .update(media.audio_level, media.microphone, reduce_motion, now)
                } else {
                    SpeechFrame::default()
                };
                animate |= voice.animate;
                self.tiles.insert(
                    participant.media_identity.clone(),
                    ParticipantTile {
                        media,
                        texture,
                        voice,
                    },
                );
            }
        }
        if animate && !self.animation_pending {
            self.animation_pending = true;
            cx.on_next_frame(window, |view, window, cx| {
                view.animation_pending = false;
                view.sync_media(window, cx);
                cx.notify();
            });
        }
    }

    fn texture(
        &mut self,
        id: &str,
        frame: &Arc<Frame>,
        window: &mut Window,
    ) -> Option<DynamicTexture> {
        let pixels = size(
            DevicePixels(frame.width as i32),
            DevicePixels(frame.height as i32),
        );
        if let Some(texture) = self.textures.get_mut(id) {
            if Arc::ptr_eq(&texture.frame, frame) {
                return Some(texture.gpu);
            }
            if texture.frame.width == frame.width && texture.frame.height == frame.height {
                if window
                    .update_dynamic_texture(
                        texture.gpu,
                        Bounds::new(point(DevicePixels(0), DevicePixels(0)), pixels),
                        &frame.bytes,
                        frame.width * 4,
                    )
                    .is_ok()
                {
                    texture.frame = frame.clone();
                    return Some(texture.gpu);
                }
                return Some(texture.gpu);
            }
        }
        let gpu = window
            .create_dynamic_texture(pixels, &frame.bytes, frame.width * 4)
            .ok()?;
        self.textures.insert(
            id.to_owned(),
            Texture {
                frame: frame.clone(),
                gpu,
            },
        );
        Some(gpu)
    }

    fn load_names(
        &mut self,
        core: Arc<ClientCore>,
        key: Option<(super::CallOwner, String)>,
        connection: Option<u64>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.names_key != key {
            self.names_key = key.clone();
            self.names_connection = None;
            self.names.clear();
            self.names_task.take();
            self.panel = None;
            self.open_device_list = None;
            self.inventory = None;
            self.inventory_task.take();
            self.search
                .update(cx, |search, cx| search.set_value("", window, cx));
        }
        if let (Some(connection), Some((_, workspace))) = (connection, key)
            && self.names_connection != Some(connection)
        {
            self.names_connection = Some(connection);
            let read =
                cx.background_spawn(async move { core.thread_call_names(connection, &workspace) });
            self.names_task = Some(cx.spawn(async move |view, cx| {
                if let Ok(names) = read.await {
                    let _ = view.update(cx, |view, cx| {
                        if view.names_connection == Some(connection) {
                            view.names = names;
                        }
                        cx.notify();
                    });
                }
            }));
        }
    }

    fn toggle_panel(&mut self, panel: Panel, cx: &mut Context<Self>) {
        self.panel = (self.panel != Some(panel)).then_some(panel);
        cx.notify();
    }

    fn open_device_panel(&mut self, panel: Panel, window: &mut Window, cx: &mut Context<Self>) {
        self.focus.focus(window, cx);
        self.panel = Some(panel);
        self.open_device_list = None;
        let Some(media) = self.owner.read(cx).media.as_ref() else {
            self.inventory = None;
            cx.notify();
            return;
        };
        let load = media.inventory();
        self.inventory_task = Some(cx.spawn(async move |view, cx| {
            let inventory = load.await.ok();
            let _ = view.update(cx, |view, cx| {
                view.inventory = inventory;
                view.inventory_task.take();
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn device_rows(
        &self,
        id: &'static str,
        label: String,
        choices: &[(String, DeviceChoice)],
        selection: &DeviceSelection,
        disabled: bool,
        cx: &mut Context<Self>,
    ) -> Div {
        let rows = v_flex().gap_2().child(
            div()
                .px_1()
                .text_size(px(15.))
                .line_height(px(20.))
                .font_semibold()
                .child(label.clone()),
        );
        if choices.is_empty() {
            return rows.child(
                div()
                    .text_color(rgba(LABEL_SECONDARY))
                    .child(t!("thread.call.no_devices").to_string()),
            );
        }
        let current = choices
            .iter()
            .find(|(_, choice)| selection.contains(choice))
            .map(|(name, _)| name.clone())
            .unwrap_or_else(|| t!("thread.call.system_default").to_string());
        let expanded = self.open_device_list == Some(id);
        rows.child(
            Button::new(id)
                .custom(call_panel_button(cx))
                .p_0()
                .h(px(44.))
                .w_full()
                .rounded(px(14.))
                .disabled(disabled)
                .aria_expanded(expanded)
                .accessibility_label(label)
                .child(
                    h_flex()
                        .w_full()
                        .h_full()
                        .px_4()
                        .gap_2()
                        .justify_between()
                        .relative()
                        .child(continuous_surface(14., rgb(SEARCH_FILL).into(), None))
                        .text_size(px(17.))
                        .line_height(px(22.))
                        .child(
                            div()
                                .min_w_0()
                                .overflow_hidden()
                                .text_ellipsis()
                                .whitespace_nowrap()
                                .child(current.clone()),
                        )
                        .child(
                            Icon::empty()
                                .data(SELECT_CARET)
                                .size(px(10.))
                                .flex_shrink_0(),
                        ),
                )
                .on_click(cx.listener(move |view, _, _, cx| {
                    cx.stop_propagation();
                    view.open_device_list = (!expanded).then_some(id);
                    cx.notify();
                })),
        )
        .when(expanded, |rows| {
            rows.child(
                v_flex()
                    .max_h(px(172.))
                    .relative()
                    .child(continuous_surface(14., rgb(SEARCH_FILL).into(), None))
                    .child(
                        v_flex()
                            .id(SharedString::from(format!("{id}-choices")))
                            .min_h_0()
                            .overflow_y_scroll()
                            .children(choices.iter().enumerate().map(|(index, (name, choice))| {
                                let choice = choice.clone();
                                let selected = selection.contains(&choice);
                                Button::new((id, index))
                                    .custom(call_panel_button(cx))
                                    .w_full()
                                    .h(px(44.))
                                    .rounded(px(10.))
                                    .disabled(disabled)
                                    .toggled(selected)
                                    .accessibility_label(name.clone())
                                    .child(
                                        h_flex()
                                            .w_full()
                                            .justify_between()
                                            .child(
                                                div()
                                                    .min_w_0()
                                                    .overflow_hidden()
                                                    .text_ellipsis()
                                                    .whitespace_nowrap()
                                                    .child(name.clone()),
                                            )
                                            .when(selected, |row| {
                                                row.child(
                                                    div().text_color(rgb(SPEECH_GREEN)).child("✓"),
                                                )
                                            }),
                                    )
                                    .on_click(cx.listener(move |view, _, _, cx| {
                                        cx.stop_propagation();
                                        view.open_device_list = None;
                                        view.owner.update(cx, |owner, cx| {
                                            owner.select_call_device(choice.clone(), cx)
                                        });
                                        cx.notify();
                                    }))
                            })),
                    ),
            )
        })
    }

    fn choice(
        &self,
        id: &'static str,
        label: String,
        enabled: bool,
        callback: impl Fn(&mut Calls, &mut Window, &mut Context<Calls>) + 'static,
    ) -> Button {
        let owner = self.owner.clone();
        Button::new(id)
            .outline()
            .label(label)
            .disabled(!enabled)
            .on_click(move |_, window, cx| {
                owner.update(cx, |owner, cx| callback(owner, window, cx))
            })
    }
}

impl Render for CallWindow {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let owner = self.owner.read(cx);
        let busy = owner.command.is_some();
        let closing = owner.closing;
        let call_owner = owner.selected_call_owner();
        let incoming = call_owner.and(owner.incoming.as_ref()).cloned();
        let active = call_owner.and(owner.active.as_ref()).cloned();
        let connected = call_owner.is_some() && owner.media.is_some();
        let media_connected = call_owner.is_some()
            && owner
                .media
                .as_ref()
                .is_some_and(|media| media.view.connected.load(Ordering::Relaxed));
        let held = call_owner.and(owner.held.as_ref()).cloned();
        let failed = owner.failed;
        let opening = owner.opening;
        let title = owner.title.clone();
        let selection = owner.device_selection.clone();
        let principal = call_owner.map(|account| account.principal.clone());
        let local_name = owner
            .core
            .current_auth()
            .filter(|auth| principal.as_deref() == Some(auth.principal.id.as_str()))
            .map(|auth| auth.principal.display_name)
            .filter(|name| !name.trim().is_empty());

        let bounds = window.viewport_size();
        let width = f32::from(bounds.width);
        let compact = width < COMPACT_CALL_WIDTH;
        let control_size = if width < 640. { 44. } else { 48. };
        let can_device = !busy && connected;
        let mut cells = Vec::new();
        let mut self_view = None;
        let mut remote_video_visible = false;
        let mut roster = Vec::new();
        if let Some(session) = &active {
            let remote_count = session
                .call
                .participants
                .iter()
                .filter(|member| {
                    !member.held && principal.as_deref() != Some(member.principal_id.as_str())
                })
                .count();
            for participant in session
                .call
                .participants
                .iter()
                .filter(|member| !member.held)
            {
                let local = principal.as_deref() == Some(participant.principal_id.as_str());
                let label = self
                    .names
                    .get(&participant.principal_id)
                    .cloned()
                    .or_else(|| local.then(|| local_name.clone()).flatten())
                    .unwrap_or_else(|| {
                        if local {
                            t!("thread.call.you").to_string()
                        } else {
                            t!("thread.call.participant").to_string()
                        }
                    });
                let tile_media = self.tiles.get(&participant.media_identity);
                let media = tile_media.map(|tile| tile.media).unwrap_or_default();
                let roster_media = if local {
                    ParticipantMedia {
                        microphone: session.devices.microphone,
                        ..media
                    }
                } else {
                    media
                };
                roster.push((label.clone(), roster_media, local));
                let camera = if local {
                    session.devices.camera
                } else {
                    media.camera
                };
                let texture = tile_media.and_then(|tile| tile.texture).filter(|_| camera);
                let voice = tile_media.map(|tile| tile.voice).unwrap_or_default();
                if local {
                    self_view = Some(tile(Tile::Preview { camera }, texture, media, voice, false));
                } else {
                    remote_video_visible |= texture.is_some();
                    cells.push(tile(
                        Tile::Participant(label),
                        texture,
                        media,
                        voice,
                        remote_count == 1,
                    ));
                }
            }
        }
        let full_bleed = cells.len() == 1;
        let grouped = cells.len() > 1;
        let grid = v_flex()
            .size_full()
            .gap_2()
            .when(!full_bleed, |grid| {
                grid.p(px(if compact { 8. } else { 16. }))
            })
            .children(cells_into_rows(
                cells,
                width,
                f32::from(bounds.height),
                full_bleed,
            ));
        let waiting = active
            .as_ref()
            .is_some_and(|session| session.call.phase == ThreadCallPhase::Ringing);
        let status = if incoming.is_some() && active.is_some() {
            t!("thread.call.waiting")
        } else if incoming.is_some() {
            t!("thread.call.incoming")
        } else if held.is_some() && active.is_none() {
            t!("thread.call.held")
        } else if waiting {
            t!("thread.call.ringing")
        } else if connected {
            t!("thread.call.connected")
        } else if opening || active.is_some() {
            t!("thread.call.connecting")
        } else {
            t!("thread.call.ended")
        };
        let incoming_name = incoming.as_ref().and_then(|call| {
            self.names_key
                .as_ref()
                .filter(|(_, workspace)| workspace == &call.workspace_id)
                .and_then(|_| self.names.get(&call.initiator_id))
                .cloned()
        });
        let heading = incoming_name
            .or((!title.is_empty()).then_some(title))
            .unwrap_or_else(|| t!("thread.call.title").to_string());

        let mut screen = div()
            .relative()
            .size_full()
            .overflow_hidden()
            .bg(rgb(CALL_BACKGROUND))
            .text_color(rgb(0xffffff))
            .track_focus(&self.focus)
            .when(
                matches!(self.panel, Some(Panel::Audio | Panel::Video | Panel::More)),
                |screen| {
                    screen.key_context("ThreadCallPanel").on_action(cx.listener(
                        |view, _: &DismissCallPanel, _, cx| {
                            if view.open_device_list.take().is_none() {
                                view.panel = None;
                            }
                            cx.notify();
                        },
                    ))
                },
            )
            .child(grid);

        if active.is_some() {
            screen = screen.child(
                div()
                    .absolute()
                    .bottom_0()
                    .left_0()
                    .right_0()
                    .h(px(128.))
                    .bg(linear_gradient(
                        180.,
                        linear_color_stop(rgba(0x00000000), 0.),
                        linear_color_stop(rgba(0x00000059), 1.),
                    )),
            );
        }
        if let Some(session) = &active {
            let mic = session.devices.microphone;
            let cam = session.devices.camera;
            let owner = self.owner.clone();
            let camera_owner = owner.clone();
            let hangup_owner = owner.clone();
            let panel = if compact {
                Panel::More
            } else {
                Panel::Participants
            };
            let open_panel = self.panel.filter(|_| incoming.is_none() && !closing);
            let pane_open = open_panel == Some(panel);
            let tint = rgba(BAR_TINT);
            // The grid needs an opaque plate to hide seams between tiles.
            let video_material = !grouped && remote_video_visible;
            let material = if grouped {
                rgb(TILE_BACKGROUND)
                    .blend(rgb(CALL_BACKGROUND).opacity(0.24))
                    .blend(tint)
            } else {
                tint
            };
            let mut shadows = vec![BoxShadow::new(px(0.), px(1.), rgba(0xffffff1a).into()).inset()];
            // GPUI paints drop shadows under the entire shape, including its
            // interior. Keep them for the opaque grid and the black canvas,
            // where they cannot dim the video through the translucent plate.
            if !video_material {
                // GPUI takes Gaussian sigma, half the CSS shadow blur radius.
                shadows.extend([
                    BoxShadow::new(px(0.), px(2.), rgba(0x00000066).into()).blur_radius(px(3.)),
                    BoxShadow::new(px(0.), px(8.), rgba(0x00000066).into()).blur_radius(px(12.)),
                ]);
            }
            let mut buttons = vec![
                icon_button(
                    control_size,
                    "call-mic",
                    if mic {
                        "icons/mic.svg"
                    } else {
                        "icons/mic-off.svg"
                    },
                    t!("thread.call.microphone").to_string(),
                    if mic {
                        t!("thread.call.mute").to_string()
                    } else {
                        t!("thread.call.unmute").to_string()
                    },
                    Some(mic),
                    false,
                )
                .disabled(!can_device)
                .on_click(move |_, _, cx| {
                    owner.update(cx, |owner, cx| owner.toggle(Device::Microphone, cx))
                }),
                icon_button(
                    control_size,
                    "call-video",
                    if cam {
                        "icons/video.svg"
                    } else {
                        "icons/video-off.svg"
                    },
                    t!("thread.call.camera").to_string(),
                    if cam {
                        t!("thread.call.camera_off").to_string()
                    } else {
                        t!("thread.call.camera_on").to_string()
                    },
                    Some(cam),
                    false,
                )
                .disabled(!can_device)
                .on_click(move |_, _, cx| {
                    camera_owner.update(cx, |owner, cx| owner.toggle(Device::Camera, cx))
                }),
                {
                    let label = if compact {
                        t!("thread.call.more")
                    } else {
                        t!("thread.call.open_participants")
                    }
                    .to_string();
                    icon_button(
                        control_size,
                        "call-panel",
                        if compact {
                            "icons/ellipsis.svg"
                        } else if pane_open {
                            "icons/users.svg"
                        } else {
                            "icons/people-off.svg"
                        },
                        label.clone(),
                        label,
                        Some(pane_open),
                        false,
                    )
                    .aria_expanded(pane_open)
                    .on_click(cx.listener(move |view, _, _, cx| view.toggle_panel(panel, cx)))
                },
            ];
            if !compact {
                let label = t!("thread.call.device_settings").to_string();
                buttons.push(
                    icon_button(
                        control_size,
                        "call-settings",
                        "icons/settings.svg",
                        label.clone(),
                        label,
                        Some(matches!(open_panel, Some(Panel::Audio | Panel::Video))),
                        false,
                    )
                    .disabled(!connected)
                    .aria_expanded(matches!(self.panel, Some(Panel::Audio | Panel::Video)))
                    .on_click(cx.listener(|view, _, window, cx| {
                        view.open_device_panel(Panel::Audio, window, cx)
                    })),
                );
            }
            buttons.push(
                icon_button(
                    control_size,
                    "call-hangup",
                    "icons/phone-off.svg",
                    t!("thread.call.hang_up").to_string(),
                    t!("thread.call.hang_up").to_string(),
                    None,
                    true,
                )
                .disabled(busy)
                .on_click(move |_, _, cx| {
                    hangup_owner.update(cx, |owner, cx| owner.act(ThreadCallTransition::Leave, cx))
                }),
            );
            // Adjacent overlays use the same buttons that the flex row renders.
            let control_count = buttons.len() as f32;
            let bar_width =
                control_count * control_size + (control_count - 1.) * CONTROL_GAP + 2. * BAR_INSET;
            let bar_height = control_size + 2. * BAR_INSET;
            let roster_open = open_panel == Some(Panel::Participants);
            let sidebar_width = if width <= COMPACT_CALL_WIDTH {
                width
            } else {
                ROSTER_WIDTH.min(width - bar_width - 32.)
            };
            let canvas_left = if roster_open && width > COMPACT_CALL_WIDTH {
                sidebar_width
            } else {
                0.
            };
            let bar_right = (width + canvas_left + bar_width) / 2.;
            let preview_width = if compact {
                width * 0.30
            } else {
                (width * 0.18).clamp(160., 240.)
            };
            let preview_right = if compact { 8. } else { 16. };
            let preview_left = width - preview_right - preview_width;
            let preview_bottom = if compact {
                92.
            } else if preview_left < bar_right + 8. {
                16. + bar_height + 8.
            } else {
                16.
            };
            if let Some(preview) = self_view {
                screen = screen.child(
                    div()
                        .absolute()
                        .right(px(preview_right))
                        .bottom(px(preview_bottom))
                        .w(px(preview_width))
                        .h(px((preview_width - 2.) * 9. / 16. + 2.))
                        .rounded(px(14.))
                        .border_1()
                        .border_color(rgba(SEPARATOR))
                        .shadow(vec![
                            BoxShadow::new(px(0.), px(2.), rgba(0x00000066).into())
                                .blur_radius(px(6.)),
                            BoxShadow::new(px(0.), px(8.), rgba(0x00000066).into())
                                .blur_radius(px(24.)),
                        ])
                        .overflow_hidden()
                        .flex()
                        .child(preview)
                        .child(
                            div()
                                .absolute()
                                .top_2()
                                .right_2()
                                .size(px(8.))
                                .rounded_full()
                                .shadow(vec![
                                    BoxShadow::new(px(0.), px(0.), rgba(0x00000059).into())
                                        .spread_radius(px(2.)),
                                ])
                                .when(media_connected, |dot| dot.bg(rgb(SPEECH_GREEN)))
                                .when(!media_connected, |dot| {
                                    dot.border_2().border_color(rgb(0xff9f0a))
                                }),
                        ),
                );
            }
            if roster_open {
                // This backdrop must sit below the control bar. The list itself
                // is painted above both, and owns hits within its own bounds.
                screen =
                    screen.child(
                        div()
                            .id("call-participants-dismiss")
                            .absolute()
                            .inset_0()
                            .left(px(sidebar_width))
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.toggle_panel(Panel::Participants, cx)
                            })),
                    );
            }
            let controls = h_flex()
                .flex_none()
                .occlude()
                .gap(px(CONTROL_GAP))
                .p_2()
                .border_1()
                .border_color(rgba(0xffffff24))
                .rounded_full()
                .bg(material)
                .shadow(shadows)
                .children(buttons);
            screen = screen.child(
                div()
                    .absolute()
                    .bottom_4()
                    .left(px(canvas_left))
                    .right_0()
                    .flex()
                    .justify_center()
                    .child(controls),
            );
            if open_panel == Some(Panel::More) {
                screen = screen.child(
                    div()
                        .id("call-panel-dismiss")
                        .absolute()
                        .inset_0()
                        .bottom(px(16. + bar_height))
                        .on_click(cx.listener(|view, _, _, cx| {
                            view.panel = None;
                            cx.notify();
                        })),
                );
                screen = screen.child(
                    v_flex()
                        .absolute()
                        .bottom(px(16. + bar_height + 8.))
                        .right(px((width - bar_width) / 2.))
                        .min_w(px(268.))
                        .max_w(px(320_f32.min((width + bar_width) / 2. - 16.)))
                        .occlude()
                        .rounded(px(18.))
                        .border_1()
                        .border_color(rgba(0))
                        .shadow(vec![
                            BoxShadow::new(px(0.), px(2.), rgba(0x00000066).into())
                                .blur_radius(px(6.)),
                            BoxShadow::new(px(0.), px(8.), rgba(0x00000066).into())
                                .blur_radius(px(24.)),
                        ])
                        .p_1()
                        .child(continuous_surface(
                            18.,
                            rgb(TILE_BACKGROUND).into(),
                            Some(rgb(0x3d3d3f).into()),
                        ))
                        .children(
                            [
                                (
                                    Panel::Participants,
                                    "icons/people-off.svg",
                                    t!("thread.call.open_participants").to_string(),
                                ),
                                (
                                    Panel::Audio,
                                    "icons/settings.svg",
                                    t!("thread.call.device_settings").to_string(),
                                ),
                            ]
                            .into_iter()
                            .enumerate()
                            .map(|(index, (panel, icon, label))| {
                                Button::new(("call-more-item", index))
                                    .custom(call_panel_button(cx))
                                    .h(px(44.))
                                    .p_0()
                                    .rounded(px(10.))
                                    .w_full()
                                    .disabled(panel != Panel::Participants && !connected)
                                    .accessibility_label(label.clone())
                                    .child(
                                        h_flex()
                                            .w_full()
                                            .min_w_0()
                                            .gap_3()
                                            .px_3()
                                            .text_color(rgb(0xffffff))
                                            .text_size(px(17.))
                                            .line_height(px(22.))
                                            .child(call_control_icon(icon).size(px(18.)))
                                            .child(div().min_w_0().truncate().child(label)),
                                    )
                                    .on_click(cx.listener(move |view, _, window, cx| {
                                        if panel == Panel::Participants {
                                            view.toggle_panel(panel, cx);
                                        } else {
                                            view.open_device_panel(Panel::Audio, window, cx);
                                        }
                                    }))
                            }),
                        ),
                );
            }
            if let Some(tab @ (Panel::Audio | Panel::Video)) = open_panel {
                let title = t!("thread.call.settings_title").to_string();
                let mut content = v_flex().gap_4().when(failed, |content| {
                    content.child(
                        div()
                            .text_color(rgb(0xffaeb7))
                            .child(t!("thread.call.failed").to_string()),
                    )
                });
                if let Some(inventory) = self.inventory.as_ref().filter(|_| connected) {
                    if tab == Panel::Audio {
                        content = content
                            .child(self.device_rows(
                                "call-microphone-choice",
                                t!("thread.call.microphone").to_string(),
                                &inventory.microphones,
                                &selection,
                                !can_device,
                                cx,
                            ))
                            .child(self.device_rows(
                                "call-speaker-choice",
                                t!("thread.call.speaker").to_string(),
                                &inventory.speakers,
                                &selection,
                                !can_device,
                                cx,
                            ));
                    } else if inventory.camera_error {
                        content = content.child(t!("thread.call.devices_unavailable").to_string());
                    } else {
                        content = content.child(self.device_rows(
                            "call-camera-choice",
                            t!("thread.call.camera").to_string(),
                            &inventory.cameras,
                            &selection,
                            !can_device,
                            cx,
                        ));
                    }
                } else {
                    let message = if connected && self.inventory_task.is_some() {
                        t!("thread.call.connecting")
                    } else {
                        t!("thread.call.devices_unavailable")
                    };
                    content = content.child(message.to_string());
                }
                let stacked = width < 640.;
                let tabs = div()
                    .flex()
                    .flex_none()
                    .gap_1()
                    .when(stacked, |tabs| tabs.flex_row())
                    .when(!stacked, |tabs| tabs.flex_col().w(px(160.)))
                    .children(
                        [
                            (Panel::Audio, t!("thread.call.audio_settings").to_string()),
                            (Panel::Video, t!("thread.call.video_settings").to_string()),
                        ]
                        .into_iter()
                        .enumerate()
                        .map(|(index, (panel, label))| {
                            Button::new(("call-settings-tab", index))
                                .custom(call_panel_button(cx))
                                .p_0()
                                .h(px(44.))
                                .rounded(px(10.))
                                .toggled(panel == tab)
                                .accessibility_label(label.clone())
                                .child(
                                    h_flex()
                                        .w_full()
                                        .h_full()
                                        .px_3()
                                        .relative()
                                        .text_size(px(17.))
                                        .line_height(px(22.))
                                        .font_weight(FontWeight::NORMAL)
                                        .when(panel == tab, |tab| {
                                            tab.child(continuous_surface(
                                                10.,
                                                rgba(0x78788052).into(),
                                                None,
                                            ))
                                            .font_semibold()
                                        })
                                        .child(label),
                                )
                                .on_click(cx.listener(move |view, _, window, cx| {
                                    cx.stop_propagation();
                                    view.open_device_panel(panel, window, cx);
                                }))
                        }),
                    );
                screen = screen.child(
                    div()
                        .id("call-settings-scrim")
                        .absolute()
                        .inset_0()
                        .flex()
                        .when(stacked, |scrim| scrim.items_end().p_2())
                        .when(!stacked, |scrim| scrim.items_center().p_4())
                        .justify_center()
                        .bg(rgba(0x0000007a))
                        .on_click(cx.listener(|view, _, _, cx| {
                            view.panel = None;
                            view.open_device_list = None;
                            cx.notify();
                        }))
                        .child(
                            v_flex()
                                .id("call-settings-dialog")
                                .occlude()
                                .on_click(cx.listener(|view, _, _, cx| {
                                    cx.stop_propagation();
                                    if view.open_device_list.take().is_some() {
                                        cx.notify();
                                    }
                                }))
                                .w_full()
                                .max_w(px(768.))
                                .max_h(px((f32::from(bounds.height)
                                    - if stacked { 16. } else { 32. })
                                .min(640.)))
                                .relative()
                                .rounded(px(28.))
                                .border_1()
                                .border_color(rgba(0))
                                .child(continuous_surface(
                                    28.,
                                    rgb(TILE_BACKGROUND).into(),
                                    Some(rgb(0x38383b).into()),
                                ))
                                .shadow(vec![
                                    BoxShadow::new(px(0.), px(4.), rgba(0x00000066).into()).blur_radius(px(12.)),
                                    BoxShadow::new(px(0.), px(16.), rgba(0x00000066).into()).blur_radius(px(48.)),
                                ])
                                .child(
                                    h_flex()
                                        .h(px(68.))
                                        .flex_none()
                                        .pl_5()
                                        .pr_3()
                                        .justify_between()
                                        .child(
                                            div().text_size(px(17.)).line_height(px(22.)).font_semibold().child(title),
                                        )
                                        .child(
                                            Button::new("call-close-settings")
                                                .custom(call_panel_button(cx))
                                                .size(px(44.))
                                                .p_0()
                                                .rounded_full()
                                                .accessibility_label(
                                                    t!("thread.call.close").to_string(),
                                                )
                                                .child(
                                                    Icon::empty()
                                                        .data(SETTINGS_CLOSE_ICON)
                                                        .size(px(24.))
                                                        .text_color(rgb(0xffffff)),
                                                )
                                                .on_click(cx.listener(|view, _, _, cx| {
                                                    cx.stop_propagation();
                                                    view.panel = None;
                                                    view.open_device_list = None;
                                                    cx.notify();
                                                })),
                                        ),
                                )
                                .child(
                                    div()
                                        .id("call-settings-scroll")
                                        .flex()
                                        .min_h_0()
                                        .overflow_y_scroll()
                                        .px_5().pt_2().pb_5()
                                        .when(stacked, |layout| layout.flex_col().gap_3())
                                        .when(!stacked, |layout| layout.gap_5())
                                        .child(tabs)
                                        .child(
                                            v_flex()
                                                .when(!stacked, |section| section.flex_1())
                                                .when(stacked, |section| section.flex_none())
                                                .min_w_0()
                                                .gap_4()
                                                .child(div().text_size(px(20.)).line_height(px(25.)).child(
                                                    if tab == Panel::Audio {
                                                        t!("thread.call.audio_settings").to_string()
                                                    } else {
                                                        t!("thread.call.video_settings").to_string()
                                                    },
                                                ))
                                                .child(div().text_size(px(15.)).line_height(px(20.))
                                                    .font_weight(FontWeight::NORMAL)
                                                    .text_color(rgba(LABEL_SECONDARY)).child(if tab == Panel::Audio {
                                                        t!("thread.call.audio_settings_description").to_string()
                                                    } else {
                                                        t!("thread.call.video_settings_description").to_string()
                                                    }))
                                                .child(content),
                                        ),
                                ),
                        ),
                );
            }
            if roster_open {
                let search = self.search.read(cx);
                let empty_search = search.value().is_empty();
                let search_focused = self.search_focus.is_focused(window);
                let query = search.value().trim().to_lowercase();
                let search_label = t!("thread.call.search_participants").to_string();
                roster.sort_by_key(|(_, _, local)| !local);
                roster.retain(|(name, _, local)| *local || name.to_lowercase().contains(&query));
                screen = screen.child(
                    v_flex()
                        .id("call-participants")
                        .absolute()
                        .top_0()
                        .bottom_0()
                        .left_0()
                        .w(px(sidebar_width))
                        .occlude()
                        .bg(rgb(TILE_BACKGROUND))
                        .when(width > COMPACT_CALL_WIDTH, |sidebar| {
                            sidebar.border_1().border_color(rgba(0xffffff24))
                        })
                        .child(
                            h_flex()
                                .flex_shrink_0()
                                .justify_between()
                                .px_4()
                                .h(px(60.))
                                .child(
                                    div()
                                        .font_semibold()
                                        .text_size(px(17.))
                                        .line_height(px(22.))
                                        .child(t!("thread.call.participants").to_string()),
                                )
                                .child(
                                    Button::new("call-participants-close")
                                        .ghost()
                                        .compact()
                                        .size(px(44.))
                                        .accessibility_label(t!("thread.call.close").to_string())
                                        .child(
                                            div()
                                                .text_size(px(22.))
                                                .line_height(px(28.))
                                                .text_color(rgb(0xffffff))
                                                .child("×"),
                                        )
                                        .on_click(cx.listener(|view, _, _, cx| {
                                            view.toggle_panel(Panel::Participants, cx)
                                        })),
                                ),
                        )
                        .child(
                            v_flex()
                                .id("call-participants-list")
                                .flex_1()
                                .min_h_0()
                                .overflow_y_scroll()
                                .px_4()
                                .pt_2()
                                .pb_4()
                                .child(
                                    h_flex()
                                        .relative()
                                        .h(px(44.))
                                        .flex_shrink_0()
                                        .rounded_full()
                                        .bg(rgb(ROSTER_SEARCH_FILL))
                                        .px_4()
                                        .when(search_focused, |field| {
                                            field.shadow(vec![
                                                BoxShadow::new(
                                                    px(0.),
                                                    px(0.),
                                                    rgb(0x0a84ff).into(),
                                                )
                                                .spread_radius(px(2.)),
                                            ])
                                        })
                                        // Keep the editor responsible for all input and
                                        // accessibility. The hint uses the call palette.
                                        .when(empty_search, |field| {
                                            field.child(
                                                h_flex()
                                                    .absolute()
                                                    .top(px(-1.))
                                                    .bottom(px(1.))
                                                    .left(px(42.))
                                                    .right_4()
                                                    .overflow_hidden()
                                                    .font_weight(FontWeight::NORMAL)
                                                    .text_size(px(17.))
                                                    .line_height(px(22.))
                                                    .text_color(rgba(ROSTER_SEARCH_HINT))
                                                    .child(search_label.clone()),
                                            )
                                        })
                                        .child(
                                            Input::new(&self.search)
                                                .aria_label(search_label)
                                                .px_0()
                                                .h_full()
                                                .gap_2()
                                                .text_size(px(17.))
                                                .line_height(px(22.))
                                                .appearance(false)
                                                .bordered(false)
                                                .text_color(rgb(0xffffff))
                                                .min_w_0()
                                                .prefix(
                                                    Icon::empty()
                                                        .data(SEARCH_ICON)
                                                        .size(px(18.))
                                                        .text_color(rgba(LABEL_SECONDARY)),
                                                ),
                                        ),
                                )
                                .child(
                                    v_flex()
                                        .pt_4()
                                        .gap_2()
                                        .child(
                                            div()
                                                .px_4()
                                                .pb_2()
                                                .text_size(px(17.))
                                                .line_height(px(22.))
                                                .font_semibold()
                                                .child(t!("thread.call.in_call").to_string()),
                                        )
                                        .child(v_flex().children(
                                            roster.into_iter().enumerate().map(|(index, row)| {
                                                participant_row(row, index > 0)
                                            }),
                                        )),
                                ),
                        ),
                );
            }
        }

        let needs_recovery = failed && active.is_some() && !connected && incoming.is_none();
        if closing
            || incoming.is_some()
            || active.is_none()
            || needs_recovery
            || !connected
            || waiting
        {
            let mut controls = h_flex().justify_center().gap_2();
            if closing {
                controls = controls
                    .child(self.choice(
                        "call-remain",
                        t!("thread.call.stay").to_string(),
                        !busy,
                        Calls::stay_in_call,
                    ))
                    .child(self.choice(
                        "call-leave-confirmed",
                        t!("thread.call.leave").to_string(),
                        !busy,
                        |owner, window, cx| {
                            if owner.active.is_some() || owner.held.is_some() {
                                owner.act(ThreadCallTransition::Leave, cx);
                            } else if owner.incoming.is_some() {
                                owner.act(ThreadCallTransition::Decline, cx);
                            } else {
                                owner.closing = false;
                                owner.window = None;
                                window.remove_window();
                            }
                        },
                    ));
            } else if incoming.is_some() {
                controls = controls
                    .child(self.choice(
                        "call-decline",
                        t!("thread.call.decline").to_string(),
                        !busy,
                        |owner, _, cx| owner.act(ThreadCallTransition::Decline, cx),
                    ))
                    .child(self.choice(
                        "call-answer",
                        if active.is_some() {
                            t!("thread.call.hold_answer").to_string()
                        } else {
                            t!("thread.call.answer").to_string()
                        },
                        !busy && !(active.is_some() && held.is_some()),
                        |owner, _, cx| owner.act(ThreadCallTransition::Join, cx),
                    ));
            } else if held.is_some() && active.is_none() {
                controls = controls
                    .child(self.choice(
                        "call-resume",
                        t!("thread.call.resume").to_string(),
                        !busy,
                        |owner, _, cx| owner.act(ThreadCallTransition::Resume, cx),
                    ))
                    .child(self.choice(
                        "call-leave-held",
                        t!("thread.call.leave").to_string(),
                        !busy,
                        |owner, _, cx| owner.act(ThreadCallTransition::Leave, cx),
                    ));
            } else if needs_recovery {
                controls = controls.child(self.choice(
                    "call-recover-media",
                    t!("thread.call.recover").to_string(),
                    !busy,
                    |owner, _, cx| owner.recover_media(cx),
                ));
            } else if active.is_none() && !opening {
                controls = controls.child(self.choice(
                    "call-close",
                    t!("thread.call.close").to_string(),
                    !busy,
                    |owner, window, _| {
                        owner.window = None;
                        window.remove_window();
                    },
                ));
            }
            let prompt = if closing {
                t!("thread.call.leave_question").to_string()
            } else {
                status.to_string()
            };
            let card = v_flex()
                .gap_3()
                .items_center()
                .max_w(px((width - 32.).max(160.)))
                .p_5()
                .bg(rgb(0x242426))
                .rounded(px(14.))
                .when(!closing, |card| {
                    card.child(div().font_semibold().text_center().child(heading.clone()))
                })
                .child(div().font_semibold().text_center().child(prompt))
                .when(failed, |card| {
                    card.child(
                        div()
                            .text_color(rgb(0xffaeb7))
                            .child(t!("thread.call.failed").to_string()),
                    )
                })
                .child(controls);
            screen = screen.child(
                div()
                    .absolute()
                    .inset_0()
                    .flex()
                    .items_center()
                    .justify_center()
                    .when(closing || incoming.is_some(), |layer| {
                        layer.occlude().bg(rgba(0x00000071))
                    })
                    .child(card),
            );
        } else {
            screen = screen.when(held.is_some() || failed, |screen| {
                screen.child(
                    div()
                        .absolute()
                        .top_2()
                        // Keep the status clear of the rightmost tile’s microphone badge.
                        .right(px(64.))
                        .bg(rgba(0x242426e5))
                        .rounded_full()
                        .px_3()
                        .py_1()
                        .child(if failed {
                            t!("thread.call.failed").to_string()
                        } else {
                            t!("thread.call.held").to_string()
                        }),
                )
            });
        }
        screen
    }
}

fn participant_row(
    (label, media, local): (String, ParticipantMedia, bool),
    separator: bool,
) -> impl IntoElement {
    let (microphone_icon, microphone_color) = if media.microphone {
        (
            "icons/mic.svg",
            if media.audio_level > 0. {
                rgb(SPEECH_GREEN)
            } else {
                rgba(LABEL_SECONDARY)
            },
        )
    } else {
        ("icons/mic-off.svg", rgba(LABEL_SECONDARY))
    };
    h_flex()
        .relative()
        .h(px(48.))
        .flex_shrink_0()
        .text_size(px(17.))
        .line_height(px(22.))
        .gap_3()
        .px_4()
        .when(separator, |row| {
            row.child(
                div()
                    .absolute()
                    .left_4()
                    .right_0()
                    .top_0()
                    .h(px(1.))
                    .bg(rgba(SEPARATOR)),
            )
        })
        .child(
            Icon::empty()
                .data(CALL_AVATAR)
                .text_color(rgba(LABEL_SECONDARY))
                .size(px(24.)),
        )
        .child(
            h_flex()
                .relative()
                .top(px(-1.))
                .flex_1()
                .min_w_0()
                .gap_2()
                .child(
                    div()
                        .min_w_0()
                        .overflow_hidden()
                        .whitespace_nowrap()
                        .text_ellipsis()
                        .child(label.clone()),
                )
                .when(local && label != t!("thread.call.you"), |row| {
                    row.child(
                        div()
                            .flex_shrink_0()
                            .rounded_full()
                            .bg(rgb(0x3a3a3c))
                            .px_2()
                            .text_size(px(12.))
                            .font_semibold()
                            .child(format!("({})", t!("thread.call.you"))),
                    )
                }),
        )
        .child(
            h_flex()
                .size(px(32.))
                .flex_shrink_0()
                .justify_center()
                .child(
                    call_control_icon(microphone_icon)
                        .text_color(microphone_color)
                        .size(px(18.)),
                ),
        )
}

// Paint continuous corners for opaque call surfaces without replacing the
// input, buttons, focus ring, or scroll container that sits above the fill.
fn continuous_surface(radius: f32, fill: Hsla, border: Option<Hsla>) -> impl IntoElement {
    // The transparent layout border belongs to the parent. Paint across it.
    let edge = if border.is_some() { px(-1.) } else { px(0.) };
    canvas(
        |_, _, _| (),
        move |bounds, _, window, _| {
            if let Some(border) = border {
                paint_continuous_surface(window, bounds, radius, border, 0.);
            }
            let inset = if border.is_some() { 1. } else { 0. };
            paint_continuous_surface(window, bounds, radius - inset, fill, inset);
        },
    )
    .absolute()
    .top(edge)
    .right(edge)
    .bottom(edge)
    .left(edge)
}

fn paint_continuous_surface(
    window: &mut Window,
    bounds: Bounds<Pixels>,
    radius: f32,
    fill: Hsla,
    inset: f32,
) {
    let x = bounds.origin.x + px(inset);
    let y = bounds.origin.y + px(inset);
    let width = bounds.size.width - px(2. * inset);
    let height = bounds.size.height - px(2. * inset);
    if width <= px(0.) || height <= px(0.) {
        return;
    }
    let r = px(radius).min(width / 2.).min(height / 2.);
    // Tangential cubic approximation of the fourth-power corner curve.
    let t = r * 0.09;
    let right = x + width;
    let bottom = y + height;
    let mut path = PathBuilder::fill();
    path.move_to(point(x + r, y));
    path.line_to(point(right - r, y));
    path.cubic_bezier_to(
        point(right, y + r),
        point(right - t, y),
        point(right, y + t),
    );
    path.line_to(point(right, bottom - r));
    path.cubic_bezier_to(
        point(right - r, bottom),
        point(right, bottom - t),
        point(right - t, bottom),
    );
    path.line_to(point(x + r, bottom));
    path.cubic_bezier_to(
        point(x, bottom - r),
        point(x + t, bottom),
        point(x, bottom - t),
    );
    path.line_to(point(x, y + r));
    path.cubic_bezier_to(point(x + r, y), point(x, y + t), point(x + t, y));
    path.close();
    if let Ok(path) = path.build() {
        window.paint_path(path, fill);
    }
}

fn call_panel_button(cx: &App) -> ButtonCustomVariant {
    ButtonCustomVariant::new(cx)
        .foreground(rgb(0xffffff).into())
        .hover(rgb(0x38383a).into())
        .active(rgb(0x444447).into())
}

fn icon_button(
    control_size: f32,
    id: &'static str,
    icon: &'static str,
    label: String,
    tooltip: String,
    toggled: Option<bool>,
    danger: bool,
) -> Button {
    let (fill, foreground) = if danger {
        (rgb(HANG_UP_RED), rgb(0xffffff))
    } else if toggled == Some(true) {
        (rgba(0xffffffff), rgb(CALL_BACKGROUND))
    } else {
        (rgba(0x78788052).into(), rgb(0xffffff))
    };
    let button = Button::new(id)
        .ghost()
        .compact()
        .p_0()
        .size(px(control_size))
        .rounded_full()
        .accessibility_label(label)
        .tooltip(tooltip)
        .child(
            div()
                .size(px(control_size))
                .rounded_full()
                .bg(fill)
                .text_color(foreground)
                .when(danger || toggled == Some(true), |button| {
                    button.shadow(vec![
                        BoxShadow::new(px(0.), px(1.), rgba(0x00000066).into()).blur_radius(px(1.)),
                        BoxShadow::new(px(0.), px(2.), rgba(0x00000066).into()).blur_radius(px(4.)),
                    ])
                })
                .flex()
                .items_center()
                .justify_center()
                .child(
                    if danger {
                        Icon::empty().data(CALL_HANG_UP)
                    } else {
                        call_control_icon(icon)
                    }
                    .size(px(24.)),
                ),
        );
    if let Some(toggled) = toggled {
        button.toggled(toggled)
    } else {
        button
    }
}

// videocall-rs bea64063, attendants.rs::compute_grid.
fn grid_dimensions(count: usize, width: f64, height: f64) -> (usize, usize) {
    const SCORING_GAP: f64 = 10.;
    match count {
        0..=1 => return (1, 1),
        2 if width >= height => return (2, 1),
        2 => return (1, 2),
        3..=4 => return (2, 2),
        _ => {}
    }
    let width = (width - 32.).max(1.);
    let height = (height - 32.).max(1.);
    let mut best = None;
    let mut fallback = (f64::INFINITY, (1, count));
    for columns in 1..=count {
        let rows = count.div_ceil(columns);
        let cell_width = (width - SCORING_GAP * (columns - 1) as f64) / columns as f64;
        let cell_height = (height - SCORING_GAP * (rows - 1) as f64) / rows as f64;
        if cell_width <= 0. || cell_height <= 0. {
            continue;
        }
        let aspect = cell_width / cell_height;
        let area = cell_width * cell_height;
        if (0.5..=2.).contains(&aspect) && best.is_none_or(|(largest, _)| area > largest) {
            best = Some((area, (columns, rows)));
        }
        let distance = (aspect - 1.).abs();
        if distance < fallback.0 {
            fallback = (distance, (columns, rows));
        }
    }
    best.unwrap_or(fallback).1
}

fn cells_into_rows(cells: Vec<AnyElement>, width: f32, height: f32, full_bleed: bool) -> Vec<Div> {
    if cells.is_empty() {
        return Vec::new();
    }
    let (columns, rows) = grid_dimensions(cells.len(), width.into(), height.into());
    let mut cells = cells.into_iter();
    (0..rows)
        .map(|_| {
            let slots =
                (0..columns).map(|_| div().flex().flex_1().min_w_0().children(cells.next()));
            div()
                .flex()
                .flex_1()
                .min_h_0()
                .gap_2()
                .when(full_bleed, |row| row.bg(rgb(0x000000)))
                .children(slots)
        })
        .collect()
}

enum Tile {
    Participant(String),
    Preview { camera: bool },
}

fn video_bounds(bounds: Bounds<Pixels>, width: u32, height: u32, cover: bool) -> Bounds<Pixels> {
    let horizontal = f32::from(bounds.size.width) / width as f32;
    let vertical = f32::from(bounds.size.height) / height as f32;
    let scale = if cover {
        horizontal.max(vertical)
    } else {
        horizontal.min(vertical)
    };
    let drawn = size(px(width as f32 * scale), px(height as f32 * scale));
    let position = bounds.origin
        + point(
            (bounds.size.width - drawn.width) / 2.,
            (bounds.size.height - drawn.height) / 2.,
        );
    Bounds::new(position, drawn)
}

fn tile(
    tile: Tile,
    texture: Option<(DynamicTexture, u32, u32)>,
    media: ParticipantMedia,
    voice: SpeechFrame,
    full_bleed: bool,
) -> AnyElement {
    let preview = matches!(tile, Tile::Preview { .. });
    let mut view = div()
        .relative()
        .flex_1()
        .min_w_0()
        .min_h_0()
        .overflow_hidden()
        .when(!full_bleed && !preview, |tile| {
            tile.rounded(px(14.))
                .border_1()
                .border_color(rgba(SEPARATOR))
        })
        .bg(if full_bleed {
            rgb(CALL_BACKGROUND)
        } else {
            rgb(TILE_BACKGROUND)
        })
        .flex()
        .items_center()
        .justify_center();
    if let Some((texture, width, height)) = texture {
        view = view.child(
            canvas(
                |_, _, _| (),
                move |bounds, _, window, _| {
                    window.with_content_mask(Some(ContentMask { bounds }), |window| {
                        let _ = window.paint_dynamic_texture(
                            video_bounds(bounds, width, height, preview),
                            texture,
                        );
                    });
                },
            )
            .absolute()
            .size_full(),
        );
    } else {
        let (icon, message) = match &tile {
            Tile::Participant(_) if full_bleed => ("", t!("thread.call.camera_disabled")),
            Tile::Participant(_) => ("", t!("thread.call.video_disabled")),
            Tile::Preview { camera: true } => ("icons/video.svg", t!("thread.call.connecting")),
            Tile::Preview { camera: false } => {
                ("icons/video-off.svg", t!("thread.call.camera_disabled"))
            }
        };
        view = view.child(
            v_flex()
                .items_center()
                .gap(px(if preview { 4. } else { 12. }))
                .text_color(if !preview && !full_bleed {
                    rgb(TILE_SECONDARY)
                } else {
                    rgba(LABEL_SECONDARY)
                })
                .text_size(px(if preview { 13. } else { 16. }))
                .line_height(px(if preview { 18. } else { 21. }))
                .child(if preview {
                    call_control_icon(icon).size(px(24.))
                } else {
                    Icon::empty().data(CALL_AVATAR).size(px(60.))
                })
                .child(
                    div()
                        .h(px(if preview { 18. } else { 21. }))
                        .child(message.to_string()),
                ),
        );
    }
    if voice.ring > 0. {
        let border_opacity = if voice.reduced_motion {
            1.
        } else {
            voice.ring * (0.4 + 0.6 * voice.level)
        };
        view = view.child(
            div()
                .absolute()
                .top_0()
                .left_0()
                .size_full()
                .rounded(px(if full_bleed { 0. } else { 14. }))
                .border_2()
                .border_color(rgb(SPEECH_GREEN).opacity(border_opacity))
                .when(!voice.reduced_motion, |overlay| {
                    let color = rgb(SPEECH_GREEN).opacity(voice.ring * (0.25 + 0.4 * voice.level));
                    overlay.shadow(vec![
                        BoxShadow::new(px(0.), px(0.), color.into())
                            .blur_radius(px(12. + 20. * voice.level))
                            .spread_radius(px(4. + 8. * voice.level))
                            .inset(),
                    ])
                }),
        );
    }
    if let Tile::Participant(label) = tile {
        let microphone = if media.microphone {
            "icons/mic.svg"
        } else {
            "icons/mic-off.svg"
        };
        view = view
            .child(
                div()
                    .absolute()
                    .left_2()
                    .top_2()
                    .max_w(relative(0.55))
                    .min_w_0()
                    .overflow_hidden()
                    .rounded(px(6.))
                    .bg(rgba(0x0000008a))
                    .px_2()
                    .h(px(26.))
                    .flex()
                    .items_center()
                    .text_size(px(13.))
                    .line_height(px(18.))
                    .font_weight(FontWeight::SEMIBOLD)
                    .child(div().min_w_0().truncate().child(label)),
            )
            .child(
                div()
                    .absolute()
                    .right_2()
                    .top_2()
                    .size(px(32.))
                    .rounded_full()
                    .bg(rgba(0x00000059))
                    .text_color(rgb(0xffffff).blend(rgb(SPEECH_GREEN).opacity(voice.mic)))
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        div()
                            .size(px(18.))
                            .rounded_full()
                            .when(voice.level > 0. && !voice.reduced_motion, |glyph| {
                                let halo = rgb(SPEECH_GREEN).opacity(
                                    (100. * voice.level).min(1.) * (0.6 + 0.4 * voice.level),
                                );
                                glyph.shadow(vec![
                                    BoxShadow::new(px(0.), px(0.), halo.into())
                                        .blur_radius(px(4. + 12. * voice.level)),
                                ])
                            })
                            .child(call_control_icon(microphone).size(px(18.))),
                    ),
            );
    }
    view.into_any_element()
}

#[cfg(test)]
mod tests {
    use super::{grid_dimensions, video_bounds};
    use gpui_kit::{Bounds, point, px, size};

    #[test]
    fn grid_uses_reference_dimensions() {
        for (participants, width, height, expected) in [
            (0, 800., 540., (1, 1)),
            (1, 800., 540., (1, 1)),
            (2, 800., 540., (2, 1)),
            (2, 390., 740., (1, 2)),
            (3, 800., 540., (2, 2)),
            (4, 390., 740., (2, 2)),
            (5, 800., 540., (3, 2)),
            (8, 800., 540., (4, 2)),
            (8, 540., 800., (2, 4)),
            (5, 2000., 100., (5, 1)),
            (5, 16., 16., (1, 5)),
        ] {
            assert_eq!(grid_dimensions(participants, width, height), expected);
        }
    }

    #[test]
    fn local_preview_covers_and_remote_video_fits_the_tile() {
        let tile = Bounds::new(point(px(8.), px(16.)), size(px(160.), px(90.)));
        assert_eq!(
            video_bounds(tile, 640, 480, true),
            Bounds::new(point(px(8.), px(1.)), size(px(160.), px(120.)))
        );
        assert_eq!(
            video_bounds(tile, 640, 480, false),
            Bounds::new(point(px(28.), px(16.)), size(px(120.), px(90.)))
        );
    }
}
