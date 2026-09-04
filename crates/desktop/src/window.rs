use crate::{
    settings::{self, WindowOpenState, WindowThemePreference},
    state::{self, WindowState},
};
use gpui_kit::{App, Bounds, Context, Window, WindowBounds, point, px, size};
use tracing::warn;

const DEFAULT_RESTORE_WIDTH: f32 = 1280.0;
const DEFAULT_RESTORE_HEIGHT: f32 = 800.0;

pub(crate) fn initial_window_bounds(cx: &mut App) -> WindowBounds {
    match state::window(cx) {
        Ok(state) => state
            .and_then(window_bounds_from_state)
            .unwrap_or_else(|| default_window_bounds(cx)),
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "failed to load desktop state; using default window bounds"
            );
            default_window_bounds(cx)
        }
    }
}

pub(crate) fn install_window_state_persistence<T: 'static>(
    window: &mut Window,
    cx: &mut Context<T>,
) {
    // Persist on every move/resize so quitting the app still keeps latest window size.
    cx.observe_window_bounds(window, |_, window, cx| {
        persist_window_settings(window, cx);
    })
    .detach();

    window.on_window_should_close(cx, |window: &mut Window, cx: &mut App| {
        persist_window_settings(window, cx);
        true
    });
}

pub(crate) fn persist_theme_preference(
    _window: &Window,
    theme: WindowThemePreference,
    cx: &mut App,
) {
    if let Err(error) = settings::set_window_theme(cx, theme) {
        warn!(
            error = %format!("{error:#}"),
            "failed to save desktop theme preference"
        );
    }
}

fn persist_window_settings(window: &Window, cx: &mut App) {
    let state = window_state_from_window_bounds(window.window_bounds());
    if let Err(error) = state::set_window(cx, state) {
        warn!(
            error = %format!("{error:#}"),
            "failed to save desktop window state"
        );
    }
}

fn default_window_bounds(cx: &App) -> WindowBounds {
    let restore_bounds = Bounds::centered(
        None,
        size(px(DEFAULT_RESTORE_WIDTH), px(DEFAULT_RESTORE_HEIGHT)),
        cx,
    );
    WindowBounds::Maximized(restore_bounds)
}

fn window_state_from_window_bounds(window_bounds: WindowBounds) -> WindowState {
    let (state, bounds) = match window_bounds {
        WindowBounds::Windowed(bounds) => (WindowOpenState::Windowed, bounds),
        WindowBounds::Maximized(bounds) => (WindowOpenState::Maximized, bounds),
        WindowBounds::Fullscreen(bounds) => (WindowOpenState::Fullscreen, bounds),
    };

    WindowState {
        state,
        x: f32::from(bounds.origin.x),
        y: f32::from(bounds.origin.y),
        width: f32::from(bounds.size.width),
        height: f32::from(bounds.size.height),
    }
}

fn window_bounds_from_state(state: WindowState) -> Option<WindowBounds> {
    if !state.x.is_finite()
        || !state.y.is_finite()
        || !state.width.is_finite()
        || !state.height.is_finite()
        || state.width <= 0.0
        || state.height <= 0.0
    {
        return None;
    }

    let bounds = Bounds {
        origin: point(px(state.x), px(state.y)),
        size: size(px(state.width), px(state.height)),
    };

    Some(match state.state {
        WindowOpenState::Windowed => WindowBounds::Windowed(bounds),
        WindowOpenState::Maximized => WindowBounds::Maximized(bounds),
        WindowOpenState::Fullscreen => WindowBounds::Fullscreen(bounds),
    })
}
