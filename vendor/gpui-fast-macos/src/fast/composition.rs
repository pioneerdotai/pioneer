// Modified by Longbridge for gpui-fast.
//! Window composition on AppKit: native views between GPUI's base content
//! and its overlays.
//!
//! Ported from zed-industries/zed#62379. The window's own view (`GPUIView`)
//! and its `CAMetalLayer` draw the base GPUI surface. Every other GPUI
//! surface gets a `GPUIOverlayView` of its own, a sibling of the native
//! views, drawn by a renderer that shares the window renderer's device and
//! sprite atlas. `set_composition_order` orders all of them as subviews of
//! the window's view.
//!
//! An overlay view covers the whole window, so it takes mouse events only
//! within the prepared GPUI hitboxes of its surface and hands them to the
//! window's view. Elsewhere, AppKit hit testing reaches the native views below.

use std::{
    any::Any,
    cell::{Cell, RefCell},
    ffi::{CStr, c_void},
    ptr::{self, NonNull},
    rc::{Rc, Weak},
    sync::atomic::{AtomicPtr, Ordering},
};

use anyhow::{Context as _, Result, anyhow};
use collections::{FxHashMap, FxHashSet};
use gpui::{
    Bounds, ComposedScene, ComposedSceneLayer, CompositionSurfaceId, DevicePixels,
    PlatformCompositionSurface, PlatformCompositionSurfaceContent, PlatformSurfaceAttachment,
    Point, Scene, Size, px,
};
use objc2::{
    class, msg_send,
    runtime::{AnyClass, AnyObject, Bool, ClassBuilder, Sel},
    sel,
};
use objc2_app_kit::{NSAutoresizingMaskOptions, NSEventType, NSViewLayerContentsRedrawPolicy};
use objc2_foundation::{NSInteger, NSPoint, NSRect, NSSize};
use parking_lot::Mutex;

#[path = "composition_capture.rs"]
mod capture;

use capture::{PointerCapture, PointerOwner, PointerPhase, PointerRoute, route_pointer_event};

use crate::{
    MetalFrames, renderer,
    window::{
        FrameSource, MacWindowState, WINDOW_STATE_IVAR, drop_window_state, get_window_state,
        handle_view_event,
    },
};

/// The ivar of a `GPUIOverlayView` holding its surface's
/// [`MacGpuiSurface::input_regions`], a UI-thread-local `Rc` turned into a raw
/// pointer.
const OVERLAY_INPUT_IVAR: &CStr = c"overlayInputRegions";
const NATIVE_INPUT_IVAR: &CStr = c"compositionInputActive";

/// An Objective-C object pointer, as AppKit hands them to us.
type Id = *mut AnyObject;
const NIL: Id = ptr::null_mut();

/// The `GPUIOverlayView` class, registered by [`build_overlay_view_class`].
static OVERLAY_VIEW_CLASS: AtomicPtr<AnyClass> = AtomicPtr::new(ptr::null_mut());

/// A window's composition state: which surface the window's own view draws,
/// and the views and renderers of its other GPUI surfaces.
pub(crate) struct MacComposition {
    /// The GPUI surface drawn by the window's own view and renderer, once the
    /// window composes its scene.
    base_surface: Option<CompositionSurfaceId>,
    /// Every GPUI surface but the base one.
    gpui_surfaces: FxHashMap<CompositionSurfaceId, MacGpuiSurface>,
    /// What the window's renderer was created with, for the surfaces'.
    renderer_context: renderer::Context,
    /// Native identities, parents and sibling order, independent of geometry.
    view_order: Vec<(CompositionSurfaceId, usize, usize)>,
    native_views: Rc<Vec<usize>>,
    input_order: Rc<Vec<InputLayer>>,
    native_event: usize,
    pointer_capture: PointerCapture,
    input_forwarders: FxHashSet<usize>,
    event_result: gpui::DispatchEventResult,
    forwarding_scroll: bool,
    /// The window's Metal display link, set aside while it holds native
    /// views (see [`follow_native_views`]).
    metal_frames: Option<Rc<MetalFrames>>,
    /// Whether the window's renderers present in the Core Animation
    /// transaction (see [`set_presents_with_transaction`]).
    presents_with_transaction: bool,
}

impl MacComposition {
    pub(crate) fn new(renderer_context: renderer::Context) -> Self {
        Self {
            base_surface: None,
            gpui_surfaces: FxHashMap::default(),
            renderer_context,
            view_order: Vec::new(),
            native_views: Rc::new(Vec::new()),
            metal_frames: None,
            presents_with_transaction: false,
            input_order: Rc::new(Vec::new()),
            native_event: 0,
            pointer_capture: PointerCapture::default(),
            input_forwarders: FxHashSet::default(),
            event_result: gpui::DispatchEventResult {
                propagate: true,
                default_prevented: false,
            },
            forwarding_scroll: false,
        }
    }
}

enum InputLayer {
    Gpui(Rc<RefCell<Vec<gpui::Hitbox>>>),
    Native(usize),
}

/// A GPUI surface above the base one: an overlay view and its renderer.
struct MacGpuiSurface {
    view: NonNull<AnyObject>,
    renderer: renderer::Renderer,
    /// Prepared GPUI hitboxes, shared with the overlay view on the UI thread.
    input_regions: Rc<RefCell<Vec<gpui::Hitbox>>>,
}

/// Registers the `GPUIOverlayView` class, with the window's other classes.
pub(crate) unsafe fn build_overlay_view_class() {
    let class = if let Some(mut decl) = ClassBuilder::new(c"GPUIOverlayView", class!(NSView)) {
        decl.add_ivar::<*mut c_void>(WINDOW_STATE_IVAR);
        decl.add_ivar::<*mut c_void>(OVERLAY_INPUT_IVAR);
        unsafe {
            decl.add_method(sel!(dealloc), dealloc_overlay_view as extern "C" fn(_, Sel));
            decl.add_method(
                sel!(hitTest:),
                overlay_hit_test as extern "C" fn(_, Sel, NSPoint) -> Id,
            );
            for selector in [
                sel!(mouseDown:),
                sel!(mouseUp:),
                sel!(rightMouseDown:),
                sel!(rightMouseUp:),
                sel!(otherMouseDown:),
                sel!(otherMouseUp:),
                sel!(mouseMoved:),
                sel!(mouseExited:),
                sel!(mouseDragged:),
                sel!(rightMouseDragged:),
                sel!(otherMouseDragged:),
                sel!(scrollWheel:),
                sel!(magnifyWithEvent:),
                sel!(swipeWithEvent:),
                sel!(pressureChangeWithEvent:),
            ] {
                decl.add_method(selector, handle_overlay_event as extern "C" fn(_, Sel, Id));
            }
        }
        decl.register()
    } else {
        AnyClass::get(c"GPUIOverlayView").map_or(ptr::null(), |class| class)
    };
    OVERLAY_VIEW_CLASS.store(class.cast_mut(), Ordering::Release);
}

extern "C" fn overlay_hit_test(this: &AnyObject, _: Sel, point: NSPoint) -> Id {
    let regions = unsafe {
        let raw = *ivar(this, OVERLAY_INPUT_IVAR).load::<*mut c_void>(this);
        &*(raw as *const RefCell<Vec<gpui::Hitbox>>)
    };
    let local: NSPoint = unsafe {
        let parent: Id = msg_send![this, superview];
        msg_send![this, convertPoint: point, fromView: parent]
    };
    let flipped: Bool = unsafe { msg_send![this, isFlipped] };
    let bounds: NSRect = unsafe { msg_send![this, bounds] };
    let height = bounds.size.height;
    let point = gpui::point(
        px(local.x as f32),
        px(if flipped.as_bool() {
            local.y
        } else {
            height - local.y
        } as f32),
    );
    if regions
        .borrow()
        .iter()
        .rev()
        .any(|region| region.bounds.contains(&point))
    {
        this as *const AnyObject as Id
    } else {
        NIL
    }
}

/// The instance variable `name` of `object`'s class.
fn ivar<'a>(object: &'a AnyObject, name: &CStr) -> &'a objc2::runtime::Ivar {
    object
        .class()
        .instance_variable(name)
        .expect("composition instance variable")
}

/// Native pointer input runs GPUI capture before AppKit delivers it.
/// No handlers are run from hitTest, and no process-wide event monitor is installed.
pub(crate) unsafe fn register_window_input(decl: &mut ClassBuilder, superclass: &AnyClass) {
    extern "C" fn window(this: &AnyObject, _: Sel, event: Id) {
        send_window_event(this, event, class!(NSWindow));
    }
    extern "C" fn panel(this: &AnyObject, _: Sel, event: Id) {
        send_window_event(this, event, class!(NSPanel));
    }
    unsafe {
        decl.add_ivar::<Bool>(NATIVE_INPUT_IVAR);
        if superclass == class!(NSPanel) {
            decl.add_method(sel!(sendEvent:), panel as extern "C" fn(_, Sel, _));
        } else {
            decl.add_method(sel!(sendEvent:), window as extern "C" fn(_, Sel, _));
        }
    }
}

fn send_window_event(this: &AnyObject, event: Id, superclass: &AnyClass) {
    if !unsafe { *ivar(this, NATIVE_INPUT_IVAR).load::<Bool>(this) }.as_bool() {
        unsafe {
            let _: () = msg_send![super(this, superclass), sendEvent: event];
        }
        return;
    }
    let kind: NSEventType = unsafe { msg_send![event, type] };
    let observe = matches!(
        kind,
        NSEventType::LeftMouseDown
            | NSEventType::RightMouseDown
            | NSEventType::OtherMouseDown
            | NSEventType::LeftMouseUp
            | NSEventType::RightMouseUp
            | NSEventType::OtherMouseUp
    );
    let wheel = kind == NSEventType::ScrollWheel;
    let dragged = matches!(
        kind,
        NSEventType::LeftMouseDragged
            | NSEventType::RightMouseDragged
            | NSEventType::OtherMouseDragged
    );
    let pointer = observe || wheel || dragged || kind == NSEventType::MouseMoved;
    if !pointer {
        unsafe {
            let _: () = msg_send![super(this, superclass), sendEvent: event];
        }
        return;
    }
    let button = if observe {
        let number: NSInteger = unsafe { msg_send![event, buttonNumber] };
        1u64.checked_shl(number as u32).unwrap_or(0)
    } else {
        0
    };
    let up = matches!(
        kind,
        NSEventType::LeftMouseUp | NSEventType::RightMouseUp | NSEventType::OtherMouseUp
    );
    let state = unsafe { get_window_state(this) };
    let (root, native_views, captured, has_forwarders) = {
        let mut state = state.lock();
        if observe && !up {
            state
                .fast_composition
                .pointer_capture
                .reconcile_down(button);
        }
        (
            state.native_view,
            state.fast_composition.native_views.clone(),
            state.fast_composition.pointer_capture.owner(),
            !state.fast_composition.input_forwarders.is_empty(),
        )
    };
    if kind == NSEventType::MouseMoved && !has_forwarders {
        unsafe {
            let _: () = msg_send![super(this, superclass), sendEvent: event];
        }
        return;
    }
    // A press on the window's own chrome (the resize border, the titlebar
    // buttons) belongs to AppKit, as in a window without composition.
    // `root.hitTest` below cannot see the frame view, so a resize grabbed where
    // a native view reaches the window's edge would make that view the press
    // owner and dispatch the press to GPUI. AppKit's live-resize loop can then
    // consume the matching mouse-up (it does with a physical mouse on macOS
    // 26.6), which leaves the press open and loses the next click. A release
    // that does reach `sendEvent:` still takes the path below, which clears
    // the owner.
    if observe && !up && unsafe { hits_window_chrome(this, root.as_ptr(), event) } {
        unsafe {
            let _: () = msg_send![super(this, superclass), sendEvent: event];
        }
        return;
    }
    if up {
        let mut state = state.lock();
        state.fast_composition.pointer_capture.release(button);
    }
    let phase = if up {
        PointerPhase::Up
    } else if observe {
        PointerPhase::Down
    } else if dragged {
        PointerPhase::Drag
    } else {
        PointerPhase::Other
    };
    if captured == Some(PointerOwner::Gpui) && phase == PointerPhase::Down {
        state
            .lock()
            .fast_composition
            .pointer_capture
            .press(button, None);
    }
    // Captured GPUI secondary downs intentionally bypass AppKit's menuForEvent:
    // path, just as GPUIView's rightMouseDown: dispatches handle_view_event
    // directly. GPUI owns context-menu handling, not the native view underneath.
    let native = match route_pointer_event(
        captured,
        phase,
        || handle_view_event(unsafe { root.as_ref() }, sel!(sendEvent:), event),
        || unsafe {
            let parent: Id = msg_send![root.as_ptr(), superview];
            let location: NSPoint = msg_send![event, locationInWindow];
            let point: NSPoint = msg_send![parent, convertPoint: location, fromView: NIL];
            let target: Id = msg_send![root.as_ptr(), hitTest: point];
            native_for_target(target, &native_views)
        },
    ) {
        PointerRoute::Delivered => return,
        PointerRoute::AppKit(native) => native,
    };
    let forward =
        native.is_some_and(|id| state.lock().fast_composition.input_forwarders.contains(&id));
    // A windowless producer consumes its press instead of starting AppKit view
    // tracking. Keep the recipient through drag/up, even when capture dismissed
    // a popup and withheld the down event from the producer.
    if forward && observe && !up {
        let mut state = state.lock();
        state.fast_composition.pointer_capture.press(button, native);
    }
    if native.is_some() && (observe || forward) {
        state.lock().fast_composition.event_result = gpui::DispatchEventResult {
            propagate: true,
            default_prevented: false,
        };
        // This wheel will follow AppKit's original path if unhandled; it must
        // not also be forwarded by the overlay's unhandled-wheel hook.
        let forwarding = if wheel {
            Some(std::mem::replace(
                &mut state.lock().fast_composition.forwarding_scroll,
                true,
            ))
        } else {
            None
        };
        handle_view_event(unsafe { root.as_ref() }, sel!(sendEvent:), event);
        let result = {
            let mut state = state.lock();
            if let Some(previous) = forwarding {
                state.fast_composition.forwarding_scroll = previous;
            }
            state.fast_composition.event_result.clone()
        };
        if forward
            || (!(captured.is_some() && up) && (!result.propagate || result.default_prevented))
        {
            if observe && !up && !forward && captured.is_none() {
                // A consumed capture-phase down belongs to GPUI, not the view
                // underneath the popup that capture may just have dismissed.
                let mut state = state.lock();
                state.fast_composition.pointer_capture.press(button, None);
            }
            return;
        }
    }
    if native.is_none() {
        if observe && !up && captured.is_none() {
            state
                .lock()
                .fast_composition
                .pointer_capture
                .press(button, None);
        }
        unsafe {
            let _: () = msg_send![super(this, superclass), sendEvent: event];
        }
        return;
    }
    let previous = {
        let mut state = state.lock();
        if observe && !up {
            state.fast_composition.pointer_capture.press(button, native);
        }
        let previous = state.fast_composition.native_event;
        state.fast_composition.native_event = event as usize;
        previous
    };
    unsafe {
        let _: () = msg_send![super(this, superclass), sendEvent: event];
    }
    state.lock().fast_composition.native_event = previous;
}

/// Whether AppKit hit-tests `event` outside the window's view `root`: to the
/// frame view itself (its resize border) or to one of its controls, such as
/// the titlebar buttons.
unsafe fn hits_window_chrome(window: &AnyObject, root: Id, event: Id) -> bool {
    unsafe {
        let content: Id = msg_send![window, contentView];
        if content.is_null() {
            return false;
        }
        let frame_view: Id = msg_send![content, superview];
        if frame_view.is_null() {
            return false;
        }
        let location: NSPoint = msg_send![event, locationInWindow];
        let target: Id = msg_send![frame_view, hitTest: location];
        if target.is_null() {
            return false;
        }
        let inside: Bool = msg_send![target, isDescendantOf: root];
        !inside.as_bool()
    }
}

unsafe fn belongs_to_native(view: Id, native: &[usize]) -> bool {
    unsafe { native_for_target(view, native).is_some() }
}

unsafe fn native_for_target(mut view: Id, native: &[usize]) -> Option<usize> {
    let overlay = OVERLAY_VIEW_CLASS.load(Ordering::Acquire);
    while !view.is_null() {
        if !overlay.is_null() {
            let gpui: Bool = unsafe { msg_send![view, isKindOfClass: overlay.cast_const()] };
            if gpui.as_bool() {
                return None;
            }
        }
        if native.contains(&(view as usize)) {
            return Some(view as usize);
        }
        view = unsafe { msg_send![view, superview] };
    }
    None
}

/// AppKit may consume releases during deactivation or a native tracking loop.
/// Relinquish both GPUI and native ownership before handing a gesture to AppKit.
pub(crate) fn clear_pointer_capture(state: &Rc<Mutex<MacWindowState>>) {
    state.lock().fast_composition.pointer_capture.clear();
}

pub(crate) fn window_activation_changed(state: &Rc<Mutex<MacWindowState>>, active: bool) {
    if !active {
        clear_pointer_capture(state);
    }
}

pub(crate) fn start_window_move(state: &Rc<Mutex<MacWindowState>>) {
    let window = {
        let state = state.lock();
        if state.simple_fullscreen_state.is_some() {
            return;
        }
        state.native_window
    };
    clear_pointer_capture(state);
    unsafe {
        let app: Id = msg_send![class!(NSApplication), sharedApplication];
        let event: Id = msg_send![app, currentEvent];
        let _: () = msg_send![window, performWindowDragWithEvent: event];
    }
}

pub(crate) fn native_event_observed(state: &MacWindowState, event: Id) -> bool {
    state.fast_composition.native_event == event as usize
}

pub(crate) fn complete_event(
    state: &Rc<Mutex<MacWindowState>>,
    event: Id,
    callback: Box<dyn FnMut(gpui::PlatformInput) -> gpui::DispatchEventResult>,
    result: gpui::DispatchEventResult,
) {
    let forward = {
        let mut state = state.lock();
        state.event_callback = Some(callback);
        state.fast_composition.event_result = result.clone();
        !state.fast_composition.native_views.is_empty()
            && result.propagate
            && !result.default_prevented
    };
    if forward {
        forward_unhandled_scroll(state, event);
    }
}

/// When a GPUI overlay permits scrolling underneath it, use the managed native
/// views and AppKit's own hit testing. This never changes visibility or geometry.
fn forward_unhandled_scroll(state: &Rc<Mutex<MacWindowState>>, event: Id) {
    let kind: NSEventType = unsafe { msg_send![event, type] };
    if kind != NSEventType::ScrollWheel {
        return;
    }
    let (input_order, height) = {
        let state = state.lock();
        if state.fast_composition.forwarding_scroll {
            return;
        }
        (
            state.fast_composition.input_order.clone(),
            state.content_size().height,
        )
    };
    let location: NSPoint = unsafe { msg_send![event, locationInWindow] };
    let point = gpui::point(px(location.x as f32), height - px(location.y as f32));
    for layer in input_order.iter().rev() {
        match layer {
            InputLayer::Gpui(regions) => {
                if regions.borrow().iter().any(|region| {
                    region.behavior == gpui::HitboxBehavior::BlockMouse
                        && region.bounds.contains(&point)
                }) {
                    return;
                }
            }
            InputLayer::Native(native) => {
                let target = unsafe { native_scroll_target(*native as Id, location) };
                if target.is_null() {
                    continue;
                }
                let previous = {
                    let mut state = state.lock();
                    state.fast_composition.forwarding_scroll = true;
                    std::mem::replace(&mut state.fast_composition.native_event, event as usize)
                };
                unsafe {
                    let _: () = msg_send![target, scrollWheel: event];
                }
                let mut state = state.lock();
                state.fast_composition.native_event = previous;
                state.fast_composition.forwarding_scroll = false;
                return;
            }
        }
    }
}

/// A nested GPUI overlay can decline a wheel event after AppKit selected it.
/// Query its native siblings below it without changing the view tree or geometry.
unsafe fn native_scroll_target(native: Id, location: NSPoint) -> Id {
    let parent: Id = unsafe { msg_send![native, superview] };
    let point: NSPoint = unsafe { msg_send![parent, convertPoint: location, fromView: NIL] };
    let mut selected: Id = unsafe { msg_send![native, hitTest: point] };
    if selected.is_null() || unsafe { belongs_to_native(selected, &[native as usize]) } {
        return selected;
    }
    while selected != native && !selected.is_null() {
        let parent: Id = unsafe { msg_send![selected, superview] };
        if parent.is_null() {
            break;
        }
        let children: Id = unsafe { msg_send![parent, subviews] };
        let count: usize = unsafe { msg_send![children, count] };
        let point: NSPoint = unsafe { msg_send![parent, convertPoint: location, fromView: NIL] };
        for index in (0..count).rev() {
            let child: Id = unsafe { msg_send![children, objectAtIndex: index] };
            if child == selected {
                continue;
            }
            let target: Id = unsafe { msg_send![child, hitTest: point] };
            if !target.is_null() && unsafe { belongs_to_native(target, &[native as usize]) } {
                return target;
            }
        }
        selected = parent;
    }
    NIL
}

extern "C" fn handle_overlay_event(this: &AnyObject, selector: Sel, native_event: Id) {
    let window_state = unsafe { get_window_state(this) };
    let native_view = window_state.lock().native_view;
    handle_view_event(unsafe { native_view.as_ref() }, selector, native_event);
}

extern "C" fn dealloc_overlay_view(this: &AnyObject, _: Sel) {
    unsafe {
        drop_window_state(this);
        let raw = *ivar(this, OVERLAY_INPUT_IVAR).load::<*mut c_void>(this);
        drop(Rc::from_raw(raw as *const RefCell<Vec<gpui::Hitbox>>));
        let _: () = msg_send![super(this, class!(NSView)), dealloc];
    }
}

/// `PlatformWindow::draw_composed`: draws each GPUI surface's part of the
/// scene with that surface's renderer, or the whole scene on the window's
/// view until the window composes.
pub(crate) fn draw_composed(window_state: &Rc<Mutex<MacWindowState>>, scene: ComposedScene<'_>) {
    let mut lock = window_state.lock();
    let state = &mut *lock;
    let Some(base_surface) = state.fast_composition.base_surface else {
        state.renderer.draw(scene.scene());
        return;
    };
    let whole_scene = whole_scene_layer(scene.scene(), scene.layers());
    for (layer_index, layer) in scene.layers().iter().enumerate() {
        let replayed_scene;
        let surface_scene = match whole_scene {
            Some((target_index, whole_scene)) if target_index == layer_index => whole_scene,
            _ => {
                replayed_scene = match scene.layer_scene(layer) {
                    Ok(surface_scene) => surface_scene,
                    Err(error) => {
                        log::error!("replaying AppKit composition scene: {error:#}");
                        return;
                    }
                };
                &replayed_scene
            }
        };
        if layer.surface == base_surface {
            state.renderer.draw(surface_scene);
        } else if let Some(surface) = state.fast_composition.gpui_surfaces.get_mut(&layer.surface) {
            {
                let mut regions = surface.input_regions.borrow_mut();
                regions.clear();
                regions.extend(scene.layer_hitboxes(layer).iter().cloned());
            }
            surface.renderer.draw(surface_scene);
        } else {
            log::error!(
                "missing AppKit GPUI composition surface {:?}",
                layer.surface
            );
        }
    }
}

/// `present` supplies the current frame's scene after `Frame::finish`.
/// Only the layer covering its full stream may borrow it; other layers must
/// have empty range lists and still replay and draw to clear their surfaces.
fn whole_scene_layer<'a>(
    scene: &'a Scene,
    layers: &[ComposedSceneLayer],
) -> Option<(usize, &'a Scene)> {
    let mut target = None;
    for (index, layer) in layers.iter().enumerate() {
        if !layer.ranges.is_empty() {
            let [range] = layer.ranges.as_slice() else {
                return None;
            };
            if target.is_some() || range.start != 0 || range.end != scene.len() {
                return None;
            }
            target = Some((index, scene));
        }
    }
    target
}

#[cfg(test)]
#[path = "composition_tests.rs"]
mod tests;

/// `PlatformWindow::enable_window_composition`: an AppKit window can always
/// compose, and sets itself up when it's first given an order.
pub(crate) fn enable_window_composition(_window_state: &Rc<Mutex<MacWindowState>>) -> Result<()> {
    Ok(())
}

/// `PlatformWindow::create_native_surface`: a layer-backed `NSView` that
/// clips the native view put in it.
pub(crate) fn create_native_surface(
    window_state: &Rc<Mutex<MacWindowState>>,
) -> Result<Rc<dyn PlatformSurfaceAttachment>> {
    enable_window_composition(window_state)?;
    let state = window_state.lock();
    let parent = state.native_view;

    unsafe {
        let view: Id = msg_send![class!(NSView), alloc];
        let view: Id = msg_send![
            view,
            initWithFrame: NSRect::new(NSPoint::new(0., 0.), NSSize::new(0., 0.))
        ];
        anyhow::ensure!(!view.is_null(), "failed to create native surface container");
        let _: () = msg_send![view, setWantsLayer: Bool::YES];
        let layer: Id = msg_send![view, layer];
        let _: () = msg_send![layer, setMasksToBounds: Bool::YES];
        let _: () = msg_send![parent.as_ptr(), addSubview: view];

        Ok(Rc::new(MacNativeSurface {
            window_state: Rc::downgrade(window_state),
            view: NonNull::new(view).context("native surface container is null")?,
            bounds: Cell::new(Bounds::default()),
        }))
    }
}

/// `PlatformWindow::set_composition_order`: creates and removes the views of
/// GPUI surfaces, then puts every surface's view under its parent's, in
/// order, at its bounds.
pub(crate) fn set_composition_order(
    window_state: &Rc<Mutex<MacWindowState>>,
    surfaces: &[PlatformCompositionSurface],
) -> Result<()> {
    let mut lock = window_state.lock();
    let base_surface = surfaces.iter().find_map(|surface| match surface.content {
        PlatformCompositionSurfaceContent::Gpui => Some(surface.id),
        PlatformCompositionSurfaceContent::Native(_)
        | PlatformCompositionSurfaceContent::ExternalGpu(_) => None,
    });
    let base_surface = base_surface.context("composition has no GPUI base surface")?;
    lock.fast_composition.base_surface = Some(base_surface);

    let active_gpui_surfaces = surfaces
        .iter()
        .filter_map(|surface| match surface.content {
            PlatformCompositionSurfaceContent::Gpui if surface.id != base_surface => {
                Some(surface.id)
            }
            _ => None,
        })
        .collect::<FxHashSet<_>>();
    let removed_surfaces = lock
        .fast_composition
        .gpui_surfaces
        .keys()
        .filter(|id| !active_gpui_surfaces.contains(id))
        .copied()
        .collect::<Vec<_>>();
    for id in removed_surfaces {
        if let Some(surface) = lock.fast_composition.gpui_surfaces.remove(&id) {
            unsafe {
                let _: () = msg_send![surface.view.as_ptr(), removeFromSuperview];
            }
        }
    }

    for surface in surfaces {
        if !matches!(surface.content, PlatformCompositionSurfaceContent::Gpui) {
            continue;
        }
        if surface.id == base_surface
            || lock
                .fast_composition
                .gpui_surfaces
                .contains_key(&surface.id)
        {
            continue;
        }
        let created_surface = unsafe { create_gpui_surface(window_state, &mut lock)? };
        lock.fast_composition
            .gpui_surfaces
            .insert(surface.id, created_surface);
    }

    let views = surfaces
        .iter()
        .filter_map(|surface| match &surface.content {
            PlatformCompositionSurfaceContent::Gpui => lock
                .fast_composition
                .gpui_surfaces
                .get(&surface.id)
                .map(|gpui_surface| Ok((surface.id, gpui_surface.view.as_ptr()))),
            PlatformCompositionSurfaceContent::Native(platform_surface)
            | PlatformCompositionSurfaceContent::ExternalGpu(platform_surface) => {
                Some(platform_surface.platform_handle().and_then(|handle| {
                    handle
                        .downcast::<usize>()
                        .map(|view| (surface.id, *view as Id))
                        .map_err(|_| anyhow!("native surface is not backed by an AppKit view"))
                }))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let views_by_id = views.iter().copied().collect::<FxHashMap<_, _>>();

    unsafe {
        let native_view = lock.native_view.as_ptr();
        let view_order = surfaces
            .iter()
            .filter_map(|surface| {
                let view = views_by_id.get(&surface.id).copied()?;
                let parent = surface
                    .parent
                    .and_then(|parent| views_by_id.get(&parent).copied())
                    .unwrap_or(native_view);
                Some((surface.id, view as usize, parent as usize))
            })
            .collect::<Vec<_>>();
        // A geometry update must preserve AppKit's mouse tracking and first
        // responder. Only a changed tree/order needs views detached and mounted.
        let parents_changed = view_order.iter().any(|(_, view, parent)| {
            let actual_parent: Id = msg_send![*view as Id, superview];
            actual_parent as usize != *parent
        });
        if lock.fast_composition.view_order != view_order || parents_changed {
            for (_, view, _) in &view_order {
                let _: () = msg_send![*view as Id, removeFromSuperview];
            }
            for (_, view, parent) in &view_order {
                let _: () = msg_send![*parent as Id, addSubview: *view as Id];
            }
            lock.fast_composition.native_views = Rc::new(
                view_order
                    .iter()
                    .filter_map(|(id, view, _)| {
                        (*id != base_surface
                            && !lock.fast_composition.gpui_surfaces.contains_key(id))
                        .then_some(*view)
                    })
                    .collect(),
            );
            lock.fast_composition.input_order = Rc::new(
                view_order
                    .iter()
                    .filter_map(|(id, view, _)| {
                        if *id == base_surface {
                            return None;
                        }
                        Some(match lock.fast_composition.gpui_surfaces.get(id) {
                            Some(surface) => InputLayer::Gpui(surface.input_regions.clone()),
                            None => InputLayer::Native(*view),
                        })
                    })
                    .collect(),
            );
            if lock.fast_composition.native_views.is_empty() {
                lock.fast_composition.pointer_capture.clear();
            }
            follow_native_views(&mut lock, window_state);
            lock.fast_composition.view_order = view_order;
        }
        lock.fast_composition.input_forwarders.clear();
        for surface in surfaces.iter().filter(|surface| match &surface.content {
            PlatformCompositionSurfaceContent::Native(attachment)
            | PlatformCompositionSurfaceContent::ExternalGpu(attachment) => {
                attachment.input_forwarding()
            }
            PlatformCompositionSurfaceContent::Gpui => false,
        }) {
            if let Some(view) = views_by_id.get(&surface.id) {
                lock.fast_composition
                    .input_forwarders
                    .insert(*view as usize);
            }
        }
        let window: Id = msg_send![native_view, window];
        if let Some(window) = window.as_ref() {
            ivar(window, NATIVE_INPUT_IVAR)
                .load_ptr::<Bool>(window)
                .write(Bool::new(!lock.fast_composition.native_views.is_empty()));
        }
        for surface in surfaces {
            let Some(view) = views_by_id.get(&surface.id).copied() else {
                continue;
            };
            let parent = surface
                .parent
                .and_then(|parent| views_by_id.get(&parent).copied())
                .unwrap_or(native_view);
            let window_frame = match surface.window_bounds {
                Some(bounds) => window_rect(native_view, bounds, lock.scale_factor()),
                None => msg_send![native_view, bounds],
            };
            let frame: NSRect = msg_send![native_view, convertRect: window_frame, toView: parent];
            let _: () = msg_send![view, setFrame: frame];
        }
    }
    Ok(())
}

/// Creates the overlay view and renderer of a GPUI surface, the size of the
/// window's view.
unsafe fn create_gpui_surface(
    window_state: &Rc<Mutex<MacWindowState>>,
    state: &mut MacWindowState,
) -> Result<MacGpuiSurface> {
    // Made before the view, so that a renderer that fails to build leaves no
    // view holding the window state behind.
    let mut renderer = renderer::new_overlay_renderer(
        state.fast_composition.renderer_context.clone(),
        &state.renderer,
    )?;
    renderer.set_presents_with_transaction(state.fast_composition.presents_with_transaction);
    let input_regions = Rc::new(RefCell::new(Vec::<gpui::Hitbox>::new()));
    let native_view = state.native_view.as_ptr();
    let class = OVERLAY_VIEW_CLASS.load(Ordering::Acquire).cast_const();
    anyhow::ensure!(!class.is_null(), "GPUIOverlayView is not registered");
    let view: Id = unsafe { msg_send![class, alloc] };
    let view: Id = unsafe {
        let bounds: NSRect = msg_send![native_view, bounds];
        msg_send![view, initWithFrame: bounds]
    };
    let object = unsafe { view.as_ref() }.context("failed to create GPUI composition view")?;
    unsafe {
        ivar(object, WINDOW_STATE_IVAR)
            .load_ptr::<*mut c_void>(object)
            .write(Rc::into_raw(window_state.clone()).cast_mut().cast());
        ivar(object, OVERLAY_INPUT_IVAR)
            .load_ptr::<*mut c_void>(object)
            .write(Rc::into_raw(input_regions.clone()).cast_mut().cast());
    }

    let scale_factor = state.scale_factor();
    renderer.update_drawable_size(state.content_size().to_device_pixels(scale_factor));
    set_contents_scale(&renderer, scale_factor);
    unsafe {
        let _: () = msg_send![
            view,
            setAutoresizingMask: NSAutoresizingMaskOptions::ViewWidthSizable
                | NSAutoresizingMaskOptions::ViewHeightSizable
        ];
        let _: () = msg_send![view, setWantsLayer: Bool::YES];
        let _: () = msg_send![view, setLayer: renderer.layer_ptr().cast::<AnyObject>()];
        let _: () = msg_send![
            view,
            setLayerContentsRedrawPolicy: NSViewLayerContentsRedrawPolicy::DuringViewResize
        ];
        let _: Id = msg_send![view, autorelease];
    }
    Ok(MacGpuiSurface {
        view: NonNull::new(view).context("GPUI composition view is null")?,
        renderer,
        input_regions,
    })
}

/// Paces the window's frames to fit its native views. A `CAMetalDisplayLink`
/// presents each drawable a refresh after the Core Animation transaction
/// that moves the window's native views, so they would lead the GPUI content
/// around them by a frame while it scrolls. While the window holds native
/// views it draws from the `CVDisplayLink` instead, presenting in that
/// transaction (see [`present_for_native_views`]), and gets its Metal link
/// back once they are gone. The switch waits for the frame being drawn to
/// end: a Metal link attached during a frame would skip that frame.
fn follow_native_views(state: &MacWindowState, window_state: &Rc<Mutex<MacWindowState>>) {
    if !needs_other_frame_source(state) {
        return;
    }
    let window_state = Rc::downgrade(window_state);
    state
        .foreground_executor
        .spawn(async move {
            if let Some(window_state) = window_state.upgrade() {
                switch_frame_source(&mut window_state.lock(), &window_state);
            }
        })
        .detach();
}

fn needs_other_frame_source(state: &MacWindowState) -> bool {
    let native = !state.fast_composition.native_views.is_empty();
    match &state.frame_source {
        FrameSource::Metal(_) => native,
        FrameSource::DisplayLink(_) => !native && state.fast_composition.metal_frames.is_some(),
    }
}

fn switch_frame_source(state: &mut MacWindowState, window_state: &Rc<Mutex<MacWindowState>>) {
    if !needs_other_frame_source(state) {
        return;
    }
    let frame_source = match &state.frame_source {
        FrameSource::Metal(metal_frames) => {
            // Detached rather than paused, as `stop_display_link` does, so
            // that the renderer takes the layer's own drawables again.
            metal_frames.replace(None);
            state.renderer.set_display_link_attached(false);
            state.fast_composition.metal_frames = Some(metal_frames.clone());
            FrameSource::DisplayLink(None)
        }
        FrameSource::DisplayLink(_) => {
            let Some(metal_frames) = state.fast_composition.metal_frames.take() else {
                return;
            };
            set_presents_with_transaction(state, false);
            FrameSource::Metal(metal_frames)
        }
    };
    // Replacing a `CVDisplayLink` source drops it, which leaves the registry.
    state.frame_source = frame_source;
    state.start_display_link(window_state);
}

/// Before a frame drawn from the `CVDisplayLink`: while the window holds
/// native views, its drawables reach the screen in the Core Animation
/// transaction that moves them.
pub(crate) fn present_for_native_views(state: &mut MacWindowState) {
    let native = !state.fast_composition.native_views.is_empty();
    if state.fast_composition.presents_with_transaction != native {
        set_presents_with_transaction(state, native);
    }
}

/// Sets whether the window's renderer and every surface's present their
/// drawables in a Core Animation transaction, as they do while AppKit waits
/// for the window to draw.
pub(crate) fn set_presents_with_transaction(state: &mut MacWindowState, enabled: bool) {
    state.fast_composition.presents_with_transaction = enabled;
    state.renderer.set_presents_with_transaction(enabled);
    for surface in state.fast_composition.gpui_surfaces.values_mut() {
        surface.renderer.set_presents_with_transaction(enabled);
    }
}

/// Follows the window's new scale factor and drawable size on every GPUI
/// surface.
pub(crate) fn scale_factor_changed(
    state: &mut MacWindowState,
    scale_factor: f32,
    drawable_size: Size<DevicePixels>,
) {
    for surface in state.fast_composition.gpui_surfaces.values_mut() {
        set_contents_scale(&surface.renderer, scale_factor);
        surface.renderer.update_drawable_size(drawable_size);
    }
}

/// Follows the window's new drawable size on every GPUI surface.
pub(crate) fn drawable_size_changed(state: &mut MacWindowState, drawable_size: Size<DevicePixels>) {
    for surface in state.fast_composition.gpui_surfaces.values_mut() {
        surface.renderer.update_drawable_size(drawable_size);
    }
}

fn set_contents_scale(renderer: &renderer::Renderer, scale_factor: f32) {
    let layer = renderer.layer_ptr().cast::<AnyObject>();
    if !layer.is_null() {
        unsafe {
            let _: () = msg_send![layer, setContentsScale: scale_factor as f64];
        }
    }
}

/// `bounds`, in window-content device pixels, as a rect in the coordinates of
/// the window's view, whose origin is at its bottom left.
unsafe fn window_rect(native_view: Id, bounds: Bounds<DevicePixels>, scale_factor: f32) -> NSRect {
    let bounds = bounds.map(|value| px(value.0 as f32 / scale_factor));
    let window_bounds: NSRect = unsafe { msg_send![native_view, bounds] };
    NSRect::new(
        NSPoint::new(
            f64::from(bounds.origin.x),
            window_bounds.size.height - f64::from(bounds.origin.y) - f64::from(bounds.size.height),
        ),
        NSSize::new(f64::from(bounds.size.width), f64::from(bounds.size.height)),
    )
}

/// A native surface: a layer-backed container view the embedder puts its
/// native view in.
struct MacNativeSurface {
    window_state: Weak<Mutex<MacWindowState>>,
    view: NonNull<AnyObject>,
    bounds: Cell<Bounds<DevicePixels>>,
}

impl PlatformSurfaceAttachment for MacNativeSurface {
    fn set_bounds(&self, bounds: Bounds<DevicePixels>) -> Result<()> {
        let window_state = self
            .window_state
            .upgrade()
            .context("native surface window has been released")?;
        let window_state = window_state.lock();
        unsafe {
            let native_view = window_state.native_view.as_ptr();
            let parent: Id = msg_send![self.view.as_ptr(), superview];
            let parent = if parent.is_null() {
                native_view
            } else {
                parent
            };
            let window_frame = window_rect(native_view, bounds, window_state.scale_factor());
            let frame: NSRect = msg_send![native_view, convertRect: window_frame, toView: parent];
            let _: () = msg_send![self.view.as_ptr(), setFrame: frame];
        }
        self.bounds.set(bounds);
        Ok(())
    }

    fn bounds(&self) -> Bounds<DevicePixels> {
        self.bounds.get()
    }

    fn set_parent_origin(&self, _origin: Point<DevicePixels>) -> Result<()> {
        self.set_bounds(self.bounds.get())
    }

    fn set_visible(&self, visible: bool) -> Result<()> {
        unsafe {
            let _: () = msg_send![self.view.as_ptr(), setHidden: Bool::new(!visible)];
        }
        Ok(())
    }

    fn platform_handle(&self) -> Result<Box<dyn Any>> {
        Ok(Box::new(self.view.as_ptr() as usize))
    }
}

impl Drop for MacNativeSurface {
    fn drop(&mut self) {
        unsafe {
            let _: () = msg_send![self.view.as_ptr(), removeFromSuperview];
            let _: () = msg_send![self.view.as_ptr(), release];
        }
    }
}
