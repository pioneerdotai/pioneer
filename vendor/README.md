# Native WebView presentation patches

These are narrow, source-pinned dependency patches, used by ordinary `cargo run -p pioneer-desktop` builds. They do not require files or Cargo overrides outside the checkout.

- `gpui-fast-macos`: crates.io 0.1.14 (upstream license included).
- `gpui-webview`: gpui-kit commit `b003decbf7ab0067f0ab7b4931ca5995d11f4282`, crate version 0.9.1 (upstream Apache license included). The standalone manifest keeps the same gpui-kit dependency identity as the workspace.
- `gpui-base`: the same gpui-kit commit and version 0.9.1, with its upstream Apache license. The standalone manifest expands the upstream workspace dependencies without changing their requirements. Its resizable-group patch uses measured sizes before drag calculations and excludes hidden slots from layout accounting and divider selection while preserving their preferred sizes and stable indices. Boundary movement transfers only available space, respecting both the growing panel's maximum and the shrinking panels' minima without changing unrelated fixed panels.

The Desktop shell marks its left/right sidebars and bottom app panel as fixed, leaving the centre flexible. Thread-local right/bottom panels follow the same policy. Hidden slots are not written back as user size preferences. These layout changes apply to macOS, Windows and Linux; they do not alter native WebView presentation or the macOS frame-readiness deadline.

## Why the patches exist

Changing WKWebView.frame sends geometry to the Web process asynchronously. DOM resize and requestAnimationFrame callbacks precede completion of WebKit's layer tree. Publishing the new native container and Metal frame before that completion exposes the native backdrop, including large L-shaped flashes during panel changes.

The macOS backend now keeps an explicit Core Animation transaction around each GPUI frame. Metal surfaces present with that transaction while native surfaces are present. After the GPUI frame callback returns and releases its App/Window borrows, native containers invoke their own presentation-readiness handlers. All handlers prepare concurrently and share one 20 ms deadline per window. The main run loop delivers replies with no GPUI borrow or window mutex held; AppKit's input-dispatch loop is not nested. Idle, hidden or unchanged WebViews request no extra paint. The deadline is a responsiveness fallback, not a fixed delay. Earlier measurements found ordinary resizes completing in roughly 6–12 ms and native fullscreen transitions taking 76–78 ms; the current shorter budget can fall back before a fullscreen presentation completes.

Metal display-link callbacks enqueue one retained update on the main run loop, in common modes. Rendering and native readiness run after the QuartzCore callback returns. Waiting for WebKit inside that callback reentered QuartzCore's display timer and caused a native fullscreen crash. Each source admits at most one queued/in-progress update; source replacement invalidates queued updates, and weak window references prevent work after window closure.

The container-owned Cocoa seam is `setGpuiPresentationHandler:` / `gpuiPrepareForPresentation`. It knows nothing about WebKit or arbitrary native descendants. Its copied block is released with the container. Other native widgets remain immediately ready unless they install a handler.

The WebView adapter uses the availability-checked WebKit SPI `_doAfterNextPresentationUpdate:` (present since macOS 10.12). This is a completed-layer-tree barrier, not a DOM event or a software snapshot. An unsupported SPI or an unresponsive Web process falls back to bounded asynchronous presentation. It is a private Apple API and must be reviewed for distribution requirements and when upgrading WebKit.

The adapter also implements the existing background-color operation on macOS: Wry's implementation is a no-op there. `_setBackgroundColor:` changes the actual outer backdrop; the public under-page color and the KVC `backgroundColor` key are different properties. Both selectors are availability-checked. Apps uses its theme color for the native backdrop. Visual resize diagnostics should temporarily use a contrasting backdrop so color matching cannot conceal a presentation regression.

WebKit also truncates `view.frame.size` to an integer drawing-area size. The adapter rounds the document viewport outward to whole logical points, while retaining the exact GPUI clip and top edge. This prevents persistent one-pixel seams at fractional panel/column widths; simply waiting for presentation cannot fill a truncated viewport.

## Windows and Linux adapters

The Windows adapter applies inherited clips to Wry's WebView2 child HWND with `SetWindowRgn`. The full browser viewport remains unchanged. Clip coordinates and window bounds use the same device-pixel rounding, including fractional DPI. Successful region assignment transfers ownership to Windows; failure deletes the region. Returning to a full document or setting explicit loading bounds removes the old clip. This fixes the missing partial-clip behavior, not GPUI overlay ordering: windowed WebView2 still covers GPUI overlays in the visible page area.

The Linux adapter no longer polls GTK every 16 ms. It follows GLib's prepare/query/poll/check/dispatch protocol, using source descriptor readiness and the actual next GLib deadline. Preparation and dispatch stay on the GTK/GPUI UI thread. One application-owned waiter thread performs only the blocking descriptor poll; a cancellation socket wakes even an infinite wait during application shutdown. Each dispatch yields to GPUI, including continuously-ready idle sources. GTK callbacks are not dispatched recursively from GPUI prepaint.

These changes do not add a WebKitGTK presentation-completion barrier to the Linux X11 compositor. Removing the artificial GTK scheduling delay cannot prove the absence of transient old browser buffers during rapid resize. Windows and Linux still require native visual validation; a successful bounds call or a compile check is not that validation. The macOS presentation and fullscreen fixes remain platform-specific and unchanged.

## Panel resize input: source review across desktop platforms

The macOS composition dispatcher now leaves a GPUI-origin drag on AppKit's original tracking path when the pointer crosses a native view. Previously it marked that event as already observed without dispatching it, so `handle_view_event` discarded AppKit's delivery to the GPUI view that owned the press. Native-origin drags still use `native_owner` and retain their original WebView selection/tracking path. Mouse-up keeps the existing capture-release and pre-dispatch path. No new capture field, event monitor or presentation wait was added.

The shared `gpui-base` resize/visibility changes (including the earlier left-sidebar fix) and Desktop/Thread panel sizing have no OS-specific branches. Source review covered both axes, hidden leading/trailing slots, last-visible-panel programmatic resizing, reversal at min/max, and preservation of unrelated fixed panels. In particular, a right panel at 600px with a 640px maximum can receive only 40px from a leftward drag; the rest is not removed from the centre or left to flex layout to correct.

Platform input review used the locked GPUI Fast 0.1.14 sources:

- macOS: `src/fast/composition.rs::send_window_event` and `src/window.rs::handle_view_event`; the incorrect observed-event suppression above is patched here.
- Windows: `src/events.rs::handle_mouse_down_msg` calls `SetCapture(handle)`; motion and release go through `handle_mouse_move_msg` / `handle_mouse_up_msg`, which releases capture. Crossing the WebView child HWND does not use the macOS hit-test/observed-event filter. No equivalent filter defect was found in this path, so no Windows backend fork was added for this fix.
- Linux X11/XWayland: `src/linux/x11/client.rs` dispatches XI button/motion/release events by their event window, with `src/fast/composition/x11.rs` mapping overlay/native-slot coordinates to the GPUI window. XI button tracking does not use the macOS observed-event filter. WebKitGTK's outside-click forwarding remains a separate synthetic press/release pair. No equivalent filter defect was found in these paths.
- Linux Wayland: `src/linux/wayland/client.rs` sends motion using `mouse_focused_window`, `mouse_location` and `button_pressed`; composition's `get_pointer_window` / `pointer_position` map subsurfaces. Shared panel sizing applies here too. The current WebKitGTK adapter requires X11/XWayland and explicitly rejects a native Wayland parent, so a Wayland+WebView runtime result is not claimed.

This is static source review only. Builds, tests and native interaction checks for these panel changes have not been run. It does not establish that all platform-specific resize or capture-cancellation issues are absent.

## Validation and updates

Validate against the actual Desktop, not only a small HTML fixture: live resize in both directions, fast drags, expanded/right/bottom transitions, a running Terminal, timeline HTML, clipping/scrolling, overlays, focus and closing/reopening. Capture the real displayed frames with a contrasting native backdrop; matching model bounds or receiving DOM events is not proof of correct presentation.

When replacing either dependency, compare against the exact versions above, port the presentation hook / frame transaction and the platform adapter fixes, and repeat the native visual checks. The remaining upstream source is intentionally retained unchanged.
