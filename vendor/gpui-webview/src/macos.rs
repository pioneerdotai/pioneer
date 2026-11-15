//! AppKit calls that do not depend on window composition.

/// Give the page an appearance of its own; `None` inherits the window's again.
pub(crate) fn set_theme(webview: &wry::WebView, theme: wry::Theme) {
    use objc2_app_kit::{
        NSAppearance, NSAppearanceCustomization as _, NSAppearanceNameAqua,
        NSAppearanceNameDarkAqua,
    };
    use wry::WebViewExtMacOS as _;

    let name = match theme {
        wry::Theme::Dark => Some(unsafe { NSAppearanceNameDarkAqua }),
        wry::Theme::Light => Some(unsafe { NSAppearanceNameAqua }),
        _ => None,
    };
    let appearance = name.and_then(NSAppearance::appearanceNamed);
    webview.webview().setAppearance(appearance.as_deref());
}

/// Wry's macOS background setter is a no-op. WebKit's public under-page
/// color does not change its outer NSView backdrop, which is exposed while
/// a resized document is awaiting presentation.
pub(crate) fn set_background_color(webview: &wry::WebView, rgba: wry::RGBA) {
    use objc2::{msg_send, sel};
    use objc2_app_kit::NSColor;
    use wry::WebViewExtMacOS as _;
    let native = webview.webview();
    let (red, green, blue, alpha) = rgba;
    let color = NSColor::colorWithSRGBRed_green_blue_alpha(
        f64::from(red) / 255.,
        f64::from(green) / 255.,
        f64::from(blue) / 255.,
        f64::from(alpha) / 255.,
    );
    // WebKit SPI is availability-checked; unsupported implementations retain
    // Wry's behavior. KVC's `backgroundColor` key targets a different property.
    unsafe {
        let accepts: bool = msg_send![&*native, respondsToSelector: sel!(_setBackgroundColor:)];
        if accepts {
            let _: () = msg_send![&*native, _setBackgroundColor: &*color];
        }
        let accepts: bool =
            msg_send![&*native, respondsToSelector: sel!(setUnderPageBackgroundColor:)];
        if accepts {
            native.setUnderPageBackgroundColor(Some(&color));
        }
    }
}

#[cfg(feature = "gpui-fast")]
pub(crate) fn install_presentation_handler(
    webview: &wry::WebView,
    container: &objc2_app_kit::NSView,
) {
    use objc2::{msg_send, runtime::Bool, sel};
    use objc2_foundation::NSSize;
    use std::{cell::Cell, rc::Rc};
    use wry::WebViewExtMacOS as _;

    #[derive(Clone, Copy, PartialEq)]
    struct Geometry {
        size: NSSize,
        scale: f64,
    }

    let native = webview.webview();
    // GPUI invokes the container's handler after the frame callback returns;
    // no App/Window borrow or platform mutex may span native IPC delivery.
    // Only the completed WebKit layer tree, rather than DOM resize/rAF, is a
    // presentation barrier. This SPI has existed since macOS 10.12.
    let supported = unsafe {
        let host: Bool =
            msg_send![container, respondsToSelector: sel!(setGpuiPresentationHandler:)];
        let browser: Bool =
            msg_send![&*native, respondsToSelector: sel!(_doAfterNextPresentationUpdate:)];
        host.as_bool() && browser.as_bool()
    };
    if !supported {
        return;
    }

    let completed = Rc::new(Cell::new(None::<Geometry>));
    let pending = Rc::new(Cell::new(None::<Geometry>));
    let prepare = block2::RcBlock::new(move || -> Bool {
        if native.isHiddenOrHasHiddenAncestor() {
            return Bool::YES;
        }
        let Some(window) = native.window() else {
            return Bool::YES;
        };
        let geometry = Geometry {
            size: native.frame().size,
            scale: window.backingScaleFactor(),
        };
        if geometry.size.width <= 0.
            || geometry.size.height <= 0.
            || completed.get() == Some(geometry)
        {
            return Bool::YES;
        }
        // One request in flight. If another resize supersedes it, the next
        // readiness pass requests the latest geometry instead of queuing sizes.
        if pending.get().is_none() {
            pending.set(Some(geometry));
            let pending = pending.clone();
            let completed = completed.clone();
            let done = block2::RcBlock::new(move || {
                completed.set(Some(geometry));
                pending.set(None);
            });
            unsafe {
                let _: () = msg_send![&*native, _doAfterNextPresentationUpdate: &*done];
            }
        }
        Bool::NO
    });
    unsafe {
        let _: () = msg_send![container, setGpuiPresentationHandler: &*prepare];
    }
}
