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
