//! Clip the windowed WebView2 host without shrinking the browser's DOM viewport.
//! This does not provide DirectComposition ordering of GPUI overlays above the page.

use std::cell::Cell;

use ::windows::Win32::{
    Foundation::HWND,
    Graphics::Gdi::{CreateRectRgn, DeleteObject, HRGN, SetWindowRgn},
    UI::HiDpi::GetDpiForWindow,
};
use anyhow::{Context as _, Result};
use gpui::{Bounds, Pixels};
use wry::{WebViewExtWindows as _, dpi::LogicalPosition};

#[derive(Clone, Copy, PartialEq, Eq)]
struct ClipRect {
    left: i32,
    top: i32,
    right: i32,
    bottom: i32,
}

#[derive(Default)]
pub(super) struct NativeClip {
    applied: Cell<Option<ClipRect>>,
}

impl NativeClip {
    pub(super) fn set_geometry(
        &self,
        webview: &wry::WebView,
        document: Bounds<Pixels>,
        visible: Bounds<Pixels>,
    ) -> Result<()> {
        let hwnd = host_window(webview)?;
        // SAFETY: Wry owns this child HWND, and geometry runs on its creating UI thread.
        let dpi = unsafe { GetDpiForWindow(hwnd) };
        anyhow::ensure!(dpi != 0, "cannot read WebView host DPI");
        let scale = f64::from(dpi) / 96.;
        let position = |x: Pixels, y: Pixels| {
            LogicalPosition::new(f64::from(x), f64::from(y)).to_physical::<i32>(scale)
        };
        let origin = position(document.origin.x, document.origin.y);
        let extent = wry::dpi::LogicalSize::new(
            f64::from(document.size.width),
            f64::from(document.size.height),
        )
        .to_physical::<i32>(scale);
        let start = position(visible.origin.x, visible.origin.y);
        let end = position(
            visible.origin.x + visible.size.width,
            visible.origin.y + visible.size.height,
        );
        let clip = ClipRect {
            left: (start.x - origin.x).clamp(0, extent.width),
            top: (start.y - origin.y).clamp(0, extent.height),
            // The window size is rounded independently from its origin. A fully
            // visible edge must reach that size even if rounding absolute edges
            // would otherwise leave one device pixel permanently clipped.
            right: if visible.origin.x + visible.size.width
                >= document.origin.x + document.size.width
            {
                extent.width
            } else {
                (end.x - origin.x).clamp(0, extent.width)
            },
            bottom: if visible.origin.y + visible.size.height
                >= document.origin.y + document.size.height
            {
                extent.height
            } else {
                (end.y - origin.y).clamp(0, extent.height)
            },
        };

        // Keep the full page size and position. Rounding the window and clip in device
        // pixels avoids a separate logical-to-device rounding phase at fractional DPI.
        webview.set_bounds(wry::Rect {
            position: origin.into(),
            size: extent.into(),
        })?;
        let full = ClipRect {
            left: 0,
            top: 0,
            right: extent.width,
            bottom: extent.height,
        };
        self.apply(hwnd, (clip != full).then_some(clip))
    }

    fn apply(&self, hwnd: HWND, clip: Option<ClipRect>) -> Result<()> {
        if self.applied.get() == clip {
            return Ok(());
        }
        if let Some(clip) = clip {
            // SAFETY: Coordinates are window-local device pixels. On success the system
            // owns the HRGN; on failure we must delete it ourselves.
            unsafe {
                let region = CreateRectRgn(clip.left, clip.top, clip.right, clip.bottom);
                anyhow::ensure!(!region.0.is_null(), "cannot create WebView clip region");
                if SetWindowRgn(hwnd, Some(region), true) == 0 {
                    let _ = DeleteObject(region.into());
                    anyhow::bail!("cannot apply WebView clip region");
                }
            }
        } else {
            // SAFETY: A null region restores the original rectangular child window.
            anyhow::ensure!(
                unsafe { SetWindowRgn(hwnd, None::<HRGN>, true) } != 0,
                "cannot clear WebView clip region"
            );
        }
        self.applied.set(clip);
        Ok(())
    }

    /// Explicit staging bounds and a fully visible page must discard an earlier clip.
    pub(super) fn clear(&self, webview: &wry::WebView) -> Result<()> {
        if self.applied.get().is_some() {
            self.apply(host_window(webview)?, None)?;
        }
        Ok(())
    }
}

fn host_window(webview: &wry::WebView) -> Result<HWND> {
    let mut hwnd = HWND::default();
    // SAFETY: The controller is UI-thread-local and Wry retains its parent child HWND.
    unsafe { webview.controller().ParentWindow(&mut hwnd) }
        .context("cannot get WebView host window")?;
    anyhow::ensure!(!hwnd.0.is_null(), "WebView has no host window");
    Ok(hwnd)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::windows::{
        Win32::{
            Foundation::RECT,
            Graphics::Gdi::{GetWindowRgnBox, SIMPLEREGION},
            UI::WindowsAndMessaging::{
                CreateWindowExW, DestroyWindow, GetClientRect, WINDOW_EX_STYLE, WS_POPUP,
            },
        },
        core::w,
    };

    #[test]
    fn native_clipping_changes_the_visible_region_without_resizing_the_document() {
        // Use a real HWND and the OS region query. No WebView2 runtime is needed to
        // prove that the host clips rather than changing the browser client extent.
        let hwnd = unsafe {
            CreateWindowExW(
                WINDOW_EX_STYLE::default(),
                w!("STATIC"),
                w!("WebView clip regression"),
                WS_POPUP,
                0,
                0,
                200,
                100,
                None,
                None,
                None,
                None,
            )
        }
        .unwrap();
        struct WindowGuard(HWND);
        impl Drop for WindowGuard {
            fn drop(&mut self) {
                unsafe { DestroyWindow(self.0) }.unwrap();
            }
        }
        let _window = WindowGuard(hwnd);
        let clip = NativeClip::default();
        for expected in [
            ClipRect {
                left: 20,
                top: 10,
                right: 120,
                bottom: 80,
            },
            ClipRect {
                left: 0,
                top: 0,
                right: 180,
                bottom: 100,
            },
        ] {
            clip.apply(hwnd, Some(expected)).unwrap();
            let mut visible = RECT::default();
            assert_eq!(unsafe { GetWindowRgnBox(hwnd, &mut visible) }, SIMPLEREGION);
            assert_eq!(
                (visible.left, visible.top, visible.right, visible.bottom),
                (expected.left, expected.top, expected.right, expected.bottom),
            );
            let mut document = RECT::default();
            unsafe { GetClientRect(hwnd, &mut document) }.unwrap();
            assert_eq!((document.right, document.bottom), (200, 100));
        }
        clip.apply(hwnd, None).unwrap();
        let mut visible = RECT::default();
        assert_eq!(unsafe { GetWindowRgnBox(hwnd, &mut visible) }.0, 0);
    }
}
