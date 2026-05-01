use super::model::{CapturedFrame, DisplayMeta, ResolvedAction, SnapshotBudget};
use super::util::{parse_hotkey_key, to_enigo_button};
use crate::error::ToolError;
use enigo::{Axis, Coordinate, Direction, Enigo, Keyboard, Mouse, Settings as EnigoSettings};
use std::io::Cursor;
use tracing::debug;
use xcap::Monitor;
use xcap::image::{DynamicImage, ImageFormat, imageops::FilterType};

pub(crate) trait ComputerUseBackend: Send + Sync {
    fn list_displays(&self) -> Result<Vec<DisplayMeta>, ToolError>;
    fn capture_display(
        &self,
        display_id: u32,
        snapshot_budget: &SnapshotBudget,
    ) -> Result<CapturedFrame, ToolError>;
    fn perform_action(&self, action: &ResolvedAction) -> Result<String, ToolError>;
}

#[derive(Default)]
pub(crate) struct LocalComputerUseBackend;

impl ComputerUseBackend for LocalComputerUseBackend {
    fn list_displays(&self) -> Result<Vec<DisplayMeta>, ToolError> {
        let monitors = Monitor::all().map_err(|error| {
            ToolError::execution_failed(format!("failed to list monitors: {error}"))
        })?;
        let mut displays = Vec::with_capacity(monitors.len());
        for monitor in monitors {
            displays.push(DisplayMeta {
                display_id: monitor.id().map_err(to_tool_error("monitor.id"))?,
                width_px: monitor.width().map_err(to_tool_error("monitor.width"))?,
                height_px: monitor.height().map_err(to_tool_error("monitor.height"))?,
                scale_factor: monitor
                    .scale_factor()
                    .map_err(to_tool_error("monitor.scale_factor"))?,
                origin_x: monitor.x().map_err(to_tool_error("monitor.x"))?,
                origin_y: monitor.y().map_err(to_tool_error("monitor.y"))?,
                is_primary: monitor
                    .is_primary()
                    .map_err(to_tool_error("monitor.is_primary"))?,
            });
        }
        Ok(displays)
    }

    fn capture_display(
        &self,
        display_id: u32,
        snapshot_budget: &SnapshotBudget,
    ) -> Result<CapturedFrame, ToolError> {
        let monitor = Monitor::all()
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to list monitors: {error}"))
            })?
            .into_iter()
            .find(|value| value.id().ok() == Some(display_id))
            .ok_or_else(|| ToolError::NotFound(format!("display {} not found", display_id)))?;

        let image = monitor.capture_image().map_err(|error| {
            ToolError::execution_failed(format!("capture_image failed: {error}"))
        })?;

        let scale_factor = monitor
            .scale_factor()
            .map_err(to_tool_error("monitor.scale_factor"))?;
        let (width_px, height_px) = normalized_snapshot_dimensions(
            image.width(),
            image.height(),
            scale_factor,
            snapshot_budget.max_side_px,
        );

        let mut encoded_image = DynamicImage::ImageRgba8(image);
        if encoded_image.width() != width_px || encoded_image.height() != height_px {
            encoded_image = encoded_image.resize_exact(width_px, height_px, FilterType::Triangle);
        }

        let pre_budget_width = encoded_image.width();
        let pre_budget_height = encoded_image.height();
        let (encoded_png, final_width, final_height, resize_passes) =
            encode_png_with_budget(encoded_image, snapshot_budget)?;
        if resize_passes > 0 {
            debug!(
                target: "pioneer_tools::computer_use",
                event = "computer_use.snapshot.transformed",
                display_id,
                reason = "attachment_budget_exceeded",
                budget_profile = snapshot_budget.profile,
                budget_max_bytes = snapshot_budget.max_bytes,
                budget_max_side_px = snapshot_budget.max_side_px,
                resize_passes,
                input_width_px = pre_budget_width,
                input_height_px = pre_budget_height,
                output_width_px = final_width,
                output_height_px = final_height,
                output_size_bytes = encoded_png.len(),
                "computer_use snapshot transformed before LLM attachment send"
            );
        }

        Ok(CapturedFrame {
            width_px: final_width,
            height_px: final_height,
            scale_factor,
            png_bytes: encoded_png,
            resize_passes,
        })
    }

    fn perform_action(&self, action: &ResolvedAction) -> Result<String, ToolError> {
        let mut enigo = init_enigo_with_permission_guidance()?;

        match action {
            ResolvedAction::Move { x, y } => {
                enigo
                    .move_mouse(*x, *y, Coordinate::Abs)
                    .map_err(to_enigo_error("move_mouse"))?;
                Ok(format!("Moved mouse to {},{}", x, y))
            }
            ResolvedAction::Click {
                x,
                y,
                button,
                click_count,
            } => {
                enigo
                    .move_mouse(*x, *y, Coordinate::Abs)
                    .map_err(to_enigo_error("move_mouse"))?;
                for _ in 0..*click_count {
                    enigo
                        .button(to_enigo_button(*button), Direction::Click)
                        .map_err(to_enigo_error("button"))?;
                }
                Ok(format!(
                    "Clicked {:?} at {},{} (count={})",
                    button, x, y, click_count
                ))
            }
            ResolvedAction::Scroll { delta_x, delta_y } => {
                if *delta_y != 0 {
                    enigo
                        .scroll(*delta_y, Axis::Vertical)
                        .map_err(to_enigo_error("scroll vertical"))?;
                }
                if *delta_x != 0 {
                    enigo
                        .scroll(*delta_x, Axis::Horizontal)
                        .map_err(to_enigo_error("scroll horizontal"))?;
                }
                Ok(format!("Scrolled x={} y={}", delta_x, delta_y))
            }
            ResolvedAction::TypeText { text } => {
                enigo.text(text.as_str()).map_err(to_enigo_error("text"))?;
                Ok(format!("Typed {} chars", text.chars().count()))
            }
            ResolvedAction::Hotkey { keys } => {
                let keys = keys
                    .iter()
                    .map(|value| parse_hotkey_key(value))
                    .collect::<Result<Vec<_>, _>>()?;
                if keys.len() == 1 {
                    enigo
                        .key(keys[0], Direction::Click)
                        .map_err(to_enigo_error("key click"))?;
                } else {
                    let last_index = keys.len().saturating_sub(1);
                    for key in &keys[..last_index] {
                        enigo
                            .key(*key, Direction::Press)
                            .map_err(to_enigo_error("key press"))?;
                    }
                    enigo
                        .key(keys[last_index], Direction::Click)
                        .map_err(to_enigo_error("key click"))?;
                    for key in keys[..last_index].iter().rev() {
                        enigo
                            .key(*key, Direction::Release)
                            .map_err(to_enigo_error("key release"))?;
                    }
                }
                Ok("Executed hotkey".to_owned())
            }
            ResolvedAction::Wait { wait_ms } => Ok(format!("Waited {}ms", wait_ms)),
        }
    }
}

fn to_tool_error(op: &'static str) -> impl FnOnce(xcap::XCapError) -> ToolError {
    move |error| ToolError::execution_failed(format!("{op} failed: {error}"))
}

fn init_enigo_with_permission_guidance() -> Result<Enigo, ToolError> {
    Enigo::new(&EnigoSettings::default()).map_err(permission_aware_enigo_init_error)
}

fn permission_aware_enigo_init_error(error: enigo::NewConError) -> ToolError {
    let raw = error.to_string();
    if cfg!(target_os = "macos") {
        let lowered = raw.to_ascii_lowercase();
        if lowered.contains("permission") || lowered.contains("accessibility") {
            let binary = std::env::current_exe()
                .map(|value| value.display().to_string())
                .unwrap_or_else(|_| "<unknown>".to_owned());
            return ToolError::execution_failed(format!(
                "failed to initialize enigo: {raw}. \
macOS input simulation permission is missing for binary `{binary}`. \
Grant access in System Settings > Privacy & Security > Accessibility and Input Monitoring, then restart this gateway process."
            ));
        }
    }
    ToolError::execution_failed(format!("failed to initialize enigo: {raw}"))
}

fn to_enigo_error(op: &'static str) -> impl FnOnce(enigo::InputError) -> ToolError {
    move |error| ToolError::execution_failed(format!("{op} failed: {error}"))
}

fn normalized_snapshot_dimensions(
    width_px: u32,
    height_px: u32,
    scale_factor: f32,
    max_side_px: u32,
) -> (u32, u32) {
    if width_px == 0 || height_px == 0 {
        return (width_px.max(1), height_px.max(1));
    }

    let mut normalized_width = width_px;
    let mut normalized_height = height_px;

    if scale_factor.is_finite() && scale_factor > 1.0 {
        normalized_width = ((f64::from(width_px) / f64::from(scale_factor)).round() as u32).max(1);
        normalized_height =
            ((f64::from(height_px) / f64::from(scale_factor)).round() as u32).max(1);
    }

    let longest = normalized_width.max(normalized_height);
    if longest <= max_side_px {
        return (normalized_width, normalized_height);
    }

    let ratio = f64::from(max_side_px) / f64::from(longest);
    let scaled_width = ((f64::from(normalized_width) * ratio).round() as u32).max(1);
    let scaled_height = ((f64::from(normalized_height) * ratio).round() as u32).max(1);
    (scaled_width, scaled_height)
}

fn encode_png_with_budget(
    mut image: DynamicImage,
    snapshot_budget: &SnapshotBudget,
) -> Result<(Vec<u8>, u32, u32, u32), ToolError> {
    let mut resize_passes = 0u32;

    loop {
        let mut buffer = Cursor::new(Vec::<u8>::new());
        image
            .write_to(&mut buffer, ImageFormat::Png)
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to encode PNG screenshot: {error}"))
            })?;
        let encoded = buffer.into_inner();
        if encoded.len() <= snapshot_budget.max_bytes {
            return Ok((encoded, image.width(), image.height(), resize_passes));
        }

        let current_width = image.width();
        let current_height = image.height();
        if current_width <= snapshot_budget.min_side_px
            || current_height <= snapshot_budget.min_side_px
        {
            return Err(ToolError::execution_failed(format!(
                "snapshot exceeds max {} bytes even after downscale (current={}x{}, size={} bytes)",
                snapshot_budget.max_bytes,
                current_width,
                current_height,
                encoded.len()
            )));
        }

        let next_width =
            ((f64::from(current_width) * snapshot_budget.downscale_factor).round() as u32).max(1);
        let next_height =
            ((f64::from(current_height) * snapshot_budget.downscale_factor).round() as u32).max(1);

        image = image.resize_exact(next_width, next_height, FilterType::Triangle);
        resize_passes = resize_passes.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_budget(max_bytes: usize, max_side_px: u32, min_side_px: u32) -> SnapshotBudget {
        SnapshotBudget {
            provider_hint: Some("openai".to_owned()),
            model_hint: Some("gpt-5".to_owned()),
            profile: "test".to_owned(),
            max_bytes,
            max_side_px,
            min_side_px,
            downscale_factor: 0.8,
        }
    }

    #[test]
    fn normalized_dimensions_apply_scale_and_max_side() {
        let (w, h) = normalized_snapshot_dimensions(3024, 1964, 2.0, 1280);
        assert!(w <= 1280);
        assert!(h <= 1280);
        assert!(w > 0 && h > 0);
    }

    #[test]
    fn encode_png_with_budget_downscales_until_limit() {
        let mut image = xcap::image::RgbaImage::new(1200, 900);
        for (x, y, pixel) in image.enumerate_pixels_mut() {
            let value = ((x.wrapping_mul(31) + y.wrapping_mul(17)) % 255) as u8;
            *pixel =
                xcap::image::Rgba([value, value.wrapping_add(53), value.wrapping_add(101), 255]);
        }
        let budget = test_budget(450_000, 1280, 320);
        let (encoded, width, height, passes) =
            encode_png_with_budget(DynamicImage::ImageRgba8(image), &budget)
                .expect("encoding should fit the budget");
        assert!(encoded.len() <= budget.max_bytes);
        assert!(width <= 1200);
        assert!(height <= 900);
        assert!(passes > 0);
    }
}
