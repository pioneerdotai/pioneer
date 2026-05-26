use super::super::model::{CapturedFrame, SnapshotBudget};
use crate::error::ToolError;
use image::{DynamicImage, ImageFormat, RgbaImage, imageops::FilterType};
use std::io::Cursor;
use tracing::debug;

pub(crate) fn captured_frame_from_rgba_buffer(
    width_px: u32,
    height_px: u32,
    scale_factor: f32,
    pixels: Vec<u8>,
    snapshot_budget: &SnapshotBudget,
) -> Result<CapturedFrame, ToolError> {
    let image = RgbaImage::from_raw(width_px, height_px, pixels)
        .ok_or_else(|| ToolError::execution_failed("screenshot had invalid RGBA buffer"))?;
    captured_frame_from_rgba_image(image, scale_factor, snapshot_budget)
}

pub(crate) fn captured_frame_from_rgba_image(
    image: RgbaImage,
    scale_factor: f32,
    snapshot_budget: &SnapshotBudget,
) -> Result<CapturedFrame, ToolError> {
    let source_width_px = image.width();
    let source_height_px = image.height();
    let (initial_transport_width, initial_transport_height) = normalized_transport_dimensions(
        source_width_px,
        source_height_px,
        scale_factor,
        snapshot_budget.max_side_px,
    );

    let mut transport = DynamicImage::ImageRgba8(image);
    if transport.width() != initial_transport_width
        || transport.height() != initial_transport_height
    {
        transport = transport.resize_exact(
            initial_transport_width,
            initial_transport_height,
            FilterType::Triangle,
        );
    }

    let pre_budget_width = transport.width();
    let pre_budget_height = transport.height();
    let (encoded_png, transport_width_px, transport_height_px, resize_passes) =
        encode_png_with_budget(transport, snapshot_budget)?;
    if resize_passes > 0 {
        debug!(
            target: "pioneer_tools::computer_use",
            event = "computer_use.snapshot.transformed",
            reason = "attachment_budget_exceeded",
            budget_profile = snapshot_budget.profile,
            budget_max_bytes = snapshot_budget.max_bytes,
            budget_max_side_px = snapshot_budget.max_side_px,
            resize_passes,
            source_width_px,
            source_height_px,
            pre_budget_width,
            pre_budget_height,
            transport_width_px,
            transport_height_px,
            output_size_bytes = encoded_png.len(),
            "computer_use snapshot transformed before LLM attachment send"
        );
    }

    Ok(CapturedFrame {
        width_px: source_width_px,
        height_px: source_height_px,
        transport_width_px,
        transport_height_px,
        scale_factor,
        png_bytes: encoded_png,
        resize_passes,
    })
}

fn normalized_transport_dimensions(
    width_px: u32,
    height_px: u32,
    scale_factor: f32,
    max_side_px: u32,
) -> (u32, u32) {
    if width_px == 0 || height_px == 0 {
        return (width_px.max(1), height_px.max(1));
    }

    let mut transport_width = width_px;
    let mut transport_height = height_px;

    if scale_factor.is_finite() && scale_factor > 1.0 {
        transport_width = ((f64::from(width_px) / f64::from(scale_factor)).round() as u32).max(1);
        transport_height = ((f64::from(height_px) / f64::from(scale_factor)).round() as u32).max(1);
    }

    let longest = transport_width.max(transport_height);
    if longest <= max_side_px {
        return (transport_width, transport_height);
    }

    let ratio = f64::from(max_side_px) / f64::from(longest);
    let scaled_width = ((f64::from(transport_width) * ratio).round() as u32).max(1);
    let scaled_height = ((f64::from(transport_height) * ratio).round() as u32).max(1);
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
        let (w, h) = normalized_transport_dimensions(3024, 1964, 2.0, 1280);
        assert!(w <= 1280);
        assert!(h <= 1280);
        assert!(w > 0 && h > 0);
    }

    #[test]
    fn encode_png_with_budget_downscales_until_limit() {
        let mut image = RgbaImage::new(1200, 900);
        for (x, y, pixel) in image.enumerate_pixels_mut() {
            let value = ((x.wrapping_mul(31) + y.wrapping_mul(17)) % 255) as u8;
            *pixel = image::Rgba([value, value.wrapping_add(53), value.wrapping_add(101), 255]);
        }
        let budget = test_budget(450_000, 1280, 320);
        let frame = captured_frame_from_rgba_image(image, 1.0, &budget)
            .expect("encoding should fit the budget");
        assert!(frame.png_bytes.len() <= budget.max_bytes);
        assert_eq!(frame.width_px, 1200);
        assert_eq!(frame.height_px, 900);
        assert!(frame.transport_width_px <= 1200);
        assert!(frame.transport_height_px <= 900);
        assert!(frame.resize_passes > 0);
    }

    #[test]
    fn captured_frame_preserves_source_and_transport_dimensions() {
        let image = RgbaImage::from_pixel(2000, 1000, image::Rgba([1, 2, 3, 255]));
        let budget = test_budget(8 * 1024 * 1024, 800, 320);
        let frame = captured_frame_from_rgba_image(image, 1.0, &budget).expect("frame");
        assert_eq!(frame.width_px, 2000);
        assert_eq!(frame.height_px, 1000);
        assert_eq!(frame.transport_width_px, 800);
        assert_eq!(frame.transport_height_px, 400);
    }
}
