use anyhow::{Context as _, Result, bail};
use image::{DynamicImage, GenericImageView as _, ImageFormat, imageops::FilterType};
use pioneer_client::artifacts::preview as client_artifact_preview;
use std::{fs, io::Cursor, path::Path};

pub(crate) struct DesktopArtifactPreviewImageRenderer;

impl client_artifact_preview::ArtifactPreviewImageRenderer for DesktopArtifactPreviewImageRenderer {
    fn write_preview_variants(
        &self,
        source_bytes: &[u8],
        targets: &[client_artifact_preview::ArtifactPreviewVariantTarget],
    ) -> Result<()> {
        let source_image = image::load_from_memory(source_bytes)
            .context("failed to decode artifact thumbnail preview image")?;
        for target in targets {
            write_artifact_preview_variant(
                &source_image,
                target.path.as_path(),
                target.width_px,
                target.height_px,
            )?;
        }
        Ok(())
    }
}

fn write_artifact_preview_variant(
    source_image: &DynamicImage,
    image_path: &Path,
    target_width: u32,
    target_height: u32,
) -> Result<()> {
    let resized = cover_crop_resize_image(source_image, target_width, target_height)?;
    let mut encoded = Cursor::new(Vec::new());
    resized
        .write_to(&mut encoded, ImageFormat::Png)
        .context("failed to encode artifact preview cache image")?;

    let temp_path = image_path.with_extension("png.tmp");
    fs::write(temp_path.as_path(), encoded.into_inner()).with_context(|| {
        format!(
            "failed to write artifact preview cache file `{}`",
            temp_path.display()
        )
    })?;
    fs::rename(temp_path.as_path(), image_path).with_context(|| {
        format!(
            "failed to publish artifact preview cache file `{}`",
            image_path.display()
        )
    })?;
    Ok(())
}

fn cover_crop_resize_image(
    source_image: &DynamicImage,
    target_width: u32,
    target_height: u32,
) -> Result<DynamicImage> {
    if target_width == 0 || target_height == 0 {
        bail!("artifact preview target size must be non-zero");
    }

    let (source_width, source_height) = source_image.dimensions();
    if source_width == 0 || source_height == 0 {
        bail!("artifact thumbnail preview image has invalid dimensions");
    }

    let source_ratio = f64::from(source_width) / f64::from(source_height);
    let target_ratio = f64::from(target_width) / f64::from(target_height);
    let (crop_x, crop_y, crop_width, crop_height) = if source_ratio > target_ratio {
        let crop_width =
            ((f64::from(source_height) * target_ratio).round() as u32).clamp(1, source_width);
        (
            (source_width - crop_width) / 2,
            0,
            crop_width,
            source_height,
        )
    } else {
        let crop_height =
            ((f64::from(source_width) / target_ratio).round() as u32).clamp(1, source_height);
        (
            0,
            (source_height - crop_height) / 2,
            source_width,
            crop_height,
        )
    };

    Ok(source_image
        .crop_imm(crop_x, crop_y, crop_width, crop_height)
        .resize_exact(target_width, target_height, FilterType::Lanczos3))
}
