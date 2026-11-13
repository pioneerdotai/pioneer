//! Common bounded image validation for native storage resources.
use image::{GenericImageView, ImageFormat, ImageReader};
use pioneer_protocol::{IMAGE_MAX_BYTES, IMAGE_MAX_DIMENSION, ImageMediaType};
use sha2::{Digest, Sha256};
use std::io::Cursor;

pub(crate) struct PreparedImage {
    pub(crate) media_type: ImageMediaType,
    pub(crate) content: Vec<u8>,
    pub(crate) content_hash: [u8; 32],
    pub(crate) width: u32,
    pub(crate) height: u32,
}

impl PreparedImage {
    pub(crate) fn revision(&self) -> String {
        hex::encode(self.content_hash)
    }
}

impl std::fmt::Debug for PreparedImage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedImage")
            .field("media_type", &self.media_type)
            .field("content", &"[redacted]")
            .field("content_hash", &"[redacted]")
            .field("width", &self.width)
            .field("height", &self.height)
            .finish()
    }
}

/// Common bounded image validation for uploaded avatars and package artwork.
pub(crate) fn prepare_image(
    media_type: ImageMediaType,
    content: Vec<u8>,
) -> Result<PreparedImage, ()> {
    if content.is_empty() || content.len() > IMAGE_MAX_BYTES {
        return Err(());
    }
    let format = image_format(media_type);
    if image::guess_format(content.as_slice()).map_err(|_| ())? != format {
        return Err(());
    }
    let (width, height) = ImageReader::with_format(Cursor::new(content.as_slice()), format)
        .into_dimensions()
        .map_err(|_| ())?;
    if width == 0 || height == 0 || width > IMAGE_MAX_DIMENSION || height > IMAGE_MAX_DIMENSION {
        return Err(());
    }
    let decoded =
        image::load_from_memory_with_format(content.as_slice(), format).map_err(|_| ())?;
    if decoded.dimensions() != (width, height) {
        return Err(());
    }
    let content_hash = Sha256::digest(content.as_slice()).into();
    Ok(PreparedImage {
        media_type,
        content,
        content_hash,
        width,
        height,
    })
}

const fn image_format(media_type: ImageMediaType) -> ImageFormat {
    match media_type {
        ImageMediaType::Png => ImageFormat::Png,
        ImageMediaType::Jpeg => ImageFormat::Jpeg,
        ImageMediaType::Webp => ImageFormat::WebP,
    }
}
