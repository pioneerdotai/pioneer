use std::io::Cursor;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use image::{GenericImageView, ImageFormat, ImageReader};
use pioneer_protocol::{
    PROFILE_AVATAR_MAX_DECODED_BYTES, PROFILE_AVATAR_MAX_DIMENSION, ProfileAvatarInput,
    ProfileAvatarMediaType,
};
use sha2::{Digest, Sha256};

pub(crate) struct PreparedAvatar {
    pub(crate) media_type: ProfileAvatarMediaType,
    pub(crate) content: Vec<u8>,
    pub(crate) content_hash: [u8; 32],
    pub(crate) width: u32,
    pub(crate) height: u32,
}

impl PreparedAvatar {
    pub(crate) fn revision(&self) -> String {
        hex::encode(self.content_hash)
    }
}

impl std::fmt::Debug for PreparedAvatar {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedAvatar")
            .field("media_type", &self.media_type)
            .field("content", &"[redacted]")
            .field("content_hash", &"[redacted]")
            .field("width", &self.width)
            .field("height", &self.height)
            .finish()
    }
}

pub(crate) fn prepare_avatar(input: ProfileAvatarInput) -> Result<PreparedAvatar, ()> {
    let content = STANDARD
        .decode(input.content_base64.as_bytes())
        .map_err(|_| ())?;
    if content.is_empty() || content.len() > PROFILE_AVATAR_MAX_DECODED_BYTES {
        return Err(());
    }
    let format = image_format(input.media_type);
    if image::guess_format(content.as_slice()).map_err(|_| ())? != format {
        return Err(());
    }
    let (width, height) = ImageReader::with_format(Cursor::new(content.as_slice()), format)
        .into_dimensions()
        .map_err(|_| ())?;
    if width == 0
        || height == 0
        || width > PROFILE_AVATAR_MAX_DIMENSION
        || height > PROFILE_AVATAR_MAX_DIMENSION
    {
        return Err(());
    }
    let decoded =
        image::load_from_memory_with_format(content.as_slice(), format).map_err(|_| ())?;
    if decoded.dimensions() != (width, height) {
        return Err(());
    }
    let content_hash = Sha256::digest(content.as_slice()).into();
    Ok(PreparedAvatar {
        media_type: input.media_type,
        content,
        content_hash,
        width,
        height,
    })
}

const fn image_format(media_type: ProfileAvatarMediaType) -> ImageFormat {
    match media_type {
        ProfileAvatarMediaType::Png => ImageFormat::Png,
        ProfileAvatarMediaType::Jpeg => ImageFormat::Jpeg,
        ProfileAvatarMediaType::Webp => ImageFormat::WebP,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::DynamicImage;

    fn encoded_image(format: ImageFormat, width: u32, height: u32) -> String {
        let mut output = Cursor::new(Vec::new());
        DynamicImage::new_rgba8(width, height)
            .write_to(&mut output, format)
            .unwrap();
        STANDARD.encode(output.into_inner())
    }

    #[test]
    fn avatar_preparation_is_media_bounded_and_redacts_content() {
        for (media_type, format) in [
            (ProfileAvatarMediaType::Png, ImageFormat::Png),
            (ProfileAvatarMediaType::Jpeg, ImageFormat::Jpeg),
            (ProfileAvatarMediaType::Webp, ImageFormat::WebP),
        ] {
            let prepared = prepare_avatar(
                ProfileAvatarInput::new(media_type, encoded_image(format, 2, 3)).unwrap(),
            )
            .unwrap();
            assert_eq!(prepared.media_type, media_type);
            assert_eq!((prepared.width, prepared.height), (2, 3));
            assert_eq!(prepared.revision().len(), 64);
            assert!(format!("{prepared:?}").contains("[redacted]"));
        }
    }
}
