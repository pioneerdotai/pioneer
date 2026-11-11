pub(crate) use crate::images::PreparedImage as PreparedAvatar;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use pioneer_protocol::ProfileAvatarInput;

pub(crate) fn prepare_avatar(input: ProfileAvatarInput) -> Result<PreparedAvatar, ()> {
    let content = STANDARD
        .decode(input.content_base64.as_bytes())
        .map_err(|_| ())?;
    crate::images::prepare_image(input.media_type, content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{DynamicImage, ImageFormat};
    use pioneer_protocol::ProfileAvatarMediaType;
    use std::io::Cursor;

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
