use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use gpui_kit::{App, AppContext};
use pioneer_desktop_foundation::profile_photo::*;
use pioneer_protocol::{
    PROFILE_AVATAR_MAX_DECODED_BYTES, PROFILE_AVATAR_MAX_DIMENSION, ProfileAvatarInput,
    ProfileAvatarMediaType,
};
pub(crate) struct DesktopProfilePhotoPort;
impl ProfilePhotoPort for DesktopProfilePhotoPort {
    fn select(
        &self,
        cx: &mut App,
    ) -> gpui_kit::Task<Result<Option<ProfilePhotoSelection>, ProfilePhotoError>> {
        let selection = cx.prompt_for_paths(gpui_kit::PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: None,
        });
        cx.spawn(async move |cx| {
            let paths = selection
                .await
                .map_err(|_| ProfilePhotoError::Picker)?
                .map_err(|_| ProfilePhotoError::Picker)?;
            let Some(path) = paths.and_then(|paths| paths.into_iter().next()) else {
                return Ok(None);
            };
            let preview = path.to_string_lossy().into_owned();
            let avatar = cx
                .background_spawn(async move { load_desktop_profile_avatar(&path) })
                .await
                .map_err(|_| ProfilePhotoError::InvalidAvatar)?;
            Ok(Some(ProfilePhotoSelection { preview, avatar }))
        })
    }
}
pub(crate) fn load_desktop_profile_avatar(
    path: &std::path::Path,
) -> anyhow::Result<ProfileAvatarInput> {
    let bytes = std::fs::read(path)?;
    if bytes.is_empty() || bytes.len() > PROFILE_AVATAR_MAX_DECODED_BYTES {
        anyhow::bail!("invalid profile avatar size");
    }
    let format = image::guess_format(bytes.as_slice())?;
    let media_type = match format {
        image::ImageFormat::Png => ProfileAvatarMediaType::Png,
        image::ImageFormat::Jpeg => ProfileAvatarMediaType::Jpeg,
        image::ImageFormat::WebP => ProfileAvatarMediaType::Webp,
        _ => anyhow::bail!("unsupported profile avatar format"),
    };
    let decoded = image::load_from_memory_with_format(bytes.as_slice(), format)?;
    if decoded.width() > PROFILE_AVATAR_MAX_DIMENSION
        || decoded.height() > PROFILE_AVATAR_MAX_DIMENSION
    {
        anyhow::bail!("invalid profile avatar dimensions");
    }
    ProfileAvatarInput::new(media_type, BASE64_STANDARD.encode(bytes)).map_err(anyhow::Error::new)
}
