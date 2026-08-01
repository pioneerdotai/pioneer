use std::io::Cursor;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use image::{GenericImageView, ImageFormat, ImageReader};
use pioneer_protocol::{
    ClientInstallationDescriptor, InvitationAcceptParams, InvitationErrorReason, NewMemberProfile,
    PROFILE_AVATAR_MAX_DECODED_BYTES, PROFILE_AVATAR_MAX_DIMENSION, ProfileAvatarInput,
    ProfileAvatarMediaType,
};
use sha2::{Digest, Sha256};

use crate::auth::validate_installation_descriptor;

#[derive(Debug)]
pub(crate) struct ValidatedInvitationAccept {
    pub(crate) profile: ValidatedMemberProfile,
    pub(crate) installation: ClientInstallationDescriptor,
}

#[derive(Debug)]
pub(crate) struct ValidatedMemberProfile {
    pub(crate) display_name: String,
    pub(crate) nickname: String,
    pub(crate) nickname_key: String,
    pub(crate) avatar: Option<PreparedAvatar>,
}

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

pub(crate) fn validate_accept_inputs(
    params: InvitationAcceptParams,
) -> Result<ValidatedInvitationAccept, InvitationErrorReason> {
    let InvitationAcceptParams {
        profile,
        mut installation,
    } = params;
    let NewMemberProfile {
        display_name,
        nickname,
        avatar,
    } = profile;
    let normalized = NewMemberProfile::new(display_name, nickname, None)
        .map_err(|_| InvitationErrorReason::InvalidProfile)?;
    let avatar = avatar
        .map(prepare_avatar)
        .transpose()
        .map_err(|_| InvitationErrorReason::AvatarInvalid)?;
    validate_installation_descriptor(&mut installation)
        .map_err(|_| InvitationErrorReason::InvalidInstallation)?;

    Ok(ValidatedInvitationAccept {
        profile: ValidatedMemberProfile {
            nickname_key: normalized.nickname_key(),
            display_name: normalized.display_name,
            nickname: normalized.nickname,
            avatar,
        },
        installation,
    })
}

fn prepare_avatar(input: ProfileAvatarInput) -> Result<PreparedAvatar, ()> {
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
    use std::io::Cursor;

    use image::{DynamicImage, ImageFormat};
    use pioneer_protocol::{ClientKind, ProfileAvatarInput};

    use super::*;

    fn encoded_image(format: ImageFormat, width: u32, height: u32) -> String {
        let mut output = Cursor::new(Vec::new());
        DynamicImage::new_rgba8(width, height)
            .write_to(&mut output, format)
            .unwrap();
        STANDARD.encode(output.into_inner())
    }

    fn params(avatar: Option<ProfileAvatarInput>) -> InvitationAcceptParams {
        InvitationAcceptParams {
            profile: NewMemberProfile::new("  Александр  ", "Alex.Smith", avatar).unwrap(),
            installation: ClientInstallationDescriptor {
                installation_id: "  installation-1  ".to_owned(),
                display_name: "  Pioneer Mobile  ".to_owned(),
                client_kind: ClientKind::Mobile,
                platform: Some("  ios  ".to_owned()),
                client_version: Some("  1.0  ".to_owned()),
            },
        }
    }

    #[test]
    fn accepts_and_prepares_png_jpeg_and_webp_locally() {
        for (media_type, format) in [
            (ProfileAvatarMediaType::Png, ImageFormat::Png),
            (ProfileAvatarMediaType::Jpeg, ImageFormat::Jpeg),
            (ProfileAvatarMediaType::Webp, ImageFormat::WebP),
        ] {
            let input = ProfileAvatarInput::new(media_type, encoded_image(format, 2, 3)).unwrap();
            let validated = validate_accept_inputs(params(Some(input))).unwrap();
            assert_eq!(validated.profile.display_name, "Александр");
            assert_eq!(validated.profile.nickname, "Alex.Smith");
            assert_eq!(validated.profile.nickname_key, "alex.smith");
            assert_eq!(validated.installation.installation_id, "installation-1");
            let avatar = validated.profile.avatar.unwrap();
            assert_eq!(avatar.media_type, media_type);
            assert_eq!((avatar.width, avatar.height), (2, 3));
            assert_eq!(avatar.revision().len(), 64);
            let expected_hash: [u8; 32] = Sha256::digest(&avatar.content).into();
            assert_eq!(avatar.content_hash, expected_hash);
            let rendered = format!("{avatar:?}");
            assert!(rendered.contains("[redacted]"));
            assert!(!rendered.contains(&STANDARD.encode(&avatar.content)));
        }
    }

    #[test]
    fn rejects_mismatched_signature_invalid_base64_and_oversized_dimensions() {
        let mismatched = ProfileAvatarInput::new(
            ProfileAvatarMediaType::Jpeg,
            encoded_image(ImageFormat::Png, 1, 1),
        )
        .unwrap();
        assert_eq!(
            validate_accept_inputs(params(Some(mismatched))).unwrap_err(),
            InvitationErrorReason::AvatarInvalid
        );
        let invalid = ProfileAvatarInput::new(ProfileAvatarMediaType::Png, "not-base64").unwrap();
        assert_eq!(
            validate_accept_inputs(params(Some(invalid))).unwrap_err(),
            InvitationErrorReason::AvatarInvalid
        );
        let oversized = ProfileAvatarInput::new(
            ProfileAvatarMediaType::Png,
            encoded_image(ImageFormat::Png, PROFILE_AVATAR_MAX_DIMENSION + 1, 1),
        )
        .unwrap();
        assert_eq!(
            validate_accept_inputs(params(Some(oversized))).unwrap_err(),
            InvitationErrorReason::AvatarInvalid
        );
    }

    #[test]
    fn invalid_profile_or_installation_returns_only_corrective_reason() {
        let mut invalid_profile = params(None);
        invalid_profile.profile.display_name = "bad\nname".to_owned();
        assert_eq!(
            validate_accept_inputs(invalid_profile).unwrap_err(),
            InvitationErrorReason::InvalidProfile
        );

        let mut invalid_installation = params(None);
        invalid_installation.installation.installation_id = "\0".to_owned();
        assert_eq!(
            validate_accept_inputs(invalid_installation).unwrap_err(),
            InvitationErrorReason::InvalidInstallation
        );
    }
}
