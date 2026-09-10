//! Existing native photo picker boundary shared by account and invitation editors.
pub struct ProfilePhotoSelection {
    pub preview: String,
    pub avatar: pioneer_client::settings::types::ProfileAvatarInput,
}
pub enum ProfilePhotoError {
    Picker,
    InvalidAvatar,
}
pub trait ProfilePhotoPort {
    fn select(
        &self,
        cx: &mut gpui_kit::App,
    ) -> gpui_kit::Task<Result<Option<ProfilePhotoSelection>, ProfilePhotoError>>;
}
