//! Localized compatibility adapter. Both native Skills and Plugins currently
//! consume the same retained package delivery view in desktop-foundation.
use pioneer_client::package_delivery::upload_flow::SkillUploadFailure;
pub struct SkillsUploadText;
impl pioneer_desktop_foundation::UploadText for SkillsUploadText {
    fn failure_message(failure: SkillUploadFailure) -> String {
        let key = match failure {
            SkillUploadFailure::SourceUnavailable => "skills.error.upload_source_unavailable",
            SkillUploadFailure::SourceChanged => "skills.error.upload_source_changed",
            SkillUploadFailure::SourceTooLarge => "skills.error.upload_source_too_large",
            SkillUploadFailure::InvalidPackage => "skills.error.upload_invalid_package",
            SkillUploadFailure::GatewayUnavailable => "skills.error.upload_gateway_unavailable",
            SkillUploadFailure::AccessDenied => "skills.error.upload_access_denied",
            SkillUploadFailure::Busy => "skills.error.upload_busy",
            SkillUploadFailure::Unknown => "skills.error.upload_unknown",
        };
        t!(key).to_string()
    }

    fn preparing() -> String {
        t!("skills.upload.preparing").to_string()
    }
    fn uploading() -> String {
        t!("skills.upload.uploading").to_string()
    }
    fn cancel() -> String {
        t!("buttons.cancel").to_string()
    }
}
pub type UploadView = pioneer_desktop_foundation::PackageUploadView<SkillsUploadText>;
pub use pioneer_desktop_foundation::UploadVisibility;
