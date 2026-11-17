use pioneer_client::package_delivery::upload_flow::SkillUploadFailure;
pub struct PluginUploadText;
impl pioneer_desktop_foundation::UploadText for PluginUploadText {
    fn failure_message(failure: SkillUploadFailure) -> String {
        let key = match failure {
            SkillUploadFailure::SourceUnavailable => "plugins.upload_source_unavailable",
            SkillUploadFailure::SourceChanged => "plugins.upload_source_changed",
            SkillUploadFailure::SourceTooLarge => "plugins.upload_source_too_large",
            SkillUploadFailure::InvalidPackage => "plugins.upload_invalid_package",
            SkillUploadFailure::GatewayUnavailable => "plugins.upload_gateway_unavailable",
            SkillUploadFailure::AccessDenied => "plugins.upload_access_denied",
            SkillUploadFailure::Busy => "plugins.upload_busy",
            SkillUploadFailure::Unknown => "plugins.upload_unknown",
        };
        t!(key).to_string()
    }

    fn preparing() -> String {
        t!("plugins.preparing").to_string()
    }
    fn uploading() -> String {
        t!("plugins.uploading").to_string()
    }
    fn cancel() -> String {
        t!("plugins.cancel").to_string()
    }
}
