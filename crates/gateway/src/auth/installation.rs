use pioneer_protocol::ClientInstallationDescriptor;

use super::{AuthError, AuthErrorCode};

const MAX_INSTALLATION_TEXT: usize = 255;
const MAX_DIAGNOSTIC_TEXT: usize = 255;

pub(crate) fn validate_installation_descriptor(
    installation: &mut ClientInstallationDescriptor,
) -> Result<(), AuthError> {
    installation.installation_id =
        bounded_trimmed(installation.installation_id.as_str(), MAX_INSTALLATION_TEXT)?;
    installation.display_name =
        bounded_trimmed(installation.display_name.as_str(), MAX_INSTALLATION_TEXT)?;
    installation.platform = bounded_optional(installation.platform.take(), MAX_DIAGNOSTIC_TEXT)?;
    installation.client_version =
        bounded_optional(installation.client_version.take(), MAX_DIAGNOSTIC_TEXT)?;
    Ok(())
}

pub(super) fn bounded_trimmed(value: &str, max_chars: usize) -> Result<String, AuthError> {
    let trimmed = value.trim();
    let count = trimmed.chars().count();
    if count == 0 || count > max_chars || trimmed.chars().any(char::is_control) {
        return Err(AuthError::new(AuthErrorCode::MalformedCredential));
    }
    Ok(trimmed.to_owned())
}

pub(super) fn bounded_optional(
    value: Option<String>,
    max_chars: usize,
) -> Result<Option<String>, AuthError> {
    value
        .map(|value| bounded_trimmed(value.as_str(), max_chars))
        .transpose()
}
