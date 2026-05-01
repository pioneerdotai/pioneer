use crate::attachments::errors::AttachmentPipelineError;
use crate::attachments::types::AttachmentNormalizationPolicy;
use crate::types::InputContentType;
use anyhow::Result;
use mime_guess::get_mime_extensions_str;
use sha2::{Digest, Sha256};
use std::path::Path;

pub fn normalize_mime(raw: &str) -> Result<String> {
    let mime = raw.trim().to_ascii_lowercase();
    if mime.is_empty() || !mime.contains('/') || mime.contains(' ') || mime.contains('\t') {
        return Err(AttachmentPipelineError::invalid_mime(raw).into());
    }
    Ok(mime)
}

pub fn reconcile_mime(
    declared_mime: &str,
    bytes: Option<&[u8]>,
    policy: &AttachmentNormalizationPolicy,
) -> Result<String> {
    let declared = normalize_mime(declared_mime)?;
    let Some(data) = bytes else {
        return Ok(declared);
    };

    let Some(sniffed) = sniff_mime_from_bytes(data) else {
        return Ok(declared);
    };

    if sniffed == declared {
        return Ok(declared);
    }

    let declared_top = declared.split('/').next().unwrap_or_default();
    let sniffed_top = sniffed.split('/').next().unwrap_or_default();
    if declared_top == sniffed_top {
        return Ok(declared);
    }

    if policy.strict_mime_match {
        return Err(AttachmentPipelineError::mime_mismatch(declared.as_str(), sniffed).into());
    }

    Ok(sniffed.to_owned())
}

pub fn normalize_attachment_name(
    provided: Option<&str>,
    source_name: Option<&str>,
    kind: InputContentType,
    mime: &str,
    policy: &AttachmentNormalizationPolicy,
) -> Result<String> {
    let candidate = provided
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| source_name.map(str::trim).filter(|value| !value.is_empty()))
        .map(str::to_owned)
        .unwrap_or_else(|| default_name_for_kind(kind, mime));

    let mut sanitized = candidate
        .chars()
        .filter(|ch| !ch.is_control())
        .map(|ch| match ch {
            '/' | '\\' | ':' | '\0' => '_',
            _ => ch,
        })
        .collect::<String>()
        .trim()
        .to_owned();

    if sanitized.is_empty() {
        sanitized = default_name_for_kind(kind, mime);
    }

    if sanitized.chars().count() > policy.max_filename_chars {
        sanitized = sanitized.chars().take(policy.max_filename_chars).collect();
    }

    // Ensure extension exists and matches the selected MIME.
    let extension = extension_for_mime(mime, kind);
    if Path::new(sanitized.as_str())
        .extension()
        .and_then(|value| value.to_str())
        .is_none()
    {
        sanitized.push('.');
        sanitized.push_str(extension);
    }

    if sanitized == "." {
        return Err(AttachmentPipelineError::invalid_file_name(candidate.as_str()).into());
    }

    Ok(sanitized)
}

pub fn estimate_decoded_base64_size(base64_data: &str) -> usize {
    let mut trimmed_len = 0usize;
    let mut padding = 0usize;
    for ch in base64_data.chars() {
        if ch.is_ascii_whitespace() {
            continue;
        }
        trimmed_len += 1;
        if ch == '=' {
            padding += 1;
        }
    }

    let quartets = trimmed_len / 4;
    quartets.saturating_mul(3).saturating_sub(padding.min(2))
}

pub fn default_name_for_kind(kind: InputContentType, mime: &str) -> String {
    let extension = extension_for_mime(mime, kind);
    let stem = fallback_stem_for_kind(kind);
    format!("{stem}.{extension}")
}

pub fn infer_mime_from_reference(reference: &str, kind: InputContentType) -> String {
    let without_query = reference
        .split_once('?')
        .map_or(reference, |(value, _)| value);
    let clean = without_query
        .split_once('#')
        .map_or(without_query, |(value, _)| value);

    if let Some(guessed) = mime_guess::from_path(clean).first_raw() {
        if let Ok(normalized) = normalize_mime(guessed) {
            return normalized;
        }
    }

    fallback_mime_for_kind(kind).to_owned()
}

pub fn extension_for_mime(mime: &str, kind: InputContentType) -> &'static str {
    // Prioritize stable common mappings to avoid provider-side ambiguity.
    match mime {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/webp" => "webp",
        "image/gif" => "gif",
        "application/pdf" => "pdf",
        "audio/wav" | "audio/x-wav" => "wav",
        "audio/mpeg" | "audio/mp3" => "mp3",
        "video/mp4" => "mp4",
        "text/plain" => "txt",
        _ => get_mime_extensions_str(mime)
            .and_then(|extensions| extensions.first().copied())
            .unwrap_or_else(|| fallback_extension_for_kind(kind)),
    }
}

fn fallback_extension_for_kind(kind: InputContentType) -> &'static str {
    match kind {
        InputContentType::Text => "txt",
        InputContentType::File => "bin",
        InputContentType::Image => "img",
        InputContentType::Audio => "aud",
        InputContentType::Video => "vid",
    }
}

fn fallback_stem_for_kind(kind: InputContentType) -> &'static str {
    match kind {
        InputContentType::Text => "text",
        InputContentType::File => "file",
        InputContentType::Image => "image",
        InputContentType::Audio => "audio",
        InputContentType::Video => "video",
    }
}

fn fallback_mime_for_kind(kind: InputContentType) -> &'static str {
    match kind {
        InputContentType::Text => "text/plain",
        InputContentType::File => "application/octet-stream",
        InputContentType::Image => "image/*",
        InputContentType::Audio => "audio/*",
        InputContentType::Video => "video/*",
    }
}

pub fn sniff_mime_from_bytes(bytes: &[u8]) -> Option<&'static str> {
    if bytes.len() >= 8 && bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return Some("image/png");
    }
    if bytes.len() >= 3 && bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some("image/jpeg");
    }
    if bytes.len() >= 6 && (bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a")) {
        return Some("image/gif");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    if bytes.len() >= 4 && bytes.starts_with(b"%PDF") {
        return Some("application/pdf");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WAVE" {
        return Some("audio/wav");
    }
    if bytes.len() >= 3 && bytes.starts_with(b"ID3") {
        return Some("audio/mpeg");
    }
    if bytes.len() >= 12
        && &bytes[4..8] == b"ftyp"
        && (bytes[8..12].starts_with(b"mp4")
            || bytes[8..12].starts_with(b"isom")
            || bytes[8..12].starts_with(b"iso2"))
    {
        return Some("video/mp4");
    }
    None
}

pub fn hash_bytes(value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    format!("{:x}", hasher.finalize())
}

pub fn hash_string(value: &str) -> String {
    hash_bytes(value.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_mime_mismatch_is_rejected() {
        let policy = AttachmentNormalizationPolicy {
            strict_mime_match: true,
            ..AttachmentNormalizationPolicy::default()
        };
        let png = [0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
        let err = reconcile_mime("text/plain", Some(&png), &policy)
            .expect_err("mismatch must fail in strict mode");
        assert!(err.to_string().contains("MIME_MISMATCH"));
    }

    #[test]
    fn filename_is_sanitized() {
        let normalized = normalize_attachment_name(
            Some("../bad:name"),
            None,
            InputContentType::File,
            "application/pdf",
            &AttachmentNormalizationPolicy::default(),
        )
        .expect("normalize filename");
        assert!(!normalized.contains('/'));
        assert!(!normalized.contains(':'));
    }

    #[test]
    fn base64_size_estimation_handles_padding() {
        assert_eq!(estimate_decoded_base64_size("TQ=="), 1);
        assert_eq!(estimate_decoded_base64_size("TWE="), 2);
        assert_eq!(estimate_decoded_base64_size("TWFu"), 3);
    }

    #[test]
    fn infer_mime_from_reference_handles_query_fragment() {
        let mime = infer_mime_from_reference(
            "https://example.com/image/photo.png?x=1#top",
            InputContentType::Image,
        );
        assert_eq!(mime, "image/png");
    }

    #[test]
    fn infer_mime_from_reference_falls_back_by_kind() {
        let mime = infer_mime_from_reference("/tmp/unknown.custom", InputContentType::File);
        assert_eq!(mime, "application/octet-stream");
    }
}
