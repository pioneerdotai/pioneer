use super::{CanonicalLanguage, CodeThemeId, HIGHLIGHT_ENGINE_REVISION, HighlightKey};
use sha2::{Digest, Sha256};
pub(crate) fn make_highlight_key(
    source: &str,
    language: CanonicalLanguage,
    theme: CodeThemeId,
) -> HighlightKey {
    HighlightKey {
        source_sha256: Sha256::digest(source.as_bytes()).into(),
        canonical_language: language.cache_name().to_owned(),
        theme,
        engine_revision: HIGHLIGHT_ENGINE_REVISION,
    }
}
