use std::{collections::BTreeSet, path::Path};

fn collect_keys(directory: &Path, keys: &mut BTreeSet<String>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_keys(&path, keys);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let source = std::fs::read_to_string(path).unwrap();
            for (offset, _) in source.match_indices(concat!("t", "!(")) {
                if offset > 0
                    && (source.as_bytes()[offset - 1].is_ascii_alphanumeric()
                        || source.as_bytes()[offset - 1] == b'_')
                {
                    continue;
                }
                if let Some(literal) = source[offset + 3..].trim_start().strip_prefix('"') {
                    keys.insert(literal.split('"').next().unwrap().to_owned());
                }
            }
        }
    }
}

#[test]
fn every_thread_translation_is_in_each_compiled_locale_without_fallback() {
    let mut keys = BTreeSet::new();
    collect_keys(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut keys,
    );
    assert!(keys.len() > 300, "scan must cover the whole thread feature");
    let mut missing = Vec::new();
    for locale in ["en", "ru", "de", "es", "fr", "hi", "jp", "zh"] {
        for key in &keys {
            if crate::_rust_i18n_backend().translate(locale, key).is_none() {
                missing.push(format!("{locale}: {key}"));
            }
        }
    }
    assert!(missing.is_empty(), "missing translations: {missing:#?}");
}
