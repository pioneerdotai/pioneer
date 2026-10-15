//! Repeated indistinguishable domain occurrences retain their own control namespace.
use std::collections::HashMap;
pub(crate) fn occurrences(keys: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashMap::<String, usize>::new();
    keys.into_iter()
        .map(|key| {
            let n = seen.entry(key.clone()).or_default();
            let id = format!("{key}:occurrence:{n}");
            *n += 1;
            id
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn insertion_and_reordering_preserve_domain_identity() {
        assert_eq!(
            occurrences(["b".into(), "a".into(), "a".into()]),
            vec!["b:occurrence:0", "a:occurrence:0", "a:occurrence:1"]
        );
    }
}
