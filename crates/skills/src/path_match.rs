fn normalize(input: &str) -> Vec<String> {
    input
        .replace('\\', "/")
        .split('/')
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .map(str::to_owned)
        .collect::<Vec<_>>()
}

fn segment_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if !pattern.contains('*') {
        return pattern == value;
    }

    let mut rest = value;
    let mut first = true;

    for part in pattern.split('*') {
        if part.is_empty() {
            continue;
        }

        if first && !pattern.starts_with('*') {
            if !rest.starts_with(part) {
                return false;
            }
            rest = &rest[part.len()..];
            first = false;
            continue;
        }

        match rest.find(part) {
            Some(index) => {
                rest = &rest[index + part.len()..];
            }
            None => return false,
        }

        first = false;
    }

    if !pattern.ends_with('*')
        && let Some(last) = pattern.split('*').next_back()
    {
        return value.ends_with(last);
    }

    true
}

fn matches_recursive(pattern: &[String], path: &[String]) -> bool {
    if pattern.is_empty() {
        return path.is_empty();
    }

    if pattern[0] == "**" {
        if pattern.len() == 1 {
            return true;
        }

        for consumed in 0..=path.len() {
            if matches_recursive(&pattern[1..], &path[consumed..]) {
                return true;
            }
        }
        return false;
    }

    if path.is_empty() {
        return false;
    }

    if !segment_matches(&pattern[0], &path[0]) {
        return false;
    }

    matches_recursive(&pattern[1..], &path[1..])
}

pub fn path_matches_pattern(pattern: &str, path: &str) -> bool {
    let normalized_pattern = normalize(pattern);
    let normalized_path = normalize(path);
    matches_recursive(normalized_pattern.as_slice(), normalized_path.as_slice())
}

pub fn path_matches_any_pattern<'a>(
    patterns: impl IntoIterator<Item = &'a str>,
    path: &str,
) -> bool {
    patterns
        .into_iter()
        .any(|pattern| path_matches_pattern(pattern, path))
}

#[cfg(test)]
mod tests {
    use super::{path_matches_any_pattern, path_matches_pattern};

    #[test]
    fn supports_literal_and_wildcard_segments() {
        assert!(path_matches_pattern("src/*.rs", "src/lib.rs"));
        assert!(!path_matches_pattern("src/*.rs", "src/lib.ts"));
    }

    #[test]
    fn supports_double_star() {
        assert!(path_matches_pattern("src/**/mod.rs", "src/a/b/mod.rs"));
        assert!(path_matches_pattern("**", "any/path/here"));
    }

    #[test]
    fn supports_multiple_patterns() {
        assert!(path_matches_any_pattern(["a/*", "b/**"], "b/c/d"));
        assert!(!path_matches_any_pattern(["a/*", "b/**"], "c/d"));
    }
}
