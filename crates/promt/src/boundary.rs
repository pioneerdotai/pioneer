pub const PROMT_CACHE_BOUNDARY: &str = "\n<!-- PIONEER_PROMT_CACHE_BOUNDARY -->\n";

pub fn strip_prompt_cache_boundary(text: &str) -> String {
    text.replace(PROMT_CACHE_BOUNDARY, "\n")
}

pub fn split_prompt_cache_boundary(text: &str) -> Option<(String, String)> {
    let index = text.find(PROMT_CACHE_BOUNDARY)?;
    let stable = text[..index].trim_end().to_owned();
    let dynamic = text[index + PROMT_CACHE_BOUNDARY.len()..]
        .trim_start()
        .to_owned();
    Some((stable, dynamic))
}

#[cfg(test)]
mod tests {
    use super::{PROMT_CACHE_BOUNDARY, split_prompt_cache_boundary, strip_prompt_cache_boundary};

    #[test]
    fn split_works_when_boundary_exists() {
        let text = format!("A{}B", PROMT_CACHE_BOUNDARY);
        let (stable, dynamic) = split_prompt_cache_boundary(&text).expect("should split");
        assert_eq!(stable, "A");
        assert_eq!(dynamic, "B");
    }

    #[test]
    fn split_none_when_boundary_missing() {
        assert!(split_prompt_cache_boundary("A\nB").is_none());
    }

    #[test]
    fn strip_removes_marker() {
        let text = format!("A{}B", PROMT_CACHE_BOUNDARY);
        assert_eq!(strip_prompt_cache_boundary(&text), "A\nB");
    }
}
