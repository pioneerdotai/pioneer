//! Thread title helpers.

pub fn fallback_title_from_first_user_text(user_text: &str) -> Option<String> {
    let words = user_text.split_whitespace().collect::<Vec<_>>();
    if words.is_empty() {
        return None;
    }

    if words.len() > 6 {
        return Some(format!("{}...", words[..6].join(" ")));
    }

    Some(words.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_title_rejects_empty_preview() {
        assert_eq!(fallback_title_from_first_user_text(" \n\t "), None);
    }

    #[test]
    fn fallback_title_uses_all_words_when_short() {
        assert_eq!(
            fallback_title_from_first_user_text("one two three"),
            Some("one two three".to_owned())
        );
    }

    #[test]
    fn fallback_title_truncates_to_six_words() {
        assert_eq!(
            fallback_title_from_first_user_text("one two three four five six seven"),
            Some("one two three four five six...".to_owned())
        );
    }
}
