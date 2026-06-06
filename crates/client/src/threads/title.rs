//! Thread title helpers.

use pioneer_protocol::Thread;

pub fn thread_display_title(thread: &Thread) -> Option<String> {
    thread_title_from_parts(thread.name.as_deref(), thread.preview.as_str())
}

pub fn thread_title_from_parts(name: Option<&str>, preview: &str) -> Option<String> {
    name.and_then(|name| {
        let name = name.trim();
        (!name.is_empty()).then(|| name.to_owned())
    })
    .or_else(|| fallback_title_from_first_user_text(preview))
}

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

    #[test]
    fn thread_title_from_parts_prefers_trimmed_name_then_preview() {
        assert_eq!(
            thread_title_from_parts(Some("  Named thread  "), "preview text"),
            Some("Named thread".to_owned())
        );
        assert_eq!(
            thread_title_from_parts(Some("   "), "one two three four five six seven"),
            Some("one two three four five six...".to_owned())
        );
        assert_eq!(thread_title_from_parts(None, " \n\t "), None);
    }
}
