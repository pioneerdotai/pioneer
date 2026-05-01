pub(crate) fn fallback_title_from_first_user_text(user_text: &str) -> Option<String> {
    let words = user_text.split_whitespace().collect::<Vec<_>>();
    if words.is_empty() {
        return None;
    }

    if words.len() > 6 {
        return Some(format!("{}...", words[..6].join(" ")));
    }

    Some(words.join(" "))
}
