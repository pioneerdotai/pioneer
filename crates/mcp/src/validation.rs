pub fn is_valid_server_name(value: &str) -> bool {
    let len = value.chars().count();
    (1..=64).contains(&len)
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}
