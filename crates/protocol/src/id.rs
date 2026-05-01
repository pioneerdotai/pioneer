use nanoid::nanoid;

const ALPHANUMERIC: [char; 62] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '1', '2', '3', '4', '5',
    '6', '7', '8', '9', '0',
];

pub fn generate_id(len: usize) -> String {
    nanoid!(len, &ALPHANUMERIC)
}

#[cfg(test)]
mod tests {
    use super::generate_id;

    #[test]
    fn generate_id_produces_alphanumeric_with_requested_length() {
        let id = generate_id(21);
        assert_eq!(id.len(), 21);
        assert!(id.chars().all(|value| value.is_ascii_alphanumeric()));
    }
}
