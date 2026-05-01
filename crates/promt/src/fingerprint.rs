use sha2::{Digest, Sha256};

pub fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::sha256_hex;

    #[test]
    fn stable_hash_for_same_input() {
        let one = sha256_hex("abc");
        let two = sha256_hex("abc");
        assert_eq!(one, two);
    }

    #[test]
    fn different_hash_for_different_input() {
        let one = sha256_hex("abc");
        let two = sha256_hex("abd");
        assert_ne!(one, two);
    }
}
