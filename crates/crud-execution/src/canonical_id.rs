use sha2::{Digest, Sha256};

pub fn canonical_agent_id(prefix: char, key: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(key.as_bytes());
    format!("{prefix}{}", &hex::encode(digest.finalize())[..20])
}
