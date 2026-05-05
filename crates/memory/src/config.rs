use pioneer_protocol::MemorySensitivity;

#[derive(Debug, Clone)]
pub struct MemoryServiceConfig {
    pub default_limit: u32,
    pub max_limit: u32,
    pub max_content_bytes: usize,
    pub content_preview_chars: usize,
    pub policy_version: String,
    pub default_read_policy: MemoryReadPolicy,
}

impl Default for MemoryServiceConfig {
    fn default() -> Self {
        Self {
            default_limit: 20,
            max_limit: 100,
            max_content_bytes: 64 * 1024,
            content_preview_chars: 240,
            policy_version: "memory_policy_v1".to_owned(),
            default_read_policy: MemoryReadPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryReadPolicy {
    pub allow_normal: bool,
    pub allow_personal: bool,
    pub allow_secret_like: bool,
    pub allow_regulated: bool,
}

impl Default for MemoryReadPolicy {
    fn default() -> Self {
        Self {
            allow_normal: true,
            allow_personal: true,
            allow_secret_like: false,
            allow_regulated: false,
        }
    }
}

impl MemoryReadPolicy {
    pub fn allow_all() -> Self {
        Self {
            allow_normal: true,
            allow_personal: true,
            allow_secret_like: true,
            allow_regulated: true,
        }
    }

    pub fn allows(&self, sensitivity: MemorySensitivity) -> bool {
        match sensitivity {
            MemorySensitivity::Normal => self.allow_normal,
            MemorySensitivity::Personal => self.allow_personal,
            MemorySensitivity::SecretLike => self.allow_secret_like,
            MemorySensitivity::Regulated => self.allow_regulated,
        }
    }
}
