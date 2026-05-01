#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BootstrapFileKind {
    Soul,
    Identity,
    User,
}

impl BootstrapFileKind {
    pub fn canonical_name(self) -> &'static str {
        match self {
            Self::Soul => "SOUL.md",
            Self::Identity => "IDENTITY.md",
            Self::User => "USER.md",
        }
    }
}

pub const CANONICAL_FILE_ORDER: [BootstrapFileKind; 3] = [
    BootstrapFileKind::Soul,
    BootstrapFileKind::Identity,
    BootstrapFileKind::User,
];
