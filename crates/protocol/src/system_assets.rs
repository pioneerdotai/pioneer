/// SHA-256 revision of the canonical Pioneer Agent avatar served by Gateway.
///
/// The revision is part of the authenticated immutable-storage contract shared
/// by Gateway and native clients. Changing the asset requires changing this
/// value in the same release.
pub const PIONEER_AGENT_AVATAR_REVISION: &str =
    "af2381c7a1e995929e5d1535db5753c97859e393d21e7c660cea5a5b1fbb3f2f";

/// SHA-256 revision of the canonical Codex CLI Agent avatar served by Gateway.
pub const CODEX_AGENT_AVATAR_REVISION: &str =
    "02112fc2b69eaccf74ee92f614450f9f82ca7f7d38326660f56748e410080dea";

/// SHA-256 revision of the canonical Claude CLI Agent avatar served by Gateway.
pub const CLAUDE_AGENT_AVATAR_REVISION: &str =
    "84646b62063741db93e9f1bd8fd80520f8439d41a3e2e8c6a08b83469f8f16ff";

/// SHA-256 revision of the OpenCode avatar for a light appearance.
pub const OPENCODE_AGENT_AVATAR_LIGHT_REVISION: &str =
    "bffac789518813b5862f9d0f4c239c4fdb6dd8c4027911a1d9e0c82211aa06a1";

/// SHA-256 revision of the OpenCode avatar for a dark appearance.
pub const OPENCODE_AGENT_AVATAR_DARK_REVISION: &str =
    "6fbba60dcebe87c2e0f6ff0e3634fdb918c892e9c989f0d72cff6bb806f3ef4b";
