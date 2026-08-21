/// SHA-256 revision of the canonical Pioneer Agent avatar served by Gateway.
///
/// The revision is part of the authenticated immutable-storage contract shared
/// by Gateway and native clients. Changing the asset requires changing this
/// value in the same release.
pub const PIONEER_AGENT_AVATAR_REVISION: &str =
    "af2381c7a1e995929e5d1535db5753c97859e393d21e7c660cea5a5b1fbb3f2f";

/// SHA-256 revision of the canonical Codex CLI Agent avatar served by Gateway.
pub const CODEX_AGENT_AVATAR_REVISION: &str =
    "e43667b51ae7671a502ee4265e59e90ae2878558b3502521331500192a7807b8";

/// SHA-256 revision of the canonical Claude CLI Agent avatar served by Gateway.
pub const CLAUDE_AGENT_AVATAR_REVISION: &str =
    "84646b62063741db93e9f1bd8fd80520f8439d41a3e2e8c6a08b83469f8f16ff";
