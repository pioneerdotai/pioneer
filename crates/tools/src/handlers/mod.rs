#[cfg(feature = "computer-use")]
mod computer_use;

pub(crate) mod apply_patch;
mod files;
mod http;
mod mcp;
mod request_tools;
mod shell;
mod skill;
mod web;

#[cfg(feature = "computer-use")]
pub use computer_use::{ComputerUseHandler, materialize_computer_use_domain_bundle};

pub use apply_patch::ApplyPatchHandler;
pub(crate) use files::FileObservationStore;
pub use files::{EditFileHandler, GrepHandler, ListDirHandler, ReadFileHandler, WriteFileHandler};
pub use mcp::{
    ExcludedMcpRuntimeTool, McpDynamicToolAnnotations, McpDynamicToolBinding,
    McpDynamicToolDescriptor, McpRuntimeToolMaterialization, McpToolCallOutput, McpToolCallRequest,
    McpToolExecutor, materialize_mcp_runtime_tools,
};
pub use request_tools::{RequestToolsDomainDiagnostic, RequestToolsHandler, RequestToolsResult};
pub use shell::UnifiedExecHandler;
pub use skill::{
    ExcludedSkillRuntimeTool, SkillDynamicToolDescriptor, SkillDynamicToolKind,
    SkillReadToolConfig, SkillReadToolEntry, SkillRuntimeToolMaterialization,
    SkillRuntimeToolPolicyDiagnostic, materialize_skill_runtime_tools,
};
pub use web::{DownloadUrlHandler, WebFetchHandler, WebSearchHandler};
