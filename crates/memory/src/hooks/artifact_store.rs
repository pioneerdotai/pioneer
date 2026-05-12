use super::*;

pub trait MemoryToolBundleArtifactStore: Send + Sync {
    fn insert_tool_bundle_artifact(
        &self,
        turn_id: &str,
        bundle_id: HookToolBundleId,
        bundle: ToolExtensionBundle,
    );
}
