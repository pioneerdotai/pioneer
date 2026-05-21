use crate::spec::ToolSpec;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct ToolVisibilitySnapshot {
    all_specs: Arc<Vec<ToolSpec>>,
    visible_specs: Arc<RwLock<Vec<ToolSpec>>>,
}

impl ToolVisibilitySnapshot {
    pub fn new(all_specs: Vec<ToolSpec>) -> Self {
        Self {
            all_specs: Arc::new(all_specs.clone()),
            visible_specs: Arc::new(RwLock::new(all_specs)),
        }
    }

    pub async fn get(&self) -> Vec<ToolSpec> {
        self.visible_specs.read().await.clone()
    }

    pub async fn contains_name(&self, name: &str) -> bool {
        self.visible_specs
            .read()
            .await
            .iter()
            .any(|spec| spec.name == name)
    }

    pub async fn replace(&self, specs: Vec<ToolSpec>) {
        *self.visible_specs.write().await = specs;
    }

    pub async fn set_visible_by_name(&self, names: &[String]) {
        let set: HashSet<&str> = names.iter().map(String::as_str).collect();
        let selected = self
            .all_specs
            .iter()
            .filter(|spec| set.contains(spec.name.as_str()))
            .cloned()
            .collect();
        self.replace(selected).await;
    }

    pub fn all_specs(&self) -> &[ToolSpec] {
        self.all_specs.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::PayloadKind;

    fn spec(name: &str) -> ToolSpec {
        ToolSpec::new(
            name,
            "test tool",
            serde_json::json!({"type": "object"}),
            PayloadKind::Function,
        )
    }

    #[tokio::test]
    async fn visibility_snapshot_contains_only_currently_visible_names() {
        let snapshot = ToolVisibilitySnapshot::new(vec![spec("tool_a"), spec("tool_b")]);

        snapshot.set_visible_by_name(&["tool_a".to_owned()]).await;

        assert!(snapshot.contains_name("tool_a").await);
        assert!(!snapshot.contains_name("tool_b").await);
        assert!(!snapshot.contains_name("unknown_tool").await);
    }
}
