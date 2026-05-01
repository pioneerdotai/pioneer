use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    pub is_active: bool,
    pub is_current: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkspaceListParams {}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceListResponse {
    #[serde(default)]
    pub workspaces: Vec<Workspace>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceCreateParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceCreateResponse {
    pub workspace: Workspace,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkspaceDefaultParams {}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceDefaultResponse {
    pub workspace: Workspace,
}

#[cfg(test)]
mod tests {
    use super::{
        WorkspaceCreateParams, WorkspaceDefaultParams, WorkspaceDefaultResponse,
        WorkspaceListResponse,
    };
    use serde_json::json;

    #[test]
    fn workspace_create_params_require_workspace_id_and_skip_absent_name() {
        let encoded = serde_json::to_value(WorkspaceCreateParams {
            workspace_id: "ws_000000000000000001".to_owned(),
            name: None,
        })
        .expect("params encode");
        assert_eq!(encoded, json!({"workspace_id": "ws_000000000000000001"}));
    }

    #[test]
    fn workspace_create_params_missing_workspace_id_fails() {
        let error = serde_json::from_value::<WorkspaceCreateParams>(json!({
            "name": "Sandbox"
        }))
        .expect_err("workspace_id is required");
        assert!(error.to_string().contains("workspace_id"));
    }

    #[test]
    fn workspace_list_response_preserves_items() {
        let value = json!({
            "workspaces": [
                {
                    "id": "ws_000000000000000001",
                    "name": "Default Workspace",
                    "is_active": true,
                    "is_current": true,
                    "created_at": 1,
                    "updated_at": 2
                }
            ]
        });

        let response: WorkspaceListResponse =
            serde_json::from_value(value).expect("response decode");
        assert_eq!(response.workspaces.len(), 1);
        assert_eq!(response.workspaces[0].id, "ws_000000000000000001");
    }

    #[test]
    fn workspace_default_params_encode_as_empty_object() {
        let encoded =
            serde_json::to_value(WorkspaceDefaultParams::default()).expect("params encode");
        assert_eq!(encoded, json!({}));
    }

    #[test]
    fn workspace_default_response_preserves_workspace() {
        let value = json!({
            "workspace": {
                "id": "ws_000000000000000001",
                "name": "Default Workspace",
                "is_active": true,
                "is_current": true,
                "created_at": 1,
                "updated_at": 2
            }
        });

        let response: WorkspaceDefaultResponse =
            serde_json::from_value(value).expect("response decode");
        assert_eq!(response.workspace.id, "ws_000000000000000001");
        assert!(response.workspace.is_active);
        assert!(response.workspace.is_current);
    }
}
