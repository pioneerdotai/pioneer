use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn is_false(value: &bool) -> bool {
    !*value
}

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
    #[serde(default, skip_serializing_if = "is_false")]
    pub make_current: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceCreateResponse {
    pub workspace: Workspace,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSelectParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub make_current: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSelectResponse {
    pub workspace: Workspace,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceUpdateParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceUpdateResponse {
    pub workspace: Workspace,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkspaceDefaultParams {}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceDefaultResponse {
    pub workspace: Workspace,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceChangeKind {
    Created,
    Updated,
    CurrentChanged,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceChangedNotification {
    pub kind: WorkspaceChangeKind,
    pub workspace: Workspace,
}

#[cfg(test)]
mod tests {
    use super::{
        Workspace, WorkspaceChangeKind, WorkspaceChangedNotification, WorkspaceCreateParams,
        WorkspaceDefaultParams, WorkspaceDefaultResponse, WorkspaceListResponse,
        WorkspaceSelectParams, WorkspaceUpdateParams,
    };
    use serde_json::json;

    fn sample_workspace() -> Workspace {
        Workspace {
            id: "ws_000000000000000001".to_owned(),
            name: "Default Workspace".to_owned(),
            is_active: true,
            is_current: true,
            created_at: 1,
            updated_at: 2,
        }
    }

    #[test]
    fn workspace_create_params_require_workspace_id_and_skip_absent_name() {
        let encoded = serde_json::to_value(WorkspaceCreateParams {
            workspace_id: "ws_000000000000000001".to_owned(),
            name: None,
            make_current: false,
        })
        .expect("params encode");
        assert_eq!(encoded, json!({"workspace_id": "ws_000000000000000001"}));
    }

    #[test]
    fn workspace_create_params_default_make_current_to_false() {
        let params = serde_json::from_value::<WorkspaceCreateParams>(json!({
            "workspace_id": "ws_000000000000000001"
        }))
        .expect("params decode");
        assert!(!params.make_current);
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
    fn workspace_select_params_require_workspace_id_and_default_make_current() {
        let error = serde_json::from_value::<WorkspaceSelectParams>(json!({
            "make_current": true
        }))
        .expect_err("workspace_id is required");
        assert!(error.to_string().contains("workspace_id"));

        let params = serde_json::from_value::<WorkspaceSelectParams>(json!({
            "workspace_id": "ws_000000000000000001"
        }))
        .expect("params decode");
        assert!(!params.make_current);
    }

    #[test]
    fn workspace_update_params_require_workspace_id() {
        let error = serde_json::from_value::<WorkspaceUpdateParams>(json!({
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

    #[test]
    fn workspace_changed_notification_round_trips() {
        let notification = WorkspaceChangedNotification {
            kind: WorkspaceChangeKind::CurrentChanged,
            workspace: sample_workspace(),
        };

        let encoded = serde_json::to_value(&notification).expect("notification encode");
        assert_eq!(encoded["kind"], json!("current_changed"));

        let decoded: WorkspaceChangedNotification =
            serde_json::from_value(encoded).expect("notification decode");
        assert_eq!(decoded.kind, WorkspaceChangeKind::CurrentChanged);
        assert_eq!(decoded.workspace.id, "ws_000000000000000001");
    }
}
