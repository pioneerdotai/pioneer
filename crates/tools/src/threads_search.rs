//! Model-controlled search arguments. Authority and retrieval policy are server-owned.
use crate::{ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolError, ToolSpec};
use serde::{Deserialize, Serialize};

pub const THREADS_SEARCH_TOOL_NAME: &str = "threads_search";
pub const THREADS_SEARCH_MAX_LIMIT: u32 = 20;
pub const THREADS_SEARCH_MAX_QUERY_CHARS: usize = 4_096;
pub const THREADS_SEARCH_MAX_CURSOR_CHARS: usize = 65_536;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadsSearchParams {
    pub query: String,
    pub limit: u32,
    #[serde(default)]
    pub cursor: Option<String>,
}

impl ThreadsSearchParams {
    pub fn validate(&self) -> Result<(), ToolError> {
        if self.query.trim().is_empty()
            || self.query.chars().count() > THREADS_SEARCH_MAX_QUERY_CHARS
            || !(1..=THREADS_SEARCH_MAX_LIMIT).contains(&self.limit)
            || self.cursor.as_ref().is_some_and(|cursor| {
                cursor.is_empty() || cursor.chars().count() > THREADS_SEARCH_MAX_CURSOR_CHARS
            })
        {
            return Err(ToolError::invalid_arguments(
                "invalid threads_search arguments",
            ));
        }
        Ok(())
    }
}

pub fn threads_search_tool_spec() -> ConfiguredToolSpec {
    ConfiguredToolSpec::new(
        ToolSpec::new(
            THREADS_SEARCH_TOOL_NAME,
            "Find conversations by indexed content in the current workspace that you are allowed to read. Returns relevant thread cards with bounded excerpts and exact source references. Follow nextCursor for more results; status and incomplete indicate an incomplete search. Full history is read separately.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "minLength": 1, "maxLength": THREADS_SEARCH_MAX_QUERY_CHARS},
                    "limit": {"type": "integer", "minimum": 1, "maximum": THREADS_SEARCH_MAX_LIMIT},
                    "cursor": {"type": ["string", "null"], "minLength": 1, "maxLength": THREADS_SEARCH_MAX_CURSOR_CHARS}
                },
                "required": ["query", "limit"],
                "additionalProperties": false
            }),
            PayloadKind::Function,
        ),
        ExecutionClass::Shared,
        pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(THREADS_SEARCH_TOOL_NAME),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn search_arguments_reject_authority_and_engine_controls_and_invalid_bounds() {
        for extra in [
            "workspaceId",
            "userId",
            "mode",
            "vector",
            "provider",
            "model",
        ] {
            let mut args = json!({"query": "providers", "limit": 20});
            args[extra] = json!("model supplied");
            assert!(serde_json::from_value::<ThreadsSearchParams>(args).is_err());
        }
        for args in [
            json!({"query":"", "limit":20}),
            json!({"query":"  ", "limit":20}),
            json!({"query":"providers", "limit":0}),
            json!({"query":"providers", "limit":21}),
            json!({"query":"providers", "limit":20, "cursor":""}),
        ] {
            assert!(
                serde_json::from_value::<ThreadsSearchParams>(args)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        for cursor in [None, Some(json!(null)), Some(json!("opaque-continuation"))] {
            let mut args = json!({"query":"настройка провайдеров", "limit":20});
            if let Some(cursor) = cursor {
                args["cursor"] = cursor;
            }
            serde_json::from_value::<ThreadsSearchParams>(args)
                .unwrap()
                .validate()
                .unwrap();
        }
    }
}
