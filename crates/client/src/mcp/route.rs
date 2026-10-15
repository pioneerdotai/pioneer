//! Reusable catalog destination backed by the Client navigation owner.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum McpRoute {
    List,
    Details(String),
}
impl crate::core::ClientCore {
    pub fn navigate_mcp(&self, route: McpRoute) -> crate::core::ClientTransition {
        let id = match route {
            McpRoute::List => None,
            McpRoute::Details(id) => Some(id),
        };
        self.navigate(
            crate::navigation::NavigationIntent::Navigate {
                destination: crate::navigation::SemanticDestination::Mcp { server_id: id },
            },
            None,
        )
    }
}
