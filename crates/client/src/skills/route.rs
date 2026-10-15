//! Reusable catalog destination backed by the Client navigation owner.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum SkillsRoute {
    List,
    Details(pioneer_protocol::SkillId),
}
impl crate::core::ClientCore {
    pub fn navigate_skills(&self, route: SkillsRoute) -> crate::core::ClientTransition {
        let id = match route {
            SkillsRoute::List => None,
            SkillsRoute::Details(id) => Some(id),
        };
        self.navigate(
            crate::navigation::NavigationIntent::Navigate {
                destination: crate::navigation::SemanticDestination::Skills { skill_id: id },
            },
            None,
        )
    }
}
