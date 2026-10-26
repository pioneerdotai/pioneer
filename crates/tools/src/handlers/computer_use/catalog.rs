// Canonical upstream schemas are maintained by scripts/update-cua-driver.py (MIT).
use crate::output_policy::computer_use_output_policy;
use crate::spec::{
    ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolIdempotencyMode, ToolRecoveryMetadata,
    ToolRetryClass, ToolSpec,
};
use serde_json::Value;

pub use pioneer_protocol::CUA_TOOL_NAMES as TOOL_NAMES;
pub fn upstream_name(name: &str) -> Option<&str> {
    TOOL_NAMES
        .contains(&name)
        .then(|| name.strip_prefix("cua_"))
        .flatten()
}
pub fn configured_specs() -> Vec<ConfiguredToolSpec> {
    let native: Value = serde_json::from_str(include_str!("native-contract.json"))
        .expect("pinned native contract is valid JSON");
    let mut schemas = native["tools"]
        .as_array()
        .expect("native tools")
        .iter()
        .map(|tool| {
            (
                tool["name"].as_str().expect("native name").to_owned(),
                tool["input_schema"].clone(),
                tool["description"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    schemas.extend(
        super::browser_catalog::schemas()
            .into_iter()
            .map(|(name, schema)| {
                (
                    name.to_owned(),
                    schema,
                    format!("Canonical CUA browser operation {name}."),
                )
            }),
    );
    schemas.into_iter().filter(|(name, _, _)| TOOL_NAMES.contains(&format!("cua_{name}").as_str())).map(|(name, mut schema, description)| {
        // This supported daemon bridge binds an implicit trusted session per
        // Gateway turn. Session selection is a host concern, not agent input.
        if let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) { properties.remove("session"); }
        if let Some(required) = schema.get_mut("required").and_then(Value::as_array_mut) { required.retain(|field| field.as_str() != Some("session")); }
        let spec = ToolSpec::new(format!("cua_{name}"), format!("{description} Controls the selected Gateway computer. Gateway owns the turn session; omit session and reserved transport fields. Use fresh observations after actions; input delivery alone does not prove the goal was achieved. Never replay an unknown outcome."), schema, PayloadKind::Function)
            .with_recovery(ToolRecoveryMetadata { retry_class: ToolRetryClass::Never, idempotency_mode: ToolIdempotencyMode::None, max_attempts: 1, can_resume: false, max_wall_clock_secs: None });
        ConfiguredToolSpec::new(spec, ExecutionClass::Shared, computer_use_output_policy())
    }).collect()
}
