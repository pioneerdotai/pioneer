use super::schema::*;

/// Transient protocol descriptor state. The process/session identity is owned
/// by Gateway; revisions are meaningful only within that exact generation.
#[derive(Clone, Debug, Default)]
pub struct ConfigurationSnapshot {
    pub initial_descriptor_fingerprint: String,
    pub configuration_replay: Vec<pioneer_protocol::AcpConfigurationChoice>,
    pub connection_id: uuid::Uuid,
    pub revision: u64,
    pub has_config_options: bool,
    pub options: Vec<SessionConfigOption>,
    pub modes: Option<SessionModeState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigurationVersion {
    pub connection_id: uuid::Uuid,
    pub revision: u64,
}

impl ConfigurationSnapshot {
    pub fn version(&self) -> ConfigurationVersion {
        ConfigurationVersion {
            connection_id: self.connection_id,
            revision: self.revision,
        }
    }

    pub(crate) fn replace(
        &mut self,
        options: Option<Vec<SessionConfigOption>>,
        modes: Option<SessionModeState>,
        revision: u64,
    ) -> Result<()> {
        validate_descriptors(options.as_deref().unwrap_or_default())?;
        self.revision = revision;
        self.has_config_options = options.is_some();
        self.options = options.unwrap_or_default();
        self.modes = modes;
        self.initial_descriptor_fingerprint =
            pioneer_protocol::acp_descriptor_fingerprint(&self.options, self.modes.as_ref())
                .map_err(|_| Error::internal_error())?;
        self.configuration_replay.clear();
        Ok(())
    }

    pub(crate) fn update(
        &mut self,
        options: Vec<SessionConfigOption>,
        revision: u64,
    ) -> Result<()> {
        validate_descriptors(&options)?;
        self.revision = revision;
        self.has_config_options = true;
        self.options = options;
        Ok(())
    }
}

fn validate_descriptors(options: &[SessionConfigOption]) -> Result<()> {
    let mut ids = std::collections::HashSet::new();
    for option in options {
        if !ids.insert(option.id.to_string()) {
            return Err(Error::invalid_params());
        }
        if let SessionConfigKind::Select(select) = &option.kind
            && let SessionConfigSelectOptions::Grouped(groups) = &select.options
        {
            let mut groups_seen = std::collections::HashSet::new();
            for group in groups {
                if !groups_seen.insert(group.group.to_string()) {
                    return Err(Error::invalid_params());
                }
            }
        }
    }
    Ok(())
}

fn select_values(options: &SessionConfigSelectOptions) -> Vec<&SessionConfigSelectOption> {
    match options {
        SessionConfigSelectOptions::Ungrouped(options) => options.iter().collect(),
        SessionConfigSelectOptions::Grouped(groups) => {
            groups.iter().flat_map(|g| &g.options).collect()
        }
        _ => Vec::new(),
    }
}

pub fn validate_config_value(
    options: &[SessionConfigOption],
    id: &SessionConfigId,
    value: &SessionConfigOptionValue,
) -> Result<()> {
    let option = options
        .iter()
        .find(|option| &option.id == id)
        .ok_or_else(Error::invalid_params)?;
    match (&option.kind, value) {
        (SessionConfigKind::Select(select), SessionConfigOptionValue::ValueId { value })
            if select_values(&select.options)
                .iter()
                .any(|c| &c.value == value) =>
        {
            Ok(())
        }
        (SessionConfigKind::Boolean(_), SessionConfigOptionValue::Boolean { .. }) => Ok(()),
        _ => Err(Error::invalid_params()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dependent_descriptors_have_distinct_initial_and_final_identity_and_ordered_choices() {
        let initial = serde_json::from_value(serde_json::json!([
            {"id":"z","name":"Opaque","type":"select","currentValue":"A","options":[{"value":"A","name":"A"},{"value":"B","name":"B"}]},
            {"id":"a","name":"Opaque","type":"select","currentValue":"old","options":[{"value":"old","name":"Old"}]},
            {"id":"flag","name":"Opaque","type":"boolean","currentValue":false}
        ])).unwrap();
        let mut config = ConfigurationSnapshot::default();
        config.replace(Some(initial), None, 1).unwrap();
        let base = config.initial_descriptor_fingerprint.clone();
        let first = pioneer_protocol::AcpConfigurationChoice {
            descriptor_fingerprint: base.clone(),
            config_id: "z".into(),
            value: SessionConfigOptionValue::value_id("B"),
        };
        validate_config_value(&config.options, &SessionConfigId::new("z"), &first.value).unwrap();
        let dependent = serde_json::from_value(serde_json::json!([
            {"id":"z","name":"Opaque","type":"select","currentValue":"B","options":[{"value":"A","name":"A"},{"value":"B","name":"B"}]},
            {"id":"a","name":"Opaque","type":"select","currentValue":"new","options":[{"group":"opaque","name":"Group","options":[{"value":"new","name":"New"}]}]},
            {"id":"flag","name":"Opaque","type":"boolean","currentValue":false}
        ])).unwrap();
        config.update(dependent, 2).unwrap();
        config.configuration_replay.push(first);
        let final_identity =
            pioneer_protocol::acp_descriptor_fingerprint(&config.options, None).unwrap();
        assert_ne!(base, final_identity);
        assert_eq!(config.initial_descriptor_fingerprint, base);
        assert!(
            validate_config_value(
                &config.options,
                &SessionConfigId::new("a"),
                &SessionConfigOptionValue::value_id("old")
            )
            .is_err()
        );
        for (id, value) in [
            ("a", SessionConfigOptionValue::value_id("new")),
            ("flag", SessionConfigOptionValue::boolean(true)),
        ] {
            validate_config_value(&config.options, &SessionConfigId::new(id), &value).unwrap();
            config
                .configuration_replay
                .push(pioneer_protocol::AcpConfigurationChoice {
                    descriptor_fingerprint: final_identity.clone(),
                    config_id: id.into(),
                    value,
                });
        }
        assert_eq!(
            config
                .configuration_replay
                .iter()
                .map(|c| c.config_id.as_str())
                .collect::<Vec<_>>(),
            vec!["z", "a", "flag"]
        );
        config.options.remove(1);
        assert_ne!(
            final_identity,
            pioneer_protocol::acp_descriptor_fingerprint(&config.options, None).unwrap()
        );
    }

    #[test]
    fn arbitrary_grouped_values_and_boolean_are_type_checked() {
        let options = serde_json::from_value(json!([
            {"id":"opaque/id", "name":"Model", "type":"select", "category":"future-category",
             "currentValue":"a/one", "options":[{"group":"anything","name":"Group", "options":[{"value":"a/one","name":"One"}]}]},
            {"id":"opaque/bool", "name":"Flag", "type":"boolean", "currentValue":false}
        ])).unwrap();
        let mut snapshot = ConfigurationSnapshot::default();
        snapshot.replace(Some(options), None, 1).unwrap();
        assert!(
            validate_config_value(
                &snapshot.options,
                &SessionConfigId::new("opaque/id"),
                &SessionConfigOptionValue::value_id("a/one")
            )
            .is_ok()
        );
        assert!(
            validate_config_value(
                &snapshot.options,
                &SessionConfigId::new("opaque/id"),
                &SessionConfigOptionValue::boolean(true)
            )
            .is_err()
        );
        assert!(
            validate_config_value(
                &snapshot.options,
                &SessionConfigId::new("opaque/bool"),
                &SessionConfigOptionValue::boolean(true)
            )
            .is_ok()
        );
        snapshot.update(Vec::new(), 2).unwrap();
        assert_eq!(snapshot.revision, 2);
        assert!(snapshot.has_config_options);
        assert!(snapshot.options.is_empty());
    }
}
