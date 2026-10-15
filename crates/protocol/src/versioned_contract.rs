use serde_json::Value;
use std::fmt;

pub(crate) type JsonContractVersionReader = fn(&Value) -> Result<u32, String>;
pub(crate) type JsonContractMigrationFn = fn(Value) -> Result<Value, String>;

#[derive(Clone, Copy)]
pub(crate) struct JsonContractMigration {
    pub(crate) from_version: u32,
    pub(crate) to_version: u32,
    pub(crate) migrate: JsonContractMigrationFn,
}

/// Executes an append-only chain of one-version-at-a-time JSON contract
/// migrations. Runtime code consumes only `current_version`; compatibility
/// with older durable values remains confined to migration steps. Every step
/// owns the immutable schema and integrity check for its source version; the
/// runner never treats decoding alone as authorization to rewrite a record.
pub(crate) struct JsonContractMigrator<'a> {
    current_version: u32,
    version_reader: JsonContractVersionReader,
    migrations: &'a [JsonContractMigration],
}

impl<'a> JsonContractMigrator<'a> {
    pub(crate) const fn new(
        current_version: u32,
        version_reader: JsonContractVersionReader,
        migrations: &'a [JsonContractMigration],
    ) -> Self {
        Self {
            current_version,
            version_reader,
            migrations,
        }
    }

    pub(crate) fn migrate_to_current(
        &self,
        mut value: Value,
    ) -> Result<Value, JsonContractMigrationError> {
        self.validate_registry()?;
        let mut version =
            (self.version_reader)(&value).map_err(JsonContractMigrationError::InvalidPayload)?;
        if version > self.current_version {
            return Err(JsonContractMigrationError::FutureVersion {
                found: version,
                current: self.current_version,
            });
        }

        while version < self.current_version {
            let migration = self
                .migrations
                .iter()
                .find(|migration| migration.from_version == version)
                .ok_or(JsonContractMigrationError::MissingMigration {
                    from_version: version,
                })?;
            value = (migration.migrate)(value).map_err(|message| {
                JsonContractMigrationError::StepFailed {
                    from_version: migration.from_version,
                    to_version: migration.to_version,
                    message,
                }
            })?;
            let migrated_version = (self.version_reader)(&value)
                .map_err(JsonContractMigrationError::InvalidPayload)?;
            if migrated_version != migration.to_version {
                return Err(JsonContractMigrationError::VersionMismatch {
                    expected: migration.to_version,
                    found: migrated_version,
                });
            }
            version = migrated_version;
        }
        Ok(value)
    }

    fn validate_registry(&self) -> Result<(), JsonContractMigrationError> {
        if self.current_version == 0 {
            return Err(JsonContractMigrationError::InvalidRegistry(
                "current contract version must be positive".to_owned(),
            ));
        }
        for (index, migration) in self.migrations.iter().enumerate() {
            if migration.from_version == 0
                || migration.from_version.checked_add(1) != Some(migration.to_version)
                || migration.to_version > self.current_version
            {
                return Err(JsonContractMigrationError::InvalidRegistry(format!(
                    "migration {} -> {} is not a valid adjacent step to current version {}",
                    migration.from_version, migration.to_version, self.current_version
                )));
            }
            if self.migrations[..index]
                .iter()
                .any(|candidate| candidate.from_version == migration.from_version)
            {
                return Err(JsonContractMigrationError::InvalidRegistry(format!(
                    "duplicate migration from version {}",
                    migration.from_version
                )));
            }
        }
        for from_version in 1..self.current_version {
            if !self
                .migrations
                .iter()
                .any(|migration| migration.from_version == from_version)
            {
                return Err(JsonContractMigrationError::MissingMigration { from_version });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JsonContractMigrationError {
    InvalidRegistry(String),
    InvalidPayload(String),
    FutureVersion {
        found: u32,
        current: u32,
    },
    MissingMigration {
        from_version: u32,
    },
    StepFailed {
        from_version: u32,
        to_version: u32,
        message: String,
    },
    VersionMismatch {
        expected: u32,
        found: u32,
    },
}

impl fmt::Display for JsonContractMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRegistry(message) => {
                write!(formatter, "invalid migration registry: {message}")
            }
            Self::InvalidPayload(message) => {
                write!(formatter, "invalid versioned contract: {message}")
            }
            Self::FutureVersion { found, current } => write!(
                formatter,
                "contract version {found} is newer than supported version {current}"
            ),
            Self::MissingMigration { from_version } => {
                write!(formatter, "missing migration from version {from_version}")
            }
            Self::StepFailed {
                from_version,
                to_version,
                message,
            } => write!(
                formatter,
                "contract migration {from_version} -> {to_version} failed: {message}"
            ),
            Self::VersionMismatch { expected, found } => write!(
                formatter,
                "contract migration produced version {found}, expected {expected}"
            ),
        }
    }
}

impl std::error::Error for JsonContractMigrationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn version(value: &Value) -> Result<u32, String> {
        value
            .get("version")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| "version must be a u32".to_owned())
    }

    fn one_to_two(mut value: Value) -> Result<Value, String> {
        value["version"] = json!(2);
        value["v2"] = json!(true);
        Ok(value)
    }

    fn two_to_three(mut value: Value) -> Result<Value, String> {
        value["version"] = json!(3);
        value["v3"] = json!(true);
        Ok(value)
    }

    #[test]
    fn migrates_every_adjacent_step_to_current() {
        let migrations = [
            JsonContractMigration {
                from_version: 1,
                to_version: 2,
                migrate: one_to_two,
            },
            JsonContractMigration {
                from_version: 2,
                to_version: 3,
                migrate: two_to_three,
            },
        ];
        let migrated = JsonContractMigrator::new(3, version, &migrations)
            .migrate_to_current(json!({ "version": 1 }))
            .expect("migration chain should succeed");
        assert_eq!(migrated, json!({ "version": 3, "v2": true, "v3": true }));
    }

    #[test]
    fn rejects_a_registry_with_a_missing_intermediate_step() {
        let migrations = [JsonContractMigration {
            from_version: 1,
            to_version: 2,
            migrate: one_to_two,
        }];
        let error = JsonContractMigrator::new(3, version, &migrations)
            .migrate_to_current(json!({ "version": 3 }))
            .expect_err("missing 2 -> 3 migration must fail closed");
        assert_eq!(
            error,
            JsonContractMigrationError::MissingMigration { from_version: 2 }
        );
    }
}
