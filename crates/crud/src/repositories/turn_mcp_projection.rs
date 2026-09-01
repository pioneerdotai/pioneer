use crate::{TurnMcpBindingRecord, TurnMcpProjectionRecord};
use anyhow::anyhow;
use pioneer_entity::{thread, turn, turn_mcp_projection};
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ConnectionTrait, EntityTrait, Set, TransactionSession, TransactionTrait,
    entity::prelude::DateTimeWithTimeZone,
};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnMcpProjectionReplacement {
    pub projection: TurnMcpProjectionRecord,
    pub bindings: Vec<TurnMcpBindingRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnMcpProjectionReplaceOutcome {
    pub turn_id: String,
    pub manifest_hash: String,
    pub tool_count: i32,
}

#[derive(Debug)]
pub enum TurnMcpProjectionPersistenceError {
    InvalidToolCount {
        declared: i32,
        actual: usize,
    },
    DuplicateCanonicalCallableName {
        callable_name: String,
    },
    DuplicateProviderCallableName {
        callable_name: String,
    },
    TurnNotFound {
        turn_id: String,
    },
    ThreadNotFound {
        thread_id: String,
    },
    WorkspaceMismatch {
        turn_id: String,
        declared_workspace_id: String,
        actual_workspace_id: String,
    },
    Storage {
        stage: &'static str,
        source: anyhow::Error,
    },
}

impl TurnMcpProjectionPersistenceError {
    fn storage(stage: &'static str, source: impl Into<anyhow::Error>) -> Self {
        Self::Storage {
            stage,
            source: source.into(),
        }
    }

    pub fn is_sqlite_lock(&self) -> bool {
        match self {
            Self::Storage { source, .. } => {
                pioneer_sqlite::is_sqlite_lock_message(format!("{source:#}").as_str())
            }
            _ => false,
        }
    }
}

impl fmt::Display for TurnMcpProjectionPersistenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidToolCount { declared, actual } => write!(
                formatter,
                "turn MCP projection declares {declared} tools but contains {actual} bindings"
            ),
            Self::DuplicateCanonicalCallableName { callable_name } => write!(
                formatter,
                "turn MCP projection contains duplicate canonical callable `{callable_name}`"
            ),
            Self::DuplicateProviderCallableName { callable_name } => write!(
                formatter,
                "turn MCP projection contains duplicate provider callable `{callable_name}`"
            ),
            Self::TurnNotFound { turn_id } => {
                write!(
                    formatter,
                    "turn MCP projection references missing turn `{turn_id}`"
                )
            }
            Self::ThreadNotFound { thread_id } => write!(
                formatter,
                "turn MCP projection references missing thread `{thread_id}`"
            ),
            Self::WorkspaceMismatch {
                turn_id,
                declared_workspace_id,
                actual_workspace_id,
            } => write!(
                formatter,
                "turn MCP projection for `{turn_id}` declares workspace `{declared_workspace_id}` but turn belongs to `{actual_workspace_id}`"
            ),
            Self::Storage { stage, source } => {
                write!(
                    formatter,
                    "turn MCP projection persistence failed at {stage}: {source:#}"
                )
            }
        }
    }
}

impl Error for TurnMcpProjectionPersistenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

pub async fn replace_turn_mcp_projection<D>(
    db: &D,
    replacement: &TurnMcpProjectionReplacement,
) -> Result<TurnMcpProjectionReplaceOutcome, TurnMcpProjectionPersistenceError>
where
    D: TransactionTrait,
{
    replace_turn_mcp_projection_inner(db, replacement, None, AtomicProjectionReplaceFault::None)
        .await
}

pub async fn replace_turn_mcp_projection_with_authorization_context<D>(
    db: &D,
    replacement: &TurnMcpProjectionReplacement,
    authorization_context_json: &str,
) -> Result<TurnMcpProjectionReplaceOutcome, TurnMcpProjectionPersistenceError>
where
    D: TransactionTrait,
{
    replace_turn_mcp_projection_inner(
        db,
        replacement,
        Some(authorization_context_json),
        AtomicProjectionReplaceFault::None,
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AtomicProjectionReplaceFault {
    None,
    AfterHeader,
    AfterDelete,
    AfterInsert(usize),
}

#[cfg(test)]
pub(crate) async fn replace_turn_mcp_projection_with_fault(
    db: &impl TransactionTrait,
    replacement: &TurnMcpProjectionReplacement,
    fault: AtomicProjectionReplaceFault,
) -> Result<TurnMcpProjectionReplaceOutcome, TurnMcpProjectionPersistenceError> {
    replace_turn_mcp_projection_inner(db, replacement, None, fault).await
}

async fn replace_turn_mcp_projection_inner<D>(
    db: &D,
    replacement: &TurnMcpProjectionReplacement,
    authorization_context_json: Option<&str>,
    fault: AtomicProjectionReplaceFault,
) -> Result<TurnMcpProjectionReplaceOutcome, TurnMcpProjectionPersistenceError>
where
    D: TransactionTrait,
{
    validate_replacement_shape(replacement)?;
    let transaction = db
        .begin()
        .await
        .map_err(|error| TurnMcpProjectionPersistenceError::storage("begin", error))?;

    let result = async {
        let outcome = replace_in_transaction(&transaction, replacement, fault).await?;
        if let Some(context_json) = authorization_context_json {
            let updated = crate::repositories::turn::set_turn_execution_authorization_context(
                &transaction,
                replacement.projection.turn_id.as_str(),
                context_json,
            )
            .await
            .map_err(|error| {
                TurnMcpProjectionPersistenceError::storage("authorization_context_update", error)
            })?;
            if !updated {
                return Err(TurnMcpProjectionPersistenceError::TurnNotFound {
                    turn_id: replacement.projection.turn_id.clone(),
                });
            }
        }
        Ok(outcome)
    }
    .await;
    match result {
        Ok(outcome) => {
            transaction
                .commit()
                .await
                .map_err(|error| TurnMcpProjectionPersistenceError::storage("commit", error))?;
            Ok(outcome)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn replace_in_transaction<C>(
    transaction: &C,
    replacement: &TurnMcpProjectionReplacement,
    fault: AtomicProjectionReplaceFault,
) -> Result<TurnMcpProjectionReplaceOutcome, TurnMcpProjectionPersistenceError>
where
    C: ConnectionTrait,
{
    validate_turn_workspace(transaction, &replacement.projection).await?;
    let created_at = unix_to_datetime(replacement.projection.created_at_unix)?;

    turn_mcp_projection::Entity::insert(turn_mcp_projection::ActiveModel {
        turn_id: Set(replacement.projection.turn_id.clone()),
        workspace_id: Set(replacement.projection.workspace_id.clone()),
        projection_version: Set(i64::from(replacement.projection.projection_version)),
        manifest_hash: Set(replacement.projection.manifest_hash.clone()),
        resolution_status: Set(replacement.projection.resolution_status.clone()),
        tool_count: Set(i64::from(replacement.projection.tool_count)),
        created_at: Set(created_at),
    })
    .on_conflict(
        OnConflict::column(turn_mcp_projection::Column::TurnId)
            .update_columns([
                turn_mcp_projection::Column::WorkspaceId,
                turn_mcp_projection::Column::ProjectionVersion,
                turn_mcp_projection::Column::ManifestHash,
                turn_mcp_projection::Column::ResolutionStatus,
                turn_mcp_projection::Column::ToolCount,
                turn_mcp_projection::Column::CreatedAt,
            ])
            .to_owned(),
    )
    .exec(transaction)
    .await
    .map_err(|error| TurnMcpProjectionPersistenceError::storage("header_upsert", error))?;
    inject_fault(fault, AtomicProjectionReplaceFault::AfterHeader)?;

    crate::repositories::turn_mcp_binding::delete_turn_mcp_bindings(
        transaction,
        replacement.projection.turn_id.as_str(),
    )
    .await
    .map_err(|error| TurnMcpProjectionPersistenceError::storage("binding_delete", error))?;
    inject_fault(fault, AtomicProjectionReplaceFault::AfterDelete)?;

    let mut bindings = replacement.bindings.iter().collect::<Vec<_>>();
    bindings.sort_by(|left, right| {
        left.canonical_callable_name
            .cmp(&right.canonical_callable_name)
            .then_with(|| {
                left.provider_callable_name
                    .cmp(&right.provider_callable_name)
            })
            .then_with(|| {
                left.server_installation_id
                    .cmp(&right.server_installation_id)
            })
            .then_with(|| left.raw_tool_name.cmp(&right.raw_tool_name))
    });
    for (index, binding) in bindings.into_iter().enumerate() {
        crate::repositories::turn_mcp_binding::insert_turn_mcp_binding(
            transaction,
            replacement.projection.turn_id.as_str(),
            binding,
            created_at,
        )
        .await
        .map_err(|error| TurnMcpProjectionPersistenceError::storage("binding_insert", error))?;
        inject_fault(fault, AtomicProjectionReplaceFault::AfterInsert(index + 1))?;
    }

    Ok(TurnMcpProjectionReplaceOutcome {
        turn_id: replacement.projection.turn_id.clone(),
        manifest_hash: replacement.projection.manifest_hash.clone(),
        tool_count: replacement.projection.tool_count,
    })
}

fn validate_replacement_shape(
    replacement: &TurnMcpProjectionReplacement,
) -> Result<(), TurnMcpProjectionPersistenceError> {
    let actual = replacement.bindings.len();
    if usize::try_from(replacement.projection.tool_count).ok() != Some(actual) {
        return Err(TurnMcpProjectionPersistenceError::InvalidToolCount {
            declared: replacement.projection.tool_count,
            actual,
        });
    }

    let mut canonical_names = std::collections::HashSet::with_capacity(actual);
    let mut provider_names = std::collections::HashSet::with_capacity(actual);
    for binding in &replacement.bindings {
        if !canonical_names.insert(binding.canonical_callable_name.as_str()) {
            return Err(
                TurnMcpProjectionPersistenceError::DuplicateCanonicalCallableName {
                    callable_name: binding.canonical_callable_name.clone(),
                },
            );
        }
        if !provider_names.insert(binding.provider_callable_name.as_str()) {
            return Err(
                TurnMcpProjectionPersistenceError::DuplicateProviderCallableName {
                    callable_name: binding.provider_callable_name.clone(),
                },
            );
        }
    }
    Ok(())
}

async fn validate_turn_workspace<C>(
    transaction: &C,
    projection: &TurnMcpProjectionRecord,
) -> Result<(), TurnMcpProjectionPersistenceError>
where
    C: ConnectionTrait,
{
    let turn = turn::Entity::find_by_id(projection.turn_id.clone())
        .one(transaction)
        .await
        .map_err(|error| TurnMcpProjectionPersistenceError::storage("turn_lookup", error))?
        .ok_or_else(|| TurnMcpProjectionPersistenceError::TurnNotFound {
            turn_id: projection.turn_id.clone(),
        })?;
    let thread = thread::Entity::find_by_id(turn.thread_id.clone())
        .one(transaction)
        .await
        .map_err(|error| TurnMcpProjectionPersistenceError::storage("thread_lookup", error))?
        .ok_or(TurnMcpProjectionPersistenceError::ThreadNotFound {
            thread_id: turn.thread_id,
        })?;
    if thread.workspace_id != projection.workspace_id {
        return Err(TurnMcpProjectionPersistenceError::WorkspaceMismatch {
            turn_id: projection.turn_id.clone(),
            declared_workspace_id: projection.workspace_id.clone(),
            actual_workspace_id: thread.workspace_id,
        });
    }
    Ok(())
}

fn unix_to_datetime(
    timestamp: i64,
) -> Result<DateTimeWithTimeZone, TurnMcpProjectionPersistenceError> {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|value| value.fixed_offset())
        .ok_or_else(|| {
            TurnMcpProjectionPersistenceError::storage(
                "timestamp_validation",
                anyhow!("invalid projection timestamp `{timestamp}`"),
            )
        })
}

fn inject_fault(
    actual: AtomicProjectionReplaceFault,
    stage: AtomicProjectionReplaceFault,
) -> Result<(), TurnMcpProjectionPersistenceError> {
    if actual == stage {
        return Err(TurnMcpProjectionPersistenceError::storage(
            "fault_injection",
            anyhow!("injected projection replacement failure at {stage:?}"),
        ));
    }
    Ok(())
}
