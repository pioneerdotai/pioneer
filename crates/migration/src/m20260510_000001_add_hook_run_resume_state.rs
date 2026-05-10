use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::{big_integer, string, text, timestamp_with_time_zone};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum HookRun {
    Table,
    Status,
    QueuedAt,
    ResumeStateJson,
}

#[derive(DeriveIden)]
enum ArtifactBlob {
    Table,
    Id,
    WorkspaceId,
    Sha256,
    SizeBytes,
    MimeType,
    StorageBackend,
    StorageKey,
    EncryptionKeyId,
    CreatedAt,
    LastVerifiedAt,
    MetadataJson,
}

#[derive(DeriveIden)]
enum Artifact {
    Table,
    Id,
    WorkspaceId,
    PrimaryThreadId,
    CurrentVersionId,
    DisplayName,
    Kind,
    MimeType,
    Status,
    CreatedByKind,
    CreatedByActorId,
    CreatedAt,
    UpdatedAt,
    DeletedAt,
    MetadataJson,
}

#[derive(DeriveIden)]
enum ArtifactVersion {
    Table,
    Id,
    WorkspaceId,
    ArtifactId,
    VersionNumber,
    BlobId,
    SourceUri,
    SourcePathRedacted,
    CreatedByTurnId,
    CreatedByMessageId,
    CreatedByTurnItemId,
    CreatedByToolCallId,
    CreatedByTaskId,
    CreatedByTaskRunId,
    CreatedAt,
    MetadataJson,
}

#[derive(DeriveIden)]
enum ArtifactBinding {
    Table,
    Id,
    ArtifactId,
    ArtifactVersionId,
    WorkspaceId,
    ThreadId,
    TurnId,
    MessageId,
    TurnItemId,
    ToolCallId,
    TaskId,
    TaskRunId,
    BindingKind,
    Direction,
    ItemIndex,
    Role,
    CreatedAt,
    MetadataJson,
}

#[derive(DeriveIden)]
enum ArtifactExternalRef {
    Table,
    Id,
    WorkspaceId,
    ArtifactId,
    ArtifactVersionId,
    Provider,
    ModelFamily,
    TransportKind,
    ExternalId,
    ExternalUri,
    ExpiresAt,
    CreatedAt,
    MetadataJson,
}

#[derive(DeriveIden)]
enum ArtifactProjection {
    Table,
    Id,
    WorkspaceId,
    ArtifactId,
    ArtifactVersionId,
    ProjectionKind,
    Status,
    TextContent,
    BlobId,
    MetadataJson,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum ArtifactUploadSession {
    Table,
    Id,
    WorkspaceId,
    ThreadId,
    TurnId,
    MessageId,
    DisplayName,
    ExpectedSizeBytes,
    ExpectedSha256,
    MimeType,
    Status,
    TempStorageKey,
    ReceivedBytes,
    CreatedAt,
    UpdatedAt,
    ExpiresAt,
    MetadataJson,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(HookRun::Table)
                    .add_column(ColumnDef::new(HookRun::ResumeStateJson).text().null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_hook_run_status_queued_at")
                    .table(HookRun::Table)
                    .col(HookRun::Status)
                    .col(HookRun::QueuedAt)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table("attachment_upload_registry")
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(ArtifactBlob::Table)
                    .if_not_exists()
                    .col(string(ArtifactBlob::Id).string_len(64).primary_key())
                    .col(string(ArtifactBlob::WorkspaceId).string_len(21))
                    .col(string(ArtifactBlob::Sha256).string_len(128))
                    .col(big_integer(ArtifactBlob::SizeBytes))
                    .col(string(ArtifactBlob::MimeType).string_len(255).null())
                    .col(string(ArtifactBlob::StorageBackend).string_len(64))
                    .col(text(ArtifactBlob::StorageKey))
                    .col(string(ArtifactBlob::EncryptionKeyId).string_len(128).null())
                    .col(
                        timestamp_with_time_zone(ArtifactBlob::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(timestamp_with_time_zone(ArtifactBlob::LastVerifiedAt).null())
                    .col(text(ArtifactBlob::MetadataJson).default("{}"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_artifact_blob_workspace_sha_size_backend")
                    .table(ArtifactBlob::Table)
                    .col(ArtifactBlob::WorkspaceId)
                    .col(ArtifactBlob::Sha256)
                    .col(ArtifactBlob::SizeBytes)
                    .col(ArtifactBlob::StorageBackend)
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_blob_workspace_sha256")
                    .table(ArtifactBlob::Table)
                    .col(ArtifactBlob::WorkspaceId)
                    .col(ArtifactBlob::Sha256)
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Artifact::Table)
                    .if_not_exists()
                    .col(string(Artifact::Id).string_len(64).primary_key())
                    .col(string(Artifact::WorkspaceId).string_len(21))
                    .col(string(Artifact::PrimaryThreadId).string_len(21).null())
                    .col(string(Artifact::CurrentVersionId).string_len(64).null())
                    .col(string(Artifact::DisplayName).string_len(255))
                    .col(string(Artifact::Kind).string_len(64))
                    .col(string(Artifact::MimeType).string_len(255).null())
                    .col(string(Artifact::Status).string_len(32))
                    .col(string(Artifact::CreatedByKind).string_len(32))
                    .col(string(Artifact::CreatedByActorId).string_len(64).null())
                    .col(
                        timestamp_with_time_zone(Artifact::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(Artifact::UpdatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(timestamp_with_time_zone(Artifact::DeletedAt).null())
                    .col(text(Artifact::MetadataJson).default("{}"))
                    .to_owned(),
            )
            .await?;

        for (name, cols) in [
            (
                "idx_artifact_workspace_created",
                vec![Artifact::WorkspaceId, Artifact::CreatedAt],
            ),
            (
                "idx_artifact_primary_thread",
                vec![
                    Artifact::WorkspaceId,
                    Artifact::PrimaryThreadId,
                    Artifact::CreatedAt,
                ],
            ),
            (
                "idx_artifact_status",
                vec![Artifact::WorkspaceId, Artifact::Status],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table(Artifact::Table);
            for col in cols {
                index.col(col);
            }
            manager.create_index(index.to_owned()).await?;
        }

        manager
            .create_table(
                Table::create()
                    .table(ArtifactVersion::Table)
                    .if_not_exists()
                    .col(string(ArtifactVersion::Id).string_len(64).primary_key())
                    .col(string(ArtifactVersion::WorkspaceId).string_len(21))
                    .col(string(ArtifactVersion::ArtifactId).string_len(64))
                    .col(big_integer(ArtifactVersion::VersionNumber))
                    .col(string(ArtifactVersion::BlobId).string_len(64))
                    .col(text(ArtifactVersion::SourceUri).null())
                    .col(text(ArtifactVersion::SourcePathRedacted).null())
                    .col(
                        string(ArtifactVersion::CreatedByTurnId)
                            .string_len(21)
                            .null(),
                    )
                    .col(
                        string(ArtifactVersion::CreatedByMessageId)
                            .string_len(64)
                            .null(),
                    )
                    .col(
                        string(ArtifactVersion::CreatedByTurnItemId)
                            .string_len(64)
                            .null(),
                    )
                    .col(
                        string(ArtifactVersion::CreatedByToolCallId)
                            .string_len(64)
                            .null(),
                    )
                    .col(
                        string(ArtifactVersion::CreatedByTaskId)
                            .string_len(21)
                            .null(),
                    )
                    .col(
                        string(ArtifactVersion::CreatedByTaskRunId)
                            .string_len(21)
                            .null(),
                    )
                    .col(
                        timestamp_with_time_zone(ArtifactVersion::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(text(ArtifactVersion::MetadataJson).default("{}"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_artifact_version_number")
                    .table(ArtifactVersion::Table)
                    .col(ArtifactVersion::WorkspaceId)
                    .col(ArtifactVersion::ArtifactId)
                    .col(ArtifactVersion::VersionNumber)
                    .unique()
                    .to_owned(),
            )
            .await?;

        for (name, cols) in [
            (
                "idx_artifact_version_blob",
                vec![ArtifactVersion::WorkspaceId, ArtifactVersion::BlobId],
            ),
            (
                "idx_artifact_version_turn",
                vec![
                    ArtifactVersion::WorkspaceId,
                    ArtifactVersion::CreatedByTurnId,
                ],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table(ArtifactVersion::Table);
            for col in cols {
                index.col(col);
            }
            manager.create_index(index.to_owned()).await?;
        }

        manager
            .create_table(
                Table::create()
                    .table(ArtifactBinding::Table)
                    .if_not_exists()
                    .col(string(ArtifactBinding::Id).string_len(64).primary_key())
                    .col(string(ArtifactBinding::ArtifactId).string_len(64))
                    .col(
                        string(ArtifactBinding::ArtifactVersionId)
                            .string_len(64)
                            .null(),
                    )
                    .col(string(ArtifactBinding::WorkspaceId).string_len(21))
                    .col(string(ArtifactBinding::ThreadId).string_len(21).null())
                    .col(string(ArtifactBinding::TurnId).string_len(21).null())
                    .col(string(ArtifactBinding::MessageId).string_len(64).null())
                    .col(string(ArtifactBinding::TurnItemId).string_len(64).null())
                    .col(string(ArtifactBinding::ToolCallId).string_len(64).null())
                    .col(string(ArtifactBinding::TaskId).string_len(21).null())
                    .col(string(ArtifactBinding::TaskRunId).string_len(21).null())
                    .col(string(ArtifactBinding::BindingKind).string_len(32))
                    .col(string(ArtifactBinding::Direction).string_len(32))
                    .col(big_integer(ArtifactBinding::ItemIndex).null())
                    .col(string(ArtifactBinding::Role).string_len(32).null())
                    .col(
                        timestamp_with_time_zone(ArtifactBinding::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(text(ArtifactBinding::MetadataJson).default("{}"))
                    .to_owned(),
            )
            .await?;

        for (name, cols) in [
            (
                "idx_artifact_binding_thread",
                vec![
                    ArtifactBinding::WorkspaceId,
                    ArtifactBinding::ThreadId,
                    ArtifactBinding::CreatedAt,
                ],
            ),
            (
                "idx_artifact_binding_turn",
                vec![
                    ArtifactBinding::WorkspaceId,
                    ArtifactBinding::TurnId,
                    ArtifactBinding::CreatedAt,
                ],
            ),
            (
                "idx_artifact_binding_message",
                vec![
                    ArtifactBinding::WorkspaceId,
                    ArtifactBinding::MessageId,
                    ArtifactBinding::CreatedAt,
                ],
            ),
            (
                "idx_artifact_binding_task",
                vec![
                    ArtifactBinding::WorkspaceId,
                    ArtifactBinding::TaskId,
                    ArtifactBinding::TaskRunId,
                    ArtifactBinding::CreatedAt,
                ],
            ),
            (
                "idx_artifact_binding_artifact",
                vec![ArtifactBinding::ArtifactId, ArtifactBinding::CreatedAt],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table(ArtifactBinding::Table);
            for col in cols {
                index.col(col);
            }
            manager.create_index(index.to_owned()).await?;
        }

        manager
            .create_table(
                Table::create()
                    .table(ArtifactExternalRef::Table)
                    .if_not_exists()
                    .col(string(ArtifactExternalRef::Id).string_len(64).primary_key())
                    .col(string(ArtifactExternalRef::WorkspaceId).string_len(21))
                    .col(string(ArtifactExternalRef::ArtifactId).string_len(64))
                    .col(
                        string(ArtifactExternalRef::ArtifactVersionId)
                            .string_len(64)
                            .null(),
                    )
                    .col(string(ArtifactExternalRef::Provider).string_len(64))
                    .col(
                        string(ArtifactExternalRef::ModelFamily)
                            .string_len(255)
                            .null(),
                    )
                    .col(string(ArtifactExternalRef::TransportKind).string_len(32))
                    .col(text(ArtifactExternalRef::ExternalId))
                    .col(text(ArtifactExternalRef::ExternalUri).null())
                    .col(timestamp_with_time_zone(ArtifactExternalRef::ExpiresAt).null())
                    .col(
                        timestamp_with_time_zone(ArtifactExternalRef::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(text(ArtifactExternalRef::MetadataJson).default("{}"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_artifact_external_ref")
                    .table(ArtifactExternalRef::Table)
                    .col(ArtifactExternalRef::WorkspaceId)
                    .col(ArtifactExternalRef::ArtifactId)
                    .col(ArtifactExternalRef::ArtifactVersionId)
                    .col(ArtifactExternalRef::Provider)
                    .col(ArtifactExternalRef::ModelFamily)
                    .col(ArtifactExternalRef::TransportKind)
                    .unique()
                    .to_owned(),
            )
            .await?;

        for (name, cols) in [
            (
                "idx_artifact_external_ref_artifact",
                vec![
                    ArtifactExternalRef::WorkspaceId,
                    ArtifactExternalRef::ArtifactId,
                ],
            ),
            (
                "idx_artifact_external_ref_expiry",
                vec![
                    ArtifactExternalRef::WorkspaceId,
                    ArtifactExternalRef::ExpiresAt,
                ],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table(ArtifactExternalRef::Table);
            for col in cols {
                index.col(col);
            }
            manager.create_index(index.to_owned()).await?;
        }

        manager
            .create_table(
                Table::create()
                    .table(ArtifactProjection::Table)
                    .if_not_exists()
                    .col(string(ArtifactProjection::Id).string_len(64).primary_key())
                    .col(string(ArtifactProjection::WorkspaceId).string_len(21))
                    .col(string(ArtifactProjection::ArtifactId).string_len(64))
                    .col(string(ArtifactProjection::ArtifactVersionId).string_len(64))
                    .col(string(ArtifactProjection::ProjectionKind).string_len(32))
                    .col(string(ArtifactProjection::Status).string_len(32))
                    .col(text(ArtifactProjection::TextContent).null())
                    .col(string(ArtifactProjection::BlobId).string_len(64).null())
                    .col(text(ArtifactProjection::MetadataJson).default("{}"))
                    .col(
                        timestamp_with_time_zone(ArtifactProjection::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(ArtifactProjection::UpdatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        for (name, cols) in [
            (
                "idx_artifact_projection_artifact",
                vec![
                    ArtifactProjection::WorkspaceId,
                    ArtifactProjection::ArtifactId,
                    ArtifactProjection::ProjectionKind,
                ],
            ),
            (
                "idx_artifact_projection_status",
                vec![ArtifactProjection::WorkspaceId, ArtifactProjection::Status],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table(ArtifactProjection::Table);
            for col in cols {
                index.col(col);
            }
            manager.create_index(index.to_owned()).await?;
        }

        manager
            .create_table(
                Table::create()
                    .table(ArtifactUploadSession::Table)
                    .if_not_exists()
                    .col(
                        string(ArtifactUploadSession::Id)
                            .string_len(64)
                            .primary_key(),
                    )
                    .col(string(ArtifactUploadSession::WorkspaceId).string_len(21))
                    .col(
                        string(ArtifactUploadSession::ThreadId)
                            .string_len(21)
                            .null(),
                    )
                    .col(string(ArtifactUploadSession::TurnId).string_len(21).null())
                    .col(
                        string(ArtifactUploadSession::MessageId)
                            .string_len(64)
                            .null(),
                    )
                    .col(string(ArtifactUploadSession::DisplayName).string_len(255))
                    .col(big_integer(ArtifactUploadSession::ExpectedSizeBytes).null())
                    .col(
                        string(ArtifactUploadSession::ExpectedSha256)
                            .string_len(128)
                            .null(),
                    )
                    .col(
                        string(ArtifactUploadSession::MimeType)
                            .string_len(255)
                            .null(),
                    )
                    .col(string(ArtifactUploadSession::Status).string_len(32))
                    .col(text(ArtifactUploadSession::TempStorageKey))
                    .col(big_integer(ArtifactUploadSession::ReceivedBytes).default(0))
                    .col(
                        timestamp_with_time_zone(ArtifactUploadSession::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(ArtifactUploadSession::UpdatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(timestamp_with_time_zone(ArtifactUploadSession::ExpiresAt))
                    .col(text(ArtifactUploadSession::MetadataJson).default("{}"))
                    .to_owned(),
            )
            .await?;

        for (name, cols) in [
            (
                "idx_artifact_upload_session_workspace_status",
                vec![
                    ArtifactUploadSession::WorkspaceId,
                    ArtifactUploadSession::Status,
                ],
            ),
            (
                "idx_artifact_upload_session_expiry",
                vec![
                    ArtifactUploadSession::WorkspaceId,
                    ArtifactUploadSession::ExpiresAt,
                ],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table(ArtifactUploadSession::Table);
            for col in cols {
                index.col(col);
            }
            manager.create_index(index.to_owned()).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(ArtifactUploadSession::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(ArtifactProjection::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(ArtifactExternalRef::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(ArtifactBinding::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(ArtifactVersion::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(Artifact::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(ArtifactBlob::Table).to_owned())
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("attachment_upload_registry")
                    .if_not_exists()
                    .col(text("registry_key").primary_key())
                    .col(string("provider").string_len(64))
                    .col(string("model_family").string_len(255))
                    .col(string("transport_kind").string_len(32))
                    .col(string("sha256").string_len(128))
                    .col(text("provider_file_id"))
                    .col(big_integer("uploaded_at_unix_ms"))
                    .col(big_integer("ttl_secs"))
                    .col(big_integer("expires_at_unix_ms"))
                    .col(string("mime_type").string_len(255))
                    .col(big_integer("size_bytes"))
                    .col(string("file_name").string_len(255))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_attachment_upload_registry_expires_at")
                    .table("attachment_upload_registry")
                    .col("expires_at_unix_ms")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_attachment_upload_registry_provider_model")
                    .table("attachment_upload_registry")
                    .col("provider")
                    .col("model_family")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_index(
                Index::drop()
                    .name("idx_hook_run_status_queued_at")
                    .table(HookRun::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(HookRun::Table)
                    .drop_column(HookRun::ResumeStateJson)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
