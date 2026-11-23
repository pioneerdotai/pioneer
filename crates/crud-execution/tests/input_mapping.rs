use anyhow::{Context, Result};
use pioneer_crud_execution::input_mapping::InputMapping;
use pioneer_entity::turn_cli_runtime_binding as binding;
use pioneer_postgres_test::with_database;
use sea_orm::sea_query::OnConflict;
use sea_orm::{EntityTrait, QuerySelect, Set, TransactionTrait};

#[tokio::test]
async fn reader_keeps_a_whole_mapping_across_replacement_and_reads_the_inline_flag() -> Result<()> {
    with_database(async |fixture| {
        let original = format!(
            r#"{{ "data": "{}", "pioneerContextBasis": {{ "historyJson": "[{{}}]" }} }}"#,
            "界🙂".repeat(24_000),
        );
        let replacement = "{}";
        let writer = fixture.connect().await?;
        let reader = fixture.connection();
        let first = InputMapping::prepare(&original, false)?;
        let second = InputMapping::prepare(replacement, false)?;
        let insert = writer.begin().await?;
        let mut active = binding::ActiveModel {
            turn_id: Set("turn".into()),
            thread_id: Set("thread".into()),
            continuation_thread_id: Set("thread".into()),
            workspace_id: Set("workspace".into()),
            runtime_id: Set("runtime".into()),
            runtime_kind: Set("codex".into()),
            native_thread_id: Set("native-id".into()),
            status: Set("running".into()),
            frozen_manifest_id: Set(None),
            ..Default::default()
        };
        first.set(&mut active)?;
        binding::Entity::insert(active.clone())
            .exec_without_returning(&insert)
            .await?;
        insert.commit().await?;

        let read = reader.begin().await?;
        let metadata = binding::Entity::find_by_id("turn")
            .select_only()
            .column(binding::Column::InputMappingHasInlineHistory)
            .column(binding::Column::InputMappingCodec)
            .column(binding::Column::InputMappingBytes)
            .into_tuple::<(bool, i16, i64)>()
            .one(&read)
            .await?
            .context("missing CLI mapping metadata")?;
        assert_eq!(metadata, (true, 1, i64::try_from(original.len())?));
        let mut captured = binding::Entity::find_by_id("turn")
            .one(&read)
            .await?
            .context("missing CLI input mapping")?;
        let original_revision = captured.portable_revision;
        let captured = InputMapping::take(&mut captured)?;
        second.set(&mut active)?;
        binding::Entity::insert(active)
            .on_conflict(
                OnConflict::column(binding::Column::TurnId)
                    .update_columns([
                        binding::Column::InputMappingJson,
                        binding::Column::InputMappingCodec,
                        binding::Column::InputMappingBytes,
                        binding::Column::InputMappingChars,
                        binding::Column::InputMappingSha256,
                        binding::Column::InputMappingHasInlineHistory,
                    ])
                    .to_owned(),
            )
            .exec_without_returning(&writer)
            .await?;
        read.commit().await?;
        assert_eq!(captured.decode()?, original);
        let mut saved = binding::Entity::find_by_id("turn")
            .one(reader)
            .await?
            .context("missing replacement mapping")?;
        assert_eq!(saved.portable_revision, original_revision + 1);
        assert!(!saved.input_mapping_has_inline_history);
        assert_eq!(saved.input_mapping_codec, 0);
        assert_eq!(InputMapping::take(&mut saved)?.decode()?, replacement);
        writer.close().await?;
        Ok(())
    })
    .await
}
