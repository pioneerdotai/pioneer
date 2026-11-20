use anyhow::{Context, Result};
use pioneer_crud_packages::skill_pack_installation as pack;
use pioneer_postgres_test::with_database;
use pioneer_protocol::SkillPackId;

fn record(id: char, name: &str, scope: &str) -> Result<pack::SkillPackInstallationRecord> {
    Ok(pack::SkillPackInstallationRecord {
        pack_id: SkillPackId::new(id.to_string().repeat(21))?,
        name: name.into(),
        scope_key: scope.into(),
        source_kind: "user".into(),
        created_at_unix: 1_700_000_000,
        updated_at_unix: 1_700_000_000,
    })
}

#[tokio::test]
async fn skill_pack_parents_are_scoped_ordered_and_independently_addressable() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        let second = record('B', "same-name", "workspace-one")?;
        let first = record('A', "same-name", "workspace-one")?;
        let other_scope = record('C', "other", "workspace-two")?;
        for parent in [&second, &first, &other_scope] {
            pack::insert_skill_pack_installation(db, parent).await?;
        }

        let rows = pack::list_skill_pack_installations(db, "workspace-one")
            .await?
            .into_iter()
            .map(pack::skill_pack_installation_record_from_model)
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            rows.iter()
                .map(|row| row.pack_id.clone())
                .collect::<Vec<_>>(),
            vec![first.pack_id.clone(), second.pack_id],
        );
        let rows = pack::list_skill_pack_installations(db, "workspace-two")
            .await?
            .into_iter()
            .map(pack::skill_pack_installation_record_from_model)
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(rows, vec![other_scope]);
        assert!(
            pack::find_skill_pack_installation(db, "workspace-one", &first.pack_id)
                .await?
                .is_some()
        );
        assert!(
            pack::find_skill_pack_installation(db, "workspace-two", &first.pack_id)
                .await?
                .is_none()
        );

        let updated = chrono::DateTime::from_timestamp(1_700_000_100, 0)
            .context("test timestamp")?
            .fixed_offset();
        assert!(
            pack::update_skill_pack_installation_name(
                db,
                "workspace-one",
                &first.pack_id,
                "renamed-empty",
                updated
            )
            .await?
        );
        let model = pack::find_skill_pack_installation(db, "workspace-one", &first.pack_id)
            .await?
            .context("renamed empty parent should exist")?;
        let renamed = pack::skill_pack_installation_record_from_model(model)?;
        assert_eq!(renamed.name, "renamed-empty");
        assert_eq!(renamed.updated_at_unix, 1_700_000_100);
        assert!(pack::delete_skill_pack_installation(db, "workspace-one", &first.pack_id).await?);
        Ok(())
    })
    .await
}
