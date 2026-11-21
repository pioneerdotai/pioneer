use anyhow::{Context, Result, ensure};
use pioneer_entity::{
    artifact, artifact_binding, artifact_version, html_preview_frozen_result as receipt, thread,
    turn,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Select, Set};
use serde_json::{Value, json};

pub fn find_frozen_result(manifest: impl AsRef<str>, ordinal: i64) -> Select<receipt::Entity> {
    receipt::Entity::find()
        .filter(receipt::Column::ManifestId.eq(manifest.as_ref()))
        .filter(receipt::Column::Ordinal.eq(ordinal))
}

/// Compact only the exact canonical preview result. Pairing, name, call id,
/// diagnostics and ordered result remain; no source/image bytes are copied.
pub fn archive_result(payload: &str) -> Result<String> {
    let mut view: Value = serde_json::from_str(payload)?;
    ensure!(
        view["kind"] == "json" && view["truncated"] == false,
        "incomplete canonical preview result"
    );
    let message = &mut view["value"];
    ensure!(
        message["role"] == "tool"
            && message["name"] == "html_preview"
            && message["tool_call_id"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
        "preview result identity mismatch"
    );
    let mut diagnostics: Value = serde_json::from_str(
        message["content"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("preview result content is not JSON"))?,
    )?;
    let object = diagnostics
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("preview diagnostics are not an object"))?;
    object.remove("llm_context");
    object.insert(
        "previewRetention".into(),
        json!("released; temporary images are no longer retained"),
    );
    message["content"] = serde_json::to_string(&diagnostics)?.into();
    message.as_object_mut().unwrap().remove("content_parts");
    Ok(serde_json::to_string(&view)?)
}

fn metadata_field(key: &'static str) -> Expr {
    Func::cust(Alias::new("json_extract_path_text"))
        .args([
            Expr::col(artifact::Column::MetadataJson).cast_as("json"),
            Expr::val("htmlTool"),
            Expr::val(key),
        ])
        .into()
}

/// Late durable delivery/retry must observe the same archived representation
/// after a release commit; it cannot republish deleted image dependencies.
pub async fn preview_result_was_released<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    call: &str,
) -> Result<bool> {
    let query = artifact::Entity::find()
        .select_only()
        .column(artifact::Column::Id)
        .filter(Expr::col(artifact::Column::CreatedByKind).eq(Expr::Constant("tool".into())))
        .filter(
            Expr::col(artifact::Column::CreatedByActorId).eq(Expr::Constant("html_preview".into())),
        )
        .filter(artifact::Column::CurrentVersionId.is_null())
        .filter(metadata_field("turnId").eq(turn))
        .filter(metadata_field("callId").eq(call))
        .filter(metadata_field("retention").eq("released"))
        .into_tuple::<String>();
    Ok(query.one(db).await?.is_some())
}

/// A supported canonical UserInput is a real adoption even when Message edits
/// do not create a UserMessage item. Prepare IDs outside the projection commit;
/// ordinary artifacts keep their existing binding behavior.
#[derive(Clone, Debug)]
pub struct PreparedPreviewInputAdoption {
    turn: String,
    artifact: String,
    version: Option<String>,
    binding: String,
    index: i64,
    at: chrono::DateTime<chrono::FixedOffset>,
}
impl PreparedPreviewInputAdoption {
    pub fn new(
        turn: &str,
        artifact: &str,
        version: Option<&str>,
        index: i64,
        at: chrono::DateTime<chrono::FixedOffset>,
    ) -> Self {
        Self {
            turn: turn.into(),
            artifact: artifact.into(),
            version: version.map(str::to_owned),
            binding: pioneer_protocol::generate_id(21),
            index,
            at,
        }
    }
}

pub async fn retain_preview_input<C: ConnectionTrait>(
    db: &C,
    input: &PreparedPreviewInputAdoption,
) -> Result<()> {
    // Retirement locks the same row before checking adoption bindings. Once
    // acquired, resolving an implicit version and publishing its binding are
    // part of the caller's input-projection transaction.
    let preview = artifact::Entity::find_by_id(&input.artifact)
        .filter(artifact::Column::CreatedByKind.eq("tool"))
        .filter(artifact::Column::CreatedByActorId.eq("html_preview"))
        .select_only()
        .column(artifact::Column::WorkspaceId)
        .column(artifact::Column::CurrentVersionId)
        .lock_exclusive()
        .into_tuple::<(String, Option<String>)>()
        .one(db)
        .await?;
    let Some((workspace, current_version)) = preview else {
        return Ok(());
    };
    let Some(thread_id) = turn::Entity::find_by_id(&input.turn)
        .select_only()
        .column(turn::Column::ThreadId)
        .into_tuple::<String>()
        .one(db)
        .await?
    else {
        return Ok(());
    };
    let Some(target_workspace) = thread::Entity::find_by_id(&thread_id)
        .select_only()
        .column(thread::Column::WorkspaceId)
        .into_tuple::<String>()
        .one(db)
        .await?
    else {
        return Ok(());
    };
    ensure!(
        target_workspace == workspace,
        "preview input crosses workspace"
    );
    let version = input
        .version
        .clone()
        .or(current_version)
        .context("released preview input is unavailable")?;
    let available = artifact_version::Entity::find_by_id(&version)
        .select_only()
        .column(artifact_version::Column::Id)
        .filter(artifact_version::Column::ArtifactId.eq(&input.artifact))
        .filter(artifact_version::Column::WorkspaceId.eq(&workspace))
        .into_tuple::<String>()
        .one(db)
        .await?;
    ensure!(
        available.is_some(),
        "released preview input version is unavailable"
    );

    let existing = artifact_binding::Entity::find()
        .select_only()
        .column(artifact_binding::Column::Id)
        .filter(artifact_binding::Column::ArtifactId.eq(&input.artifact))
        .filter(artifact_binding::Column::ArtifactVersionId.eq(&version))
        .filter(artifact_binding::Column::WorkspaceId.eq(&workspace))
        .filter(artifact_binding::Column::ThreadId.eq(&thread_id))
        .filter(artifact_binding::Column::TurnId.eq(&input.turn))
        .filter(artifact_binding::Column::BindingKind.eq("user_input"))
        .filter(artifact_binding::Column::Direction.eq("input"))
        .into_tuple::<String>()
        .one(db)
        .await?;
    if existing.is_none() {
        artifact_binding::Entity::insert(artifact_binding::ActiveModel {
            id: Set(input.binding.clone()),
            artifact_id: Set(input.artifact.clone()),
            artifact_version_id: Set(Some(version)),
            workspace_id: Set(workspace),
            thread_id: Set(Some(thread_id)),
            turn_id: Set(Some(input.turn.clone())),
            binding_kind: Set("user_input".to_owned()),
            direction: Set("input".to_owned()),
            item_index: Set(Some(input.index)),
            role: Set(Some("user".to_owned())),
            created_at: Set(input.at),
            metadata_json: Set("{}".to_owned()),
            ..Default::default()
        })
        .exec_without_returning(db)
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_postgres_test::with_database;
    use sea_orm::TransactionTrait;

    #[tokio::test]
    async fn versionless_input_preserves_the_bound_version_when_current_changes() -> Result<()> {
        with_database(async |fixture| {
            let db = fixture.connection();
            db.execute_unprepared(r#"
                INSERT INTO workspace(id,name) VALUES ('ws','Workspace');
                INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
                VALUES ('parent','ws','','agent','model','provider','active');
                INSERT INTO turn(id,thread_id,status) VALUES ('parent-turn','parent','in_progress');
                INSERT INTO artifact_blob(id,workspace_id,sha256,size_bytes,storage_backend,storage_key)
                VALUES ('blob','ws','fixture-sha',1,'local','fixture-key');
                INSERT INTO artifact(id,workspace_id,primary_thread_id,display_name,kind,mime_type,status,created_by_kind,created_by_actor_id,metadata_json)
                VALUES ('preview','ws','parent','Preview','screenshot','image/png','ready','tool','html_preview',
                    '{"htmlTool":{"version":1,"publication":false,"turnId":"parent-turn","callId":"preview-call"}}');
                INSERT INTO artifact_version(id,workspace_id,artifact_id,version_number,blob_id)
                VALUES ('v1','ws','preview',1,'blob'),('v2','ws','preview',2,'blob');
                UPDATE artifact SET current_version_id='v1' WHERE id='preview';
                INSERT INTO artifact_binding(id,artifact_id,artifact_version_id,workspace_id,thread_id,turn_id,tool_call_id,binding_kind,direction,role)
                VALUES ('output','preview','v1','ws','parent','parent-turn','preview-call','tool_output','output','tool');
            "#).await?;
            let temporary = async || {
                artifact::Entity::find_by_id("preview")
                    .filter(crate::visibility::temporary_preview_predicate("artifact"))
                    .one(db)
                    .await
                    .map(|model| model.is_some())
            };
            assert!(temporary().await?);
            let now = chrono::Utc::now().fixed_offset();
            let input = PreparedPreviewInputAdoption::new("parent-turn", "preview", None, 0, now);
            let tx = db.begin().await?;
            retain_preview_input(&tx, &input).await?;
            assert!(temporary().await?);
            tx.rollback().await?;
            assert!(temporary().await?);
            let tx = db.begin().await?;
            retain_preview_input(&tx, &input).await?;
            retain_preview_input(&tx, &input).await?;
            tx.commit().await?;
            assert!(!temporary().await?);
            db.execute_unprepared("UPDATE artifact SET current_version_id='v2' WHERE id='preview'").await?;
            assert!(!temporary().await?);
            let bindings = artifact_binding::Entity::find()
                .filter(artifact_binding::Column::BindingKind.eq("user_input"))
                .all(db).await?;
            assert_eq!(bindings.len(), 1);
            assert_eq!(bindings[0].artifact_version_id.as_deref(), Some("v1"));
            Ok(())
        }).await
    }
}
