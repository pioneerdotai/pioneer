//! Typed package presentation persistence. Lists are relational rows, never encoded JSON.
use anyhow::{Result, bail};
use pioneer_entity::{plugin_default_prompt, plugin_installation, plugin_keyword};
use pioneer_protocol::{ImageReference, PluginAuthor, PluginMetadata};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};
use std::collections::HashMap;

#[derive(Clone)]
pub(crate) struct PreparedMetadata {
    pub columns: plugin_installation::ActiveModel,
    pub keywords: Vec<plugin_keyword::ActiveModel>,
    pub prompts: Vec<plugin_default_prompt::ActiveModel>,
}
pub(crate) fn prepare(id: &str, metadata: &PluginMetadata) -> Result<PreparedMetadata> {
    if metadata.keywords.len() > 64 || metadata.default_prompts.len() > 12 {
        bail!("plugin metadata list too large");
    }
    Ok(PreparedMetadata {
        columns: plugin_installation::ActiveModel {
            display_name: Set(metadata.display_name.clone()),
            short_description: Set(metadata.short_description.clone()),
            description: Set(metadata.description.clone()),
            long_description: Set(metadata.long_description.clone()),
            developer_name: Set(metadata.developer_name.clone()),
            author_name: Set(metadata.author.name.clone()),
            author_email: Set(metadata.author.email.clone()),
            author_url: Set(metadata.author.url.clone()),
            homepage: Set(metadata.homepage.clone()),
            repository: Set(metadata.repository.clone()),
            documentation_url: Set(metadata.documentation_url.clone()),
            support_url: Set(metadata.support_url.clone()),
            privacy_policy_url: Set(metadata.privacy_policy_url.clone()),
            terms_of_service_url: Set(metadata.terms_of_service_url.clone()),
            license: Set(metadata.license.clone()),
            category: Set(metadata.category.clone()),
            brand_color: Set(metadata.brand_color.clone()),
            logo_path: Set(metadata.logo.as_ref().map(|image| image.path.clone())),
            logo_revision: Set(metadata.logo.as_ref().map(|image| image.revision.clone())),
            composer_icon_path: Set(metadata
                .composer_icon
                .as_ref()
                .map(|image| image.path.clone())),
            composer_icon_revision: Set(metadata
                .composer_icon
                .as_ref()
                .map(|image| image.revision.clone())),
            ..Default::default()
        },
        keywords: metadata
            .keywords
            .iter()
            .enumerate()
            .map(|(position, keyword)| plugin_keyword::ActiveModel {
                plugin_id: Set(id.to_owned()),
                position: Set(position as i64),
                keyword: Set(keyword.clone()),
                ..Default::default()
            })
            .collect(),
        prompts: metadata
            .default_prompts
            .iter()
            .enumerate()
            .map(|(position, prompt)| plugin_default_prompt::ActiveModel {
                plugin_id: Set(id.to_owned()),
                position: Set(position as i64),
                prompt: Set(prompt.clone()),
                ..Default::default()
            })
            .collect(),
    })
}
pub(crate) fn apply_columns(
    row: &mut plugin_installation::ActiveModel,
    columns: plugin_installation::ActiveModel,
) {
    row.display_name = columns.display_name;
    row.short_description = columns.short_description;
    row.description = columns.description;
    row.long_description = columns.long_description;
    row.developer_name = columns.developer_name;
    row.author_name = columns.author_name;
    row.author_email = columns.author_email;
    row.author_url = columns.author_url;
    row.homepage = columns.homepage;
    row.repository = columns.repository;
    row.documentation_url = columns.documentation_url;
    row.support_url = columns.support_url;
    row.privacy_policy_url = columns.privacy_policy_url;
    row.terms_of_service_url = columns.terms_of_service_url;
    row.license = columns.license;
    row.category = columns.category;
    row.brand_color = columns.brand_color;
    row.logo_path = columns.logo_path;
    row.logo_revision = columns.logo_revision;
    row.composer_icon_path = columns.composer_icon_path;
    row.composer_icon_revision = columns.composer_icon_revision;
}
/// Caller has validated/reserved the parent under this same writer transaction.
pub(crate) async fn replace_lists(
    db: &pioneer_sqlite::SqliteTransaction,
    id: &str,
    keywords: Vec<plugin_keyword::ActiveModel>,
    prompts: Vec<plugin_default_prompt::ActiveModel>,
) -> Result<()> {
    plugin_keyword::Entity::delete_many()
        .filter(plugin_keyword::Column::PluginId.eq(id))
        .exec(db)
        .await?;
    plugin_default_prompt::Entity::delete_many()
        .filter(plugin_default_prompt::Column::PluginId.eq(id))
        .exec(db)
        .await?;
    if !keywords.is_empty() {
        plugin_keyword::Entity::insert_many(keywords)
            .exec_without_returning(db)
            .await?;
    }
    if !prompts.is_empty() {
        plugin_default_prompt::Entity::insert_many(prompts)
            .exec_without_returning(db)
            .await?;
    }
    Ok(())
}
pub(crate) async fn load<C: ConnectionTrait>(
    db: &C,
    ids: &[String],
) -> Result<(
    Vec<plugin_installation::Model>,
    Vec<plugin_keyword::Model>,
    Vec<plugin_default_prompt::Model>,
)> {
    let parents = plugin_installation::Entity::find()
        .filter(plugin_installation::Column::Id.is_in(ids.iter().cloned()))
        .all(db)
        .await?;
    let keywords = plugin_keyword::Entity::find()
        .filter(plugin_keyword::Column::PluginId.is_in(ids.iter().cloned()))
        .order_by_asc(plugin_keyword::Column::PluginId)
        .order_by_asc(plugin_keyword::Column::Position)
        .all(db)
        .await?;
    let prompts = plugin_default_prompt::Entity::find()
        .filter(plugin_default_prompt::Column::PluginId.is_in(ids.iter().cloned()))
        .order_by_asc(plugin_default_prompt::Column::PluginId)
        .order_by_asc(plugin_default_prompt::Column::Position)
        .all(db)
        .await?;
    Ok((parents, keywords, prompts))
}
/// Runs only after releasing the read snapshot; every list is bounded by schema positions.
pub(crate) fn project(
    parents: Vec<plugin_installation::Model>,
    keywords: Vec<plugin_keyword::Model>,
    prompts: Vec<plugin_default_prompt::Model>,
) -> HashMap<String, (i64, PluginMetadata)> {
    let mut keyword_lists: HashMap<String, Vec<String>> = HashMap::new();
    let mut prompt_lists: HashMap<String, Vec<String>> = HashMap::new();
    for row in keywords {
        keyword_lists
            .entry(row.plugin_id)
            .or_default()
            .push(row.keyword);
    }
    for row in prompts {
        prompt_lists
            .entry(row.plugin_id)
            .or_default()
            .push(row.prompt);
    }
    parents
        .into_iter()
        .filter(|parent| parent.state == "installed" && parent.pending_json.is_none())
        .map(|parent| {
            let metadata = PluginMetadata {
                display_name: parent.display_name.clone(),
                short_description: parent.short_description.clone(),
                description: parent.description.clone(),
                long_description: parent.long_description.clone(),
                developer_name: parent.developer_name.clone(),
                homepage: parent.homepage.clone(),
                repository: parent.repository.clone(),
                documentation_url: parent.documentation_url.clone(),
                support_url: parent.support_url.clone(),
                privacy_policy_url: parent.privacy_policy_url.clone(),
                terms_of_service_url: parent.terms_of_service_url.clone(),
                license: parent.license.clone(),
                category: parent.category.clone(),
                brand_color: parent.brand_color.clone(),
                author: PluginAuthor {
                    name: parent.author_name.clone(),
                    email: parent.author_email.clone(),
                    url: parent.author_url.clone(),
                },
                logo: parent
                    .logo_path
                    .clone()
                    .zip(parent.logo_revision.clone())
                    .map(|(path, revision)| ImageReference { path, revision }),
                composer_icon: parent
                    .composer_icon_path
                    .clone()
                    .zip(parent.composer_icon_revision.clone())
                    .map(|(path, revision)| ImageReference { path, revision }),
                keywords: keyword_lists.remove(&parent.id).unwrap_or_default(),
                default_prompts: prompt_lists.remove(&parent.id).unwrap_or_default(),
            };
            (parent.id, (parent.revision, metadata))
        })
        .collect()
}
