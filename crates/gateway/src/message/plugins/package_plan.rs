//! Two format adapters feeding one installation/lifecycle coordinator.
use super::*;

#[derive(Clone)]
pub(super) struct PackagePresentation {
    pub name: String,
    pub version: Option<String>,
    pub description: Option<String>,
    pub metadata: PluginMetadata,
}
#[derive(Clone)]
pub(super) struct PackagePlan {
    pub kind: PackageKind,
    pub presentation: PackagePresentation,
    pub tree_digest: String,
    pub components: Vec<ComponentPlan>,
    pub diagnostics: Vec<pioneer_plugins::Diagnostic>,
    pub app: Option<pioneer_plugins::app::LoadedAppPackage>,
}
impl From<pioneer_plugins::LoadedPluginPlan> for PackagePlan {
    fn from(plan: pioneer_plugins::LoadedPluginPlan) -> Self {
        Self {
            kind: PackageKind::Plugin,
            presentation: PackagePresentation {
                name: plan.manifest.name,
                version: plan.manifest.version,
                description: plan.manifest.description,
                metadata: plan.manifest.metadata,
            },
            tree_digest: plan.tree_digest,
            components: plan.components,
            diagnostics: plan.diagnostics,
            app: None,
        }
    }
}
pub(super) fn app_metadata(
    manifest: &pioneer_plugins::app::AppManifest,
    images: &std::collections::BTreeMap<String, ImageReference>,
    localizations: &std::collections::BTreeMap<String, serde_json::Value>,
) -> PluginMetadata {
    let document = manifest.document();
    let text = |field: &str| {
        document
            .get(field)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    let image = |value: &serde_json::Value| {
        let path = value.get("light")?.as_str()?;
        images
            .get(&pioneer_plugins::app::canonical_package_path(path, "image").ok()?)
            .cloned()
    };
    let themed = |value: &serde_json::Value| -> Option<AppThemedIcon> {
        let get = |theme: &str| {
            let path = value.get(theme)?.as_str()?;
            images
                .get(&pioneer_plugins::app::canonical_package_path(path, "image").ok()?)
                .cloned()
        };
        Some(AppThemedIcon {
            light: get("light")?,
            dark: get("dark")?,
        })
    };
    let presentation = manifest.presentation("", localizations);
    PluginMetadata {
        app: Some(AppMetadata {
            icon: themed(&document["icon"]),
            composer_icon: themed(manifest.composer_icon()),
            developer_logo: themed(&document["developer"]["logo"]),
            developer_url: document["developer"]["url"].as_str().map(str::to_owned),
            categories: document["categories"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
            default_locale: text("defaultLocale"),
            localizations: localizations
                .iter()
                .map(|(locale, v)| {
                    (
                        locale.clone(),
                        AppLocalization {
                            name: v["name"].as_str().map(str::to_owned),
                            short_description: v["shortDescription"].as_str().map(str::to_owned),
                            long_description: v["longDescription"].as_str().map(str::to_owned),
                        },
                    )
                })
                .collect(),
        }),
        display_name: Some(presentation.name),
        short_description: Some(presentation.short_description.clone()),
        description: Some(presentation.short_description),
        long_description: presentation.long_description,
        developer_name: document["developer"]["name"].as_str().map(str::to_owned),
        homepage: text("websiteURL"),
        repository: text("repositoryURL"),
        support_url: text("supportURL"),
        privacy_policy_url: text("privacyPolicyURL"),
        terms_of_service_url: text("termsOfServiceURL"),
        license: text("license"),
        logo: image(&document["icon"]),
        composer_icon: image(manifest.composer_icon()),
        ..Default::default()
    }
}
pub(super) fn load_package(snapshot: &Snapshot, kind: PackageKind) -> anyhow::Result<PackagePlan> {
    match kind {
        PackageKind::Plugin => Ok(pioneer_plugins::load(snapshot)
            .map_err(|_| anyhow::anyhow!("plugins.package_rejected"))?
            .into()),
        PackageKind::App => {
            let app = pioneer_plugins::app::load(snapshot)?;
            let doc = app.manifest.document();
            let presentation = PackagePresentation {
                name: app.manifest.slug().into(),
                version: doc["version"].as_str().map(str::to_owned),
                description: doc["shortDescription"].as_str().map(str::to_owned),
                metadata: app_metadata(&app.manifest, &app.images, &app.localizations),
            };
            Ok(PackagePlan {
                kind,
                presentation,
                tree_digest: app.tree_digest.clone(),
                components: app.components.clone(),
                diagnostics: app.diagnostics.clone(),
                app: Some(app),
            })
        }
    }
}
