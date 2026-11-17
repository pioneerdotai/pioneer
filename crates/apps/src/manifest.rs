use super::json;
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestError {
    pub code: &'static str,
    pub field: String,
}
impl ManifestError {
    pub(super) fn new(code: &'static str, field: &str) -> Self {
        Self {
            code,
            field: field.into(),
        }
    }
}
impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.field)
    }
}
impl std::error::Error for ManifestError {}
type Result<T> = std::result::Result<T, ManifestError>;

/// Validated authored document; never contains runtime identity or authority.
#[derive(Debug, Clone)]
pub struct AppManifest {
    document: Value,
    locales: BTreeMap<String, String>,
    runtime: Option<crate::runtime::Runtime>,
    ui: Option<crate::runtime::Ui>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppPresentation {
    pub name: String,
    pub short_description: String,
    pub long_description: Option<String>,
}

impl AppManifest {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let document = json::parse(bytes, "app.json")?;
        let m = object(
            &document,
            "app.json",
            &[
                "slug",
                "name",
                "version",
                "shortDescription",
                "longDescription",
                "developer",
                "icon",
                "composerIcon",
                "license",
                "categories",
                "websiteURL",
                "supportURL",
                "privacyPolicyURL",
                "termsOfServiceURL",
                "repositoryURL",
                "skills",
                "mcp",
                "locales",
                "defaultLocale",
                "runtime",
                "ui",
            ],
        )?;
        let slug = required(m, "slug", "app.json", 64)?;
        if slug.split('-').any(|s| {
            s.is_empty()
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        }) {
            return Err(ManifestError::new("invalid_slug", "app.json/slug"));
        }
        required(m, "name", "app.json", 80)?;
        required(m, "shortDescription", "app.json", 240)?;
        let version = required(m, "version", "app.json", 256)?;
        semver::Version::parse(version)
            .map_err(|_| ManifestError::new("invalid_version", "app.json/version"))?;
        optional(m, "longDescription", "app.json", 16000)?;
        optional(m, "license", "app.json", 256)?;
        image(
            m.get("icon")
                .ok_or_else(|| ManifestError::new("missing_field", "app.json/icon"))?,
            "app.json/icon",
        )?;
        if let Some(v) = m.get("composerIcon") {
            image(v, "app.json/composerIcon")?;
        }
        let developer = object(
            m.get("developer")
                .ok_or_else(|| ManifestError::new("missing_field", "app.json/developer"))?,
            "app.json/developer",
            &["name", "logo", "url"],
        )?;
        required(developer, "name", "app.json/developer", 120)?;
        if let Some(v) = developer.get("logo") {
            image(v, "app.json/developer/logo")?;
        }
        https(developer, "url", "app.json/developer")?;
        for key in [
            "websiteURL",
            "supportURL",
            "privacyPolicyURL",
            "termsOfServiceURL",
            "repositoryURL",
        ] {
            https(m, key, "app.json")?;
        }
        if let Some(v) = m.get("categories") {
            let mut seen = BTreeSet::new();
            for (i, v) in array(v, "app.json/categories", 8)?.iter().enumerate() {
                let p = format!("app.json/categories/{i}");
                let s = string(v, &p, 64)?;
                if ![
                    "development",
                    "productivity",
                    "communication",
                    "education",
                    "finance",
                    "utilities",
                ]
                .contains(&s)
                {
                    return Err(ManifestError::new("invalid_category", &p));
                }
                if !seen.insert(s) {
                    return Err(ManifestError::new("duplicate_category", &p));
                }
            }
        }
        if let Some(v) = m.get("skills") {
            path(string(v, "app.json/skills", 4096)?, "app.json/skills")?;
        }
        if let Some(v) = m.get("mcp") {
            let mcp = object(v, "app.json/mcp", &["config", "include"])?;
            path(
                required(mcp, "config", "app.json/mcp", 4096)?,
                "app.json/mcp/config",
            )?;
            if let Some(v) = mcp.get("include") {
                for (i, v) in array(v, "app.json/mcp/include", 256)?.iter().enumerate() {
                    let p = format!("app.json/mcp/include/{i}");
                    path(string(v, &p, 4096)?, &p)?;
                }
            }
        }
        let default = optional(m, "defaultLocale", "app.json", 128)?
            .map(|s| locale(s, "app.json/defaultLocale"))
            .transpose()?;
        let mut locales = BTreeMap::new();
        if let Some(v) = m.get("locales") {
            let map = v
                .as_object()
                .ok_or_else(|| ManifestError::new("invalid_type", "app.json/locales"))?;
            if map.is_empty() || map.len() > 64 {
                return Err(ManifestError::new("locale_limit", "app.json/locales"));
            }
            for (key, value) in map {
                let p = format!(
                    "app.json/locales/{}",
                    key.replace('~', "~0").replace('/', "~1")
                );
                let canonical = locale(key, &p)?;
                if locales
                    .insert(canonical, path(string(value, &p, 4096)?, &p)?)
                    .is_some()
                {
                    return Err(ManifestError::new("duplicate_locale", &p));
                }
            }
            if !default.as_ref().is_some_and(|d| locales.contains_key(d)) {
                return Err(ManifestError::new(
                    "invalid_default_locale",
                    "app.json/defaultLocale",
                ));
            }
        }
        let mut runtime: Option<crate::runtime::Runtime> = m
            .get("runtime")
            .map(|v| {
                serde_json::from_value(v.clone())
                    .map_err(|_| ManifestError::new("invalid_runtime", "app.json/runtime"))
            })
            .transpose()?;
        if let Some(runtime) = &mut runtime {
            runtime.validate()?;
        }
        let ui: Option<crate::runtime::Ui> = m
            .get("ui")
            .map(|v| {
                serde_json::from_value(v.clone())
                    .map_err(|_| ManifestError::new("invalid_ui", "app.json/ui"))
            })
            .transpose()?;
        if let Some(ui) = &ui {
            if runtime.is_none() || !crate::runtime::valid_http_path(&ui.path) {
                return Err(ManifestError::new("invalid_ui", "app.json/ui/path"));
            }
        }
        Ok(Self {
            document,
            locales,
            runtime,
            ui,
        })
    }
    pub fn runtime(&self) -> Option<&crate::runtime::Runtime> {
        self.runtime.as_ref()
    }
    pub fn ui(&self) -> Option<&crate::runtime::Ui> {
        self.ui.as_ref()
    }
    pub fn document(&self) -> &Value {
        &self.document
    }
    pub fn slug(&self) -> &str {
        self.document["slug"].as_str().expect("validated slug")
    }
    pub fn locales(&self) -> &BTreeMap<String, String> {
        &self.locales
    }
    pub fn composer_icon(&self) -> &Value {
        self.document
            .get("composerIcon")
            .unwrap_or(&self.document["icon"])
    }
    /// Validate every declared translation; missing/invalid files are errors, even if not selected.
    pub fn load_localizations<'a>(
        &self,
        mut file: impl FnMut(&str) -> Option<&'a [u8]>,
    ) -> Result<BTreeMap<String, Value>> {
        let mut result = BTreeMap::new();
        for (locale, p) in &self.locales {
            let v = json::parse(
                file(p).ok_or_else(|| ManifestError::new("missing_locale", p))?,
                p,
            )?;
            let m = object(&v, p, &["name", "shortDescription", "longDescription"])?;
            for (key, limit) in [
                ("name", 80),
                ("shortDescription", 240),
                ("longDescription", 16000),
            ] {
                optional(m, key, p, limit)?;
            }
            result.insert(locale.clone(), v);
        }
        Ok(result)
    }
    /// Validate the stored translation snapshot with the same strict JSON rules.
    /// It must cover exactly the declared canonical locales, never extra authority.
    pub fn validate_localizations_json(&self, bytes: &[u8]) -> Result<BTreeMap<String, Value>> {
        let value = json::parse(bytes, "localizations")?;
        let map = value
            .as_object()
            .ok_or_else(|| ManifestError::new("invalid_type", "localizations"))?;
        if map.len() != self.locales.len() || map.keys().any(|key| !self.locales.contains_key(key))
        {
            return Err(ManifestError::new(
                "locale_snapshot_mismatch",
                "localizations",
            ));
        }
        for (key, value) in map {
            let field = format!("localizations/{key}");
            let m = object(
                value,
                &field,
                &["name", "shortDescription", "longDescription"],
            )?;
            for (key, limit) in [
                ("name", 80),
                ("shortDescription", 240),
                ("longDescription", 16000),
            ] {
                optional(m, key, &field, limit)?;
            }
        }
        Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
    pub fn presentation(
        &self,
        requested: &str,
        localizations: &BTreeMap<String, Value>,
    ) -> AppPresentation {
        let exact = locale(requested, "locale").ok();
        let base = exact.as_deref().and_then(|s| s.split('-').next());
        let default = self
            .document
            .get("defaultLocale")
            .and_then(Value::as_str)
            .and_then(|s| locale(s, "locale").ok());
        let text = |key: &str| {
            exact
                .as_deref()
                .into_iter()
                .chain(base)
                .chain(default.as_deref())
                .filter_map(|l| localizations.get(l))
                .find_map(|v| v.get(key).and_then(Value::as_str))
                .or_else(|| self.document.get(key).and_then(Value::as_str))
                .map(str::to_owned)
        };
        AppPresentation {
            name: text("name").expect("validated name"),
            short_description: text("shortDescription").expect("validated description"),
            long_description: text("longDescription"),
        }
    }
}
fn object<'a>(v: &'a Value, p: &str, allowed: &[&str]) -> Result<&'a Map<String, Value>> {
    let m = v
        .as_object()
        .ok_or_else(|| ManifestError::new("invalid_type", p))?;
    for k in m.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(ManifestError::new(
                "unknown_field",
                &format!("{p}/{}", k.replace('~', "~0").replace('/', "~1")),
            ));
        }
    }
    Ok(m)
}
fn string<'a>(v: &'a Value, p: &str, max: usize) -> Result<&'a str> {
    let s = v
        .as_str()
        .ok_or_else(|| ManifestError::new("invalid_type", p))?;
    if s.trim().is_empty() || s.chars().count() > max {
        return Err(ManifestError::new("invalid_string", p));
    }
    Ok(s)
}
fn required<'a>(m: &'a Map<String, Value>, k: &str, p: &str, max: usize) -> Result<&'a str> {
    let p = format!("{p}/{k}");
    string(
        m.get(k)
            .ok_or_else(|| ManifestError::new("missing_field", &p))?,
        &p,
        max,
    )
}
fn optional<'a>(
    m: &'a Map<String, Value>,
    k: &str,
    p: &str,
    max: usize,
) -> Result<Option<&'a str>> {
    m.get(k)
        .map(|v| string(v, &format!("{p}/{k}"), max))
        .transpose()
}
fn array<'a>(v: &'a Value, p: &str, max: usize) -> Result<&'a Vec<Value>> {
    let a = v
        .as_array()
        .ok_or_else(|| ManifestError::new("invalid_type", p))?;
    if a.len() > max {
        return Err(ManifestError::new("count_limit", p));
    }
    Ok(a)
}
fn https(m: &Map<String, Value>, k: &str, p: &str) -> Result<()> {
    if let Some(s) = optional(m, k, p, 2048)? {
        let u = url::Url::parse(s)
            .map_err(|_| ManifestError::new("invalid_url", &format!("{p}/{k}")))?;
        if u.scheme() != "https" || u.host_str().is_none() || s.trim() != s {
            return Err(ManifestError::new("invalid_url", &format!("{p}/{k}")));
        }
    }
    Ok(())
}
fn image(v: &Value, p: &str) -> Result<()> {
    let m = object(v, p, &["light", "dark"])?;
    for k in ["light", "dark"] {
        path(required(m, k, p, 4096)?, &format!("{p}/{k}"))?;
    }
    Ok(())
}
pub fn path(s: &str, p: &str) -> Result<String> {
    let s = s.strip_prefix("./").unwrap_or(s);
    let s = s.strip_suffix('/').unwrap_or(s);
    if s.is_empty()
        || s.contains(['\\', '\0', ':'])
        || s.split('/').any(|s| matches!(s, "" | "." | ".."))
    {
        return Err(ManifestError::new("invalid_path", p));
    }
    Ok(s.into())
}
fn locale(s: &str, p: &str) -> Result<String> {
    pioneer_protocol::canonical_app_locale(s).map_err(|_| ManifestError::new("invalid_locale", p))
}
