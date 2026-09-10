//! Executable, handwritten ownership contract for production composition.
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};
use syn::visit::{self, Visit};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Contract {
    version: u32,
    platforms: Vec<String>,
    ui_cases: Vec<String>,
    scene_caches: Vec<String>,
    custom_elements: Vec<String>,
    foundation_modules: Vec<String>,
    foundation_sources: Vec<String>,
    framework_hosts: Vec<String>,
    features: Vec<Feature>,
    units: BTreeMap<String, Unit>,
    supporting_units: BTreeMap<String, Vec<String>>,
    caches: BTreeMap<String, toml::Value>,
    divergence_owners: BTreeMap<String, String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Feature {
    package: String,
    path: String,
    root: String,
    constructor: String,
    public_modules: Vec<String>,
    edges: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Unit {
    kind: String,
    owner: String,
    symbol: String,
    file: String,
}
fn contract() -> Contract {
    toml::from_str(include_str!("desktop_architecture_contract.toml"))
        .expect("handwritten contract must parse")
}
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned()
}
fn source(file: &str) -> String {
    fs::read_to_string(root().join("crates").join(file)).unwrap_or_else(|e| panic!("{file}: {e}"))
}
fn sources(path: &Path) -> Vec<PathBuf> {
    let mut result = vec![];
    for entry in fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            result.extend(sources(&path));
        } else if path.extension().is_some_and(|x| x == "rs") {
            result.push(path);
        }
    }
    result.sort();
    result
}
fn ast(file: &Path) -> syn::File {
    syn::parse_file(&fs::read_to_string(file).unwrap())
        .unwrap_or_else(|e| panic!("{}: {e}", file.display()))
}
fn is_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("cfg") && a.parse_args::<syn::Ident>().is_ok_and(|i| i == "test")
    })
}
fn production_items(items: &[syn::Item]) -> Vec<&syn::Item> {
    items
        .iter()
        .filter(|item| !match item {
            syn::Item::Mod(m) => is_test(&m.attrs),
            syn::Item::Fn(f) => is_test(&f.attrs),
            syn::Item::Impl(i) => is_test(&i.attrs),
            syn::Item::Struct(s) => is_test(&s.attrs),
            _ => false,
        })
        .collect()
}
fn dependencies(value: &toml::Value) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    for table in ["dependencies", "build-dependencies"] {
        if let Some(deps) = value.get(table).and_then(toml::Value::as_table) {
            result.extend(deps.iter().map(|(name, spec)| {
                spec.get("package")
                    .and_then(toml::Value::as_str)
                    .unwrap_or(name)
                    .to_owned()
            }));
        }
    }
    if let Some(targets) = value.get("target").and_then(toml::Value::as_table) {
        for target in targets.values() {
            result.extend(dependencies(target));
        }
    }
    result
}
fn manifests() -> BTreeMap<String, (PathBuf, toml::Value)> {
    let mut result = BTreeMap::new();
    for entry in fs::read_dir(root().join("crates")).unwrap() {
        let path = entry.unwrap().path().join("Cargo.toml");
        if !path.is_file() {
            continue;
        }
        let value: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        let name = value["package"]["name"].as_str().unwrap().to_owned();
        result.insert(name, (path, value));
    }
    result
}
fn acyclic(graph: &BTreeMap<String, BTreeSet<String>>) -> bool {
    fn visit(
        node: &str,
        graph: &BTreeMap<String, BTreeSet<String>>,
        active: &mut BTreeSet<String>,
        done: &mut BTreeSet<String>,
    ) -> bool {
        if done.contains(node) {
            return true;
        }
        if !active.insert(node.to_owned()) {
            return false;
        }
        if let Some(edges) = graph.get(node) {
            for edge in edges {
                if !visit(edge, graph, active, done) {
                    return false;
                }
            }
        }
        active.remove(node);
        done.insert(node.to_owned());
        true
    }
    let mut done = BTreeSet::new();
    graph
        .keys()
        .all(|node| visit(node, graph, &mut BTreeSet::new(), &mut done))
}
#[test]
fn desktop_dependency_graph_is_acyclic() {
    let manifests = manifests();
    let graph: BTreeMap<_, _> = manifests
        .iter()
        .map(|(name, (_, m))| {
            (
                name.clone(),
                dependencies(m)
                    .into_iter()
                    .filter(|d| manifests.contains_key(d))
                    .collect(),
            )
        })
        .collect();
    assert!(acyclic(&graph));
    let mut negative = graph.clone();
    negative
        .entry("pioneer-client".into())
        .or_default()
        .insert("pioneer-desktop".into());
    assert!(
        !acyclic(&negative),
        "cycle detector must reject a reverse shell dependency"
    );
    for feature in contract().features {
        let edges = &graph[&feature.package];
        assert_eq!(
            *edges,
            feature.edges.into_iter().collect(),
            "{} edges",
            feature.package
        );
        assert!(dependencies(&manifests[&feature.package].1).contains("gpui-kit"));
    }
    for package in ["pioneer-client", "pioneer-client-ffi"] {
        for dependency in dependencies(&manifests[package].1) {
            assert!(
                !dependency.contains("desktop")
                    && !dependency.contains("gpui")
                    && !dependency.contains("nitro")
            );
        }
    }
}
#[test]
fn desktop_foundation_is_narrow() {
    let c = contract();
    let dir = root().join("crates/desktop-foundation/src");
    let files: Vec<_> = sources(&dir)
        .iter()
        .map(|p| p.file_name().unwrap().to_str().unwrap().to_owned())
        .collect();
    assert_eq!(files, c.foundation_sources);
    let manifest: toml::Value = toml::from_str(&source("desktop-foundation/Cargo.toml")).unwrap();
    assert_eq!(
        dependencies(&manifest),
        ["gpui-kit".into(), "pioneer-client".into()]
            .into_iter()
            .collect()
    );
    let modules: Vec<_> = ast(&dir.join("lib.rs"))
        .items
        .iter()
        .filter_map(|i| {
            if let syn::Item::Mod(m) = i {
                Some(m.ident.to_string())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(modules, c.foundation_modules);
    for file in sources(&dir) {
        let text = fs::read_to_string(file).unwrap();
        let text = text.split("#[cfg(test)]").next().unwrap();
        for forbidden in [
            "ClientIntent",
            "ClientEffectPlan",
            "Entity<",
            "ClientCore",
            "impl Element for",
        ] {
            assert!(!text.contains(forbidden), "Foundation: {forbidden}");
        }
    }
}
#[test]
fn feature_public_seams_are_encapsulated() {
    for feature in contract().features {
        let dir = root().join("crates").join(&feature.path).join("src");
        let root = ast(&dir.join("lib.rs"));
        let mut modules = vec![];
        for item in production_items(&root.items) {
            if let syn::Item::Mod(module) = item {
                if matches!(module.vis, syn::Visibility::Public(_)) {
                    modules.push(module.ident.to_string());
                }
            }
            if let syn::Item::Use(item) = item {
                if matches!(item.vis, syn::Visibility::Public(_)) {
                    fn check(tree: &syn::UseTree) {
                        match tree {
                            syn::UseTree::Glob(_) => panic!("feature exports must be explicit"),
                            syn::UseTree::Path(p) => {
                                assert!(p.ident != "binding", "private binding export");
                                check(&p.tree);
                            }
                            syn::UseTree::Group(g) => {
                                for t in &g.items {
                                    check(t)
                                }
                            }
                            _ => {}
                        }
                    }
                    check(&item.tree);
                }
            }
        }
        assert_eq!(modules, feature.public_modules);
        for path in sources(&dir) {
            if path.file_name().unwrap().to_str().unwrap().contains("test")
                || path.components().any(|c| c.as_os_str() == "fixtures")
            {
                continue;
            }
            let text = fs::read_to_string(&path).unwrap();
            for other in contract().features {
                if other.package != feature.package {
                    assert!(
                        !text.contains(&other.package.replace('-', "_")),
                        "{} imports sibling {}",
                        path.display(),
                        other.package
                    );
                }
            }
            assert!(!text.contains("pioneer_client_ffi"));
        }
    }
}
#[test]
fn desktop_shell_is_composition_only() {
    let shell = source("desktop/src/desktop_shell.rs");
    let file = syn::parse_file(&shell).unwrap();
    let owner = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Struct(s) if s.ident == "DesktopShellView" => Some(s),
            _ => None,
        })
        .unwrap();
    assert!(matches!(owner.vis, syn::Visibility::Restricted(_)));
    for field in &owner.fields {
        let name = field.ident.as_ref().unwrap().to_string();
        for forbidden in [
            "retry",
            "pending",
            "domain",
            "revision",
            "request",
            "cache",
            "poller",
            "hydration",
        ] {
            assert!(!name.contains(forbidden), "shell field {name}");
        }
    }
    for forbidden in [
        "GatewayNotification",
        "reduce_gateway",
        "ClientRuntimeNotification",
        "thread_start_mutation",
        "composer_intent(",
        "provider_collection_intent(",
        "workspaces_loading",
        "HashMap<",
        "impl Element for",
    ] {
        assert!(!shell.contains(forbidden), "shell owns {forbidden}");
    }
    let bootstrap = source("desktop/src/client_runtime/mod.rs");
    assert!(source("desktop/src/main.rs").contains("DesktopRuntimeCoordinator::open_window"));
    assert!(bootstrap.contains("Root::new(shell, window, cx)"));
    assert_eq!(bootstrap.matches("Root::new(").count(), 1);
    assert!(bootstrap.contains("DesktopShellView::new"));
    for name in ["sheet", "dialog", "notification"] {
        assert_eq!(
            shell
                .matches(&format!("Root::render_{name}_layer("))
                .count(),
            1
        );
    }
    for file in sources(&root().join("crates/desktop/src/client_runtime")) {
        if file.file_name().unwrap() == "mod.rs" {
            continue;
        }
        let text = fs::read_to_string(file).unwrap();
        for capture in [
            "WeakEntity<DesktopShellView>",
            "Entity<DesktopShellView>",
            "WeakEntity<ThreadView>",
        ] {
            assert!(!text.contains(capture));
        }
    }
}
#[test]
fn feature_removal_boundary_is_complete() {
    let shell = source("desktop/src/desktop_shell.rs");
    for feature in contract().features {
        assert!(
            shell.contains(&feature.constructor),
            "unmounted {}",
            feature.root
        );
        assert!(
            shell.contains(&format!(
                "Entity<pioneer_{}::{}>",
                feature.path.replace('-', "_"),
                feature.root
            )),
            "missing retained {}",
            feature.root
        );
        let manifests = manifests();
        for (name, (_, manifest)) in &manifests {
            if name != "pioneer-desktop" {
                assert!(
                    !dependencies(manifest).contains(&feature.package),
                    "{name} prevents independent removal of {}",
                    feature.package
                );
            }
        }
    }
    for obsolete in [
        "desktop/src/app",
        "desktop/src/components",
        "desktop/src/code_highlight",
        "desktop/src/gateway/http.rs",
        "desktop/src/gateway/ws",
    ] {
        assert!(!root().join("crates").join(obsolete).exists());
    }
    for package in ["desktop", "client", "client-ffi"] {
        for file in sources(&root().join("crates").join(package).join("src")) {
            let text = fs::read_to_string(&file).unwrap();
            for forbidden in [
                concat!("Pioneer", "Desktop"),
                concat!("LegacyScreen", "Adapter"),
                "GatewayCompatibilityQueue",
                "ClientRuntimePostEventSink",
                "gateway_compatibility_events",
                "ClientFfiActiveThreadInner",
            ] {
                assert!(!text.contains(forbidden), "{}: {forbidden}", file.display());
            }
        }
    }
}
struct RenderAudit {
    violations: Vec<String>,
    closures: bool,
}
impl<'ast> Visit<'ast> for RenderAudit {
    fn visit_expr_call(&mut self, e: &'ast syn::ExprCall) {
        // This feature helper's last parameter is passed to PopupMenuItem::on_click.
        if matches!(e.func.as_ref(),syn::Expr::Path(p) if p.path.segments.last().is_some_and(|s|s.ident=="composer_add_menu_item"))
        {
            for arg in e.args.iter().take(2) {
                self.visit_expr(arg);
            }
        } else if matches!(e.func.as_ref(),syn::Expr::Path(p) if p.path.is_ident("canvas")) {
            // GPUI invokes intrinsic bounds callbacks in prepaint, after Render.
            // Existing selector layout owners retain only the measured width.
        } else {
            visit::visit_expr_call(self, e);
        }
    }

    fn visit_expr_closure(&mut self, e: &'ast syn::ExprClosure) {
        if self.closures {
            visit::visit_expr_closure(self, e);
        }
    }
    fn visit_expr_method_call(&mut self, e: &'ast syn::ExprMethodCall) {
        if [
            "notify",
            "spawn",
            "background_spawn",
            "subscribe",
            "observe",
            "focus_handle",
            "new_entity",
            "borrow_mut",
            "dispatch",
            "dispatch_action",
            "gateway_registry",
            "gateway_session",
            "gateway_settings",
            "auth_sessions",
            "device_activation_presentation",
            "navigation_snapshot",
            "composer_snapshot",
        ]
        .contains(&e.method.to_string().as_str())
        {
            self.violations.push(e.method.to_string());
        }
        if e.method == "snapshot" {
            fn client_receiver(e: &syn::Expr) -> bool {
                match e {
                    syn::Expr::Path(p) => p
                        .path
                        .segments
                        .iter()
                        .any(|s| s.ident == "client" || s.ident == "core"),
                    syn::Expr::Field(f) => {
                        matches!(&f.member, syn::Member::Named(n) if n == "client" || n == "core")
                            || client_receiver(&f.base)
                    }
                    _ => false,
                }
            }
            if client_receiver(&e.receiver) {
                self.violations.push("Client snapshot in Render".into());
            }
        }
        if e.method == "new" {
            self.violations.push("new Entity".into());
        }
        // These arguments register deferred interaction callbacks. Value-builder
        // callbacks (map/when/children/content/fallback) still run under this audit.
        if e.method.to_string().starts_with("on_")
            || ["listener", "listener_for", "processor"].contains(&e.method.to_string().as_str())
        {
            self.visit_expr(&e.receiver);
        } else {
            visit::visit_expr_method_call(self, e);
        }
    }
    fn visit_expr_assign(&mut self, e: &'ast syn::ExprAssign) {
        fn retained(e: &syn::Expr) -> bool {
            match e {
                syn::Expr::Path(p) => p.path.is_ident("self"),
                syn::Expr::Field(f) => retained(&f.base),
                syn::Expr::Index(i) => retained(&i.expr),
                syn::Expr::Unary(_) => true,
                _ => false,
            }
        }
        if retained(&e.left) {
            self.violations.push("assignment in Render".into());
        }
        visit::visit_expr_assign(self, e);
    }
}
#[test]
fn render_units_are_pure_and_optional_caches_are_absent() {
    let c = contract();
    assert!(c.scene_caches.is_empty());
    let mut elements = vec![];
    let mut findings = vec![];
    for dir in std::iter::once("desktop".to_owned())
        .chain(std::iter::once("desktop-foundation".to_owned()))
        .chain(c.features.iter().map(|f| f.path.clone()))
    {
        for path in sources(&root().join("crates").join(&dir).join("src")) {
            if path.file_name().unwrap().to_str().unwrap().contains("test")
                || path.components().any(|c| c.as_os_str() == "fixtures")
            {
                continue;
            }
            let file = ast(&path);
            for item in production_items(&file.items) {
                if let syn::Item::Impl(implementation) = item {
                    let Some((_, tr, _)) = &implementation.trait_ else {
                        for method in &implementation.items {
                            if let syn::ImplItem::Fn(method) = method {
                                if method.sig.ident.to_string().starts_with("render_") {
                                    let mut audit = RenderAudit {
                                        violations: vec![],
                                        closures: true,
                                    };
                                    audit.visit_block(&method.block);
                                    if !audit.violations.is_empty() {
                                        findings.push(format!(
                                            "{} {}: {:?}",
                                            path.display(),
                                            method.sig.ident,
                                            audit.violations
                                        ));
                                    }
                                }
                            }
                        }
                        continue;
                    };
                    let name = tr.segments.last().unwrap().ident.to_string();
                    if name == "Element" {
                        if let syn::Type::Path(ty) = implementation.self_ty.as_ref() {
                            elements.push(format!(
                                "{}:{}",
                                path.strip_prefix(root().join("crates")).unwrap().display(),
                                ty.path.segments.last().unwrap().ident
                            ));
                        }
                    }
                    if !["Render", "RenderOnce", "Element"].contains(&name.as_str()) {
                        continue;
                    }
                    if let syn::Type::Path(ty) = implementation.self_ty.as_ref() {
                        let symbol = ty.path.segments.last().unwrap().ident.to_string();
                        let key = format!("{dir}:{symbol}");
                        let primary = c.units.values().any(|u| {
                            u.file.starts_with(&format!("{dir}/"))
                                && u.symbol == symbol
                                && match u.kind.as_str() {
                                    "Entity" => name == "Render",
                                    "RenderOnce" => name == "RenderOnce",
                                    "Element" => name == "Element",
                                    _ => false,
                                }
                        });
                        let support = c.supporting_units.get(&key).is_some_and(|row| {
                            row.len() == 3
                                && row[0] == name
                                && !row[1].is_empty()
                                && c.units.contains_key(&row[2])
                        });
                        let framework = c.framework_hosts.iter().any(|host| {
                            host.starts_with(&format!("{dir}/"))
                                && host.ends_with(&format!(":{symbol}"))
                        });
                        let icon = symbol == "PioneerIconName"
                            && name == "RenderOnce"
                            && path.file_name().unwrap() == "assets.rs";
                        assert!(
                            primary || support || framework || icon,
                            "unreviewed UI owner {key} ({name})"
                        );
                    }

                    for method in &implementation.items {
                        if let syn::ImplItem::Fn(method) = method {
                            let mut audit = RenderAudit {
                                violations: vec![],
                                closures: true,
                            };
                            audit.visit_block(&method.block);
                            if !audit.violations.is_empty() {
                                findings.push(format!(
                                    "{} {}: {:?}",
                                    path.display(),
                                    method.sig.ident,
                                    audit.violations
                                ));
                            }
                        }
                    }
                }
            }
            struct CacheAudit(bool);
            impl<'a> Visit<'a> for CacheAudit {
                fn visit_expr_method_call(&mut self, e: &'a syn::ExprMethodCall) {
                    if e.method == "cached" {
                        self.0 = true;
                    }
                    visit::visit_expr_method_call(self, e);
                }
            }
            let mut caches = CacheAudit(false);
            for item in production_items(&file.items) {
                caches.visit_item(item)
            }
            assert!(!caches.0, "unqualified scene cache {}", path.display());
        }
    }
    assert!(findings.is_empty(), "{}", findings.join("\n"));
    elements.sort();
    assert_eq!(elements, c.custom_elements);
    let fixture: syn::Expr = syn::parse_str("{ cx.notify(); self.cache = value; }").unwrap();
    let mut audit = RenderAudit {
        violations: vec![],
        closures: true,
    };
    audit.visit_expr(&fixture);
    assert_eq!(audit.violations.len(), 2);
    for text in [
        "rows.map(|row| { cx.notify(); row })",
        "popover.content(|cx| cx.new(|cx| State::new(cx)))",
    ] {
        let mut audit = RenderAudit {
            violations: vec![],
            closures: true,
        };
        audit.visit_expr(&syn::parse_str::<syn::Expr>(text).unwrap());
        assert!(
            !audit.violations.is_empty(),
            "value callback bypass: {text}"
        );
    }
    let mut event = RenderAudit {
        violations: vec![],
        closures: true,
    };
    event.visit_expr(&syn::parse_str::<syn::Expr>("button.on_click(|cx| cx.notify())").unwrap());
    assert!(event.violations.is_empty());
}
#[test]
fn unit_identity_cache_and_cross_shell_policies_match_the_contract() {
    let c = contract();
    assert_eq!(c.version, 1);
    assert_eq!(c.platforms, ["desktop", "ios", "android"]);
    assert_eq!(c.ui_cases.iter().collect::<BTreeSet<_>>().len(), 47);
    assert_eq!(c.units.len(), 47);
    assert_eq!(c.divergence_owners.len(), 10);
    for number in 1..=47 {
        let unit = &c.units[&format!("TUI-{number:02}")];
        assert!(!unit.owner.is_empty());
        let text = source(&unit.file);
        assert!(
            text.contains(&unit.symbol),
            "{number}: missing {} in {}",
            unit.symbol,
            unit.file
        );
        let required = match unit.kind.as_str() {
            "Entity" => Some("Render"),
            "RenderOnce" => Some("RenderOnce"),
            "Element" => Some("Element"),
            "Behavior" => None,
            _ => panic!("unreviewed unit kind"),
        };
        if let Some(tr) = required {
            assert!(
                text.contains(&format!("impl {tr} for {}", unit.symbol)),
                "unit {number} has wrong kind"
            );
        }
    }
    fn integer(e: &syn::Expr) -> u64 {
        match e {
            syn::Expr::Lit(l) => {
                if let syn::Lit::Int(n) = &l.lit {
                    n.base10_parse().unwrap()
                } else {
                    panic!("integer constant")
                }
            }
            syn::Expr::Binary(b) if matches!(b.op, syn::BinOp::Mul(_)) => {
                integer(&b.left) * integer(&b.right)
            }
            syn::Expr::Call(c) => integer(c.args.first().expect("duration seconds")),
            _ => panic!("cache limit must stay statically reviewable"),
        }
    }
    let limit = |file: &str, name: &str| {
        let file = syn::parse_file(&source(file)).unwrap();
        file.items
            .iter()
            .find_map(|i| {
                if let syn::Item::Const(c) = i {
                    (c.ident == name).then(|| integer(&c.expr))
                } else {
                    None
                }
            })
            .expect("cache limit constant")
    };
    let avatars = &c.caches["avatar_bytes"];
    assert_eq!(
        limit(
            avatars["source"].as_str().unwrap(),
            "AVATAR_CACHE_MAX_FILES"
        ),
        avatars["max_files"].as_integer().unwrap() as u64
    );
    assert_eq!(
        limit(avatars["source"].as_str().unwrap(), "AVATAR_CACHE_MAX_AGE"),
        avatars["max_age_days"].as_integer().unwrap() as u64 * 24 * 60 * 60
    );
    let previews = &c.caches["artifact_preview"];
    assert_eq!(
        limit(
            previews["source"].as_str().unwrap(),
            "ARTIFACT_PREVIEW_CACHE_MAX_BYTES"
        ),
        previews["max_bytes"].as_integer().unwrap() as u64
    );
    assert_eq!(
        limit(
            previews["source"].as_str().unwrap(),
            "ARTIFACT_PREVIEW_MAX_BYTES"
        ),
        previews["max_input_bytes"].as_integer().unwrap() as u64
    );
    for n in 1..=10 {
        assert!(!c.divergence_owners[&format!("BDL-{n:02}")].is_empty());
    }
    for cache in c.caches.values() {
        assert!(source(cache["source"].as_str().unwrap()).len() > 100);
        assert!(!cache["owner"].as_str().unwrap().is_empty());
    }
    for host in c.framework_hosts {
        let (file, name) = host.split_once(':').unwrap();
        assert!(source(file).contains(&format!("impl Render for {name}")));
    }
    // The measured option vector is freshly computed with current width/style;
    // no render-time retained-map mutation can reuse stale locale/rem geometry.
    for path in [
        "desktop-thread/src/model_picker.rs",
        "desktop-settings/src/model_selector.rs",
    ] {
        let picker = source(path);
        assert!(!picker.contains("model_row_layout_cache"));
        assert!(picker.contains("layout_as_root("));
    }
}
