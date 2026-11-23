//! App format regressions. NOT_RUN / NOT_COMPILED under the implementation embargo.
use pioneer_apps::{AppManifest, SelectionPlan};
use pioneer_packages::{Entry, Limits, Snapshot};
use serde_json::{Value, json};
use std::fs;

fn manifest() -> Value {
    json!({
        "slug": "example-app", "name": "Example", "version": "1.2.3",
        "shortDescription": "Base description", "developer": {"name": "Author"},
        "icon": {"light": "./icon.png", "dark": "./icon.png"}
    })
}
fn limits() -> Limits {
    Limits {
        expanded_bytes: 4096,
        entry_bytes: 2048,
        entries: 32,
        depth: 8,
        components: 16,
    }
}
fn parse(v: &Value) -> Result<AppManifest, pioneer_apps::ManifestError> {
    AppManifest::parse(&serde_json::to_vec(v).unwrap())
}

#[test]
fn strict_app_fields_have_diagnostics_and_never_vendor_fallback() {
    for (pointer, value, code, field) in [
        (
            "/manifestVersion",
            json!(1),
            "unknown_field",
            "app.json/manifestVersion",
        ),
        ("/name", json!(null), "null_field", "app.json/name"),
        ("/name", json!(" \t"), "invalid_string", "app.json/name"),
        (
            "/slug",
            json!("Upper-case"),
            "invalid_slug",
            "app.json/slug",
        ),
        (
            "/version",
            json!("1.0"),
            "invalid_version",
            "app.json/version",
        ),
        ("/mcp", json!("./mcp.json"), "invalid_type", "app.json/mcp"),
        ("/mcp", json!({}), "missing_field", "app.json/mcp/config"),
        (
            "/icon",
            json!({"light": "icon.png"}),
            "missing_field",
            "app.json/icon/dark",
        ),
        (
            "/skills",
            json!("../skills"),
            "invalid_path",
            "app.json/skills",
        ),
        (
            "/mcp",
            json!({"config": "mcp.json", "include": ["."]}),
            "invalid_path",
            "app.json/mcp/include/0",
        ),
        (
            "/developer",
            json!({"name": "Author", "email": "a@example.test"}),
            "unknown_field",
            "app.json/developer/email",
        ),
        (
            "/locales",
            json!({"en": "en.json"}),
            "invalid_default_locale",
            "app.json/defaultLocale",
        ),
    ] {
        let mut value_manifest = manifest();
        let key = pointer.strip_prefix('/').unwrap();
        value_manifest[key] = value;
        let error = parse(&value_manifest).unwrap_err();
        assert_eq!((error.code, error.field.as_str()), (code, field));
    }
    let duplicate = serde_json::to_string(&manifest()).unwrap().replace(
        "\"developer\":{\"name\":\"Author\"}",
        "\"developer\":{\"name\":\"Author\",\"name\":\"Other\"}",
    );
    let error = AppManifest::parse(duplicate.as_bytes()).unwrap_err();
    assert_eq!(
        (error.code, error.field.as_str()),
        ("duplicate_key", "app.json/developer/name")
    );
    let vendor_only = br#"{"name":"Example","skills":"./skills"}"#;
    assert_eq!(
        AppManifest::parse(vendor_only).unwrap_err().field,
        "app.json/slug"
    );
}

#[test]
fn translations_fall_back_per_field_and_invalid_declared_files_are_errors() {
    let mut value = manifest();
    value["locales"] = json!({"en": "en.json", "pt": "pt.json", "pt-BR": "br.json"});
    value["defaultLocale"] = json!("en");
    let manifest = parse(&value).unwrap();
    let files = [
        (
            "en.json",
            br#"{"longDescription":"Default long"}"#.as_slice(),
        ),
        (
            "pt.json",
            br#"{"shortDescription":"Base language"}"#.as_slice(),
        ),
        ("br.json", br#"{"name":"Exact"}"#.as_slice()),
    ];
    let translations = manifest
        .load_localizations(|p| files.iter().find(|(k, _)| *k == p).map(|(_, v)| *v))
        .unwrap();
    let display = manifest.presentation("pt-BR", &translations);
    assert_eq!(display.name, "Exact");
    assert_eq!(display.short_description, "Base language");
    assert_eq!(display.long_description.as_deref(), Some("Default long"));
    assert_eq!(
        manifest.load_localizations(|_| None).unwrap_err().code,
        "missing_locale"
    );
    assert_eq!(
        manifest
            .load_localizations(|_| Some(br#"{"slug":"injected"}"#))
            .unwrap_err()
            .code,
        "unknown_field"
    );
    value["locales"] = json!({"en-US": "a.json", "en-us": "b.json"});
    value["defaultLocale"] = json!("en-US");
    assert_eq!(parse(&value).unwrap_err().code, "duplicate_locale");
}

#[test]
fn selective_capture_excludes_siblings_and_gateway_plan_rejects_extra_entries() {
    let root = tempfile::tempdir().unwrap();
    fs::write(
        root.path().join("app.json"),
        serde_json::to_vec(&manifest()).unwrap(),
    )
    .unwrap();
    fs::write(root.path().join("icon.png"), b"selected").unwrap();
    // Excluded input exceeds the entire snapshot budget. It must never be captured.
    fs::create_dir(root.path().join("node_modules")).unwrap();
    fs::write(root.path().join("node_modules/large"), vec![0; 8192]).unwrap();
    let (_, first) = SelectionPlan::capture(root.path(), limits(), || false).unwrap();
    assert_eq!(
        first
            .entries()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["app.json", "icon.png"]
    );
    fs::write(
        root.path().join("node_modules/large"),
        b"changed excluded content",
    )
    .unwrap();
    let (_, second) = SelectionPlan::capture(root.path(), limits(), || false).unwrap();
    assert_eq!(first.tree_digest(), second.tree_digest());
    fs::write(root.path().join("icon.png"), b"changed selected content").unwrap();
    let (_, third) = SelectionPlan::capture(root.path(), limits(), || false).unwrap();
    assert_ne!(first.tree_digest(), third.tree_digest());
    let mut entries = first.entries().clone();
    entries.insert(
        "secret".into(),
        Entry::File {
            bytes: vec![1],
            mode: 0o644,
        },
    );
    let injected = Snapshot::from_entries(entries, limits()).unwrap();
    assert_eq!(
        SelectionPlan::validate_package(&injected).unwrap_err().code,
        "unselected_entry"
    );
    assert!(
        SelectionPlan::capture(root.path(), limits(), || true)
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
}

#[cfg(unix)]
#[test]
fn selected_symlink_is_rejected_even_when_target_is_contained() {
    let root = tempfile::tempdir().unwrap();
    fs::write(
        root.path().join("app.json"),
        serde_json::to_vec(&manifest()).unwrap(),
    )
    .unwrap();
    fs::write(root.path().join("real.png"), b"image").unwrap();
    std::os::unix::fs::symlink("real.png", root.path().join("icon.png")).unwrap();
    assert!(
        SelectionPlan::capture(root.path(), limits(), || false)
            .unwrap_err()
            .to_string()
            .contains("selected_symlink")
    );
}

#[test]
fn app_loader_supports_declared_component_combinations_and_rejects_invalid_artwork() {
    use pioneer_apps as app;
    use pioneer_packages::ComponentPlan;
    for (skills, mcp, local, http_type, count) in [
        (false, false, false, "", 0),
        (true, false, false, "", 1),
        (false, true, false, "", 1),
        (false, true, false, "streamable-http", 1),
        (false, true, false, "http", 1),
        (true, true, false, "http", 2),
        (false, true, true, "", 1),
        (true, true, true, "", 2),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut value = manifest();
        let mut png = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgba8(1, 1)
            .write_to(&mut png, image::ImageFormat::Png)
            .unwrap();
        fs::write(root.path().join("icon.png"), png.into_inner()).unwrap();
        if skills {
            value["skills"] = json!("./capabilities/");
            fs::create_dir_all(root.path().join("capabilities/one")).unwrap();
            fs::write(
                root.path().join("capabilities/one/SKILL.md"),
                "---\nname: Display title\nallowed-tools: [Read]\n---\nRead bundled data.\n",
            )
            .unwrap();
        }
        if mcp {
            let server = if local {
                value["mcp"] = json!({"config": "./config/mcp.json", "include": ["./dist/mcp/"]});
                fs::create_dir_all(root.path().join("dist/mcp")).unwrap();
                fs::write(
                    root.path().join("dist/mcp/server"),
                    b"local runtime payload",
                )
                .unwrap();
                json!({"type":"stdio", "command":"./dist/mcp/server", "args":["${APP_ROOT}/dist/mcp/server"], "cwd":"${APP_ROOT}"})
            } else {
                value["mcp"] = json!({"config": "./config/mcp.json"});
                let mut server = json!({"url":"https://example.test/mcp",
                    "oauth":{"client_id":"registered"},"startup_timeout_sec":7});
                if !http_type.is_empty() {
                    server["type"] = http_type.into();
                }
                server
            };
            fs::create_dir(root.path().join("config")).unwrap();
            fs::write(
                root.path().join("config/mcp.json"),
                json!({"$schema":"native-schema-reference","mcpServers":{"example":server}})
                    .to_string(),
            )
            .unwrap();
        }
        fs::write(root.path().join("app.json"), value.to_string()).unwrap();
        let (_, snapshot) = SelectionPlan::capture(root.path(), limits(), || false).unwrap();
        let package = app::load(&snapshot).unwrap();
        assert!(package.diagnostics.is_empty(), "{:?}", package.diagnostics);
        assert_eq!(package.components.len(), count);
        if mcp && !local {
            assert!(
                package
                    .components
                    .iter()
                    .any(|component| matches!(component,
                        ComponentPlan::Mcp { server, .. }
                            if server.config()["url"] == "https://example.test/mcp"
                                && server.config()["oauth"]["client_id"] == "registered"
                                && server.config()["startup_timeout_sec"] == 7
                    ))
            );
        }
        if skills {
            assert!(
                package
                    .components
                    .iter()
                    .any(|component| matches!(component,
                ComponentPlan::Skill { member_path, .. } if member_path == "capabilities/one"))
            );
        }
        if local {
            assert_eq!(
                snapshot.file("dist/mcp/server"),
                Some(b"local runtime payload".as_slice())
            );
        }
        // Declaration alone is not image validity: the loader must decode bytes.
        fs::write(root.path().join("icon.png"), b"not an image").unwrap();
        let (_, invalid) = SelectionPlan::capture(root.path(), limits(), || false).unwrap();
        let error = app::load(&invalid).unwrap_err();
        assert_eq!(
            (error.code, error.field.as_str()),
            ("invalid_image", "app.json/icon/light")
        );
    }
}

fn runtime_manifest() -> Value {
    let mut document = manifest();
    document["runtime"] = json!({"environment":"native","artifacts":{
        "linux/amd64":{"path":"payloads/amd64","sha256":"cd3987227dd5ee1fde03e575f04747c0f407673c50a3e1ace90a16aa6b932210"},
        "linux/arm64":{"path":"payloads/arm64","sha256":"cd3987227dd5ee1fde03e575f04747c0f407673c50a3e1ace90a16aa6b932210"}
    },"cwd":"src","command":["/payload/run"],"port":8000,"readiness":{"path":"/ready"}});
    document["ui"] = json!({"path":"/ui/"});
    document
}
fn runtime_entries(document: &Value) -> std::collections::BTreeMap<String, Entry> {
    std::collections::BTreeMap::from([
        (
            "app.json".into(),
            Entry::File {
                bytes: serde_json::to_vec(document).unwrap(),
                mode: 0o644,
            },
        ),
        (
            "icon.png".into(),
            Entry::File {
                bytes: b"icon".to_vec(),
                mode: 0o644,
            },
        ),
        ("src".into(), Entry::Directory { mode: 0o755 }),
        ("payloads".into(), Entry::Directory { mode: 0o755 }),
        ("payloads/amd64".into(), Entry::Directory { mode: 0o755 }),
        (
            "payloads/amd64/run".into(),
            Entry::File {
                bytes: b"payload".to_vec(),
                mode: 0o755,
            },
        ),
        ("payloads/arm64".into(), Entry::Directory { mode: 0o755 }),
        (
            "payloads/arm64/run".into(),
            Entry::File {
                bytes: b"payload".to_vec(),
                mode: 0o755,
            },
        ),
    ])
}
#[test]
fn selected_guest_payload_is_checked_and_other_architectures_are_not_published() {
    let document = runtime_manifest();
    let entries = runtime_entries(&document);
    let snapshot = Snapshot::from_entries(entries.clone(), limits()).unwrap();
    let selected = SelectionPlan::select_architecture(&snapshot, "linux/arm64").unwrap();
    assert_eq!(
        selected.file("payloads/arm64/run"),
        Some(b"payload".as_slice())
    );
    assert!(!selected.entries().contains_key("payloads/amd64"));
    assert!(!selected.entries().contains_key("payloads/amd64/run"));
    assert!(SelectionPlan::validate_package_for_architecture(&selected, "linux/arm64").is_ok());
    assert!(SelectionPlan::select_architecture(&snapshot, "linux/riscv64").is_err());
    let mut corrupted = entries.clone();
    corrupted.insert(
        "payloads/arm64/run".into(),
        Entry::File {
            bytes: b"changed".to_vec(),
            mode: 0o755,
        },
    );
    let corrupted = Snapshot::from_entries(corrupted, limits()).unwrap();
    assert!(
        SelectionPlan::select_architecture(&corrupted, "linux/arm64")
            .unwrap_err()
            .to_string()
            .contains("artifact_checksum_mismatch")
    );
    let mut non_executable = entries;
    non_executable.insert(
        "payloads/arm64/run".into(),
        Entry::File {
            bytes: b"payload".to_vec(),
            mode: 0o644,
        },
    );
    assert!(
        SelectionPlan::select_architecture(
            &Snapshot::from_entries(non_executable, limits()).unwrap(),
            "linux/arm64"
        )
        .unwrap_err()
        .to_string()
        .contains("runtime_executable_mode")
    );
}
#[test]
fn portable_runtime_has_one_payload_and_cannot_assign_images_or_authority() {
    let mut document = runtime_manifest();
    document["runtime"]["environment"] = json!("node");
    document["runtime"]["artifacts"] = json!({});
    document["runtime"]["command"] = json!(["node", "main.js"]);
    let mut entries = runtime_entries(&document);
    entries.retain(|key, _| !key.starts_with("payloads"));
    entries.insert(
        "src/main.js".into(),
        Entry::File {
            bytes: b"ready()".to_vec(),
            mode: 0o644,
        },
    );
    let snapshot = Snapshot::from_entries(entries, limits()).unwrap();
    for architecture in ["linux/arm64", "linux/amd64"] {
        assert!(SelectionPlan::select_architecture(&snapshot, architecture).is_ok());
    }
    for (pointer, value) in [
        ("/runtime/environment", json!("docker")),
        ("/runtime/command", json!("node main.js")),
        ("/runtime/cwd", json!("../outside")),
        ("/ui/path", json!("/../other-app")),
    ] {
        let mut invalid = document.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        assert!(parse(&invalid).is_err(), "{pointer}");
    }
    for key in ["image", "identity", "grantedCapabilities"] {
        let mut invalid = document.clone();
        invalid["runtime"][key] = json!("host-assigned");
        assert!(parse(&invalid).is_err(), "{key}");
    }
}

#[test]
fn authoring_source_requires_components_but_not_release_payloads() {
    use pioneer_apps::PreparedAppSource;

    let root = tempfile::tempdir().unwrap();
    let mut document = runtime_manifest();
    document["skills"] = json!("capabilities");
    document["locales"] = json!({"en": "en.json"});
    document["defaultLocale"] = json!("en");
    fs::write(root.path().join("app.json"), document.to_string()).unwrap();
    let mut png = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgba8(1, 1)
        .write_to(&mut png, image::ImageFormat::Png)
        .unwrap();
    fs::write(root.path().join("icon.png"), png.into_inner()).unwrap();
    fs::write(root.path().join("en.json"), br#"{"name":"Localized"}"#).unwrap();
    fs::create_dir_all(root.path().join("capabilities/one")).unwrap();
    fs::write(
        root.path().join("capabilities/one/SKILL.md"),
        "---\nname: One\ndescription: Read data.\n---\nOriginal instructions.\n",
    )
    .unwrap();
    // None of src, payloads/amd64, or payloads/arm64 exists on the host.
    // A normal release capture must still require those declared inputs.
    assert!(SelectionPlan::capture(root.path(), limits(), || false).is_err());
    let prepared = PreparedAppSource::capture(root.path(), limits(), || false).unwrap();
    assert_eq!(prepared.package.components.len(), 1);
    assert_eq!(
        prepared
            .package
            .manifest
            .presentation("en", &prepared.package.localizations)
            .name,
        "Localized"
    );
    assert_eq!(prepared.package.images.len(), 1);
    assert!(SelectionPlan::validate_package(&prepared.snapshot).is_err());

    // Invalid edits cannot become an accepted revision; the captured bytes of
    // the prior revision remain usable by an already prepared agent turn.
    fs::write(root.path().join("capabilities/one/SKILL.md"), "").unwrap();
    let error = PreparedAppSource::capture(root.path(), limits(), || false).unwrap_err();
    let error = error.downcast_ref::<pioneer_apps::ManifestError>().unwrap();
    assert_eq!(error.code, "invalid_source_component");
    assert_eq!(error.field, "capabilities/one/SKILL.md");
    assert_eq!(
        prepared.snapshot.file("capabilities/one/SKILL.md"),
        Some(b"---\nname: One\ndescription: Read data.\n---\nOriginal instructions.\n".as_slice())
    );
    fs::remove_file(root.path().join("en.json")).unwrap();
    assert!(PreparedAppSource::capture(root.path(), limits(), || false).is_err());
}
