fn snapshot_limits() -> pioneer_plugins::Limits {
    pioneer_plugins::Limits {
        expanded_bytes: 50 * 1024 * 1024,
        entry_bytes: 1024 * 1024,
        entries: 2048,
        depth: 2048,
        components: 2048,
    }
}

use pioneer_plugins::*;
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs};

const SKILL: &str =
    "---\nname: good\ndescription: Inspect bundled data.\n---\nRead assets/data.json.\n";

#[test]
fn zenmoney_host_manifests_import_both_skill_and_http_connector() {
    let package = package(
        manifest(),
        &[
            (
                ".codex-plugin/plugin.json",
                r#"{"name":"zenmoney","version":"0.1.0","skills":"./skills/","mcpServers":"./.mcp.json","interface":{"displayName":"Zenmoney"},"hooks":"../outside"}"#,
            ),
            (
                ".claude-plugin/plugin.json",
                r#"{"name":"zenmoney","version":"0.1.0"}"#,
            ),
            (
                ".mcp.json",
                r#"{"mcpServers":{"zenmoney":{"type":"http","url":"https://api.zenmoney.app/mcp"}}}"#,
            ),
            (
                "skills/financial-brief/SKILL.md",
                "---\nname: financial-brief\ndescription: Summarise finances.\n---\nRead financial data.\n",
            ),
        ],
    );
    let mut entries = package.entries().clone();
    entries.remove("plugin.json");
    let source = Snapshot::from_entries(entries, snapshot_limits()).unwrap();
    let plan = load(&source).unwrap();
    assert_eq!(plan.manifest.name, "zenmoney");
    assert_eq!(plan.manifest.version.as_deref(), Some("0.1.0"));
    assert_eq!(
        plan.manifest.metadata.display_name.as_deref(),
        Some("Zenmoney")
    );
    for (code, pointer) in [
        ("vendor_subset_import", ".codex-plugin/plugin.json"),
        ("ignored_vendor_manifest", ".claude-plugin/plugin.json"),
        ("ignored_vendor_feature", ".codex-plugin/plugin.json/hooks"),
    ] {
        assert!(
            plan.diagnostics
                .iter()
                .any(|d| d.code == code && d.pointer == pointer)
        );
    }
    assert_eq!(plan.components.len(), 2);
    assert!(plan.components.iter().any(|component| matches!(component,
        ComponentPlan::Skill { member_key, member_path, .. }
        if member_key == "financial-brief" && member_path == "skills/financial-brief"
    )));
    assert!(plan.components.iter().any(|component| matches!(component,
        ComponentPlan::Mcp { member_key, server: PortableServer::StreamableHttp { url, headers } }
        if member_key == "zenmoney" && url == "https://api.zenmoney.app/mcp" && headers.is_empty()
    )));
}

#[test]
fn host_import_never_overrides_a_canonical_manifest_or_reads_external_component_paths() {
    let source = package(
        json!({"name":"invalid-canonical"}),
        &[(".codex-plugin/plugin.json", r#"{"name":"valid-host"}"#)],
    );
    assert_eq!(
        load(&source).unwrap_err().code,
        "unsupported_schema_version"
    );
    for declaration in [
        r#"{"skills":"../outside"}"#,
        r#"{"mcpServers":"/tmp/secret.json"}"#,
    ] {
        let mut raw: Value = serde_json::from_str(declaration).unwrap();
        raw["name"] = json!("host-plugin");
        let source = package(
            manifest(),
            &[(".codex-plugin/plugin.json", &raw.to_string())],
        );
        let mut entries = source.entries().clone();
        entries.remove("plugin.json");
        let source = Snapshot::from_entries(entries, snapshot_limits()).unwrap();
        assert_eq!(load(&source).unwrap_err().code, "unsupported_plugin_layout");
    }
}

#[test]
fn vendor_subsets_report_ignored_components_without_following_them() {
    for vendor in [".codex-plugin", ".claude-plugin", ".cursor-plugin"] {
        let path = format!("{vendor}/plugin.json");
        let source = package(
            manifest(),
            &[
                (
                    &path,
                    r#"{"name":"vendor-subset","foreign/key~":"/secret"}"#,
                ),
                ("skills/good/SKILL.md", SKILL),
                ("commands/run.md", "Never execute this command."),
            ],
        );
        let mut entries = source.entries().clone();
        entries.remove("plugin.json");
        let plan = load(&Snapshot::from_entries(entries, snapshot_limits()).unwrap()).unwrap();
        assert_eq!(plan.manifest.name, "vendor-subset");
        assert_eq!(plan.components.len(), 1);
        for (code, pointer) in [
            ("vendor_subset_import", path.clone()),
            ("ignored_vendor_feature", format!("{path}/foreign~1key~0")),
            ("ignored_vendor_feature", "commands".into()),
        ] {
            assert!(
                plan.diagnostics
                    .iter()
                    .any(|d| d.code == code && d.pointer == pointer)
            );
        }
        assert!(
            plan.diagnostics
                .iter()
                .all(|d| !d.message.contains("/secret"))
        );
    }
}

#[test]
fn manifest_priority_is_deterministic_and_selected_errors_do_not_fall_back() {
    let source = package(
        manifest(),
        &[
            (".codex-plugin/plugin.json", "not JSON"),
            (".claude-plugin/plugin.json", r#"{"name":"claude"}"#),
            (".cursor-plugin/plugin.json", r#"{"name":"cursor"}"#),
        ],
    );
    let plan = load(&source).unwrap();
    assert_eq!(plan.manifest.name, "fixture.tools");
    assert!(plan.diagnostics.is_empty());
    let mut entries = source.entries().clone();
    entries.remove("plugin.json");
    let error =
        load(&Snapshot::from_entries(entries.clone(), snapshot_limits()).unwrap()).unwrap_err();
    assert_eq!(error.code, "fatal_manifest");
    assert_eq!(error.pointer, ".codex-plugin/plugin.json");
    entries.remove(".codex-plugin/plugin.json");
    let plan = load(&Snapshot::from_entries(entries, snapshot_limits()).unwrap()).unwrap();
    assert_eq!(plan.manifest.name, "claude");
    assert!(plan.diagnostics.iter().any(|d| {
        d.code == "ignored_vendor_manifest" && d.pointer == ".cursor-plugin/plugin.json"
    }));
}

#[test]
fn bundled_skills_require_authored_fields_with_native_frontmatter_parsing() {
    for fields in [
        "description: Inspect data.",
        "name: good",
        "name: ''\ndescription: Inspect data.",
        "name: good\ndescription: ' '",
        "name: 123\ndescription: Inspect data.",
        "name: good\ndescription: 123",
        "name: ' good '\ndescription: Inspect data.",
        "name: other\ndescription: Inspect data.",
        "name: good\ndescription: Inspect data.\nallowed-tools: [Read]",
    ] {
        let markdown = format!("---\n{fields}\n---\nRead bundled data.");
        let plan = load(&package(manifest(), &[("skills/good/SKILL.md", &markdown)])).unwrap();
        assert!(plan.components.is_empty(), "{fields}");
        assert!(
            plan.diagnostics
                .iter()
                .any(|d| { d.code == "invalid_skill" && d.pointer == "skills/good/SKILL.md" })
        );
    }
    for markdown in [
        SKILL.to_owned(),
        SKILL.replace('\n', "\r\n"),
        format!("\u{feff}{SKILL}"),
        "---\nname: good\ndescription: |\n  Inspect data.\n  Keep context.\n---\nRead assets.\n"
            .into(),
    ] {
        let plan = load(&package(manifest(), &[("skills/good/SKILL.md", &markdown)])).unwrap();
        assert_eq!(plan.components.len(), 1, "{markdown}");
        assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
    }
}
fn manifest() -> Value {
    json!({"$schema":PLUGIN_SCHEMA,"name":"fixture.tools"})
}
fn package(value: Value, files: &[(&str, &str)]) -> Snapshot {
    let mut entries = BTreeMap::new();
    entries.insert(
        "plugin.json".into(),
        Entry::File {
            bytes: serde_json::to_vec(&value).unwrap(),
            mode: 0o644,
        },
    );
    for (name, contents) in files {
        let mut parent = std::path::Path::new(name).parent();
        while let Some(path) = parent.filter(|path| !path.as_os_str().is_empty()) {
            entries.insert(
                path.to_str().unwrap().into(),
                Entry::Directory { mode: 0o755 },
            );
            parent = path.parent();
        }
        entries.insert(
            (*name).into(),
            Entry::File {
                bytes: contents.as_bytes().to_vec(),
                mode: 0o644,
            },
        );
    }
    Snapshot::from_entries(entries, snapshot_limits()).unwrap()
}
#[test]
fn canonical_schemas_are_selected_offline_and_pinned() {
    use sha2::{Digest, Sha256};
    assert_eq!(
        hex::encode(Sha256::digest(PLUGIN_SCHEMA_BYTES)),
        "0a4aad95ce337878ad38802ebf0daa3fde76abe3f65400c86bcbb1ec0b3ab883"
    );
    assert_eq!(
        hex::encode(Sha256::digest(MCP_SCHEMA_BYTES)),
        "6539175bfcdf43085855183e86da40ea94b166547a72b47ae9a0a390516d3acb"
    );
    for invalid in [
        json!([]),
        json!({"$schema":"https://agent-plugins.org/schemas/1.1.0/plugin.schema.json","name":"good"}),
        json!({"$schema":PLUGIN_SCHEMA,"name":"Bad"}),
        json!({"$schema":PLUGIN_SCHEMA,"name":"good","author":{"unexpected":true}}),
    ] {
        assert_eq!(
            load(&package(invalid, &[("skills/good/SKILL.md", SKILL)]))
                .unwrap_err()
                .boundary,
            Boundary::Package
        );
    }
}
#[test]
fn unknown_manifest_fields_and_foreign_extension_values_are_ignored() {
    let mut value = manifest();
    value["skills"] = json!("alternative");
    value["extensions"] = json!({"com.foreign":null});
    value["version"] = json!("release tomorrow");
    value["homepage"] = json!("opaque metadata");
    let plan = load(&package(value, &[("skills/good/SKILL.md", SKILL)])).unwrap();
    assert_eq!(plan.components.len(), 1);
    assert_eq!(plan.diagnostics.len(), 1);
    let mut value = manifest();
    value["extensions"] = json!(false);
    assert_eq!(
        load(&package(value, &[])).unwrap().diagnostics[0].boundary,
        Boundary::Extension
    );
}
#[test]
fn empty_skills_only_mcp_only_mixed_and_partial_packages_are_valid() {
    let mcp = serde_json::to_string(&json!({"$schema":MCP_SCHEMA,"mcpServers":{
        "good":{"type":"stdio","command":"node"}, "bad":{"type":"stdio","url":"https://example.org"}}})).unwrap();
    for (files, count) in [
        (vec![], 0),
        (vec![("skills/good/SKILL.md", SKILL)], 1),
        (vec![("mcp.json", mcp.as_str())], 1),
        (
            vec![("skills/good/SKILL.md", SKILL), ("mcp.json", mcp.as_str())],
            2,
        ),
    ] {
        assert_eq!(
            load(&package(manifest(), &files)).unwrap().components.len(),
            count
        );
    }
    let plan = load(&package(
        manifest(),
        &[
            ("skills/good/SKILL.md", SKILL),
            ("mcp.json", "invalid JSON"),
        ],
    ))
    .unwrap();
    assert_eq!(plan.components.len(), 1);
    assert_eq!(plan.diagnostics[0].boundary, Boundary::Mcp);
}
#[test]
fn discovery_is_fixed_nonrecursive_and_invalid_skill_does_not_disable_siblings() {
    let plan = load(&package(
        manifest(),
        &[
            ("skills/good/SKILL.md", SKILL),
            ("skills/bad/SKILL.md", SKILL),
            ("skills/nested/deep/SKILL.md", SKILL),
            ("skills/lower/skill.md", SKILL),
            ("elsewhere/SKILL.md", SKILL),
        ],
    ))
    .unwrap();
    assert_eq!(plan.components.len(), 1);
    assert_eq!(plan.diagnostics.len(), 1);
}
#[test]
fn mcp_versions_envelope_closed_variants_and_sse_have_narrow_failures() {
    for document in [
        json!({"$schema":"wrong","mcpServers":{}}),
        json!({"$schema":MCP_SCHEMA,"mcpServers":{},"extra":true}),
    ] {
        assert_eq!(
            parse_mcp(&serde_json::to_vec(&document).unwrap(), 256)
                .unwrap_err()
                .boundary,
            Boundary::Mcp
        );
    }
    for config in [
        json!({"type":"stdio","command":"/absolute"}),
        json!({"type":"stdio","command":"bash -c"}),
        json!({"type":"stdio","command":"${PLUGIN_ROOT}/bin"}),
        json!({"type":"stdio","command":"node","cwd":"unrooted"}),
        json!({"type":"stdio","command":"node","cwd":null}),
        json!({"type":"stdio","command":"node","env":{"PLUGIN_DATA":"override"}}),
        json!({"type":"streamable-http","url":"http://example.org"}),
        json!({"type":"streamable-http","url":"https://@example.org"}),
        json!({"type":"streamable-http","url":"https://example.org/#fragment"}),
        json!({"type":"streamable-http","url":"https://example.org","headers":{"X-Name":"a","x-name":"b"}}),
        json!({"type":"stdio","command":"node","url":"https://example.org"}),
        json!({"type":"sse","url":"https://example.org"}),
    ] {
        let entries = parse_mcp(
            &serde_json::to_vec(&json!({"$schema":MCP_SCHEMA,"mcpServers":{"invalid":config,
            "valid":{"type":"streamable-http","url":"https://example.org/mcp?query=allowed"}}}))
            .unwrap(),
            256,
        )
        .unwrap();
        assert!(
            entries
                .iter()
                .find(|(name, _)| name == "invalid")
                .unwrap()
                .1
                .is_err()
        );
        assert!(
            entries
                .iter()
                .find(|(name, _)| name == "valid")
                .unwrap()
                .1
                .is_ok()
        );
    }
}
#[test]
fn expansion_is_single_pass_and_http_configuration_is_opaque() {
    assert_eq!(
        portable_mcp::expand(
            "${PLUGIN_ROOT}/${PLUGIN_DATA}/${HOME}/${PLUGIN_OTHER}",
            "${PLUGIN_DATA}",
            "data"
        ),
        "${PLUGIN_DATA}/data/${HOME}/${PLUGIN_OTHER}"
    );
    let root = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let server = PortableServer::Stdio {
        command: "node".into(),
        args: vec!["${HOME}".into(), "${PLUGIN_ROOT}/file".into()],
        env: BTreeMap::from([("CONFIG".into(), "${PLUGIN_DATA}".into())]),
        cwd: None,
    };
    let ResolvedServer::Stdio { args, env, cwd, .. } =
        portable_mcp::resolve(&server, root.path(), data.path()).unwrap()
    else {
        panic!()
    };
    assert_eq!(args[0], "${HOME}");
    assert_eq!(cwd, root.path().canonicalize().unwrap().to_str().unwrap());
    assert_eq!(env["CONFIG"], env["PLUGIN_DATA"]);
    let server = PortableServer::StreamableHttp {
        url: "https://example.org/mcp?q=${PLUGIN_ROOT}".into(),
        headers: BTreeMap::from([("X-Value".into(), "${PLUGIN_DATA}".into())]),
    };
    let ResolvedServer::StreamableHttp { url, headers } =
        portable_mcp::resolve(&server, root.path(), data.path()).unwrap()
    else {
        panic!()
    };
    assert!(url.contains("${PLUGIN_ROOT}"));
    assert_eq!(headers["X-Value"], "${PLUGIN_DATA}");
}
#[test]
fn command_placeholder_text_is_literal_when_it_is_an_executable_token() {
    let root = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("bin")).unwrap();
    fs::write(root.path().join("bin/${PLUGIN_ROOT}"), "bundled").unwrap();
    let server = PortableServer::Stdio {
        command: "./bin/${PLUGIN_ROOT}".into(),
        args: vec![],
        env: BTreeMap::new(),
        cwd: None,
    };
    let ResolvedServer::Stdio { command, .. } =
        portable_mcp::resolve(&server, root.path(), data.path()).unwrap()
    else {
        panic!()
    };
    assert!(command.ends_with("bin/${PLUGIN_ROOT}"));
}

#[test]
fn asset_bytes_and_modes_affect_tree_identity() {
    let a = package(
        manifest(),
        &[
            ("skills/good/SKILL.md", SKILL),
            ("skills/good/assets/data.json", "a"),
        ],
    );
    let b = package(
        manifest(),
        &[
            ("skills/good/SKILL.md", SKILL),
            ("skills/good/assets/data.json", "b"),
        ],
    );
    assert_ne!(
        a.member_digest("skills/good"),
        b.member_digest("skills/good")
    );
    let mut entries = a.entries().clone();
    if let Entry::File { mode, .. } = entries.get_mut("skills/good/assets/data.json").unwrap() {
        *mode = 0o755;
    }
    let c = Snapshot::from_entries(entries, snapshot_limits()).unwrap();
    assert_ne!(a.tree_digest(), c.tree_digest());
}
#[cfg(unix)]
#[test]
fn denied_asset_keeps_valid_skill_but_native_copy_still_rejects_it() {
    use std::os::unix::fs::symlink;
    let source = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::write(
        source.path().join("plugin.json"),
        serde_json::to_vec(&manifest()).unwrap(),
    )
    .unwrap();
    fs::create_dir_all(source.path().join("skills/good/assets")).unwrap();
    fs::write(source.path().join("skills/good/SKILL.md"), SKILL).unwrap();
    fs::write(source.path().join("asset"), "public").unwrap();
    fs::write(outside.path().join("asset"), "private").unwrap();
    symlink("asset", source.path().join("alias")).unwrap();
    symlink(
        outside.path().join("asset"),
        source.path().join("skills/good/assets/escape"),
    )
    .unwrap();
    let snapshot = Snapshot::capture(source.path(), snapshot_limits(), || false).unwrap();
    assert_eq!(snapshot.file("alias"), Some(b"public".as_slice()));
    let plan = load(&snapshot).unwrap();
    assert!(
        matches!(&plan.components[0], ComponentPlan::Skill { member_key, .. } if member_key == "good")
    );
    assert!(!plan.diagnostics.iter().any(|d| d.code == "invalid_skill"));
    assert!(
        plan.diagnostics
            .iter()
            .any(|d| d.code == "denied_package_path" && d.pointer == "skills/good/assets/escape")
    );
    assert!(snapshot.file("skills/good/assets/escape").is_none());
    fs::create_dir_all(source.path().join("skills/escaped")).unwrap();
    fs::write(outside.path().join("SKILL.md"), SKILL).unwrap();
    symlink(
        outside.path().join("SKILL.md"),
        source.path().join("skills/escaped/SKILL.md"),
    )
    .unwrap();
    let escaped_snapshot = Snapshot::capture(source.path(), snapshot_limits(), || false).unwrap();
    let escaped_plan = load(&escaped_snapshot).unwrap();
    assert_eq!(escaped_plan.components.len(), 1);
    assert!(
        escaped_plan
            .diagnostics
            .iter()
            .any(|d| d.code == "invalid_skill" && d.pointer == "skills/escaped/SKILL.md")
    );
    let install_root = tempfile::tempdir().unwrap();
    assert!(
        pioneer_skills::stage_skill_folder(
            &source.path().join("skills/good"),
            install_root.path(),
            &Default::default(),
            true,
        )
        .is_err(),
        "existing native folder security must still reject the symlink"
    );
    assert_eq!(
        fs::read_to_string(outside.path().join("asset")).unwrap(),
        "private"
    );
}
#[test]
fn bounded_capture_can_be_cancelled_without_materialization() {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("plugin.json"), "{}").unwrap();
    assert!(Snapshot::capture(root.path(), snapshot_limits(), || true).is_err());
    assert!(
        Snapshot::capture(
            root.path(),
            Limits {
                entries: 0,
                ..snapshot_limits()
            },
            || false
        )
        .is_err()
    );
}

#[test]
fn spaced_executable_is_one_literal_path_and_args_remain_separate() {
    let root = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("bin")).unwrap();
    let name = "bin/my server ${PLUGIN_ROOT}";
    fs::write(root.path().join(name), "bundled").unwrap();
    let config = json!({"$schema": MCP_SCHEMA, "mcpServers": {
        "spaced": {"type":"stdio", "command":format!("./{name}"), "args":["--title", "two words"]},
        "shell": {"type":"stdio", "command":"node --eval anything"},
        "nul": {"type":"stdio", "command":"./bin/my\u{0} server"}
    }});
    let parsed = parse_mcp(&serde_json::to_vec(&config).unwrap(), 256).unwrap();
    let server = parsed
        .iter()
        .find(|(key, _)| key == "spaced")
        .unwrap()
        .1
        .as_ref()
        .unwrap();
    fs::write(
        root.path().join("plugin.json"),
        serde_json::to_vec(&manifest()).unwrap(),
    )
    .unwrap();
    fs::write(
        root.path().join("mcp.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let snapshot = Snapshot::capture(root.path(), snapshot_limits(), || false).unwrap();
    assert_eq!(load(&snapshot).unwrap().components.len(), 1);
    let ResolvedServer::Stdio { command, args, .. } =
        portable_mcp::resolve(server, root.path(), data.path()).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        command,
        root.path()
            .canonicalize()
            .unwrap()
            .join(name)
            .to_str()
            .unwrap()
    );
    assert_eq!(args, ["--title", "two words"]);
    assert!(
        parsed
            .iter()
            .filter(|(key, _)| key != "spaced")
            .all(|(_, server)| server.is_err())
    );
    #[cfg(unix)]
    {
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("server"), "outside").unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("server"),
            root.path().join("bin/escape server"),
        )
        .unwrap();
        let escaped = PortableServer::Stdio {
            command: "./bin/escape server".into(),
            args: vec![],
            env: BTreeMap::new(),
            cwd: None,
        };
        let captured = Snapshot::capture(root.path(), snapshot_limits(), || false).unwrap();
        assert!(captured.file("bin/escape server").is_none());
        fs::write(
            root.path().join("mcp.json"),
            serde_json::to_vec(&json!({"$schema":MCP_SCHEMA,"mcpServers":{"escape":escaped}}))
                .unwrap(),
        )
        .unwrap();
        let captured = Snapshot::capture(root.path(), snapshot_limits(), || false).unwrap();
        let plan = load(&captured).unwrap();
        assert!(plan.components.is_empty());
        assert!(
            plan.diagnostics
                .iter()
                .any(|d| d.code == "invalid_mcp_path")
        );
        assert!(portable_mcp::resolve(&escaped, root.path(), data.path()).is_err());
    }
    for command in [
        "./../outside server",
        "./bin/my server --flag",
        "sh -c anything",
    ] {
        let server = PortableServer::Stdio {
            command: command.into(),
            args: vec![],
            env: BTreeMap::new(),
            cwd: None,
        };
        assert!(portable_mcp::resolve(&server, root.path(), data.path()).is_err());
    }
}

#[test]
fn invalid_or_denied_skill_markdown_still_skips_only_that_skill() {
    let snapshot = package(
        manifest(),
        &[
            ("skills/good/SKILL.md", SKILL),
            ("skills/invalid/SKILL.md", "missing frontmatter"),
            ("skills/escaped/SKILL.md", SKILL),
        ],
    );
    let mut entries = snapshot.entries().clone();
    entries.insert("skills/escaped/SKILL.md".into(), Entry::Denied);
    let snapshot = Snapshot::from_entries(entries, snapshot_limits()).unwrap();
    let plan = load(&snapshot).unwrap();
    assert_eq!(plan.components.len(), 1);
    assert!(
        matches!(&plan.components[0], ComponentPlan::Skill { member_key, .. } if member_key == "good")
    );
    for path in ["skills/invalid/SKILL.md", "skills/escaped/SKILL.md"] {
        assert!(
            plan.diagnostics
                .iter()
                .any(|d| d.code == "invalid_skill" && d.pointer == path)
        );
    }
    assert!(
        plan.diagnostics
            .iter()
            .any(|d| d.code == "denied_package_path" && d.pointer == "skills/escaped/SKILL.md")
    );
}
