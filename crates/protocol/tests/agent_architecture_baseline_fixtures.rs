use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const FIXTURE: &str = include_str!("fixtures/agent_architecture_baseline/graph.json");

fn fixture() -> Value {
    serde_json::from_str(FIXTURE).expect("agent domain baseline fixture must be valid JSON")
}

fn strings(value: &Value, key: &str) -> BTreeSet<String> {
    value[key]
        .as_array()
        .expect("fixture field must be an array")
        .iter()
        .map(|item| {
            item.as_str()
                .expect("fixture item must be a string")
                .to_owned()
        })
        .collect()
}

#[test]
fn baseline_fixture_defines_required_workspace_graph_without_network() {
    let fixture = fixture();
    assert_eq!(fixture["schema_version"], 1);
    assert_eq!(fixture["network"], "none");
    assert_eq!(fixture["workspace"]["id"], "W");
    assert_eq!(
        strings(&fixture["workspace"], "participants"),
        BTreeSet::from(["Alice".to_owned(), "Bob".to_owned(), "Carol".to_owned()])
    );

    let roots = fixture["roots"].as_array().expect("roots must be an array");
    let root_ids: BTreeSet<_> = roots
        .iter()
        .map(|root| root["id"].as_str().expect("root id must be a string"))
        .collect();
    assert_eq!(root_ids, BTreeSet::from(["A", "B", "C", "D"]));
    assert_eq!(roots[0]["members"], serde_json::json!(["Alice", "Bob"]));
    assert_eq!(roots[1]["visibility"], "private");
    assert_eq!(roots[2]["members"], serde_json::json!(["Carol"]));
    assert_eq!(roots[3]["visibility"], "workspace");
}

#[test]
fn baseline_fixture_keeps_native_configs_and_cli_instances_distinct() {
    let fixture = fixture();
    let native_configs = fixture["native_configs"]
        .as_array()
        .expect("native_configs must be an array");
    assert_eq!(native_configs.len(), 2);
    assert!(
        native_configs
            .iter()
            .all(|config| config["runtime_id"] == "native-runtime-1")
    );
    assert_ne!(native_configs[0]["id"], native_configs[1]["id"]);

    let cli_instances = fixture["cli_instances"]
        .as_array()
        .expect("cli_instances must be an array");
    let mut ids = BTreeSet::new();
    let mut binaries = BTreeSet::new();
    let mut homes = BTreeSet::new();
    let mut nicknames = BTreeSet::new();
    for instance in cli_instances {
        ids.insert(instance["id"].as_str().expect("instance id"));
        binaries.insert(instance["binary"].as_str().expect("instance binary"));
        homes.insert(instance["home"].as_str().expect("instance home"));
        nicknames.insert(instance["nickname"].as_str().expect("instance nickname"));
    }
    assert_eq!(ids, BTreeSet::from(["claude-h1", "codex-c1", "codex-c2"]));
    assert_eq!(binaries.len(), cli_instances.len());
    assert_eq!(homes.len(), cli_instances.len());
    assert_eq!(nicknames.len(), cli_instances.len());
}

#[test]
fn baseline_fixture_contains_live_restart_and_revoked_routes() {
    let fixture = fixture();
    let routes = fixture["routes"]
        .as_array()
        .expect("routes must be an array");
    let by_id: BTreeMap<_, _> = routes
        .iter()
        .map(|route| (route["id"].as_str().expect("route id"), route))
        .collect();
    assert_eq!(by_id["route-live"]["status"], "live");
    assert_eq!(by_id["route-restart"]["status"], "restart_pending");
    assert_eq!(by_id["route-revoked"]["status"], "revoked");
    assert_eq!(
        by_id["route-live"]["actions"],
        serde_json::json!(["send_message"])
    );
    assert_eq!(
        by_id["route-restart"]["actions"],
        serde_json::json!(["send_message", "start_agent"])
    );
}

#[test]
fn baseline_fixture_covers_all_resource_failure_and_fairness_scenarios() {
    let fixture = fixture();
    let scenarios = fixture["scenarios"]
        .as_array()
        .expect("scenarios must be an array");
    let ids: BTreeSet<_> = scenarios
        .iter()
        .map(|scenario| scenario["id"].as_str().expect("scenario id"))
        .collect();
    assert_eq!(
        ids,
        BTreeSet::from([
            "sequential-chain-depth-boundary",
            "parallel-fanout-permit-saturation",
            "observation-loss-one-child",
            "local-payload-overflow-one-sibling",
            "fair-runnable-branch",
            "route-state-replay",
        ])
    );

    for scenario in scenarios {
        assert!(
            scenario["target_invariants"]
                .as_array()
                .is_some_and(|invariants| !invariants.is_empty())
        );
    }

    let observation_loss = scenarios
        .iter()
        .find(|scenario| scenario["id"] == "observation-loss-one-child")
        .expect("observation-loss scenario");
    assert_eq!(
        observation_loss["observed_baseline"]["unknown_observation"],
        "treated_as_terminal"
    );

    let overflow = scenarios
        .iter()
        .find(|scenario| scenario["id"] == "local-payload-overflow-one-sibling")
        .expect("payload-overflow scenario");
    assert_eq!(
        overflow["target_invariants"],
        serde_json::json!([
            "request_fails_exactly",
            "shared_transport_stays_usable",
            "siblings_continue"
        ])
    );

    let fairness = scenarios
        .iter()
        .find(|scenario| scenario["id"] == "fair-runnable-branch")
        .expect("fairness scenario");
    assert_eq!(fairness["waiting_sibling"], "codex-c2");
    assert!(
        fairness["target_invariants"]
            .as_array()
            .expect("fairness invariants")
            .iter()
            .any(|invariant| invariant == "waiting_sibling_eventually_gets_permit")
    );
}
