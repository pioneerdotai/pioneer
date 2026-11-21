use pioneer_crud_execution::agent_grant::{
    agent_execution_grant_fingerprint, parse_agent_execution_launch_grant,
    upgrade_agent_execution_grant_model_to_current,
};
use pioneer_entity::agent_execution_grant;

fn version_one_child_launch_grant() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/child-launch-grant-v1.json")).unwrap()
}

#[test]
fn execution_grant_history_verifies_v1_and_preserves_both_original_checksums() {
    let child = version_one_child_launch_grant();
    let grant_json = serde_json::json!({
        "kind": "test",
        "identity": child["identities"][0].clone(),
        "profile": child["profiles"][0].clone(),
        "child_launch_grant": child
    })
    .to_string();
    let stored_fingerprint =
        agent_execution_grant_fingerprint(grant_json.as_str()).expect("stored fingerprint");
    let model = agent_execution_grant::Model {
        id: "G00000000000000000001".to_owned(),
        execution_id: "E00000000000000000001".to_owned(),
        parent_execution_id: None,
        child_identity_id: "A00000000000000000001".to_owned(),
        grant_fingerprint: stored_fingerprint.clone(),
        grant_json,
        created_at: chrono::Utc::now().fixed_offset(),
    };

    let upgraded = upgrade_agent_execution_grant_model_to_current(model.clone())
        .expect("valid version 1 grant should upcast");
    let upgraded_json: serde_json::Value =
        serde_json::from_str(upgraded.grant_json.as_str()).expect("upgraded grant JSON");
    assert_eq!(
        upgraded_json["child_launch_grant"]["version"],
        serde_json::json!(1)
    );
    assert_eq!(upgraded.grant_json, model.grant_json);
    assert_eq!(upgraded.grant_fingerprint, stored_fingerprint);
    let (_, _, child) = parse_agent_execution_launch_grant(&upgraded.grant_json).unwrap();
    assert!(child.validate_for_execution().is_err());
    let mut widened_profile = model.clone();
    let mut outer: serde_json::Value = serde_json::from_str(&widened_profile.grant_json).unwrap();
    outer["profile"]["modelId"] = serde_json::json!("not-in-historical-ceiling");
    widened_profile.grant_json = outer.to_string();
    widened_profile.grant_fingerprint =
        agent_execution_grant_fingerprint(&widened_profile.grant_json).unwrap();
    assert!(upgrade_agent_execution_grant_model_to_current(widened_profile).is_err());
    let mut broken_child = model.clone();
    let mut outer: serde_json::Value = serde_json::from_str(&broken_child.grant_json).unwrap();
    outer["child_launch_grant"]["allowServerDerivedEphemeral"] = serde_json::json!(true);
    broken_child.grant_json = outer.to_string();
    broken_child.grant_fingerprint =
        agent_execution_grant_fingerprint(&broken_child.grant_json).unwrap();
    assert!(
        upgrade_agent_execution_grant_model_to_current(broken_child).is_err(),
        "a valid outer checksum cannot replace the nested integrity check"
    );
    assert_eq!(
        upgraded.grant_fingerprint,
        agent_execution_grant_fingerprint(upgraded.grant_json.as_str())
            .expect("upgraded outer fingerprint")
    );

    let mut tampered = model;
    tampered.grant_json = tampered
        .grant_json
        .replace("model-opaque", "tampered-model");
    assert!(upgrade_agent_execution_grant_model_to_current(tampered).is_err());
}

#[test]
fn persisted_execution_grant_projection_restores_large_authenticated_snapshot() {
    let migrated = pioneer_protocol::migrate_embedded_child_launch_grant_json_to_current(
        &serde_json::json!({"child_launch_grant":version_one_child_launch_grant()}).to_string(),
    )
    .unwrap();
    let migrated: serde_json::Value = serde_json::from_str(&migrated).unwrap();
    let seed: pioneer_protocol::ChildAgentLaunchGrantSet =
        serde_json::from_value(migrated["child_launch_grant"].clone()).unwrap();
    let identities = (0..140)
        .map(|n| {
            let mut identity = seed.identities[0].clone();
            identity.id = pioneer_protocol::AgentIdentityId::new(format!("A{n:020}")).unwrap();
            identity
        })
        .collect::<Vec<_>>();
    let mut profile: pioneer_protocol::AgentExecutionProfileProjection =
        serde_json::from_value(version_one_child_launch_grant()["profiles"][0].clone()).unwrap();
    profile.compatible_agent_identity_ids = vec![identities[0].id.clone()];
    let ceiling = pioneer_protocol::ChildAgentLaunchGrantSet::new(identities.clone()).unwrap();
    let grant_json = serde_json::json!({"identity":identities[0],"profile":profile,"child_launch_grant":ceiling}).to_string();
    let model = agent_execution_grant::Model {
        id: "G00000000000000000001".into(),
        execution_id: "E00000000000000000001".into(),
        parent_execution_id: None,
        child_identity_id: identities[0].id.to_string(),
        grant_fingerprint: agent_execution_grant_fingerprint(&grant_json).unwrap(),
        grant_json,
        created_at: chrono::Utc::now().fixed_offset(),
    };
    let restored = upgrade_agent_execution_grant_model_to_current(model.clone()).unwrap();
    assert_eq!(restored.grant_json, model.grant_json);
    assert_eq!(restored.grant_fingerprint, model.grant_fingerprint);
    let (_, _, recovered) = parse_agent_execution_launch_grant(&restored.grant_json).unwrap();
    assert_eq!(recovered, ceiling);
    let mut foreign = model;
    foreign.child_identity_id = "Z00000000000000000001".into();
    assert!(upgrade_agent_execution_grant_model_to_current(foreign).is_err());
}
