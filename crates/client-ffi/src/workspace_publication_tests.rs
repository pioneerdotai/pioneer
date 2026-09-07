use crate::client_binding::snapshot_dto;
use pioneer_client::core::{ClientCore, ClientScope};
use pioneer_protocol::{GatewayNotification, Thread, ThreadReadCursor, ThreadUpdatedNotification};
use serde_json::{Value, json};
use std::{num::NonZeroUsize, sync::Arc};

fn thread(id: &str, workspace: &str, updated: i64) -> Thread {
    serde_json::from_value(json!({"id":id,"workspace_id":workspace,"name":id,"preview":"","mode":"Chat","model":"model","model_provider":"provider","created_at":1,"updated_at":updated,"status":"Idle","turns":[]})).unwrap()
}

#[test]
fn workspace_directory_direct_and_wire_scopes_reject_stale_and_noop_inputs() {
    let direct = Arc::new(ClientCore::new());
    let ffi = Arc::new(ClientCore::new());
    let scopes = [
        ClientScope::WorkspaceTree {
            workspace_id: Some("a".into()),
        },
        ClientScope::WorkspaceTree {
            workspace_id: Some("b".into()),
        },
    ];
    for core in [&direct, &ffi] {
        core.upsert_thread(thread("a1", "a", 1));
        core.upsert_thread(thread("a2", "a", 2));
        core.upsert_thread(thread("b1", "b", 1));
    }
    let initial = scopes
        .iter()
        .map(|scope| snapshot_dto(direct.snapshot(scope).unwrap()))
        .collect::<Vec<_>>();
    let before_b = direct.workspace_tree("b").unwrap();
    let b_sink = direct.subscribe(scopes[1].clone(), NonZeroUsize::new(16).unwrap());
    let navigation = direct.navigation_snapshot();
    for core in [&direct, &ffi] {
        let cursor = ThreadReadCursor {
            sort_key: "2".into(),
            through_turn_id: "turn".into(),
        };
        core.apply_directory_read("a", "a1", &cursor, 4);
        let publication = core.workspace_tree("a").unwrap();
        core.apply_directory_read("a", "a1", &cursor, 4);
        core.apply_directory_read("b", "a1", &cursor, 8);
        core.apply_directory_read(
            "a",
            "a1",
            &ThreadReadCursor {
                sort_key: "1".into(),
                ..cursor
            },
            8,
        );
        assert!(Arc::ptr_eq(
            &publication,
            &core.workspace_tree("a").unwrap()
        ));
        let mut updated = thread("a2", "a", 3);
        updated.preview = "new preview".into();
        core.apply_thread_notification(GatewayNotification::ThreadUpdated(
            ThreadUpdatedNotification {
                thread: updated,
                placement: None,
            },
        ));
        let publication = core.workspace_tree("a").unwrap();
        core.upsert_thread(thread("a2", "a", 1));
        core.upsert_thread(thread("a2", "wrong", 9));
        assert!(Arc::ptr_eq(
            &publication,
            &core.workspace_tree("a").unwrap()
        ));
        core.remove_thread_store("a1");
    }
    let updated = scopes
        .iter()
        .map(|scope| snapshot_dto(ffi.snapshot(scope).unwrap()))
        .collect::<Vec<_>>();
    for (scope, expected) in scopes.iter().zip(&updated) {
        assert_eq!(
            serde_json::to_value(snapshot_dto(direct.snapshot(scope).unwrap())).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }
    assert!(Arc::ptr_eq(&before_b, &direct.workspace_tree("b").unwrap()));
    assert!(b_sink.try_next().is_none());
    assert_eq!(navigation.as_ref(), direct.navigation_snapshot().as_ref());
    let wire = json!({"initial":initial,"updated":updated});
    assert_eq!(
        wire,
        serde_json::from_str::<Value>(include_str!(
            "../tests/fixtures/workspace-directory-wire.json"
        ))
        .unwrap()
    );
}
