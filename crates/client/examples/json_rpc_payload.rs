use pioneer_client::rpc::{build_json_rpc_request_payload, decode_json_rpc_response_value};
use pioneer_protocol::{JSONRPC_VERSION, JsonRpcRequest};
use serde_json::json;

fn main() -> anyhow::Result<()> {
    let payload = build_json_rpc_request_payload(
        "workspace/list",
        json!({
            "include_archived": false
        }),
    )?;

    let request: JsonRpcRequest = serde_json::from_str(payload.payload.as_str())?;
    assert_eq!(request.jsonrpc, JSONRPC_VERSION);
    assert_eq!(request.id.as_str(), payload.request_id);
    assert_eq!(request.method, "workspace/list");

    let response = json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": payload.request_id,
        "result": {
            "workspaces": []
        }
    });

    let (response_id, result) =
        decode_json_rpc_response_value(&response).expect("response should include id");
    assert_eq!(response_id, request.id.as_str());
    assert_eq!(
        result.map_err(anyhow::Error::new)?,
        json!({"workspaces": []})
    );

    println!("built JSON-RPC request {}", request.id.as_str());
    Ok(())
}
