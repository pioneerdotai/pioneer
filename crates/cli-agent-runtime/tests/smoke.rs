use pioneer_cli_agent_runtime as runtime;

#[test]
fn imports_cli_agent_runtime_crate() {
    assert_eq!(runtime::CLI_AGENT_RUNTIME_BOUNDARY, "cli-agent-runtime");
}
