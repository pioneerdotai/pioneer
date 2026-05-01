#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pioneer_gateway::run_gateway_until_shutdown().await
}
