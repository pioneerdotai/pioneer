use anyhow::{Context, Result, bail};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    let output = arguments
        .next()
        .map(PathBuf::from)
        .context("usage: pioneer-model-catalog <output-file> [proxy-url]")?;
    let proxy = arguments
        .next()
        .map(|value| value.to_string_lossy().into_owned());
    if arguments.next().is_some() {
        bail!("usage: pioneer-model-catalog <output-file> [proxy-url]");
    }
    pioneer_provider::catalog::runtime::generate_catalog_file(&output, proxy.as_deref()).await
}
