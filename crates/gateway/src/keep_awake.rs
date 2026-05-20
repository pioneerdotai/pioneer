use anyhow::{Context, Result};
use keepawake::{Builder, KeepAwake};
use std::sync::Mutex;
use tracing::info;

#[derive(Default)]
pub(crate) struct GatewayKeepAwake {
    handle: Mutex<Option<KeepAwake>>,
}

impl GatewayKeepAwake {
    pub(crate) fn set_enabled(&self, enabled: bool) -> Result<()> {
        let mut handle = self
            .handle
            .lock()
            .expect("gateway keepawake lock should not be poisoned");

        if enabled {
            if handle.is_none() {
                let awake = Builder::default()
                    .display(false)
                    .idle(true)
                    .sleep(false)
                    .reason("AI Agent running")
                    .create()
                    .context("failed to prevent system sleep")?;
                *handle = Some(awake);
                info!("gateway keepawake enabled");
            }
        } else if handle.take().is_some() {
            info!("gateway keepawake disabled");
        }

        Ok(())
    }
}
