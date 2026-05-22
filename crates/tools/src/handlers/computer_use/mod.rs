mod backend;
mod handler;
mod model;
mod state;
mod util;

#[cfg(test)]
mod tests;

pub use handler::ComputerUseHandler;

use crate::registry::ToolHandler;
use crate::spec::computer_use_configured_spec;
use crate::{ComputerUseToolsConfig, ToolExtensionBundle};
use std::sync::Arc;

pub fn materialize_computer_use_domain_bundle(
    config: ComputerUseToolsConfig,
) -> ToolExtensionBundle {
    let handler: Arc<dyn ToolHandler> = Arc::new(ComputerUseHandler::new(config));
    ToolExtensionBundle {
        specs: vec![computer_use_configured_spec()],
        handlers: vec![("computer_use".to_owned(), handler)],
    }
}
