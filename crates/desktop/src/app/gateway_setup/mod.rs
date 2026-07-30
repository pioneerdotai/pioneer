mod dialog;
mod view;

pub(crate) use view::{GatewaySetupDialogState, GatewaySetupFormState, render_gateway_setup_form};

pub(crate) const GATEWAY_SETUP_FORM_WIDTH_PX: f32 = 300.0;
pub(crate) const GATEWAY_SETUP_DIALOG_WIDTH_PX: f32 = 350.0;
pub(crate) const GATEWAY_SETUP_INITIAL_CARD_WIDTH_PX: f32 = 334.0;
