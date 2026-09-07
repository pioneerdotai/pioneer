//! Independently retained Desktop update workflow and presentation.
mod check;
mod controller;
mod snapshot;
mod updater;
mod view;
pub use controller::{
    DesktopUpdateCompletion, DesktopUpdateController, DesktopUpdateOperation, DesktopUpdatePlan,
    DesktopUpdatePort, DesktopUpdateStore, DesktopUpdateTransition, NativeDesktopUpdatePort,
};
pub use snapshot::DesktopUpdateSnapshot;
pub use updater::relaunch::{DesktopPostUpdateReceipt, claim_post_update_receipt};
pub use view::{
    ApplyUpdate, CancelUpdate, CheckForUpdate, DesktopUpdateConfig, DesktopUpdateView,
    DownloadUpdate,
};
pub fn desktop_current_version() -> &'static str {
    updater::desktop_current_version()
}
