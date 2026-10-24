// Release-only compatibility entry point. The embedded package is completed
// and signed before this executable is compiled and signed.
include!(concat!(env!("OUT_DIR"), "/pioneer-cu-update-package.rs"));

fn main() {
    pioneer_cli::legacy_cu_update_main(PACKAGE);
}
