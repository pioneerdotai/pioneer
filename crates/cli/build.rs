#[path = "../observability/build_support.rs"]
mod build_support;
#[path = "../gateway/build_support.rs"]
mod cua_build_support;
fn main() {
    cua_build_support::macos_link_inputs();
    build_support::manifest();
}
