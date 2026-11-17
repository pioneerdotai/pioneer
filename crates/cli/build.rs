#[path = "../observability/build_support.rs"]
mod build_support;
#[path = "../gateway/build_support.rs"]
mod cua_build_support;
fn main() {
    println!(
        "cargo:rustc-env=PIONEER_CUA_TARGET={}",
        std::env::var("TARGET").unwrap()
    );
    cua_build_support::macos_link_inputs();
    build_support::manifest();
}
