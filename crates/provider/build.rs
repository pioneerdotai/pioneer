use std::{env, fs, path::PathBuf};

const BUNDLED_CATALOG_ENV: &str = "PIONEER_BUNDLED_MODEL_CATALOG";
const OUTPUT_FILE: &str = "bundled_model_catalog.json";
const MAX_CATALOG_BYTES: u64 = 64 * 1024 * 1024;

fn main() {
    println!("cargo:rerun-if-env-changed={BUNDLED_CATALOG_ENV}");
    let output = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is missing")).join(OUTPUT_FILE);
    let bytes = match env::var_os(BUNDLED_CATALOG_ENV) {
        Some(path) => {
            let path = PathBuf::from(path);
            println!("cargo:rerun-if-changed={}", path.display());
            let metadata = fs::metadata(&path).unwrap_or_else(|error| {
                panic!(
                    "failed to inspect bundled model catalog {}: {error}",
                    path.display()
                )
            });
            assert!(
                metadata.is_file() && metadata.len() <= MAX_CATALOG_BYTES,
                "bundled model catalog must be a regular file no larger than {MAX_CATALOG_BYTES} bytes"
            );
            fs::read(&path).unwrap_or_else(|error| {
                panic!(
                    "failed to read bundled model catalog {}: {error}",
                    path.display()
                )
            })
        }
        None => Vec::new(),
    };
    fs::write(&output, bytes).unwrap_or_else(|error| {
        panic!(
            "failed to stage bundled model catalog {}: {error}",
            output.display()
        )
    });
}
