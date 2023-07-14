use cargo_lock::Lockfile;
use std::env;
use std::path::Path;

fn main() {
    let lock_file_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock");
    let lock_file = Lockfile::load(lock_file_path)
        .unwrap_or_else(|err| panic!("Could not load Cargo.lock file: {}", err));

    for package in lock_file.packages {
        if package.name.as_str() == "casper-node" {
            println!("cargo:rustc-env=CASPER_NODE_VERSION={}", package.version);
        }
    }
}
