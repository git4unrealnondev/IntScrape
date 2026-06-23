use std::fs::{self, create_dir_all};
use std::path::Path;
use std::process::Command;

fn main() {
    // 1. Path to the plugins folder relative to your app's directory
    let plugins_dir = Path::new("../plugins");

    // 2. Tell Cargo to only rerun this script if files in the plugins folder change.
    println!("cargo:rerun-if-changed=../plugins");

    if !plugins_dir.exists() {
        println!(
            "cargo:warning=Plugins directory not found at {:?}",
            plugins_dir
        );
        return;
    }

    let entries = fs::read_dir(plugins_dir)
        .expect("Failed to read plugins directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir());

    for entry in entries {
        let plugin_name = entry.file_name().into_string().unwrap();

        // Skip hidden folders (like .git or .vscode)
        if plugin_name.starts_with('.') {
            continue;
        }

        println!(
            "cargo:warning=Building plugin workspace member: {}",
            plugin_name
        );

        // Escape app directory using `../`
        create_dir_all("../target/plugins_build").expect("Failed to create target dir");

        // 3. Execute compilation using Cargo's package flag
        let status = Command::new("cargo")
            .env_clear() // CRITICAL: Erases parent Cargo locks/flags
            .env("PATH", std::env::var("PATH").unwrap_or_default()) // Restores system path so cargo can run
            .args([
                "build",
                "-p",
                &plugin_name,
                "--release",
                // CRITICAL: Must be outside the app folder to prevent deadlocks
                "--target-dir",
                "../plugins_compiled",
            ])
            .status()
            .expect("Failed to run cargo build for plugin");

        if !status.success() {
            panic!("Failed to compile plugin: {}", plugin_name);
        }
    }
}
