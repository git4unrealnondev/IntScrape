use std::env;
use std::fs::{self, create_dir_all};
use std::path::Path;
use std::process::Command;

fn main() {
    if Ok("debug".to_owned()) == env::var("PROFILE") {
        return;
    }

    let plugins_dir = Path::new("plugins");

    // Tell Cargo to watch the build script and the plugins directory
    println!("cargo:rerun-if-changed=build.rs");

    if !plugins_dir.exists() {
        println!(
            "cargo:warning=Plugins directory not found at {:?}",
            plugins_dir
        );
        return;
    }

    // Create our clean final output directory
    let final_output_dir = Path::new("compiled_plugins");
    create_dir_all(final_output_dir).expect("Failed to create final output dir");

    // Use a hidden or separate target folder for cargo's messy build cache
    let intermediate_target_dir = "target/plugins_intermediate";
    create_dir_all(intermediate_target_dir).expect("Failed to create intermediate target dir");

    let entries = fs::read_dir(plugins_dir)
        .expect("Failed to read plugins directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir());

    for entry in entries {
        let plugin_name = entry.file_name().into_string().unwrap();

        if plugin_name.starts_with('.') {
            continue;
        }

        // Dynamically watch each plugin's folder changes
        let plugin_path = entry.path();
        println!("cargo:rerun-if-changed={}", plugin_path.to_string_lossy());
        if plugin_path.join("src").exists() {
            println!(
                "cargo:rerun-if-changed={}/src",
                plugin_path.to_string_lossy()
            );
        }

        println!(
            "cargo:warning=Building plugin workspace member: {}",
            plugin_name
        );

        // 1. Build the plugin into the intermediate directory
        let status = Command::new("cargo")
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .args([
                "build",
                "-p",
                &plugin_name,
                "--release",
                "--target-dir",
                intermediate_target_dir,
            ])
            .status()
            .expect("Failed to run cargo build for plugin");

        if !status.success() {
            panic!("Failed to compile plugin: {}", plugin_name);
        }

        // 2. Locate the compiled artifact
        // Adjust these filenames if your plugins are dynamic libraries (.so / .dll / .dylib)
        // instead of standard binary executables!
        let binary_name = if cfg!(windows) {
            format!("{}.dll", plugin_name)
        } else if cfg!(target_os = "macos") {
            format!("lib{}.dylib", plugin_name)
        } else {
            format!("lib{}.so", plugin_name)
        };

        let built_binary_path = Path::new(intermediate_target_dir)
            .join("release")
            .join(&binary_name);

        let destination_path = final_output_dir.join(&binary_name);

        if built_binary_path.exists() {
            // 3. Copy the fresh binary to your clean `compiled_plugins` folder
            fs::copy(&built_binary_path, &destination_path)
                .expect("Failed to copy compiled plugin to final destination");

            // 4. Strip symbols out of the copied binary to minimize file size
            println!("cargo:warning=Stripping symbols from {}", binary_name);

            let strip_status = Command::new("strip").arg(&destination_path).status();

            // If the system doesn't have 'strip' (like a bare Windows machine), log a warning instead of crashing
            if strip_status.is_err() || !strip_status.unwrap().success() {
                println!(
                    "cargo:warning=Could not strip binary (unsupported OS or missing 'strip' utility). File copied un-stripped."
                );
            }
        } else {
            println!(
                "cargo:warning=Expected binary missing at {:?}",
                built_binary_path
            );
        }
    }
}
