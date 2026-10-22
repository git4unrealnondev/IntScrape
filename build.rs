use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::process::Command;

/// File extension used by dynamically linked plugin artifacts on this platform.
fn dylib_extension() -> &'static str {
    if cfg!(windows) {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    }
}

/// File name of a plugin's dynamic library artifact, e.g. `librule34.so`.
fn dylib_file_name(plugin_name: &str) -> String {
    let crate_name = plugin_name.replace('-', "_");
    if cfg!(windows) {
        format!("{crate_name}.dll")
    } else if cfg!(target_os = "macos") {
        format!("lib{crate_name}.dylib")
    } else {
        format!("lib{crate_name}.so")
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    if env::var("PROFILE").is_ok_and(|profile| profile == "debug") {
        return Ok(());
    }

    let profile = env::var("PROFILE").unwrap_or_else(|_| "release".to_string());

    // Docker builds the whole workspace itself and copies the plugin artifacts
    // out of `target/release` afterwards. This flag keeps the build script from
    // compiling the plugins a second time there.
    if env::var("INTSCRAPE_SKIP_PLUGIN_BUILD").is_ok_and(|value| value == "1") {
        return Ok(());
    }

    let plugins_dir = Path::new("plugins");

    // Tell Cargo to watch the build script and the plugins directory.
    //
    // The plugins directory has to be declared explicitly. Cargo's default is to
    // rerun a build script when any file in the package changes, but naming
    // `build.rs` here opts out of that default, so without this line editing a
    // plugin never reruns the script and `compiled_plugins/` keeps serving the
    // previously built shared object. That failure is silent: the build reports
    // success and the stale plugin is what gets loaded at runtime.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", plugins_dir.display());

    if !plugins_dir.exists() {
        println!(
            "cargo:warning=Plugins directory not found at {}",
            plugins_dir.to_string_lossy()
        );
        return Err("Missing plugins directory".into());
    }

    let plugin_names: Vec<String> = fs::read_dir(plugins_dir)?
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| !name.starts_with('.'))
        .collect();

    if plugin_names.is_empty() {
        return Err("No plugin workspace members found in plugins/".into());
    }

    // A build script cannot invoke cargo on the same target directory (the
    // parent cargo holds its lock for the whole build), so plugins compile into
    // a separate cache. Building every plugin in ONE invocation lets cargo
    // parallelize them and pays workspace resolution once instead of once per
    // plugin.
    //
    // Plugins build with the same profile as the main build. Custom release-*
    // profiles place their artifacts in their own target subdirectory; other
    // cargo modes (test, bench, ...) fall back to plain release.
    let plugin_profile = if profile.starts_with("release") {
        profile.as_str()
    } else {
        "release"
    };
    let intermediate_target_dir = Path::new("target/plugins_intermediate");
    fs::create_dir_all(intermediate_target_dir)?;

    let mut command = Command::new("cargo");
    command
        .arg("build")
        .arg("--profile")
        .arg(plugin_profile)
        .arg("--target-dir")
        .arg(intermediate_target_dir);
    for name in &plugin_names {
        command.arg("-p").arg(name);
    }
    let status = command.status()?;
    if !status.success() {
        return Err(format!(
            "Failed to compile plugin workspace members: {}",
            plugin_names.join(", ")
        )
        .into());
    }

    // Copy the freshly built plugin artifacts into the clean final output
    // directory. The copy overwrites previous artifacts one-to-one, so a failed
    // build leaves `compiled_plugins/` untouched.
    let final_output_dir = Path::new("compiled_plugins");
    fs::create_dir_all(final_output_dir)?;
    let built_dir = intermediate_target_dir.join(plugin_profile);
    let extension = dylib_extension();
    let mut fresh_names: HashSet<std::ffi::OsString> = HashSet::new();
    for name in &plugin_names {
        let file_name = dylib_file_name(name);
        let built_path = built_dir.join(&file_name);
        if built_path.exists() {
            fs::copy(&built_path, final_output_dir.join(&file_name))?;
            fresh_names.insert(file_name.into());
        } else {
            println!(
                "cargo:warning=Expected plugin artifact missing at {}",
                built_path.to_string_lossy()
            );
        }
    }

    // Remove shared objects left behind by plugins that no longer exist under
    // `plugins/`, otherwise the host keeps loading them at runtime.
    for entry in fs::read_dir(final_output_dir)?.filter_map(std::result::Result::ok) {
        let file_name = entry.file_name();
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == extension) && !fresh_names.contains(&file_name)
        {
            fs::remove_file(path)?;
        }
    }

    Ok(())
}
