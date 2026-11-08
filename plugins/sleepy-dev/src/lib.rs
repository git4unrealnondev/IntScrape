use shared_types::GlobalCallbacks;

#[unsafe(no_mangle)]
fn get_plugin_info() -> shared_types::CVec<u8> {
    // The host consumes this via `plugin_ffi::take` (see `src/plugins.rs`):
    // a bitcode encoding of `Vec<Plugin>` in an owned byte buffer.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(get_plugin_info_impl));
    match result {
        Ok(value) => shared_types::plugin_ffi::encode(&value),
        Err(_) => shared_types::plugin_ffi::encode(&Vec::<shared_types::Plugin>::new()),
    }
}

fn get_plugin_info_impl() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "SleepyDev".into(),
        callbacks: vec![GlobalCallbacks::Start(
            shared_types::StartupThreadType::Spawn,
        )],
        ..Default::default()
    }]
}

#[unsafe(no_mangle)]
pub fn on_start() {
    // A panicking plugin must not unwind across the FFI boundary into the
    // host (which builds with panic = "abort").
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(on_start_impl));
}

#[unsafe(no_mangle)]
pub fn on_start_impl() {
    use std::{thread, time};
    let wait = time::Duration::from_secs(1);
    loop {
        let server_status = client::should_exit();

        if server_status.is_err() || server_status.unwrap() {
            break;
        }
        thread::sleep(wait);
    }
}
