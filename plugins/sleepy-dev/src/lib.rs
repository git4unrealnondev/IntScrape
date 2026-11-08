use shared_types::GlobalCallbacks;

#[unsafe(no_mangle)]
fn get_plugin_info() -> shared_types::CVec<shared_types::Plugin> {
    // The host consumes this via `CVec::into_vec` (see `src/plugins.rs`).
    // The inner helper keeps the original `Vec` body unchanged.
    shared_types::CVec::from_vec(get_plugin_info_impl())
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
