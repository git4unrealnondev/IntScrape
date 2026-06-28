use shared_types::{GlobalCallbacks, SearchHolder, SearchObj};

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
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
        thread::sleep(wait);
        dbg!(client::search_db_files(
            SearchObj {
                searches: vec![SearchHolder::And(vec![18])],
                search_relate: None
            },
            None
        ));
    }
}
