use cfg_if::cfg_if;

pub fn get_sys_time_in_secs() -> u64 {
    use std::time::SystemTime;
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

///
/// Clears memory if called
///
pub fn memory_manage() {
    cfg_if! {
        if #[cfg(target_env = "gnu")] {
            unsafe {
        libc::malloc_trim(0);
    }
               } else {
        }
    }
}
