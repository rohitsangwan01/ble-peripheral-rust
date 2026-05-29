use std::os::raw::{c_char, c_void};

pub const DISPATCH_QUEUE_SERIAL: *const c_void = 0 as *const c_void;

#[cfg_attr(target_os = "macos", link(name = "AppKit", kind = "framework"))]
#[link(name = "Foundation", kind = "framework")]
#[link(name = "CoreBluetooth", kind = "framework")]
extern "C" {
    pub fn dispatch_queue_create(label: *const c_char, attr: *const c_void) -> *mut c_void;
}
