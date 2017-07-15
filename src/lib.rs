
pub use rowevents::reader::{ Reader };
pub use rowevents::stream::{ Stream };
pub use rowevents::parser::{ Parser };
pub use rowevents::event_header::{ EventHeader };
pub use rowevents::events::*;
pub use rowevents::value_type::*;
pub use rowevents::descriptor::*;

pub mod rowevents;

extern crate byteorder;
extern crate chrono;
extern crate regex;


use std::ffi::{CString, CStr}; 
use std::os::raw::c_char;
use std::ptr;

#[no_mangle]
pub extern fn binlog_reader_new(filename: *const c_char) -> *mut Reader {
    let c = unsafe {    
        let cstr = CStr::from_ptr(filename);
        cstr.to_string_lossy().into_owned()
    };
    if let Ok(reader) = Reader::new(&c) {
        Box::into_raw(Box::new(reader))
    } else {
        ptr::null_mut()
    }
}

#[no_mangle]
pub extern fn binlog_reader_free(reader: *mut Reader) {
    if reader.is_null() { 
        return 
    }
    unsafe { 
        Box::from_raw(reader); 
    }
}

#[no_mangle]
pub extern fn binlog_reader_read_event(reader: *mut Reader) {
    if reader.is_null() {
        return 
    }
    
}
