
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
        let p = Box::into_raw(Box::new(reader));
        println!("{:?}", p);
        p
    } else {
        ptr::null_mut()
    }
}

#[no_mangle]
pub extern fn binlog_reader_free(ptr: *mut Reader) {
    if ptr.is_null() {
        return 
    }
    unsafe { 
        Box::from_raw(ptr);
    }
}

#[no_mangle]
pub extern fn binlog_reader_read_event_header(ptr: *mut Reader) -> *mut EventHeader {
    let mut reader = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };
    
    if let Ok(header) = reader.read_event_header() {
        Box::into_raw(Box::new(header))
    } else {
        ptr::null_mut()
    }
}

#[no_mangle]
pub extern fn binlog_reader_read_event(ptr: *mut Reader, header: *mut EventHeader) -> *mut Event {
    let mut reader = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };

    let header = unsafe {
        assert!(!header.is_null());
        &mut *header
    };
    
    if let Ok(event) = reader.read_event_detail(&header) {
        Box::into_raw(Box::new(event))
    } else {
        ptr::null_mut()
    }
}

#[no_mangle]
pub extern fn binlog_reader_parse_event(event: *mut Event) {
    let event = unsafe {
        assert!(!header.is_null());
        &mut *event
    };
}