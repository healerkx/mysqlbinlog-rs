
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
use std::rc::Rc;

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
pub extern fn binlog_reader_read_event_header(ptr: *mut Reader, in_header: *mut EventHeader) -> bool {
    let mut reader = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };
    
    if let Ok(ref mut header) = reader.read_event_header() {
        // Copy for avoid alloc too much heap-memory
        unsafe {
            (*in_header).type_code = header.type_code;
            (*in_header).timestamp = header.timestamp;
            (*in_header).server_id = header.server_id;
            (*in_header).event_len = header.event_len;
            (*in_header).next_pos = header.next_pos;
            (*in_header).flags = header.flags
        }
        true
    } else {
        false
    }
}

///////////////////////////////////////
// For C code read the event
#[derive(Debug)]
#[repr(C)]
pub struct EventInfo {
    pub type_code: u8,
    pub db_name_len: u32,
    pub table_name_len: u32, 
    pub row_count: u32,
    pub col_count: u32
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
pub extern fn binlog_reader_read_event_info(ptr: *mut Event, info: *mut EventInfo) -> bool {
    let event = unsafe {
        assert!(!ptr.is_null());
        &*ptr
    };

    match event {
        &Event::TableMap(ref e) => {
            unsafe {
                (*info).db_name_len = e.db_name.len() as u32;
                (*info).table_name_len = e.table_name.len() as u32;
            }
        },
        
        &Event::Insert(ref e) => {
            unsafe {
                (*info).row_count = e.entry.len() as u32;
                (*info).col_count = e.entry[0].len() as u32;
            }
        },

        &Event::Delete(ref e) => {
            unsafe {
                (*info).row_count = e.entry.len() as u32;
                (*info).col_count = e.entry[0].len() as u32;
            }
        },

        &Event::Update(ref e) => {
            unsafe {
                (*info).row_count = e.entry1.len() as u32;
                (*info).col_count = e.entry1[0].len() as u32;
            }
        },

        _ => {assert!(false)}
    }

    true
}

#[no_mangle]
pub extern fn binlog_reader_read_table_map_event(ptr: *mut Event, info: *mut EventInfo, db_name: *mut u8, table_name: *mut u8) -> bool {
    let event = unsafe {
        assert!(!ptr.is_null());
        &*ptr
    };

    match event {
        &Event::TableMap(ref e) => {
            unsafe {
                ptr::copy_nonoverlapping(e.db_name.as_bytes().as_ptr(), db_name, e.db_name.len());
                ptr::copy_nonoverlapping(e.table_name.as_bytes().as_ptr(), table_name, e.table_name.len());
            }
        },
        _ => {}
    }
    true
}

#[no_mangle]
pub extern fn binlog_reader_free_event(ptr: *mut Event) -> bool {
    if ptr.is_null() {
        return false;
    }
    unsafe {
        Box::from_raw(ptr);
    }
    return true;
}
