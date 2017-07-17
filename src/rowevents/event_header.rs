
use chrono::{NaiveDateTime, NaiveDate};


#[derive(Debug)]
#[repr(C)]
pub struct EventHeader {
    timestamp: i32, 
    type_code: i8,
    server_id: i32, 
    event_len: i32, 
    next_pos: i32, 
    flags: i16
}

impl EventHeader {
    pub fn new(timestamp: i32, type_code: i8, server_id: i32, event_len: i32, next_pos: i32, flags: i16) -> EventHeader {
        EventHeader {
            timestamp: timestamp,
            type_code: type_code,
            server_id: server_id,
            event_len: event_len,
            next_pos: next_pos,
            flags: flags
        }
    }

    pub fn get_time(&self) -> String {
        let dt = NaiveDateTime::from_timestamp(self.timestamp as i64, 0);
        dt.to_string()
    }

    pub fn get_event_len(&self) -> usize {
        self.event_len as usize
    }

    pub fn get_event_type(&self) -> i8 {
        self.type_code
    }
}