
use rowevents::value_type::ValueType;

pub const UNKNOWN_EVENT: i8 = 0;
pub const START_EVENT_V3: i8 = 1;
pub const QUERY_EVENT: i8 = 2;
pub const STOP_EVENT: i8 = 3;
pub const ROTATE_EVENT: i8 = 4;
pub const INTVAR_EVENT: i8 = 5;
pub const LOAD_EVENT: i8 = 6;
pub const SLAVE_EVENT: i8 = 7;
pub const CREATE_FILE_EVENT: i8 = 8;
pub const APPEND_BLOCK_EVENT: i8 = 9;
pub const EXEC_LOAD_EVENT: i8 = 10;
pub const DELETE_FILE_EVENT: i8 = 11;
pub const NEW_LOAD_EVENT: i8 = 12;
pub const RAND_EVENT: i8 = 13;
pub const USER_VAR_EVENT: i8 = 14;
pub const FORMAT_DESCRIPTION_EVENT: i8 = 15;
pub const XID_EVENT: i8 = 16;
pub const BEGIN_LOAD_QUERY_EVENT: i8 = 17;
pub const EXECUTE_LOAD_QUERY_EVENT: i8 = 18;
pub const TABLE_MAP_EVENT: i8  = 19;
pub const PRE_GA_WRITE_ROWS_EVENT: i8  = 20;
pub const PRE_GA_UPDATE_ROWS_EVENT: i8  = 21;
pub const PRE_GA_DELETE_ROWS_EVENT: i8  = 22;
    
// From MySQL 5.1.18 events
pub const WRITE_ROWS_EVENT: i8    = 23;
pub const UPDATE_ROWS_EVENT: i8    = 24;
pub const DELETE_ROWS_EVENT: i8    = 25;
// # ----------------------------------
pub const INCIDENT_EVENT: i8   = 26;
pub const HEARTBEAT_LOG_EVENT: i8   = 27;

// From MySQL 5.6.2 events 
pub const WRITE_ROWS_EVENT2: i8 = 30;
pub const UPDATE_ROWS_EVENT2: i8 = 31;
pub const DELETE_ROWS_EVENT2: i8 = 32;


pub struct FormatDescriptorEvent {

}

#[derive(Debug)]
pub struct XidEvent {
    xid: i64
}

#[derive(Debug)]
pub struct TableMapEvent {

}

pub struct DeleteEvent {
    
}

pub struct InsertEvent {
    entry: Vec<ValueType>
}

pub struct UpdateEvent {
    entry1: Vec<ValueType>,
    entry2: Vec<ValueType>
}

impl FormatDescriptorEvent {
    pub fn new() -> FormatDescriptorEvent {
         FormatDescriptorEvent{}
    }
}

impl XidEvent {
    pub fn new(xid: i64) -> XidEvent {
         XidEvent{ xid: xid }
    }  
}

impl TableMapEvent {
    pub fn new() -> TableMapEvent {
         TableMapEvent{}
    }  
}

impl InsertEvent {
    pub fn new(entry: Vec<ValueType>) -> InsertEvent {
         InsertEvent{entry: entry}
    }
}


pub enum Event {

    FormatDescriptor(FormatDescriptorEvent),
    Xid(XidEvent),
    TableMap(TableMapEvent),
    Delete(DeleteEvent),
    Insert(InsertEvent),
    Update(UpdateEvent),
}
