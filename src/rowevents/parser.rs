
use rowevents::stream::Stream;
use rowevents::event_header::EventHeader;
use rowevents::events::*;
use byteorder::{LittleEndian, ReadBytesExt};
use rowevents::value_type::ValueType;
use std::option::Option;
use std::io::Cursor;
use std::io::Result;

pub struct Parser {
    stream: Stream
}

impl Parser {

    pub fn new(stream: Stream) -> Parser {
        Parser{
            stream: stream
        }
    }


    pub fn read_binlog_file_header(&mut self) -> bool {
        self.stream.read(4);
        true
    }

    pub fn read_event_header(&mut self) -> Result<EventHeader> {
    
        let data = self.stream.read(19);
        
        let mut cursor = Cursor::new(&data);
    
        let timestamp = cursor.read_i32::<LittleEndian>()?;
        let type_code = cursor.read_i8()?;
        let server_id = cursor.read_i32::<LittleEndian>()?;
        let event_len = cursor.read_i32::<LittleEndian>()?;
        let next_pos = cursor.read_i32::<LittleEndian>()?;
        let flags = cursor.read_i16::<LittleEndian>()?;
    
        Ok(EventHeader::new(
            timestamp,
            type_code,
            server_id,
            event_len,
            next_pos,
            flags
        ))
    }

    pub fn read_event(&mut self, eh: &EventHeader) -> Result<Event> {
        if eh.get_event_type() == UNKNOWN_EVENT {

        }
        let data = self.stream.read(eh.get_event_len() - 19);

        let entry: Vec<ValueType> = vec![];
        Ok(Event::Insert(InsertEvent::new(entry)))
    }

    pub fn read_rotate_event(&mut self, eh: &EventHeader) -> Result<Event> {
        self.read_event(eh)
    }

    pub fn read_format_descriptor_event(&mut self, eh: &EventHeader) -> Result<Event> {
        self.read_event(eh)
    }


    pub fn read_xid_event(&mut self, eh: &EventHeader) -> Result<Event> {
        self.read_event(eh)
    }

    pub fn read_table_map_event(&mut self, eh: &EventHeader) -> Result<Event> {
        self.read_event(eh)
    }


    pub fn read_query_event(&mut self, eh: &EventHeader) -> Result<Event> {
        self.read_event(eh)
    }

}