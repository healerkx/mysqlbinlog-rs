
use rowevents::stream::Stream;
use rowevents::event_header::EventHeader;
use rowevents::events::*;
use byteorder::{LittleEndian, ReadBytesExt};
use rowevents::value_type::ValueType;
use std::option::Option;
use std::io::Cursor;
use std::io::Result;
use std::str;

pub struct Parser {
    stream: Stream
}

fn get_table_id(i1: i64, i2: i64, i3: i64) -> i64 {
    i1 << 32 + i2 << 16 + i3
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
        /*
        if eh.get_event_type() == EventTypes::UNKNOWN_EVENT {

        }
        */
        let data = self.stream.read(eh.get_event_len() - 19);

        let entry: Vec<ValueType> = vec![];
        Ok(Event::Insert(InsertEvent::new(entry)))
    }

    pub fn read_rotate_event(&mut self, eh: &EventHeader) -> Result<Event> {
        self.read_event(eh)
    }

    pub fn read_format_descriptor_event(&mut self, eh: &EventHeader) -> Result<Event> {
        //self.read_event(eh);
        {
            let data = self.stream.read(57);
        }

        let length_array = self.stream.read(eh.get_event_len() - (57 + 19));
        Ok(Event::FormatDescriptor(FormatDescriptorEvent::new()))
    }


    pub fn read_xid_event(&mut self, eh: &EventHeader) -> Result<Event> {
        let data = self.stream.read(12);
        let mut cursor = Cursor::new(&data);
        let xid = cursor.read_i64::<LittleEndian>()?;
        Ok(Event::Xid(XidEvent::new(xid)))
    }

    pub fn read_table_map_event(&mut self, eh: &EventHeader) -> Result<Event> {
        let mut db_name_len = 0;
        let mut table_name_len = 0;
        {
            let data = self.stream.read(9);
            let mut cursor = Cursor::new(&data);
            let i1 = cursor.read_i16::<LittleEndian>()?;
            let i2 = cursor.read_i16::<LittleEndian>()?;
            let i3 = cursor.read_i16::<LittleEndian>()?;
            let table_id = get_table_id(i1 as i64, i2 as i64, i3 as i64);
            let flags = cursor.read_i16::<LittleEndian>()?;
            db_name_len = cursor.read_i8()? as usize;
        }

        let db_name = {
            let db_name_data = self.stream.read(db_name_len as usize + 1);
            String::from_utf8_lossy(db_name_data).into_owned()
        };

        {
            let table_name_len_data = self.stream.read(1);
            let mut cursor = Cursor::new(&table_name_len_data);
            table_name_len = cursor.read_i8()?  as usize
        }

        let table_name = {
            let table_name_data = self.stream.read(table_name_len as usize + 1);
            String::from_utf8_lossy(table_name_data).into_owned()
        };

        println!("{}.{}", db_name, table_name);

        let data_len = eh.get_event_len() - 19 - 16 - db_name_len - table_name_len + 4;
        {
            let data = self.stream.read(data_len);

            // TODO: chunksum
        }
        Ok(Event::TableMap(TableMapEvent{}))
    }


    pub fn read_query_event(&mut self, eh: &EventHeader) -> Result<Event> {
        self.read_event(eh)
    }

}