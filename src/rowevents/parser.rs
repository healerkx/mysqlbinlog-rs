
use rowevents::stream::Stream;
use rowevents::event_header::EventHeader;
use rowevents::events::*;
use rowevents::value_type;
use rowevents::value_type::*;
use rowevents::descriptor::*;
use byteorder::{LittleEndian, ReadBytesExt};
//use rowevents::value_type::ValueType;

use std::option::Option;
use std::io::Cursor;
use std::io::Result;
use std::io::{Error, ErrorKind};
use std::str;

pub struct Parser {
    stream: Stream,
    field_types: Vec<(u8, bool, u8, u8)>
}

fn get_table_id(i1: i64, i2: i64, i3: i64) -> i64 {
    i1 << 32 + i2 << 16 + i3
}

fn bytes_2_leuint(bytes: &[u8]) -> i64 {
    let mut n: i64 = 0;
    let mut i = 0;
    for b in bytes {
        let m = *b as i64;
        n += m << (i * 8);
        i += 1;
    }
    n
}

fn get_field_length(data: &[u8]) -> (i64, usize) {
    if data.len() == 0 {
        return (-1, 0)
    }
    let v = data[0] as i64;
    if v < 251 {
        return (v, 1)
    } else if v == 251 {
        return (-1, 0)
    }
    let mut size = 9;
    if v == 252 {
        size = 3;
    } else if v == 253 {
        size = 4;
    }

    if data.len() < size {
        return (-1, 0)
    }
    return (bytes_2_leuint(&data[1 .. size]), size)
}

impl Parser {

    pub fn new(stream: Stream) -> Parser {
        Parser{
            stream: stream,
            field_types: Vec::with_capacity(100)
        }
    }

    pub fn read_binlog_file_header(&mut self) -> bool {
        self.stream.read(4);
        true
    }

    pub fn read_next_binlog_file(&mut self) -> bool {
        self.stream.read_next_binlog_file();
        true
    }

    pub fn read_event_header(&mut self) -> Result<EventHeader> {
    
        let data = self.stream.read(19);
        if data.len() > 0 {
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
        } else {
            Err(Error::new(ErrorKind::Other, "Nothing Read!"))
        }
    }

    pub fn read_unknown_event(&mut self, eh: &EventHeader) -> Result<Event> {
        let data = self.stream.read(eh.get_event_len() - 19);
        Ok(Event::Unknown)
    }

    pub fn read_rotate_event(&mut self, eh: &EventHeader) -> Result<Event> {
        Ok(Event::Rotate(RotateEvent::new()))
    }

    pub fn read_format_descriptor_event(&mut self, eh: &EventHeader) -> Result<Event> {
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
        println!("XID={}", xid);
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
            let db_name_data = self.stream.read(db_name_len as usize);
            String::from_utf8_lossy(db_name_data).into_owned()
        };
        self.stream.read(1);    // Read more 1 byte(This byte is for zero-ending?)

        {
            let table_name_len_data = self.stream.read(1);
            let mut cursor = Cursor::new(&table_name_len_data);
            table_name_len = cursor.read_i8()?  as usize
        }
        
        let table_name = {
            let table_name_data = self.stream.read(table_name_len as usize);
            String::from_utf8_lossy(table_name_data).into_owned()
        };

        self.stream.read(1);    // Read more 1 byte(This byte is for zero-ending?)

        let data_len = eh.get_event_len() - 19 - 16 - db_name_len - table_name_len + 4;
        let content = {
            let data = self.stream.read(data_len);
            Vec::from(data)
        };

        {
            self.parse_current_fields_discriptors(&content);
        }

        Ok(Event::TableMap(TableMapEvent::new(db_name, table_name)))
    }

    pub fn read_query_event(&mut self, eh: &EventHeader) -> Result<Event> {
        self.read_unknown_event(eh)
    }

    pub fn read_write_event(&mut self, eh: &EventHeader) -> Result<Event> {
        if let Ok((v, col_count)) = self.read_rows_event(eh, false) {
            let mut from:usize = 0;
            let len = v.len();
            let offset = 0;
            let mut rows = vec![];
            while from < len {
                let (values, offset) = self.parse_row_values(&v[from..], col_count);
                from += offset;
                rows.push(values);
            }
            let e = InsertEvent::new(rows);
            Ok(Event::Insert(e))
        } else {
            // TODO:?
            let e = InsertEvent::new(vec![]);
            Ok(Event::Insert(e))
        }
    }

    pub fn read_update_event(&mut self, eh: &EventHeader) -> Result<Event> {
        if let Ok((v, col_count)) = self.read_rows_event(eh, true) {
            let mut from:usize = 0;
            let len = v.len();
            let offset = 0;
            let mut rows_before = vec![];
            let mut rows_after = vec![];
            while from < len {
                let (values1, offset) = self.parse_row_values(&v[from ..], col_count);
                rows_before.push(values1);
                from += offset;
                let (values2, offset) = self.parse_row_values(&v[from ..], col_count);
                rows_after.push(values2);
                from += offset;
            }
            let e = UpdateEvent::new(rows_before, rows_after);
            Ok(Event::Update(e))
        } else {
            let e = UpdateEvent::new(vec![], vec![]);
            Ok(Event::Update(e))
        }
    }

    pub fn read_delete_event(&mut self, eh: &EventHeader) -> Result<Event> {
        if let Ok((v, col_count)) = self.read_rows_event(eh, false) {
            let mut from:usize = 0;
            let len = v.len();
            let offset = 0;
            let mut rows = vec![];
            while from < len {
                let (v, offset) = self.parse_row_values(&v[from ..], col_count);
                from += offset;
                rows.push(v);
            }
            let e = DeleteEvent::new(rows);
            Ok(Event::Delete(e))
        } else {
            let e = DeleteEvent::new(vec![]);
            Ok(Event::Delete(e))
        }
    }

    pub fn read_rows_event(&mut self, eh: &EventHeader, is_update_event: bool) -> Result<(Vec<u8>, usize)> {
        {
            let data = self.stream.read(8);
            let mut cursor = Cursor::new(&data);
            let i1 = cursor.read_i16::<LittleEndian>()?;
            let i2 = cursor.read_i16::<LittleEndian>()?;
            let i3 = cursor.read_i16::<LittleEndian>()?;
            let table_id = get_table_id(i1 as i64, i2 as i64, i3 as i64);
            let flags = cursor.read_i16::<LittleEndian>()?;
        }
        
        let mut extra_data_len:usize = 2; // MySQL 5.6~
        {
            let data = self.stream.read(extra_data_len as usize);
            let mut cursor = Cursor::new(&data);
            extra_data_len = cursor.read_i16::<LittleEndian>()? as usize;
        }

        if extra_data_len > 2 {
            let extra_data = self.stream.read(extra_data_len - 2);
        }

        let data_len = eh.get_event_len() - 19 - 8 - extra_data_len;
        let (rows_data, col_count) = {
            let data = self.stream.read(data_len - 4);
            let (col_count, size) = get_field_length(data);
            
            let data = &data[size ..];

            let bitmap_size:usize = (col_count as usize + 7) / 8;
            // https://github.com/wenerme/myfacility/blob/master/binlog/row.go#L164
            if is_update_event {
                (Vec::from(&data[bitmap_size * 2 .. ]), col_count)
            } else {
                (Vec::from(&data[bitmap_size .. ]), col_count)
            }
        };

        {
            // For chuncksum
            self.stream.read(4);
        }
        
        Ok((rows_data, col_count as usize))
    }

    fn parse_row_values(&self, data: &[u8], col_count: usize) -> (Vec<ValueType>, usize) {
        
        let mut values = Vec::with_capacity(col_count);
        let mut nulls = Vec::with_capacity(col_count);
        for i in 0 .. col_count {
            let is_null = (data[i / 8] & (1 << (i % 8))) != 0;  // # is_null OK?
            nulls.push(is_null);
        }
        
        let p = (col_count + 7) / 8;
        let data = &data[ p .. ];
        
        let mut from = 0;
        for i in 0 .. col_count {
            if nulls[i] {
                values.push(ValueType::Null);
                continue;
            }
            let remain = &data[from .. ];
            
            let (field_type, nullable, metadata1, metadata2) = self.field_types[i];
            if let Ok((value, offset)) = parse_field(field_type, nullable, metadata1, metadata2, remain) {
                values.push(value);
                from += offset;
            }
        }

        (values, p + from)
    }

    fn parse_current_fields_discriptors(&mut self, data: &Vec<u8>) {
        let (col_count, size) = get_field_length(data);

        let s = size + col_count as usize;
        let col_types = &data[size .. s];
        
        // Rebind
        let data = &data[s .. ];

        let (metadata_size, size) = get_field_length(data);

        let s = size + metadata_size as usize;
        let metadata = &data[size .. s];

        let data = &data[s .. ];

        let nullable_bits_size: usize = (col_count as usize + 7) / 8;
        let nullable_bits = &data[0 .. nullable_bits_size];

        self.set_current_fields_discriptors(col_types, metadata, nullable_bits);
    }

    fn set_current_fields_discriptors(&mut self, col_types: &[u8], metadata: &[u8], nullable_bits: &[u8]) {
        self.field_types.clear();
        
        let col_count = col_types.len();
        let mut nullable_list = Vec::with_capacity(col_count);
        
        for i in 0 .. col_count {
            let bit = (nullable_bits[i / 8]) & (1 << (i % 8));
            nullable_list.push(bit != 0);
        }

        let mut i = 0;
        let mut slice_begin = 0;
        let mut slice_end = 0;
        for col_type in col_types {
            let mut metadata1 = 0;
            let mut metadata2 = 0;
            let md = &metadata[slice_begin ..];
            let col_type = *col_type;
            let nullable = nullable_list[i];
            if col_type == FieldType::VarString as u8 || 
               col_type == FieldType::String as u8 {
            
                metadata1 = md[0];
                metadata2 = md[1];
                slice_begin += 2;
            } else if col_type == FieldType::Datetime2 as u8 {
                metadata1 = md[0];
                slice_begin += 1;
            } else if col_type == FieldType::NewDecimal as u8 {
                metadata1 = md[0];
                metadata2 = md[1];
                slice_begin += 2;
            } else if col_type == FieldType::Varchar as u8 {
                metadata1 = md[0];
                metadata2 = md[1];
                slice_begin += 2;
            } else if col_type == FieldType::Double as u8 ||
                      col_type == FieldType::Float as u8 {
                // What's in metadata for float/double?
                metadata1 = md[0];
                slice_begin += 1;
            } else if col_type == FieldType::Timestamp2 as u8 {
                metadata1 = md[0];
                slice_begin += 1
            }
            
            i += 1;
            // println!("{}-{}-{}-{}", col_type, nullable, metadata1, metadata2);
            self.field_types.push((col_type, nullable, metadata1, metadata2));
        }
    }


}