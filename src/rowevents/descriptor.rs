
use std::io::Cursor;
use std::io::Result;
use rowevents::value_type::*;
use byteorder::{LittleEndian, ReadBytesExt};
use rowevents::descriptor_datetime;

pub trait BaseDescriptor {

    fn handle_metadata(&self);
    
}

pub struct IntDescriptor {

}

impl IntDescriptor {

}

impl BaseDescriptor for IntDescriptor {

    fn handle_metadata(&self) {

    }
}

pub fn parse_field(field_type: u8, nullable: bool, metadata1: u8, metadata2: u8, data: &[u8]) -> Result<(ValueType, usize)> {
    
    let (value, offset) = if field_type == FieldType::Tiny as u8 {
        let mut cursor = Cursor::new(&data);
        let v:i8 = cursor.read_i8()?;
        (ValueType::Tinyint(v), 1)
    } else if field_type == FieldType::Short as u8 {
        let mut cursor = Cursor::new(&data);
        let v:i16 = cursor.read_i16::<LittleEndian>()?;
        (ValueType::Shortint(v), 2)
    } else if field_type == FieldType::Long as u8 {
        let mut cursor = Cursor::new(&data);
        let v:i32 = cursor.read_i32::<LittleEndian>()?;
        (ValueType::Int(v), 4)
    } else if field_type == FieldType::Longlong as u8 {
        let mut cursor = Cursor::new(&data);
        let v:i64 = cursor.read_i64::<LittleEndian>()?;        
        (ValueType::Longlong(v), 8)
    } else if field_type == FieldType::Float as u8 {
        let mut cursor = Cursor::new(&data);
        let v:f32 = cursor.read_f32::<LittleEndian>()?;            
        (ValueType::Float(v), 8)
    } else if field_type == FieldType::Double as u8 {
        let mut cursor = Cursor::new(&data);
        let v:f64 = cursor.read_f64::<LittleEndian>()?;         
        (ValueType::Double(v), 8)
    }  else if field_type == FieldType::Datetime2 as u8 {
        (ValueType::Null, 0)
    } else if field_type == FieldType::NewDecimal as u8 {
        (ValueType::Null, 0)
    } else if field_type == FieldType::VarString as u8 {
        (ValueType::String("".to_string()), 0)
    } else if field_type == FieldType::String as u8 {
        (ValueType::String("".to_string()), 0)
    } else if field_type == FieldType::Blob as u8 {
        (ValueType::String("".to_string()), 0)
    } else {
        (ValueType::Unknown, 0)
    };
    Ok((value, offset))
}