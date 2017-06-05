
use std::io::Cursor;
use std::io::Result;
use rowevents::value_type::*;
use byteorder::{LittleEndian, ReadBytesExt};
use rowevents::descriptor_datetime::*;


fn parse_string(metadata1: u8, metadata2: u8, data: &[u8]) -> Result<(ValueType, usize)> {
    let max_len = metadata1 + metadata2 * 256;
    let strlen = data[0] as usize;
    let v = Vec::from(&data[1 .. strlen + 1]);
    Ok((ValueType::String(v), strlen + 1))
}

fn parse_varchar(metadata1: u8, metadata2: u8, data: &[u8]) -> Result<(ValueType, usize)> {
    let max_len: u32 = metadata1 as u32 + metadata2 as u32 * 256;
    let mut cursor = Cursor::new(&data);
    
    let (from, strlen) = if max_len < 256 {
        (1, cursor.read_u8()? as usize)
    } else {
        (2, cursor.read_i16::<LittleEndian>()? as usize)
    };
    
    let v = Vec::from(&data[from .. strlen + from]);
    Ok((ValueType::String(v), strlen + from))
}

fn parse_blob(metadata1: u8, metadata2: u8, data: &[u8]) -> Result<(ValueType, usize)> {
    let max_len = metadata1 + metadata2 * 256;
    let mut cursor = Cursor::new(&data);
    let strlen = cursor.read_i16::<LittleEndian>()? as usize;
    let v = Vec::from(&data[2 .. strlen + 2]);
    Ok((ValueType::String(v), strlen + 2)) 
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
        (ValueType::Float(v), 4)
    } else if field_type == FieldType::Double as u8 {
        let mut cursor = Cursor::new(&data);
        let v:f64 = cursor.read_f64::<LittleEndian>()?;         
        (ValueType::Double(v), 8)
    }  else if field_type == FieldType::Datetime2 as u8 {
        let (dt, offset) = parse_datetime2(metadata1, metadata2, data)?;
        (dt, offset)
    } else if field_type == FieldType::NewDecimal as u8 {
        (ValueType::Null, 0)
    } else if field_type == FieldType::Varchar as u8 {
        let (strval, offset) = parse_varchar(metadata1, metadata2, data)?;
        (strval, offset)
    } else if field_type == FieldType::String as u8 {
        let (strval, offset) = parse_string(metadata1, metadata2, data)?;
        (strval, offset)
    } else if field_type == FieldType::Blob as u8 {
        let (strval, offset) = parse_blob(metadata1, metadata2, data)?;
        (strval, offset)
    } else {
        (ValueType::Unknown, 0)
    };
    Ok((value, offset))
}