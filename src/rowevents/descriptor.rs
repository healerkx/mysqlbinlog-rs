
use rowevents::value_type::*;

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

pub fn parse_field(field_type: u8, nullable: bool, metadata1: u8, metadata2: u8, data: &[u8]) -> Option<(ValueType, usize)> {
    
    let (value, offset) = if field_type == FieldType::Tiny as u8 {
        (ValueType::Tinyint(0), 1)
    } else if field_type == FieldType::Short as u8 {
        (ValueType::Shortint(0), 2)
    } else if field_type == FieldType::Long as u8 {
        (ValueType::Int(0), 4)
    } else if field_type == FieldType::Longlong as u8 {
        (ValueType::Longlong(0), 8)
    } else if field_type == FieldType::Float as u8 {
        (ValueType::Float(0.0), 8)
    } else if field_type == FieldType::Double as u8 {
        (ValueType::Double(0.0), 8)
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
    Some((value, offset))
}