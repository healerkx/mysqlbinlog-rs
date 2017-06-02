
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;
use std::io::Result;
use rowevents::value_type::*;
use chrono::{NaiveDateTime, NaiveDate};

pub fn parse_datetime2(ms_precision: u8, metadata2: u8, data: &[u8]) -> Result<(ValueType, usize)> {
    
    let mut cursor = Cursor::new(&data[0 .. 5]);
    let t:i64 = cursor.read_int::<BigEndian>(5)? as i64 - 0x8000000000;

    let (msec, offset) = if ms_precision == 1 || ms_precision == 2 {
        let mut cursor = Cursor::new(&data[5 .. 6]);
        let msec:i64 = cursor.read_i8()? as i64 * 10000;
        (msec, 6)
    } else if ms_precision == 3 || ms_precision == 4 {
        let mut cursor = Cursor::new(&data[5 .. 7]);
        let msec:i64 = cursor.read_i16::<BigEndian>()? as i64 * 100;        
        (msec, 7)
    } else if ms_precision == 5 || ms_precision == 6 {
        let mut cursor = Cursor::new(&data[5 .. 8]);
        let msec:i64 = cursor.read_int::<BigEndian>(3)? as i64;  
        (msec, 8)
    } else {
        (0, 5)
    };

    let ymd = t >> 17;
    let ym = ymd >> 5;
    let hms = t % (1 << 17);

    let day = ymd % (1 << 5);
    let month = ym % 13;
    let year = ym / 13;

    let second = hms % (1 << 6);
    let minute = (hms >> 6) % (1 << 6);
    let hour = hms >> 12;
    let nd = NaiveDate::from_ymd(year as i32, month as u32, day as u32)
                        .and_hms_milli(hour as u32, minute as u32, second as u32, msec as u32);

    Ok((ValueType::Datetime2(nd.timestamp()), offset))

}