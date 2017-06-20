/**
 * https://github.com/wenerme/myfacility/blob/master/binlog/decimal.go
 * http://python-mysql-replication.readthedocs.io/en/latest/_modules/pymysqlreplication/row_event.html
 *
 * Read MySQL's new decimal format introduced in MySQL 5
 *
 * This project was a great source of inspiration for
 * understanding this storage format.
 * https://github.com/jeremycole/mysql_binlog
 */

use rowevents::value_type::*;
use std::io::Result;
use std::io::Cursor;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

pub fn parse_new_decimal(precision: u8, decimals: u8, data: &[u8]) -> Result<(ValueType, usize)> {
    let digits_per_integer = 9;
    let compressed_bytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    let integral = precision - decimals;
    let uncomp_integral = integral / digits_per_integer;
    let uncomp_fractional = decimals / digits_per_integer;
    let comp_integral = integral - (uncomp_integral * digits_per_integer);
    let comp_fractional = decimals - (uncomp_fractional * digits_per_integer);

    let mut data = Vec::from(data);
    let (negative, mask) = {
        // Support negative
        // The sign is encoded in the high bit of the the byte
        // But this bit can also be used in the value
        let value = data[0];
        let (negative, mask):(bool, u64) = if value & 0x80 != 0 { (false, 0) } else { (true, -1 as i64 as u64) };
        
        // Python3
        // byte = struct.pack('<B', value ^ 0x80)
        // BigEndian.uint8(byte)
        data[0] = value ^ 0x80;
        (negative, mask)
    };

    if (negative) {
        print!("??");
    }
    

    let size = compressed_bytes[comp_integral as usize];

    let mut d = if size > 0 {
        let mut cursor = Cursor::new(&data);
        let i:u64 = cursor.read_uint::<BigEndian>(size)?;
        let a = i ^ mask;
        // Handle u64 value XOR mask in different bits, for example: 24bit int
        let move_bits = 64 - size * 8;
        let a = a << move_bits >> move_bits;
        a.to_string()
    } else {
        "".to_string()
    };

    if negative {
        d = "-".to_string() + &d;
    }

    let mut from:usize = size;
    let remain = &data[from .. ];

    for i in 0 .. uncomp_integral {
        let i = i as usize;
        let b = &remain[i * 4 .. (i + 1) * 4];
        let n = Cursor::new(b).read_u32::<LittleEndian>()? as u64 ^ mask;
        d += &format!("{:09}", n);
        from += (i * 4);
    }

    d += ".";

    let remain = &data[from .. ];
    for i in 0 .. uncomp_fractional {
        let i = i as usize;
        let b = &remain[i * 4 .. (i + 1) * 4];
        let n = Cursor::new(b).read_u32::<LittleEndian>()? as u64 ^ mask;
        d += &format!("{:09}", n);
        from += (i * 4);
    }

    let size = compressed_bytes[comp_fractional as usize];
    
    let remain = &data[from .. ];
    if size > 0 {
        let i = Cursor::new(remain).read_uint::<BigEndian>(size)? as u64;
        let value = i ^ mask;
        // Handle u64 value XOR mask in different bits, for example: 24bit int
        let move_bits = 64 - size * 8;
        let value = value << move_bits >> move_bits;
        d += &format!("{:0w$}", value, w=comp_fractional as usize);
        from += size;
    }

    Ok((ValueType::Decimal(d), from))
}
