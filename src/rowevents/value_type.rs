

pub enum FieldType {
    Unknown = -1,
    Decimal = 0,
    Tiny = 1,
    Short = 2,
    Long = 3,
    Float = 4,
    Double = 5,
    Null = 6,
    Timestamp = 7,
    Longlong = 8,
    Int24 = 9,
    Date = 10,
    Time = 11,
    Datetime = 12,
    Year = 13,
    Newdate = 14,
    Varchar = 15,
    Bit = 16,
    Timestamp2 = 17,
    Datetime2 = 18,
    Time2 = 19,
    NewDecimal = 246,
    Enum = 247,
    Set = 248,
    TinyBlob = 249,
    MediumBlob = 250,
    LongBlob = 251,
    Blob = 252,
    VarString = 253,
    String = 254,
    Geometry = 255
}

pub enum ValueType {
    Null,
    Tinyint(i8),
    Int(i32),
    Chars(String)

}