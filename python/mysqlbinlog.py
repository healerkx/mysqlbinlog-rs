

from ctypes import *

"""
timestamp: i32, 
type_code: i8,
server_id: i32, 
event_len: i32, 
next_pos: i32, 
flags: i16
"""
class StructPointer(Structure):
    _fields_ = [
        ("timestamp", c_int),
        ("type_code", c_byte),
        ("server_id", c_int),
        ("event_len", c_int),
        ("next_pos", c_int),
        ("flags", c_short)
        ]  

class BinLogReader:

    def __init__(self, filename):
        self.d = cdll.LoadLibrary('/Users/healer/Projects/privates/mysqlbinlog-rs/target/debug/libmysqlbinlog.dylib')
        self.d.binlog_reader_new.restype = c_void_p
        self.reader = self.d.binlog_reader_new(bytes(filename, 'utf8'))


    def __delattr__(self):
        print("End1")
        self.d.binlog_reader_free(self.reader)
        print("End")

    def read_event_header(self):
        self.d.binlog_reader_read_event_header.restype = POINTER(StructPointer)
        self.d.binlog_reader_read_event_header.argtypes = [c_void_p]
        header = self.d.binlog_reader_read_event_header(self.reader)
        return header