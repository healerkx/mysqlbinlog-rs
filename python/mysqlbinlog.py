

from ctypes import *

class EventType:
    UNKNOWN_EVENT = 0
    START_EVENT_V3 = 1
    QUERY_EVENT = 2
    STOP_EVENT = 3
    ROTATE_EVENT = 4
    INTVAR_EVENT = 5
    LOAD_EVENT = 6
    SLAVE_EVENT = 7
    CREATE_FILE_EVENT = 8
    APPEND_BLOCK_EVENT = 9
    EXEC_LOAD_EVENT = 10
    DELETE_FILE_EVENT = 11
    NEW_LOAD_EVENT = 12
    RAND_EVENT = 13
    USER_VAR_EVENT = 14
    FORMAT_DESCRIPTION_EVENT = 15
    XID_EVENT = 16
    BEGIN_LOAD_QUERY_EVENT = 17
    EXECUTE_LOAD_QUERY_EVENT = 18
    TABLE_MAP_EVENT  = 19
    PRE_GA_WRITE_ROWS_EVENT  = 20
    PRE_GA_UPDATE_ROWS_EVENT  = 21
    PRE_GA_DELETE_ROWS_EVENT  = 22
    """
    From MySQL 5.1.18 events
    """    
    WRITE_ROWS_EVENT  = 23
    UPDATE_ROWS_EVENT  = 24
    DELETE_ROWS_EVENT  = 25
    # ----------------------------------

    INCIDENT_EVENT = 26
    HEARTBEAT_LOG_EVENT = 27

    """
    From MySQL 5.6.2 events
    """
    WRITE_ROWS_EVENT2 = 30
    UPDATE_ROWS_EVENT2 = 31
    DELETE_ROWS_EVENT2 = 32
    # ----------------------------------


class EventHeader(Structure):
    _fields_ = [
        ("timestamp", c_int),
        ("type_code", c_byte),
        ("server_id", c_int),
        ("event_len", c_int),
        ("next_pos", c_int),
        ("flags", c_short)
        ]

    def __str__(self):
        return "< %d %d >" % ( self.timestamp, self.type_code)



class EventInfo(Structure):
    _fields_ = [
        ("row_count", c_int),
        ("col_count", c_int)
    ]

    def __str__(self):
        return "(Row:%d Col:%d)" % (self.row_count, self.col_count)

    def __del__(self):
        print("###")



class BinLogReader:
    """
    """

    def __init__(self, filename):
        self.d = cdll.LoadLibrary('/Users/healer/Projects/privates/mysqlbinlog-rs/target/debug/libmysqlbinlog.dylib')
        self.d.binlog_reader_new.restype = c_void_p
        self.reader = self.d.binlog_reader_new(bytes(filename, 'utf8'))


    def close(self):
        self.d.binlog_reader_free.argtypes = [c_void_p]
        self.d.binlog_reader_free(self.reader)

    def read_event_header(self, header):
        self.d.binlog_reader_read_event_header.restype = c_byte
        self.d.binlog_reader_read_event_header.argtypes = [c_void_p, POINTER(EventHeader)]
        b = self.d.binlog_reader_read_event_header(self.reader, byref(header))
        return b

    def read_event(self, header):
        self.d.binlog_reader_read_event.restype = c_void_p
        self.d.binlog_reader_read_event.argtypes = [c_void_p, POINTER(EventHeader)]
        
        event = self.d.binlog_reader_read_event(self.reader, byref(header))
        
        return event
        
        event_type = header.type_code
        if event_type == EventType.WRITE_ROWS_EVENT2:
            pass
        elif event_type == EventType.UPDATE_ROWS_EVENT2:
            pass
        elif event_type == EventType.DELETE_ROWS_EVENT2:
            pass
        elif event_type == EventType.XID_EVENT:
            pass
        elif event_type == EventType.TABLE_MAP_EVENT:
            pass
        elif event_type == EventType.ROTATE_EVENT:
            pass
        elif event_type == EventType.STOP_EVENT:
            pass                                             
        elif event_type == EventType.UNKNOWN_EVENT:
            pass
        else:
            pass
    
    def read_event_info(self, event):
        self.d.binlog_reader_read_event_info.restype = c_bool
        self.d.binlog_reader_read_event_info.argtypes = [c_void_p, POINTER(EventInfo)]
        info = EventInfo()
        self.d.binlog_reader_read_event_info(event, byref(info))

    def free_event(self, event):
        self.d.binlog_reader_free_event.restype = c_bool
        self.d.binlog_reader_free_event.argtypes = [c_void_p]
        return self.d.binlog_reader_free_event(event)